package sh.oso.salesforce.streaming;

import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSession;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.http.jetty.JettyHttpClientTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.auth.SessionSupplier;
import sh.oso.salesforce.common.SalesforceException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * CometD/Bayeux client for the legacy Salesforce Streaming API with durable replay:
 * bearer-token auth on every HTTP request and the Salesforce replay extension
 * ({@code ext.replay = {channel: replayId}}) on subscribe.
 */
final class StreamingClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingClient.class);

    private final SessionSupplier sessions;
    private final String endpoint;
    private final String channel;
    private final long connectTimeoutMs;
    private final AtomicLong replayId;
    private final Consumer<Map<String, Object>> onEvent;
    private final AtomicBoolean invalidReplay = new AtomicBoolean();

    private HttpClient httpClient;
    private BayeuxClient bayeux;

    StreamingClient(SessionSupplier sessions, String endpoint, String channel, long replayFrom,
                    long connectTimeoutMs, Consumer<Map<String, Object>> onEvent) {
        this.sessions = sessions;
        this.endpoint = endpoint;
        this.channel = channel;
        this.replayId = new AtomicLong(replayFrom);
        this.connectTimeoutMs = connectTimeoutMs;
        this.onEvent = onEvent;
    }

    void connect() {
        try {
            httpClient = new HttpClient();
            httpClient.start();
        } catch (Exception e) {
            throw new SalesforceException("Failed to start CometD HTTP client", e);
        }
        Map<String, Object> options = new HashMap<>();
        JettyHttpClientTransport transport = new JettyHttpClientTransport(options, httpClient) {
            @Override
            protected void customize(Request request) {
                request.headers(headers ->
                        headers.put("Authorization", "Bearer " + sessions.session().accessToken()));
            }
        };
        bayeux = new BayeuxClient(endpoint, transport);
        bayeux.addExtension(new ReplayExtension());
        bayeux.handshake();
        if (!bayeux.waitFor(connectTimeoutMs, BayeuxClient.State.CONNECTED)) {
            throw new SalesforceException("CometD handshake with " + endpoint + " timed out", null, true);
        }
        subscribe();
    }

    private void subscribe() {
        java.util.concurrent.CountDownLatch replied = new java.util.concurrent.CountDownLatch(1);
        bayeux.getChannel(channel).subscribe(
                (ClientSessionChannel c, Message message) -> {
                    Object data = message.getData();
                    if (data instanceof Map<?, ?> map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> event = (Map<String, Object>) map;
                        onEvent.accept(event);
                    }
                },
                (Message reply) -> {
                    if (!reply.isSuccessful()) {
                        String error = String.valueOf(reply.get(Message.ERROR_FIELD));
                        LOG.warn("Subscribe to {} failed: {}", channel, error);
                        if (error != null && error.toLowerCase().contains("replayid")) {
                            invalidReplay.set(true);
                        }
                    } else {
                        LOG.info("Subscribed to {} (replay from {})", channel, replayId.get());
                    }
                    replied.countDown();
                });
        try {
            if (!replied.await(connectTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                throw new SalesforceException("Subscribe to " + channel + " timed out", null, true);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SalesforceException("Interrupted while subscribing to " + channel, e);
        }
    }

    /** True when the server rejected the replayId; caller resets replay and resubscribes. */
    boolean isReplayInvalid() {
        return invalidReplay.get();
    }

    void resubscribeFrom(long newReplayId) {
        invalidReplay.set(false);
        replayId.set(newReplayId);
        subscribe();
    }

    void updateReplayId(long value) {
        replayId.set(value);
    }

    boolean isConnected() {
        return bayeux != null && bayeux.isConnected();
    }

    @Override
    public void close() {
        if (bayeux != null) {
            bayeux.disconnect();
            bayeux.waitFor(5_000, BayeuxClient.State.DISCONNECTED);
        }
        if (httpClient != null) {
            try {
                httpClient.stop();
            } catch (Exception e) {
                LOG.warn("Error stopping CometD HTTP client", e);
            }
        }
    }

    /** Salesforce durable-replay extension: sends the resume position on subscribe. */
    private final class ReplayExtension implements ClientSession.Extension {
        @Override
        public boolean sendMeta(ClientSession session, Message.Mutable message) {
            if (Channel.META_SUBSCRIBE.equals(message.getChannel())) {
                Map<String, Object> ext = message.getExt(true);
                Map<String, Object> replay = new HashMap<>();
                replay.put(channel, replayId.get());
                ext.put("replay", replay);
            }
            return true;
        }
    }
}
