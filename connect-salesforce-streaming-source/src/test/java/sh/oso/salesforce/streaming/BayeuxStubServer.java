package sh.oso.salesforce.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Protocol-level stub of the Salesforce Streaming API's CometD/Bayeux endpoint:
 * handshake / connect / subscribe with the Salesforce replay extension
 * ({@code ext.replay}), a retained-event buffer honoring -1/-2/N semantics, and
 * invalid-replayId rejection — the wire contract a durable subscriber depends on.
 */
final class BayeuxStubServer implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpServer server;
    private final List<RetainedEvent> retained = new ArrayList<>();
    private final Map<String, Long> deliveredUpTo = new ConcurrentHashMap<>();
    private final AtomicLong replaySequence = new AtomicLong();
    private volatile long minValidReplayId = Long.MIN_VALUE;
    private volatile String subscribedChannel;
    private volatile Long requestedReplay;

    private record RetainedEvent(String channel, Map<String, Object> data, long replayId) {
    }

    BayeuxStubServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/cometd/", this::handle);
        server.start();
    }

    String endpoint() {
        return "http://localhost:" + server.getAddress().getPort() + "/cometd/60.0/";
    }

    /** Publishes an event with an auto-assigned replayId; delivered to a live subscriber. */
    synchronized long publish(String channel, Map<String, Object> data) {
        long replayId = replaySequence.incrementAndGet();
        Map<String, Object> withMeta = new HashMap<>(data);
        @SuppressWarnings("unchecked")
        Map<String, Object> eventMeta =
                new HashMap<>((Map<String, Object>) data.getOrDefault("event", Map.of()));
        eventMeta.put("replayId", replayId);
        withMeta.put("event", eventMeta);
        retained.add(new RetainedEvent(channel, withMeta, replayId));
        return replayId;
    }

    /** Events with replayId below this are "expired": subscribing at them fails. */
    void setMinValidReplayId(long value) {
        minValidReplayId = value;
    }

    Long lastRequestedReplay() {
        return requestedReplay;
    }

    private void handle(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
        JsonNode request = MAPPER.readTree(exchange.getRequestBody());
        ArrayNode response = MAPPER.createArrayNode();
        for (JsonNode message : request) {
            dispatch(message, response);
        }
        byte[] body = MAPPER.writeValueAsBytes(response);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private synchronized void dispatch(JsonNode message, ArrayNode response) {
        String channel = message.path("channel").asText();
        String id = message.path("id").asText(null);
        switch (channel) {
            case "/meta/handshake" -> {
                ObjectNode reply = reply(channel, id, true);
                reply.put("clientId", "stub-client");
                reply.put("version", "1.0");
                reply.putArray("supportedConnectionTypes").add("long-polling");
                response.add(reply);
            }
            case "/meta/connect" -> {
                ObjectNode reply = reply(channel, id, true);
                ObjectNode advice = reply.putObject("advice");
                advice.put("reconnect", "retry");
                advice.put("interval", 50);
                response.add(reply);
                deliverPending(response);
            }
            case "/meta/subscribe" -> {
                String subscription = message.path("subscription").asText();
                long replayFrom = message.path("ext").path("replay").path(subscription)
                        .asLong(SalesforceStreamingSourceTask.REPLAY_LATEST);
                requestedReplay = replayFrom;
                if (replayFrom >= 0 && replayFrom < minValidReplayId) {
                    ObjectNode reply = reply(channel, id, false);
                    reply.put("subscription", subscription);
                    reply.put("error", "400::The replayId {" + replayFrom + "} you provided was invalid");
                    response.add(reply);
                    return;
                }
                subscribedChannel = subscription;
                long startAfter = replayFrom == SalesforceStreamingSourceTask.REPLAY_ALL
                        ? Long.MIN_VALUE
                        : replayFrom == SalesforceStreamingSourceTask.REPLAY_LATEST
                        ? replaySequence.get()
                        : replayFrom;
                deliveredUpTo.put(subscription, startAfter);
                ObjectNode reply = reply(channel, id, true);
                reply.put("subscription", subscription);
                response.add(reply);
            }
            case "/meta/unsubscribe", "/meta/disconnect" -> response.add(reply(channel, id, true));
            default -> response.add(reply(channel, id, true));
        }
    }

    private void deliverPending(ArrayNode response) {
        String channel = subscribedChannel;
        if (channel == null) {
            return;
        }
        long after = deliveredUpTo.getOrDefault(channel, Long.MIN_VALUE);
        for (RetainedEvent event : retained) {
            if (event.channel().equals(channel) && event.replayId() > after) {
                ObjectNode delivery = MAPPER.createObjectNode();
                delivery.put("channel", channel);
                delivery.set("data", MAPPER.valueToTree(event.data()));
                response.add(delivery);
                after = event.replayId();
            }
        }
        deliveredUpTo.put(channel, after);
    }

    private static ObjectNode reply(String channel, String id, boolean successful) {
        ObjectNode reply = MAPPER.createObjectNode();
        reply.put("channel", channel);
        if (id != null) {
            reply.put("id", id);
        }
        reply.put("successful", successful);
        return reply;
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
