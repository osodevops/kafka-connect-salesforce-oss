package sh.oso.salesforce.pubsub;

import org.apache.avro.generic.GenericRecord;

/** A decoded Pub/Sub event: Avro payload plus checkpointing metadata. */
public record DecodedEvent(
        String topicName,
        String eventId,
        String schemaId,
        GenericRecord payload,
        byte[] replayId) {
}
