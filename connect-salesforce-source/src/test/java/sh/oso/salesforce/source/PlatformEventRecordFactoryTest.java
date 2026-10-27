package sh.oso.salesforce.source;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.pubsub.DecodedEvent;
import sh.oso.salesforce.testing.TestSchemas;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PlatformEventRecordFactoryTest {

    static final String EVENT = "Integration_Message__e";

    final PlatformEventRecordFactory factory = new PlatformEventRecordFactory(EVENT, "salesforce." + EVENT);

    private static DecodedEvent event(String schemaId, GenericRecord payload, long replay) {
        return new DecodedEvent("/event/" + EVENT, "uuid-" + replay, schemaId, payload,
                ByteBuffer.allocate(8).putLong(replay).array());
    }

    @Test
    void cachesValueSchemaPerSchemaId() {
        Schema v1 = TestSchemas.integrationMessageEventSchema();
        Schema v2 = TestSchemas.integrationMessageEventSchemaV2();
        SourceRecord a = factory.fromEvent(event("s1", TestSchemas.integrationMessage(v1, "A", 1, "{}"), 1), Map.of());
        SourceRecord b = factory.fromEvent(event("s1", TestSchemas.integrationMessage(v1, "B", 2, "{}"), 2), Map.of());
        SourceRecord c = factory.fromEvent(event("s2", TestSchemas.integrationMessage(v2, "C", 3, "{}"), 3), Map.of());

        assertThat(b.valueSchema()).isSameAs(a.valueSchema());
        assertThat(c.valueSchema()).isNotSameAs(a.valueSchema());
        assertThat(c.valueSchema().field("Initiator_Id__c")).isNotNull();
    }

    @Test
    void unsetEventFieldsAreNull() {
        Schema schema = TestSchemas.integrationMessageEventSchema();
        GenericRecord payload = TestSchemas.integrationMessage(schema, "A", 1, null);
        Struct value = (Struct) factory.fromEvent(event("s1", payload, 1), Map.of()).value();

        assertThat(value.get("Payload__c")).isNull();
        assertThat(value.getString("ReplayId")).isEqualTo("1");
        assertThat(value.getString("EventUuid")).isEqualTo("uuid-1");
    }

    @Test
    void doesNotDuplicateReplayIdOrEventUuidCarriedInThePayload() {
        Schema schema = SchemaBuilder.record(EVENT).namespace("com.sforce.eventbus").fields()
                .name("CreatedDate").type().longType().noDefault()
                .name("EventUuid").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                .name("ReplayId").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                .endRecord();
        GenericRecord payload = new GenericData.Record(schema);
        payload.put("CreatedDate", 1L);
        payload.put("EventUuid", "from-payload");
        payload.put("ReplayId", "42");

        Struct value = (Struct) factory.fromEvent(event("s9", payload, 7), Map.of()).value();
        assertThat(value.schema().fields()).extracting(f -> f.name())
                .containsExactly("CreatedDate", "EventUuid", "ReplayId", "_ObjectType", "_EventType");
        assertThat(value.getString("EventUuid")).isEqualTo("from-payload");
        assertThat(value.getString("ReplayId")).isEqualTo("42");
    }
}
