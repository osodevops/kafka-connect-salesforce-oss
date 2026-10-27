package sh.oso.salesforce.testing;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

/** Canned Avro schemas mirroring Salesforce CDC and Platform Event shapes. */
public final class TestSchemas {

    private TestSchemas() {
    }

    /** A trimmed AccountChangeEvent: ChangeEventHeader + a few Account fields. */
    public static Schema accountChangeEventSchema() {
        Schema header = SchemaBuilder.record("ChangeEventHeader").namespace("com.sforce.eventbus")
                .fields()
                .name("entityName").type().stringType().noDefault()
                .name("recordIds").type().array().items().stringType().noDefault()
                .name("changeType").type().enumeration("ChangeType")
                    .symbols("CREATE", "UPDATE", "DELETE", "UNDELETE",
                            "GAP_CREATE", "GAP_UPDATE", "GAP_DELETE", "GAP_UNDELETE", "GAP_OVERFLOW")
                    .noDefault()
                .name("changeOrigin").type().stringType().noDefault()
                .name("transactionKey").type().stringType().noDefault()
                .name("sequenceNumber").type().intType().noDefault()
                .name("commitTimestamp").type().longType().noDefault()
                .name("commitNumber").type().longType().noDefault()
                .name("commitUser").type().stringType().noDefault()
                .name("nulledFields").type().array().items().stringType().noDefault()
                .name("diffFields").type().array().items().stringType().noDefault()
                .name("changedFields").type().array().items().stringType().noDefault()
                .endRecord();
        return SchemaBuilder.record("AccountChangeEvent").namespace("com.sforce.eventbus")
                .fields()
                .name("ChangeEventHeader").type(header).noDefault()
                .name("Name").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                .name("Industry").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                .name("AnnualRevenue").type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
                .name("NumberOfEmployees").type().unionOf().nullType().and().intType().endUnion().nullDefault()
                .name("LastModifiedDate").type().unionOf().nullType().and().longType().endUnion().nullDefault()
                .endRecord();
    }

    /** A simple platform event: Order_Shipped__e. */
    public static Schema orderShippedEventSchema() {
        return SchemaBuilder.record("Order_Shipped__e").namespace("com.sforce.eventbus")
                .fields()
                .name("CreatedDate").type().longType().noDefault()
                .name("CreatedById").type().stringType().noDefault()
                .name("OrderNumber__c").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                .name("Carrier__c").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                .name("ShippedAt__c").type().unionOf().nullType().and().longType().endUnion().nullDefault()
                .endRecord();
    }

    /**
     * A "fat" integration platform event, Integration_Message__e: the publisher (a Flow or Apex)
     * serialises the whole business record as JSON into a long text area field, so consumers
     * never need to call Salesforce back.
     */
    public static Schema integrationMessageEventSchema() {
        return integrationMessageFields().endRecord();
    }

    /** {@link #integrationMessageEventSchema()} after an admin added Initiator_Id__c to the event. */
    public static Schema integrationMessageEventSchemaV2() {
        return integrationMessageFields()
                .name("Initiator_Id__c").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                .endRecord();
    }

    private static SchemaBuilder.FieldAssembler<Schema> integrationMessageFields() {
        return SchemaBuilder.record("Integration_Message__e").namespace("com.sforce.eventbus")
                .fields()
                .name("CreatedDate").type().longType().noDefault()
                .name("CreatedById").type().stringType().noDefault()
                .name("Message_Type__c").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                .name("Sequence__c").type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
                .name("Payload__c").type().unionOf().nullType().and().stringType().endUnion().nullDefault();
    }

    /** A sample business payload of roughly {@code approxChars} characters of JSON. */
    public static String samplePayloadJson(String recordId, int approxChars) {
        StringBuilder lines = new StringBuilder();
        int i = 0;
        while (lines.length() < approxChars) {
            if (i > 0) {
                lines.append(',');
            }
            lines.append("{\"lineId\":\"L").append(i).append("\",\"description\":\"Consulting services, phase ")
                    .append(i).append("\",\"amount\":").append(1000 + i).append(".50,\"currency\":\"GBP\"}");
            i++;
        }
        return "{\"recordId\":\"" + recordId + "\",\"name\":\"Example Holdings Ltd\",\"stage\":\"Closed Won\","
                + "\"address\":{\"city\":\"London\",\"country\":\"GB\"},\"lines\":[" + lines + "]}";
    }

    public static GenericRecord integrationMessage(Schema schema, String messageType, double sequence,
                                                   String payloadJson) {
        GenericRecord event = new GenericData.Record(schema);
        event.put("CreatedDate", 1_767_225_600_000L);
        event.put("CreatedById", "005xx000001X8UzAAK");
        event.put("Message_Type__c", messageType);
        event.put("Sequence__c", sequence);
        event.put("Payload__c", payloadJson);
        return event;
    }

    public static GenericRecord accountChangeEvent(Schema schema, String changeType, String recordId,
                                                   long commitTimestamp, String name) {
        Schema headerSchema = schema.getField("ChangeEventHeader").schema();
        GenericRecord header = new GenericData.Record(headerSchema);
        header.put("entityName", "Account");
        header.put("recordIds", List.of(recordId));
        header.put("changeType", new GenericData.EnumSymbol(
                headerSchema.getField("changeType").schema(), changeType));
        header.put("changeOrigin", "com/salesforce/api/soap/60.0;client=test");
        header.put("transactionKey", "0001-key");
        header.put("sequenceNumber", 1);
        header.put("commitTimestamp", commitTimestamp);
        header.put("commitNumber", commitTimestamp * 10);
        header.put("commitUser", "005xx000001X8UzAAK");
        header.put("nulledFields", List.of());
        header.put("diffFields", List.of());
        header.put("changedFields", List.of());

        GenericRecord event = new GenericData.Record(schema);
        event.put("ChangeEventHeader", header);
        event.put("Name", name);
        event.put("Industry", "Technology");
        event.put("AnnualRevenue", 1_000_000.0);
        event.put("NumberOfEmployees", 42);
        event.put("LastModifiedDate", commitTimestamp);
        return event;
    }

    /** A minimal Account describe JSON for schema-mapping and REST tests. */
    public static String accountDescribeJson() {
        return """
                {
                  "name": "Account",
                  "fields": [
                    {"name": "Id", "type": "id", "createable": false, "updateable": false},
                    {"name": "Name", "type": "string", "createable": true, "updateable": true},
                    {"name": "Industry", "type": "picklist", "createable": true, "updateable": true},
                    {"name": "AnnualRevenue", "type": "currency", "createable": true, "updateable": true},
                    {"name": "NumberOfEmployees", "type": "int", "createable": true, "updateable": true},
                    {"name": "IsDeleted", "type": "boolean", "createable": false, "updateable": false},
                    {"name": "CreatedDate", "type": "datetime", "createable": false, "updateable": false},
                    {"name": "SystemModstamp", "type": "datetime", "createable": false, "updateable": false},
                    {"name": "BillingAddress", "type": "address", "createable": false, "updateable": false},
                    {"name": "BillingCity", "type": "string", "createable": true, "updateable": true},
                    {"name": "OwnerId", "type": "reference", "createable": true, "updateable": true}
                  ]
                }
                """;
    }
}
