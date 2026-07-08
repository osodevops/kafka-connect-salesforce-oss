package sh.oso.salesforce.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.pubsub.ChangeEventUtils;
import sh.oso.salesforce.testing.TestSchemas;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaMappingTest {

    @Test
    void avroChangeEventMapsToConnectStruct() {
        org.apache.avro.Schema avro = TestSchemas.accountChangeEventSchema();
        Schema connect = AvroToConnect.toConnectSchema(avro);

        assertThat(connect.type()).isEqualTo(Schema.Type.STRUCT);
        assertThat(connect.field("ChangeEventHeader").schema().type()).isEqualTo(Schema.Type.STRUCT);
        assertThat(connect.field("Name").schema().isOptional()).isTrue();
        assertThat(connect.field("AnnualRevenue").schema().type()).isEqualTo(Schema.Type.FLOAT64);

        var event = TestSchemas.accountChangeEvent(avro, "CREATE", "001A", 1234L, "Acme");
        Struct struct = (Struct) AvroToConnect.toConnectValue(connect, event);
        assertThat(struct.getString("Name")).isEqualTo("Acme");
        Struct header = struct.getStruct("ChangeEventHeader");
        assertThat(header.getString("changeType")).isEqualTo("CREATE");
        assertThat(header.getArray("recordIds")).containsExactly("001A");
        assertThat(header.getInt64("commitTimestamp")).isEqualTo(1234L);
    }

    @Test
    void describeMapsFieldTypesAndSkipsCompound() throws Exception {
        var describe = new ObjectMapper().readTree(TestSchemas.accountDescribeJson());
        Schema connect = DescribeToConnect.toConnectSchema(describe, Set.of("CreatedDate"));

        assertThat(connect.name()).isEqualTo("Account");
        assertThat(connect.field("BillingAddress")).isNull();
        assertThat(connect.field("BillingCity").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(connect.field("NumberOfEmployees").schema().type()).isEqualTo(Schema.Type.INT32);
        assertThat(connect.field("AnnualRevenue").schema().type()).isEqualTo(Schema.Type.FLOAT64);
        // date policy: SystemModstamp → epoch INT64; CreatedDate opted out → ISO string
        assertThat(connect.field("SystemModstamp").schema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(connect.field("CreatedDate").schema().type()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    void csvValuesConvertPerSchema() throws Exception {
        var describe = new ObjectMapper().readTree(TestSchemas.accountDescribeJson());
        Schema connect = DescribeToConnect.toConnectSchema(describe, Set.of("CreatedDate"));

        Struct struct = CsvValueConverter.toStruct(connect, Map.of(
                "Id", "001A",
                "Name", "Acme",
                "NumberOfEmployees", "42",
                "AnnualRevenue", "1000000.5",
                "IsDeleted", "false",
                "SystemModstamp", "2024-01-31T09:30:00.000+0000",
                "CreatedDate", "2024-01-31T09:30:00.000+0000"));
        assertThat(struct.getInt32("NumberOfEmployees")).isEqualTo(42);
        assertThat(struct.getFloat64("AnnualRevenue")).isEqualTo(1000000.5);
        assertThat(struct.getBoolean("IsDeleted")).isFalse();
        assertThat(struct.getInt64("SystemModstamp")).isEqualTo(1706693400000L);
        assertThat(struct.getString("CreatedDate")).isEqualTo("2024-01-31T09:30:00.000+0000");
    }

    @Test
    void dateOnlyAndTimeOnlyConvert() {
        assertThat(CsvValueConverter.parseTemporalToEpochMillis("2024-01-31")).isEqualTo(1706659200000L);
        assertThat(CsvValueConverter.parseTemporalToEpochMillis("09:30:00.000Z"))
                .isEqualTo((9 * 3600 + 30 * 60) * 1000L);
    }

    @Test
    void changeEventBitmapsExpandToFieldNames() {
        org.apache.avro.Schema avro = TestSchemas.accountChangeEventSchema();
        // Fields: 0=ChangeEventHeader, 1=Name, 2=Industry, 3=AnnualRevenue, 4=NumberOfEmployees, 5=LastModifiedDate
        List<String> changed = ChangeEventUtils.expandBitmapFields(avro, List.of("0x06"));
        assertThat(changed).containsExactly("Name", "Industry");

        List<String> withNested = ChangeEventUtils.expandBitmapFields(avro, List.of("0x20", "0-0x01"));
        assertThat(withNested).containsExactly("LastModifiedDate", "ChangeEventHeader.entityName");
    }
}
