package sh.oso.salesforce.e2e;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.pesink.PeSinkConfig;
import sh.oso.salesforce.sink.SinkConfig;
import sh.oso.salesforce.source.SourceConfig;
import sh.oso.salesforce.streaming.StreamingConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Generates the Confluent-style, property-by-property configuration reference pages
 * for the docs site directly from each connector's ConfigDef — so the published
 * reference can never drift from the code. Runs as part of the normal build; output
 * is deterministic, so an intentional config change shows up as a docs diff in the
 * same commit.
 */
class ConfigDocsGeneratorTest {

    private static final Path OUT_DIR =
            Path.of(System.getProperty("repo.root", "..")).resolve("website/docs/reference/configuration");

    @Test
    void generateConfigurationReferencePages() throws IOException {
        Files.createDirectories(OUT_DIR);
        write("source.md", page(
                "Source Connector Configuration",
                "Every configuration property of the Salesforce source connector, with types, defaults, and valid values.",
                "sh.oso.salesforce.source.SalesforceSourceConnector",
                "connectors/source",
                SourceConfig.configDef(),
                1,
                """
                Framework properties (`name`, `tasks.max`, converters, `errors.*`) follow the
                standard [Kafka Connect worker/connector configuration](https://kafka.apache.org/documentation/#connectconfigs).
                Topics are derived per SObject as `{sf.topic.prefix}.{SObject}` — there is no `topics` property on the source.
                """));
        write("sobject-sink.md", page(
                "SObject Sink Configuration",
                "Every configuration property of the Salesforce SObject sink connector, including the per-object sf.<object>.* keys.",
                "sh.oso.salesforce.sink.SalesforceSinkConnector",
                "connectors/sobject-sink",
                SinkConfig.configDef(),
                2,
                """
                Select input with the standard framework properties `topics` or `topics.regex`.
                For dead-letter queues use `errors.tolerance=all` with `errors.deadletterqueue.topic.name`.
                """) + PER_OBJECT_SECTION);
        write("platform-event-sink.md", page(
                "Platform Event Sink Configuration",
                "Every configuration property of the Salesforce Platform Event sink connector.",
                "sh.oso.salesforce.pesink.SalesforcePlatformEventSinkConnector",
                "connectors/platform-event-sink",
                PeSinkConfig.configDef(),
                3,
                """
                Select input with the standard framework properties `topics` or `topics.regex`.
                """));
        write("streaming-source.md", page(
                "Streaming Source Configuration",
                "Every configuration property of the legacy CometD streaming source connector.",
                "sh.oso.salesforce.streaming.SalesforceStreamingSourceConnector",
                "connectors/streaming-source",
                StreamingConfig.configDef(),
                4,
                ""));
        assertThat(OUT_DIR.resolve("source.md")).exists();
    }

    private static void write(String file, String content) throws IOException {
        Files.writeString(OUT_DIR.resolve(file), content);
    }

    private static String page(String title, String description, String connectorClass,
                               String connectorDocPath, ConfigDef configDef, int position, String intro) {
        StringBuilder md = new StringBuilder();
        md.append("---\n")
          .append("title: \"").append(title).append("\"\n")
          .append("description: \"").append(description).append("\"\n")
          .append("sidebar_label: \"").append(title.replace(" Configuration", "")).append("\"\n")
          .append("sidebar_position: ").append(position).append("\n")
          .append("---\n\n")
          .append("<!-- GENERATED from the connector's ConfigDef by ConfigDocsGeneratorTest.\n")
          .append("     Do not edit by hand: change the ConfigDef documentation instead. -->\n\n")
          .append("# ").append(title).append("\n\n")
          .append("`").append(connectorClass).append("`\n\n")
          .append("Property-by-property reference, generated from the connector's `ConfigDef` — ")
          .append("guaranteed to match the release you are running. Usage guide: ")
          .append("[connector documentation](/").append(connectorDocPath).append(").\n");
        if (!intro.isBlank()) {
            md.append("\n").append(intro.strip()).append("\n");
        }
        for (ConfigDef.Importance importance : List.of(ConfigDef.Importance.HIGH,
                ConfigDef.Importance.MEDIUM, ConfigDef.Importance.LOW)) {
            List<ConfigDef.ConfigKey> keys = configDef.configKeys().values().stream()
                    .filter(k -> k.importance == importance)
                    .filter(k -> k.documentation == null || !k.documentation.startsWith("Internal"))
                    .toList();
            if (keys.isEmpty()) {
                continue;
            }
            md.append("\n## ").append(importanceHeading(importance)).append("\n");
            for (ConfigDef.ConfigKey key : keys) {
                md.append(renderProperty(key));
            }
        }
        return md.toString();
    }

    private static String importanceHeading(ConfigDef.Importance importance) {
        return switch (importance) {
            case HIGH -> "High importance";
            case MEDIUM -> "Medium importance";
            case LOW -> "Low importance";
        };
    }

    private static String renderProperty(ConfigDef.ConfigKey key) {
        StringBuilder md = new StringBuilder();
        md.append("\n### `").append(key.name).append("`\n\n");
        if (key.documentation != null && !key.documentation.isBlank()) {
            // MDX parses bare <object> placeholders as JSX and {prefix} as expressions; escape both.
            md.append(key.documentation.strip()
                    .replace("<", "&lt;").replace(">", "&gt;")
                    .replace("{", "&#123;").replace("}", "&#125;")).append("\n\n");
        }
        md.append("- **Type:** ").append(key.type.name().toLowerCase()).append("\n");
        md.append("- **Default:** ").append(renderDefault(key)).append("\n");
        if (key.validator != null) {
            String valid = key.validator.toString();
            if (!valid.isBlank()) {
                md.append("- **Valid values:** ").append(valid).append("\n");
            }
        }
        md.append("- **Importance:** ").append(key.importance.name().toLowerCase()).append("\n");
        return md.toString();
    }

    private static String renderDefault(ConfigDef.ConfigKey key) {
        if (!key.hasDefault()) {
            return "none — **required**";
        }
        Object value = key.defaultValue;
        if (value == null) {
            return "null";
        }
        String rendered = value instanceof List<?> list ? String.join(",",
                list.stream().map(String::valueOf).toList()) : String.valueOf(value);
        return rendered.isEmpty() ? "\"\" (empty)" : "`" + rendered + "`";
    }

    /** Dynamic per-object keys are not part of the ConfigDef; documented statically. */
    private static final String PER_OBJECT_SECTION = """

            ## Per-object properties (`sf.<object>.*`)

            Each SObject listed in `sf.objects` is configured with dynamic keys, where `<object>`
            is the exact API name (e.g. `sf.Lead.topics`). These are read from the raw connector
            config and therefore do not appear in `config()` validation output.

            ### `sf.<object>.topics`

            Kafka topic(s) feeding this object. Each topic maps to exactly one SObject.

            - **Type:** list
            - **Default:** none — required when more than one object is configured

            ### `sf.<object>.operation`

            Write operation used when `_EventType` is absent or overridden: `insert`, `update`,
            `upsert`, or `delete`.

            - **Type:** string
            - **Default:** `insert`

            ### `sf.<object>.override.event.type`

            Ignore the record's `_EventType` field and always apply `sf.<object>.operation`.

            - **Type:** boolean
            - **Default:** `false`

            ### `sf.<object>.use.custom.id.field`

            Address records by an external ID field instead of the Salesforce `Id` — enables
            idempotent cross-org upserts and deletes.

            - **Type:** boolean
            - **Default:** `false`

            ### `sf.<object>.custom.id.field.name`

            API name of the external ID field (must be marked as an External ID in the target org).

            - **Type:** string
            - **Default:** none — required when `sf.<object>.use.custom.id.field=true`

            ### `sf.<object>.type`

            `standard_custom` for regular objects, or `big_object` for Big Objects (`__b`).
            Big Objects are insert-only; update/delete records are routed to the DLQ.

            - **Type:** string
            - **Default:** `standard_custom`

            ### `sf.<object>.ignore.fields`

            Comma-separated record fields to exclude from writes.

            - **Type:** list
            - **Default:** "" (empty)
            """;
}
