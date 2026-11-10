package sh.oso.salesforce.source;

/**
 * Derives the names Salesforce expects for an SObject listed in {@code sf.sobjects}.
 *
 * <p>A custom object has two names: its API name ({@code My_Object__c}), used by the REST
 * and Bulk APIs and in SOQL, and the change event entity name without the trailing
 * {@code c} ({@code My_Object__ChangeEvent}), used by the CDC channel. Either spelling may be
 * configured; both resolve to the same pair. Standard objects ({@code Account}) are unchanged.
 *
 * <p>The configured value itself is still what names the Kafka topic and the offset
 * partition, so existing deployments keep their topics and positions.
 */
final class SObjectNames {

    private static final String CUSTOM_SUFFIX = "__c";
    private static final String CHANGE_EVENT_STEM_SUFFIX = "__";

    private SObjectNames() {
    }

    /** The API name for REST and SOQL: {@code My_Object__} becomes {@code My_Object__c}. */
    static String apiName(String configured) {
        return configured.endsWith(CHANGE_EVENT_STEM_SUFFIX) ? configured + "c" : configured;
    }

    /** The Pub/Sub CDC channel: {@code My_Object__c} becomes {@code /data/My_Object__ChangeEvent}. */
    static String cdcChannel(String configured) {
        String stem = configured.endsWith(CUSTOM_SUFFIX)
                ? configured.substring(0, configured.length() - 1)
                : configured;
        return "/data/" + stem + "ChangeEvent";
    }
}
