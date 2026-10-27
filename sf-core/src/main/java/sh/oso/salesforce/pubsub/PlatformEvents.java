package sh.oso.salesforce.pubsub;

/** Naming rules for Salesforce platform events and their Pub/Sub channels. */
public final class PlatformEvents {

    /** Validation message for a name that is not a platform event API name. */
    public static final String NAME_RULE = "Platform event API names end with __e";

    private PlatformEvents() {
    }

    /** True for a platform event API name such as {@code Order_Shipped__e}. */
    public static boolean isValidName(String name) {
        return name != null && name.length() > "__e".length() && name.endsWith("__e");
    }

    /** The Pub/Sub (and Streaming API) channel for a platform event: {@code /event/<name>}. */
    public static String channel(String eventName) {
        return "/event/" + eventName;
    }
}
