package sh.oso.salesforce.streaming;

/** Connector version, read from the jar manifest (Implementation-Version set by the build). */
final class Version {
    static final String VERSION = resolve();

    private static String resolve() {
        String version = Version.class.getPackage().getImplementationVersion();
        return version != null ? version : "unknown";
    }

    private Version() {
    }
}
