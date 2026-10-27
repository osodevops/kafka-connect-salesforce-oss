package sh.oso.salesforce.source;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/** One independently failing stream in a task: an SObject or a platform event. */
interface SourcePipeline extends AutoCloseable {

    /** The SObject or platform event name, for logs. */
    String name();

    List<SourceRecord> poll() throws InterruptedException;

    @Override
    void close();
}
