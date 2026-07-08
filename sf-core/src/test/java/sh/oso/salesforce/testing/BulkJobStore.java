package sh.oso.salesforce.testing;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** In-memory Bulk API 2.0 job state for the mock Salesforce server. */
public final class BulkJobStore {

    public static final class QueryJob {
        public final String id;
        public final String soql;
        public final String operation;
        public final List<String> resultPages = new ArrayList<>();
        public String state = "UploadComplete";
        public int pollsUntilComplete;

        QueryJob(String id, String soql, String operation) {
            this.id = id;
            this.soql = soql;
            this.operation = operation;
        }
    }

    public static final class IngestJob {
        public final String id;
        public final String sobject;
        public final String operation;
        public final String externalIdFieldName;
        public String uploadedCsv;
        public String state = "Open";
        public int pollsUntilComplete;

        IngestJob(String id, String sobject, String operation, String externalIdFieldName) {
            this.id = id;
            this.sobject = sobject;
            this.operation = operation;
            this.externalIdFieldName = externalIdFieldName;
        }
    }

    private final AtomicInteger sequence = new AtomicInteger();
    private final Map<String, QueryJob> queryJobs = new ConcurrentHashMap<>();
    private final Map<String, IngestJob> ingestJobs = new ConcurrentHashMap<>();
    private final List<String> queryJobOrder = new ArrayList<>();
    private final List<String> ingestJobOrder = new ArrayList<>();

    /**
     * Rows for the next created query jobs, in creation order. Each element is the list of
     * CSV pages (header included) served for that job.
     */
    private final List<List<String>> pendingQueryResults = new ArrayList<>();

    /** Maps an uploaded ingest CSV row (as raw line) to an error; null/absent = success. */
    private volatile Function<String, String> ingestRowFailer = row -> null;

    public synchronized QueryJob createQueryJob(String soql, String operation) {
        QueryJob job = new QueryJob("750QRY" + sequence.incrementAndGet(), soql, operation);
        if (!pendingQueryResults.isEmpty()) {
            job.resultPages.addAll(pendingQueryResults.remove(0));
        } else {
            job.resultPages.add("");
        }
        queryJobs.put(job.id, job);
        queryJobOrder.add(job.id);
        return job;
    }

    public synchronized IngestJob createIngestJob(String sobject, String operation, String extIdField) {
        IngestJob job = new IngestJob("750ING" + sequence.incrementAndGet(), sobject, operation, extIdField);
        ingestJobs.put(job.id, job);
        ingestJobOrder.add(job.id);
        return job;
    }

    /** Queue CSV result pages (header included per page) for the next created query job. */
    public synchronized void enqueueQueryResultPages(List<String> pages) {
        pendingQueryResults.add(new ArrayList<>(pages));
    }

    public void setIngestRowFailer(Function<String, String> failer) {
        this.ingestRowFailer = failer != null ? failer : row -> null;
    }

    public Function<String, String> ingestRowFailer() {
        return ingestRowFailer;
    }

    public QueryJob queryJob(String id) {
        return queryJobs.get(id);
    }

    public IngestJob ingestJob(String id) {
        return ingestJobs.get(id);
    }

    public synchronized List<QueryJob> allQueryJobs() {
        List<QueryJob> out = new ArrayList<>();
        for (String id : queryJobOrder) {
            out.add(queryJobs.get(id));
        }
        return out;
    }

    public synchronized List<IngestJob> allIngestJobs() {
        List<IngestJob> out = new ArrayList<>();
        for (String id : ingestJobOrder) {
            out.add(ingestJobs.get(id));
        }
        return out;
    }

    public record CsvDoc(String header, List<String> rows) {
    }

    /** Splits an uploaded CSV document into header + data lines (LF endings). */
    public static CsvDoc splitCsv(String csv) {
        List<String> lines = new ArrayList<>(List.of(csv.split("\n")));
        String header = lines.isEmpty() ? "" : lines.remove(0);
        List<String> rows = new ArrayList<>();
        for (String line : lines) {
            if (!line.isBlank()) {
                rows.add(line);
            }
        }
        return new CsvDoc(header, rows);
    }
}
