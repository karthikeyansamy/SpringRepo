import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * WorkItemFetcher
 *
 * Features:
 * 1) Reads WorkItem IDs from Oracle DB in batches (configurable batchSize)
 * 2) Calls ADO REST endpoint in batch (configurable adoBatchSize) and in parallel using an ExecutorService
 * 3) Parses response and extracts fields: WorkItem ID, WorkItem Type, AssignedTo, CreatedBy, ChangedDate, CreatedDate, State
 * 4) Inserts/Upserts results back into Oracle table using MERGE (batch)
 *
 * Dependencies:
 * - Java 11+ (for HttpClient)
 * - com.fasterxml.jackson.core:jackson-databind
 * - Oracle JDBC driver on classpath
 *
 * Notes:
 * - Tune DB_BATCH_SIZE, ADO_BATCH_SIZE and THREAD_POOL_SIZE according to available resources
 * - ADO API shape is assumed to accept a JSON array of ids and return JSON array of work item objects; adjust request/response handling to match your API
 */
public class WorkItemFetcher {

    // ---------- CONFIGURATION ----------
    private static final String DB_URL = "jdbc:oracle:thin:@//dbhost:1521/ORCLPDB1";
    private static final String DB_USER = "your_db_user";
    private static final String DB_PASSWORD = "your_db_password";

    // Table where results will be stored - adjust column names/types accordingly
    private static final String TARGET_TABLE = "WORKITEM_SUMMARY";

    // How many IDs to fetch from Oracle per cycle (reading large table in chunks)
    private static final int DB_BATCH_SIZE = 5000;

    // How many workitem IDs to pass to ADO in one API call
    private static final int ADO_BATCH_SIZE = 200;

    // Number of concurrent threads to call ADO in parallel
    private static final int THREAD_POOL_SIZE = 8;

    // ADO endpoint and auth (update for your environment). Example: https://dev.azure.com/{org}/_apis/wit/workitemsbatch?api-version=6.0
    private static final String ADO_ENDPOINT = "https://dev.azure.com/yourOrg/_apis/wit/workitemsbatch?api-version=6.0";
    private static final String ADO_PERSONAL_ACCESS_TOKEN = "REPLACE_WITH_PAT"; // Basic auth using PAT as username:password (username empty)

    // How long to wait for threads to finish (minutes)
    private static final long SHUTDOWN_WAIT_MINUTES = 30;

    // Retry attempts for HTTP calls
    private static final int HTTP_MAX_RETRIES = 2;

    // ---------- END CONFIG ----------

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public WorkItemFetcher() {
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(20))
                .build();
        objectMapper = new ObjectMapper();
    }

    public static void main(String[] args) throws Exception {
        WorkItemFetcher fetcher = new WorkItemFetcher();
        fetcher.run();
    }

    public void run() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            conn.setAutoCommit(false);

            long offset = 0;
            boolean more = true;

            while (more) {
                List<Long> ids = fetchIdBatch(conn, offset, DB_BATCH_SIZE);
                if (ids.isEmpty()) {
                    more = false;
                    break;
                }

                // Partition into ADO_BATCH_SIZE chunks
                List<List<Long>> partitions = partition(ids, ADO_BATCH_SIZE);

                List<Future<Integer>> futures = new ArrayList<>();
                for (List<Long> part : partitions) {
                    Callable<Integer> task = () -> processAdoBatchAndInsert(part);
                    futures.add(executor.submit(task));
                }

                // Wait for tasks and collect results
                for (Future<Integer> f : futures) {
                    try {
                        f.get(); // you can log returned count
                    } catch (ExecutionException ee) {
                        System.err.println("Task failed: " + ee.getCause());
                    }
                }

                offset += ids.size();

                // continue loop until fewer than batchSize returned
                if (ids.size() < DB_BATCH_SIZE) {
                    more = false;
                }
            }

        } finally {
            executor.shutdown();
            executor.awaitTermination(SHUTDOWN_WAIT_MINUTES, TimeUnit.MINUTES);
        }
    }

    /**
     * Fetches a batch of WorkItem IDs using OFFSET/FETCH NEXT. Adjust SQL if your Oracle version requires different pagination.
     */
    private List<Long> fetchIdBatch(Connection conn, long offset, int batchSize) throws SQLException {
        String sql = "SELECT WORKITEM_ID FROM (SELECT WORKITEM_ID, ROW_NUMBER() OVER (ORDER BY WORKITEM_ID) rn FROM SOURCE_WORKITEM_TABLE) WHERE rn > ? AND rn <= ?";
        List<Long> ids = new ArrayList<>(batchSize);
        long low = offset;
        long high = offset + batchSize;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, low);
            ps.setLong(2, high);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong("WORKITEM_ID"));
                }
            }
        }
        System.out.println("Fetched " + ids.size() + " ids (offset=" + offset + ")");
        return ids;
    }

    /**
     * Main worker: call ADO to get details for the given list of IDs, parse, and insert back into Oracle.
     */
    private int processAdoBatchAndInsert(List<Long> ids) throws Exception {
        if (ids.isEmpty()) return 0;
        String requestBody = buildAdoRequestBody(ids);

        String responseBody = callAdoWithRetries(requestBody);
        if (responseBody == null) {
            System.err.println("Failed to get response from ADO for batch starting with " + ids.get(0));
            return 0;
        }

        List<WorkItemRecord> records = parseAdoResponse(responseBody);

        if (records.isEmpty()) {
            System.out.println("No records parsed for batch starting with " + ids.get(0));
            return 0;
        }

        // insert/upsert into DB
        int inserted = upsertWorkItems(records);
        System.out.println("Upserted " + inserted + " records for batch starting with " + ids.get(0));
        return inserted;
    }

    private String buildAdoRequestBody(List<Long> ids) throws Exception {
        // Adapt this JSON payload to match the ADO API you use. Example for workitemsbatch API
        // {
        //   "ids": [1,2,3],
        //   "fields": ["System.Id","System.WorkItemType","System.AssignedTo","System.CreatedBy","System.ChangedDate","System.CreatedDate","System.State"]
        // }
        ArrayNode root = objectMapper.createArrayNode();
        // Using a simple structure: { "ids": [...], "fields": [...] }
        JsonNode wrapper = objectMapper.createObjectNode()
                .putPOJO("ids", ids);
        // but putPOJO will produce an array — to be explicit build
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        sb.append("\"ids\":");
        sb.append(objectMapper.writeValueAsString(ids));
        sb.append(',');
        sb.append("\"fields\":");
        sb.append(objectMapper.writeValueAsString(new String[]{
                "System.Id",
                "System.WorkItemType",
                "System.AssignedTo",
                "System.CreatedBy",
                "System.ChangedDate",
                "System.CreatedDate",
                "System.State"
        }));
        sb.append('}');
        return sb.toString();
    }

    private String callAdoWithRetries(String body) throws Exception {
        int attempt = 0;
        while (attempt <= HTTP_MAX_RETRIES) {
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(ADO_ENDPOINT))
                        .timeout(Duration.ofSeconds(30))
                        .header("Content-Type", "application/json")
                        .header("Accept", "application/json")
                        .header("Authorization", "Basic " + java.util.Base64.getEncoder().encodeToString((":" + ADO_PERSONAL_ACCESS_TOKEN).getBytes()))
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();

                HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                int status = resp.statusCode();
                if (status >= 200 && status < 300) {
                    return resp.body();
                } else {
                    System.err.println("ADO returned status " + status + ": " + resp.body());
                }
            } catch (Exception e) {
                System.err.println("ADO call failed on attempt " + attempt + ": " + e.getMessage());
            }
            attempt++;
            Thread.sleep(1000L * attempt); // exponential-ish backoff
        }
        return null;
    }

    private List<WorkItemRecord> parseAdoResponse(String responseBody) throws Exception {
        List<WorkItemRecord> out = new ArrayList<>();
        JsonNode root = objectMapper.readTree(responseBody);

        // Adjust this based on actual ADO response JSON shape. Here we assume a top-level 'value' array or direct array
        JsonNode listNode = root.has("value") ? root.get("value") : root;
        if (listNode == null || !listNode.isArray()) {
            return Collections.emptyList();
        }

        for (JsonNode node : listNode) {
            try {
                // The ADO work item JSON usually has 'id' and 'fields' map
                long id = node.path("id").asLong();
                JsonNode fields = node.path("fields");
                String workItemType = fields.path("System.WorkItemType").asText(null);
                String assignedTo = extractPersonDisplayName(fields.path("System.AssignedTo"));
                String createdBy = extractPersonDisplayName(fields.path("System.CreatedBy"));
                String changedDate = fields.path("System.ChangedDate").asText(null);
                String createdDate = fields.path("System.CreatedDate").asText(null);
                String state = fields.path("System.State").asText(null);

                WorkItemRecord rec = new WorkItemRecord(id, workItemType, assignedTo, createdBy, changedDate, createdDate, state);
                out.add(rec);
            } catch (Exception e) {
                System.err.println("Failed to parse one work item: " + e.getMessage());
            }
        }
        return out;
    }

    private String extractPersonDisplayName(JsonNode personNode) {
        if (personNode == null || personNode.isNull()) return null;
        if (personNode.isTextual()) return personNode.asText();
        if (personNode.has("displayName")) return personNode.get("displayName").asText();
        if (personNode.has("uniqueName")) return personNode.get("uniqueName").asText();
        return personNode.toString();
    }

    /**
     * Upserts the parsed workitems into Oracle. Uses MERGE statement for upsert. Each thread opens its own connection.
     */
    private int upsertWorkItems(List<WorkItemRecord> records) throws SQLException {
        String mergeSql = "MERGE INTO " + TARGET_TABLE + " t USING (SELECT ? AS workitem_id, ? AS workitem_type, ? AS assigned_to, ? AS created_by, ? AS changed_date, ? AS created_date, ? AS state FROM dual) src "
                + "ON (t.workitem_id = src.workitem_id) "
                + "WHEN MATCHED THEN UPDATE SET t.workitem_type = src.workitem_type, t.assigned_to = src.assigned_to, t.created_by = src.created_by, t.changed_date = src.changed_date, t.created_date = src.created_date, t.state = src.state "
                + "WHEN NOT MATCHED THEN INSERT (workitem_id, workitem_type, assigned_to, created_by, changed_date, created_date, state) VALUES (src.workitem_id, src.workitem_type, src.assigned_to, src.created_by, src.changed_date, src.created_date, src.state)";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(mergeSql)) {
                int count = 0;
                for (WorkItemRecord r : records) {
                    ps.setLong(1, r.workitemId);
                    ps.setString(2, r.workitemType);
                    ps.setString(3, r.assignedTo);
                    ps.setString(4, r.createdBy);
                    ps.setString(5, r.changedDate);
                    ps.setString(6, r.createdDate);
                    ps.setString(7, r.state);
                    ps.addBatch();
                    count++;
                    if (count % 500 == 0) {
                        ps.executeBatch();
                    }
                }
                ps.executeBatch();
                conn.commit();
                return count;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
    }

    private static <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> parts = new ArrayList<>();
        int n = list.size();
        for (int i = 0; i < n; i += size) {
            parts.add(list.subList(i, Math.min(n, i + size)));
        }
        return parts;
    }

    // Simple holder for required fields
    static class WorkItemRecord {
        final long workitemId;
        final String workitemType;
        final String assignedTo;
        final String createdBy;
        final String changedDate;
        final String createdDate;
        final String state;

        WorkItemRecord(long workitemId, String workitemType, String assignedTo, String createdBy, String changedDate, String createdDate, String state) {
            this.workitemId = workitemId;
            this.workitemType = workitemType;
            this.assignedTo = assignedTo;
            this.createdBy = createdBy;
            this.changedDate = changedDate;
            this.createdDate = createdDate;
            this.state = state;
        }
    }
}
