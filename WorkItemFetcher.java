import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class WorkItemFetcher {

    private static final String DB_URL = "jdbc:oracle:thin:@//dbhost:1521/ORCLPDB1";
    private static final String DB_USER = "your_db_user";
    private static final String DB_PASSWORD = "your_db_password";

    private static final String TARGET_TABLE = "WORKITEM_SUMMARY";

    private static final int DB_BATCH_SIZE = 5000;
    private static final int ADO_BATCH_SIZE = 200;
    private static final int THREAD_POOL_SIZE = 8;

    private static final String ADO_ENDPOINT = "https://dev.azure.com/yourOrg/_apis/wit/workitemsbatch?api-version=6.0";
    private static final String ADO_PERSONAL_ACCESS_TOKEN = "REPLACE_WITH_PAT";

    private static final long SHUTDOWN_WAIT_MINUTES = 30;
    private static final int HTTP_MAX_RETRIES = 2;

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

            long lastId = 0;
            boolean more = true;

            while (more) {
                List<Long> ids = fetchIdBatch(conn, lastId, DB_BATCH_SIZE);
                if (ids.isEmpty()) {
                    more = false;
                    break;
                }

                List<List<Long>> partitions = partition(ids, ADO_BATCH_SIZE);

                List<Future<Integer>> futures = new ArrayList<>();
                for (List<Long> part : partitions) {
                    Callable<Integer> task = () -> processAdoBatchAndInsert(part);
                    futures.add(executor.submit(task));
                }

                for (Future<Integer> f : futures) {
                    try {
                        f.get();
                    } catch (ExecutionException ee) {
                        System.err.println("Task failed: " + ee.getCause());
                    }
                }

                lastId = ids.get(ids.size() - 1);

                if (ids.size() < DB_BATCH_SIZE) {
                    more = false;
                }
            }

        } finally {
            executor.shutdown();
            executor.awaitTermination(SHUTDOWN_WAIT_MINUTES, TimeUnit.MINUTES);
        }
    }

    private List<Long> fetchIdBatch(Connection conn, long lastId, int batchSize) throws SQLException {
        String sql = "SELECT WORKITEM_ID FROM SOURCE_WORKITEM_TABLE WHERE WORKITEM_ID > ? ORDER BY WORKITEM_ID FETCH FIRST ? ROWS ONLY";
        List<Long> ids = new ArrayList<>(batchSize);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, lastId);
            ps.setInt(2, batchSize);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong("WORKITEM_ID"));
                }
            }
        }
        System.out.println("Fetched " + ids.size() + " ids (lastId=" + lastId + ")");
        return ids;
    }

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

        int inserted = upsertWorkItems(records);
        System.out.println("Upserted " + inserted + " records for batch starting with " + ids.get(0));
        return inserted;
    }

    private String buildAdoRequestBody(List<Long> ids) throws Exception {
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
            Thread.sleep(1000L * attempt);
        }
        return null;
    }

    private List<WorkItemRecord> parseAdoResponse(String responseBody) throws Exception {
        List<WorkItemRecord> out = new ArrayList<>();
        JsonNode root = objectMapper.readTree(responseBody);

        JsonNode listNode = root.has("value") ? root.get("value") : root;
        if (listNode == null || !listNode.isArray()) {
            return Collections.emptyList();
        }

        for (JsonNode node : listNode) {
            try {
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
                    ps.setTimestamp(5, parseIsoToTimestamp(r.changedDate));
                    ps.setTimestamp(6, parseIsoToTimestamp(r.createdDate));
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

    private Timestamp parseIsoToTimestamp(String isoDate) {
        if (isoDate == null || isoDate.isBlank()) {
            return null;
        }
        try {
            OffsetDateTime odt = OffsetDateTime.parse(isoDate, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            return Timestamp.from(odt.toInstant());
        } catch (Exception e) {
            System.err.println("Failed to parse date: " + isoDate + " (" + e.getMessage() + ")");
            return null;
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
