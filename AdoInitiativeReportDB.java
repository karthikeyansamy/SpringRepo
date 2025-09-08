import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class AdoInitiativeReportDB {

    /* ---------------- CONFIG ---------------- */
    private static final String ORG_URL = "https://dev.azure.com/YOUR_ORG";
    private static final String PAT = "YOUR_PERSONAL_ACCESS_TOKEN";

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/yourdb";
    private static final String DB_USER = "youruser";
    private static final String DB_PASS = "yourpass";

    private static final HttpClient CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final ExecutorService EXEC = Executors.newFixedThreadPool(10); // multithread fetch

    public static void main(String[] args) throws Exception {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            conn.setAutoCommit(false);

            List<Integer> initiatives = fetchInitiativesFromDB(conn);

            for (int initiativeId : initiatives) {
                Map<Integer, Set<Integer>> adjacency = new HashMap<>();
                Set<Integer> allIds = new HashSet<>();

                fetchRecursiveHierarchy("DefaultProject", initiativeId, adjacency, allIds);

                Map<Integer, WorkItem> details = fetchWorkItemsBatch(allIds);

                // Insert into DB
                insertReportRows(conn, initiativeId, adjacency, details);
            }

            conn.commit();
        } finally {
            EXEC.shutdown();
        }

        System.out.println("✅ Report stored in DB.");
    }

    /* ---------------- DB FETCH ---------------- */

    private static List<Integer> fetchInitiativesFromDB(Connection conn) throws SQLException {
        List<Integer> ids = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT initiative_id FROM initiatives")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getInt(1));
                }
            }
        }
        return ids;
    }

    /* ---------------- ADO FETCH ---------------- */

    private static Set<Integer> fetchDirectChildren(String project, int parentId) throws Exception {
        String wiql = "{ \"query\": \"SELECT [System.Id] FROM WorkItemLinks " +
                "WHERE [Source].[System.Id] = " + parentId + " " +
                "AND [System.Links.LinkType] = 'System.LinkTypes.Hierarchy-Forward' " +
                "ORDER BY [System.Id]\" }";

        String url = ORG_URL + "/" + encode(project) + "/_apis/wit/wiql?api-version=7.0";
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", authHeader())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(wiql))
                .timeout(Duration.ofMinutes(2))
                .build();

        HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
        checkStatus(resp);

        JsonNode rels = MAPPER.readTree(resp.body()).path("workItemRelations");
        Set<Integer> children = new HashSet<>();
        for (JsonNode rel : rels) {
            if (rel.has("target")) {
                children.add(rel.path("target").path("id").asInt());
            }
        }
        return children;
    }

    private static void fetchRecursiveHierarchy(
            String project,
            int parentId,
            Map<Integer, Set<Integer>> adjacency,
            Set<Integer> allIds
    ) throws Exception {
        if (allIds.contains(parentId)) return;
        allIds.add(parentId);

        Set<Integer> children = fetchDirectChildren(project, parentId);
        adjacency.put(parentId, children);

        for (int childId : children) {
            fetchRecursiveHierarchy(project, childId, adjacency, allIds);
        }
    }

    private static Map<Integer, WorkItem> fetchWorkItemsBatch(Set<Integer> ids) throws Exception {
        Map<Integer, WorkItem> map = new ConcurrentHashMap<>();
        if (ids.isEmpty()) return map;

        List<Integer> idList = new ArrayList<>(ids);
        int batchSize = 200;

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < idList.size(); i += batchSize) {
            List<Integer> batch = idList.subList(i, Math.min(i + batchSize, idList.size()));
            tasks.add(() -> {
                String url = ORG_URL + "/_apis/wit/workitems?ids=" +
                        join(batch) +
                        "&$expand=Fields&api-version=7.0";

                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Authorization", authHeader())
                        .timeout(Duration.ofSeconds(120))
                        .build();

                HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
                checkStatus(resp);

                JsonNode arr = MAPPER.readTree(resp.body()).path("value");
                for (JsonNode wi : arr) {
                    WorkItem item = new WorkItem();
                    item.id = wi.path("id").asInt();
                    item.projectName = wi.path("fields").path("System.TeamProject").asText();
                    item.areaPath = wi.path("fields").path("System.AreaPath").asText();
                    item.type = wi.path("fields").path("System.WorkItemType").asText();
                    item.assignedTo = wi.path("fields").path("System.AssignedTo").path("displayName").asText("");
                    item.createdBy = wi.path("fields").path("System.CreatedBy").path("displayName").asText("");
                    map.put(item.id, item);
                }
                return null;
            });
        }

        EXEC.invokeAll(tasks);
        return map;
    }

    /* ---------------- DB INSERT ---------------- */

    private static void insertReportRows(
            Connection conn,
            int initiativeId,
            Map<Integer, Set<Integer>> adjacency,
            Map<Integer, WorkItem> details
    ) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO initiative_report " +
                        "(initiative_id, workitem_id, project_name, area_path, type, assignee, reporter, depth) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
            traverseAndInsert(ps, initiativeId, adjacency, details, 0, initiativeId);
        }
    }

    private static void traverseAndInsert(
            PreparedStatement ps,
            int parentId,
            Map<Integer, Set<Integer>> adjacency,
            Map<Integer, WorkItem> details,
            int depth,
            int initiativeId
    ) throws SQLException {
        WorkItem wi = details.get(parentId);
        if (wi != null) {
            ps.setInt(1, initiativeId);
            ps.setInt(2, wi.id);
            ps.setString(3, wi.projectName);
            ps.setString(4, wi.areaPath);
            ps.setString(5, wi.type);
            ps.setString(6, wi.assignedTo);
            ps.setString(7, wi.createdBy);
            ps.setInt(8, depth);
            ps.addBatch();
        }

        for (int childId : adjacency.getOrDefault(parentId, Collections.emptySet())) {
            traverseAndInsert(ps, childId, adjacency, details, depth + 1, initiativeId);
        }

        ps.executeBatch();
    }

    /* ---------------- HELPERS ---------------- */

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static String join(List<Integer> ids) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(ids.get(i));
        }
        return sb.toString();
    }

    private static String authHeader() {
        String token = ":" + PAT;
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }

    private static void checkStatus(HttpResponse<?> resp) throws Exception {
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new RuntimeException("HTTP " + resp.statusCode() + ": " + resp.body());
        }
    }

    /* ---------------- MODELS ---------------- */

    private static class WorkItem {
        int id;
        String projectName;
        String areaPath;
        String type;
        String assignedTo;
        String createdBy;
    }
}
