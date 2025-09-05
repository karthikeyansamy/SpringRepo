import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileWriter;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

public class AdoInitiativeReportOrgWide2 {

    private static final String ORG_URL = "https://dev.azure.com/YOUR_ORG";
    private static final String PAT = "YOUR_PERSONAL_ACCESS_TOKEN";
    private static final HttpClient CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        List<String> projects = fetchProjects();

        List<ReportRow> reportRows = new ArrayList<>();
        Map<Integer, WorkItem> details = new HashMap<>();

        for (String project : projects) {
            List<Integer> initiatives = fetchAllInitiatives(project);

            expandInitiatives(project, initiatives, details, reportRows);
        }

        writeReport(reportRows);
        System.out.println("✅ Report generated successfully.");
    }

    /* ---------------- CORE LOGIC ---------------- */

    private static List<String> fetchProjects() throws Exception {
        String url = ORG_URL + "/_apis/projects?api-version=7.0";
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", authHeader())
                .timeout(Duration.ofSeconds(60))
                .build();

        HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
        checkStatus(resp);

        List<String> projects = new ArrayList<>();
        JsonNode arr = MAPPER.readTree(resp.body()).path("value");
        for (JsonNode p : arr) {
            projects.add(p.path("name").asText());
        }
        return projects;
    }

    private static List<Integer> fetchAllInitiatives(String project) throws Exception {
        List<Integer> initiatives = new ArrayList<>();
        int skip = 0;
        int batch = 200;

        while (true) {
            String wiql = "{ \"query\": \"SELECT [System.Id] FROM WorkItems WHERE " +
                    "[System.WorkItemType] = 'Initiative' " +
                    "ORDER BY [System.Id] OFFSET " + skip + " ROWS FETCH NEXT " + batch + " ROWS ONLY\" }";

            String url = ORG_URL + "/" + encode(project) + "/_apis/wit/wiql?api-version=7.0";
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", authHeader())
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(wiql))
                    .timeout(Duration.ofSeconds(120))
                    .build();

            HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
            checkStatus(resp);

            JsonNode arr = MAPPER.readTree(resp.body()).path("workItems");
            if (arr.isEmpty()) break;

            for (JsonNode item : arr) {
                initiatives.add(item.path("id").asInt());
            }

            skip += batch;
        }
        return initiatives;
    }

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

    private static void expandInitiatives(
            String project,
            List<Integer> initiatives,
            Map<Integer, WorkItem> details,
            List<ReportRow> reportRows
    ) throws Exception {
        for (int initiativeId : initiatives) {
            Map<Integer, Set<Integer>> adjacency = new HashMap<>();
            Set<Integer> allIds = new HashSet<>();

            fetchRecursiveHierarchy(project, initiativeId, adjacency, allIds);

            details.putAll(fetchWorkItemsBatch(allIds));

            traverseAndAddRows(initiativeId, adjacency, details, reportRows, 0, initiativeId);
        }
    }

    private static void traverseAndAddRows(
            int parentId,
            Map<Integer, Set<Integer>> adjacency,
            Map<Integer, WorkItem> details,
            List<ReportRow> reportRows,
            int depth,
            int initiativeId
    ) {
        WorkItem wi = details.get(parentId);
        if (wi != null) {
            ReportRow row = new ReportRow();
            row.projectName = wi.projectName;
            row.areaPath = wi.areaPath;
            row.type = wi.type;
            row.initiativeId = initiativeId;
            row.workItemId = wi.id;
            row.assignedTo = wi.assignedTo;
            row.createdBy = wi.createdBy;
            row.depth = depth;
            reportRows.add(row);
        }

        for (int childId : adjacency.getOrDefault(parentId, Collections.emptySet())) {
            traverseAndAddRows(childId, adjacency, details, reportRows, depth + 1, initiativeId);
        }
    }

    /* ---------------- FETCH DETAILS ---------------- */

    private static Map<Integer, WorkItem> fetchWorkItemsBatch(Set<Integer> ids) throws Exception {
        Map<Integer, WorkItem> map = new HashMap<>();
        if (ids.isEmpty()) return map;

        List<Integer> idList = new ArrayList<>(ids);
        int batchSize = 200;

        for (int i = 0; i < idList.size(); i += batchSize) {
            List<Integer> batch = idList.subList(i, Math.min(i + batchSize, idList.size()));
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
        }
        return map;
    }

    /* ---------------- REPORT ---------------- */

    private static void writeReport(List<ReportRow> rows) throws Exception {
        try (FileWriter writer = new FileWriter("ado_report.csv")) {
            writer.write("\"ProjectName\",\"AreaPath\",\"Workitem Type\",\"Initiative ID\",\"Workitem ID\",\"Assignee\",\"Reporter\",\"Depth\"\n");
            for (ReportRow r : rows) {
                writer.write(String.format("\"%s\",\"%s\",\"%s\",\"%d\",\"%d\",\"%s\",\"%s\",\"%d\"\n",
                        r.projectName, r.areaPath, r.type, r.initiativeId, r.workItemId, r.assignedTo, r.createdBy, r.depth));
            }
        }
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

    private static class ReportRow {
        String projectName;
        String areaPath;
        String type;
        int initiativeId;
        int workItemId;
        String assignedTo;
        String createdBy;
        int depth;
    }
}
