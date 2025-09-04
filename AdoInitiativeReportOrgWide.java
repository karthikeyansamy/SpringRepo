import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class AdoInitiativeReportOrgWide {

    private static final String ORG_URL = "https://dev.azure.com/YOURORG";
    private static final String PAT = "YOUR_PERSONAL_ACCESS_TOKEN";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        List<Integer> initiativeIds = fetchAllInitiatives();

        // adjacency map (parent → children)
        Map<Integer, Set<Integer>> adjacency = new HashMap<>();
        Map<Integer, WorkItem> details = new HashMap<>();

        // fetch hierarchy for all initiatives
        for (int initId : initiativeIds) {
            fetchHierarchy(initId, adjacency);
        }

        // fetch details for all collected IDs
        Set<Integer> allIds = new HashSet<>();
        allIds.addAll(initiativeIds);
        adjacency.values().forEach(allIds::addAll);
        fetchWorkItemDetails(new ArrayList<>(allIds), details);

        // traverse and collect report rows
        List<ReportRow> reportRows = new ArrayList<>();
        for (int initId : initiativeIds) {
            traverseAndAddRows(initId, adjacency, details, reportRows, 0, initId);
        }

        // write CSV
        writeCsv(reportRows);
        System.out.println("Report generated: initiative_hierarchy_report.csv");
    }

    // ----------------- Fetch Initiatives -----------------
    private static List<Integer> fetchAllInitiatives() throws Exception {
        List<Integer> ids = new ArrayList<>();
        int skip = 0, batch = 200;
        boolean hasMore = true;

        while (hasMore) {
            String wiql = "{ \"query\": \"SELECT [System.Id] FROM WorkItems WHERE [System.WorkItemType] = 'Initiative' ORDER BY [System.Id] ASC\" }";
            List<Integer> batchIds = runWiql(wiql, skip, batch);
            if (batchIds.isEmpty()) {
                hasMore = false;
            } else {
                ids.addAll(batchIds);
                skip += batch;
            }
        }
        return ids;
    }

    private static List<Integer> runWiql(String wiql, int skip, int top) throws Exception {
        List<Integer> ids = new ArrayList<>();
        String url = ORG_URL + "/_apis/wit/wiql?api-version=7.0&$skip=" + skip + "&$top=" + top;
        JsonNode json = postJson(url, wiql);
        for (JsonNode item : json.get("workItems")) {
            ids.add(item.get("id").asInt());
        }
        return ids;
    }

    // ----------------- Fetch Hierarchy Links -----------------
    private static void fetchHierarchy(int rootId, Map<Integer, Set<Integer>> adjacency) throws Exception {
        String url = ORG_URL + "/_apis/wit/workitems/" + rootId + "?$expand=relations&api-version=7.0";
        JsonNode json = getJson(url);

        if (json.has("relations")) {
            for (JsonNode rel : json.get("relations")) {
                if (rel.get("rel").asText().equals("System.LinkTypes.Hierarchy-Forward")) {
                    String urlRef = rel.get("url").asText();
                    int childId = Integer.parseInt(urlRef.substring(urlRef.lastIndexOf("/") + 1));
                    adjacency.computeIfAbsent(rootId, k -> new HashSet<>()).add(childId);
                    // recurse to build deeper hierarchy
                    fetchHierarchy(childId, adjacency);
                }
            }
        }
    }

    // ----------------- Fetch Work Item Details -----------------
    private static void fetchWorkItemDetails(List<Integer> ids, Map<Integer, WorkItem> details) throws Exception {
        int batch = 200;
        for (int i = 0; i < ids.size(); i += batch) {
            List<Integer> sub = ids.subList(i, Math.min(i + batch, ids.size()));
            String idStr = String.join(",", sub.stream().map(Object::toString).toArray(String[]::new));
            String url = ORG_URL + "/_apis/wit/workitems?ids=" + idStr + "&api-version=7.0";
            JsonNode json = getJson(url);

            for (JsonNode wi : json.get("value")) {
                WorkItem item = new WorkItem();
                item.id = wi.get("id").asInt();
                JsonNode f = wi.get("fields");
                item.projectName = f.path("System.TeamProject").asText("");
                item.areaPath = f.path("System.AreaPath").asText("");
                item.type = f.path("System.WorkItemType").asText("");
                item.assignedTo = f.path("System.AssignedTo").path("displayName").asText("");
                item.createdBy = f.path("System.CreatedBy").path("displayName").asText("");
                details.put(item.id, item);
            }
        }
    }

    // ----------------- Traverse + Build Report -----------------
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

    // ----------------- Write CSV -----------------
    private static void writeCsv(List<ReportRow> reportRows) throws IOException {
        try (FileWriter fw = new FileWriter("initiative_hierarchy_report.csv", StandardCharsets.UTF_8)) {
            fw.write("ProjectName,AreaPath,WorkitemType,InitiativeID,WorkitemID,Assignee,Reporter,HierarchyLevel\n");
            for (ReportRow r : reportRows) {
                fw.write(String.format("\"%s\",\"%s\",\"%s\",%d,%d,\"%s\",\"%s\",%d\n",
                        safe(r.projectName),
                        safe(r.areaPath),
                        safe(r.type),
                        r.initiativeId,
                        r.workItemId,
                        safe(r.assignedTo),
                        safe(r.createdBy),
                        r.depth
                ));
            }
        }
    }

    private static String safe(String s) {
        return s == null ? "" : s.replace("\"", "'");
    }

    // ----------------- HTTP Helpers -----------------
    private static JsonNode getJson(String url) throws Exception {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet req = new HttpGet(url);
            req.addHeader("Authorization", "Basic " +
                    Base64.getEncoder().encodeToString((":" + PAT).getBytes(StandardCharsets.UTF_8)));
            HttpResponse res = client.execute(req);
            return mapper.readTree(res.getEntity().getContent());
        }
    }

    private static JsonNode postJson(String url, String body) throws Exception {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            org.apache.http.client.methods.HttpPost req = new org.apache.http.client.methods.HttpPost(url);
            req.addHeader("Content-Type", "application/json");
            req.addHeader("Authorization", "Basic " +
                    Base64.getEncoder().encodeToString((":" + PAT).getBytes(StandardCharsets.UTF_8)));
            req.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
            HttpResponse res = client.execute(req);
            return mapper.readTree(res.getEntity().getContent());
        }
    }

    // ----------------- Models -----------------
    static class WorkItem {
        int id;
        String projectName;
        String areaPath;
        String type;
        String assignedTo;
        String createdBy;
    }

    static class ReportRow {
        String projectName;
        String areaPath;
        String type;
        int initiative
