import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class AdoInitiativeReportOrgWide2 {

    // === CONFIG ===
    private static final String ORG_URL = "https://dev.azure.com/{your_org}"; // replace
    private static final String PAT = "{your_pat}";                           // replace

    // Tunables
    private static final int INITIATIVE_PAGE_SIZE = 500;
    private static final int INITIATIVE_BATCH_SIZE = 200;
    private static final int WORKITEM_BATCH_SIZE = 200;
    private static final int SLEEP_MS_BETWEEN_HEAVY_CALLS = 200;

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        List<String> projects = fetchProjects();
        System.out.println("Projects found: " + projects.size());

        List<ReportRow> reportRows = new ArrayList<>();

        for (String project : projects) {
            System.out.println("\n=== Processing project: " + project + " ===");
            int lastInitiativeId = 0;
            int pageNum = 1;

            while (true) {
                // 1) Page initiatives using keyset pagination
                List<Integer> initiatives = fetchInitiativesPage(project, lastInitiativeId, INITIATIVE_PAGE_SIZE);
                if (initiatives.isEmpty()) {
                    System.out.println("  No more initiatives for project: " + project);
                    break;
                }
                System.out.printf("  Page %d: fetched %d initiatives (ids %d..%d)%n",
                        pageNum++, initiatives.size(),
                        initiatives.get(0), initiatives.get(initiatives.size() - 1));

                // 2) Process initiatives in batches to expand hierarchies
                for (int i = 0; i < initiatives.size(); i += INITIATIVE_BATCH_SIZE) {
                    List<Integer> batch = initiatives.subList(i, Math.min(i + INITIATIVE_BATCH_SIZE, initiatives.size()));
                    System.out.printf("    Expanding batch %d..%d (size=%d)%n", batch.get(0), batch.get(batch.size() - 1), batch.size());

                    // Expand hierarchies for this batch (recursive WIQL)
                    Map<Integer, Set<Integer>> initiativeToChildren = fetchHierarchiesForInitiatives(project, batch);

                    // Collect all involved IDs (initiatives + their children)
                    Set<Integer> allIds = new HashSet<>(batch);
                    initiativeToChildren.values().forEach(allIds::addAll);

                    // 3) Enrich details via workitemsbatch
                    Map<Integer, WorkItem> details = fetchWorkItemsBatch(allIds);

                    // 4) Traverse and build rows (preserves hierarchy depth)
                    for (int initiativeId : batch) {
                        traverseAndAddRows(
                                initiativeId,
                                initiativeToChildren,
                                details,
                                reportRows,
                                0, // start depth = 0
                                initiativeId
                        );
                    }

                    // small pause to reduce throttle risk
                    Thread.sleep(SLEEP_MS_BETWEEN_HEAVY_CALLS);
                }

                // advance keyset
                lastInitiativeId = initiatives.get(initiatives.size() - 1);
            }
        }

        // Write CSV final
        try (FileWriter fw = new FileWriter("initiative_report_orgwide.csv", StandardCharsets.UTF_8)) {
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
        System.out.println("\n✅ Done. CSV: initiative_report_orgwide.csv (rows=" + reportRows.size() + ")");
    }

    // ---------- NEW: Iterative fetch for a single Initiative ----------
    /**
     * Iteratively expands the hierarchy under a single initiative using BFS.
     * This safely handles very large hierarchies (20k+ nodes) by fetching
     * direct children per node and paginating on Target.[System.Id].
     *
     * Returns an adjacency map: parentId -> set(childIds)
     */
    public static Map<Integer, Set<Integer>> fetchHierarchyIterativeForInitiative(String project, int initiativeId) throws Exception {
        Map<Integer, Set<Integer>> adjacency = new HashMap<>();
        Set<Integer> visited = new HashSet<>();
        Deque<Integer> queue = new ArrayDeque<>();

        visited.add(initiativeId);
        queue.add(initiativeId);

        while (!queue.isEmpty()) {
            int source = queue.poll();
            // fetch direct children of 'source' using keyset pagination on Target.Id
            Set<Integer> children = fetchChildrenForSource(project, source);
            adjacency.put(source, children);

            // enqueue unseen children
            for (int child : children) {
                if (!visited.contains(child)) {
                    visited.add(child);
                    queue.add(child);
                }
            }

            // small pause to avoid throttling in large trees
            Thread.sleep(SLEEP_MS_BETWEEN_HEAVY_CALLS);
        }

        return adjacency;
    }

    /**
     * Helper: fetches ALL direct children for a given source work item id,
     * using keyset pagination on Target.[System.Id] to handle very large numbers.
     */
    private static Set<Integer> fetchChildrenForSource(String project, int sourceId) throws Exception {
        Set<Integer> children = new LinkedHashSet<>();
        int lastTargetId = 0;
        int progressGuard = 0;

        while (true) {
            // Build WIQL to fetch links where Source = sourceId (and Target > lastTargetId if paginating)
            String where = "Source.[System.Id] = " + sourceId + " AND [System.Links.LinkType] = 'System.LinkTypes.Hierarchy-Forward'";
            if (lastTargetId > 0) where += " AND Target.[System.Id] > " + lastTargetId;

            String wiql = "{ \"query\": \"SELECT [System.Id] FROM WorkItemLinks WHERE " +
                    where +
                    " ORDER BY Target.[System.Id] ASC\" }";

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
            if (rels == null || rels.isEmpty()) break;

            int maxTargetThisPage = lastTargetId;
            int addedThisPage = 0;

            for (JsonNode rel : rels) {
                if (rel.has("target")) {
                    int target = rel.path("target").path("id").asInt();
                    if (!children.contains(target)) {
                        children.add(target);
                        addedThisPage++;
                    }
                    if (target > maxTargetThisPage) maxTargetThisPage = target;
                }
            }

            // guard: if no progress, break to avoid infinite loop
            if (maxTargetThisPage <= lastTargetId) {
                progressGuard++;
                if (progressGuard > 3) break;
                else break;
            }

            lastTargetId = maxTargetThisPage;

            // If fewer than what we expect returned, assume end for this source
            if (addedThisPage == 0) break;
            // Continue loop to fetch next page (Target.Id > lastTargetId)
        }

        return children;
    }

    // ---------- Recursive traversal (preserve hierarchy) ----------
    private static void traverseAndAddRows(
            int currentId,
            Map<Integer, Set<Integer>> adjacency,
            Map<Integer, WorkItem> details,
            List<ReportRow> reportRows,
            int depth,
            int initiativeId
    ) {
        WorkItem wi = details.get(currentId);
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

        for (int childId : adjacency.getOrDefault(currentId, Collections.emptySet())) {
            traverseAndAddRows(childId, adjacency, details, reportRows, depth + 1, initiativeId);
        }
    }

    // ---------- HTTP helper & auth ----------
    private static String authHeader() {
        return "Basic " + Base64.getEncoder().encodeToString((":" + PAT).getBytes(StandardCharsets.UTF_8));
    }

    // ---------- 1) Projects ----------
    private static List<String> fetchProjects() throws Exception {
        String url = ORG_URL + "/_apis/projects?api-version=7.0";
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", authHeader())
                .timeout(Duration.ofSeconds(60))
                .GET()
                .build();
        HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
        checkStatus(resp);
        JsonNode arr = MAPPER.readTree(resp.body()).path("value");
        List<String> projects = new ArrayList<>();
        for (JsonNode n : arr) {
            projects.add(n.path("name").asText());
        }
        return projects;
    }

    // ---------- 2) Initiatives paging (keyset pagination per project) ----------
    private static List<Integer> fetchInitiativesPage(String project, int lastIdExclusive, int pageSize) throws Exception {
        String where = "WHERE [System.WorkItemType] = 'Initiative'";
        if (lastIdExclusive > 0) where += " AND [System.Id] > " + lastIdExclusive;

        String wiql = "{ \"query\": \"SELECT [System.Id] FROM WorkItems " +
                where +
                " ORDER BY [System.Id] ASC\" }";

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
        List<Integer> ids = new ArrayList<>();
        for (JsonNode n : arr) ids.add(n.path("id").asInt());

        if (ids.size() > pageSize) return ids.subList(0, pageSize);
        return ids;
    }

    // ---------- 3) Expand hierarchies for a batch of initiatives using one recursive WIQL ----------
    private static Map<Integer, Set<Integer>> fetchHierarchiesForInitiatives(String project, List<Integer> initiativeIds) throws Exception {
        if (initiativeIds.isEmpty()) return Collections.emptyMap();

        String idList = initiativeIds.stream().map(String::valueOf).collect(Collectors.joining(","));
        String wiql = "{ \"query\": \"SELECT [System.Id] FROM WorkItemLinks " +
                "WHERE [Source].[System.Id] IN (" + idList + ") " +
                "AND [System.Links.LinkType] = 'System.LinkTypes.Hierarchy-Forward' " +
                "ORDER BY [System.Id] MODE (Recursive)\" }";

        String url = ORG_URL + "/" + encode(project) + "/_apis/wit/wiql?api-version=7.0";
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", authHeader())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(wiql))
                .timeout(Duration.ofMinutes(3))
                .build();

        HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
        checkStatus(resp);

        JsonNode rels = MAPPER.readTree(resp.body()).path("workItemRelations");

        Map<Integer, Set<Integer>> map = new HashMap<>();
        for (JsonNode rel : rels) {
            if (rel.has("source") && rel.has("target")) {
                int source = rel.path("source").path("id").asInt();
                int target = rel.path("target").path("id").asInt();
                map.computeIfAbsent(source, k -> new HashSet<>()).add(target);
            }
        }

        for (Integer id : initiativeIds) map.putIfAbsent(id, Collections.emptySet());

        return map;
    }

    // ---------- 4) Batch fetch work item details ----------
    private static Map<Integer, WorkItem> fetchWorkItemsBatch(Set<Integer> ids) throws Exception {
        Map<Integer, WorkItem> result = new HashMap<>();
        List<Integer> idList = new ArrayList<>(ids);
        for (int i = 0; i < idList.size(); i += WORKITEM_BATCH_SIZE) {
            List<Integer> batch = idList.subList(i, Math.min(i + WORKITEM_BATCH_SIZE, idList.size()));
            String url = ORG_URL + "/_apis/wit/workitemsbatch?api-version=7.0";

            Map<String, Object> bodyObj = new HashMap<>();
            bodyObj.put("ids", batch);
            bodyObj.put("fields", List.of(
                    "System.Id",
                    "System.Title",
                    "System.WorkItemType",
                    "System.TeamProject",
                    "System.AreaPath",
                    "System.AssignedTo",
                    "System.CreatedBy"
            ));

            String body = MAPPER.writeValueAsString(bodyObj);

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", authHeader())
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofMinutes(2))
                    .build();

            HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
            checkStatus(resp);

            JsonNode arr = MAPPER.readTree(resp.body()).path("value");
            for (JsonNode wi : arr) {
                WorkItem w = new WorkItem();
                w.id = wi.path("id").asInt();
                w.title = wi.at("/fields/System.Title").asText("");
                w.type = wi.at("/fields/System.WorkItemType").asText("");
                w.projectName = wi.at("/fields/System.TeamProject").asText("");
                w.areaPath = wi.at("/fields/System.AreaPath").asText("");
                JsonNode at = wi.at("/fields/System.AssignedTo");
                w.assignedTo = at.isObject() ? at.path("displayName").asText("") : at.asText("");
                JsonNode cb = wi.at("/fields/System.CreatedBy");
                w.createdBy = cb.isObject() ? cb.path("displayName").asText("") : cb.asText("");
                result.put(w.id, w);
            }
            Thread.sleep(SLEEP_MS_BETWEEN_HEAVY_CALLS);
        }
        return result;
    }

    // ---------- Helpers ----------
    private static void checkStatus(HttpResponse<?> resp) {
        int c = resp.statusCode();
        if (c < 200 || c >= 300) {
            throw new RuntimeException("HTTP " + c + " -> " + String.valueOf(resp.body()));
        }
    }

    private static String safe(String s) { return s == null ? "" : s.replace("\"", "\"\""); }

    private static String encode(String s) {
        return s.replace(" ", "%20");
    }

    // ---------- POJOs ----------
    static class WorkItem {
        int id;
        String title;
        String type;
        String projectName;
        String areaPath;
        String assignedTo;
        String createdBy;
    }

    static class ReportRow {
        String projectName;
        String areaPath;
        String type;
        int initiativeId;
        int workItemId;
        String assignedTo;
        String createdBy;
        int depth; // NEW → hierarchy level
    }
}
