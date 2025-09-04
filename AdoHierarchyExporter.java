import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class AdoHierarchyExporter {
    private static final String ORG_URL = "https://dev.azure.com/{your_org}";
    private static final String PAT = "{your_pat}";

    private static final HttpClient client = HttpClient.newHttpClient();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Map<Integer, WorkItem> workItemMap = new HashMap<>();
        List<WorkItemLink> allLinks = fetchAllWorkItemLinks();

        for (WorkItemLink link : allLinks) {
            fetchAndFillWorkItem(link.sourceId, workItemMap);
            fetchAndFillWorkItem(link.targetId, workItemMap);
        }

        // Write CSV
        try (FileWriter writer = new FileWriter("workitem_hierarchy.csv", StandardCharsets.UTF_8)) {
            writer.write("ParentId,ChildId,ParentType,ChildType,ParentTitle,ChildTitle,Project,AreaPath,Assignee,Reporter\n");
            for (WorkItemLink link : allLinks) {
                WorkItem parent = workItemMap.get(link.sourceId);
                WorkItem child = workItemMap.get(link.targetId);
                writer.write(String.format("%d,%d,%s,%s,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n",
                        link.sourceId,
                        link.targetId,
                        parent != null ? parent.type : "",
                        child != null ? child.type : "",
                        parent != null ? parent.title : "",
                        child != null ? child.title : "",
                        child != null ? child.projectName : "",
                        child != null ? child.areaPath : "",
                        child != null ? child.assignedTo : "",
                        child != null ? child.createdBy : ""
                ));
            }
        }
        System.out.println("CSV exported: workitem_hierarchy.csv");
    }

    private static List<WorkItemLink> fetchAllWorkItemLinks() throws Exception {
        List<WorkItemLink> links = new ArrayList<>();
        int lastSeenId = 0;

        while (true) {
            String wiql = "{ \"query\": \"SELECT [System.Id],[System.WorkItemType],[System.Title] " +
                    "FROM WorkItemLinks WHERE [System.Links.LinkType] = 'System.LinkTypes.Hierarchy-Forward' " +
                    "AND Target.[System.Id] > " + lastSeenId +
                    " ORDER BY Target.[System.Id] ASC\" }";

            String url = ORG_URL + "/_apis/wit/wiql?api-version=7.0";

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "Basic " + Base64.getEncoder().encodeToString((":" + PAT).getBytes()))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(wiql))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode root = mapper.readTree(response.body());

            JsonNode workItemRelations = root.get("workItemRelations");
            if (workItemRelations == null || workItemRelations.isEmpty()) {
                break;
            }

            int maxIdThisPage = lastSeenId;
            for (JsonNode relation : workItemRelations) {
                if (relation.has("source")) {
                    int sourceId = relation.get("source").get("id").asInt();
                    int targetId = relation.get("target").get("id").asInt();
                    links.add(new WorkItemLink(sourceId, targetId));
                    if (targetId > maxIdThisPage) {
                        maxIdThisPage = targetId;
                    }
                }
            }

            if (maxIdThisPage == lastSeenId) {
                break; // no progress -> stop
            }
            lastSeenId = maxIdThisPage;
        }

        // Deduplicate
        Set<String> seen = new HashSet<>();
        List<WorkItemLink> deduped = new ArrayList<>();
        for (WorkItemLink l : links) {
            String key = l.sourceId + "-" + l.targetId;
            if (seen.add(key)) {
                deduped.add(l);
            }
        }

        return deduped;
    }

    private static void fetchAndFillWorkItem(int id, Map<Integer, WorkItem> cache) throws Exception {
        if (cache.containsKey(id)) return;

        String url = ORG_URL + "/_apis/wit/workitems/" + id + "?api-version=7.0";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString((":" + PAT).getBytes()))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            System.err.println("Failed to fetch workitem " + id + " : " + response.body());
            return;
        }

        JsonNode wi = mapper.readTree(response.body());
        WorkItem item = new WorkItem();
        item.id = id;
        item.title = wi.at("/fields/System.Title").asText("");
        item.type = wi.at("/fields/System.WorkItemType").asText("");
        item.projectName = wi.at("/fields/System.TeamProject").asText("");
        item.areaPath = wi.at("/fields/System.AreaPath").asText("");
        item.assignedTo = wi.at("/fields/System.AssignedTo/displayName").asText("");
        item.createdBy = wi.at("/fields/System.CreatedBy/displayName").asText("");

        cache.put(id, item);
    }

    static class WorkItem {
        int id;
        String title;
        String type;
        String projectName;
        String areaPath;
        String assignedTo;
        String createdBy;
    }

    static class WorkItemLink {
        int sourceId;
        int targetId;

        WorkItemLink(int s, int t) {
            this.sourceId = s;
            this.targetId = t;
        }
    }
}
