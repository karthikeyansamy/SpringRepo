import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class MultiSystemFetcher {

    private static final ExecutorService executor = Executors.newFixedThreadPool(4);
    private static final RestTemplate restTemplate = new RestTemplate();
    private static final ObjectMapper mapper = new ObjectMapper();

    // ======== STEP 1: Get SystemId from System A ========
    private static String getSystemIdFromSystemA() {
        // Mock REST call or any external fetch
        return "SYS-12345";
    }

    // ======== STEP 2: Fetch AppId from Postgres ========
    private static String getAppIdFromPostgres(String systemId) throws Exception {
        String appId = null;
        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/mydb", "user", "password")) {
            PreparedStatement stmt = conn.prepareStatement("SELECT appid FROM systems WHERE systemid=?");
            stmt.setString(1, systemId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                appId = rs.getString("appid");
            }
        }
        return appId;
    }

    // ======== STEP 3: System B API ========
    private static JsonNode callSystemB(String appId) {
        String url = "http://system-b/api/data/" + appId;
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
        return parseJson(response.getBody());
    }

    // ======== STEP 4: System C API (returns HostList) ========
    private static List<String> callSystemC(String appId) {
        String url = "http://system-c/api/hosts/" + appId;
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
        JsonNode node = parseJson(response.getBody());
        List<String> hostList = new ArrayList<>();
        node.get("hosts").forEach(h -> hostList.add(h.asText()));
        return hostList;
    }

    // ======== STEP 5: System D API ========
    private static JsonNode callSystemD(String appId, String systemId) {
        String url = "http://system-d/api/details?appid=" + appId + "&systemid=" + systemId;
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
        return parseJson(response.getBody());
    }

    // ======== STEP 6: System F API ========
    private static JsonNode callSystemF(String appId, List<String> hostList) {
        String url = "http://system-f/api/scan?appid=" + appId;
        ResponseEntity<String> response = restTemplate.postForEntity(url, hostList, String.class);
        return parseJson(response.getBody());
    }

    private static JsonNode parseJson(String json) {
        try {
            return mapper.readTree(json);
        } catch (Exception e) {
            throw new RuntimeException("Invalid JSON response", e);
        }
    }

    // ======== MAIN EXECUTION ========
    public static void main(String[] args) throws Exception {
        String systemId = getSystemIdFromSystemA();
        String appId = getAppIdFromPostgres(systemId);

        // Run System B, C, D in parallel first
        CompletableFuture<JsonNode> futureB = CompletableFuture.supplyAsync(() -> callSystemB(appId), executor);
        CompletableFuture<List<String>> futureC = CompletableFuture.supplyAsync(() -> callSystemC(appId), executor);
        CompletableFuture<JsonNode> futureD = CompletableFuture.supplyAsync(() -> callSystemD(appId, systemId), executor);

        // System F depends on C (hostList), so chain it
        CompletableFuture<JsonNode> futureF = futureC.thenApplyAsync(hosts -> callSystemF(appId, hosts), executor);

        // Combine all futures
        CompletableFuture<Void> allDone = CompletableFuture.allOf(futureB, futureC, futureD, futureF);

        // Wait for completion
        allDone.join();

        // Consolidate into final JSON
        Map<String, Object> finalResult = new LinkedHashMap<>();
        finalResult.put("systemId", systemId);
        finalResult.put("appId", appId);
        finalResult.put("systemB", futureB.get());
        finalResult.put("systemC_Hosts", futureC.get());
        finalResult.put("systemD", futureD.get());
        finalResult.put("systemF", futureF.get());

        String consolidatedJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(finalResult);
        System.out.println(consolidatedJson);

        executor.shutdown();
    }
}
