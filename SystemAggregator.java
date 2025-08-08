import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.web.client.RestTemplate;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.*;

public class SystemAggregator {

    private static final ExecutorService executor = Executors.newFixedThreadPool(3);
    private static final RestTemplate restTemplate = new RestTemplate();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String systemId = getSystemIdFromSystemA();
        String appId = getAppIdFromPostgres(systemId);

        Future<String> responseB = executor.submit(() -> callSystem("http://system-b/api?appId=" + appId));
        Future<String> responseC = executor.submit(() -> callSystem("http://system-c/api?appId=" + appId));
        Future<String> responseD = executor.submit(() -> callSystem("http://system-d/api?appId=" + appId + "&systemId=" + systemId));

        // Combine responses
        ObjectNode consolidated = mapper.createObjectNode();
        consolidated.put("systemId", systemId);
        consolidated.put("appId", appId);
        consolidated.set("systemBResponse", mapper.readTree(responseB.get()));
        consolidated.set("systemCResponse", mapper.readTree(responseC.get()));
        consolidated.set("systemDResponse", mapper.readTree(responseD.get()));

        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(consolidated));

        executor.shutdown();
    }

    private static String getSystemIdFromSystemA() {
        // Simulate System A call
        return "SYS12345";
    }

    private static String getAppIdFromPostgres(String systemId) throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/yourdb";
        String user = "youruser";
        String password = "yourpassword";
        String appId = null;

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            PreparedStatement stmt = conn.prepareStatement("SELECT appid FROM apps WHERE systemid = ?");
            stmt.setString(1, systemId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                appId = rs.getString("appid");
            }
        }

        if (appId == null) {
            throw new RuntimeException("AppId not found for SystemId: " + systemId);
        }

        return appId;
    }

    private static String callSystem(String url) {
        return restTemplate.getForObject(url, String.class);
    }
}
