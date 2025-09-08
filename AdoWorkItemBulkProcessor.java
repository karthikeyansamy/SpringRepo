import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class AdoWorkItemBulkProcessor {

    // === CONFIG ===
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/yourdb"; // change
    private static final String DB_USER = "youruser"; // change
    private static final String DB_PASS = "yourpass"; // change

    private static final int FETCH_DB_BATCH_SIZE = 50000;   // how many IDs per DB fetch
    private static final int ADO_BATCH_SIZE = 200;          // Azure DevOps workitemsbatch limit
    private static final int THREAD_POOL_SIZE = 10;         // number of parallel API workers

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public static void main(String[] args) throws Exception {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

            int offset = 0;
            int processed = 0;

            while (true) {
                List<Integer> ids = fetchWorkItemIds(conn, offset, FETCH_DB_BATCH_SIZE);
                if (ids.isEmpty()) {
                    System.out.println("✅ No more IDs left. Done.");
                    break;
                }

                System.out.printf("🔄 Processing DB batch offset=%d size=%d%n", offset, ids.size());

                // Submit ADO fetch jobs in parallel
                List<Future<Map<Integer, AdoWorkItemFetcher.WorkItem>>> futures = new ArrayList<>();

                for (int i = 0; i < ids.size(); i += ADO_BATCH_SIZE) {
                    List<Integer> subBatch = ids.subList(i, Math.min(i + ADO_BATCH_SIZE, ids.size()));
                    Set<Integer> batchSet = new HashSet<>(subBatch);

                    futures.add(EXECUTOR.submit(() -> {
                        try {
                            return AdoWorkItemFetcher.fetchWorkItems(batchSet);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return Collections.emptyMap();
                        }
                    }));
                }

                // Collect results and insert into DB
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO workitems_details " +
                                "(id, title, type, project_name, area_path, assignee, reporter) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                                "ON CONFLICT (id) DO UPDATE SET " +
                                "title=EXCLUDED.title, type=EXCLUDED.type, " +
                                "project_name=EXCLUDED.project_name, area_path=EXCLUDED.area_path, " +
                                "assignee=EXCLUDED.assignee, reporter=EXCLUDED.reporter"
                )) {
                    for (Future<Map<Integer, AdoWorkItemFetcher.WorkItem>> f : futures) {
                        Map<Integer, AdoWorkItemFetcher.WorkItem> details = f.get();

                        for (AdoWorkItemFetcher.WorkItem wi : details.values()) {
                            ps.setInt(1, wi.id);
                            ps.setString(2, wi.title);
                            ps.setString(3, wi.type);
                            ps.setString(4, wi.projectName);
                            ps.setString(5, wi.areaPath);
                            ps.setString(6, wi.assignedTo);
                            ps.setString(7, wi.createdBy);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    }
                }

                processed += ids.size();
                offset += FETCH_DB_BATCH_SIZE;

                System.out.printf("✅ Written %d records so far%n", processed);

                // Release memory
                ids.clear();
                System.gc();
            }
        }

        EXECUTOR.shutdown();
        AdoWorkItemFetcher.EXECUTOR.shutdown();
    }

    private static List<Integer> fetchWorkItemIds(Connection conn, int offset, int limit) throws SQLException {
        List<Integer> ids = new ArrayList<>();
        String sql = "SELECT workitem_id FROM workitems_table ORDER BY workitem_id ASC OFFSET ? LIMIT ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, offset);
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getInt(1));
                }
            }
        }
        return ids;
    }
}
