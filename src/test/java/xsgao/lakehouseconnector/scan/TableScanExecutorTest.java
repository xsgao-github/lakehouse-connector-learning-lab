package xsgao.lakehouseconnector.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import xsgao.lakehouseconnector.planner.TableSplit;

/**
 * Unit tests for TableScanExecutor verifying concurrent task execution,
 * thread pool scaling, and correct result aggregation across table splits.
 */
class TableScanExecutorTest {

    @Test
    void testExecuteWithMultipleSplits() throws Exception {
        TableSplit split1 = new TableSplit("s3://lakehouse-bucket/data/dt=2026-11-04/file1.parquet", 100L, 2048L, 1L);
        TableSplit split2 = new TableSplit("s3://lakehouse-bucket/data/dt=2026-11-04/file2.parquet", 250L, 4096L, 1L);
        
        TableScanExecutor executor = new TableScanExecutor(2);
        List<ScanResult> results = executor.execute(Arrays.asList(split1, split2));

        assertEquals(2, results.size(), "Should return a ScanResult for every input TableSplit");
        
        boolean found1 = results.stream().anyMatch(r -> 
            r.getFilePath().equals("s3://lakehouse-bucket/data/dt=2026-11-04/file1.parquet") && 
            r.getRecordCount() == 100L
        );
        boolean found2 = results.stream().anyMatch(r -> 
            r.getFilePath().equals("s3://lakehouse-bucket/data/dt=2026-11-04/file2.parquet") && 
            r.getRecordCount() == 250L
        );
        
        assertTrue(found1, "ScanResult for file1 should be present with correct record count");
        assertTrue(found2, "ScanResult for file2 should be present with correct record count");
    }

    @Test
    void testExecuteEmptyOrNullSplits() throws Exception {
        TableScanExecutor executor = new TableScanExecutor(4);
        
        // Test empty list
        List<ScanResult> emptyResults = executor.execute(Collections.emptyList());
        assertTrue(emptyResults.isEmpty(), "Empty splits list should yield empty scan results");
        
        // Test null list
        List<ScanResult> nullResults = executor.execute(null);
        assertTrue(nullResults.isEmpty(), "Null splits input should yield empty scan results safely");
    }

    @Test
    void testConcurrentExecutionScaling() throws Exception {
        TableSplit split1 = new TableSplit("file1.parquet", 10L, 1024L, 1L);
        TableSplit split2 = new TableSplit("file2.parquet", 20L, 1024L, 1L);
        TableSplit split3 = new TableSplit("file3.parquet", 30L, 1024L, 1L);
        TableSplit split4 = new TableSplit("file4.parquet", 40L, 1024L, 1L);

        // Initialize executor with a constrained thread pool
        TableScanExecutor executor = new TableScanExecutor(2);
        
        long startTime = System.currentTimeMillis();
        List<ScanResult> results = executor.execute(Arrays.asList(split1, split2, split3, split4));
        long duration = System.currentTimeMillis() - startTime;

        assertEquals(4, results.size(), "All 4 splits must be successfully processed and aggregated");
        assertTrue(duration < 2000L, "Execution completed within expected timing constraints: " + duration + "ms");
    }
}