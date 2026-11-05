package xsgao.lakehouseconnector.scan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;
import java.util.logging.Level;

import xsgao.lakehouseconnector.planner.TableSplit;

/**
 * Data-plane execution engine that processes a list of TableSplits concurrently
 * using a bounded thread pool (ExecutorService).
 */
public class TableScanExecutor {
    private static final Logger logger = Logger.getLogger(TableScanExecutor.class.getName());
    private final int maxThreads;

    public TableScanExecutor() {
        this(Runtime.getRuntime().availableProcessors());
    }

    public TableScanExecutor(int maxThreads) {
        this.maxThreads = Math.max(1, maxThreads);
    }

    /**
     * Executes table splits concurrently across worker threads.
     *
     * @param splits List of pruned TableSplits from the control plane
     * @return List of ScanResults capturing scan metrics for each split
     */
    public List<ScanResult> execute(List<TableSplit> splits) throws Exception {
        if (splits == null || splits.isEmpty()) {
            return Collections.emptyList();
        }

        int poolSize = Math.min(splits.size(), maxThreads);
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);

        try {
            List<Callable<ScanResult>> tasks = new ArrayList<>();
            for (TableSplit split : splits) {
                tasks.add(() -> scanSplit(split));
            }

            List<Future<ScanResult>> futures = executor.invokeAll(tasks);
            List<ScanResult> results = new ArrayList<>();

            for (Future<ScanResult> future : futures) {
                // Blocks until the individual split scan completes and retrieves the result
                results.add(future.get());
            }

            return results;
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Scans an individual TableSplit.
     * In Module 04, this acts as the parallel task dispatcher.
     * In Module 05, this will be wired to the Parquet reader for actual column projection and decoding.
     */
    private ScanResult scanSplit(TableSplit split) {
        long startTime = System.currentTimeMillis();

        // Placeholder simulation for fetching bytes from S3 per split
        // (Will be replaced with Parquet reader invocation in Module 05)
        long recordCount = split.getRecordCount();

        try {
            // Simulate execution time for the scan
            logger.info("***** [SPLIT SCAN] File: " + split.getFilePath() + " | Records: " + recordCount + " *****");
            Thread.sleep(250 + (long)(Math.random() * 250)); // Random sleep between 250-500ms
        } catch (InterruptedException ex) {
            logger.log(Level.SEVERE, "Scan interrupted", ex);
        }

        long bytesRead = recordCount * 1024L;
        long duration = System.currentTimeMillis() - startTime;

        return new ScanResult(split.getFilePath(), recordCount, bytesRead, duration);
    }
}