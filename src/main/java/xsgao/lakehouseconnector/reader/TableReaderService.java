package xsgao.lakehouseconnector.reader;

import java.util.List;

import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.CatalogService;
import xsgao.lakehouseconnector.catalog.TableMetadata;
import xsgao.lakehouseconnector.planner.QueryPredicate;
import xsgao.lakehouseconnector.planner.SplitPlanner;
import xsgao.lakehouseconnector.planner.TableSplit;
import xsgao.lakehouseconnector.scan.ScanResult;
import xsgao.lakehouseconnector.scan.TableScanExecutor;

/**
 * Orchestrates the read path: loads table metadata from the catalog control plane,
 * executes partition and column-statistics pruning via SplitPlanner, and executes
 * table scans concurrently via TableScanExecutor.
 */
public class TableReaderService {
    private final CatalogService catalogService;
    private final SplitPlanner splitPlanner;
    private final TableScanExecutor scanExecutor;

    public TableReaderService(S3Client s3Client, String bucketName) {
        this(s3Client, bucketName, Runtime.getRuntime().availableProcessors());
    }

    public TableReaderService(S3Client s3Client, String bucketName, int maxThreads) {
        this.catalogService = new CatalogService(s3Client, bucketName);
        this.splitPlanner = new SplitPlanner();
        this.scanExecutor = new TableScanExecutor(maxThreads);
    }

    /**
     * Plans execution splits for the latest active snapshot of a table,
     * applying both partition pruning and column statistics bounds pruning.
     * 
     * @param tableName       The name of the table
     * @param partitionFilter Optional partition prefix filter (e.g., "dt=2026-11-04")
     * @param predicates      List of query predicates for min/max stats pruning
     * @return List of executable TableSplits ready for worker scanning
     */
    public List<TableSplit> planScan(
            String tableName, 
            String partitionFilter, 
            List<QueryPredicate> predicates) throws Exception {
        
        // 1. Load latest table metadata (e.g., vN.json)
        TableMetadata metadata = catalogService.loadTableMetadata(tableName);

        // 2. Generate and prune splits using SplitPlanner
        return splitPlanner.planSplits(metadata, partitionFilter, predicates);
    }

    /**
     * Supports Time-Travel auditing: plans query execution splits against 
     * a specific historical metadata version (e.g., v1.json or v2.json).
     * 
     * @param tableName       The name of the table
     * @param version         The target version number to query against
     * @param partitionFilter Optional partition prefix filter
     * @param predicates      List of query predicates for min/max stats pruning
     * @return List of executable TableSplits for the historical snapshot
     */
    public List<TableSplit> planTimeTravelScan(
            String tableName, 
            int version, 
            String partitionFilter, 
            List<QueryPredicate> predicates) throws Exception {
        
        // 1. Load historical table metadata version
        TableMetadata metadata = catalogService.loadTableMetadata(tableName, version);

        // 2. Generate and prune splits
        return splitPlanner.planSplits(metadata, partitionFilter, predicates);
    }

    /**
     * End-to-end read execution: plans the scan using pruning and executes 
     * the resulting splits concurrently across worker threads.
     * 
     * @param tableName       The name of the table
     * @param partitionFilter Optional partition prefix filter
     * @param predicates      List of query predicates
     * @return List of execution results from all scanned splits
     */
    public List<ScanResult> executeScan(
            String tableName, 
            String partitionFilter, 
            List<QueryPredicate> predicates) throws Exception {
        
        // 1. Plan and prune splits
        List<TableSplit> splits = planScan(tableName, partitionFilter, predicates);

        // 2. Execute concurrently via data plane executor
        return scanExecutor.execute(splits);
    }
}