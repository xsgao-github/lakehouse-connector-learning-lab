package xsgao.lakehouseconnector.reader;

import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.CatalogService;
import xsgao.lakehouseconnector.catalog.TableMetadata;
import xsgao.lakehouseconnector.model.RecordBatch;
import xsgao.lakehouseconnector.planner.QueryPredicate;
import xsgao.lakehouseconnector.planner.SplitPlanner;
import xsgao.lakehouseconnector.planner.TableSplit;
import xsgao.lakehouseconnector.scan.ScanExecutor;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Orchestrates metadata loading and pruning, then executes independent operator
 * pipelines concurrently through ScanExecutor.
 */
public class TableReaderService {
    private final CatalogService catalogService;
    private final SplitPlanner splitPlanner;
    private final ScanExecutor scanExecutor;

    public TableReaderService(S3Client s3Client, String bucketName) {
        this(s3Client, bucketName, 3);
    }

    public TableReaderService(S3Client s3Client, String bucketName, int maxThreads) {
        this.catalogService = new CatalogService(s3Client, bucketName);
        this.splitPlanner = new SplitPlanner();
        this.scanExecutor = new ScanExecutor(maxThreads);
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
     * Plans and executes independent pipelines, returning batches in input split order.
     * The factory is invoked separately inside each worker task.
     */
    public List<RecordBatch> executeScan(
            String tableName,
            String partitionFilter,
            List<QueryPredicate> predicates,
            Function<TableSplit, Iterator<RecordBatch>> pipelineFactory) throws Exception {
        List<TableSplit> splits = planScan(tableName, partitionFilter, predicates);
        return scanExecutor.execute(splits, pipelineFactory);
    }
}
