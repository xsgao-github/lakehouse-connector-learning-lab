package xsgao.lakehouseconnector.reader;

import java.util.List;

import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.CatalogService;
import xsgao.lakehouseconnector.catalog.TableMetadata;
import xsgao.lakehouseconnector.planner.QueryPredicate;
import xsgao.lakehouseconnector.planner.SplitPlanner;
import xsgao.lakehouseconnector.planner.TableSplit;

/**
 * Orchestrates the read path: loads table metadata from the catalog control plane
 * and executes partition and column-statistics pruning via SplitPlanner.
 */
public class TableReaderService {
    private final CatalogService catalogService;
    private final SplitPlanner splitPlanner;

    public TableReaderService(S3Client s3Client, String bucketName) {
        this.catalogService = new CatalogService(s3Client, bucketName);
        this.splitPlanner = new SplitPlanner();
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
2     * a specific historical metadata version (e.g., v1.json or v2.json).
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
}