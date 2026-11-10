package xsgao.lakehouseconnector.query;

import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.TableMetadata;
import xsgao.lakehouseconnector.model.RecordBatch;
import xsgao.lakehouseconnector.operator.TableScanOperator;
import xsgao.lakehouseconnector.planner.QueryPredicate;
import xsgao.lakehouseconnector.planner.TableScanPlanner;
import xsgao.lakehouseconnector.planner.TableSplit;
import xsgao.lakehouseconnector.reader.RowReader;

import java.util.List;
import java.util.function.Function;

/**
 * Caller-facing query entry point: plans splits, wraps each split's RowReader in a
 * TableScanOperator, and runs the pipelines concurrently through PipelineExecutor.
 *
 * <p>Module scope: projection pushdown arrives in Module 6 and edge aggregation in
 * Module 7; until then the caller's reader factory decides the per-split source.
 */
public class QueryExecutor {
    private final TableScanPlanner planner;
    private final PipelineExecutor pipelineExecutor;

    public QueryExecutor(S3Client s3Client, String bucketName) {
        this(s3Client, bucketName, 3);
    }

    public QueryExecutor(S3Client s3Client, String bucketName, int maxThreads) {
        this.planner = new TableScanPlanner(s3Client, bucketName);
        this.pipelineExecutor = new PipelineExecutor(maxThreads);
    }

    /**
     * Executes a scan against one table snapshot.
     *
     * @param tableName       Table to scan
     * @param partitionFilter Optional partition prefix filter (e.g., "dt=2026-11-11")
     * @param predicates      Used for min/max statistics pruning; residual row
     *                        filtering arrives with FilterOperator in Module 6
     * @param readerFactory   Creates the format-specific RowReader for a split; runs inside
     *                        the worker task because construction may perform I/O
     * @return Record batches in input split order
     */
    public List<RecordBatch> execute(
            String tableName,
            String partitionFilter,
            List<QueryPredicate> predicates,
            Function<TableSplit, RowReader> readerFactory) throws Exception {

        // 1. Control plane: one metadata read, then prune to executable splits
        TableMetadata metadata = planner.loadTableMetadata(tableName);
        List<TableSplit> splits = planner.planSplits(metadata, partitionFilter, predicates);

        // 2. Data plane: every split gets its own reader + scan operator chain
        return pipelineExecutor.execute(splits, split ->
                new TableScanOperator(readerFactory.apply(split)));
    }
}
