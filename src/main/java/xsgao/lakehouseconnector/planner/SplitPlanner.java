package xsgao.lakehouseconnector.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import xsgao.lakehouseconnector.catalog.ColumnStats;
import xsgao.lakehouseconnector.catalog.DataFile;
import xsgao.lakehouseconnector.catalog.Snapshot;
import xsgao.lakehouseconnector.catalog.TableMetadata;

public class SplitPlanner {

    /**
     * Plans execution splits by evaluating partition filters and column statistics
     * against the latest table snapshot.
     * 
     * @param metadata        The loaded TableMetadata (e.g., from v3.json)
     * @param partitionFilter Optional partition prefix filter (e.g., "dt=2026-11-04")
     * @param predicates      List of query filter predicates for statistics pruning
     * @return List of executable TableSplits for the query engine
     */
    public List<TableSplit> planSplits(
            TableMetadata metadata, 
            String partitionFilter, 
            List<QueryPredicate> predicates) {
        
        List<TableSplit> splits = new ArrayList<>();

        // 1. Locate the active snapshot using current-snapshot-id
        Long currentSnapshotId = metadata.getCurrentSnapshotId();
        if (metadata.getCurrentSnapshotId() == null || metadata.getSnapshots() == null) {
            return Collections.emptyList(); // Empty table, return zero splits
        }
        Snapshot activeSnapshot = metadata.getSnapshots().stream()
                .filter(s -> s.getSnapshotId() == currentSnapshotId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Active snapshot ID " + currentSnapshotId + " not found."));

        long currentSchemaId = metadata.getSchema().getSchemaId();

        // 2. Iterate through all active files in the snapshot
        for (DataFile file : activeSnapshot.getAddedFiles()) {
            
            // --- PARTITION PRUNING ---
            // If a partition filter is specified, check if the file path contains the partition key.
            if (partitionFilter != null && !partitionFilter.isEmpty()) {
                if (!file.getFilePath().contains(partitionFilter)) {
                    // Pruned at partition level! Skip S3 GET cost entirely.
                    continue;
                }
            }

            // --- STATISTICS PRUNING ---
            boolean fileCanBePruned = false;
            if (predicates != null && !predicates.isEmpty()) {
                Map<String, ColumnStats> statsMap = file.getColumnStats();
                
                for (QueryPredicate pred : predicates) {
                    if (statsMap != null && statsMap.containsKey(pred.getColumnName())) {
                        ColumnStats stats = statsMap.get(pred.getColumnName());
                        
                        // If the file is NOT valid by stats, it can be pruned
                        if (!isValidByStats(pred, stats)) {
                            fileCanBePruned = true;
                            break;
                        }
                    }
                }
            }

            if (fileCanBePruned) {
                continue; // Skip this file—zero data plane interaction needed.
            }

            // --- SPLIT GENERATION ---
            splits.add(new TableSplit(
                file.getFilePath(),
                file.getRecordCount(),
                file.getFileSizeInBytes(),
                currentSchemaId
            ));
        }

        return splits;
    }

/**
     * Evaluates if a predicate satisfies the file's min/max column bounds.
     * Returns true if the file CANNOT be pruned (i.e. it potentially contains matching rows).
     */
    private boolean isValidByStats(QueryPredicate pred, ColumnStats stats) {
        String lowerStr = stats.getLowerBound();
        String upperStr = stats.getUpperBound();
        
        if (lowerStr == null || upperStr == null) {
            return true; // Conservative: cannot prune if stats are missing/null, so keep file valid
        }

        try {
            double fileMin = Double.parseDouble(lowerStr);
            double fileMax = Double.parseDouble(upperStr);
            double threshold = Double.parseDouble(pred.getExpression().toString());

            switch (pred.getOperator()) {
                case GREATER_THAN:
                    // Valid if fileMax > threshold (at least some values exceed threshold)
                    return fileMax > threshold;
                case GT_OR_EQUALS:
                    // Valid if fileMax >= threshold (at least some values meet/exceed threshold)
                    return fileMax >= threshold;
                case LESS_THAN:
                    // Valid if fileMin < threshold (at least some values are below threshold)
                    return fileMin < threshold;
                case LT_OR_EQUALS:
                    // Valid if fileMin <= threshold (at least some values meet/below threshold)
                    return fileMin <= threshold;
                case EQUALS:
                    // Valid if threshold falls within [fileMin, fileMax]
                    return threshold >= fileMin && threshold <= fileMax;
                case NOT_EQUALS:
                    // Valid if threshold is outside [fileMin, fileMax]
                    return threshold < fileMin || threshold > fileMax;
                default:
                    return true;
            }
        } catch (NumberFormatException e) {
            int cmpMin = lowerStr.compareTo(pred.getExpression().toString());
            int cmpMax = upperStr.compareTo(pred.getExpression().toString());

            switch (pred.getOperator()) {
                case GREATER_THAN:
                    return cmpMax > 0; // upper bound > threshold
                case GT_OR_EQUALS:
                    return cmpMax >= 0; // upper bound >= threshold
                case LESS_THAN:
                    return cmpMin < 0; // lower bound < threshold
                case LT_OR_EQUALS:
                    return cmpMin <= 0; // lower bound <= threshold
                case EQUALS:
                    return cmpMin <= 0 && cmpMax >= 0; // threshold inside [lower, upper]
                case NOT_EQUALS:
                    return cmpMin < 0 || cmpMax > 0; // threshold outside [lower, upper]    
                default:
                    return true;
            }
        }
    }
}