package xsgao.lakehouseconnector.planner;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import xsgao.lakehouseconnector.catalog.ColumnStats;
import xsgao.lakehouseconnector.catalog.DataFile;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.catalog.Snapshot;
import xsgao.lakehouseconnector.catalog.TableMetadata;
import xsgao.lakehouseconnector.planner.QueryPredicate.Operator;

class SplitPlannerTest {

    private SplitPlanner splitPlanner;
    private TableMetadata metadata;

    @BeforeEach
    void setUp() {
        splitPlanner = new SplitPlanner();

        // Construct mock schema and snapshot
        Schema schema = new Schema(1, List.of(
            new Field(1, "order_id", "long", true),
            new Field(2, "amount", "double", false),
            new Field(3, "dt", "string", true)
        ));

        DataFile file1 = new DataFile("data/dt=2026-11-03/part-001.parquet", 100, 1024L, 
            Map.of("amount", new ColumnStats("10.0", "100.0", 0)));
        DataFile file2 = new DataFile("data/dt=2026-11-04/part-002.parquet", 200, 2048L, 
            Map.of("amount", new ColumnStats("200.0", "500.0", 0)));

        Snapshot snapshot = new Snapshot(1L, System.currentTimeMillis(), (long)1, List.of(file1, file2));
        metadata = new TableMetadata(1, "test_table", "test_location", schema, 1L, List.of(snapshot));
    }

    @Test
    void testPartitionPruning() {
        List<TableSplit> splits = splitPlanner.planSplits(metadata, "dt=2026-11-04", List.of());
        assertEquals(1, splits.size());
        assertEquals("data/dt=2026-11-04/part-002.parquet", splits.get(0).getFilePath());
    }

    @Test
    void testStatisticsPruning() {
        QueryPredicate predicate = new QueryPredicate("amount", Operator.GREATER_THAN, 300.0);
        List<TableSplit> splits = splitPlanner.planSplits(metadata, null, List.of(predicate));
        assertEquals(1, splits.size());
        assertEquals("data/dt=2026-11-04/part-002.parquet", splits.get(0).getFilePath());
    }
}