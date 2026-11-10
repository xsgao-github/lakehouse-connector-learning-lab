package xsgao.lakehouseconnector.operator;

import xsgao.lakehouseconnector.model.RecordBatch;
import xsgao.lakehouseconnector.model.Row;
import xsgao.lakehouseconnector.planner.QueryPredicate;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class FilterOperator implements Iterator<RecordBatch> {
    private final Iterator<RecordBatch> upstream;
    private final QueryPredicate predicate;
    private RecordBatch nextFilteredBatch = null;

    // Generic constructor: accepts ANY upstream Iterator<RecordBatch>
    public FilterOperator(Iterator<RecordBatch> upstream, QueryPredicate predicate) {
        this.upstream = upstream;
        this.predicate = predicate;
        advance();
    }

    private void advance() {
        nextFilteredBatch = null;
        while (upstream.hasNext()) {
            RecordBatch rawBatch = upstream.next();
            RecordBatch filteredBatch = filterBatch(rawBatch);

            if (filteredBatch.getRowCount() > 0) {
                nextFilteredBatch = filteredBatch;
                break;
            }
        }
    }

    private RecordBatch filterBatch(RecordBatch batch) {
        RecordBatch filtered = new RecordBatch(batch.getFields());
        for (int i = 0; i < batch.getRowCount(); i++) {
            Row row = batch.getRow(i);
            if (predicate == null || predicate.evaluate(row)) {
                filtered.addRow(row);
            }
        }
        return filtered;
    }

    @Override
    public boolean hasNext() {
        return nextFilteredBatch != null;
    }

    @Override
    public RecordBatch next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more matching records in filter operator.");
        }
        RecordBatch current = nextFilteredBatch;
        advance();
        return current;
    }
}