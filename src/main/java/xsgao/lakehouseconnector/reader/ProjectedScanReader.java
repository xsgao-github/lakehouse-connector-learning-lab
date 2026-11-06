package xsgao.lakehouseconnector.reader;

import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.planner.QueryPredicate;
import xsgao.lakehouseconnector.planner.TableSplit;
import xsgao.lakehouseconnector.model.RecordBatch; 
import xsgao.lakehouseconnector.model.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ProjectedScanReader implements Iterator<RecordBatch> {
    private final Iterator<RecordBatch> underlyingIterator;
    private final QueryPredicate predicate;
    private final List<Field> fields;
    private RecordBatch nextFilteredBatch = null;

    public ProjectedScanReader(
        TableSplit split,
        List<String> columnNames,
        QueryPredicate predicate,
        Schema tableSchema,
        TableReaderService tableReaderService
    ) {
        this.predicate = predicate;
        
        // Validate or project schema against the catalog schema if needed
        fields = projectColumns(tableSchema, columnNames);
        
        // Delegate low-level scan and columnar projection to TableReaderService
        underlyingIterator = tableReaderService.readSplit(split, tableSchema, columnNames);
        
        // Prime the iterator
        advance();
    }

    private List<Field> projectColumns(Schema tableSchema, List<String> columnNames) {
        List<Field> projectedFields = new ArrayList<>();
        // Validate that requested columns exist in the schema
        for (Field f : tableSchema.getFields()) {
            if (columnNames.contains(f.getName())) {
                projectedFields.add(f);
            }
        }
        return projectedFields;
    }

    private void advance() {
        nextFilteredBatch = null;
        while (underlyingIterator.hasNext()) {
            RecordBatch rawBatch = underlyingIterator.next();
            RecordBatch filteredBatch = filterBatchRowByRow(rawBatch);
            
            if (filteredBatch.getRowCount() > 0) {
                nextFilteredBatch = filteredBatch;
                break;
            }
        }
    }

    private RecordBatch filterBatchRowByRow(RecordBatch batch) {
        RecordBatch filteredBatch = new RecordBatch(fields);
        
        for (int i = 0; i < batch.getRowCount(); i++) {
            Row row = batch.getRow(i);
            // Evaluate QueryPredicate row-by-row at the edge
            if (predicate == null || predicate.evaluate(row)) {
                filteredBatch.addRow(row);
            }
        }
        return filteredBatch;
    }

    @Override
    public boolean hasNext() {
        return nextFilteredBatch != null;
    }

    @Override
    public RecordBatch next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more matching records in split.");
        }
        RecordBatch current = nextFilteredBatch;
        advance();
        return current;
    }
}