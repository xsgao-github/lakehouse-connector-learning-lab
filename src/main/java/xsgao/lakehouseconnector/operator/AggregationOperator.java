package xsgao.lakehouseconnector.operator;

import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.model.RecordBatch;
import xsgao.lakehouseconnector.model.Row;

import java.util.*;

/**
 * Edge aggregator operator that consumes an upstream RecordBatch stream,
 * computes local reductions grouped by groupByKeys, and materializes
 * aggregated RecordBatch results.
 *
 * <p>Learning-lab scope: only the edge (per-split, partial) phase is implemented.
 * Re-feeding these partial batches into another AggregationOperator composes for
 * SUM, MIN and MAX, but not for COUNT (it counts partial rows instead of summing
 * partial counts) and AVG is unsupported (it would need sum + count carried through).
 * A production engine uses an explicit final/merge aggregator with edge and final modes.
 *
 * <p>This operator is blocking: it drains the whole upstream on the first hasNext().
 */
public class AggregationOperator implements Iterator<RecordBatch> {

    public static enum AggFunction {
        SUM, COUNT, MIN, MAX
    }

    public static class AggExpr {
        private final String inputColumn;
        private final AggFunction function;
        private final String outputAlias;

        public AggExpr(String inputColumn, AggFunction function, String outputAlias) {
            this.inputColumn = inputColumn;
            this.function = function;
            this.outputAlias = outputAlias;
        }

        public String getInputColumn() { return inputColumn; }
        public AggFunction getFunction() { return function; }
        public String getOutputAlias() { return outputAlias; }
    }

    private final Iterator<RecordBatch> upstream;
    private final List<String> groupByKeys;
    private final List<AggExpr> aggregateExpressions;
    private List<Field> outputFields;

    private Iterator<RecordBatch> resultIterator;
    private boolean executed = false;

    public AggregationOperator(
            Iterator<RecordBatch> upstream,
            List<String> groupByKeys,
            List<AggExpr> aggregateExpressions) {
        this.upstream = upstream;
        this.groupByKeys = groupByKeys != null ? groupByKeys : Collections.emptyList();
        this.aggregateExpressions = aggregateExpressions != null ? aggregateExpressions : Collections.emptyList();
        // outputFields will be built lazily upon consuming the first upstream batch
    }

    private List<Field> buildOutputFields(List<Field> upstreamFields) {
        List<Field> fields = new ArrayList<>();
        Map<String, Field> fieldMap = new HashMap<>();
        if (upstreamFields != null) {
            for (Field f : upstreamFields) {
                fieldMap.put(f.getName(), f);
            }
        }

        int fieldId = 1;
        for (String key : groupByKeys) {
            Field orig = fieldMap.get(key);
            String type = orig != null ? orig.getType() : "STRING";
            fields.add(new Field(fieldId++, key, type, false));
        }

        for (AggExpr agg : aggregateExpressions) {
            String type = agg.getFunction() == AggFunction.COUNT ? "INTEGER" : "DOUBLE";
            fields.add(new Field(fieldId++, agg.getOutputAlias(), type, false));
        }

        return fields;
    }

    private void executeAggregation(RecordBatch firstBatch) {
        Map<List<Object>, double[]> accumulatorMap = new LinkedHashMap<>();

        while (firstBatch != null || upstream.hasNext()) {
            RecordBatch batch = firstBatch != null ? firstBatch : upstream.next();
            firstBatch = null;
            for (int i = 0; i < batch.getRowCount(); i++) {
                Row row = batch.getRow(i);

                List<Object> groupKey = new ArrayList<>(groupByKeys.size());
                for (String key : groupByKeys) {
                    groupKey.add(row.getValue(key));
                }

                double[] accs = accumulatorMap.computeIfAbsent(groupKey, k -> {
                    double[] initial = new double[aggregateExpressions.size()];
                    for (int a = 0; a < aggregateExpressions.size(); a++) {
                        AggFunction fn = aggregateExpressions.get(a).getFunction();
                        if (fn == AggFunction.MIN) initial[a] = Double.POSITIVE_INFINITY;
                        else if (fn == AggFunction.MAX) initial[a] = Double.NEGATIVE_INFINITY;
                        else initial[a] = 0.0;
                    }
                    return initial;
                });

                for (int a = 0; a < aggregateExpressions.size(); a++) {
                    AggExpr expr = aggregateExpressions.get(a);
                    Object rawVal = row.getValue(expr.getInputColumn());
                    
                    if (expr.getFunction() == AggFunction.COUNT) {
                        accs[a] += 1.0;
                    } else if (rawVal instanceof Number) {
                        double v = ((Number) rawVal).doubleValue();
                        switch (expr.getFunction()) {
                            case SUM -> accs[a] += v;
                            case MIN -> accs[a] = Math.min(accs[a], v);
                            case MAX -> accs[a] = Math.max(accs[a], v);
                            default -> {}
                        }
                    }
                }
            }
        }

        RecordBatch resultBatch = new RecordBatch(outputFields);
        for (Map.Entry<List<Object>, double[]> entry : accumulatorMap.entrySet()) {
            Map<String, Object> rowMap = new HashMap<>();
            List<Object> keys = entry.getKey();
            for (int k = 0; k < groupByKeys.size(); k++) {
                rowMap.put(groupByKeys.get(k), keys.get(k));
            }

            double[] accs = entry.getValue();
            for (int a = 0; a < aggregateExpressions.size(); a++) {
                AggExpr expr = aggregateExpressions.get(a);
                if (expr.getFunction() == AggFunction.COUNT) {
                    rowMap.put(expr.getOutputAlias(), (int) accs[a]);
                } else {
                    rowMap.put(expr.getOutputAlias(), accs[a]);
                }
            }
            resultBatch.addRow(new Row(rowMap));
        }

        List<RecordBatch> batches = resultBatch.getRowCount() > 0 ? List.of(resultBatch) : Collections.emptyList();
        this.resultIterator = batches.iterator();
        this.executed = true;
    }

    @Override
    public boolean hasNext() {
        RecordBatch firstBatch = null;
        // Lazily build output fields based on the first upstream batch if not already done
        if (outputFields == null && upstream.hasNext()) {
            firstBatch = upstream.next();
            this.outputFields = buildOutputFields(firstBatch.getFields());
        }
        // Execute aggregation if not already done
        if (!executed) {
            executeAggregation(firstBatch);
        }
        return resultIterator.hasNext();
    }

    @Override
    public RecordBatch next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more aggregated batches.");
        }
        return resultIterator.next();
    }
}