package xsgao.lakehouseconnector.planner;

import xsgao.lakehouseconnector.model.Row;

public class QueryPredicate {
    private final String columnName;
    private final Operator operator;
    private final Comparable<?> expression;

    public enum Operator { GREATER_THAN, GT_OR_EQUALS, LESS_THAN, LT_OR_EQUALS, EQUALS, NOT_EQUALS }

    public QueryPredicate(String columnName, Operator operator, Comparable<?> expression) {
        this.columnName = columnName;
        this.operator = operator;
        this.expression = expression;
    }

    public String getColumnName() { return columnName; }
    public Operator getOperator() { return operator; }
    public Comparable<?> getExpression() { return expression; }

    public boolean evaluate(Row row) {
        Object value = row.getValue(columnName);
        if (value == null) return false;

        try {
            Comparable val = (Comparable) value;
            Comparable expr = (Comparable) expression;
            int cmp = val.compareTo(expr);

            switch (operator) {
                case GREATER_THAN: return cmp > 0;
                case GT_OR_EQUALS: return cmp >= 0;
                case LESS_THAN: return cmp < 0;
                case LT_OR_EQUALS: return cmp <= 0;
                case EQUALS: return cmp == 0;
                case NOT_EQUALS: return cmp != 0;
                default: throw new UnsupportedOperationException("Unknown operator: " + operator);
            }
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public String toString() {
        return String.format("QueryPredicate{column='%s', operator=%s, expression=%s}", columnName, operator, expression);
    }
}