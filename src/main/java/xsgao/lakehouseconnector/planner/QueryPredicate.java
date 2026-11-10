package xsgao.lakehouseconnector.planner;

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

    @Override
    public String toString() {
        return String.format("QueryPredicate{column='%s', operator=%s, expression=%s}", columnName, operator, expression);
    }
}
