package weather;

import java.io.Serializable;

public class AggregateCondition implements Serializable {
    private final String aggregationType, fieldName, operator;
    private final double value;

    public AggregateCondition(String aggregationType, String fieldName, String operator, double value) {
        this.aggregationType = aggregationType;
        this.fieldName = fieldName;
        this.operator = operator;
        this.value = value;
    }
    public String getAggregationType() { return aggregationType; }
    public String getFieldName() { return fieldName; }
    public String getOperator() { return operator; }
    public double getValue() { return value; }

    @Override
    public String toString() { return "(" + aggregationType + "_" + fieldName + ", " + operator + ", " + value + ")"; }
}