package weather;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SubscriptionTemplate implements Serializable {
    private final List<FieldOperator> fieldOperators = new ArrayList<>();

    public void addField(String field, String operator) { fieldOperators.add(new FieldOperator(field, operator)); }
    public List<FieldOperator> getFieldOperators() { return fieldOperators; }

    public static class FieldOperator implements Serializable {
        private final String field;
        private final String operator;
        public FieldOperator(String field, String operator) { this.field = field; this.operator = operator; }
        public String getField() { return field; }
        public String getOperator() { return operator; }
    }
}