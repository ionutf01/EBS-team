package stormy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * SubscriptionTemplate represents the structure of a subscription filter
 * before concrete values are assigned.
 *
 * This class serves as an intermediate representation that defines which fields
 * will be included in a subscription and what operators will be used for filtering.
 * It works as a blueprint that the SubscriptionGeneratorBolt will transform into
 * a complete subscription with actual values.
 */
public class SubscriptionTemplate implements Serializable {
    private final List<FieldOperator> fieldOperators = new ArrayList<>();

    public void addField(String field, String operator) {
        fieldOperators.add(new FieldOperator(field, operator));
    }

    public List<FieldOperator> getFieldOperators() {
        return fieldOperators;
    }

    public static class FieldOperator implements Serializable {
        private final String field;
        private final String operator;

        public FieldOperator(String field, String operator) {
            this.field = field;
            this.operator = operator;
        }

        public String getField() {
            return field;
        }

        public String getOperator() {
            return operator;
        }
    }
}