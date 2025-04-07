package stormy;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class Subscription {
    private final List<Condition> conditions = new ArrayList<>();
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy");

    public void addCondition(String field, String operator, Object value) {
        conditions.add(new Condition(field, operator, value));
    }

    public boolean hasField(String field) {
        return conditions.stream().anyMatch(c -> c.field.equals(field));
    }

    public boolean hasFieldWithOperator(String field, String operator) {
        return conditions.stream().anyMatch(c -> c.field.equals(field) && c.operator.equals(operator));
    }

    @Override
    public String toString() {
        if (conditions.isEmpty()) {
            return "{}";
        }

        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < conditions.size(); i++) {
            if (i > 0) {
                sb.append(";");
            }
            sb.append(conditions.get(i));
        }
        sb.append("}");
        return sb.toString();
    }

    private static class Condition {
        final String field;
        final String operator;
        final Object value;

        Condition(String field, String operator, Object value) {
            this.field = field;
            this.operator = operator;
            this.value = value;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("(");
            sb.append(field).append(",").append(operator).append(",");

            if (value instanceof String) {
                sb.append("\"").append(value).append("\"");
            } else if (value instanceof LocalDate) {
                sb.append(((LocalDate) value).format(DATE_FORMATTER));
            } else {
                sb.append(value);
            }

            sb.append(")");
            return sb.toString();
        }
    }
}