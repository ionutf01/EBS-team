package weather;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Subscription implements Serializable {
    private static final long serialVersionUID = 1L;
    protected final List<Condition> conditions;

    public Subscription() { this.conditions = new ArrayList<>(); }
    public void addCondition(String field, String operator, Object value) { this.conditions.add(new Condition(field, operator, value)); }

    public boolean matches(Publication p) {
        for (Condition condition : this.conditions) {
            if (!condition.isMetBy(p)) return false;
        }
        return true;
    }

    @Override
    public String toString() { return "CONDITIONS" + conditions.toString(); }

    private static class Condition implements Serializable {
        private final String field;
        private final String operator;
        private final Object value;

        Condition(String field, String operator, Object value) {
            this.field = field;
            this.operator = operator;
            this.value = value;
        }

        public String getField() { return field; }

        boolean isMetBy(Publication p) {
            try {
                switch (field.toLowerCase()) {
                    case "city": return compareStrings(p.getCity(), (String) value);
                    case "direction": return compareStrings(p.getDirection(), (String) value);
                    case "stationid": return compareNumbers(p.getStationId(), ((Number) value).doubleValue());
                    case "temp": return compareNumbers(p.getTemp(), ((Number) value).doubleValue());
                    case "wind": return compareNumbers(p.getWind(), ((Number) value).doubleValue());
                    case "rain": return compareNumbers(p.getRain(), ((Number) value).doubleValue());
                    case "date": return compareDates(p.getDate(), (LocalDate) value);
                    default: return false;
                }
            } catch (Exception e) { return false; }
        }

        private boolean compareStrings(String pubVal, String condVal) { return operator.equals("=") ? Objects.equals(pubVal, condVal) : !Objects.equals(pubVal, condVal); }
        private boolean compareDates(LocalDate pubVal, LocalDate condVal) { return operator.equals("=") ? pubVal.isEqual(condVal) : !pubVal.isEqual(condVal); }

        private boolean compareNumbers(double pubVal, double condVal) {
            switch (operator) {
                case "=": return pubVal == condVal;
                case ">": return pubVal > condVal;
                case "<": return pubVal < condVal;
                case ">=": return pubVal >= condVal;
                case "<=": return pubVal <= condVal;
                case "!=": return pubVal != condVal;
                default: return false;
            }
        }

        @Override
        public String toString() { return "(" + field + "," + operator + "," + value + ")"; }
    }
}