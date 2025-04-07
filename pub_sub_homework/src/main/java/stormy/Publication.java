package stormy;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

public class Publication {
    private final Map<String, Object> fields = new LinkedHashMap<>();
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy");

    public Publication(int stationId, String city, int temp, double rain, int wind, String direction, LocalDate date) {
        fields.put("stationid", stationId);
        fields.put("city", city);
        fields.put("temp", temp);
        fields.put("rain", rain);
        fields.put("wind", wind);
        fields.put("direction", direction);
        fields.put("date", date);
    }

    public Object getValue(String field) {
        return fields.get(field);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;

        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            if (!first) {
                sb.append(";");
            }
            first = false;

            String key = entry.getKey();
            Object value = entry.getValue();

            sb.append("(").append(key).append(",");

            if (value instanceof String) {
                sb.append("\"").append(value).append("\"");
            } else if (value instanceof LocalDate) {
                sb.append(((LocalDate) value).format(DATE_FORMATTER));
            } else {
                sb.append(value);
            }

            sb.append(")");
        }

        sb.append("}");
        return sb.toString();
    }
}