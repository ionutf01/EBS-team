package weather;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class WindowedSubscription extends Subscription {
    private final List<AggregateCondition> aggregateConditions;

    public WindowedSubscription() { this.aggregateConditions = new ArrayList<>(); }
    public void addAggregateCondition(AggregateCondition condition) { this.aggregateConditions.add(condition); }

    public boolean checkWindow(List<Publication> window) {
        for (AggregateCondition condition : aggregateConditions) {
            if (!evaluateSingleCondition(window, condition)) return false;
        }
        return true;
    }

    private boolean evaluateSingleCondition(List<Publication> window, AggregateCondition condition) {
        List<Double> values = window.stream()
                .map(p -> getPublicationFieldValue(p, condition.getFieldName()))
                .collect(Collectors.toList());
        double aggregateValue = 0;
        switch (condition.getAggregationType()) {
            case "avg": aggregateValue = values.stream().mapToDouble(d -> d).average().orElse(0); break;
            case "max": aggregateValue = values.stream().mapToDouble(d -> d).max().orElse(0); break;
            default: return false;
        }
        switch (condition.getOperator()) {
            case ">": return aggregateValue > condition.getValue();
            case "<": return aggregateValue < condition.getValue();
            case "=": return aggregateValue == condition.getValue();
            case ">=": return aggregateValue >= condition.getValue();
            case "<=": return aggregateValue <= condition.getValue();
            default: return false;
        }
    }

    private double getPublicationFieldValue(Publication p, String field) {
        switch (field) {
            case "temp": return p.getTemp();
            case "rain": return p.getRain();
            case "wind": return p.getWind();
            case "stationid": return p.getStationId();
            default: return 0;
        }
    }

    @Override
    public String toString() { return super.toString() + ", AGGREGATES" + aggregateConditions.toString(); }
}