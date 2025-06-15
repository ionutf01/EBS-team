package weather;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import weather.Subscription.Condition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FilteringBolt extends BaseRichBolt {
    private static final Logger LOG = LogManager.getLogger(FilteringBolt.class);
    private OutputCollector collector;
    private final String responsibleField;
    private List<Condition> conditions;

    public FilteringBolt(String responsibleField) {
        this.responsibleField = responsibleField.toUpperCase();
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.conditions = new ArrayList<>();
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().endsWith("-subscriptions")) {
            Condition condition = (Condition) input.getValueByField("condition");
            if (condition.getField().equalsIgnoreCase(this.responsibleField)) {
                LOG.info("[Filtru {}] Am înregistrat o nouă regulă: {}", this.responsibleField, condition);
                this.conditions.add(condition);
            }
        } else {
            Publication publication = (Publication) input.getValueByField("publication");
            LOG.debug("[Filtru {}] Verific publicația: {}", this.responsibleField, publication);

            boolean passed;
            String reason;

            if (conditions.isEmpty()) {
                passed = true;
                reason = "Nicio regulă înregistrată (mod passthrough)";
            } else if (checkConditions(publication)) {
                passed = true;
                reason = "Potrivire găsită";
            } else {
                passed = false;
                reason = "Nicio potrivire găsită";
            }

            if (passed) {
                LOG.info("[Filtru {}] PASSED -> Publicația {} a trecut. Motiv: {}", this.responsibleField, publication.getCity(), reason);
                collector.emit(new Values(publication));
            } else {
                LOG.warn("[Filtru {}] DROPPED -> Publicația {} a fost blocată. Motiv: {}", this.responsibleField, publication.getCity(), reason);
            }
        }
        collector.ack(input);
    }

    private boolean checkConditions(Publication publication) {
        for (Condition condition : this.conditions) {
            Comparable<?> pubValue = getPublicationValueByField(publication, this.responsibleField);
            if (pubValue == null) continue;
            Object condValue = condition.getValue();
            @SuppressWarnings("unchecked")
            int comparisonResult = ((Comparable<Object>) pubValue).compareTo(condValue);
            boolean matched = false;
            switch (condition.getOperator()) {
                case "=":  matched = comparisonResult == 0; break;
                case "!=": matched = comparisonResult != 0; break;
                case ">":  matched = comparisonResult > 0;  break;
                case "<":  matched = comparisonResult < 0;  break;
                case ">=": matched = comparisonResult >= 0; break;
                case "<=": matched = comparisonResult <= 0; break;
            }
            if (matched) return true;
        }
        return false;
    }

    private Comparable<?> getPublicationValueByField(Publication pub, String field) {
        switch (field.toLowerCase()) {
            case "city": return pub.getCity();
            case "temp": return pub.getTemp();
            case "wind": return pub.getWind();
            case "rain": return pub.getRain();
            default: return null;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publication"));
    }
}