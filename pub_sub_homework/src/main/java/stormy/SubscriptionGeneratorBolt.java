package stormy;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.example.Subscription;

import java.time.LocalDate;
import java.util.Map;
import java.util.Random;

public class SubscriptionGeneratorBolt extends BaseRichBolt {
    private OutputCollector collector;
    private static final String[] CITIES = {"Bucharest", "Cluj", "Iasi", "Timisoara", "Constanta", "Brasov", "Craiova"};
    private static final String[] DIRECTIONS = {"N", "NE", "E", "SE", "S", "SW", "W", "NW"};

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        int subscriptionId = input.getIntegerByField("subscriptionId");
        SubscriptionTemplate template = (SubscriptionTemplate) input.getValueByField("template");

        // Generate complete subscription from template
        Subscription subscription = generateSubscriptionFromTemplate(template);

        // Emit the subscription
        collector.emit(new Values(subscriptionId, subscription));
        collector.ack(input);
    }

    private Subscription generateSubscriptionFromTemplate(SubscriptionTemplate template) {
        Random rand = new Random();
        Subscription subscription = new Subscription();

        // Add conditions based on the template
        for (SubscriptionTemplate.FieldOperator fo : template.getFieldOperators()) {
            String field = fo.getField();
            String operator = fo.getOperator();

            // Generate value based on field type
            Object value;
            switch (field) {
                case "stationid":
                    value = rand.nextInt(100) + 1;
                    break;
                case "city":
                    value = CITIES[rand.nextInt(CITIES.length)];
                    break;
                case "temp":
                    value = rand.nextInt(41) - 10;
                    break;
                case "rain":
                    value = Math.round(rand.nextDouble() * 50 * 10) / 10.0;
                    break;
                case "wind":
                    value = rand.nextInt(101);
                    break;
                case "direction":
                    value = DIRECTIONS[rand.nextInt(DIRECTIONS.length)];
                    break;
                case "date":
                    LocalDate startDate = LocalDate.of(2023, 1, 1);
                    long days = startDate.toEpochDay();
                    long endDays = LocalDate.of(2025, 12, 31).toEpochDay();
                    long randomDay = rand.nextInt((int) (endDays - days)) + days;
                    value = LocalDate.ofEpochDay(randomDay);
                    break;
                default:
                    value = null;
            }

            if (value != null) {
                subscription.addCondition(field, operator, value);
            }
        }

        return subscription;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("subscriptionId", "subscription"));
    }
}