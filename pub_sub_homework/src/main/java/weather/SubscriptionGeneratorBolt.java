package weather;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.time.LocalDate;
import java.util.Map;
import java.util.Random;

public class SubscriptionGeneratorBolt extends BaseRichBolt {
    private OutputCollector collector;
    private static final String[] CITIES = {"Bucharest", "Cluj", "Iasi", "Timisoara", "Constanta"};
    private static final String[] DIRECTIONS = {"N", "E", "S", "W"};

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) { this.collector = collector; }

    @Override
    public void execute(Tuple input) {
        int subscriptionId = input.getIntegerByField("subscriptionId");
        Subscription subscription;

        // =================================================================================
        // ===== Forțăm crearea câtorva subscripții pe fereastră pentru testare =====
        // =================================================================================
        if (subscriptionId == 9) { // Folosim un ID specific pentru a-l găsi ușor
            System.out.println("--> SUB_GENERATOR: Creating a WINDOWED subscription for Bucharest (avg temp).");
            WindowedSubscription ws = new WindowedSubscription();
            ws.addCondition("city", "=", "Bucharest"); // Condiția de bază pentru a intra în fereastră
            ws.addAggregateCondition(new AggregateCondition("avg", "temp", ">", 10.0)); // Condiția de agregare
            subscription = ws;
        } else if (subscriptionId == 10) { // Încă una pentru diversitate
            System.out.println("--> SUB_GENERATOR: Creating a WINDOWED subscription for Iasi (max wind).");
            WindowedSubscription ws = new WindowedSubscription();
            ws.addCondition("city", "=", "Iasi"); // Condiția de bază
            ws.addAggregateCondition(new AggregateCondition("max", "wind", ">", 25.0)); // Condiția de agregare
            subscription = ws;
        }
        else {
            // Pentru toate celelalte ID-uri, folosim logica originală pentru subscripții simple
            SubscriptionTemplate template = (SubscriptionTemplate) input.getValueByField("template");
            subscription = generateSimpleSubscriptionFromTemplate(template);
        }
        // =================================================================================

        collector.emit(new Values(subscriptionId, subscription));
        collector.ack(input);
    }

    private Subscription generateSimpleSubscriptionFromTemplate(SubscriptionTemplate template) {
        Random rand = new Random();
        Subscription subscription = new Subscription();
        for (SubscriptionTemplate.FieldOperator fo : template.getFieldOperators()) {
            String field = fo.getField();
            String operator = fo.getOperator();
            Object value = null;
            switch (field) {
                case "stationid": value = rand.nextInt(100) + 1; break;
                case "city": value = CITIES[rand.nextInt(CITIES.length)]; break;
                case "temp": value = rand.nextInt(41) - 10; break;
                case "rain": value = Math.round(rand.nextDouble() * 50 * 10) / 10.0; break;
                case "wind": value = rand.nextInt(101); break;
                case "direction": value = DIRECTIONS[rand.nextInt(DIRECTIONS.length)]; break;
                case "date": value = LocalDate.ofEpochDay(LocalDate.of(2023, 1, 1).toEpochDay() + rand.nextInt(365*2)); break;
            }
            if (value != null) { subscription.addCondition(field, operator, value); }
        }
        return subscription;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields("subscriptionId", "subscription")); }
}