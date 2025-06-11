package weather;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TemplateGeneratorBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Random rand;
    private static final List<String> FIELDS_POOL = List.of("city", "temp", "wind", "rain", "stationid", "direction", "date");
    private static final List<String> OPERATORS_POOL = List.of("=", ">", "<", ">=", "<=", "!=");

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.rand = new Random();
    }

    @Override
    public void execute(Tuple input) {
        int publicationCount = input.getIntegerByField("publicationCount");
        int subscriptionCount = input.getIntegerByField("subscriptionCount");

        System.out.println("TEMPLATE_GENERATOR: Emitting " + publicationCount + " publication tasks...");
        for (int i = 0; i < publicationCount; i++) {
            collector.emit("publication-stream", new Values(i));
        }

        System.out.println("TEMPLATE_GENERATOR: Emitting " + subscriptionCount + " subscription templates...");
        for (int i = 0; i < subscriptionCount; i++) {
            SubscriptionTemplate template = createRandomTemplate();
            collector.emit("subscription-stream", new Values(i, template));
        }
        collector.ack(input);
    }

    private SubscriptionTemplate createRandomTemplate() {
        SubscriptionTemplate template = new SubscriptionTemplate();
        int conditionCount = rand.nextInt(3) + 1;
        for (int i = 0; i < conditionCount; i++) {
            String randomField = FIELDS_POOL.get(rand.nextInt(FIELDS_POOL.size()));
            String randomOperator = getRandomOperatorForField(randomField);
            template.addField(randomField, randomOperator);
        }
        return template;
    }

    private String getRandomOperatorForField(String field) {
        if (field.equals("city") || field.equals("direction") || field.equals("date")) {
            return "=";
        }
        return OPERATORS_POOL.get(rand.nextInt(OPERATORS_POOL.size()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("publication-stream", new Fields("publicationId"));
        declarer.declareStream("subscription-stream", new Fields("subscriptionId", "template"));
    }
}