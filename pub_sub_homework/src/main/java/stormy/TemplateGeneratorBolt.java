package stormy;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.*;

public class TemplateGeneratorBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            int publicationCount = input.getIntegerByField("publicationCount");
            int subscriptionCount = input.getIntegerByField("subscriptionCount");
            Map<String, Integer> fieldFrequency =
                    (Map<String, Integer>) input.getValueByField("fieldFrequency");
            Map<String, Integer> equalityFrequency =
                    (Map<String, Integer>) input.getValueByField("equalityFrequency");

            // Calculate exact counts for each field
            Map<String, Integer> fieldCounts = new HashMap<>();
            for (Map.Entry<String, Integer> entry : fieldFrequency.entrySet()) {
                fieldCounts.put(entry.getKey(), Math.round((entry.getValue() * subscriptionCount) / 100f));
            }

            // Calculate exact counts for equality operators
            Map<String, Integer> equalityCounts = new HashMap<>();
            for (Map.Entry<String, Integer> entry : equalityFrequency.entrySet()) {
                String field = entry.getKey();
                if (fieldCounts.containsKey(field)) {
                    int fieldCount = fieldCounts.get(field);
                    equalityCounts.put(field, Math.round((entry.getValue() * fieldCount) / 100f));
                }
            }

            // Generate subscription templates with exact field and operator distributions
            List<SubscriptionTemplate> templates = generateSubscriptionTemplates(
                    subscriptionCount, fieldCounts, equalityCounts);

            // Emit publication generation tasks
            for (int i = 0; i < publicationCount; i++) {
                collector.emit("publication-stream", new Values(i));
            }

            // Emit subscription template tasks
            for (int i = 0; i < templates.size(); i++) {
                collector.emit("subscription-stream", new Values(i, templates.get(i)));
            }

            collector.ack(input);
        } catch (Exception e) {
            System.err.println("Error in TemplateGeneratorBolt: " + e.getMessage());
            e.printStackTrace();
            collector.fail(input);
        }
    }

    private List<SubscriptionTemplate> generateSubscriptionTemplates(
            int count, Map<String, Integer> fieldCounts, Map<String, Integer> equalityCounts) {

        // Create subscription templates that only specify which fields and operators to use
        List<SubscriptionTemplate> templates = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            templates.add(new SubscriptionTemplate());
        }

        Random rand = new Random(42); // Fixed seed for reproducibility

        // Assign fields to subscriptions to match exact percentages
        for (Map.Entry<String, Integer> entry : fieldCounts.entrySet()) {
            String field = entry.getKey();
            int fieldCount = entry.getValue();

            if (fieldCount <= 0 || fieldCount > count) {
                System.err.println("Warning: Invalid field count for " + field + ": " + fieldCount);
                continue;
            }

            // Get indices of subscriptions that will contain this field
            List<Integer> indices = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                indices.add(i);
            }
            Collections.shuffle(indices, rand);
            indices = indices.subList(0, fieldCount);

            // Calculate how many of these fields should use equality operator
            int equalityCount = equalityCounts.getOrDefault(field, 0);

            if (equalityCount > fieldCount) {
                System.err.println("Warning: Equality count exceeds field count for " + field);
                equalityCount = fieldCount;
            }

            // Assign field and operator to selected subscription templates
            for (int i = 0; i < fieldCount; i++) {
                int index = indices.get(i);
                String operator = (i < equalityCount) ? "=" : getRandomNonEqualityOperator(rand);
                templates.get(index).addField(field, operator);
            }
        }

        return templates;
    }

    private String getRandomNonEqualityOperator(Random rand) {
        String[] operators = {">", "<", ">=", "<=", "!="};
        return operators[rand.nextInt(operators.length)];
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("publication-stream",
                new Fields("publicationId"));
        declarer.declareStream("subscription-stream",
                new Fields("subscriptionId", "template"));
    }
}