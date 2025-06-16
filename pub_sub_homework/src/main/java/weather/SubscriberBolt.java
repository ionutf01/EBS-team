package weather;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import java.util.Map;

public class SubscriberBolt extends BaseRichBolt {
    private int subscriberId;
    private static int receivedPublications = 0;
    private static long totalLatency = 0;
    private static int latencyCount = 0;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.subscriberId = context.getThisTaskId();
    }

    @Override
    public void execute(Tuple input) {
        System.out.println("Receiving publication at: " + System.currentTimeMillis());

        Integer subId = input.getIntegerByField("subscriptionId");
        Subscription sub = (Subscription) input.getValueByField("subscriptionObject");
        String content = input.getStringByField("matchedContent");

        System.out.println("\n=============================================");
        System.out.println("SUBSCRIBER " + subscriberId + " RECEIVED NOTIFICATION:");
        System.out.println("  > For Subscription ID: " + subId);
        System.out.println("  > Subscription Condition: " + sub.toString());
        System.out.println("  > Matched Content: " + content);
        System.out.println("=============================================\n");

        receivedPublications++;
        // Check if it's a real publication (not a window meta-publication)
        if (!content.contains("emitTimeMillis=")) {
           System.out.println("Skipping meta-publication, no emitTimeMillis: " + content);
            return;  // skip this latency measurement
        }

        long emitTimeMillis = extractEmitTimeMillis(content);
        if (emitTimeMillis == 0) {
            System.err.println("emitTimeMillis could not be extracted: " + content);
            return;
        }

        long latency = System.currentTimeMillis() - emitTimeMillis;

        totalLatency += latency;
        latencyCount++;
    }

    private long extractEmitTimeMillis(String content) {
        try {
            int start = content.indexOf("emitTimeMillis=");
            if (start == -1) return 0;
            start += "emitTimeMillis=".length();

            // Extract until end or next comma/brace
            int end = content.indexOf('}', start);
            if (end == -1) end = content.length();

            String value = content.substring(start, end).replaceAll("[^0-9]", "");
            return Long.parseLong(value);
        } catch (Exception e) {
            System.err.println("Failed to extract emitTimeMillis from: " + content);
            return 0;
        }
    }


    public static int getReceivedCount() {
        return receivedPublications;
    }

    public static double getAverageLatency() {
        return latencyCount == 0 ? 0 : (double) totalLatency / latencyCount;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Bolt terminal
    }
}