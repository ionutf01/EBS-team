package weather;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import java.util.Map;

public class SubscriberBolt extends BaseRichBolt {
    private int subscriberId;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.subscriberId = context.getThisTaskId();
    }

    @Override
    public void execute(Tuple input) {
        // Preluăm toate informațiile din tuplu
        Integer subId = input.getIntegerByField("subscriptionId");
        // LINIE NOUĂ: Preluăm obiectul Subscription
        Subscription sub = (Subscription) input.getValueByField("subscriptionObject");
        String content = input.getStringByField("matchedContent");

        System.out.println("\n=============================================");
        System.out.println("SUBSCRIBER " + subscriberId + " RECEIVED NOTIFICATION:");
        System.out.println("  > For Subscription ID: " + subId);
        // LINIE NOUĂ: Afișăm condiția subscripției
        System.out.println("  > Subscription Condition: " + sub.toString());
        System.out.println("  > Matched Content: " + content);
        System.out.println("=============================================\n");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Bolt terminal
    }
}