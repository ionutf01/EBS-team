package weather;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SubscriptionSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private final AtomicBoolean emitted = new AtomicBoolean(false);

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (!emitted.get()) {
            List<Subscription> subscriptions = createSubscriptions();
            for (int i = 0; i < subscriptions.size(); i++) {
                Subscription sub = subscriptions.get(i);
                collector.emit(new Values(i, sub));
                System.out.println("SUBSCRIPTION_SPOUT: Emitting subscription " + i + ": " + sub);
            }
            emitted.set(true);
        }
    }

    private List<Subscription> createSubscriptions() {
        List<Subscription> subscriptions = new ArrayList<>();

        Subscription s1 = new Subscription();
        s1.addCondition("city", "=", "Iasi");
        s1.addCondition("temp", ">=", 20);
        subscriptions.add(s1);

        WindowedSubscription ws1 = new WindowedSubscription();
        ws1.addCondition("city", "=", "Bucharest");
        ws1.addAggregateCondition(new AggregateCondition("avg", "temp", ">", 15));
        ws1.addAggregateCondition(new AggregateCondition("avg", "wind", "<=", 40));
        subscriptions.add(ws1);

        return subscriptions;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("subscriptionId", "subscription"));
    }
}