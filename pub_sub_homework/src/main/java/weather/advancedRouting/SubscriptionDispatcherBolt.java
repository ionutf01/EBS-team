package weather.advancedRouting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import weather.Subscription;
import weather.Subscription.Condition;

import java.util.List;
import java.util.Map;

public class SubscriptionDispatcherBolt extends BaseRichBolt {
    private static final Logger LOG = LogManager.getLogger(SubscriptionDispatcherBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Subscription subscription = (Subscription) input.getValueByField("subscription");
        LOG.info("Dispatcher: Am primit abonamentul: {}", subscription);

        List<Condition> conditions = subscription.conditions;

        if (conditions != null) {
            for (Condition condition : conditions) {
                String field = condition.getField();
                String streamId = field + "-subscriptions";

                LOG.info("Dispatcher: Trimit condiția [{}] pe stream-ul [{}]", condition, streamId);

                collector.emit(streamId, new Values(condition));
            }
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("city-subscriptions", new Fields("condition"));
        declarer.declareStream("temp-subscriptions", new Fields("condition"));
        declarer.declareStream("wind-subscriptions", new Fields("condition"));
        declarer.declareStream("rain-subscriptions", new Fields("condition"));
    }
}