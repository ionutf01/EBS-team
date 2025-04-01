package stormy;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.Map;

public class ConfigurationSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private final int publicationCount;
    private final int subscriptionCount;
    private final Map<String, Integer> fieldFrequency;
    private final Map<String, Integer> equalityFrequency;
    private boolean emitted = false;

    public ConfigurationSpout(int publicationCount, int subscriptionCount,
                              Map<String, Integer> fieldFrequency,
                              Map<String, Integer> equalityFrequency) {
        this.publicationCount = publicationCount;
        this.subscriptionCount = subscriptionCount;
        this.fieldFrequency = fieldFrequency;
        this.equalityFrequency = equalityFrequency;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (!emitted) {
            collector.emit(new Values(publicationCount, subscriptionCount,
                    fieldFrequency, equalityFrequency));
            emitted = true;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publicationCount", "subscriptionCount",
                "fieldFrequency", "equalityFrequency"));
    }
}