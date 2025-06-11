package weather;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.Map;

public class ConfigurationSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private final int publicationCount, subscriptionCount;
    private final Map<String, Integer> fieldFrequency, equalityFrequency;
    private boolean emitted = false;

    public ConfigurationSpout(int pubCount, int subCount, Map<String, Integer> fieldFreq, Map<String, Integer> eqFreq) {
        this.publicationCount = pubCount;
        this.subscriptionCount = subCount;
        this.fieldFrequency = fieldFreq;
        this.equalityFrequency = eqFreq;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) { this.collector = collector; }

    @Override
    public void nextTuple() {
        if (!emitted) {
            collector.emit(new Values(publicationCount, subscriptionCount, fieldFrequency, equalityFrequency));
            emitted = true;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publicationCount", "subscriptionCount", "fieldFrequency", "equalityFrequency"));
    }
}