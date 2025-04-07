package stormy;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.Map;

public class ConfigurationSpout extends BaseRichSpout {
    //  SpoutOutputCollector is used to emit tuples
    private SpoutOutputCollector collector;
    private final int publicationCount;
    private final int subscriptionCount;
    // Maps field names to their frequency percentage in generated subscriptions
    private final Map<String, Integer> fieldFrequency;
    // Controls how often equality operators (vs range operators) are used for fields
    private final Map<String, Integer> equalityFrequency;
    private boolean emitted = false;

    /**
     * Constructor to initialize the spout with configuration parameters.
     *
     * @param publicationCount Total number of publications to generate
     * @param subscriptionCount Total number of subscriptions to generate
     * @param fieldFrequency Map controlling how often each field appears in subscriptions
     * @param equalityFrequency Map controlling how often equality operators are used
     */
    public ConfigurationSpout(int publicationCount, int subscriptionCount,
                              Map<String, Integer> fieldFrequency,
                              Map<String, Integer> equalityFrequency) {
        // Constructor to initialize the spout with configuration parameters
        this.publicationCount = publicationCount;
        this.subscriptionCount = subscriptionCount;
        this.fieldFrequency = fieldFrequency;
        this.equalityFrequency = equalityFrequency;
    }


    /**
     * Called when the spout is initialized.
     * Storm provides the output collector for emitting tuples.
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // Open method is called when the spout is initialized
        this.collector = collector;
    }


    /**
     * Called repeatedly by Storm to emit tuples.
     * This implementation emits configuration parameters exactly once.
     */
    @Override
    public void nextTuple() {
        // nextTuple is called repeatedly to emit tuples
        if (!emitted) {
            collector.emit(new Values(publicationCount, subscriptionCount,
                    fieldFrequency, equalityFrequency));
            emitted = true;
        }
    }

    /**
     * Declares the output fields (schema) for tuples emitted by this spout.
     * Downstream bolts use these field names to access tuple values.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publicationCount", "subscriptionCount",
                "fieldFrequency", "equalityFrequency"));
    }
}