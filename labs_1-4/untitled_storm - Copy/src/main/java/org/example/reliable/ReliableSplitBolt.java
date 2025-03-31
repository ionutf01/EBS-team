package org.example.reliable;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class ReliableSplitBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Random random;

    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
    }

    @Override
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[] words = sentence.split("\\s+");

        try {
            // Simulăm ocazional o eroare pentru a demonstra mecanismul de fail
            if (random.nextInt(20) == 0) { // ~5% șansă de eșec
                throw new RuntimeException("Eroare simulată în ReliableSplitBolt");
            }

            for (String word : words) {
                word = word.trim().toLowerCase();
                if (!word.isEmpty()) {
                    // Ancorăm tuplul emis la tuplul de intrare
                    collector.emit(input, new Values(word));
                    System.out.println("Split: '" + word + "' (ancorat la tuplul: " + input.getMessageId() + ")");
                }
            }

            // Confirmăm procesarea cu succes a tuplului de intrare
            collector.ack(input);
            System.out.println("SplitBolt a confirmat procesarea pentru: " + input.getMessageId());

        } catch (Exception e) {
            System.err.println("Eroare la procesarea '" + sentence + "': " + e.getMessage());
            // Notificăm eșecul procesării
            collector.fail(input);
            System.out.println("SplitBolt a raportat eșec pentru: " + input.getMessageId());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}