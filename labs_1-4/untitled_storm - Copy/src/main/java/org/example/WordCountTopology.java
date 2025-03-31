package org.example;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
public class WordCountTopology {

    public static void main(String[] args) throws Exception {
        // Create the topology builder
        TopologyBuilder builder = new TopologyBuilder();

        // Set the spout
        builder.setSpout("source-spout", new SourceTextSpout(), 1);

        // Set the bolts
        builder.setBolt("split-bolt", new SplitTextBolt(), 2)
                .shuffleGrouping("source-spout");

        builder.setBolt("count-bolt", new WordCountBolt(), 2)
                .fieldsGrouping("split-bolt", new Fields("word"));

        builder.setBolt("terminal-bolt", new TerminalBolt(), 1)
                .globalGrouping("count-bolt");

        // Configure the topology
        Config config = new Config();
        config.setDebug(false);

        // Create and submit the local cluster
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("word-count-topology", config, builder.createTopology());

            // Let the topology run for 30 seconds
            Utils.sleep(30000);

            // Kill the topology
            cluster.killTopology("word-count-topology");
            cluster.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}