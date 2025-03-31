package org.example;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class WordCountTopology_withGrouping {
    private static final String SPOUT_ID = "source_text_spout";
    private static final String SPLIT_BOLT_ID = "split_bolt";
    private static final String COUNT_BOLT_ID = "count_bolt";
    private static final String TERMINAL_BOLT_ID = "terminal_bolt";

    public static void main(String[] args) throws Exception {
        // Create the topology builder
        TopologyBuilder builder = new TopologyBuilder();
        SourceTextSpout spout = new SourceTextSpout();
        SplitTextBolt splitbolt = new SplitTextBolt();
        WordCountBolt countbolt = new WordCountBolt();
        TerminalBolt terminalbolt = new TerminalBolt();

        // Set the spout
        builder.setSpout(SPOUT_ID, spout, 2);
        //    	builder.setBolt(SPLIT_BOLT_ID, splitbolt).shuffleGrouping(SPOUT_ID);
        //    	builder.setBolt(SPLIT_BOLT_ID, splitbolt, 2).setNumTasks(4).allGrouping(SPOUT_ID);

        //    	builder.setBolt(COUNT_BOLT_ID, countbolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));

        // Set the bolts
//        builder.setBolt(SPLIT_BOLT_ID, splitbolt, 2)
//                .shuffleGrouping(SPOUT_ID);
//        builder.setBolt(SPLIT_BOLT_ID, splitbolt, 2).setNumTasks(4).shuffleGrouping(SPOUT_ID);
        builder.setBolt(SPLIT_BOLT_ID, splitbolt, 2).setNumTasks(4).customGrouping(SPOUT_ID, new MyGrouping());

        builder.setBolt(COUNT_BOLT_ID, countbolt, 4)
                .fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));

        builder.setBolt(TERMINAL_BOLT_ID, terminalbolt)
                .globalGrouping(COUNT_BOLT_ID);

        // Configure the topology
        Config config = new Config();
        config.setDebug(false);

        // Create and submit the local cluster
        LocalCluster cluster = new LocalCluster();

        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,1024);
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