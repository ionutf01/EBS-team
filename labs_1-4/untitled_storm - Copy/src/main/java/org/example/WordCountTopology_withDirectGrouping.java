package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class WordCountTopology_withDirectGrouping {

    public static void main(String[] args) throws Exception {
        // Creăm builder-ul topologiei
        TopologyBuilder builder = new TopologyBuilder();

        // Setăm componentele (spout și bolt-uri)
        builder.setSpout("source-spout", new SourceTextSpout(), 1);

        builder.setBolt("split-bolt", new SplitTextBolt(), 2)
                .shuffleGrouping("source-spout");

        builder.setBolt("count-bolt", new WordCountBolt(), 2)
                .fieldsGrouping("split-bolt", new Fields("word"));

        builder.setBolt("terminal-bolt", new TerminalBolt(), 1)
                .globalGrouping("count-bolt");

        // Adăugăm noul bolt de statistici cu grupare directă de la source-spout și split-bolt
        builder.setBolt("statistics-bolt", new StatisticsBolt(), 1)
                .directGrouping("source-spout", "sentenceCount")
                .directGrouping("split-bolt", "wordCount");

        // Configurăm topologia
        Config config = new Config();
        config.setDebug(false);

        // Creăm și submittem clusterul local
        LocalCluster cluster = new LocalCluster();
        try {
            System.out.println("Submitting topology...");
            cluster.submitTopology("word-count-topology", config, builder.createTopology());

            // Lăsăm topologia să ruleze pentru 30 de secunde
            System.out.println("Topology running...");
            Utils.sleep(30000);

            // Oprim topologia
            System.out.println("Shutting down...");
            cluster.killTopology("word-count-topology");
            cluster.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}