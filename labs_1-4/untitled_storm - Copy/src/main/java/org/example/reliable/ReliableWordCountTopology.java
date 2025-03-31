package org.example.reliable;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class ReliableWordCountTopology {

    public static void main(String[] args) throws Exception {
        // Construim topologia
        TopologyBuilder builder = new TopologyBuilder();

        // Adăugăm componentele
        builder.setSpout("source-spout", new ReliableSourceSpout(), 1);

        builder.setBolt("split-bolt", new ReliableSplitBolt(), 2)
                .shuffleGrouping("source-spout");

        builder.setBolt("count-bolt", new ReliableCountBolt(), 2)
                .fieldsGrouping("split-bolt", new Fields("word"));

        builder.setBolt("terminal-bolt", new ReliableTerminalBolt(), 1)
                .globalGrouping("count-bolt");

        // Configurație specială pentru toleranță la căderi
        Config config = new Config();
        config.setDebug(false);

        // Setăm timeout-ul de detectare a eșecurilor la 10 secunde
        config.setMessageTimeoutSecs(10);

        // Creăm și pornim clusterul local
        LocalCluster cluster = new LocalCluster();
        try {
            System.out.println("Pornire topologie cu toleranță la căderi...");
            cluster.submitTopology("reliable-word-count", config, builder.createTopology());

            // Rulăm pentru 30 de secunde
            Utils.sleep(30000);

            // Oprim topologia
            System.out.println("Oprire topologie...");
            cluster.killTopology("reliable-word-count");
            cluster.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}