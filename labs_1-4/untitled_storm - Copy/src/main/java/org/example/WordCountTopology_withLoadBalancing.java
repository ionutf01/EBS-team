package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class WordCountTopology_withLoadBalancing {

    public static void main(String[] args) throws Exception {
        // Creăm builder-ul topologiei
        TopologyBuilder builder = new TopologyBuilder();

        // Setăm spout-ul
        builder.setSpout("source-spout", new SourceTextSpout(), 1);

        // Setăm bolts
        // Primul bolt folosește shuffleGrouping standard
        builder.setBolt("split-bolt", new SplitTextBolt(), 2)
                .shuffleGrouping("source-spout");

        // Al doilea bolt folosește load balancer-ul custom
        builder.setBolt("count-bolt", new WordCountBolt(), 3)  // Folosim 3 pentru a demonstra load balancing
                .customGrouping("split-bolt", new LoadBalancingGrouping());

        // Ultimul bolt folosește globalGrouping
        builder.setBolt("terminal-bolt", new TerminalBolt(), 1)
                .globalGrouping("count-bolt");

        // Configurăm topologia
        Config config = new Config();
        config.setDebug(false);  // Setăm false pentru a reduce output-ul de debugging

        // Creăm și submittem clusterul local
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("word-count-topology", config, builder.createTopology());

            // Lăsăm topologia să ruleze pentru 60 de secunde
            Utils.sleep(60000);

            // Oprim topologia
            cluster.killTopology("word-count-topology");
            cluster.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}