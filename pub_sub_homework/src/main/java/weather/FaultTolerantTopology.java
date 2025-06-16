package weather;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class FaultTolerantTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        String publicationsFilePath = "C:/Users/Ionut/Documents/REPO/Masters/second_semester/ebs/EBS-team/pub_sub_homework/publications.txt";
        builder.setSpout("fault-tolerant-publisher-spout", new FaultTolerantPublisherSpout(publicationsFilePath), 1);

        // ... (partea de rutare, dacă vrei să o incluzi, rămâne la fel)
        builder.setSpout("subscription-spout", new SubscriptionSpout(), 1);
        builder.setBolt("subscription-dispatcher", new SubscriptionDispatcherBolt(), 1).shuffleGrouping("subscription-spout");
        builder.setBolt("city-filter", new FilteringBolt("city"), 2)
                .shuffleGrouping("fault-tolerant-publisher-spout")
                .shuffleGrouping("subscription-dispatcher", "city-subscriptions");

        // CORECȚIE: Înlocuim StatefulAggregatingBolt cu noul ManuallyStatefulBolt.
        // Îi dăm o cale unde să salveze fișierul cu starea.
        String stateFile = "window_state.ser";
        builder.setBolt("manually-stateful-bolt", new ManuallyStatefulBolt(stateFile), 1)
                .shuffleGrouping("city-filter");

        Config conf = new Config();
        conf.setDebug(true);

        // CORECȚIE: Am ȘTERS complet liniile de configurare pentru StateProvider.
        // conf.put(Config.TOPOLOGY_STATE_PROVIDER, ...);
        // conf.put(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL, ...);

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("fault-tolerant-topology-manual-state", conf, builder.createTopology());
            System.out.println("Topologia (cu stare manuală) a pornit. Rulează pentru 60 de secunde...");
            Thread.sleep(60000);
        } finally {
            cluster.shutdown();
        }
    }
}