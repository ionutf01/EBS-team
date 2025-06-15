package weather;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Punctul de intrare (main class) pentru a rula topologia cu RUTARE AVANSATĂ.
 * Folosește componentele specializate: SubscriptionDispatcherBolt, FilteringBolt și AdvancedSubscriberBolt.
 */
public class AdvancedRoutingTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("publisher-spout", new PublisherSpout(), 1);
        builder.setSpout("subscription-spout", new SubscriptionSpout(), 1);

        builder.setBolt("subscription-dispatcher", new SubscriptionDispatcherBolt(), 1)
                .shuffleGrouping("subscription-spout");

        // 1. PRIMA OPRIRE: Toate publicațiile de la Spout merg la "city-filter"
        builder.setBolt("city-filter", new FilteringBolt("city"), 2)
                .shuffleGrouping("publisher-spout"); // <-- Sursa este Spout-ul

        // 2. A DOUA OPRIRE: "temp-filter" primește publicații DOAR de la "city-filter"
        builder.setBolt("temp-filter", new FilteringBolt("temp"), 2)
                .shuffleGrouping("city-filter"); // <-- Sursa este brokerul anterior

        // 3. A TREIA OPRIRE: "wind-filter" primește publicații DOAR de la "temp-filter"
        builder.setBolt("wind-filter", new FilteringBolt("wind"), 2)
                .shuffleGrouping("temp-filter"); // <-- Sursa este brokerul anterior

        // 4. DESTINAȚIA FINALĂ: Subscriber-ul primește publicații DOAR de la "wind-filter"
        builder.setBolt("advanced-subscriber-bolt", new AdvancedSubscriberBolt(), 2)
                .shuffleGrouping("wind-filter"); // <-- Sursa este ultimul broker


        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("advanced-routing-topology", conf, builder.createTopology());
            System.out.println("Topologia AVANSATĂ a pornit. Rulează pentru 60 de secunde...");
            Thread.sleep(60000);
        } finally {
            cluster.shutdown();
        }
    }
}