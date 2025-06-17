package weather.advancedRouting;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import weather.PublisherSpout;
import weather.SubscriptionSpout;

public class AdvancedRoutingTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("publisher-spout", new PublisherSpout(), 1);
        builder.setSpout("subscription-spout", new SubscriptionSpout(), 1);

        builder.setBolt("subscription-dispatcher", new SubscriptionDispatcherBolt(), 1)
                .shuffleGrouping("subscription-spout");

        builder.setBolt("city-filter", new FilteringBolt("city"), 2)
                .shuffleGrouping("publisher-spout") // Primește publicații
                .shuffleGrouping("subscription-dispatcher", "city-subscriptions");

        builder.setBolt("temp-filter", new FilteringBolt("temp"), 2)
                .shuffleGrouping("city-filter") // Primește publicații de la filtrul anterior
                .shuffleGrouping("subscription-dispatcher", "temp-subscriptions");

        builder.setBolt("wind-filter", new FilteringBolt("wind"), 2)
                .shuffleGrouping("temp-filter") // Primește publicații de la filtrul anterior
                .shuffleGrouping("subscription-dispatcher", "wind-subscriptions");

        builder.setBolt("advanced-subscriber-bolt", new AdvancedSubscriberBolt(), 2)
                .shuffleGrouping("wind-filter");


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