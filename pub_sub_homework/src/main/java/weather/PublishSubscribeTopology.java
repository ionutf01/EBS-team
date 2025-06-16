package weather;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

public class PublishSubscribeTopology {
    private static final int MAX_PUBLICATIONS = 10000;
    private static final int MAX_SUBSCRIPTIONS = 10000;
    private static final int TUMBLING_WINDOW_SIZE = 3;

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        Map<String, Integer> fieldFrequency = new HashMap<>();
        fieldFrequency.put("stationid", 30);
        fieldFrequency.put("city", 90);
        fieldFrequency.put("temp", 60);
        fieldFrequency.put("rain", 40);
        fieldFrequency.put("wind", 50);
        fieldFrequency.put("direction", 20);
        fieldFrequency.put("date", 10);

        long runDuration = 3 * 60 * 1000; // 3 minutes in milliseconds

        Map<String, Integer> equalityFrequency = new HashMap<>();
        equalityFrequency.put("city", 25);

        builder.setSpout("config-spout", new ConfigurationSpout(
                MAX_PUBLICATIONS, MAX_SUBSCRIPTIONS, fieldFrequency, equalityFrequency), 1);

        builder.setBolt("template-generator", new TemplateGeneratorBolt(), 1)
                .shuffleGrouping("config-spout");

        builder.setBolt("publication-generator", new PublicationGeneratorBolt(), 4)
                .shuffleGrouping("template-generator", "publication-stream");

        builder.setBolt("subscription-generator", new SubscriptionGeneratorBolt(), 4)
                .shuffleGrouping("template-generator", "subscription-stream");

        builder.setBolt("broker-bolt", new BrokerBolt(TUMBLING_WINDOW_SIZE), 3)
                .shuffleGrouping("publication-generator")
                .shuffleGrouping("subscription-generator");

        builder.setBolt("subscriber-bolt", new SubscriberBolt(), 3)
                .shuffleGrouping("broker-bolt", "notifications");

        Config conf = new Config();
        conf.setDebug(false);
        conf.registerSerialization(java.time.LocalDate.class);

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("hybrid-pub-sub-topology", conf, builder.createTopology());
            System.out.println("APP LOG: Topologie pornită. Așteptăm finalizarea generării și procesării...");
            Utils.sleep(runDuration);
        } finally {
            cluster.killTopology("hybrid-pub-sub-topology");
            cluster.shutdown();
            System.out.println("APP LOG: Topologia oprită.");
            System.out.println("===== EVALUARE SISTEM =====");
            System.out.println("a) Publicații livrate cu succes: " + SubscriberBolt.getReceivedCount());
            System.out.printf("b) Latență medie de livrare: %.2f ms%n", SubscriberBolt.getAverageLatency());
            System.out.println("c) Publicații care au fost potrivite (matched): " + BrokerBolt.getMatchedCount());
            System.out.println("============================");
            System.exit(0);
        }
    }
}