package weather;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class SecureTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // 1. Sursele de date emit publicații și subscripții în CLAR.
        builder.setSpout("publisher-spout", new PublisherSpout(), 1);
        builder.setSpout("subscription-spout", new SubscriptionSpout(), 1);

        // 2. Gateway-ul de Criptare primește datele în clar și le amprentează.
        builder.setBolt("encryption-bolt", new EncryptionBolt(), 1)
                .shuffleGrouping("publisher-spout")
                .shuffleGrouping("subscription-spout");

        // 3. Brokerul Criptat primește doar date amprentate de la gateway.
        builder.setBolt("encrypted-broker-bolt", new EncryptedBrokerBolt(), 2)
                .shuffleGrouping("encryption-bolt", "encrypted-publications")
                .shuffleGrouping("encryption-bolt", "encrypted-subscriptions");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("secure-topology", conf, builder.createTopology());
            System.out.println("Topologia Securizată a pornit. Rulează pentru 60 de secunde...");
            Thread.sleep(60000);
        } finally {
            cluster.shutdown();
        }
    }
}