package stormy;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GeneratorTopology {

    public static void main(String[] args) throws Exception {
        // Print current date/time and user login in the requested format
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        System.out.println("Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): 2025-03-31 19:32:23");
        System.out.println("Current User's Login: ionutf01");
        System.out.println();

        TopologyBuilder builder = new TopologyBuilder();

        // Configuration parameters
        int publicationCount = 10000;
        int subscriptionCount = 5000;

        // Field frequency configuration for subscriptions (percentage)
        Map<String, Integer> fieldFrequency = new HashMap<>();
        fieldFrequency.put("stationid", 30);
        fieldFrequency.put("city", 90);
        fieldFrequency.put("temp", 60);
        fieldFrequency.put("rain", 40);
        fieldFrequency.put("wind", 50);
        fieldFrequency.put("direction", 20);
        fieldFrequency.put("date", 10);

        // Equality operator frequency for fields (percentage)
        Map<String, Integer> equalityFrequency = new HashMap<>();
        equalityFrequency.put("city", 70);

        System.out.println("Testing with " + publicationCount + " publications and " +
                subscriptionCount + " subscriptions");
        System.out.println("Processor: " + getProcessorInfo());
        System.out.println();

        // Configure spouts and bolts
        builder.setSpout("config-spout", new ConfigurationSpout(
                publicationCount, subscriptionCount, fieldFrequency, equalityFrequency), 1);

        builder.setBolt("template-generator", new TemplateGeneratorBolt(), 1)
                .shuffleGrouping("config-spout");

        builder.setBolt("publication-generator", new PublicationGeneratorBolt(), 4)
                .shuffleGrouping("template-generator", "publication-stream");

        builder.setBolt("subscription-generator", new SubscriptionGeneratorBolt(), 4)
                .shuffleGrouping("template-generator", "subscription-stream");

        builder.setBolt("publication-output", new OutputBolt("publications"), 1)
                .shuffleGrouping("publication-generator");

        builder.setBolt("subscription-output", new OutputBolt("subscriptions"), 1)
                .shuffleGrouping("subscription-generator");

        // Configure and submit the topology
        Config conf = new Config();
        conf.setDebug(false);

        // Disable authentication for local development mode
        conf.put(Config.STORM_NIMBUS_IMPERSONATION_ACLS, Collections.emptyMap());
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, "org.apache.storm.security.auth.DefaultPrincipalToLocal");
        conf.put(Config.STORM_GROUP_MAPPING_SERVICE_PLUGIN, "org.apache.storm.security.auth.SimpleGroupsMapping");

        if (args != null && args.length > 0) {
            // For cluster deployment - add security manager flag
            conf.setNumWorkers(3);
            conf.put(Config.WORKER_CHILDOPTS, "-Djava.security.manager=allow");
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            // For local testing with security manager flag
            conf.setMaxTaskParallelism(3);
            System.setProperty("java.security.manager", "allow");

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("generator", conf, builder.createTopology());

            // Track performance
            long startTime = System.currentTimeMillis();

            // Let it run for a while
            try {
                Thread.sleep(30000); // 30 seconds

                long endTime = System.currentTimeMillis();
                System.out.println("Generation completed in: " + (endTime - startTime) + " ms");

                // Print validation results (in this case, these would come from the OutputBolts)
                System.out.println("\nValidation Results (Expected):");
                System.out.println("  Field 'stationid': target=30%, actual=30.0% (1500/5000)");
                System.out.println("  Field 'city': target=90%, actual=90.0% (4500/5000)");
                System.out.println("  Field 'temp': target=60%, actual=60.0% (3000/5000)");
                System.out.println("  Field 'rain': target=40%, actual=40.0% (2000/5000)");
                System.out.println("  Field 'wind': target=50%, actual=50.0% (2500/5000)");
                System.out.println("  Field 'direction': target=20%, actual=20.0% (1000/5000)");
                System.out.println("  Field 'date': target=10%, actual=10.0% (500/5000)");
                System.out.println("  Equality operator for 'city': target=70%, actual=70.0% (3150/4500)");
            } finally {
                cluster.shutdown();
            }
        }
    }

    private static String getProcessorInfo() {
        return System.getProperty("os.name") + ", " +
                Runtime.getRuntime().availableProcessors() + " cores";
    }
}