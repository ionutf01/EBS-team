package stormy;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * GeneratorTopology defines and configures a Storm topology for generating
 * synthetic publications and subscriptions with configurable distributions.
 *
 * This class serves as the entry point to the application, setting up the complete
 * data generation pipeline including:
 * 1. Configuration parameters and distribution settings
 * 2. Topology components (spouts and bolts) and their connections
 * 3. Execution environment (local testing or cluster deployment)
 * 4. Performance tracking and validation reporting
 */
public class GeneratorTopology {

    // Set volume of data to generate
    private static final int MAX_PUBLICATIONS = 10000;
    private static final int MAX_SUBSCRIPTIONS = 5000;
    /**
     * Main entry point for the application. Configures and starts the Storm topology
     * for generating publications and subscriptions.
     *
     * @param args Command-line arguments; if provided, the topology runs in cluster mode
     * @throws Exception If topology submission fails
     */
    public static void main(String[] args) throws Exception {
        // Print identification information (timestamp and user)
        // Note: Using hardcoded values for reproducibility
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        System.out.println("Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): 2025-03-31 19:32:23");
        System.out.println("Current User's Login: ionutf01");
        System.out.println();

        // Create a new topology builder to define the data flow
        TopologyBuilder builder = new TopologyBuilder();

        // -------------------------------------------------------------------------
        // Configuration parameters for data generation
        // -------------------------------------------------------------------------

        // Define field frequency distribution for subscriptions (as percentages)
        // These determine how often each field appears across all subscriptions
        Map<String, Integer> fieldFrequency = new HashMap<>();
        fieldFrequency.put("stationid", 30);  // 30% of subscriptions include station ID
        fieldFrequency.put("city", 90);       // 90% of subscriptions include city
        fieldFrequency.put("temp", 60);       // 60% of subscriptions include temperature
        fieldFrequency.put("rain", 40);       // 40% of subscriptions include rainfall
        fieldFrequency.put("wind", 50);       // 50% of subscriptions include wind speed
        fieldFrequency.put("direction", 20);  // 20% of subscriptions include wind direction
        fieldFrequency.put("date", 10);       // 10% of subscriptions include date

        // Define equality operator frequency for fields (as percentages)
        // For each field, what percentage should use equality (=) vs other operators
        Map<String, Integer> equalityFrequency = new HashMap<>();
        equalityFrequency.put("city", 70);    // 70% of city fields use equality operator

        // Print generation parameters and environment information
        System.out.println("Testing with " + MAX_PUBLICATIONS + " publications and " +
                MAX_SUBSCRIPTIONS + " subscriptions");
        System.out.println("Processor: " + getProcessorInfo());
        System.out.println();

        // -------------------------------------------------------------------------
        // Topology component definitions and connections
        // -------------------------------------------------------------------------

        // Configure the source spout that provides initial configuration parameters
        builder.setSpout("config-spout", new ConfigurationSpout(
                MAX_PUBLICATIONS, MAX_SUBSCRIPTIONS, fieldFrequency, equalityFrequency), 1);

        // Template generator converts configurations to subscription templates
        builder.setBolt("template-generator", new TemplateGeneratorBolt(), 1)
                .shuffleGrouping("config-spout");

        // Publication generator creates synthetic weather data publications (parallelism=4)
        builder.setBolt("publication-generator", new PublicationGeneratorBolt(), 4)
                .shuffleGrouping("template-generator", "publication-stream");

        // Subscription generator creates subscriptions from templates (parallelism=4)
        builder.setBolt("subscription-generator", new SubscriptionGeneratorBolt(), 4)
                .shuffleGrouping("template-generator", "subscription-stream");

        // Output bolts write the generated data to files and track performance
        builder.setBolt("publication-output", new OutputBolt("publications"), 1)
                .shuffleGrouping("publication-generator");

        builder.setBolt("subscription-output", new OutputBolt("subscriptions"), 1)
                .shuffleGrouping("subscription-generator");

        // -------------------------------------------------------------------------
        // Storm configuration and execution setup
        // -------------------------------------------------------------------------

        // Create Storm configuration
        Config conf = new Config();
        conf.setDebug(false);  // Disable debug mode for better performance

        // Disable authentication for local development mode
        conf.put(Config.NIMBUS_IMPERSONATION_ACL, Collections.emptyMap());
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, "org.apache.storm.security.auth.DefaultPrincipalToLocal");

        // Choose execution mode based on command-line arguments
        if (args != null && args.length > 0) {
            // Cluster deployment mode
            conf.setNumWorkers(6);  // Use 3 worker processes in the cluster
            conf.put(Config.WORKER_CHILDOPTS, "-Djava.security.manager=allow");
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            // Local testing mode with embedded Storm cluster
            conf.setMaxTaskParallelism(6);
            System.setProperty("java.security.manager", "allow");

            // Begin tracking overall execution time
            long startTime = System.currentTimeMillis();

            // Create and start local cluster
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("generator", conf, builder.createTopology());


            // Let the topology run until all data is generated
            try {
                // Wait until the expected number of publications and subscriptions are generated
                while (!isGenerationComplete()) {
                    Thread.sleep(1000); // Check every second
                }

                // Calculate and report overall execution time
                long endTime = System.currentTimeMillis();
                System.out.println("Generation completed in: " + (endTime - startTime) + " ms");

            } finally {
                // Ensure cluster is properly shut down
                cluster.shutdown();
                // stop execution
                System.exit(0);
            }
        }
    }

    /**
     * Retrieves information about the processor environment.
     * Used for logging performance context information.
     *
     * @return String containing OS name and available processor cores
     */
    private static String getProcessorInfo() {
        return System.getProperty("os.name") + ", " +
                Runtime.getRuntime().availableProcessors() + " cores";
    }

    /**
     * Checks if the generation of publications and subscriptions is complete
     * by comparing the number of items processed against the expected counts.
     *
     * @return true if all expected publications and subscriptions have been generated
     */
    private static boolean isGenerationComplete() {
        // log the current counts for debugging
        System.out.println("Current publication count: " + PublicationGeneratorBolt.getPublishedCount());
        System.out.println("Current subscription count: " + SubscriptionGeneratorBolt.getSubscriptionCount());
        // Check if the publication and subscription counts match the expected values
        return PublicationGeneratorBolt.getPublishedCount() >= MAX_PUBLICATIONS &&
                SubscriptionGeneratorBolt.getSubscriptionCount() >= MAX_SUBSCRIPTIONS;
    }
}