package org.example;

    import java.io.FileWriter;
    import java.io.IOException;
    import java.io.PrintWriter;
    import java.time.LocalDate;
    import java.time.format.DateTimeFormatter;
    import java.util.*;
    import java.util.concurrent.*;
    import java.util.stream.Collectors;
    import java.util.stream.IntStream;

    public class Generator {
        private static final String[] CITIES = {"Bucharest", "Cluj", "Iasi", "Timisoara", "Constanta", "Brasov", "Craiova"};
        private static final String[] DIRECTIONS = {"N", "NE", "E", "SE", "S", "SW", "W", "NW"};
        private static final String[] OPERATORS = {"=", ">", "<", ">=", "<=", "!="};

        public static void main(String[] args) {
            System.out.println("Current Date and Time (UTC): 2025-03-31 18:35:05");
            System.out.println();

            // Test configurations
            List<TestConfig> testConfigs = new ArrayList<>();

            // Baseline test case
            testConfigs.add(new TestConfig(10000, 5000, "Baseline test case"));
            // Small-scale test
            testConfigs.add(new TestConfig(1000, 500, "Small-scale test"));
            // Large-scale test
            testConfigs.add(new TestConfig(50000, 25000, "Large-scale test"));
            // Subscription-heavy test
            testConfigs.add(new TestConfig(10000, 20000, "Subscription-heavy test"));

            // Thread counts to test with
            int[] threadCounts = {1, 4, 6, 8};

            // Field frequency configuration for subscriptions (percentage)
            Map<String, Integer> fieldFrequency = new HashMap<>();
            fieldFrequency.put("stationid", 30);
            fieldFrequency.put("city", 80);
            fieldFrequency.put("temp", 60);
            fieldFrequency.put("rain", 40);
            fieldFrequency.put("wind", 50);
            fieldFrequency.put("direction", 20);
            fieldFrequency.put("date", 10);

            // Equality operator frequency for fields (percentage)
            Map<String, Integer> equalityFrequency = new HashMap<>();
            equalityFrequency.put("city", 70);

            String processorInfo = getProcessorInfo();
            System.out.println("Processor: " + processorInfo);
            System.out.println();

            // Create results for the MD file
            List<TestResult> results = new ArrayList<>();
            int runNumber = 1;

            for (TestConfig config : testConfigs) {
                System.out.println("Testing with " + config.publicationCount + " publications and " +
                        config.subscriptionCount + " subscriptions");

                for (int threadCount : threadCounts) {
                    System.out.println("Thread count: " + threadCount);

                    // Measure generation time
                    long startTime = System.currentTimeMillis();
                    List<Publication> publications = generatePublications(config.publicationCount, threadCount);
                    List<Subscription> subscriptions = generateSubscriptionsExact(config.subscriptionCount, fieldFrequency, equalityFrequency, threadCount);
                    long endTime = System.currentTimeMillis();

                    double executionTimeSeconds = (endTime - startTime) / 1000.0;
                    System.out.println("  Generation time: " + (endTime - startTime) + " ms (" + executionTimeSeconds + " seconds)");

                    // Print sample data
                    System.out.println("  Sample Publication: " + publications.get(0));
                    System.out.println("  Sample Subscription: " + subscriptions.get(0));

                    // Add result to the list
                    results.add(new TestResult(
                        runNumber++,
                        config.publicationCount,
                        config.subscriptionCount,
                        threadCount,
                        executionTimeSeconds
                    ));

                    System.out.println();
                }

                // Only validate the last test configuration for efficiency
                if (config == testConfigs.get(testConfigs.size() - 1)) {
                    validateSubscriptions(generateSubscriptionsExact(config.subscriptionCount, fieldFrequency, equalityFrequency, 1),
                            fieldFrequency, equalityFrequency);
                }
            }

            // Write results to MD file
            writeResultsToMdFile(results, processorInfo);
            System.out.println("Results written to performance_results.md");

            // write all the subscriptions and publications to 2 separate files
            try (PrintWriter pubWriter = new PrintWriter(new FileWriter("publications.txt"));
                 PrintWriter subWriter = new PrintWriter(new FileWriter("subscriptions.txt"))) {
                for (Publication pub : generatePublications(10000, 1)) {
                    pubWriter.println(pub);
                }
                for (Subscription sub : generateSubscriptionsExact(5000, fieldFrequency, equalityFrequency, 1)) {
                    subWriter.println(sub);
                }
            } catch (IOException e) {
                System.err.println("Error writing to file: " + e.getMessage());
            }
        }
        private static void writeResultsToMdFile(List<TestResult> results, String processorInfo) {
            try (PrintWriter writer = new PrintWriter(new FileWriter("performance_results.md"))) {
                // Write header
                writer.println("# Performance Results");
                writer.println();
                writer.println("This file contains performance metrics from each run of the program.");
                writer.println("Processor: " + processorInfo);
                writer.println();

                // Write table header
                writer.println("| Run # | Publications | Subscriptions | Parallelism (Processes) | Time (seconds) |");
                writer.println("|-------|--------------|---------------|-------------------------|----------------|");

                // Write table rows
                for (TestResult result : results) {
                    writer.printf("| %-5d | %-12d | %-13d | %-25d | %-14.2f |\n",
                            result.runNumber,
                            result.publications,
                            result.subscriptions,
                            result.parallelism,
                            result.timeSeconds);
                }
            } catch (IOException e) {
                System.err.println("Error writing to Markdown file: " + e.getMessage());
            }
        }

        private static String getProcessorInfo() {
            return System.getProperty("os.name") + ", " +
                    Runtime.getRuntime().availableProcessors() + " cores";
        }

        private static List<Publication> generatePublications(int count, int threadCount) {
            if (threadCount <= 1) {
                return IntStream.range(0, count)
                        .mapToObj(i -> generateRandomPublication())
                        .collect(Collectors.toList());
            } else {
                return generateParallel(count, threadCount, i -> generateRandomPublication());
            }
        }

        private static List<Subscription> generateSubscriptionsExact(int count, Map<String, Integer> fieldFrequency,
                                                                     Map<String, Integer> equalityFrequency, int threadCount) {
            // Calculate exact counts for each field
            Map<String, Integer> fieldCounts = new HashMap<>();
            for (Map.Entry<String, Integer> entry : fieldFrequency.entrySet()) {
                fieldCounts.put(entry.getKey(), Math.round((entry.getValue() * count) / 100f));
            }

            // Calculate exact counts for equality operators
            Map<String, Integer> equalityCounts = new HashMap<>();
            for (Map.Entry<String, Integer> entry : equalityFrequency.entrySet()) {
                String field = entry.getKey();
                if (fieldCounts.containsKey(field)) {
                    int fieldCount = fieldCounts.get(field);
                    equalityCounts.put(field, Math.round((entry.getValue() * fieldCount) / 100f));
                }
            }

            // Generate subscription templates (field and operator combinations)
            List<SubscriptionTemplate> templates = generateSubscriptionTemplates(count, fieldCounts, equalityCounts);

            // Generate actual subscriptions using the templates
            if (threadCount <= 1) {
                return IntStream.range(0, count)
                        .mapToObj(i -> generateSubscriptionFromTemplate(templates.get(i)))
                        .collect(Collectors.toList());
            } else {
                return generateParallelWithTemplate(templates, threadCount);
            }
        }

        private static List<SubscriptionTemplate> generateSubscriptionTemplates(int count,
                                                                                Map<String, Integer> fieldCounts,
                                                                                Map<String, Integer> equalityCounts) {
            // Create subscription templates that only specify which fields and operators to use
            List<SubscriptionTemplate> templates = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                templates.add(new SubscriptionTemplate());
            }

            Random rand = new Random(42); // Fixed seed for reproducibility

            // Assign fields to subscriptions to match exact percentages
            for (Map.Entry<String, Integer> entry : fieldCounts.entrySet()) {
                String field = entry.getKey();
                int fieldCount = entry.getValue();

                // Get indices of subscriptions that will contain this field
                List<Integer> indices = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    indices.add(i);
                }
                Collections.shuffle(indices, rand);
                indices = indices.subList(0, fieldCount);

                // Calculate how many of these fields should use equality operator
                int equalityCount = equalityCounts.getOrDefault(field, 0);

                // Assign field and operator to selected subscription templates
                for (int i = 0; i < fieldCount; i++) {
                    int index = indices.get(i);
                    String operator = (i < equalityCount) ? "=" : getRandomNonEqualityOperator(rand);
                    templates.get(index).addField(field, operator);
                }
            }

            return templates;
        }

        private static String getRandomNonEqualityOperator(Random rand) {
            String[] operators = {">", "<", ">=", "<=", "!="};
            return operators[rand.nextInt(operators.length)];
        }

        private static Subscription generateSubscriptionFromTemplate(SubscriptionTemplate template) {
            Random rand = new Random();
            Subscription subscription = new Subscription();

            // Add conditions based on the template
            for (FieldOperator fo : template.getFieldOperators()) {
                String field = fo.field;
                String operator = fo.operator;

                // Generate value based on field type
                Object value;
                switch (field) {
                    case "stationid":
                        value = rand.nextInt(100) + 1;
                        break;
                    case "city":
                        value = CITIES[rand.nextInt(CITIES.length)];
                        break;
                    case "temp":
                        value = rand.nextInt(41) - 10;
                        break;
                    case "rain":
                        value = Math.round(rand.nextDouble() * 50 * 10) / 10.0;
                        break;
                    case "wind":
                        value = rand.nextInt(101);
                        break;
                    case "direction":
                        value = DIRECTIONS[rand.nextInt(DIRECTIONS.length)];
                        break;
                    case "date":
                        LocalDate startDate = LocalDate.of(2023, 1, 1);
                        long days = startDate.toEpochDay();
                        long endDays = LocalDate.of(2025, 12, 31).toEpochDay();
                        long randomDay = rand.nextInt((int) (endDays - days)) + days;
                        value = LocalDate.ofEpochDay(randomDay);
                        break;
                    default:
                        value = null;
                }

                if (value != null) {
                    subscription.addCondition(field, operator, value);
                }
            }

            return subscription;
        }

        private static <T> List<T> generateParallel(int count, int threadCount, Function<Integer, T> generator) {
            List<T> result = Collections.synchronizedList(new ArrayList<>(count));
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);

            int batchSize = count / threadCount;
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                int start = t * batchSize;
                int end = (t == threadCount - 1) ? count : (t + 1) * batchSize;

                futures.add(executor.submit(() -> {
                    for (int i = start; i < end; i++) {
                        result.add(generator.apply(i));
                    }
                }));
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            executor.shutdown();
            return result;
        }

        private static List<Subscription> generateParallelWithTemplate(List<SubscriptionTemplate> templates, int threadCount) {
            List<Subscription> result = Collections.synchronizedList(new ArrayList<>(templates.size()));
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);

            int count = templates.size();
            int batchSize = count / threadCount;
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                int start = t * batchSize;
                int end = (t == threadCount - 1) ? count : (t + 1) * batchSize;

                futures.add(executor.submit(() -> {
                    for (int i = start; i < end; i++) {
                        result.add(generateSubscriptionFromTemplate(templates.get(i)));
                    }
                }));
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            executor.shutdown();
            return result;
        }

        private static Publication generateRandomPublication() {
            Random rand = new Random();

            int stationId = rand.nextInt(100) + 1;
            String city = CITIES[rand.nextInt(CITIES.length)];
            int temp = rand.nextInt(41) - 10; // -10 to 30 degrees
            double rain = Math.round(rand.nextDouble() * 50 * 10) / 10.0; // 0 to 50 mm
            int wind = rand.nextInt(101); // 0 to 100 km/h
            String direction = DIRECTIONS[rand.nextInt(DIRECTIONS.length)];

            // Random date between 2023-01-01 and 2025-12-31
            LocalDate startDate = LocalDate.of(2023, 1, 1);
            long days = startDate.toEpochDay();
            long endDays = LocalDate.of(2025, 12, 31).toEpochDay();
            long randomDay = rand.nextInt((int) (endDays - days)) + days;
            LocalDate date = LocalDate.ofEpochDay(randomDay);

            return new Publication(stationId, city, temp, rain, wind, direction, date);
        }

        private static void validateSubscriptions(List<Subscription> subscriptions,
                                                  Map<String, Integer> targetFieldFreq,
                                                  Map<String, Integer> targetEqFreq) {
            System.out.println("Validation Results:");

            // Check field frequencies
            for (Map.Entry<String, Integer> entry : targetFieldFreq.entrySet()) {
                String field = entry.getKey();
                int targetFreq = entry.getValue();

                long count = subscriptions.stream()
                        .filter(s -> s.hasField(field))
                        .count();

                double actualFreq = (double) count / subscriptions.size() * 100;
                System.out.printf("  Field '%s': target=%d%%, actual=%.1f%% (%d/%d)\n",
                        field, targetFreq, actualFreq, count, subscriptions.size());
            }

            // Check equality operator frequencies
            for (Map.Entry<String, Integer> entry : targetEqFreq.entrySet()) {
                String field = entry.getKey();
                int targetEqFreqValue = entry.getValue();

                long fieldCount = subscriptions.stream()
                        .filter(s -> s.hasField(field))
                        .count();

                long eqCount = subscriptions.stream()
                        .filter(s -> s.hasFieldWithOperator(field, "="))
                        .count();

                double actualEqFreq = fieldCount > 0 ? (double) eqCount / fieldCount * 100 : 0;
                System.out.printf("  Equality operator for '%s': target=%d%%, actual=%.1f%% (%d/%d)\n",
                        field, targetEqFreqValue, actualEqFreq, eqCount, fieldCount);
            }
        }

        // Configuration class
        static class TestConfig {
            final int publicationCount;
            final int subscriptionCount;
            final String description;

            TestConfig(int publicationCount, int subscriptionCount, String description) {
                this.publicationCount = publicationCount;
                this.subscriptionCount = subscriptionCount;
                this.description = description;
            }
        }

        // Result class for the table
        static class TestResult {
            final int runNumber;
            final int publications;
            final int subscriptions;
            final int parallelism;
            final double timeSeconds;

            TestResult(int runNumber, int publications, int subscriptions, int parallelism, double timeSeconds) {
                this.runNumber = runNumber;
                this.publications = publications;
                this.subscriptions = subscriptions;
                this.parallelism = parallelism;
                this.timeSeconds = timeSeconds;
            }
        }

        @FunctionalInterface
        interface Function<T, R> {
            R apply(T t);
        }

        static class SubscriptionTemplate {
            private final List<FieldOperator> fieldOperators = new ArrayList<>();

            public void addField(String field, String operator) {
                fieldOperators.add(new FieldOperator(field, operator));
            }

            public List<FieldOperator> getFieldOperators() {
                return fieldOperators;
            }
        }

        static class FieldOperator {
            final String field;
            final String operator;

            FieldOperator(String field, String operator) {
                this.field = field;
                this.operator = operator;
            }
        }
    }