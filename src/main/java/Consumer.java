import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Consumer {
    public static void main(String[] args) throws InterruptedException {
		/*
			Usage: Consumer <group> <initialId> <numConsumers>
			group2 1 3
		 */
        if (args.length < 1) {
            err();
        }

        final String group = "GroupConsumer";
        final int initialId = 1;
        final int numConsumers = Integer.parseInt(args[0]);
        String ipServer = "localhost";

        if (args.length == 2)
            ipServer = args[1];

        final List<String> topic = Collections.singletonList("covidStats");

        final Properties consumerProps = createConsumerProperties(group, ipServer);

        System.out.println("[Connecting to " + ipServer + "...]");
        final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        for (int i = 0; i < numConsumers; i++) {
            final int id = initialId + i;
            executor.submit(new ConsumerRunnable(id, consumerProps, topic));
        }
        executor.shutdown();
        while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        }
    }

    private static void err() {
        System.out.println("Usage: Consumer <numConsumers> <ipServer>");
        System.exit(1);
    }

    /**
     * This method create an instance of type "Properties"
     *
     * @return the properties to be set as "Consumer Properties"
     */
    private static Properties createConsumerProperties(String group, String ip) {

        final Properties consumerProps = new Properties();
        /*
            Property "bootstrap.servers" represent here a host:port pair that is
            the address of one brokers in a Kafka cluster
         */
        consumerProps.put("bootstrap.servers", ip + ":9092");
        /*
            A unique string that identifies the consumer group this consumer belongs to.
         */
        consumerProps.put("group.id", group);
        /*
            Properties "key/value.deserializer instruct how to turn bytes of the key and value
             into objects to be used and transformed.
         */
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        /*
            Controls how to read messages written transactionally.
            If set to read_committed, consumer.poll() will only return transactional messages which have been committed.
            In this way, aborted transaction won't be read by the consumer.
         */
        consumerProps.put("isolation.level", "read_committed");
        /*
            If true the consumer's offset will be periodically committed in the background.
            In this case, it is set to FALSE in order to be committed only by the producer associated to this consumer.
         */
        consumerProps.put("enable.auto.commit", "false");
        /*
            It specifies what to do when there is no initial offset in Kafka or if the current offset does not exist
            any more on the server (e.g. because that data has been deleted).
                earliest: automatically reset the offset to the earliest offset
         */

        return consumerProps;
    }
}

class ConsumerRunnable implements Runnable {
    private final int id;
    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private volatile boolean running;

    public ConsumerRunnable(int id, Properties props, List<String> topics) {
        this.id = id;
        this.topics = topics;
        this.consumer = new KafkaConsumer<>(props);
        running = true;
    }

    @Override
    public void run() {
        try {
            System.out.println("[Consumer " + id + "]: Connected. Topic: " + topics);
            consumer.subscribe(topics);
            while (running) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
                for (final ConsumerRecord<String, String> record : records) {
                    String value = record.value();
                    println(" <<< Received -> Partition: " + record.partition() + ".\t" +
                            "Offset: " + record.offset() + ".\t" +
                            "Key: " + record.key() + ".\t");
                    String positives = value.split("@")[0];
                    String diseasePercentage = value.split("@")[1];

                    String statsRecovers = value.split("@")[2];
                    String recovered = statsRecovers.split("#")[0];
                    String avgRecovered = statsRecovers.split("#")[1];
                    String femalePercentageRecovered = statsRecovers.split("#")[2];
                    String malePercentageRecovered = statsRecovers.split("#")[3];

                    String statsDeaths = value.split("@")[3];
                    String deaths = statsDeaths.split("#")[0];
                    String avgDeaths = statsDeaths.split("#")[1];
                    String femalePercentageDeaths = statsDeaths.split("#")[2];
                    String malePercentageDeaths = statsDeaths.split("#")[3];

                    println("\t--------> GLOBAL STATS <--------");
                    println("\tTotal Positives: " + positives + "\tDisease Percentage: " + diseasePercentage + "%");
                    println("\t--------> RECOVERY STATS <--------");
                    println("\tTotal Recovered: " + recovered + "\tAverage Age: " + avgRecovered);
                    println("\tFemale Recovered: " + femalePercentageRecovered + "%" +
                            "\tMale Recovered: " + malePercentageRecovered + "%");
                    println("\t--------> DEATHS STATS <--------");
                    println("\tTotal Deaths: " + deaths + "\tAverage Age: " + avgDeaths);
                    println("\tFemale Recovered: " + femalePercentageDeaths + "%" +
                            "\tMale Recovered: " + malePercentageDeaths + "%\n");

					/*
						Commit offsets returned on the last poll() for all the subscribed list of topics and partitions.
						This is a synchronous commits and will block until either the commit succeeds or
						an unrecoverable error is encountered (in which case it is thrown to the caller).
					 */
                    consumer.commitSync();
                }
            }
        } finally {
            consumer.close();
        }
    }

    private void println(String text) {
        System.out.println("[" + getClass().getCanonicalName() + "-" + id + "]: " + text);
    }

    private void errPrintln(String text) {
        System.err.println("[" + getClass().getCanonicalName() + "-" + id + "]: " + text);
    }

}