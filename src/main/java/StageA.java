import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StageA extends Stage {

    public StageA(String group, String transactionalID, int numConsumers) {
        // Super constructor for class Stage, which sets consumer and producer properties.
        super(group, transactionalID, numConsumers);

        int initialId = 1;
        /*
            Pipeline schema:
                (covidData) -> Stage A ---> (covidStageB1) -> Stage B1 ---> (covidStageC) -> Stage C -> (covidStats)
                                        |-> (covidStageB2) -> Stage B2 -|
         */
        List<String> inTopics = Collections.singletonList("covidData");
        List<String> outTopics = new ArrayList<>(List.of("covidStageB1", "covidStageB2"));

        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        for (int i = 0; i < numConsumers; i++) {
            final int id = initialId + i;
            executor.submit(new StageARunnable(id, consumerProps, producerProps, group, inTopics, outTopics));
        }
        executor.shutdown();
    }
}

class StageARunnable implements Runnable {

    private final int id;
    private final List<String> inTopics;
    private final List<String> outTopics;
    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final String group;
    private final boolean abortTest;
    private final Random rand = new Random();
    private final boolean running;
    private final boolean recovered;

    public StageARunnable(int id, Properties consumerProps, Properties producerProps, String group, List<String> inTopics, List<String> outTopics) {

        this.id = id;
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.producer = new KafkaProducer<>(producerProps);
        this.group = group;
        this.inTopics = inTopics;
        this.outTopics = outTopics;

        running = true;
        recovered = false;
        abortTest = false;
    }

    @Override
    public void run() {

        /*
            Subscribe to the given list of topics to get dynamically assigned partitions.
         */
        consumer.subscribe(Collections.singleton(inTopics.get(0)));
        /*
            Needs to be called before any other methods when the transactional.id is set in the configuration.
            This method does the following:
                1.  Ensures any transactions initiated by previous instances of the producer with the same
                    transactional.id are completed. If the previous instance had failed with a transaction in progress,
                    it will be aborted. If the last transaction had begun completion, but not yet finished,
                    this method awaits its completion.
                2.  Gets the internal producer id and epoch, used in all future transactional messages
                    issued by the producer.
            Once the transactional state has been successfully initialized, this method should no longer be used.
         */
        producer.initTransactions();

        while (running) {
            /*
                Fetch data for the topics or partitions specified using one of the subscribe/assign APIs
             */
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            producer.beginTransaction();
            for (final ConsumerRecord<String, String> record : records) {

                final String key = record.key();
                String value = record.value();
                println("<<< Received from partition " + record.partition() + "(offset=" + record.offset() + "):\t" +
                        value
                );

                String status = value.split("#")[4];
                String outTopic = (status.endsWith("R")) ? outTopics.get(0) : outTopics.get(1);
                producer.send(new ProducerRecord<>(outTopic, key, value));
                println(">>> Forwarding to " + outTopic + ": " + value);
            }

            // The producer manually commits the outputs for the consumer within the transaction
            final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
            for (final TopicPartition partition : records.partitions()) {
                final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                map.put(partition, new OffsetAndMetadata(lastOffset + 1));
            }
            producer.sendOffsetsToTransaction(map, group);
            producer.commitTransaction();
        }
        consumer.close();
        producer.close();
    }

    private void println(String text) {
        System.out.println("[" + getClass().getCanonicalName() + "-" + id + "]: " + text);
    }

    private void errPrintln(String text) {
        System.err.println("[" + getClass().getCanonicalName() + "-" + id + "]: " + text);
    }
}