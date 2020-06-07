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

public class StageC extends Stage {

    public StageC(String group, String transactionalID, int numConsumers) throws InterruptedException {
        super(group, transactionalID, numConsumers);
        int initialId = 1;
        List<String> inTopics = new ArrayList<>(List.of("covidStageC"));
        List<String> outTopics = Collections.singletonList("covidStats");
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        for (int i = 0; i < numConsumers; i++) {
            final int id = initialId + i;
            executor.submit(new StageCRunnable(id, consumerProps, producerProps, group, inTopics, outTopics));
        }
        executor.shutdown();

    }

}

class StageCRunnable implements Runnable {

    private final int id;
    private final List<String> inTopics;
    private final List<String> outTopics;
    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final String group;
    private final Random rand = new Random();
    private final boolean abortTest;
    private final boolean running;
    private final boolean recovered;
    private int positives, covidRecovered, covidDeaths;
    private String statsRecovered, statsDeaths;

    public StageCRunnable(int id, Properties consumerProps, Properties producerProps, String group, List<String> inTopics, List<String> outTopics) {

        this.id = id;
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.producer = new KafkaProducer<>(producerProps);
        this.group = group;
        this.inTopics = inTopics;
        this.outTopics = outTopics;

        positives = 0;
        covidRecovered = 0;
        covidDeaths = 0;
        statsRecovered = "0#0#0#0";
        statsDeaths = "0#0#0#0";

        running = true;
        recovered = false;
        abortTest = false;
    }

    @Override
    public void run() {

        consumer.subscribe(inTopics);
        producer.initTransactions();

        while (running) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            producer.beginTransaction();
            for (final ConsumerRecord<String, String> record : records) {

                final String key = record.key();
                String value = record.value();

                println("<<< Received from partition " + record.partition() + "(offset=" + record.offset() + "):\t" +
                        value
                );

                String valueToSend;
                if (value.endsWith("Recovered"))
                    statsRecovered = computeStatsForRecovers(value.split("@")[0]);
                else
                    statsDeaths = computeStatsForDeaths(value.split("@")[0]);

                positives = covidRecovered + covidDeaths;
                float disease = ((float) covidDeaths / (positives)) * 100;
                valueToSend = positives + "@" + String.format("%.02f", disease) + "@" + statsRecovered + "@" + statsDeaths;

                String outTopic = outTopics.get(0);
                producer.send(new ProducerRecord<>(outTopic, key, valueToSend));
                println(">>> Forwarding to " + outTopic + ": " + valueToSend);

                /*
                String key = record.key();
                String value = record.value();
                println("\t <<< Received: " + key + " : " + value);
                int positives = Integer.parseInt(value.split("#")[0]);
                int recovered = Integer.parseInt(value.split("#")[1]);
                String ratio = String.format("%.02f", ((float) recovered / positives) * 100);
                String valueToSend =
                        "Overall cases: " + positives +
                                "\tRecovered: " + recovered +
                                "\tRecovery ratio: " + ratio + "%";
                producer.send(new ProducerRecord<>(outTopics.get(0), key, valueToSend));
                println(">>> Sending: " + valueToSend);

              */
            }

            // The producer manually commits the outputs for the consumer within the
            // transaction
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

    private String computeStatsForDeaths(String value) {
        covidDeaths = Integer.parseInt(value.split("#")[0]);
        String avgAge = value.split("#")[1];
        int numFemale = Integer.parseInt(value.split("#")[2]);
        int numMale = Integer.parseInt(value.split("#")[3]);

        float percentageFemale = ((float) numFemale / covidDeaths) * 100;
        float percentageMale = ((float) numMale / covidDeaths) * 100;

        return covidDeaths + "#" + avgAge + "#" + String.format("%.02f", percentageFemale) + "#" + String.format("%.02f", percentageMale);
    }

    private String computeStatsForRecovers(String value) {
        covidRecovered = Integer.parseInt(value.split("#")[0]);
        String avgAge = value.split("#")[1];
        int numFemale = Integer.parseInt(value.split("#")[2]);
        int numMale = Integer.parseInt(value.split("#")[3]);

        float percentageFemale = ((float) numFemale / covidRecovered) * 100;
        float percentageMale = ((float) numMale / covidRecovered) * 100;

        return covidRecovered + "#" + avgAge + "#" + String.format("%.02f", percentageFemale) + "#" +
                String.format("%.02f", percentageMale);
    }


    private void println(String text) {
        System.out.println("[" + getClass().getCanonicalName() + "-" + id + "]: " + text);
    }

    private void errPrintln(String text) {
        System.err.println("[" + getClass().getCanonicalName() + "-" + id + "]: " + text);
    }

}