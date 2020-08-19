import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StageB1 extends Stage {

    StageB1(String group, String transactionalID, int numConsumers) {
        // Super constructor for class Stage, which sets consumer and producer properties.
        super(group, transactionalID, numConsumers);

        int initialId = 1;
        /*
            Pipeline schema:
                (covidData) -> Stage A ---> (covidStageB1) -> Stage B1 ---> (covidStageC) -> Stage C -> (covidStats)
                                        |-> (covidStageB2) -> Stage B2 -|
         */
        List<String> inTopics = Collections.singletonList("covidStageB1");
        List<String> outTopics = new ArrayList<>(List.of("covidStageC"));

        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        for (int i = 0; i < numConsumers; i++) {
            final int id = initialId + i;
            executor.submit(new StageB1Runnable(id, consumerProps, producerProps, group, inTopics, outTopics));
        }
        executor.shutdown();
    }
}

class StageB1Runnable implements Runnable {

    private final int id;
    private final List<String> inTopics;
    private final List<String> outTopics;
    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final Map<String, Person> covidRecovered;
    private final String group;
    private final boolean abortTest;
    private final Random rand = new Random();
    private final boolean running;
    private boolean recovered;

    public StageB1Runnable(int id, Properties consumerProps, Properties producerProps, String group, List<String> inTopics, List<String> outTopics) {

        this.id = id;
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.producer = new KafkaProducer<>(producerProps);
        this.group = group;
        this.inTopics = inTopics;
        this.outTopics = outTopics;

        covidRecovered = new HashMap<>();
        running = true;
        recovered = false;
        abortTest = false;
    }

    @Override
    public void run() {
        /*
            Subscribe to the given list of topics to get dynamically assigned partitions.
         */
        consumer.subscribe(inTopics);
        try {
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
                if (!recovered) {
                    seekToBeginning();
                    recovery();
                    recovered = true;
                }
                consumeTransformProduce();
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("PATTERN ERROR");
        } catch (KafkaException e) {
            e.printStackTrace();
            System.err.println("B1 - ABORT");
            producer.flush();
            producer.abortTransaction();
        }
        consumer.close();
        producer.close();
    }

    /**
     * 1. The consumer associated to that stage executes a “dummy” poll
     * to join the consumer group and to access data from the KafkaTopic.
     * <p>
     * 2. Then the consumer seek to zero the offset for each TopicPartition
     * assigned to the KafkaTopic, so that reading will start from the first
     * ever record in each TopicPartition.
     */
    private void seekToBeginning() {
        consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
        consumer.seekToBeginning(consumer.assignment());
    }

    /**
     * 1. The consumer executes now a useful poll to fetch records from the beginning.
     * 2. For each TopicPartition, a subset of records are retrieved.
     * 3. For each of these record, if the record offset is less or equal to the last committed offset
     * in that TopicPartion, then the record is put in the HashMap
     * 4. For each TopicPartition, set as offset, the last committed one
     */
    private void recovery() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));

        for (final TopicPartition partition : records.partitions()) {

            bigPrint("RECOVERY STARTED - PARTITION: " + partition.partition());
            final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            for (ConsumerRecord<String, String> record : partitionRecords) {
                println(">>> Recovering at offset: " + record.offset() + "; value: " + record.value());
                try {
                    if (consumer.committed(partition) != null) {
                        if (record.offset() <= consumer.committed(partition).offset())
                            synchronized (covidRecovered) {
                                covidRecovered.put(record.key(), createNewPerson(record.value()));
                            }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println("Previous committed offset was NULL");
                }
            }
            bigPrint("RECOVERY COMPLETED");
            println(records.records(partition).size() + " records recovered");
        }

        bigPrint("LAST COMMITTED OFFSET");
        for (TopicPartition tp : consumer.assignment()) {
            if (consumer.committed(tp) != null) {
                println("[Partition " + tp.partition() + "] -> last committed offset was: " +
                        consumer.committed(tp).offset());
                consumer.seek(tp, consumer.committed(tp));
            } else {
                println("[Partition " + tp.partition() + "] -> NO OFFSET COMMITTED");
                consumer.seek(tp, 0);
            }
        }
    }

    private void consumeTransformProduce() throws KafkaException {
        /*
            Fetch data for the topics or partitions specified using one of the subscribe/assign APIs
        */
        ConsumerRecords<String, String> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));

        // HERE the process is CONSUMING new messages

        for (final ConsumerRecord<String, String> record : records) {

            final String key = record.key();
            String value = record.value();
            Person person = createNewPerson(value);
            println(" <<< Received from partition " + record.partition() + "(offset=" + record.offset() + "): \n" +
                    "\t\t\t\t\t\tName: " + person.getName() +
                    "\tSex: " + person.getSex() +
                    "\tAge: " + person.getAge() +
                    "\tFrom: " + person.getRegion() +
                    "\ttStatus: " + person.getStatus()
            );

            producer.beginTransaction();

            String valueToSend;
            int numRecovered, avgAge, numFemale, numMale;

            synchronized (covidRecovered) {
                if (!covidRecovered.containsKey(key) /*|| person.getStatus().equals("D")*/) {
                    //New positive -> Recovered
                    covidRecovered.put(key, person);
                } else {
                    producer.flush();
                    producer.abortTransaction();
                    errPrintln("ABORT");
                    break;
                }

                //HERE IS TRANSFORMING

                numRecovered = covidRecovered.size();
                avgAge = (int) covidRecovered.values()
                        .stream()
                        .mapToInt(Person::getAge)
                        .summaryStatistics()
                        .getAverage();

                numMale = (int) covidRecovered.values()
                        .stream()
                        .filter(x -> x.getSex().equals("M"))
                        .count();
                numFemale = (int) covidRecovered.values()
                        .stream()
                        .filter(x -> x.getSex().equals("F"))
                        .count();
            }
            /*
            Map<String, Long> regionsRank = covidRecovered.values()
                    .stream()
                    .collect(Collectors.groupingBy(Person::getRegion, Collectors.counting()));
            String rank;
            for (Map.Entry<String, Long> entry : regionsRank.entrySet()){
                rank = rank + entry.getKey() + entry.
            }*/

            valueToSend = numRecovered + "#" + avgAge + "#" + numFemale + "#" + numMale + "@Recovered";

            //HERE IS PRODUCING
            try {
                producer.send(new ProducerRecord<>(outTopics.get(0), String.valueOf(rand.nextInt(1000)), valueToSend),
                        (metadata, e) -> {
                            if (e != null)
                                e.printStackTrace();
                            //println("The offset of the record we just sent is: " + metadata.offset());
                        });
            } catch (Exception e) {
                e.printStackTrace();
            }
            println("[Consumer" + id + "->" + person.getStatus() + "] - Sending >>>" + valueToSend);

            // The producer manually commits the outputs for the consumer within the transaction
            final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
            for (final TopicPartition partition : records.partitions()) {
                final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                map.put(partition, new OffsetAndMetadata(lastOffset + 1));
            }

            try {

                if (abortTest) {
                    Random r = new Random();
                    if (r.nextBoolean()) {
                        errPrintln("!!! Transaction ABORTED");
                        producer.flush();
                        producer.abortTransaction();
                        covidRecovered.remove(key, person);
                    } else {
                        println(" ^^^ COMMITTED\n");
                        producer.sendOffsetsToTransaction(map, group);
                        producer.commitTransaction();
                    }
                } else {
                    println(" ^^^ COMMITTED\n");
                    producer.sendOffsetsToTransaction(map, group);
                    producer.commitTransaction();
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private void bigPrint(String text) {
        println("##############################################################################");
        println("\t\t\t\t\t" + text);
        println("##############################################################################");
    }

    private Person createNewPerson(String value) {
        return new Person(
                value.split("#")[0],
                value.split("#")[1],
                Integer.parseInt(value.split("#")[2]),
                value.split("#")[3],
                value.split("#")[4]
        );
    }

    private void println(String text) {
        System.out.println("[" + getClass().getCanonicalName() + "-" + id + "]: " + text);
    }

    private void errPrintln(String text) {
        System.err.println("[" + getClass().getCanonicalName() + "-" + id + "]: " + text);
    }

}
