import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public abstract class Stage {
    final Properties consumerProps;
    final Properties producerProps;
    final int numConsumers;

    public Stage(String group, String transactionalID, int numConsumers) {
        this.consumerProps = createConsumerProperties(group);
        this.producerProps = createProducerProperties(group, transactionalID);
        this.numConsumers = numConsumers;
    }

    /**
     * This method create an instance of type "Properties"
     *
     * @return the properties to be set as "Consumer Properties"
     */
    private Properties createConsumerProperties(String group) {

        final Properties consumerProps = new Properties();
        /*
            Property "bootstrap.servers" represent here a host:port pair that is
            the address of one brokers in a Kafka cluster
         */
        consumerProps.put("bootstrap.servers", "localhost:9092");
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
        consumerProps.put("auto.offset.reset", "earliest");

        return consumerProps;
    }

    /**
     * This method create an instance of type "Properties"
     *
     * @return the properties to be set as "Producer Properties"
     */
    private Properties createProducerProperties(String group, String transactionalID) {

        final Properties producerProps = new Properties();
        /*
            Property "bootstrap.servers" represent here a host:port pair that is
            the address of one brokers in a Kafka cluster
         */
        producerProps.put("bootstrap.servers", "localhost:9092");
        /*
            A unique string that identifies the consumer group this consumer belongs to.
            Even if these are producer properties, the stages works on
            "consume-transform-produce" pattern: the producer must be able to commit the offset for its respective
            consumer and, to do so, it need the know the group id of the consumer.
         */
        producerProps.put("group.id", group);
        /*
            Properties "key/value.serializer instruct how to turn the key and value
             objects the user provides with their ProducerRecord into bytes.
         */
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        /*
            Property "enable.idempotence" is used to set or no the producer as "idempotent"
            The idempotent producer strengthens Kafka's delivery semantics from at least once
            to exactly once delivery. In particular producer retries will no longer
            introduce duplicates.
         */
        producerProps.put("enable.idempotence", true);
        /* Property "transactional.id" is used for fault tolerance, i.e. to identify
            the "same" producer if it crashes and gets restarted.
           When it does so, the Kafka broker checks for open transactions with the
           given transactional.id and completes them.
        */
        producerProps.put("transactional.id", transactionalID);

        return producerProps;
    }

}
