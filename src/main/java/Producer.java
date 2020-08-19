import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A Kafka client that publishes records to the Kafka cluster.
 */
public class Producer {
    private static final boolean print = true;
    private static final boolean debug = true;
    private static final boolean waitAck = false;
    private static final int waitBetweenMsgs = 1000;
    private static final String[] regions = {
            "Abruzzo",
            "Basilicata",
            "Calabria",
            "Campania",
            "Emilia-Romagna",
            "Friuli-Venezia Giulia",
            "Lazio",
            "Liguria",
            "Lombardia",
            "Marche",
            "Molise",
            "Piemonte",
            "Puglia",
            "Sardegna",
            "Sicilia",
            "Toscana",
            "Trentino-Alto Adige",
            "Umbria",
            "Valle d’Aosta",
            "Veneto"
    };
    // used in randomized value
    private static final int disease = 14;

    static KafkaProducer<String, String> producer;

    /**
     * This method create an instance of type "Properties
     *
     * @return the properties to be set as "Producer Properties"
     */
    private static Properties createProperties(String ip) {
        Properties props = new Properties();

        /*
            Property "bootstrap.servers" represent here a host:port pair that is
            the address of one brokers in a Kafka cluster
         */
        props.put("bootstrap.servers", ip + ":9092");
        /*
            Properties "key/value.serializer instruct how to turn the key and value
             objects the user provides with their ProducerRecord into bytes.
         */
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        /*
            Property "enable.idempotence" is used to set or no the producer as "idempotent"
            The idempotent producer strengthens Kafka's delivery semantics from at least once
            to exactly once delivery. In particular producer retries will no longer
            introduce duplicates.
         */
        props.put("enable.idempotence", true);
        /* Property "transactional.id" is used for fault tolerance, i.e. to identify
            the "same" producer if it crashes and gets restarted.
           When it does so, the Kafka broker checks for open transactions with the
           given transactional.id and completes them.
        */
        props.put("transactional.id", "ID0-DataProducer");

        return props;
    }

    public static void main(String[] args) {

        String ipServer = "localhost";

        if (args.length > 0)
            ipServer = args[0];

        System.out.println("[Connecting to " + ipServer + "...]");

        /*
            Setting new properties for producer
         */
        final Properties props = createProperties(ipServer);
        /*
            Create the instance for a producer, using properties created just before
         */
        producer = new KafkaProducer<>(props);

        System.out.println("[Connected]");
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
        try {
            menu();
        } catch (NumberFormatException e) {
            System.err.println("Please enter a valid option");
            menu();
        }

    }

    /**
     * This method produces data of people and sends records to Kafka
     *
     * @param random: if set to true, it will produce data for deaths/recovered, following a default disease ratio
     */
    private static void produceValues(boolean random) {

        //Topics will be managed as strings
        final String topic = "covidData";
        int howMany = 1;

        // Establishing how many records will be produced
        do {
            System.out.print("How many messages? ");
            howMany = (new Scanner(System.in)).nextInt();
        } while (howMany <= 0);

        Random r = new Random();

        for (int i = 0; i < howMany; i++) {

            //Set a value in this format -> Name#Sex#Age#Region#Status
            String value =
                    generateAlphaNumericString(r.nextInt(5) + 4, true, true, false) + "#"
                            + (r.nextBoolean() ? "M" : "F") + "#"
                            + r.nextInt(100) + "#"
                            + regions[r.nextInt(20)];

            //Set a random key, based on hashing data (excluding status of the positive)
            final String key = //generateAlphaNumericString(uniqueCodeLength,true,false,true);
                    String.valueOf(value.hashCode());

            // Probability for a positive to die is "disease"/100, i.e. over 100 people, the disease% dies
            // D -> death
            // R -> Recover
            if (random)
                value = value + "#" + ((r.nextInt(100 / disease) == 0) ? "D" : "R");
            else {
                String c;
                // User set manually the status of Covid-19 positive.
                do {
                    System.out.print("Insert R/D: ");
                    c = (new Scanner(System.in)).next().toUpperCase();
                } while (!c.equals("R") && !c.equals("D"));
                value = value + "#" + c;
            }

            if (print) {
                System.out.print("Topic: " + topic + "\t");
                System.out.print("Key: " + key + "\t");
                System.out.print("Value: " + value + "\t");
                System.out.println();
            }

            /*
               It generates a key/value pair to be sent to Kafka.
               This consists of a topic name to which the record is being sent, an optional partition number,
               and an optional key and value.

               If a valid partition number is specified that partition will be used when sending the record.
               If no partition is specified but a key is present a partition will be chosen using a hash of the key.
               If neither key nor partition is present a partition will be assigned in a round-robin fashion.

               The record also has an associated timestamp.
               If the user did not provide a timestamp, the producer will stamp the record with its current time.
               The timestamp eventually used by Kafka depends on the timestamp type configured for the topic.
             */
            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            Future<RecordMetadata> future = null;

            try {
                /*
                    Should be called before the start of each new transaction.
                    Note that prior to the first invocation of this method,
                    you must invoke initTransactions() exactly one time.
                 */
                producer.beginTransaction();
                /*
                    Asynchronously send a record to a topic and invoke the provided callback
                    when the send has been acknowledged.
                    The send is asynchronous and this method will return immediately once the record has been stored
                    in the buffer of records waiting to be sent. This allows sending many records in parallel without
                    blocking to wait for the response after each one.

                    The result of the send is a RecordMetadata specifying the partition the record was sent to,
                    the offset it was assigned and the timestamp of the record.
                 */
                future = producer.send(record, (metadata, e) -> {
                    if (e != null)
                        e.printStackTrace();
                    if (debug)
                        System.out.println("\t>> SENT to partition: " + metadata.partition() + " at offset " + metadata.offset());
                });

                /*
                    Commits the ongoing transaction. This method will flush any unsent records before actually
                    committing the transaction. Further, if any of the send(ProducerRecord) calls which were part
                    of the transaction hit irrecoverable errors, this method will throw the last received exception
                    immediately and the transaction will not be committed. So all send(ProducerRecord) calls in a
                    transaction must succeed in order for this method to succeed.
                 */
                producer.commitTransaction();
            } catch (ProducerFencedException e) {
                e.printStackTrace();
                /*
                    Close this producer. This method blocks until all previously sent requests complete.
                 */
                producer.close();
            } catch (KafkaException e) {
                e.printStackTrace();
                /*
                    Invoking this method makes all buffered records immediately available to send
                    and blocks on the completion of the requests associated with these records.

                    The post-condition of flush() is that any previously sent record will have
                    completed (e.g. Future.isDone() == true). A request is considered completed when it is
                    successfully acknowledged according to the acks configuration specified
                    or else it results in an error.
                 */
                producer.flush();
                /*
                    Aborts the ongoing transaction.
                    Any unflushed produce messages will be aborted when this call is made.
                 */
                producer.abortTransaction();
            }

            if (waitAck) {
                try {
                    /*
                        Since the send call is asynchronous it returns a Future for the RecordMetadata that will be
                        assigned to this record. Invoking get() on this future will block until the associated request
                        completes and then return the metadata for the record or throw any exception
                        that occurred while sending the record.
                     */
                    System.out.println(future != null ? future.get() : "Future not set");
                } catch (InterruptedException | ExecutionException e1) {
                    e1.printStackTrace();
                }
            }

            try {
                Thread.sleep(waitBetweenMsgs);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }
        /*
            Close this producer. This method blocks until all previously sent requests complete.
         */
        producer.close();
    }

    /**
     * This method creates a random string, with a certain range of characters and as long
     * as the uniqueCodeLength variable is set. If the boolean parameters are all false,
     * then they will be set as all true.
     *
     * @param n              length of the final string
     * @param allowUpperCase if true, then the final string could contain upper case letter
     * @param allowLowerCase if true, then the final string could contain lower case letter
     * @param allowNumbers   if true, then the final string could contain numbers.
     * @return a random n-length string
     */
    private static String generateAlphaNumericString(int n, boolean allowUpperCase, boolean allowLowerCase, boolean allowNumbers) {

        if (!allowLowerCase && !allowNumbers && !allowUpperCase)
            allowLowerCase = allowNumbers = allowUpperCase = true;

        // chose a Character random from this String
        String alphaNumericString = "";
        if (allowUpperCase)
            alphaNumericString = alphaNumericString + "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        if (allowLowerCase)
            alphaNumericString = alphaNumericString + "abcdefghijklmnopqrstuvwxyz";
        if (allowNumbers)
            alphaNumericString = alphaNumericString + "0123456789";


        // create StringBuffer size of alphaNumericString
        StringBuilder sb = new StringBuilder(n);
        Random r = new Random();

        do {
            // generate a random number between
            // 0 to alphaNumericString variable length
            int index = r.nextInt(alphaNumericString.length());

            // add Character one by one in end of sb
            sb.append(alphaNumericString.charAt(index));
        } while (sb.length() < n);

        return sb.toString();

    }

    /**
     * Prints a menu for the application
     *
     * @throws NumberFormatException in case of not Integer values as input
     */
    private static void menu() throws NumberFormatException {
        Scanner scanner = new Scanner(System.in);

        int swValue;

        // Display menu graphics
        System.out.println("============================");
        System.out.println("|   MENU SELECTION DEMO    |");
        System.out.println("============================");
        System.out.println("| Options:                 |");
        System.out.println("|        1. Random Values  |");
        System.out.println("|        2. Manual Values  |");
        System.out.println("|        3. Exit           |");
        System.out.println("============================");
        System.out.print("Select option: ");
        swValue = Integer.parseInt(scanner.nextLine());

        // Switch construct for Menu
        switch (swValue) {
            case 1:
                produceValues(true);
                break;
            case 2:
                produceValues(false);
                break;
            case 3:
                System.out.println("Exit selected");
                System.exit(1);
                break;
            default:
                System.out.println("Invalid selection");
        }
    }

}