# Middleware Technologies For Distributed Systems 2019/2020
## Processing pipeline in Kafka 
Implement a data processing pipeline in Kafka.
### Requirements

 - Provide administrative tools / scripts to create and deploy a
   processing pipeline that processes messages from a given topic.
 - A processing pipeline consists of multiple stages, each of them
   processing an input message at a time and producing one output
   message for the downstream stage.
 - Different processing stages could run on different processes for scalability.
 - Messages have a key, and the processing of messages with different keys is independent.
	 - Stages are stateful and their state is partitioned by key (where is the state stored?).
	 - Each stage consists of multiple processes that handle messages with different keys in parallel.
 - Messages having the same key are processed in FIFO order with end-to-end exactly once delivery semantics.
### Assumptions
 - Processes can fail.
 - Kafka topics with replication factor > 1 can be considered reliable.
 - You are only allowed to use Kafka Producers and Consumers API
	 - You cannot use Kafka Processors or Streams API,  but you can take inspiration from their model. 
 - You can assume a set of predefined functions to implement stages, and you can refer to them by name in the scripts that create and deploy a processing pipeline.

### Admin (start Zookeeper and Apache Kafka)

* Start ZooKeeper: ./bin/zookeeper-server-start.sh config/zookeeper.properties
* Start a Kafka server: ./bin/windows/kafka-server-start.bat config/server.properties