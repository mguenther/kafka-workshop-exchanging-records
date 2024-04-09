# Lab Assignment

## Task 1: Producers (Multiple Choice)

Before we dive into the coding exercise, let's take a minute to refresh a couple of things that we've just learned in the session before. We have assorted a couple of questions for you to test your understanding of the topic.

### Question 1

How does a Kafka producer determine which partition to send a message to?

1. Based on the message size.
2. Using a partitioning key.
3. The Kafka broker decides.
4. By random selection.

### Question 2

What is the purpose of the `acks` configuration in a Kafka producer?

1. To specify the compression type.
2. To determine the number of consumer acknowledgements.
3. To control the acknowledgement level required for a successful message send.
4. To set the maximum message size.

### Question 3

How can a Kafka producer improve throughput?

1. By using a single partition.
2. By reducing the batch size.
3. By increasing the batch size.
4. By sending each message synchronously.

### Question 4

What mechanism does a Kafka producer use to handle failures when sending messages?

1. Automatic topic creation.
2. Schema validation.
3. Retry mechanism with backoff.
4. Partition reassignment.

### Question 5

When a Kafka producer sends a message, what does the `key` of the message determine?

1. The value of the message.
2. The partition to which the message will be sent.
3. The format of the message.
4. The priority of the message in the queue.

## Task 2: Publish Messages w/o Keys

Take a look at class `Producer`. This class provides the skeleton for the producer you are about to implement. For this task, we start off quite simple:

1. Construct a `Map<String, Object>` which holds the minimal configuration for a `KafkaProducer`. This requires you to set the bootstrap servers as well as the key and value serializers. Look into class `ProducerConfig` to select the correct key for the resp. parameters.

2. Create a `KafkaProducer` with the minimal configuration.

3. Provide an implementation for method `public Future<RecordMetadata> publish(String topic, String message)`.

If everything works as expected, the test labeled with "Task #2" should turn green.

## Task 3: Topic-Partition Distribution

There is another test case that publishes multiple messages w/o key to the same topic. This topic is configured with three partitions. What do you expect in terms of "shared load" between these topic-partitions if you publish 10 messages? Try to play around with the number of messages and re-run the test. Are your expectations still satisfied?

## Task 4: Consumers (Multiple Choice)

Before we dive into the coding exercise, let's take a minute to refresh a couple of things that we've just learned in the session before. We have assorted a couple of questions for you to test your understanding of the topic.

### Question 1

Which of the following is a feature of Kafka consumers?

1. Schema validation.
2. Load balancing of messages between consumers in the same group.
3. Automatic topic creation.
4. Real-time analytics.

### Question 2

How does a Kafka consumer handle partition assignment in a consumer group?

1. Through manual assignment by the administrator.
2. It's randomly assigned by the Kafka broker.
3. Via a group coordinator and a partition assignment strategy.
4. Each consumer selects its own partitions.

### Question 3

What is the purpose of the `auto.offset.reset` property in a Kafka consumer?

1. To reset the consumer's connection to the broker.
2. To specify the consumer's initial offset when no previous offset is found.
3. To reset the offset after each message is consumed.
4. To periodically reset offsets for load balancing.

### Question 4

A consumer wants to read messages from a specific partition of a topic. How can this be achieved?

1. Call `assign` and pass a collection of topic-partitions as argument.
2. Call `subscribe` and pass a topic-partition as argument.
3. Call `subscribe` and pass a topic-partition and number of partitions of the topic as arguments.

### Question 5

Have a look at the following code.

```java
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(100);
    try {
        consumer.commitSync();
    } catch (CommitFailedException e) {
        log.error("commit failed", e);
    }
    for (ConsumerRecord<String, String> record : records) {
        System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
        record.topic(), record.partition(),
        record.offset(), record.key(), record.value());
    }
}
```

What kind of delivery guarantee does this consumer offer?

1. At-most-once
2. Exactly-once
3. At-least-once

## Task 5: Consuming Messages

Take a look at class `Consumer`. This class provides the skeleton for a simple consumer you are about to implement. For this task, we start off quite simple. First, familiarize yourself with the implementation of the `run` method. There, you can find already some key ingredients of the consumer's poll loop.

1. Your first task is to provide an implementation for the `initializeConsumer` method. You should do the following things in this method:
    * Construct a `Map<String, Object>` which holds the configuration for a `KafkaConsumer`. Apart from the minimal configuration, also make sure that the `auto.offset.reset` policy is set to `earliest` and that a `group.id` is set.
    * Create an instance of `KafkaConsumer<String, String>` and assign it to the member variable `consumer`.
    * Subscribe to topic `consumer-test`.
2. The consumer won't be of much use if we don't poll for records. Let's implement that! Take a look again at the `run` method. The outline of the poll loop is there. What's missing? Use the `KafkaConsumer<String, String>` instance to poll for records. Delegate each and every consumed record to method `process`.

After you've completed both of these sub-tasks, the resp. test should turn green.

## Task 6: Delivery Guarantees

Evaluate your solution of task 5 in terms of the delivery guarantee that it satisfies.

## (Optional) Task 7: Publish Messages with Keys

Provide an implementation for method `public Future<RecordMetadata> publish(String topic, String key, String message)`.

What can you observe in terms of the partitions that your messages have been written to? Why?

## (Optional) Task 8: Record Metadata

We are now going to include additional metadata with the messages that our producer publishes. Add the header `trace-id` to every `ProducerRecord`. This trace ID should be a random `String`, unique to each record.

## (Optional) Task 9: Callbacks and Error Handling

Study the definition and JavaDoc for interface `org.apache.kafka.clients.producer.Callback`. This interface provides the means to add a callback handler to every call to `KafkaProducer.send()`. Providing a `Callback` handler allows you not only to get notified on the success of the `send` operation, but more importantly lets you act upon any error situation that might have occurred.

1. Provide an implementation for method `public Future<RecordMetadata> publish(String topic, String key, String message, Callback onCompletion)`. This method accepts a callback from the caller and delegates it to the `KafkaProducer`. Testcase `task_9_1` should turn green if you hooked up everything correctly.
2. For the next part, we're trying to simulate some error. Check out test `task_9_2`. Let's see if we can generate an error by providing an incorrect topic name (the test currently uses a valid topic identifier). Allowed characters in a topic name are: `[a-zA-Z0-9\\._\\-]`. Change the name of the topic the test uses to something invalid. The test should turn green if you've successfully generated the error.
3. The `InvalidTopicException` belongs to the class of non-retryable errors. Some error situations might be transient and thus can be retried. A `KafkaProducer` can retry a failed publication request up to `ProducerConfig.RETRIES_CONFIG` (property: `retries`) times. Familiarize yourself with the non-retryable and retryable exceptions that can occur when `send`ing a record to a Kafka broker (cf. JavaDoc of `Callback`).
4. If the in-sync replica set of a given topic falls below its minimum threshold, the `KafkaProducer` will throw a `NotEnoughReplicasException`. This exception is retryable, as replicas might come back online after a short amount of time. At which point does a `KafkaProducer` generate the actual exception?

> You can actually test this one! Take a look at test class `RetryableExceptionsProducerTest`. In the implemented testcase, we are starting up a Kafka cluster with three brokers and a topic named `test-topic` with 5 partitions and 3 replicas. We also set its `min.insync.replicas` parameter to 2. If the number of available brokers falls below this minimum size, the producer cannot write to any topic-partition of that topic, since there are not enough replicas to guarantee data consistency. The test simulates this situation by disconnecting brokers until the minimum ISR is no longer satisfied.

Why does the test not terminate? Can you alter the behavior of the `KafkaProducer` via its configuration so that the test has a chance to complete?

## That's it! You've done great!

You have completed all assignments. If you have any further questions or need clarification, please don't hesitate to reach out to us. We're here to help.