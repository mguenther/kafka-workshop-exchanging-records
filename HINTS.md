# Hints

**Spoiler Alert**

We encourage you to work on the assignment yourself or together with your peers. However, situations may present themselves to you where you're stuck on a specific assignment. Thus, this document contains a couple of hints/solutions that ought to guide you through a specific task of the lab assignment.

In any case, don't hesitate to talk to us if you're stuck on a given problem!

## Task 1

### Question 1

Answer 2.

### Question 2

Answer 3.

### Question 3

Answer 3.

### Question 4

Answer 3.

### Question 5

Answer 2.

## Task 2.1

The minimal configuration requires you to add three properties that we've also shown in the slides. These are:

* `bootstrap.servers` (cf. `ProducerConfig.BOOTSTRAP_SERVERS_CONFIG`)
* `key.serializer` (cf. `ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG`)
* `value.serializer` (cf. `ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG`)

You can either provide a `Map<String, Object>` that contains values for these parameters or an instance of `java.util.Properties`.

Possible solution:

```java
var producerConfig = Map.of(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
```

## Task 2.2

The configuration that you constructed as part of task 2.1 needs to be passed along to the `KafkaProducer<K,V>` upon instance creation.

## Task 2.3

Bind the `KafkaProducer<String, String>` to a member variable of the `Producer` class or use it within a try-with-resources-block inside the method. The former should be preferred in a production-grade setup (but then, `Producer` should also implement `Closeable` and delegate the call to close to the underlying `KafkaProducer`), the latter is acceptable given in the context of this lab assignment.

Construct a `ProducerRecord<String, String>` and pass along `topic` as well as `message`.

Pass the `ProducerRecord<String, String>` to `send` method of the `KafkaProducer`.

Possible solution:

```java
public Future<RecordMetadata> publish(String topic, String message) {
    var producerConfig = ...; // cf. task 2.1
    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
        var record = new ProducerRecord<String, String>(topic, message);
        return producer.send(record);
    }
}
```

## Task 3

You'd probably expect an even distribution of messages across all topic-partitions. In previous versions, Kafka claimed to follow a round-robin-strategy in this case, but since Apache Kafka 2.4+ there seems to be an issue with this implementation (cf. [KAFKA-9965: Uneven distribution with RoundRobinPartitioner in AK 2.4+
](https://issues.apache.org/jira/browse/KAFKA-9965?page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel&focusedCommentId=17260987#comment-17260987)). Records are pinned to a topic-partition if they are in the same batch the producer sends them to the broker. Pinning batches to a topic-partition does not necessarily follow a round-robin strategy, though. Although this might not be a problem in the long run, for smaller workloads you might experience a slight imbalance in load on your topic-partitions given due to this behavior.

## Task 4

### Question 1

Answer 2.

### Question 2

Answer 3.

### Question 3

Answer 2.

### Question 4

In general: Use `assign` if you want to have control over the topic-partition assignments per consumer, otherwise use `subscribe`. A call to `subscribe` will result in dynamically assigned topic-partitions.

`assign` takes a collection of `TopicPartition` objects as argument. Hence, option 1 is the correct one.

See also the [Javadoc](https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#assign-java.util.Collection-) for more details.

### Question 5

The consumer commits (blocking) offsets before any kind of processing happens on the received records. This is by design at-most-once (option 1). This design has negative impact if the consumer crashes before it is able to process the received records. In this case, these records will be lost, as the offsets have already been committed. Once the consumer comes back online (or re-balancing shifts the resp. partition to another consumer) it will not fetch these records again

## Task 5.1

The minimal configuration requires you to add at least three properties that we've also shown in the slides. These are:

* `bootstrap.servers` (cf. `ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG`)
* `key.deserializer` (cf. `ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG`)
* `value.deserializer` (cf. `ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG`)

However, you also should provide a value for `group.id` (cf. `ConsumerConfig.GROUP_ID_CONFIG`), even if there is only a single consumer instance. Otherwise, group management and commit APIs, which are essential if you use `subscribe` to read records from a topic, will not work as expected.

You can either provide a `Map<String, Object>` that contains values for these parameters or an instance of `java.util.Properties`.

Possible solution:

```java
void initializeConsumer() {
    var topic = "consumer-test";
    Map<String, Object> config = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.GROUP_ID_CONFIG, "consumer-test",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumer = new KafkaConsumer<>(config);
    consumer.subscribe(Set.of(topic));
}
```

## Task 5.2

Once the initialization of the `KafkaConsumer<String, String>` is done, this is a fairly easy task. Simple use the `poll(Duration timeout)` method to poll for records. This yields an instance of `ConsumerRecords<String, String>`, wherein each record is represented as `ConsumerRecord<String, String>`. Pass these individual records to the `process` method.

Possible solution:

```java
@Override
public void run() {
    initializeConsumer();
    try {
        while (running) {
            var records = consumer.poll(Duration.ofMillis(100));
            for (var record : records) {
                process(record);
            }
        }
    } catch (WakeupException ignore) {
        // this exception should be ignored (see comment in stop())
    } finally {
        shutdown();
    }
}
```

## Task 6

This is more of open question, as there are many possible solutions among your peers. We'll discuss your solutions in the group.

## Task 7

Given that we attach the same key in the test case to all of the records are going to be published, we'd expect that all of these records are written to the same topic partition. The test asserts that this is actually the case.

Possible solution:

```java
public Future<RecordMetadata> publish(String topic, String key, String message) {
    var producerConfig = ...; // cf. task 2.1
    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
        var record = new ProducerRecord<String, String>(topic, key, message);
        return producer.send(record);
    }
}
```

## Task 8

If in doubt, use `UUID.randomUUID().toString()` to generate the random `String`.

There is a `RecordHeaders` class that allows you to set headers. Construct an instance of it, add the appropriate headers to it and associate it with the `ProducerRecord`.

Possible solution:

```java
public Future<RecordMetadata> publish(String topic, String message) {
    var producerConfig = ...; // cf. task 2.1
    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
        var headers = new RecordHeaders();
        var traceId = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        headers.add(new RecordHeader("trace-id", traceId));
        var record = new ProducerRecord<String, String>(topic, null, null, message, headers);
        return producer.send(record);
    }
}
```

## Task 9.1

You're not expected to implement a `Callback` handler by yourself. Just pass the handler along with the call to `send´.

Possible solution:

```java
public Future<RecordMetadata> publish(String topic, String key, String message, Callback onCompletion) {
    try (var producer = new KafkaProducer<String, String>(configuration(bootstrapServers))) {
        var record = new ProducerRecord<String, String>(topic, key, message);
        return producer.send(record, onCompletion);
    }
}
```

## Task 9.2

You don't need to change anything in the `Producer` class.

Change the name of the topic in the test.

Possible solution:

`:` is an invalid character inside a topic name. If you change the name of the topic to `invalid:topic:name` for example, then the producer will throw an `InvalidTopicException`.

## Task 9.3

Consult the JavaDoc of the `Callback` class.

Consult the JavaDoc of the `ProducerConfig.RETRIES_CONFIG` attribute.

## Task 9.4

What is the default value of the `retries` producer configuration parameter (cf. `ProducerConfig.RETRIES_CONFIG`)?

Even in case of a retryable exception, the `KafkaProducer` will throw the resp. exception not before retries are exhausted. Given the default value of `retries` parameter, this could take quite some time!

Change the value of the `retries` parameter to a lower value (might be as low as 1) and see if this changes anything wrt. the test.

Possible solution:

```java
var producerConfig = Map.of(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
    ProducerConfig.RETRIES_CONFIG, 1);
```