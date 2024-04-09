package workshop.kafka;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValueMetadata;
import net.mguenther.kafka.junit.ObserveKeyValues;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static org.assertj.core.api.Assertions.assertThat;

public class AdvancedProducerTest {

    private EmbeddedKafkaCluster kafka;
    private AdvancedProducer sut;

    @BeforeEach
    void prepareTest() {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();
        kafka.createTopic(withName("producer-test")
                .withNumberOfPartitions(3)
                .build());
        sut = new AdvancedProducer(kafka.getBrokerList());
    }

    @AfterEach
    void tearDown() {
        kafka.stop();
    }

    @Test
    @DisplayName("Task 7: Publish multiple messages with a key")
    void task_7() throws Exception {

        var howManyMessages = 10;
        var partitionsWrittenTo = new HashSet<Integer>();

        for (int i = 0; i < howManyMessages; i++) {
            partitionsWrittenTo.add(
                    sut
                            .publish("producer-test", "my-key", String.format("message #%s", i))
                            .get(1, TimeUnit.SECONDS)
                            .partition());
        }

        assertThat(partitionsWrittenTo).hasSize(1);

        var observation = ObserveKeyValues.on("producer-test", howManyMessages)
                .observeFor(10, TimeUnit.SECONDS)
                .includeMetadata()
                .build();
        var consumedMessages = kafka.observe(observation);

        var partitions = consumedMessages.stream()
                .flatMap(kv -> kv.getMetadata().stream())
                .map(KeyValueMetadata::getPartition)
                .collect(Collectors.toSet());

        assertThat(partitions).containsAll(partitionsWrittenTo);
    }

    @Test
    @DisplayName("Task 8: Add a trace ID to the metadata of the record")
    void task_8() throws Exception {

        sut.publish("producer-test", "my-key", "abc");

        var observation = ObserveKeyValues.on("producer-test", 1)
                .observeFor(10, TimeUnit.SECONDS)
                .filterOnHeaders(record -> record.lastHeader("trace-id") != null)
                .includeMetadata()
                .build();
        var consumedMessages = kafka.observe(observation);
        var traceId = new String(consumedMessages.get(0).getHeaders().lastHeader("trace-id").value());

        assertThat(traceId).isNotEmpty();
    }

    @Test
    @DisplayName("Task 9.1: Provide the means to use callback handlers")
    void task_9_1() throws Exception {

        var writtenToPartition = new ArrayList<Integer>(1);
        var writtenToTopic = new ArrayList<String>(1);
        var latch = new CountDownLatch(1);

        sut.publish("producer-test", "my-key", "my-payload", (metadata, exception) -> {

            writtenToPartition.add(metadata.partition());
            writtenToTopic.add(metadata.topic());

            latch.countDown();
        });

        latch.await(5, TimeUnit.SECONDS);

        var observation = ObserveKeyValues.on("producer-test", 1)
                .observeFor(10, TimeUnit.SECONDS)
                .includeMetadata()
                .build();
        var consumedMessages = kafka.observe(observation);

        assertThat(consumedMessages).hasSize(1);
        assertThat(consumedMessages.get(0).getMetadata().map(KeyValueMetadata::getPartition).orElse(-1)).isIn(writtenToPartition);
        assertThat(consumedMessages.get(0).getMetadata().map(KeyValueMetadata::getTopic).orElse(StringUtils.EMPTY)).isIn(writtenToTopic);
    }

    @Test
    @DisplayName("Task 9.2: Simulate an error by providing an incorrect topic name")
    void task_9_2() throws Exception {

        var caughtExceptions = new ArrayList<Exception>(1);
        var latch = new CountDownLatch(1);

        sut.publish("supposed-to-be-invalid", "my-key", "my-payload", (metadata, exception) -> {

            if (exception != null) {
                caughtExceptions.add(exception);

                System.out.println("Caught exception of type '" + exception.getClass() + "' with message '" + exception.getMessage() + "'.");
            }

            latch.countDown();
        });

        latch.await(5, TimeUnit.SECONDS);

        assertThat(caughtExceptions).hasSize(1);
        assertThat(caughtExceptions.get(0)).isInstanceOf(InvalidTopicException.class);
    }
}
