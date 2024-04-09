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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static org.assertj.core.api.Assertions.assertThat;

class ProducerTest {

    private EmbeddedKafkaCluster kafka;
    private Producer sut;

    @BeforeEach
    void prepareTest() {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();
        kafka.createTopic(withName("producer-test")
                .withNumberOfPartitions(3)
                .build());
        sut = new Producer(kafka.getBrokerList());
    }

    @AfterEach
    void tearDown() {
        kafka.stop();
    }

    @Test
    @DisplayName("Task 2: Publish a message without a key")
    void task_2() throws Exception {

        var message = UUID.randomUUID().toString();

        sut.publish("producer-test", message);

        var observation = ObserveKeyValues.on("producer-test", 1)
                .observeFor(10, TimeUnit.SECONDS)
                .build();
        var consumedMessages = kafka.observeValues(observation);

        assertThat(consumedMessages).hasSize(1);
        assertThat(consumedMessages).contains(message);
    }

    @Test
    @DisplayName("Task 3: Publish multiple messages without a key")
    void task_3() throws Exception {

        var howManyMessages = 10;

        for (int i = 0; i < howManyMessages; i++) {
            sut.publish("producer-test", String.format("message #%s", i));
        }

        var observation = ObserveKeyValues.on("producer-test", howManyMessages)
                .observeFor(10, TimeUnit.SECONDS)
                .includeMetadata()
                .build();
        var consumedMessages = kafka.observe(observation);

        var partitions = consumedMessages.stream()
                .flatMap(kv -> kv.getMetadata().stream())
                .map(KeyValueMetadata::getPartition)
                .collect(Collectors.toSet());

        assertThat(partitions).contains(0, 1, 2);
    }
}
