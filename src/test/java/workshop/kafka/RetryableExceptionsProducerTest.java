package workshop.kafka;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.time.Duration.ofSeconds;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.newClusterConfig;
import static net.mguenther.kafka.junit.EmbeddedKafkaConfig.brokers;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static org.assertj.core.api.Assertions.assertThat;
import static workshop.kafka.Wait.delay;

class RetryableExceptionsProducerTest {

    private EmbeddedKafkaCluster kafka;
    private AdvancedProducer sut;

    @BeforeEach
    void prepareTest() {
        kafka = provisionWith(newClusterConfig()
                .configure(brokers().withNumberOfBrokers(3).build())
                .build());
        kafka.start();
        sut = new AdvancedProducer(kafka.getBrokerList());
    }

    @AfterEach
    void tearDown() {
        kafka.stop();
    }

    @Test
    @DisplayName("Task 6.4: At which point does the producer see the 'NotEnoughReplicasException'?")
    void task6_4() throws Exception {

        var caughtExceptions = new ArrayList<Exception>(1);
        var latch = new CountDownLatch(1);

        kafka.createTopic(withName("retryable-exceptions-producer-test")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .with("min.insync.replicas", "2"));

        delay(ofSeconds(2));

        var disconnectedBrokers = kafka.disconnectUntilIsrFallsBelowMinimumSize("retryable-exceptions-producer-test");

        delay(ofSeconds(2));

        sut.publish("retryable-exceptions-producer-test", "my-key", "abc", ((metadata, exception) -> {
            if (exception != null) {
                caughtExceptions.add(exception);
                System.out.println("Caught exception of type '" + exception.getClass() + "' with message '" + exception.getMessage() + "'.");
            }
            latch.countDown();
        }));

        kafka.connect(disconnectedBrokers);

        latch.await(10, TimeUnit.SECONDS);

        assertThat(caughtExceptions).hasSize(1);
        assertThat(caughtExceptions.get(0)).isInstanceOf(NotEnoughReplicasException.class);
    }
}