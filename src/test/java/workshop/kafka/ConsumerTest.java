package workshop.kafka;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.SendValues.to;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerTest {

    private EmbeddedKafkaCluster kafka;

    @BeforeEach
    void prepareTest() {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();
        kafka.createTopic(withName("consumer-test").useDefaults());
    }

    @AfterEach
    void tearDown() {
        kafka.stop();
    }

    @Test
    @DisplayName("Task 5: Consuming messages")
    void task_5() throws Exception {

        var values = List.of("1", "2", "3", "4", "5");

        kafka.send(to("consumer-test", values));

        var latch = new CountDownLatch(values.size());
        var consumer = new InstrumentingConsumer(kafka.getBrokerList(), latch);
        var consumerThread = new Thread(consumer);
        consumerThread.start();

        latch.await(10, TimeUnit.SECONDS);

        var observedValues = consumer
                .getObservedRecords()
                .map(ConsumerRecord::value)
                .collect(Collectors.toList());

        assertThat(observedValues).isEqualTo(values);

        consumer.stop();
        consumerThread.join(2_000);
    }

    public static class InstrumentingConsumer extends Consumer {

        private final CountDownLatch latch;

        public InstrumentingConsumer(final String bootstrapServers, final CountDownLatch latch) {
            super(bootstrapServers);
            this.latch = latch;
        }

        @Override
        void process(ConsumerRecord<String, String> record) {
            latch.countDown();
            super.process(record);
        }
    }
}
