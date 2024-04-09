package workshop.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class Consumer implements Runnable {

    private final List<ConsumerRecord<String, String>> observedRecords;
    private final String bootstrapServers;
    private volatile boolean running = true;
    private KafkaConsumer<String, String> consumer;

    public Consumer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.observedRecords = new ArrayList<>();
    }

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
        } catch (WakeupException e) {
            // This exception should be ignored, as throwing it is the normal way to abort
            // a long call to poll in case the consumer is supposed to shut down
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            shutdown();
        }
    }

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

    void process(ConsumerRecord<String, String> record) {
        observedRecords.add(record);
    }

    public void stop() {
        running = false;
        // The consumer might be waiting on the result of a poll. By issuing an explicit wakeup,
        // the consumer aborts the long call to poll and throws a WakeUpException (cf. run method).
        // We are not supposed to process the WakeUpException - it is merely a trigger that tells
        // us that we can shut down the consumer and free up any resources that it acquired.
        consumer.wakeup();
    }

    private void shutdown() {
        consumer.commitSync(Duration.ofSeconds(5_000));
        consumer.close(Duration.ofSeconds(1_000));
    }

    public Stream<ConsumerRecord<String, String>> getObservedRecords() {
        return observedRecords.stream();
    }
}
