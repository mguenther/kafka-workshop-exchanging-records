package workshop.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
            }
        } catch (WakeupException ignore) {
            // this exception should be ignored (see comment in stop())
        } finally {
            shutdown();
        }
    }

    void initializeConsumer() {
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
