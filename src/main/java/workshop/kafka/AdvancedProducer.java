package workshop.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class AdvancedProducer {

    public AdvancedProducer(String bootstrapServers) {
    }

    // TODO: Your implementation for task #7 and #8 goes here
    public Future<RecordMetadata> publish(String topic, String key, String message) {
        return null;
    }

    // TODO: Your implementation for task #9 goes here
    public Future<RecordMetadata> publish(String topic, String key, String message, Callback onCompletion) {
        return null;
    }
}
