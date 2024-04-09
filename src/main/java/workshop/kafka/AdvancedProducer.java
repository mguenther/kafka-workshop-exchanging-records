package workshop.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

public class AdvancedProducer {

    private final String bootstrapServers;

    public AdvancedProducer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    // TODO: Your implementation for task #7 and #8 goes here
    public Future<RecordMetadata> publish(String topic, String key, String message) {
        try (var producer = new KafkaProducer<String, String>(configuration(bootstrapServers))) {
            var headers = withHeaders();
            var record = new ProducerRecord<String, String>(topic, null, key, message, headers);
            return producer.send(record);
        }
    }

    private Map<String, Object> configuration(String bootstrapServers) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.RETRIES_CONFIG, 1);
    }

    private RecordHeaders withHeaders() {
        var headers = new RecordHeaders();
        var traceId = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        headers.add(new RecordHeader("trace-id", traceId));
        return headers;
    }

    // TODO: Your implementation for task #9 goes here
    public Future<RecordMetadata> publish(String topic, String key, String message, Callback onCompletion) {
        try (var producer = new KafkaProducer<String, String>(configuration(bootstrapServers))) {
            var headers = withHeaders();
            var record = new ProducerRecord<String, String>(topic, null, key, message, headers);
            return producer.send(record, onCompletion);
        }
    }
}
