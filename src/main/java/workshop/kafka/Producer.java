package workshop.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.concurrent.Future;

public class Producer {

    private final String bootstrapServers;

    public Producer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    // TODO: Your implementation for task #2 and task #3 goes here
    public Future<RecordMetadata> publish(String topic, String message) {
        try (var producer = new KafkaProducer<String, String>(configuration(bootstrapServers))) {
            var record = new ProducerRecord<String, String>(topic, null, null, message);
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
}
