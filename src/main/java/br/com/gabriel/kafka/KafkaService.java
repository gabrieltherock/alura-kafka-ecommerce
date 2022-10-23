package br.com.gabriel.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, T> kafkaConsumer;
    private final ConsumerFunction<T> consumerFunction;

    private boolean isOpen;

    public KafkaService(String groupId, String topic, ConsumerFunction<T> consumerFunction, Class<T> deserializeClass,
                        Map<String, String> additionalProperties) {
        this(groupId, consumerFunction, deserializeClass, additionalProperties);
        this.kafkaConsumer.subscribe(Collections.singleton(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> consumerFunction, Class<T> deserializeClass,
                        Map<String, String> additionalProperties) {
        this(groupId, consumerFunction, deserializeClass, additionalProperties);
        this.kafkaConsumer.subscribe(topic);
    }

    private KafkaService(String groupId, ConsumerFunction<T> consumerFunction, Class<T> deserializeClass,
                         Map<String, String> additionalProperties) {
        this.kafkaConsumer = new KafkaConsumer<>(properties(groupId, deserializeClass, additionalProperties));
        this.consumerFunction = consumerFunction;
        this.isOpen = true;
    }

    public void run() {
        while (isOpen) {
            var consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

            if (!consumerRecords.isEmpty()) {
                System.out.printf("Encontrei %s registro(s) e vou processá-los.%n", consumerRecords.count());
                consumerRecords.forEach(consumerFunction::consume);
            }
        }
    }

    private Properties properties(String groupId, Class<T> deserializeClass, Map<String, String> additionalProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, deserializeClass.getName());
        properties.putAll(additionalProperties);
        return properties;
    }

    @Override
    public void close() {
        isOpen = false;
        kafkaConsumer.close();
    }
}
