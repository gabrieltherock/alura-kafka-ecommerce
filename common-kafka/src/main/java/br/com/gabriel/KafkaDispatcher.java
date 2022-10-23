package br.com.gabriel;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, T> kafkaProducer;

    public KafkaDispatcher() {
        kafkaProducer = new KafkaProducer<>(properties());
    }

    private Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        return properties;
    }

    public void send(String topic, String key, T value) throws ExecutionException, InterruptedException {
        var producerRecord = new ProducerRecord<>(topic, key, value);

        Callback callback = (data, e) -> {
            if (e != null) {
                e.printStackTrace();
                return;
            }
            System.out.printf("SUCESSO --> topic[%s]:::partition[%s]:::offset[%s]:::timestamp[%s]%n",
                    data.topic(), data.partition(), data.offset(), data.timestamp());
        };

        kafkaProducer.send(producerRecord, callback).get();
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }
}
