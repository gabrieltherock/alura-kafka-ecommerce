package br.com.gabriel.consumer;

import br.com.gabriel.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public interface ConsumerService<T> {

    void parse(ConsumerRecord<String, Message<T>> consumerRecord) throws IOException;
    String getTopic();
    String getConsumerGroup();
}
