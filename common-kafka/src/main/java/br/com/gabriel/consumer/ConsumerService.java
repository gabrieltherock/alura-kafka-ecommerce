package br.com.gabriel.consumer;

import br.com.gabriel.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerService<T> {

    void parse(ConsumerRecord<String, Message<T>> consumerRecord) throws Exception;
    String getTopic();
    String getConsumerGroup();
}
