package br.com.gabriel.consumer;

import br.com.gabriel.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction<T> {

    void consume(ConsumerRecord<String, Message<T>> consumerRecord) throws Exception;
}
