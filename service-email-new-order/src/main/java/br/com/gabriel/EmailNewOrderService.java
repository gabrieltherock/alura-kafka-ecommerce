package br.com.gabriel;

import br.com.gabriel.consumer.ConsumerService;
import br.com.gabriel.consumer.ServiceRunner;
import br.com.gabriel.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {

    private final KafkaDispatcher<String> emailKafkaDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) {
        new ServiceRunner<>(EmailNewOrderService::new).start(1);
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> consumerRecord) throws ExecutionException, InterruptedException {
        System.out.println("---------------------------------------------");
        System.out.println("Preparando um novo email para o pedido...");
        System.out.println("VALUE --> " + consumerRecord.value());
        var message = consumerRecord.value();
        var emailCode = "Estamos processando seu pedido!";
        var order = message.getPayload();

        emailKafkaDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(),
                message.getId().continueWith(EmailNewOrderService.class.getSimpleName()), emailCode);
        System.out.println("---------------------------------------------");
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }
}
