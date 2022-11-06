package br.com.gabriel;

import br.com.gabriel.consumer.KafkaService;
import br.com.gabriel.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EmailNewOrderService {

    private final KafkaDispatcher<String> emailKafkaDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var fraudDetectorService = new EmailNewOrderService();
        try (var kafkaService = new KafkaService<>(EmailNewOrderService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                Map.of())) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> consumerRecord) throws ExecutionException, InterruptedException {
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


}
