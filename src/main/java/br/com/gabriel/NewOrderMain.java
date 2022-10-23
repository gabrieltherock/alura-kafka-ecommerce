package br.com.gabriel;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (KafkaDispatcher kafkaDispatcher = new KafkaDispatcher()) {
            for (var i = 0; i <= 100; i++) {
                var key = UUID.randomUUID().toString();

                var newOrderValue = UUID.randomUUID().toString();
                kafkaDispatcher.send("ECOMMERCE_NEW_ORDER", key, newOrderValue);

                var emailValue = String.format("Welcome! We are processing your order. [%s]", UUID.randomUUID());
                kafkaDispatcher.send("ECOMMERCE_SEND_EMAIL", key, emailValue);
            }
        }
    }
}