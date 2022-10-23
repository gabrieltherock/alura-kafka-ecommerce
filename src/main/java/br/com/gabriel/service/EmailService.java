package br.com.gabriel.service;

import br.com.gabriel.Email;
import br.com.gabriel.kafka.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();
        try (var kafkaService = new KafkaService<>(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL", emailService::parse, Email.class)) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, Email> consumerRecord) {
        System.out.println("----------------------------------------------");
        System.out.println("Enviando email...");
        System.out.println("KEY --> " + consumerRecord.key());
        System.out.println("VALUE --> " + consumerRecord.value());
        System.out.println("PARTITION --> " + consumerRecord.partition());
        System.out.println("OFFSET --> " + consumerRecord.offset());
        System.out.println("----------------------------------------------");
    }
}
