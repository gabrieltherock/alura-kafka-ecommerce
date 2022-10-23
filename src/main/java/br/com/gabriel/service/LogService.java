package br.com.gabriel.service;

import br.com.gabriel.kafka.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {
        var logService = new LogService();
        try (var kafkaService = new KafkaService(LogService.class.getSimpleName(), Pattern.compile("ECOMMERCE.*"), logService::parse)) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, String> consumerRecord) {
        System.out.println("----------------------------------------------");
        System.out.println("Logando tudo...");
        System.out.printf("TOPIC --> %s%n", consumerRecord.topic());
        System.out.println("KEY --> " + consumerRecord.key());
        System.out.println("VALUE --> " + consumerRecord.value());
        System.out.println("PARTITION --> " + consumerRecord.partition());
        System.out.println("OFFSET --> " + consumerRecord.offset());
        System.out.printf("DATA --> %s%n", LocalDateTime.ofInstant(Instant.ofEpochMilli(consumerRecord.timestamp()), ZoneId.systemDefault()));
        System.out.println("--------------------------------------------");
    }
}
