package br.com.gabriel;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class LogService {

    public static void main(String[] args) {
        var logService = new LogService();
        var kafkaService = new KafkaService(LogService.class.getSimpleName(), "ECOMMERCE_SEND_EMAIL", logService::parse);

        kafkaService.run();
    }

    private void parse(ConsumerRecord<String, String> consumerRecord) {
        System.out.println("----------------------------------------------");
        System.out.println("Logando tudo...");
        System.out.println("KEY --> " + consumerRecord.key());
        System.out.println("VALUE --> " + consumerRecord.value());
        System.out.println("PARTITION --> " + consumerRecord.partition());
        System.out.println("OFFSET --> " + consumerRecord.offset());
        System.out.println("--------------------------------------------");
    }
}
