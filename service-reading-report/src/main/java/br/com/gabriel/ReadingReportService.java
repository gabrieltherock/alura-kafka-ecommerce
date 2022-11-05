package br.com.gabriel;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class ReadingReportService {

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) {
        var readingReportService = new ReadingReportService();
        try (var kafkaService = new KafkaService<>(ReadingReportService.class.getSimpleName(),
                "USER_GENERATE_READING_REPORT",
                readingReportService::parse,
                User.class,
                Map.of())) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, User> consumerRecord) throws IOException {
        System.out.println("---------------------------------------------");
        System.out.println("Processing report for new value --> " + consumerRecord.value());

        var user = consumerRecord.value();
        var target = new File(user.getReportValue());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for user " + user.getUuid());

        System.out.println("File created --> " + target.getAbsolutePath());
    }
}
