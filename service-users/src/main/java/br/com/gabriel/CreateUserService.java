package br.com.gabriel;

import br.com.gabriel.consumer.ConsumerService;
import br.com.gabriel.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class CreateUserService implements ConsumerService<Order> {

    private final Connection connection;

    CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:users_database.db";
        connection = DriverManager.getConnection(url);
        try (var statemanent = connection.createStatement()) {
            statemanent.execute("create table Users (uuid varchar(200) primary key, email varchar(200))");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new ServiceRunner<>(CreateUserService::new).start(5);
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> consumerRecord) throws SQLException {
        System.out.println("---------------------------------------------");
        System.out.println("Processando um novo pedido... Verificando usuário");
        System.out.println("KEY --> " + consumerRecord.key());
        System.out.println("VALUE --> " + consumerRecord.value());
        System.out.println("PARTITION --> " + consumerRecord.partition());
        System.out.println("OFFSET --> " + consumerRecord.offset());

        var order = consumerRecord.value().getPayload();
        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getUserId(), order.getEmail());
        }
        System.out.println("---------------------------------------------");
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    private void insertNewUser(String uuid, String email) throws SQLException {
        try (var insert = connection.prepareStatement("insert into Users (uuid, email) values (?, ?)")) {
            insert.setString(1, uuid);
            insert.setString(2, email);
            insert.execute();
        }

        System.out.printf("Usuário [%s] adicionado!%n", email);
    }

    private boolean isNewUser(String email) throws SQLException {
        try (var select = connection.prepareStatement("select uuid from Users u where u.email = ? limit 1")) {
            select.setString(1, email);
            var results = select.executeQuery();

            return !results.next();
        }
    }

}