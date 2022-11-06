package br.com.gabriel;

import lombok.Data;

import java.util.UUID;

@Data
public class CorrelationId {

    private final String id;

    CorrelationId() {
        id = UUID.randomUUID().toString();
    }
}
