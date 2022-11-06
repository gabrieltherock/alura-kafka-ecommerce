package br.com.gabriel;

import lombok.Data;

import java.util.UUID;

@Data
public class CorrelationId {

    private final String id;

    CorrelationId(String name) {
        id = name + "[" + UUID.randomUUID() + "]";
    }

    public CorrelationId continueWith(String name) {
        return new CorrelationId(id + "-" + name);
    }
}
