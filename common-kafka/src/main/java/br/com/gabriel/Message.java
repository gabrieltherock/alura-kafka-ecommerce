package br.com.gabriel;

import lombok.Data;

@Data
public class Message<T> {

    private final CorrelationId id;
    private final T payload;

    Message(CorrelationId id, T payload) {
        this.id = id;
        this.payload = payload;
    }
}
