package br.com.gabriel;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Email {

    private final String subject;
    private final String body;
}
