package br.com.gabriel;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class Order {

    private final String userId;
    private final String orderId;
    private final BigDecimal value;
}
