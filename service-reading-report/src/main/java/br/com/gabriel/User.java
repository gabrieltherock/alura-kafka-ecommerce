package br.com.gabriel;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class User {

    private final String uuid;

    public String getReportValue() {
        return "target/" + uuid + "-report.txt";
    }
}
