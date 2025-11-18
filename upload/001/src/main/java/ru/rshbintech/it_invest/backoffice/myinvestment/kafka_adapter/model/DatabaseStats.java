package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class DatabaseStats {
    private List<DatabaseInfo> databases;

    @Data
    @AllArgsConstructor
    public static class DatabaseInfo {
        private String name;
        private String type;
        private boolean available;
        private boolean pollingEnabled;
        private Long pollingInterval;
    }
}
