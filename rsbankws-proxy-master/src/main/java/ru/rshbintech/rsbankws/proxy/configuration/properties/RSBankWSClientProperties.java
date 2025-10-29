package ru.rshbintech.rsbankws.proxy.configuration.properties;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.rs-bank-ws-client")
public class RSBankWSClientProperties {

    /**
     * URL для обращения к сервису RSBankWS
     */
    @NotEmpty
    private String url;
    /**
     * Таймаут подключения к сервису RSBankWS в секундах
     */
    private int connectTimeout = 5;
    /**
     * Таймаут ожидания ответа от сервиса RSBankWS в секундах
     */
    private int readTimeout = 60;
    /**
     * Количество одновременно открытых сетевых соединений, которые могут быть доступны в клиенте RSBankWS.
     * Вычисляется как: количество_запросов_на_RSBankWS_в_секунду * среднее_время_обработки_одного_запроса * 1.5,
     * где величина = 1.5 - это полуторный запас.
     */
    @NotNull
    private Integer maxConnections = 10_000;

}
