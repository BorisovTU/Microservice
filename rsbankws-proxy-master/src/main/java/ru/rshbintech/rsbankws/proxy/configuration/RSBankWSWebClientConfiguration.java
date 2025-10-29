package ru.rshbintech.rsbankws.proxy.configuration;

import io.netty.handler.timeout.ReadTimeoutHandler;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.lang.NonNull;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import ru.rshbintech.rsbankws.proxy.configuration.properties.RSBankWSClientProperties;

import java.time.Duration;

import static io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSProxyConstants.SOAP_CONTENT_TYPE;

@Configuration
@SuppressWarnings("unused")
@EnableConfigurationProperties(RSBankWSClientProperties.class)
public class RSBankWSWebClientConfiguration {

    @Bean
    public WebClient rsBankWSWebClient(@NonNull WebClient.Builder webClientBuilder,
                                       @NonNull RSBankWSClientProperties rsBankWSClientProperties) {
        final HttpClient httpClient = HttpClient.create(
                        ConnectionProvider.create(
                                "customConnectionProvider",
                                rsBankWSClientProperties.getMaxConnections()
                        )
                )
                .option(
                        CONNECT_TIMEOUT_MILLIS,
                        ((Long) Duration.ofSeconds(rsBankWSClientProperties.getConnectTimeout()).toMillis()).intValue()
                )
                .doOnConnected(connection ->
                        connection.addHandlerLast(
                                new ReadTimeoutHandler(rsBankWSClientProperties.getReadTimeout(), SECONDS)
                        )
                );
        return webClientBuilder
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                /*
                На вызываемый SOAP сервис нужно отдать 2 заголовка:
                1. Content-Type.
                2. Content-Length.
                Заголовок Content-Length будет установлен автоматически Spring'овым RestClient (а именно, посредством
                BufferingClientHttpRequestFactory), поэтому нужно вручную установить только заголовок Content-Type.
                 */
                .defaultHeader(CONTENT_TYPE, SOAP_CONTENT_TYPE)
                .build();
    }

}
