package ru.rshbintech.rsbankws.proxy;

import lombok.RequiredArgsConstructor;
import org.apache.ibatis.annotations.Mapper;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.web.reactive.config.EnableWebFlux;
import ru.rshbintech.rsbankws.proxy.configuration.properties.ProxyProperties;

@EnableWebFlux
@SpringBootApplication
@RequiredArgsConstructor
@MapperScan(
        basePackages = "ru.rshbintech.rsbankws.proxy.dao",
        annotationClass = Mapper.class,
        sqlSessionFactoryRef = "sqlSessionFactory"
)
@EnableConfigurationProperties(ProxyProperties.class)
public class RSBankWSProxyApplication {

    public static void main(String[] args) {
        SpringApplication.run(RSBankWSProxyApplication.class, args);
    }

}