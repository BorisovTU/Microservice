package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Конфигурация Jackson.
 */
@Configuration
@SuppressWarnings("unused")
public class JacksonConfiguration {

  /**
   * Конфигурация Jackson ObjectMapper.
   *
   * @return Jackson ObjectMapper
   */
  @Bean
  public ObjectMapper objectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new ParameterNamesModule());
    objectMapper.registerModule(new JavaTimeModule());
    return objectMapper;
  }

  @Bean
  public JsonFactory jsonFactory() {
    return JsonFactory.builder()
            .enable(JsonReadFeature.ALLOW_JAVA_COMMENTS)
            .build();
  }

}
