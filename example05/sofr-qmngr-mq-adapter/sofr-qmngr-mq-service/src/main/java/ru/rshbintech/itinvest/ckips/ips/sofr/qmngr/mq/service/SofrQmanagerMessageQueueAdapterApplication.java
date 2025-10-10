package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.service;

import static java.util.Locale.ENGLISH;

import java.util.Locale;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import ru.rshbintech.itinvest.ckips.ips.auditserviceclient.annotation.EnableAuditClientConfig;

/**
 * Основной класс, запускающий микросервис.
 */
@EnableKafka
@EnableAuditClientConfig
@EnableTransactionManagement
@EnableScheduling
@SpringBootApplication(scanBasePackages = "ru.rshbintech.itinvest.ckips.ips.sofr.qmngr")
public class SofrQmanagerMessageQueueAdapterApplication {

  public static void main(String[] args) {
    Locale.setDefault(ENGLISH);
    SpringApplication.run(SofrQmanagerMessageQueueAdapterApplication.class, args);
  }

}
