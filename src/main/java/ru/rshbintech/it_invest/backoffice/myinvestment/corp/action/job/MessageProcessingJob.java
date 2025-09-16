package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.job;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.property.CorpActionJobProperties;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.MessageProcessingService;

@EnableScheduling
@Component
@Slf4j
@RequiredArgsConstructor
public class MessageProcessingJob {

    private final MessageProcessingService processingService;

    private final CorpActionJobProperties jobProperties;

    @Scheduled(fixedRateString = "#{@corpActionJobProperties.consumerRate}")
    @Transactional
    public void processNewMessages() {
        log.debug("Executing consumer job with rate: {}", jobProperties.getConsumerRate());
        processingService.run();
    }
}
