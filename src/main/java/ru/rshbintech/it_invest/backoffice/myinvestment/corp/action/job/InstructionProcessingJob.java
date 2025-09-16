package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.job;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.property.CorpActionJobProperties;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.InstructionProcessingService;

@EnableScheduling
@Component
@Slf4j
@RequiredArgsConstructor
public class InstructionProcessingJob {

    private final InstructionProcessingService processingService;

    private final CorpActionJobProperties jobProperties;

    @Scheduled(fixedRateString = "#{@corpActionJobProperties.producerRate}")
    @Transactional
    public void processNewMessages() {
        log.debug("Executing producer job with rate: {}", jobProperties.getProducerRate());
        processingService.run();
    }
}
