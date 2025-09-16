package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.constant.BeanConstants;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionRequest;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotificationDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCANotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper.NotificationMapper;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.ViewCANotificationRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.InstructionProcessorService;

import java.util.List;

@EnableKafka
@Component
@Slf4j
@RequiredArgsConstructor
public class InternalInstructionListener {

    private final InstructionProcessorService instructionProcessorService;

    @KafkaListener(
            groupId = "${kafka.internal-instruction.consumer.group-id}",
            topics = "${kafka.internal-instruction.topic}",
            containerFactory = BeanConstants.INTERNAL_INSTRUCTION_CONSUMER_FACTORY
    )
    @Transactional
    public void processInternalInstruction(CorporateActionInstructionRequest instructionRequest) {
        instructionProcessorService.processInstruction(instructionRequest);
    }
}
