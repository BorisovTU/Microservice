package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.constant.BeanConstants;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionRequest;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.InstructionProcessorService;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.InstructionViewService;

@EnableKafka
@Component
@Slf4j
@RequiredArgsConstructor
public class InternalInstructionListener {

    private final InstructionProcessorService instructionService;
    private final InstructionViewService instructionViewService;

    @KafkaListener(
            groupId = "${kafka.internal-instruction-view.consumer.group-id}",
            topics = "${kafka.internal-instruction-view.topic}",
            containerFactory = BeanConstants.INTERNAL_INSTRUCTION_VIEW_CONSUMER_FACTORY
    )
    @Transactional
    public void processInstructionView(CorporateActionInstructionRequest instructionRequest) {
        instructionViewService.postView(instructionRequest);
    }

    @KafkaListener(
            groupId = "${kafka.internal-instruction.consumer.group-id}",
            topics = "${kafka.internal-instruction.topic}",
            containerFactory = BeanConstants.INTERNAL_INSTRUCTION_VIEW_CONSUMER_FACTORY
    )
    @Transactional
    public void processInstruction(CorporateActionInstructionRequest instructionRequest) {
        instructionService.processInstruction(instructionRequest);
    }
}
