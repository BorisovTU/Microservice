package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionRequest;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.exception.FlkException;

@Service
@RequiredArgsConstructor
public class InstructionProcessorService {
    private final KafkaTemplate<String, CorporateActionInstruction> instructionToDiasoftKafkaTemplate;
    private final CorporateActionInstructionAdapter corporateActionInstructionAdapter;

    public void processInstruction(CorporateActionInstructionRequest instructionRequest) {
        String ownerSecurityID = instructionRequest.getBnfclOwnrDtls().getOwnerSecurityID();
        CorporateActionNotification corporateActionNotification = corporateActionInstructionAdapter.getCorporateActionNotification(Long.parseLong(ownerSecurityID));
        String optnNb = instructionRequest.getCorpActnOptnDtls().getOptnNb();
        CorporateActionNotification.CorpActnOptnDtls corpActnOptnDtls = corporateActionNotification.getCorpActnOptnDtls().stream()
                .filter(dtls -> optnNb.equals(dtls.getOptnNb())).findFirst().orElseThrow(()-> new FlkException("corpActnOptnDtls_NOT_AVAILABLE","Опция не найдена " + optnNb));
        CorporateActionNotification.BnfclOwnrDtls ownerSecurityIdNotAvailable = corporateActionNotification.getBnfclOwnrDtls().stream().filter(bnfclOwnrDtls -> bnfclOwnrDtls.getOwnerSecurityID().equals(ownerSecurityID))
                .findFirst()
                .orElseThrow(() -> new FlkException("OwnerSecurityId_NOT_AVAILABLE", "Owner Security Id не  найден " + ownerSecurityID));

    }
}
