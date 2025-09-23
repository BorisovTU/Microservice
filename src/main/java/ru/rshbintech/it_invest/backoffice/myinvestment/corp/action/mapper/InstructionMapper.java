package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper;

import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionRequest;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.SendCorpActionsAssignmentReq;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.exception.FlkException;

@Component
public class InstructionMapper {
    public CorporateActionInstruction map(CorporateActionInstructionRequest instruction, CorporateActionNotification notification) {
        String ownerSecurityID = instruction.getBnfclOwnrDtls().getOwnerSecurityID();
        String optnNb = instruction.getCorpActnOptnDtls().getOptnNb();

        CorporateActionInstruction.CorporateActionNotificationShort corpActionNotificationShort = mapCorporateActionNotificationShort(notification);
        CorporateActionInstruction.BnfclOwnrDtlsShort bnfclOwnrDtlsShort = mapBnfclOwnrDtlsShort(notification, ownerSecurityID);

        CorporateActionInstruction result = new CorporateActionInstruction();
        CorporateActionInstruction.CorpActnOptnDtlsRequest corpActnOptnDtls = new CorporateActionInstruction.CorpActnOptnDtlsRequest();
        corpActnOptnDtls.setOptnNb(optnNb);
        result.setCorpActnOptnDtls(corpActnOptnDtls);
        result.setStatus("ACCEPTED");
        result.setBnfclOwnrDtls(bnfclOwnrDtlsShort);
        result.setBal(instruction.getBal());
        result.setInstrDt(instruction.getInstrDt());
        result.setInstrNmb(instruction.getInstrNmb());
        result.setCorporateActionNotification(corpActionNotificationShort);
        return result;
    }

    public SendCorpActionsAssignmentReq mapToSendCorpActionsAssignmentReq(CorporateActionInstructionRequest instruction, CorporateActionNotification notification) {
        String ownerSecurityID = instruction.getBnfclOwnrDtls().getOwnerSecurityID();
        String optnNb = instruction.getCorpActnOptnDtls().getOptnNb();

        // Находим данные владельца
        CorporateActionNotification.BnfclOwnrDtls ownerDetails = notification.getBnfclOwnrDtls().stream()
                .filter(bnfclOwnrDtls -> bnfclOwnrDtls.getOwnerSecurityID().equals(ownerSecurityID))
                .findFirst()
                .orElseThrow(() -> new FlkException("OwnerSecurityId_NOT_AVAILABLE", "Owner Security Id не найден " + ownerSecurityID));

        // Находим данные варианта корпоративного действия
        CorporateActionNotification.CorpActnOptnDtls optionDetails = notification.getCorpActnOptnDtls().stream()
                .filter(corpActnOptnDtls -> corpActnOptnDtls.getOptnNb().equals(optnNb))
                .findFirst()
                .orElseThrow(() -> new FlkException("OptionNb_NOT_AVAILABLE", "Option Nb не найден " + optnNb));

        SendCorpActionsAssignmentReq result = new SendCorpActionsAssignmentReq();
        SendCorpActionsAssignmentReq.SendCorpActionsAssignmentReqData reqData = new SendCorpActionsAssignmentReq.SendCorpActionsAssignmentReqData();
        SendCorpActionsAssignmentReq.CorporateActionInstructionData instructionData = new SendCorpActionsAssignmentReq.CorporateActionInstructionData();

        // Заполняем данные инструкции
        instructionData.setCorporateActionIssuerID(notification.getCorporateActionIssuerID());
        instructionData.setCorpActnEvtId(notification.getCorpActnEvtId());
        instructionData.setCftid(ownerDetails.getCftid());
        instructionData.setOwnerSecurityID(ownerSecurityID);
        instructionData.setInstrDt(instruction.getInstrDt());
        instructionData.setInstrNmb(instruction.getInstrNmb());

        // Заполняем финансовый инструмент
        SendCorpActionsAssignmentReq.FinInstrmIdData finInstrmIdData = new SendCorpActionsAssignmentReq.FinInstrmIdData();
        finInstrmIdData.setIsin(notification.getFinInstrmId().getIsin());
        finInstrmIdData.setRegNumber(notification.getFinInstrmId().getRegNumber());
        finInstrmIdData.setNsdr(notification.getFinInstrmId().getNsdr());
        instructionData.setFinInstrmId(finInstrmIdData);

        // Заполняем данные счета
        instructionData.setAcct(ownerDetails.getAcct());
        instructionData.setSubAcct(ownerDetails.getSubAcct());
        instructionData.setSfkpgAcct(notification.getSfkpgAcct());

        // Заполняем вариант корпоративного действия
        SendCorpActionsAssignmentReq.CorpActnOptnDtlsData corpActnOptnDtlsData = new SendCorpActionsAssignmentReq.CorpActnOptnDtlsData();
        corpActnOptnDtlsData.setOptnNb(optnNb);
        corpActnOptnDtlsData.setOptnTp(optionDetails.getOptnTp());
        corpActnOptnDtlsData.setPricVal(optionDetails.getPricVal() != null ? optionDetails.getPricVal().toString() : null);
        corpActnOptnDtlsData.setPricValCcy(optionDetails.getPricValCcy());
        corpActnOptnDtlsData.setBal(instruction.getBal().toString());
        instructionData.setCorpActnOptnDtls(corpActnOptnDtlsData);

        reqData.setCorporateActionInstruction(instructionData);
        result.setSendCorpActionsAssignmentReq(reqData);

        return result;
    }

    private static CorporateActionInstruction.CorporateActionNotificationShort mapCorporateActionNotificationShort(CorporateActionNotification notification) {
        CorporateActionInstruction.CorporateActionNotificationShort corpActionNotificationShort = new CorporateActionInstruction.CorporateActionNotificationShort();
        corpActionNotificationShort.setCorpActnEvtId(notification.getCorpActnEvtId());
        corpActionNotificationShort.setAddtlInf(notification.getAddtlInf());
        corpActionNotificationShort.setCorporateActionType(notification.getCorporateActionType());
        corpActionNotificationShort.setEvtTp(notification.getEvtTp());
        corpActionNotificationShort.setCorpActnEvtId(notification.getCorpActnEvtId());
        corpActionNotificationShort.setMndtryVlntryEvtTp(notification.getMndtryVlntryEvtTp());
        corpActionNotificationShort.setFinInstrmId(notification.getFinInstrmId());
        corpActionNotificationShort.setActnPrd(notification.getActnPrd());
        return corpActionNotificationShort;
    }

    private static CorporateActionInstruction.BnfclOwnrDtlsShort mapBnfclOwnrDtlsShort(CorporateActionNotification notification, String ownerSecurityID) {
        CorporateActionInstruction.BnfclOwnrDtlsShort bnfclOwnrDtlsShort = notification.getBnfclOwnrDtls().stream().filter(bnfclOwnrDtls -> bnfclOwnrDtls.getOwnerSecurityID().equals(ownerSecurityID))
                .findFirst()
                .map(notifBnfclOwnrDtls -> {
                    CorporateActionInstruction.BnfclOwnrDtlsShort result = new CorporateActionInstruction.BnfclOwnrDtlsShort();
                    result.setOwnerSecurityID(notifBnfclOwnrDtls.getOwnerSecurityID());
                    result.setAcct(notifBnfclOwnrDtls.getAcct());
                    result.setCftid(notifBnfclOwnrDtls.getCftid());
                    result.setSubAcct(notifBnfclOwnrDtls.getSubAcct());
                    return result;
                })
                .orElseThrow(() -> new FlkException("OwnerSecurityId_NOT_AVAILABLE", "Owner Security Id не  найден " + ownerSecurityID));
        return bnfclOwnrDtlsShort;
    }
}
