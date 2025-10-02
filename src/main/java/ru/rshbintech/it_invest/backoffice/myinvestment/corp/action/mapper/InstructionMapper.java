package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper;

import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.exception.FlkException;

import java.util.UUID;

@Component
public class InstructionMapper {
    private static CorporateActionNotification.CorpActnOptnDtls getCorpActionOptDetailByOptNb(CorporateActionNotification notification, String optnNb) {
        return notification.getCorpActnOptnDtls().stream()
                .filter(corpActnOptnDtls -> corpActnOptnDtls.getOptnNb().equals(optnNb))
                .findFirst()
                .orElseThrow(() -> new FlkException("OptionNb_NOT_AVAILABLE", "Option Nb не найден " + optnNb));
    }

    private static CorporateActionViewInstruction.CorporateActionNotificationShort mapCorporateActionNotificationShort(CorporateActionNotification notification) {
        CorporateActionViewInstruction.CorporateActionNotificationShort corpActionNotificationShort = new CorporateActionViewInstruction.CorporateActionNotificationShort();
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

    private static CorporateActionViewInstruction.BnfclOwnrDtlsShort mapBnfclOwnrDtlsShort(CorporateActionNotification notification, String ownerSecurityID) {
        CorporateActionViewInstruction.BnfclOwnrDtlsShort bnfclOwnrDtlsShort = notification.getBnfclOwnrDtls().stream().filter(bnfclOwnrDtls -> bnfclOwnrDtls.getOwnerSecurityID().equals(ownerSecurityID))
                .findFirst()
                .map(notifBnfclOwnrDtls -> {
                    CorporateActionViewInstruction.BnfclOwnrDtlsShort result = new CorporateActionViewInstruction.BnfclOwnrDtlsShort();
                    result.setOwnerSecurityID(notifBnfclOwnrDtls.getOwnerSecurityID());
                    result.setAcct(notifBnfclOwnrDtls.getAcct());
                    result.setCftid(notifBnfclOwnrDtls.getCftid());
                    result.setSubAcct(notifBnfclOwnrDtls.getSubAcct());
                    return result;
                })
                .orElseThrow(() -> new FlkException("OwnerSecurityId_NOT_AVAILABLE", "Owner Security Id не  найден " + ownerSecurityID));
        return bnfclOwnrDtlsShort;
    }

    public CorporateActionViewInstruction mapToInstructionView(CorporateActionInstructionRequest instruction,
                                                               CorporateActionNotification notification,
                                                               InsgtructionBalanceStatus status) {
        String ownerSecurityID = instruction.getBnfclOwnrDtls().getOwnerSecurityID();
        String optnNb = instruction.getCorpActnOptnDtls().getOptnNb();
        CorporateActionNotification.CorpActnOptnDtls optionDetails = getCorpActionOptDetailByOptNb(notification, optnNb);

        CorporateActionViewInstruction.CorporateActionNotificationShort corpActionNotificationShort = mapCorporateActionNotificationShort(notification);
        CorporateActionViewInstruction.BnfclOwnrDtlsShort bnfclOwnrDtlsShort = mapBnfclOwnrDtlsShort(notification, ownerSecurityID);

        CorporateActionViewInstruction result = new CorporateActionViewInstruction();
        CorporateActionViewInstruction.CorpActnOptnDtlsRequest corpActnOptnDtls = new CorporateActionViewInstruction.CorpActnOptnDtlsRequest();
        corpActnOptnDtls.setOptnNb(optnNb);
        if (optionDetails != null) {
            corpActnOptnDtls.setPricValCcy(optionDetails.getPricValCcy());
            corpActnOptnDtls.setPricVal(optionDetails.getPricVal());
        }
        result.setCorpActnOptnDtls(corpActnOptnDtls);
        result.setStatus(status.name());
        result.setBnfclOwnrDtls(bnfclOwnrDtlsShort);
        result.setBal(instruction.getBal());
        result.setInstrDt(instruction.getInstrDt());
        result.setInstrNmb(UUID.fromString(instruction.getInstrNmb()));
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
        CorporateActionNotification.CorpActnOptnDtls optionDetails = getCorpActionOptDetailByOptNb(notification, optnNb);

        SendCorpActionsAssignmentReq result = new SendCorpActionsAssignmentReq();
        SendCorpActionsAssignmentReq.SendCorpActionsAssignmentReqData reqData = new SendCorpActionsAssignmentReq.SendCorpActionsAssignmentReqData();
        SendCorpActionsAssignmentReq.CorporateActionInstructionData instructionData = new SendCorpActionsAssignmentReq.CorporateActionInstructionData();

        // Заполняем данные инструкции
        instructionData.setCorporateActionIssuerID(notification.getCorporateActionIssuerID());
        instructionData.setCorpActnEvtId(notification.getCorpActnEvtId());
        instructionData.setCftid(ownerDetails.getCftid());
        instructionData.setOwnerSecurityID(ownerSecurityID);
        instructionData.setInstrDt(instruction.getInstrDt());
        instructionData.setInstrNmb(UUID.fromString(instruction.getInstrNmb()));

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
}
