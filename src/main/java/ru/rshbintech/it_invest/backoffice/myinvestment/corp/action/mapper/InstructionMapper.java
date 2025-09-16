package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper;

import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionRequest;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotification;
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
        result.setBnfclOwnrDtls(bnfclOwnrDtlsShort);
        result.setBal(instruction.getBal());
        result.setInstrDt(instruction.getInstrDt());
        result.setInstrNmb(instruction.getInstrNmb());
        result.setCorporateActionNotification(corpActionNotificationShort);
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
        corpActionNotificationShort.setFinInstrmId(notification.getFinInstrmId());
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
