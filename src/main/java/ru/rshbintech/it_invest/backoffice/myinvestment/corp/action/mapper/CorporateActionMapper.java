package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.CorpActionsOptDetails;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.projection.IClientInfo;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.projection.ICorporateActionProjection;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.ClientAccRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.CorpActionsOptDetailsRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util.ParseUtil;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

import static ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util.ParseUtil.parseLocalDate;
import static ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util.ParseUtil.parseLong;

@Component
@RequiredArgsConstructor
public class CorporateActionMapper {
    private final ClientAccRepository clientAccRepository;
    private final CorpActionsOptDetailsRepository corpActionsOptDetailsRepository;
    public CorporateActionResponse toResponse(
            List<ICorporateActionProjection> projections,
            String nextCursor,
            Long cftId,
            boolean status,
            int limit
    ) {
        List<CorporateActionNotificationResponseDTO> data = projections.stream()
                .limit(limit) // На случай если взяли на 1 больше
                .map(cat->toDTO(cat, cftId, status))
                .collect(Collectors.toList());

        CorporateActionResponse response = new CorporateActionResponse();
        response.setData(data);
        response.setNextId(nextCursor);
        return response;
    }

    public CorpActionsOptDetails dtoToCorpActionsOptDetails(Long caid, CorpActnOptnDtls opt) {
        CorpActionsOptDetails detail = new CorpActionsOptDetails();
        // detail.setId(UUID.randomUUID());
        detail.setCaid(caid);
        detail.setOptnNb(parseLong(opt.getOptnNb(), "Invalid OptnNb format: {}"));
        detail.setOptnTp(opt.getOptnTp());
        // Преобразуем DfltOptnInd в Boolean
        if (StringUtils.hasText(opt.getDfltOptnInd())) {
            String dflt = opt.getDfltOptnInd().trim();
            detail.setDfltOptnInd("1".equals(dflt) || "true".equalsIgnoreCase(dflt));
        }

        // Преобразуем PricVal из String в Long
        detail.setPricVal(parseLong(opt.getPricVal(), "Invalid PricVal format: {}"));
        detail.setPricValCcy(opt.getPricValCcy());

        // Обрабатываем даты из ActnPrd
        if (opt.getActnPrd() != null) {
            detail.setStartDt(parseLocalDate(opt.getActnPrd().getStartDt(), "Invalid StartDt format: {}"));
            detail.setEndDt(parseLocalDate(opt.getActnPrd().getEndDt(), "Invalid EndDt format: {}"));
        }
        return detail;
    }

    public CorporateActionNotificationResponseDTO toDTO(ICorporateActionProjection projection, Long cftId, boolean status) {
        List<IClientInfo> clientInfo = (cftId != null) ? clientAccRepository.findClientInfoByCaidAndCftid(projection.getCaid(), cftId)
                                                        : clientAccRepository.findClientInfoByCaid(projection.getCaid());
        List<CorpActionsOptDetails> caOptDetails = corpActionsOptDetailsRepository.findByCaid(projection.getCaid());
        CorporateActionNotificationResponseDTO dto = new CorporateActionNotificationResponseDTO();
        CorporateActionNotificationResponse notification =
                new CorporateActionNotificationResponse();

        // Маппинг основных полей
        notification.setCorporateActionIssuerID(String.valueOf(projection.getCaid()));
        notification.setCorporateActionType(projection.getCatype());
        notification.setCorpActnEvtId(projection.getReference());
        notification.setEvtTp(projection.getSwiftType());
        notification.setMndtryVlntryEvtTp(projection.getMaVoCode());
        notification.setRcrdDt(ParseUtil.toString(projection.getDateRegOwners()));

        notification.setOrgNm(projection.getOrgNm());
        notification.setSfkpgAcct(projection.getSfkpgAcct());

        // Маппинг финансового инструмента
        setFinInstr(projection, notification);

        notification.setAddtlInf(projection.getAddtlInf());
        notification.setLwsInPlcCd(projection.getLwsInPlcCd());
        notification.setSbrdntLwsInPlcCd(projection.getSbrdntLwsInPlcCd());

        List<BnfclOwnrDtlsResponse> listBnfclOwnrDtlResponses =
                clientInfo.stream().map(this::clientToBnfclOwnrDtls).toList();

        List<CorpActnOptnDtls> listCorpActionOptDetails = caOptDetails.stream()
                .map(this::toCorpActionOptDetails)
                .filter(detail -> {
                    return !status ||
                            detail.getActnPrd() == null ||
                            detail.getActnPrd().getEndDt() == null ||
                            LocalDate.parse(detail.getActnPrd().getEndDt()).isAfter(LocalDate.now());
                })
                .toList();
        notification.setCorpActnOptnDtls(listCorpActionOptDetails);

        notification.setBnfclOwnrDtls(listBnfclOwnrDtlResponses);
        dto.setCorporateActionNotification(notification);
        return dto;
    }

    private CorpActnOptnDtls toCorpActionOptDetails(CorpActionsOptDetails corpActionsOptDetails) {
        CorpActnOptnDtls result = new CorpActnOptnDtls();
        result.setOptnTp(corpActionsOptDetails.getOptnTp());
        result.setOptnNb(ParseUtil.toString(corpActionsOptDetails.getOptnNb()));
        result.setPricVal(ParseUtil.toString(corpActionsOptDetails.getPricVal()));
        result.setPricValCcy(corpActionsOptDetails.getPricValCcy());
        result.setDfltOptnInd(ParseUtil.toString(corpActionsOptDetails.getDfltOptnInd()));
        ActnPrd actnPrd = new ActnPrd();
        actnPrd.setEndDt(ParseUtil.toString(corpActionsOptDetails.getEndDt()));
        actnPrd.setStartDt(ParseUtil.toString(corpActionsOptDetails.getStartDt()));
        result.setActnPrd(actnPrd);
        return result;
    }

    private static void setFinInstr(ICorporateActionProjection projection, CorporateActionNotificationResponse notification) {
        FinInstrmId finInstrmId =
                new FinInstrmId();
        finInstrmId.setIsin(projection.getIsin());
        finInstrmId.setRegNumber(projection.getRegnumber());
        finInstrmId.setNsdr(projection.getNsdr());
        notification.setFinInstrmId(finInstrmId);
    }

    private static void setPeriod(ICorporateActionProjection projection, CorporateActionNotificationResponse notification) {
        if (projection.getDateStart() != null && projection.getDateEnd() != null) {
            ActnPrd actnPrd =
                    new ActnPrd();
            actnPrd.setStartDt(ParseUtil.toString(projection.getDateStart()));
            actnPrd.setEndDt(ParseUtil.toString(projection.getDateEnd()));

            CorpActnOptnDtls option =
                    new CorpActnOptnDtls();
            option.setActnPrd(actnPrd);

            notification.setCorpActnOptnDtls(List.of(option));
        }
    }

    private BnfclOwnrDtlsResponse clientToBnfclOwnrDtls(IClientInfo clientInfo) {
        BnfclOwnrDtlsResponse result = new BnfclOwnrDtlsResponse();
        if (clientInfo.getCftid() != null) {
            Client client = new Client();
            client.setObjectId(ParseUtil.toString(clientInfo.getCftid()));
            client.setSystemId("CFT");
            result.setClientId(client);
        }
        result.setOwnerSecurityID(ParseUtil.toString(clientInfo.getOwnerSecurityID()));
        result.setAcct(clientInfo.getAccDepo());
        result.setSubAcct(clientInfo.getSubAccDepo());
        result.setBal(clientInfo.getBal().toString());
        return result;
    }
}
