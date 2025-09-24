package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.CorporateActionNotificationDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotificationDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionResponse;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCANotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.exception.FlkException;

import java.util.List;
import java.util.Objects;

import static ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util.ParseUtil.parseLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationViewService {
    private final CorporateActionNotificationDao corporateActionNotificationDao;
    private final ObjectMapper objectMapper;

    private static Long getRequiredLong(String cftid, String message) {
        Long cft = parseLong(cftid, message + ":{}");
        if (Objects.isNull(cft)) {
            throw new FlkException("WRONG_ARGUMENT_FORMAT", message);
        }
        return cft;
    }

    public CorporateActionNotification getCorporateActionById(String corporateActionIssuerId, String cftid) throws JsonProcessingException {
        Long caid = getRequiredLong(corporateActionIssuerId, "corporateActionIssuerId is not valid");
        Long cft = getRequiredLong(cftid, "cft is not valid");
        String payload = corporateActionNotificationDao.getByCaIdAndCftId(caid, cft);
        if (payload == null) {
            throw new FlkException("ENTITY_NOT_FOUND", "Запись не найдена");
        }
        return objectMapper.readValue(payload, CorporateActionNotificationDto.class).getCorporateActionNotification();
    }

    public CorporateActionResponse getCorporateActions(String cft, Boolean active, Integer limit, String sort, String from) {
        Long cftid = getRequiredLong(cft, "cftid is not valid");
        List<ViewCANotification> list = corporateActionNotificationDao.findAllBy(cftid, active, limit + 1, sort, from);
        boolean hasNextPage = list.size() > limit;
        List<CorporateActionNotification> data = list.stream().map(ViewCANotification::getPayload)
                .map(payload -> {
                    try {
                        return objectMapper.readValue(payload, CorporateActionNotificationDto.class).getCorporateActionNotification();
                    } catch (JsonProcessingException e) {
                        log.error("Failed to parse corporate action payload", e);
                        return null;
                    }
                }).toList();
        CorporateActionResponse corporateActionResponse = new CorporateActionResponse();
        corporateActionResponse.setData(data);
        if (hasNextPage) {
            Long nextId = list.get(list.size() - 1).getCaid();
            corporateActionResponse.setNextId(nextId.toString());
        }
        return corporateActionResponse;
    }
}
