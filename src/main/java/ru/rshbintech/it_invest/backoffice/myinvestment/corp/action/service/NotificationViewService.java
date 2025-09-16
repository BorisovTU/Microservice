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

import java.util.List;
import java.util.Objects;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationViewService {
    private final CorporateActionNotificationDao corporateActionNotificationDao;
    private final ObjectMapper objectMapper;
    public CorporateActionNotificationDto getCorporateActionById(String corporateActionIssuerId, String cftid) throws JsonProcessingException {
        String payload = corporateActionNotificationDao.getByCaIdAndCftId(corporateActionIssuerId, cftid);
        return objectMapper.readValue(payload, CorporateActionNotificationDto.class);
    }

    public CorporateActionResponse getCorporateActions(String cftid, Boolean active, Integer limit, String sort, String from) {
        List<ViewCANotification> list = corporateActionNotificationDao.findAllBy(cftid, active, limit + 1, sort, from);
        boolean hasNextPage = list.size() > limit;
        List<CorporateActionNotificationDto> data = list.stream().map(ViewCANotification::getPayload)
                .map(payload -> {
                    try {
                        return objectMapper.readValue(payload, CorporateActionNotificationDto.class);
                    } catch (JsonProcessingException e) {
                        log.error("Failed to parse corporate action payload", e);
                        return null;
                    }
                }).toList();
        CorporateActionResponse corporateActionResponse = new CorporateActionResponse();
        corporateActionResponse.setData(data);
        if (hasNextPage) {
            Long nextId = list.get(list.size() - 1).getCaid();
            corporateActionResponse.setNextId(Objects.toString(nextId.toString()));
        }
        return corporateActionResponse;
    }
}
