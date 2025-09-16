package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotificationDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionResponse;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.NotificationViewService;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Slf4j
public class CorporateActionController {

    private final NotificationViewService notificationViewService;

    @GetMapping("/corporate-actions/{corporateActionIssuerId}")
    public ResponseEntity<CorporateActionNotificationDto> getCorporateActionV1(
            @PathVariable("corporateActionIssuerId") String corporateActionIssuerId,
            @RequestParam("cftid") String cftid) throws JsonProcessingException {

        log.info("GET /corporate-actions/{} with cftid: {}", corporateActionIssuerId, cftid);
        CorporateActionNotificationDto notification = notificationViewService.getCorporateActionById(corporateActionIssuerId, cftid);
        return ResponseEntity.ok(notification);
    }

    @GetMapping("/corporate-actions")
    public ResponseEntity<CorporateActionResponse> getCorporateActionsV1(
            @RequestParam("cftid") String cftid,
            @RequestParam(value = "active", defaultValue = "true") Boolean active,
            @RequestParam(value = "limit", defaultValue = "50") Integer limit,
            @RequestParam(value = "sort", defaultValue = "start_date") String sort,
            @RequestParam(value = "nextid", required = false) String nextid) {

        log.info("GET /corporate-actions with cftid: {}, active: {}, limit: {}, sort: {}, nextid: {}",
                cftid, active, limit, sort, nextid);

        CorporateActionResponse response = notificationViewService.getCorporateActions(cftid, active, limit, sort, nextid);
        return ResponseEntity.ok(response);
    }
}
