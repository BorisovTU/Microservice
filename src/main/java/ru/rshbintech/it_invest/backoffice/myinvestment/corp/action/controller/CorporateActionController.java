package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.NotificationViewService;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Slf4j
public class CorporateActionController {

    private final NotificationViewService notificationViewService;

    @Operation(summary = "Получение уведомления о корпоративном действии")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Успешная обработка",
                    content = @Content(schema = @Schema(implementation = CorporateActionNotificationDto.class))),
            @ApiResponse(responseCode = "400", description = "Ошибка валидации",
                    content = @Content(schema = @Schema(implementation = ValidationErrorResponseDto.class))),
            @ApiResponse(responseCode = "500", description = "Внутренняя ошибка сервера",
                    content = @Content(schema = @Schema(implementation = ErrorResponseDto.class)))
    })
    @GetMapping("/corporate-actions/{corporateActionIssuerId}")
    public ResponseEntity<CorporateActionNotification> getCorporateActionV1(
            @PathVariable("corporateActionIssuerId") String corporateActionIssuerId,
            @RequestParam("cftid") String cftid) throws JsonProcessingException {

        log.info("GET /corporate-actions/{} with cftid: {}", corporateActionIssuerId, cftid);
        CorporateActionNotification notification = notificationViewService.getCorporateActionById(corporateActionIssuerId, cftid);
        return ResponseEntity.ok(notification);
    }

    @Operation(summary = "Получение уведомлений о корпоративном действии")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Успешная обработка",
                    content = @Content(schema = @Schema(implementation = CorporateActionNotificationDto.class))),
            @ApiResponse(responseCode = "400", description = "Ошибка валидации",
                    content = @Content(schema = @Schema(implementation = ValidationErrorResponseDto.class))),
            @ApiResponse(responseCode = "500", description = "Внутренняя ошибка сервера",
                    content = @Content(schema = @Schema(implementation = ErrorResponseDto.class)))
    })
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
