package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.CorporateActionInstructionAdapter;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.InstructionViewService;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Slf4j
public class CorporateActionInstructionController {

    private final CorporateActionInstructionAdapter instructionService;

    @Operation(summary = "Добавление инструкции")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Успешно добавлена"),
            @ApiResponse(responseCode = "400", description = "Ошибка валидации",
                    content = @Content(schema = @Schema(implementation = ValidationErrorResponseDto.class))),
            @ApiResponse(responseCode = "500", description = "Внутренняя ошибка сервера",
                    content = @Content(schema = @Schema(implementation = ErrorResponseDto.class)))
    })
    @PostMapping("/instructions")
    public ResponseEntity<?> postCorporateActionInstruction(
            @Valid @RequestBody CorporateActionInstructionRequest instructionRequest) throws JsonProcessingException {

        log.info("POST /instructions with request: {}", instructionRequest);
        instructionService.processInstruction(instructionRequest);
        return ResponseEntity.status(201).build();
    }

    private final InstructionViewService instructionViewService;

    @Operation(summary = "Получение инструкции")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Успешная обработка",
                    content = @Content(schema = @Schema(implementation = CorporateActionInstruction.class))),
            @ApiResponse(responseCode = "400", description = "Ошибка валидации",
                    content = @Content(schema = @Schema(implementation = ValidationErrorResponseDto.class))),
            @ApiResponse(responseCode = "500", description = "Внутренняя ошибка сервера",
                    content = @Content(schema = @Schema(implementation = ErrorResponseDto.class)))
    })
    @GetMapping("/instructions/{instrNmb}")
    public ResponseEntity<CorporateActionInstruction> getCorporateActionInstruction(
            @PathVariable("instrNmb") String instrNmb) {

        log.info("GET /instructions/{}", instrNmb);
        Optional<CorporateActionInstruction> instruction = instructionViewService.getInstructionByNumber(instrNmb);

        return instruction.map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @Operation(summary = "Получение инструкций")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Успешная обработка",
                    content = @Content(schema = @Schema(implementation = CorporateActionInstruction.class))),
            @ApiResponse(responseCode = "400", description = "Ошибка валидации",
                    content = @Content(schema = @Schema(implementation = ValidationErrorResponseDto.class))),
            @ApiResponse(responseCode = "500", description = "Внутренняя ошибка сервера",
                    content = @Content(schema = @Schema(implementation = ErrorResponseDto.class)))
    })
    @GetMapping("/instructions")
    public ResponseEntity<?> getCorporateActionInstructions(
            @RequestParam(value = "cftid", required = false) String cftid,
            @RequestParam(value = "limit", defaultValue = "50") Integer limit,
            @RequestParam(value = "sort", defaultValue = "InstrDt") String sort,
            @RequestParam(value = "nextid", required = false) String nextid) {

        log.info("GET /instructions with cftid: {}, limit: {}, sort: {}, nextid: {}",
                cftid, limit, sort, nextid);

        // Параметр cftid пока не используется, так как в таблице нет этого поля
        List<CorporateActionInstruction> instructions = instructionViewService.getInstructions(null, limit, cftid, nextid);
        String nextId = instructionViewService.getNextId(instructions, limit);

        // Создаем ответ согласно спецификации API
        CorporateActionInstructionResponse response = new CorporateActionInstructionResponse();
        response.setData(instructions);
        response.setNextId(nextId);

        return ResponseEntity.ok(response);
    }

    // Вспомогательный класс для ответа со списком инструкций
    @lombok.Data
    public static class CorporateActionInstructionResponse {
        private List<CorporateActionInstruction> data;
        private String nextId;
    }

}
