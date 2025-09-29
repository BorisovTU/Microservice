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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.exception.FlkException;
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
    private final InstructionViewService instructionViewService;

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

    @Operation(summary = "Получение инструкции")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Успешная обработка",
                    content = @Content(schema = @Schema(implementation = CorporateActionViewInstruction.class))),
            @ApiResponse(responseCode = "400", description = "Ошибка валидации",
                    content = @Content(schema = @Schema(implementation = ValidationErrorResponseDto.class))),
            @ApiResponse(responseCode = "500", description = "Внутренняя ошибка сервера",
                    content = @Content(schema = @Schema(implementation = ErrorResponseDto.class)))
    })
    @GetMapping("/instructions/{instrNmb}")
    public ResponseEntity<CorporateActionViewInstruction> getCorporateActionInstruction(
            @PathVariable("instrNmb") String instrNmb) {

        log.info("GET /instructions/{}", instrNmb);
        Optional<CorporateActionViewInstruction> instruction = instructionViewService.getInstructionByNumber(instrNmb);

        return instruction.map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @Operation(summary = "Получение инструкций")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Успешная обработка",
                    content = @Content(schema = @Schema(implementation = CorporateActionInstructionResponse.class))),
            @ApiResponse(responseCode = "400", description = "Ошибка валидации",
                    content = @Content(schema = @Schema(implementation = ValidationErrorResponseDto.class))),
            @ApiResponse(responseCode = "500", description = "Внутренняя ошибка сервера",
                    content = @Content(schema = @Schema(implementation = ErrorResponseDto.class)))
    })
    @GetMapping("/instructions")
    public ResponseEntity<CorporateActionInstructionResponse> getCorporateActionInstructions(
            @RequestParam(value = "cftid") String cftid,
            @RequestParam(value = "limit", defaultValue = "50") Integer limit,
            @RequestParam(value = "sort", defaultValue = "InstrDt") InstructionSortType sort,
            @RequestParam(value = "nextid", required = false) String nextid) {

        log.info("GET /instructions with cftid: {}, limit: {}, sort: {}, nextid: {}",
                cftid, limit, sort, nextid);

        InstructionViewService.PaginatedInstructionsResult result =
                instructionViewService.getPaginatedInstructions(null, limit, cftid, sort.getValue(), nextid);

        CorporateActionInstructionResponse response = new CorporateActionInstructionResponse();
        response.setData(result.getInstructions());
        response.setNextId(result.getNextId());

        return ResponseEntity.ok(response);
    }

    @ExceptionHandler(FlkException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ValidationErrorResponseDto handleValidationExceptions(FlkException ex) {
        log.error("MethodArgumentNotValidException", ex);
        return new ValidationErrorResponseDto(ex.getMessage(), ex.getCode());
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ValidationErrorResponseDto handleMissingParams(MissingServletRequestParameterException ex) {
        String parameterName = ex.getParameterName();
        String errorMessage = String.format("Обязательный параметр '%s' отсутствует в запросе", parameterName);
        log.error("MissingServletRequestParameterException: {}", errorMessage);
        return new ValidationErrorResponseDto(errorMessage, "MISSING_PARAMETER");
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ValidationErrorResponseDto handleValidationExceptions(MethodArgumentNotValidException ex) {
        List<String> errors = ex.getBindingResult().getFieldErrors()
                .stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .toList();

        String errorMessage = String.join("; ", errors);
        log.error("MethodArgumentNotValidException: {}", errorMessage);
        return new ValidationErrorResponseDto(errorMessage, "VALIDATION_ERROR");
    }
    // Вспомогательный класс для ответа со списком инструкций
    @lombok.Data
    public static class CorporateActionInstructionResponse {
        private List<CorporateActionViewInstruction> data;
        private String nextId;
    }
}
