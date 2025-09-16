package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.persistence.EntityNotFoundException;
import jakarta.validation.Valid;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.support.DefaultMessageSourceResolvable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.CorporateActionService;

import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/corporate-actions")
@Slf4j
public class CorporateActionController {
    private final CorporateActionService corporateActionService;

    public CorporateActionController(CorporateActionService corporateActionService) {
        this.corporateActionService = corporateActionService;
    }

    @GetMapping
    @Operation(summary = "Получение уведомлений о корпоративном действии")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Успешная обработка",
                    content = @Content(schema = @Schema(implementation = CorporateActionInstructionResponseDTO.class))),
            @ApiResponse(responseCode = "400", description = "Ошибка валидации",
                    content = @Content(schema = @Schema(implementation = ValidationErrorResponseDto.class))),
            @ApiResponse(responseCode = "500", description = "Внутренняя ошибка сервера",
                    content = @Content(schema = @Schema(implementation = ErrorResponseDto.class)))
    })
    public CorporateActionResponse get(
            @RequestParam(required = false) Long cftid,
            @RequestParam(required = false) MandatoryEnum mndtryVlntryEvtTp,
            @RequestParam(required = false) boolean status,
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(name = "sort", defaultValue = "START_DATE", required = false) CASEnum sort,
            @RequestParam(required = false) Long nextId,
            @RequestParam(required = false) Long caid
    ) {

        CorporateActionRequestDTO requestDto = new CorporateActionRequestDTO();
        requestDto.setCaid(caid);
        requestDto.setSort(sort);
        requestDto.setStatus(status);
        if (mndtryVlntryEvtTp != null) {
            requestDto.setMndtryVlntryEvtTp(mndtryVlntryEvtTp.name());
        }
        requestDto.setCftid(cftid);
        requestDto.setNextId(nextId);
        requestDto.setLimit(limit);

        return corporateActionService.findCorporateActions(requestDto);
    }

    @PostMapping
    @Operation(summary = "Обработать инструкцию корпоративного действия")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Успешная обработка",
                    content = @Content(schema = @Schema(implementation = CorporateActionInstructionResponseDTO.class))),
            @ApiResponse(responseCode = "400", description = "Ошибка валидации",
                    content = @Content(schema = @Schema(implementation = ValidationErrorResponseDto.class))),
            @ApiResponse(responseCode = "500", description = "Внутренняя ошибка сервера",
                    content = @Content(schema = @Schema(implementation = ErrorResponseDto.class)))
    })
    public ResponseEntity<CorporateActionInstructionResponseDTO> createInstruction(
            @Valid @RequestBody CorporateActionInstructionDTO instructionDTO) {
        CorporateActionInstructionResponseDTO response = corporateActionService.processInstruction(instructionDTO);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }


    @ExceptionHandler(ValidationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponseDto handleValidationException(ValidationException ex) {
        log.error("ValidationException", ex);
        return new ErrorResponseDto(ex.getMessage(), "VALIDATION_ERROR");
    }


    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ValidationErrorResponseDto handleValidationExceptions(MethodArgumentNotValidException ex) {
        log.error("MethodArgumentNotValidException", ex);

        String errorFields = ex.getBindingResult().getFieldErrors().stream()
                .map(DefaultMessageSourceResolvable::getDefaultMessage)
                .collect(Collectors.joining("; "));

        if (!errorFields.isBlank()) {
            return new ValidationErrorResponseDto(errorFields, "MISSING_REQUIRED_FIELD");
        }

        return new ValidationErrorResponseDto("Validation failed", "VALIDATION_ERROR");
    }

//    @ExceptionHandler(MethodArgumentNotValidException.class)
//    @ResponseStatus(HttpStatus.BAD_REQUEST)
//    public ValidationErrorResponseDto handleValidationExceptions(MethodArgumentNotValidException ex) {
//        log.error("MethodArgumentNotValidException", ex);
//        String errorFields = ex.getBindingResult().getFieldErrors().stream()
//                .map(FieldError::getField)
//                .distinct()
//                .collect(Collectors.joining(", "));
//        if (errorFields.split(",").length > 1) {
//            errorFields = "Обязательные параметры: " + errorFields + " не указаны;";
//        }
//        else if (!errorFields.isBlank()) {
//            ex.printStackTrace();
//            errorFields = "Обязательный параметр: " + errorFields + " не указан";
//        }
//        return new ValidationErrorResponseDto(errorFields, "MISSING_REQUIRED_FIELD");
//    }
//
    @ExceptionHandler(EntityNotFoundException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponseDto handleEntityNotFoundException(Exception ex) {
        log.error("EntityNotFoundException", ex);
        return new ErrorResponseDto(ex.getMessage(), "INTERNAL_SERVER_ERROR");
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponseDto handleGenericException(Exception ex) {
        log.error("GenericException", ex);
        return new ErrorResponseDto(" ", "INTERNAL_SERVER_ERROR");
    }
}
