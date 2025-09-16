package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionRequest;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.CorporateActionInstructionAdapter;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Slf4j
public class CorporateActionInstructionController {

    private final CorporateActionInstructionAdapter instructionService;

    @PostMapping("/instructions")
    public ResponseEntity<?> postCorporateActionInstruction(
            @Valid @RequestBody CorporateActionInstructionRequest instructionRequest) throws JsonProcessingException {

        log.info("POST /instructions with request: {}", instructionRequest);
        instructionService.processInstruction(instructionRequest);
        return ResponseEntity.status(201).build();
    }
}
