package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionRequest;
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

    @PostMapping("/instructions")
    public ResponseEntity<?> postCorporateActionInstruction(
            @Valid @RequestBody CorporateActionInstructionRequest instructionRequest) throws JsonProcessingException {

        log.info("POST /instructions with request: {}", instructionRequest);
        instructionService.processInstruction(instructionRequest);
        return ResponseEntity.status(201).build();
    }

    private final InstructionViewService instructionViewService;

    @GetMapping("/instructions/{instrNmb}")
    public ResponseEntity<CorporateActionInstruction> getCorporateActionInstruction(
            @PathVariable("instrNmb") String instrNmb) {

        log.info("GET /instructions/{}", instrNmb);
        Optional<CorporateActionInstruction> instruction = instructionViewService.getInstructionByNumber(instrNmb);

        return instruction.map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/instructions")
    public ResponseEntity<?> getCorporateActionInstructions(
            @RequestParam(value = "cftid", required = false) String cftid,
            @RequestParam(value = "limit", defaultValue = "50") Integer limit,
            @RequestParam(value = "sort", defaultValue = "InstrDt") String sort,
            @RequestParam(value = "nextid", required = false) String nextid) {

        log.info("GET /instructions with cftid: {}, limit: {}, sort: {}, nextid: {}",
                cftid, limit, sort, nextid);

        // Параметр cftid пока не используется, так как в таблице нет этого поля
        List<CorporateActionInstruction> instructions = instructionViewService.getInstructions(null, limit, nextid);
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
