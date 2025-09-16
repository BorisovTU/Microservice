package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.SendCorpActionsAssignmentReqDTO;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ResultEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.projection.IResultResponseProjection;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.property.CorpActionJobProperties;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.property.CustomKafkaProperties;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.ResultRepository;

import java.time.format.DateTimeFormatter;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class InstructionProcessingService {
    private final CorpActionJobProperties jobProperties;
    private final ResultRepository resultRepository;
    private final KafkaTemplate<String, SendCorpActionsAssignmentReqDTO> kafkaTemplate;
    private final CustomKafkaProperties customKafkaProperties;
    @Transactional
    public void run() {
        List<ResultEntity> results = resultRepository.findReadyForResponse(jobProperties.getProducerBatchSize());

        results.forEach(result -> {
            try {
                List<IResultResponseProjection> resultEntities = resultRepository.findResultResponse(result.getRequestId());
                resultEntities.stream().map(entity -> {
                    return mapToDto(entity, result);
                }).forEach(dto -> {
                    kafkaTemplate.send(customKafkaProperties.getInstructionToDiasoftTopic(), dto);
                });
                result.setStatus(3);
            }
            catch (Exception e) {
                log.error("Result: {}, status: {}, error message:{}",result.getId(), -1, e.getMessage());
                result.setStatus(-1);
            }
        });
    }

    private SendCorpActionsAssignmentReqDTO mapToDto(IResultResponseProjection projection, ResultEntity result) {
        SendCorpActionsAssignmentReqDTO dto = new SendCorpActionsAssignmentReqDTO();

        // Создаем и заполняем объект CorporateActionInstruction
        SendCorpActionsAssignmentReqDTO.CorporateActionInstructionDTO instruction =
                new SendCorpActionsAssignmentReqDTO.CorporateActionInstructionDTO();

        instruction.setCorporateActionIssuerID(String.valueOf(projection.getCaid()));
        instruction.setCorpActnEvtId(projection.getReference());
        instruction.setCFTID(projection.getCftid());
        instruction.setOwnerSecurityID(String.valueOf(result.getClientDiaId()));

        // Форматируем дату в требуемый формат
        if (result.getInstrDt() != null) {
            instruction.setInstrDt(result.getInstrDt().format(DateTimeFormatter.ISO_INSTANT));
        }
        instruction.setInstrNmb(String.valueOf(result.getInstrNmb()));

        // Заполняем информацию о финансовом инструменте
        SendCorpActionsAssignmentReqDTO.FinInstrmIdDTO finInstrmId =
                new SendCorpActionsAssignmentReqDTO.FinInstrmIdDTO();
        finInstrmId.setISIN(projection.getIsin());
        finInstrmId.setRegNumber(projection.getRegNumber());
        finInstrmId.setNSDR(projection.getNsdr());
        instruction.setFinInstrmId(finInstrmId);

        // Заполняем информацию о счетах
        instruction.setAcct(projection.getAccDepo());
        instruction.setSubAcct(projection.getSubAccDepo());
        instruction.setSfkpgAcct(projection.getSfkpgAcct());

        // Заполняем информацию о варианте корпоративного действия
        SendCorpActionsAssignmentReqDTO.CorpActnOptnDtlsDTO optionDetails =
                new SendCorpActionsAssignmentReqDTO.CorpActnOptnDtlsDTO();
        optionDetails.setOptnNb(String.valueOf(result.getOptNumber()));
        optionDetails.setOptnTp(result.getOptType());
        optionDetails.setBal(String.valueOf(result.getSecQtyClient()));

        // Добавляем опциональные поля, если они есть
        if (result.getRedemptionPrice() != null) {
            optionDetails.setPricVal(result.getRedemptionPrice().toString());
        }
        if (result.getRedemptionCurrency() != null) {
            optionDetails.setPricValCcy(result.getRedemptionCurrency());
        }

        instruction.setCorpActnOptnDtls(optionDetails);

        dto.setCorporateActionInstruction(instruction);

        return dto;
    }
}
