package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service;

import jakarta.validation.Valid;
import jakarta.validation.ValidationException;
import lombok.RequiredArgsConstructor;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.CorporateActionDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionDTO;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionResponseDTO;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionRequestDTO;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionResponse;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ResultEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.projection.ICorporateActionProjection;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper.CorporateActionMapper;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.CorpActionsRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util.ParseUtil;

import java.util.List;

import static ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CASEnum.START_DATE;

@Service
@RequiredArgsConstructor
public class CorporateActionService {

    private final CorpActionsRepository corporateActionRepository;
    private final CorporateActionMapper corporateActionMapper;
    private final CorporateActionDao corporateActionDao;


    public CorporateActionResponse findCorporateActions(CorporateActionRequestDTO requestDTO) {
        validate(requestDTO);
        int limit = Math.min(requestDTO.getLimit(), 1000);
        if (limit < 1) limit = 50;
        requestDTO.setLimit(limit+1);

        List<ICorporateActionProjection> results = (START_DATE.equals(requestDTO.getSort())) ? corporateActionRepository.findCorporateActionsNativeByStartDate(requestDTO) :
        corporateActionRepository.findCorporateActionsNativeByCaid(requestDTO);

        boolean hasNext = results.size() > limit;

        List<ICorporateActionProjection> data = hasNext ?
                results.subList(0, limit) :
                results;

        String nextCursor = hasNext ? ParseUtil.toString(results.get(limit).getCaid()) : null;

        return corporateActionMapper.toResponse(data, nextCursor, requestDTO.getCftid(), requestDTO.isStatus(), limit);
    }

    private void validate(CorporateActionRequestDTO requestDTO) {
        if (requestDTO.getLimit() < 1 || requestDTO.getLimit() > 1000) {
            throw new ValidationException("Limit must be between 1 and 1000");
        }
    }

    @Retryable(value = ObjectOptimisticLockingFailureException.class,
            maxAttempts = 3,
            backoff = @Backoff(delay = 100))
    public CorporateActionInstructionResponseDTO processInstruction(@Valid CorporateActionInstructionDTO dto) {
        // Извлекаем внутренний объект CorporateActionInstruction
        CorporateActionInstructionDTO.CorporateActionInstruction instruction = dto.getCorporateActionInstruction();

        // Валидация и извлечение данных из DTO
        CorporateActionInstructionDTO.ClientId firstClient = instruction.getClientId().get(0);
        CorporateActionInstructionDTO.CorpActnOptnDtls firstOption = instruction.getCorpActnOptnDtls().get(0);

        // Сохраняем инструкцию через DAO (включая проверку существования связанных сущностей)
        ResultEntity savedResult = corporateActionDao.saveInstructionResult(instruction, firstClient, firstOption);

        // Формирование ответа
        return createResponseDTO(savedResult);
    }

    private CorporateActionInstructionResponseDTO createResponseDTO(ResultEntity result) {
        CorporateActionInstructionResponseDTO response = new CorporateActionInstructionResponseDTO();
        //ToDo: Заполнения более детальной информацией
        return response;
    }
}
