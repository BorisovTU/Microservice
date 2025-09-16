package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao;

import org.springframework.transaction.annotation.Transactional;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionDTO;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ResultEntity;

public interface CorporateActionDao {
    @Transactional
    ResultEntity saveInstructionResult(
            CorporateActionInstructionDTO.CorporateActionInstruction instruction,
            CorporateActionInstructionDTO.ClientId client,
            CorporateActionInstructionDTO.CorpActnOptnDtls option);
}
