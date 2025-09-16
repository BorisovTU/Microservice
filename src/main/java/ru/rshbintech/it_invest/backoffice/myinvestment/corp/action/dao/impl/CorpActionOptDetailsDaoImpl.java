package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.impl;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.CorpActionsOptDetailsDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorpActnOptnDtls;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.CorpActionsOptDetails;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper.CorporateActionMapper;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.CorpActionsOptDetailsRepository;

import java.util.List;

@RequiredArgsConstructor
@Slf4j
@Component
public class CorpActionOptDetailsDaoImpl implements CorpActionsOptDetailsDao {

    private final CorpActionsOptDetailsRepository corpActionsOptDetailsRepository;
    private final CorporateActionMapper corporateActionMapper;

    @Override
    @Transactional(Transactional.TxType.REQUIRED)
    public void save(Long caid, List<CorpActnOptnDtls> corpActnOptnDtls) {
        if (corpActnOptnDtls == null || corpActnOptnDtls.isEmpty()) {
            return;
        }

        List<CorpActionsOptDetails> detailsList = corpActnOptnDtls.stream()
                .map(action -> corporateActionMapper.dtoToCorpActionsOptDetails(caid, action))
                .toList();

        // Батчевое сохранение
        corpActionsOptDetailsRepository.saveAll(detailsList);
    }
}
