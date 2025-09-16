package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao;

import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorpActnOptnDtls;

import java.util.List;

public interface CorpActionsOptDetailsDao {
    void save(Long caid, List<CorpActnOptnDtls> corpActnOptnDtls);
}
