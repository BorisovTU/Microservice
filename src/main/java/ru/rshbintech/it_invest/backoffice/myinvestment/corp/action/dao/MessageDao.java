package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao;

import jakarta.transaction.Transactional;

public interface MessageDao {
    @Transactional
    void saveIncomeDiasoftMessage(String payload);
}
