package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.CorpActionsOptDetails;

import java.util.List;
import java.util.UUID;

public interface CorpActionsOptDetailsRepository extends JpaRepository<CorpActionsOptDetails, UUID> {
    List<CorpActionsOptDetails> findByCaid(Long caid);
}
