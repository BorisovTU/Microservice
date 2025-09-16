package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.LinkEntity;

import java.util.Optional;
import java.util.UUID;

public interface LinkRepository extends JpaRepository<LinkEntity, UUID> {
    Optional<LinkEntity> findFirstByCaidAndAccId(Long caid, UUID id);
}
