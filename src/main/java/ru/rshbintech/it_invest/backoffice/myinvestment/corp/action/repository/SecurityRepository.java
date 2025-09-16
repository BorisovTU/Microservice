package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.SecurityEntity;

import java.util.Optional;
import java.util.UUID;

public interface SecurityRepository extends JpaRepository<SecurityEntity, UUID> {
    Optional<SecurityEntity> findByIsin(String isin);
}
