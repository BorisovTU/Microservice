package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeEntity;

import java.util.Optional;

@Repository
public interface TradeRepository extends JpaRepository<TradeEntity, Long> {

    Optional<TradeEntity> findByExternalCodeAndDirection(String externalCode, String direction);
}

