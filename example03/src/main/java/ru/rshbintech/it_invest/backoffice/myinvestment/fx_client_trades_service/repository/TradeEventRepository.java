package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeEventEntity;

import java.util.UUID;

@Repository
public interface TradeEventRepository extends JpaRepository<TradeEventEntity, UUID> {
}
