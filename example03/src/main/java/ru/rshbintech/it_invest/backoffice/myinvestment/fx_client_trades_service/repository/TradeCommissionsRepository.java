package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeCommissionEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeEntity;

@Repository
public interface TradeCommissionsRepository extends JpaRepository<TradeCommissionEntity, Long> {

    void deleteByTrade(TradeEntity trade);
}
