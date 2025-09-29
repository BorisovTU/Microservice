package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.OrderEntity;

import java.util.Optional;

@Repository
public interface OrderRepository extends JpaRepository<OrderEntity, Long> {

    Optional<OrderEntity> findByExternalCodeAndExchangeId(String externalCode, Long exchangeId);
}
