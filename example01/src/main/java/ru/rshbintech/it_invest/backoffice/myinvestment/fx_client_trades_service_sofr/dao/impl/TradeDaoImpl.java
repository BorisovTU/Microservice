package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dao.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dao.TradeDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto.TradeCommissionDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto.TradeDto;

import java.sql.CallableStatement;

@Repository
@RequiredArgsConstructor
@Slf4j
public class TradeDaoImpl implements TradeDao {

    private final JdbcTemplate jdbcTemplate;
    @Override
    public void saveTrade(TradeDto tradeDto) {

        jdbcTemplate.execute((ConnectionCallback<Void>) con -> {
            try (CallableStatement cs = con.prepareCall(
                    "{ call SAVE_TRADE(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) }")) {

                cs.setInt(1, tradeDto.tradeKind());
                cs.setString(2, tradeDto.code());
                cs.setString(3, tradeDto.externalCode());
                cs.setString(4, tradeDto.direction());
                cs.setObject(5, tradeDto.createdDate());
                cs.setLong(6, tradeDto.contractId());
                cs.setLong(7, tradeDto.clientId());
                cs.setLong(8, tradeDto.fiId());
                cs.setBigDecimal(9, tradeDto.amount());
                cs.setBigDecimal(10, tradeDto.price());
                cs.setObject(11, tradeDto.clearingDate());
                cs.setInt(12, tradeDto.counterpartyId());
                cs.setLong(13, tradeDto.exchangeId());
                cs.setLong(14, tradeDto.orderId());
                cs.setInt(15, tradeDto.marketScheme());
                cs.setString(16, tradeDto.status());

                cs.execute();
            }
            return null;
        });

        log.info("Трейд успешно сохранён: externalCode={}", tradeDto.externalCode());

        if (tradeDto.commissions() != null && !tradeDto.commissions().isEmpty()) {
            for (TradeCommissionDto commission : tradeDto.commissions()) {
                jdbcTemplate.execute((ConnectionCallback<Void>) con -> {
                    try (CallableStatement cs = con.prepareCall(
                            "{ call SAVE_TRADE_COMMISSION(?, ?, ?, ?) }")) {

                        cs.setLong(1, tradeDto.orderId());
                        cs.setInt(2, commission.commissionId());
                        cs.setBigDecimal(3, commission.sum());
                        cs.setBigDecimal(4, commission.nds());

                        cs.execute();
                    }
                    return null;
                });
                log.info("Комиссия сохранена: commissionId={}, tradeExternalCode={}",
                        commission.commissionId(), tradeDto.externalCode());
            }
        }
    }
}


