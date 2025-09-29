package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dao;

import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto.TradeDto;

public interface TradeDao {

    void saveTrade(TradeDto tradeDto);

}
