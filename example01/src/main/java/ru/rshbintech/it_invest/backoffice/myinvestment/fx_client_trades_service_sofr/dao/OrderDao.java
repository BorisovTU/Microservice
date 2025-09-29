package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dao;

import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto.OrderDto;

public interface OrderDao {

    void saveOrder(OrderDto orderDto);
}
