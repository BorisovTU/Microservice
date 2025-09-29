package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dao.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dao.OrderDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto.OrderDto;

import java.sql.CallableStatement;

@Repository
@RequiredArgsConstructor
@Slf4j
public class OrderDaoImpl implements OrderDao {

    private final JdbcTemplate jdbcTemplate;


    @Override
    public void saveOrder(OrderDto orderDto) {
        jdbcTemplate.execute((ConnectionCallback<Void>) con -> {
            try (CallableStatement cs = con.prepareCall(
                    "{ call SAVE_ORDER(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) }")) {

                cs.setString(1, orderDto.externalCode());
                cs.setString(2, orderDto.code());
                cs.setLong(3, orderDto.clientId());
                cs.setString(4, orderDto.direction());
                cs.setLong(5, orderDto.fiId());
                cs.setBigDecimal(6, orderDto.amount());
                cs.setBigDecimal(7, orderDto.price());
                cs.setString(8, orderDto.priceType());
                cs.setLong(9, orderDto.contractId());
                cs.setInt(10, orderDto.statusId());
                cs.setInt(11, orderDto.orderMethodsId());
                cs.setObject(12, orderDto.lastUpdateTime());
                cs.registerOutParameter(13, java.sql.Types.VARCHAR);

                cs.execute();
            }
            return null;
        });

        log.info("Заявка успешно сохранена: externalCode={}", orderDto.externalCode());
    }
}
