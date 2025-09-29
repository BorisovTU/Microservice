package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.mapper;

import org.mapstruct.*;
import org.mapstruct.factory.Mappers;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.dto.TradeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeCommissionEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeEntity;

import java.util.Collections;
import java.util.List;

@Mapper(componentModel = "spring")
public interface TradeMapper {

    TradeMapper INSTANCE = Mappers.getMapper(TradeMapper.class);

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "orderId", ignore = true)
    TradeEntity toEntity(TradeDto dto);

    @BeanMapping(nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE)
    @Mapping(target = "id", ignore = true)
    @Mapping(target = "orderId", source = "externalCode")
    void updateEntityFromDto(TradeDto dto, @MappingTarget TradeEntity entity);

    default List<TradeCommissionEntity> toCommissions(TradeDto dto, TradeEntity trade) {
        if (dto.commissions() == null || dto.commissions().isEmpty()) {
            return Collections.emptyList();
        }
        return dto.commissions().stream()
                .map(c -> {
                    TradeCommissionEntity entity = new TradeCommissionEntity();
                    entity.setTrade(trade);
                    entity.setCommissionId(c.commissionId());
                    entity.setSum(c.sum());
                    entity.setNds(c.nds());
                    return entity;
                })
                .toList();
    }
}
