package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.mapper;

import org.mapstruct.*;
import org.mapstruct.factory.Mappers;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.dto.OrderDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.OrderEntity;

@Mapper(componentModel = "spring")
public interface OrderMapper {

    OrderMapper INSTANCE = Mappers.getMapper(OrderMapper.class);

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "counterpartyId", constant = "2")
    @Mapping(target = "statusId", source = "statusId")
    @Mapping(target = "orderMethodsId", source = "orderMethodsId")
    OrderEntity toEntity(OrderDto dto);

    @BeanMapping(nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE)
    void updateEntityFromDto(OrderDto dto, @MappingTarget OrderEntity entity);

}
