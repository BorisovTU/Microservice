package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders")
@Getter
@Setter
public class OrderEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "code", nullable = false, unique = true)
    private String code;

    @Column(name = "external_code", nullable = false)
    private String externalCode;

    @Column(name = "created_date", nullable = false)
    private LocalDateTime createdDate;

    @Column(name = "contract_id", nullable = false)
    private Long contractId;

    @Column(name = "client_id", nullable = false)
    private Long clientId;

    @Column(name = "direction", nullable = false)
    private String direction;

    @Column(name = "fiid", nullable = false)
    private Long fiid;

    @Column(name = "amount", nullable = false)
    private BigDecimal amount;

    @Column(name = "price")
    private BigDecimal price;

    @Column(name = "price_type")
    private String priceType;

    @Column(name = "price_fiid")
    private Long priceFiid;

    @Column(name = "status_id", nullable = false)
    private Integer statusId;

    @Column(name = "counterparty_id", nullable = false)
    private Integer counterpartyId;

    @Column(name = "exchange_id", nullable = false)
    private Long exchangeId;

    @Column(name = "order_methods_id", nullable = false)
    private Integer orderMethodsId;

    @Column(name = "last_update_time", nullable = false)
    private LocalDateTime lastUpdateTime;
}
