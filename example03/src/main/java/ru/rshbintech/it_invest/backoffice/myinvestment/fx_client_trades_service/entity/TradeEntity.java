package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "trades")
@Getter
@Setter
public class TradeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Integer tradeKind;

    @Column(nullable = false)
    private String code;

    @Column(nullable = false)
    private String externalCode;

    @Column(nullable = false)
    private String direction;

    private LocalDateTime createdDate;

    private Long contractId;

    private Long clientId;

    private Long fiid;

    private BigDecimal amount;

    private BigDecimal price;

    private LocalDateTime clearingDate;

    private Integer counterpartyId;

    private Integer exchangeId;

    private Long orderId;

    private Integer marketScheme;
}
