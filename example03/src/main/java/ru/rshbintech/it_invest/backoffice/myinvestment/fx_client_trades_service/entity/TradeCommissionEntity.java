package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Entity
@Table(name = "trade_commissions")
@Getter
@Setter
public class TradeCommissionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "commission_id", nullable = false)
    private Integer commissionId;

    @Column(nullable = false)
    private BigDecimal sum;

    @Column(name = "nds")
    private BigDecimal nds;

    @ManyToOne
    @JoinColumn(name = "trade_id", nullable = false)
    private TradeEntity trade;
}
