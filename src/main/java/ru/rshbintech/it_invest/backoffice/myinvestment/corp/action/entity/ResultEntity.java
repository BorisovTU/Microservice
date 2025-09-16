package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.UUID;

@Entity
@Table(name = "result")
@Getter
@Setter
public class ResultEntity {
    @Id
    @GeneratedValue
    @Column(name = "id")
    private UUID id;

    @Column(name = "client_dia_id", nullable = false)
    private Long clientDiaId;

    @Column(name = "default_options")
    private Boolean defaultOptions;

    @Column(name = "redemption_price", precision = 19, scale = 4)
    private BigDecimal redemptionPrice;

    @Column(name = "redemption_currency", length = 5)
    private String redemptionCurrency;

    @Column(name = "opt_number", nullable = false)
    private Integer optNumber;

    @Column(name = "opt_type", nullable = false, length = 5)
    private String optType;

    @Column(name = "sec_qty_mess")
    private Float secQtyMess;

    @Column(name = "sec_qty_client", nullable = false)
    private Float secQtyClient;

    @Column(name = "status", nullable = false)
    private Integer status;

    @Column(name = "instr_dt")
    private ZonedDateTime instrDt;

    @Column(name = "instr_nmb")
    private Integer instrNmb;

    @Column(name ="request_id")
    private UUID requestId;

    @Column(name = "create_date_time", nullable = false)
    private ZonedDateTime createDateTime;
}
