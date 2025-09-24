package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Entity
@Table(name = "data_ca_owner_balance")
@Data
public class DataCaOwnerBalance {
    @Id
    @Column(name = "owner_security_id")
    private Long ownerSecurityId;

    private BigDecimal bal;

    @Column(name = "create_date_time")
    private OffsetDateTime createDateTime;

    private Long caid;

    private Long cftid;
}
