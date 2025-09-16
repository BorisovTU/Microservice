package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity;

import lombok.Data;
import org.hibernate.annotations.Type;

import jakarta.persistence.*;
import java.time.OffsetDateTime;

@Entity
@Table(name = "data_ca_owner_balance")
@Data
public class DataCaOwnerBalance {
    @Id
    @Column(name = "owner_security_id")
    private Long ownerSecurityId;

    private Long bal;

    @Column(name = "create_date_time")
    private OffsetDateTime createDateTime;

    private Long caid;

    private Long cftid;
}
