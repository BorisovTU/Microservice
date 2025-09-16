package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Entity
@Table(name = "client_acc")
@Getter
@Setter
public class ClientAccEntity {
    @Id
    @GeneratedValue
    @Column(name = "id")
    private UUID id;

    @Column(name = "cftid", nullable = false)
    private Long cftid;

    @Column(name = "bal")
    private Long bal;

    @Column(name = "acc_depo", nullable = false, unique = true)
    private String accDepo;

    @Column(name = "sub_acc_depo")
    private String subAccDepo;
}
