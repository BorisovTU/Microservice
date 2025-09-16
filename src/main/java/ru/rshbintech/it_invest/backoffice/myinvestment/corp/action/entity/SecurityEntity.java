package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Entity
@Table(name = "security")
@Getter
@Setter
public class SecurityEntity {
    @Id
    @GeneratedValue
    @Column(name = "id")
    private UUID id;

    @Column(name = "isin", nullable = false)
    private String isin;

    @Column(name = "reg_number", nullable = false)
    private String regNumber;

    @Column(name = "nsdr", nullable = false)
    private String nsdr;
}
