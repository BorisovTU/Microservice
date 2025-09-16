package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;
import java.util.UUID;

@Entity
@Table(name = "corp_actions_opt_details")
@Getter
@Setter
public class CorpActionsOptDetails {

    @Id
    @GeneratedValue
    @Column(name = "id", nullable = false, columnDefinition = "UUID")
    private UUID id;

    @Column(name = "caid", nullable = false)
    private Long caid;

    @Column(name = "option_number")
    private Long optnNb;

    @Column(name = "option_type", length = 50)
    private String optnTp;

    @Column(name = "dflt_optn_ind")
    private Boolean dfltOptnInd;

    @Column(name = "pric_val")
    private Long pricVal;

    @Column(name = "pric_val_ccy", length = 50)
    private String pricValCcy;

    @Column(name = "start_dt")
    private LocalDate startDt;

    @Column(name = "end_dt")
    private LocalDate endDt;
}
