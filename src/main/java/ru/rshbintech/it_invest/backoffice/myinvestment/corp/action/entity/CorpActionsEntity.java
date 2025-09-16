package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;
import java.util.UUID;

@Entity
@Table(name = "corp_actions")
@Getter
@Setter
public class CorpActionsEntity {

    @Id
    @Column(name = "Id")
    @GeneratedValue
    private UUID id;

    @Column(name = "caid")
    private Long caid;

    @Column(name = "catype", nullable = false)
    private String caType;

    @Column(name = "reference", nullable = false)
    private String reference;

    @Column(name = "swift_type", nullable = false)
    private String swiftType;

    @Column(name = "ma_vo_code", nullable = false)
    private String maVoCode;

    @Column(name = "org_nm")
    private String orgNm;

    @Column(name = "sfkpg_acct")
    private String sfkpgAcct;

    @Column(name = "date_reg_owners")
    private LocalDate dateRegOwners;

    @Column(name = "isin", nullable = false)
    private String isin;

    @Column(name = "min_date_start")
    private LocalDate minDateStart;

    @Column(name = "max_date_end")
    private LocalDate maxDateEnd;

    @Column(name = "addtl_inf")
    private String addtlInf;
    @Column(name = "lws_in_plc_cd")
    private String lwsInPlcCd;
    @Column(name = "sbrdnt_lws_in_plc_cd")
    private String sbrdntLwsInPlcCd;
}
