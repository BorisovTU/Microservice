package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionRequestDTO;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.CorpActionsEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.projection.ICorporateActionProjection;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface CorpActionsRepository extends JpaRepository<CorpActionsEntity, UUID> {

    @Query(value = """
            SELECT 
                ca.caid                 AS caid,
                ca.catype               AS catype,
                ca.org_nm               AS orgNm,
                ca.sfkpg_acct           AS sfkpgAcct,            
                ca.reference            AS reference,
                ca.swift_type           AS swiftType,
                ca.ma_vo_code           AS maVoCode,
                ca.date_reg_owners      AS dateRegOwners,
                ca.isin                 AS isin,
                ca.min_date_start       AS dateStart,
                ca.max_date_end         AS dateEnd,
                ca.addtl_inf            AS addtlInf,
                ca.lws_in_plc_cd        AS lwsInPlcCd,
                ca.sbrdnt_lws_in_plc_cd AS sbrdntLwsInPlcCd,
                sec.reg_number          AS regnumber,
                sec.nsdr                AS nsdr           
            FROM corp_actions ca
            left join security sec on sec.isin = ca.isin
            WHERE 
                (cast(:#{#req.isin} as text) IS NULL OR ca.isin = cast(:#{#req.isin} as text)) AND
                (cast(:#{#req.cftid} as text) IS NULL OR EXISTS (
                                                                  SELECT 1 FROM link l
                                                                  JOIN client_acc c ON l.acc_id = c.id
                                                                  WHERE l.caid = ca.caid AND c.cftid = :#{#req.cftid}
                                                              )) AND    
                (cast(:#{#req.nextId} as bigint) IS NULL OR ca.caid > cast(:#{#req.nextId} as bigint)) AND
                (cast(:#{#req.caid} as text) IS NULL OR ca.caid::text = cast(:#{#req.caid} as text)) AND
                (cast(:#{#req.mndtryVlntryEvtTp} as text) IS NULL OR ca.ma_vo_code = cast(:#{#req.mndtryVlntryEvtTp} as text))
            ORDER BY ca.min_date_start, ca.caid ASC
            LIMIT cast(:#{#req.limit} as integer)
            """,
            nativeQuery = true)
    List<ICorporateActionProjection> findCorporateActionsNativeByStartDate(
            @Param("req") CorporateActionRequestDTO requestDTO);

    @Query(value = """
            SELECT 
                ca.caid                 AS caid,
                ca.catype               AS catype,
                ca.org_nm               AS orgNm,
                ca.sfkpg_acct           AS sfkpgAcct,            
                ca.reference            AS reference,
                ca.swift_type           AS swiftType,
                ca.ma_vo_code           AS maVoCode,
                ca.date_reg_owners      AS dateRegOwners,
                ca.isin                 AS isin,
                ca.min_date_start       AS dateStart,
                ca.max_date_end         AS dateEnd,
                ca.addtl_inf            as addtlInf,
                ca.lws_in_plc_cd        as lwsInPlcCd,
                ca.sbrdnt_lws_in_plc_cd as sbrdntLwsInPlcCd,
                sec.reg_number           as regnumber,
                sec.nsdr                as nsdr    
            FROM corp_actions ca
            left join security sec on sec.isin = ca.isin
            WHERE 
                (cast(:#{#req.isin} as text) IS NULL OR ca.isin = cast(:#{#req.isin} as text)) AND
                (cast(:#{#req.cftid} as text) IS NULL OR EXISTS (
                                                                  SELECT 1 FROM link l
                                                                  JOIN client_acc c ON l.acc_id = c.id
                                                                  WHERE l.caid = ca.caid AND c.cftid = :#{#req.cftid}
                                                              )) AND    
                (cast(:#{#req.nextId} as bigint) IS NULL OR ca.caid > cast(:#{#req.nextId} as bigint)) AND
                (cast(:#{#req.caid} as text) IS NULL OR ca.caid::text = cast(:#{#req.caid} as text)) AND
                (cast(:#{#req.mndtryVlntryEvtTp} as text) IS NULL OR ca.ma_vo_code = cast(:#{#req.mndtryVlntryEvtTp} as text))
            ORDER BY ca.caid ASC
            LIMIT cast(:#{#req.limit} as integer)
            """,
            nativeQuery = true)
    List<ICorporateActionProjection> findCorporateActionsNativeByCaid(
            @Param("req") CorporateActionRequestDTO requestDTO);

    Optional<CorpActionsEntity> findByReference(String reference);

    boolean existsCorpActionsEntityByReference(String reference);

    boolean existsCorpActionsEntityByCaid(long caid);
}
