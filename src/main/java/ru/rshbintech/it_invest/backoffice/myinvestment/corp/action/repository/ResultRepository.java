package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ResultEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.projection.IResultResponseProjection;

import java.util.List;
import java.util.UUID;

public interface ResultRepository extends JpaRepository<ResultEntity, Long> {

    @Query(nativeQuery = true, value = """
                SELECT distinct
                   ca.caid caid,
               ca.reference reference,
               acc.cftid cftid,
               sec.isin isin,
               sec.reg_number regNumber,
               sec.nsdr nsdr,
               acc.acc_depo accDepo,
               acc.sub_acc_depo subAccDepo,
               ca.sfkpg_acct sfkpgAcct
                        FROM link l
                        LEFT JOIN corp_actions ca on ca.caid = l.caid
                        LEFT JOIN client_acc acc on acc.id = l.acc_id
                        left join security sec on sec.isin = ca.isin
                   WHERE result_id = :resultId
            """
            )
    List<IResultResponseProjection> findResultResponse(@Param("resultId") UUID resultId);

    @Query(
            nativeQuery = true,
            value = "select * from result where status = 2 " +
                    "ORDER BY create_date_time limit :limit for update skip locked"
    )
    List<ResultEntity> findReadyForResponse(@Param("limit") int limit);

    @Query(nativeQuery = true, value = "select client_dia_id from result where id = :id")
    Long getClientDiaIdById(@Param("id") UUID resultId);
}
