package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ClientAccEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.projection.IClientInfo;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ClientAccRepository extends JpaRepository<ClientAccEntity, UUID> {

    @Query(value = """
SELECT 
    res.client_dia_id as ownerSecurityID,
    c.cftid as cftid,
    c.acc_depo as accDepo,
    c.sub_acc_depo as subAccDepo,
    c.bal as bal
FROM link l
LEFT JOIN result res on res.id = l.result_id    
LEFT JOIN client_acc c ON l.acc_id = c.id
WHERE l.caid = :caid and c.cftid = :cftid
""",
            nativeQuery = true)
    List<IClientInfo> findClientInfoByCaidAndCftid(@Param("caid") Long caid,
                                                   @Param("cftid") Long cftid);

    @Query(value = """
SELECT 
    res.client_dia_id as ownerSecurityID,
    c.cftid as cftid,
    c.acc_depo as accDepo,
    c.sub_acc_depo as subAccDepo,
    c.bal as bal
FROM link l
LEFT JOIN result res on res.id = l.result_id
LEFT JOIN client_acc c ON l.acc_id = c.id
WHERE l.caid = :caid
""",
            nativeQuery = true)
    List<IClientInfo> findClientInfoByCaid(@Param("caid") Long caid);

    Optional<ClientAccEntity> findByAccDepo(String accDepo);
    Optional<ClientAccEntity> findByAccDepoAndCftid(String accDepo, Long cftid);
}
