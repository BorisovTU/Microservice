package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCaInstruction;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ViewCaInstructionRepository extends JpaRepository<ViewCaInstruction, UUID> {

    Optional<ViewCaInstruction> findByInstrNmb(UUID instrNmb);

    @Query("SELECT v FROM ViewCaInstruction v WHERE " +
            "(:nextid IS NULL OR v.instrNmb > :nextid) AND " +
            "(:cftid IS NULL OR v.cftid = :cftid) " +
            "ORDER BY v.instrNmb DESC")
    List<ViewCaInstruction> findWithPagination(@Param("cftid") Long cftid, @Param("nextid") UUID nextid, Pageable pageable);

    @Query("SELECT v FROM ViewCaInstruction v WHERE " +
            "v.status = :status AND " +
            "(:nextid IS NULL OR v.instrNmb > :nextid) AND " +
            "(:cftid IS NULL OR v.cftid = :cftid) " +
            "ORDER BY v.instrNmb DESC")
    List<ViewCaInstruction> findByStatusWithPagination(@Param("cftid") Long cftid,
                                                       @Param("status") String status,
                                                       @Param("nextid") UUID nextid,
                                                       Pageable pageable);
}