package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCaInstruction;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ViewCaInstructionRepository extends JpaRepository<ViewCaInstruction, UUID> {

    Optional<ViewCaInstruction> findByInstrNmb(UUID instrNmb);

    // For InstrDt strategy
    @Query(value = """
   SELECT * FROM view_ca_instruction v WHERE (
   (CAST(:nextInstrDt AS TEXT) IS NULL)
   OR (CAST(:nextInstrNmb AS TEXT) IS NULL)
   OR (v.instr_dt > :nextInstrDt
   OR (v.instr_dt = :nextInstrDt AND v.instr_nmb > :nextInstrNmb)))
        AND v.cftid = :cftid
    ORDER BY
        v.instr_dt ASC,
        v.instr_nmb ASC
   fetch first :limit row only
""", nativeQuery = true)
    List<ViewCaInstruction> findWithInstrDtPagination(@Param("cftid") Long cftid,
                                                      @Param("nextInstrDt") OffsetDateTime nextInstrDt,
                                                      @Param("nextInstrNmb") UUID nextInstrNmb,
                                                      @Param("limit") Integer limit);

    @Query(value = """
   SELECT * FROM view_ca_instruction v WHERE 
   v.status = :status AND 
   (
   (CAST(:nextInstrDt AS TEXT) IS NULL)
   OR (CAST(:nextInstrNmb AS TEXT) IS NULL)
   OR (v.instr_dt > :nextInstrDt
   OR (v.instr_dt = :nextInstrDt AND v.instr_nmb > :nextInstrNmb)))
        AND v.cftid = :cftid
    ORDER BY
        v.instr_dt ASC,
        v.instr_nmb ASC
   fetch first :limit row only
""", nativeQuery = true)
    List<ViewCaInstruction> findByStatusWithInstrDtPagination(@Param("cftid") Long cftid,
                                                              @Param("status") String status,
                                                              @Param("nextInstrDt") OffsetDateTime nextInstrDt,
                                                              @Param("nextInstrNmb") UUID nextInstrNmb,
                                                              @Param("limit") Integer limit);

    // For InstrNmb strategy
    @Query(value = "SELECT * FROM view_ca_instruction v WHERE " +
            "((CAST(:nextInstrNmb AS TEXT) IS NULL) OR v.instr_nmb > :nextInstrNmb) AND " +
            "((CAST(:cftid AS TEXT) IS NULL) OR v.cftid = :cftid) " +
            "ORDER BY v.instr_nmb ASC" +
            " fetch first :limit row only", nativeQuery = true)
    List<ViewCaInstruction> findWithInstrNmbPagination(@Param("cftid") Long cftid,
                                                       @Param("nextInstrNmb") UUID nextInstrNmb,
                                                       @Param("limit") Integer pageable);

    @Query(value = "SELECT * FROM view_ca_instruction v WHERE " +
            "v.status = :status AND " +
            "((CAST(:nextInstrNmb AS TEXT) IS NULL) OR v.instr_nmb > :nextInstrNmb) AND " +
            "((CAST(:cftid AS TEXT) IS NULL) OR v.cftid = :cftid) " +
            "ORDER BY v.instr_nmb ASC" +
            " fetch first :limit row only", nativeQuery = true)
    List<ViewCaInstruction> findByStatusWithInstrNmbPagination(@Param("cftid") Long cftid,
                                                               @Param("status") String status,
                                                               @Param("nextInstrNmb") UUID nextInstrNmb,
                                                               @Param("limit") Integer limit);

    // For Status strategy
    @Query(value = "SELECT * FROM view_ca_instruction v WHERE " +
            "((CAST(:nextStatus AS TEXT) IS NULL) OR (CAST(:nextInstrNmb AS TEXT) IS NULL) IS NULL OR " +
            "(v.status > :nextStatus OR (v.status = :nextStatus AND v.instr_nmb > :nextInstrNmb))) AND " +
            "((CAST(:cftid AS TEXT) IS NULL) OR v.cftid = :cftid) " +
            "ORDER BY v.status ASC, v.instr_nmb ASC " +
            " fetch first :limit row only", nativeQuery = true)
    List<ViewCaInstruction> findWithStatusPagination(@Param("cftid") Long cftid,
                                                     @Param("nextStatus") String nextStatus,
                                                     @Param("nextInstrNmb") UUID nextInstrNmb,
                                                     @Param("limit") Integer limit);

    @Query(value = "SELECT * FROM view_ca_instruction v WHERE " +
            "v.status = :status AND " +
            "((CAST(:nextInstrNmb AS TEXT) IS NULL) OR " +
            "(v.status > :nextStatus OR (v.status = :nextStatus AND v.instr_nmb > :nextInstrNmb))) AND " +
            "(:cftid IS NULL OR v.cftid = :cftid) " +
            "ORDER BY v.status ASC, v.instr_nmb ASC " +
            " fetch first :limit row only", nativeQuery = true)
    List<ViewCaInstruction> findByStatusWithStatusPagination(@Param("cftid") Long cftid,
                                                             @Param("status") String status,
                                                             @Param("nextStatus") String nextStatus,
                                                             @Param("nextInstrNmb") UUID nextInstrNmb,
                                                             @Param("limit") Integer limit);

}
