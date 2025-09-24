package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCANotification;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ViewCANotificationRepository extends JpaRepository<ViewCANotification, UUID> {

    Optional<ViewCANotification> getFirstByCaidAndCftidOrderByCreateDateTimeDesc(long caid, long cftId);

    @Query(value = """
                SELECT * FROM view_ca_notification 
                WHERE cftid = :cftid 
                AND (:active = false OR (
                    (CURRENT_DATE BETWEEN start_dt AND rspnddln::date) 
                    OR (rspnddln IS NULL AND CURRENT_DATE >= start_dt)
                ))
                AND (:from IS NULL OR caid >= :from)
                ORDER BY start_dt DESC, caid ASC 
                LIMIT :limit
            """, nativeQuery = true)
    List<ViewCANotification> findAllByCftidWithFilters(
            @Param("cftid") Long cftid,
            @Param("active") Boolean active,
            @Param("from") Long from,
            @Param("limit") int limit);

}
