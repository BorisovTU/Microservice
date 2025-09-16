package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.message.BaseMessage;

import java.util.List;
import java.util.UUID;

public interface MessageRepository extends JpaRepository<BaseMessage, UUID> {

    @Query(nativeQuery = true, value = "SELECT * FROM message m WHERE m.status = :status " +
            "ORDER BY m.create_date_time limit :limit FOR UPDATE SKIP LOCKED")
    List<BaseMessage> findByStatusWithLock(@Param("status") int status, @Param("limit") int limit);

}
