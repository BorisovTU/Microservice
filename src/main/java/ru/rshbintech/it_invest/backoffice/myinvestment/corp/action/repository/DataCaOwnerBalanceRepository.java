package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.DataCaOwnerBalance;

import java.util.List;

@Repository
public interface DataCaOwnerBalanceRepository extends JpaRepository<DataCaOwnerBalance, Long> {
}
