package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.DataCAInstruction;

import java.util.UUID;

@Repository
public interface DataCAInstructionRepository extends JpaRepository<DataCAInstruction, UUID> {
    boolean existsByInstrNmb(UUID instrNmb);
}
