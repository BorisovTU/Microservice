package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewInstruction;

import java.util.UUID;

@Repository
public interface ViewInstructionRepository extends JpaRepository<ViewInstruction, UUID> {
}