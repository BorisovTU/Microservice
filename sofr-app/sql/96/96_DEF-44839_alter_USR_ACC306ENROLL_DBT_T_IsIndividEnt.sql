-- Добавление колонки
ALTER TABLE USR_ACC306ENROLL_DBT
    ADD (T_IsIndividEnt CHAR (1) DEFAULT CHR (0))
/