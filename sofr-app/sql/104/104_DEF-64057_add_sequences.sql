-- Добавление sequence'ов для поля t_number таблицы dPrcContract_dbt 
DECLARE
    stat NUMBER;
    -- Получение имени sequence для указанного бэк-офиса
    FUNCTION GetPrcContractSequenceName(backOffice IN NUMBER) RETURN VARCHAR2
    IS
    BEGIN
      RETURN 'DPRCCONTRACT_DBT_BO' || backOffice || '_SEQ';
    END;

    -- Создание sequence для указанного бэк-офиса
    -- возвращает 0 если sequence существует или создан, -1 в случае ошибки
    FUNCTION PrcContractCreateSequence(backOffice IN NUMBER) RETURN NUMBER
    IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      v_result NUMBER := 0;
      v_cnt NUMBER;
      v_maxNumber NUMBER;
      v_sequenceName VARCHAR2(40);
      v_sql VARCHAR2(150);
    BEGIN
      v_sequenceName := GetPrcContractSequenceName(backOffice);
  
      SELECT COUNT(1) 
        INTO v_cnt 
        FROM user_sequences
       WHERE UPPER(sequence_name) = v_sequenceName;
  
      IF v_cnt = 0 THEN
        SELECT NVL(MAX(t_number), 0)
          INTO v_maxNumber
          FROM dPrcContract_dbt
         WHERE t_backOffice = backOffice;
     
        v_sql := 'CREATE SEQUENCE ' || v_sequenceName || ' ' ||
                 'START WITH ' || (v_maxNumber + 1) || ' ' ||
                 'MAXVALUE 999999999999999999999999999 ' ||
                 'NOCYCLE NOCACHE NOORDER';
        BEGIN
          EXECUTE IMMEDIATE v_sql;
        EXCEPTION WHEN OTHERS THEN
          v_result := -1;
        END;
      END IF;
  
      RETURN v_result;
    END;
  
BEGIN
  stat := PrcContractCreateSequence(1);
  stat := PrcContractCreateSequence(2);
  stat := PrcContractCreateSequence(7);
END;
/
