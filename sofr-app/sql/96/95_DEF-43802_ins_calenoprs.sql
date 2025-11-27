--Добавление записи в DDLCALENOPRS_DBT
declare   
    v_Count NUMBER(10) := 0;
   begin
    SELECT COUNT(1) INTO v_Count FROM DDLCALENOPRS_DBT WHERE T_NAME = 'Погашение доп. дохода ВО' AND T_IDENTPROGRAM = 83;
    
    IF v_Count = 0 THEN
        INSERT INTO DDLCALENOPRS_DBT (T_IDENTPROGRAM, T_OBJTYPE, T_NAME, T_ISSYSTEM) VALUES (83,3,'Погашение доп. дохода ВО', 'X');
    END IF;
    COMMIT;
EXCEPTION
WHEN OTHERS THEN NULL;     
end;
/