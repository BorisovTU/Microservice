--Добавление тройской унции в справочник
BEGIN
   EXECUTE IMMEDIATE 'INSERT INTO DMEASURE_DBT (T_MEASURECODE,T_NAME,T_DEFINITION,T_CNM,T_RESERVE) VALUES (201,''Унция'',''Унция'',''унция'',chr(1))';
   EXCEPTION WHEN OTHERS THEN NULL;
END;
/

--Вставка информации о значении унции
DECLARE
   v_ParentId NUMBER;
   v_cnt NUMBER := 0;
   v_ID  NUMBER := 0;
BEGIN
   SELECT t_KeyId INTO v_ParentId FROM DREGPARM_DBT WHERE LOWER(T_NAME) = LOWER('ПРОИЗВОДНЫЕ ИНСТРУМЕНТЫ') AND T_PARENTID = 0;

   IF (v_ParentId <> 0) THEN 
      SELECT COUNT(*) INTO v_cnt FROM DREGPARM_DBT WHERE t_ParentId = v_ParentId AND t_Name = 'ТРОЙСКАЯ УНЦИЯ В ГРАММАХ';
      
      IF v_cnt = 0 THEN
         INSERT INTO DREGPARM_DBT 
           (T_KEYID, T_PARENTID, T_NAME, T_TYPE, T_GLOBAL, T_DESCRIPTION, T_SECURITY, T_ISBRANCH, T_TEMPLATE)
         VALUES 
            (0, v_ParentId, 'ТРОЙСКАЯ УНЦИЯ В ГРАММАХ', 1, CHR(88),'Тройская унция в граммах', CHR(0), CHR(0), CHR(1)) RETURNING T_KEYID INTO v_ID;

         INSERT INTO DREGVAL_DBT 
           (T_KEYID,T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
         VALUES
           (v_ID,0,0,CHR(0),0,0,31.1035,'');
     END IF;
   END IF;
END;
/

--Редактирование информации об артикулах на драг. металл
BEGIN
  --Проставляем ParentFI для артикулов на драгметаллы
  update dfininstr_dbt fin 
     set fin.t_parentfi = 7 --доллар
  where  fin.t_fi_code IN ('GOLD', 'PLT', 'SILV', 'PLD') AND fin.t_fi_kind = 7; -- артикул

  --Проставляем валюту шага цены для производных инструментов с БА = эти артикулы
  update dfideriv_dbt deriv 
     set deriv.t_tickfiid = 7 --доллар
  where  deriv.t_fiid IN ( select fin.t_fiid 
                             from dfininstr_dbt fin
                             where fin.t_fi_kind = 4 --ПИ 
                               and t_avoirkind = 1 --фьючерс
                               and fin.t_facevaluefi IN ( select fibase.t_fiid 
                                                          from   dfininstr_dbt fibase
                                                           where fibase.t_fi_code IN ('GOLD', 'PLT', 'SILV', 'PLD') 
                                                             AND fibase.t_fi_kind = 7 -- артикул
                                                         )
                          );

  --Проставляем для этих артикулов единицу измерения = тройская унция
  update darticle_dbt art 
     set art.t_measurecode = 201 --унция
  where  art.t_fiid IN (select fin.t_fiid 
                        from   dfininstr_dbt fin
                         where fin.t_fi_code IN ('GOLD', 'PLT', 'SILV', 'PLD') 
                           AND fin.t_fi_kind = 7 -- артикул
                       );

END;
/
