DECLARE
    cnt   NUMBER;
BEGIN
  SELECT COUNT (*)
    INTO cnt
    FROM DFUNC_DBT
   WHERE T_NAME = 'Сервис поиска карточек ЮЛ в AC CDI';

  IF CNT = 0
  THEN
    INSERT INTO dfunc_dbt(T_FUNCID, T_CODE, T_NAME, T_TYPE, T_FILENAME, T_FUNCTIONNAME, T_INTERVAL, T_VERSION)
      VALUES(5053, 'CdiFindOrg', 'Сервис поиска карточек ЮЛ в AC CDI', 1, 'ws_synch_SOFR', 'ws_CdiFindOrg', 0, 0);
  
    INSERT INTO dllvalues_dbt (T_LIST,T_ELEMENT,T_CODE,T_NAME,T_FLAG,T_NOTE,T_RESERVE)
      VALUES (5002,5053,'5053','Сервис поиска карточек ЮЛ в AC CDI',5053,'',CHR (1)) ;   

  COMMIT;
  END IF;
END;
/

DECLARE
    v_id   INTEGER;
    cnt    NUMBER;
BEGIN
    SELECT MAX (t_id) + 1 INTO v_id FROM dsimpleservice_dbt;

    SELECT COUNT (*)
      INTO cnt
      FROM DSIMPLESERVICE_DBT
     WHERE T_NAME = 'Обмен данными с AC CDI - исходящий';

    IF CNT = 0
    THEN
        INSERT INTO dsimpleservice_dbt
                 VALUES (v_id,                                 --t_id
                         'Обмен данными с AC CDI - исходящий', --t_name
                         'Обмен данными с AC CDI - исходящий', --t_description
                         'X'                                   --t_isactive
                        );


        INSERT INTO dss_func_dbt
                 VALUES (v_id - 10000 + 100000,                --t_id
                         v_id,                                 --t_service
                         1,                                    --t_level
                         'Обмен данными с AC CDI - исходящий', --t_name
                         1,                                    --t_type
                         'ws_check_job',                       --t_executorname
                         'ws_check_job_cdi_in',                --t_executorfunc
                         0,                                    --t_startdelay,
                         0,                                    --t_period
                         1,                                    --t_maxattempt,
                         NULL,                                 --t_timeout
                         NULL                                  --t_parameters
                        );

        --Periodtype 1 секунды, 2 минута, 3 часы, 4 дни. period это количество или секунд или минут и т.д.
        INSERT INTO dss_sheduler_dbt 
                 VALUES (v_id,                                 --t_id
                         'Обмен данными с AC CDI - исходящий', --t_name
                         'Обмен данными с AC CDI - исходящий', --t_description
                         v_id,                                 --t_service
                         1,                                    --t_module
                         1,                                    --t_shedulertype
                         TO_DATE ('01.01.0001 03:53:01',
                                  'dd.mm.yyyy hh24:mi:ss'),    --t_workstarttime
                         TO_DATE ('01.01.0001 08:59:59',
                                  'dd.mm.yyyy hh24:mi:ss'),    --t_workendtime
                         TO_DATE ('01.01.0001 03:53:01',
                                  'dd.mm.yyyy hh24:mi:ss'),    --t_starttime
                         2,                                    --t_periodtype
                         5,                                    --t_period
                         TO_DATE ('01.01.2023 03:00:01',
                                  'dd.mm.yyyy hh24:mi:ss'),    --t_nextstamp
                         NULL,
                         NULL);
    END IF;

    COMMIT;
END;
/

declare
    pelement number;
    psysname varchar2(15) := 'CDI';

    procedure InsDataType (pservice in varchar2, psys in number) is
       cnt number;
    begin 
       select count(t_id) into cnt from DDATATYPEWS_DBT where T_PARMIP = pservice;
       if cnt = 0 then
           Insert into DDATATYPEWS_DBT
              (T_ID, T_TYPE, T_ACCOUNTERPARTY, T_PARMIP, T_PARMBO, T_TIMEOUT, T_ISMACINFO)
            Values
              (0, 3, psys, pservice, 'empty', 500, 'X');
       end if;
    end;

begin
    -- система точно уже есть
    select t_element into pelement from DLLVALUES_DBT where t_list = 4067 and upper(t_code) = psysname;

    InsDataType ('CdiGetOrg', pelement); 

    InsDataType ('CdiFindOrg', pelement);

    InsDataType ('CdiCreateOrg', pelement);

    InsDataType ('CdiUpdateOrg', pelement);
    commit;
end;
/

DECLARE
    cnt     NUMBER;
    v_keyId NUMBER;
BEGIN
  SELECT COUNT (1)
    INTO cnt
    FROM DREGPARM_DBT
   WHERE T_NAME = 'Взаимодействие с AC CDI';

  IF CNT = 0
  THEN
    SELECT T_KEYID INTO v_keyId FROM DREGPARM_DBT WHERE T_NAME = 'РСХБ' AND t_parentid = 0;
    SELECT T_KEYID INTO v_keyId FROM DREGPARM_DBT WHERE T_NAME = 'ИНТЕГРАЦИЯ' AND t_parentid = v_keyId;

    INSERT INTO DREGPARM_DBT (T_NAME, T_TYPE, T_DESCRIPTION, T_PARENTID)
      VALUES('ВЗАИМОДЕЙСТВИЕ С AC CDI',0,'ВЗАИМОДЕЙСТВИЕ С AC CDI',v_keyId) 
      RETURNING t_keyid INTO v_keyId;

    INSERT INTO DREGPARM_DBT (T_NAME, T_TYPE, T_DESCRIPTION, T_PARENTID)
      VALUES('АКТИВНО',4,'АКТИВНО',v_keyId)  
      RETURNING t_keyid INTO v_keyId;

    INSERT INTO DREGVAL_DBT (T_KEYID, T_LINTVALUE, T_EXPDEP, T_OBJECTID, T_REGKIND)
      VALUES(v_keyId,88,88,0,0);

  COMMIT;
  END IF;
END;
/

CREATE OR REPLACE TRIGGER DPARTY_DBT_CDI
  AFTER INSERT
  ON dparty_dbt
  FOR EACH ROW
DECLARE
  v_retval CHAR;
BEGIN
  v_retval := RSB_COMMON.GetRegFlagValue( 'РСХБ\Интеграция\Взаимодействие с AC CDI\АКТИВНО');

  IF (v_retval = chr(88))
  THEN 
    INSERT INTO utableprocessevent_dbt (T_OBJECTID, T_OBJECTTYPE, T_STATUS,T_TIMESTAMP, T_NOTE, T_TYPE)
      VALUES (:NEW.t_partyid, 5053, 1, CURRENT_DATE, -1, 1);
  END IF;
END;
/

