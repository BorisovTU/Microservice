--Обновление новых полей в таблицы событий СНОБ
declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASE_DBT_IDX_CD' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASE_DBT_IDX_CD';
  end if;
  execute immediate 'CREATE INDEX DNPTXTOTALBASE_DBT_IDX_CD ON DNPTXTOTALBASE_DBT (T_CANCELDATE)';
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASE_DBT_IDX_SD' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASE_DBT_IDX_SD';
  end if;
  execute immediate 'CREATE INDEX DNPTXTOTALBASE_DBT_IDX_SD ON DNPTXTOTALBASE_DBT (T_SENDDATE)';
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_CD' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_CD';
  end if;
  execute immediate 'CREATE INDEX DNPTXTOTALBASEBC_DBT_IDX_CD ON DNPTXTOTALBASEBC_DBT (T_CANCELDATE)';
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_SD' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_SD';
  end if;
  execute immediate 'CREATE INDEX DNPTXTOTALBASEBC_DBT_IDX_SD ON DNPTXTOTALBASEBC_DBT (T_SENDDATE)';
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_U1' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_U1';
  end if;
  execute immediate 'CREATE INDEX DNPTXTOTALBASEBC_DBT_IDX_U1 ON DNPTXTOTALBASEBC_DBT (T_TBID, T_INSTANCE)';
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_U2' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_U2';
  end if;
  execute immediate 'CREATE INDEX DNPTXTOTALBASEBC_DBT_IDX_U2 ON DNPTXTOTALBASEBC_DBT (T_TBID, T_STORSTATE, T_CONFIRMSTATE)';
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_U3' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_U3';
  end if;
  execute immediate 'CREATE INDEX DNPTXTOTALBASEBC_DBT_IDX_U3 ON DNPTXTOTALBASEBC_DBT (T_TBID, T_SENDDATE)';
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_U4' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_U4';
  end if;
  execute immediate 'CREATE INDEX DNPTXTOTALBASEBC_DBT_IDX_U4 ON DNPTXTOTALBASEBC_DBT (T_STORSTATE, T_CONFIRMSTATE)';
end;
/


declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_U5' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_U5';
  end if;
  execute immediate 'CREATE INDEX DNPTXTOTALBASEBC_DBT_IDX_U5 ON DNPTXTOTALBASEBC_DBT (T_TBID, T_CANCELDATE)';
end;
/




--Для Активных и Подтвержденных

DECLARE
BEGIN
  UPDATE dnptxtotalbase_dbt stb
     SET (stb.t_SendDate, stb.t_SendTime) = (SELECT TO_DATE('01.01.0001','DD.MM.YYYY'), TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS') FROM DUAL) 
   WHERE stb.t_SendDate IS NULL;

  UPDATE dnptxtotalbasebc_dbt bc
     SET (bc.t_SendDate, bc.t_SendTime) = (SELECT TO_DATE('01.01.0001','DD.MM.YYYY'), TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS') FROM DUAL) 
   WHERE bc.t_SendDate IS NULL;


  UPDATE dnptxtotalbase_dbt stb
     SET (stb.t_SendDate, stb.t_SendTime) = (SELECT step.t_Syst_Date, step.t_Syst_Time
                                               FROM doprstep_dbt step
                                              WHERE step.t_ID_Operation = stb.t_ID_Operation
                                                AND step.t_ID_Step = stb.t_ID_Step
                                            )
   WHERE stb.t_ConfirmState = 1
     AND stb.t_StorState = 1
     AND stb.t_SendDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY') 
     AND EXISTS
            (SELECT 1
               FROM dnptxtotalbasebc_dbt bc
              WHERE bc.t_TBID = stb.t_TBID
                AND bc.t_Instance = stb.t_Instance - 1
                AND bc.t_ConfirmState <> stb.t_ConfirmState);


  UPDATE dnptxtotalbase_dbt stb
      SET (stb.t_SendDate, stb.t_SendTime) =
             (SELECT step.t_Syst_Date, step.t_Syst_Time
                FROM dnptxtotalbasebc_dbt bc, doprstep_dbt step
               WHERE bc.t_TBID = stb.t_TBID
                     AND bc.t_BCID =
                            (SELECT MAX (bc1.t_BCID)
                               FROM dnptxtotalbasebc_dbt bc1
                              WHERE     bc1.t_TBID = bc.t_TBID
                                    AND bc1.t_ConfirmState = 1
                                    AND bc1.t_StorState = 1
                                    AND EXISTS
                                           (SELECT 1
                                              FROM dnptxtotalbasebc_dbt bc2
                                             WHERE bc2.t_TBID = bc1.t_TBID
                                                   AND bc2.t_Instance =
                                                          bc1.t_Instance - 1
                                                   AND bc2.t_ConfirmState <>
                                                          bc1.t_ConfirmState))
                     AND step.t_ID_Operation = bc.t_ID_Operation
                     AND step.t_ID_Step = bc.t_ID_Step)
    WHERE stb.t_SendDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY')
          AND EXISTS
                 (SELECT step.t_Syst_Date, step.t_Syst_Time
                    FROM dnptxtotalbasebc_dbt bc, doprstep_dbt step
                   WHERE     bc.t_TBID = stb.t_TBID
                         AND bc.t_ConfirmState = 1
                         AND bc.t_StorState = 1
                         AND EXISTS
                                (SELECT 1
                                   FROM dnptxtotalbasebc_dbt bc1
                                  WHERE bc1.t_TBID = bc.t_TBID
                                        AND bc1.t_Instance =
                                               bc.t_Instance - 1
                                        AND bc1.t_ConfirmState <>
                                               bc.t_ConfirmState)
                         AND step.t_ID_Operation = bc.t_ID_Operation
                         AND step.t_ID_Step = bc.t_ID_Step);

  UPDATE dnptxtotalbasebc_dbt bc
     SET (bc.t_SendDate, bc.t_SendTime) =
            (SELECT stb.t_SendDate, stb.t_SendTime from dnptxtotalbase_dbt stb where stb.t_TBID = bc.t_TBID)
   WHERE (bc.t_TBID, bc.t_SendDate) IN
            (SELECT DISTINCT bc2.t_TBID, TO_DATE ('01.01.0001', 'DD.MM.YYYY') 
               FROM dnptxtotalbasebc_dbt bc2
              WHERE bc2.t_ConfirmState = 1 AND bc2.t_StorState = 1)
         AND Exists(select 1 from dnptxtotalbasebc_dbt bc2
                     WHERE     bc2.t_TBID = bc.t_TBID
                           AND bc2.t_ConfirmState = 1
                           AND bc2.t_StorState = 1
                           AND bc2.t_Instance <= bc.t_Instance);

  UPDATE dnptxtotalbase_dbt stb
     SET (stb.t_SendDate, stb.t_SendTime) = (SELECT TO_DATE('01.01.0001','DD.MM.YYYY'), TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS') FROM DUAL) 
   WHERE stb.t_SendDate IS NULL;

  UPDATE dnptxtotalbasebc_dbt bc
     SET (bc.t_SendDate, bc.t_SendTime) = (SELECT TO_DATE('01.01.0001','DD.MM.YYYY'), TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS') FROM DUAL) 
   WHERE bc.t_SendDate IS NULL;

--Для Отмененных

  UPDATE dnptxtotalbase_dbt stb
     SET (stb.t_CancelDate, stb.t_CancelTime) = (SELECT TO_DATE('01.01.0001','DD.MM.YYYY'), TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS') FROM DUAL) 
   WHERE stb.t_CancelDate IS NULL;

  UPDATE dnptxtotalbasebc_dbt bc
     SET (bc.t_CancelDate, bc.t_CancelTime) = (SELECT TO_DATE('01.01.0001','DD.MM.YYYY'), TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS') FROM DUAL) 
   WHERE bc.t_CancelDate IS NULL;


  UPDATE dnptxtotalbase_dbt stb
     SET (stb.t_CancelDate, stb.t_CancelTime) = (SELECT step.t_Syst_Date, step.t_Syst_Time
                                                   FROM doprstep_dbt step
                                                  WHERE step.t_ID_Operation = stb.t_ID_Operation
                                                    AND step.t_ID_Step = stb.t_ID_Step
                                                )
   WHERE stb.t_StorState = 0
     AND stb.t_CancelDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY') 
     AND (stb.t_Instance = 0 OR NOT EXISTS(SELECT 1
                                             FROM dnptxtotalbasebc_dbt bc
                                            WHERE bc.t_TBID = stb.t_TBID
                                              AND bc.t_Instance = stb.t_Instance - 1
                                              AND bc.t_StorState = 0
                                          )
         )
     AND stb.t_ID_Operation > 0;


   UPDATE dnptxtotalbase_dbt stb
     SET (stb.t_CancelDate, stb.t_CancelTime) = (SELECT step.t_Syst_Date, step.t_Syst_Time
                                                   FROM dnptxtotalbasebc_dbt bc, doprstep_dbt step
                                                  WHERE bc.t_TBID = stb.t_TBID
                                                    AND bc.t_Instance = (SELECT MIN(bc1.t_Instance)
                                                                           FROM dnptxtotalbasebc_dbt bc1
                                                                          WHERE bc1.t_TBID = bc.t_TBID
                                                                            AND bc1.t_StorState = 0
                                                                        )
                                                    AND step.t_ID_Operation = bc.t_ID_Operation
                                                    AND step.t_ID_Step = bc.t_ID_Step
                                                )
   WHERE stb.t_CancelDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY') 
     AND stb.t_StorState = 0
     AND EXISTS(SELECT step.t_Syst_Date, step.t_Syst_Time
                  FROM dnptxtotalbasebc_dbt bc, doprstep_dbt step
                 WHERE bc.t_TBID = stb.t_TBID
                   AND bc.t_Instance = (SELECT MIN(bc1.t_Instance)
                                          FROM dnptxtotalbasebc_dbt bc1
                                         WHERE bc1.t_TBID = bc.t_TBID
                                           AND bc1.t_StorState = 0
                                       )
                   AND step.t_ID_Operation = bc.t_ID_Operation
                   AND step.t_ID_Step = bc.t_ID_Step
               );


   UPDATE dnptxtotalbasebc_dbt bc
     SET (bc.t_CancelDate, bc.t_CancelTime) = (SELECT step.t_Syst_Date, step.t_Syst_Time
                                                 FROM dnptxtotalbasebc_dbt bc1, doprstep_dbt step
                                                WHERE bc1.t_TBID = bc.t_TBID
                                                  AND bc1.t_Instance = (SELECT MIN(bc2.t_Instance)
                                                                          FROM dnptxtotalbasebc_dbt bc2
                                                                         WHERE bc2.t_TBID = bc1.t_TBID
                                                                           AND bc2.t_StorState = 0
                                                                       )
                                                  AND bc1.t_Instance <= bc.t_Instance
                                                  AND step.t_ID_Operation = bc1.t_ID_Operation
                                                  AND step.t_ID_Step = bc1.t_ID_Step
                                              )
   WHERE bc.t_CancelDate = TO_DATE ('01.01.0001', 'DD.MM.YYYY') 
     AND bc.t_StorState = 0
     AND EXISTS (SELECT step.t_Syst_Date, step.t_Syst_Time
                   FROM dnptxtotalbasebc_dbt bc1, doprstep_dbt step
                  WHERE bc1.t_TBID = bc.t_TBID
                    AND bc1.t_Instance = (SELECT MIN(bc2.t_Instance)
                                            FROM dnptxtotalbasebc_dbt bc2
                                           WHERE bc2.t_TBID = bc1.t_TBID
                                             AND bc2.t_StorState = 0
                                         )
                    AND bc1.t_Instance <= bc.t_Instance
                    AND step.t_ID_Operation = bc1.t_ID_Operation
                    AND step.t_ID_Step = bc1.t_ID_Step
                );  

  UPDATE dnptxtotalbase_dbt stb
     SET (stb.t_CancelDate, stb.t_CancelTime) = (SELECT TO_DATE('01.01.0001','DD.MM.YYYY'), TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS') FROM DUAL) 
   WHERE stb.t_CancelDate IS NULL;

  UPDATE dnptxtotalbasebc_dbt bc
     SET (bc.t_CancelDate, bc.t_CancelTime) = (SELECT TO_DATE('01.01.0001','DD.MM.YYYY'), TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS') FROM DUAL) 
   WHERE bc.t_CancelDate IS NULL;
END;
/


declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASE_DBT_IDX_CD' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASE_DBT_IDX_CD';
  end if;
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASE_DBT_IDX_SD' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASE_DBT_IDX_SD';
  end if;
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_CD' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_CD';
  end if;
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_SD' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_SD';
  end if;
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_U1' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_U1';
  end if;
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_U2' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_U2';
  end if;
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_U3' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_U3';
  end if;
end;
/

declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_U4' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_U4';
  end if;
end;
/


declare
  cnt number;
begin
  select count(*) into cnt from user_indexes i where i.INDEX_NAME = 'DNPTXTOTALBASEBC_DBT_IDX_U5' ;
  if cnt = 1 then
     execute immediate 'drop index DNPTXTOTALBASEBC_DBT_IDX_U5';
  end if;
end;
/

