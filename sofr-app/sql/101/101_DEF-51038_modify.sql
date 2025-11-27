-- Изменения по DEF-51038, Новое заполнение таблицы DBROKACC_ACC_DBT
DECLARE
  logID VARCHAR2(9) := 'DEF-51038';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Новое заполнение таблицы DBROKACC_ACC_DBT
  PROCEDURE FillDBrokAccAcc
  AS
  BEGIN
    LogIt('Удаление данных в таблице ''DBROKACC_ACC_DBT''');
    execute immediate 'truncate table DBROKACC_ACC_DBT';
    LogIt('Удалены данные в таблице ''DBROKACC_ACC_DBT''');

    --добавим заново по ЕДП
    LogIt('Заполнение таблицы ''DBROKACC_ACC_DBT'' по ЕДП');
    INSERT INTO DBROKACC_ACC_DBT (T_BROKACC_KEY,T_ACCOUNTID ) 
    ( 
       SELECT brok.t_autokey ,j.t_accountid                                                                                                                                        
         FROM dmcaccdoc_dbt mcdoc, dmccateg_dbt mccat, dbrokacc_dbt brok, DACCOUNT_DBT j, dparty_dbt E , DPERSN_DBT P
         WHERE  mccat.T_ID = mcdoc.T_CATID
         AND mccat.t_id in(70,344)
         AND E.T_PARTYID = J.T_CLIENT
         AND E.T_LEGALFORM = 2
         AND P.T_PERSONID =  J.T_CLIENT
         AND P.T_ISEMPLOYER <> CHR(88)
         AND instr(t_usertypeaccount, chr(133)) != 0
         AND brok.T_SERVKIND = 0
         AND brok.T_SERVKINDSUB = 0
         AND brok.T_CURRENCY = mcdoc.T_CURRENCY
         AND mcdoc.T_ACCOUNT =j.T_ACCOUNT
         AND J.T_CHAPTER = mcdoc.T_CHAPTER
         AND J.T_CODE_CURRENCY = mcdoc.T_CURRENCY
         AND SUBSTR(brok.T_ACCOUNT, 1, 5) = SUBSTR(mcdoc.T_ACCOUNT, 1, 5)
         AND (mcdoc.T_ISCOMMON = 'X' or ( ( mcdoc.T_ISCOMMON = chr(0) ) and (mcdoc.t_disablingdate <> to_date('01010001','ddmmyyyy')) and (mcdoc.t_dockind = 0) ) )
         AND J.T_ACCOUNTID NOT IN (SELECT A.T_ACCOUNTID FROM DBROKACC_ACC_DBT A WHERE A.T_ACCOUNTID = J.T_ACCOUNTID AND A.T_BROKACC_KEY = brok.T_AUTOKEY)
         GROUP BY brok.t_autokey , j.t_accountid
    );
    LogIt('Таблицы ''DBROKACC_ACC_DBT'' заполнена по ЕДП');

    --добавим заново не по ЕДП
    LogIt('Заполнение таблицы ''DBROKACC_ACC_DBT'' по не ЕДП');
    INSERT INTO DBROKACC_ACC_DBT ( T_BROKACC_KEY, T_ACCOUNTID ) 
    ( 
       SELECT brok.t_autokey,j.t_accountid
         FROM dmcaccdoc_dbt mcdoc, dmccateg_dbt B, dsfcontr_dbt C, dbrokacc_dbt brok ,DACCOUNT_DBT j, dparty_dbt E, DPERSN_DBT P
         WHERE  B.T_ID = mcdoc.T_CATID
         AND b.t_id in(70,344)
         AND E.T_PARTYID = J.T_CLIENT
         AND E.T_LEGALFORM = 2
         AND P.T_PERSONID =  J.T_CLIENT
         AND P.T_ISEMPLOYER <> CHR(88)
         AND instr(t_usertypeaccount, chr(133)) = 0
         AND C.T_ID = mcdoc.T_CLIENTCONTRID
         AND brok.T_SERVKIND = C.T_SERVKIND
         AND brok.T_SERVKINDSUB = C.T_SERVKINDSUB
         AND brok.T_CURRENCY = mcdoc.T_CURRENCY
         AND mcdoc.T_ACCOUNT =j.T_ACCOUNT
         AND J.T_CHAPTER = mcdoc.T_CHAPTER
         AND J.T_CODE_CURRENCY = mcdoc.T_CURRENCY
         AND  SUBSTR(brok.T_ACCOUNT, 1, 5) = SUBSTR(mcdoc.T_ACCOUNT, 1, 5)
         AND ((mcdoc.T_ISCOMMON = 'X') or ( ( mcdoc.T_ISCOMMON = chr(0) ) and (t_disablingdate <> to_date('01010001','ddmmyyyy')) and (t_dockind = 0) )
          or (( mcdoc.T_ISCOMMON = chr(0) ) and (t_dockind = 3001) ) )
         AND J.T_ACCOUNTID NOT IN (SELECT A.T_ACCOUNTID FROM DBROKACC_ACC_DBT A WHERE A.T_ACCOUNTID = J.T_ACCOUNTID AND A.T_BROKACC_KEY = brok.T_AUTOKEY)
         GROUP BY brok.t_autokey, j.t_accountid 
    );
    LogIt('Таблицы ''DBROKACC_ACC_DBT'' заполнена по ЕДП');

    --обновим сохраненные id
    LogIt('обновление id для DBROKACC_ACC_DBT');
    UPDATE DCOMPARE_SETTINGS_DBT 
      SET t_value = (select max(t_id) from DMCACCDOC_DBT ) 
      WHERE t_key = 'MCDOCACC_LAST_MAXTVAL'
    ;
    UPDATE DCOMPARE_SETTINGS_DBT 
      SET t_value = (SELECT last_number from user_sequences WHERE sequence_name = 'DMCACCDOC_DBT_SEQ' ) 
      WHERE t_key = 'MCDOCACC_SEQ_LASTVAL';
    LogIt('обновлены id для DBROKACC_ACC_DBT');

    execute immediate 'COMMIT';
    LogIt('Завершено заполнение таблицы ''DBROKACC_ACC_DBT''');

  EXCEPTION
    WHEN OTHERS THEN
       LogIt('Ошибка заполнения таблицы ''DBROKACC_ACC_DBT''');
       execute immediate 'ROLLBACK';
  END;
BEGIN
  -- Новое заполнение таблицы DBROKACC_ACC_DBT
  FillDBrokAccAcc();
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/

