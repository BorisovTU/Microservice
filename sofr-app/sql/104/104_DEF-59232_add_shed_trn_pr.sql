BEGIN
   INSERT INTO DITEMSYST_DBT(T_CIDENTPROGRAM,T_ICASEITEM,T_IKINDMETHOD,T_IKINDPROGRAM,T_IHELP,T_RESERVE,T_SZNAMEITEM,T_PARM )
               VALUES('Г',11005,1,2,0,CHR(1),'Рассылка (email) отчета о запрещенных проводках','00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000436865636B54726E50726F68426C536865642E6D61630000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');
               
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE DTRN_PROHIBITED_DBT ADD T_SYSTEMDATE DATE';
   EXECUTE IMMEDIATE 'ALTER TABLE DTRN_PROHIBITED_DBT ADD T_SYSTEMTIME DATE';
   EXECUTE IMMEDIATE 'ALTER TABLE DTRN_PROHIBITED_DBT ADD T_TRNRUL_ID NUMBER(10)';
   EXECUTE IMMEDIATE 'ALTER TABLE DTRN_PROHIBITED_DBT ADD T_TRNRUL_COND VARCHAR2(200)';
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/

DECLARE
   v_ParentId NUMBER;
   v_cnt NUMBER := 0;
   v_ID  NUMBER := 0;
BEGIN
   SELECT t_KeyId INTO v_ParentId FROM DREGPARM_DBT WHERE LOWER(T_NAME) = LOWER('РСХБ');

   IF (v_ParentId <> 0) THEN 
      SELECT COUNT(*) INTO v_cnt FROM DREGPARM_DBT WHERE t_ParentId = v_ParentId AND t_Name = 'РАССЫЛКА_ОТЧ_ЗАПР_ПРОВОДОК';
      
      IF v_cnt = 0 THEN
          INSERT INTO DREGPARM_DBT 
            (T_KEYID, T_PARENTID, T_NAME, T_TYPE, T_GLOBAL, T_DESCRIPTION, T_SECURITY, T_ISBRANCH, T_TEMPLATE)
          VALUES 
            (0, v_ParentId, 'РАССЫЛКА_ОТЧ_ЗАПР_ПРОВОДОК', 2, CHR(0),'Email для рассылки отчета запрещенных проводок, разделитель между email - ;', CHR(0), CHR(0), CHR(1)) RETURNING T_KEYID INTO v_ID;

         INSERT INTO DREGVAL_DBT 
           (T_KEYID,T_REGKIND,T_OBJECTID,T_BLOCKUSERVALUE,T_EXPDEP,T_LINTVALUE,T_LDOUBLEVALUE,T_FMTBLOBDATA_XXXX)
         VALUES
           (v_ID,0,0,CHR(0),0,0,0,to_blob(utl_raw.cast_to_raw('bo_securities@rshb.ru;MMBASE@rshb.ru')));
     END IF;
   END IF;
        
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/

BEGIN
INSERT INTO DSHEDULE_DBT (T_ID,
                          T_CIDENTPROGRAM,
                          T_EVENTTYPE,
                          T_PERIODICALEVENT,
                          T_STARTDATE,
                          T_STARTTIME,
                          T_ENDDATE,
                          T_ENDTIME,
                          T_NEXTDATE,
                          T_NEXTTIME,
                          T_PERIODTYPE,
                          T_PERIODLENGTH,
                          T_WORKDAYS,
                          T_DAYSOFWEEK,
                          T_DAYSOFMONTH,
                          T_STATUS,
                          T_ACTION,
                          T_PARMS,
                          T_PAUSED,
                          T_COMMENT,
                          T_SYSEVENTS,
                          T_DEPARTMENT,
                          T_PRIORITY,
                          T_OPENPHASE,
                          T_USEREVENTCODE,
                          T_USEDAYEVENT,
                          T_EVENTDAYORDER,
                          T_EVENTDAYKIND,
                          T_EVENTPERIODTYPE,
                          T_NOTIFYOFERROR,
                          T_NOTIFYOFCOMPLETION,
                          T_ONEXACTTIME,
                          T_EXACTTIME,
                          T_NOTIFYOFACTCOMPLETE,
                          T_RUNACTIONSCHAIN)
        VALUES (
                  0,
                  'Г',
                  CHR (0),
                  CHR (88),
                  TO_DATE ('01.01.2001', 'dd.mm.yyyy'),
                  TO_DATE ('01.01.2001 08:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                  TO_DATE ('01.01.0001', 'dd.mm.yyyy'),
                  TO_DATE ('01.01.0001 00:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                  TO_DATE ('01.01.2001', 'dd.mm.yyyy'),
                  TO_DATE ('01.01.0001 08:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                  4,
                  1,
                  CHR (0),
                  0,
                  0,
                  0,
                  1,
                  '-exec:11005',
                  CHR (0),
                  'Рассылка (email) отчета о запрещенных проводках',
                  0,
                  1,
                  0,
                  0,
                  CHR (1),
                  CHR (0),
                  0,
                  0,
                  0,
                  CHR (0),
                  CHR (0),
                  CHR (88),
                  TO_DATE ('01.01.2001 08:00:00', 'dd.mm.yyyy hh24:mi:ss'),
                  CHR (0),
                  CHR (0));
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/