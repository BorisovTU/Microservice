CREATE OR REPLACE PACKAGE BODY RSB_TEST_GT is

  function GetReplicRecords(p_ApplicationCond IN VARCHAR2,
                            p_SeanseDate      IN DATE,
                            p_ObjType         IN VARCHAR2,
                            p_SeanseId        IN INTEGER default 0) return clob is
    l_Sql  CLOB;
    l_Clob CLOB;
  begin
    l_Sql := 'SELECT XMLAgg(xmlelement("OBJECT",
                              xmlattributes(obj.t_objectid as "ObjectId",
                                            obj.t_objectkind as "ObjectKind",
                                            obj.t_name as "Name",
                                            obj.t_sysdate as "SysDate",
                                            to_char(obj.t_systime, ''HH24:MI:SS'') as "SysTime"),
                              (select XMLAgg(xmlelement("CODE",
                                               xmlattributes(obj.t_objectid as "ObjectId",
                                                             c.t_codeid as "CodeId",
                                                             c.t_applicationid as "ApplicationId",
                                                             c.t_objectcode as "ObjectCode",
                                                             c.t_objectkind as "ObjectKind")))
                                 from DGTCODE_DBT c
                                where c.t_objectid = obj.t_objectid
                                  and c.t_applicationid in ('||p_ApplicationCond||')),
                              xmlelement("RECORD",
                                xmlattributes(obj.t_objectid as "ObjectId",
                                              rec.t_recordid as "RecordId",
                                              rec.t_applicationid_from as "ApplicationIdFrom",
                                              rec.t_applicationid_to as "ApplicationIdTo",
                                              rec.t_actionid as "ActionId",
                                              rec.t_statusid as "StatusId",
                                              rec.t_sysdate as "SysDate",
                                              to_char(rec.t_systime, ''HH24:MI:SS'') as "SysTime",
                                              rec.t_clientid as "ClientId"),
                                (select XMLAgg(xmlelement("RECPRM",
                                                 xmlattributes(rec.t_recordid as "RecordId",
                                                               rp.t_id as "Id",
                                                               rp.t_koprmid as "KoprmId",
                                                               rp.t_refapplicationid as "RefApplicationId",
                                                               rp.t_intval as "IntVal",
                                                               rp.t_moneyval as "MoneyVal",
                                                               rp.t_doubleval as "DoubleVal",
                                                               decode(rp.t_stringval, chr(0), null,
                                                                                      chr(1), null,
                                                                                      rp.t_stringval) as "StringVal",
                                                               rp.t_dateval as "DateVal",
                                                               to_char(rp.t_timeval, ''HH24:MI:SS'') as "TimeVal",
                                                               k.t_objectkind as "ObjectKind",
                                                               k.t_code as "Code")))
                                   from DGTRECPRM_DBT rp,
                                        DGTKOPRM_DBT k
                                  where rp.t_recordid = rec.t_recordid
                                    and rp.t_koprmid = k.t_koprmid)))).getclobval()
                FROM DGTOBJECT_DBT obj,
                     DGTSNCREC_DBT rs,
                     DGTRECORD_DBT rec,
                     DGTSEANCE_DBT s
               WHERE obj.t_objectid = rs.t_objectid
                 AND obj.t_objectkind IN('||p_ObjType||')
                 AND s.t_seanceid = rs.t_seanceid
                 AND obj.t_objectid = rec.t_objectid
                 AND to_date(to_char(s.t_sysdate, ''DD.MM.YYYY'')) = :p_SeanseDate
                 AND s.t_applicationid IN('||p_ApplicationCond||')
                 AND s.t_kind = 1
                 AND s.t_direction = 1
                 AND rs.t_issue = 0'||
                 CASE WHEN p_SeanseId > 0
                      THEN 'AND '||p_SeanseId||' = s.T_SEANCEID'
                 END;
    execute immediate l_Sql into l_Clob using p_SeanseDate;
    IF length(l_Clob) > 0 THEN
      l_Clob := '<RECORDS>'||chr(10)||
                   l_Clob||chr(10)||'
                 </RECORDS>';
      SELECT xmlroot(xmltype(l_Clob), version '1.0" encoding="WINDOWS-1251').getclobval()
        INTO l_Clob
        FROM dual;
    END IF;
    Return l_Clob;
  end GetReplicRecords;

  procedure DeleteReplicRecords as
  begin
    DELETE FROM DGTOBJECT_REPL_DBT;
    DELETE FROM DGTCODE_REPL_DBT;
    DELETE FROM DGTRECORD_REPL_DBT;
    DELETE FROM DGTRECPRM_REPL_DBT;
  end DeleteReplicRecords;

  procedure LoadReplicObject(p_Clob IN CLOB) as
  begin
    INSERT INTO DGTOBJECT_REPL_DBT(T_OBJECTID,
                                   T_OBJECTKIND,
                                   T_NAME,
                                   T_SYSDATE,
                                   T_SYSTIME)
    SELECT x.T_OBJECTID, x.T_OBJECTKIND, x.T_NAME, to_date(x.T_SYSDATE, 'yyyy-mm-dd'),
           to_date(to_char('01.01.0001'||' '||x.T_SYSTIME), 'dd.mm.yyyy hh24:mi:ss')
      FROM XMLTABLE('//OBJECT' PASSING xmltype(p_Clob)
                    COLUMNS T_OBJECTID   PATH '@ObjectId',
                            T_OBJECTKIND PATH '@ObjectKind',
                            T_NAME       PATH '@Name',
                            T_SYSDATE    PATH '@SysDate',
                            T_SYSTIME    PATH '@SysTime') x;
  end LoadReplicObject;

  procedure LoadReplicObjectCode(p_Clob IN CLOB) as
  begin
    INSERT INTO DGTCODE_REPL_DBT(T_CODEID,
                                 T_OBJECTID,
                                 T_APPLICATIONID,
                                 T_OBJECTCODE,
                                 T_OBJECTKIND)
    SELECT x.T_CODEID, x.T_OBJECTID, x.T_APPLICATIONID, T_OBJECTCODE, T_OBJECTKIND
      FROM XMLTABLE('//CODE' PASSING xmltype(p_Clob)
                    COLUMNS T_CODEID        PATH '@CodeId',
                            T_OBJECTID      PATH '@ObjectId',
                            T_APPLICATIONID PATH '@ApplicationId',
                            T_OBJECTCODE    PATH '@ObjectCode',
                            T_OBJECTKIND    PATH '@ObjectKind') x;
  end LoadReplicObjectCode;

  procedure LoadReplicRecord(p_Clob IN CLOB) as
  begin
    INSERT INTO DGTRECORD_REPL_DBT(T_RECORDID,
                                   T_OBJECTID,
                                   T_APPLICATIONID_FROM,
                                   T_APPLICATIONID_TO,
                                   T_ACTIONID,
                                   T_STATUSID,
                                   T_SYSDATE,
                                   T_SYSTIME,
                                   T_CLIENTID)
    SELECT x.T_RECORDID, x.T_OBJECTID, x.T_APPLICATIONID_FROM, x.T_APPLICATIONID_TO, x.T_ACTIONID, x.T_STATUSID,
           to_date(x.T_SYSDATE, 'yyyy-mm-dd'), to_date(to_char('01.01.0001'||' '||x.T_SYSTIME), 'dd.mm.yyyy hh24:mi:ss'), x.T_CLIENTID
      FROM XMLTABLE('//RECORD' PASSING xmltype(p_Clob)
                    COLUMNS T_RECORDID           PATH '@RecordId',
                            T_OBJECTID           PATH '@ObjectId',
                            T_APPLICATIONID_FROM PATH '@ApplicationIdFrom',
                            T_APPLICATIONID_TO   PATH '@ApplicationIdTo',
                            T_ACTIONID           PATH '@ActionId',
                            T_STATUSID           PATH '@StatusId',
                            T_SYSDATE            PATH '@SysDate',
                            T_SYSTIME            PATH '@SysTime',
                            T_CLIENTID           PATH '@ClientId') x;
  end LoadReplicRecord;

  procedure LoadReplicRecordParam(p_Clob IN CLOB) as
  begin
    INSERT INTO DGTRECPRM_REPL_DBT(T_ID,
                                   T_RECORDID,
                                   T_KOPRMID,
                                   T_REFAPPLICATIONID,
                                   T_INTVAL,
                                   T_MONEYVAL,
                                   T_DOUBLEVAL,
                                   T_STRINGVAL,
                                   T_DATEVAL,
                                   T_TIMEVAL,
                                   T_OBJECTKIND,
                                   T_CODE)
    SELECT x.T_ID, x.T_RECORDID, x.T_KOPRMID, x.T_REFAPPLICATIONID, x.T_INTVAL, x.T_MONEYVAL, x.T_DOUBLEVAL, nvl(x.T_STRINGVAL, chr(1)),
           to_date(x.T_DATEVAL, 'yyyy-mm-dd'), to_date(to_char('01.01.0001'||' '||x.T_TIMEVAL), 'dd.mm.yyyy hh24:mi:ss'),
           x.T_OBJECTKIND, x.T_CODE
      FROM XMLTABLE('//RECPRM' PASSING xmltype(p_Clob)
                    COLUMNS T_ID               PATH '@Id',
                            T_RECORDID         PATH '@RecordId',
                            T_KOPRMID          PATH '@KoprmId',
                            T_REFAPPLICATIONID PATH '@RefApplicationId',
                            T_INTVAL           PATH '@IntVal',
                            T_MONEYVAL         PATH '@MoneyVal',
                            T_DOUBLEVAL        PATH '@DoubleVal',
                            T_STRINGVAL        PATH '@StringVal',
                            T_DATEVAL          PATH '@DateVal',
                            T_TIMEVAL          PATH '@TimeVal',
                            T_OBJECTKIND       PATH '@ObjectKind',
                            T_CODE             PATH '@Code') x;
  end LoadReplicRecordParam;

  procedure LoadReplicRecords(p_Clob IN CLOB) as
  begin
    RSB_TEST_GT.DeleteReplicRecords;
    RSB_TEST_GT.LoadReplicObject(p_Clob);
    RSB_TEST_GT.LoadReplicObjectCode(p_Clob);
    RSB_TEST_GT.LoadReplicRecord(p_Clob);
    RSB_TEST_GT.LoadReplicRecordParam(p_Clob);
  end LoadReplicRecords;

  procedure AppendToFileStorageClob(p_id IN NUMBER, p_chunk IN CLOB)
    IS
     v_clob   CLOB;
     null_count NUMBER;
  begin
     SELECT T_FILE_CONTENT
       INTO v_clob
       FROM dfile_storage_dbt
      WHERE T_ID = p_id
     FOR UPDATE;

     if (v_clob is null) THEN
        update dfile_storage_dbt set T_FILE_CONTENT = p_chunk where T_ID = p_id;
     else
        DBMS_LOB.append (v_clob, p_chunk);
     end if;

     COMMIT;
  end AppendToFileStorageClob;

  function GetReportCompare(p_ApplicationCond IN VARCHAR2,
                            p_SeanseDate      IN DATE,
                            p_ObjType         IN VARCHAR2,
                            p_SeanseId        IN INTEGER default 0) return clob is
    l_Clob CLOB;
    l_Obj  CLOB;
    l_Rec  CLOB;
  begin
    --Сверка шлюза DGTOBJECT_DBT
    l_Obj := RSB_TEST_GT.CompareObject(p_ApplicationCond,
                                       p_SeanseDate,
                                       p_ObjType,
                                       p_SeanseId);
    IF l_Obj IS NOT NULL THEN
      l_Clob := l_Obj;
    END IF;

    --Сверка шлюза DGTRECORD_DBT
    l_Rec := RSB_TEST_GT.CompareObjectRecord();
    IF l_Rec IS NOT NULL THEN
      IF l_Clob IS NULL THEN
        l_Clob := l_Rec;
      ELSE
        l_Clob := l_Clob||chr(10)||l_Rec;
      END IF;
    END IF;
dbms_output.put_line(substr(l_Clob, 1, 4000));
    Return l_Clob;
  end GetReportCompare;

  function CompareObject(p_ApplicationCond IN VARCHAR2,
                         p_SeanseDate      IN DATE,
                         p_ObjType         IN VARCHAR2,
                         p_SeanseId        IN INTEGER default 0) return clob is
    l_Sql_ObjGt   CLOB;
    l_Sql_ObjRepl CLOB;
    l_Sql         CLOB;
    l_Clob        CLOB;
    l_Obj         CLOB;
    l_Code        CLOB;
  begin
    delete from DGTREPLOBJECT_TMP;
    l_Sql_ObjGt := 'SELECT obj.t_objectid, obj.t_name
                      FROM DGTOBJECT_DBT obj,
                           DGTSNCREC_DBT rs,
                           DGTRECORD_DBT rec,
                           DGTSEANCE_DBT s
                     WHERE obj.t_objectid = rs.t_objectid
                       AND obj.t_objectkind IN('||p_ObjType||')
                       AND s.t_seanceid = rs.t_seanceid
                       AND obj.t_objectid = rec.t_objectid
                       AND to_date(to_char(s.t_sysdate, ''DD.MM.YYYY'')) = :p_SeanseDate
                       AND s.t_applicationid IN('||p_ApplicationCond||')
                       AND s.t_kind = 1
                       AND s.t_direction = 1
                       AND rs.t_issue = 0'||
                       CASE WHEN p_SeanseId > 0
                            THEN 'AND '||p_SeanseId||' = s.T_SEANCEID'
                       END;
    l_Sql_ObjRepl := 'SELECT obj.t_objectid, obj.t_name
                        FROM DGTOBJECT_REPL_DBT obj';
    l_Sql := 'INSERT INTO DGTREPLOBJECT_TMP(T_OBJECTID_GT, T_OBJECTID_REPL)
                SELECT gt.t_objectid gtid, repl.t_objectid replid
                  FROM ('||l_Sql_ObjGt||') gt,
                       ('||l_Sql_ObjRepl||') repl
                 WHERE gt.t_name = repl.t_name(+)';
    execute immediate l_Sql using p_SeanseDate;

    FOR c IN(SELECT t.T_OBJECTID_GT
               FROM DGTREPLOBJECT_TMP t
              WHERE t.T_OBJECTID_REPL IS NULL) LOOP
      IF l_Clob IS NULL THEN
        l_Clob := 'Объект шлюза ИД='||c.T_OBJECTID_GT||' не найден в ЗР (DGTOBJECT_REPL)';
      ELSE
        l_Obj := 'Объект шлюза ИД='||c.T_OBJECTID_GT||' не найден в ЗР (DGTOBJECT_REPL)';
        l_Clob := l_Clob||chr(10)||l_Obj;
      END IF;
    END LOOP;

    FOR c IN(SELECT r.t_objectid
               FROM DGTOBJECT_REPL_DBT r
              WHERE r.t_objectid NOT IN(SELECT t.T_OBJECTID_REPL
                                          FROM DGTREPLOBJECT_TMP t
                                         WHERE t.T_OBJECTID_REPL IS NOT NULL)) LOOP
      IF l_Clob IS NULL THEN
        l_Clob := 'Объект ЗР ИД='||c.t_objectid||' не найден в шлюзе (DGTOBJECT)';
      ELSE
        l_Obj := 'Объект ЗР ИД='||c.t_objectid||' не найден в шлюзе (DGTOBJECT)';
        l_Clob := l_Clob||chr(10)||l_Obj;
      END IF;
    END LOOP;

    FOR s IN(SELECT t.T_OBJECTID_GT, t.t_objectid_repl
               FROM DGTREPLOBJECT_TMP t
              WHERE t.T_OBJECTID_REPL IS NOT NULL
              ORDER BY t.T_OBJECTID_GT) LOOP
      l_Code := RSB_TEST_GT.CompareObjectCode(s.t_objectid_gt, s.t_objectid_repl, ','||p_ApplicationCond||',');
      IF l_Code IS NOT NULL THEN
        IF l_Clob IS NULL THEN
          l_Clob := l_Code;
        ELSE
          l_Clob := l_Clob||chr(10)||l_Code;
        END IF;
      END IF;
    END LOOP;
    Return l_Clob;
  end CompareObject;

  function CompareObjectCode(p_ObjGtId IN INTEGER, p_ObjReplId IN INTEGER, p_ApplicationCond IN VARCHAR2) return clob is
    l_Clob CLOB;
    l_Code CLOB;
    l_CodeId INTEGER;
  begin
    BEGIN
      SELECT distinct gt.t_codeid
        INTO l_CodeId
               FROM (select g.*
                       from DGTCODE_DBT g
               where g.t_objectid = p_ObjGtId
                 and instr(p_ApplicationCond, ','||g.t_applicationid||',') <> 0) gt,
                     (select r.*
                        from DGTCODE_REPL_DBT r
               where r.t_objectid = p_ObjReplId
                 and instr(p_ApplicationCond, ','||r.t_applicationid||',') <> 0) repl
       WHERE gt.t_objectcode <> repl.t_objectcode
          OR gt.t_objectkind <> repl.t_objectkind;
      IF l_Clob IS NULL THEN
        l_Clob := 'Идентификатор ИД='||l_CodeId||' объекта шлюза ИД='||p_ObjGtId||' не найден в ЗР (DGTCODE_REPL_DBT)';
      ELSE
        l_Code := 'Идентификатор ИД='||l_CodeId||' объекта шлюза ИД='||p_ObjGtId||' не найден в ЗР (DGTCODE_REPL_DBT)';
        l_Clob := l_Clob||chr(10)||l_Code;
      END IF;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;

    BEGIN
      SELECT distinct repl.t_codeid
        INTO l_CodeId
               FROM (select g.*
                       from DGTCODE_DBT g
               where g.t_objectid = p_ObjGtId
                 and instr(p_ApplicationCond, ','||g.t_applicationid||',') <> 0) gt,
                     (select r.*
                        from DGTCODE_REPL_DBT r
               where r.t_objectid = p_ObjReplId
                 and instr(p_ApplicationCond, ','||r.t_applicationid||',') <> 0) repl
       WHERE gt.t_objectcode <> repl.t_objectcode
          OR gt.t_objectkind <> repl.t_objectkind;
      IF l_Clob IS NULL THEN
        l_Clob := 'Идентификатор ИД='||l_CodeId||' объекта ЗР ИД='||p_ObjReplId||' не найден в шлюзе (DGTCODE_DBT)';
      ELSE
         l_Code := 'Идентификатор ИД='||l_CodeId||' объекта ЗР ИД='||p_ObjReplId||' не найден в шлюзе (DGTCODE_DBT)';
        l_Clob := l_Clob||chr(10)||l_Code;
      END IF;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        NULL;
    END;
    Return l_Clob;
  end CompareObjectCode;

  function CompareObjectRecord return clob is
    l_Rec  CLOB;
    l_Parm CLOB;
    l_Clob CLOB;
  begin
    delete from DGTREPLRECORD_TMP;
    INSERT INTO DGTREPLRECORD_TMP(T_OBJECTID_GT, T_RECORDID_GT, T_APPLICATIONID_TO_GT, T_CLIENTID_GT,
                                  T_OBJECTID_REPL, T_RECORDID_REPL, T_APPLICATIONID_TO_REPL, T_CLIENTID_REPL)
      SELECT gt.t_objectid, gt.t_recordid, gt.t_applicationid_to, gt.t_clientid,
             repl.t_objectid, repl.t_recordid, repl.t_applicationid_to, repl.t_clientid
        FROM DGTRECORD_DBT gt,
             DGTREPLOBJECT_TMP t,
             (select r.t_objectid, r.t_recordid, r.t_applicationid_to, r.t_clientid, o.t_objectid_gt
                from DGTRECORD_REPL_DBT r,
                     DGTREPLOBJECT_TMP o
               where r.t_objectid = o.t_objectid_repl) repl
       WHERE gt.t_objectid = t.t_objectid_gt
         AND t.t_objectid_repl is not null
         AND repl.t_objectid_gt = t.t_objectid_gt;

    FOR k IN(SELECT t.t_objectid_gt, t.t_recordid_gt
               FROM DGTREPLRECORD_TMP t
              WHERE t.t_recordid_repl IS NULL
                 OR (t.t_applicationid_to_gt <> t.t_applicationid_to_repl
                     or t.t_clientid_gt <> t.t_clientid_repl)) LOOP
      IF l_Clob IS NULL THEN
        l_Clob := 'Запись ИД='||k.t_recordid_gt||' объекта шлюза ИД='||k.t_objectid_gt||' не найдена в ЗР (DGTRECORD_REPL_DBT)';
      ELSE
        l_Rec := 'Запись ИД='||k.t_recordid_gt||' объекта шлюза ИД='||k.t_objectid_gt||' не найдена в ЗР (DGTRECORD_REPL_DBT)';
        l_Clob := l_Clob||chr(10)||l_Rec;
      END IF;
    END LOOP;

    FOR s IN(SELECT t.t_recordid_gt, t.t_recordid_repl
               FROM DGTREPLRECORD_TMP t
              WHERE t.t_recordid_repl IS NOT NULL
                AND t.t_applicationid_to_gt = t.t_applicationid_to_repl
                AND t.t_clientid_gt = t.t_clientid_repl) LOOP
      l_Parm := RSB_TEST_GT.CompareRecordParam(s.t_recordid_gt, s.t_recordid_repl);
      IF l_Parm IS NULL THEN
        CONTINUE;
      ELSE
        IF l_Clob IS NULL THEN
          l_Clob := l_Parm;
        ELSE
          l_Clob := l_Clob||chr(10)||l_Parm;
        END IF;
      END IF;
    END LOOP;
    Return l_Clob;
  end CompareObjectRecord;

  function CompareRecordParam(p_RecGtId IN INTEGER, p_RecReplId IN INTEGER) return clob is
    l_Parm CLOB;
    l_Clob CLOB;
  begin
    delete from DGTREPLRECORDPRM_TMP;
    INSERT INTO DGTREPLRECORDPRM_TMP(T_PARAMID_GT, T_PARAMID_REPL)
      SELECT gt.t_id, repl.t_id
        FROM (select g.*, k.t_objectkind, k.t_code
                from DGTRECPRM_DBT g,
                     DGTKOPRM_DBT k
               where g.t_recordid = p_RecGtId
                 and g.t_koprmid = k.t_koprmid
                 and not g.t_koprmid in (RGDLTC_MARKETREPORT_ID_DEAL, RGDLTC_MARKETREPORT_ID_CLEARING, RGDVNDL_MARKETREPORT_ID, RGDVFXDL_MARKETREPORT_ID, RGDVDL_MARKETREPORT_ID)) gt,
             (select r.*
                from DGTRECPRM_REPL_DBT r
               where r.t_recordid = p_RecReplId
                 and not r.t_koprmid in (RGDLTC_MARKETREPORT_ID_DEAL, RGDLTC_MARKETREPORT_ID_CLEARING, RGDVNDL_MARKETREPORT_ID, RGDVFXDL_MARKETREPORT_ID, RGDVDL_MARKETREPORT_ID)) repl
       WHERE gt.t_objectkind = repl.t_objectkind(+)
         AND gt.t_code = repl.t_code(+)
         AND gt.t_refapplicationid = repl.t_refapplicationid(+)
         AND gt.t_intval = repl.t_intval(+)
         AND ABS(gt.t_moneyval - nvl(repl.t_moneyval(+),0)) <= 0.00000001 --Может быть погрешность при хранении
         AND ABS(gt.t_doubleval - nvl(repl.t_doubleval(+),0)) <= 0.00000001 --Может быть погрешность при хранении
         AND gt.t_stringval = repl.t_stringval(+)
         AND gt.t_dateval = repl.t_dateval(+)
         AND gt.t_timeval = repl.t_timeval(+);

    FOR c IN(SELECT t.t_paramid_gt
               FROM DGTREPLRECORDPRM_TMP t
              WHERE t.t_paramid_repl IS NULL) LOOP
      IF l_Clob IS NULL THEN
        l_Clob := 'Параметр ИД='||c.t_paramid_gt||' записи шлюза ИД='||p_RecGtId||' не найден в ЗР (DGTRECPRM_REPL_DBT)';
      ELSE
        l_Parm := 'Параметр ИД='||c.t_paramid_gt||' записи шлюза ИД='||p_RecGtId||' не найден в ЗР (DGTRECPRM_REPL_DBT)';
        l_Clob := l_Clob||chr(10)||l_Parm;
      END IF;
    END LOOP;

    FOR c IN(SELECT r.t_id
               FROM DGTRECPRM_REPL_DBT r
              WHERE r.t_recordid = p_RecReplId
                AND NOT r.t_koprmid IN (RGDLTC_MARKETREPORT_ID_DEAL, RGDLTC_MARKETREPORT_ID_CLEARING, RGDVNDL_MARKETREPORT_ID, RGDVFXDL_MARKETREPORT_ID, RGDVDL_MARKETREPORT_ID)
                AND r.t_id NOT IN(SELECT t.t_paramid_repl
                                    FROM DGTREPLRECORDPRM_TMP t
                                   WHERE t.t_paramid_repl is not null)) LOOP
      IF l_Clob IS NULL THEN
        l_Clob := 'Параметр ИД='||c.t_id||' записи ЗР ИД='||p_RecReplId||' не найден в шлюзе (DGTRECPRM_DBT)';
      ELSE
        l_Parm := 'Параметр ИД='||c.t_id||' записи ЗР ИД='||p_RecReplId||' не найден в шлюзе (DGTRECPRM_DBT)';
        l_Clob := l_Clob||chr(10)||l_Parm;
      END IF;
    END LOOP;
    Return l_Clob;
  end CompareRecordParam;

end RSB_TEST_GT;
/