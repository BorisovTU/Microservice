BEGIN
   EXECUTE IMMEDIATE 'alter table dtrnrules_dbt add T_CHANGEDATE DATE default TO_DATE(''01/01/0001'', ''dd/mm/yyyy'')';

EXCEPTION
   WHEN OTHERS
   THEN
      NULL;
END;
/

BEGIN
   INSERT INTO DNAMEALG_DBT (T_ITYPEALG,
                             T_INUMBERALG,
                             T_SZNAMEALG,
                             T_ILENNAME,
                             T_IQUANTALG,
                             T_RESERVE)
        VALUES (8554,
                0,
                'Создание правила',
                21,
                4,
                CHR (1));
   INSERT INTO DNAMEALG_DBT (T_ITYPEALG,
                             T_INUMBERALG,
                             T_SZNAMEALG,
                             T_ILENNAME,
                             T_IQUANTALG,
                             T_RESERVE)
        VALUES (8554,
                1,
                'Изменение параметров',
                21,
                4,
                CHR (1));

   INSERT INTO DNAMEALG_DBT (T_ITYPEALG,
                             T_INUMBERALG,
                             T_SZNAMEALG,
                             T_ILENNAME,
                             T_IQUANTALG,
                             T_RESERVE)
        VALUES (8554,
                2,
                'Изменение статуса',
                21,
                4,
                CHR (1));
   INSERT INTO DNAMEALG_DBT (T_ITYPEALG,
                             T_INUMBERALG,
                             T_SZNAMEALG,
                             T_ILENNAME,
                             T_IQUANTALG,
                             T_RESERVE)
        VALUES (8554,
                3,
                'Удаление правила',
                21,
                4,
                CHR (1));
EXCEPTION
   WHEN OTHERS
   THEN
      NULL;
END;
/

BEGIN
   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS)
           VALUES (
                     29200,
                     0,
                     'Есть запись в истории позже текущей даты');

   INSERT INTO DBANK_MSG (T_NUMBER, T_PAGE, T_CONTENTS)
           VALUES (
                     29201,
                     0,
                     'Дата начала действия правила больше, чем дата окончания');
EXCEPTION
   WHEN OTHERS
   THEN
      NULL;
END;
/

DECLARE
   TYPE rules_type IS TABLE OF DTRNRULES_DBT%ROWTYPE;

   v_rules   rules_type;
BEGIN
   SELECT *
     BULK COLLECT INTO v_rules
     FROM DTRNRULES_DBT;

   FOR i IN 1 .. v_rules.COUNT
   LOOP
      INSERT INTO DTRNRULESHIST_DBT (T_HISTID,
                                     T_ACTION,
                                     T_CHANGEDATEHIST,
                                     T_OPERID,
                                     T_ID,
                                     T_ACC_MASK,
                                     T_SIDE,
                                     T_ATTR_KIND,
                                     T_OPER,
                                     T_ATTR_VALUE,
                                     T_ISACTIVE,
                                     T_STARTDATE,
                                     T_ENDDATE,
                                     T_CHANGEDATE)
           VALUES (0,
                   0,
                   TO_DATE ('01/01/0001', 'dd/mm/yyyy'),
                   9999,
                   v_rules (i).T_ID,
                   v_rules (i).T_ACC_MASK,
                   v_rules (i).T_SIDE,
                   v_rules (i).T_ATTR_KIND,
                   v_rules (i).T_OPER,
                   v_rules (i).T_ATTR_VALUE,
                   v_rules (i).T_ISACTIVE,
                   v_rules (i).T_STARTDATE,
                   v_rules (i).T_ENDDATE,
                   v_rules (i).T_CHANGEDATE);
   END LOOP;
   commit;
END;
/

CREATE OR REPLACE TRIGGER DTRNRULES_DBT_T1_HIST
   AFTER INSERT OR DELETE
   ON DTRNRULES_DBT
   FOR EACH ROW
DECLARE
BEGIN
   IF INSERTING
   THEN
      INSERT INTO DTRNRULESHIST_DBT (T_HISTID,
                                  T_ACTION,
                                  T_CHANGEDATEHIST,
                                  T_OPERID,
                                  T_ID,
                                  T_ACC_MASK,
                                  T_SIDE,
                                  T_ATTR_KIND,
                                  T_OPER,
                                  T_ATTR_VALUE,
                                  T_ISACTIVE,
                                  T_STARTDATE,
                                  T_ENDDATE,
                                  T_CHANGEDATE)
        VALUES (0,
                0,
                TRUNC(SYSDATE),
                NVL (RsbSessionData.Oper, 9999),
                :NEW.T_ID,
                :NEW.T_ACC_MASK,
                :NEW.T_SIDE,
                :NEW.T_ATTR_KIND,
                :NEW.T_OPER,
                :NEW.T_ATTR_VALUE,
                :NEW.T_ISACTIVE,
                :NEW.T_STARTDATE,
                :NEW.T_ENDDATE,
                :NEW.T_CHANGEDATE);
   ELSE
      INSERT INTO DTRNRULESHIST_DBT (T_HISTID,
                                  T_ACTION,
                                  T_CHANGEDATEHIST,
                                  T_OPERID,
                                  T_ID,
                                  T_ACC_MASK,
                                  T_SIDE,
                                  T_ATTR_KIND,
                                  T_OPER,
                                  T_ATTR_VALUE,
                                  T_ISACTIVE,
                                  T_STARTDATE,
                                  T_ENDDATE,
                                  T_CHANGEDATE)
        VALUES (0,
                3,
                TRUNC(SYSDATE),
                NVL (RsbSessionData.Oper, 9999),
                :OLD.T_ID,
                :OLD.T_ACC_MASK,
                :OLD.T_SIDE,
                :OLD.T_ATTR_KIND,
                :OLD.T_OPER,
                :OLD.T_ATTR_VALUE,
                :OLD.T_ISACTIVE,
                :OLD.T_STARTDATE,
                :OLD.T_ENDDATE,
                :OLD.T_CHANGEDATE);
   END IF;

   
END;
/