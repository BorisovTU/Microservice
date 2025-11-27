CREATE OR REPLACE TRIGGER DVSBANNER_DBT_TRGVSP
  AFTER INSERT OR UPDATE OF T_DEPARTMENT, T_BRANCH ON DVSBANNER_DBT FOR EACH ROW
DECLARE
  l_oper varchar2(320);
BEGIN
  IF INSERTING OR (UPDATING AND (:OLD.t_Department != :NEW.t_Department OR :OLD.t_Branch != :NEW.t_Branch)) THEN
    l_oper := SUBSTR(RsbSessionData.GetOperName(RsbSessionData.Oper()), 1, 320);
    
    IF l_oper = ' ' THEN
      l_oper := to_char(RsbSessionData.Oper());
    END IF;
    
    INSERT INTO dchbranchhist_dbt (t_bcID
                                  ,t_bcSeries
                                  ,t_bcNumber
                                  ,t_sysDate
                                  ,t_operDate
                                  ,t_oper
                                  ,t_department
                                  ,t_branch)
                           VALUES (:NEW.t_bcID
                                  ,:NEW.t_bcSeries
                                  ,:NEW.t_bcNumber
                                  ,SYSDATE
                                  ,RsbSessionData.curdate()
                                  ,l_oper
                                  ,:NEW.t_department
                                  ,:NEW.t_branch);
  END IF;

END;
/

































































