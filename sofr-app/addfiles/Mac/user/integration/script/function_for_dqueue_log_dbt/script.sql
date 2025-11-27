CREATE OR REPLACE FUNCTION WriteQueueLog(p_qname IN VARCHAR2, p_direction IN NUMBER, p_type IN NUMBER, p_sync IN NUMBER, p_Message IN CLOB, p_jmsmessageid IN VARCHAR2, p_jmscorrelationid IN VARCHAR2,
p_xr_reqid IN VARCHAR2,  p_errcode IN NUMBER, p_errmessage IN VARCHAR2, p_jmsmessageguid IN VARCHAR2) RETURN NUMBER

  AS
     pragma autonomous_transaction;
     v_stat NUMBER;
  BEGIN
    v_stat :=0;
    
    
    insert into dqueue_log_dbt (t_qname, t_direction, t_type, t_sync, t_message, t_jmsmessageid, t_jmscorrelationid, t_xr_reqid, t_errcode, t_errmessage, t_sysdate, t_systime, t_jmsmessageguid) 
                          values (p_qname, p_direction, p_type, p_sync, p_message, p_jmsmessageid, p_jmscorrelationid, p_xr_reqid, p_errcode, p_errmessage, 
                          to_date(to_char(sysdate, 'DDMMYYY'), 'DDMMYYY'), to_timestamp('01010001' || to_char( systimestamp, 'hh24missff'), 'ddmmyyyyhh24missff'), p_jmsmessageguid) RETURNING T_ID into v_stat;

    COMMIT;

    RETURN v_stat;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RETURN -1;

  END WriteQueueLog;
/

CREATE OR REPLACE FUNCTION UpdateQueueLog(p_errcode IN VARCHAR2, p_errmessage IN VARCHAR2, p_id IN NUMBER) RETURN NUMBER

  AS
     pragma autonomous_transaction;
     v_stat NUMBER;
  BEGIN
    v_stat :=0;
    
    update dqueue_log_dbt set t_errcode = p_errcode, t_errmessage = p_errmessage where t_id = p_id;
    --insert into dqueue_log_dbt (t_qname, t_direction, t_type, t_sync, t_message, t_jmsmessageid, t_jmscorrelationid, t_xr_reqid, t_errcode, t_errmessage, t_sysdate, t_systime, t_jmsmessageguid) 
     --                     values (p_qname, p_direction, p_type, p_sync, p_message, p_jmsmessageid, p_jmscorrelationid, p_xr_reqid, p_errcode, p_errmessage, 
     --                     to_date(to_char(sysdate, 'DDMMYYY'), 'DDMMYYY'), to_timestamp('01010001' || to_char( systimestamp, 'hh24missff'), 'ddmmyyyyhh24missff'), p_jmsmessageguid) RETURNING T_ID into v_stat;

    COMMIT;

    RETURN v_stat;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RETURN -1;

  END UpdateQueueLog;
/