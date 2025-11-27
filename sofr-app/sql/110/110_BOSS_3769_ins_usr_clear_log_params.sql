begin

  insert into USR_CLEAR_LOG_PARAMS
    (T_TABLE_NAME
    ,T_QUERY)
  values
    ('DUSERBROKREPREQ_DBT'
    , 'DECLARE
  max_dtend date ;
  min_dtend date ;
BEGIN
  
  select max(t_reqsysdate),min(t_reqsysdate)
  INTO max_dtend, min_dtend 
  from DUSERBROKREPREQ_DBT d
  where d.t_reqsysdate < sysdate -  NUMTODSINTERVAL(30, ''DAY'');
  
  WHILE max_dtend >= min_dtend
  LOOP
    DELETE FROM DUSERBROKREPREQ_DBT l WHERE  t_reqsysdate  BETWEEN (max_dtend - NUMTODSINTERVAL(5, ''DAY'')) AND max_dtend;
    max_dtend := max_dtend - NUMTODSINTERVAL(5, ''DAY'');
  END LOOP;
END;');

end;
/
