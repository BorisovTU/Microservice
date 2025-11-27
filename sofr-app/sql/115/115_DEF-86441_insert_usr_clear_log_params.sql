begin
  insert into usr_clear_log_params (t_table_name,
                                    t_query)
  values ('DUSERLEBROKREPREQ_DBT',
'DECLARE
  max_dtend date ;
  min_dtend date ;
BEGIN
  
  select max(t_reqsysdate),min(t_reqsysdate)
  INTO max_dtend, min_dtend 
  from DUSERLEBROKREPREQ_DBT d
  where d.t_reqsysdate < sysdate -  NUMTODSINTERVAL(30, ''DAY'');
  
  WHILE max_dtend >= min_dtend
  LOOP
    DELETE FROM DUSERLEBROKREPREQ_DBT l WHERE  t_reqsysdate  BETWEEN (max_dtend - NUMTODSINTERVAL(5, ''DAY'')) AND max_dtend;
    max_dtend := max_dtend - NUMTODSINTERVAL(5, ''DAY'');
  END LOOP;
END;');
end; 