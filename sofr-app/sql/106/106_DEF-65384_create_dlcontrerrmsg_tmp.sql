begin
  execute immediate q'[
    create global temporary table dlcontrerrmsg_tmp
    (
    t_errorcode  NUMBER(10),
    t_error      VARCHAR2(512) 
  )
  on commit preserve rows
  ]';  
   
end;
/