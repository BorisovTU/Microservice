create or replace trigger "DPERSON_DBT_UD_AD"
  after update or delete
  on DPERSON_DBT
  for each row
begin
  if Deleting or :new.T_USERCLOSED != :old.T_USERCLOSED then
    if Deleting or :new.T_USERCLOSED = 'X' then
      update  DOBJCODE_DBT T
         set t.t_state =  1 
           ,t.t_sysdate = trunc(sysdate)
           ,t.t_systime = to_date('00010101'||to_char(sysdate,'hh24:mi:ss'),'yyyymmddhh24:mi:ss')
           ,t.t_userid = RsbSessionData.oper()
           ,t.t_bankclosedate = nvl(:new.T_DATECLOSEOPER,trunc(sysdate))
         WHERE T.T_OBJECTTYPE = 3
          and  t.T_CODEKIND = 104
          and T.T_OBJECTID = :old.T_PARTYID ;
    else
      update  DOBJCODE_DBT T
         set t.t_state =  0 
           ,t.t_sysdate = trunc(sysdate)
           ,t.t_systime = to_date('00010101'||to_char(sysdate,'hh24:mi:ss'),'yyyymmddhh24:mi:ss')
           ,t.t_userid = RsbSessionData.oper()
           ,t.t_bankclosedate = date'0001-01-01'
         WHERE T.T_OBJECTTYPE = 3
          and  t.T_CODEKIND = 104
          and T.T_OBJECTID = :old.T_PARTYID ;
    end if;
  end if;
end ;
/
