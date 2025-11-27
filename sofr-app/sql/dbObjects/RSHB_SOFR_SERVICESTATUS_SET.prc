
create or replace procedure rshb_Sofr_ServiceStatus_Set(Req_id in number, ReceiptIn in clob) IS
 /*Процедура приема квитанций в СОФР */
BEGIN
  /* <ErrorList>
    <Error>
        <ErrorCode>0</ErrorCode>
    </Error>
    <Error>
        <ErrorCode>-56</ErrorCode>
        <ErrorDesc>Unknown Mistake</ErrorDesc>
    </Error>
    </ErrorList> */
  
  for error_list in (
    SELECT ErrorCode, nvl(ErrorDesc,'OK!') ErrorDesc
      FROM XMLTABLE('/ErrorList/Error' PASSING xmltype(ReceiptIn)
                    COLUMNS ErrorCode varchar2(200)  PATH 'ErrorCode',
                            ErrorDesc varchar2(200)  PATH 'ErrorDesc') 
                            ) loop
    dbms_output.put_line(error_list.ErrorCode||'~'||error_list.ErrorDesc);
  end loop;
  
EXCEPTION
  when others then 
    it_error.put_error_in_stack;
    it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR, p_msg => 'Ошибка rshb_Sofr_ServiceStatus_Set: '||sqlerrm);
end;