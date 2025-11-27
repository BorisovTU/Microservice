create or replace procedure rshb_Sofr_ServiceReceive_Message(ServiceName in varchar2, Message in clob, Req_id out number, ReceiptOut out clob) IS
/* Процедура приема запросов в СОФР */ 
  temp_clob CLOB;
  verror_code number;
  verror_desc varchar2(2000 char);
  
BEGIN
  insert into SOFR_IPS_MESSAGE (T_ID, T_ServiceID, T_Message, T_FileName, T_Direction, 
                                T_Status, T_UserField1, T_UserField2, T_Flag1, T_Flag2 )
  values (0, 1, Message, chr(1), 1,
          0, chr(1), chr(1), chr(0), chr(0))
  RETURNING t_id into Req_id;

  -- если ошибки будут где-то хранится, то лучше использовать нормальное формирование XMLType
  /*select XMLRoot(xmlelement("ErrorList",  
                               xmlagg( xmlelement("Error", 
                                                   xmlforest( ErrorCode as "ErrorCode", ErrorDesc as "ErrorDesc"))
                                      ) 
                               ),VERSION '1.0').getClobVal()
      from ( select 0 ErrorCode, null ErrorDesc from dual
              union
             select 1, 'Terrible mistake' from dual);*/
                 
  -- в простом случае просто соберем строку. XMLRoot устаревший вариант, но он форматирует в отличии от XMLSERIALIZE
  -- select XMLSERIALIZE( DOCUMENT XMLType('<ErrorList><Error><ErrorCode>0</ErrorCode><ErrorDesc></ErrorDesc></Error></ErrorList>')) from dual;
  verror_code := 0;
  verror_desc := NULL;
  select XMLRoot(XMLType('<ErrorList><Error><ErrorCode>'||verror_code||'</ErrorCode>
                                            <ErrorDesc>'||verror_desc||'</ErrorDesc></Error></ErrorList>'),VERSION '1.0').getClobVal() 
    into ReceiptOut
    from dual;
    
   commit; 
    
EXCEPTION
  when others then 
    rollback;
    it_error.put_error_in_stack;
    it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR, p_msg => 'Ошибка rshb_Sofr_ServiceReceive_Message: '||sqlerrm);
    Req_id := 0;
    verror_code := SQLCODE;
    verror_desc := SQLERRM;
    select XMLRoot(XMLType('<ErrorList><Error><ErrorCode>'||verror_code||'</ErrorCode>
                                              <ErrorDesc>'||verror_desc||'</ErrorDesc></Error></ErrorList>'),VERSION '1.0').getClobVal() 
      into ReceiptOut
      from dual;
      
END;