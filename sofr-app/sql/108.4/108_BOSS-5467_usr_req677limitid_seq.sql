declare 
 n integer ;
begin
   select count(*) into n  from user_sequences s where s.SEQUENCE_NAME = 'USR_REQ677LIMITID_SEQ';
   if n = 0  then
    execute immediate 'create sequence USR_REQ677LIMITID_SEQ minvalue 1 maxvalue 1999999999 start with 1 increment by 1 nocache cycle';
   end if;
end;
