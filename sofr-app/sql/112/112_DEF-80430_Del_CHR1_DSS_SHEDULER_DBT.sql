declare 
	voidClob clob := to_clob(chr(1));
begin
	UPDATE dss_sheduler_dbt SET T_PARAMETERS=EMPTY_CLOB() WHERE dbms_lob.compare(T_PARAMETERS,voidClob,1)=0;
end;
/
