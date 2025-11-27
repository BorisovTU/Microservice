declare
	n        number;
	seq_name varchar2(100) := 'its_log';
begin
	select count(*) into n from user_sequences t where t.sequence_name = upper(seq_name);
	if n = 0
	then
		execute immediate 'create sequence its_log minvalue 1 maxvalue 9999999999999999999999999 start
			with 100000000 increment by 1 cache 20';
	end if;
end;
/
