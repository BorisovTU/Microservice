declare
  n      number;
  t_name varchar2(100) := 'itt_parallel_exec';
begin
  select count(*) into n from user_tables t where t.table_name = upper(t_name);
  if n = 1
  then
    execute immediate 'drop table itt_parallel_exec';
  end if;
  execute immediate 'create table itt_parallel_exec
(
  calc_id number not null,
  row_id  NUMBER GENERATED ALWAYS AS IDENTITY (cycle cache 20 maxvalue 999999999999999999999999999) ,
  str01 varchar2(4000) ,
  str02 varchar2(4000) ,
  str03 varchar2(4000) ,
  str04 varchar2(4000) ,
  str05 varchar2(4000) ,
  num01 number ,
  num02 number ,
  num03 number ,
  num04 number ,
  num05 number ,
  num06 number ,
  num07 number ,
  num08 number ,
  num09 number ,
  num10 number ,
  dat01 date ,
  dat02 date ,
  dat03 date ,
  dat04 date ,
  dat05 date ,
  str06 varchar2(4000) ,
  str07 varchar2(4000) ,
  str08 varchar2(4000) ,
  str09 varchar2(4000) ,
  str10 varchar2(4000) ,
  num11 number ,
  num12 number ,
  num13 number ,
  num14 number ,
  num15 number ,
  num16 number ,
  num17 number ,
  num18 number ,
  num19 number ,
  num20 number , 
  dat06 date ,
  dat07 date ,
  dat08 date ,
  dat09 date ,
  dat10 date 

)
PARTITION BY LIST (calc_id) 
( 
     PARTITION p999999990 
         VALUES (0)  
 )
';
    execute immediate 'comment on table ITT_PARALLEL_EXEC  is ''Буферная таблица для распаралеливания вычислений''';
    execute immediate 'create index ITI_PARALLEL_EXEC_ID on ITT_PARALLEL_EXEC (row_id) local ';
end;
/
