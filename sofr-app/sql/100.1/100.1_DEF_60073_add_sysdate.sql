declare
  type t_tab is table of varchar2(50);
  vt_tab t_tab := t_tab('D724ARREAR_DBT'
                       ,'D724PFI_DBT'
                       ,'D724WRTAVR_DBT'
                       ,'D724WRTMONEY_DBT'
                       ,'D724FIREST_DBT'
                       ,'D724ACCREST_DBT'
                       ,'D724CLIENT_DBT'
                       ,'D724CONTR_DBT'
                       ,'D724R3CLIENT_GROUP');
 cnt number;
begin
   for p in 1 .. vt_tab.count
     loop
       select count(*) into cnt from user_tab_columns t where t.TABLE_NAME = vt_tab(p) and t.COLUMN_NAME  = upper('t_sysdate');
       if cnt =0 then
         execute immediate 'alter table '||vt_tab(p)||' add t_sysdate date default sysdate not null';
       end if;
       select count(*) into cnt from user_indexes i where i.INDEX_NAME= vt_tab(p)||'_USR1' ;
       if cnt =0 then
         execute immediate 'create index '||vt_tab(p)||'_USR1 on '||vt_tab(p)||'(t_sysdate) tablespace INDX';
       end if;
    end loop;
end;
