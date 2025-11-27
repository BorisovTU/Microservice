DECLARE
    e_table_does_not_exist EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_table_does_not_exist, -942);
BEGIN
    EXECUTE IMMEDIATE 'delete from dmenuparm_dbt parm where exists (select 1 from dmenuitem_dbt item where item.t_objectid = parm.t_objectid and item.t_istemplate = parm.t_istemplate and item.t_inumberpoint = parm.t_inumberpoint and parm.t_cidentprogram = chr(item.t_iidentprogram)and item.t_icaseitem=1700 and item.t_csystemitem=''X'' and item.t_iprogitem=73 and item.T_IIDENTPROGRAM=83)';
EXCEPTION
    WHEN e_table_does_not_exist THEN NULL;
END;
/

DECLARE
    e_table_does_not_exist EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_table_does_not_exist, -942);
BEGIN
    EXECUTE IMMEDIATE 'insert into dmenuparm_dbt (t_objectid, t_istemplate, t_cidentprogram, t_inumberpoint, t_parm )' || 
      '(select t_objectid, t_istemplate, chr(t_iidentprogram), t_inumberpoint, ''2D666E3A2294AEE0ACA02037313122000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'' ' ||
        'from dmenuitem_dbt ' ||
       'where t_icaseitem=1700 and t_csystemitem=''X'' and t_iprogitem=73 and T_IIDENTPROGRAM=83)';
EXCEPTION
    WHEN e_table_does_not_exist THEN NULL;
END;
/
