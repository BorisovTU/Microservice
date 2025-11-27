declare 
 p_file_mac      varchar2(100) := 'RSHB_chd_sec.mac'; -- Наименование макроса
 p_cidentprogram char := chr(131); -- Символьный идентификатор приложения (Г - Главная книга) 
 v_itemparm ditemuser_dbt.t_parm%type := utl_raw.cast_to_raw(rpad(lpad(chr(0), 259, chr(0)) 
                                             || lower(p_file_mac), 400, chr(0)));
begin
  update 
    ditemuser_dbt i
  set i.t_parm = v_itemparm
     where i.t_cidentprogram = p_cidentprogram 
           and i.T_ICASEITEM=193
           and i.T_IKINDMETHOD = 1
           and i.T_IKINDPROGRAM = 2 ;

  commit;
exception when others then
  it_log.log('Ошибка при обновлении наименования макроса');
end;

