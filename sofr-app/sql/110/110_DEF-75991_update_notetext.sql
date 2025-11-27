--Корректировка примечания по заявке с кодом трейдера tr8 от 29/08/2024 года. Изменить код на tr1
begin
update dnotetext_dbt n
   set n.t_text = rpad(utl_raw.cast_to_raw(c => 'tr1'), 3000, 0)
 where n.t_id in (select t.t_id
                    from dnotetext_dbt t
                   where t.t_objecttype = 149
                     and t.t_notekind = 102
                     and t.t_date = to_date('29.08.2024','dd.mm.yyyy')
                     and trim(chr(0) from RSB_STRUCT.getString(t.T_TEXT)) = 'tr8');
end;
/

--Корректировка примечания по заявкам с кодом трейдера tr4 за период с 29/11/2023 по 12/12/2023 включительно, заменить код на tr1
begin
update dnotetext_dbt n
   set n.t_text = rpad(utl_raw.cast_to_raw(c => 'tr1'), 3000, 0)
 where n.t_id in (select t.t_id
                    from dnotetext_dbt t
                   where t.t_objecttype = 149
                     and t.t_notekind = 102
                     and t.t_date between to_date('29.11.2023','dd.mm.yyyy') and to_date('12.12.2023','dd.mm.yyyy')
                     and trim(chr(0) from RSB_STRUCT.getString(t.T_TEXT)) = 'tr4');
end;
/

--Корректировка примечания по заявкам с кодом трейдера tr8 за период с 22/11/2023 по 19/01/2024 включительно, заменить код на tr11
begin
update dnotetext_dbt n
   set n.t_text = rpad(utl_raw.cast_to_raw(c => 'tr11'), 3000, 0)
 where n.t_id in (select t.t_id
                    from dnotetext_dbt t
                   where t.t_objecttype = 149
                     and t.t_notekind = 102
                     and t.t_date between to_date('22.11.2023','dd.mm.yyyy') and to_date('19.01.2024','dd.mm.yyyy')
                     and trim(chr(0) from RSB_STRUCT.getString(t.T_TEXT)) = 'tr8');
end;
/
