-- Необходимо заменить в DDIASRESTDEPO_DBT T_ISIN у записей, 
-- для которых вместо ISIN указан № гос. регистрации через сверку с таблицей DAVOIRISS_DBT
 
declare
  oldisin integer;
  newisin integer;
  old_timestamp timestamp;
  cnt1 integer :=0;
  cnt2 integer :=0;
  cnt3 integer :=0;
begin
      for rec in (
        -- несовпадение номера госрегистрации и ISIN у ЦБ по соответствующим записям в DDIAS*
        select /*+ ORDERED INDEX (r, DDIASRESTDEPO_DBT_IDX3) USE_NL (r d) */ 
          d.t_isin old_isin, a.t_isin new_isin, d.t_id id_old_isin,
          (select d1.t_id from ddiasisin_dbt d1 where d1.t_isin = a.t_isin)  id_new_isin,
          r.reportdate, r.accdepoid, r.t_timestamp
        from DAVOIRISS_DBT a, ddiasisin_dbt d, ddiasrestdepo_dbt r 
        where 
        a.t_lsin = to_char(d.t_isin) 
        and a.t_isin <> to_char(d.t_isin)
        and r.isin = d.t_id
        )
      loop
        -- поиск максимальной даты для записей с новым ISIN на который будет меняться старый ISIN
        select nvl(max(r.t_timestamp),to_date('01011001','ddmmyyyy')) into old_timestamp 
          from ddiasrestdepo_dbt r where 
          r.isin = rec.id_new_isin
          and r.accdepoid = rec.accdepoid
          and r.reportdate = rec.reportdate
          ;
        if (old_timestamp = to_date('01011001','ddmmyyyy')) then
        -- не нашли записей с новым ISIN
          -- меняем ISIN, не боясь исключения в DDIASRESTDEPO_DBT_IDX2
          update ddiasrestdepo_dbt r 
            set r.isin = rec.id_new_isin 
            where r.isin = rec.id_old_isin  
            and r.accdepoid = rec.accdepoid
            and r.reportdate = rec.reportdate ;
          cnt1 := cnt1 + 1;
        elsif (old_timestamp<rec.t_timestamp) then
        -- время записей с новыым isin меньше, чем у записи со старым ISIN
          -- удаляем запись со старым ISIN
          delete from ddiasrestdepo_dbt r where
            r.isin = rec.id_new_isin
            and r.accdepoid = rec.accdepoid
            and r.reportdate = rec.reportdate;
          -- меняем ISIN, не боясь исключения в DDIASRESTDEPO_DBT_IDX2
          update ddiasrestdepo_dbt r 
            set r.isin = rec.id_new_isin 
            where r.isin = rec.id_old_isin  
            and r.accdepoid = rec.accdepoid
            and r.reportdate = rec.reportdate ;
          cnt2 := cnt2 + 1;
        else 
        -- время записи с новыым ISIN больше или равно, чем запись со старым ISIN 
          -- удаляем запись со старым ISIN
          delete from ddiasrestdepo_dbt r where
            r.isin = rec.id_old_isin
            and r.accdepoid = rec.accdepoid
            and r.reportdate = rec.reportdate;
          cnt3 := cnt3 + 1;
        end if;
      end loop;
      it_log.log(
        ' DEF-63390. Обновлено записей '||cnt1||
        '. Удалено и добавлено записей '||cnt2||
        '. Удалено '||cnt2
        );
end;
