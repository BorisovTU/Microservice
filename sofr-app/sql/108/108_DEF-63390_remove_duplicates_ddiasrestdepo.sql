-- стереть дубли из DDIASRESTDEPO_DBT - записи с одинаковым reportdate,isin,accdepoid - оставляем с более поздним timestamp 
-- на DEBUG отработал за 5 минут   
delete from DDIASRESTDEPO_DBT d
 where d.rowid in (select rid
                     from (select d.*,
                                  row_number() over(partition by d.accdepoid, d.reportdate, d.isin order by d.t_timestamp desc) rnk,
                                  rowid rid
                             from DDIASRESTDEPO_DBT d)
                    where rnk > 1 )
/