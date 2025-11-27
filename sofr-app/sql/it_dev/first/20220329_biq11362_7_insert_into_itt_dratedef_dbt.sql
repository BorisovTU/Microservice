
insert into itt_dratedef_dbt ( t_fiid,t_otherfi,t_name, t_definition, t_type, t_market_place,  t_rate,t_point, t_sincedate, t_isrelative)
select f.t_fiid as t_fiid
     , a.t_fiid as t_otherfi 
     , r.t_security_name as t_name
     , f.t_definition as t_definition
     , 1001 as t_type/*Цена Bloomberg для ф.707 */
     , 0 as market_place
     , replace(r.correct_amt,',','.') as t_rate
     , 0 as t_point 
     , r.t_sincedate as t_sincedate
     , chr(88) as t_isrelative
  from itt_dratedef_dbt_rst r
 inner join davoiriss_dbt a on a.t_isin = r.t_isin   
  left join dfininstr_dbt f on f.t_ccy = r.crncy
  where 1=1
    and r.correct_amt !='0'
    --f.t_fiid is null
    ;
   commit; 
