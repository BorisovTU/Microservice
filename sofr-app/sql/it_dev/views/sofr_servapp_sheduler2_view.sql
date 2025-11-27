create or replace view sofr_servapp_sheduler2_view as
select T_IOPER
      ,t_autokey
      ,t_shedid
      ,t_name
      ,t_startdate
      ,t_starttime
      ,case
         when (t_shedid = 464)
              and (sysdate - TRUNC(sysdate) < 0.375) then
          0 -- "Обработка БО ЦБ" и время меньше 9-00
         else
          (TRUNC(sysdate) - t_startdate + (sysdate - TRUNC(sysdate)) - (t_starttime - TO_DATE('01.01.0001 00:00:00', 'DD.MM.YYYY HH24:MI:SS'))) * 24 * 60 * 60
       end as t_work_seconds
  from (select *
          from (select * from DSHEDLOG_DBT where T_IOPER = 1 order by 1 desc)
         where ROWNUM = 1
        union all
        select *
          from (select * from DSHEDLOG_DBT where T_IOPER = 2 order by 1 desc)
         where ROWNUM = 1)
/
