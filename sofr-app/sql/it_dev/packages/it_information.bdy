create or replace package body it_information is

  /**************************************************************************************************\
    BIQ-9225. Разработка очереди и журнала событий
              Работа с информационным журналом
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
    08.11.2023  Зыков М.В.       DEF-54476                        BIQ-13171. Доработка механизма отправки сообщений из SiteScope
    15.05.2023  Зыков М.В.       CCBO-4870                        BIQ-13171. Разработка процедур работы с очередью событий
    03.08.2022  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/
  function get_MAIL_GROUP_DEF return varchar2 as
  begin
    return C_C_MAIL_GROUP_DEF;
  end;

  -- Запись сообщения в журнал 
  procedure store_info(p_info_type    itt_information.info_type%type
                      ,p_mail_group   itt_information.mail_group%type default C_C_MAIL_GROUP_DEF
                      ,p_info_title   itt_information.info_title%type default null
                      ,p_info_content clob default null) is
    pragma autonomous_transaction;
  begin
    insert into itt_information
      (info_id
      ,info_type
      ,mail_group
      ,info_title
      ,info_content)
    values
      (its_q_log.nextval
      ,p_info_type
      ,p_mail_group
      ,p_info_title
      ,p_info_content);
    commit;
  end;

  ------------------------------------------------------------------------------------------------------------------
  -- Вывод инфы о WORKERах в HTML
  function show_stat_qworkers(p_title     varchar2 default null
                             ,p_queue_num itt_q_message_log.queue_num%type) return clob is
    v_title    varchar2(2000) := nvl(p_title, ' Состояние AQ-ОБРАБОТЧИКОВ и очереди № ' || p_queue_num);
    html_start varchar2(32000) := '<!DOCTYPE HTML>
<html>
<head>
<meta charset="window-1251">
<style>
td {font-size: 11pt;}
</style>
</head>
<body>
<p></p>
<p></p>
<p>' || to_char(sysdate, 'DD-MM-YYYY HH24:MI:SS') || ' ' || v_title || '</p>' ||
                                  '<p>Заданий во вх.очереди %QIN_COUNT%</p>' || '<p>Из них SF %QIN_SF%</p><p></p>
<table border="1" style="border-collapse: collapse; border: 1px solid black;">
<col valign="center">
<tr>
<th width="50">№</th>
<th width="50">N/F</th>
<th width="50">Остановлен</th>
<th width="10">Доступен</th>
<th width="10">Свободен</th>
<th width="50">Кол-во заданий</th>
<th width="120">Начало выполнения</th>
<th width="600">Выполняется бизнеспроцесс </th>
<th width="80">Прогноз завершения очереди заданий (с)</th>';
    html_body  varchar2(32000) := '<tr>
<td align="center">%NUM%</td>
<td align="center">%PRIORITY%</td>
<td align="center">%J_STOP%</td>
<td align="center">%W_ENABLED%</td>
<td align="center">%W_FREE%</td>
<td align="right">%RUN_COUNT% </td>
<td align="left"> %RUN_START%</td>
<td align="left"> %SERVICENAME%</td>
<td align="right">%TIMEFINISH% </td>
</tr>';
    html_fun constant varchar2(50) := '</body> </html>';
    temp_body_html varchar2(32000);
    vi_qin_count   integer;
    vi_qin_SF      integer;
    v_cl_report    clob default null;
    v_sel          varchar2(2000) := 'select count(*),nvl(sum(case when priority =''' || IT_Q_MESSAGE.C_C_MSG_PRIORITY_F ||
                                     ''' and delivery_type = ''' || IT_Q_MESSAGE.C_C_MSG_DELIVERY_S || ''' then 1 else 0 end),0)  from ' ||
                                     IT_Q_MESSAGE.C_C_QVIEW_TASK_PREFIX || p_queue_num;
    function get_zn_tag(p_chk_0 number
                       ,p_zn_n  varchar2
                       ,p_zn_y  varchar2 default '-') return varchar2 as
    begin
      return case when p_chk_0 = 0 then nvl(p_zn_n, '-') else nvl(p_zn_y, '-') end;
    end;
  
  begin
    --dbms_output.put_line(v_sel);
    execute immediate v_sel
      into vi_qin_count, vi_qin_SF;
    v_cl_report := replace(html_start, '%QIN_COUNT%', vi_qin_count);
    v_cl_report := replace(v_cl_report, '%QIN_SF%', vi_qin_SF);
    for c in (select q.*
                    ,m.servicename as work_servicename
                from itt_q_worker q
                left join (select wm.worker_num
                                ,wm.servicename
                                ,wm.work_ready
                                ,max(wm.work_ready) over(partition by wm.worker_num) as max_work_ready
                            from itt_q_work_messages wm) m
                  on q.worker_num = m.worker_num
                 and m.work_ready = m.max_work_ready
               order by q.worker_num)
    loop
      temp_body_html := html_body;
      temp_body_html := replace(temp_body_html, '%NUM%', c.worker_num);
      temp_body_html := replace(temp_body_html, '%PRIORITY%', c.worker_priority);
      temp_body_html := replace(temp_body_html
                               ,'%J_STOP%'
                               ,case
                                  when c.job_stoptime is null then
                                   'Нет'
                                  else
                                   'Да'
                                end);
      temp_body_html := replace(temp_body_html, '%W_ENABLED%', get_zn_tag(c.worker_enabled, 'Нет', 'Да'));
      temp_body_html := replace(temp_body_html, '%W_FREE%', get_zn_tag(c.worker_free, 'Нет', 'Да'));
      temp_body_html := replace(temp_body_html, '%RUN_COUNT%', get_zn_tag(c.worker_free, to_char(c.run_count), to_char(0)));
      temp_body_html := replace(temp_body_html, '%RUN_START%', get_zn_tag(c.worker_free, to_char(c.run_starttime, 'dd.mm.yyyy hh24:mi:ss'), '-'));
      temp_body_html := replace(temp_body_html, '%SERVICENAME%', get_zn_tag(c.worker_free, nvl(c.work_servicename, c.servicename)));
      temp_body_html := replace(temp_body_html
                               ,'%TIMEFINISH%'
                               ,get_zn_tag(c.worker_free
                                          ,trim(to_char(round((c.run_coins -
                                                              it_xml.calc_interval_millisec(p_ts_start => c.run_lasttime, p_ts_stop => systimestamp)) / 1000
                                                             ,3)
                                                       ,'9999999999999990.999'))));
      v_cl_report    := v_cl_report || temp_body_html;
    end loop;
    v_cl_report := v_cl_report || html_fun;
    return v_cl_report;
  end;

  -- Возвращает новый набор сообщений для почтовой группы 
  function get_list_info(p_mail_group itt_information.mail_group%type
                        ,p_format     integer default 0 -- зарезервировано 0 - текст 1 - HTML
                        ,p_len        integer default 4000
                        ,p_str_begin  varchar2 default null
                        ,p_str_end    varchar2 default chr(13) || chr(10)) return varchar2 is
    v_res varchar2(32676);
    v_ln  varchar2(32000);
  begin
    for inf in (select *
                  from itt_information i
                 where i.mail_group = p_mail_group
                   and i.info_title is null
                   and i.SENDDT is null
                 order by CREATE_SYSDATE
                   for update skip locked)
    loop
      v_ln := p_str_begin || to_char(inf.create_sysdate, 'dd.mm.yyyy hh24:mi:ss') || ' [' || inf.info_type || ']:' ||
              replace(replace(it_xml.Clob_to_str(inf.info_content, 200), p_str_begin), p_str_end) || p_str_end;
      exit when length(v_res) + length(v_ln) > p_len or length(v_res) + length(v_ln) > 32676;
      v_res := v_res || v_ln;
      update itt_information i set i.SENDDT = sysdate where i.info_id = inf.info_id;
    end loop;
    return v_res;
  end;

  -- Возвращает новое сообщение для почтовой группы 
  function get_mess_info(p_mail_group itt_information.mail_group%type
                        ,o_Title      out itt_information.info_title%type) return varchar2 is
    v_res varchar2(32676);
  begin
    for inf in (select *
                  from itt_information i
                 where i.mail_group = p_mail_group
                   and i.info_title is not null
                   and i.SENDDT is null
                 order by CREATE_SYSDATE
                   for update skip locked)
    loop
      o_Title := inf.info_title;
      v_res   := substr(inf.info_content, 4000);
      update itt_information i set i.SENDDT = sysdate where i.info_id = inf.info_id;
      exit;
    end loop;
    return v_res;
  end;

end it_information;
/
