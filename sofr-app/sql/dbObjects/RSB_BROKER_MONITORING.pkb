create or replace package body RSB_BROKER_MONITORING is

 /**
   @brief Проверка клиента на принадлежность к ФЛ
   @param[in] p_partyid  Идентификатор клиента
   @return True/False -является клиент ФЛ?
  */  
 function getIsLegalEntity(p_partyid dpersn_dbt.t_personid%type) return boolean

 is
 l_result integer;
 begin
   select count(*)
     into l_result
    FROM DPARTY_DBT p
   where p.t_legalform = 2
     and p.t_partyid = p_partyid;

   return l_result > 0;

 end getIsLegalEntity;
 
 /**
   @brief Очистка таблиц с данными по собранным метрикам. Запускается oracle Sheduler - CLEAR_METRICS_JOB
          Запуск в раз в сутки. Проверяет на заполняемость две таблицы DALLMETRICSKAFKA_DBT DMETRICSKAFKA_DBT
          каждую вторую неделю чистим данные
  */  
 PROCEDURE clearTableMetrics
   
 is 
 l_diff_day  number;
 l_ErrorDesc varchar2(3000);
 l_ErrorCode integer;
 l_pref varchar2(256):= 'clear_metrics';
 BEGIN
   
  BEGIN

   --сколько дней собираем информацию
   SELECT trunc(sysdate) - MIN(trunc(cast(d.t_timestamp as date))) 
     INTO l_diff_day
     FROM dallmetricskafka_dbt d;
    
   --если данных накоплено более чем за две недели 
   IF(l_diff_day > 14) THEN
     --оставляем только последнюю неделю по метрикам
     DELETE FROM dallmetricskafka_dbt d
      WHERE d.t_timestamp < SYSDATE - 7;
   END IF; 
  EXCEPTION WHEN no_data_found
    THEN 
      l_ErrorDesc := 'Таблица dallmetricskafka_dbt не содержит данных';
      l_ErrorCode := -1;
  END;
   
  BEGIN
   --сколько дней собираем информацию
   SELECT trunc(sysdate) - MIN(trunc(cast(d.t_timestamp as date))) 
     INTO l_diff_day
     FROM dmetricskafka_dbt d;
    
   --если данных накоплено более чем за две недели 
   IF(l_diff_day >= 14) THEN
     --оставляем только последнюю неделю по метрикам
     DELETE FROM dmetricskafka_dbt d
      WHERE d.t_timestamp < SYSDATE - 7;
   END IF; 
  EXCEPTION WHEN no_data_found
    THEN 
      l_ErrorDesc := 'Таблица dmetricskafka_dbt не содержит данных';
      l_ErrorCode := -1;
  END;
    
  IF(l_ErrorCode = -1) THEN
     it_event.AddErrorITLog(p_SystemId    => 'SINV',
                            p_ServiceName => 'KafkaMetrics',
                            p_ErrorCode   => l_ErrorCode,
                            p_ErrorDesc   => l_ErrorDesc,
                            p_LevelInfo   => 1);  
  END IF;

 END;
 
 /**
   @brief Сбор данных по метрикам 
   @param[in] p_partyid  Идентификатор клиента
   @param[in] p_tab_name  Наименование таблицы, в которй сработал триггер на изменение
   @param[in] p_actiontype Тип операции, изменение или добавление данных по клиенту. UPD|INS|DEL
  */
 procedure collectDataMetrics(p_partyid      dpersn_dbt.t_personid%type
                                 ,p_tab_name      varchar2
                                 ,p_actiontype    varchar2)
 is
 l_ErrorDesc varchar2(3000);
 l_ErrorCode integer;
 l_tableMetrics rsb_broker_monitoring.t_tableMetrics;
 NO_RegVal EXCEPTION;
 begin
   
 /*
  Описание метрик на данный момент хранится в параметре МЕТРИКИ_ОПИСАНИЕ
  l_tableMetrics(1) - sofr_new_fl_count
  l_tableMetrics(2) - sofr_change_fl_count
  l_tableMetrics(3) - sofr_nontrading_orders_fl_count
  
  Более подробное описание в функции fillCollectMetrics
 */
 
  l_tableMetrics := RSB_BROKER_MONITORING.fillCollectMetrics(p_KeyPath => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ИНТЕГРАЦИЯ КАФКА\МЕТРИКИ_ОПИСАНИЕ');
  IF(l_tableMetrics.count = 0) THEN
    l_ErrorDesc:= 'Отсутствуют параметр с данными по метрикам';
    l_ErrorCode:= -1;
    RAISE NO_RegVal;
  END IF;
 
 /*Рубильник по Сбору данных, 1 - включенное состояние*/
  IF(rsb_common.GetRegIntValue(p_KeyPath => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ИНТЕГРАЦИЯ КАФКА\МЕТРИКИ СОФР') <> 1) THEN
     l_ErrorDesc:= 'Сбор метрик отключен';
     l_ErrorCode:= -1;
     RAISE NO_RegVal;
  END IF;
  /*Смотрим только ФЛ клиентов */  
    IF(getIsLegalEntity(p_partyid)) THEN
               INSERT INTO DALLMETRICSKAFKA_DBT(T_PARTYID,
                                        T_TAB_NAME,
                                        T_ACTION_TYPE,
                                        T_NAME_METRIC,
                                        T_TYPE_METRIC,
                                        T_VALUE,
                                        T_TIMESTAMP)

               VALUES(p_partyid,
                      p_tab_name,
                      p_actiontype,
                      --Будем считать вставкой, только новых клиентов, 
                      --а это определенные таблицы, остальное нужно считать за изменение
                      CASE 
                        WHEN UPPER(p_actiontype) = 'INS' and UPPER(p_tab_name) in ('DPERSN_DBT','DPARTY_DBT') 
                          THEN 
                            l_tableMetrics(1).t_name
                        WHEN UPPER(p_tab_name) in ('DNPTXOP_DBT')
                          THEN 
                            l_tableMetrics(3).t_name
                        ELSE
                            l_tableMetrics(2).t_name
                      END,
                      rsb_broker_monitoring.C_GAUGE,
                      1, --будем считать каждое изменение как +1
                      systimestamp);
               COMMIT;       
      END IF;
 
 
 EXCEPTION
    WHEN NO_RegVal       
      THEN  
        NULL;
   WHEN OTHERS         
      THEN 
     l_ErrorDesc := 'Произошла ошибка при сборе метрик.';  l_ErrorCode :=-1;    
     it_event.AddErrorITLog(p_SystemId    => 'SINV',
                            p_ServiceName => 'KafkaMetrics',
                            p_ErrorCode   => l_ErrorCode,
                            p_ErrorDesc   => l_ErrorDesc,
                            p_LevelInfo   => 1);     
 end collectDataMetrics;
 
 
  /**
   @brief ХП1
          Группировка/агрегирование данных по собранным метрикам. Запускается oracle Sheduler - COLLECT_METRICS_JOB
          Запуск в 14.00 и 18.00. Группируем все собранные метрики в этом интервале по определенным правилам
   @param[in] p_timestamp - дата, на которую запускаем отбор метрик
  */  
  PROCEDURE aggregateMetrics(p_timestamp date default sysdate)

  is
  l_ErrorDesc  varchar2(3000);
  l_ErrorCode  integer;
  l_pref       varchar2(100):='aggregate_metrics: ';
  l_tableMetrics rsb_broker_monitoring.t_tableMetrics;
  NO_RegVal EXCEPTION;
  BEGIN
    
  l_tableMetrics := RSB_BROKER_MONITORING.fillCollectMetrics(p_KeyPath => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ИНТЕГРАЦИЯ КАФКА\МЕТРИКИ_ОПИСАНИЕ');
  IF(l_tableMetrics.count = 0) THEN
    l_ErrorDesc:= 'Отсутствуют параметр с данными по метрикам';
    l_ErrorCode:= -1;
    RAISE NO_RegVal;
  END IF;

  IF(rsb_common.GetRegIntValue(p_KeyPath => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ИНТЕГРАЦИЯ КАФКА\МЕТРИКИ СОФР') <> 1) THEN
     l_ErrorDesc:= 'Сбор метрик отключен';
     l_ErrorCode:= -1;
     RAISE NO_RegVal;
  END IF;
  
  IF(EXTRACT(hour from cast (p_timestamp as timestamp)) in (14,18)) THEN  
  
   
   insert into DMETRICSKAFKA_DBT (T_NAME,
                                  T_TYPE,
                                  T_VALUE,
                                  T_DESCRIPTION,
                                  T_TIMESTAMP)
    with t_dt as (
    select 
          case 
            when t.dt_type = 0 then t.dt - 4/24
            when t.dt_type = 1 then t.dt - 20/24
          end dt_start,
          t.dt as dt_end, 
          decode(t.dt_type,0,2,1,1) as dt_pos
     from 
     (select trunc(sysdate+2)-level/2 + decode(mod(level,2),0,18/24,1,2/24) as dt, mod(level,2) as dt_type
        from dual
      connect by level <= 7 * 2 * 2) t)

    select distinct
      d.t_name_metric, d.t_type_metric,
      case
        when d.t_name_metric = l_tableMetrics(1).t_name
           then
              count(distinct d.t_partyid) over (partition by d.t_name_metric, t.dt_end)   
        when d.t_name_metric IN (l_tableMetrics(2).t_name,l_tableMetrics(3).t_name)
          then
              sum(t_value) over (partition by d.t_name_metric, t.dt_end) 
        end as sum_m, 
        case 
           when d.t_name_metric = l_tableMetrics(1).t_name then l_tableMetrics(1).t_description
           when d.t_name_metric = l_tableMetrics(2).t_name then l_tableMetrics(2).t_description
           when d.t_name_metric = l_tableMetrics(3).t_name then l_tableMetrics(3).t_description
       end   as t_description,
        t.dt_end
    from DALLMETRICSKAFKA_DBT d left join t_dt t on (d.t_timestamp > t.dt_start and d.t_timestamp <= t.dt_end)
    where t.dt_end = trunc(p_timestamp,'hh24');
    
  END IF;
  
   
 EXCEPTION
  WHEN NO_RegVal       
      THEN  
        NULL;
  WHEN OTHERS         
   THEN 
     l_ErrorDesc := 'Произошла ошибка при сборе метрик.';  l_ErrorCode :=-1;    
     it_event.AddErrorITLog(p_SystemId    => 'SINV',
                            p_ServiceName => 'KafkaMetrics',
                            p_ErrorCode   => l_ErrorCode,
                            p_ErrorDesc   => l_ErrorDesc,
                            p_LevelInfo   => 8);  
                                                   
 END aggregateMetrics;


  /**
   @brief Cбор необходимых актуальных метрик с буферной таблицы, собирает актуальные данные на момент запуска процедуры
   @param[out] p_out_JSON Строка в виде JSON структуры
  */
  PROCEDURE getValuesMetrics(p_out_JSON out varchar2)

  IS
  l_ErrorDesc varchar2(3000);
  l_ErrorCode  integer;
  l_dt         date;
  BEGIN
    --если в диапазоне 14:00:01 b 18.00, то относится к группе 18
   l_dt := case 
                when sysdate between trunc(sysdate) + 14/24 + 1/24/60/60 and trunc(sysdate) + 18/24 
                  then  trunc(sysdate) + 18/24
                when  sysdate between trunc(sysdate) and trunc(sysdate) + 14/24 
                  then trunc(sysdate) + 14/24
                when  extract(HOUR from cast(sysdate as timestamp)) >= 18
                  then trunc(sysdate + 1) + 14/24    
             end;
           

   
  select JSON_OBJECT( 
           KEY 'metrics' VALUE 
               (SELECT JSON_ARRAYAGG( 
                         JSON_OBJECT( 
                           KEY 'name' VALUE d.t_name, 
                           KEY 'value' VALUE d.t_value,
                           KEY 'timestamp' VALUE d.t_timestamp,
                           KEY 'description' VALUE d.t_description
                         ) 
                       ) 
                FROM   DMETRICSKAFKA_DBT d where d.t_timestamp = l_dt
               ) 
                )
  into p_out_JSON              
  from dual;

 EXCEPTION
  WHEN OTHERS         
   THEN 
     l_ErrorDesc := 'Произошла ошибка при сборе метрик в JSON.';  l_ErrorCode :=-1;    
     it_event.AddErrorITLog(p_SystemId    => 'SINV',
                            p_ServiceName => 'KafkaMetrics',
                            p_ErrorCode   => l_ErrorCode,
                            p_ErrorDesc   => l_ErrorDesc,
                            p_LevelInfo   => 8);  
 END getValuesMetrics;
 
 
 /**
   @brief Заполнить коллекцию t_tableMetrics данными метрик sofr_new_fl_count, sofr_change_fl_count.
          Наименование - Описание.
          На случай если вздумается менять наименование или описание.
          l_tableMetrics(1) - кол-во новых клиентов.
          l_tableMetrics(2) - кол-во обновленных клиентов.
          l_tableMetrics(3) - кол-во сообщений по неторговым операциям
   @param[in] p_KeyPath    Путь к параметру, строковой
   @param[in] p_delim_out  Разделитель между метриками
   @param[in] p_delim_in   Разделитель внутри метрики
  */
 FUNCTION fillCollectMetrics(p_KeyPath   in varchar2, 
                             p_delim_out in varchar2 default ';', 
                             p_delim_in  in varchar2 default ':') return rsb_broker_monitoring.t_tableMetrics
                             
 is
 l_table_Metrics rsb_broker_monitoring.t_tableMetrics := rsb_broker_monitoring.t_tableMetrics();
 begin
  
 WITH G AS (
        SELECT rsb_common.GetRegStrValue(p_KeyPath => p_KeyPath) as str
          FROM dual
           ),
      T AS (
      SELECT regexp_substr(str, '[^'|| p_delim_out ||']+', 1, level) str
        FROM g
       WHERE g.str is not null
     CONNECT BY NVL(regexp_instr(str, '[^'|| p_delim_out ||']+', 1, level), 0) <> 0
           )
     SELECT substr(str,0,instr(str,p_delim_in) - 1) as m_name,
            substr(str,instr(str,p_delim_in) + 1) as m_desc
      BULK COLLECT INTO l_table_Metrics
      FROM t;

    RETURN l_table_Metrics;
 END fillCollectMetrics;
                             
end RSB_BROKER_MONITORING;
/