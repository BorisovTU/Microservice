create or replace package RSB_BROKER_MONITORING  is

 /**************************************************************************************************\
    BOSS-3399. Поздние требования: "Обеспечение процесса мониторинга бизнес-процесса создания
               и обновления клиентов из СОФР в Брокер 2.0"
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание
    ----------  ---------------  ------------------------------   -------------------------------------
    31.07.2024  Шестаков Д.В.     BOSS-3399                       Подсчет новых/обновленных клиентов в СИ
    26.12.2024  Шестаков Д.В.     BOSS-3574                       Подсчет отправленных сообщений для неторговых поручений в СИ
  \**************************************************************************************************/
  
  /*типы метрик*/
  C_COUNTER   CONSTANT NUMBER := 1;
  C_GAUGE     CONSTANT NUMBER := 2;
  C_HISTOGRAM CONSTANT NUMBER := 3;
  C_SUMMARY   CONSTANT NUMBER := 4;
  C_UNTYPED   CONSTANT NUMBER := 5;
  
  /*рекорд с информацией по метрикам */
  TYPE rec_metrics IS RECORD
  (
    t_name        VARCHAR2(128), 
    t_description VARCHAR2(512)
   );
   
  /*массив с информацией по метрикам */ 
  TYPE t_tableMetrics IS TABLE OF rec_metrics;


  /**
   @brief Сбор данных по метрикам 
   @param[in] p_partyid  Идентификатор клиента
   @param[in] p_tab_name  Наименование таблицы, в которй сработал триггер на изменение
   @param[in] p_actiontype Тип операции, изменение или добавление данных по клиенту. UPD|INS|DEL
  */
  procedure collectDataMetrics(p_partyid     dpersn_dbt.t_personid%type
                               ,p_tab_name    varchar2
                               ,p_actiontype  varchar2);

   /**
   @brief Cбор необходимых актуальных метрик с буферной таблицы, собирает актуальные данные на момент запуска процедуры
   @param[out] p_out_JSON Строка в виде JSON структуры
  */
  PROCEDURE getValuesMetrics(p_out_JSON out varchar2);
  
  /**
   @brief ХП1
          Группировка/агрегирование данных по собранным метрикам. Запускается oracle Sheduler - COLLECT_METRICS_JOB
          Запуск в 14.00 и 18.00. Группируем все собранные метрики в этом интервале по определенным правилам
   @param[in] p_timestamp - дата, на которую запускаем отбор метрик
  */                         
  PROCEDURE aggregateMetrics(p_timestamp date default sysdate); 
  
  /**
   @brief Очистка таблиц с данными по собранным метрикам. Запускается oracle Sheduler - CLEAR_METRICS_JOB
          Запуск в раз в сутки. Проверяет на заполняемость две таблицы DALLMETRICSKAFKA_DBT DMETRICSKAFKA_DBT
          каждую вторую неделю чистим данные
  */   
  PROCEDURE clearTableMetrics; 
  
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
  function fillCollectMetrics(p_KeyPath   in varchar2, 
                             p_delim_out in varchar2 default ';', 
                             p_delim_in  in varchar2 default ':') return rsb_broker_monitoring.t_tableMetrics;                       

end RSB_BROKER_MONITORING;
/