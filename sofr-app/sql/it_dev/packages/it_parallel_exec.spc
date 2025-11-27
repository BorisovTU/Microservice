create or replace package it_parallel_exec is

  /**************************************************************************************************\
    Распараллеливание вычислений
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
     05.12.2024  Зыков М.В.       DEF-77577                        Рефакторинг
     04.12.2023  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/
  function init_calc return number;

  procedure clear_calc(p_id number);

  function Date_to_sql(p_date date) return varchar2;

  function DateTime_to_sql(p_date date) return varchar2;

  function Str_to_sql(p_str varchar2) return varchar2;

  procedure run_task_chunks_by_sql(p_parallel_level integer
                                  ,p_chunk_sql      varchar2
                                  ,p_sql_stmt       varchar2
                                  ,p_comment        varchar2 default null );

  procedure run_task_chunks_by_calc(p_parallel_level integer
                                   ,p_id             number
                                   ,p_sql_stmt       varchar2
                                   ,p_force          number default 1 -- Если задания различны по времени исполнения  > 1
                                   ,p_comment        varchar2 default null );

 -- Cервис параллельного выполнения заданий ;
  procedure ParallelExec_by_sql(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype);


end it_parallel_exec;
/
