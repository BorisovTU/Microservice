create or replace package CDC_BY_TRIGGER AUTHID CURRENT_USER is

  -- Author  : TOMILIN-EE
  -- Created : 17.05.2024 19:12:52
  -- Purpose : BOSS-1642

/*********************************************************************************
   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.1       12.07.2024   Shishkin-EV       Адаптация CDC под 1642 

******************************************************************************/
  
  -- Public type declarations
type column_list is table of varchar(255) index by PLS_INTEGER;   -- тип под лист полей которые нужно захватить на триггере

  -- Public constant declarations

C_DEFAULT_TRIG_SUBSCR CONSTANT VARCHAR(128) := 'CDC_TRIG_SUBSCR'; -- подписка в триггере на вызове add_table_trigger
C_DEFAULT_TRIG_QUEUE CONSTANT VARCHAR(128) := 'CDC_TRIG_QUEUE';  -- очередь в триггере на вызове add_table_trigger

function add_table_trigger(          -- создать CDC триггер на таблице  
  p_table varchar2 default 'DUAL'    -- ссылка на таблицу куда добавлять триггер вида schema_name.table_name
 ,p_field_list column_list           -- включающий фильтр колонок , по которым нужно отслеживать изменение
 ,p_PK varchar2 default NULL         -- ссылка на первичный ключ , он будет откидываться в теле сообщения в очередь
) return varchar2 ;                  -- NULL если все ОК, иначе ошибка текстом

-- создать триггера 
function setup_CDC return varchar2;         -- вернет NULL если все ОК или описание ошибок в строке

procedure CDC_callback (  -- CallBack процедура которую дерагает подписка, из нее вызывается процедура сборки набора данных
          context IN RAW                    --
          ,reginfo IN SYS.AQ$_REG_INFO      -- 
          ,descr IN SYS.AQ$_DESCRIPTOR      -- 
          ,payload IN RAW                   --
          ,payloadl IN NUMBER);             -- length of payload
 
function flatList( -- свернуть лист в строчку с разделителями
         List column_list
         , Delim IN varchar default ','
) return varchar2; -- вернет NULL если все ОК или описание первой ошибки

procedure ExHandler  ( 
                msg varchar2 default null 
              , msg_type varchar2 default null
              , msg_clob clob default null);

end CDC_BY_TRIGGER;
