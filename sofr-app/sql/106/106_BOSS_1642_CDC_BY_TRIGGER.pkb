create or replace package body CDC_BY_TRIGGER is

procedure ExHandler  ( -- обертка для IT_LOG.LOG 
                msg varchar2 default null 
              , msg_type varchar2 default null    -- константами 'DEBUG' 'MSG' 'ERROR'
              , msg_clob clob default null
) AS
SQLText varchar(32676);
typ varchar (5);
BEGIN
typ := msg_type;
typ := NVL(typ , 'MSG');
SQLText :=  ' begin it_log.log(p_msg => '''|| msg || ''','
||CHR(13)|| ' p_msg_type => '''|| typ ||''','
||CHR(13)|| ' p_msg_clob => '''|| msg_clob ||''');end;';
--dbms_output.put_line(SQLText);   
execute immediate SQLText;
END ;

-- добавить триггер на таблицу
function add_table_trigger(
  p_table varchar2 default 'DUAL'            -- schema_name.table_name
 ,p_field_list column_list                   -- inclusion filter for columns, that should be logged
 ,p_PK varchar2 default NULL                 -- PK col sent in trigger message
) return varchar2
 AS                         -- NULL = OK, error description on fail

v_trigName varchar (255) ;
v_tab_no_schema varchar (255) ;       -- имя таблицы без схемы
v_sch_pos integer;
v_cols varchar (5000);
SQLText varchar(32676);
v_ret varchar(32676);
BEGIN
-- муторно через bind , просто собираем строку, ниже переменные для сборки
-- набил с переносами чтобы не ломать глаза в отладчике, подстановки в комментах 

v_sch_pos:= instr( p_table,'.');
if v_sch_pos <> 0 Then
  v_tab_no_schema := substr ( p_table, v_sch_pos+1);
  else
  v_tab_no_schema := p_table;
end if;

v_trigName :='TR_'|| v_tab_no_schema ||'_CDC' ;                    -- название триггера = TR_ + имя таблицы (без схемы) + _CDC
v_cols :=  flatList(List => p_field_list,Delim => ',');            -- разворачивает массиив в строку через ,

-- subst as comments
SQLText :=  ' CREATE OR REPLACE TRIGGER ' || v_trigName            -- v_trigName имя триггера
||CHR(13)|| '  FOR update or delete or insert'
||CHR(13)|| '   OF ' || v_cols                                     -- v_cols перечень колонок
||CHR(13)|| '   ON ' || p_table                                    -- P_table таблица , входной аргумент
||CHR(13)|| '  compound trigger'
||CHR(13)|| '  after each row is'
||CHR(13)|| ''
||CHR(13)|| '  msgkey RAW(32);'
||CHR(13)|| '  eqopts dbms_aq.enqueue_options_t;'
||CHR(13)|| '  msgprops dbms_aq.message_properties_t;'
||CHR(13)|| '  msgdata sys.anydata ;'
||CHR(13)|| '  msgmeta varchar2(128)  ;'
||CHR(13)|| '  reclist DBMS_AQ.aq$_recipient_list_t;'
||CHR(13)|| ''
||CHR(13)|| 'BEGIN'
||CHR(13)|| ''                                                     
||CHR(13)|| '   IF :NEW.' || p_PK || ' IS NOT NULL THEN'           -- p_PK входной аргумент , колонка с первичным ключом                   
||CHR(13)|| '      IF :OLD.' || p_PK || ' IS NOT NULL THEN'        -- p_PK входной аргумент , колонка с первичным ключом
||CHR(13)|| '         msgmeta := ''<XML action = "UPD" tab = "'|| v_tab_no_schema || '" pkcol = "' || p_PK || '" pk = "'' || CAST(:NEW.' || p_PK || ' AS VARchar2) || ''"/>'';'    -- p_PK входной аргумент , колонка с первичным ключом 
||CHR(13)|| '      ELSE'
||CHR(13)|| '         msgmeta := ''<XML action = "INS" tab = "'|| v_tab_no_schema || '" pkcol = "' || p_PK ||'" pk = "'' || CAST(:NEW.' || p_PK || ' AS VARchar2) || ''"/>'';'    -- p_PK входной аргумент , колонка с первичным ключом
||CHR(13)|| '      END IF;'
||CHR(13)|| '   ELSE  msgmeta := ''<XML action = "DEL" tab = "'|| v_tab_no_schema || '" pkcol = "' || p_PK ||'" pk = "'' || CAST(:OLD.' || p_PK || ' AS VARchar2) || ''"/>'';'    -- p_PK входной аргумент , колонка с первичным ключом
||CHR(13)|| '   END IF;'
||CHR(13)|| ''
||CHR(13)|| '   reclist(1):= SYS.AQ$_AGENT('''|| C_DEFAULT_TRIG_SUBSCR ||''', NULL, NULL);'   
||CHR(13)|| ''
||CHR(13)|| '   eqopts.visibility     := SYS.DBMS_AQ.IMMEDIATE;'
||CHR(13)|| '   eqopts.relative_msgid    := NULL;'
||CHR(13)|| '   eqopts.sequence_deviation  :=  NULL;'
||CHR(13)|| '   eqopts.transformation   := NULL;'
||CHR(13)|| '   eqopts.delivery_mode  := SYS.DBMS_AQ.BUFFERED ;'
||CHR(13)|| ''
||CHR(13)|| '   msgprops.priority    := 0;'
||CHR(13)|| '   msgprops.delay       := SYS.DBMS_AQ.NO_DELAY;'
||CHR(13)|| '   msgprops.expiration  :=  3600;'
||CHR(13)|| '   msgprops.correlation :=  msgmeta;'
||CHR(13)|| '   msgprops.recipient_list := reclist;'
||CHR(13)|| ''
||CHR(13)|| '   msgdata :=  anydata.ConvertVarchar2(msgmeta) ;'
||CHR(13)|| ''
||CHR(13)|| '  DBMS_AQ.ENQUEUE ('
||CHR(13)|| '   queue_name=> '''||C_DEFAULT_TRIG_QUEUE||''','                                
||CHR(13)|| '   enqueue_options     => eqopts,'
||CHR(13)|| '   message_properties  => msgprops,'
||CHR(13)|| '   payload             => msgdata ,'
||CHR(13)|| '   msgid               => msgkey );'
||CHR(13)|| ''
||CHR(13)|| 'EXCEPTION'
||CHR(13)|| '  WHEN OTHERS THEN '
||CHR(13)|| ' dbms_output.put_line(sqlerrm);'
||CHR(13)|| ' CDC_BY_TRIGGER.ExHandler(msg => sqlerrm,msg_type => ''ERROR'',msg_clob => NULL);'
||CHR(13)|| 'end after each row;'
||CHR(13)|| 'end '|| v_trigName ||';' ;                                                         -- v_trigName имя триггера

execute immediate SQLText;
CDC_BY_TRIGGER.ExHandler( 'trigger created ' || v_trigName , 'DEBUG', NULL);    -- откинуть в журнал
 return NULL;

exception 
 when others Then
   dbms_output.put_line(sqlerrm);   
  CDC_BY_TRIGGER.ExHandler(msg => sqlerrm, msg_type => 'ERROR', msg_clob => NULL);    --откинуть ошибку в журнал
  return sqlerrm;
END;

-- создать триггера на таблицы 
-- структура организованна намеренно , чтобы изменения в триггерах (конфигурации CDC) прокатывались через git и затирались на развертывании
function setup_CDC return varchar2 as v_ret varchar2(1024) ;                                 -- NULL = OK, error description on fail
 v_reth varchar(1024) ; -- holder for error codes

-- нейминг > v_ИМЯ ТАБЛИЦЫ_cols
 v_DPERSN_DBT_cols column_list ; 
 v_DPAPRKIND_DBT_cols column_list ;
 v_DPERSNIDC_DBT_cols column_list ;
 v_DADRESS_DBT_cols column_list ;
 v_DPARTY_DBT_cols column_list;
 v_DOBJCODE_DBT_cols column_list;
 v_DCONTACT_DBT_cols column_list;
 v_DDLCONTR_DBT_cols column_list; 
 v_DSCQINV_DBT_cols column_list; 
 v_DSFCONTR_DBT_cols column_list;
 v_DNOTETEXT_DBT_cols column_list;
 v_DOBJATTR_DBT_cols column_list;
 v_DDLCONTRMP_DBT_cols column_list;
 v_DDL_LIMITPRM_DBT_cols column_list;
 v_DACCOUNT_DBT_cols column_list;
 v_DSFCONTRPLAN_DBT_cols column_list;

 BEGIN
  v_DPERSN_DBT_cols(1) := 't_name1';
  v_DPERSN_DBT_cols(2) := 't_name2';
  v_DPERSN_DBT_cols(3) := 't_name3';
  v_DPERSN_DBT_cols(4) := 't_born';
  v_DPERSN_DBT_cols(5) := 't_ismale';
  v_DPERSN_DBT_cols(6) := 't_isemployer';
       
  v_ret := cdc_by_trigger.add_table_trigger(
           p_table => ('dpersn_dbt')
          ,p_field_list => v_DPERSN_DBT_cols
          ,p_PK =>'T_PERSONID');
  v_reth:= v_reth || v_ret;

  v_DPERSNIDC_DBT_cols(1) := 'T_PAPERSERIES';
  v_DPERSNIDC_DBT_cols(2) := 'T_PAPERNUMBER';
  v_DPERSNIDC_DBT_cols(3) := 'T_PAPERISSUEDDATE';
  v_DPERSNIDC_DBT_cols(4) := 'T_PAPERISSUER';
  v_DPERSNIDC_DBT_cols(5) := 'T_PAPERISSUERCODE';
  v_DPERSNIDC_DBT_cols(6) := 'T_ISMAIN';
  v_DPERSNIDC_DBT_cols(7) := 'T_ISNOTVALID';
  v_DPERSNIDC_DBT_cols(8) := 'T_VALIDTODATE';
  
  v_ret := cdc_by_trigger.add_table_trigger(
           p_table =>  ('DPERSNIDC_DBT')
          ,p_field_list => v_DPERSNIDC_DBT_cols
          ,p_PK =>'T_PERSONID');
  v_reth:= v_reth || v_ret;

  v_DADRESS_DBT_cols(1) := 't_adress';
  v_DADRESS_DBT_cols(2) := 't_type';

  v_ret := cdc_by_trigger.add_table_trigger(
           p_table =>  ('DADRESS_DBT') 
          ,p_field_list => v_DADRESS_DBT_cols
          ,p_PK =>'T_PARTYID');
  v_reth:= v_reth || v_ret;

v_DPARTY_DBT_cols(1) := 't_name';
v_DPARTY_DBT_cols(1) := 't_shortname';

v_ret := cdc_by_trigger.add_table_trigger(
            p_table =>  ('DPARTY_DBT') 
           ,p_field_list => v_DPARTY_DBT_cols
           ,p_PK =>'T_PARTYID');
v_reth:= v_reth || v_ret;

  v_DCONTACT_DBT_cols(1) := 'T_CONTACTTYPE';
  v_DCONTACT_DBT_cols(2) := 'T_VALUE';
  v_DCONTACT_DBT_cols(3) := 'T_ISMAIN';

  v_ret := cdc_by_trigger.add_table_trigger(
           p_table =>  ('DCONTACT_DBT') 
          ,p_field_list => v_DCONTACT_DBT_cols
          ,p_PK =>'T_RECID');
  v_reth:= v_reth || v_ret;

  v_DDLCONTR_DBT_cols(1) := 't_email';
  v_DDLCONTR_DBT_cols(2) := 't_iis';
 
  v_ret := cdc_by_trigger.add_table_trigger(
           p_table =>  ('DDLCONTR_DBT') 
          ,p_field_list => v_DDLCONTR_DBT_cols
          ,p_PK =>'T_DLCONTRID');
  v_reth:= v_reth || v_ret;

  v_DSCQINV_DBT_cols(1):= 't_regdate';
  v_DSCQINV_DBT_cols(2):= 't_sysdate';
  v_DSCQINV_DBT_cols(3):= 't_systime';
  v_DSCQINV_DBT_cols(4):= 't_changedate';
  v_DSCQINV_DBT_cols(5):= 't_state';

  v_ret := cdc_by_trigger.add_table_trigger(
           p_table =>  ('DSCQINV_DBT') 
          ,p_field_list => v_DSCQINV_DBT_cols
          ,p_PK =>'T_PARTYID');
  v_reth:= v_reth || v_ret;

  v_DSFCONTR_DBT_cols(1) := 't_number';
  v_DSFCONTR_DBT_cols(2) := 't_datebegin';
  v_DSFCONTR_DBT_cols(3) := 't_dateclose';
  v_DSFCONTR_DBT_cols(4) := 't_servkind';
  v_DSFCONTR_DBT_cols(5) := 't_servkindsub';
    
  v_ret := cdc_by_trigger.add_table_trigger(
           p_table =>  ('DSFCONTR_DBT') 
          ,p_field_list => v_DSFCONTR_DBT_cols
          ,p_PK =>'T_ID');
  v_reth:= v_reth || v_ret;

  v_DNOTETEXT_DBT_cols(1):='t_date';
  v_DNOTETEXT_DBT_cols(2):='t_text';

  v_ret := cdc_by_trigger.add_table_trigger(
           p_table =>  ('DNOTETEXT_DBT') 
          ,p_field_list => v_DNOTETEXT_DBT_cols
          ,p_PK =>'T_ID');
  v_reth:= v_reth || v_ret;

  v_DDLCONTRMP_DBT_cols(1):= 't_marketid';
  v_DDLCONTRMP_DBT_cols(2):= 't_mpcode';

  v_ret := cdc_by_trigger.add_table_trigger(
           p_table =>  ('DDLCONTRMP_DBT') 
          ,p_field_list => v_DDLCONTRMP_DBT_cols
          ,p_PK =>'T_ID');
  v_reth:= v_reth || v_ret;

  v_DACCOUNT_DBT_cols(1) := 't_account';
  v_DACCOUNT_DBT_cols(2) := 't_code_currency';
  v_DACCOUNT_DBT_cols(3) := 't_open_date';
  v_DACCOUNT_DBT_cols(4) := 't_close_date';

  v_ret := cdc_by_trigger.add_table_trigger(
           p_table =>  ('DACCOUNT_DBT') 
          ,p_field_list => v_DACCOUNT_DBT_cols
          ,p_PK =>'T_ACCOUNTID');
  v_reth:= v_reth || v_ret;

  v_DSFCONTRPLAN_DBT_cols(1) := 't_sfplanid';
  v_DSFCONTRPLAN_DBT_cols(2) := 't_begin';

  v_ret := cdc_by_trigger.add_table_trigger(
           p_table =>  ('DSFCONTRPLAN_DBT') 
          ,p_field_list => v_DSFCONTRPLAN_DBT_cols
          ,p_PK =>'T_ID');
  v_reth:= v_reth || v_ret;

  IF v_reth IS NOT NULL THEN
   dbms_standard.raise_application_error (num => -20001, msg => 'ошибки вызова setup_CDC '|| v_reth);
  ELSE
    CDC_BY_TRIGGER.ExHandler(msg => 'триггеры успешно развернуты setup_CDC ' , msg_type => 'DEBUG', msg_clob => NULL);    --откинуть успех в журнал
  END IF;

  return NULL;

 exception 
  when others Then
    dbms_output.put_line(sqlerrm);   
    CDC_BY_TRIGGER.ExHandler(msg => sqlerrm, msg_type => 'ERROR', msg_clob => NULL);    --откинуть ошибку в журнал
   return sqlerrm;
END;

procedure CDC_callback (                  -- эта процедура вызывается с подписки , вычитывает очередь, нельзя менять сигнатуру
           context  RAW                   --
          ,reginfo  SYS.AQ$_REG_INFO      -- 
          ,descr    SYS.AQ$_DESCRIPTOR    -- 
          ,payload  RAW                   --
          ,payloadl NUMBER) is            -- length of payload


  dequeue_options SYS.DBMS_AQ.dequeue_options_t;
  msgprops SYS.DBMS_AQ.message_properties_t;
  msgnum RAW(16);
  msg sys.anydata;
  msgmeta varchar2(128);
  v_ErrorCode varchar(1000);
  v_ErrorDesc varchar(1000);

BEGIN
     
        dequeue_options.consumer_name := 'CDC_TRIG_SUBSCR';                       -- имя вычитывающего потребителя из recipient_list в триггере
        dequeue_options.dequeue_mode := sys.dbms_aq.remove;                       -- BROWSE неблокирующий, но нужно будет стирать
        dequeue_options.navigation := SYS.DBMS_AQ.FIRST_MESSAGE;                  -- FIRST_MESSAGE,брать первое сверху
        dequeue_options.visibility := sys.dbms_aq.IMMEDIATE;                      -- ON_COMMIT блокирует. в этом же вызове        
        dequeue_options.wait := sys.dbms_aq.no_wait ;                             -- в секундах , не ждать = sys.dbms_aq.no_wait
        dequeue_options.delivery_mode := SYS.DBMS_AQ.PERSISTENT_OR_BUFFERED ;     -- только из памяти SYS.DBMS_AQ.BUFFERED  
        
        SYS.DBMS_AQ.DEQUEUE(
            queue_name => 'CDC_TRIG_QUEUE',
            dequeue_options => dequeue_options,
            message_properties => msgprops,
            payload => msg,
            msgid => msgnum );
   
         it_broker.Client_info (sys.anydata.AccessVarchar2(msg), v_ErrorCode, v_ErrorDesc);    
      
END;

function flatList(
           List IN column_list                             -- table of values
         , Delim IN varchar default ','                    -- delimeter used , mb several chars
) return varchar2 as 
v_out_str string (10000) ; 
begin
for i in 1 .. List.count loop
 v_out_str := v_out_str || List(i) || Delim;
end loop;
 return rtrim(v_out_str,Delim);
end;
 
end CDC_BY_TRIGGER;
/