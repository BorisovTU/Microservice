create or replace package body it_limit is

  /**
   @file it_limit.bdy
   @brief Пакет запуска расчета лимитов с использованием QManagera
   
    
   # tag
   - functional_block:Лимиты
   - code_type:API 
    
   # link
   - https://sdlc.go.rshbank.ru/confluence/pages/viewpage.action?pageId=2879234](21)
    
   # changeLog
   |date       |author      |tasks                                                     |note                                                        
   |-----------|------------|----------------------------------------------------------|-------------------------------------------------------------
   |2025.02.12 |Зыков М.В.  |BOSS-5710                                                 | Убрать из расчета лимитов нулевые лимиты
   |2024.06.15 |Зыков М.В.  |BOSS-2461 / BIQ-16667                                     | Перевод процедуры расчета лимитов на обработчик сервисов QManager                     
    
    
  */
  g_qserv_LimitBegin varchar2(128) := 'Limit.Begin';

  g_qserv_FillContrTablenotDeriv varchar2(128) := 'Limit.FillContrTablenotDeriv'; --
  g_qserv_FillContrTablebyDeriv  varchar2(128) := 'Limit.FillContrTablebyDeriv'; --
  g_qserv_FillContrTableAcc      varchar2(128) := 'Limit.FillContrTableAcc'; --
  g_qserv_FillContrTableCOM      varchar2(128) := 'Limit.FillContrTableCOM';

  g_qserv_CheckContrTable varchar2(128) := 'Limit.CheckContrTable';

  g_qserv_SetLotTmpStart varchar2(128) := 'Limit.LotTmpStart'; --
  g_qserv_SetTickTmp     varchar2(128) := 'Limit.TickTmp'; --
  g_qserv_SetLotTmp      varchar2(128) := 'Limit.LotTmp'; --
  g_qserv_SetCMTick      varchar2(128) := 'Limit.CollectPlanSumCur'; --

  g_qserv_CashStockLimits    varchar2(128) := 'Limit.CashStockLimits'; --
  g_qserv_CashStockLimByKind varchar2(128) := 'Limit.CashStockLimByKind';

  g_qserv_ClearSecurLimits varchar2(128) := 'Limit.ClearSecurLimits'; --
  g_qserv_SecurLimits      varchar2(128) := 'Limit.SecurLimits'; --
  g_qserv_SecurLimByKind   varchar2(128) := 'Limit.SecurLimByKind'; --
  g_qserv_WAPositionPrice  varchar2(128) := 'Limit.WAPositionPrice';

  g_qserv_CashStockLimitsCur    varchar2(128) := 'Limit.CashStockLimitsCur'; --
  g_qserv_CashStockLimCurByKind varchar2(128) := 'Limit.CashStockLimitsCurByKind';

  g_qserv_FutureMarkLimits varchar2(128) := 'Limit.FutureMarkLimits';

  g_qserv_ClearEDPLimits varchar2(128) := 'Limit.ClearLimitsEDP';

  g_qserv_CashEDPLimits    varchar2(128) := 'Limit.CashEDPLimits'; --
  g_qserv_CashEDPLimByKind varchar2(128) := 'Limit.CashEDPLimByKind';

  g_qserv_CashEDPLimitsCur    varchar2(128) := 'Limit.CashEDPLimitsCur'; --
  g_qserv_CashEDPLimCurByKind varchar2(128) := 'Limit.CashEDPLimitsCurByKind';

  g_qserv_CashFINISH varchar2(128) := 'Limit.CashFINISH';

  g_qserv_LimitFINISH varchar2(128) := 'Limit.FINISH';

  type tr_limit_start is record(
     Msgid           varchar2(128)
    ,CalcDate        date
    ,ByStockMB       number
    ,ByStockSPB      number
    ,ByCurMB         number
    ,ByFortsMB       number
    ,ByEDP           number
    ,MarketID        number
    ,MarketCount     number
    ,UseListClients  number
    ,parallel        number
    ,queue_num       itt_q_message_log.queue_num%type
    ,calc_panelcontr varchar2(128)
    ,calc_clientinfo varchar2(128));

  type tr_limit_create is record(
     Msgid           varchar2(128)
    ,NPP             number
    ,MarketID        number
    ,MarketCode      varchar2(64)
    ,ByStock         number
    ,ByCurr          number
    ,ByDeriv         number
    ,ByEDP           number
    ,mainsessionid   number
    ,calc_panelcontr varchar2(128)
    ,calc_clientinfo varchar2(128));

  type tr_LimitByKind is record(
     LimitService  varchar2(128)
    ,LimitMsgid    varchar2(128)
    ,RootSessionID number
    ,KindCnt       number
    ,Kind          number
    ,param1        number
    ,param2        number
    ,param3        number
    ,paramS1       varchar2(128));

  type tt_table_partition is table of varchar2(128) index by pls_integer; --
  gt_table_partition tt_table_partition;

  type tt_qservice_Limit is table of varchar2(128) index by pls_integer; --
  gt_qservice_Limit tt_qservice_Limit;

  gt_qservice_CashLimit tt_qservice_Limit;

  gt_qservice_AllLimit tt_qservice_Limit;

  -- Возвращает значение пакетной переменной по имени
  function get_constant_str(p_constant varchar2) return varchar2 deterministic as
    v_ret varchar2(32676);
  begin
    execute immediate ' begin :1 := it_limit.' || p_constant || '; end;'
      using out v_ret;
    return v_ret;
  exception
    when others then
      return null;
  end;

  procedure add_log(p_MarketID number
                   ,p_msg      varchar2) as
  begin
    null;
   /* it_log.log(p_msg => to_char(mod(extract(second from systimestamp), 1) * 1000000, '099999') || '-' || case
                          when p_MarketID is null then
                           'ALL MARKET'
                          else
                           'MarketID#' || p_MarketID
                        end || ':' || p_msg);*/
  end;

  procedure get_calc_sid(p_calc_direct     varchar2
                        ,o_calc_clientinfo out varchar2
                        ,o_calc_panelcontr out varchar2) as
  begin
    with param as
     (select xmltype((select l.t_calcparam from DDL_LIMITOP_DBT l where l.t_calc_direct = p_calc_direct)) x from dual)
    select EXTRACTVALUE(param.x, '/XML/@calc_panelcontr')
          ,EXTRACTVALUE(param.x, '/XML/@calc_clientinfo')
      into o_calc_panelcontr
          ,o_calc_clientinfo
      from param;
  end;

  procedure set_calc_sid(p_calc_direct varchar2) as
    v_calc_panelcontr varchar2(128);
    v_calc_clientinfo varchar2(128);
  begin
    RSHB_RSI_SCLIMIT.g_calc_DIRECT := p_calc_direct;
    get_calc_sid(p_calc_direct, v_calc_clientinfo, v_calc_panelcontr);
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := v_calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := v_calc_panelcontr;
  end;

  function get_calc_panelcontr(p_calc_direct varchar2 default null) return varchar2 deterministic as
    v_calc_panelcontr varchar2(128);
    v_calc_clientinfo varchar2(128);
  begin
    if p_calc_direct is null
    then
      return RSHB_RSI_SCLIMIT.g_calc_panelcontr;
    end if;
    get_calc_sid(p_calc_direct, v_calc_clientinfo, v_calc_panelcontr);
    return v_calc_panelcontr;
  end;

  function GetCalcSID return varchar2 as
  begin
    return to_char(sysdate, 'yyyymmdd') || '#' || its_main.nextval(); ---sys_guid();
  end;

  procedure CALCLIMITLOG(p_calc_direct  varchar2
                        ,p_CalcDate     date
                        ,p_action       pls_integer
                        ,p_label        varchar2
                        ,p_NPPmarket    pls_integer default 0
                        ,p_MarketCode   varchar2 default null
                        ,p_group        pls_integer default 1000
                        ,p_dtstart      timestamp default null
                        ,p_dtend        timestamp default SYSTIMESTAMP
                        ,p_EXCEPSQLCODE pls_integer default null) as
    pragma autonomous_transaction;
    v_ACTION number;
  begin
    if not nvl(p_group, -1) between 0 and 9999
    then
      raise_application_error(-20000, 'Ошибка параметров функции it_limit.CALCLIMITLOG ! p_group=' || p_group);
    end if;
    if not nvl(p_NPPmarket, -1) between 0 and 9
    then
      raise_application_error(-20000, 'Ошибка параметров функции it_limit.CALCLIMITLOG ! p_NPPmarket=' || p_NPPmarket);
    end if;
    if not nvl(abs(p_action), -1) between 0 and 999999
    then
      raise_application_error(-20000, 'Ошибка параметров функции it_limit.CALCLIMITLOG ! p_action=' || p_action);
    end if;
    v_ACTION := case
                  when p_action >= 0 then
                   1
                  else
                   -1
                end * to_number(p_group || p_NPPmarket || lpad(abs(p_action), 6, '0'));
    insert into DCALCLIMITLOG_DBT
      (T_DATE
      ,T_LABEL
      ,T_START
      ,T_END
      ,T_ACTION
      ,T_EXCEPSQLCODE
      ,T_CALC_DIRECT)
    values
      (p_CalcDate
      ,p_MarketCode || ' ' || trim(p_label)
      ,p_dtstart
      ,nvl(p_dtend, systimestamp)
      ,v_ACTION
      ,p_EXCEPSQLCODE
      ,p_calc_direct);
    commit;
  end;

  procedure table_add_partition(p_table    varchar2
                               ,v_sid_calc varchar2) as
  begin
    execute immediate 'alter table ' || p_table || ' add partition p' || v_sid_calc || ' values (''' || v_sid_calc || ''')';
  exception
    when others then
      it_log.log(p_msg => 'Error ADD partition p_table=' || p_table || ' v_sid_calc=' || v_sid_calc);
      raise;
  end;

  procedure table_clear_partition(p_table    varchar2
                                 ,v_sid_calc varchar2) as
  begin
    for tab in (select p.partition_name
                  from user_tab_partitions p
                 where p.table_name = upper(p_table)
                   and ((p.partition_name = upper('p' || v_sid_calc) and 'DDL_CLIENTINFO_DBT' != upper(p_table)) --  Не удаляем текущий расчет 
                       or substr(p.partition_name, 2, 8) < to_char(sysdate - 1, 'yyyymmdd')))
    loop
      begin
        execute immediate 'alter table ' || p_table || ' drop partition ' || tab.partition_name;
      exception
        when others then
          null;
      end;
    end loop;
  end;

  function select_Market(p_messmeta xmltype) return tt_Market
    pipelined as
  begin
    for cur in (with x as
                   (select p_messmeta xml_data from dual)
                  select res.*
                    from x
                        ,XMLTABLE('*/Start/MarketList/Market' PASSING x.xml_data COLUMNS NPP number(10) PATH 'NPP'
                                 ,MarketID number(10) PATH 'MarketID'
                                 ,MarketCode varchar2(64) PATH 'MarketCode'
                                 ,ByStock number(10) PATH 'ByStock'
                                 ,ByCurr number(10) PATH 'ByCurr'
                                 ,ByDeriv number(10) PATH 'ByDeriv'
                                 ,ByEDP number(10) PATH 'ByEDP'
                                 ,MsgID varchar2(128) PATH 'MsgID') res)
    loop
      pipe row(cur);
    end loop;
  end;

  function select_parallel_sid(p_messmeta xmltype) return tt_parallel_sid
    pipelined as
  begin
    for cur in (with x as
                   (select p_messmeta xml_data from dual)
                  select res.*
                    from x
                        ,XMLTABLE('*/Start/ParallelCALC/ParallelSID' PASSING x.xml_data COLUMNS Num number PATH 'Num'
                                 ,calc_panelcontr varchar2(128) PATH 'calc_panelcontr'
                                 ,addparam varchar2(4000) PATH 'addparam') res)
    loop
      pipe row(cur);
    end loop;
  end;

  function xml_insert_Market(p_messmeta   xmltype
                            ,p_npp        pls_integer
                            ,p_MarketID   pls_integer
                            ,p_MarketCode varchar2
                            ,p_ByStock    pls_integer
                            ,p_ByCurr     pls_integer
                            ,p_ByDeriv    pls_integer
                            ,p_ByEDP      pls_integer
                            ,p_MsgID      varchar2) return xmltype as
    vx_res xmltype;
  begin
    select insertchildxml(p_messmeta
                         ,'*/Start/MarketList'
                         ,'Market'
                         ,xmlelement("Market"
                                     ,xmlelement("NPP", p_npp)
                                     ,xmlelement("MarketID", p_MarketID)
                                     ,xmlelement("MarketCode", p_MarketCode)
                                     ,xmlelement("ByStock", p_ByStock)
                                     ,xmlelement("ByCurr", p_ByCurr)
                                     ,xmlelement("ByDeriv", p_ByDeriv)
                                     ,xmlelement("ByEDP", p_ByEDP)
                                     ,xmlelement("MsgID", p_MsgID)))
      into vx_res
      from dual;
    return vx_res;
  end;

  function xml_insert_Create(p_messmeta        xmltype
                            ,p_Msgid           varchar2
                            ,p_NPP             pls_integer
                            ,p_MarketID        pls_integer
                            ,p_MarketCode      varchar2
                            ,p_ByStock         pls_integer
                            ,p_ByCurr          pls_integer
                            ,p_ByDeriv         pls_integer
                            ,p_ByEDP           pls_integer
                            ,p_calc_panelcontr varchar2
                            ,p_calc_clientinfo varchar2) return xmltype as
    vx_res xmltype;
  begin
    select deletexml(p_messmeta, '*/Create') into vx_res from dual;
    select insertchildxml(vx_res
                         ,'*'
                         ,'Create'
                         ,xmlelement("Create"
                                     ,xmlelement("Msgid", p_Msgid)
                                     ,xmlelement("NPP", p_NPP)
                                     ,xmlelement("MarketID", p_MarketID)
                                     ,xmlelement("MarketCode", p_MarketCode)
                                     ,xmlelement("ByStock", p_ByStock)
                                     ,xmlelement("ByCurr", p_ByCurr)
                                     ,xmlelement("ByDeriv", p_ByDeriv)
                                     ,xmlelement("ByEDP", p_ByEDP)
                                     ,xmlelement("mainsessionid", USERENV('sessionid'))
                                     ,xmlelement("calc_panelcontr", p_calc_panelcontr)
                                     ,xmlelement("calc_clientinfo", p_calc_clientinfo)))
      into vx_res
      from dual;
    return vx_res;
  end;

  function xml_insert_LimitByKind(p_messmeta     xmltype
                                 ,p_LimitService itt_q_message_log.servicename%type
                                 ,p_LimitMsgid   itt_q_message_log.msgid%type
                                 ,p_KindCnt      pls_integer
                                 ,p_Kind         pls_integer
                                 ,p_param1       pls_integer
                                 ,p_param2       pls_integer
                                 ,p_param3       pls_integer
                                 ,p_paramS1      varchar2) return xmltype as
    vx_res xmltype;
  begin
    select deletexml(p_messmeta, '*/LimitByKind') into vx_res from dual;
    select insertchildxml(vx_res
                         ,'*'
                         ,'LimitByKind'
                         ,xmlelement("LimitByKind"
                                     ,xmlelement("LimitService", p_LimitService)
                                     ,xmlelement("LimitMsgid", p_LimitMsgid)
                                     ,xmlelement("RootSessionID", USERENV('sessionid'))
                                     ,xmlelement("KindCnt", p_KindCnt)
                                     ,xmlelement("Kind", p_Kind)
                                     ,xmlelement("param1", p_param1)
                                     ,xmlelement("param2", p_param2)
                                     ,xmlelement("param3", p_param3)
                                     ,xmlelement("paramS1", it_xml.encode_spec_chr(p_paramS1))))
      into vx_res
      from dual;
    return vx_res;
  end;

  function xml_insert_ServiceParallel(p_messmeta        xmltype
                                     ,p_Num             pls_integer
                                     ,p_calc_panelcontr varchar2
                                     ,p_addparam        varchar2 default null) return xmltype as
    vx_res xmltype;
  begin
    select deletexml(p_messmeta, '*/ServiceParallel') into vx_res from dual;
    select insertchildxml(vx_res
                         ,'*'
                         ,'ServiceParallel'
                         ,xmlelement("ServiceParallel", xmlelement("Num", p_Num), xmlelement("calc_panelcontr", p_calc_panelcontr), xmlelement("addparam", p_addparam)))
      into vx_res
      from dual;
    return vx_res;
  end;

  function get_param_limit_start(p_xmeta xmltype) return tr_limit_start as
    v_res tr_limit_start;
  begin
    with meta as
     (select p_xmeta as x from dual)
    select EXTRACTVALUE(meta.x, '*/Start/Msgid')
          ,trunc(it_xml.char_to_date_iso8601(EXTRACTVALUE(meta.x, '*/Start/CalcDate')))
          ,to_number(EXTRACTVALUE(meta.x, '*/Start/ByStockMB'))
          ,to_number(EXTRACTVALUE(meta.x, '*/Start/ByStockSPB'))
          ,to_number(EXTRACTVALUE(meta.x, '*/Start/ByCurMB'))
          ,to_number(EXTRACTVALUE(meta.x, '*/Start/ByFortsMB'))
          ,to_number(EXTRACTVALUE(meta.x, '*/Start/ByEDP'))
          ,to_number(EXTRACTVALUE(meta.x, '*/Start/MarketID'))
          ,to_number(EXTRACTVALUE(meta.x, '*/Start/MarketCount'))
          ,to_number(EXTRACTVALUE(meta.x, '*/Start/UseListClients'))
          ,to_number(EXTRACTVALUE(meta.x, '*/Start/parallel'))
          ,EXTRACTVALUE(meta.x, '*/Start/queue_num')
          ,EXTRACTVALUE(meta.x, '*/Start/calc_panelcontr')
          ,EXTRACTVALUE(meta.x, '*/Start/calc_clientinfo')
      into v_res
      from meta;
    return v_res;
  end;

  function get_param_limit_create(p_xmeta xmltype) return tr_limit_create as
    v_res tr_limit_create;
  begin
    with meta as
     (select p_xmeta as x from dual)
    select EXTRACTVALUE(meta.x, '*/Create/Msgid')
          ,to_number(EXTRACTVALUE(meta.x, '*/Create/NPP'))
          ,to_number(EXTRACTVALUE(meta.x, '*/Create/MarketID'))
          ,EXTRACTVALUE(meta.x, '*/Create/MarketCode')
          ,to_number(EXTRACTVALUE(meta.x, '*/Create/ByStock'))
          ,to_number(EXTRACTVALUE(meta.x, '*/Create/ByCurr'))
          ,to_number(EXTRACTVALUE(meta.x, '*/Create/ByDeriv'))
          ,to_number(EXTRACTVALUE(meta.x, '*/Create/ByEDP'))
          ,to_number(EXTRACTVALUE(meta.x, '*/Create/mainsessionid'))
          ,EXTRACTVALUE(meta.x, '*/Create/calc_panelcontr')
          ,EXTRACTVALUE(meta.x, '*/Create/calc_clientinfo')
      into v_res
      from meta;
    return v_res;
  end;

  function get_param_LimitByKind(p_xmeta xmltype) return tr_LimitByKind as
    v_res tr_LimitByKind;
  begin
    with meta as
     (select p_xmeta as x from dual)
    select EXTRACTVALUE(meta.x, '*/LimitByKind/LimitService')
          ,EXTRACTVALUE(meta.x, '*/LimitByKind/LimitMsgid')
          ,to_number(EXTRACTVALUE(meta.x, '*/LimitByKind/RootSessionID'))
          ,to_number(EXTRACTVALUE(meta.x, '*/LimitByKind/KindCnt'))
          ,to_number(EXTRACTVALUE(meta.x, '*/LimitByKind/Kind'))
          ,to_number(EXTRACTVALUE(meta.x, '*/LimitByKind/param1'))
          ,to_number(EXTRACTVALUE(meta.x, '*/LimitByKind/param2'))
          ,to_number(EXTRACTVALUE(meta.x, '*/LimitByKind/param3'))
          ,it_xml.decode_spec_chr(EXTRACTVALUE(meta.x, '*/LimitByKind/paramS1'))
      into v_res
      from meta;
    return v_res;
  end;

  function get_param_ServiceParallel(p_xmeta xmltype) return tr_parallel_sid as
    v_res tr_parallel_sid;
  begin
    with meta as
     (select p_xmeta as x from dual)
    select to_number(EXTRACTVALUE(meta.x, '*/ServiceParallel/Num'))
          ,EXTRACTVALUE(meta.x, '*/ServiceParallel/calc_panelcontr')
          ,EXTRACTVALUE(meta.x, '*/ServiceParallel/addparam')
      into v_res
      from meta;
    return v_res;
  end;

  function service_is_start(p_servicename varchar2
                           ,p_corrmsgid   varchar2 default null
                           ,p_calc_direct varchar2 default null
                           ,p_MarketID    integer default null
                           ,p_messmeta    xmltype default null) return boolean as
    cnt                  pls_integer;
    v_MarketID           integer := p_MarketID;
    vr_param_limit_start tr_limit_start;
  begin
    if p_servicename is null
    then
      return false;
    end if;
    if v_MarketID is null
    then
      vr_param_limit_start := get_param_limit_start(p_messmeta);
      v_MarketID           := vr_param_limit_start.MarketID;
    end if;
    if v_MarketID is null
    then
      select count(*)
        into cnt
        from itt_q_message_log l
       where l.queuetype = 'OUT'
         and l.corrmsgid = p_corrmsgid
         and l.servicename = p_servicename;
    else
      if p_calc_direct is null
      then
        select count(*)
          into cnt
          from (select to_number(EXTRACTVALUE(xmltype(l.messmeta), '*/Create/MarketID')) MarketID
                -- from table(it_q_message.select_answer_msg(p_msgid => p_corrmsgid, p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT)) l
                  from itt_q_message_log l
                 where l.queuetype = 'OUT'
                   and l.corrmsgid = p_corrmsgid
                   and l.servicename = p_servicename)
         where MarketID = v_MarketID;
      else
        select count(*)
          into cnt
          from (select to_number(EXTRACTVALUE(xmltype(l.messmeta), '*/Create/MarketID')) MarketID
                  from table(it_q_message.select_answer_msg(p_msgid => p_calc_direct, p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT)) l
                 where l.servicename = p_servicename)
         where MarketID = v_MarketID;
      end if;
    end if;
    return(cnt > 0);
  end;

  procedure qmanager_load_msg(p_servicename  itt_q_message_log.servicename%type
                             ,p_messmeta     xmltype
                             ,p_corrmsgid    itt_q_message_log.corrmsgid%type default null
                             ,p_msgid        itt_q_message_log.msgid%type default null
                             ,p_withparallel boolean default false
                             ,p_add_log      varchar2 default null) as
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    v_msgid               itt_q_message_log.msgid%type := p_msgid;
    v_corrmsgid           itt_q_message_log.msgid%type := p_corrmsgid;
    v_messmeta            xmltype := p_messmeta;
  begin
    vr_param_limit_create := get_param_limit_create(p_messmeta);
    if v_corrmsgid is null
    then
      vr_param_limit_start := get_param_limit_start(v_messmeta);
      v_corrmsgid          := vr_param_limit_start.Msgid;
    end if;
    if not p_withparallel
    then
      select deletexml(v_messmeta, '*/ServiceParallel') into v_messmeta from dual;
      select deletexml(v_messmeta, '*/LimitByKind') into v_messmeta from dual;
      if service_is_start(p_servicename => p_servicename, p_corrmsgid => v_corrmsgid, p_messmeta => p_messmeta)
      then
        add_log(p_MarketID => vr_param_limit_create.MarketID, p_msg => p_add_log || ' Повторный запуск ' || p_servicename);
        return;
      end if;
      /*else
      vr_param_limit_parallel := get_param_ServiceParallel(p_messmeta);
      if vr_param_limit_parallel.num is not null
        -- select deletexml(v_messmeta, '*\LimitByKind') into v_messmeta from dual;
      
      then
      else
      end if;*/
    end if;
    it_q_message.load_msg(io_msgid => v_msgid
                         ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                         ,p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
                          --,p_Sender =>
                          --,p_Priority =>
                          --,p_Correlation =>
                         ,p_CORRmsgid => v_corrmsgid
                          --,p_SenderUser =>
                         ,p_ServiceName => p_servicename
                          --,p_ServiceGroup =>
                          --,p_Receiver =>
                          --,p_BTUID =>
                          --,p_MSGCode =>
                          --,p_MSGText =>
                          --,p_MESSBODY =>
                         ,p_MessMETA => v_messmeta
                          --,p_queue_num => p_queue_num
                          --,p_RequestDT =>
                          --,p_ESBDT =>
                          --,p_delay =>
                          --,p_comment =>
                          );
    add_log(p_MarketID => vr_param_limit_create.MarketID, p_msg => ' СТАРТ ' || p_servicename || ' ' || p_add_log);
  end;

  procedure qmanager_do_service_market(p_servicename1 itt_q_message_log.servicename%type
                                      ,p_servicename2 itt_q_message_log.servicename%type default null
                                      ,p_servicename3 itt_q_message_log.servicename%type default null
                                      ,p_messmeta     xmltype
                                      ,p_MarketID     integer default null) as
    vx_messmeta          xmltype := p_messmeta;
    vr_param_limit_start tr_limit_start;
    v_Msgid              itt_q_message_log.msgid%type;
  begin
    vr_param_limit_start := get_param_limit_start(p_messmeta);
    for cur in (select * from table(select_market(p_messmeta)) m where m.MarketID = nvl(p_MarketID, m.MarketID))
    loop
      if p_servicename1 = g_qserv_LimitBegin
      then
        v_Msgid := cur.Msgid;
      else
        v_Msgid := it_q_message.get_sys_guid;
      end if;
      vx_messmeta := xml_insert_Create(p_messmeta => vx_messmeta
                                      ,p_Msgid => cur.Msgid
                                      ,p_Npp => cur.NPP
                                      ,p_MarketID => cur.MarketID
                                      ,p_MarketCode => cur.MarketCode
                                      ,p_ByStock => cur.ByStock
                                      ,p_ByCurr => cur.ByCurr
                                      ,p_ByDeriv => cur.ByDeriv
                                      ,p_ByEDP => cur.ByEDP
                                      ,p_calc_panelcontr => vr_param_limit_start.calc_panelcontr
                                      ,p_calc_clientinfo => vr_param_limit_start.calc_clientinfo);
      qmanager_load_msg(p_servicename => p_servicename1, p_messmeta => vx_messmeta, p_corrmsgid => vr_param_limit_start.Msgid, p_Msgid => v_Msgid);
      if p_servicename2 is not null
      then
        qmanager_load_msg(p_servicename => p_servicename2, p_messmeta => vx_messmeta, p_corrmsgid => vr_param_limit_start.Msgid);
      end if;
      if p_servicename3 is not null
      then
        qmanager_load_msg(p_servicename => p_servicename3, p_messmeta => vx_messmeta, p_corrmsgid => vr_param_limit_start.Msgid);
      end if;
    end loop;
  end;

  procedure qmanager_do_service_ByKind(p_servicename  itt_q_message_log.servicename%type
                                      ,p_messmeta     xmltype
                                      ,p_LimitService itt_q_message_log.servicename%type
                                      ,p_LimitMsgid   itt_q_message_log.msgid%type
                                      ,p_param1       number
                                      ,p_param2       number
                                      ,p_param3       number default 0
                                      ,p_paramS1      varchar2 default null
                                      ,p_KindCnt      number default 4) as
    vx_messmeta           xmltype := p_messmeta;
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
  begin
    vr_param_limit_start  := get_param_limit_start(p_messmeta);
    vr_param_limit_create := get_param_limit_create(p_messmeta);
    if service_is_start(p_servicename => p_servicename, p_corrmsgid => vr_param_limit_start.Msgid, p_messmeta => p_messmeta)
    then
      add_log(p_MarketID => vr_param_limit_create.MarketID
             ,p_msg => 'do_service_ByKind повторный запуск ' || p_servicename || ' corrmsgid=' || vr_param_limit_start.Msgid);
      return;
    end if;
    for Kind in 1 .. p_KindCnt
    loop
      vx_messmeta := xml_insert_LimitByKind(p_messmeta => vx_messmeta
                                           ,p_LimitService => p_LimitService
                                           ,p_LimitMsgid => p_LimitMsgid
                                           ,p_KindCnt => p_KindCnt
                                           ,p_Kind => Kind
                                           ,p_param1 => p_param1
                                           ,p_param2 => p_param2
                                           ,p_param3 => p_param3
                                           ,p_paramS1 => p_paramS1);
      qmanager_load_msg(p_servicename => p_servicename
                       ,p_messmeta => vx_messmeta
                       ,p_corrmsgid => vr_param_limit_start.Msgid
                       ,p_withparallel => true
                       ,p_add_log => ' Kind=' || Kind);
    end loop;
  end;

  procedure qmanager_do_service_parallel(p_servicename itt_q_message_log.servicename%type
                                        ,p_messmeta    xmltype
                                        ,p_addparam    varchar2 default null
                                        ,p_MarketID    integer default null) as
    vx_messmeta           xmltype := p_messmeta;
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
  begin
    vr_param_limit_start  := get_param_limit_start(vx_messmeta);
    vr_param_limit_create := get_param_limit_create(vx_messmeta);
    if p_MarketID is not null
    then
      for cur in (select * from table(select_market(vx_messmeta)) m where m.MarketID = p_MarketID)
      loop
        vx_messmeta := xml_insert_Create(p_messmeta => vx_messmeta
                                        ,p_Msgid => cur.Msgid
                                        ,p_Npp => cur.NPP
                                        ,p_MarketID => cur.MarketID
                                        ,p_MarketCode => cur.MarketCode
                                        ,p_ByStock => cur.ByStock
                                        ,p_ByCurr => cur.ByCurr
                                        ,p_ByDeriv => cur.ByDeriv
                                        ,p_ByEDP => cur.ByEDP
                                        ,p_calc_panelcontr => vr_param_limit_start.calc_panelcontr
                                        ,p_calc_clientinfo => vr_param_limit_start.calc_clientinfo);
      end loop;
      vr_param_limit_create := get_param_limit_create(vx_messmeta);
    end if;
    if service_is_start(p_servicename => p_servicename, p_corrmsgid => vr_param_limit_start.Msgid, p_messmeta => p_messmeta)
    then
      add_log(p_MarketID => vr_param_limit_create.MarketID
             ,p_msg => 'do_service_parallel повторный запуск ' || p_servicename || ' corrmsgid=' || vr_param_limit_start.Msgid);
      return;
    end if;
    if vr_param_limit_start.UseListClients <> 0
    then
      vx_messmeta := xml_insert_ServiceParallel(p_messmeta => vx_messmeta, p_num => 1, p_calc_panelcontr => vr_param_limit_start.calc_panelcontr, p_addparam => p_addparam);
      qmanager_load_msg(p_servicename => p_servicename, p_messmeta => vx_messmeta, p_corrmsgid => vr_param_limit_start.Msgid, p_withparallel => true);
    else
      for p in (select * from table(select_parallel_sid(vx_messmeta)))
      loop
        vx_messmeta := xml_insert_ServiceParallel(p_messmeta => vx_messmeta, p_num => p.Num, p_calc_panelcontr => p.calc_panelcontr, p_addparam => p_addparam);
        qmanager_load_msg(p_servicename => p_servicename
                         ,p_messmeta => vx_messmeta
                         ,p_corrmsgid => vr_param_limit_start.Msgid
                         ,p_withparallel => true
                         ,p_add_log => 'Parallel#' || p.Num);
      end loop;
    end if;
  end;

  procedure qmanager_do_service_FINISH(p_Service    itt_q_message_log.servicename%type
                                      ,p_KeyService itt_q_message_log.servicename%type
                                      ,p_messmeta   xmltype
                                      ,p_corrmsgid  itt_q_message_log.msgid%type default null
                                      ,p_is_tran    boolean default true) as
    vr_param_limit_start tr_limit_start;
    vx_messmeta          xmltype := p_messmeta;
    v_msgid              itt_q_message_log.msgid%type;
  begin
    vr_param_limit_start := get_param_limit_start(p_messmeta);
    select deletexml(p_messmeta, '*/FINISH') into vx_messmeta from dual;
    select insertchildxml(vx_messmeta, '*', 'FINISH', xmlelement("FINISH", xmlelement("KeyService", p_KeyService))) into vx_messmeta from dual;
    if p_is_tran
    then
      qmanager_load_msg(p_servicename => p_Service
                       ,p_messmeta => vx_messmeta
                       ,p_corrmsgid => nvl(p_corrmsgid, vr_param_limit_start.Msgid)
                       ,p_withparallel => true
                       ,p_add_log => 'KeyService=' || p_KeyService);
    else
      it_q_message.do_a_service(p_servicename => p_Service
                                -- ,p_receiver =>
                                -- ,p_messbody =>
                               ,p_messmeta => vx_messmeta
                                -- ,p_servicegroup =>
                                -- ,p_queue_num =>
                               ,p_corrmsgid => nvl(p_corrmsgid, vr_param_limit_start.Msgid)
                                -- ,p_comment =>
                                -- ,p_delay =>
                               ,io_msgid => v_msgid);
    end if;
  end;

  function qmanager_chk_startservice(p_MarketID    integer
                                    ,p_servicename varchar2
                                    ,p_corrmsgid   itt_q_message_log.msgid%type
                                    ,p_lockmsgid   itt_q_message_log.msgid%type
                                    ,p_chkService  itt_q_message_log.servicename%type
                                    ,p_KeyService  itt_q_message_log.servicename%type default null) return boolean as
    vr_mess_log      itt_q_message_log%rowtype;
    v_count_pre_exec integer;
  begin
    if p_lockmsgid is not null
    then
      vr_mess_log := it_q_message.messlog_get_withlock(p_msgid => p_lockmsgid);
      if vr_mess_log.log_id is null
      then
        add_log(p_MarketID => p_MarketID, p_msg => 'заблокировано СТАРТ сообщение ' || p_servicename || ' lockmsgid=' || p_lockmsgid);
        return false;
      end if;
    end if;
    if p_KeyService is not null
       and p_chkService is not null
    then
      select count(*)
        into v_count_pre_exec
        from (select to_number(EXTRACTVALUE(xmltype(l.messmeta), '*/Create/MarketID')) MarketID
                    ,EXTRACTVALUE(xmltype(l.messmeta), '*/FINISH/KeyService') KeyService
                from itt_q_message_log l
               where l.queuetype = 'OUT'
                 and l.corrmsgid = p_corrmsgid
                 and l.servicename = p_chkService)
       where MarketID = p_MarketID
         and KeyService = p_KeyService;
      if v_count_pre_exec > 0
      then
        add_log(p_MarketID => p_MarketID, p_msg => p_chkService || 'Повторный запуск c ' || p_KeyService);
        return false;
      end if;
    else
      if service_is_start(p_servicename => p_chkService, p_calc_direct => p_corrmsgid, p_MarketID => p_MarketID)
      then
        add_log(p_MarketID => p_MarketID, p_msg => 'Повторный запуск ' || p_chkService || ' для ' || p_servicename);
        return false;
      end if;
    end if;
    if p_chkService is not null
    then
      add_log(p_MarketID => p_MarketID, p_msg => 'ПРОДОЛЖЕНИЕ  ' || p_servicename || ' lockmsgid=' || p_lockmsgid || ' corrmsgid=' || p_corrmsgid);
    end if;
    return true;
  end;

  function qmanager_wait_service_allMarket(p_cnt_Market      integer
                                          ,p_servicenameList varchar2
                                          ,p_cnt_service     integer
                                          ,p_corrmsgid       itt_q_message_log.msgid%type
                                          ,p_KeyService      itt_q_message_log.servicename%type) return boolean as
    v_count_pre_exec integer;
    vr_mess_log      itt_q_message_log%rowtype;
  begin
    execute immediate 'select count(*)
  from (select distinct MarketID
                       ,servicename
          from (select to_number(EXTRACTVALUE(xmltype(l.messmeta), ''*/Create/MarketID'')) MarketID
                      ,l.servicename
                  from itt_q_message_log l
                 where l.queuetype = ''IN''
                   and l.corrmsgid = :p_corrmsgid
                   and l.servicename in (' || p_servicenameList || ')
                   and l.status in (select * from table(it_q_message.select_status(p_kind_status => ''DONE'')))))'
      into v_count_pre_exec
      using p_corrmsgid;
    if v_count_pre_exec < (p_cnt_Market * p_cnt_service)
    then
      add_log(p_MarketID => null, p_msg => 'Закончен ' || v_count_pre_exec || ' из ' || (p_cnt_Market * p_cnt_service) || ' ' || p_servicenameList);
      return false;
    end if;
    execute immediate 'select count(*) from (select count(*) over(partition by MarketID,l.servicename) cnt ,l.parallel ,num
          from (select to_number(EXTRACTVALUE(xmltype(l.messmeta), ''*/Create/MarketID'')) MarketID
                      ,to_number(EXTRACTVALUE(xmltype(l.messmeta), ''*/Start/parallel'')) parallel
                      ,to_number(EXTRACTVALUE(xmltype(l.messmeta), ''*/ServiceParallel/Num'')) Num
                      ,l.servicename
                  from itt_q_message_log l
                 where l.queuetype = ''IN''
                   and l.corrmsgid = :p_corrmsgid
                   and l.servicename in (' || p_servicenameList || ')
                   and l.status in (select * from table(it_q_message.select_status(p_kind_status => ''DONE'')))) l
         where num is not null) where cnt != parallel'
      into v_count_pre_exec
      using p_corrmsgid;
    if v_count_pre_exec != 0
    then
      add_log(p_MarketID => null, p_msg => 'Не все паралельные процессы ' || v_count_pre_exec || p_servicenameList);
      return false;
    end if;
    vr_mess_log := it_q_message.messlog_get_withlock(p_msgid => p_corrmsgid);
    if vr_mess_log.log_id is null
    then
      add_log(p_MarketID => null, p_msg => 'Другим сервисом заблокировано СТАРТ сообщение ' || p_servicenameList);
      return false;
    end if;
    if service_is_start(p_servicename => p_KeyService, p_corrmsgid => p_corrmsgid)
    then
      add_log(p_MarketID => null, p_msg => 'Повторный запуск ' || p_KeyService || ' для ' || p_servicenameList);
      return false;
    end if;
    add_log(p_MarketID => null, p_msg => 'ПРОДОЛЖЕНИЕ ' || p_servicenameList);
    return true;
  end;

  function qmanager_wait_service_byMarket(p_MarketID        integer
                                         ,p_servicenameList varchar2
                                         ,p_cnt_service     integer
                                         ,p_corrmsgid       itt_q_message_log.msgid%type
                                         ,p_lockmsgid       itt_q_message_log.msgid%type
                                         ,p_chkService      itt_q_message_log.servicename%type
                                         ,p_KeyService      itt_q_message_log.servicename%type default null) return boolean as
    v_count_pre_exec integer;
  begin
    execute immediate ' select count(distinct servicename) from (
     select l.servicename, to_number(EXTRACTVALUE(xmltype(l.messmeta), ''*/Create/MarketID'')) MarketID
      from itt_q_message_log l
     where l.queuetype = ''IN''
       and l.corrmsgid = :p_corrmsgid
       and l.servicename in (' || p_servicenameList || ')
       and l.status in (select * from table(it_q_message.select_status(p_kind_status => ''DONE'')))
       ) where MarketID = :p_MarketID'
      into v_count_pre_exec
      using p_corrmsgid, p_MarketID;
    if v_count_pre_exec < p_cnt_service
    then
      add_log(p_MarketID => p_MarketID, p_msg => 'Закончен ' || v_count_pre_exec || ' из ' || p_cnt_service || ' ' || p_servicenameList);
      return false;
    end if;
    execute immediate 'select count(*)  from (select count(*) over(partition by l.servicename) cnt ,l.parallel ,num
          from (select to_number(EXTRACTVALUE(xmltype(l.messmeta), ''*/Create/MarketID'')) MarketID
                      ,to_number(EXTRACTVALUE(xmltype(l.messmeta), ''*/Start/parallel'')) parallel
                      ,to_number(EXTRACTVALUE(xmltype(l.messmeta), ''*/ServiceParallel/Num'')) Num
                      ,l.servicename
                  from itt_q_message_log l
                 where l.queuetype = ''IN''
                   and l.corrmsgid = :p_corrmsgid
                   and l.servicename in (' || p_servicenameList || ')
                   and l.status in (select * from table(it_q_message.select_status(p_kind_status => ''DONE'')))) l
           where MarketID = :p_MarketID and num is not null ) where cnt != parallel'
      into v_count_pre_exec
      using p_corrmsgid, p_MarketID;
    if v_count_pre_exec != 0
    then
      add_log(p_MarketID => p_MarketID, p_msg => 'Не все паралельные процессы ' || v_count_pre_exec || ' ' || p_servicenameList);
      return false;
    end if;
    return qmanager_chk_startservice(p_MarketID => p_MarketID
                                    ,p_servicename => p_servicenameList
                                    ,p_corrmsgid => p_corrmsgid
                                    ,p_lockmsgid => p_lockmsgid
                                    ,p_chkService => p_chkService
                                    ,p_KeyService => p_KeyService);
  end;

  function qmanager_wait_service_byKind(p_MarketID    integer
                                       ,p_servicename varchar2
                                       ,p_KindCnt     integer
                                       ,p_corrmsgid   itt_q_message_log.msgid%type
                                       ,p_lockmsgid   itt_q_message_log.msgid%type
                                       ,p_chkService  itt_q_message_log.servicename%type
                                       ,p_KeyService  itt_q_message_log.servicename%type) return boolean as
    v_count_pre_exec integer;
    vr_mess_log      itt_q_message_log%rowtype;
  begin
    execute immediate ' select count(*) from (
     select to_number(EXTRACTVALUE(xmltype(l.messmeta), ''*/Create/MarketID'')) MarketID
      from itt_q_message_log l
     where l.queuetype = ''IN''
       and l.corrmsgid = :p_corrmsgid
       and l.servicename = :p_servicename
       and l.status in (select * from table(it_q_message.select_status(p_kind_status => ''DONE'')))
       ) where MarketID = :p_MarketID'
      into v_count_pre_exec
      using p_corrmsgid, p_servicename, p_MarketID;
    if v_count_pre_exec < p_KindCnt
    then
      add_log(p_MarketID => p_MarketID, p_msg => p_servicename || ' Закончен ' || v_count_pre_exec || ' из ' || p_KindCnt);
      return false;
    end if;
    vr_mess_log := it_q_message.messlog_get_withlock(p_msgid => p_lockmsgid);
    if vr_mess_log.log_id is null
    then
      add_log(p_MarketID => p_MarketID, p_msg => p_servicename || ' Другим сервисом заблокировано СТАРТ сообщение ');
      return false;
    end if;
    return qmanager_chk_startservice(p_MarketID => p_MarketID
                                    ,p_servicename => p_servicename
                                    ,p_corrmsgid => p_corrmsgid
                                    ,p_lockmsgid => p_lockmsgid
                                    ,p_chkService => p_chkService
                                    ,p_KeyService => p_KeyService);
  end;

  function qmanager_wait_service(p_servicename1 varchar2
                                ,p_servicename2 varchar2 default null
                                ,p_servicename3 varchar2 default null
                                ,p_msgid        itt_q_message_log.msgid%type
                                ,p_MarketID     integer
                                ,p_corrmsgid    itt_q_message_log.msgid%type
                                ,p_lockmsgid    itt_q_message_log.msgid%type
                                ,p_chkService   itt_q_message_log.servicename%type) return boolean as
    v_msgid       itt_q_message_log.msgid%type;
    v_servicename varchar2(32000) := '''' || p_servicename1 || '''';
    v_cnt_service pls_integer := 1;
  begin
    if p_servicename2 is not null
    then
      v_servicename := v_servicename || ',''' || p_servicename2 || '''';
      v_cnt_service := v_cnt_service + 1;
    end if;
    if p_servicename3 is not null
    then
      v_servicename := v_servicename || ',''' || p_servicename3 || '''';
      v_cnt_service := v_cnt_service + 1;
    end if;
    if not qmanager_wait_service_byMarket(p_MarketID => p_MarketID
                                         ,p_servicenameList => v_servicename
                                         ,p_cnt_service => v_cnt_service
                                         ,p_corrmsgid => p_corrmsgid
                                         ,p_lockmsgid => null
                                         ,p_chkService => null)
    then
      add_log(p_MarketID => p_MarketID, p_msg => 'MarketID#' || p_MarketID || 'qmanager_wait_service  ждем ' || v_servicename);
      it_q_message.repeat_message(p_msgid => p_msgid, p_delay => 2, o_msgid => v_msgid); -- Ждем 2 сек
      return false;
    end if;
    return qmanager_chk_startservice(p_MarketID => p_MarketID
                                    ,p_servicename => p_servicename1
                                    ,p_corrmsgid => p_corrmsgid
                                    ,p_lockmsgid => p_lockmsgid
                                    ,p_chkService => p_chkService);
  end;

  -- Возвращает кол-во сервисов расчета с ошибкой. 
  function get_sevice_calc_error(p_msgid     itt_q_message_log.msgid%type default null
                                ,o_messerror out itt_q_message_log.commenttxt%type -- Текст первой ошибки
                                 ) return pls_integer as
    v_cnt         pls_integer;
    v_MarketCode  varchar2(128);
    v_servicename varchar2(128);
    v_messerror   varchar2(4000);
  begin
    if p_msgid is null
    then
      raise_application_error(-20000, 'Ошибка параметров функции it_limit.get_sevice_calc_error !');
    end if;
    begin
      select cnt
            ,case
               when messmeta is not null then
                EXTRACTVALUE(xmltype(messmeta), '*/Create/MarketCode')
             end MarketCode
            ,servicename
            ,it_q_message.get_errtxt(commenttxt)
        into v_cnt
            ,v_MarketCode
            ,v_servicename
            ,v_messerror
        from (select count(*) over() as cnt
                    ,statusdt
                    ,messmeta
                    ,servicename
                    ,msgcode
                    ,msgtext
                    ,commenttxt
                from table(it_q_message.select_answer_msg(p_msgid => p_msgid))
               where status in (select * from table(it_q_message.select_status(p_kind_status => 'ERROR')))
               order by statusdt)
       where rownum < 2;
      o_messerror := v_MarketCode || ' [' || v_servicename || ']:' || v_messerror;
    exception
      when no_data_found then
        v_cnt := 0;
    end;
    return v_cnt;
  end;

  -- Контроль сервисов расчета на ошибку 
  procedure chk_sevice_calc_error(p_messmeta xmltype) as
    v_messerror itt_q_message_log.commenttxt%type;
    pragma autonomous_transaction;
    vr_param_limit_start tr_limit_start;
    v_msgid              itt_q_message_log.msgid%type;
  begin
    vr_param_limit_start := get_param_limit_start(p_messmeta);
    v_msgid              := vr_param_limit_start.Msgid;
    if get_sevice_calc_error(p_msgid => v_msgid, o_messerror => v_messerror) > 0
    then
      set_calc_status(p_calc_direct => v_msgid, p_ErrorCode => 1, p_ErrorDesc => v_messerror);
      commit;
      raise_application_error(-20000, 'Ошибка при выполнении сервиса ' || v_messerror);
    end if;
    rollback;
  end;

  function Get_nameLimitKind(p_Kind pls_integer) return varchar2 as
  begin
    if p_Kind = 1
    then
      return 'T0';
    elsif p_Kind = 2
    then
      return 'T1';
    elsif p_Kind = 3
    then
      return 'T2';
    else
      return 'T365';
    end if;
  end;

  procedure LockCreateLimits(p_CalcDate       in date
                            ,p_UseListClients in number default 0
                            ,o_ErrorCode      out number -- != 0 о
                            ,o_ErrorDesc      out varchar2) as
    row_locked exception;
    pragma exception_init(row_locked, -54);
    res_locked exception;
    pragma exception_init(res_locked, -30006);
  begin
    lock table DDL_LIMITOP_DBT in exclusive mode WAIT 10;
    o_ErrorCode := 0;
  exception
    when row_locked
         or res_locked then
      o_ErrorCode := 100;
      o_ErrorDesc := 'Запущен другой экземпляр расчета лимитов. Повторный запуск невозможен.';
  end;

  -- Сохранение статуса и протокола расчета 
  procedure set_calc_status(p_calc_direct in varchar2
                           ,p_ErrorCode   in number -- != 0 ошиб
                           ,p_ErrorDesc   in varchar2 default null
                           ,p_log         clob default null) as
    v_status DDL_LIMITOP_DBT.T_STATUS%type := case
                                                when p_ErrorCode = 0 then
                                                 it_q_message.C_KIND_STATUS_DONE
                                                else
                                                 it_q_message.C_KIND_STATUS_ERROR
                                              end;
    v_status_ERROR DDL_LIMITOP_DBT.T_STATUS%type := it_q_message.C_KIND_STATUS_ERROR;
    v_calcdate     date;
  begin
    update DDL_LIMITOP_DBT l
       set l.t_status    = v_status
          ,l.t_statustxt = case
                             when p_ErrorDesc is null then
                              l.t_statustxt
                             else
                              substr(p_ErrorDesc, 1, 2000)
                           end
          ,l.t_statusdt  = sysdate
     where l.t_calc_direct = p_calc_direct
       and l.t_status != v_status
       and l.t_status != v_status_ERROR
    returning l.t_calcdate into v_calcdate;
    if sql%rowcount != 0
       and p_ErrorCode != 0
    then
      RSHB_RSI_SCLIMIT.g_calc_DIRECT := p_calc_direct;
      RSHB_RSI_SCLIMIT.TimeStamp_(Label_ => p_ErrorDesc, date_ => v_calcdate, start_ => null, end_ => SYSTIMESTAMP, excepsqlcode_ => p_ErrorCode, all_log_ => true);
    end if;
    if dbms_lob.getlength(p_log) != 0
    then
      update DDL_LIMITOP_DBT l set l.t_calclog = p_log where l.t_calc_direct = p_calc_direct;
    end if;
  end;

  function select_calc_log(p_calc_direct in varchar2
                          ,p_log_king    pls_integer default 0 --(0- протокол расчета,1-(ошибки,предупреждения,информационные) )
                           ) return tt_calc_log
    pipelined as
    v_where_action varchar2(200);
    v_order        varchar2(200);
    v_sel          varchar2(2000);
    vc_cur         sys_refcursor;
    vr_cur         tr_calc_log;
  begin
    /*
    
    1-10000 - протокол старый
    XXXXMAAAAAA  - новый X- гркппа (1000 - протокол 2000 - расчет цены ) M - пп маркет (0 -все ) A - action  
    DL_KeyPair(10161,10199),//ошибки
    (10131,10160),//предупреждения
    DL_KeyPair(10100,10130)//информационные*/
    /*type tr_calc_log is record(
     t_label        dcalclimitlog_dbt.t_label%type
    ,tmb            varchar2(8)
    ,tme            varchar2(8)
    ,t_excepsqlcode dcalclimitlog_dbt.t_excepsqlcode%type
    ,t_star         date
    ,t_end          date
    ,t_action       dcalclimitlog_dbt.t_action%type)*/
    case p_log_king
      when 0 then
        if substr(p_calc_direct, 1, 1) = chr(88)
        then
          v_order        := 't_end';
          v_where_action := ' abs(t_action) between 1 and 10000 or abs(t_action) >= 10000000000 ';
        else
          v_order        := 'abs(t_action),t_end';
          v_where_action := ' abs(t_action) >= 10000000000 ';
        end if;
      when 1 then
        v_order        := 'abs(t_action) desc,t_end';
        v_where_action := ' abs(t_action) between 10001 and (10000000000-1)';
      else
        raise_application_error(-20000, 'Ошибка параметров функции it_limit.select_calc_log ! p_log_king=' || p_log_king);
    end case;
    v_sel := ' select t_label
                      ,isinfo
                      ,tme
                      ,t_excepsqlcode
                      ,t_start
                      ,t_end
                      ,t_action  
      from (select log.*,count(*) over(partition by t_action,decode(isinfo,1,null,t_end),t_label order by t_end) npp 
            from ( select t_label
                      ,case when t_start is null or nvl(t_excepsqlcode, 0) !=0 then 1 else 0 end  as isinfo
                      ,to_char(t_end, ''hh24:mi:ss'') tme
                      ,nvl(t_excepsqlcode, 0) t_excepsqlcode
                      ,t.t_start
                      ,t.t_end
                      ,t.t_action
                  from dcalclimitlog_dbt t
                 where t.t_calc_direct = :p_calc_direct
                   and ((' || v_where_action || ') or nvl(t_excepsqlcode, 0) != 0)
                 ) log
            )
       where npp = 1
       order by ' || v_order;
    open vc_cur for v_sel
      using p_calc_direct;
    loop
      fetch vc_cur
        into vr_cur;
      exit when vc_cur%notfound;
      pipe row(vr_cur);
    end loop;
    close vc_cur;
  end;

  -- Получение СИД и параметров расчета 
  function get_calc_direct(p_CalcDate       in date
                          ,p_ByStockMB      in number default 0
                          ,p_ByStockSPB     in number default 0
                          ,p_ByCurMB        in number default 0
                          ,p_ByFortsMB      in number default 0
                          ,p_ByEDP          in number default 0
                          ,o_ErrorCode      out number -- != 0 ошиб
                          ,o_ErrorDesc      out varchar2
                          ,p_MarketID       in number default -1 --
                          ,p_UseListClients in number default 0
                          ,p_useQMng        in number default 0) return varchar2 as
    v_param_qdaychk constant integer := greatest(1, nvl(it_rs_interface.get_parm_number_path(GC_PARAM_QDAYCHK), 1));
    vx_CalcParam xmltype;
    v_QMsgid constant varchar2(128) := case
                                         when p_useQMng = 0 then
                                          rshb_rsi_sclimit.GC_CALC_SID_DEFAULT
                                       end || it_q_message.get_sys_guid;
    p_calc_panelcontr constant varchar(128) := case
                                                 when p_useQMng = 0 then
                                                  rshb_rsi_sclimit.GC_CALC_SID_DEFAULT
                                                 else
                                                  GetCalcSID
                                               end;
    p_calc_clientinfo constant varchar2(128) := case
                                                  when p_useQMng = 0 then
                                                   rshb_rsi_sclimit.GC_CALC_SID_DEFAULT
                                                  else
                                                   GetCalcSID
                                                end;
    v_cnt       integer;
    v_status    DDL_LIMITOP_DBT.T_STATUS%type;
    v_statustxt DDL_LIMITOP_DBT.T_STATUSTXT%type;
  begin
    o_ErrorCode := 0;
    o_ErrorDesc := '';
    select xmlelement("XML"
                      ,xmlattributes(nvl(p_ByStockMB, 0) as "ByStockMB"
                                    ,nvl(p_ByStockSPB, 0) as "ByStockSPB"
                                    ,nvl(p_ByCurMB, 0) as "ByCurMB"
                                    ,nvl(p_ByFortsMB, 0) as "ByFortsMB"
                                    ,nvl(p_ByEDP, 0) as "ByEDP"
                                    ,p_MarketID as "MarketID"
                                    ,p_UseListClients as "UseListClients"
                                    ,p_calc_panelcontr as "calc_panelcontr"
                                    ,p_calc_clientinfo as "calc_clientinfo"))
      into vx_CalcParam
      from dual;
    if nvl(p_ByStockMB, 0) = 0
       and nvl(p_ByStockSPB, 0) = 0
       and nvl(p_ByCurMB, 0) = 0
       and nvl(p_ByFortsMB, 0) = 0
       and nvl(p_ByEDP, 0) = 0
    then
      o_ErrorCode := 300;
      o_ErrorDesc := 'Укажите позицию лимитов для расчета';
    end if;
    if o_ErrorCode = 0
    then
      LockCreateLimits(p_CalcDate => p_CalcDate, p_UseListClients => p_UseListClients, o_ErrorCode => o_ErrorCode, o_ErrorDesc => o_ErrorDesc);
    end if;
    if o_ErrorCode = 0
    then
      for cur in (select l.t_calcdate
                        ,l.t_calc_direct
                        ,l.t_startdt
                        ,l.t_status
                        ,l.t_statusdt
                        ,l.t_user
                    from DDL_LIMITOP_DBT l
                   where l.t_startdt > sysdate - v_param_qdaychk
                     and l.t_status not in (it_q_message.C_KIND_STATUS_DONE, it_q_message.C_KIND_STATUS_ERROR)
                   order by l.t_startdt)
      loop
        if substr(cur.t_calc_direct, 1, 1) = chr(88)
        then
          /*begin
            select 1 into v_cnt from DGLOBALLOCK_DBT where T_LOCKID like 'LIMITCALCPROCESS' for update nowait;
          exception
            when others then
              o_ErrorCode := 200;
              o_ErrorDesc := 'Запущен другой экземпляр расчета лимитов. Повторный запуск невозможен.' || chr(10) ||
                             '(если какой-либо из расчетов завершен аварийно, рекоммендуется перезапустить окна RS-Bank)';
          end;*/
          update DDL_LIMITOP_DBT l
             set l.t_status    = it_q_message.C_KIND_STATUS_ERROR
                ,l.t_statusdt  = sysdate
                ,l.t_statustxt = 'Отложенная установка статуса'
           where l.t_calc_direct = cur.t_calc_direct;
        else
          v_status := it_q_message.C_KIND_STATUS_WORK;
          v_cnt    := get_sevice_calc_error(p_msgid => cur.t_calc_direct, o_messerror => v_statustxt);
          if v_cnt > 0
          then
            v_status := it_q_message.C_KIND_STATUS_ERROR;
          else
            select count(*)
              into v_cnt
              from table(it_q_message.select_answer_msg(p_msgid => cur.t_calc_direct, p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT))
             where ServiceName = g_qserv_LimitCleaner
               and status in (select * from table(it_q_message.select_status(it_q_message.C_KIND_STATUS_DONE)));
            if v_cnt > 0
            then
              v_status := it_q_message.C_KIND_STATUS_DONE;
            else
              select count(*)
                into v_cnt
                from table(it_q_message.select_answer_msg(p_msgid => cur.t_calc_direct, p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT))
               where status in (select * from table(it_q_message.select_status(it_q_message.C_KIND_STATUS_WORK)));
              if v_cnt = 0
              then
                v_status := it_q_message.C_KIND_STATUS_ERROR;
              end if;
            end if;
          end if;
          update DDL_LIMITOP_DBT l
             set l.t_status    = v_status
                ,l.t_statusdt  = sysdate
                ,l.t_statustxt = substr('Отложенная установка статуса:' || v_statustxt, 1, 2000)
           where l.t_calc_direct = cur.t_calc_direct
             and l.t_status != v_status;
          if v_status = it_q_message.C_KIND_STATUS_WORK
          then
            o_ErrorCode := 200;
            o_ErrorDesc := 'Не завершен расчет за ' || to_char(cur.t_calcdate, 'dd.mm.yyyy') || ' запущенный ' || cur.t_user || ' ' ||
                           to_char(cur.t_startdt, 'dd.mm.yyyy hh24:mi:ss');
            exit;
          end if;
        end if;
      end loop;
    end if;
    if o_ErrorCode = 0
    then
      begin
        DBMS_SCHEDULER.drop_job('RUN_LIMIT', true);
      exception
        when others then
          null;
      end;
      insert into DDL_LIMITOP_DBT
        (T_CALCDATE
        ,T_CALCPARAM
        ,T_CALC_DIRECT
        ,T_USER
        ,T_STATUS)
      values
        (p_CalcDate
        ,vx_CalcParam.getClobVal()
        ,v_QMsgid
        ,it_q_message.get_q_user
        ,it_q_message.C_KIND_STATUS_WORK);
      return v_QMsgid;
    else
      return null;
    end if;
  exception
    when others then
      rollback;
      o_ErrorCode := abs(sqlcode);
      o_ErrorDesc := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
      add_log(p_MarketID => p_MarketID, p_msg => 'get_calc_direct ERROR#' || o_ErrorCode || ':' || o_ErrorDesc);
      return null;
  end;

  -- Проверка окончания расчета (0- не окончен 1 - Ok )
  function chk_finish_calc_direct(p_calc_direct in varchar2) return number as
    v_status ddl_limitop_dbt.t_status%type;
  begin
    select o.t_status into v_status from ddl_limitop_dbt o where o.t_calc_direct = p_calc_direct;
    return case when v_status = it_q_message.C_KIND_STATUS_DONE then 1 else 0 end;
  exception
    when no_data_found then
      return 0;
  end;

  -- Старт расчета без QManager по площадке 
  procedure CreateLimits_market(p_calc_direct    in varchar2
                               ,p_CalcDate       in date
                               ,p_ByStockMB      in number default 0
                               ,p_ByStockSPB     in number default 0
                               ,p_ByCurMB        in number default 0
                               ,p_ByFortsMB      in number default 0
                               ,p_ByEDP          in number default 0
                               ,p_MarketID       in number
                               ,o_ErrorCode      out number -- != 0 ошибка создания сообщения  o_ErrorDesc
                               ,o_ErrorDesc      out varchar2
                               ,p_UseListClients in number default 0) as
    v_action varchar2(2000);
  begin
    o_ErrorCode := 0;
    v_action    := 'begin
         rshb_rsi_sclimit.g_calc_DIRECT     := ''' || p_calc_direct || ''';
         ';
    if p_MarketID = RSHB_RSI_SCLIMIT.GetMicexID
       and (p_ByStockMB = 1 or p_ByCurMB = 1 or p_ByFortsMB = 1 or p_ByEDP = 1)
    then
      v_action := v_action || 'rshb_rsi_sclimit.RSI_CreateLimits (' || p_MarketID || ',''' || RSB_Common.GetRegStrValue('SECUR\MICEX_CODE') || '''
                  ,to_date(''' || TO_CHAR(p_CalcDate, 'dd.mm.yyyy') || ''',''dd.mm.yyyy''), ' || p_ByStockMB || ', ' || p_ByCurMB || ', ' || p_ByFortsMB || ', ' ||
                  p_ByEDP || ',' || p_UseListClients || ' ); end;';
    elsif p_MarketID = RSHB_RSI_SCLIMIT.GetSpbexID
          and (p_ByStockSPB = 1 or p_ByEDP = 1)
    then
      v_action := v_action || 'rshb_rsi_sclimit.RSI_CreateLimits (' || p_MarketID || ',''' || RSB_Common.GetRegStrValue('SECUR\SPBEX_CODE') || '''
                  ,to_date(''' || TO_CHAR(p_CalcDate, 'dd.mm.yyyy') || ''',''dd.mm.yyyy''), ' || p_ByStockSPB || ', 0, 0, ' || p_ByEDP || ',' || p_UseListClients ||
                  ' ); end;';
    else
      o_ErrorCode := 500;
      o_ErrorDesc := 'Ошибка параметров запуска расчета лимитов';
    end if;
    if o_ErrorCode = 0
    then
      for j in (select j.job_name
                      ,j.ENABLED
                      ,j.state -- 'RUNNING'
                  from sys.user_scheduler_jobs j
                 where j.job_name = 'RUN_LIMIT')
      loop
        begin
          if j.enabled = 'TRUE'
          then
            sys.dbms_scheduler.disable(j.job_name, force => true);
          end if;
          if j.state = 'RUNNING'
          then
            sys.dbms_scheduler.stop_job(j.job_name);
          end if;
          sys.dbms_scheduler.DROP_JOB(j.job_name, true);
        exception
          when others then
            null;
        end;
      end loop;
      -- it_log.log(p_msg => 'CreateLimits_market', p_msg_clob => v_action);
      DBMS_SCHEDULER.create_job(job_name => 'RUN_LIMIT', job_type => 'PLSQL_BLOCK', job_action => v_action);
      DBMS_SCHEDULER.run_job('RUN_LIMIT', false);
    end if;
  end;

  -- Старт расчета через QManager
  procedure CreateLimits(p_CalcDate       in date
                        ,p_ByStockMB      in number default 0
                        ,p_ByStockSPB     in number default 0
                        ,p_ByCurMB        in number default 0
                        ,p_ByFortsMB      in number default 0
                        ,p_ByEDP          in number default 0
                        ,o_calc_direct    out varchar2
                        ,o_ErrorCode      out number -- != 0 ошибка создания сообщения  o_ErrorDesc
                        ,o_ErrorDesc      out varchar2
                        ,p_MarketID       in number default -1 -- если -1 все площадки
                        ,p_UseListClients in number default 0) as
    pragma autonomous_transaction;
    vx_messmeta           xmltype;
    v_parallel            integer;
    v_worker_load_percent integer := it_q_manager.get_worker_load_percent;
    v_param_QFORCE constant char(1) := nvl(it_rs_interface.get_parm_varchar_path(GC_PARAM_QFORCE), chr(0));
    p_calc_panelcontr varchar2(128);
    p_calc_clientinfo varchar2(128);
  begin
    o_ErrorCode := 0;
    add_log(p_MarketID => null
           ,p_msg => 'CreateLimits START  p_ByStockMB=' || p_ByStockMB || ' p_ByStockSPB=' || p_ByStockSPB || ' p_ByCurMB=' || p_ByCurMB || ' p_ByFortsMB=' || p_ByFortsMB ||
                     ' p_ByEDP=' || p_ByEDP);
    o_calc_direct := get_calc_direct(p_CalcDate => p_CalcDate
                                    ,p_ByStockMB => p_ByStockMB
                                    ,p_ByStockSPB => p_ByStockSPB
                                    ,p_ByCurMB => p_ByCurMB
                                    ,p_ByFortsMB => p_ByFortsMB
                                    ,p_ByEDP => p_ByEDP
                                    ,o_ErrorCode => o_ErrorCode
                                    ,o_ErrorDesc => o_ErrorDesc
                                    ,p_MarketID => p_MarketID
                                    ,p_UseListClients => p_UseListClients
                                    ,p_useQMng => 1);
    if o_ErrorCode = 0
    then
      get_calc_sid(o_calc_direct, p_calc_clientinfo, p_calc_panelcontr);
      v_parallel := case
                      when p_UseListClients != 0 then
                       1
                      when v_worker_load_percent > 60
                           and v_param_QFORCE != chr(88) then
                       4
                      when v_worker_load_percent > 60
                           and v_param_QFORCE = chr(88) then
                       8
                      when v_worker_load_percent > 30
                           and v_param_QFORCE != chr(88) then
                       8
                      when v_worker_load_percent > 30
                           and v_param_QFORCE = chr(88) then
                       12
                      when v_param_QFORCE = chr(88) then
                       16
                      else
                       8
                    end;
      select xmlelement("Limits"
                        ,xmlelement("Start"
                                   ,xmlelement("Msgid", o_calc_direct)
                                   ,xmlelement("CalcDate", it_xml.date_to_char_iso8601(p_CalcDate))
                                   ,xmlelement("ByStockMB", p_ByStockMB)
                                   ,xmlelement("ByStockSPB", p_ByStockSPB)
                                   ,xmlelement("ByCurMB", p_ByCurMB)
                                   ,xmlelement("ByFortsMB", p_ByFortsMB)
                                   ,xmlelement("ByEDP", p_ByEDP)
                                   ,xmlelement("MarketID", p_MarketID)
                                   ,xmlelement("UseListClients", p_UseListClients)
                                   ,case
                                     when p_UseListClients = 1 then
                                      (select xmlelement("ListClients"
                                                         ,XMLAGG(xmlelement("PANELCONTR"
                                                                           ,XMLFOREST(t_setflag as "t_setflag"
                                                                                     ,t_clientid as "t_clientid"
                                                                                     --   ,t_clientcode as "t_clientcode"
                                                                                     ,t_clientname as "t_clientname"
                                                                                     ,t_dlcontrid as "t_dlcontrid"
                                                                                     --   ,t_contrnumber as "t_contrnumber"
                                                                                     --   ,t_contrname as "t_contrname"
                                                                                     --   ,t.t_contrdate as "t_contrdate"
                                                                                     --   ,t_ekk as "t_ekk"
                                                                                     --   ,t_isedp as "t_isedp"
                                                                                     ))))
                                         from DDL_PANELCONTR_DBT t
                                        where t_setflag = chr(88)
                                          and t.t_calc_sid = 'X')
                                   end case
                                   ,xmlelement("MarketList")
                                   ,case when p_UseListClients <> 1 then
                                   (select xmlelement("ParallelCALC", XMLAGG(xmlelement("ParallelSID", XMLFOREST(level as "Num", GetCalcSID as "calc_panelcontr"))))
                                      from dual
                                    connect by level <= v_parallel) end case
                                   ,xmlelement("parallel", v_parallel)
                                   ,xmlelement("mainsessionid", USERENV('sessionid'))
                                   ,xmlelement("calc_panelcontr", p_calc_panelcontr)
                                   ,xmlelement("calc_clientinfo", p_calc_clientinfo)))
        into vx_messmeta
        from dual;
      it_q_message.load_msg(io_msgid => o_calc_direct
                           ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                           ,p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
                            --,p_Sender =>
                            --,p_Priority =>
                            --,p_Correlation =>
                            -- ,p_CORRmsgid => 
                            --,p_SenderUser =>
                           ,p_ServiceName => 'Limit.Start'
                            --,p_ServiceGroup =>
                            --,p_Receiver =>
                            --,p_BTUID =>
                            --,p_MSGCode =>
                            --,p_MSGText =>
                            --,p_MESSBODY =>
                           ,p_MessMETA => vx_messmeta
                            --,p_queue_num => p_queue_num
                            --,p_RequestDT =>
                            --,p_ESBDT =>
                            --,p_delay =>
                            --,p_comment =>
                            );
    end if;
    if o_ErrorCode = 0
    then
      add_log(p_MarketID => p_MarketID, p_msg => 'CreateLimits FINISH o_calc_direct=' || o_calc_direct);
      commit;
    else
      add_log(p_MarketID => p_MarketID, p_msg => 'CreateLimits ERROR#' || o_ErrorCode || ':' || o_ErrorDesc);
      rollback;
    end if;
  exception
    when others then
      rollback;
      o_ErrorCode := abs(sqlcode);
      o_ErrorDesc := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
      add_log(p_MarketID => p_MarketID, p_msg => 'CreateLimits ERROR#' || o_ErrorCode || ':' || o_ErrorDesc);
  end;

  -- Сервис старта расчета лимитов
  procedure Limit_Start(p_worklogid integer
                       ,p_messbody  clob
                       ,p_messmeta  xmltype
                       ,o_msgid     out varchar2
                       ,o_MSGCode   out integer
                       ,o_MSGText   out varchar2
                       ,o_messbody  out clob
                       ,o_messmeta  out xmltype) is
    v_QMsgid               varchar2(128);
    vr_message             itt_q_message_log%rowtype;
    vx_pmessmeta           xmltype := p_messmeta;
    vx_messmeta            xmltype;
    vr_param_limit_start   tr_limit_start;
    v_queue_num            itt_q_message_log.queue_num%type;
    v_MarketCount          pls_integer := 0;
    p_CalcDate             date;
    p_UseListClients       number;
    p_parallel             number;
    v_CheckSecurLimits     boolean := false;
    v_CheckCashStockLimits boolean := false;
  begin
    vr_message  := it_q_message.messlog_get(p_logid => p_worklogid);
    v_queue_num := it_q_message.get_queue_num(p_objname => vr_message.queuename);
    select insertchildxml(vx_pmessmeta, '*/Start', 'queue_num', xmlelement("queue_num", v_queue_num)) into vx_pmessmeta from dual;
    vr_param_limit_start := get_param_limit_start(vx_pmessmeta);
    p_CalcDate           := vr_param_limit_start.CalcDate;
    p_parallel           := vr_param_limit_start.parallel;
    p_UseListClients     := vr_param_limit_start.UseListClients;
    --RSHB_RSI_SCLIMIT.limit_add_partition(p_CalcDate,p_UseListClients);
    CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                ,p_CalcDate => vr_param_limit_start.CalcDate
                ,p_action => 0
                ,p_label => 'Старт расчета'
                ,p_dtstart => systimestamp);
    for n in 1 .. gt_table_partition.count
    loop
      table_add_partition(p_table => gt_table_partition(n), v_sid_calc => vr_param_limit_start.calc_clientinfo);
    end loop;
    if p_UseListClients = 1
    then
      table_add_partition(p_table => 'DDL_PANELCONTR_DBT', v_sid_calc => vr_param_limit_start.calc_panelcontr);
      insert into ddl_panelcontr_dbt
        (t_setflag
        ,t_clientid
        ,t_clientcode
         --   ,t_clientname
        ,t_dlcontrid
         --   ,t_contrnumber
         --   ,t_contrname
         --   ,t_contrdate
         --   ,t_ekk
         --   ,t_isedp
        ,t_calc_sid)
        with x as
         (select vx_pmessmeta xml_data from dual)
        select res.*
              ,vr_param_limit_start.calc_panelcontr
          from x
              ,XMLTABLE('/Limits/Start/ListClients/PANELCONTR' PASSING x.xml_data COLUMNS t_setflag char(1) PATH 't_setflag'
                       ,t_clientid number(10) PATH 't_clientid'
                       ,t_clientcode varchar2(30) PATH 't_clientcode'
                        --      ,t_clientname varchar2(320) PATH 't_clientname'
                       ,t_dlcontrid number(10) PATH 't_dlcontrid'
                        --      ,t_contrnumber varchar2(20) PATH 't_contrnumber'
                        --      ,t_contrname varchar2(200) PATH 't_contrname'
                        --      ,t_contrdate date PATH 't_contrdate'
                        --      ,t_ekk varchar2(20) PATH 't_ekk'
                        --        ,t_isedp char(1) PATH 't_isedp'
                        ) res;
    else
      for cur in (select * from table(select_parallel_sid(vx_pmessmeta)))
      loop
        table_add_partition(p_table => 'DDL_PANELCONTR_DBT', v_sid_calc => cur.calc_panelcontr);
      end loop;
      insert into ddl_panelcontr_dbt
        (t_setflag
        ,t_clientid
         --  ,t_clientcode
        ,t_clientname
        ,t_dlcontrid
         -- ,t_contrnumber
         -- ,t_contrname
         -- ,t_contrdate
         -- ,t_ekk
         -- ,t_isedp
        ,t_calc_sid)
        select 'X'
              ,pc.T_CLIENTID
              ,pc.T_CLIENTNAME
              ,pc.T_DLCONTRID
              ,p.calc_panelcontr
          from table(it_limit.select_parallel_sid(vx_pmessmeta)) p
          join (select case
                         when count(*) over(partition by T_CLIENTID, num_max) > count(*) over(partition by T_CLIENTID, num_min) then
                          num_max
                         else
                          num_min
                       end num
                      ,T_CLIENTID
                      ,T_CLIENTNAME
                      ,T_DLCONTRID
                  from (select min(num_cl) over(partition by T_CLIENTID) num_min
                              ,max(num_cl) over(partition by T_CLIENTID) num_max
                              ,T_CLIENTID
                              ,T_CLIENTNAME
                              ,T_DLCONTRID
                          from (select ntile(p_parallel) over(order by npp, T_CLIENTID, T_DLCONTRID) num_cl
                                      ,T_CLIENTID
                                      ,T_CLIENTNAME
                                      ,T_DLCONTRID
                                  from (select T_CLIENTID
                                              ,T_CLIENTNAME
                                              ,T_DLCONTRID
                                              ,case
                                                 when mod(ra, 2) = 0 then
                                                  ra
                                                 else
                                                  rd
                                               end npp
                                          from (select T_CLIENTID
                                                      ,T_CLIENTNAME
                                                      ,T_DLCONTRID
                                                      ,rank() over(order by min_datebegin, T_CLIENTID) ra
                                                      ,rank() over(order by min_datebegin desc, T_CLIENTID) rd
                                                  from (select party.t_partyid T_CLIENTID
                                                              ,party.t_name T_CLIENTNAME
                                                              ,dlcontr.t_dlcontrid T_DLCONTRID
                                                              ,min(sfcontr.t_datebegin) over(partition by party.t_partyid) min_datebegin
                                                          from ddlcontr_dbt dlcontr
                                                          join dsfcontr_dbt sfcontr
                                                            on sfcontr.t_id = dlcontr.t_sfcontrid
                                                           and sfcontr.t_datebegin <= p_CalcDate
                                                           and (sfcontr.t_dateclose = to_date('01.01.0001', 'dd.mm.yyyy') or sfcontr.t_dateclose >= p_CalcDate)
                                                          join dparty_dbt party
                                                            on party.t_partyid = sfcontr.t_partyid)))))) pc
            on (pc.num = p.num)
         order by pc.num
                 ,pc.t_clientid;
    end if;
    /* if (byStockMB.checked or byCurMB.checked or byFortsMB.checked or ByEDP.checked)
       CreateLimits (this.getControl("CalcDate").value, 
                     MMVB_ID(), 
                     this.getControl("byStockMB").checked, 
                     this.getControl("byCurMB")  .checked, 
                     this.getControl("byFortsMB").checked, 
                     this.getControl("byEDP")    .checked, 
                     this.getControl("forAll")   .checked );
       WaitingCalcLimits("Расчет МБ", @str);
       LogText = LogText + str+"\n\n";
       Sessions[Sessions.size] = String(GetLastLimitCalcSessionId());
    end; */
    vx_messmeta := vx_pmessmeta;
    -- Формируем список площадок
    if (vr_param_limit_start.MarketID = -1 or vr_param_limit_start.MarketID = RSHB_RSI_SCLIMIT.GetMicexID)
       and (vr_param_limit_start.ByStockMB = 1 or vr_param_limit_start.ByCurMB = 1 or vr_param_limit_start.ByFortsMB = 1 or vr_param_limit_start.ByEDP = 1)
    then
      v_MarketCount := v_MarketCount + 1;
      vx_messmeta   := xml_insert_Market(p_messmeta => vx_messmeta
                                        ,p_npp => v_MarketCount
                                        ,p_MarketID => RSHB_RSI_SCLIMIT.GetMicexID
                                        ,p_MarketCode => RSB_Common.GetRegStrValue('SECUR\MICEX_CODE')
                                        ,p_ByStock => vr_param_limit_start.ByStockMB
                                        ,p_ByCurr => vr_param_limit_start.ByCurMB
                                        ,p_ByDeriv => vr_param_limit_start.ByFortsMB
                                        ,p_ByEDP => vr_param_limit_start.ByEDP
                                        ,p_MsgID => it_q_message.get_sys_guid);
    end if;
    /* if (this.getControl("byStockSPB").checked or this.getControl("byEDP").checked)
       CreateLimits (this.getControl("CalcDate").value, 
                     SPB_ID(), 
                     this.getControl("byStockSPB").checked, 
                     false, 
                     false, 
                     this.getControl("byEDP")    .checked, 
                     this.getControl("forAll")   .checked);
       WaitingCalcLimits("Расчет СПБ", @str);
       LogText = LogText + str+"\n\n";
       Sessions[Sessions.size] = String(GetLastLimitCalcSessionId());
    end;*/
    if (vr_param_limit_start.MarketID = -1 or vr_param_limit_start.MarketID = RSHB_RSI_SCLIMIT.GetSpbexID)
       and (vr_param_limit_start.ByStockSPB = 1 or vr_param_limit_start.ByEDP = 1)
    then
      v_MarketCount := v_MarketCount + 1;
      vx_messmeta   := xml_insert_Market(p_messmeta => vx_messmeta
                                        ,p_npp => v_MarketCount
                                        ,p_MarketID => RSHB_RSI_SCLIMIT.GetSpbexID
                                        ,p_MarketCode => RSB_Common.GetRegStrValue('SECUR\SPBEX_CODE')
                                        ,p_ByStock => vr_param_limit_start.ByStockSPB
                                        ,p_ByCurr => 0
                                        ,p_ByDeriv => 0
                                        ,p_ByEDP => vr_param_limit_start.ByEDP
                                        ,p_MsgID => it_q_message.get_sys_guid);
    end if;
    if v_MarketCount > 0
    then
      --execute immediate 'alter sequence ddl_limitcashstock_dbt_seq cache 10';
      --execute immediate 'alter sequence ddl_limitsecurites_dbt_seq cache 10';
      for cur in (select * from table(select_market(vx_messmeta)))
      loop
        if not v_CheckSecurLimits
           and (((cur.ByCurr = 1 or cur.ByEDP = 1) and cur.MarketID = RSHB_RSI_SCLIMIT.GetMicexID()) or cur.ByStock <> 0)
        then
          begin
            RSHB_RSI_SCLIMIT.RSI_CheckSecurLimits(p_CalcDate);
          exception
            when others then
              o_MSGCode := 901;
              o_MSGText := it_q_message.get_errtxt(sqlerrm);
          end;
          v_CheckSecurLimits := true;
        end if;
        if not v_CheckCashStockLimits
           and (cur.ByEDP = 1 or cur.ByStock = 1 or (cur.ByCurr <> 0 and cur.MarketID = RSHB_RSI_SCLIMIT.GetMicexID()))
        then
          begin
            RSHB_RSI_SCLIMIT.RSI_CheckCashStockLimits(p_CalcDate);
          exception
            when others then
              o_MSGCode := 901;
              o_MSGText := it_q_message.get_errtxt(sqlerrm);
          end;
          v_CheckCashStockLimits := true;
        end if;
      end loop;
      if nvl(o_MSGCode, 0) = 0
      then
        select insertchildxml(vx_messmeta, '*/Start', 'MarketCount', xmlelement("MarketCount", v_MarketCount)) into vx_messmeta from dual;
        qmanager_do_service_market(p_servicename1 => g_qserv_LimitBegin, p_messmeta => vx_messmeta);
      end if;
    else
      o_MSGCode := 900;
      o_MSGText := 'Не определены площадки для расчета лимитов';
    end if;
    add_log(p_MarketID => null, p_msg => 'Limit_Start');
  end;

  -- Сервис начала  расчета лимитов по площадке
  procedure Limit_Begin(p_worklogid integer
                       ,p_messbody  clob
                       ,p_messmeta  xmltype
                       ,o_msgid     out varchar2
                       ,o_MSGCode   out integer
                       ,o_MSGText   out varchar2
                       ,o_messbody  out clob
                       ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                ,p_CalcDate => p_CalcDate
                ,p_action => 100
                ,p_label => 'Сбор данных по договорам'
                ,p_NPPmarket => vr_param_limit_create.NPP
                ,p_MarketCode => vr_param_limit_create.MarketCode
                ,p_dtstart => systimestamp);
    if ((p_ByStock <> 0) or (p_ByCurr <> 0) or (p_ByEDP <> 0) or (p_ByDeriv <> 0))
    then
      if (p_ByStock <> 0)
      then
        RSHB_RSI_SCLIMIT.RSI_ClearContrTable(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients);
        /*
        RSHB_RSI_SCLIMIT.RSI_FillContrTablenotDeriv(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients);
        RSHB_RSI_SCLIMIT. RSI_FillContrTablebyDeriv(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients);
        RSHB_RSI_SCLIMIT.RSI_FillContrTableAcc(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients);
        RSHB_RSI_SCLIMIT.RSI_FillContrTableCOM(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients);
         */
      else
        RSHB_RSI_SCLIMIT.RSI_ClearContrTable(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
        /*RSHB_RSI_SCLIMIT.RSI_FillContrTablenotDeriv(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
        RSHB_RSI_SCLIMIT.RSI_FillContrTablebyDeriv(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
        RSHB_RSI_SCLIMIT.RSI_FillContrTableAcc(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
        RSHB_RSI_SCLIMIT.RSI_FillContrTableCOM(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
        */
      end if;
      qmanager_do_service_parallel(p_servicename => g_qserv_FillContrTablenotDeriv, p_messmeta => p_messmeta);
      qmanager_load_msg(p_servicename => g_qserv_FillContrTablebyDeriv, p_messmeta => p_messmeta);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => ' Limit_Begin ');
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис Сбор данных по договорам на площадке
  procedure Limit_FCTnotDeriv_parallel(p_worklogid integer
                                      ,p_messbody  clob
                                      ,p_messmeta  xmltype
                                      ,o_msgid     out varchar2
                                      ,o_MSGCode   out integer
                                      ,o_MSGText   out varchar2
                                      ,o_messbody  out clob
                                      ,o_messmeta  out xmltype) is
    vr_param_limit_start    tr_limit_start;
    vr_param_limit_create   tr_limit_create;
    vr_param_limit_parallel tr_parallel_sid;
    p_MarketID              number;
    p_MarketCode            varchar2(100);
    p_CalcDate              date;
    p_ByStock               number;
    p_ByCurr                number;
    p_ByDeriv               number;
    p_ByEDP                 number;
    p_UseListClients        number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_limit_parallel            := get_param_ServiceParallel(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_parallel.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := 1;
    if (p_ByStock <> 0)
    then
      RSHB_RSI_SCLIMIT.RSI_FillContrTablenotDeriv(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients);
    else
      RSHB_RSI_SCLIMIT.RSI_FillContrTablenotDeriv(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => ' Limit_FCTnotDeriv_parallel ' || vr_param_limit_parallel.num);
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис Сбор данных по договорам на площадке(Deriv)
  procedure Limit_FCTbyDeriv(p_worklogid integer
                            ,p_messbody  clob
                            ,p_messmeta  xmltype
                            ,o_msgid     out varchar2
                            ,o_MSGCode   out integer
                            ,o_MSGText   out varchar2
                            ,o_messbody  out clob
                            ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if (p_ByStock <> 0)
    then
      RSHB_RSI_SCLIMIT. RSI_FillContrTablebyDeriv(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients);
    else
      RSHB_RSI_SCLIMIT.RSI_FillContrTablebyDeriv(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_FCTbyDeriv ');
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  --Сервис расчета лимитов. Старт расчета данных по счетам 
  procedure Limit_FCTAccStart(p_worklogid integer
                             ,p_messbody  clob
                             ,p_messmeta  xmltype
                             ,o_msgid     out varchar2
                             ,o_MSGCode   out integer
                             ,o_MSGText   out varchar2
                             ,o_messbody  out clob
                             ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if not qmanager_wait_service_byMarket(p_MarketID => vr_param_limit_create.MarketID
                                         ,p_servicenameList => '''' || g_qserv_FillContrTablenotDeriv || ''',''' || g_qserv_FillContrTablebyDeriv || ''''
                                         ,p_cnt_service => 2
                                         ,p_corrmsgid => vr_param_limit_start.Msgid
                                         ,p_lockmsgid => vr_param_limit_create.Msgid
                                         ,p_chkService => g_qserv_FillContrTableCOM)
    then
      return;
    end if;
    --  RSHB_RSI_SCLIMIT.Gather_Table_Stats('DDL_CLIENTINFO_DBT');
    qmanager_load_msg(p_servicename => g_qserv_FillContrTableCOM, p_messmeta => p_messmeta);
    qmanager_do_service_parallel(p_servicename => g_qserv_FillContrTableAcc, p_messmeta => p_messmeta);
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_FCTAccStart ');
  end;

  -- Сервис Расчет данных по счетам 
  procedure Limit_FCTAcc_parallel(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype) is
    vr_param_limit_start    tr_limit_start;
    vr_param_limit_create   tr_limit_create;
    vr_param_limit_parallel tr_parallel_sid;
    p_MarketID              number;
    p_MarketCode            varchar2(100);
    p_CalcDate              date;
    p_ByStock               number;
    p_ByCurr                number;
    p_ByDeriv               number;
    p_ByEDP                 number;
    p_UseListClients        number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_limit_parallel            := get_param_ServiceParallel(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_parallel.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := 1;
    if (p_ByStock <> 0)
    then
      RSHB_RSI_SCLIMIT.RSI_FillContrTableAcc(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients);
    else
      RSHB_RSI_SCLIMIT.RSI_FillContrTableAcc(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_FCTAcc_parallel ' || vr_param_limit_parallel.num);
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Расчет сумм неоплаченных комиссий
  procedure Limit_FCTCOM(p_worklogid integer
                        ,p_messbody  clob
                        ,p_messmeta  xmltype
                        ,o_msgid     out varchar2
                        ,o_MSGCode   out integer
                        ,o_MSGText   out varchar2
                        ,o_messbody  out clob
                        ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if (p_ByStock <> 0)
    then
      RSHB_RSI_SCLIMIT.RSI_FillContrTableCOM(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByStock, p_ByDeriv, p_UseListClients);
    else
      RSHB_RSI_SCLIMIT.RSI_FillContrTableCOM(p_MarketID, p_MarketCode, p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_FCTCOM ');
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис Запуска Контроль данных по договорам
  procedure Limit_CheckContrTableStart(p_worklogid integer
                                      ,p_messbody  clob
                                      ,p_messmeta  xmltype
                                      ,o_msgid     out varchar2
                                      ,o_MSGCode   out integer
                                      ,o_MSGText   out varchar2
                                      ,o_messbody  out clob
                                      ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    v_Msgid               itt_q_message_log.msgid%type;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if not qmanager_wait_service_allMarket(p_cnt_Market => vr_param_limit_start.MarketCount
                                          ,p_servicenameList => '''' || g_qserv_FillContrTableAcc || ''',''' || g_qserv_FillContrTableCOM || ''''
                                          ,p_cnt_service => 2
                                          ,p_corrmsgid => vr_param_limit_start.Msgid
                                          ,p_KeyService => g_qserv_ClearEDPLimits)
    then
      return;
    end if;
    -- RSHB_RSI_SCLIMIT.Gather_Table_Stats('DDL_CLIENTINFO_DBT');
    if p_ByCurr <> 0
       or p_ByEDP = 1 -- Так как точка 1 
    then
      RSHB_RSI_SCLIMIT.ClearPlanSumCur(p_CalcDate, p_ByCurr, p_ByEDP, RSHB_RSI_SCLIMIT.GetMicexID(), p_UseListClients);
    end if;
    qmanager_do_service_market(p_servicename1 => g_qserv_ClearEDPLimits, p_messmeta => p_messmeta);
    qmanager_do_service_market(p_servicename1 => g_qserv_ClearSecurLimits, p_messmeta => p_messmeta);
    --qmanager_do_service_market(p_servicename1 => g_qserv_SetCMComm, p_MarketID => RSHB_RSI_SCLIMIT.GetMicexID(), p_messmeta => p_messmeta);
    qmanager_do_service_parallel(p_servicename => g_qserv_SetCMTick, p_MarketID => RSHB_RSI_SCLIMIT.GetMicexID(), p_messmeta => p_messmeta);
    qmanager_do_service_parallel(p_servicename => g_qserv_CheckContrTable, p_messmeta => p_messmeta);
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CheckContrTableStart');
  end;

  -- Сервис Контроль данных по договорам
  procedure Limit_CheckContrTable_parallel(p_worklogid integer
                                          ,p_messbody  clob
                                          ,p_messmeta  xmltype
                                          ,o_msgid     out varchar2
                                          ,o_MSGCode   out integer
                                          ,o_MSGText   out varchar2
                                          ,o_messbody  out clob
                                          ,o_messmeta  out xmltype) is
    vr_param_limit_start    tr_limit_start;
    vr_param_limit_create   tr_limit_create;
    vr_param_limit_parallel tr_parallel_sid;
    p_MarketID              number;
    p_MarketCode            varchar2(100);
    p_CalcDate              date;
    p_ByStock               number;
    p_ByCurr                number;
    p_ByDeriv               number;
    p_ByEDP                 number;
    p_UseListClients        number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_limit_parallel            := get_param_ServiceParallel(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_parallel.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := 1;
    --      TimeStamp_('Контроль данных по договорам ' || p_MarketCode, p_CalcDate, null, SYSTIMESTAMP);
    RSHB_RSI_SCLIMIT.RSI_CheckContrTable(p_CalcDate, p_ByStock, p_ByCurr, p_ByEDP, p_ByDeriv, p_UseListClients);
    --      TimeStamp_('RSI_CheckContrTable  Завершена  ' || p_MarketID, p_CalcDate, null, SYSTIMESTAMP);
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CheckContrTable_parallel ' || vr_param_limit_parallel.num);
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис Старт отбора лотов и сделок по площадке
  procedure Limit_SetStart(p_worklogid integer
                          ,p_messbody  clob
                          ,p_messmeta  xmltype
                          ,o_msgid     out varchar2
                          ,o_MSGCode   out integer
                          ,o_MSGText   out varchar2
                          ,o_messbody  out clob
                          ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if not qmanager_wait_service_allMarket(p_cnt_Market => 1
                                          ,p_servicenameList => '''' || g_qserv_CheckContrTable || ''''
                                          ,p_cnt_service => 1
                                          ,p_corrmsgid => vr_param_limit_start.Msgid
                                          ,p_KeyService => g_qserv_SetLotTmpStart)
    then
      return;
    end if;
    qmanager_do_service_market(p_servicename1 => g_qserv_SetLotTmpStart, p_servicename2 => g_qserv_SetTickTmp, p_messmeta => p_messmeta);
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_SetStart ');
  end;

  -- Сервис параллельгного старта отбора лотов
  procedure Limit_LotTmpStart(p_worklogid integer
                             ,p_messbody  clob
                             ,p_messmeta  xmltype
                             ,o_msgid     out varchar2
                             ,o_MSGCode   out integer
                             ,o_MSGText   out varchar2
                             ,o_messbody  out clob
                             ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                ,p_CalcDate => p_CalcDate
                ,p_action => 120
                ,p_label => 'Отбор лотов'
                ,p_NPPmarket => vr_param_limit_create.NPP
                ,p_MarketCode => vr_param_limit_create.MarketCode
                ,p_dtstart => systimestamp);
    if p_ByStock <> 0
    then
      RSHB_RSI_SCLIMIT.ClearLotTmp(p_CalcDate, p_MarketID, p_UseListClients);
    end if;
    qmanager_do_service_parallel(p_servicename => g_qserv_SetLotTmp, p_messmeta => p_messmeta);
    add_log(p_MarketID => p_MarketID, p_msg => 'SetLotTmpStart ');
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис расчета лимитов по площадке.Отбор сделок
  procedure Limit_TickTmp(p_worklogid integer
                         ,p_messbody  clob
                         ,p_messmeta  xmltype
                         ,o_msgid     out varchar2
                         ,o_MSGCode   out integer
                         ,o_MSGText   out varchar2
                         ,o_messbody  out clob
                         ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if p_ByStock <> 0
       or p_ByEDP != 0
    then
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 110
                  ,p_label => 'Отбор сделок'
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => systimestamp);
      RSHB_RSI_SCLIMIT.ClearTickTmp(p_CalcDate, p_ByStock, p_ByEDP, p_MarketID, p_UseListClients);
      RSHB_RSI_SCLIMIT.SetTickTmp(p_CalcDate, p_ByStock, p_ByEDP, p_MarketID, p_UseListClients);
    end if;
    if p_ByStock <> 0
    then
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 130
                  ,p_label => 'Отбор фин.инструментов в поставке'
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => systimestamp);
      RSHB_RSI_SCLIMIT.ClearFIIDTmp(p_MarketID, p_UseListClients);
      RSHB_RSI_SCLIMIT.SetFIIDTmp(p_CalcDate, 0, p_MarketID, p_UseListClients);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_TickTmp ');
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис расчета лимитов по площадке.Отбор требований и обязательств
  procedure Limit_CollectPlanSumCur(p_worklogid integer
                        ,p_messbody  clob
                        ,p_messmeta  xmltype
                        ,o_msgid     out varchar2
                        ,o_MSGCode   out integer
                        ,o_MSGText   out varchar2
                        ,o_messbody  out clob
                        ,o_messmeta  out xmltype) is
    vr_param_limit_start    tr_limit_start;
    vr_param_limit_create   tr_limit_create;
    vr_param_limit_parallel tr_parallel_sid;
    p_MarketID              number;
    p_MarketCode            varchar2(100);
    p_CalcDate              date;
    p_ByStock               number;
    p_ByCurr                number;
    p_ByDeriv               number;
    p_ByEDP                 number;
    p_UseListClients        number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_limit_parallel            := get_param_ServiceParallel(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_parallel.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := 1;
    if ((p_ByCurr <> 0 or p_ByEDP = 1) and p_MarketID = RSHB_RSI_SCLIMIT.GetMicexID())
    then
      if vr_param_limit_parallel.num = 1
      then
        CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                    ,p_CalcDate => p_CalcDate
                    ,p_action => 140
                    ,p_label => 'Отбор требований и обязательств'
                    ,p_NPPmarket => vr_param_limit_create.NPP
                    ,p_MarketCode => vr_param_limit_create.MarketCode
                    ,p_dtstart => systimestamp);
      end if;
      RSHB_RSI_SCLIMIT.CollectPlanSumCur(p_CalcDate, p_ByCurr, p_ByEDP, p_MarketID, p_UseListClients);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CMTick  parallel' || vr_param_limit_parallel.num);
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;


  -- Сервис расчета лимитов по площадке.Отбор лотов
  procedure Limit_LotTmp_parallel(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype) is
    vr_param_limit_start    tr_limit_start;
    vr_param_limit_create   tr_limit_create;
    vr_param_limit_parallel tr_parallel_sid;
    p_MarketID              number;
    p_MarketCode            varchar2(100);
    p_CalcDate              date;
    p_ByStock               number;
    p_ByCurr                number;
    p_ByDeriv               number;
    p_ByEDP                 number;
    p_UseListClients        number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_limit_parallel            := get_param_ServiceParallel(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_parallel.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := 1;
    if p_ByStock <> 0
    then
      RSHB_RSI_SCLIMIT.SetLotTmp(p_CalcDate, 0, p_MarketID, p_UseListClients);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_LotTmp_parallel ' || vr_param_limit_parallel.num);
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис Запуск расчета димитов по площадке
  procedure Limit_Create(p_worklogid integer
                        ,p_messbody  clob
                        ,p_messmeta  xmltype
                        ,o_msgid     out varchar2
                        ,o_MSGCode   out integer
                        ,o_MSGText   out varchar2
                        ,o_messbody  out clob
                        ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    v_count_pre_exec      number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if not qmanager_wait_service_byMarket(p_MarketID => vr_param_limit_create.MarketID
                                         ,p_servicenameList => '''' || g_qserv_SetTickTmp || ''',''' || g_qserv_SetLotTmp || ''''
                                         ,p_cnt_service => 2
                                         ,p_corrmsgid => vr_param_limit_start.Msgid
                                         ,p_lockmsgid => vr_param_limit_create.Msgid
                                         ,p_chkService => gt_qservice_AllLimit(1))
    then
      return;
    end if;
    for n in 1 .. gt_qservice_AllLimit.count
    loop
      --continue when gt_qservice_Limit(n) in(g_qserv_CashEDPLimitsCur); -- блокировки Запустим после Cash 
      qmanager_load_msg(p_servicename => gt_qservice_AllLimit(n), p_messmeta => p_messmeta, p_corrmsgid => vr_param_limit_start.Msgid);
    end loop;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_Create ');
  end;

  -- Сервис Расчет лимитов MONEY фондовый рынок
  procedure Limit_CashStockLimits(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    vr_message_log        itt_q_message_log%rowtype;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_message_log                     := it_q_message.messlog_get(p_logid => p_worklogid);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if p_ByStock <> 0
    then
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 200
                  ,p_label => 'Расчет лимитов MONEY фондовый рынок'
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => systimestamp);
      RSHB_RSI_SCLIMIT.RSI_ClearCashStockLimits(p_CalcDate, p_ByStock, 0, 0, p_MArketCode, p_MarketID, /* RSHB_RSI_SCLIMIT.mainsessionid,*/ p_UseListClients);
      --RSHB_RSI_SCLIMIT.RSI_CreateCashStockLimits(p_CalcDate, p_ByStock, 0, 0, p_MArketCode, p_MarketID, mainsessionid, p_UseListClients);
      qmanager_do_service_ByKind(p_servicename => g_qserv_CashStockLimByKind
                                ,p_messmeta => p_messmeta
                                ,p_LimitService => g_qserv_CashStockLimits
                                ,p_LimitMsgid => vr_message_log.msgid
                                ,p_param1 => p_ByStock
                                ,p_param2 => 0
                                ,p_param3 => 0);
    else
      -- qmanager_load_msg(p_servicename => g_qserv_CashEDPLimits, p_messmeta => p_messmeta, p_corrmsgid => vr_param_limit_start.Msgid);
      qmanager_do_service_FINISH(p_Service => g_qserv_CashFINISH, p_KeyService => g_qserv_CashStockLimits, p_messmeta => p_messmeta);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CashStockLimits ');
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис Расчет лимитов MONEY фондовый рынок
  procedure Limit_CashStockLimByKind(p_worklogid integer
                                    ,p_messbody  clob
                                    ,p_messmeta  xmltype
                                    ,o_msgid     out varchar2
                                    ,o_MSGCode   out integer
                                    ,o_MSGText   out varchar2
                                    ,o_messbody  out clob
                                    ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    vr_param_LimitByKind  tr_LimitByKind;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    p_Kind                number;
    p_param1              number;
    p_param2              number;
    p_param3              number;
    v_dtstart             timestamp := systimestamp;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_LimitByKind               := get_param_LimitByKind(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    p_Kind                             := vr_param_LimitByKind.Kind;
    p_param1                           := vr_param_LimitByKind.param1;
    p_param2                           := vr_param_LimitByKind.param2;
    p_param3                           := vr_param_LimitByKind.param3;
    if p_ByStock <> 0
    then
      rshb_rsi_sclimit.RSI_CreateCashStockLimByKind(p_Kind, p_Kind, p_CalcDate, p_param1, p_param2, p_param3, p_MarketCode, p_MarketID);
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 200 + (p_Kind * 10)
                  ,p_label => 'Расчет лимита ' || Get_nameLimitKind(p_Kind) || ' MONEY'
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => v_dtstart);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CashStockLimByKind #' || p_Kind);
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис окончания Расчет лимитов MONEY фондовый рынок
  procedure Limit_CashStockLimByKindFINISH(p_worklogid integer
                                          ,p_messbody  clob
                                          ,p_messmeta  xmltype
                                          ,o_msgid     out varchar2
                                          ,o_MSGCode   out integer
                                          ,o_MSGText   out varchar2
                                          ,o_messbody  out clob
                                          ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    vr_param_LimitByKind  tr_LimitByKind;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    p_Kind                number;
    p_param1              number;
    p_param2              number;
    p_paramS1             varchar2(128);
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_LimitByKind               := get_param_LimitByKind(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    p_Kind                             := vr_param_LimitByKind.Kind;
    p_param1                           := vr_param_LimitByKind.param1;
    p_param2                           := vr_param_LimitByKind.param2;
    p_paramS1                          := vr_param_LimitByKind.paramS1;
    if not qmanager_wait_service_byKind(p_MarketID => p_MarketID
                                       ,p_servicename => g_qserv_CashStockLimByKind
                                       ,p_KindCnt => vr_param_LimitByKind.KindCnt
                                       ,p_corrmsgid => vr_param_limit_start.Msgid
                                       ,p_Lockmsgid => vr_param_LimitByKind.LimitMsgid
                                       ,p_chkService => g_qserv_CashFINISH
                                       ,p_KeyService => g_qserv_CashStockLimits)
    then
      return;
    end if;
    qmanager_do_service_FINISH(p_Service => g_qserv_CashFINISH, p_KeyService => g_qserv_CashStockLimits, p_messmeta => p_messmeta, p_corrmsgid => vr_param_LimitByKind.LimitMsgid);
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CashStockLimByKindFINISH ');
  end;

  -- Сервис Очистка расчетов лимитов DEPO фондовый рынок
  procedure Limit_ClearSecurLimits(p_worklogid integer
                                  ,p_messbody  clob
                                  ,p_messmeta  xmltype
                                  ,o_msgid     out varchar2
                                  ,o_MSGCode   out integer
                                  ,o_MSGText   out varchar2
                                  ,o_messbody  out clob
                                  ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    v_IsDepo              number := 0;
    v_IsKind2             number := 0;
    v_DepoAcc             varchar2(20); -- := CHR(1);
    vr_message_log        itt_q_message_log%rowtype;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_message_log                     := it_q_message.messlog_get(p_logid => p_worklogid);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if p_ByStock <> 0
    then
      RSHB_RSI_SCLIMIT.getFlagLimitPrm(p_MarketID, RSHB_RSI_SCLIMIT.MARKET_KIND_STOCK, v_IsDepo, v_IsKind2, v_DepoAcc);
      RSHB_RSI_SCLIMIT.RSI_ClearSecurLimits(p_CalcDate, p_ByStock, 0, v_DepoAcc, p_MArketCode, p_MarketID, p_UseListClients);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_ClearSecurLimits ');
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис Старт расчет лимитов DEPO фондовый рынок
  procedure Limit_SecurLimits(p_worklogid integer
                             ,p_messbody  clob
                             ,p_messmeta  xmltype
                             ,o_msgid     out varchar2
                             ,o_MSGCode   out integer
                             ,o_MSGText   out varchar2
                             ,o_messbody  out clob
                             ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    v_IsDepo              number := 0;
    v_IsKind2             number := 0;
    v_DepoAcc             varchar2(20); -- := CHR(1);
    vr_message_log        itt_q_message_log%rowtype;
    v_lockmsgid           itt_q_message_log.msgid%type;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_message_log                     := it_q_message.messlog_get(p_logid => p_worklogid);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if p_ByStock <> 0
    then
      if not qmanager_wait_service(p_servicename1 => g_qserv_ClearSecurLimits
                                  ,p_msgid => vr_message_log.msgid
                                  ,p_MarketID => p_MarketID
                                  ,p_corrmsgid => vr_param_limit_start.Msgid
                                  ,p_lockmsgid => null
                                  ,p_chkService => g_qserv_SecurLimByKind)
      then
        return;
      end if;
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 300
                  ,p_label => 'Расчет лимитов DEPO фондовый рынок'
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => systimestamp);
      RSHB_RSI_SCLIMIT.getFlagLimitPrm(p_MarketID, RSHB_RSI_SCLIMIT.MARKET_KIND_STOCK, v_IsDepo, v_IsKind2, v_DepoAcc);
      -- RSHB_RSI_SCLIMIT.RSI_ClearSecurLimits(p_CalcDate, p_ByStock, 0, v_DepoAcc, p_MArketCode, p_MarketID, RSHB_RSI_SCLIMIT.mainsessionid, p_UseListClients);
      -- RSHB_RSI_SCLIMIT.RSI_CreateSecurLimits(p_CalcDate, p_ByStock, 0, v_DepoAcc, p_MArketCode, p_MarketID, RSHB_RSI_SCLIMIT.mainsessionid, p_UseListClients);
      qmanager_do_service_ByKind(p_servicename => g_qserv_SecurLimByKind
                                ,p_messmeta => p_messmeta
                                ,p_LimitService => g_qserv_SecurLimits
                                ,p_LimitMsgid => vr_message_log.msgid
                                ,p_param1 => p_ByStock
                                ,p_param2 => 0
                                ,p_paramS1 => v_DepoAcc);
    else
      qmanager_do_service_FINISH(p_Service => g_qserv_LimitFINISH, p_KeyService => g_qserv_SecurLimits, p_messmeta => p_messmeta);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_SecurLimits ');
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис Расчет лимитов DEPO фондовый рынок
  procedure Limit_SecurLimByKind(p_worklogid integer
                                ,p_messbody  clob
                                ,p_messmeta  xmltype
                                ,o_msgid     out varchar2
                                ,o_MSGCode   out integer
                                ,o_MSGText   out varchar2
                                ,o_messbody  out clob
                                ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    vr_param_LimitByKind  tr_LimitByKind;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    p_Kind                number;
    p_param1              number;
    p_param2              number;
    p_paramS1             varchar2(128);
    v_dtstart             timestamp := systimestamp;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_LimitByKind               := get_param_LimitByKind(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    p_Kind                             := vr_param_LimitByKind.Kind;
    p_param1                           := vr_param_LimitByKind.param1;
    p_param2                           := vr_param_LimitByKind.param2;
    p_paramS1                          := vr_param_LimitByKind.paramS1;
    if p_ByStock <> 0
    then
      rshb_rsi_sclimit.RSI_CreateSecurLimByKind(p_Kind, p_Kind, p_CalcDate, p_param1, p_param2, p_paramS1, p_MarketCode, p_MarketID);
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 300 + (p_Kind * 10)
                  ,p_label => 'Расчет лимита DEPO ' || Get_nameLimitKind(p_Kind)
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => v_dtstart);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_SecurLimByKind #' || p_Kind);
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис окончания Расчет лимитов DEPO фондовый рынок
  procedure Limit_SecurLimByKindFINISH(p_worklogid integer
                                      ,p_messbody  clob
                                      ,p_messmeta  xmltype
                                      ,o_msgid     out varchar2
                                      ,o_MSGCode   out integer
                                      ,o_MSGText   out varchar2
                                      ,o_messbody  out clob
                                      ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    vr_param_LimitByKind  tr_LimitByKind;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    p_Kind                number;
    p_param1              number;
    p_param2              number;
    p_paramS1             varchar2(128);
    v_IsDepo              number := 0;
    v_IsKind2             number := 0;
    v_DepoAcc             varchar2(20); -- := CHR(1);
    vx_addparam           xmltype;
    p_parallel            integer;
    vr_mess_log           itt_q_message_log%rowtype;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_LimitByKind               := get_param_LimitByKind(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    p_Kind                             := vr_param_LimitByKind.Kind;
    p_param1                           := vr_param_LimitByKind.param1;
    p_param2                           := vr_param_LimitByKind.param2;
    p_paramS1                          := vr_param_LimitByKind.paramS1;
    p_parallel                         := vr_param_limit_start.parallel;
    if not qmanager_wait_service_byKind(p_MarketID => p_MarketID
                                       ,p_servicename => g_qserv_SecurLimByKind
                                       ,p_KindCnt => vr_param_LimitByKind.KindCnt
                                       ,p_corrmsgid => vr_param_limit_start.Msgid
                                       ,p_Lockmsgid => vr_param_LimitByKind.LimitMsgid
                                       ,p_chkService => g_qserv_WAPositionPrice -- g_qserv_CashFINISH
                                       ,p_KeyService => null)
    then
      return;
    end if;
    if it_rs_interface.get_parm_varchar_path(p_parm_path => 'РСХБ\ИНТЕГРАЦИЯ\CHECK_WRITEOFF') = chr(88)
       and p_CalcDate >= trunc(sysdate) -- BIQ-1304 Вызывается в конце процедуры расчета лимитов
    then
      it_diasoft.PKO_CheckAndCorrectSecuritiesLimits(p_MarketID, p_CalcDate, p_UseListClients);
    end if;
    rshb_rsi_sclimit.RSI_LOCKSecurLimits(p_CalcDate, p_param1, p_param2, p_paramS1, p_MArketCode, p_MarketID, p_UseListClients);
    CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                ,p_CalcDate => p_CalcDate
                ,p_group => 2000
                ,p_action => 100
                ,p_label => 'Расчет цен приобретения'
                ,p_NPPmarket => vr_param_limit_create.NPP
                ,p_MarketCode => vr_param_limit_create.MarketCode
                ,p_dtstart => systimestamp);

    vr_mess_log := it_q_message.messlog_get(p_logid => p_worklogid);

    if p_UseListClients = 1
    then
      select xmlelement("addparam"
                        ,xmlelement("StartMsgId", vr_mess_log.msgid)
                        ,XMLAGG(xmlelement("parallel", XMLFOREST(ntile as "num", min(t_id) as "min_id", max(t_id) as "max_id"))))
        into vx_addparam
        from (select i.t_id
                    ,NTILE(1) over(order by t_id) as ntile
                from DDL_LIMITSECURITES_DBT i
               where t_market_kind = 'фондовый'
                 and t_client in (select t_clientid
                                    from ddl_panelcontr_dbt
                                   where t_calc_sid = vr_param_limit_create.calc_panelcontr
                                     and t_setflag = chr(88))
                 and t_limit_kind = 2
                 and t_open_balance <> 0
                 and t_market = p_MarketID
                 and t_date = p_CalcDate)
       group by ntile;
    else
      select xmlelement("addparam"
                        ,xmlelement("StartMsgId", vr_mess_log.msgid)
                        ,XMLAGG(xmlelement("parallel", XMLFOREST(ntile as "num", min(t_id) as "min_id", max(t_id) as "max_id"))))
        into vx_addparam
        from (select i.t_id
                    ,NTILE(p_parallel) over(order by t_id) as ntile
                from DDL_LIMITSECURITES_DBT i
               where t_market_kind = 'фондовый'
                 and t_limit_kind = 2
                 and t_open_balance <> 0
                 and t_market = p_MarketID
                 and t_date = p_CalcDate)
       group by ntile;
    end if;
    qmanager_do_service_parallel(p_servicename => g_qserv_WAPositionPrice, p_messmeta => p_messmeta, p_addparam => vx_addparam.getStringVal);
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_SecurLimByKindFINISH ');
  end;

  -- Сервис расчет цен приобритения
  procedure Limit_WAPositionPrice_parallel(p_worklogid integer
                                          ,p_messbody  clob
                                          ,p_messmeta  xmltype
                                          ,o_msgid     out varchar2
                                          ,o_MSGCode   out integer
                                          ,o_MSGText   out varchar2
                                          ,o_messbody  out clob
                                          ,o_messmeta  out xmltype) is
    vr_param_limit_start    tr_limit_start;
    vr_param_limit_create   tr_limit_create;
    vr_param_limit_parallel tr_parallel_sid;
    p_MarketID              number;
    p_MarketCode            varchar2(100);
    p_CalcDate              date;
    p_ByStock               number;
    p_ByCurr                number;
    p_ByDeriv               number;
    p_ByEDP                 number;
    p_UseListClients        number;
    vx_addparam             xmltype;
    v_min_id                number;
    v_max_id                number;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_limit_parallel            := get_param_ServiceParallel(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    vx_addparam                        := xmltype(vr_param_limit_parallel.addparam);
    begin
      with x as
       (select vx_addparam xml_data from dual)
      select res.min_id
            ,res.max_id
        into v_min_id
            ,v_max_id
        from x
            ,XMLTABLE('/addparam/parallel' PASSING x.xml_data COLUMNS num number PATH 'num', min_id number PATH 'min_id', max_id number PATH 'max_id') res
       where res.num = vr_param_limit_parallel.num;
    exception
      when no_data_found then
        v_min_id := null;
        v_max_id := null;
    end;
    if v_min_id is not null
       and v_max_id is not null
    then
      RSHB_RSI_SCLIMIT.SetWAPositionPrice(p_MarketID => p_MarketID, p_id_first => v_min_id, p_id_last => v_max_id, p_UseListClients => p_UseListClients);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_WAPositionPrice_parallel ' || vr_param_limit_parallel.num || ' start ' || v_min_id || ' stop ' || v_max_id);
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Завершение расчета цен приобритения  
  procedure Limit_SecurLimitsFINISH(p_worklogid integer
                                   ,p_messbody  clob
                                   ,p_messmeta  xmltype
                                   ,o_msgid     out varchar2
                                   ,o_MSGCode   out integer
                                   ,o_MSGText   out varchar2
                                   ,o_messbody  out clob
                                   ,o_messmeta  out xmltype) is
    vr_param_limit_start    tr_limit_start;
    vr_param_limit_create   tr_limit_create;
    vr_param_limit_parallel tr_parallel_sid;
    p_MarketID              number;
    p_MarketCode            varchar2(100);
    p_CalcDate              date;
    p_ByStock               number;
    p_ByCurr                number;
    p_ByDeriv               number;
    p_ByEDP                 number;
    p_UseListClients        number;
    vx_addparam             xmltype;
    v_StartMsgId            varchar2(128);
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_limit_parallel            := get_param_ServiceParallel(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    vx_addparam                        := xmltype(vr_param_limit_parallel.addparam);
    with meta as
     (select vx_addparam as x from dual)
    select EXTRACTVALUE(meta.x, '/addparam/StartMsgId') into v_StartMsgId from meta;
    if not qmanager_wait_service_byMarket(p_MarketID => vr_param_limit_create.MarketID
                                         ,p_servicenameList => '''' || g_qserv_WAPositionPrice || ''''
                                         ,p_cnt_service => 1
                                         ,p_corrmsgid => vr_param_limit_start.Msgid
                                         ,p_lockmsgid => v_StartMsgId
                                         ,p_chkService => g_qserv_LimitFINISH
                                         ,p_KeyService => g_qserv_SecurLimits)
    then
      return;
    end if;
    RSHB_RSI_SCLIMIT.SetWAPositionPrice365(p_CalcDate => p_CalcDate, p_MarketID => p_MarketID, p_UseListClients => p_UseListClients);
    CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                ,p_CalcDate => p_CalcDate
                ,p_group => 2000
                ,p_action => 100
                ,p_label => 'Цены рассчитаны'
                ,p_NPPmarket => vr_param_limit_create.NPP
                ,p_MarketCode => vr_param_limit_create.MarketCode
                ,p_dtstart => systimestamp);
    qmanager_do_service_FINISH(p_Service => g_qserv_LimitFINISH, p_KeyService => g_qserv_SecurLimits, p_messmeta => p_messmeta);
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_SecurLimitsFINISH');
  end;

  -- Сервис Расчет лимитов валютный рынок
  procedure Limit_CashStockLimitsCur(p_worklogid integer
                                    ,p_messbody  clob
                                    ,p_messmeta  xmltype
                                    ,o_msgid     out varchar2
                                    ,o_MSGCode   out integer
                                    ,o_MSGText   out varchar2
                                    ,o_messbody  out clob
                                    ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    v_IsDepo              number := 0;
    v_IsKind2             number := 0;
    v_DepoAcc             varchar2(20); -- := CHR(1);
    vr_message_log        itt_q_message_log%rowtype;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_message_log                     := it_q_message.messlog_get(p_logid => p_worklogid);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if (p_ByCurr <> 0 and p_MarketID = RSHB_RSI_SCLIMIT.GetMicexID())
    then
      if not qmanager_wait_service(--p_servicename1 => g_qserv_SetCMComm
                                  p_servicename1 => g_qserv_SetCMTick
                                  ,p_msgid => vr_message_log.msgid
                                  ,p_MarketID => p_MarketID
                                  ,p_corrmsgid => vr_param_limit_start.Msgid
                                  ,p_lockmsgid => null
                                  ,p_chkService => g_qserv_CashStockLimCurByKind)
      then
        return;
      end if;
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 700
                  ,p_label => 'Расчет лимитов валютный рынок'
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => systimestamp);
      p_ByEDP := 0;
      RSHB_RSI_SCLIMIT.getFlagLimitPrm(p_MarketID, RSHB_RSI_SCLIMIT.MARKET_KIND_CURR, v_IsDepo, v_IsKind2, v_DepoAcc); -- для валютного определим признаки(настройки)
      -- для валютного только биржа
      RSHB_RSI_SCLIMIT.RSI_ClearCashStockLimitsCur(p_CalcDate, 0 /*EDP*/, p_UseListClients);
      qmanager_do_service_ByKind(p_servicename => g_qserv_CashStockLimCurByKind
                                ,p_messmeta => p_messmeta
                                ,p_LimitService => g_qserv_CashStockLimitsCur
                                ,p_LimitMsgid => vr_message_log.msgid
                                ,p_param1 => v_IsKind2
                                ,p_param2 => v_IsDepo
                                ,p_param3 => p_ByEDP
                                ,p_paramS1 => v_DepoAcc
                                ,p_KindCnt => case
                                                when (v_IsKind2 = 1)
                                                     or (p_ByEDP = 1) then
                                                 4
                                                else
                                                 2
                                              end);
    else
      qmanager_do_service_FINISH(p_Service => g_qserv_CashFINISH, p_KeyService => g_qserv_CashStockLimitsCur, p_messmeta => p_messmeta);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CashStockLimitsCur ');
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис Расчет лимитов Расчет лимитов валютный рынок
  procedure Limit_CashStockLimCurByKind(p_worklogid integer
                                       ,p_messbody  clob
                                       ,p_messmeta  xmltype
                                       ,o_msgid     out varchar2
                                       ,o_MSGCode   out integer
                                       ,o_MSGText   out varchar2
                                       ,o_messbody  out clob
                                       ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    vr_param_LimitByKind  tr_LimitByKind;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    p_Kind                number;
    p_param1              number;
    p_param2              number;
    p_param3              number;
    p_paramS1             varchar2(128);
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_LimitByKind               := get_param_LimitByKind(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    p_Kind                             := vr_param_LimitByKind.Kind;
    p_param1                           := vr_param_LimitByKind.param1; --,p_param1 => v_IsKind2
    p_param2                           := vr_param_LimitByKind.param2; --,p_param2 => v_IsDepo
    p_param3                           := vr_param_LimitByKind.param3; --,p_param3 => p_ByEDP
    p_paramS1                          := vr_param_LimitByKind.paramS1; --,p_paramS1 => v_DepoAcc
    if (p_ByCurr <> 0 and p_MarketID = RSHB_RSI_SCLIMIT.GetMicexID())
    then
      rshb_rsi_sclimit.RSI_CreateCashStockLimByKindCur_job(p_Kind, p_Kind, p_CalcDate, p_param2, p_param3);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CashStockLimCurByKind #' || p_Kind);
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис окончания Расчет лимитов Расчет лимитов валютный рынок
  procedure Limit_CashStockLimCurByKindFINISH(p_worklogid integer
                                             ,p_messbody  clob
                                             ,p_messmeta  xmltype
                                             ,o_msgid     out varchar2
                                             ,o_MSGCode   out integer
                                             ,o_MSGText   out varchar2
                                             ,o_messbody  out clob
                                             ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    vr_param_LimitByKind  tr_LimitByKind;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    p_Kind                number;
    v_IsKind2             number;
    v_IsDepo              number;
    p_param3              number;
    v_DepoAcc             varchar2(128);
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_LimitByKind               := get_param_LimitByKind(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    p_Kind                             := vr_param_LimitByKind.Kind;
    v_IsKind2                          := vr_param_LimitByKind.param1; --,p_param1 => v_IsKind2
    v_IsDepo                           := vr_param_LimitByKind.param2; --,p_param2 => v_IsDepo
    p_param3                           := vr_param_LimitByKind.param2; --,p_param3 => p_ByEDP
    v_DepoAcc                          := vr_param_LimitByKind.paramS1; --,p_paramS1 => v_DepoAcc
    if not qmanager_wait_service_byKind(p_MarketID => p_MarketID
                                       ,p_servicename => g_qserv_CashStockLimCurByKind
                                       ,p_KindCnt => vr_param_LimitByKind.KindCnt
                                       ,p_corrmsgid => vr_param_limit_start.Msgid
                                       ,p_Lockmsgid => vr_param_LimitByKind.LimitMsgid
                                       ,p_chkService => g_qserv_CashFINISH --g_qserv_LimitFINISH --
                                       ,p_KeyService => g_qserv_CashStockLimitsCur)
    then
      return;
    end if;
    if (p_ByCurr <> 0 and p_MarketID = RSHB_RSI_SCLIMIT.GetMicexID())
    then
      rshb_rsi_sclimit.RSI_DeleteCashStockLimitsCur(p_CalcDate, v_IsKind2, v_IsDepo, 0 /*EDP*/, p_UseListClients);
      if (v_IsDepo <> 0)
      then
        -- формируется только с установленным признаком Depo
        rshb_rsi_sclimit.RSI_CreateSecurLimitsCur(p_CalcDate, v_IsKind2, v_DepoAcc);
      else
        rshb_rsi_sclimit.RSI_CreateSecurLimByKindCurZero(p_CalcDate, 0, v_DepoAcc, p_MarketID, p_MarketCode, 0 /*EDP*/, p_UseListClients);
        rshb_rsi_sclimit.RSI_DeleteZeroSecurLimByCur(p_CalcDate);
      end if;
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 799
                  ,p_label => 'Расчет лимитов по валютному рынку завершен '
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => systimestamp);
    end if;
    qmanager_do_service_FINISH(p_Service => g_qserv_CashFINISH
                              ,p_KeyService => g_qserv_CashStockLimitsCur
                              ,p_messmeta => p_messmeta
                              ,p_corrmsgid => vr_param_LimitByKind.LimitMsgid);
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CashStockLimCurByKindFINISH ');
  end;

  -- Сервис Старт расчета по срочному рынку
  procedure Limit_FutureMarkLimits(p_worklogid integer
                                  ,p_messbody  clob
                                  ,p_messmeta  xmltype
                                  ,o_msgid     out varchar2
                                  ,o_MSGCode   out integer
                                  ,o_MSGText   out varchar2
                                  ,o_messbody  out clob
                                  ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    v_IsDepo              number := 0;
    v_IsKind2             number := 0;
    v_DepoAcc             varchar2(20); -- := CHR(1);
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if p_ByDeriv <> 0
    then
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 800
                  ,p_label => 'Расчет лимитов срочный рынок'
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => systimestamp);
      rshb_rsi_sclimit.RSI_CreateFutureMarkLimits(p_CalcDate, p_UseListClients);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_FutureMarkLimits ');
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис окончания расчета по срочному рынку
  procedure Limit_FutureMarkLimitsFINISH(p_worklogid integer
                                        ,p_messbody  clob
                                        ,p_messmeta  xmltype
                                        ,o_msgid     out varchar2
                                        ,o_MSGCode   out integer
                                        ,o_MSGText   out varchar2
                                        ,o_messbody  out clob
                                        ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    vr_param_LimitByKind  tr_LimitByKind;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    p_Kind                number;
    v_IsKind2             number;
    v_IsDepo              number;
    p_param3              number;
    v_DepoAcc             varchar2(128);
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_LimitByKind               := get_param_LimitByKind(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    p_Kind                             := vr_param_LimitByKind.Kind;
    v_IsKind2                          := vr_param_LimitByKind.param1; --,p_param1 => v_IsKind2
    v_IsDepo                           := vr_param_LimitByKind.param2; --,p_param2 => v_IsDepo
    p_param3                           := vr_param_LimitByKind.param2; --,p_param3 => p_ByEDP
    v_DepoAcc                          := vr_param_LimitByKind.paramS1; --,p_paramS1 => v_DepoAcc
    if p_ByDeriv <> 0
    then
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 899
                  ,p_label => 'Расчет лимитов срочной секции завершен'
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => systimestamp);
    end if;
    qmanager_do_service_FINISH(p_Service => g_qserv_LimitFINISH, p_KeyService => g_qserv_FutureMarkLimits, p_messmeta => p_messmeta);
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_FutureMarkLimitsFINISH ');
  end;

  -- Сервис очистки лимитов ЕДП
  procedure Limit_ClearLimitsEDP(p_worklogid integer
                                ,p_messbody  clob
                                ,p_messmeta  xmltype
                                ,o_msgid     out varchar2
                                ,o_MSGCode   out integer
                                ,o_MSGText   out varchar2
                                ,o_messbody  out clob
                                ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    vr_message_log        itt_q_message_log%rowtype;
    MaxLotChangeDate      date;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_message_log                     := it_q_message.messlog_get(p_logid => p_worklogid);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_ClearLimitsEDP Старт ');
    if p_MarketID = RSHB_RSI_SCLIMIT.GetSpbexID()
    then
      -- Предварительное вычисление 
      MaxLotChangeDate := RSHB_RSI_SCLIMIT.GetLotMaxChangeDate;
    end if;
    if p_ByEDP <> 0
    then
      RSHB_RSI_SCLIMIT.RSI_ClearCashStockLimits(p_CalcDate, p_ByStock, 0, 1, p_MarketCode, p_MarketID, p_UseListClients);
      if p_MarketID = RSHB_RSI_SCLIMIT.GetMicexID()
      then
        RSHB_RSI_SCLIMIT.RSI_ClearCashStockLimitsCur(p_CalcDate, 1 /*EDP*/, p_UseListClients);
      end if;
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_ClearLimitsEDP ');
  end;

  -- Сервис Расчет лимитов MONEY EDP
  procedure Limit_CashEDPLimits(p_worklogid integer
                               ,p_messbody  clob
                               ,p_messmeta  xmltype
                               ,o_msgid     out varchar2
                               ,o_MSGCode   out integer
                               ,o_MSGText   out varchar2
                               ,o_messbody  out clob
                               ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    vr_message_log        itt_q_message_log%rowtype;
    v_msgid               itt_q_message_log.msgid%type;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_message_log                     := it_q_message.messlog_get(p_logid => p_worklogid);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if p_ByEDP <> 0
    then
      if not qmanager_wait_service(p_servicename1 => g_qserv_ClearEDPLimits
                                  ,p_msgid => vr_message_log.msgid
                                  ,p_MarketID => p_MarketID
                                  ,p_corrmsgid => vr_param_limit_start.Msgid
                                  ,p_lockmsgid => null
                                  ,p_chkService => g_qserv_CashEDPLimByKind)
      then
        return;
      end if;
      -- Вынесено в Limit_CheckContrTableStart
      --RSHB_RSI_SCLIMIT.RSI_ClearCashStockLimits(p_CalcDate, p_ByStock, 0, 1, p_MArketCode, p_MarketID, RSHB_RSI_SCLIMIT.mainsessionid, p_UseListClients);
      --RSHB_RSI_SCLIMIT.RSI_CreateCashStockLimits(p_CalcDate, p_ByStock, 0, 0, p_MArketCode, p_MarketID, mainsessionid, p_UseListClients);
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 500
                  ,p_label => 'Расчет по фондовому рынку ЕДП'
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => systimestamp);
      qmanager_do_service_ByKind(p_servicename => g_qserv_CashEDPLimByKind
                                ,p_messmeta => p_messmeta
                                ,p_LimitService => g_qserv_CashEDPLimits
                                ,p_LimitMsgid => vr_message_log.msgid
                                ,p_param1 => p_ByStock
                                ,p_param2 => 0
                                ,p_param3 => 1);
    else
      qmanager_do_service_FINISH(p_Service => g_qserv_CashFINISH, p_KeyService => g_qserv_CashEDPLimits, p_messmeta => p_messmeta);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CashEDPLimits ');
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис Расчет лимитов MONEY EDP
  procedure Limit_CashEDPLimByKind(p_worklogid integer
                                  ,p_messbody  clob
                                  ,p_messmeta  xmltype
                                  ,o_msgid     out varchar2
                                  ,o_MSGCode   out integer
                                  ,o_MSGText   out varchar2
                                  ,o_messbody  out clob
                                  ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    vr_param_LimitByKind  tr_LimitByKind;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    p_Kind                number;
    p_param1              number;
    p_param2              number;
    p_param3              number;
    v_dtstart             timestamp := systimestamp;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_LimitByKind               := get_param_LimitByKind(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    p_Kind                             := vr_param_LimitByKind.Kind;
    p_param1                           := vr_param_LimitByKind.param1;
    p_param2                           := vr_param_LimitByKind.param2;
    p_param3                           := vr_param_LimitByKind.param3;
    if p_ByEDP <> 0
    then
      rshb_rsi_sclimit.RSI_CreateCashStockLimByKind(p_Kind, p_Kind, p_CalcDate, p_param1, p_param2, p_param3, p_MarketCode, p_MarketID);
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 500 + (p_Kind * 10)
                  ,p_label => 'Расчет лимита ' || Get_nameLimitKind(p_Kind) || ' MONEY'
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => v_dtstart);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CashEDPLimByKind #' || p_Kind);
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис окончания Расчет лимитов MONEY EDP
  procedure Limit_CashEDPLimByKindFINISH(p_worklogid integer
                                        ,p_messbody  clob
                                        ,p_messmeta  xmltype
                                        ,o_msgid     out varchar2
                                        ,o_MSGCode   out integer
                                        ,o_MSGText   out varchar2
                                        ,o_messbody  out clob
                                        ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    vr_param_LimitByKind  tr_LimitByKind;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    p_Kind                number;
    p_param1              number;
    p_param2              number;
    p_paramS1             varchar2(128);
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_LimitByKind               := get_param_LimitByKind(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    p_Kind                             := vr_param_LimitByKind.Kind;
    p_param1                           := vr_param_LimitByKind.param1;
    p_param2                           := vr_param_LimitByKind.param2;
    p_paramS1                          := vr_param_LimitByKind.paramS1;
    if not qmanager_wait_service_byKind(p_MarketID => p_MarketID
                                       ,p_servicename => g_qserv_CashEDPLimByKind
                                       ,p_KindCnt => vr_param_LimitByKind.KindCnt
                                       ,p_corrmsgid => vr_param_limit_start.Msgid
                                       ,p_Lockmsgid => vr_param_LimitByKind.LimitMsgid
                                       ,p_chkService => g_qserv_CashFINISH --g_qserv_LimitFINISH --
                                       ,p_KeyService => g_qserv_CashEDPLimits)
    then
      return;
    end if;
    --qmanager_load_msg(p_servicename => g_qserv_CashEDPLimitsCur, p_messmeta => p_messmeta, p_corrmsgid => vr_param_limit_start.Msgid);
    qmanager_do_service_FINISH(p_Service => g_qserv_CashFINISH, p_KeyService => g_qserv_CashEDPLimits, p_messmeta => p_messmeta, p_corrmsgid => vr_param_LimitByKind.LimitMsgid);
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CashEDPLimByKindFINISH ');
  end;

  -- Сервис Расчет лимитов валютный рынок ЕДП
  procedure Limit_CashEDPLimitsCur(p_worklogid integer
                                  ,p_messbody  clob
                                  ,p_messmeta  xmltype
                                  ,o_msgid     out varchar2
                                  ,o_MSGCode   out integer
                                  ,o_MSGText   out varchar2
                                  ,o_messbody  out clob
                                  ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    v_IsDepo              number := 0;
    v_IsKind2             number := 0;
    v_DepoAcc             varchar2(20); -- := CHR(1);
    vr_message_log        itt_q_message_log%rowtype;
    v_msgid               itt_q_message_log.msgid%type;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_message_log                     := it_q_message.messlog_get(p_logid => p_worklogid);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    if (p_ByEDP = 1 and p_MarketID = RSHB_RSI_SCLIMIT.GetMicexID())
    then
      if not qmanager_wait_service(p_servicename1 => g_qserv_ClearEDPLimits
                                  ,p_servicename2 => g_qserv_SetCMTick
                                  ,p_msgid => vr_message_log.msgid
                                  ,p_MarketID => p_MarketID
                                  ,p_corrmsgid => vr_param_limit_start.Msgid
                                  ,p_lockmsgid => null
                                  ,p_chkService => g_qserv_CashEDPLimCurByKind)
      then
        return;
      end if;
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 600
                  ,p_label => 'Расчет лимитов валютный рынок ЕДП'
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => systimestamp);
      RSHB_RSI_SCLIMIT.getFlagLimitPrm(p_MarketID, RSHB_RSI_SCLIMIT.MARKET_KIND_CURR, v_IsDepo, v_IsKind2, v_DepoAcc); -- для валютного определим признаки(настройки)
      -- для валютного только биржа
      -- Вынесено в Limit_CheckContrTableStart
      --RSHB_RSI_SCLIMIT.RSI_ClearCashStockLimitsCur(p_CalcDate, 1 /*EDP*/, p_UseListClients);
      qmanager_do_service_ByKind(p_servicename => g_qserv_CashEDPLimCurByKind
                                ,p_messmeta => p_messmeta
                                ,p_LimitService => g_qserv_CashEDPLimitsCur
                                ,p_LimitMsgid => vr_message_log.msgid
                                ,p_param1 => v_IsKind2
                                ,p_param2 => v_IsDepo
                                ,p_param3 => p_ByEDP
                                ,p_paramS1 => v_DepoAcc
                                ,p_KindCnt => case
                                                when (v_IsKind2 = 1)
                                                     or (p_ByEDP = 1) then
                                                 4
                                                else
                                                 2
                                              end);
    else
      qmanager_do_service_FINISH(p_Service => g_qserv_CashFINISH, p_KeyService => g_qserv_CashEDPLimitsCur, p_messmeta => p_messmeta);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CashEDPLimitsCur ');
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис Расчет лимитов Расчет лимитов валютный рынок ЕДП
  procedure Limit_CashEDPLimCurByKind(p_worklogid integer
                                     ,p_messbody  clob
                                     ,p_messmeta  xmltype
                                     ,o_msgid     out varchar2
                                     ,o_MSGCode   out integer
                                     ,o_MSGText   out varchar2
                                     ,o_messbody  out clob
                                     ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    vr_param_LimitByKind  tr_LimitByKind;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    p_Kind                number;
    p_param1              number;
    p_param2              number;
    p_param3              number;
    p_paramS1             varchar2(128);
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_LimitByKind               := get_param_LimitByKind(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    p_Kind                             := vr_param_LimitByKind.Kind;
    p_param1                           := vr_param_LimitByKind.param1; --,p_param1 => v_IsKind2
    p_param2                           := vr_param_LimitByKind.param2; --,p_param2 => v_IsDepo
    p_param3                           := vr_param_LimitByKind.param3; --,p_param3 => p_ByEDP
    p_paramS1                          := vr_param_LimitByKind.paramS1; --,p_paramS1 => v_DepoAcc
    if (p_ByEDP = 1 and p_MarketID = RSHB_RSI_SCLIMIT.GetMicexID())
    then
      rshb_rsi_sclimit.RSI_CreateCashStockLimByKindCur_job(p_Kind, p_Kind, p_CalcDate, p_param2, p_param3);
    end if;
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CashEDPLimCurByKind #' || p_Kind);
    o_messmeta := p_messmeta;
    o_messbody := 'p_MarketID=' || vr_param_limit_create.MarketID;
  end;

  -- Сервис окончания Расчет лимитов  валютный рынок ЕДП
  procedure Limit_CashEDPLimCurByKindFINISH(p_worklogid integer
                                           ,p_messbody  clob
                                           ,p_messmeta  xmltype
                                           ,o_msgid     out varchar2
                                           ,o_MSGCode   out integer
                                           ,o_MSGText   out varchar2
                                           ,o_messbody  out clob
                                           ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    vr_param_LimitByKind  tr_LimitByKind;
    p_MarketID            number;
    p_MarketCode          varchar2(100);
    p_CalcDate            date;
    p_ByStock             number;
    p_ByCurr              number;
    p_ByDeriv             number;
    p_ByEDP               number;
    p_UseListClients      number;
    p_Kind                number;
    v_IsKind2             number;
    v_IsDepo              number;
    p_param3              number;
    v_DepoAcc             varchar2(128);
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    vr_param_LimitByKind               := get_param_LimitByKind(p_messmeta);
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    p_MarketID                         := vr_param_limit_create.MarketID;
    p_MarketCode                       := vr_param_limit_create.MarketCode;
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStock                          := vr_param_limit_create.ByStock;
    p_ByCurr                           := vr_param_limit_create.ByCurr;
    p_ByDeriv                          := vr_param_limit_create.ByDeriv;
    p_ByEDP                            := vr_param_limit_create.ByEDP;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    p_Kind                             := vr_param_LimitByKind.Kind;
    v_IsKind2                          := vr_param_LimitByKind.param1; --,p_param1 => v_IsKind2
    v_IsDepo                           := vr_param_LimitByKind.param2; --,p_param2 => v_IsDepo
    p_param3                           := vr_param_LimitByKind.param3; --,p_param3 => p_ByEDP
    v_DepoAcc                          := vr_param_LimitByKind.paramS1; --,p_paramS1 => v_DepoAcc
    if not qmanager_wait_service_byKind(p_MarketID => p_MarketID
                                       ,p_servicename => g_qserv_CashEDPLimCurByKind
                                       ,p_KindCnt => vr_param_LimitByKind.KindCnt
                                       ,p_corrmsgid => vr_param_limit_start.Msgid
                                       ,p_Lockmsgid => vr_param_LimitByKind.LimitMsgid
                                       ,p_chkService => g_qserv_CashFINISH --g_qserv_LimitFINISH --
                                       ,p_KeyService => g_qserv_CashEDPLimitsCur)
    then
      return;
    end if;
    if (p_ByEDP = 1 and p_MarketID = RSHB_RSI_SCLIMIT.GetMicexID())
    then
      -- rshb_rsi_sclimit.RSI_DeleteCashStockLimitsCur(p_CalcDate, v_IsKind2, v_IsDepo, 1 /*EDP*/, p_UseListClients);
      qmanager_do_service_FINISH(p_Service => g_qserv_CashFINISH, p_KeyService => g_qserv_CashEDPLimitsCur, p_messmeta => p_messmeta, p_is_tran => false);
      if (v_IsDepo <> 0)
      then
        -- формируется только с установленным признаком Depo
        rshb_rsi_sclimit.RSI_CreateSecurLimitsCur(p_CalcDate, v_IsKind2, v_DepoAcc);
      else
        rshb_rsi_sclimit.RSI_CreateSecurLimByKindCurZero(p_CalcDate, 0, v_DepoAcc, p_MarketID, p_MarketCode, 1 /*EDP*/, p_UseListClients);
        rshb_rsi_sclimit.RSI_DeleteZeroSecurLimByCur(p_CalcDate);
      end if;
      CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                  ,p_CalcDate => p_CalcDate
                  ,p_action => 699
                  ,p_label => 'Расчет лимитов по валютному рынку ЕДП завершен'
                  ,p_NPPmarket => vr_param_limit_create.NPP
                  ,p_MarketCode => vr_param_limit_create.MarketCode
                  ,p_dtstart => systimestamp);
    end if;
    qmanager_do_service_FINISH(p_Service => g_qserv_CashFINISH, p_KeyService => g_qserv_CashEDPLimitsCur, p_messmeta => p_messmeta, p_corrmsgid => vr_param_LimitByKind.LimitMsgid);
    add_log(p_MarketID => p_MarketID, p_msg => 'Limit_CashEDPLimCurByKindFINISH ');
  end;

  procedure CashFINISH(p_CalcDate        date
                      ,p_calc_direct     varchar2
                      ,p_calc_clientinfo varchar2
                      ,p_UseListClients  pls_integer) as
    pragma autonomous_transaction;
  begin
    add_log(p_MarketID => null, p_msg => 'Запуск InsertLIMITCASHSTOCKFromInt ');
    rshb_rsi_sclimit.InsertLIMITCASHSTOCKFromInt(p_CalcDate);
    --table_clear_partition(p_table => 'DDL_LIMITCASHSTOCK_INT', v_sid_calc => p_calc_clientinfo);
    add_log(p_MarketID => null, p_msg => 'Запуск DeleteWoOpenBalance ');
    rshb_rsi_sclimit.DeleteWoOpenBalance(p_CalcDate => p_CalcDate);
    add_log(p_MarketID => null, p_msg => 'Запуск CheckCashStockForDuplAndSetErr ');
    rshb_rsi_sclimit.CheckCashStockForDuplAndSetErr(p_CalcDate);
    commit;
  end;

  -- Сервис Завершения расчета Cash лимитов
  procedure Limit_CashFINISH(p_worklogid integer
                            ,p_messbody  clob
                            ,p_messmeta  xmltype
                            ,o_msgid     out varchar2
                            ,o_MSGCode   out integer
                            ,o_MSGText   out varchar2
                            ,o_messbody  out clob
                            ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    p_MarketID            number;
    p_CalcDate            date;
    p_ByStockMB           number;
    p_ByStockSPB          number;
    p_ByCurMB             number;
    p_ByFortsMB           number;
    p_ByEDP               number;
    p_UseListClients      number;
    v_servicenameList     varchar2(32600) := '';
    v_msgid               itt_q_message_log.msgid%type;
    v_count_pre_exec      pls_integer;
    vr_mess_log           itt_q_message_log%rowtype;
    vx_messmeta           xmltype;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_ByStockMB                        := vr_param_limit_start.ByStockMB;
    p_ByStockSPB                       := vr_param_limit_start.ByFortsMB;
    p_ByCurMB                          := vr_param_limit_start.ByCurMB;
    p_ByFortsMB                        := vr_param_limit_start.ByFortsMB;
    p_ByEDP                            := vr_param_limit_start.ByEDP;
    p_MarketID                         := vr_param_limit_start.MarketID;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    execute immediate 'select count(*) 
     from (select distinct to_number(EXTRACTVALUE(xmltype(l.messmeta), ''*/Create/MarketID'')) MarketID
             ,EXTRACTVALUE(xmltype(l.messmeta), ''*/FINISH/KeyService'') KeyService
           from itt_q_message_log l
          where l.queuetype = ''IN''
            and l.corrmsgid in (select msgid from table(it_q_message.select_answer_msg(p_msgid => :p_corrmsgid,p_queuetype => ''OUT'')))
            and l.servicename = :p_servicename )
           where MarketID is not null and KeyService is not null '
      into v_count_pre_exec
      using vr_param_limit_start.Msgid, g_qserv_CashFINISH;
    if v_count_pre_exec < vr_param_limit_start.MarketCount * gt_qservice_CashLimit.count
    then
      add_log(p_MarketID => null
             ,p_msg => 'CashFINISH Закончен расчет ' || v_count_pre_exec || ' из ' || vr_param_limit_start.MarketCount * gt_qservice_CashLimit.count);
      return;
    end if;
    vr_mess_log := it_q_message.messlog_get_withlock(p_msgid => vr_param_limit_start.Msgid);
    if vr_mess_log.log_id is null
    then
      add_log(p_MarketID => null, p_msg => 'CashFINISH Другим сервисом заблокировано СТАРТ сообщение ');
      return;
    end if;
    select count(*)
      into v_count_pre_exec
      from (select EXTRACTVALUE(xmltype(l.messmeta), '*/FINISH/KeyService') KeyService
              from itt_q_message_log l
             where l.queuetype = 'OUT'
               and l.corrmsgid in (select msgid from table(it_q_message.select_answer_msg(p_msgid => vr_param_limit_start.Msgid, p_queuetype => 'OUT')))
               and l.servicename = g_qserv_LimitFINISH)
     where KeyService = g_qserv_CashStockLimits;
    if v_count_pre_exec != 0
    then
      add_log(p_MarketID => null, p_msg => 'Повторный запуск CashFINISH');
      return;
    end if;
    CashFINISH(p_CalcDate, vr_param_limit_start.Msgid, vr_param_limit_create.calc_clientinfo, p_UseListClients); -- выносим для сохранения блокировки.
    add_log(p_MarketID => null, p_msg => 'Запуск SetLogErrContrTable ');
    RSHB_RSI_SCLIMIT.SetLogErrContr(p_calc_direct => vr_param_limit_start.Msgid
                                   ,p_CalcDate => p_CalcDate
                                   ,p_ByStockMB => p_ByStockMB
                                   ,p_ByStockSPB => p_ByStockSPB
                                   ,p_ByCurMB => p_ByCurMB
                                   ,p_ByFortsMB => p_ByFortsMB
                                   ,p_ByEDP => p_ByEDP
                                   ,p_MarketID => p_MarketID
                                   ,p_UseListClients => p_UseListClients);
    for cur in (select * from table(select_market(p_messmeta)))
    loop
      vx_messmeta := xml_insert_Create(p_messmeta => p_messmeta
                                      ,p_Msgid => it_q_message.get_sys_guid
                                      ,p_NPP => cur.NPP
                                      ,p_MarketID => cur.MarketID
                                      ,p_MarketCode => cur.MarketCode
                                      ,p_ByStock => cur.ByStock
                                      ,p_ByCurr => cur.ByCurr
                                      ,p_ByDeriv => cur.ByDeriv
                                      ,p_ByEDP => cur.ByEDP
                                      ,p_calc_panelcontr => vr_param_limit_start.calc_panelcontr
                                      ,p_calc_clientinfo => vr_param_limit_start.calc_clientinfo);
      qmanager_do_service_FINISH(p_Service => g_qserv_LimitFINISH, p_KeyService => g_qserv_CashStockLimits, p_messmeta => vx_messmeta);
    end loop;
    add_log(p_MarketID => null, p_msg => 'CashLimit_FINISH');
  end;

  -- Сервис Завершения расчета
  procedure Limit_FINISH(p_worklogid integer
                        ,p_messbody  clob
                        ,p_messmeta  xmltype
                        ,o_msgid     out varchar2
                        ,o_MSGCode   out integer
                        ,o_MSGText   out varchar2
                        ,o_messbody  out clob
                        ,o_messmeta  out xmltype) is
    vr_param_limit_start  tr_limit_start;
    vr_param_limit_create tr_limit_create;
    v_servicenameList     varchar2(32600) := '';
    v_msgid               itt_q_message_log.msgid%type;
    v_count_pre_exec      pls_integer;
    vr_mess_log           itt_q_message_log%rowtype;
    p_UseListClients      number;
    p_CalcDate            date;
  begin
    chk_sevice_calc_error(p_messmeta);
    vr_param_limit_start               := get_param_limit_start(p_messmeta);
    vr_param_limit_create              := get_param_limit_create(p_messmeta);
    p_CalcDate                         := vr_param_limit_start.CalcDate;
    p_UseListClients                   := vr_param_limit_start.UseListClients;
    RSHB_RSI_SCLIMIT.g_calc_DIRECT     := vr_param_limit_start.Msgid;
    RSHB_RSI_SCLIMIT.g_calc_clientinfo := vr_param_limit_create.calc_clientinfo;
    RSHB_RSI_SCLIMIT.g_calc_panelcontr := vr_param_limit_create.calc_panelcontr;
    execute immediate 'select count(*) 
              from (select distinct to_number(EXTRACTVALUE(xmltype(l.messmeta), ''*/Create/MarketID'')) MarketID
                      ,EXTRACTVALUE(xmltype(l.messmeta), ''*/FINISH/KeyService'') KeyService
                    from itt_q_message_log l
                   where l.queuetype = ''OUT''
                     and l.corrmsgid in (select msgid from table(it_q_message.select_answer_msg(p_msgid => :p_corrmsgid,p_queuetype => ''OUT'')))
                     and l.servicename = :p_servicename )
                    where MarketID is not null and KeyService is not null'
      into v_count_pre_exec
      using vr_param_limit_start.Msgid, g_qserv_LimitFINISH;
    if v_count_pre_exec < vr_param_limit_start.MarketCount * gt_qservice_Limit.count
    then
      add_log(p_MarketID => vr_param_limit_create.MarketID
             ,p_msg => 'LimitFINISH Закончен расчет ' || v_count_pre_exec || ' из ' || vr_param_limit_start.MarketCount * gt_qservice_Limit.count);
      return;
    end if;
    vr_mess_log := it_q_message.messlog_get_withlock(p_msgid => vr_param_limit_start.Msgid);
    if vr_mess_log.log_id is null
    then
      add_log(p_MarketID => vr_param_limit_create.MarketID, p_msg => 'LimitFINISH Другим сервисом заблокировано СТАРТ сообщение ');
      return;
    end if;
    if chk_finish_calc_direct(vr_param_limit_start.Msgid) != 0
    then
      add_log(p_MarketID => vr_param_limit_create.MarketID, p_msg => 'Повторный запуск Limit_FINISH');
      return;
    end if;
    if vr_param_limit_start.ByStockMB = 1
       or vr_param_limit_start.ByStockSPB = 1
    then
      RSHB_RSI_SCLIMIT.CheckWAPositionPrice(p_calc_direct => vr_param_limit_start.Msgid, p_CalcDate => p_CalcDate);
    end if;
    RSHB_RSI_SCLIMIT.SetLogItog(p_calc_direct => vr_param_limit_start.Msgid
                               ,p_CalcDate => p_CalcDate
                               ,p_ByStockMB => vr_param_limit_start.ByStockMB
                               ,p_ByStockSPB => vr_param_limit_start.ByStockSPB
                               ,p_ByCurMB => vr_param_limit_start.ByCurMB
                               ,p_ByFortsMB => vr_param_limit_start.ByFortsMB
                               ,p_ByEDP => vr_param_limit_start.ByEDP
                               ,p_MarketID => vr_param_limit_start.MarketID
                               ,p_UseListClients => vr_param_limit_start.UseListClients);
    CALCLIMITLOG(p_calc_direct => vr_param_limit_start.Msgid
                ,p_CalcDate => p_CalcDate
                ,p_group => 5000
                ,p_action => 999
                ,p_label => 'Расчет лимитов QUIK завершён'
                ,p_NPPmarket => 9
                ,p_dtstart => systimestamp);
    it_q_message.do_a_service(p_servicename => g_qserv_LimitCleaner, p_messmeta => p_messmeta, io_msgid => v_msgid);
    set_calc_status(p_calc_direct => vr_param_limit_start.Msgid, p_ErrorCode => 0);
    add_log(p_MarketID => vr_param_limit_create.MarketID, p_msg => 'Limit_FINISH');
  end;

  -- Сервис Очистки промежуточных данных
  procedure Limit_Cleaner(p_worklogid integer
                         ,p_messbody  clob
                         ,p_messmeta  xmltype
                         ,o_msgid     out varchar2
                         ,o_MSGCode   out integer
                         ,o_MSGText   out varchar2
                         ,o_messbody  out clob
                         ,o_messmeta  out xmltype) is
    vr_param_limit_start tr_limit_start;
    v_servicenameList    varchar2(32600) := '';
  begin
    vr_param_limit_start := get_param_limit_start(p_messmeta);
    for n in 1 .. gt_table_partition.count
    loop
      table_clear_partition(p_table => gt_table_partition(n), v_sid_calc => vr_param_limit_start.calc_clientinfo);
    end loop;
    table_clear_partition(p_table => 'DDL_PANELCONTR_DBT', v_sid_calc => vr_param_limit_start.calc_panelcontr);
    for p in (select * from table(select_parallel_sid(p_messmeta)))
    loop
      table_clear_partition(p_table => 'DDL_PANELCONTR_DBT', v_sid_calc => p.calc_panelcontr);
    end loop;
    add_log(p_MarketID => null, p_msg => 'Limit_Cleaner');
  end;

begin
  gt_table_partition(1) := 'DDL_CLIENTINFO_DBT';
  gt_table_partition(2) := 'D_LIMITLOTS_TMP';
  gt_table_partition(3) := 'DDL_FIID_DBT';
  gt_table_partition(4) := 'DDL_LIMITCOM_DBT';
  gt_table_partition(5) := 'DLIMIT_DLTICK_DBT';
  gt_table_partition(6) := 'DLIMIT_CMTICK_DBT';
  gt_table_partition(7) := 'DDL_LIMITCASHSTOCK_INT';
  --
  gt_qservice_CashLimit(1) := g_qserv_CashStockLimits;
  gt_qservice_CashLimit(2) := g_qserv_CashStockLimitsCur;
  gt_qservice_CashLimit(3) := g_qserv_CashEDPLimits;
  gt_qservice_CashLimit(4) := g_qserv_CashEDPLimitsCur;
  --
  gt_qservice_Limit(1) := g_qserv_CashStockLimits;
  gt_qservice_Limit(2) := g_qserv_SecurLimits;
  gt_qservice_Limit(3) := g_qserv_FutureMarkLimits;
  --
  gt_qservice_AllLimit(1) := g_qserv_CashStockLimits;
  gt_qservice_AllLimit(2) := g_qserv_CashStockLimitsCur;
  gt_qservice_AllLimit(3) := g_qserv_CashEDPLimits;
  gt_qservice_AllLimit(4) := g_qserv_CashEDPLimitsCur;
  gt_qservice_AllLimit(5) := g_qserv_SecurLimits;
  gt_qservice_AllLimit(6) := g_qserv_FutureMarkLimits;
end it_limit;
/
