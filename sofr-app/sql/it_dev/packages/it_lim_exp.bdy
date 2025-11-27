create or replace package body it_lim_exp is

  /***************************************************************************************************\
   Формирование файла лимитов для выгрузки в QUIK
   **************************************************************************************************
    Изменения:
   ---------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание
   ----------  ---------------  ---------------------------   ----------------------------------------
   23.03.2025  Зыков М.В.       DEF-84801                     Файл лимитов не был загружен в QUIK при запуске торгов в выходные дни
   17.10.2023  Зыков М.В.       BOSS-358                      BIQ-13699.2. СОФР. Этап 2 - добавление файла ограничений по срочному рынку в обработку IPS
   14.08.2023  Зыков М.В.       DEF-49457                     BIQ-13050. Исключить частичное формирование данных для передачи в IPS
   22.06.2023  Зыков М.В.       DEF-46248                     BIQ-13050. Неверная выгрузка дробных бумаг в лимиты
   08.02.2023  Зыков М.В.       DEF-38832                     BIQ-13050. По таблице событий itt_file_save невозможно однозначно идентифицировать событие, присланное IPS
   07.02.2023  Зыков М.В.       DEF-38714                     BIQ-13050. Выгруженный файл лимитов содержит "пустую" строку (ошибка валидации в IPS)
   03.10.2022  Тютюнник С.С.    BIQ-11358                     DEF-31102 Убран параметр dur => dbms_lob.lob_readwrite
   05.09.2022  Зыков М.В.       BIQ-11358                     PRJ-2146 BIQ-11358 Добавление параметра LimitCount
   26.07.2022  Зыков М.В.       BIQ-11358                     PRJ-2146 BIQ-11358 Загрузка файла глобальных лимитов и выгрузка общего файла лимитов
   25.04.2022  Мелихова О.С.    BIQ-11358                     Создание
  \**************************************************************************************************/
  function add_txtmess(p_FILE_CODE varchar2
                      ,p_txtmess   varchar2
                      ,p_addmess   varchar2) return varchar2 as
  begin
    return p_txtmess || case when p_txtmess is not null then chr(13) || chr(10) end || case when p_FILE_CODE is not null then p_FILE_CODE || ' : ' else chr(13) || chr(10) end || p_addmess;
  end;

  -- Проверка состояния расчетов БА
  function chk_calc_BA(p_date         date
                      ,p_autp_type    number
                      ,p_autp_subtype number) return boolean as
    v_BA_date       date;
    v_BA_lastSTATUS dprocess_u_dbt.t_isfinish%type;
  begin
    v_BA_date := RSHB_RSI_SCLIMIT.GetCheckDateByParams(p_Kind => -1, p_Date => p_date, p_MarketID => -1, p_IsEDP => 1);
    it_log.log(p_msg => 'Check BA from ' || to_char(v_BA_date, 'dd.mm.yyyy') || ' p_autp_subtype = ' || p_autp_subtype);
    for cur in (select *
                  from dprocess_u_dbt t
                 where t.t_subtype = p_autp_subtype
                   and t.t_type = p_autp_type
                   and nvl(t.t_isfinish, 'N') not in ('S',chr(0))
                   and t.t_operdate >= v_BA_date
                 order by t_operdate)
    loop
      it_log.log(p_msg => 'Calc limit ' || to_char(cur.t_operdate, 'dd.mm.yyyy') || ' Status:' || cur.t_statusname);
      v_BA_lastSTATUS := cur.t_isfinish;
    end loop;
    return nvl(v_BA_lastSTATUS, 'N') = 'X';
  end;

  --формирование выгрузки
  function make_file_lim_quik(p_date    in date default trunc(sysdate)
                             ,o_txtmess out varchar2) return number is
    pragma autonomous_transaction;
    v_id_lim_exp_pack  number;
    v_clob             clob;
    v_clob_gl          clob;
    v_clob_gl_amount   integer;
    v_id_file          number;
    v_file_dir         varchar2(1000);
    v_file_name        varchar2(250);
    v_create_user      number;
    v_error_txt        varchar2(4000);
    v_date             date := trunc(p_date);
    v_file_code_gl     itt_file.file_code%type := it_file.C_FILE_CODE_LIMIT_GLOBAL;
    v_itt_lim_exp_pack itt_lim_exp_pack%rowtype;
    v_note             itt_file.note%type;
    v_count_line       integer := 0;
    v_count_line_gl    integer := 0;
    v_xml              xmltype;
    v_txtmess          varchar2(32000) := '';
  begin
    it_log.log('START p_date = ' || to_char(p_date, 'dd.mm.yyyy'));
    it_error.clear_error_stack;
    v_file_name                        := 'Export_' || it_file.c_file_code_quik || '_' || to_char(trunc(sysdate), 'yyyymmdd_hh24miss') || '.txt';
    v_create_user                      := 1;
    v_id_lim_exp_pack                  := its_main.nextval();
    v_itt_lim_exp_pack                 := null;
    v_itt_lim_exp_pack.create_sysdate  := sysdate;
    v_itt_lim_exp_pack.id_lim_exp_pack := v_id_lim_exp_pack;
    v_itt_lim_exp_pack.create_user     := 1;
    v_itt_lim_exp_pack.start_date      := sysdate;
    /*
    
    MONEY: FIRM_ID = NC0038900000; TAG = EQTS; CURR_CODE = SUR ; CLIENT_CODE = 004; OPEN_BALANCE = -46.52; OPEN_LIMIT = 0.00;
    MONEY: FIRM_ID = NC0038900000; TAG = EQTS; CURR_CODE = SUR ; CLIENT_CODE = 004; OPEN_BALANCE = -46.52; LEVERAGE = -1;
    MONEY: FIRM_ID = NC0038900000; TAG = EQTV; CURR_CODE = SUR ; CLIENT_CODE = 004; OPEN_BALANCE = 1048.51; OPEN_LIMIT = 450000.00; LIMIT_KIND = 1;
    DEPO: FIRM_ID = NC0038900000; SECCODE = HYDR; CLIENT_CODE =10004/10004; OPEN_BALANCE = 113955; OPEN_LIMIT = 0; TRDACCID = L01-00000F00;
    DEPO: FIRM_ID = NC0038900000; SECCODE = IRAO-004D; CLIENT_CODE =10006/10006; OPEN_BALANCE = 738467; OPEN_LIMIT = 0; TRDACCID = L01-00000F00; LIMIT_KIND = 1;
    
    */
    -- Формирование выгрузки
    it_log.log(p_msg => it_file.c_file_code_quik || ' create clob Begin ');
    dbms_lob.createtemporary(lob_loc => v_clob, cache => true); --DEF-31102 Убран параметр dur => dbms_lob.lob_readwrite
    -- Загрузка глобального лимита
    begin
      select f.file_clob
            ,f.note
        into v_clob_gl
            ,v_note
        from itt_file f
       where f.file_code = v_file_code_gl
         and rownum < 2
         and f.create_sysdate = (select max(f.create_sysdate) from itt_file f where f.file_code = v_file_code_gl);
      v_clob_gl_amount := dbms_lob.getlength(lob_loc => v_clob_gl);
      if v_clob_gl_amount > 2
      then
        dbms_lob.append(v_clob, v_clob_gl);
        begin
          v_xml := xmltype(v_note);
          select extractvalue(v_xml, '/XML/@LimitCount') into v_count_line from dual;
        exception
          when others then
            v_count_line := 0;
        end;
        if v_count_line = 0
        then
          v_count_line := regexp_count(v_clob_gl, chr(10));
        end if;
      end if;
    exception
      when no_data_found then
        null;
    end;
    it_log.log(p_msg => 'ADD Global LIM ' || v_count_line || ' line');
    v_count_line_gl := v_count_line;
    v_txtmess       := add_txtmess(it_file.c_file_code_quik
                                  ,v_txtmess
                                  ,'Добавление глобальных лимитов  ' || v_count_line || ' строк');
    if not chk_calc_BA(v_date, 1, 702)
    then
      it_log.log(p_msg => 'NO FINISH CALC LIMIT !');
      v_txtmess := add_txtmess(it_file.c_file_code_quik
                              ,v_txtmess
                              ,'Расчет лимитов в БА не завершен. Сохраняются только глобальные лимиты !');
    else
      for cCur in (select x.r
                         ,x.type_lim
                         ,x.client_code
                         ,x.limit_kind
                     from (select /*+ full(lc)*/
                            'MONEY:  FIRM_ID = ' || lc.T_FIRM_ID || ';' || ' TAG = ' || lc.T_TAG || ';' || ' CURR_CODE = ' || lc.T_CURR_CODE || ';' ||
                             ' CLIENT_CODE = ' || lc.T_CLIENT_CODE || ';' || ' LIMIT_KIND = ' || lc.T_LIMIT_KIND || ';' ||
                            -- ' OPEN_BALANCE = '    ||rtrim(to_char(lc.t_open_balance,'FM99999999999999999990D999999999999','NLS_NUMERIC_CHARACTERS = ''. '''),'.')||';'||
                             ' OPEN_BALANCE = ' ||
                             rtrim(to_char(lc.t_open_balance, 'FM99999999999999999990D00', ' NLS_NUMERIC_CHARACTERS = ''.,'' '), '.') || ';' ||
                            -- ' OPEN_LIMIT = '      ||rtrim(to_char(lc.t_open_limit,'FM99999999999999999990D00', ' NLS_NUMERIC_CHARACTERS = ''.,'' '),'.')||';'||
                            -- ' CURRENT_LIMIT = '   ||rtrim(to_char(lc.t_current_limit,'FM99999999999999999990D999999999999','NLS_NUMERIC_CHARACTERS = ''. '''),'.')||';'||
                            -- Атрибут OPEN_LIMIT для MONEY показываем 0.00
                             ' OPEN_LIMIT = 0.00' || ';' || case
                               when lc.t_leverage != -1 then
                                ' LEVERAGE = ' ||
                                rtrim(to_char(lc.t_leverage, 'FM99999999999999999990D999999999999', 'NLS_NUMERIC_CHARACTERS = ''. '''), '.') || ';'
                               else
                                null
                             end as r
                           ,'MONEY' as type_lim
                           ,lc.t_client_code as client_code
                           ,lc.t_limit_kind as limit_kind
                           ,lc.t_curr_code as fi
                             from DDL_LIMITCASHSTOCK_DBT lc
                            where lc.t_isblocked <> CHR(88)
                              and lc.t_date = v_date
                           union all
                           select /*+ full(ls)*/
                            'DEPO:  FIRM_ID = ' || ls.t_firm_id || ';' || ' SECCODE = ' || ls.t_seccode || ';' || ' CLIENT_CODE = ' || ls.t_client_code || ';' ||
                             ' LIMIT_KIND = ' || ls.t_limit_kind || ';' ||
                            --' OPEN_BALANCE = '     ||rtrim(to_char(ls.t_open_balance,'FM99999999999999999990D999999999999','NLS_NUMERIC_CHARACTERS = ''. '''),'.')||';'||
                             ' OPEN_BALANCE = ' || rtrim(trunc(ls.t_open_balance)) || ';' ||
                            --' OPEN_LIMIT = '       ||rtrim(to_char(ls.t_open_limit,'FM99999999999999999990D999999999999','NLS_NUMERIC_CHARACTERS = ''. '''),'.')||';'||
                            -- Атрибут OPEN_LIMIT для DEPO показываем 0
                             ' OPEN_LIMIT = 0' || ';' ||
                            --' WA_POSITION_PRICE = '||rtrim(to_char(ls.t_wa_position_price,'FM99999999999999999990D999999999999','NLS_NUMERIC_CHARACTERS = ''. '''),'.')||';'||
                             ' WA_POSITION_PRICE = ' ||
                             rtrim(to_char(ls.t_wa_position_price, 'FM99999999999999999990D000000', ' NLS_NUMERIC_CHARACTERS = ''.,'' '), '.') || ';' ||
                             ' TRDACCID = ' || ls.t_trdaccid || ';'/* || ' CURRENT_LIMIT = ' || -- DEF-67928
                             rtrim(to_char(ls.t_current_limit, 'FM99999999999999999990D000000', 'NLS_NUMERIC_CHARACTERS = ''.,'''), '.') || ';'*/ as r
                           ,'DEPO' as type_lim
                           ,ls.t_client_code as client_code
                           ,ls.t_limit_kind as limit_kind
                           ,ls.t_seccode as fi
                             from DDL_LIMITSECURITES_DBT ls
                            where ls.t_isblocked <> CHR(88)
                              and ls.t_date = v_date) x
                    order by type_lim desc
                            ,client_code
                            ,fi
                            ,limit_kind)
      loop
        dbms_lob.append(v_clob, regexp_replace(cCur.r, '[[:cntrl:]]+', '') || chr(13) || chr(10));
        v_count_line := v_count_line + 1;
      end loop;
      if v_count_line_gl = v_count_line
      then
        v_txtmess := add_txtmess(it_file.c_file_code_quik
                                ,v_txtmess
                                ,'Нет расчетных данных за ' || to_char(v_date, 'dd.mm.yyyy') || ' !');
      end if;
    end if;
    if dbms_lob.getlength(v_clob) > 2
    then
      dbms_lob.trim(v_clob, dbms_lob.getlength(v_clob) - 2); -- BIQ-11358 Удаляем последние chr(13)||chr(10))
    end if;
    it_log.log(p_msg => 'create clob End');
    select xmlelement("XML", xmlattributes(to_char(v_date, 'dd.mm.yyyy') as "LimitDate", v_count_line as "LimitCount")) into v_xml from dual;
    v_id_file := it_file.insert_file(p_file_dir => v_file_dir
                                    ,p_file_name => v_file_name
                                    ,p_file_clob => v_clob
                                    ,p_from_system => it_file.C_SOFR_DB
                                    ,p_from_module => $$plsql_unit
                                    ,p_to_system => it_file.C_QUIK
                                    ,p_to_module => null
                                    ,p_create_user => v_create_user
                                    ,p_file_code => it_file.c_file_code_quik
                                    ,p_note => v_xml.getStringVal);
    it_log.log('v_id_file=' || v_id_file);
    v_itt_lim_exp_pack.id_file  := v_id_file;
    v_itt_lim_exp_pack.end_date := sysdate;
    insert into itt_lim_exp_pack values v_itt_lim_exp_pack;
    --
    it_log.log(it_file.c_file_code_quik || ' Формирование выгрузки окончено');
    commit;
    v_txtmess := add_txtmess(it_file.c_file_code_quik
                            ,v_txtmess
                            ,' Формирование выгрузки окончено ! id_file= ' || v_id_file);
    o_txtmess := v_txtmess;
    return v_id_lim_exp_pack;
  exception
    when others then
      rollback;
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      v_error_txt                  := sqlerrm;
      v_itt_lim_exp_pack.error_txt := v_error_txt;
      insert into itt_lim_exp_pack values v_itt_lim_exp_pack;
      it_error.clear_error_stack;
      o_txtmess := it_file.c_file_code_quik || ' ОШИБКА Формирования выгрузки :' || v_error_txt;
      return null;
  end;

  --формирование выгрузки
  function make_file_lim_forts(p_date    in date default trunc(sysdate)
                              ,o_txtmess out varchar2) return number is
    pragma autonomous_transaction;
    v_id_lim_exp_pack  number;
    v_clob             clob;
    v_id_file          number;
    v_file_dir         varchar2(1000);
    v_file_name        varchar2(250);
    v_create_user      number;
    v_error_txt        varchar2(4000);
    v_date             date := trunc(p_date);
    v_itt_lim_exp_pack itt_lim_exp_pack%rowtype;
    v_count_line       integer := 0;
    v_xml              xmltype;
    v_txtmess          varchar2(32000) := '';
  begin
    it_log.log('START p_date = ' || to_char(p_date, 'dd.mm.yyyy'));
    it_error.clear_error_stack;
    v_file_name                        := 'FORTSAutoLim_' || to_char(trunc(sysdate), 'yyyymmdd_hh24miss') || '.fli';
    v_create_user                      := 1;
    v_id_lim_exp_pack                  := its_main.nextval();
    v_itt_lim_exp_pack                 := null;
    v_itt_lim_exp_pack.create_sysdate  := sysdate;
    v_itt_lim_exp_pack.id_lim_exp_pack := v_id_lim_exp_pack;
    v_itt_lim_exp_pack.create_user     := 1;
    v_itt_lim_exp_pack.start_date      := sysdate;
    -- Формирование выгрузки
    it_log.log(p_msg => it_file.C_FILE_CODE_LIMIT_FORTS || ' create clob Begin ');
    if not chk_calc_BA(v_date, 2, 711)
    then
      it_log.log(p_msg => 'NO FINISH CALC LIMIT !');
      o_txtmess := it_file.C_FILE_CODE_LIMIT_FORTS || ' ОШИБКА Расчет лимитов в БА не завершен ! ';
      return null;
    else
      dbms_lob.createtemporary(lob_loc => v_clob, cache => true);
      /*FUT_MONEY: FIRM_ID=SPBFUTF502;ACCOUNT=CLF502001;VOLUMEMN=591.79;VOLUMEPL=0.00;KFL=0.00;KGO=1.00;USE_KGO=N;
      T_ACCOUNT ACCOUNT CLF502001
      T_VOLUMEMN  VOLUMEMN  591.79
      T_VOLUMEPL  VOLUMEPL  0.00
      T_KFL KFL 0.00
      T_KGO KGO 1.00
      T_USE_KGO USE_KGO N
      T_FIRM_ID FIRM_ID SPBFUTF502*/
      for cCur in (select /*+ full(lc)*/
                    'FUT_MONEY:FIRM_ID=' || lc.T_FIRM_ID || ';' || 'ACCOUNT=' || lc.T_ACCOUNT || ';' || 'VOLUMEMN=' ||
                    to_char(lc.T_VOLUMEMN, 'FM99999999999999999990D00', 'NLS_NUMERIC_CHARACTERS = ''.,''') || ';' || 'VOLUMEPL=' ||
                    to_char(lc.T_VOLUMEPL, 'FM99999999999999999990D00', 'NLS_NUMERIC_CHARACTERS = ''.,''') || ';' || 'KFL=' ||
                    to_char(lc.T_KFL, 'FM99999999999999999990D00', 'NLS_NUMERIC_CHARACTERS = ''.,''') || ';' || 'KGO=' ||
                    to_char(lc.T_KGO, 'FM99999999999999999990D00', 'NLS_NUMERIC_CHARACTERS = ''.,''') || ';' || 'USE_KGO=' || lc.T_USE_KGO || ';' as r
                     from ddl_limitfuturmark_dbt lc
                    where t_isblocked <> chr(88)
                      and upper(t_ACCOUNT) like 'CLF5%'
                      and ROUND(T_VOLUMEMN, 4) <> 0
                      and lc.t_date = v_date)
      loop
        dbms_lob.append(v_clob, regexp_replace(cCur.r, '[[:cntrl:]]+', '') || chr(13) || chr(10));
        v_count_line := v_count_line + 1;
      end loop;
      if v_count_line = 0
      then
        v_txtmess := add_txtmess(it_file.C_FILE_CODE_LIMIT_FORTS
                                ,v_txtmess
                                ,'Нет расчетных данных за ' || to_char(v_date, 'dd.mm.yyyy') || ' !');
      end if;
      if dbms_lob.getlength(v_clob) > 2
      then
        dbms_lob.trim(v_clob, dbms_lob.getlength(v_clob) - 2);
      end if;
      it_log.log(p_msg => 'create clob End');
      select xmlelement("XML", xmlattributes(to_char(v_date, 'dd.mm.yyyy') as "LimitDate", v_count_line as "LimitCount")) into v_xml from dual;
      v_id_file := it_file.insert_file(p_file_dir => v_file_dir
                                      ,p_file_name => v_file_name
                                      ,p_file_clob => v_clob
                                      ,p_from_system => it_file.C_SOFR_DB
                                      ,p_from_module => $$plsql_unit
                                      ,p_to_system => it_file.C_QUIK
                                      ,p_to_module => null
                                      ,p_create_user => v_create_user
                                      ,p_file_code => it_file.C_FILE_CODE_LIMIT_FORTS
                                      ,p_note => v_xml.getStringVal);
      it_log.log('v_id_file=' || v_id_file);
      v_itt_lim_exp_pack.id_file  := v_id_file;
      v_itt_lim_exp_pack.end_date := sysdate;
      insert into itt_lim_exp_pack values v_itt_lim_exp_pack;
      --
      it_log.log(it_file.c_file_code_quik || ' Формирование выгрузки окончено');
      commit;
      v_txtmess := add_txtmess(it_file.C_FILE_CODE_LIMIT_FORTS
                              ,v_txtmess
                              ,' Формирование выгрузки окончено ! id_file= ' || v_id_file);
      o_txtmess := v_txtmess;
      return v_id_lim_exp_pack;
    end if;
  exception
    when others then
      rollback;
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      v_error_txt                  := sqlerrm;
      v_itt_lim_exp_pack.error_txt := v_error_txt;
      insert into itt_lim_exp_pack values v_itt_lim_exp_pack;
      it_error.clear_error_stack;
      commit;
      o_txtmess := it_file.C_FILE_CODE_LIMIT_FORTS || ' ОШИБКА Формирования выгрузки :' || v_error_txt;
      return null;
  end;

  --удаление файлов старше p_file_keep_day_cnt дней
  procedure delete_file_lim_quik(p_file_keep_day_cnt in number) is
  begin
    it_log.log('p_file_keep_day_cnt=' || p_file_keep_day_cnt);
    it_error.clear_error_stack;
    if p_file_keep_day_cnt != 0
    then
      for c1 in (select *
                   from (select t.id_lim_exp_pack
                               ,t.id_file
                               ,trunc(t.create_sysdate) dt
                               ,trunc(max(t.create_sysdate) over(partition by f.file_code)) last_create_day
                           from itt_lim_exp_pack t
                           join itt_file f
                             on t.id_file = f.id_file) x
                  where x.dt <= x.last_create_day - p_file_keep_day_cnt)
      loop
        delete from itt_file f where f.id_file = c1.id_file;
        --проставляем дату удаления файлов
        update itt_lim_exp_pack l
           set l.delete_date    = sysdate
              ,l.update_sysdate = sysdate
         where l.id_file = c1.id_file
           and delete_date is null;
      end loop;
    end if;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  --получениe clob-файла
  procedure get_last_file_lim_quik(p_date      in date
                                  ,p_file_code in varchar2
                                  ,p_file_clob out clob
                                  ,p_id_file   out number
                                  ,p_note      out varchar2) is
  begin
    it_log.log('START p_date = ' || to_char(p_date, 'dd.mm.yyyy'));
    it_error.clear_error_stack;
    p_file_clob := null;
    p_id_file   := null;
    select f.file_clob
          ,f.id_file
          ,f.note
      into p_file_clob
          ,p_id_file
          ,p_note
      from (select f.file_clob
                  ,f.id_file
                  ,f.note
                  ,row_number() over(partition by trunc(start_date) order by p.id_file desc) rn
              from itt_lim_exp_pack p
              join itt_file f
                on f.id_file = p.id_file
               and f.file_code = p_file_code
             where start_date between trunc(p_date) and trunc(p_date + 1) - 1 / 24 / 60 / 60
               and p.error_txt is null) f
     where f.rn = 1;
    it_log.log('END p_id_file ' || p_id_file);
  exception
    when no_data_found then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      p_file_clob := null;
      p_id_file   := null;
      it_error.clear_error_stack;
      it_log.log('END exception');
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      it_error.clear_error_stack;
      it_log.log('END exception');
  end;

  --установить атрибуты записи
  --  <XML file_dir="d:\quik" file_name="file_quik.txt id_file="123456"/>
  function ins_file_save(p_xml in clob) return number is
    v_xml          xmltype;
    v_id_file_save number;
    pragma autonomous_transaction;
    v_file_save_rec itt_file_save%rowtype;
  begin
    it_log.log('START p_xml = ' || substr(p_xml, 1, 3500));
    it_error.clear_error_stack;
    v_xml                          := xmltype(p_xml);
    v_id_file_save                 := its_main.nextval();
    v_file_save_rec                := null;
    v_file_save_rec.id_file_save   := v_id_file_save;
    v_file_save_rec.save_user      := 1;
    v_file_save_rec.save_user_name := null;
    v_file_save_rec.create_sysdate := sysdate;
    select extractvalue(v_xml, '/XML/@file_name')
          ,extractvalue(v_xml, '/XML/@file_dir')
          ,extractvalue(v_xml, '/XML/@id_file')
          ,extractvalue(v_xml, '/XML/@integration_id') -- DEF-38832      
      into v_file_save_rec.file_name
          ,v_file_save_rec.file_dir
          ,v_file_save_rec.id_file
          ,v_file_save_rec.integration_id
      from dual;
    v_file_save_rec.save_date := sysdate;
    --вставка записи в таблицу
    insert into itt_file_save values v_file_save_rec;
    it_log.log('END id_file_save = ' || v_id_file_save);
    commit;
    return v_id_file_save;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      it_log.log('END exception');
      raise;
  end;

  --формирование выгрузки
  function execute_process(p_date              in date default sysdate
                          ,p_file_keep_day_cnt in number
                          ,o_messtxt           out varchar2) return number is
    v_id_file_quik  number;
    v_id_file_forts number;
    v_messtxt       varchar2(32600);
    v_limitdate date;
    v_dateKind1 date := RSHB_RSI_SCLIMIT.GetCheckDateByParams(p_Kind => 1, p_Date => trunc(p_date), p_MarketID => -1, p_IsEDP => 1) ;
  begin
    if  RSHB_RSI_SCLIMIT.GetCheckDateByParams(p_Kind => -1, p_Date => v_dateKind1, p_MarketID => -1, p_IsEDP => 1) = trunc(p_date) then
      v_limitdate := trunc(p_date);
    else
      v_limitdate := v_dateKind1;
    end if;
    it_log.log('p_date=' || to_char(p_date, 'dd.mm.yyyy') ||' LimitDate=' || to_char(v_limitdate, 'dd.mm.yyyy') || ' p_file_keep_day_cnt =' || p_file_keep_day_cnt);
    v_id_file_quik  := make_file_lim_quik(trunc(v_limitdate), v_messtxt);
    o_messtxt       := add_txtmess(null, o_messtxt, v_messtxt);
    v_id_file_forts := make_file_lim_forts(trunc(v_limitdate), v_messtxt);
    o_messtxt       := add_txtmess(null, o_messtxt, v_messtxt);
    delete_file_lim_quik(p_file_keep_day_cnt);
    return case when v_id_file_quik is not null and v_id_file_forts is not null then 1 else 0 end;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      it_log.log('END exception');
      raise;
  end;

  -- Добавление файла глобальных лимитов
  procedure add_global_limit(p_file_dir  varchar2
                            ,p_file_name varchar2
                            ,p_file_gl   clob
                            ,o_state     out varchar2) as
    pragma autonomous_transaction;
    v_id_file     number;
    v_create_user number := 1;
    v_file_code   itt_file.file_code%type := it_file.c_file_code_limit_global;
    v_length_clob integer;
    v_cnt_line    integer;
    v_res_clob    clob;
    vx_param      xmltype;
  begin
    v_length_clob := dbms_lob.getlength(p_file_gl);
    if v_length_clob > 2
    then
      delete itt_file f where f.file_code = v_file_code;
      dbms_lob.createtemporary(lob_loc => v_res_clob, cache => true); --DEF-31102 Убран параметр dur => dbms_lob.lob_readwrite
      if dbms_lob.substr(p_file_gl, 2, v_length_clob - 1) != chr(13) || chr(10)
      then
        dbms_lob.append(v_res_clob, p_file_gl || chr(13) || chr(10));
      else
        dbms_lob.append(v_res_clob, p_file_gl);
      end if;
      v_cnt_line := regexp_count(v_res_clob, chr(10));
      select xmlelement("XML", xmlattributes(v_cnt_line as "LimitCount")) into vx_param from dual;
      v_id_file := it_file.insert_file(p_file_dir => p_file_dir
                                      ,p_file_name => p_file_name
                                      ,p_file_clob => v_res_clob
                                      ,p_from_system => it_file.C_QUIK
                                      ,p_from_module => $$plsql_unit
                                      ,p_to_system => it_file.C_SOFR_DB
                                      ,p_to_module => null
                                      ,p_create_user => v_create_user
                                      ,p_file_code => v_file_code
                                      ,p_note => vx_param.getStringVal);
      commit;
      o_state := 'Ок';
    else
      o_state := 'Файл ' || p_file_dir || p_file_name || ' НЕ ЗАГРУЖЕН ! Отсутствуют данные !';
    end if;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

end it_lim_exp;
/
