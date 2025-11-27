create or replace package body it_xml is

  /**************************************************************************************************\
    BIQ-9225. Разработка очереди и журнала событий
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
    23.10.2023  Зыков М.В.       BOSS-1230                        BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
    15.05.2023  Зыков М.В.       CCBO-4870                        BIQ-13171. Разработка процедур работы с очередью событий
    18.08.2022  Зыков М.В.       BIQ-9225                         Добавление функции Clob_to_xml
    03.08.2022  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/
  -- Переводим в NUMBER то что пришло
  function char_to_number(p_snumber varchar2
                         ,p_decim   integer default null
                         ,p_messerr varchar2 default null) return number is
    v_c_format varchar2(100);
    v_snumber  varchar2(100) := translate(p_snumber, ', ', '.');
    v_n_res    number;
    vn_decim   integer;
    vn_pz      integer;
  begin
    if p_decim is null
    then
      vn_pz    := instr(v_snumber, '.');
      vn_decim := nvl(p_decim
                     ,case
                        when vn_pz = 0 then
                         0
                        else
                         length(v_snumber) - vn_pz
                      end);
    else
      vn_decim := p_decim;
    end if;
    v_c_format := lpad('9', (37 - vn_decim), '9') || '.' || lpad('9', vn_decim, '9');
    begin
      v_n_res := to_number(v_snumber, v_c_format);
    exception
      when others then
        raise_application_error(-20101
                               ,nvl(p_messerr
                                   ,'Ошибка перевода строки ' || p_snumber || ' в число до ' || vn_decim || ' знаков после запятой !'));
    end;
    return v_n_res;
  end;

  -- Переводим в Varchar2 из NUMBER
  function number_to_char(p_number  number
                         ,p_decim   integer default 2 -- Если < 0 '0' справа удаляются  
                         ,p_messerr varchar2 default null) return varchar2 is
    v_c_format varchar2(100);
    v_c_res    varchar2(40);
    v_n_res    number;
    vn_decim   integer ;
  begin
    if nvl(p_decim, 2) < 0 then
      vn_decim := abs(p_decim);  
      v_c_format := lpad('9', (36 - vn_decim), '9') || '0.' || lpad('9', vn_decim, '9');  
      v_c_res    := rtrim(rtrim(trim(to_char(p_number, v_c_format)),'0'),'.');
    else
      vn_decim := nvl(p_decim, 2);  
      v_c_format := lpad('9', (36 - vn_decim), '9') || '0.' || lpad('9', vn_decim, '9');  
      v_c_res    := trim(to_char(p_number, v_c_format));
    end if;
    if substr(v_c_res, 1, 1) = '#'
    then
      raise_application_error(-20102
                             ,nvl(p_messerr
                                 ,'Ошибка перевода числа ' || p_number || ' в формат до ' || vn_decim || ' знаков после запятой !'));
    else
      begin
        v_n_res := v_c_res;
      exception
        when others then
          v_n_res := replace(v_c_res, '.', ',');
      end;
      if v_n_res != p_number
      then
        raise_application_error(-20103
                               ,nvl(p_messerr
                                   ,'Ошибка перевода числа ' || p_number || ' в формат до ' || vn_decim || ' знаков после запятой !'));
      end if;
    end if;
    return v_c_res;
  exception
    when others then
      raise_application_error(-20103
                             ,nvl(p_messerr
                                 ,'Ошибка перевода числа ' || p_number || ' в формат до ' || vn_decim || ' знаков после запятой !'));
  end;

  -- Переводим дату в строку
  function date_to_char(p_date date) return varchar2 deterministic is
  begin
    return to_char(p_date, 'YYYY-MM-DD HH24:MI:SS');
  end;

  -- Переводим дату в формат ISO 8601
  function date_to_char_iso8601(p_date date) return varchar2 deterministic is
  begin
    return to_char(p_date, 'YYYY-MM-DD"T"HH24:MI:SS');
  end;

  -- Переводим строку  в дату
  function char_to_date(p_sdate varchar2) return date deterministic is
  begin
    return to_date(p_sdate, 'YYYY-MM-DD HH24:MI:SS');
  end;

  -- Переводим строку формата ISO 8601 в дату
  function char_to_date_iso8601(p_sdate varchar2) return date deterministic is
  begin
    return to_date(p_sdate, 'YYYY-MM-DD"T"HH24:MI:SS');
  end;

  -- Переводим TIMESTAMP в формат ISO 8601
  function timestamp_to_char_iso8601(p_timestamp timestamp) return varchar2 deterministic is
  begin
    return to_char(p_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.FF3');
  end;

  -- Переводим строку  в TIMESTAMP
  function char_to_timestamp(p_stimestamp varchar2) return timestamp deterministic is
  begin
    case
      when p_stimestamp like ('____-__-__') then
        return to_timestamp(p_stimestamp, 'YYYY-MM-DD');
      when p_stimestamp like ('____-__-__ %') then
        return to_timestamp(p_stimestamp, 'YYYY-MM-DD HH24:MI:SS.FF6');
      when p_stimestamp like ('____-__-__T%') then
        return to_timestamp(p_stimestamp, 'YYYY-MM-DD"T"HH24:MI:SS.FF6');
      else
        return to_timestamp(p_stimestamp);
    end case;
  end;

  -- Переводим строку формата ISO 8601 в TIMESTAMP
  function char_to_timestamp_iso8601(p_stimestamp varchar2) return timestamp deterministic is
  begin
    return to_timestamp(p_stimestamp, 'YYYY-MM-DD"T"HH24:MI:SS.FF3');
  end;

  -- Вычисление кол-ва миллисекунд между двумя метками
  function calc_interval_millisec(p_ts_start timestamp
                                 ,p_ts_stop  timestamp) return integer deterministic is
    pragma udf;
    ret integer;
  begin
    ret := (cast(p_ts_stop as date) - cast(p_ts_start as date)) * 86400000 /*24 * 60 * 60 * 1000*/
           + (mod(extract(second from p_ts_stop), 1) - mod(extract(second from p_ts_start), 1)) * 1000;
    return ret;
  end;

  -- Преобразование CLOB в XLMType
  function Clob_to_xml(p_clob     clob
                      ,p_errparam varchar2 default null) return xmltype is
    v_errtxt varchar2(2000) := 'Ошибка формата XML ' || p_errparam;
  begin
    if p_clob is null
       or dbms_lob.getlength(p_clob) = 0
    then
      return null;
    else
      return xmltype(p_clob);
    end if;
  exception
    when others then
      raise_application_error(-20104, v_errtxt);
  end;

 /* -- Преобразование CLOB в XLMType игнорировать namesapce
  function Clob_to_xml_delns(p_clob   clob  
                            ,p_errparam varchar2 default null) return xmltype is
    v_errtxt   varchar2(2000) := 'Ошибка преобразования XML ' || p_errparam;
    v_poz1     integer;
    v_poz2     integer;
    v_pattern1 varchar2(20) := ' xmlns="';
    v_pattern2 varchar2(20) := '"';
    v_clob clob;

  begin
    if p_clob is null
       or dbms_lob.getlength(p_clob) = 0
    then
      return null;
    else
      v_poz1 := dbms_lob.instr(lob_loc => p_clob, pattern => v_pattern1);
      v_poz2 := dbms_lob.instr(lob_loc => p_clob, pattern => v_pattern2, offset => v_poz1 + length(v_pattern1));
      if v_poz1 > 0
         and v_poz2 > v_poz1
      then
        dbms_lob.createtemporary(lob_loc => v_clob, cache => true);
        dbms_lob.copy(dest_lob    => v_clob,
                    src_lob     => p_clob,
                    amount      => v_poz1-1,
                    dest_offset => 1,
                    src_offset  => 1);
        dbms_lob.copy(dest_lob    => v_clob,
                    src_lob     => p_clob,
                    amount      => dbms_lob.getlength(p_clob)-v_poz2,
                    dest_offset => v_poz1,
                    src_offset  => v_poz2+1);
--        dbms_lob.fragment_delete(lob_loc => p_clob, amount => v_poz1, offset => v_poz2 - v_poz1 + 1);
      end if;
      return Clob_to_xml(v_clob, p_errparam);
    end if;
  exception
    when others then
      raise_application_error(-20105, v_errtxt);
  end;*/

  -- Преобразование XLMType в CLOB 
  function xml_to_Clob(p_xml xmltype) return clob as
  begin
    return case when p_xml is not null then p_xml.getClobVal end;
  end;

  -- Выделение из CLOB в Varchar2
  function Clob_to_str(p_clob clob
                      ,p_len  integer default 4000) return varchar2 is
    v_res      varchar2(32676);
    v_len_res  integer := greatest(3, least(nvl(p_len, 4000), 32676));
    v_len_clob integer;
  begin
    v_len_clob := dbms_lob.getlength(p_clob);
    if p_clob is null
       or v_len_clob = 0
    then
      v_res := null;
    elsif v_len_clob <= v_len_res
    then
      v_res := dbms_lob.substr(p_clob, 32676);
    else
      v_res := dbms_lob.substr(p_clob, v_len_res - 3) || '...';
    end if;
    return v_res;
  end;

  -- Преобразование CLOB в Varchar2
  function Clob_to_varchar2(p_clob    clob
                           ,p_messerr varchar2 default null) return varchar2 is
    v_res      varchar2(32676);
    v_len_clob integer;
  begin
    v_len_clob := dbms_lob.getlength(p_clob);
    if p_clob is null
       or v_len_clob = 0
    then
      v_res := null;
    elsif v_len_clob <= 32676
    then
      v_res := dbms_lob.substr(p_clob, 32676);
    else
      raise_application_error(-20106, nvl(p_messerr, 'Ошибка преобразования CLOB в Varchar2 !'));
    end if;
    return v_res;
  end;

  -- Получение куска из строки с разделителями
  function token_substr(p_source varchar2 -- где
                       ,p_delim  char -- разделитель
                       ,p_num    pls_integer -- № части
                        ) return varchar2 deterministic is
    v_aloc pls_integer;
    v_bloc pls_integer;
  begin
    v_aloc := instr(p_delim || p_source || p_delim, p_delim, 1, p_num);
    v_bloc := instr(p_delim || p_source || p_delim, p_delim, v_aloc + 1);
    return substr(p_source, v_aloc, v_bloc - v_aloc - 1);
  end;

  -- Кодирование спецсимволов
  function encode_spec_chr(p_source varchar2) return varchar2 deterministic as
    p_res varchar2(32676) := p_source;
    c_schar constant varchar2(5) := chr(9) || '&#x';
  begin
    for n in 0 .. 8
    loop
      p_res := replace(p_res, chr(n), c_schar || to_char(n));
    end loop;
    return p_res;
  end;

  function decode_spec_chr(p_source varchar2) return varchar2 deterministic as
    p_res varchar2(32676) := p_source;
    c_schar constant varchar2(5) := chr(9) || '&#x';
  begin
    if instr(p_res, c_schar) > 0
    then
      for n in 0 .. 8
      loop
        p_res := replace(p_res, c_schar || to_char(n), chr(n));
      end loop;
    end if;
    return p_res;
  end;

end;
/
