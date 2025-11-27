create or replace package body it_rate is
  /**************************************************************************************************\
    Работа с курсами СОФР 
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
    23.05.2022  Зыков М.В.                                        Создание 
  \**************************************************************************************************/

  /**
  *  Вставка курса
  *    p_BaseFIID       Код ФИ (Базовый ФИ)
  *    p_FIID           Код валюты котировки (FICK_...)
  *    p_RateType       Вид курса
  *    p_SinceDate      Дата установки курса
  *    p_MarketPlace    Торговая площадка
  *    p_MarketSection  Сектор торговой площадки
  *    p_Rate           Значение курса
  *    p_Scale          Масштаб курса
  *    p_Point          Количество значащих цифр
  *    p_IsRelative     Признак относительной котировки (облигации - true, для остальных - false)
  *    p_IsDominant     Признак основного курса false
  *    p_IsInverse      Признак обратной котировки false
  *    p_Oper           Номер пользователя
  */
  procedure add_rate(p_BaseFIID      dratedef_dbt.t_otherfi%type,
                     p_FIID          dratedef_dbt.t_fiid%type,
                     p_RateType      dratedef_dbt.t_type%type,
                     p_SinceDate     dratedef_dbt.t_sincedate%type,
                     p_Rate          dratedef_dbt.t_rate%type,
                     p_Scale         dratedef_dbt.t_scale%type,
                     p_Point         dratedef_dbt.t_point%type,
                     p_MarketPlace   dratedef_dbt.t_market_place%type,
                     p_MarketSection dratedef_dbt.t_section%type,
                     p_IsRelative    dratedef_dbt.t_isrelative%type default null,
                     p_IsDominant    dratedef_dbt.t_isdominant%type default chr(0),
                     p_IsInverse     dratedef_dbt.t_isinverse%type default chr(0),
                     p_Oper          dratedef_dbt.t_oper%type default 0)
  /* Шпаргалка:
        Торговая площадка = select * from dparty_dbt where t_partyid = (select t_market_place from Dratedef_Dbt where t_rateid = ВидКурса);
        Секция торговой площадки = select t_officeid from dptoffice_dbt where t_partyid = (select t_market_place from Dratedef_Dbt where t_rateid = ВидКурса) and t_officeid = (select t_section from Dratedef_Dbt where t_rateid = ВидКурса);
    */
  
   is
    Ratedef_Rec  Dratedef_Dbt%rowtype;
    Cnt          number;
    First_Save   number(1) := 0;
    l_IsRelative dratedef_dbt.T_ISRELATIVE%type;
    l_IsDominant dratedef_dbt.T_ISDOMINANT%type;
    l_IsInverse  dratedef_dbt.T_ISINVERSE%type;
    l_Oper       dratedef_dbt.t_oper%type := nvl(p_Oper, 0);
    v_err_add    varchar2(32000);
  begin
  
    -- Признак относительной котировки по умолчанию для облигации - true, для остальных - false
    if (p_IsRelative is null) then
      select case
               when exists (select 1
                              from (select t_avoirkind from dfininstr_dbt where t_fiid in (p_BaseFIID, p_FIID)) t1
                                  ,(select t_avoirkind
                                      from davrkinds_dbt
                                      where t_fi_kind = 2
                                        and t_numlist like '2%') t2
                              where t1.t_avoirkind = t2.t_avoirkind) then
                 chr(88)
               else
                 chr(0)
             end
        into l_IsRelative
        from dual;
    else
      l_IsRelative := p_IsRelative;
    end if;
  
    if ((p_IsDominant != chr(88) and p_IsDominant != chr(0)) or (p_IsDominant is null)) then
      l_IsDominant := chr(0);
    else
      l_IsDominant := p_IsDominant;
    end if;
  
    if ((p_IsInverse != chr(88) and p_IsInverse != chr(0)) or (p_IsInverse is null)) then
      l_IsInverse := chr(0);
    else
      l_IsInverse := p_IsInverse;
    end if;
  
    -- Проверка параметров
    v_err_add := ' курс типа ['||p_RateType||'] пары ' || p_FIID || '/' || p_BaseFIID ||
                 case
                   when p_MarketPlace = 0
                       and p_MarketSection = 0 then  
                     null
                   else
                     ' площадка [' || p_MarketPlace || '](' || p_MarketSection || ')'
                 end || ' за ' || to_char(p_SinceDate, 'dd.mm.yyyy');
    select count(*) into Cnt from dratetype_dbt where t_type = p_RateType;
    if Cnt = 0 then
      raise_application_error(-20001, 'Тип курса = ' || p_RateType || ' не найден' || v_err_add);
    end if;
    select count(*) into Cnt from dfininstr_dbt where t_fiid in (p_BaseFIID, p_FIID);
    if Cnt < 2 then
      raise_application_error(-20001, 'Ошибка параметра p_FIID/p_BaseFIID' || v_err_add);
    end if;
    if nvl(p_MarketPlace, -1) < 0 then
      raise_application_error(-20001, 'Ошибка параметра p_MarketPlace' || v_err_add);
    end if;
    if nvl(p_MarketSection, -1) < 0 then
      raise_application_error(-20001, 'Ошибка параметра p_MarketSection' || v_err_add);
    end if;
    if p_SinceDate is null then
      raise_application_error(-20001, 'Ошибка параметра p_SinceDate' || v_err_add);
    end if;
    if nvl(p_Scale, -1) <= 0 then
      raise_application_error(-20001, 'Ошибка параметра p_Scale' || v_err_add);
    end if;
    if nvl(p_Point, -1) <= 0 then
      raise_application_error(-20001, 'Ошибка параметра p_Point' || v_err_add);
    end if;
    if nvl(p_Rate, -1) < 0 then
      raise_application_error(-20001, 'Ошибка параметра p_Rate' || v_err_add);
    end if;
  
    -- блокируем строку для данной валюты и вида курса dratedef_dbt
    begin
      select *
        into Ratedef_Rec
        from Dratedef_Dbt Rd
       where Rd.t_Fiid = p_FIID
         and Rd.t_Otherfi = p_BaseFIID
         and Rd.t_Type = p_RateType
         and Rd.t_Market_Place = p_MarketPlace
         and Rd.t_Section = p_MarketSection
         for update nowait;
    exception
      when too_many_rows then
        raise_application_error(-20002, 'Множественный выбор курса' || v_err_add);
      when No_Data_Found then
        First_Save := 1;
      when others then
        raise_application_error(-20002, 'Ошибка совместного доступа ' || v_err_add);
    end;
  
    if (First_Save = 0) then
      if Ratedef_Rec.t_Sincedate > p_SinceDate then
        raise_application_error(-20003
                               ,'Курс уже установлен на ' || to_char(Ratedef_Rec.t_Sincedate, 'dd.mm.yyyy') || chr(10) || ' Ошибка для ' || v_err_add);
      end if;
    
      merge into Dratehist_Dbt h
      using (select Ratedef_Rec.t_Rateid    t_Rateid
                   ,Ratedef_Rec.t_Sincedate t_Sincedate
               from dual) n
      on (h.t_Rateid = n.t_Rateid and h.t_Sincedate = n.t_Sincedate)
      when matched then
        update
           set h.t_isinverse     = Ratedef_Rec.t_Isinverse
              ,h.t_Rate          = Ratedef_Rec.t_Rate
              ,h.t_scale         = Ratedef_Rec.t_Scale
              ,h.t_point         = Ratedef_Rec.t_Point
              ,h.t_inputdate     = Ratedef_Rec.t_Inputdate
              ,h.t_inputtime     = Ratedef_Rec.t_Inputtime
              ,h.t_oper          = Ratedef_Rec.t_Oper
              ,h.t_IsManualInput = CHR(0)
      when not matched then
        insert
          (h.t_rateid
          ,h.t_isinverse
          ,h.t_rate
          ,h.t_scale
          ,h.t_point
          ,h.t_inputdate
          ,h.t_inputtime
          ,h.t_oper
          ,h.t_sincedate
          ,h.t_ismanualinput)
        values
          (Ratedef_Rec.t_Rateid
          ,Ratedef_Rec.t_Isinverse
          ,Ratedef_Rec.t_Rate
          ,Ratedef_Rec.t_Scale
          ,Ratedef_Rec.t_Point
          ,Ratedef_Rec.t_Inputdate
          ,Ratedef_Rec.t_Inputtime
          ,Ratedef_Rec.t_Oper
          ,Ratedef_Rec.t_Sincedate
          ,Ratedef_Rec.t_Ismanualinput);
    
      update Dratedef_Dbt Rd
         set Rd.t_Rate          = p_Rate
            ,Rd.t_Scale         = p_Scale
            ,Rd.t_Point         = p_Point
            ,Rd.t_Inputdate     = Trunc(sysdate)
            ,Rd.t_Inputtime     = To_Date('01010001' || To_Char(sysdate, 'HH24MISS'), 'DDMMYYYYHH24MISS')
            ,Rd.t_Oper          = l_Oper
            ,Rd.t_Sincedate     = p_SinceDate
            ,Rd.t_IsManualInput = CHR(0)
       where Rd.t_Fiid = p_FIID
         and Rd.t_Otherfi = p_BaseFIID
         and Rd.t_Type = p_RateType
         and Rd.t_Market_Place = p_MarketPlace
         and Rd.t_Section = p_MarketSection;
    else
      -- вводим впервые
      insert into Dratedef_Dbt
        (t_Rateid
        ,t_Fiid
        ,t_Otherfi
        ,t_Name
        ,t_Definition
        ,t_Type
        ,t_IsDominant
        ,t_IsRelative
        ,t_Informator
        ,t_Market_Place
        ,t_IsInverse
        ,t_Rate
        ,t_Scale
        ,t_Point
        ,t_Inputdate
        ,t_Inputtime
        ,t_Oper
        ,t_Sincedate
        ,t_Section
        ,t_Version
        ,t_IsManualInput)
      values
        (0
        ,p_FIID
        ,p_BaseFIID
        ,Chr(1)
        ,Chr(1)
        ,p_RateType
        ,l_IsDominant
        ,l_IsRelative
        ,0
        ,p_MarketPlace
        ,l_IsInverse
        ,p_Rate
        ,p_Scale
        ,p_Point
        ,Trunc(sysdate)
        ,To_Date('01010001' || To_Char(sysdate, 'HH24MISS'), 'DDMMYYYYHH24MISS')
        ,l_Oper
        ,p_SinceDate
        ,p_MarketSection
        ,null
        ,CHR(0));
    end if;
  end add_rate;
  
end it_rate;
/
