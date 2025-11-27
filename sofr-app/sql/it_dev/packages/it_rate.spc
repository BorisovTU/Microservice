create or replace package it_rate is
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
                     p_Oper          dratedef_dbt.t_oper%type default 0);
end it_rate;
/
