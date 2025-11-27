create or replace package qb_dwh_utils is

  -- Author  : k_guslyakov
  -- Created : 11.10.2018 7:27:49
  -- Purpose : набор процедур облегчающих работу с выгрузкой в dwh
  -- Version : 20191029

  -- "0" - Событие, которое заключается в добавлении либо изменении компонента предметной области;
  REC_ADD        varchar2(1)  := '0';
  -- "1" - Событие, которое заключается в прекращении существования компонента предметной области;
  REC_CLOSED     varchar2(1)  := '1';
  -- "4" - техническое значение, необходимое для физического удаления (не путать с закрытием периода действия) строк по определенному компоненту предметной области;
  REC_DELETE     varchar2(1)  := '4';
  -- Код для подсистемы BISQUIT используемый в DWH для генерации кодов
  System_BISQUIT varchar2(10) := 'BISQUIT';
  -- Код для подсистемы "ЦФТ-БАНК. Розница" используемый в DWH для генерации кодов
  System_R2      varchar2(10) := 'R2XXXXX';
  -- Код для подсистемы IBSO используемый в DWH для генерации кодов
  System_IBSO    varchar2(10) := 'IBSOXXX';
  -- Код для подсистемы EBPZ используемый в DWH для генерации кодов
  System_EBPZ    varchar2(10) := 'EBPZXXX';
  -- Код для подсистемы RS_BANK используемый в DWH для генерации кодов
  System_RS      varchar2(10) := 'SOFRXXX';
  -- Неопределенная дата в прошлом
  DT_BEGIN       date         := to_date('01.01.1980','dd.mm.yyyy');
  -- Неопределенная дата в будущем
  DT_END         date         := to_date('01.01.3001','dd.mm.yyyy');
  --
  /** <font color=teal><b>Функция обработки на пустоту для начальных дат</b></font>
  *   @param in_Date входящий параметр дата
  *   @return Возвращает либо входящую дату либо значение соответствующее константе DT_BEGIN
  */
  function NvlBegDate (in_Date in Date) return date;

  /** <font color=teal><b>Функция обработки на пустоту для конечных дат</b></font>
  *   @param in_Date входящий параметр дата
  *   @return Возвращает либо входящую дату либо значение соответствующее константе DT_END
  */
  function NvlEndDate (in_Date in Date) return date;

  /** <font color=teal><b>Функция преобразования даты в строку перед записью в DWH</b></font>
  *   @param in_Date входящий параметр дата подлежащая преобразованию
  *   @return Возвращает строку в следующем формате:
  *           DD-MM-YYYY
  *           где DD - порядковый номер дня в месяце
  *               MM - порядковый номер месяца в году
  *               YYYY - порядковый номер  года
  *           Пример: 01-12-2010
  */
  function DateToChar (in_Date in Date) return varchar2 PARALLEL_ENABLE;

  /** <font color=teal><b>Функция преобразования даты в строку со временем перед записью в DWH</b></font>
  *   @param in_Date входящий параметр дата подлежащая преобразованию
  *   @return Возвращает строку в следующем формате:
  *           DD-MM-YYYY HH:MI:SS
  *           где DD-MM-YYYY - дата (см. описание функции DateToChar)
  *               HH - порядковый номер часа в дне
  *               MI - порядковый номер минуты в часе
  *               SS - порядковый номер секунды в минуте
  *           Пример: 01-12-2010 23:00:45
  */
  function DateTimeToChar (in_Date in Date) return varchar2 PARALLEL_ENABLE;


  /** <font color=teal><b>Функция преобразования числа в строку перед записью в DWH</b></font>
  *   @param in_Number Входящий параметр число подлежащее преобразованию
  *   @param in_Lingth Входящий параметр количество символов после запятой
  *   @param in_RoundType Входящий параметр Тип округления 1-Round/2-Trunc
  *   @return Возвращает преоразованное число в строку. Разделить бробной части точка(.), окруление и тип округления согласно входящих параметров
  */
  function NumberToChar (in_Number in number, in_Lingth in number default 4, in_RoundType in number default 1) return varchar2;

  /** <font color=teal><b>Функция Возвращает подразделение для сделки</b></font>
  *   @param in_Kind_DocID Входящий параметр Вид объекта
  *   @param in_DocID      Входящий параметр ID объекта
  *   @return Возвращает филиал сделки
  */
  function GetDepartmentByDealID(in_Kind_DocID number, in_DocID number) return number;

  /** <font color=teal><b>Функция Возвращает дату старта ЦХД из реестра</b></font>
  *   @return Возвращает дату старта ЦХД из реестра
  */
  function GetDWHMigrationDate return date;

  /** <font color=teal><b>Получить запись примечания</b></font>
  *   @param in_Obj_Tp  Входящий параметр Вид объекта
  *   @param in_Obj_Id  Входящий параметр ID объекта
  *   @param in_Note_Id Входящий параметр ID примечания
  *   @param in_Date    Входящий параметр дата действия примечания
  *   @return Возвращает строку примичания
  */
   FUNCTION Get_Note(in_Obj_Tp  in number,
                     in_Obj_Id  in varchar2,
                     in_Note_Id in Dnotetext_Dbt.t_Notekind%TYPE,
                     in_Date    in date default null)
      RETURN Dnotetext_Dbt%ROWTYPE;

  /** <font color=teal><b>Получить символьное значение примечания</b></font>
  *   @param in_Obj_Tp  Входящий параметр Вид объекта
  *   @param in_Obj_Id  Входящий параметр ID объекта
  *   @param in_Note_Id Входящий параметр ID примечания
  *   @param in_Date    Входящий параметр дата действия примечания
  *   @return Возвращает значение примичания тип данных varchar2
  */
   FUNCTION Get_Note_Chr(in_Obj_Tp  in number,
                         in_Obj_Id  in varchar2,
                         in_Note_Id in Dnotetext_Dbt.t_Notekind%TYPE,
                         in_Date    in date default null) RETURN VARCHAR2;

  /** <font color=teal><b>Получить цифровое значение примечания</b></font>
  *   @param in_Obj_Tp  Входящий параметр Вид объекта
  *   @param in_Obj_Id  Входящий параметр ID объекта
  *   @param in_Note_Id Входящий параметр ID примечания
  *   @param in_Date    Входящий параметр дата действия примечания
  *   @return Возвращает значение примичания тип данных NUMBER
  */
   FUNCTION Get_Note_Num(in_Obj_Tp  in number,
                         in_Obj_Id  in varchar2,
                         in_Note_Id in Dnotetext_Dbt.t_Notekind%TYPE,
                         in_Date    in date default null) RETURN NUMBER;

  /** <font color=teal><b>Получить значение даты-время примечания</b></font>
  *   @param in_Obj_Tp  Входящий параметр Вид объекта
  *   @param in_Obj_Id  Входящий параметр ID объекта
  *   @param in_Note_Id Входящий параметр ID примечания
  *   @param in_Date    Входящий параметр дата действия примечания
  *   @return Возвращает значение примичания тип данных date
  */
   FUNCTION Get_Note_Dat(in_Obj_Tp  in number,
                         in_Obj_Id  in varchar2,
                         in_Note_Id in Dnotetext_Dbt.t_Notekind%TYPE,
                         in_Date    in date default null) RETURN DATE;

  /** <font color=teal><b>Функция получения кода субъекта DWH на основании ID субъекта в RS-Bank.</b></font>
  *   @param in_ID Входящий параметр PartyID(ID субъекта экономики в RS-Bank)
  *   @return Возвращает код субъекта DWH(DET_SUBJECT.CODE_SUBJECT). Код берем из кода №101 субъекта экономики в RS.
  */
  function GetCODE_SUBJECT (in_ID in number) return varchar2;

  /** <font color=teal><b>Функция получения кода подразделения DWH на основании ID подразделения в RS-Bank.</b></font>
  *   @param in_ID Входящий параметр ID Субъекта в RS-Bank
  *   @return Возвращает код подразделения Банка, в котором зарегистрирован субъект сделки DWH(DET_DEPARTMENT.CODE_DEPARTMENT).
  */
  function GetCODE_DEPARTMENT (in_ID in number) return varchar2;

  /** <font color=teal><b>Функция получения кода финансового инструмента сделки DWH на основании DFININSTR.T_FIID в RS-Bank.</b></font>
  *   @param in_ID Входящий параметр ИД финансового инструмента DFININSTR.T_FIID
  *   @return Возвращает код типа сделки DWH(DET_FINSTR.FINSTR_CODE). ISO - код.
  *           Данная логика верна для валюты, если другие инструменты надо смотреть
  */
  function GetFINSTR_CODE (in_ID in number) return varchar2;

  /** <font color=teal><b>Функция получения код валюты DWH на основании DFININSTR.T_FIID в RS-Bank.</b></font>
  *   @param in_ID Входящий параметр ИД валюты DFININSTR.T_FIID
  *   @return Возвращает код типа сделки DWH(DET_CURRENCY.CURR_CODE_TXT).  код - USD/EUR и т.д..
  */
  function GetCURR_CODE_TXT (in_ID in number) return varchar2;

  /** <font color=teal><b>Функция возвращает сгенерированный ID компаненты для EXT_FILE. Используется в GetComponentCode</b></font>
  *   @param in_EventID Входящий параметр ИД события выгрузки
  *   @param in_DT Входящий параметр дата события выгрузки должно быть в формате yyyymmdd
  *   @param in_Rec_Status Входящий параметр тип события выгрузки, соответствует REC_STATUS
  *   @return возвращает сгенерированный ID компаненты для EXT_FILE. Используется в GetComponentCode
  */
  function GetEXT_FILE_ID (in_EventID in number, in_DT in varchar2, in_Rec_Status varchar2) return varchar2;

  /** <font color=teal><b>Функция Возвращает код субъекта согласно требованиям 2.0</b></font>
  *   @param codesum Входящий параметр кода субъекта
  *   @return Возвращает сгенерированный код субъкта согласно требованиям 2.0
  */
  function ModifyCodeSubject(codesub in varchar2) return varchar2;

  /** <font color=teal><b>Функция генератор кодов, идентифицирующих компоненты предметной области, затронутые учетным событием Требования 2.4</b></font>
  *   @param in_Component Входящий параметр Вид компоненты(DET_FINSTR, DET_CURRENCY и т.д.) позволяет определить уникальность генерации
  *   @param in_System Входящий параметр системы для которой происходит генерация, нужно использовать список констант, склеивается в ход
  *   @param in_department Входящий параметр подразделения RS-Bank, преобразуется в нужный вид при необходимости
  *   @param in_ComponentID Входящий параметр ID/Номер счета и т.д. в RS-Bank склеивается при генерации и принеобходимости преобразуется
  *   @return Возвращает сгенерированный код компаненты предметной области согласно требований 2,4
  */
  function GetComponentCode (in_Component in varchar2, in_System in varchar2, in_department in number, in_ComponentID in varchar2, in_dockind in number default 0) return varchar2;


  /** <font color=teal><b>Запись данных ASS_FCT_DEAL@LDR_INFA (Связь между сделками) </b></font>
  *   @param in_Parent_Code Код общих условий родительской (вышестоящей) сделки. ОДЗ: множество значений поля CODE таблицы FCT_DEAL.
  *   @param in_Child_Code  Код общих условий дочерней (подчиненной) сделки. ОДЗ: множество значений поля CODE таблицы FCT_DEAL.
  *   @param in_Type_Deal_Rel_Code Код типа отношения между сделками из таблицы DET_TYPE_DEAL_REL.CODE
  *   @param in_Rec_Status Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT Дата учетного события
  *   @param in_SysMoment Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */

  function Get_BKI_GUID (in_DocID in number, in_Kind_DocID number) return varchar2;


  procedure ins_ASS_FCT_DEAL (in_Parent_Code        in varchar2,
                              in_Child_Code         in varchar2,
                              in_Type_Deal_Rel_Code in varchar2,
                              in_Rec_Status         in varchar2,
                              in_DT                 in varchar2,
                              in_SysMoment          in varchar2,
                              in_Ext_File           in varchar2
                              );

  /** <font color=teal><b>Запись данных в FCT_DEAL@LDR_INFA (Общие условия по сделке) </b></font>
  *   @param in_Code            Код, идентифицирующий основные условия сделки
  *   @param in_Subject_Code    Код субъекта, с которым заключена сделка на условиях, которые отражены данной строкой. ОДЗ: множество значений поля CODE_SUBJECT таблицы DET_SUBJECT.
  *   @param in_Department_Code Код подразделения Банка, в котором зарегистрирован субъект, указанный в поле SUBJECT_CODE. ОДЗ: множество значений поля CODE_DEPARTMENT таблицы DET_DEPARTMENT.
  *   @param in_DealType        Тип сделки
  *   @param in_DocNum          Номер документа, на основании которого действуют условия сделки, отражаемые данной строкой
  *   @param in_Is_Interior     Признак внутренней сделки:
  *                             ?2? ? техническая сделка (используется для технических пролонгаций);
  *                             ?1? ? внутренняя сделка;
  *                             ?0? ? внешняя сделка.
  *   @param in_BeginDate       Дата начала исполнения по сделке
  *   @param in_EndDate         Дата окончания исполнения по сделке
  *   @param in_Note            Примечание
  *   @param in_Rec_Status      Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT              Дата учетного события
  *   @param in_SysMoment       Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File        Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_FCT_DEAL (in_Code            in varchar2,
                          in_Subject_Code    in varchar2,
                          in_Department_Code in varchar2,
                          in_DealType        in varchar2,
                          in_DocNum          in varchar2,
                          in_Is_Interior     in varchar2,
                          in_BeginDate       in varchar2,
                          in_EndDate         in varchar2,
                          in_Note            in varchar2,
                          in_Rec_Status      in varchar2,
                          in_DT              in varchar2,
                          in_SysMoment       in varchar2,
                          in_Ext_File        in varchar2);

  /** <font color=teal><b>Запись данных в FCT_MBKCREDEAL@LDR_INFA (Специфические условия по межбанковскому кредиту) </b></font>
  *   @param in_Deal_Code   Код общих условий сделки. ОДЗ: множество значений поля CODE таблицы FCT_DEAL.
  *   @param in_Finstr_Code Код финансового инструмента валюты сделки. ОДЗ: множество значений поля FINSTR_CODE таблицы DET_FINSTR.
  *   @param in_DealSum     Сумма межбанковского кредита. Точность значения: количество цифр в дробной части числа должно быть не более 3-х.
  *   @param in_DealTypeMBK Тип сделки МБК: 1 ? Обычный МБК;
  *   @param in_TypeSynd    Тип синдицированности: ?1? - Не синдицированный кредит.
  *   @param in_Rec_Status  Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT          Дата учетного события
  *   @param in_SysMoment   Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File    Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */

  procedure ins_FCT_MBKCREDEAL (in_Deal_Code   in varchar2,
                                in_Finstr_Code in varchar2,
                                in_DealSum     in varchar2,
                                in_DealTypeMBK in varchar2,
                                in_TypeSynd    in varchar2,
                                in_Rec_Status  in varchar2,
                                in_SysMoment   in varchar2,
                                in_Ext_File    in varchar2);

  /** <font color=teal><b>Запись данных в FCT_MBKDEPDEAL@LDR_INFA (Специфические условия по иежбанковскому депозиту) </b></font>
  *   @param in_Deal_Code   Код общих условий сделки. ОДЗ: множество значений поля CODE таблицы FCT_DEAL.
  *   @param in_Finstr_Code Код финансового инструмента валюты сделки. ОДЗ: множество значений поля FINSTR_CODE таблицы DET_FINSTR.
  *   @param in_DealSum     Сумма межбанковского депозита. Точность значения: количество цифр в дробной части числа должно быть не более 3-х.
  *   @param in_DealTypeMBK Тип сделки МБК: 1 ? Обычный МБК;
  *   @param in_Rec_Status  Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT          Дата учетного события
  *   @param in_SysMoment   Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File    Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */

  procedure ins_FCT_MBKDEPDEAL (in_Deal_Code   in varchar2,
                                in_Finstr_Code in varchar2,
                                in_DealSum     in varchar2,
                                in_DealTypeMBK in varchar2,
                                in_Rec_Status  in varchar2,
                                in_SysMoment   in varchar2,
                                in_Ext_File    in varchar2) ;

  /** <font color=teal><b>Запись данных в FCT_PROLONGATION@LDR_INFA (Пролонгация сделок) </b></font>
  *   @param in_Parent_Code      Код сделки-родителя (без усечения суффикса) ОДЗ: множество значений поля CODE таблицы FCT_DEAL.
  *   @param in_Child_Code       Код дочерней сделки (без усечения суффикса) ОДЗ: множество значений поля CODE таблицы FCT_DEAL.
  *   @param in_LongSum          Сумма пролонгации Точность значения: количество цифр в дробной части числа должно быть не более 3-х.
  *   @param in_longsum_Nat      Сумма в нац валюте Точность значения: количество цифр в дробной части числа должно быть не более 3-х.
  *   @param in_Code_Parent_Orig Оригинальный код сделки-родителя (без усечения суффикса)
  *   @param in_Code_Child_Orig  Оригинальный код дочерней сделки (без усечения суффикса)
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_FCT_PROLONGATION (in_Parent_Code in varchar2,
                                  in_Child_Code  in varchar2,
                                  in_LongSum     in varchar2,
                                  in_longsum_Nat in varchar2,
                                  in_Code_Parent_Orig in varchar2,
                                  in_Code_Child_Orig  in varchar2,
                                  in_Rec_Status  in varchar2,
                                  in_DT          in varchar2,
                                  in_SysMoment   in varchar2,
                                  in_Ext_File    in varchar2
                                  );




  /** <font color=teal><b>Запись данных в ASS_ACCOUNTDEAL@LDR_INFA (Связь сделки со счетом) </b></font>
  *   @param in_Account_Code
  *   @param in_Deal_Code
  *   @param in_RoleAccount_Deal_Code
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_ASS_ACCOUNTDEAL (in_Account_Code in varchar2,
                                 in_Deal_Code    in varchar2,
                                 in_RoleAccount_Deal_Code in varchar2,
                                 in_Rec_Status  in varchar2,
                                 in_DT          in varchar2,
                                 in_SysMoment   in varchar2,
                                 in_Ext_File    in varchar2,
                                 in_DT_END      in varchar2 default null -- KS 23.02.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                                  );


  /** <font color=teal><b>Функция возвращает роль лицевого счета в сделке для категории учета для DWH</b></font>
  *   @param in_CategoryID ID категории учета dmccateg_dbt.t_id
  *   @return Возвращает сгенерированный код DWH для категории учета
  */
  function GetRoleAccount_Deal_Code (in_CategoryID in number) return varchar2;


  /** <font color=teal><b>Процедура добавления связей ASS_ACCOUNTDEAL@LDR_INFA (Связь сделки со счетом) на основании привязанных к документу Категорий Учета </b></font>
  *   @param in_Kind_DocID  Входящий параметр. Вид документа к которому привязаны категории учета
  *   @param in_DocID       Входящий параметр. ID документа к которому привязаны категории учета
  *   @param in_Date        Дата последнего события с привязкой категории учета
  *   @param in_dwhDeal     Код сдежки используемый при генерации Кода сделки для DWH( IDсделки # Тип сделки)
  *   @param in_Rec_Status  Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT          Дата учетного события
  *   @param in_SysMoment   Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File    Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure add_ASS_ACCOUNTDEAL (in_Kind_DocID in number,
                                          in_DocID      in number,
                                          in_Date       in date,
                                          in_dwhDeal    in varchar2,
                                          in_Rec_Status in varchar2,
                                          in_DT         in varchar2,
                                          in_SysMoment  in varchar2,
                                          in_Ext_File   in varchar2
                                         );




  /** <font color=teal><b>Запись данных в FCT_DEALRISK@LDR_INFA (Риски по сделке) </b></font>
  *   @param in_Deal_Code
  *   @param in_RiskCat_Code_TypeRisk
  *   @param in_RiskCat_Code
  *   @param in_Reserve_Rate
  *   @param in_GROUND
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_FCT_DEALRISK (in_Deal_Code              in varchar2,
                              in_RiskCat_Code_TypeRisk  in varchar2,
                              in_RiskCat_Code           in varchar2,
                              in_Reserve_Rate           in varchar2,
                              in_GROUND                 in varchar2,
                              in_Rec_Status             in varchar2,
                              in_DT                     in varchar2,
                              in_SysMoment              in varchar2,
                              in_Ext_File               in varchar2
                             );

  /** <font color=teal><b>Запись данных в FCT_PROCRATE_DEAL@LDR_INFA (Ставка по сделке) </b></font>
  *   @param in_Deal_Code
  *   @param in_KindProcRate_Code
  *   @param in_SubKindProcRate_Code
  *   @param in_ProcBase_Code
  *   @param in_ProcRate
  *   @param in_ProcSum
  *   @param in_DT_Next_OverValue
  *   @param in_DT_Contract
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_FCT_PROCRATE_DEAL (in_Deal_Code            in varchar2,
                                   in_KindProcRate_Code    in varchar2,
                                   in_SubKindProcRate_Code in varchar2,
                                   in_ProcBase_Code        in varchar2,
                                   in_ProcRate             in varchar2,
                                   in_ProcSum              in varchar2,
                                   in_DT_Next_OverValue    in varchar2,
                                   in_DT_Contract          in varchar2,
                                   in_Rec_Status           in varchar2,
                                   in_DT                   in varchar2,
                                   in_SysMoment            in varchar2,
                                   in_Ext_File             in varchar2
                                  );

  /** <font color=teal><b>Запись данных в FCT_SUBJECT_ROLEDEAL@LDR_INFA (Ставка по сделке) </b></font>
  *   @param in_Deal_Code
  *   @param in_Subject_Code
  *   @param in_Role_Code
  *   @param in_Is_Agreement
  *   @param in_DT_Agreement
  *   @param in_BeginDate
  *   @param in_EndDate
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_FCT_SUBJECT_ROLEDEAL (in_Deal_Code    in varchar2,
                                      in_Subject_Code in varchar2,
                                      in_Role_Code    in varchar2,
                                      in_Is_Agreement in varchar2,
                                      in_DT_Agreement in varchar2,
                                      in_BeginDate    in varchar2,
                                      in_EndDate      in varchar2,
                                      in_Rec_Status   in varchar2,
                                      in_DT           in varchar2,
                                      in_SysMoment    in varchar2,
                                      in_Ext_File     in varchar2
                                      );

  --

  /** <font color=teal><b>Запись данных в ASS_DEAL_CAT_VAL@LDR_INFA (Связь сделки со значением ограниченного доп.атрибута) </b></font>
  *   @param in_Deal_Code
  *   @param in_Deal_Cat_Val_Code_Deal_Cat
  *   @param in_Deal_Cat_Val_CODE
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_ASS_DEAL_CAT_VAL (in_Deal_Code    in varchar2,
                                  in_Deal_Cat_Val_Code_Deal_Cat in varchar2,
                                  in_Deal_Cat_Val_CODE    in varchar2,
                                  in_Rec_Status   in varchar2,
                                  in_DT           in varchar2,
                                  in_SysMoment    in varchar2,
                                  in_Ext_File     in varchar2
                                 );

  ------------------------------------------------------
  -- Добавляем данные в ASS_DEAL_CAT_VAL@LDR_INFA (Связь сделки со значением ограниченного доп.атрибута)
  -- на основании категорий учета
  ------------------------------------------------------
  procedure add_ASS_DEAL_CAT_VAL (in_Kind_DocID in number,
                                  in_DocID      in number,
                                  in_Date       in date,
                                          in_dwhDeal    in varchar2,
                                  in_Rec_Status in varchar2,
                                  in_DT         in varchar2,
                                  in_SysMoment  in varchar2,
                                  in_Ext_File   in varchar2
                                 ) ;
  /** <font color=teal><b>Запись данных в FCT_DEAL_INDICATOR@LDR_INFA (Значение свободного доп.атрибута сделки) </b></font>
  *   @param in_Deal_Code
  *   @param in_Deal_ATTR_Code
  *   @param in_Measurement_Unit_Code
  *   @param in_Number_Value
  *   @param in_Date_Value
  *   @param in_String_Value
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_FCT_DEAL_INDICATOR (in_Deal_Code              in varchar2,
                                    in_Deal_ATTR_Code         in varchar2,
                                    in_Currency_Curr_Code_TXT in varchar2,
                                    in_Measurement_Unit_Code  in varchar2, -- Одно значение -1 Не определено
                                    in_Number_Value           in varchar2,
                                    in_Date_Value             in varchar2,
                                    in_String_Value           in varchar2,
                                    in_Rec_Status   in varchar2,
                                    in_DT           in varchar2,
                                    in_SysMoment    in varchar2,
                                    in_Ext_File     in varchar2
                                 );

  ------------------------------------------------------
  --Добавление данных в FCT_DEAL_INDICATOR@LDR_INFA (Значение свободного доп.атрибута сделки)
  -- На основании Примечаний по объекту
  ------------------------------------------------------
  procedure add_FCT_DEAL_INDICATOR (in_Kind_DocID in number,
                                    in_DocID      in number,
                                    in_Date       in date,
                                    in_dwhDeal    in varchar2,
                                    in_Rec_Status in varchar2,
                                    in_DT         in varchar2,
                                    in_SysMoment  in varchar2,
                                    in_Ext_File   in varchar2,
                                    in_CSA_TYPE   in number default 0
                                   ) ;
---------------------------------------
-- 4 очередь
--------------------------------------

  /** <font color=teal><b>Запись данных в ASS_CARRYDEAL@LDR_INFA (Связь проводки со сделкой) </b></font>
  *   @param in_Carry_Code
  *   @param in_Deal_Code
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_ASS_CARRYDEAL (in_Carry_Code   in varchar2,
                               in_Deal_Code    in varchar2,
                               in_Rec_Status   in varchar2,
                               in_DT           in varchar2,
                               in_SysMoment    in varchar2,
                               in_Ext_File     in varchar2
                              );

  procedure ins_ASS_HALFCARRYDEAL (in_HalfCarry_Code   in varchar2,
                               in_Deal_Code    in varchar2,
                               in_Rec_Status   in varchar2,
                               in_DT           in varchar2,
                               in_SysMoment    in varchar2,
                               in_Ext_File     in varchar2
                              );
  /** <font color=teal><b>Запись данных в FCT_DEAL_CARRY@LDR_INFA (Проводка сделочной модели) </b></font>
  *   @param in_Account_DBT_Code
  *   @param in_Account_CRD_Code
  *   @param in_Code
  *   @param in_docnum
  *   @param in_ground
  *   @param in_value
  *   @param in_value_nat
  *   @param in_info
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_FCT_DEAL_CARRY (in_Account_DBT_Code in varchar2,
                                in_Account_CRD_Code in varchar2,
                                in_Code             in varchar2,
                                in_docnum           in varchar2,
                                in_ground           in varchar2,
                                in_value            in varchar2,
                                in_value_nat        in varchar2,
                                in_info                in varchar2,
                                in_Rec_Status       in varchar2,
                                in_DT               in varchar2,
                                in_SysMoment        in varchar2,
                                in_Ext_File         in varchar2
                               );

  ------------------------------------------------------
  --Получение кода связи проводки и сделки на основании dpmpaym_dbt.t_purpose
  ------------------------------------------------------

  /** <font color=teal><b>Получение кода связи проводки и сделки на основании dpmpaym_dbt.t_purpose </b></font>
  *   @param in_Purpose входящий параметр dpmpaym_dbt.t_purpose
  *   @return Возвращает сгенерированный код для связи
  */
  function GetCode_Carry_Links(in_Purpose in number) return varchar2;
  /** <font color=teal><b>Запись данных в FCT_DM_CARRY_ASS@LDR_INFA (Связь сделки и проводки сделочной модели) </b></font>
  *   @param in_Code
  *   @param in_Deal_Carry_Code
  *   @param in_Deal_Code
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_FCT_DM_CARRY_ASS (in_Code            in varchar2,
                                  in_Deal_Carry_Code in varchar2,
                                  in_Deal_Code       in varchar2,
                                  in_Rec_Status      in varchar2,
                                  in_DT              in varchar2,
                                  in_SysMoment       in varchar2,
                                  in_Ext_File        in varchar2
                                 );

--FCT_DEAL_RST     Остатки и обороты в разрезе сделок и счетов

  /** <font color=teal><b>Запись данных в FCT_DEAL_RST@LDR_INFA (Остатки и обороты в разрезе сделок и счетов) </b></font>
  *   @param in_Deal_Code
  *   @param in_Account_Code
  *   @param in_Val_RST_ACC_IN
  *   @param in_Val_RST_CUR_IN
  *   @param in_Val_RST_NAT_IN
  *   @param in_Val_RST_RUR_IN
  *   @param in_Val_RST_AMT_IN
  *   @param in_Val_DBT_ACC
  *   @param in_Val_DBT_CUR
  *   @param in_Val_DBT_NAT
  *   @param in_Val_DBT_RUR
  *   @param in_Val_DBT_AMT
  *   @param in_Val_CRD_ACC
  *   @param in_Val_CRD_CUR
  *   @param in_Val_CRD_NAT
  *   @param in_Val_CRD_RUR
  *   @param in_Val_CRD_AMT
  *   @param in_Val_RST_ACC_OUT
  *   @param in_Val_RST_CUR_OUT
  *   @param in_Val_RST_NAT_OUT
  *   @param in_Val_RST_RUR_OUT
  *   @param in_Val_RST_RUR_OUT
  *   @param in_Val_RST_AMT_OUT
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_FCT_DEAL_RST (in_Deal_Code     in varchar2,
                              in_Account_Code in varchar2,
                              in_Val_RST_ACC_IN  in varchar2,
                              in_Val_RST_CUR_IN  in varchar2,
                              in_Val_RST_NAT_IN  in varchar2,
                              in_Val_RST_RUR_IN  in varchar2,
                              in_Val_RST_AMT_IN  in varchar2,
                              in_Val_DBT_ACC     in varchar2,
                              in_Val_DBT_CUR     in varchar2,
                              in_Val_DBT_NAT     in varchar2,
                              in_Val_DBT_RUR     in varchar2,
                              in_Val_DBT_AMT     in varchar2,
                              in_Val_CRD_ACC     in varchar2,
                              in_Val_CRD_CUR     in varchar2,
                              in_Val_CRD_NAT     in varchar2,
                              in_Val_CRD_RUR     in varchar2,
                              in_Val_CRD_AMT     in varchar2,
                              in_Val_RST_ACC_OUT  in varchar2,
                              in_Val_RST_CUR_OUT in varchar2,
                              in_Val_RST_NAT_OUT in varchar2,
                              in_Val_RST_RUR_OUT in varchar2,
                              in_Val_RST_AMT_OUT in varchar2,
                              in_Rec_Status      in varchar2,
                              in_DT              in varchar2,
                              in_SysMoment       in varchar2,
                              in_Ext_File        in varchar2
                             );

-- 5 очередь
--FCT_ATTR_SCHEDULE    Набор параметров планового графика

  /** <font color=teal><b>Запись данных в FCT_ATTR_SCHEDULE@LDR_INFA (Набор параметров планового графика) </b></font>
  *   @param in_Deal_Code
  *   @param in_TypeRePay_Code
  *   @param in_External_TypeRePay_Code
  *   @param in_TypeSCH
  *   @param in_Periodicity
  *   @param in_Count_Period
  *   @param in_Month_Pay
  *   @param in_Day_Pay
  *   @param in_Is_WorkDay
  *   @param in_Grace_Periodicity
  *   @param in_Grace_Count_Period
  *   @param in_Sum_RePay
  *   @param in_DT_Open_Per
  *   @param in_DT_Close_Per
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_FCT_ATTR_SCHEDULE (in_Deal_Code               in varchar2,
                                   in_TypeRePay_Code          in varchar2,
                                   in_External_TypeRePay_Code in varchar2,
                                   in_TypeSCH                 in varchar2,
                                   in_Periodicity             in varchar2,
                                   in_Count_Period            in varchar2,
                                   in_Month_Pay               in varchar2,
                                   in_Day_Pay                 in varchar2,
                                   in_Is_WorkDay              in varchar2,
                                   in_Grace_Periodicity       in varchar2,
                                   in_Grace_Count_Period      in varchar2,
                                   in_Sum_RePay               in varchar2,
                                   in_DT_Open_Per             in varchar2,
                                   in_DT_Close_Per            in varchar2,
                                   in_Rec_Status              in varchar2,
                                   in_DT                      in varchar2,
                                   in_SysMoment               in varchar2,
                                   in_Ext_File                in varchar2
                                  );
--FCT_REPAYSCHEDULE_DM    Строка планового графика ? таблица с неактуальными данными

  /** <font color=teal><b>Запись данных в FCT_REPAYSCHEDULE_DM@LDR_INFA (Строка планового графика ? таблица с неактуальными данными) </b></font>
  *   @param in_Deal_Code
  *   @param in_Code
  *   @param in_TypeSchedule
  *   @param in_TypeRepayCode
  *   @param in_MovingDirection
  *   @param in_FinStr_Code
  *   @param in_Event_Sum
  *   @param in_FinStrAmount
  *   @param in_DealSum
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_FCT_REPAYSCHEDULE_DM (in_Deal_Code       in varchar2,
                                      in_Code            in varchar2,
                                      in_Dt_Open         in varchar2,
                                      in_TypeSchedule    in varchar2,
                                      in_TypeRepay_Code   in varchar2,
                                      in_MovingDirection in varchar2,
                                      in_FinStr_Code     in varchar2,
                                      in_EventSum       in varchar2,
                                      in_FinStrAmount    in varchar2,
                                      in_DealSum         in varchar2,
                                      in_Rec_Status      in varchar2,
                                      --in_DT              in varchar2,
                                      in_SysMoment       in varchar2,
                                      in_Ext_File        in varchar2
                                      );
--FCT_REPAYSCHEDULE_H    Строка планового графика ? таблица с актуальными данными

  /** <font color=teal><b>Запись данных в FCT_REPAYSCHEDULE_H@LDR_INFA (Строка планового графика ? таблица с актуальными данными) </b></font>
  *   @param in_Deal_Code
  *   @param in_TypeRepayCode
  *   @param in_FinStr_Code
  *   @param in_Code
  *   @param in_PayDate
  *   @param in_TypeSchedule
  *   @param in_MovingDirection
  *   @param in_Event_Sum
  *   @param in_FinStrAmount
  *   @param in_DealSum
  *   @param in_Prev_Dt_Open
  *   @param in_Rec_Status       Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_FCT_REPAYSCHEDULE_H (in_Deal_Code        in varchar2,
                                     in_TypeRepay_Code   in varchar2,
                                     in_FinStr_Code      in varchar2,
                                     in_Code             in varchar2,
                                     in_Pay_Date          in varchar2,
                                     in_TypeSchedule     in varchar2,
                                     in_MovingDirection  in varchar2,
                                     in_EventSum        in varchar2,
                                     in_FinStrAmount     in varchar2,
                                     in_DealSum          in varchar2,
                                     in_Prev_DT_Open     in varchar2,
                                     in_Rec_Status       in varchar2,
                                     in_DT               in varchar2,
                                     in_SysMoment        in varchar2,
                                     in_Ext_File         in varchar2
                                     );


  -----------------------------------------------------
  --FCT_PROVISION_OBJECT (Объект залога)
  -----------------------------------------------------
  procedure ins_FCT_PROVISION_OBJECT (in_CODE        in varchar2,
                                      in_TypeProvision_Object_Code in varchar2,
                                      in_finstr_code   in varchar2,
                                      in_Amount        in varchar2,
                                      in_Balance_value in varchar2,
                                      in_Market_value  in varchar2,
                                      in_Note        in varchar2,
                                      in_Rec_Status  in varchar2,
                                      in_DT          in varchar2,
                                      in_SysMoment   in varchar2,
                                      in_Ext_File    in varchar2
                                     ) ;


  -----------------------------------------------------
  --FCT_PROVISION_OBJ_ATTR (Значение атрибута по объекту обеспечения)
  -----------------------------------------------------

  procedure ins_FCT_PROVISION_OBJ_ATTR (in_Object_Code in varchar2,
                                        in_Object_TypeAttr_Code in varchar2,
                                        in_FinSTR_Code in varchar2,
                                        in_Value       in varchar2,
                                        in_Rec_Status  in varchar2,
                                        in_DT          in varchar2,
                                        in_SysMoment   in varchar2,
                                        in_Ext_File    in varchar2
                                       ) ;

  -----------------------------------------------------
  --FCT_PROVISIONDEAL (Специфические условия по сделке обеспечения)
  -----------------------------------------------------
  procedure ins_FCT_PROVISIONDEAL (in_Deal_Code   in varchar2,
                                   in_FinSTR_Code in varchar2,
                                   in_ProvisionDeal_Type_Code in varchar2,
                                   in_Quality     in varchar2,
                                   in_Summa       in varchar2,
                                   in_Rec_Status  in varchar2,
                                   in_SysMoment   in varchar2,
                                   in_Ext_File    in varchar2
                                  ) ;



  -----------------------------------------------------
   --FCT_PROVISIONDEAL_CRED_OBJ (Связь объекта обеспечения со сделкой и с кредитной сделкой)
  -----------------------------------------------------

  procedure ins_FCT_PROVISIONDEAL_CRED_OBJ (in_ProvisionDeal_Code   in varchar2,
                                            in_CreditDeal_Code in varchar2,
                                            in_Provision_Sum in varchar2,
                                            in_Amount     in varchar2,
                                            in_Rec_Status  in varchar2,
                                            in_DT          in varchar2,
                                            in_SysMoment   in varchar2,
                                            in_Ext_File    in varchar2
                                           ) ;
  /** <font color=teal><b>Запись данных в ASS_DEAL_MIGRATION@LDR_INFA () </b></font>
  *   @param in_Deal_Cur_Code     - Текущий код сделки
  *   @param in_Deal_Prev_Code    - Предыдущий код сделки
  *   @param in_Department_Cur_Code  Текщий филиал сделки
  *   @param in_Department_Prev_Code Предыдущий филиал сделки
  *   @param in_Migration_Type    - Тип миграции заполняется числовым значением (сейчас: 1 ? миграция между филиалами, 2 ? миграция между АБС), в вопрос что вы туда должны вписывать, остается открытым (возможно добавят новый тип миграции, например, 3)
  *   @param in_Rec_Status        - Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT               Дата учетного события
  *   @param in_SysMoment        Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File         Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_ASS_DEAL_MIGRATION ( in_Deal_Cur_Code    in varchar2,
                                     in_Deal_Prev_Code   in varchar2,
                                     in_Department_Cur_Code  in varchar2,
                                     in_Department_Prev_Code in varchar2,
                                     in_Migration_Type   in varchar2,
                                     in_Rec_Status       in varchar2,
                                     in_DT               in varchar2,
                                     in_SysMoment        in varchar2,
                                     in_Ext_File         in varchar2);

  procedure clearAll(in_Type number default 0);

  /** <font color=teal><b>Запись данных в DET_DEAL_CAT (Ограниченный доп.атрибут сделки) </b></font>
  *   @param in_Code_Deal_Cat   - Код в верхнем регистре, идентифицирующий ограниченный доп.атрибут сделки, сформированный согласно пункту 2 требования 2.4.
  *   @param in_Name_Deal_Cat   - Наименование ограниченного доп.атрибута сделки
  *   @param in_is_MultiValued  - Признак многозначности. Если ?1?, то в один момент сделка может иметь несколько значений данного атрибута; если ?0? ? только одно.
  *   @param in_Rec_Status      - Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT              - Дата учетного события
  *   @param in_SysMoment       - Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File        - Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_DET_DEAL_CAT ( in_Code_Deal_Cat    in varchar2,
                               in_Name_Deal_Cat in varchar2,
                               in_is_MultiValued in varchar2,
                               in_Rec_Status       in varchar2,
                               in_DT               in varchar2,
                               in_SysMoment        in varchar2,
                               in_Ext_File         in varchar2);


  /** <font color=teal><b>Запись данных в DET_DEAL_CAT_VAL  (Значение ограниченного доп.атрибута сделки) </b></font>
  *   @param in_Deal_Code_Cat     - Код в верхнем регистре ограниченного доп.атрибута сделки ОДЗ: множество значений поля CODE_DEAL_CAT таблицы DET_DEAL_CAT.
  *   @param in_Code_Deal_Cat_Val - Код в верхнем регистре, идентифицирующий значение ограниченного доп.атрибута сделки, сформированный согласно пункту 2 требования 2.4.
  *   @param in_Name_Deal_Cat_Val - Значение ограниченного доп.атрибута сделки
  *   @param in_Rec_Status        - Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT                - Дата учетного события
  *   @param in_SysMoment         - Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File          - Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_DET_DEAL_CAT_VAL ( in_Deal_Cat_Code    in varchar2,
                                   in_Code_Deal_Cat_Val in varchar2,
                                   in_Name_Deal_Cat_Val  in varchar2,
                                   in_Rec_Status       in varchar2,
                                   in_DT               in varchar2,
                                   in_SysMoment        in varchar2,
                                   in_Ext_File         in varchar2);


  -----------------------------------------------------
  --Добавление ограниченного аттрибута и значений
  -----------------------------------------------------
  procedure add_DET_DEAL_CAT  (in_Object_Type in number,
                               in_Rec_Status  in varchar2,
                               in_DT          in varchar2,
                               in_SysMoment   in varchar2,
                               in_Ext_File    in varchar2);

  /** <font color=teal><b>Запись данных в DET_DEAL_TYPEATTR  (Свободный доп.атрибут по сделке) </b></font>
  *   @param in_Code     - Код в верхнем регистре свободного доп.атрибута сделки
  *   @param in_Name     - Наименование свободного доп.атрибута сделки
  *   @param in_Is_Money_Value - Признак стоимостного атрибута:
  *                              1 ? стоимостной;
  *                              0 ? натуральный (не стоимостной).
  *   @param in_Data_Type      - Тип данных:
  *                            1 ? число;
  *                            2 ? дата;
  *                            3 ? строка.
  *   @param in_Rec_Status        - Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT                - Дата учетного события
  *   @param in_SysMoment         - Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File          - Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_DET_DEAL_TYPEATTR ( in_Code           in varchar2,
                                    in_Name           in varchar2,
                                    in_Is_Money_Value in varchar2,
                                    in_Data_Type      in varchar2,
                                    in_Rec_Status     in varchar2,
                                    in_DT             in varchar2,
                                    in_SysMoment      in varchar2,
                                    in_Ext_File       in varchar2);

  -----------------------------------------------------
  --Заполнить справочник DET_DEAL_TYPEATTR  (Свободный доп.атрибут по сделке)
  -----------------------------------------------------
  procedure add_DET_DEAL_TYPEATTR ( in_Object_Type in number,
                                    in_Rec_Status     in varchar2,
                                    in_DT             in varchar2,
                                    in_SysMoment      in varchar2,
                                    in_Ext_File       in varchar2);
  /** <font color=teal><b>Запись данных в DET_DEPARTMENT  (Подразделение Банка) </b></font>
  *   @param in_Code_Department     - Код, идентифицирующий подразделение
  *   @param in_Name_Department     - Название подразделения
  *   @param in_Rank                - Приоритет (используется при загрузке субъектов).
  *   @param in_is_HaveBalance      - Признак наличия собственного баланса:
  *                                 ?1? ? подразделение имеет собственный баланс;
  *                                 ?0? ? не имеет.
  *   @param in_Currency_curr_code_txt - Код валюты ISO из таблицы DET_CURRENCY
  *   @param in_Rec_Status        - Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT                - Дата учетного события
  *   @param in_SysMoment         - Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File          - Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_DET_DEPARTMENT ( in_Code_Department        in varchar2,
                                 in_Name_Department        in varchar2,
                                 in_Rank                   in varchar2,
                                 in_is_HaveBalance         in varchar2,
                                 in_Currency_curr_code_txt in varchar2,
                                 in_Rec_Status             in varchar2,
                                 in_DT                     in varchar2,
                                 in_SysMoment              in varchar2,
                                 in_Ext_File               in varchar2);

  -----------------------------------------------------
  --Добавим DET_DEPARTMENT  (Подразделение Банка)
  -----------------------------------------------------
  procedure add_DET_DEPARTMENT ( in_Rec_Status             in varchar2,
                                 in_DT                     in varchar2,
                                 in_SysMoment              in varchar2,
                                 in_Ext_File               in varchar2);
  /** <font color=teal><b>Запись данных в DET_MEASUREMENT_UNIT  (Единица измерения) </b></font>
  *   @param in_Code       - Код, идентифицирующий единицу измерения
  *   @param in_Name       - Наименование единицы измерения
  *   @param in_Rec_Status - Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT         - Дата учетного события
  *   @param in_SysMoment  - Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File   - Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_DET_MEASUREMENT_UNIT ( in_Code        in varchar2,
                                       in_Name        in varchar2,
                                       in_Rec_Status  in varchar2,
                                       in_DT          in varchar2,
                                       in_SysMoment   in varchar2,
                                       in_Ext_File    in varchar2);

  -----------------------------------------------------
  --Добавим данные DET_MEASUREMENT_UNIT  (Единица измерения)
  -----------------------------------------------------
  procedure add_DET_MEASUREMENT_UNIT ( in_Rec_Status  in varchar2,
                                       in_DT          in varchar2,
                                       in_SysMoment   in varchar2,
                                       in_Ext_File    in varchar2);
  /** <font color=teal><b>Запись данных в DET_RISK  (Группа риска) </b></font>
  *   @param in_TypeRisk_Code - Код классификатора рисков из таблицы DET_TYPERISK
  *   @param in_Code_RiskCat  - Код, идентифицирующий группу риска в пределах классификатора, код которого указан в поле CODE_TYPERISK
  *   @param in_Name_RiskCat  - Наименование
  *   @param in_Min_Proc      - Минимальный процент резерва. Точность ? 4 знака.
  *   @param in_Max_Proc      - Максимальный процент резерва. Точность ? 4 знака.
  *   @param in_Rec_Status    - Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT            - Дата учетного события
  *   @param in_SysMoment     - Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File      - Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_DET_RISK ( in_TypeRisk_Code in varchar2,
                           in_Code_RiskCat  in varchar2,
                           in_Name_RiskCat  in varchar2,
                           in_Min_Proc      in varchar2,
                           in_Max_Proc      in varchar2,
                           in_Rec_Status    in varchar2,
                           in_DT            in varchar2,
                           in_SysMoment     in varchar2,
                           in_Ext_File      in varchar2);

  -----------------------------------------------------
  --Добавим в DET_RISK  (Группа риска)
  -----------------------------------------------------
  procedure add_DET_RISK ( in_Rec_Status    in varchar2,
                           in_DT            in varchar2,
                           in_SysMoment     in varchar2,
                           in_Ext_File      in varchar2);

  /** <font color=teal><b>Запись данных в DET_PROCBASE  (База расчета процентов) </b></font>
  *   @param in_Code           - Код
  *   @param in_Name           - Наименование
  *   @param in_Days_Year      - Количество дней в году
  *   @param in_Days_Month     - Количество дней в месяце
  *   @param in_Sign           - Учет 31 числа:
  *                              1 ? учитывать;
  *                              0 ? не учитывать.
  *   @param in_First_Day      - Учитывать первый день:
  *                              1 ? учитывать;
  *                              0 ? не учитывать.
  *                              В текущей реализации всегда = 1
  *   @param in_Last_Day       - Учитывать последний день:
  *                              1 ? учитывать;
  *                              0 ? не учитывать.
  *                              В текущей реализации всегда = 0
  *   @param in_Null_MainSum   - Учитывать проценты при нулевом ОД:
  *                              1 ? учитывать;
  *                              0 ? не учитывать.
  *   @param in_Rec_Status    - Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT            - Дата учетного события
  *   @param in_SysMoment     - Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File      - Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_DET_PROCBASE ( in_Code         in varchar2,
                               in_Name         in varchar2,
                               in_Days_Year    in varchar2,
                               in_Days_Month   in varchar2,
                               in_Sign_31      in varchar2,
                               in_First_Day    in varchar2,
                               in_Last_Day     in varchar2,
                               in_Null_MainSum in varchar2,
                               in_Rec_Status   in varchar2,
                               in_DT           in varchar2,
                               in_SysMoment    in varchar2,
                               in_Ext_File     in varchar2);

  -----------------------------------------------------
  --Запись данных в DET_PROCBASE  (База расчета процентов)
  -----------------------------------------------------
  procedure add_DET_PROCBASE ( in_Rec_Status   in varchar2,
                               in_DT           in varchar2,
                               in_SysMoment    in varchar2,
                               in_Ext_File     in varchar2);
  /** <font color=teal><b>Запись данных в DET_PROVISIONDEAL_TYPE  (Тип сделки обеспечения) </b></font>
  *   @param in_Code       - Код типа сделки обеспечения
  *   @param in_Name       - Наименование типа сделки обеспечения
  *   @param in_RSDH_Type  - Код типа сделки обеспечения, принятый в RSDH:
  *                          - ?1? ? Поручительство;
  *                          - ?2? ? Залог;
  *                          - ?3? ? Заклад;
  *                          - ?4? ? Гарантия.
  *   @param in_Rec_Status    - Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT            - Дата учетного события
  *   @param in_SysMoment     - Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File      - Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_DET_PROVISIONDEAL_TYPE ( in_Code       in varchar2,
                                         in_Name       in varchar2,
                                         in_RSDH_Type  in varchar2,
                                         in_Rec_Status in varchar2,
                                         in_DT         in varchar2,
                                         in_SysMoment  in varchar2,
                                         in_Ext_File   in varchar2);

  -----------------------------------------------------
  --Запись данных в DET_PROVISIONDEAL_TYPE  (Тип сделки обеспечения)
  -----------------------------------------------------
  procedure add_DET_PROVISIONDEAL_TYPE ( in_Rec_Status in varchar2,
                                         in_DT         in varchar2,
                                         in_SysMoment  in varchar2,
                                         in_Ext_File   in varchar2) ;
  /** <font color=teal><b>Запись данных в FCT_CREDITLINEDEAL  (Специфические условия по кредитной линии) </b></font>
  *   @param in_Deal_Code      - Код общих условий сделки ОДЗ: множество значений поля CODE таблицы FCT_DEAL.
  *   @param in_FinStr_Code    - Код финансового инструмента валюты сделки ОДЗ: множество значений поля FINSTR_CODE таблицы DET_FINSTR.
  *   @param in_Type_By_Limit  - Код типа сделки обеспечения, принятый в RSDH:
  *   @param in_Type_Contract  - Тип кредитной линии по лимиту:
  *                              0 ? Не определено
  *                              1 ? С лимитом по выдаче
  *                              2 ? С лимитом по задолженности
  *                              3 ? Комбинированная (лимиты по выдаче и по задолженности)
  *   @param in_PaymentLimit   -Величина лимита выдачи. Точность значения: количество цифр в дробной части числа должно быть не более 3-х.
  *   @param in_DebtLimit      - Величина лимита задолженности. Точность значения: количество цифр в дробной части числа должно быть не более 3-х.
  *   @param in_Rec_Status     - Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT             - Дата учетного события
  *   @param in_SysMoment      - Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File       - Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure ins_FCT_CREDITLINEDEAL ( in_Deal_Code     in varchar2,
                                     in_FinStr_Code   in varchar2,
                                     in_Type_By_Limit in varchar2,
                                     in_Type_Contract in varchar2,
                                     in_PaymentLimit  in varchar2,
                                     in_DebtLimit     in varchar2,
                                     in_Rec_Status    in varchar2,
                                     in_DT            in varchar2,
                                     in_SysMoment     in varchar2,
                                     in_Ext_File      in varchar2);


  procedure ins_DET_SUBJECT_ROLEDEAL ( in_Code           in varchar2,
                                       in_Name           in varchar2,
                                       in_Rsdh_Role_Code in varchar2,
                                       in_Rec_Status     in varchar2,
                                       in_DT             in varchar2,
                                       in_SysMoment      in varchar2,
                                       in_Ext_File       in varchar2);
  -----------------------------------------------------
  --Запишем DET_SUBJECT_ROLEDEAL
  -----------------------------------------------------
  procedure add_DET_SUBJECT_ROLEDEAL ( in_Rec_Status     in varchar2,
                                       in_DT             in varchar2,
                                       in_SysMoment      in varchar2,
                                       in_Ext_File       in varchar2);
  -----------------------------------------------------
  --Запись данных в DET_SYSTEM  ()
  -----------------------------------------------------
  procedure add_DET_SYSTEM ( in_Rec_Status     in varchar2,
                             in_DT             in varchar2,
                             in_SysMoment      in varchar2,
                             in_Ext_File       in varchar2 );
  -----------------------------------------------------
  --Запись данных в DET_ROLEACCOUNT_DEAL  ()
  -----------------------------------------------------
  procedure add_DET_ROLEACCOUNT_DEAL ( in_Rec_Status     in varchar2,
                             in_DT             in varchar2,
                             in_SysMoment      in varchar2,
                             in_Ext_File       in varchar2 ) ;


  -----------------------------------------------------
  --Запись данных в FCT_LC  (Аккредитивы)
  -----------------------------------------------------
  procedure ins_FCT_LC ( in_Deal_Code            in varchar2,
                         in_MovingDirection in varchar2,
                         in_TypeLC          in varchar2,
                         --in_SubTypeLC       in varchar2,
                         --in_TypeExecute     in varchar2,
                         in_AmountLC        in varchar2,
                         in_NumberLC        in varchar2,
                         in_Beneficiary_Code_Subject in varchar2,
                         in_Principal_Code_Subject   in varchar2,
                         in_Bank_beneficiary_Code_Subject in varchar2,
                         in_Bank_Principal_Code_Subject in varchar2,
                         in_FinStr_Code    in varchar2,
                         in_Rec_Status     in varchar2,
                         in_DT             in varchar2,
                         in_SysMoment      in varchar2,
                         in_Ext_File       in varchar2);
  -----------------------------------------------------
  --Запись данных в FCT_HEDG_CHG  (Хеджирование)
  -----------------------------------------------------
  procedure ins_FCT_HEDG_CHG (in_dt                       in varchar2,
                              in_code                     in varchar2, 
                              in_deal_code                in varchar2, 
                              in_finstr_code                in varchar2, 
                              in_asudr_deal_code          in varchar2, 
                              in_portfolio_code           in varchar2, 
                              in_sub_portf_code           in varchar2, 
                              in_currency_curr_code_txt   in varchar2, 
                              in_cost_on_date             in varchar2, 
                              in_prev_cost                in varchar2, 
                              in_chg_amount               in varchar2, 
                              in_deal_kind_code           in varchar2, 
                              in_hedge_rel_code           in varchar2, 
                              in_hedg_begin_dt            in varchar2, 
                              in_hedg_end_dt              in varchar2, 
                              in_hedg_tool_code           in varchar2,
                              in_tool_code_sofr           in varchar2,
                              in_inc_acc_code             in varchar2, 
                              in_dec_acc_code             in varchar2, 
                              in_inc_acc_num              in varchar2, 
                              in_dec_acc_num              in varchar2, 
                              in_rec_status               in varchar2, 
                              in_sysmoment                in varchar2, 
                              in_ext_file                 in varchar2);
  
  -----------------------------------------------------
  --Получить последний операционный дня
  -----------------------------------------------------
  function GetLastClosedOD (in_Department in number) return date;
  -----------------------------------------------------
  --Запись данных в в лог выгрузки с указанием филиала и последнего операционного дня
  -----------------------------------------------------
  procedure add_export_log ( in_id          in number,
                             in_id_pre      in number,
                             in_filcode     in varchar2,
                             in_datelastod  in date,
                             in_beg_date    in date,
                             in_end_date    in date )  ;
  -----------------------------------------------------
  --Получить t_userfield4 по номеру счета
  -----------------------------------------------------
  function GetAccountUF4(acc in daccount_dbt.t_account%type) return varchar2 DETERMINISTIC ;
  -----------------------------------------------------
  --Запись данных в в лог выгрузки с указанием филиала и последнего операционного дня (для выгрузки прочих ПФИ)
  -----------------------------------------------------
  procedure add_export_log_pfi ( in_id          in number,
                                 in_id_pre      in number,
                                 in_filcode     in varchar2,
                                 in_datelastod  in date,
                                 in_beg_date    in date,
                                 in_end_date    in date)  ;



end qb_dwh_utils;
/
