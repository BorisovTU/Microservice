create or replace package qb_dwh_export is
  -- Author  : k_guslyakov
  -- Created : 15.10.2018 8:47:39
  -- Purpose : Выгрузка межбанковских кредитов/депозитов в DWH
  --           СОФР осуществляет выгрузку каждой из таблиц как полный срез данных на момент выгрузки.
  -- Version : 20191204
  -- Добавление выгрузки хэджирования 04.03.2022 

  -- ID аттрибута Department - филиал выгрузки
  cAttrDepartment number := 1;
  -- ID аттрибута Rec_Status - Вид события над учетной копанентой
  cAttrRec_Status number := 2;
  -- ID аттрибута DT - дата выгрузки
  cAttrDT number := 3;
  -- Родительский аттрибут объеденяющий коллекцию сделок
  cDeals number := 4;
  -- ID сделки подлежащей выгрузке
  cDealID number := 5;
  -- Справочник на котором произошла ошибка
  cDict number := 6;

  cMBK number := 1; -- 0 - не выгружать 1 выгружать драгоценные металы
  cDateMBK date := nvl(qb_dwh_utils.GetDWHMigrationDate, to_date('01.03.2019', 'dd.mm.yyyy'));

  cPrecious_Metals     number := 1; -- 0 - не выгружать 1 выгружать драгоценные металы
  cDatePrecious_Metals date := to_date('01.04.2019', 'dd.mm.yyyy');

  cCSA     number := 1; -- 0 - не выгружать 1 выгружать
  cDateCSA date := to_date('01.04.2019', 'dd.mm.yyyy');

  cHEDG    number := 1; -- 0 - не выгружать 1 выгружать

  -----------------------
  --События
  -----------------------
  -- Экспорт данных о миграции МБК в DWH
  cEvent_EXPORT_MBK_MIGRATION number := 1;
  -- Выгрузка Генеральных соглашений МБК в DWH
  cEvent_EXPORT_MBK_GENAGR number := 2;
  -- Выгрузка кредитной линии/транша/сделки МБК в DWH
  cEvent_EXPORT_MBK_DEALS number := 3;
  -- Выгрузка Договоров обеспечения в DWH
  cEvent_EXPORT_MBK_ENS_CONTRACT number := 4;
  -- Выгрузка справочников
  cEvent_EXPORT_MBK_Dict number := 5;
  -- Выгрузка Драгоценных металов
  cEvent_Precious_Metals number := 6;
  -- Выгрузка CSA - Credit Support Agreement
  cEvent_EXPORT_CSA number := 7;
  -- Выгрузка хеджирования
  cEvent_EXPORT_HEDG number := 8;
  dateMigrCFT date := to_date('11012021','ddmmyyyy');

  type rec_carry is record (DockKind             number,         -- Вид Сделки
                            DealId               number,         -- ИД сделки
                            Deal_Code            varchar2(500),  -- Код Сделки
                            DWH_Deal_Code        varchar2(500),  -- Код сделки для DWH
                            Kind_Operation       number,         -- Вид операции
                            OperationID          number,         -- ИД операции
                            Stepname          varchar2(100),    -- Шаг операции
                            AcctrnID             number,         -- ИД проводки
                            ResultBisquitID    varchar2(4000), -- Результат поиска связей с проводками Бисквит
                            trn_Date             date,           -- Дата проводки
                            trn_Pack             number,         -- Пачка в проводке
                            trn_DopInfo          varchar2(4000), -- Доп информация в проводке
                            trn_PayerAccount     varchar2(50),   -- Дебет
                            trn_ReceiverAccount  varchar2(50),   -- Кредит
                            trn_Description      varchar2(4000), -- Основание
                            trn_Currency_Code    number,         -- Валюта
                            trn_Amount           number,         -- Сумма
                            trn_U4               varchar2(4000), -- UserField4 в проводке
                            trn_IsHalfCarry      number,         -- Тип проводки 0 - проводка, 1 - полупроводка
                            Dealpayment_Id      number,
                            Dealpayment_Amount  number(32,12),
                            Dealpayment_U4      varchar2(4000),
                            payment_Id          number,
                            payment_dockind     number,
                            payment_Amount      number(32,12),
                            payment_U4          varchar2(4000),
                            p322_ID              number,         -- ИД связанного (входящего) платежа
                            p322_Dockind              number,         --dockind связанного (входящего) платежа
                            p322_NumDoc          varchar2(100),  -- Номер связанного(входящего) платежа
                            p322_Pack            number,         -- Пачка связанного(входящего) платежа
                            p322_PayerAccount    varchar2(50),   -- дебет связанного(входящего) платежа
                            p322_ReceiverAccount varchar2(50),   -- кредит связанного(входящего) платежа
                            p322_Description     varchar2(4000), -- Основание связанного(входящего) платежа
                            p322_U4              varchar2(4000)  -- UserField4 в связанного(входящего) платеже

                            );
  type tab_Carry is table of rec_Carry;

  type arr_Carry is table of rec_Carry index by binary_integer;

  function Get_Carry (in_DocKind number default 0, in_DocID number default 0) return tab_Carry pipelined;
  function Get_DWH_Carry (in_DocKind number default 0, in_DocID number default 0) return tab_Carry pipelined;

  /** <font color=teal><b>Получит значение для смешения даты пдалтежа в не рабочии дни</b></font>
  *   @param in_DealId  ID сделки
  *   @param in_Purpose Назначение платежей (Погашение ОД/Погашение %% и т.д.)
  *   @return Признак расчетов только по рабочим дням:
              ?1? ? расчеты только в рабочие дни;
              ?0? ? расчеты в любой день.
  */
  function GetWorkDayDiff(in_DealId in number, in_Purpose number)
    return varchar2;

  ------------------------------------------------------------
  -- На основании события инициируем общие параметры выгрузки
  ------------------------------------------------------------
  procedure InitExportData(in_EventID       in number,
                           out_dwhRecStatus out varchar2,
                           out_dwhDT        out varchar2,
                           out_dwhSysMoment out varchar2,
                           out_dwhEXT_FILE  out varchar2,
                           in_version       in number := 1);

  ------------------------------------------------------
  -- Рекурсивная функция возвращает DealID первичной сделки для пролонгаций, если это не пролонгация то входящий DealId
  ------------------------------------------------------
  function GetFirstDealId(in_dealId            in number,
                          CurrentProlongedDeal number default 0 -- 0-вернет самую верхнюю сделку, 1 - Вернуть текшую пролангированную сделку
                          ) return number;

  /** <font color=teal><b>Процедура первичной выгрузки связей после миграции в ASS_DEAL_MIGRATION@LDR_INFA</b></font>*/
  procedure add_ASS_DEAL_MIGRATION(in_date in date);

  /** <font color=teal><b>Выгрузка Генеральных соглашений МБК в DWH</b></font>
  *   @param in_Department Подразделение выгрузки
  *   @param in_date       Операционная дата выгрузки
  */
  procedure export_GenAgr_Status_Add(in_UploadID in number,
                                     in_department in number,
                                     in_date       in date);

  /** <font color=teal><b>Выгрузка Кредитных линий/Траншей/Обычных сделок(кредиты/депозиты) МБК заключенных и пролонгированных</b></font>
  *   @param in_Department Подразделение выгрузки
  *   @param in_date       Операционная дата выгрузки
  */
  procedure export_Deals_Status_Add(in_UploadID in number,
                                    in_department in number,
                                    in_date       in date);

  /** <font color=teal><b>Выгрузка Договоров МБК в DWH</b></font>
  *   @param in_Department Подразделение выгрузки
  *   @param in_date       Операционная дата выгрузки
  */
  procedure export_Ens_Contract_Status_Add(in_UploadID in number,
                                           in_department in number,
                                           in_date       in date);
  ------------------------------------------------------
  -- Выгрузка Справочников
  ------------------------------------------------------
  procedure export_Dict_Status_Add(in_UploadID in number,
                                   in_department in number,
                                   in_date       in date);
  ------------------------------------------------------
  --Выгрузка драгоценных металов в DWH
  ------------------------------------------------------
  procedure export_Precious_Metals(in_DocKind      in number,
                                   in_DealId       in number,
                                   in_date         in date,
                                   in_dwhRecStatus in Varchar2,
                                   in_dwhDT        in Varchar2,
                                   in_dwhSysMoment in Varchar2,
                                   in_dwhEXT_FILE  in Varchar2) ;
  ------------------------------------------------------
  -- Выгрузка драг металов
  ------------------------------------------------------
  procedure export_Precious_Metals_Status_Add(in_UploadID in number,
                                              in_department in number,
                                              in_date       in date);

  ------------------------------------------------------
  -- Выгрузка CSA
  ------------------------------------------------------
  procedure export_CSA_Status_Add(in_UploadID in number,
                                  in_department in number,
                                  in_date       in date);
  ------------------------------------------------------
  -- Выгрузка хеджирования 
  ------------------------------------------------------
  procedure export_HEDG_Status_Add(in_UploadID in number,
                                  in_department in number,
                                  in_date       in date);
  /** <font color=teal><b>Процедура выгрузки всех объектов в @LDR_INFA</b></font>
  *   @param in_date Операционная дата события выгрузки
  */
  procedure export_all(in_date date,
                       in_id     in number default 0,
                       in_id_pre in number default 0);

end qb_dwh_export;
/
