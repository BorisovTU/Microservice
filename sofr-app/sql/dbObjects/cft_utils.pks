CREATE OR REPLACE PACKAGE cft_utils IS

 /**

  -- Author  : Велигжанин А.В.
  -- Created : 18.01.2024 10:15
  -- Purpose : процедуры (функции), которые используеются при сверке проводок между ЦФТ и СОФР

# changelog
 |date       |autor          |tasks                                           |note
 |-----------|---------------|------------------------------------------------|-------------------------------------------------------------
 |07.07.2025 |Велигжанин А.В.|DEF-94420                                       |CFT_GetSofr306OnDate(), получения проводок СОФР по 306-ым счетам за дату
 |27.05.2025 |Велигжанин А.В.|DEF-87040                                       |CFT_CheckLegal2(), для отбора проводок для ФЛ
 |10.03.2025 |Велигжанин А.В.|DEF-82701                                       |CFT_GetValidName(), функция для получения имени строчки до '//'
 |21.02.2025 |Велигжанин А.В.|DEF-82701                                       |CFT_CheckName(), функция проверки строк
 |12.11.2024 |Велигжанин А.В.|DEF-76032                                       |CFT_CheckLegal(), для отбора проводок для ФЛ
 |15.10.2024 |Велигжанин А.В.|BOSS-1266_BOSS-5751                             |CFT_CheckCorrespondence(), проверки для проводок по 306-ым
 |11.10.2024 |Велигжанин А.В.|BOSS-1266_BOSS-5723                             |CFT_CreateCompare306(), проводки по 306-ым счетам
 |18.01.2024 |Велигжанин А.В.|DEF-51036                                       |Создан

  */

  TYPE rec_matchA IS RECORD (
     SofrID             number         	-- ID СОФРа
     , CftID		number		-- ID ЦФТ
     , SofrDate         date		-- Дата проводки
     , Db               varchar2(25)    -- Счет по дебету
     , Cr               varchar2(25)    -- Счет по кредиту
  );
  TYPE tab_matchA IS TABLE OF rec_matchA;

  TYPE rec_matchB IS RECORD (
     SofrID             number         	-- ID СОФРа
     , CftID		number		-- ID ЦФТ
     , SofrDate         date		-- Дата проводки
     , SofrDb           varchar2(25)    -- Счет по дебету СОФР
     , SofrCr           varchar2(25)    -- Счет по кредиту СОФР
     , CftDb            varchar2(25)    -- Счет по дебету ЦФТ
     , CftCr            varchar2(25)    -- Счет по кредиту ЦФТ
  );
  TYPE tab_matchB IS TABLE OF rec_matchB;

  /** Табличная функция, возвращает результат сравнения проводок СОФР и ЦФТ,
      если полностью совпадают номера счетов
  */
  FUNCTION MatchSofrCftEnts (
    p_ReqID IN varchar2			-- номер запроса ЦФТ
    , p_StartDate IN date		-- начальная дата диапазона
    , p_EndDate IN date			-- конечная дата диапазона
  ) 
  RETURN tab_matchA pipelined;

  /** Табличная функция, возвращает результат сравнения проводок СОФР и ЦФТ,
      у которых совпадают балансовые счета и валюты, 
      а также балансовые счета соответствуют заданным маскам
  */
  FUNCTION MatchSofrCftEntsByMask (
    p_ReqID IN varchar2			-- номер запроса ЦФТ
    , p_StartDate IN date		-- начальная дата диапазона
    , p_EndDate IN date			-- конечная дата диапазона
    , p_MaskDb IN varchar2 DEFAULT '*'  -- маска для счета по дебету
    , p_MaskCr IN varchar2 DEFAULT '*'	-- маска для счета по кредиту
  ) 
  RETURN tab_matchB pipelined;

  /** Возвращает номер пачки для сопоставления проводок ЦФТ и СОФР
  */
  FUNCTION CFT_GetPersNumberBisquit (
    p_NumberPack IN number
    , p_Oper IN Number
    , p_Department IN number
    , p_DateCarry IN Date
  ) 
  RETURN varchar2;

  /** Возвращает код формы для счета: 1 - юр, 2 - физ, 0 - банк
  */
  FUNCTION CFT_ClientLegalForm (
    p_AccountID IN number
  ) 
  RETURN number;

  /** Проверяет, есть ли полученный счет в таблице dbrokacc_dbt. 
      Возвращает true, если есть. 
  */
  FUNCTION CFT_InDbrokacc( 
    p_Account IN varchar2
  )
  RETURN number deterministic;

  /** Сравнивает счета ЦФТ и СОФР. 
      Возвращает 1, если совпадают. 
      0, если не совпадают.
  */
  FUNCTION CFT_MatchAccount(
    p_CftAccount IN varchar2, p_CftBal IN varchar2, p_CftVal IN varchar2
    , p_SofrAccount IN varchar2, p_SofrFuture IN varchar2, p_SofrBal IN varchar2, p_SofrVal IN varchar2, p_SofrLegalForm IN number
  )
  RETURN number;

  /** Сравнивает суммы проводок ЦФТ и СОФР. 
      Возвращает 1, если совпадают. 
      0, если не совпадают.
  */
  FUNCTION CFT_MatchSum(
    p_CftSum IN number, p_SofrSum IN number
  )
  RETURN number DETERMINISTIC;

  /** Сравнивает даты проводок ЦФТ и СОФР. 
      Возвращает 1, если совпадают. 
      0, если не совпадают.
  */
  FUNCTION CFT_MatchDate(
    p_CftOpDate IN date, p_CftDocDate IN date, p_SofrDate IN date
  )
  RETURN number DETERMINISTIC;

  /** Сравнивает основание проводок ЦФТ и СОФР. 
      Возвращает 1, если совпадают. 
      0, если не совпадают.
  */
  FUNCTION CFT_MatchText(
    p_CftText IN varchar2, p_SofrText IN varchar2, p_Flag IN number DEFAULT 0
  )
  RETURN number DETERMINISTIC;

  /** Возвращает наименование клиента счета
  */
  FUNCTION CFT_GetAccountPartyName(p_Account IN VARCHAR2)
     RETURN dparty_dbt.t_name%type DETERMINISTIC;

  /** Функция изменения счета по дебету для сводных счетов
  */
  FUNCTION CFT_UpdateSyncPayer RETURN number;

  /** Функция изменения счета по крудиту для сводных счетов
  */
  FUNCTION CFT_UpdateSyncReceiver RETURN number;

  /** Функция получения проводок СОФР за дату. 
      Возвращает кол-во записей.
  */
  FUNCTION CFT_GetSofrOnDate(
    p_ReqStart IN date, p_ReqEnd IN date, p_UseParam IN NUMBER DEFAULT 0
  )
  RETURN number;

  /** Функция получения проводок СОФР по 306-ым счетам за дату. 
      Возвращает кол-во записей.
  */
  FUNCTION CFT_GetSofr306OnDate(
    p_ReqStart IN date, p_ReqEnd IN date
  )
  RETURN number;

  /** Функция создает записи сравнения проводок ЦФТ и СОФР. 
      Возвращает кол-во записей.
  */
  FUNCTION CFT_CreateCompare(
    p_ReqID IN varchar2, p_ReqDate IN date
  )
  RETURN number;

  /** BOSS-1266_BOSS-5723
      Функция создает записи сравнения зачисления-списания ДС ЦФТ и СОФР. 
      Проводки по 306-ым счетам.
      Возвращает кол-во записей.
  */
  FUNCTION CFT_CreateCompare306(
    p_ReqID IN varchar2, p_ReqStart IN date, p_ReqEnd IN date
  )
  RETURN number;

  /** BOSS-1266_BOSS-5723
      Функция получает условие доп.фильтра на выборку проводок из СОФР. 
      По данным файла uEntCompareParam_tmp
      Если p_Mode = 0, то возвращается полное условие
      Если p_Mode = 1, то возвращается условие для маски счетов дебета
      Если p_Mode = 2, то возвращается условие для маски счетов кредита
      Если p_Mode = 3, то возвращается условие по параметрам t_eq_debit, t_non_debit, t_eq_credit, t_non_creedit
  */
  FUNCTION CFT_GetExtCondition( p_Mode IN number, p_Part IN number DEFAULT 1 ) RETURN VARCHAR2;

  /** Процедура для добавления записей в темперную таблицу для фильтрации данных отчета-сверки проводок ЦФТ и СОФР. 
      При добавлении p_Id меньше 1, предварительно будут удаляться предыдущие данные
  */
  PROCEDURE CFT_AddEntCompareParam (
    p_Id IN number
    , p_PersN IN varchar2 DEFAULT ''
    , p_AccD IN varchar2 DEFAULT ''
    , p_AccC IN varchar2 DEFAULT ''
  );

  /** BOSS-1266_BOSS-5723
      Процедура для добавления записей в темперную таблицу для фильтрации данных отчета-сверки проводок ЦФТ и СОФР. 
      При добавлении p_Id меньше 1, предварительно будут удаляться предыдущие данные
  */
  PROCEDURE CFT_AddEntCompareParam1 (
    p_Id IN number
    , p_EqDebit IN varchar2 DEFAULT ''
    , p_NonDebit IN varchar2 DEFAULT ''
    , p_EqCredit IN varchar2 DEFAULT ''
    , p_NonCredit IN varchar2 DEFAULT ''
  );

  /** BOSS-1266_BOSS-5751
      Функция проверки корректности корреспонденции
      Если корреспонденция счетов идет в соответствии с резиденством (Д 30601 К 40817, Д 30606 К 40820 и наоборот), тогда  "успешно".
  */
  FUNCTION CFT_CheckCorrespondence(p_Debit IN VARCHAR2, p_Credit IN VARCHAR2)
     RETURN number;

  /** DEF-82701
      Возвращает валидное имя строки (первую часть до '//')
  */
  FUNCTION CFT_GetValidName(p_Name IN VARCHAR2)
     RETURN varchar2;

  /** DEF-82701
      Возвращает 1, если наименования совпадают.
  */
  FUNCTION CFT_CheckName(p_CftName IN VARCHAR2, p_SofrName IN VARCHAR2)
     RETURN number;

  /** DEF-76032
      Возвращает 2, если полученный счет является счетом ФЛ.
  */
  FUNCTION CFT_CheckLegal(p_Account IN VARCHAR2)
     RETURN number;

  FUNCTION CFT_CheckLegal2(p_Account IN VARCHAR2, p_Account2 IN VARCHAR2)
     RETURN number;

  /** Функция определения ID выгрузки отчета-сверки проводок.
      Если получен параметр, то возвращается он.
      Если параметр не задан, он определяется по последней выгрузке.
  */
  FUNCTION CFT_GetReqID (p_ReqID IN VARCHAR2 DEFAULT null) RETURN varchar2;

  /** Функция определения даты отчета-сверки проводок
  */
  FUNCTION CFT_GetStartDate (p_ReqID IN VARCHAR2, p_StartDate IN DATE DEFAULT null) RETURN date;

  /** Функция собирает данные по последней выгрузке сравнения проводок СОФР и ЦФТ (если p_ReqID is null).
      Или собирает данные по выгрузке с p_ReqID (если не null)
      Используется для функционала авто-тестирования
  */
  FUNCTION PrepareEntCompareTestTmp(
    p_ReqID IN VARCHAR2 DEFAULT null
    , p_StartDate IN DATE DEFAULT null
  )
  RETURN number;

  /** Если возможно, функция возвратит синтетический счет для полученного аналитического
  */
  FUNCTION CFT_getSyncAcc ( 
    p_TestAcc IN varchar2, p_FIID IN number 
  ) 
  RETURN varchar2 DETERMINISTIC;

  /**
   @brief    Функция возвращает строку ошибки.
  */
  FUNCTION GetStrEventErr( p_Mode IN INTEGER, p_Objecttype IN NUMBER, p_ObjectId IN NUMBER ) RETURN VARCHAR2;

  /** 
   @brief    Функция для сопоставления основания проводок
  */
  FUNCTION CFT_MatchTextLoop RETURN number;

END cft_utils;
/
