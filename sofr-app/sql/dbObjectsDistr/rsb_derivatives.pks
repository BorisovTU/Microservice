/**
 * Author  : Azartsov V.V.
 * Created : AV 25.07.05
 * Пакет RSB_Derivatives для выполнения операций со срочными производными финансовыми инструментами
 */
CREATE OR REPLACE PACKAGE RSB_Derivatives IS

/**
 * Системные статусы сделок\операций исполнения с ПИ */
  DVDEAL_STATE_UNDEF CONSTANT INTEGER :=  -1; --Статус не задан
  DVDEAL_STATE_PREP  CONSTANT INTEGER :=  0;  --Операция на этапе подготовки
  DVDEAL_STATE_OPEN  CONSTANT INTEGER :=  1;  --Операция открыта
  DVDEAL_STATE_CLOSE CONSTANT INTEGER :=  2;  --Операция закрыта

/**
 * Системные статусы операций расчета с ПИ */
  DVOPER_STATE_UNDEF CONSTANT INTEGER :=  -1; --Статус не задан
  DVOPER_STATE_PREP  CONSTANT INTEGER :=  0;  --Операция на этапе подготовки
  DVOPER_STATE_OPEN  CONSTANT INTEGER :=  1;  --Операция открыта
  DVOPER_STATE_CLOSE CONSTANT INTEGER :=  2;  --Операция закрыта

/**
 * Системные статусы позиций ПИ */
  DVPOS_STATE_UNDEF CONSTANT INTEGER :=  -1; --Статус не задан
  DVPOS_STATE_PREP  CONSTANT INTEGER :=  0;  --Отложена
  DVPOS_STATE_OPEN  CONSTANT INTEGER :=  1;  --Открыта
  DVPOS_STATE_CLOSE CONSTANT INTEGER :=  2;  --Закрыта

/**
 * Системные статусы итогов дня по позиции по ПИ */
  DVTURN_STATE_UNDEF CONSTANT INTEGER :=  -1; --Статус не задан
  DVTURN_STATE_PREP  CONSTANT INTEGER :=  0;  --Отложен
  DVTURN_STATE_OPEN  CONSTANT INTEGER :=  1;  --Открыт
  DVTURN_STATE_CLOSE CONSTANT INTEGER :=  2;  --Закрыт

/**
 * Виды значений объекта - Виды комиссий ПИ (OBJTYPE_DV_COMKINDS) */
  DV_COMKINDS_UNDEF      CONSTANT INTEGER := -1;    --Не опред.
  DV_COMKINDS_BANK       CONSTANT INTEGER :=  0;    --Комиссия банка
  DV_COMKINDS_BROKER     CONSTANT INTEGER :=  10;   --Комиссия брокера
  DV_COMKINDS_EXCHANGE   CONSTANT INTEGER :=  100;  --Биржевой сбор
  DV_COMKINDS_EXECUTION  CONSTANT INTEGER :=  110;  --Сбор за исполнение
  DV_COMKINDS_NONSYSTEM  CONSTANT INTEGER :=  120;  --Сбор за внесистемные сделки
  DV_COMKINDS_CLIRING    CONSTANT INTEGER :=  130;  --Клиринговая комиссия
  DV_COMKINDS_ITS        CONSTANT INTEGER :=  140;  --Сбор за ИТС

/**
 * Значения полей Source, ComSource, OurComSource в DVFITURN */
  DV_TURN_SOURCE_UNDEF          CONSTANT INTEGER := -1; --не задано
  DV_TURN_SOURCE_ENTER          CONSTANT INTEGER := 0;  --ввод
  DV_TURN_SOURCE_IMPORT         CONSTANT INTEGER := 1;  --импорт
  DV_TURN_SOURCE_CALC           CONSTANT INTEGER :=  2; --расчет
  DV_TURN_SOURCE_CORRECT_IMPORT CONSTANT INTEGER :=  3; --коррекция импорта
  DV_TURN_SOURCE_CORRECT_CALC   CONSTANT INTEGER :=  4; --коррекция расчетов

/**
 * Виды действий */
  DV_ACTION_CALC                CONSTANT INTEGER := 1;  --Расчеты
  DV_ACTION_EDIT                CONSTANT INTEGER := 2;  --Редактирование
  DV_ACTION_IMPORT              CONSTANT INTEGER := 3;  --Импорт
  DV_ACTION_CALCITOG            CONSTANT INTEGER := 4;  --Расчет итоговых сумм
  DV_ACTION_CALCCOM             CONSTANT INTEGER := 5;  --Расчет клиентской комиссии
  DV_ACTION_DISTRIBITOG         CONSTANT INTEGER := 6;  --Распределение ВМ по сделкам

/**
 * Виды позиции */
   DV_POSITION_SHORT CONSTANT INTEGER := 1; -- Короткая
   DV_POSITION_LONG  CONSTANT INTEGER := 2; -- Длинная

/**
 * виды объектов */
   OBJTYPE_OUTOPER_DV CONSTANT NUMBER := 145; -- Внебиржевая операция с ПИ
   OBJTYPE_FXOPER_DV  CONSTANT NUMBER := 148; -- Конверсионная операция (в ПИ)

/**
 * настройки, инициализируются при старте ПИ */
  DV_Setting_TurnPos NUMBER := 1;      -- Сворачивать позиции при исполнении контрактов
                                       --  0 - нет, 1 - по фьючерсам (умолч), 2 - по опционам, 3 - по всем контрактам
  DV_Setting_NullingTurn NUMBER := 1;  -- Обнулять итоги по нулевой нетто-позиции
                                       --  0 - нет, 1 - по фьючерсам (умолч), 2 - по опционам, 3 - по всем контрактам

  DV_Setting_ExecDealBefore NUMBER := 0;  -- Признак - разрешить начинать выполнять сделки с ПИ до ввода операции расчетов
  DV_Setting_CreateTurn NUMBER := 0;      -- Признак - Создавать запись итогов дня при нулевых оборотах
  DV_Setting_KindMinTickCost NUMBER := 0; -- Вид курса "Стоимость минимального шага цены"

  DV_Setting_AccExContracts NUMBER := 0; -- Учет биржевых контрактов
                                               --  0 - по позиции, 1 - по сделке

  vDV_Setting_RunFromTrust CHAR := chr(0); -- Признак запуска из подсистемы ДУ
  v_LastErrPackage VARCHAR2(256) := '';

  COMM_ALG_A   CONSTANT VARCHAR2(80) := 'Алгоритм A';
  COMM_ALG_B   CONSTANT VARCHAR2(80) := 'Алгоритм B';
  COMM_ALG_C   CONSTANT VARCHAR2(80) := 'Алгоритм C';
  COMM_ALG_AB  CONSTANT VARCHAR2(80) := 'Алгоритмы A и B';
  COMM_ALG_ABC CONSTANT VARCHAR2(80) := 'Алгоритм A и B и C';

/**
 * Виды ПИ */
  DV_DERIVATIVE_FUTURES CONSTANT INTEGER := 1; --Фьючерс
  DV_DERIVATIVE_OPTION  CONSTANT INTEGER := 2; --Опцион

/**
 * значения алгоритма ALG_DV_PERIODKIND_PRRATE */
  ALG_DV_PERIODKIND_PRRATE_DAY    CONSTANT INTEGER := 1; -- День
  ALG_DV_PERIODKIND_PRRATE_MONTH  CONSTANT INTEGER := 2; -- Месяц
  ALG_DV_PERIODKIND_PRRATE_YEAR   CONSTANT INTEGER := 3; -- Год
  ALG_DV_PERIODKIND_PRRATE_PERIOD CONSTANT INTEGER := 4; -- Период

/**
 * Первичные документы */
  DL_DVDEAL  CONSTANT INTEGER := 192; -- Операции с ПИ
  DL_DVFIPOS CONSTANT INTEGER := 193; -- Позиция по ПИ
  DL_DVNDEAL CONSTANT INTEGER := 199; -- Внебиржевая операции с ПИ
  DL_DVFXDEAL CONSTANT INTEGER := 4813; -- Конверсионная сделка ФИСС и КО
  DL_DVDEALT3 CONSTANT INTEGER := 4815; -- Сделка Т+3

/**
 * Номера операций на срочном рынке */
  DV_OPTIONBUY   CONSTANT INTEGER := 2615; -- Покупка опционов
  DV_OPTIONSELL  CONSTANT INTEGER := 2625; -- Продажа опционов
  DV_OPTIONEXEC  CONSTANT INTEGER := 2640; -- Исполнение опционов
  DV_OPTIONEXPIR CONSTANT INTEGER := 2645; -- Экспирация опционов
  DV_FUTURESBUY  CONSTANT INTEGER := 2610; -- Покупка фьючерсов
  DV_FUTURESSELL CONSTANT INTEGER := 2620; -- Продажа фьючерсов
  DV_FUTURESEXEC CONSTANT INTEGER := 2630; -- Исполнение фьючерсов

/**
 * значения алгоритма ALG_SP_MONEY_SOURCE */
  ALG_SP_MONEY_SOURCE_OWN    CONSTANT INTEGER := 1; -- Собственные
  ALG_SP_MONEY_SOURCE_CLIENT CONSTANT INTEGER := 2; -- Клиентские
  ALG_SP_MONEY_SOURCE_TRUST  CONSTANT INTEGER := 3; -- ДУ

  DV_OVERVALUE_MARKET_PRICE    CONSTANT INTEGER :=  1; -- "По рыночной цене ц/б"
  DV_OVERVALUE_CALC_PRICE      CONSTANT INTEGER :=  2; -- "По расчетной цене фьючерса"  - больше не используется, осталось для старых операций
  DV_OVERVALUE_MARKET_PRICE_AR CONSTANT INTEGER :=  3; -- "По рыночной цене артикула"
  DV_OVERVALUE_CHANGE          CONSTANT INTEGER :=  4; -- "По изменениям"

/**
 * Виды изменений графика платежей */
  DV_PMGR_CHANGE_OVERVALUE         CONSTANT INTEGER := 1; -- Переоценка
  DV_PMGR_CHANGE_TRANSFER          CONSTANT INTEGER := 2; -- Перенос по срокам
  DV_PMGR_CHANGE_PUTONBALANCE      CONSTANT INTEGER := 3; -- Постановка на внебаланс
  DV_PMGR_CHANGE_WITHDRWLBLNCE        CONSTANT INTEGER := 4; -- Снятие с внебаланса

/**
 * значения алгоритма ALG_DV_PMGR_SIDE = 7022 */
  ALG_DV_PMGR_SIDE_UNDEF     CONSTANT INTEGER := 0; -- Не задано
  ALG_DV_PMGR_SIDE_LIABILITY CONSTANT INTEGER := 1; -- Обязательство
  ALG_DV_PMGR_SIDE_DEMAND    CONSTANT INTEGER := 2; -- Требование

/**
 * значения алгоритма ALG_DV_TYPECALC = 7025 */
  ALG_DV_TYPECALC_UNDEF     CONSTANT INTEGER := 0; -- Не задан
  ALG_DV_TYPECALC_FOLLOWING CONSTANT INTEGER := 1; -- Following
  ALG_DV_TYPECALC_MODIFIED  CONSTANT INTEGER := 2; -- Modified
  ALG_DV_TYPECALC_PRECEDING CONSTANT INTEGER := 3; -- Preceding

/**
 * значения алгоритма ALG_DV_KINDDVDEAL = 7004 */
  ALG_DV_BUY         CONSTANT INTEGER := 1;   --Покупка
  ALG_DV_SALE        CONSTANT INTEGER := 2;   --Продажа
  ALG_DV_LONGEXEC    CONSTANT INTEGER := 3;   --Исполнение длинных позиций
  ALG_DV_SHORTEXEC   CONSTANT INTEGER := 4;   --Исполнение коротких позиций
  ALG_DV_BS          CONSTANT INTEGER := 5;   --Покупка-продажа
  ALG_DV_SB          CONSTANT INTEGER := 6;   --Продажа-покупка
  ALG_DV_FIX_FLOAT   CONSTANT INTEGER := 7;   --Fix/Float
  ALG_DV_FLOAT_FIX   CONSTANT INTEGER := 8;   --Float/Fix
  ALG_DV_FIX_FIX     CONSTANT INTEGER := 9;   --Fix/Fix
  ALG_DV_FLOAT_FLOAT CONSTANT INTEGER := 10;  --Float/Float

/**
 * значения DVNFI.Type */
  DV_NFIType_BaseActiv  CONSTANT INTEGER := 0;
  DV_NFIType_Forward    CONSTANT INTEGER := 1;
  DV_NFIType_BaseActiv2 CONSTANT INTEGER := 2;

/**
 * способ исполнения ПИ */
  DVSETTLEMET_CALC  CONSTANT INTEGER := 0; -- расчетный
  DVSETTLEMET_STATE CONSTANT INTEGER := 1; -- поставочный

/**
 * значения алгоритма ALG_DV_KINDOP */
  DV_FORWARD     CONSTANT INTEGER := 1;
  DV_OPTION      CONSTANT INTEGER := 2;
  DV_CURSWAP     CONSTANT INTEGER := 3;
  DV_PCTSWAP     CONSTANT INTEGER := 4;
  DV_FORWARD_T3  CONSTANT INTEGER := 5;
  DV_CURSWAP_FX  CONSTANT INTEGER := 6;
  DV_FORWARD_FX  CONSTANT INTEGER := 7;
  DV_BANKNOTE_FX CONSTANT INTEGER := 8;
  DV_FORWARD_MET_FX CONSTANT INTEGER := 9;

/**
 * Признак запуска из модуля ДУ
 */
  FUNCTION DV_Setting_RunFromTrust RETURN CHAR;

  PROCEDURE DV_GetLastErrPackage( ErrPkg OUT VARCHAR2 );

/**
 * Значение категории для субъекта "Учет ГО при посделочном учете бирж. контрактов" */
  FUNCTION RSI_DV_PartyAttrGuaranty( PartyID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Значение категории для субъекта "Расчет итог. сумм сделок по ПИ" */
  FUNCTION RSI_DV_PartyCalcTotalAmount( PartyID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Процедура осуществляет проверку позиции по заданным параметрам, и при необходимости, открывает ее.
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID производный инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_CLIENT Клиент
 * @param v_BROKERCONTR Договор с брокером
 * @param v_CLIENTCONTR Договор с клиентом
 * @param v_OPERTYPE Тип операции
 * @param v_ISTRUST Признак ДУ
 * @param v_OFBU Признак договора ОФБУ (для ДУ)
 * @param v_GenAgrID Генеральное соглашение
 */
  PROCEDURE RSI_DV_CheckAndOpenPosition
            (
               v_FIID         IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_BROKER       IN INTEGER,
               v_CLIENT       IN INTEGER,
               v_BROKERCONTR  IN INTEGER,
               v_CLIENTCONTR  IN INTEGER,
               v_OPERTYPE     IN ddvdeal_dbt.t_Type%TYPE,
               v_ISTRUST      IN CHAR,
               v_OFBU         IN CHAR,
               v_GenAgrID     IN INTEGER
            );

/**
 * Привязка используемой записи по итогам дня по позиции.
 * Процедура проверяет наличие и состояние записи по итогам дня по позиции и, при необходимости, создает ее. Счетчик ссылок увеличивается.
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID Производный инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_ClientContr Договор с клиентом
 * @param v_Date Дата
 * @param v_GenAgrID Генеральное соглашение
 */
  PROCEDURE RSI_DV_AttachPositionTurn
            (
               v_FIID         IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_BROKER       IN INTEGER,
               v_ClientContr  IN INTEGER,
               v_Date         IN DATE,
               v_GenAgrID     IN INTEGER
            );

/**
 * Освобождение используемой записи по итогам дня по позиции.
 * Процедура уменьшает счетчик ссылок. Если запись больше не используется, она удаляется.
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID Производный инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_ClientContr Договор с клиентом
 * @param v_Date Дата
 * @param v_GenAgrID Генеральное соглашение
 */
  PROCEDURE RSI_DV_DetachPositionTurn
            (
               v_FIID         IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_BROKER       IN INTEGER,
               v_ClientContr  IN INTEGER,
               v_Date         IN DATE,
               v_GenAgrID     IN INTEGER
            );

/**
 * Возвращает число не рассчитываемых комиссий по позиции за день.
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID Производный инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_ClientContr Договор с клиентом
 * @param v_Date Дата
 * @param v_GenAgrID Генеральное соглашение
 */
  FUNCTION RSI_DV_GetCountNotCalcPosCom
            (
               v_FIID         IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_BROKER       IN INTEGER,
               v_ClientContr  IN INTEGER,
               v_Date         IN DATE,
               v_GenAgrID     IN INTEGER
            )
   RETURN NUMBER;

/**
 * Закрытие одной позиции.
 * Используется при закрытии позиции из интерфейса.
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID Производный инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_ClientContr Договор с клиентом
 * @param v_GenAgrID Генеральное соглашение
 */
  PROCEDURE RSI_DV_CloseOnePosition
            (
               v_FIID         IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_BROKER       IN INTEGER,
               v_CLIENTCONTR  IN INTEGER,
               v_GenAgrID     IN INTEGER
            );

/**
 * Откат закрытия позиции.
 * Используется при откате закрытия (открытии) позиции из интерфейса.
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID Производный инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_ClientContr Договор с клиентом
 * @param v_GenAgrID Генеральное соглашение
 */
  PROCEDURE RSI_DV_RecoilCloseOnePosition
            (
               v_FIID         IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_BROKER       IN INTEGER,
               v_CLIENTCONTR  IN INTEGER,
               v_GenAgrID     IN INTEGER
            );

/**
 * Открытие итогов дня по позициям
 * Выполняется в операции расчетов.
 * @since 6.20.029
 * @qtest NO
 * @param v_PARTYKIND Вид контрагента по расчетам
 * @param v_PARTY Контрагент
 * @param v_PARTYCONTR Договор с контрагентом
 * @param v_DEPARTMENT Филиал
 * @param v_Date Дата
 * @param v_Flag1 Принадлежность средств
 * @param v_GenAgrID Генеральное соглашение
 */
  PROCEDURE RSI_DV_OpenPositionTurns
            (
               v_PARTYKIND    IN INTEGER,
               v_PARTY        IN INTEGER,
               v_PARTYCONTR   IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_Date         IN DATE,
               v_Flag1        IN INTEGER,
               v_GenAgrID     IN INTEGER
            );

/**
 * Откат открытия итогов дня по позициям
 * Процедура откатывает открытие дня.
 * Выполняется при откате операции расчетов.
 * @since 6.20.029
 * @qtest NO
 * @param v_PARTYKIND Вид контрагента по расчетам
 * @param v_PARTY Контрагент
 * @param v_PARTYCONTR Договор с контрагентом
 * @param v_DEPARTMENT Филиал
 * @param v_Date Дата
 * @param v_Flag1 Принадлежность средств
 * @param v_GenAgrID Генеральное соглашение
 */
  PROCEDURE RSI_DV_RecoilOpenPositionTurns
            (
               v_PARTYKIND    IN INTEGER,
               v_PARTY        IN INTEGER,
               v_PARTYCONTR   IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_Date         IN DATE,
               v_Flag1        IN INTEGER,
               v_GenAgrID     IN INTEGER
            );

/**
 * Закрытие итогов дня
 * Выполняется в операции расчетов.
 * @since 6.20.029
 * @qtest NO
 * @param v_PARTYKIND Вид контрагента по расчетам
 * @param v_PARTY Контрагент
 * @param v_PARTYCONTR Договор с контрагентом
 * @param v_DEPARTMENT Филиал
 * @param v_Date Дата
 * @param v_Flag1 Принадлежность средств
 * @param v_GenAgrID Генеральное соглашение
 */
   PROCEDURE RSI_DV_ClosePositionTurns
            (
               v_PARTYKIND    IN INTEGER,
               v_PARTY        IN INTEGER,
               v_PARTYCONTR   IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_Date         IN DATE,
               v_Flag1        IN INTEGER,
               v_GenAgrID     IN INTEGER
            );

/**
 * Откат закрытия итогов дня.
 * Процедура откатывает закрытие позиций и итогов дня дня.
 * Выполняется при откате операции расчетов.
 * @since 6.20.029
 * @qtest NO
 * @param v_PARTYKIND Вид контрагента по расчетам
 * @param v_PARTY Контрагент
 * @param v_PARTYCONTR Договор с контрагентом
 * @param v_DEPARTMENT Филиал
 * @param v_Date Дата
 * @param v_Flag1 Принадлежность средств
 * @param v_GenAgrID Генеральное соглашение
 */
   PROCEDURE RSI_DV_RecoilClosePosTurns
            (
               v_PARTYKIND    IN INTEGER,
               v_PARTY        IN INTEGER,
               v_PARTYCONTR   IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_Date         IN DATE,
               v_Flag1        IN INTEGER,
               v_GenAgrID     IN INTEGER
            );

/**
 * Простановка признака расчета (t_TotalCalc) на итоги
 * Выполняется в операции расчетов.
 * @since 6.20.029
 * @qtest NO
 * @param v_PARTYKIND Вид контрагента по расчетам
 * @param v_PARTY Контрагент
 * @param v_PARTYCONTR Договор с контрагентом
 * @param v_DEPARTMENT Филиал
 * @param v_Date Дата
 * @param v_Flag1 Принадлежность средств
 * @param v_GenAgrID Генеральное соглашение
 */
   PROCEDURE RSI_DV_SetTotalCalcPosTurns
            (
               v_PARTYKIND    IN INTEGER,
               v_PARTY        IN INTEGER,
               v_PARTYCONTR   IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_Date         IN DATE,
               v_Flag1        IN INTEGER,
               v_GenAgrID     IN INTEGER
            );

/**
 * Снятие признака расчета (t_TotalCalc) на итогах
 * Выполняется при откате операции расчетов.
 * @since 6.20.029
 * @qtest NO
 * @param v_PARTYKIND Вид контрагента по расчетам
 * @param v_PARTY Контрагент
 * @param v_PARTYCONTR Договор с контрагентом
 * @param v_DEPARTMENT Филиал
 * @param v_Date Дата
 * @param v_Flag1 Принадлежность средств
 * @param v_GenAgrID Генеральное соглашение
 */
   PROCEDURE RSI_DV_UnSetTotalCalcPosTurns
            (
               v_PARTYKIND    IN INTEGER,
               v_PARTY        IN INTEGER,
               v_PARTYCONTR   IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_Date         IN DATE,
               v_Flag1        IN INTEGER,
               v_GenAgrID     IN INTEGER
            );

/**
 * Обнуление итогов дня
 * Выполняется в операции расчетов.
 * @since 6.20.029
 * @qtest NO
 * @param v_PARTYKIND Вид контрагента по расчетам
 * @param v_PARTY Контрагент
 * @param v_PARTYCONTR Договор с контрагентом
 * @param v_DEPARTMENT Филиал
 * @param v_Date Дата
 * @param v_OPERID Операция расчетов
 * @param v_Flag1 Принадлежность средств
 * @param v_GenAgrID Генеральное соглашение
 */
   PROCEDURE RSI_DV_NullingPositions
            (
               v_PARTYKIND    IN INTEGER,
               v_PARTY        IN INTEGER,
               v_PARTYCONTR   IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_Date         IN DATE,
               v_OPERID       IN INTEGER,
               v_Flag1        IN INTEGER,
               v_GenAgrID     IN INTEGER
            );

/**
 * Откат зануления итогов дня.
 * Выполняется при откате операции расчетов.
 * @since 6.20.029
 * @qtest NO
 * @param v_OPERID Операция расчетов
 */
   PROCEDURE RSI_DV_RecoilNullingPositions
            (
               v_OPERID       IN INTEGER
            );

/**
 * Закрытие позиций
 * Выполняется в операции расчетов.
 * @since 6.20.029
 * @qtest NO
 * @param v_DvoperID Код операции расчётов
 */
   PROCEDURE RSI_DV_ClosePositions ( v_DvoperID   IN INTEGER  /* Операция расчётов*/ );

/**
 * Откат закрытия итогов дня и позиций.
 * Процедура откатывает закрытие позиций и итогов дня дня.
 * Выполняется при откате операции расчетов.
 * @since 6.20.029
 * @qtest NO
 * @param v_DvoperID Код операции расчётов
 */
   PROCEDURE RSI_DV_RecoilClosePositions (v_DvoperID   IN INTEGER  /* Операция расчётов*/ );

/**
 * Вставка данных по позиции. Используется в процедуре импорта и при вставке итогов и редактирования.
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID Финансовый инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_ClientContr Договор с клиентом
 * @param v_DATE Дата
 * @param v_MARGIN Вариационная маржа
 * @param v_GUARANTY Гарантийное обеспечение
 * @param v_FAIRVALUECALC Признак расчета справедливой стоимости
 * @param v_FAIRVALUE Справедливая стоимость
 * @param v_INSERTMARGIN Вставка вариационной маржи
 * @param v_INSERTGUARANTY  Вставка гарантийного обеспечения
 * @param v_INSERTFAIRVALUE Вставка справедливой стоимости
 * @param v_ACTION Действие
 * @param v_GenAgrID Генеральное соглашение
 */
   PROCEDURE RSI_DV_InsertPositionTurn
            (
               v_FIID            IN INTEGER,
               v_DEPARTMENT      IN INTEGER,
               v_BROKER          IN INTEGER,
               v_ClientContr     IN INTEGER,
               v_DATE            IN DATE,
               v_MARGIN          IN NUMBER,
               v_GUARANTY        IN NUMBER,
               v_FAIRVALUECALC   IN CHAR,
               v_FAIRVALUE       IN NUMBER,
               v_INSERTMARGIN    IN INTEGER,
               v_INSERTGUARANTY  IN INTEGER,
               v_INSERTFAIRVALUE IN INTEGER,
               v_ACTION          IN INTEGER,
               v_GenAgrID        IN INTEGER,
               v_MARGINDAY       IN NUMBER,
               v_MARGINDEALS     IN NUMBER
            );

/**
 * Вставка рассчитанных данных по позиции. Используется в операции расчетов по позиции.
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID Финансовый инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_ClientContr Договор с клиентом
 * @param v_DATE Дата
 * @param v_MARGIN Вариационная маржа
 * @param v_FAIRVALUE Справедливая стоимость
 * @param v_CALCSUM Расчет сумм 1=Да, 0=Нет
 * @param v_CALCFAIRVALUE Расчет справедливой стоимости
 * @param v_GenAgrID Генеральное соглашение
 */
   PROCEDURE RSI_DV_CalcPositionTurn
            (
               v_FIID            IN INTEGER,
               v_DEPARTMENT      IN INTEGER,
               v_BROKER          IN INTEGER,
               v_ClientContr     IN INTEGER,
               v_DATE            IN DATE,
               v_MARGIN          IN NUMBER,
               v_FAIRVALUE       IN NUMBER,
               v_CALCSUM         IN INTEGER,
               v_CALCFAIRVALUE   IN INTEGER,
               v_GenAgrID        IN INTEGER
            );
/**
 * Массовый откат вставки данных по сделке */
  PROCEDURE RSI_DV_MassRecoilInsertPosTurn(  v_PosID    IN INTEGER, -- Позиция
                                             v_DvoperID IN INTEGER, -- Операция расчётов
                                             v_Action   IN INTEGER  -- Действие
                                           );

/**
 * Откат вставки данных по позиции
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID Финансовый инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_ClientContr Договор с клиентом
 * @param v_DATE Дата
 * @param v_ACTION Действие
 * @param v_GenAgrID Генеральное соглашение
 */
   PROCEDURE RSI_DV_RecoilInsertPosTurn
            (
               v_FIID            IN INTEGER,
               v_DEPARTMENT      IN INTEGER,
               v_BROKER          IN INTEGER,
               v_ClientContr     IN INTEGER,
               v_DATE            IN DATE,
               v_ACTION          IN INTEGER,
               v_GenAgrID        IN INTEGER
            );

/**
 * Выполняет вставку объекта, вызывается в макросах шагов, без проверки наличия существующего.
 * @since 6.20.029
 * @qtest NO
 */
   PROCEDURE RSI_DV_InsertFiCom
            (
              pComissID    IN NUMBER,
              pFIID        IN NUMBER,
              pDepartment  IN NUMBER,
              pBroker      IN NUMBER,
              pClientContr IN NUMBER,
              pDate        IN DATE,
              pSum         IN NUMBER,
              pNDS         IN NUMBER,
              pGenAgrID    IN INTEGER
            );

/**
 * Выполняет вставку комиссии по операции */
   PROCEDURE RSI_DV_InsertDlCom
            (
              pDealID      IN NUMBER,
              pComissID    IN NUMBER,
              pSum         IN NUMBER,
              pNDS         IN NUMBER
            );

/**
 * Массовая вставка комиссий по биржевым операциям
 * @since 6.20.031
 * @qtest NO
 */
  PROCEDURE DV_MassInsertDlCom;

/**
 * Выполняет удаление комиссии по операции */
   PROCEDURE RSI_DV_DeleteDlCom
            (
              pDealID      IN NUMBER,
              pComissID    IN NUMBER
            );

/**
 * Импорт данных по позиции.
 * @since 6.20.029 @qtest-YES
 * @param v_FIID Финансовый инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_ClientContr Договор с клиентом
 * @param v_DATE Дата
 * @param v_MARGIN Вариационная маржа
 * @param v_GUARANTY Гарантийное обеспечение
 * @param v_ISTRUST Признак ДУ
 * @param v_IMPORTMARGIN Импорт вариационной маржи 1=Да, 0=Нет
 * @param v_IMPORTGUARANTY Импорт гарантийного обеспечения 1=Да, 0=Нет
 * @param v_GenAgrID Генеральное соглашение
 */
   PROCEDURE RSI_DV_ImportPositionTurn
            (
               v_FIID            IN INTEGER,
               v_DEPARTMENT      IN INTEGER,
               v_BROKER          IN INTEGER,
               v_Client          IN INTEGER,
               v_ClientContr     IN INTEGER,
               v_DATE            IN DATE,
               v_MARGIN          IN NUMBER,
               v_GUARANTY        IN NUMBER,
               v_ISTRUST         IN CHAR,
               v_IMPORTMARGIN    IN INTEGER,
               v_IMPORTGUARANTY  IN INTEGER,
               v_GenAgrID        IN INTEGER,
               v_MARGINDAY       IN NUMBER,
               v_MARGINDEALS     IN NUMBER
            );

/**
 * Вставка данных по позиции. Используется при вводе новых итогов из скроллинга.
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID Финансовый инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_ClientContr Договор с клиентом
 * @param v_DATE Дата
 * @param v_MARGIN Вариационная маржа
 * @param v_GUARANTY Гарантийное обеспечение
 * @param v_FAIRVALUECALC Признак расчета справедливой стоимости
 * @param v_FAIRVALUE Справедливая стоимость
 * @param v_GenAgrID Генеральное соглашение
 */
   PROCEDURE RSI_DV_InputPositionTurn
           (
              v_FIID            IN INTEGER,
              v_DEPARTMENT      IN INTEGER,
              v_BROKER          IN INTEGER,
              v_ClientContr     IN INTEGER,
              v_DATE            IN DATE,
              v_MARGIN          IN NUMBER,
              v_GUARANTY        IN NUMBER,
              v_FAIRVALUECALC   IN CHAR,
              v_FAIRVALUE       IN NUMBER,
              v_GenAgrID        IN INTEGER
           );

/**
 * Редактирование данных по позиции.
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID Финансовый инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_ClientContr Договор с клиентом
 * @param v_DATE Дата
 * @param v_MARGIN Вариационная маржа
 * @param v_GUARANTY Гарантийное обеспечение
 * @param v_FAIRVALUECALC Признак расчета справедливой стоимости
 * @param v_FAIRVALUE Справедливая стоимость
 * @param v_EDITMARGIN Редактирование вариационной марж
 * @param v_EDITGUARANTY Редактирование гарантийного обеси 1=Да, 0=Нет
 * @param v_EDITFAIRVALUE Редактирование справедливой стоипечения 1=Да, 0=Нет
 * @param v_COUNTNOTCALCCOMM Число не рассчитываемых комиссиймости 1=Да, 0=Нет
 * @param v_GenAgrID Генеральное соглашение
 */
   PROCEDURE RSI_DV_EditPositionTurn
            (
               v_FIID             IN INTEGER,
               v_DEPARTMENT       IN INTEGER,
               v_BROKER           IN INTEGER,
               v_ClientContr      IN INTEGER,
               v_DATE             IN DATE,
               v_MARGIN           IN NUMBER,
               v_GUARANTY         IN NUMBER,
               v_FAIRVALUECALC    IN CHAR,
               v_FAIRVALUE        IN NUMBER,
               v_EDITMARGIN       IN INTEGER,
               v_EDITGUARANTY     IN INTEGER,
               v_EDITFAIRVALUE    IN INTEGER,
               v_COUNTNOTCALCCOMM IN INTEGER,
               v_GenAgrID         IN INTEGER
            );

/**
 * Переоценка стоимости по позиции. Используется в процедуре переоценки НЕ В ДУ.
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID Финансовый инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_ClientContr Договор с клиентом
 * @param v_DATE Дата
 * @param v_LONGPOSITIONCOST Стоимость длинных позиций
 * @param v_SHORTPOSITIONCOST Стоимость коротких позиций
 * @param v_OPERID ID операции переоценки
 * @param v_FLAG Вид переоценки
 * @param v_GenAgrID Генеральное соглашение
 */
   PROCEDURE RSI_DV_OvervaluePositionTurn
            (
               v_FIID               IN INTEGER,
               v_DEPARTMENT         IN INTEGER,
               v_BROKER             IN INTEGER,
               v_ClientContr        IN INTEGER,
               v_DATE               IN DATE,
               v_LONGPOSITIONCOST   IN NUMBER,
               v_SHORTPOSITIONCOST  IN NUMBER,
               v_OPERID             IN INTEGER,
               v_FLAG               IN INTEGER,
               v_GenAgrID           IN INTEGER
            );

/**
 * Переоценка стоимости по позиции для модуля ДУ.
 * @since 6.20.029
 * @qtest NO
 * @param v_OPERID Операция расчетов
 * @param v_PosID Позиция
 * @param v_Date Дата
 * @param v_Summa Сумма переоценки
 * @param v_Rate Курс
 * @param v_NewLongCost Новая стоимость длинных позиций
 * @param v_NewShortCost Новая стоимость коротких позиций
 */
   PROCEDURE RSI_TS_OvervaluePositionDV
            (
               v_OPERID             IN INTEGER,
               v_PosID              IN INTEGER,
               v_Date               IN DATE,
               v_Summa              IN NUMBER,
               v_Rate               IN NUMBER,
               v_NewLongCost        IN NUMBER,
               v_NewShortCost       IN NUMBER
            );

/**
 * Откат переоценки стоимости по позиции. Используется при откате переоценки (НЕ В ДУ).
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID Финансовый инструмент
 * @param v_DEPARTMENT Филиал
 * @param v_BROKER Брокер
 * @param v_ClientContr Договор с клиентом
 * @param v_DATE Дата
 * @param v_OPERID ID операции переоценки
 * @param v_FLAG Вид переоценки
 * @param v_GenAgrID Генеральное соглашение
 */
   PROCEDURE RSI_DV_RecoilOvervaluePosTurn
            (
              v_FIID        IN INTEGER,
              v_DEPARTMENT  IN INTEGER,
              v_BROKER      IN INTEGER,
              v_ClientContr IN INTEGER,
              v_DATE        IN DATE,
              v_OPERID      IN INTEGER,
              v_FLAG        IN INTEGER,
              v_GenAgrID    IN INTEGER
            );

/**
 * Откат переоценки стоимости по позиции. Используется при откате переоценки (НЕ В ДУ).
 * @since 6.20.029
 * @qtest NO
 * @param v_OPERID Операция расчетов
 * @param v_PosID Позиция
 * @param v_Date Дата
 */
   PROCEDURE RSI_TS_RecoilOvervaluePosDV
            (
               v_OPERID             IN INTEGER,
               v_PosID              IN INTEGER,
               v_Date               IN DATE
            );

/**
 * Импорт комиссии по позиции.
 * @since 6.20.029 @qtest-YES
 * @param v_FIID        Финансовый инструмент
 * @param v_DEPARTMENT  Филиал
 * @param v_BROKER      Брокер
 * @param v_Client      Клиент
 * @param v_ClientContr Договор с клиентом
 * @param v_DATE        Дата
 * @param v_ISTRUST     Признак ДУ
 * @param v_ComissID    Комисиия
 * @param v_SUM         Сумма
 * @param v_NDS         НДС
 * @param v_GenAgrID    Генеральное соглашение
 */
   PROCEDURE RSI_DV_ImportComPosition
            (
               v_FIID            IN INTEGER,
               v_DEPARTMENT      IN INTEGER,
               v_BROKER          IN INTEGER,
               v_Client          IN INTEGER,
               v_ClientContr     IN INTEGER,
               v_DATE            IN DATE,
               v_ISTRUST         IN CHAR,
               v_ComissID        IN INTEGER,
               v_SUM             IN NUMBER,
               v_NDS             IN NUMBER,
               v_GenAgrID        IN INTEGER
            );

/**
 * Процедура расчета комиссии по позиции
 * @since 6.20.029
 * @qtest NO
 * @param pFIID        Финансовый инструмент
 * @param pDEPARTMENT  Филиал
 * @param pBROKER      Брокер
 * @param pClientContr Договор с клиентом
 * @param pDATE        Дата
 * @param pComissID    Комиссия
 * @param pSUM         Сумма
 * @param pNDS         НДС
 * @param pGenAgrID    Генеральное соглашение
 */
   PROCEDURE RSI_DV_CalcPosCom
            (
               pFIID            IN INTEGER,
               pDEPARTMENT      IN INTEGER,
               pBROKER          IN INTEGER,
               pClientContr     IN INTEGER,
               pDATE            IN DATE,
               pComissID        IN INTEGER,
               pSUM             IN NUMBER,
               pNDS             IN NUMBER,
               pGenAgrID        IN INTEGER
            );

/**
 * Процедура обновления пересчитываемой комиссии по позиции
 * @since 6.20.029
 * @qtest NO
 * @param pFIID        Финансовый инструмент
 * @param pDEPARTMENT  Филиал
 * @param pBROKER      Брокер
 * @param pClientContr Договор с клиентом
 * @param pDATE        Дата
 * @param pComissID    Комиссия
 * @param pGenAgrID    Генеральное соглашение
 */
   PROCEDURE RSI_DV_UpdatePosCom
            (
               pFIID            IN INTEGER,
               pDEPARTMENT      IN INTEGER,
               pBROKER          IN INTEGER,
               pClientContr     IN INTEGER,
               pDATE            IN DATE,
               pComissID        IN INTEGER,
               pGenAgrID        IN INTEGER
            );

/**
 * Импорт комиссии по операции
 * @since 6.20.029
 * @qtest NO
 * @param pDealID   ID сделки
 * @param pComissID Комиссия
 * @param pSUM      Сумма
 * @param pNDS      НДС
 */
   PROCEDURE RSI_DV_ImportDealCom
            (
               pDealID          IN INTEGER,
               pComissID        IN INTEGER,
               pSUM             IN NUMBER,
               pNDS             IN NUMBER
            );

/**
 * Процедура получения сумм комиссий по итогам дня
 * @since 6.20.029
 * @qtest NO
 * @param pFIID         Финансовый инструмент
 * @param pDEPARTMENT   Филиал
 * @param pBROKER       Брокер
 * @param pClientContr  Договор с клиентом
 * @param pDATE         Дата
 * @param pComissID     Комиссия
 * @param pSUM          Выходной параметр - Сумма
 * @param pNDS          Выходной параметр - НДС
 * @param pGenAgrID     Генеральное соглашение
 */
   PROCEDURE RSI_DV_GetTurnCom
            (
               pFIID            IN INTEGER,
               pDEPARTMENT      IN INTEGER,
               pBROKER          IN INTEGER,
               pClientContr     IN INTEGER,
               pDATE            IN DATE,
               pComissID        IN INTEGER,
               pSum             OUT NUMBER,
               pNDS             OUT NUMBER,
               pGenAgrID        IN INTEGER
            );

/**
 * Функция, возвращает текущую стоимость минимального шага цены
 * @since 6.20.029
 * @qtest NO
 * @param p_FIID ПИ
 * @param p_TICKDATE Дата курса
 * @return Текущая стоимость минимального шага цены
 */
   FUNCTION DV_TickCost( p_FIID IN INTEGER, p_TICKDATE IN DATE ) RETURN NUMBER;

/**
 * Вид ПД в операции расчетов
 * @since 6.20.029
 * @qtest NO
 * @return Вид ПД в операции расчетов
 */
   FUNCTION DV_GetDocKind RETURN NUMBER;

   FUNCTION DV_GetDvDealDate (
               p_DealDate IN DATE,
               p_DealTime IN DATE
   ) RETURN DATE;
/**
 * Получить операцию с ПИ
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID         Производный инструмент
 * @param v_DEPARTMENT   Филиал
 * @param v_BROKER       Брокер
 * @param v_ClientContr  Договор с клиентом
 * @param v_Date         Дата
 * @param v_RefOperation Выходной параметр - Операция с ПИ
 * @param v_GenAgrID     Генеральное соглашение
 * @return ID найденной операции в случае успеха, иначе - 0
 */
   FUNCTION DV_GetOperatonByTurn(
               v_FIID         IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_BROKER       IN INTEGER,
               v_ClientContr  IN INTEGER,
               v_Date         IN DATE,
               v_RefOperation OUT ddvoper_dbt%ROWTYPE,
               v_GenAgrID     IN INTEGER
   ) RETURN NUMBER;

/**
 * Получить позицию ПИ
 * @since 6.20.029
 * @qtest NO
 * @param v_FIID         Производный инструмент
 * @param v_DEPARTMENT   Филиал
 * @param v_BROKER       Брокер
 * @param v_ClientContr  Договор с клиентом
 * @param v_GenAgrID     Генеральное соглашение
 * @return ID найденной позиции в случае успеха, иначе - 0
 */
   FUNCTION DV_GetPosByTurn(
               v_FIID         IN INTEGER,
               v_DEPARTMENT   IN INTEGER,
               v_BROKER       IN INTEGER,
               v_ClientContr  IN INTEGER,
               v_GenAgrID     IN INTEGER
   ) RETURN NUMBER;

/**
 * Создать Т/О по операции с ПИ в ДУ
 */
   PROCEDURE RSI_DV_CreateDemandTrust( ID_Operation         IN INTEGER,
                                   UserMark             IN VARCHAR2,
                                   Role                 IN INTEGER,
                                   KVTO                 IN VARCHAR2,
                                   KUS                  IN VARCHAR2,
                                   KVDR                 IN VARCHAR2,
                                   Dplan                IN DATE,
                                   Summa                IN NUMBER,
                                   ActiveFIID           IN INTEGER,
                                   ActiveDepositoryID   IN INTEGER,
                                   SFClientContr        IN INTEGER,
                                   BaseFIID             IN INTEGER
                                 );

/**
 * Создать Т/О по оплате комиссии
 * @since 6.20.029
 * @qtest NO
 * @param v_UserMarkMain Метка
 * @param v_KindTOMain   Т/О
 * @param v_ID_Operation ID операции
 * @param v_Party        Контрагент
 * @param v_FIID         Производный инструмент
 * @param v_DEPARTMENT   Филиал
 * @param v_BROKER       Брокер
 * @param v_ClientContr  Договор с клиентом
 * @param v_Date         Дата
 * @param v_Summa        Сумма комиссии
 * @param v_SummaFIID    Валюта суммы
 */
   PROCEDURE DV_CreateDemandCommiss(  v_UserMarkMain IN VARCHAR2,
                                      v_KindTOMain   IN INTEGER,
                                      v_ID_Operation IN INTEGER,
                                      v_Party        IN INTEGER,
                                      v_FIID         IN INTEGER,
                                      v_DEPARTMENT   IN INTEGER,
                                      v_BROKER       IN INTEGER,
                                      v_ClientContr  IN INTEGER,
                                      v_Date         IN DATE,
                                      v_Summa        IN NUMBER,
                                      v_SummaFIID    IN NUMBER
                                    );

/**
 * Создать Т/О по оплате вар. маржи
 * @since 6.20.029
 * @qtest NO
 * @param v_UserMarkMain Метка
 * @param v_KindTOMain   Т/О
 * @param v_ID_Operation ID операции
 * @param v_Party        Контрагент
 * @param v_FIID         Производный инструмент
 * @param v_DEPARTMENT   Филиал
 * @param v_BROKER       Брокер
 * @param v_ClientContr  Договор с клиентом
 * @param v_Date         Дата
 * @param v_Summa        Сумма вар. маржи
 */
   PROCEDURE DV_CreateDemandMargin( v_UserMarkMain IN VARCHAR2,
                                    v_KindTOMain   IN INTEGER,
                                    v_ID_Operation IN INTEGER,
                                    v_Party        IN INTEGER,
                                    v_FIID         IN INTEGER,
                                    v_DEPARTMENT   IN INTEGER,
                                    v_BROKER       IN INTEGER,
                                    v_ClientContr  IN INTEGER,
                                    v_Date         IN DATE,
                                    v_Summa        IN NUMBER
                                  );
/**
 * Создать Т/О по оплате премии
 * @since 6.20.029
 * @qtest NO
 * @param v_UserMarkMain Метка
 * @param v_KindTOMain   Т/О
 * @param v_ID_Operation ID операции
 * @param v_Party        Контрагент
 * @param v_FIID         Производный инструмент
 * @param v_DEPARTMENT   Филиал
 * @param v_BROKER       Брокер
 * @param v_ClientContr  Договор с клиентом
 * @param v_Date         Дата
 * @param v_Bonus        Сумма премии
 */
   PROCEDURE DV_CreateDemandBonus( v_UserMarkMain IN VARCHAR2,
                                   v_KindTOMain   IN INTEGER,
                                   v_ID_Operation IN INTEGER,
                                   v_Party        IN INTEGER,
                                   v_FIID         IN INTEGER,
                                   v_DEPARTMENT   IN INTEGER,
                                   v_BROKER       IN INTEGER,
                                   v_ClientContr  IN INTEGER,
                                   v_Date         IN DATE,
                                   v_Bonus        IN NUMBER
                                 );

/**
 * Изменить суммы Т/О модуля ДУ
 * @since 6.20.029
 * @qtest NO
 * @param v_ID_Operation ID операции
 * @param v_UserMark Метка
 * @param v_NewSumma Новая сумма
 */
   PROCEDURE DV_ChangeDemandTurn(
               v_ID_Operation     IN NUMBER,
               v_UserMark         IN VARCHAR2,
               v_NewSumma         IN NUMBER
            );

/**
 * Определить есть ли среди шагов операции шаг c заданным символом
 * @since 6.20.029
 * @qtest NO
 * @param v_DealID ID документа операции
 * @param v_DocKind Вид документа операции
 * @param v_BranchSymbol Символ шага
 * @param v_IsExecute Признак - шаг выполнен
 * @return > 0 - есть, иначе - нет
 */
  FUNCTION DV_IsExistOperStep( v_DealID IN NUMBER, v_DocKind IN NUMBER, v_BranchSymbol IN CHAR, v_IsExecute IN CHAR, v_RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) RETURN NUMBER;

/**
 * Отобрать сделки в DDVFVOVER_TMP для проведения сервисной операции переоценки СС
 * @since 6.20.030
 * @qtest NO
 * @param ServDocID ID сервисной операции
 */
  PROCEDURE DV_SelectNDealForOverFrVal( ServDocID IN NUMBER );

/**
 * Отобрать сделки по сервисной операции в DDVFVOVER_TMP
 * @since 6.20.030
 * @qtest NO
 * @param ServDocID ID сервисной операции
 */
   PROCEDURE DV_SelectNDealIncomOverFrVal( ServDocID IN NUMBER );

/**
 * Количество дней в году по базису
 * @since 6.20.030
 * @qtest NO
 * @param basis базис
 * @param d дата с искомым годом
 * @return Количество дней в году по базису
 */
  FUNCTION DV_DaysInYearByBasis( basis in int,
                                 d     in date
                               ) RETURN int;

/**
 * количество дней в месяце по базису
 * @since 6.20.030
 * @qtest NO
 * @param basis базис
 * @param d дата с искомым месяцем
 * @return Количество дней в месяце по базису
 */
  FUNCTION DV_DaysInMonthByBasis( basis in int,
                                  d     in date
                                ) RETURN int;

/**
 * количество дней в периоде по базису
 * @since 6.20.030
 * @qtest NO
 * @param basis базис
 * @param d дата начала периода
 * @param Period период
 * @param PeriodKind вид периода
 * @return Количество дней в периоде по базису
 */
  FUNCTION DV_DaysInPeriodByBasis( basis      in int,
                                   d          in date,
                                   Period     in int,
                                   PeriodKind in int
                                 ) RETURN int;

/**
 * Переменное количество дней в месяце
 * @since 6.20.030
 * @qtest NO
 * @param start_dt начало периода начисления
 * @param end_dt окончние периода начисления
 * @param basis базис ставки (cnst.BASIS_***)
 * @return Переменное количество дней в месяце
 */
  FUNCTION DV_VarMonth( start_dt    in DATE,
                        end_dt      in DATE,
                        basis       in NUMBER
                      ) RETURN number;

/**
 * Постоянное количество дней в месяце
 * @since 6.20.030
 * @qtest NO
 * @param start_dt начало периода начисления
 * @param end_dt окончние периода начисления
 * @param basis базис ставки (cnst.BASIS_***)
 * @return Постоянное количество дней в месяце
 */
  FUNCTION DV_ConstMonth( start_dt    in DATE,
                          end_dt      in DATE,
                          basis       in NUMBER
                        ) RETURN number;

/**
 * Количество лет по базису
 * @since 6.20.030
 * @qtest NO
 * @param start_dt начало периода начисления
 * @param end_dt окончние периода начисления
 * @param basis базис ставки (cnst.BASIS_***)
 * @return Количество лет по базису
 */
  FUNCTION DV_Years( start_dt    in DATE,
                     end_dt      in DATE,
                     basis       in NUMBER
                   ) RETURN number;

/**
 * Процедура удаления графика платежей
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID Сделки
 * @param pSide Сторона
 */
   PROCEDURE RSI_DV_DeletePMGR( pDealID IN NUMBER, pSide IN NUMBER );

/**
 * Процедура удаления графика платежей
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID Сделки
 * @param pSide Сторона
 */
   PROCEDURE RSI_DV_DeletePMGR_TMP( pDealID IN NUMBER, pSide IN NUMBER );

/**
 * Процедура построения графика платежей
 * @since 6.20.030
 * @qtest NO
 * @param pD Сделка
 * @param pF Актив
 * @param pSide Сторона
 */
   PROCEDURE DV_CreatePMGR( pD IN DDVNDEAL_DBT%ROWTYPE, pF IN DDVNFI_DBT%ROWTYPE, pSide IN NUMBER );

/**
 * Процедура построения графика платежей
 * @since 6.20.030
 * @qtest NO
 * @param pD Сделка
 * @param pF Актив
 * @param pSide Сторона
 */
   PROCEDURE DV_CreatePMGR_TMP( pD IN DDVNDEAL_DBT%ROWTYPE, pF IN DDVNFI_DBT%ROWTYPE, pSide IN NUMBER );

/**
 * Процедура генерации графика платежей
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID сделки
 * @param pLiability Расчет обязательств
 * @param pDemand Расчет требований
 */
   PROCEDURE DV_UpdatePMGR( pDealID IN NUMBER, pLiability IN NUMBER DEFAULT 1, pDemand IN NUMBER DEFAULT 1 );

/**
 * Процедура генерации графика платежей
 * @since 6.20.030
 * @qtest NO
 * @param pDealID ID сделки
 * @param pLiability Расчет обязательств
 * @param pDemand Расчет требований
 */
   PROCEDURE DV_UpdatePMGR_TMP( pDealID IN NUMBER, pLiability IN NUMBER DEFAULT 1, pDemand IN NUMBER DEFAULT 1 );

/**
 * Установка признака учета по позиции на операции */
  PROCEDURE RSI_DV_SetDealPosAcc( pDealID IN NUMBER );

/**
 * Установка признака учета по позиции на операции при массовом исполнении
 * @since 6.20.031
 * @qtest NO
 */
  PROCEDURE DV_MassSetDealPosAcc;

/**
 * Сброс признака учета по позиции на операции */
  PROCEDURE RSI_DV_UnsetDealPosAcc( pDealID IN NUMBER );

/**
 * Связывание сделок при посделочном учёте*/
   PROCEDURE RSI_DV_LinkDeals( v_DvoperID IN INTEGER );

   /**
 * Процедура отката связывания сделок при посделочном учёте*/
   PROCEDURE RSI_DV_RecoilLinkDeals( v_DvoperID IN INTEGER );

/**
 * Включение ПП в неттинг*/
   PROCEDURE RSI_DV_SetNettingPMGR( v_PMGRID IN INTEGER );

/**
 * Исключение ПП из неттинга*/
   PROCEDURE RSI_DV_UnsetNettingPMGR( v_PMGRID IN INTEGER );

/**
 * Процедура связывания сделок при исполнении */
  PROCEDURE RSI_DV_ExecDeals( v_ExecID IN INTEGER /* Операция исполнения */ );

/**
 * Процедура отката связывания сделок при исполнении */
  PROCEDURE RSI_DV_RecoilExecDeals( v_ExecID IN INTEGER /* Операция исполнения */ );

/**
 * Привязка используемой записи по итогам дня по сделке */
  PROCEDURE RSI_DV_AttachDealTurn( v_DealID   IN INTEGER,             -- Сделка
                                   v_Date     IN DATE,                -- Дата
                                   v_InTrg    IN CHAR DEFAULT CHR(0), -- Признак вызова из триггера
                                   v_DealCost IN NUMBER DEFAULT 0     -- Сумма по сделке
                                 );

/**
 * Освобождение используемой записи по итогам дня по сделке */
  PROCEDURE RSI_DV_DetachDealTurn( v_DealID  IN INTEGER,            -- Сделка
                                   v_Date    IN DATE,               -- Дата
                                   v_InTrg   IN CHAR DEFAULT CHR(0) -- Признак вызова из триггера
                                 );

/**
 * Вставка итогов по сделке */
  PROCEDURE RSI_DV_InsertDealTurn( v_DealID          IN INTEGER, -- Сделка
                                   v_Date            IN DATE,    -- Дата
                                   v_Margin          IN NUMBER,  -- Вариационная маржа
                                   v_Guaranty        IN NUMBER,  -- Гарантийное обеспечение
                                   v_FairValueCalc   IN CHAR,    -- Признак расчета справедливой стоимости
                                   v_FairValue       IN NUMBER,  -- Справедливая стоимость
                                   v_InsertMargin    IN INTEGER, -- Вставка вариационной маржи
                                   v_InsertGuaranty  IN INTEGER, -- Вставка гарантийного обеспечения
                                   v_InsertFairValue IN INTEGER, -- Вставка справедливой стоимости
                                   v_Action          IN INTEGER  -- Действие
                                 );
/**
 * Массовый откат вставки данных по сделке */
  PROCEDURE RSI_DV_MassRecoilInsertDealTurn( v_DealID   IN INTEGER, -- Сделка
                                             v_DvoperID IN INTEGER,    -- Операция расчётов
                                             v_Action   IN INTEGER  -- Действие
                                           );

/**
 * Откат вставки данных по сделке */
  PROCEDURE RSI_DV_RecoilInsertDealTurn( v_DealID IN INTEGER, -- Сделка
                                         v_Date   IN DATE,    -- Дата
                                         v_Action IN INTEGER  -- Действие
                                       );

/**
 * Вставка рассчитанных данных по сделке */
  PROCEDURE RSI_DV_CalcDealTurn( v_DealID        IN INTEGER, -- Сделка
                                 v_Date          IN DATE,    -- Дата
                                 v_Margin        IN NUMBER,  -- Вариационная маржа
                                 v_FairValue     IN NUMBER,  -- Справедливая стоимость
                                 v_CalcSum       IN INTEGER, -- Расчет суммы вариационной маржи 1=Да, 0=Нет
                                 v_CalcFairValue IN INTEGER, -- Расчет справедливой стоимости
                                 v_Action        IN INTEGER  -- Тип изменения итогов дня по сделке
                               );
/**
 * Вставка рассчитанного гар.обеспечения по сделке */
  PROCEDURE RSI_DV_GuarantDealTurn( v_DealID        IN INTEGER, -- Сделка
                                    v_Date          IN DATE,    -- Дата
                                    v_Guaranty        IN NUMBER,  -- гар.обеспечения
                                    v_Action        IN INTEGER  -- Тип изменения итогов дня по сделке
                                    );
/**
 * Импорт данных по сделке
 * @since 6.20.030 @qtest-YES
 * @param v_DealID          Сделка
 * @param v_Date            Дата
 * @param v_Margin          Вариационная маржа
 * @param v_Guaranty        Гарантийное обеспечение
 * @param v_ImportMargin    Импорт вариационной маржи 1=Да, 0=Нет
 * @param v_ImportGuaranty  Импорт гарантийного обеспечения 1=Да, 0=Нет
 */
  PROCEDURE RSI_DV_ImportDealTurn( v_DealID          IN INTEGER,
                                   v_Date            IN DATE,
                                   v_Margin          IN NUMBER,
                                   v_Guaranty        IN NUMBER,
                                   v_ImportMargin    IN INTEGER,
                                   v_ImportGuaranty  IN INTEGER
                                 );

/**
 * Вставка данных по сделке */
  PROCEDURE RSI_DV_InputDealTurn( v_DealID         IN INTEGER, -- Сделка
                                  v_Date           IN DATE,    -- Дата
                                  v_Margin         IN NUMBER,  -- Вариационная маржа
                                  v_Guaranty       IN NUMBER,  -- Гарантийное обеспечение
                                  v_FairValueCalc  IN CHAR,    -- Признак расчета справедливой стоимости
                                  v_FairValue      IN NUMBER   -- Справедливая стоимость
                                );

/**
 * Редактирование данных по сделке */
  PROCEDURE RSI_DV_EditDealTurn( v_DealID         IN INTEGER, -- Сделка
                                 v_Date           IN DATE,    -- Дата
                                 v_Margin         IN NUMBER,  -- Вариационная маржа
                                 v_Guaranty       IN NUMBER,  -- Гарантийное обеспечение
                                 v_FairValueCalc  IN CHAR,    -- Признак расчета справедливой стоимости
                                 v_FairValue      IN NUMBER,  -- Справедливая стоимость
                                 v_EditMargin     IN INTEGER, -- Редактирование вариационной маржи 1=Да, 0=Нет
                                 v_EditGuaranty   IN INTEGER, -- Редактирование гарантийного обеспечения 1=Да, 0=Нет
                                 v_EditFairValue  IN INTEGER  -- Редактирование справедливой стоимости 1=Да, 0=Нет
                               );

   -- Массовая вставка данных по сервисной операции
  PROCEDURE RSI_DV_MassSaveServAction( v_ID_Operation IN INTEGER, -- ID операции расчётов
                                       v_ID_Step      IN INTEGER  -- ID шага расчётов
                                     );

   -- Массовый откат данных по сервисной операции
  PROCEDURE RSI_DV_MassRecoilServAction( v_ID_Operation IN INTEGER, -- ID операции расчётов
                                         v_ID_Step      IN INTEGER  -- ID шага расчётов
                                       );

  -- Массовая вставка связей проводок со сделками срочного рынка
  PROCEDURE RSI_DV_MassInsertDvdealTrnLink  ( v_ID_Operation IN INTEGER, -- ID операции расчётов
                                              v_ID_Step      IN INTEGER  -- ID шага расчётов
                                           );
                                       
  -- Массовый откат связей проводок со сделками срочного рынка
  PROCEDURE RSI_DV_MassRecoilDvdealTrnLink  ( v_ID_Operation IN INTEGER, -- ID операции расчётов
                                              v_ID_Step      IN INTEGER  -- ID шага расчётов
                                           );                                      

  -- Изменение признака обработки биржевой информации по процентному платежу
  PROCEDURE RSI_DV_SetPmGrGt  ( v_DealCode IN VARCHAR2, -- Код сделки
                                v_Date     IN DATE,     -- Дата изменения данных ПП
                                v_Action   IN INTEGER   -- 1 - установка признака, 0 - снятие признака
                              );

/**
 * Выполняет синхронное обновление итогов дня по позиции */
  PROCEDURE RSI_DV_OnUpdateDealTurn( v_FIID                IN INTEGER, -- производный инструмент
                                     v_Department          IN INTEGER, -- Филиал
                                     v_Broker              IN INTEGER, -- Брокер
                                     v_ClientContr         IN INTEGER, -- Договор с клиентом
                                     v_Date                IN DATE,    -- Дата
                                     v_Margin              IN NUMBER,  -- Вариационная маржа
                                     v_Guaranty            IN NUMBER,  -- Гарантийное обеспечение
                                     v_FairValue           IN NUMBER,  -- Справедливая стоимость
                                     v_FairValueCalcNumber IN INTEGER, -- Изменение числа расчетов справедливой стоимости -1/0/+1
                                     v_CreateErr           IN BOOLEAN, -- Генерировать ошибку при отсутствии записи
                                     v_GenAgrID            IN INTEGER  -- ГС
                                   );

/**
 * Переоценка стоимости по сделке */
  PROCEDURE RSI_DV_OvervalueDealTurn( v_DealID   IN INTEGER, -- Сделка
                                      v_Date     IN DATE,    -- Дата
                                      v_DealCost IN NUMBER,  -- Стоимость неисполненных контрактов
                                      v_OperID   IN INTEGER, -- ID операции переоценки
                                      v_Flag     IN INTEGER  -- Вид переоценки
                                    );

/**
 * Откат переоценки стоимости по сделке */
  PROCEDURE RSI_DV_RecoilOvervalueDealTurn( v_DealID   IN INTEGER, -- Сделка
                                            v_Date     IN DATE,    -- Дата
                                            v_OperID   IN INTEGER, -- ID операции переоценки
                                            v_Flag     IN INTEGER  -- Вид переоценки
                                          );

/**
 * Закрытие сделок */
  PROCEDURE RSI_DV_CloseDeals( v_DvoperID     IN INTEGER  /* Операция расчётов*/ );

/**
 * Откат закрытия сделок */
  PROCEDURE RSI_DV_RecoilCloseDeals( v_DvoperID     IN INTEGER  /* Операция расчётов*/ );

/**
 * Изменение статуса внебиржевой сделки */
  PROCEDURE RSI_DV_SetStateNDeal( v_DealID IN INTEGER, -- ID сделки
                                  v_State  IN INTEGER  -- Статус
                                );
/**
 * Выполняет сохранение архивных данных промежуточного платежа */
  PROCEDURE RSI_DVSavePm( pPmID         IN NUMBER, -- Изменяемый платеж
                          pID_Operation IN NUMBER, -- Операция
                          pID_Step      IN NUMBER, -- Шаг операции
                          pChangeDate   IN DATE,   -- Дата изменения
                          pAction       IN NUMBER  -- Действие
                        );

/**
 * Выполняет восстановление платежа по архивным данным */
  PROCEDURE RSI_RestorePM( pID_Operation IN NUMBER, -- Операция
                           pID_Step      IN NUMBER  -- Шаг операции
                         );

/**
 * Переоценка промежуточного платежа */
  PROCEDURE RSI_DVOvervaluePm( pPmID           IN NUMBER, -- Изменяемый платеж
                               pID_Operation   IN NUMBER, -- Операция
                               pID_Step        IN NUMBER, -- Шаг операции
                               pChangeDate     IN DATE,   -- Дата изменения
                               pDemandSum      IN NUMBER, -- Новая сумма требования
                               pLiabilitySum   IN NUMBER, -- Новая сумма обязательства
                               pPaymentSum     IN NUMBER, -- Новая сумма платежа
                               pFloatRateValue IN NUMBER, -- Новая процентная ставка
                               pAction         IN NUMBER  -- Действие
                             );

/**
 * Перенос по срокам промежуточного платежа */
  PROCEDURE RSI_DVTransferPm( pPmID             IN NUMBER,   -- Изменяемый платеж
                              pID_Operation     IN NUMBER,   -- Операция
                              pID_Step          IN NUMBER,   -- Шаг операции
                              pChangeDate       IN DATE,     -- Дата изменения
                              pTransferDate     IN DATE,     -- Новая дата переноса
                              pDemandAccount    IN VARCHAR2, -- Новый счет требований
                              pLiabilityAccount IN VARCHAR2, -- Новый счет обязательства
                              pAction           IN NUMBER DEFAULT DV_PMGR_CHANGE_TRANSFER   -- Вид изменения в истории
                            );

/**
 * Функция получения ID календаря по ФИ */
  FUNCTION RSI_DV_GetLinkCalKind( pFIID in NUMBER  -- Идентификатор ФИ
                                ) RETURN NUMBER;

/**
 * Процедура генерации и актуализации списка календарей */
  PROCEDURE RSI_DV_CreateCALKIND( pDealID in NUMBER  -- Идентификатор сделки
                                );

/**
 * Функция получения даты платежа с корректировкой по календарям */
  FUNCTION RSI_DV_GetPayDate( pDealID    IN NUMBER,  -- Сделка
                              pFIID      IN NUMBER,  -- Актив
                              pSide      IN NUMBER,  -- Сторона
                              pTypeCalc  IN NUMBER,  -- Тип расчета
                              pEndDate   IN DATE     -- Дата окончания
                            ) RETURN DATE;

/**
 * Функция получения даты фиксации с корректировкой по календарям */
  FUNCTION RSI_DV_GetFixDate( pDealID    IN NUMBER,  -- Сделка
                              pFIID      IN NUMBER,  -- Актив
                              pSide      IN NUMBER,  -- Сторона
                              pTypeCalc  IN NUMBER,  -- Тип расчета
                              pBegDate   IN DATE,    -- Дата начала
                              pFixDays   IN NUMBER,  -- Дней до даты фиксации
                              pCalKindID IN NUMBER   -- Календарь из параметров стороны СВОПа
                            ) RETURN DATE;

/**
 * Процедура обновления дат в строках графика */
  PROCEDURE RSI_DV_UpdatePMGR_Dates( pID      IN NUMBER,   -- Строка графика платежей
                                     pDealID  IN NUMBER,   -- Сделка
                                     pSide    IN NUMBER,   -- Сторона
                                     pPayDate IN DATE,     -- Дата платежа
                                     pBegDate IN OUT DATE, -- Дата начала
                                     pEndDate IN OUT DATE  -- Дата окончания
                                   );

/**
 * Процедура обновления дат в строках графика */
  PROCEDURE RSI_DV_UpdatePMGR_Dates_TMP( pID      IN NUMBER,   -- Строка графика платежей
                                         pDealID  IN NUMBER,   -- Сделка
                                         pSide    IN NUMBER,   -- Сторона
                                         pPayDate IN DATE,     -- Дата платежа
                                         pBegDate IN OUT DATE, -- Дата начала
                                         pEndDate IN OUT DATE  -- Дата окончания
                                   );

/**
 * Процедура обновления и проверки дат в строках графика - даты окончания в предыдущей строке и даты начала в следующей */
  PROCEDURE RSI_DV_SetPMGR_Dates_prev_next( pMode    IN  NUMBER, -- Режим работы процедуры, возможные значения - (1 - проверка, 2 - расчет)
                                            pID      IN  NUMBER, -- Текущая строка графика платежей
                                            pBegDate IN  DATE,   -- Новая дата начала в текущей строке графика
                                            pEndDate IN  DATE,   -- Новая дата окончания в текущей строке графика
                                            pWarning OUT NUMBER  -- Код предупреждения
                                          );

/**
 * Используются ли срочные счета при учете сделки
 * @since 6.20.031
 * @qtest NO
 * @param pID Идентификатор сделки
 * @return 1 - срочные счета используются; 1- срочные счета не используются
 */
  FUNCTION DV_UseUrgentAccount( pID in NUMBER  -- Идентификатор сделки
                              ) RETURN NUMBER;

/**
 * Получение номера договора для начисления комиссий ПЗО по биржевой операции */
  FUNCTION RSI_GetSfContrID( p_DealID IN NUMBER ) RETURN NUMBER;

/**
 * Получение контрагента по биржевой операции */
  FUNCTION RSI_DVGetContractorID( p_DealID IN NUMBER ) RETURN NUMBER;

/**
 * Отбор комиссий по биржевой сделке
 * @since 6.20.031
 * @qtest NO
 * @return 0 в случае успеха, !0 в случае ошибки
 */
  FUNCTION DV_ChooseComBatch RETURN INTEGER;

/**
 * Получение алгоритма для единовременной комиссии */
  FUNCTION DV_GetAlgNameCom( p_DealID IN NUMBER ) RETURN VARCHAR2;

/**
 * Получение алгоритма из таблицы dsfcalcal_dbt */
  FUNCTION RSI_DV_GetAlgNameSfCalCal( p_CalcalID IN NUMBER ) return VARCHAR2;

/**
 * Получение базовой суммы для расчета единовременной комиссии */
  FUNCTION DV_GetBaseQuont( p_DealID IN NUMBER, p_CalcalID IN NUMBER ) return NUMBER;

/**
 * Получение идентификатора субъекта по коду
 * @since 6.20.031
 * @qtest NO
 * @param p_Code     Код
 * @param p_CodeKind Вид кода
 * @return Идентификатор субъекта
 */
  FUNCTION DV_GetPartyIDByCode( p_Code IN VARCHAR2, p_CodeKind IN NUMBER ) RETURN NUMBER;

/**
 * Получение идентификатора ММВБ
 * @since 6.20.031
 * @qtest NO
 * @return Идентификатор ММВБ
 */
  FUNCTION DV_GetPartyIDMMVB RETURN NUMBER;

/**
 * Получение идентификатора НРД
 * @since 6.20.031
 * @qtest NO
 * @return Идентификатор НРД
 */
  FUNCTION DV_GetPartyIDNRD RETURN NUMBER;

/**
 * Массово установить статус для ПД
 * @since 6.20.031
 * @qtest NO
 */
  PROCEDURE RSI_DV_MassCloseDeal;

/**
 * Получение идентификатора базового актива, но не ПИ
 * @since 6.20.031
 * @qtest NO
 * @param pFIID Идентификатор ФИ
 * @return Идентификатор базового актива
 */
  FUNCTION DV_BaseFINotDV( pFIID IN NUMBER ) RETURN NUMBER;

/**
 * Значение категории "Отнесение к ФИСС" для внебиржевой сделки и сделки КО в ПИ
 * @since 6.20.031
 * @qtest NO
 * @param pDealID Идентификатор сделки
 * @param pObjectType Тип объекта
 * @return Значение категории "Отнесение к ФИСС"
 */
  FUNCTION DV_DealAttrIsFISS( pDealID IN NUMBER, pObjectType IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Проверка является ли внебиржевая сделка или сделка Т+3 ФИСС
 * @since 6.20.031
 * @qtest NO
 * @param pDealID Идентификатор сделки
 * @return 0 - нет, !0 - да
 */
  FUNCTION DV_NDealIsFISS( pDealID IN NUMBER ) RETURN NUMBER;

/**
 * Значение категории "Инструмент Хэджирования" для операции "Процентный СВОП" на дату
 * @since 6.20.031
 * @qtest NO
 * @param pDealID Идентификатор сделки
 * @param pOperationalDate Дата проверки
 * @return Значение категории "Инструмент Хэджирования"
 */
 /*FUNCTION DV_DealAttrIsHedge( pDealID IN NUMBER, pOperationalDate IN Date ) RETURN NUMBER DETERMINISTIC;*/

/**
 * Проверка наличия активных отношений хеджирования за дату
 * @since 6.20.031
 * @qtest NO
 * @param pDocID Идентификатор документа
 * @param pDocKind Вид документа
 * @param pCheckDate Дата проверки
 * @return 1 - отношения существуют, 0 - не существуют.
 */
 FUNCTION DV_ActiveHdgRelation( pDocID IN NUMBER, pDocKind IN NUMBER, pCheckDate IN Date) RETURN NUMBER DETERMINISTIC;

/*
*  Проставить/Снять на комиссию признак dvnacdl.t_ServOperID. Используется на шаге Экспорт ПЗО БО ЦБ.
*/
  PROCEDURE RSI_DV_SetCommisDVNACDLExport( pComID IN INTEGER, pServId IN INTEGER );

/*
*  Проставить/Снять на комиссию признак dvdlcom.t_ServOperID. Используется на шаге Экспорт ПЗО БО ЦБ.
*/
  PROCEDURE RSI_DV_SetCommisDVDLCOMExport( pComID IN INTEGER, pServId IN INTEGER );

/*
*  Процедура сохранения расчетных операций
*/
  PROCEDURE RSI_DV_SaveDV_VALUE( p_OperID IN NUMBER DEFAULT -1 );

/**
 *  Процедура удаления итогов с удалением последней сделки с ключевыми параметрами
 */
  PROCEDURE RSI_DV_DeleteTurnByLastDeal
            (
               v_FIID         IN INTEGER, -- производный инструмент
               v_DEPARTMENT   IN INTEGER, -- Филиал
               v_BROKER       IN INTEGER, -- Брокер
               v_ClientContr  IN INTEGER, -- Клиент договор
               v_Date         IN DATE,    -- Дата
               v_GenAgrID     IN INTEGER  -- ГС
            );

/**
 * Проверяет, готова ли сделка для квитовки данного вида платежа
 * @since 6.20.031
 * @qtest NO
 * @param v_DealID Идентификатор сделки
 * @param v_DocKind вид ПД
 * @param v_Purpose вид платежа
 * @return 1 - да, 0 - нет
 */
  FUNCTION DV_DealIsReadyForKvit( v_DealID IN NUMBER, v_DocKind IN NUMBER, v_Purpose IN NUMBER, v_PaymentId IN NUMBER ) RETURN NUMBER;

/**
 * Пересчёт итогов операции расчётов на валютном рынке */
PROCEDURE DV_SetTotal ( v_ID IN INTEGER, v_Total IN NUMBER, v_LateCom IN NUMBER );

/**
 * Связывание внебиржевых сделок на валютных торгах */
  PROCEDURE RSI_DV_LinkNDeals( v_CommID IN INTEGER );

/**
 * Откат связей внебиржевых сделок на валютных торгах */
  PROCEDURE RSI_DV_RecoilLinkNDeals( v_CommID IN INTEGER );

/**
 * Закрытие расчётов по ФИ */
 PROCEDURE RSI_DV_CurMarketCompleteCalc( v_DocID IN INTEGER, v_ID_Step IN INTEGER );

/**
 * Откат закрытия расчётов по ФИ */
 PROCEDURE RSI_DV_RecoilCurMarketCompleteCalc( v_DocID IN INTEGER, v_ID_Step IN INTEGER );

/**
 * Сохранение изменение номиналов */
  PROCEDURE RSI_DV_SaveHistoryFaceValue (DealID         IN INTEGER,
                                         Side           IN INTEGER,
                                         OldSum         IN NUMBER,
                                         NewSum         IN NUMBER,
                                         FIID           IN INTEGER,
                                         OldInstance    IN INTEGER,
                                         ID_Operation   IN INTEGER,
                                         ID_Step        IN INTEGER,
                                         ChangeDate     IN DATE);

/**
 * Откат истории изменения номиналов */
  PROCEDURE RSI_DV_BackOutHistoryFaceValue (v_DealID         IN INTEGER,
                                            v_Instance       IN INTEGER,
                                            v_ID_Operation   IN INTEGER,
                                            v_ID_Step        IN INTEGER);

/**
 * Проверяет, является ли сделка сделкой валютного рынка, принятой в клиринг
 * @since 6.20.031
 * @qtest NO
 * @param v_DealID Идентификатор сделки
 * @param v_OperDate Дата торгового дня, в который осуществляется клиринг
 * @param v_OperSubKind Принадлежность средств
 * @return 1 - да, 0 - нет
 */
  FUNCTION DV_CurNDealInCliring( v_DealID IN NUMBER, v_OperDate IN DATE, v_OperSubKind IN NUMBER ) RETURN NUMBER;

/**
 * Проверяет, является ли сделка сделкой рынка СПФИ, принятой в клиринг
 * @since 6.20.031
 * @qtest NO
 * @param v_DealID Идентификатор сделки
 * @param v_OperDate Дата торгового дня, в который осуществляется клиринг
 * @param v_OperSubKind Принадлежность средств
 * @return 1 - да, 0 - нет
 */
  FUNCTION DV_SPFI_NDealInCliring( v_DealID IN NUMBER, v_OperDate IN DATE, v_OperSubKind IN NUMBER ) RETURN NUMBER;

/**
 * Проверяет, является ли биржевая сделка сделкой срочного рынка, принятой в клиринг
 * @since 6.20.031
 * @qtest NO
 * @param v_DealID Идентификатор сделки
 * @param v_OperDate Дата торгового дня, в который осуществляется клиринг
 * @param v_OperSubKind Принадлежность средств
 * @return 1 - да, 0 - нет
 */
  FUNCTION DV_DerivDealInCliring( v_DealID IN NUMBER, v_OperDate IN DATE, v_OperSubKind IN NUMBER ) RETURN NUMBER;

/**
 * Процедура формирования проводок по неттингу требований\обязательств в операции обработки итогов торгов (во временной таблице)
 * @since 6.20.031
 * @qtest NO
 * @param p_DocID ID операции обработки итогов торгов
 * @param p_DocKind вид документа операции обработки итогов торгов
 */
  PROCEDURE DV_FillNtgAccTrnTmpByOp(p_DocID IN NUMBER, p_DocKind IN NUMBER);

/**
 * Процедура формирования проводок по неттингу требований\обязательств в операции обработки итогов торгов СПФИ (во временной таблице)
 * @since 6.20.031
 * @qtest NO
 * @param p_DocID ID операции обработки итогов торгов
 * @param p_DocKind вид документа операции обработки итогов торгов
 */
  PROCEDURE DV_FillNtgAccTrnTmpByOpSPFI(p_DocID IN NUMBER, p_DocKind IN NUMBER);

/**
 * Проверяет, является ли сделка сделкой с встроенным ПФИ
 * @since 6.20.031
 * @qtest NO
 * @param v_DealID Идентификатор сделки
 * @return 1 - да, 0 - нет
 */
  FUNCTION DV_DealIsInPFI( v_DealID IN NUMBER) RETURN NUMBER;

  /**
 * Проверяет, является ли СВОП валютно-процентным
 * @since 6.20.031
 * @qtest NO
 * @param v_DealID Идентификатор сделки
 * @return 1 - да, 0 - нет
 */
  FUNCTION DV_IsCurPcSWAP( v_DealID IN NUMBER) RETURN NUMBER;


  /**
 * Проверяет, есть ли остаток на счетах хедж. ДП дл сделки
 * @since 6.20.031
 * @qtest NO
 * @param DealID Идентификатор сделки
 * @param DocKind Вид документа сделки
 * @param SvOpDate Дата СО
 * @return 1 - да, 0 - нет
 */
  FUNCTION IsHdgDPRestAccExists( DealID IN NUMBER, DocKind IN NUMBER,
                           SvOpDate IN DATE)
        RETURN NUMBER;
  /**
 * Получить дату первоначального признания для ОХ 
 * @since 6.20.031
 * @qtest NO
 * @param v_ObjId Идентификатор ОХ
 * @param v_ObjDocKind Вид документа ОХ
 * @param v_ObjType Тип ОХ
 * @param SvOpDate Дата СО
 * @return дата
 */
  FUNCTION GetFirstDateForObj (v_ObjId IN NUMBER, v_ObjDocKind IN NUMBER, v_ObjType IN NUMBER, v_SvOpDate IN DATE)
  RETURN DATE;
  
  /**
 * олучить дату предыдущей корректировки
 * @since 6.20.031
 * @qtest NO
 * @param v_DealID Идентификатор сделки
 * @param v_SvOpDate Дата СО
 * @return дата
 */
  FUNCTION GetPrevDateCorr (v_DealID IN NUMBER, v_SvOpDate IN DATE)
  RETURN DATE;
  
  /**
 * Заполнение таблицы для Корректировки РХДП
 * @since 6.20.031
 * @qtest NO
 * @param SvOpId Идентификатор СО
 * @param SvOpDate Дата СО
 * @param DealID Идентификатор сделки, если расчет выполняется для сделки
 */
  PROCEDURE FillHdgCorRHDPTable (SvOpId IN NUMBER, SvOpDate IN DATE, DealID IN NUMBER );
  
  /**
 * Проверка, есть ли проводки по счетам ДП
 * @since 6.20.031
 * @qtest NO
 * @param DealID Идентификатор сделки
 * @param DocKind Вид документа сделки
 * @return 1- есть 0 - нет
 */
  FUNCTION IsHdgDPExistsTrn( DealID IN NUMBER, DocKind IN NUMBER)
        RETURN NUMBER;
  /**
 * Проверка дат для амортизации РХДП
 * @since 6.20.031
 * @qtest NO
 * @param RelationID Идентификатор ОХ
 * @param SvOpDate Дата операции
 * @return 1- подходит под операцию по датам  0 - неподходит под операцию по датам
 */      
  FUNCTION HDGAmortCheckDate( RelationID IN NUMBER, SvOpDate IN DATE)
    RETURN NUMBER;

  /**
 * Блокировка операций для договоров, у которых категория "Сведения о наличии или отсутствии ИИС у другого ПУ" = "Да" И категория "Предоставлены подтвержд. документы о расторжении ИИС у другого ПУ" на дату операции равно "Нет" или не заполнено
 * @since 6.20.031
 * @qtest NO
 * @param p_ContrID Идентификатор субдоговора
 * @param p_DealDate Дата операции
 */    
  PROCEDURE DV_CheckContrIISwithAnotherSP(p_ContrID IN NUMBER,  p_DealDate IN DATE);

END RSB_Derivatives;
/