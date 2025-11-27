/**
 *  Author  : IAl
 *  Created : 15.09.2005
 *  Purpose : Secur
 *  Пакет Rsb_Secur для получения характеристик по сделкам и операциям с финансовами инструментами (ФИ)
 */
CREATE OR REPLACE PACKAGE Rsb_Secur
IS
       UnknownDate CONSTANT DATE := TO_DATE('01.01.0001', 'DD.MM.YYYY');
       MAX_DATE    CONSTANT DATE := TO_DATE('31.12.9999', 'DD.MM.YYYY');

       SUBTYPE DealTypeName_t         IS doprkoper_dbt.t_Name%type;
       SUBTYPE DealClientShortName_t  IS dparty_dbt.t_ShortName%type;
       SUBTYPE DealSfContrNumber_t    IS dsfcontr_dbt.t_Number%type;
       SUBTYPE DealContrShortName_t   IS dparty_dbt.t_ShortName%type;
       SUBTYPE DealFICcy_t            IS dfininstr_dbt.t_CCY%type;
       SUBTYPE DealDepName_t          IS ddp_dep_dbt.t_Name%type;
       SUBTYPE DealOperName_t         IS dperson_dbt.t_Name%type;

       SUBTYPE DealSupplyDate_t       IS ddl_leg_dbt.t_Maturity%type;
       SUBTYPE DealSupplyTime_t       IS ddl_leg_dbt.t_SupplyTime%type;
       SUBTYPE DealPayDate_t          IS ddl_leg_dbt.t_Maturity%type;
       SUBTYPE DealIsNett_t           IS NUMBER;
       SUBTYPE DealRejectDate_t       IS ddl_leg_dbt.t_RejectDate%type;


       SUBTYPE PmWrtGrpAvrName_t      IS davrkinds_dbt.t_Name%type;
       SUBTYPE PmWrtGrpFiCode_t       IS dfininstr_dbt.t_FI_Code%type;
       SUBTYPE PmWrtGrpFiName_t       IS dfininstr_dbt.t_Name%type;
       SUBTYPE PmWrtGrpFiFvCode_t     IS dfininstr_dbt.t_ISO_Number%type;


/**
 * Системные типы видов операций */
       TYPEKOPER_BUY          CONSTANT VARCHAR2(1) := 'B'; -- Покупка
       TYPEKOPER_SALE         CONSTANT VARCHAR2(1) := 'S'; -- Продажа
       TYPEKOPER_CALL         CONSTANT VARCHAR2(1) := 'C'; -- Опцион CALL
       TYPEKOPER_PUT          CONSTANT VARCHAR2(1) := 'P'; -- Опцион PUT
       TYPEKOPER_EXCHANGE     CONSTANT VARCHAR2(1) := 'X'; -- Биржевая сделка
       TYPEKOPER_CONVERS      CONSTANT VARCHAR2(1) := 'V'; -- Конверсионная операция
       TYPEKOPER_SWAP         CONSTANT VARCHAR2(1) := 'W'; -- Своп
       TYPEKOPER_OPTION       CONSTANT VARCHAR2(1) := 'O'; -- Опцион
       TYPEKOPER_FUTURES      CONSTANT VARCHAR2(1) := 'U'; -- Фьючерс
       TYPEKOPER_MOVING       CONSTANT VARCHAR2(1) := 'M'; -- Смена места хранения (ценной бумаги)
       TYPEKOPER_INDEX2       CONSTANT VARCHAR2(1) := '2'; -- Платежное поручение через картотеку N2
       TYPEKOPER_MMARK        CONSTANT VARCHAR2(1) := 'D'; -- Признак процентной операции денежного рынка
       TYPEKOPER_FLTRATE      CONSTANT VARCHAR2(1) := 'i'; -- Плавающая ставка
       TYPEKOPER_INDXFLTRATE  CONSTANT VARCHAR2(1) := 'f'; -- Индексированная плавающая ставка
       TYPEKOPER_LOMBARD      CONSTANT VARCHAR2(1) := 'p'; -- Залоговое кредитование; ломбард
       TYPEKOPER_ONCALL       CONSTANT VARCHAR2(1) := 'o'; -- До востребования
       TYPEKOPER_INDEX1       CONSTANT VARCHAR2(1) := '1'; -- Платежное поручение через картотеку N1
       TYPEKOPER_CONVERT      CONSTANT VARCHAR2(1) := 'k'; -- Конвертирование
       TYPEKOPER_REJECT       CONSTANT VARCHAR2(1) := 'R'; -- Отвергнутый документ
       TYPEKOPER_DEPOSIT      CONSTANT VARCHAR2(1) := 'd'; -- Депозитарная операция
       TYPEKOPER_FULL         CONSTANT VARCHAR2(1) := 'F'; -- Полная (есть бухучет в МБК)
       TYPEKOPER_TREASURY     CONSTANT VARCHAR2(1) := 'T'; -- Работают в казначей
       TYPEKOPER_AKKREDITIV   CONSTANT VARCHAR2(1) := 'A'; -- Рублёвый аккредитив
       TYPEKOPER_CONV         CONSTANT VARCHAR2(1) := 'K'; -- Конверсия
       TYPEKOPER_LOANS        CONSTANT VARCHAR2(1) := 'L'; -- Обработка платежа в Лоанс
       TYPEKOPER_CLB          CONSTANT VARCHAR2(1) := 'C'; -- Клиент банка
       TYPEKOPER_RDD_BO       CONSTANT VARCHAR2(1) := 'Б'; -- РДД из БО
       TYPEKOPER_BANK         CONSTANT VARCHAR2(1) := 'b'; -- банк
       TYPEKOPER_MARKET       CONSTANT VARCHAR2(1) := 'm'; -- биржа
       TYPEKOPER_BACK         CONSTANT VARCHAR2(1) := 'c'; -- обратная
       TYPEKOPER_FREE         CONSTANT VARCHAR2(1) := 's'; -- свободная
       TYPEKOPER_ALWAYS       CONSTANT VARCHAR2(1) := 'w'; -- обязательная
       TYPEKOPER_ISSUE        CONSTANT VARCHAR2(1) := 'r'; -- Погашение выпуска
       TYPEKOPER_COUPON       CONSTANT VARCHAR2(1) := 'n'; -- Погашение купона
       TYPEKOPER_PARTLY       CONSTANT VARCHAR2(1) := 'x'; -- Частичное погашение
       TYPEKOPER_ADDINCOME    CONSTANT VARCHAR2(1) := 'i'; -- Частичное погашение
       TYPEKOPER_BROKER       CONSTANT VARCHAR2(1) := 'e'; -- Брокерское
       TYPEKOPER_OUTEXCHANGE  CONSTANT VARCHAR2(1) := 'v'; -- Внебиржевая сделка
       TYPEKOPER_BACKSALE     CONSTANT VARCHAR2(1) := 'l'; -- С обратной продажей
       TYPEKOPER_REPO         CONSTANT VARCHAR2(1) := 't'; -- РЕПО
       TYPEKOPER_AVRWRTIN     CONSTANT VARCHAR2(1) := 'Z'; -- Зачисление ц/б
       TYPEKOPER_AVRWRTOUT    CONSTANT VARCHAR2(1) := 'G'; -- Списание ц/б
       TYPEKOPER_CONV_SHARE   CONSTANT VARCHAR2(1) := 'A'; -- Конвертация акций
       TYPEKOPER_CONV_RECEIPT CONSTANT VARCHAR2(1) := 'D'; -- Конвертация деп. расп.
       TYPEKOPER_IFRSFORMCOR  CONSTANT VARCHAR2(1) := 'К'; -- Формирование КП
       TYPEKOPER_PAYCOM       CONSTANT VARCHAR2(1) := 'П'; -- Оплата коммисии
       TYPEKOPER_EXEC_DV      CONSTANT VARCHAR2(1) := 'E'; -- Исполнение ПИ
       TYPEKOPER_LOAN         CONSTANT VARCHAR2(1) := 'z'; -- Займ
       TYPEKOPER_BASKET       CONSTANT VARCHAR2(1) := 'K'; -- Корзина ЦБ
       TYPEKOPER_TODAY        CONSTANT VARCHAR2(1) := 'T'; -- TODAY
       TYPEKOPER_DVFX_SWAP    CONSTANT VARCHAR2(1) := 'S'; -- Конверсионная операция СВОП в ПИ
       TYPEKOPER_DEAL_KSU     CONSTANT VARCHAR2(1) := 'C'; -- Сделка с КСУ
       TYPEKOPER_INTERDEALER_REPO CONSTANT VARCHAR2(1) := 'D'; -- Сделка междиллерского РЕПО
       TYPEKOPER_OTC          CONSTANT VARCHAR2(1) := 'Q'; -- Cделка внебиржевая ОТС
       TYPEKOPER_DVFX_DEAL    CONSTANT VARCHAR2(1) := 'D'; -- Конверсионная операция Покупка/продажа в ПИ

       TYPEKOPER_DIVIDEND_RETURN_REPO  CONSTANT VARCHAR2(1) :=  'r';  --// Возврат дивидендов по РЕПО
       TYPEKOPER_GET_DIVIDEND_REPO     CONSTANT VARCHAR2(1) :=  'R';  --// Получение дивидендов по РЕПО
       TYPEKOPER_GET_DIVIDEND_EMITENT CONSTANT VARCHAR2(1)  :=  'I';   --// Получение дивидендов от Эмитента

       TYPEKPOPER_SHORTSWAP   CONSTANT VARCHAR2(1) := 'H';   -- Короткий СВОП

/**
 * Все числа в шестнадцатиричной системе */
       IS_SALE        CONSTANT VARCHAR2(10) := '01';  --(00000001) Продажа
       IS_PUT         CONSTANT VARCHAR2(10) := '02';  --(00000010) Опцион "PUT" (только для опционов)
       IS_EXCHANGE    CONSTANT VARCHAR2(10) := '04';  --(00000100) Биржевая сделка (только для опционов и фьючерсов)
       DEAL_TYPE      CONSTANT VARCHAR2(10) := '38';  --(00111000) Маска для определения типа сделки
       DT_CONVERS     CONSTANT VARCHAR2(10) := '00';  --(00000000) Тип - Конверсионная операция
       DT_SWAP        CONSTANT VARCHAR2(10) := '08';  --(00001000) Тип - Своп (репо для ценных бумаг)
       DT_FUTURES     CONSTANT VARCHAR2(10) := '10';  --(00010000) Тип - Фьючерс
       DT_OPTION      CONSTANT VARCHAR2(10) := '18';  --(00011000) Тип - Опцион
       DT_MOVING      CONSTANT VARCHAR2(10) := '20';  --(00100000) Тип - Смена хранения (для ценных бумаг)
       DT_MMARK       CONSTANT VARCHAR2(10) := '28';  --(00101000) Тип - Процентная операция денежного рынка (для МБК)
       DT_DVFX_DEAL   CONSTANT VARCHAR2(10) := '002';
       DT_DVFX_SWAP   CONSTANT VARCHAR2(10) := '004';  --
       DT_SHORT_SWAP  CONSTANT VARCHAR2(10) := '36';  -- Короткий СВОП
       IS_FLTRATE     CONSTANT VARCHAR2(10) := '40';  --(01000000) Признак плавающей ставки
       IS_INDXFLTRATE CONSTANT VARCHAR2(10) := '80';  --(10000000) Признак "Индексированная плав. ставка"
       IS_CONV_SHARE  CONSTANT VARCHAR2(10) := '100';--(100000000) Признак "Конвертация акций в депозитарные расписки"
       IS_CONV_RECEIPT CONSTANT VARCHAR2(10) :='200';--(1000000000) Признак "Конвертация депозитарных расписок в акции"
       IS_ONCALL      CONSTANT VARCHAR2(10) := '100';--(100000000) Признак "до востребования"
       IS_FULL        CONSTANT VARCHAR2(10) := '200';--(1000000000) Признак "до востребования"
       IS_BUY         CONSTANT VARCHAR2(10) := '400';--(10000000000) Признак "покупка"
       IS_RET_ISSUE   CONSTANT VARCHAR2(10) := '800';--(100000000000) Признак "погашение ц/б"
       IS_RET_COUPON  CONSTANT VARCHAR2(10) := '1000';--(1000000000000) Признак "погашение купона"
       IS_OUTEXCHANGE  CONSTANT VARCHAR2(10) := '2000';--(10000000000000) Признак "внебиржевая сделка"
       IS_BROKER     CONSTANT VARCHAR2(10) := '4000';--(100000000000000) Признак "через брокера"
       IS_BACKSALE   CONSTANT VARCHAR2(10) := '8000';--(1000000000000000) Признак "с обратной продажей"
       IS_REPO       CONSTANT VARCHAR2(10) := '10000';--(10000000000000000) Признак "РЕПО"
       IS_AVRWRTIN   CONSTANT VARCHAR2(10) := '20000';--(100000000000000000) Зачисление ц/б
       IS_AVRWRTOUT  CONSTANT VARCHAR2(10) := '40000';--(1000000000000000000) Списание ц/б
       IS_RET_PARTLY CONSTANT VARCHAR2(10) := '80000';--(10000000000000000000) Частичное погашение
       IS_LOAN       CONSTANT VARCHAR2(10) := '100000';--(100000000000000000000) Займ
       IS_CALL       CONSTANT VARCHAR2(10) := '200000';--(1000000000000000000000) Опцион CALL (Forex)
       IS_EXEC_DV    CONSTANT VARCHAR2(10) := '400000';--(10000000000000000000000) Исполнение ПИ
       IS_OTC        CONSTANT VARCHAR2(10) := '2000000';--(10000000000000000000000000) Сделка ОТС
       IS_RET_ADDINCOME CONSTANT VARCHAR2(10) := '4000000';--(100000000000000000000000000) Погашение доп. дохода
       IS_TODAY      CONSTANT VARCHAR2(10) := '10000000';-- (100000000000000000000000000000) Признак today
       IS_BASKET     CONSTANT VARCHAR2(10) := '20000000';-- (100000000000000000000000000000) Корзина ЦБ
       IS_DEAL_KSU   CONSTANT VARCHAR2(10) := '40000000';-- Сделка с КСУ
       IS_INTERDEALER_REPO CONSTANT VARCHAR2(10) := '28';-- Сделка междиллерсокого репо

       IS_DIVIDEND_RETURN_REPO CONSTANT VARCHAR2(10) := '01';--(0000 0001) - Возвращение дивидендов по РЕПО
       IS_GET_DIVIDEND_REPO CONSTANT VARCHAR2(10) := '10';--(0000 0001 0000) - Получение дивидендов по РЕПО
       IS_GET_DIVIDEND_EMITENT CONSTANT VARCHAR2(10) := '20';--(0000 0010 0000) - Получение дивидендов от Эмитента


/**
 * виды ц/б */
       FIKIND_ALLAVOIRISS         CONSTANT NUMBER := RSI_RSB_FIInstr.FIKIND_ALLAVOIRISS;          -- Все ценные бумаги
       AVOIRKIND_EQUITY_SHARE     CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_EQUITY_SHARE     ; -- Акция обыкновенная
       AVOIRKIND_PREFERENCE_SHARE CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_PREFERENCE_SHARE ; -- Акция привилегированная
       AVOIRKIND_ORDINARY_BOND    CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_ORDINARY_BOND    ; -- Облигация бескупонная
       AVOIRKIND_COUPON_BOND      CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_COUPON_BOND      ; -- Облишация купонная
       AVOIRKIND_BILL             CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_BILL             ; -- Вексель
       AVOIRKIND_DRAFT            CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_DRAFT            ; -- Чек
       AVOIRKIND_PREF_CONV_SHARE  CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_PREF_CONV_SHARE  ; -- Акция привилегированная с правом конверсии
       AVOIRKIND_CONV_BOND        CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_CONV_BOND        ; -- Облигация с правом конверсии
       AVOIRKIND_DEPOS_CERTIF     CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_DEPOS_CERTIF     ; -- Депозитный сертификат
       AVOIRKIND_DEPOS_RECEIPT    CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_DEPOS_RECEIPT    ; -- Депозитарная расписка
       AVOIRKIND_PROMISSORY_NOTE  CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_PROMISSORY_NOTE  ; -- Долговое обязательство
       AVOIRKIND_SAVING_CERTIF    CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_SAVING_CERTIF    ; -- Сберегательный сертификат
       AVOIRKIND_STORAGE_CERTIF   CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_STORAGE_CERTIF   ; -- Складское свидетельство
       AVOIRKIND_COUPON_BOND_AD   CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_COUPON_BOND_AD   ; -- Облишация купонная АД
       AVOIRKIND_ORDINARY_BOND_AD CONSTANT NUMBER := RSI_RSB_FIInstr.AVOIRKIND_ORDINARY_BOND_AD ; -- Облигация бескупонная АД

/**
 * виды объектов */
       OBJTYPE_PARTY      CONSTANT NUMBER := 3;   -- Субъект экономики
       OBJTYPE_FININSTR   CONSTANT NUMBER := 9;   -- Финансовый инструмент
       OBJTYPE_AVOIRISS   CONSTANT NUMBER := 12;  -- ценная бумага
       OBJTYPE_FICERT     CONSTANT NUMBER := 24;  -- Сертификат документарной ЦБ
       OBJTYPE_SECDEAL    CONSTANT NUMBER := 101; -- сделка с ц/б
       OBJTYPE_NETTING    CONSTANT NUMBER := 108; -- сделка неттинга
       OBJTYPE_OPERATIONDEPO CONSTANT NUMBER := 109; -- Поручение на операцию депо
       OBJTYPE_RETIRE     CONSTANT NUMBER := 117; -- погашение ц/б
       OBJTYPE_AVRWRT     CONSTANT NUMBER := 127; -- списание/зачисление ц/б
       OBJTYPE_OPER_DV    CONSTANT NUMBER := 140; -- Операция с ПИ
       OBJTYPE_OUTOPER_DV CONSTANT NUMBER := 145; -- Внебиржевая операция с ПИ
       OBJTYPE_FXOPER_DV  CONSTANT NUMBER := 148; -- Конверсионная операция
       OBJTYPE_NTGSEC     CONSTANT NUMBER := 154; -- Документ неттинга БОЦБ
       OBJTYPE_SECUROWN   CONSTANT NUMBER := 180; -- Сделка СЭБ
       OBJTYPE_RETIREOWN  CONSTANT NUMBER := 181; -- Погашения СЭБ
       OBJTYPE_BROKERCONTR_DL CONSTANT NUMBER := 207; -- Договор брокерского обслуживания
       OBJTYPE_VEKSEL     CONSTANT NUMBER := 651; -- анкета векселя
       OBJTYPE_SFCONTR    CONSTANT NUMBER := 659; -- Договор обслуживания
       OBJTYPE_NPTXOBJ    CONSTANT NUMBER := 984; -- объект НДР
       OBJTYPE_DLRQ       CONSTANT NUMBER := 993; -- Объект ТО в БОЦБ
/**
 * виды примечаний сделок */
       NOTEKIND_DEAL_REGULAT CONSTANT NUMBER := 14; -- урегулирование сделки
       NOTEKIND_NETT_NUMDEAL CONSTANT NUMBER := 2;  -- номер итоговой сделки

/**
 * виды списаний */
       PM_WRITEOFF_LIFO    CONSTANT NUMBER := 0;
       PM_WRITEOFF_FIFO    CONSTANT NUMBER := 1;
       PM_WRITEOFF_MAXRATE CONSTANT NUMBER := 2;
       PM_WRITEOFF_MINRATE CONSTANT NUMBER := 3;
       PM_WRITEOFF_AVERAGE CONSTANT NUMBER := 4;

/**
 * виды лотов для sctaxlot */
       TAXLOTS_UNDEF    CONSTANT NUMBER := 0; -- не определено
       TAXLOTS_BUY      CONSTANT NUMBER := 1; -- покупка
       TAXLOTS_SALE     CONSTANT NUMBER := 2; -- продажа
       TAXLOTS_REPO     CONSTANT NUMBER := 3; -- репо прямое
       TAXLOTS_BACKREPO CONSTANT NUMBER := 4; -- репо обратное

/**
 * виды связей лотов для sctaxlnk */
       TAXLNK_UNDEF   CONSTANT NUMBER := 0;  -- не определно
       TAXLNK_BS      CONSTANT NUMBER := 1;  -- купля продажа
       TAXLNK_REPO    CONSTANT NUMBER := 2;  -- репо прямое
       TAXLNK_OPSPOS  CONSTANT NUMBER := 3;  -- открытие короткой позиции
       TAXLNK_CLPOS   CONSTANT NUMBER := 4;  -- закрытие короткой позиции
       TAXLNK_BREPO   CONSTANT NUMBER := 5;  -- возврат из прямого репо
       TAXLNK_DELIVER CONSTANT NUMBER := 6;  -- поставка
       TAXLNK_DELREPO CONSTANT NUMBER := 7;  -- поставка - прямое репо
       TAXLNK_DELBREP CONSTANT NUMBER := 8;  -- поставка - возврат из прямого репо
       TAXLNK_RETURN2 CONSTANT NUMBER := 9;  -- возврат 2
       TAXLNK_DELRET2 CONSTANT NUMBER := 10; -- поставка - возврат 2
       TAXLNK_RETSPOS CONSTANT NUMBER := 11; -- возврат в короткую позицию

/**
 * режим сбора данных для налоговых регистров */
       TAXREG_ORDINARY CONSTANT NUMBER := 1; -- стандартный
       TAXREG_NETTEXCL CONSTANT NUMBER := 2; -- с исключением неттинга

/**
 * ПД СО */
       DLDOC_ISSUE             CONSTANT NUMBER := 5;    -- Первичный документ "Анкета выпуска ценных бумаг"
       DLDOC_PAYMENT           CONSTANT NUMBER := 29;   -- Платеж/ТО
       DL_SECURITYDOC          CONSTANT NUMBER := 101;  -- Сделка с ценными бумагами
       DL_MOVINGDOC            CONSTANT NUMBER := 105;  -- Перемещение ценных бумаг
       DL_RESERVEDOC           CONSTANT NUMBER := 106;  -- Операции резервирования средств под обесценивание ЦБ
       DL_OVERVALUE            CONSTANT NUMBER := 108;  -- Операции переоценки ценных бумаг
       DL_VEKSELORDER          CONSTANT NUMBER := 109;  -- Вексельный договор
       DL_VSBARTERORDER        CONSTANT NUMBER := 113;  -- Договор мены векселей
       DL_RETIREMENT           CONSTANT NUMBER := 117;  -- Погашение выпуска
       DL_OVERVALUE_RD         CONSTANT NUMBER := 123;  -- Переоценка внебаланса
       DL_VSSALE               CONSTANT NUMBER := 124;  -- Договор продажи векселей
       DL_AVRWRT               CONSTANT NUMBER := 127;  -- Списание/зачисление ц/б
       DL_OVERVALUE_NVPI       CONSTANT NUMBER := 134;  -- Переоценка НВПИ
       DL_ISSUE_UNION          CONSTANT NUMBER := 135;  -- Конвертация выпусков ц/б
       DL_CONVAVR              CONSTANT NUMBER := 138;  -- Конвертация ц/б
       DL_CHGAVRNOM            CONSTANT NUMBER := 139;  -- Изменение номинала ц/б
       DL_RETURN_DIVIDEND      CONSTANT NUMBER := 151;  -- Возвращение дивидендов
       DL_NTGSEC               CONSTANT NUMBER := 154;  -- Документ неттинга БОЦБ
       DL_INVESTSHARE          CONSTANT NUMBER := 155;  -- Получение инвестиционных паев --UPD 82_5 константа исключена из дистрибутива, но встречается в RSB_REALIZREG, поэтому пока вернул. GAA
       DL_GET_INCOME           CONSTANT NUMBER := 157;  -- БОЦБ - начисление процентного/дисконтного дохода
       DL_VSBANNER             CONSTANT NUMBER := 164;  -- Анкета векселя
       DL_SECURLEG             CONSTANT NUMBER := 176;  -- Часть сделки с ц/б
       DL_DVOPER               CONSTANT NUMBER := 194;  -- Операция с ПИ
       DL_DVOPER_OVERVALUE     CONSTANT NUMBER := 196;  -- Переоценка ПИ
       DL_DVNDEAL              CONSTANT NUMBER := 199;  -- Внебиржевая сделка с ПИ
       DL_FIXING               CONSTANT NUMBER := 235;  -- Фиксинг
       SP_DEPOPER_DIVIDEND     CONSTANT NUMBER := 870;  -- Расчет выплаты доходов в Депозитарии
       DP_CALCCOMMRESERV       CONSTANT NUMBER := 875;  -- Операция расчета резерва по депозитарным комиссиям
       DL_DLINACC              CONSTANT NUMBER := 4601; -- Расчетные операции Внутреннего учета
       DL_CALCTSS              CONSTANT NUMBER := 4604; -- Процедура расчета ТСС
       DL_CALCNDFL             CONSTANT NUMBER := 4605; -- Операция расчета НОБ для НДФЛ
       DL_CLOSENDFL            CONSTANT NUMBER := 4606; -- Операция закрытия расчетных периодов для НДФЛ
       DL_WRTMONEY             CONSTANT NUMBER := 4607; -- Операция зачисления/списания денежных средств
       DL_HOLDNDFL             CONSTANT NUMBER := 4608; -- Операция удержания НДФЛ
       DL_INACCSRVOP           CONSTANT NUMBER := 4613; -- Операция внутреннего учета
       DL_OFFBALTRANSFSRVOP    CONSTANT NUMBER := 4614; -- Операция переноса по внебалансу
       DL_SCACCOUNTING         CONSTANT NUMBER := 4615; -- Операция бухгалтерского учета в БОЦБ
       DL_DEPOACCSRVOP         CONSTANT NUMBER := 4616; -- Операция депозитарного учета в БОЦБ
       DL_CRPAYMSRVOP          CONSTANT NUMBER := 4617; -- Операция выгрузки платежей по ТО в БОЦБ
       DL_TICK_ENS_DOC         CONSTANT NUMBER := 4620; -- обеспечение по сделке
       DL_RESTTRANS            CONSTANT NUMBER := 4621; -- Перенос остатков в КДУ
       SP_TRNCOLLAT            CONSTANT NUMBER := 4622; -- Перевод обеспечения
       SP_BLOCKBATCH           CONSTANT NUMBER := 4623; -- Блокировка партий
       SP_ACCEXPREVOEB         CONSTANT NUMBER := 4624; -- Начисление расходов и доходов ОЭБ
       SP_AJUSTVALOEB          CONSTANT NUMBER := 4625; -- Корректировка стоимости ОЭБ
       DV_CSA                  CONSTANT NUMBER := 4626; -- Соглашение CSA
       SP_IMPOWNLIST           CONSTANT NUMBER := 4630; -- Импорт списка владельцев ц/б
       DL_SETTLEMENTFCURM      CONSTANT NUMBER := 4632; -- Расчеты с биржей (переводы)
       DL_DVCURMARKET          CONSTANT NUMBER := 4633; -- Обработка итогов биржевых валютных торгов
       DL_RENUMBERINGDEALS     CONSTANT NUMBER := 4634; -- Перенумерация сделок/операций
       SP_SRVBROKERREP         CONSTANT NUMBER := 4635; -- Сервисная операция отправки отчета брокера
       SP_AMORTHEDGCORR        CONSTANT NUMBER := 4636; -- Амортизация корректировок хеджирования
       SP_AVRHDINSTR           CONSTANT NUMBER := 4637; -- Инструмент хеджирования для ц/б
       SP_AVRHDGRELATION       CONSTANT NUMBER := 4640; -- Инструмнт хеджирования для ц/б для таблицы DDLHDGRELATION_DBT
       DL_RETREQ               CONSTANT NUMBER := 4644; -- Заявление на возрват
       DL_NKDREQDIAS           CONSTANT NUMBER := 4651; -- Запрос УНКД от Диасофта
       SP_SRVBROKERREPNEW      CONSTANT NUMBER := 4660; -- Сервисная операция отправки отчета брокера новая
       SP_CALCRESERV           CONSTANT NUMBER := 4720; -- Расчет суммы обеспечения по резервам
       DL_DVFXDEAL             CONSTANT NUMBER := 4813; -- Конверсионная сделка ФИСС и КО
       DL_DVDEALT3             CONSTANT NUMBER := 4815; -- Сделка Т+3
       DL_SECUROWN             CONSTANT NUMBER := 4830; -- Покупка/продажа СЭБ
       DL_AVRWRTOWN            CONSTANT NUMBER := 4831; -- Списание/Зачисление СЭБ
       DL_RETIREMENT_OWN       CONSTANT NUMBER := 4832; -- Погашение СЭБ
       SP_TRANSFERPA           CONSTANT NUMBER := 4833; -- Перечисление средств платежному агенту
       SP_COMISSION_PA         CONSTANT NUMBER := 4834; -- Оплата комиссии платежному агенту
       DV_SRVOP_AMORT_RHDP     CONSTANT NUMBER := 4840; -- Амортизация РХДП
       REPOS_SERVICEOPERATION  CONSTANT NUMBER := 4851; -- СОРУ
       REPOS_MESSAGEGENERATION CONSTANT NUMBER := 4852; -- Генерация сообщений
       REPOS_GENERALINF        CONSTANT NUMBER := 4853; -- СРС
       REPOS_CM022             CONSTANT NUMBER := 4854; -- связка экономических параметров CM022 со строкой графика
       REPOS_CM023             CONSTANT NUMBER := 4855; -- связка экономических параметров CM023 со строкой графика
       REPOS_CM041             CONSTANT NUMBER := 4856; -- связка экономических параметров CM041 со строкой графика
       REPOS_CM083             CONSTANT NUMBER := 4857; -- связка экономических параметров CM083 со строкой графика
       REPOS_CM093             CONSTANT NUMBER := 4858; -- связка экономических параметров CM093 со строкой графика
       REPOS_CM021             CONSTANT NUMBER := 4862; -- связка экономических параметров CM021 со строкой графика
       REPOS_CM051             CONSTANT NUMBER := 4863; -- связка экономических параметров CM051 со строкой графика
       REPOS_CM094             CONSTANT NUMBER := 4864; -- связка экономических параметров CM094 со строкой графика
       REPOS_CM032             CONSTANT NUMBER := 4865; -- связка экономических параметров CM032 со строкой графика
       REPOS_CM042             CONSTANT NUMBER := 4866; -- связка экономических параметров CM042 со строкой графика
       REPOS_CM046             CONSTANT NUMBER := 4867; -- связка экономических параметров CM046 со строкой графика
       REPOS_CM053             CONSTANT NUMBER := 4868; -- связка экономических параметров CM053 со строкой графика
       REPOS_INBOUNDMSG        CONSTANT NUMBER := 4869; -- Операция обработки входящих сообщений
       REPOS_CM043             CONSTANT NUMBER := 4870; -- связка экономических параметров CM043 со строкой графика
       REPOS_CM044             CONSTANT NUMBER := 4871; -- связка экономических параметров CM044 со строкой графика
       REPOS_CM045             CONSTANT NUMBER := 4872; -- связка экономических параметров CM045 со строкой графика
       REPOS_CM047             CONSTANT NUMBER := 4873; -- связка экономических параметров CM047 со строкой графика
       REPOS_CM048             CONSTANT NUMBER := 4874; -- связка экономических параметров CM048 со строкой графика
       LIMITQ_PROTOCOL         CONSTANT NUMBER := 4876; -- Протоколы расчетов лимитов QUIK
/**
 * НЕ ПД */
       DV_CSAPM          CONSTANT NUMBER := 4627; -- Платеж по CSA
       DV_CSAPSO         CONSTANT NUMBER := 4628; -- СО начисления процентов по CSA
       DL_SECURITYCOM    CONSTANT NUMBER := 4721; -- Комиссии в БОЦБ
       DL_DLGRDEAL       CONSTANT NUMBER := 4724; -- Строка графика
       DL_DEPODRAFT      CONSTANT NUMBER := 4725; -- Поручение депо
       DL_PMWRTSUM       CONSTANT NUMBER := 4735; -- Лот (ценнобумажный, денежный)

/**
 * Виды сумм dlsum.dbt */
       DLSUM_BANK_ONCE                    CONSTANT NUMBER := 10;   -- Единовременная комиссия клиента банку
       DLSUM_BANK_PERIOD                  CONSTANT NUMBER := 20;   -- Периодическая комиссия клиента банку
       DLSUM_AGENT_PERIOD                 CONSTANT NUMBER := 30;   -- Периодическая комиссия посреднику
       DLSUM_AGENT_ONCE                   CONSTANT NUMBER := 40;   -- Единовременная комиссия посреднику
       DLSUM_REGISTAR_ONCE                CONSTANT NUMBER := 50;   -- Единовременная комиссия регистратору
       DLSUM_BANK_ONCE_CONTR              CONSTANT NUMBER := 60;   -- Единовременная комиссия контрагента банку (зарезервировано для 2 оч.)
       DLSUM_BANK_PERIOD_CONTR            CONSTANT NUMBER := 70;   -- Периодическая комиссия контрагента банку
       DLSUM_REGISTAR_ONCE_CONTR          CONSTANT NUMBER := 80;   -- Единовременная комиссия контрагента регистратору
       DLSUM_AGENT_ONCE_IMPORT            CONSTANT NUMBER := 1000; -- Импортированная единовременная комиссия посреднику в валюте начисления
       DLSUM_BANK_ONCE_IMPORT             CONSTANT NUMBER := 1010; -- Импортированная единовременная комиссия банку в валюте начисления
       DLSUM_OUTLAY                       CONSTANT NUMBER := 1100; -- Затраты на прирбретение
       DLSUM_OVERVALUE                    CONSTANT NUMBER := 1110; -- Переоценка
       DLSUM_INTERESTINCOME               CONSTANT NUMBER := 1120; -- Процентный доход
       DLSUM_INTERESTINCOMENOTCARRY       CONSTANT NUMBER := 1121; -- Процентный доход, не отн. на доходы
       DLSUM_DISCONTINCOME                CONSTANT NUMBER := 1130; -- Дисконтный доход
       DLSUM_BEGDISCONTINCOME             CONSTANT NUMBER := 1131; -- Начальный дисконт
       DLSUM_OLDBEGDISCONTINCOME          CONSTANT NUMBER := 1132; -- Начальный дисконт до перевода
       DLSUM_DISCONTINCOMENOTCARRY        CONSTANT NUMBER := 1133; -- Дисконтный доход, не отн. на доходы
       DLSUM_DISCONTCORR                  CONSTANT NUMBER := 1134; -- Начисл. дисконтный доход,списанный при переводе
       DLSUM_OUTBAL_SUM1                  CONSTANT NUMBER := 1140; -- 1-е кол-во на внебалансе для операции перемещения
       DLSUM_OUTBAL_SUM2                  CONSTANT NUMBER := 1150; -- 2-е кол-во на внебалансе для операции перемещения
       DLSUM_OUTBAL_SUM3                  CONSTANT NUMBER := 1160; -- 3-е кол-во на внебалансе для операции перемещения
       DLSUM_BPP_SUM                      CONSTANT NUMBER := 1170; -- кол-во БПП для операции перемещения
       DLSUM_BONUS                        CONSTANT NUMBER := 1210; -- Начисленная премия
       DLSUM_BEGBONUS                     CONSTANT NUMBER := 1211; -- Текущая начальная премия
       DLSUM_OLDBEGBONUS                  CONSTANT NUMBER := 1212; -- Начальная премия до перевода (если был)
       DLSUM_NOTWRTBONUS                  CONSTANT NUMBER := 1213; -- Начисленная премия, не списанная на расходы
       DLSUM_PRICEWRTTAX                  CONSTANT NUMBER := 1220; -- Цена покупки в НУ
       DLSUM_COSTWRTTAX                   CONSTANT NUMBER := 1230; -- Стоимость покупки в НУ
       DLSUM_NKDWRTTAX                    CONSTANT NUMBER := 1240; --  НКД в НУ
       DLSUM_OUTLAYWRTTAX                 CONSTANT NUMBER := 1250; --  Затраты в НУ
       DLSUM_SUM_CLAIMS_LIABILITE         CONSTANT NUMBER := 2000; -- Сумма требования/обязательства
       DLSUM_SUM_TO_PERCENT               CONSTANT NUMBER := 2010; -- Сумма начисленных процентов - в ВР
       DLSUM_SUM_TO_PERCENT_CFI           CONSTANT NUMBER := 2011; -- Сумма начисленных процентов - в ВЦ
       DLSUM_SUM_NOTCARRY_PERCENT         CONSTANT NUMBER := 2020; -- Сумма начисленных процентов, не отнесенная на доходы - в ВР
       DLSUM_SUM_NOTCARRY_PERCENT_CFI     CONSTANT NUMBER := 2021; -- Сумма начисленных процентов, не отнесенная на доходы - в ВЦ
       DLSUM_SUM_COUPON_INCOME_BACK       CONSTANT NUMBER := 2100; -- Сумма купонного дохода, подлежащего возврату
       DLSUM_SUM_PARTIAL_INCOME_BACK      CONSTANT NUMBER := 2110; -- Сумма дохода по ЧП, подлежащего возврату
       DLSUM_RESERV_DELAY                 CONSTANT NUMBER := 2200; -- Сумма резерва по сделкам с отсрочкой платежа
       DLSUM_RESERV_OVERDUE               CONSTANT NUMBER := 2210; -- Сумма резерва по просроченным требованиям
       DLSUM_RESERV_BACKREPO              CONSTANT NUMBER := 2220; -- Сумма резерва по требованиям по 2 части сделок ОР
       DLSUM_GT_ITSCOMM                   CONSTANT NUMBER := 2230; -- Сумма импортированной комиссии ИТС
       DLSUM_GT_CLRCOMM                   CONSTANT NUMBER := 2240; -- Сумма импортированной комиссии за клиринг
       DLSUM_RESERV_SALEREPO2             CONSTANT NUMBER := 2250; -- Сумма резерва по требованиям по 2 части сделок ПР
       DLSUM_CM_IN_TORGACC_NATCUR         CONSTANT NUMBER := 2300; -- Руб. сумма перевода на торг. счет
       DLSUM_CM_IN_TORGACC_CUR            CONSTANT NUMBER := 2310; -- Вал. сумма перевода на торг. счет
       DLSUM_CM_IN_CLIRINGACC_NATCUR      CONSTANT NUMBER := 2320; -- Руб. сумма перевода на клир. счет
       DLSUM_CM_IN_CLIRINGACC_CUR         CONSTANT NUMBER := 2330; -- Вал. сумма перевода на клир. счет
       DLSUM_CM_OUT_TORGACC_NATCUR        CONSTANT NUMBER := 2340; -- Руб. сумма перевода с торг. счета
       DLSUM_CM_OUT_TORGACC_CUR           CONSTANT NUMBER := 2350; -- Вал. сумма перевода с торг. счета
       DLSUM_CM_OUT_CLIRINGACC_NATCUR     CONSTANT NUMBER := 2360; -- Руб. сумма перевода с клир. счета
       DLSUM_CM_OUT_CLIRINGACC_CUR        CONSTANT NUMBER := 2370; -- Вал. сумма перевода с клир. счета
       DLSUM_NOTCARRY_BONUS_TRADE         CONSTANT NUMBER := 2390; --Сумма не отнесённой на расходы премии в ТП NotWrtBonus при учёте купона
       DLSUM_NOTCARRY_BONUS_SALE          CONSTANT NUMBER := 2391; --Сумма не отнесённой на расходы премии в ППР NotWrtBonus при учёте купона
       DLSUM_NOTCARRY_BONUS_RETIRE        CONSTANT NUMBER := 2392; --Сумма не отнесённой на расходы премии в ПУДП NotWrtBonus при учёте купона
       DLSUM_SUM_DISCOUNT0_KORR_TP        CONSTANT NUMBER := 2460; -- Сумма доначисления дисконта в рамках корректировки начального дисконта в ТП 
       DLSUM_SUM_DISCOUNT0_KORR_PPR       CONSTANT NUMBER := 2461; -- Сумма доначисления дисконта в рамках корректировки начального дисконта в ППР 
       DLSUM_SUM_DISCOUNT0_KORR_PUDP      CONSTANT NUMBER := 2462; -- Сумма доначисления дисконта в рамках корректировки начального дисконта в ПУДП 
       DLSUM_SUM_DEFDIFF0_KORR_TP         CONSTANT NUMBER := 2480; -- Сумма доначисления отсроченной разницы в рамках корректировки начальной отсроченной разницы в ТП 
       DLSUM_SUM_DEFDIFF0_KORR_PPR        CONSTANT NUMBER := 2481; -- Сумма доначисления отсроченной разницы в рамках корректировки начальной отсроченной разницы в ППР 
       DLSUM_SUM_DEFDIFF0_KORR_PUDP       CONSTANT NUMBER := 2482; -- Сумма доначисления отсроченной разницы в рамках корректировки начальной отсроченной разницы в ПУДП 
       DLSUM_COMPDEL_SUMTP                CONSTANT NUMBER := 2500; -- Сумма в ТП
       DLSUM_COMPDEL_NKDTP                CONSTANT NUMBER := 2501; -- НКД в ТП
       DLSUM_COMPDEL_BONUSTP              CONSTANT NUMBER := 2502; -- Премия в ТП
       DLSUM_COMPDEL_SUMPPR               CONSTANT NUMBER := 2510; -- Сумма в ППР
       DLSUM_COMPDEL_NKDPPR               CONSTANT NUMBER := 2511; -- НКД в ППР
       DLSUM_COMPDEL_BONUSPPR             CONSTANT NUMBER := 2512; -- Премия в ППР
       DLSUM_COMPDEL_SUMPUDP              CONSTANT NUMBER := 2520; -- Сумма в ПУДП
       DLSUM_COMPDEL_NKDPUDP              CONSTANT NUMBER := 2521; -- НКД в ПУДП
       DLSUM_COMPDEL_BONUSPUDP            CONSTANT NUMBER := 2522; -- Премия в ПУДП
       DLSUM_COMPDEL_SUMOD                CONSTANT NUMBER := 2530; -- Сумма в ОД
       DLSUM_COMPDEL_NKDOD                CONSTANT NUMBER := 2531; -- НКД в ОД
       DLSUM_COMPDEL_SUMPKU               CONSTANT NUMBER := 2540; -- Сумма в ПКУ
       DLSUM_COMPDEL_SUMBACKKSU           CONSTANT NUMBER := 2550; -- Сумма в ПВО_КСУ
       DLSUM_KIND_OWNAVR_AMORTCOM         CONSTANT NUMBER := 2710; -- Сумма самортизированных затрат по комисси ОЭБ
       DLSUM_KIND_OWNAVR_DEALAMORTCOM     CONSTANT NUMBER := 2720; -- Сумма самортизированных затрат по комисси ОЭБ по сделке выкупа/погашения

/**
 * Направление обеспечения */
       TICKENS_KIND_IN   CONSTANT NUMBER := 0; -- ввод
       TICKENS_KIND_OUT  CONSTANT NUMBER := 1; -- вывод

/**
  * Виды переводов в операции перемещения */
       SUBKIND_SALE_TO_RETIRE       CONSTANT NUMBER := 1;  -- Переклассификация ц/б "для продажи" в "удерживаемые до погашения"
       SUBKIND_RETIRE_TO_SALE       CONSTANT NUMBER := 2;  -- Переклассификация ц/б, "удерживаемых до погашения", в "ц/б для продажи"
       SUBKIND_TO_CONTROL           CONSTANT NUMBER := 3;  -- Перемещение в портфель контрольного участия
       SUBKIND_FROM_CONTROL         CONSTANT NUMBER := 4;  -- Перемещение из портфеля контрольного участия
       SUBKIND_UNRETIRE             CONSTANT NUMBER := 5;  -- Перевод на счета долговых обязательств, не погашенных в срок
       SUBKIND_TRADE_TO_RETIRE_2129 CONSTANT NUMBER := 6;  -- Перевод ц/б торг.портфеля в "удерж.до погашения" (по 2129-У)
       SUBKIND_TRADE_TO_SALE_2129   CONSTANT NUMBER := 7;  -- Перевод ц/б торг.портфеля в "ц/б для продажи" (по 2129-У)
       SUBKIND_SALE_TO_RETIRE_2129  CONSTANT NUMBER := 8;  -- Перевод ц/б "для продажи" в "удерж.до погашения"(по 2129-У)
       SUBKIND_TRADE_TO_RETIRE_2014 CONSTANT NUMBER := 9;  -- Перевод ц/б торг.портфеля в "удерж.до погашения" (кризисный 2014)
       SUBKIND_TRADE_TO_SALE_2014   CONSTANT NUMBER := 10; -- Перевод ц/б торг.портфеля в "ц/б для продажи" (кризисный 2014)
       SUBKIND_SALE_TO_RETIRE_2014  CONSTANT NUMBER := 11; -- Перевод ц/б "для продажи" в "удерж.до погашения" (кризисный 2014)

/**
 * Типы периодов */
       CB_PERIOD_DAY   CONSTANT NUMBER := 1; -- день
       CB_PERIOD_WEEK  CONSTANT NUMBER := 2; -- неделя
       CB_PERIOD_MONTH CONSTANT NUMBER := 3; -- месяц
       CB_PERIOD_YEAR  CONSTANT NUMBER := 4; -- год

/**
  * Виды операций ФИССиКО */
       DV_FORWARD_T3 CONSTANT NUMBER := 5;

/**
 * Значения справочника OBJTYPE_DLCONTRMSG = 1145 - Уровни существенности*/
       LEVELESSENTIAL_AC            CONSTANT NUMBER := 1; -- Отклонение АС
       LEVELESSENTIAL_CC            CONSTANT NUMBER := 2; -- Отклонение СС
       LEVELESSENTIAL_FACTPRICE     CONSTANT NUMBER := 3; -- Отклонение фактической цены (ФЦ)
       LEVELESSENTIAL_EPS           CONSTANT NUMBER := 4; -- Отклонение ЭПС
       LEVELESSENTIAL_CONTRACTCOSTS CONSTANT NUMBER := 5; -- Затраты по договору
       LEVELESSENTIAL_FP_UP         CONSTANT NUMBER := 6; -- Отклонение ФЦ вверх
       LEVELESSENTIAL_FP_DOWN       CONSTANT NUMBER := 7; -- Отклонение ФЦ вниз
       LEVELESSENTIAL_EPS_DOWN      CONSTANT NUMBER := 8; -- Отклонение вниз ЭПС
       LEVELESSENTIAL_EPS_UP        CONSTANT NUMBER := 9; -- Отклонение вверх ЭПС

/**
 * Виды наименований ставок */
       RATE_KIND_UNDEF           CONSTANT NUMBER := -1; -- Не определено
       RATE_KIND_EPS             CONSTANT NUMBER := 2;  -- ЭПС
       RATE_KIND_RPS             CONSTANT NUMBER := 3;  -- РПС
       RATE_KIND_SPD             CONSTANT NUMBER := 4;  -- СПД (ставка по договору)

/**
 * Виды расчетов ЭПС/АС_ЭПС/%ЭПС */
       CALCKIND_AVR        CONSTANT NUMBER := 1;  -- ЦБ (покупка)
       CALCKIND_OREPO      CONSTANT NUMBER := 2;  -- ОРЕПО
       CALCKIND_PREPO      CONSTANT NUMBER := 3;  -- ПРЕПО
       CALCKIND_VS         CONSTANT NUMBER := 4;  -- СВ
       CALCKIND_VA         CONSTANT NUMBER := 5;  -- УВ
       CALCKIND_OWN        CONSTANT NUMBER := 6;  -- ОЭБ

/**
 * Метод расчета АС */
       AMORTCALCKIND_LM   CONSTANT NUMBER := 1;-- ЛМ
       AMORTCALCKIND_EPS  CONSTANT NUMBER := 2;-- ЭПС
       AMORTCALCKIND_RPS  CONSTANT NUMBER := 3;-- РПС

/**
 * Виды базы расчета ALG_KIND_BASISCALC */
       SP_KINDBASISCALC_365_CAL CONSTANT NUMBER :=  0; -- 365 дней в году / в месяце по календарю
       SP_KINDBASISCALC_360_30  CONSTANT NUMBER :=  1; -- 360 дней в году / 30 дней в месяце
       SP_KINDBASISCALC_CAL_CAL CONSTANT NUMBER :=  2; -- в году по календарю / в месяце по календарю
       SP_KINDBASISCALC_360_CAL CONSTANT NUMBER :=  3; -- 360 дней в году / в месяце по календарю
       SP_KINDBASISCALC_365_30  CONSTANT NUMBER :=  4; -- 365 дней в году / 30 дней в месяце
       SP_KINDBASISCALC_CAL_30  CONSTANT NUMBER :=  5; -- в году по календарю / 30 в месяце

/**
 * Виды округления НКД для ценных бумаг */
       AVOIRISSNKDROUND_NO      CONSTANT NUMBER := 0; -- без округления
       AVOIRISSNKDROUND_COUPON  CONSTANT NUMBER := 1; -- округление НКД для одного купона
       AVOIRISSNKDROUND_SUM     CONSTANT NUMBER := 2; -- округление платежа по НКД

/**
 * Виды используемых алгоритмов расчета СС */
       ALG_FINDMARKETRATE   CONSTANT NUMBER := 1; -- Алгоритм поиска рыночной котировки
       ALG_COMPAREFIRATE    CONSTANT NUMBER := 2; -- Алгоритм котировки сопоставимого выпуска
       ALG_DISCCOSTSTREAM   CONSTANT NUMBER := 3; -- Алгоритм дисконтирование денежных потоков

/**
 * Виды записей в DVSINCOME*/
       VSINCOMETYPE_EPRPERC CONSTANT NUMBER := 5;

/**
 * Виды алгоритмов расчета отклонения ЭПС */
       ALG_CALCDEV_EPS1 CONSTANT NUMBER := 1; -- Алгоритм 1
       ALG_CALCDEV_EPS2 CONSTANT NUMBER := 2; -- Алгоритм 2

/**
 * Виды алгоритмов расчета отклонения фактической цены */
       ALG_CALCDEV_FP1 CONSTANT NUMBER := 1; -- Алгоритм 1
       ALG_CALCDEV_FP2 CONSTANT NUMBER := 2; -- Алгоритм 2

/**
 * Типы отбираемых операций в СОБУ */
       SC_ACCOPERTYPE_ALL         CONSTANT NUMBER := 0; --Все
       SC_ACCOPERTYPE_EXCHANGE    CONSTANT NUMBER := 1; --Биржевые
       SC_ACCOPERTYPE_OUTEXCHANGE CONSTANT NUMBER := 2; --Внебиржевые
       SC_ACCOPERTYPE_OTHER       CONSTANT NUMBER := 3; --Прочие

/**
 * Виды биржевого рынка для заявок, сделок ФИССиКО и видов обязательств*/
       DV_MARKETKIND_STOCK       CONSTANT NUMBER := 1; -- Фондовый
       DV_MARKETKIND_CURRENCY    CONSTANT NUMBER := 2; -- Валютный
       DV_MARKETKIND_SPFIMARKET  CONSTANT NUMBER := 3; -- Рынок СПФИ
       DV_MARKETKIND_DERIV       CONSTANT NUMBER := 4; -- Срочный
       DV_MARKETKIND_ALL         CONSTANT NUMBER := 5; -- Все (единый пул обеспечения)                                                                            

/**
 * Виды биржевого рынка для расчетов*/
       DL_MARKETKIND_SETTLE_STOCK_T0    CONSTANT NUMBER := 1; -- Фондовый T0
       DL_MARKETKIND_SETTLE_CURRENCY    CONSTANT NUMBER := 2; -- Валютный
       DL_MARKETKIND_SETTLE_SPFIMARKET  CONSTANT NUMBER := 3; -- Рынок СПФИ
       DL_MARKETKIND_SETTLE_DERIV       CONSTANT NUMBER := 4; -- Срочный
       DL_MARKETKIND_SETTLE_STOCK_TPLUS CONSTANT NUMBER := 5; -- Фондовый Т+

/**
 * Виды кодов  */
       CODE_FI_Code              CONSTANT NUMBER := 1;
       CODE_ISO_Number           CONSTANT NUMBER := 2;
       CODE_Ccy                  CONSTANT NUMBER := 3;
       CODE_LSIN                 CONSTANT NUMBER := 4;
       CODE_ISIN                 CONSTANT NUMBER := 5;
       CODE_CodeInAccount        CONSTANT NUMBER := 6;
       CODE_MICEX                CONSTANT NUMBER := 11;
       CODE_PTC                  CONSTANT NUMBER := 12;
       CODE_PTCC                 CONSTANT NUMBER := 13;
       CODE_NADC                 CONSTANT NUMBER := 14;
       CODE_DCLC                 CONSTANT NUMBER := 15;
       CODE_F404                 CONSTANT NUMBER := 16;

       MAX_INNER_SYSTEM_CODEKIND CONSTANT NUMBER := 10;

/**
 * Типы базовых активов ПИ */
       DVBASEACT_NOTDEF                CONSTANT NUMBER := 0;    -- не задан   
       DVBASEACT_FUTURES               CONSTANT NUMBER := 10;   -- фьючерс
       DVBASEACT_FORWARD               CONSTANT NUMBER := 15;   -- форвард
       DVBASEACT_SHARE                 CONSTANT NUMBER := 20;   -- акция
       DVBASEACT_BOND                  CONSTANT NUMBER := 30;   -- облигация
       DVBASEACT_INVESTMENT_SHARE      CONSTANT NUMBER := 33;   -- Инв. пай
       DVBASEACT_DEPOSITORY_RECEIPT    CONSTANT NUMBER := 36;   -- Деп. расписка
       DVBASEACT_CURRENCY              CONSTANT NUMBER := 40;   -- валюта
       DVBASEACT_INDEX                 CONSTANT NUMBER := 50;   -- индекс
       DVBASEACT_METAL                 CONSTANT NUMBER := 60;   -- драг. металл
       DVBASEACT_ARTICLE               CONSTANT NUMBER := 70;   -- товар
       DVBASEACT_BASKET                CONSTANT NUMBER := 80;   -- корзина ц/б


type tr_DLCONTRID is record (T_DLCONTRID ddlcontr_dbt.t_dlcontrid%type) ;
type tt_DLCONTRID is table of tr_DLCONTRID ;



/**
 * Процедура получения последней ошибки из пакета
 * @since 6.20.031
 * @qtest NO
 * @param ErrMes Ошибка
 */
  PROCEDURE RSI_GetLastErrorMessage( ErrMes OUT VARCHAR2 );

/**
 * Функция возвращает меньшую из двух дат
 * @since 6.20.029
 * @qtest NO
 * @param d1 Дата1
 * @param d2 Дата2
 * @return Меньшая из двух дат
 */
  FUNCTION date_min( d1 IN DATE, d2 IN DATE )
  RETURN DATE;

/**
 * Функция возвращает большую из двух дат
 * @since 6.20.029
 * @qtest NO
 * @param d1 Дата1
 * @param d2 Дата2
 * @return Большая из двух дат
 */
  FUNCTION date_max( d1 IN DATE, d2 IN DATE )
  RETURN DATE;

/**
 * Определяет вид ФИ
 * @since 6.20.029
 * @qtest NO
 * @param AvoirKind Подвид ФИ
 * @param DeposReceiptAsShare ДР считать как Акцию
 * @return вид ФИ
 */
  FUNCTION SecurKind( AvoirKind IN NUMBER, DeposReceiptAsShare IN NUMBER DEFAULT 1 ) RETURN NUMBER DETERMINISTIC;

/**
 * Определяет вид облигации
 * @since 6.20.029
 * @qtest NO
 * @param in_FIID ID ФИ
 * @return вид облигации
 */
  FUNCTION BondKind( in_FIID IN NUMBER ) RETURN VARCHAR2 DETERMINISTIC;

/**
 * Определяет метод списания для субъекта
 * @since 6.20.029
 * @qtest NO
 * @param in_ClientID ID клиента
 * @param in_ContractID ID договора обслуживания клиента
 * @return метод списания для субъекта
 */
  FUNCTION AmortizationMethod( in_ClientID IN NUMBER, in_ContractID IN NUMBER DEFAULT 0 ) RETURN NUMBER DETERMINISTIC;

/**
 * Остаток ц/б в шт. на лоте
 * @deprecated Код закомментирован - всегда возвращает 0.
 * @since 6.20.029
 * @qtest NO
 * @param DocKind Вид документа операции
 * @param DocID ID документа операции
 * @param PartNum Номер лота по документу
 * @param ReportDate Дата
 * @return Остаток ц/б
 */
  FUNCTION get_RestAmount( DocKind IN NUMBER,
                           DocID   IN NUMBER,
                           PartNum IN NUMBER,
                           ReportDate IN DATE ) RETURN NUMBER;
/**
 * Балансовая стоимость на дату
 * @deprecated Код закомментирован - всегда возвращает 0.
 * @since 6.20.029
 * @qtest NO
 * @param DocKind Вид документа операции
 * @param DocID ID документа операции
 * @param PartNum Номер лота по документу
 * @param ReportDate Дата
 * @return Стоимость
 */
  FUNCTION get_BalanceCost( DocKind IN NUMBER,
                            DocID   IN NUMBER,
                            PartNum IN NUMBER,
                            ReportDate IN DATE ) RETURN NUMBER;
/**
 * Остаток НКД на лоте
 * @deprecated Код закомментирован - всегда возвращает 0.
 * @since 6.20.029
 * @qtest NO
 * @param DocKind Вид документа операции
 * @param DocID ID документа операции
 * @param PartNum Номер лота по документу
 * @param ReportDate Дата
 * @return Стоимость
 */
  FUNCTION get_RestNKD( DocKind IN NUMBER,
                        DocID   IN NUMBER,
                        PartNum IN NUMBER,
                        ReportDate IN DATE ) RETURN NUMBER;

/**
 * Определение группы, к которой относится сделка
 * @since 6.20.029
 * @qtest NO
 * @param oper Тип операции
 * @return Группа сделки
 */
  FUNCTION get_OperationGroup( oper IN doprkoper_dbt.T_SYSTYPES%TYPE ) RETURN NUMBER;

/**
 * Определение группы, к которой относится сделка
 * @since 6.20.031
 * @qtest NO
 * @param oper Тип операции
 * @param DocKind Вид операции
 * @return Группа сделки
 */
  FUNCTION get_OperationGroupDocKind( oper IN doprkoper_dbt.T_SYSTYPES%TYPE, DocKind IN NUMBER ) RETURN NUMBER;

/**
 * Определение системного типа сделки
 * @since 6.20.029
 * @qtest NO
 * @param TypeID Тип операции
 * @param DocKind Вид документа операции
 * @return Системный тип сделки
 */
  FUNCTION get_OperSysTypes( TypeID IN NUMBER, DocKind IN NUMBER ) RETURN doprkoper_dbt.T_SYSTYPES%TYPE RESULT_CACHE;
/**
 * Анализ флагов группы видов операций, флаг задается числом
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @param Mask Маска
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION check_Group( OGroup IN NUMBER,
                        Mask   IN NUMBER  ) RETURN NUMBER;
/**
 * Анализ флагов группы видов операций, флаг задается строкой в которой записано число в HEX формате
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @param Mask Маска
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION check_GroupStr( OGroup IN NUMBER,
                           Mask   IN VARCHAR2  ) RETURN NUMBER DETERMINISTIC;
/**
 * Анализ флагов группы видов операций
 * Это покупка?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsBuy( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это продажа?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsSale( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это сделка РЕПО?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsRepo( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это сделка фьючерс?
 * @since 6.20.031
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsFUTURES( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это сделка конверсионный СВОП?
 * @since 6.20.031
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsDVFXSWAP( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это сделка покупка/продажа?
 * @since 6.20.031
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsDVFXDEAL( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это сделка короткий СВОП?
 * @since 6.20.031
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsSHORTSWAP( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Проверка вида операции сделки
 * Это сделка РЕПО?
 * @since 6.20.031
 * @qtest NO
 * @param DealID Идентификатор сделки
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION DealIsRepo( DealID IN NUMBER ) RETURN NUMBER DETERMINISTIC;


/**
 * Анализ флагов группы видов операций
 * Это сделка на корзину ЦБ?
 * @since 6.20.031
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsBasket( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это сделка с КСУ?
 * @since 6.20.031
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsDealKSU( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это сделка с междиллерского РЕПО?
 * @since 6.20.031
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsInterDealerRepo( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;


/**
 * Анализ флагов группы видов операций
 * Это покупка с обратной продажей?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsBackSale( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это операция зачисления ц/б?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsAvrWrtIn( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это операция списания ц/б?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsAvrWrtOut( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это биржевая сделка?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @param isExcludeOTC Признак исключения из отбора сделок ОТС: 1 - исключаем, 0 - нет
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsExchange( OGroup IN NUMBER, isExcludeOTC IN NUMBER DEFAULT 0 ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это ВНЕбиржевая сделка?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @param isIncludeOTC Признак добавления в отбор сделок ОТС: 1 - включаем, 0 - нет
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsOutExchange( OGroup IN NUMBER, isIncludeOTC IN NUMBER DEFAULT 0 ) RETURN NUMBER DETERMINISTIC;
  
/**
 * Анализ флагов группы видов операций
 * Это сделка ОТС?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsOTC( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это погашение купона?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsRet_Coupon( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это частичное погашение?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsRet_Partly( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это погашение доп. дохода?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsRet_ADDINCOME( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это погашение выпуска?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsRet_Issue( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это операция займа?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsLoan( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Проверка вида операции сделки
 * Это операция займа?
 * @since 6.20.031
 * @qtest NO
 * @param DealID Идентификатор сделки
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION DealIsLoan( DealID IN NUMBER ) RETURN NUMBER DETERMINISTIC;


/**
 * Анализ флагов группы видов операций
 * Это Конвертация акций в депозитарные расписки?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится операция
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsConvShare( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это Конвертация депозитарных расписок в акции?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится операция
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsConvReceipt( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это брокерская сделка?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsBroker( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это сделка today?
 * @since 6.20.031
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsToday( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Анализ флагов группы видов операций
 * Это операция Получение дивидендов по РЕПО ?
 * @since 6.20.031
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */

   FUNCTION IsDividendReturnRepo( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

  /**
 * Анализ флагов группы видов операций
 * Это операция Получение дивидендов от Эмитента ?
 * @since 6.20.031
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */

  FUNCTION IsGetDividRepo( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

  /**
 * Анализ флагов группы видов операций
 * Это операция Получение дивидендов от Эмитента ?
 * @since 6.20.031
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsGetDividEmitent( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает тип сделки.
 * @since 6.20.029
 * @qtest NO
 * @param DocKind Вид документа операции
 * @param DocKind Вид операции
 * @param REJECTDATE Дата отказа
 * @return тип сделки:
 *         S - продажа
 *         BS- покупка с обратной продажей (2 ч.)
 *         SB - продажа с обратным выку-пом (1 ч.)
 *         RBS - репо покупка (2 ч)
 *         RSB - репо продажа. (1 ч.)
 *         DI - погашение выпуска
 *         DW - погашение купона
 *         DP - частичное погашение
 *         MS - Перемещение из портфеля
 *         FS - Списание лота
 *         FB - Зачисление лота
 *         zS - Размещение займа
 *         zB - Привлечение займа
 *         CSD - Конвертация акция в ДР
 *         CDS - Конвертация ДР в акции
 */
  FUNCTION get_OperationType( DocKind IN NUMBER, KindOper IN NUMBER, REJECTDATE IN DATE DEFAULT NULL ) RETURN VARCHAR2;

/**
 * Возвращает сделку погашения, в кот. гасился или купон или ЧП или выпуск данного ФИ
 * @since 6.20.029
 * @qtest NO
 * @param FIID ID ц/б
 * @param NUM Номер ЧП
 * @param IsPartial Признак ЧП
 * @return ID сделки в случае успеха, NULL - иначе
 */
  FUNCTION get_DrawingOperation( FIID IN NUMBER,
                                 NUM IN VARCHAR2,
                                 IsPartial IN VARCHAR2 ) RETURN NUMBER;

/**
 * функция прверяет является ли часть по сделке покупкой али продажей
 * @since 6.20.029
 * @qtest NO
 * @param TypeID Тип операции
 * @param DocKind Вид документа операции
 * @param IsBack Признак 2 части
 * @return 1 - покупка, 0 - продажа, -1 - иначе
 */
  FUNCTION get_DealPartBuyType( TypeID  IN NUMBER,
                                DocKind IN NUMBER,
                                IsBack  IN NUMBER ) RETURN NUMBER;

/**
 * Возвращает ID шага операции, если он имеется в данной операции
 * Можно задать либо BranchSymbol, либо ActionStep(KindAction) и DealPart
 * @since 6.20.029
 * @qtest NO
 * @param OperationID ID операции
 * @param DocKind Вид документа операции (T_BOFFICEKIND сделки в ddl_tick_dbt)
 * @param KindOper Вид операции (T_DEALTYPE сделки в ddl_tick_dbt)
 * @param BranchSymbol Символ шага
 * @param ActionStep Действие шага
 * @param DealPart Часть сделки
 * @return ID шага операции
 */
  FUNCTION get_OperStepID( OperationID IN NUMBER,
                           DocKind IN NUMBER,
                           KindOper IN NUMBER,
                           BranchSymbol IN doproblck_dbt.T_SYMBOL%TYPE,
                           ActionStep IN NUMBER,
                           DealPart IN NUMBER ) RETURN NUMBER;
/**
 * Находит документ по шагу операции, если есть
 * @since 6.20.029
 * @qtest NO
 * @param DealID ID сделки (--T_DEALID сделки в ddl_tick_dbt)
 * @param DocKind Вид документа операции (T_DEALTYPE сделки в ddl_tick_dbt)
 * @param KindOper Вид операции (T_BOFFICEKIND сделки в ddl_tick_dbt)
 * @param ToFI Валюта суммы
 * @param BranchSymbol Символ шага
 * @param ActionStep Действие шага
 * @param DealPart Часть сделки
 * @return Сумма из проводки шага
 */
  FUNCTION get_StepCarrySum( DealID IN NUMBER,
                             DocKind IN NUMBER,
                             KindOper IN NUMBER,
                             ToFI     IN NUMBER,
                             BranchSymbol IN doproblck_dbt.T_SYMBOL%TYPE,
                             ActionStep IN NUMBER,
                             DealPart IN NUMBER ) RETURN NUMBER;

/**
 * Возвращает дату шага операции. Если IsExecute = 'X', то
 * вернет дату выполненого шага, если таковой имеет место быть -
 * в противном случае вернет Плановую дату.
 * @since 6.20.029
 * @qtest NO
 * @param DealID ID сделки (--T_DEALID сделки в ddl_tick_dbt)
 * @param DocKind Вид документа операции (T_DEALTYPE сделки в ddl_tick_dbt)
 * @param KindOper Вид операции (T_BOFFICEKIND сделки в ddl_tick_dbt)
 * @param BranchSymbol Символ шага
 * @param ActionStep Действие шага
 * @param DealPart Часть сделки
 * @return Дата шага операции
 */
  FUNCTION get_OperStepDate( DealID IN NUMBER,
                             DocKind IN NUMBER,
                             KindOper IN NUMBER,
                             BranchSymbol IN doproblck_dbt.T_SYMBOL%TYPE,
                             ActionStep IN NUMBER,
                             DealPart IN NUMBER ) RETURN DATE;

/**
 * получить дату приема ц/б на обслуживание
 * @since 6.20.029
 * @qtest NO
 * @param EndPerion Дата
 * @param FIID ID ц/б
 * @param OperDepartment Филиал. Если не задаан - филиал текущего пользователя.
 * @return Дата приема на обслуживание
 */
  FUNCTION get_StartServiceDate( EndPerion   IN DATE,
                                 FIID        IN NUMBER,
                                 OperDepartment IN NUMBER DEFAULT NULL ) RETURN DATE;

/**
 * получить дату снятия ц/б с обслуживания обслуживание
 * @since 6.20.029
 * @qtest NO
 * @param EndPerion Дата
 * @param FIID ID ц/б
 * @param OperDepartment Филиал. Если не задаан - филиал текущего пользователя.
 * @return Дата снятия на обслуживание
 */
  FUNCTION get_EndServiceDate( EndPerion   IN DATE,
                               FIID        IN NUMBER,
                               OperDepartment IN NUMBER DEFAULT NULL ) RETURN DATE;

/**
 * рассчитать резерв по сделке с цб
 * @since 6.20.029
 * @qtest NO
 * @param EndPerion Дата
 * @param RP процент риска
 * @param DealID ID сделки
 * @param DocKind Вид документа операции
 * @param KindOper Вид операции
 * @return Сумма резерва по сделке с ц/б
 */
  FUNCTION get_CalculatedReserve( EndPerion   IN DATE,
                                  RP          IN NUMBER,
                                  DealID      IN NUMBER,
                                  DocKind     IN NUMBER,
                                  KindOper    IN NUMBER ) RETURN NUMBER;
/**
 * Получить сумму просроченых Т/О по сделке
 * @since 6.20.029
 * @qtest NO
 * @param DocKind Вид документа операции
 * @param TypeOp Тип операции
 * @param DocID ID документа операции
 * @param PayFIID Валюта, в которой вернуть сумму Т/О
 * @param ReportDate Дата
 * @return Сумма просроченых Т/О по сделке
 */
  function GetOverdueDeal(DocKind IN NUMBER,
                          TypeOp  IN NUMBER,
                          DocID   IN NUMBER,
                          PayFIID IN NUMBER,
                          ReportDate IN DATE) return NUMBER;

/**
 * Получить сумму просроченых Т/О по части сделки
 * @since 6.20.029
 * @qtest NO
 * @param DocKind Вид документа операции
 * @param TypeOp Тип операции
 * @param DocID ID документа операции
 * @param Part Часть сделки
 * @param PayFIID Валюта, в которой вернуть сумму Т/О
 * @param ReportDate Дата
 * @return Сумма просроченых Т/О по части сделке
 */
  function GetOverduePart(DocKind IN NUMBER,
                          TypeOp  IN NUMBER,
                          DocID   IN NUMBER,
                          Part    IN NUMBER,
                          PayFIID IN NUMBER,
                          ReportDate IN DATE) return NUMBER;

/**
 * Функция возвращает большую из двух сумм
 * @since 6.20.029
 * @qtest NO
 * @param Sum1 Сумма1
 * @param Sum2 Сумма2
 * @return Большая из двух сумм
 */
  function max_sum( Sum1 IN NUMBER,
                    Sum2 IN NUMBER ) return NUMBER;

/**
 * Функция возвращает меньшую из двух сумм
 * @since 6.20.029
 * @qtest NO
 * @param Sum1 Сумма1
 * @param Sum2 Сумма2
 * @return Меньшая из двух сумм
 */
  function min_sum( Sum1 IN NUMBER,
                    Sum2 IN NUMBER ) return NUMBER;

/**
 * заполнение файла отчета налогового регистра №8
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 * @param BegDate_in Дата начала пересчета
 * @param EndDate_in Дата конца пересчета
 * @param ExclNetting_in Признак исключения неттинга
 * @param DebugFIID_in Сбор данных по ц/б для отладки
 */
  PROCEDURE CreateTaxLots( BegDate_in IN DATE,
                           EndDate_in IN DATE,
                           ExclNetting_in IN BOOLEAN DEFAULT FALSE,
                           DebugFIID_in IN NUMBER DEFAULT -1
                         );
/**
 * достраивает "возврат в КП"
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 * @param BegDate_in Дата начала пересчета
 * @param EndDate_in Дата конца пересчета
 * @param DebugFIID_in Сбор данных по ц/б для отладки
 */
  PROCEDURE CreateReturnSP( BegDate_in IN DATE,
                            EndDate_in IN DATE,
                            DebugFIID_in IN NUMBER DEFAULT -1
                          );

/**
 * достраивает возвраты 2 для госбумаг
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 * @param BegDate_in Дата начала пересчета
 * @param EndDate_in Дата конца пересчета
 * @param DebugFIID_in Сбор данных по ц/б для отладки
 */
  PROCEDURE CreateReturn2( BegDate_in IN DATE,
                           EndDate_in IN DATE,
                           DebugFIID_in IN NUMBER DEFAULT -1
                          );

/**
 * Вычислить свободное количество ц/б в лоте покупки по ИД по состоянию на дату/время для отчетов
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 */
  FUNCTION TaxFreeAmountByIDToDate(lotID IN dsctaxlot_dbt.t_ID%TYPE, in_Date IN DATE, in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY, in_RID IN NUMBER DEFAULT 0)
  RETURN NUMBER;

/**
 * Вычислить свободное количество ц/б в лоте покупки по ИД по состоянию на дату/время
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 */
  FUNCTION TaxFreeAmountByID(lotID IN dsctaxlot_dbt.t_ID%TYPE, in_Date IN DATE, in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY, in_RID IN NUMBER DEFAULT 0)
  RETURN NUMBER;

/**
 * Вычислить количество в неттинге для сделки по платежу по неттингу
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 */
  FUNCTION TaxGetNettingAmountById(in_TickID IN ddl_tick_dbt.t_DealID%TYPE, in_PMID IN dpmlink_dbt.t_PaymLinkID%TYPE)
  RETURN NUMBER;

/**
 * Вычислить свободное количество ц/б в лоте репо прямого, связанного с лотом покупки
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 */
  FUNCTION TaxRepoFreeAmount(in_RepoID IN dsctaxlot_dbt.t_ID%TYPE, in_BuyID IN dsctaxlot_dbt.t_ID%TYPE, in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY)
  RETURN NUMBER;

/**
 * Вычислить свободное количество ц/б в лоте репо прямого, связанного с лотом покупки, до заданной даты
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 */
  FUNCTION TaxRepoFreeAmountToDate( in_RepoID IN dsctaxlot_dbt.t_ID%TYPE,
                                    in_BuyID IN dsctaxlot_dbt.t_ID%TYPE,
                                    in_EndDate IN DATE,
                                    in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY )
  RETURN NUMBER;

/**
 * Вычислить свободное количество ц/б для связи "возврат 2"
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 */
  FUNCTION TaxRepoFreeAmount2( in_RepoID IN dsctaxlot_dbt.t_ID%TYPE,
                               in_RepoID1 IN dsctaxlot_dbt.t_ID%TYPE,
                               in_BuyID IN dsctaxlot_dbt.t_ID%TYPE,
                               in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY )
  RETURN NUMBER;

/**
 * Вычислить свободное количество ц/б для связи "возврат 2"
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 */
  FUNCTION TaxRepoFreeAmountSP( in_SourceID IN dsctaxlot_dbt.t_ID%TYPE,
                                in_BuyID IN dsctaxlot_dbt.t_ID%TYPE,
                                in_DestID IN dsctaxlot_dbt.t_ID%TYPE,
                                in_S IN dsctaxlot_dbt.t_Amount%TYPE )
  RETURN NUMBER;

/**
 * Вычислить нераспределенное (доступное для связывания) возвращенное количество ц/б в лоте типа
 * "Продажа" или "Репо прямое" из связи со сделкой покупки
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 */
  FUNCTION TaxRepoUnallottedAmount(in_SaleID IN dsctaxlot_dbt.t_ID%TYPE, in_BuyID IN dsctaxlot_dbt.t_ID%TYPE, in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY)
  RETURN NUMBER;

/**
 * Вычислить свободное количество ц/б для связи "Возврат в КП"
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 */
  FUNCTION TaxRepoUnallottedAmount2( in_RepoID IN dsctaxlot_dbt.t_ID%TYPE,
                                     in_SaleID IN dsctaxlot_dbt.t_ID%TYPE,
                                     in_BuyID IN dsctaxlot_dbt.t_ID%TYPE,
                                     in_Mode IN NUMBER DEFAULT TAXREG_ORDINARY )
  RETURN NUMBER;

/**
 * Вычислить нераспределенное (доступное для связывания) количество ц/б  для связи "Возврат в КП"
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 */
  FUNCTION TaxRepoUnallottedAmountSP( in_SourceID IN dsctaxlot_dbt.t_ID%TYPE,
                                      in_SaleID IN dsctaxlot_dbt.t_ID%TYPE,
                                      in_S IN dsctaxlot_dbt.t_Amount%TYPE )
  RETURN NUMBER;

/**
 * Вычислить незакрытый остаток короткой позиции по лоту продажи, возникших по лоту обратного репо
 * @deprecated Код устарел.
 * @since 6.20.029
 * @qtest NO
 */
  FUNCTION TaxSalePosition( in_LotID  IN dsctaxlot_dbt.t_ID%TYPE,
                            in_RepoID IN dsctaxlot_dbt.t_ID%TYPE )
  RETURN NUMBER;

/**
 * Ф-я проверяет является ли сделка продажей или покупкой
 * в понятие "продажа" входит также и погашение бумаг
 * погашение купонов выделяется в отдельный тип
 * @since 6.20.029
 * @qtest NO
 * @param DealType Тип сделки
 * @param BofficeKind  Вид документа операции
 * @param IsBack Признвк второй части сделки
 * @return Вид сделки (покупка, продажа, погашение купона и т.п.)
 */
  function GetDealBuySale( DealType    IN NUMBER,
                           BofficeKind IN NUMBER,
                           IsBack      IN NUMBER ) return NUMBER;

/**
 * Получить наименование типа сделки
 * @since 6.20.029
 * @qtest NO
 * @param p_DealType Тип сделки
 * @return Наименование типа сделки
 */
  FUNCTION GetDealTypeName(p_DealType IN NUMBER) RETURN DealTypeName_t DETERMINISTIC;

/**
 * Получить наименование клиента по сделке
 * @since 6.20.029
 * @qtest NO
 * @param p_DealType Тип сделки
 * @return Наименование типа сделки
 */
  FUNCTION GetDealClientShortName(p_ClientID IN NUMBER) RETURN DealClientShortName_t DETERMINISTIC;

/**
 * Получить номер договора обслуживания клиента по сделке
 * @since 6.20.029
 * @qtest NO
 * @param p_ClientContrID ID договора обслуживания
 * @return Номер договора обслуживания клиента по сделке
 */
  FUNCTION GetDealSfContrNumber(p_ClientContrID IN NUMBER) RETURN DealSfContrNumber_t DETERMINISTIC;

/**
 * Получить наименование контрагента по сделке
 * @since 6.20.029
 * @qtest NO
 * @param p_PartyID ID контрагента
 * @return Наименование контрагента по сделке
 */
  FUNCTION GetDealContrShortName(p_PartyID IN NUMBER) RETURN DealContrShortName_t DETERMINISTIC;

/**
 * Получить буквенный код валюты
 * @since 6.20.029
 * @qtest NO
 * @param p_FIID ID ФИ
 * @return Буквенный код валюты
 */
  FUNCTION GetDealFICcy(p_FIID IN NUMBER) RETURN DealFICcy_t DETERMINISTIC;

/**
 * Получить название филиала
 * @since 6.20.029
 * @qtest NO
 * @param p_DepartmentID Филиал
 * @return Наименование филиала
 */
  FUNCTION GetDealDepName(p_DepartmentID IN NUMBER) RETURN DealDepName_t DETERMINISTIC;

/**
 * Получить наименование операциониста
 * @since 6.20.029
 * @qtest NO
 * @param p_Oper Операционист
 * @return Наименование операциониста
 */
  FUNCTION GetDealOperName(p_Oper IN NUMBER) RETURN DealOperName_t DETERMINISTIC;

/**
 * Получить дату поставки
 * @since 6.20.029
 * @qtest NO
 * @param p_MaturityIsPrincipal Признак - дата завершения транша - дата ФИ принципала
 * @param p_Maturity Дата завершения транша
 * @param p_Expiry Дата фиксинга, пересмотра ставки или истечения транша
 * @return Дата поставки
 */
  FUNCTION GetDealSupplyDate1(p_MaturityIsPrincipal IN CHAR,
                              p_Maturity IN DATE,
                              p_Expiry IN DATE
                             ) RETURN DealSupplyDate_t DETERMINISTIC;

/**
 * Получить дату поставки по второй части
 * @since 6.20.029
 * @qtest NO
 * @param p_DealID ID сделки
 * @param p_LegKind Часть сделки
 * @return Дата поставки по второй части
 */
  FUNCTION GetDealSupplyDate2(p_DealID IN NUMBER,
                              p_LegKind IN NUMBER
                             ) RETURN DealSupplyDate_t DETERMINISTIC;

/**
 * Получить время поставки по второй части
 * @since 6.20.029
 * @qtest NO
 * @param p_DealID ID сделки
 * @param p_LegKind Часть сделки
 * @return Время поставки по второй части
 */
  FUNCTION GetDealSupplyTime2(p_DealID IN NUMBER,
                              p_LegKind IN NUMBER
                             ) RETURN DealSupplyTime_t DETERMINISTIC;

/**
 * Получить дату отказа от второй части
 * @since 6.20.029
 * @qtest NO
 * @param p_DealID ID сделки
 * @param p_LegKind Часть сделки
 * @return Дата отказа от второй части
 */
  FUNCTION GetDealRejectDate2(p_DealID IN NUMBER,
                              p_LegKind IN NUMBER
                             ) RETURN DealRejectDate_t DETERMINISTIC;

/**
 * Получить дату оплаты
 * @since 6.20.029
 * @qtest NO
 * @param p_MaturityIsPrincipal Признак - дата завершения транша - дата ФИ принципала
 * @param p_Maturity Дата завершения транша
 * @param p_Expiry Дата фиксинга, пересмотра ставки или истечения транша
 * @return Дата оплаты
 */
  FUNCTION GetDealPayDate1(p_MaturityIsPrincipal IN CHAR,
                           p_Maturity IN DATE,
                           p_Expiry IN DATE
                          ) RETURN DealPayDate_t DETERMINISTIC;

/**
 * Получить дату оплаты второй части
 * @since 6.20.029
 * @qtest NO
 * @param p_DealID ID сделки
 * @param p_LegKind Часть сделки
 * @return Дата оплаты второй части
 */
  FUNCTION GetDealPayDate2(p_DealID IN NUMBER,
                           p_LegKind IN NUMBER
                          ) RETURN DealPayDate_t DETERMINISTIC;

/**
 * Получить признак включения сделки в неттинг
 * @since 6.20.029
 * @qtest NO
 * @param p_DealID ID сделки
 * @param p_BOfficeKind Вид документа операции
 * @return Признак включения сделки в неттинг
 */
  FUNCTION GetDealIsNett(p_DealID IN NUMBER,
                         p_BOfficeKind IN NUMBER
                        ) RETURN DealIsNett_t DETERMINISTIC;

/**
 * получение номера договора для начисления комиссий ПЗО по сделке\погашению
 * @since 6.20.031
 * @qtest NO
 * @param p_DealID ID сделки
 * @return ID договора
 */
  FUNCTION RSI_GetSfContrID(p_DealID IN NUMBER, p_PayerID IN NUMBER DEFAULT 0, p_RegistrarByLeg IN NUMBER DEFAULT -1) RETURN NUMBER;

/**
 * получение номера договора для начисления комиссий ПЗО по сделке\погашению
 * @since 6.20.031
 * @qtest NO
 * @param p_DealID ID сделки
 * @return ID договора
 */
  FUNCTION GetSfContrID(p_DealID IN NUMBER, p_PayerID IN NUMBER DEFAULT 0, p_RegistrarByLeg IN NUMBER DEFAULT -1) RETURN NUMBER;

/**
 * получение даты закрытия сделки
 * @param p_DealID ID сделки
 * @return CloseDate фактическая дата закртыия
 */
 FUNCTION GetTickCloseDate( p_DealID IN NUMBER )
   RETURN DATE;

/**
 * получить код сделки
 * @since 6.20.031
 * @qtest NO
 * @param p_DocumentKind Вид сделки
 * @param p_DocumentID ID сделки
 * @param p_IsInner 1 - для получения внутреннего кода, 0 - внешнего
 * @return Код сделки
 */
  FUNCTION GetDealCode(p_DocumentKind IN NUMBER, p_DocumentID IN NUMBER, p_IsInner IN NUMBER DEFAULT 1) RETURN VARCHAR2;

/**
 * получить ID сделки
 * @since 6.20.031
 * @qtest NO
 * @param p_DocumentKind Вид сделки
 * @param p_DocumentID ID сделки
 * @return ID сделки
 */
  FUNCTION GetDealID(p_DocumentKind IN NUMBER, p_DocumentID IN NUMBER) RETURN NUMBER;

/**
 * получить ID сделки c учетом операции неттинга
 * @since 6.20.031
 * @qtest NO
 * @param p_DocumentKind Вид сделки
 * @param p_DocumentID ID сделки
 * @return ID сделки
 */
  FUNCTION GetDealIdEx(p_DocumentKind IN NUMBER, p_DocumentID IN NUMBER) RETURN NUMBER;

/**
 * получить код сделки с учетом операции неттинга, операций НУ, операций перевода ДС ПА, комиссии ПА
 * @since 6.20.031
 * @qtest NO
 * @param p_DocumentKind Вид сделки
 * @param p_DocumentID ID сделки
 * @param p_IsInner 1 - для получения внутреннего кода, 0 - внешнего
 * @return Код сделки
 */
  FUNCTION RSI_GetDealCodeEx( p_DocumentKind IN NUMBER, p_DocumentID IN NUMBER, p_IsInner IN NUMBER DEFAULT 1 ) RETURN VARCHAR2;

/**
 * получить вид операции учетом операции неттинга
 * @since 6.20.031
 * @qtest NO
 * @param p_DocumentKind Вид сделки
 * @param p_DocumentID ID сделки
 * @return Вид операции
 */
  FUNCTION RSI_GetDealKindName( p_DocumentKind IN NUMBER, p_DocumentID IN NUMBER ) RETURN DealTypeName_t DETERMINISTIC;

/**
 * Процедура сохранения обеспечения
 * @since 6.20.031
 * @qtest NO
 * @param p_DealID Идентификатор сделки
 */
  PROCEDURE RSI_SC_SaveTickEns( p_DealID IN NUMBER DEFAULT -1 );

/**
 * Получить, есть ли строки графика нужного статуса по комиссии(для использования ф-и в макросах)
 * @since 6.20.031
 * @qtest NO
 * @param pDocKind     Вид документа
 * @param pDocID       ID документа
 * @param pCONTRACT    ID договора
 * @param pPLANPAYDATE Плановая дата
 * @param pState       Статус строки графика по виду учета
 * @return 0 - если нет строк, 1- если есть строки
 */
  FUNCTION CheckExistGrDealByCom(pDocKind IN NUMBER, pDocID IN NUMBER, pCONTRACT IN NUMBER, pPLANPAYDATE IN DATE, pState IN NUMBER, pTemplNum IN NUMBER DEFAULT 0) RETURN NUMBER DETERMINISTIC;

/**
 * Получить ID ТО по строке графика (без генерации ошибок)
 * @since 6.20.031
 * @qtest NO
 * @param pGrDealID ID строки графика
 * @return ID ТО
 */
  FUNCTION SC_GetRQByGrDeal( pGrDealID IN NUMBER ) RETURN NUMBER;

/**
 * функция определяет, является ли сделка технической
 * @param p_DealID ID сделки
 * @param p_OnDate текущая дата
 * @return 1 или 0. (1 - сделка является технической, 0 - нет)
 */
  FUNCTION RSI_IsTechDeal (p_DealID IN NUMBER, p_OnDate IN DATE)
     RETURN NUMBER
     DETERMINISTIC;

/**
 * Получить срок в днях в соответствии с базисом расчёта
 * @since 6.20.031
 * @qtest NO
 * @param pDuration      срок
 * @param pTypeDuration  вид срока (RSB_SECUR.CB_PERIOD_DAY,RSB_SECUR.CB_PERIOD_WEEK,RSB_SECUR.CB_PERIOD_MONTH,RSB_SECUR.CB_PERIOD_YEAR)
 * @param pBasis         базис (cnst.BASIS_30360 и т.д.)
 * @return срок в днях в соответствии с базисом расчёта
 */
  FUNCTION GetTermInDaysByTypeDuration( pDuration     IN NUMBER,
                                        pTypeDuration IN NUMBER,
                                        pBasis        IN NUMBER
                                      ) RETURN number;

/**
 * функция определяет дату закрытия сделки = max{Valudate платежей по сделке, PlanDate шагов сделки, DateCarry проводок по сделке}
 * @param p_dockind вид документа
 * @param p_docid ID сделки
 * @return дата закрытия сделки
 */
  FUNCTION RSI_GetOperCloseDate (p_dockind IN NUMBER, p_docid IN NUMBER) RETURN DATE;

/**
 * Количество лет по базису
 * @since 6.20.031
 * @qtest NO
 * @param start_dt начало периода начисления
 * @param end_dt окончние периода начисления
 * @param basis базис ставки (cnst.BASIS_***)
 * @return Количество лет по базису
 */
  FUNCTION SC_Years( start_dt    in DATE,
                     end_dt      in DATE,
                     basis       in NUMBER
                   ) RETURN number;

/**
 * Процедура установки признака необходимости перенумерации сделок
 * @since 6.20.031
 * @qtest NO
 * @param pDate Дата
 */
  PROCEDURE RSI_SetDateCalc( pDate IN DATE );

/**
 * Анализ флагов группы видов операций
 * Это сделка из двух частей (REPO или Займ)?
 * @since 6.20.029
 * @qtest NO
 * @param OGroup Группа, к которой относится сделка
 * @return 1 - в случае успеха, иначе - 0
 */
  FUNCTION IsTwoPart( OGroup IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/*
 * Получить вид объекта по типу документа для внебиржевой операции с ПИ
 * @since 6.20.031.40.12
 * @qtest NO
 * @param p_DocKind Тип документа
 * @return вид объекта
 */
  FUNCTION GetDvnObjType( p_DocKind IN ddvndeal_dbt.t_DocKind%TYPE )
    RETURN dobjatcor_dbt.t_ObjectType%TYPE;

/*
 * Получить основную категорию объекта
 * @since 6.20.031.40.12
 * @qtest NO
 * @param p_ObjectType Вид объекта
 * @param p_Object Идентификатор объекта
 * @param p_GroupID Группа, к которой принадлежит объект
 * @param p_ValidFromDate На какую дату необходимо определить категорию
 * @return Номер категории
 */
  FUNCTION GetMainObjAttr(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE
                         ,p_Object IN dobjatcor_dbt.t_Object%TYPE
                         ,p_GroupID IN dobjatcor_dbt.t_GroupID%TYPE
                         ,p_Date IN dobjatcor_dbt.t_ValidFromDate%TYPE)
    RETURN dobjattr_dbt.t_AttrID%TYPE;

/*
 * Получить основное значение категории объекта
 * @since 6.20.031.49
 * @qtest NO
 * @param p_ObjectType Вид объекта
 * @param p_Object Идентификатор объекта
 * @param p_GroupID Группа, к которой принадлежит объект
 * @param p_ValidFromDate На какую дату необходимо определить категорию
 * @return Номер категории
 */
  FUNCTION GetGeneralMainObjAttr(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE
                                ,p_Object IN dobjatcor_dbt.t_Object%TYPE
                                ,p_GroupID IN dobjatcor_dbt.t_GroupID%TYPE
                                ,p_Date IN dobjatcor_dbt.t_ValidFromDate%TYPE)
    RETURN dobjattr_dbt.t_AttrID%TYPE DETERMINISTIC ;


/*
 * Получить основную категорию объекта без даты
 * @param p_ObjectType Вид объекта
 * @param p_Object Идентификатор объекта
 * @param p_GroupID Группа, к которой принадлежит объект
 * @return Номер категории
 */
  FUNCTION GetMainObjAttrNoDate(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE
                         ,p_Object IN dobjatcor_dbt.t_Object%TYPE
                         ,p_GroupID IN dobjatcor_dbt.t_GroupID%TYPE)
    RETURN dobjattr_dbt.t_AttrID%TYPE;

/*
 * Получить настройку bool как int
 * @return 0/1
 */
  FUNCTION GetRegBoolValueAsInt(p_KeyPath IN VARCHAR2,
                              p_Oper IN NUMBER DEFAULT NULL)
  RETURN NUMBER;

/*
 * Получить короткое название категории объекта
 * @since 6.20.031.40.12
 * @qtest NO
 * @param p_ObjectType Вид объекта
 * @param p_GroupID Группа, к которой принадлежит объект
 * @param p_AttrID Номер категории
 * @return Название
 */
  FUNCTION GetObjAttrName(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE
                         ,p_GroupID IN dobjatcor_dbt.t_GroupID%TYPE
                         ,p_AttrID IN dobjatcor_dbt.t_AttrID%TYPE)
    RETURN dobjattr_dbt.t_Name%TYPE;

/*
 * Получить полное название категории объекта
 * @since 6.20.031.48.00
 * @qtest NO
 * @param p_ObjectType Вид объекта
 * @param p_GroupID Группа, к которой принадлежит объект
 * @param p_AttrID Номер категории
 * @return Полное название
 */
  FUNCTION GetObjAttrFullName(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE
                             ,p_GroupID IN dobjatcor_dbt.t_GroupID%TYPE
                             ,p_AttrID IN dobjatcor_dbt.t_AttrID%TYPE)
    RETURN dobjattr_dbt.t_FullName%TYPE;

/*
 * Получить номер категории объекта
 * @since 6.20.031.48.00
 * @qtest NO
 * @param p_ObjectType Вид объекта
 * @param p_GroupID Группа, к которой принадлежит объект
 * @param p_AttrID Номер категории
 * @return Номер
 */
  FUNCTION GetObjAttrNumber(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE
                           ,p_GroupID IN dobjatcor_dbt.t_GroupID%TYPE
                           ,p_AttrID IN dobjatcor_dbt.t_AttrID%TYPE)
    RETURN dobjattr_dbt.t_NumInList%TYPE;

/*
 * Получить наименование объекта
 * @since 6.20.031.48.00
 * @qtest NO
 * @param p_ObjectType Вид объекта
 * @param p_GroupID Группа, к которой принадлежит объект
 * @param p_AttrID Номер категории
 * @return Наименование объекта
 */
  FUNCTION GetObjAttrNameObject(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE
                               ,p_GroupID IN dobjatcor_dbt.t_GroupID%TYPE
                               ,p_AttrID IN dobjatcor_dbt.t_AttrID%TYPE)
    RETURN dobjattr_dbt.t_NameObject%TYPE DETERMINISTIC;

/*
 * Получить запись справочника строк
 * @since 6.20.031.48.00
 * @qtest NO
 * @param p_TypeAlg Тип записи
 * @param p_NumberAlg Номер
 * @return Запись справочника строк
 */
  FUNCTION GetNameAlg(p_TypeAlg IN dNameAlg_dbt.t_iTypeAlg%TYPE
                     ,p_NumberAlg IN dNameAlg_dbt.t_iNumberAlg%TYPE)
    RETURN dNameAlg_dbt%ROWTYPE;

/*
 * Получить название записи справочника строк
 * @since 6.20.031.48.00
 * @qtest NO
 * @param p_TypeAlg Тип записи
 * @param p_NumberAlg Номер
 * @return Название записи справочника строк
 */
  FUNCTION GetNameAlgName(p_TypeAlg IN dNameAlg_dbt.t_iTypeAlg%TYPE
                         ,p_NumberAlg IN dNameAlg_dbt.t_iNumberAlg%TYPE)
    RETURN dNameAlg_dbt.t_szNameAlg%TYPE DETERMINISTIC;

/**
 * Получить номинальную стоимость за период
 * @since 6.20.031
 * @qtest NO
 * @param pFIID ФИ
 * @param pBegDate Начало периода
 * @param pEndDate Конец периода
 */
  function SC_GetNominalCostByPeriod( pFIID              IN NUMBER,
                                      pBegDate           IN DATE,
                                      pEndDate           IN DATE
                                    ) return NUMBER;

/**
 * Получить курс НКД за период (используется в отчете)
 * @since 6.20.031
 * @qtest NO
 * @param p_FIID ФИ
 * @param p_BegDate Нач. дата периода
 * @param p_EndDate Конеч. дата периода
 * @param p_NKDCourseType Вид курса
 * @param p_FaceValueFI Валюта номинала
 * @param p_HandMode Признак отбора курсов: -1 - все, 0 - репликация, 1 - ручной ввод
 * @return значение курса НКД за дату
 */
  FUNCTION SC_GetNKDCourseByPeriod( p_FIID IN NUMBER,
                                    p_BegDate IN DATE,
                                    p_EndDate IN DATE,
                                    p_NKDCourseType IN NUMBER,
                                    p_FaceValueFI IN NUMBER,
                                    p_HandMode IN NUMBER DEFAULT -1
                                  ) return NUMBER;

/**
 * Проверка для субъекта, что он является налоговым резидентом
 * @since 6.20.031
 * @qtest NO
 * @param PartyID ИД субъекта
 * @param OperDate Нач. дата
 * @return признак, что субъект является РЕЗИДЕНТОМ
 */
  FUNCTION IsTaxResident( PartyID IN NUMBER,
                          OperDate IN DATE ) RETURN NUMBER DETERMINISTIC;


/** Получить репозитарный статус сделки
 * @param dealID id сделки
 * @param kindID
 */
  FUNCTION GetRStatus (dealID IN NUMBER, kindID IN NUMBER)
   RETURN NUMBER;

/** Проверить на сделку на 093 сообщение
 * @param dealID id сделки
 * @param kindID
 */

   FUNCTION StatusSRS_Is093 (dealID IN NUMBER, kindID IN NUMBER)
   RETURN NUMBER;

/** Проверить на сделку на 094 сообщение
 * @param dealID id сделки
 * @param kindID
 */

   FUNCTION StatusSRS_Is094 (dealID IN NUMBER, kindID IN NUMBER)
   RETURN NUMBER;

/** Найти ID графика(последнего,по которому утснавливается статус)
 * @param dealID id сделки
 * @param kindID
 */

   FUNCTION Get_GR_ID_SRS (dealID IN NUMBER, kindID IN NUMBER)
   RETURN NUMBER;

/**
 * получить сумму переведенную из pFromFI в pToFI по курсу pType и далее в pToVR (используется в отчете)
 * @since 6.20.031
 * @qtest NO
 * @param SumB    сумма
 * @param pFromFI из какого ФИ
 * @param pToFI   в какой ФИ 1
 * @param pToVR   в какой ФИ 2
 * @param pType   тип курса
 * @param pbdate  на дату курса
 * @param pMarketId  ID торговой площадки
 * @param pRate      значение курса
 * @param pRateID    ID курса
 * @param pRateDate  дата курса
 */
 
  PROCEDURE SC_ConvSumTypeRepProc( SumB      IN NUMBER
                                  ,pFromFI   IN NUMBER
                                  ,pToFI     IN NUMBER
                                  ,pToVR     IN NUMBER
                                  ,pType     IN NUMBER
                                  ,pbdate    IN DATE
                                  ,pRate     OUT NUMBER
                                  ,pRateID   OUT NUMBER
                                  ,pRateDate OUT DATE
                                  ,pMarketId IN NUMBER DEFAULT -1
                                 );

/**
 * получить сумму переведенную из pFromFI в pToFI по курсу pType и далее в pToVR (используется в отчете)
 * @since 6.20.031
 * @qtest NO
 * @param SumB    сумма
 * @param pFromFI из какого ФИ
 * @param pToFI   в какой ФИ 1
 * @param pToVR   в какой ФИ 2
 * @param pType   тип курса
 * @param pbdate  на дату курса
 * @param pMarketId  ID торговой площадки
 * @return сконвертированная сумма
 */
  FUNCTION SC_ConvSumTypeRep( SumB     IN NUMBER
                             ,pFromFI  IN NUMBER
                             ,pToFI    IN NUMBER
                             ,pToVR    IN NUMBER
                             ,pType    IN NUMBER
                             ,pbdate   IN DATE
                             ,pMarketId IN NUMBER DEFAULT -1
                            ) return NUMBER;
                            
/**
 * Получить ставку налога для покупателя/продавца по 1 части РЕПО при выплате купона
 * @since 6.20.031
 * @qtest NO
 * @param p_PartyID  Идентификатор субъекта
 * @param p_FIID     Идентификатор ц/б
 * @param p_OnDate   Дата
 * @return ставка налога*/
  FUNCTION GetCoupREPO_TaxRate(p_PartyID IN NUMBER, p_FIID IN NUMBER, p_OnDate IN DATE) RETURN ddlrq_dbt.t_TaxRateBuy%type;

/**
 * Получение стоимости бумаг по курсу для 712 формы
 * @since 6.20.031
 * @qtest NO
 * @param Amount     кол-во ц/б
 * @param pFIID      ID ц/б
 * @param pbdate     на дату курса
 * @param pFI_Kind   вид фи
 * @param pAvoirKind вид бумаги
 * @param pMarketId  ID торговой площадки
 * @return стоимость бумаг по курсу
 */
  FUNCTION SC_GetCostByCourse712(Amount     IN NUMBER,
                                 pFIID      IN NUMBER,
                                 pbdate     IN DATE,
                                 pFI_Kind   IN NUMBER,
                                 pAvoirKind IN NUMBER,
                                 pMarketId  IN NUMBER) RETURN NUMBER;

/** Проверяет есть ли номинал на дату котировки ДЛЯ БУМАГ С ИНДЕКСИРУЕМЫМ НОМИНАЛОМ.
 *  Hardcoded для 712 формы
 * @since 6.20.031
 * @qtest NO
 * @param pFIID       ID ц/б
 * @param pDate       дата отчета
 * @return            1 - номинал на дату котировки есть или бумага не с ИН, 0 - нет 
 */
  FUNCTION SC_HasNominalOnRateDate712(pFIID IN NUMBER,
                                      pDate IN DATE) RETURN NUMBER;

/** Возвращает дату номинала ДЛЯ БУМАГ С ИНДЕКСИРУЕМЫМ НОМИНАЛОМ.
 *  Hardcoded для 712 формы
 * @since 6.20.031
 * @qtest NO
 * @param pFIID       ID ц/б
 * @param pDate       дата отчета
 * @return            Дата номинала (BegDate)
 */
  FUNCTION SC_GetNominalDate712(pFIID IN NUMBER,
                                pDate IN DATE) RETURN DATE;

/**
 * Получить количество ц/б в выписки на дату для строки регистра КДУ
 * @since 6.20.031
 * @qtest NO
 * @param p_QRID идентификатор записи в регистре КДУ
 * @param p_OnDate дата остатка
 * @return количество ц/б
 */
  FUNCTION GetQRRestOnDate(p_QRID   IN NUMBER
                          ,p_OnDate IN DATE
                          ) return NUMBER;

/**
 * Получить количество ц/б в обеспечении на дату
 * @since 6.20.031
 * @qtest NO
 * @param p_SecAgrID идентификатор договора обеспечения
 * @param p_FIID идентификатор ц/б
 * @param p_Department филиал
 * @param p_OnDate дата
 * @return количество ц/б
 */
  FUNCTION GetQntySecPledgedOnDate(p_SecAgrID IN NUMBER
                                   ,p_FIID IN NUMBER
                                   ,p_Department IN NUMBER
                                   ,p_OnDate IN DATE
                                   ) return NUMBER;

/**
 * Получить количество заблокированных ц/б на дату
 * @since 6.20.031
 * @qtest NO
 * @param p_SecAgrID идентификатор договора обеспечения
 * @param p_FIID идентификатор ц/б
 * @param p_Department филиал
 * @param p_OnDate дата
 * @return количество ц/б
 */
  FUNCTION GetQntySecBlockedOnDate(p_SecAgrID IN NUMBER
                                  ,p_FIID IN NUMBER
                                  ,p_Department IN NUMBER
                                  ,p_OnDate IN DATE
                                  ) return NUMBER;

/**
 * Получить количество заблокированных ц/б на дату по сделке
 * @since 6.20.031
 * @qtest NO
 * @param p_SecAgrID идентификатор договора обеспечения
 * @param p_FIID идентификатор ц/б
 * @param p_DealID идентификатор сделки
 * @param p_OnDate дата
 * @return количество ц/б
 */
  FUNCTION GetQntySecBlockedOnDateByDeal(p_SecAgrID IN NUMBER
                                        ,p_FIID IN NUMBER
                                        ,p_DealID IN NUMBER
                                        ,p_OnDate IN DATE
                                        ) return NUMBER;


/**
 * Получить вид объекта сделки по виду первичного документа
 * @since 6.20.031
 * @qtest NO
 * @param p_BOfficeKind Вид первичного документа
 * @return Вид объекта
 */
  FUNCTION GetDealObjType(p_BOfficeKind IN NUMBER) RETURN NUMBER;

/**
 * Установить значение категории "Тест на рыночность пройден" для сделки
 * @deprecated Код устарел.
 * @since 6.20.031.48
 * @qtest NO
 * @param p_DealID Идентификатор сделки
 * @param p_OnDate Дата установки
 * @param p_AttrID Идентификатор значения
 */
  PROCEDURE SetDealMarketTestAttrID(p_DealID IN NUMBER, p_OnDate IN DATE, p_AttrID IN NUMBER);

/**
 * Провести тест на рыночность для сделки размещения ОЭБ- результат возвращается
 * @deprecated Код устарел.
 * @since 6.20.031.48
 * @qtest NO
 * @param p_DealID Идентификатор сделки
 * @param p_OnDate Дата установки
 * @return Идентификатор значения категории "Тест на рыночность пройден"
 */
  FUNCTION CalcExecDealMarketTestOwn(p_DealID IN NUMBER, p_OnDate IN DATE) RETURN NUMBER;

/**
 * Выполнение теста на рыночность на шаге сделки размещения ОЭБ
 * @deprecated Код устарел.
 * @since 6.20.031.48
 * @qtest NO
 * @param p_DealID Идентификатор сделки
 * @param p_OnDate Дата установки
 */
  PROCEDURE ExecDealMarketTestOwn(p_DealID IN NUMBER, p_OnDate IN DATE);

/**
 * Пакетное выполнение теста на рыночность при пакетном исполнении сделок размещения ОЭБ
 * @deprecated Код устарел.
 * @since 6.20.031.48
 * @qtest NO
 */
  PROCEDURE Mass_ExecDealMarketTestOwn;

/**
 * Рассчитать ЭПС по сделке
 * @since 6.20.031.48
 * @qtest NO
 * @param p_CalcKind Вид расчета
 * @param p_DocID id сделки
 * @param p_SumID SumId лота (можно передавать, если лот существует, иначе  0)
 * @param p_CalcDate Дата расчета
 * @return Значение ЭПС
 */
  FUNCTION CalcEPS(p_CalcKind IN NUMBER, p_DocID IN NUMBER, p_SumID IN NUMBER, p_CalcDate IN DATE DEFAULT UnknownDate) RETURN NUMBER;

/**
 * Рассчитать ЭПС
 * @since 6.20.031.48
 * @qtest NO
 * @return Значение ЭПС
 */
  FUNCTION CalcEPSNoFill RETURN NUMBER;


/**
 * Рассчитать АС в дату первоначального признания
 * @since 6.20.031.60
 * @qtest NO   
 * @param p_CalcKind Вид расчета
 * @param p_DocID id сделки
 * @param p_SumID SumId лота (можно передавать, если лот существует, иначе  0)
 * @param p_CalcDate дата расчета
 * @param p_FairValue Справедливая стоимость на дату расчета
 * @return Значение АС (для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)
 */
  FUNCTION CalcAS_EPS0(p_CalcKind IN NUMBER,
                       p_DocID IN NUMBER,
                       p_SumID IN NUMBER,
                       p_CalcDate IN DATE,
                       p_FairValue IN NUMBER) RETURN NUMBER;

/**
 * Рассчитать АС по ЭПС
 * @since 6.20.031.48
 * @qtest NO
 * @param p_CalcKind Вид расчета
 * @param p_DocID id сделки
 * @param p_SumID SumId лота (можно передавать, если лот существует, иначе  0)
 * @param p_CalcDate дата расчета
 * @param p_EPS ЭПС
 * @return Значение АС (для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)
 */
  FUNCTION CalcAS_EPS(p_CalcKind IN NUMBER,
                      p_DocID IN NUMBER,
                      p_SumID IN NUMBER,
                      p_CalcDate IN DATE,
                      p_EPS IN NUMBER) RETURN NUMBER;

/**
 * Рассчитать АС ФИ линейным методом
 * @since 6.20.031.53
 * @qtest NO
 * @param p_CalcKind Вид расчета
 * @param p_DocID id сделки
 * @param p_SumID SumId лота (можно передавать, если лот существует, иначе  0)
 * @param p_CalcDate дата расчета
 * @return Значение АС (для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)
 */
  FUNCTION CalcAS_Line(p_CalcKind IN NUMBER,
                       p_DocID IN NUMBER,
                       p_SumID IN NUMBER,
                       p_CalcDate IN DATE) RETURN NUMBER;

/**
 * Рассчитать корректировку % до ЭПС
 * @since 6.20.031.48
 * @qtest NO
 * @param p_CalcKind Вид расчета
 * @param p_DocID id сделки
 * @param p_SumID SumId лота
 * @param p_CalcDate дата расчета (t)
 * @param p_PrevCalcDate дата расчета (t-1)
 * @param p_InterestIncome SПД (Сумма начисленных процентов за интервал дат[t0;t])
 * @param p_Bonus SПр (Сумма начисленной/списанной премии за интервал дат[t0;t])
 * @param p_DiscountIncome SДД (сумма начисленного/списанного дисконта за интервал дат[t0;t])
 * @param p_Outlay Sзатр (сумма списанных на дату расчета затрат за интервал дат[t0;t])
 * @param p_FairValue СС (СС ФИ при первоначальном признании)обязательный параметр, если t-1 = дате первоначального признания
 * @param p_EPS ЭПС действующий на дату t
 * @return Корректировка%_ЭПС со знаком (для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)
 */
  FUNCTION CalcCorrectPersentEPS(p_CalcKind IN NUMBER,
                                 p_DocID IN NUMBER,
                                 p_SumID IN NUMBER,
                                 p_CalcDate IN DATE,
                                 p_PrevCalcDate IN DATE,
                                 p_InterestIncome IN NUMBER,
                                 p_Bonus IN NUMBER,
                                 p_DiscountIncome IN NUMBER,
                                 p_Outlay IN NUMBER,
                                 p_FairValue IN NUMBER,
                                 p_EPS IN NUMBER) RETURN NUMBER;

/**
 * Получить значение РПС для уровня существенности отклонение ЭПС (с учетом настройки SECUR\МСФО\МЕТОД_РАСЧЕТА_ОТКЛ_ЭПС)
 * @since 6.20.54
 * @qtest NO
 * @param pCalcKind вид расчета (ПРЕПО/ОРЕПО, ОЭБ, УВ/СВ)
 * @param pServiseKind Вид обслуживания
 * @param pBranchID Подразделение ТС
 * @param pProductKindID Вид банковского продукта
 * @param pProductID Банковский продукт
 * @param pPeriodDays Срок в днях
 * @param pContractSum Сумма договора
 * @param pFiID Валюта договора
 * @param pClientType Тип клиента
 * @param pDate Дата
 * @param pMarketRateMin Мин. значение РПС (выходной)
 * @param pMarketRateMax Макс. значение РПС (выходной)
 * @param pSourceDataLayer Уровень исходных данных (выходной)
 * @param pIssuer Эмитент
 * @param pSumID Id лота 
 * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
  FUNCTION GetRPS_LevelDevEPS(pCalcKind IN NUMBER,
                              pServiseKind IN NUMBER,
                              pBranchID IN NUMBER,
                              pProductKindID IN NUMBER,
                              pProductID IN NUMBER,
                              pPeriodDays IN NUMBER,
                              pContractSum IN NUMBER,
                              pFiID IN NUMBER,
                              pClientType IN NUMBER,
                              pDate IN DATE,
                              pMarketRateMin IN OUT NUMBER,
                              pMarketRateMax IN OUT NUMBER,
                              pSourceDataLayer IN OUT NUMBER,
                              pIssuer IN NUMBER DEFAULT 0,
                              pSumID IN NUMBER DEFAULT 0
                             ) RETURN NUMBER;

/**
 * Определение значениий уровня существенности
 * @since 6.20.048
 * @qtest NO
 * @param p_LevelEssential вид уровня
 * @param p_Portfolio портфель
 * @param p_DateStart дата значения
 * @param p_AbsoluteValue абсолютное значение уровня
 * @param p_RelativeValue относительное значение уровня
 * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
  FUNCTION GetLevelEssential(p_LevelEssential IN NUMBER
                            ,p_Portfolio IN NUMBER
                            ,p_DateStart IN DATE
                            ,p_AbsoluteValue OUT NUMBER
                            ,p_RelativeValue OUT NUMBER
                            ) return NUMBER;

/**
 * Проверка на существенность отклонения
 * @since 6.20.048
 * @qtest NO
 * @param p_LevelEssential вид уровня
 * @param p_Portfolio портфель
 * @param p_CalcDate дата расчета
 * @param p_DoCompare что сравниваем
 * @param p_ToCompare1 с чем сравниваем 1
 * @param p_ToCompare2 с чем сравниваем 2
 * @param p_ObjectKind вид объекта
 * @param p_ObjectID id объекта
 * @param p_IsEssential Да/нет
 * @param p_RateKind Наименование ставки используемой в дальнейшем
 * @param p_RateVal Значение ставки
 * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
  FUNCTION GetEssentialDev(p_LevelEssential IN NUMBER
                          ,p_Portfolio IN NUMBER
                          ,p_CalcDate IN DATE
                          ,p_DoCompare IN NUMBER
                          ,p_ToCompare1 IN NUMBER
                          ,p_ToCompare2 IN NUMBER
                          ,p_ObjectKind IN NUMBER
                          ,p_ObjectID IN NUMBER
                          ,p_IsEssential OUT BOOLEAN
                          ,p_RateKind OUT NUMBER
                          ,p_RateVal OUT NUMBER
                          ) return NUMBER;

/**
 * Проверка на существенность отклонения
 * @since 6.20.048
 * @qtest NO
 * @param p_LevelEssential вид уровня
 * @param p_Portfolio портфель
 * @param p_CalcDate дата расчета
 * @param p_DoCompare что сравниваем
 * @param p_ToCompare1 с чем сравниваем 1
 * @param p_ToCompare2 с чем сравниваем 2
 * @param p_ObjectKind вид объекта
 * @param p_ObjectID id объекта
 * @param p_IsEssential Да/нет
 * @param p_RateKind Наименование ставки используемой в дальнейшем
 * @param p_RateVal Значение ставки
 * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
  FUNCTION GetEssentialDevInt(p_LevelEssential IN NUMBER
                             ,p_Portfolio IN NUMBER
                             ,p_CalcDate IN DATE
                             ,p_DoCompare IN NUMBER
                             ,p_ToCompare1 IN NUMBER
                             ,p_ToCompare2 IN NUMBER
                             ,p_ObjectKind IN NUMBER
                             ,p_ObjectID IN NUMBER
                             ,p_IsEssential OUT NUMBER
                             ,p_RateKind OUT NUMBER
                             ,p_RateVal OUT NUMBER
                             ) return NUMBER;

/**
 * Получить значение категории "Тест на рыночность пройден" для сделки
 * @since 6.20.031.48
 * @qtest NO
 * @param p_BOfficeKind Вид ПД сделки
 * @param p_DealID Идентификатор сделки
 * @return Идентификатор значения категории "Тест на рыночность пройден"
 */
  FUNCTION GetDealMarketTestAttrID(p_BOfficeKind IN NUMBER, p_DealID IN NUMBER) RETURN NUMBER;

/**
 * Проверка на существенность отклонения затрат по договору
 * @since 6.20.031.48
 * @qtest NO
 * @param p_CalcDate дата расчета
 * @param p_Portfolio портфель
 * @param p_DoCompare что сравниваем в рублях
 * @param p_ToCompare с чем сравниваем в реблях
 * @return 1 - существенны, 0 - не существенны
 */
  FUNCTION IsEssentialSumByContrCost(p_CalcDate DATE, p_PortfolioID NUMBER, p_DoCompareRUB NUMBER, p_ToCompareRUB NUMBER) RETURN NUMBER;

/**
 * Проверить, существенность отклонения СС по сделке T+ на дату заключения от СС на дату расчета переоценки
 * @since 6.20.031.53
 * @qtest NO
 * @param p_DealID идентификатор сделки
 * @param p_OperDate  дата переоценки
 * @return ? 0-нет, 1 ? да
 */
  FUNCTION DealIsEssentialByOverTPlus( p_DealID IN NUMBER ,  p_OperDate IN DATE ) RETURN NUMBER;

/**
 * Получить сумму переоценки Т+ по сделке на дату
 * @since 6.20.031.53
 * @qtest NO
 * @param p_DealID идентификатор сделки
 * @param p_Kind  вид записи
 * @param p_OnDate  дата переоценки
 * @param p_ExcludeWrt признак необходимости исключить обнуление переоценки Т+ при поставке, т.е. получить последнее значение до поставки
 * @return сумму переоценки
 */
  FUNCTION GetDealSumOverTPlusOnDate( p_DealID IN NUMBER ,  p_Kind IN NUMBER , p_OnDate IN DATE, p_ExcludeWrt IN NUMBER DEFAULT 0 ) RETURN NUMBER;

/**
 * Установить значение категории для ФИ
 * @since 6.20.031.53
 * @qtest NO
 * @param pGroupID id группы
 * @param pFIID id ФИ
 * @param pOnDate на дату
 * @param pAttrID id атрибута значения
 */
  PROCEDURE SetFICategoryAttrID(pGroupID IN NUMBER, pFIID IN NUMBER, pOnDate IN DATE, pAttrID IN NUMBER);

/**
 * Определить справедливую стоимость (Порядок использования алгоритмов)
 * @since 6.20.031.53
 * @qtest NO
 * @param pFIID идентификатор ц/б
 * @param pCalcDate дата расчета
 * @param pCalcKind вид расчета (0 необязательный)
 * @param pDealID id сделки (0 необязательный)
 * @param pSumID id лота (0 необязательный)
 * @param pEndCoupDate Дата окончания периода расчета купона
 * @param pFairValue сумма СС (в ВН). Если pAlgUsed=ALG_DISCCOSTSTREAM, то СС на кол-во бумаг из сделки (для ЦБ(покупка), ОЭБ в ВН, для РЕПО в ВЦ)
 * @param pMsg сообщение
 * @param pRateID курс, использованный для вычисления СС
 * @param pCat55 значение для категории "Уровень исходных данных иерархии СС МСФО 13"
 * @param pCat60 значение для категории "Наблюдаемые исходные данные"
 * @param pAlgUsed примененный алгоритм
 * @param pEIR ставка (может быть изменена, если применен алгоритм ALG_DISCCOSTSTREAM)
 * @param pAmortCalcKind Вид расчета АС (может быть изменен, если применен алгоритм ALG_DISCCOSTSTREAM)
 * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
  FUNCTION SC_CalcFairValue( pFIID IN NUMBER,
                             pCalcDate IN DATE,
                             pCalcKind IN NUMBER,
                             pDealID IN NUMBER,
                             pSumID IN NUMBER,
                             pEndCoupDate IN DATE,
                             pFairValue OUT NUMBER,
                             pMsg OUT VARCHAR2,
                             pRateID OUT NUMBER,
                             pCat55 OUT NUMBER,
                             pCat60 OUT NUMBER,
                             pAlgUsed OUT NUMBER,
                             pEIR OUT NUMBER,
                             pAmortCalcKind OUT NUMBER
                           ) RETURN NUMBER;

/**
 * Заполнить таблицу DDLSUBACCMP_TMP
 * @since 6.20.031.53
 * @qtest NO
 * @param p_dlcontrid DLCONTR ID
 */
  PROCEDURE LoadSubAcc(p_dlcontrid number);

/**
 * Добавить сообщение во временную таблицу лога
 * @since 6.20.031.54
 * @qtest NO
 * @param pDealCode код сделки
 * @param pErrCode код ошибки
 * @param pMsg сообщение
 */
PROCEDURE AddWarningLogTmp( pDealCode IN VARCHAR2, pErrCode IN NUMBER, pMsg IN VARCHAR2 );

/**
 * Перевести из кириллицы в латиницу
 * @since 6.20.031.54
 * @qtest NO
 * @param pStr исходная строка
 * @return конвертированная строка
 */
FUNCTION Translit(pStr IN VARCHAR2) RETURN VARCHAR2;

/**
 * Сохранить рассчитанные на шаге налоги для владельцев ц/б
 * @since 6.20.031.54
 * @qtest NO
 * @param p_ID_Operation Идентификатор операции
 * @param p_ID_Step Идентификатор шага
 */
  PROCEDURE SaveOwnTaxFromTMP( p_ID_Operation IN NUMBER,
                               p_ID_Step      IN NUMBER
                             );

/**
 * Откатить рассчитанные на шаге налоги для владельцев ц/б
 * @since 6.20.031.54
 * @qtest NO
 * @param p_ID_Operation Идентификатор операции
 * @param p_ID_Step Идентификатор шага
 */
  PROCEDURE BackOutOwnTax( p_ID_Operation IN NUMBER,
                           p_ID_Step      IN NUMBER
                         );

  PROCEDURE RSI_SetAccountedFrVal( pDocID IN NUMBER, pDocKind IN NUMBER, pOperDate  IN DATE, pID_Operation IN NUMBER, pID_Step IN NUMBER, pGrpID IN NUMBER );
  PROCEDURE RSI_RestoreSetAccountedFrVal( pID_Operation IN NUMBER, pID_Step IN NUMBER, pGrpID IN NUMBER );

/**
 * Функция получения кода на заданную дату
 * @since 6.20.031.54
 * @qtest NO
 * @param p_ObjectType Тип объекта
 * @param p_CodeKind Вид кода
 * @param p_ObjectID Идентификатор объекта
 * @param p_Date Дата
 * @return Код на дату
 */
FUNCTION SC_GetObjCodeOnDate( p_ObjectType IN NUMBER,
                              p_CodeKind   IN NUMBER,
                              p_ObjectID   IN NUMBER,
                              p_Date       IN DATE DEFAULT NULL
                            ) RETURN VARCHAR2;

/**
 * Функция получения даты окончания периода расчета купона, в зависимости от даты расчета
 * @since 6.20.031.54
 * @qtest NO
 * @param pFIID идентификатор ц/б
 * @param pCalcDate дата расчета
 * @return Дата окончания периода расчета купона
 */
FUNCTION GetEndCoupDate( pFIID IN NUMBER, pCalcDate IN DATE ) RETURN DATE;

/**
 * Проверить соответствие схемы расчетов виду рынка и принадлежности средств (используется в листалке схем расчетов в панели расчетов с биржей и обработки итогв вал торгов)
 * @since 6.20.031.61
 * @qtest NO
 * @param pSchemeID ID схемы расчетов
 * @param pMoneySource принадлежность средств из панели операции
 * @param pMarketKind вид биржевого рынка
 * @return 1 - да, 0 - нет
 */
  FUNCTION DL_IsSuitMarketScheme( pSchemeID IN NUMBER, pMoneySource IN NUMBER, pMarketKind IN NUMBER ) RETURN NUMBER;

/**
 * Проверить соответствие схемы расчетов расчетному коду заданного рынка и принадлежности средств
 * @since 6.20.031.61
 * @qtest NO
 * @param pSchemeID ID схемы расчетов
 * @param pMarketKind вид рынка (помимо Т+ ЕП)
 * @param pMoneySource принадлежность средств из панели операции
 * @return 1 - да, 0 - нет
 */
  FUNCTION DL_IsSuitMarketSchemeInSettleCode( pSchemeID IN NUMBER, pMarketKind IN NUMBER, pMoneySource IN NUMBER ) RETURN NUMBER;
  
/**
 * Проверить соответствие схемы расчетов заданному сектору расчетного центра и принадлежности средств
 * @since 6.20.031.61
 * @qtest NO
 * @param pSchemeID ID схемы расчетов
 * @param pCentrID идентификатор расчетного центра
 * @param pCentrOfficeID идентификатор сектора расчетного центра
 * @param pMoneySource принадлежность средств из панели операции
 * @return 1 - да, 0 - нет
 */
  FUNCTION DL_IsSuitMarketSchemeByOffice( pSchemeID IN NUMBER, pCentrID IN NUMBER, pCentrOfficeID IN NUMBER, pMoneySource IN NUMBER ) RETURN NUMBER;

  function get_BA_Kind(FI_Kind number, AvoirKind number, root number) return number DETERMINISTIC;
  
  function get_FI_Code(FIID number, ObjectType number, CodeKind number) return varchar2;

/**
 * Подкачать название вида ц/б по группе обработки
 * @since 6.20.031.63
 * @qtest NO
 * @param p_GrpID ID группы обработки
 * @return Название вида ц/б
 */
  FUNCTION GetPmWrtGrpAvrName(p_GrpID IN NUMBER) RETURN PmWrtGrpAvrName_t DETERMINISTIC;

/**
 * Подкачать код ц/б по группе обработки
 * @since 6.20.031.63
 * @qtest NO
 * @param p_GrpID ID группы обработки
 * @return Код ц/б
 */
  FUNCTION GetPmWrtGrpFiCode(p_GrpID IN NUMBER) RETURN PmWrtGrpFiCode_t DETERMINISTIC;

/**
 * Подкачать наименование ц/б по группе обработки
 * @since 6.20.031.63
 * @qtest NO
 * @param p_GrpID ID группы обработки
 * @return Наименование ц/б
 */
  FUNCTION GetPmWrtGrpFiName(p_GrpID IN NUMBER) RETURN PmWrtGrpFiName_t DETERMINISTIC;

/**
 * Подкачать код валюты номинала ц/б по группе обработки
 * @since 6.20.031.63
 * @qtest NO
 * @param p_GrpID ID группы обработки
 * @return Код валюты номинала
 */
  FUNCTION GetPmWrtGrpFiFvCode(p_GrpID IN NUMBER) RETURN PmWrtGrpFiFvCode_t DETERMINISTIC;

/**
 * Процедура заливки ID итоговых проводок клиринга во временную таблицу для отображения на вкладке "Проводки клиринга" конкретной операции "Расчетов с биржей"
 * @since 6.20.031
 * @qtest NO
 * @param p_DocID ID операции расчетов с биржей
 * @param p_DocKind вид документа операции расчетов с биржей
 */
  PROCEDURE FillItogClirAccTrn( p_DocID IN NUMBER, p_DocKind IN NUMBER );                                                   


/**
 * Определить СС ФИ используя курсы
 * @since 6.20.031.63
 * @qtest NO
 * @param pFIID Идентификатор ФИ
 * @param pCalcDate Дата расчета
 * @param pRateID Идентификатор курса (выходной)
 * @param pRateDate Дата курса (выходной)
 * @return Справедливая стоимость
 */
  FUNCTION GetFairValueFromRates( pFIID IN NUMBER,
                                  pCalcDate IN DATE,
                                  pRateID OUT NUMBER,
                                  pRateDate OUT DATE ) RETURN NUMBER;

/**
 * Получить верхнюю дату расчета, если на бумаге задана категория Расчет корректировок до даты ближайшей оферты
 * @since 6.20.031.63
 * @qtest NO
 * @param pCalcKind вид расчета (Ц/Б, ОЭБ)
 * @param pFiID идентификатор ФИ
 * @param pCalcDate дата расчета
 * @param pIsOffer признак оферты на ФИ
 * @param pTermless признак бессрочной ФИ
 * @param pOfferDate дата оферты
 * @return Верхняя дата расчета
 */
  FUNCTION SC_GetTopCalcDate(pCalcKind IN NUMBER, pFiID IN NUMBER, pCalcDate IN DATE, pIsOffer OUT NUMBER, pTermless OUT CHAR, pOfferDate OUT DATE) RETURN DATE;

/**
 * Функции заполнения dcashflows_tmp
 * @since 6.20.031.64
 * @qtest NO
 * @param pSumID - ID лота
 * @param pDate  - За дату
 */
  PROCEDURE FillCashFlowsTmpForAC(pSumID IN NUMBER, pDate IN DATE);
  PROCEDURE FillCashFlowsTmpForEIR(pSumID IN NUMBER, pDate IN DATE);

/**
 * Загрузка счетов для выбора в листалке панели переводов по РЕПО в операции "Расчетов с биржей"
 * @since 6.20.031
 * @qtest NO
 * @param p_Kind вид перевода
 * @param p_Kind валюта перевода
 * @param p_Date дата перевода
 */
  PROCEDURE LoadRepoAccForTransf(p_Kind IN NUMBER, p_Currency IN NUMBER, p_Date IN DATE);
 
 /**
 * Определить СС ФИ используя курсы (!!!с учетом инсталяции РСХБ!!!)
 * @since 6.20.031.63
 * @qtest NO
 * @param pFIID Идентификатор ФИ
 * @param pCalcDate Дата расчета
 * @param pRateID Идентификатор курса (выходной)
 * @param pRateDate Дата курса (выходной)
 * @return Справедливая стоимость
 */
  FUNCTION GetFairValueFromRates_RSHB( pFIID IN NUMBER,
                                  pCalcDate IN DATE,
                                  pRateID OUT NUMBER,
                                  pRateDate OUT DATE ) RETURN NUMBER;

 /**
    * процедура вставки/изменения параметров подтверждения сделки
    * @since 6.20.031
    * @qtest NO
    * p_DocKind  Вид сделки
    * p_DocId    ID сделки
    * p_Status    Статус подтверждения
    * p_Condition Состояние подтверждения
    Если запись с ключом p_DocKind/p_DocID уже существует, то происходит ее обновление заданными p_Status/p_Condition
    Если р_Status <= 0, то T_STATUS := p_Condition.
    */
   PROCEDURE InsertDLMes(p_DocKind IN INTEGER, p_DocID IN INTEGER, p_Status IN INTEGER, p_Condition IN INTEGER);

 /**
 * Функция получения признака нерезидентности на заданную дату
 * @since 6.20.031
 * @qtest NO
 * @param p_PartyID Идентификатор субъекта
 * @param p_Date Дата
 * @return значение нерезидентности: 'Х' - нерезидент, ' ' - резидент
 */
  FUNCTION DL_GetNotResidentForBrokClientRep (p_PartyID IN NUMBER, p_Date IN DATE) RETURN CHAR;

  /**
 * Функция получения статуса платежа на заданную дату
 * @since 6.20.031
 * @qtest NO
 * @param p_PaymID Идентификатор платежа
 * @param p_Date Дата, на которую требуется узнать статус
 * @return статус платежа по DPMSTATUS_DBT
 */  
  FUNCTION DL_GetPaymStatusOnDate( p_PaymID IN NUMBER, p_Date IN DATE ) RETURN NUMBER;

/**
 * Функция получения ставки налога для ФЛР в виде строки в список истории владения облигациями банка 
 * @since 6.20.031                        
 * @qtest NO
 * @param p_SHID Идентификатор записи в истории
 * @return Строковое значение ставки налога
 */
  FUNCTION GetOwnTaxRateStr(p_SHID IN NUMBER) RETURN VARCHAR2;

/**
 * Функция определения необходимости помечать ДБО в списке 
 * @since 6.20.031                        
 * @qtest NO
 * @param p_DlContrID Идентификатор ДБО
 * @param p_OnDate Дата
 * @return 0 - не нужно помечать; 1 - нужно помечать
 */
  FUNCTION NeedDlContrMark(p_DlContrID IN NUMBER, p_OnDate IN DATE) RETURN NUMBER deterministic ;

/**
 * Функция возвращающая список отмеченых ДБО ( обратная от NeedDlContrMark)
 * @since 6.20.031
 * @qtest NO
 * @param p_OnDate Дата
 */
  function SelectNeedDlContrMark(p_OnDate in date) return tt_DLCONTRID   pipelined ;

/**
 * Функция получения суммы НОБ по ДБО 
 * @since 6.20.031                        
 * @qtest NO
 * @param p_DlContrID Идентификатор ДБО
 * @param p_OnDate Дата
 * @return сумма НОБ
 */
  FUNCTION GetDlContrTaxBase(p_DlContrID IN NUMBER, p_OnDate IN DATE, p_TaxBaseKind IN NUMBER DEFAULT 0) RETURN NUMBER deterministic;


/**
 * Расчет налога по плательщику в операции возврата дивидендов по РЕПО
 * @qtest  NO
 * @since  6.20.031.70.0
 * @param  p_PartyID         Владелец ц/б, по которого считается доход
 * @param  p_FIID            Ц/б, по которой считается доход
 * @param  p_CalcDate        Дата, на которую нужно искать курс, если будет необходимость в конвертации
 * @param  p_DivDeal         Сумма, от которой считать налог                                           
 * @param  p_DivCurr         Валюта, в которой указанна сумма                                          
 * @param  p_RatePay         Ставка налога                         
 * @param  p_D1              Значение Д1                                                                          
 * @param  p_D2              Значение Д2                                                                          
 * @param  p_TaxCurr         Валюта, в которой считать налог                                           
 * @param  p_Tax             Сумма налога по общей ставке                                                                           
 * @param  p_TaxIncr         Сумма налога по повышенной ставке                                                                           
 */

  PROCEDURE CalcPayerTaxDivRet(p_PartyID IN NUMBER,
                               p_FIID IN NUMBER,
                               p_CalcDate IN DATE,
                               p_DivDeal IN NUMBER,
                               p_DivCurr IN NUMBER,
                               p_RatePay IN NUMBER,
                               p_D1 IN NUMBER,
                               p_D2 IN NUMBER,
                               p_TaxCurr IN NUMBER,
                               p_Tax OUT NUMBER,
                               p_IncrTax OUT NUMBER
                              );


/**
 * Расчет налога по получателю в операции возврата дивидендов по РЕПО
 * @qtest  NO
 * @since  6.20.031.70.0
 * @param  p_PartyID         Владелец ц/б, по которого считается доход
 * @param  p_FIID            Ц/б, по которой считается доход
 * @param  p_CalcDate        Дата, на которую нужно искать курс, если будет необходимость в конвертации
 * @param  p_RetDivDealID    Идентификатор операции возврата
 * @param  p_DivDeal         Сумма, от которой считать налог                                          
 * @param  p_DivCurr         Валюта, в которой указанна сумма                                          
 * @param  p_RatePay         Ставка налога                         
 * @param  p_D1              Значение Д1                                                                          
 * @param  p_D2              Значение Д2                                                                          
 * @param  p_TaxCurr         Валюта, в которой считать налог                                           
 * @param  p_Tax             Сумма налога по общей ставке                                                                           
 * @param  p_TaxIncr         Сумма налога по повышенной ставке
 * @param  p_ParmStr         Строка с данными для объектов НДР                                                                           
 */
  PROCEDURE CalcReceiverTaxDivRet(p_PartyID IN NUMBER,
                                  p_FIID IN NUMBER,
                                  p_CalcDate IN DATE,
                                  p_RetDivDealID IN NUMBER,
                                  p_DivDeal IN NUMBER,
                                  p_DivCurr IN NUMBER,
                                  p_RateGet IN NUMBER,
                                  p_D1 IN NUMBER,
                                  p_D2 IN NUMBER,
                                  p_TaxCurr IN NUMBER,
                                  p_Tax OUT NUMBER,
                                  p_IncrTax OUT NUMBER,
                                  p_ParmStr OUT VARCHAR2
                                 );

 /**
 * Поиск таблицы для онлайн импорта сделок ASTS
 * @since 6.20.031.67
 * @qtest NO
 * @param p_MarketKind Вид биржевого рынка
 * @return полное название таблицы для импорта ASTS (вместе со схемой, если есть) 
 */
  FUNCTION GetFullNameASTSTable(p_MarketKind IN NUMBER) RETURN VARCHAR2 DETERMINISTIC;

/**
  * Получить код объекта хеджирования
  * @qtest  NO
  * @since  6.20.031.72.0
  * @param  p_ObjKind         Вид объекта хеджирования
  * @param  p_ObjID           Идентификатор объекта хеджирования
  * @return Код объекта
  */
  FUNCTION SC_GetAvrHdCode( p_ObjKind IN NUMBER,
                            p_ObjID   IN NUMBER
                          ) RETURN VARCHAR2;

/**
 * Функция получения кода на заданную дату (по ddlobjcode_dbt)
 * @since 6.20.031.75
 * @qtest NO
 * @param p_ObjectType Тип объекта
 * @param p_CodeKind Вид кода
 * @param p_ObjectID Идентификатор объекта
 * @param p_Date Дата
 * @return Код на дату
 */
FUNCTION SC_GetDlObjCodeOnDate( p_ObjectType IN NUMBER,
                                p_CodeKind   IN NUMBER,
                                p_ObjectID   IN NUMBER,
                                p_Date       IN DATE DEFAULT NULL
                              ) RETURN VARCHAR2;
 
/**
 * Получение всех DocId из GrDeal по параметрам
 * @qtest  NO
 * @since  6.20.031.78.0
 * @param  p_PlanDate         Дата строки графика
 * @param  p_FIID             Идентификатор фин инструмента
 * @param  p_docKindsInClause Строка содержащая ид документов через запятую для конструкции IN ()
 * @return GRDEALID_T массив идентификаторов t_docid
 */
  FUNCTION GetAllGrDealIdsByParam(p_PlanDate IN DATE, p_FIID IN NUMBER DEFAULT NULL, p_docKindsInClause IN VARCHAR2 DEFAULT NULL)
        RETURN GRDEALID_T DETERMINISTIC;


/**
 * Проверка по графику необработанных записей по депо учёту
 * @qtest  NO
 * @since  6.20.031.78.0
 * @param  p_BeginDate        Начальная дата проверки
 * @param  p_EndDate          Конечная дата проверки
 * @param  p_dlComId          Идентификатор сервисной операции
 */
  PROCEDURE CheckWorkedDepoLastDate (p_BeginDate         IN DATE,
                                     p_EndDate           IN DATE,
                                     p_dlComId           IN NUMBER,
                                     p_tableName         IN VARCHAR2);

/**
 * Проверка по графику необработанных записей по депо учёту по годам
 * @qtest  NO
 * @since  6.20.031.78.0
 * @param  p_BeginDate        Начальный год проверки
 * @param  p_EndDate          Конечный год проверки
 * @param  p_dlComId          Идентификатор сервисной операции
 */
  PROCEDURE CheckWorkedDepoLastDateByYear (p_BeginYear         IN NUMBER,
                                           p_EndYear           IN NUMBER,
                                           p_dlComId           IN NUMBER,
                                           p_tableName         IN VARCHAR2);

/**
 * Получение платежей для хеджирование ДП
 * @qtest  NO
 * @since  6.20.031.78.0
 * @param  pRealtionID  Идентификатор ОХ
 * @param  pFIID        Идентификатор ц/б
 * @param  DocKind      Вид документа
 * @param  ObjType      Вид объекта
 * @param  pDate0       Дата от
 * @param  pDate1       Дата до
 */
  PROCEDURE GetPaymentsForHdgDP (  pRealtionID IN NUMBER,
                                   pFIID       IN NUMBER,
                                   DocKind     IN NUMBER,
                                   ObjType     IN NUMBER,
                                   pDate0      IN DATE,
                                   pDate1      IN DATE);

/**
 * Установить значение категории "Тест на рыночность пройден" для сделки
 * @since 6.20.031.48
 * @qtest NO
 * @param p_DealID Идентификатор сделки
 * @param p_OnDate Дата установки
 * @param p_AttrID Идентификатор значения
 */
  PROCEDURE SetDealInTaxAttrID(p_DealID IN NUMBER, p_OnDate IN DATE, p_AttrID IN NUMBER);

/**
 * Пакетное установка признаков зачисления НДФЛ
 * @since 6.20.031.48
 * @qtest NO
 */
  PROCEDURE Mass_ExecDealInTaxAttr;

/**
  * Установить значение категории для сделки
  * Если данная процедура запускается из интеграции (то есть не из окружения RS), 
  *  то желательно инициализировать RsbSessionData.SetOper
  * Параметры: 
  * @param p_DealID - код сделки - то есть ddl_tick_dbt.t_DealID
  * @param p_OnDate - дата начала действия категории
  * @param p_AttrID - код установливаемого атрибута (признака). 
  *   Например "Да" - 1, "Нет" - 2. Проверять по таблице DOBJATTR_DBT 
  * @paramp_GroupId - идентификатор категории
  */
  PROCEDURE SetDealAttrID(p_DealID IN NUMBER, p_OnDate IN DATE, 
    p_AttrID IN NUMBER, p_GroupId IN NUMBER );

/**
  * Получить строку договоров обслуживания, которые привязаны к операции отправки ОБ
  * Параметры: 
  * @param p_scsrvrepID - ID операции отправки ОБ
  * @param p_lineLimit - лимит возвращаемой строки по символам
  * @return строка договоров обслуживания, которые привязаны к операции отправки ОБ
  */
  FUNCTION GetSCSrvRepContrNumber( p_scsrvrepID IN NUMBER, p_lineLimit IN NUMBER DEFAULT 100 ) RETURN VARCHAR2;

/**
  * Получить строку договоров обслуживания, которые привязаны к операции отправки ОБ, для временной таблицы
  * Параметры: 
  * @param p_scsrvrepID - ID операции отправки ОБ
  * @param p_lineLimit - лимит возвращаемой строки по символам
  * @return строка договоров обслуживания, которые привязаны к операции отправки ОБ
  */
  FUNCTION GetSCSrvRepContrNumberTmp( p_scsrvrepID IN NUMBER, p_lineLimit IN NUMBER DEFAULT 100 ) RETURN VARCHAR2;

/**
  * Заполнить временную таблицу привязанных договоров обслуживания по данным из постоянной
  * Параметры: 
  * @param p_scsrvrepID - ID операции отправки ОБ
  */
  PROCEDURE FillSCSrvRepContrTmp( p_scsrvrepID IN NUMBER);

/**
  * Отредактировать постоянную таблицу привязанных договоров обслуживания исходя из правок временной
  * Параметры: 
  * @param p_scsrvrepID - ID операции отправки ОБ
  */
  PROCEDURE SyncSCSrvRepContrTmp( p_scsrvrepID IN NUMBER);

/**
  * Возвращает ID шага операции если он имеется в данной операции
  * Ищет шаг Получение средств от платежного агента 115. Не возможно искать через символ, т.к. у шага его нет
  * @param  DealID  Идентификатор сделки
  * @param  DocKind Вид документа
  */
  FUNCTION CheckNotExistStepReceivingFunds( DealID IN NUMBER,
                           DocKind IN NUMBER)
    RETURN NUMBER;

/**
  * Отправка сообщений об изменении информации по договору в Diasoft при создании\изменении записи в реестре квал инвесторов
  * Параметры: 
  * @param p_DlContrID - ID ДБО
  */
  PROCEDURE SendBrokerContractDepoMessAtChngQInv(p_DlContrID IN NUMBER);

/**
   Проверить, что субъект-эмитент зарегистрирован в государстве - члене ЕАЭС на дату
  */
  FUNCTION IsIssuerEAEU(p_PartyID IN NUMBER, p_OnDate IN DATE) RETURN NUMBER;

/**
   Получить ставку РЕПО на дату из истории
  */
  FUNCTION GetIncRateOnDate(p_DealId IN NUMBER, p_DocKind IN NUMBER, p_OnDate DATE) RETURN NUMBER;

END Rsb_Secur;
/
