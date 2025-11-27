CREATE OR REPLACE PACKAGE RSB_SPREPFUN IS

  g_SessionID NUMBER := NULL;
  g_RepKind   NUMBER := NULL;

  UnknownParty CONSTANT NUMBER := -1;

  FIROLE_UNDEF                  CONSTANT INTEGER := 0;   --не задан
  FIROLE_FIREQ                  CONSTANT INTEGER := 1;   --ФИ требования по сделке
  FIROLE_FICOM                  CONSTANT INTEGER := 2;   --ФИ обязательства по сделке
  FIROLE_BA                     CONSTANT INTEGER := 3;   --Базовый актив сделки
  FIROLE_CA                     CONSTANT INTEGER := 4;   --Контрактив сделки
  FIROLE_FIDOC                  CONSTANT INTEGER := 5;   --ФИ документа
  FIROLE_INCOME                 CONSTANT INTEGER := 6;   --ФИ дохода от переоценки
  FIROLE_EXPEND                 CONSTANT INTEGER := 7;   --ФИ расхода от переоценки
  FIROLE_BA_RESALE              CONSTANT INTEGER := 8;   --Базовый актив для перепродажи
  FIROLE_BA_INVEST              CONSTANT INTEGER := 9;   --Базовый актив для инвестирования
  FIROLE_SRCA                   CONSTANT INTEGER := 10;  --Исходный актив
  FIROLE_SRCA2                  CONSTANT INTEGER := 11;  --Второй исходный актив
  FIROLE_DSTA                   CONSTANT INTEGER := 12;  --Целевой актив
  FIROLE_DSTA2                  CONSTANT INTEGER := 13;  --Второй целевой актив
  FIROLE_BAININVEST             CONSTANT INTEGER := 14;  --Базовый актив в ИП
  FIROLE_BAINCONTR              CONSTANT INTEGER := 15;  --Базовый актив в ПКУ
  FIROLE_BAINPROMISSORYSALE     CONSTANT INTEGER := 16;  --Базовый актив для перепродажи в ПДО
  FIROLE_BAINPROMISSORYINVEST   CONSTANT INTEGER := 17;  --Базовый актив для инвестирования в ПДО
  FIROLE_BAINPROMISSORY         CONSTANT INTEGER := 18;  --Базовый актив в ПДО
  FIROLE_BA_REPO                CONSTANT INTEGER := 19;  --Базовый актив в договорах обратной продажи
  FIROLE_BA_LOAN                CONSTANT INTEGER := 20;  --Базовый актив в договорах займа
  FIROLE_BUYACTIVE              CONSTANT INTEGER := 21;  --Приобретаемый актив
  FIROLE_SALEACTIVE             CONSTANT INTEGER := 22;  --Реализуемый актив
  FIROLE_CALC_BA                CONSTANT INTEGER := 23;  --ФИ учета базового актива
  FIROLE_OVERVALUE_REQ          CONSTANT INTEGER := 24;  --ФИ переоценки требований
  FIROLE_OVERVALUE_COM          CONSTANT INTEGER := 25;  --ФИ переоценки обязательств
  FIROLE_OVERVALUE_DIFF         CONSTANT INTEGER := 26;  --ФИ переоценки доплаты в мене
  FIROLE_FIBARTERDIFF           CONSTANT INTEGER := 27;  --ФИ доплаты в мене
  FIROLE_CALC_ANOTHER_OPER      CONSTANT INTEGER := 28;  --Расчеты по прочим операциям
  FIROLE_CALC_AVOIRISS_OPER     CONSTANT INTEGER := 29;  --Расчеты по операциям с ЦБ
  FIROLE_FIREQ_BACK             CONSTANT INTEGER := 30;  --ФИ требований обратной части сделки
  FIROLE_FICOM_BACK             CONSTANT INTEGER := 31;  --ФИ обязательств обратной части сделки
  FIROLE_ACTIVE_OUTBAL          CONSTANT INTEGER := 32;  --Актив на внебалансе
  FIROLE_SETTL_CONTR            CONSTANT INTEGER := 33;  --Расчеты с контрагентом
  FIROLE_DEALS_TERMREQ          CONSTANT INTEGER := 34;  --Требования по срочным сделкам (для резерва по сделкам в УВ)
  FIROLE_DEALS_OVERDUE          CONSTANT INTEGER := 35;  --Просроченные требования по сделкам (для резерва по сделкам в УВ)
  FIROLE_BA_BACK                CONSTANT INTEGER := 36;  --Базовый актив обр.
  FIROLE_BA_RESERV_REQ          CONSTANT INTEGER := 37;  --Резерв по просроченным требованиям
  FIROLE_FUTURES                CONSTANT INTEGER := 38;  --Фьючерс
  FIROLE_OPTION                 CONSTANT INTEGER := 39;  --Опцион
  FIROLE_OLDREQ                 CONSTANT INTEGER := 40;  --ФИ старых требований
  FIROLE_OLDCOM                 CONSTANT INTEGER := 41;  --ФИ старых обязательств
  FIROLE_PERCENT                CONSTANT INTEGER := 42;  --ФИ учета процентов
  FIROLE_DISCOUNT               CONSTANT INTEGER := 43;  --ФИ учета дисконта
  FIROLE_OVERVALUE_SRCA         CONSTANT INTEGER := 44;  --Переоценка исходного актива/требования
  FIROLE_CORACC_ACTIVE          CONSTANT INTEGER := 45;  --ФИ активного корреспондирующего счета
  FIROLE_CORACC_PASSIVE         CONSTANT INTEGER := 46;  --ФИ пассивного корреспондирующего счета
  FIROLE_BANKROLL               CONSTANT INTEGER := 50;  --Денежные средства
  FIROLE_SECURITIES             CONSTANT INTEGER := 51;  --Ценные бумаги
  FIROLE_PRECIOUS_METALS        CONSTANT INTEGER := 52;  --Драг. металлы
  FIROLE_INCOME_DEBET           CONSTANT INTEGER := 53;  --Положительные курсовые разницы дебета
  FIROLE_INCOME_CREDIT          CONSTANT INTEGER := 54;  --Положительные курсовые разницы кредита
  FIROLE_EXPEND_DEBET           CONSTANT INTEGER := 55;  --Отрицательные курсовые разницы дебета
  FIROLE_EXPEND_CREDIT          CONSTANT INTEGER := 56;  --Отрицательные курсовые разницы кредита
  FIROLE_EXCHFI                 CONSTANT INTEGER := 57;  --Валюта торгов
  FIROLE_COMISSION              CONSTANT INTEGER := 60;  --Комиссия
  FIROLE_NKD                    CONSTANT INTEGER := 61;  --ДУ
  FIROLE_ORCB                   CONSTANT INTEGER := 62;  --Расчеты на ОРЦБ
  FIROLE_TRUSTOR                CONSTANT INTEGER := 63;  --Расчеты с учредителем
  FIROLE_PROPERTY               CONSTANT INTEGER := 64;  --Расчеты; связанные с реализацией имущества
  FIROLE_BA_CONTR               CONSTANT INTEGER := 65;  --Базовый актив контрагента
  FIROLE_CA_CONTR               CONSTANT INTEGER := 66;  --Контрактив контрагента
  FIROLE_AVANCE                 CONSTANT INTEGER := 67;  --Аванс/Задаток
  FIROLE_AVANCE_CONTR           CONSTANT INTEGER := 68;  --Аванс/Задаток контрагента
  FIROLE_COMISS_CLIENT          CONSTANT INTEGER := 69;  --Комиссия клиента
  FIROLE_COMISS_CONTR           CONSTANT INTEGER := 70;  --Комиссия контрагента
  FIROLE_COMISS_BROKER          CONSTANT INTEGER := 71;  --Комиссия посреднику
  FIROLE_REGTAX                 CONSTANT INTEGER := 72;  --Регистрационный сбор
  FIROLE_REGTAX_CONTR           CONSTANT INTEGER := 73;  --Регистрационный сбор контрагента
  FIROLE_PERC                   CONSTANT INTEGER := 74;  --Проценты
  FIROLE_PERCENT_CONTR          CONSTANT INTEGER := 75;  --Проценты контрагента
  FIROLE_NKD_CONTR              CONSTANT INTEGER := 76;  --НКД контрагента
  FIROLE_REGTAX2                CONSTANT INTEGER := 77;  --Регистрационный сбор возврат
  FIROLE_REGTAX2_CONTR          CONSTANT INTEGER := 78;  --Регистрационный сбор контрагента возврат
  FIROLE_BA_BACK_CONTR          CONSTANT INTEGER := 79;  --Базовый актив обр. контрагента
  FIROLE_BA_TP                  CONSTANT INTEGER := 80;  --Базовый актив в ТП
  FIROLE_BA_PPR                 CONSTANT INTEGER := 81;  --Базовый актив в ППР
  FIROLE_PD_TP                  CONSTANT INTEGER := 82;  --ПД в ТП (ПД CONSTANT INTEGER := процентный доход; ДД CONSTANT INTEGER := дисконтный)
  FIROLE_DD_TP                  CONSTANT INTEGER := 83;  --ДД в ТП
  FIROLE_PD_PPR                 CONSTANT INTEGER := 84;  --ПД в ППР
  FIROLE_DD_PPR                 CONSTANT INTEGER := 85;  --ДД в ППР
  FIROLE_PD_PUDP                CONSTANT INTEGER := 88;  --ПД в ПУДП
  FIROLE_DD_PUDP                CONSTANT INTEGER := 89;  --ДД в ПУДП
  FIROLE_BA_PUDP                CONSTANT INTEGER := 90;  --Базовый актив в ПУДП
  FIROLE_SRCA3                  CONSTANT INTEGER := 91;  --3 исходный актив
  FIROLE_DSTA3                  CONSTANT INTEGER := 92;  --3 целевой актив
  FIROLE_EMIT_INC_TR            CONSTANT INTEGER := 93;  --Требования к эмитенту по доходам; полученным БПП
  FIROLE_EMIT_INC_OB            CONSTANT INTEGER := 94;  --Обязательства эмитента по доходам; полученным БПП
  FIROLE_PD_TP_BPP              CONSTANT INTEGER := 95;  --ПД в ТП БПП (ПД CONSTANT INTEGER := процентный доход; ДД CONSTANT INTEGER := дисконтный)
  FIROLE_DD_TP_BPP              CONSTANT INTEGER := 96;  --ДД в ТП БПП
  FIROLE_PD_PPR_BPP             CONSTANT INTEGER := 97;  --ПД в ППР БПП
  FIROLE_DD_PPR_BPP             CONSTANT INTEGER := 98;  --ДД в ППР БПП
  FIROLE_BA_TP_OVERDUE          CONSTANT INTEGER := 99;  --Базовый просроченный актив в ТП
  FIROLE_BA_PPR_OVERDUE         CONSTANT INTEGER := 100; --Базовый просроченный актив в ППР
  FIROLE_BA_OVERDUE             CONSTANT INTEGER := 101; --Базовый просроченный актив
  FIROLE_RESERV_RVCB            CONSTANT INTEGER := 102; --РВЦБ    Резерв по вложениям в ц/б
  FIROLE_RESERV_RVPCB           CONSTANT INTEGER := 103; --РВПЦБ   Резерв по начисленному ПДД по ц/б
  FIROLE_RESERV_RSS             CONSTANT INTEGER := 104; --РСС     Резерв по срочным сделкам
  FIROLE_RESERV_ROR             CONSTANT INTEGER := 105; --РОР     Резерв по требованиям по 2 части сделок ОР
  FIROLE_RESERV_RUOKXOR         CONSTANT INTEGER := 106; --РУОКХОР Резерв по условным обязятельствам кред. характер сделок ОР
  FIROLE_RESERV_RSOP            CONSTANT INTEGER := 107; --РСОП    Резерв по сделкам с отсрочкой платежа
  FIROLE_RESERV_RPT             CONSTANT INTEGER := 108; --РПТ     Резерв по просроченным требованиям
  FIROLE_RESERV_RPOR            CONSTANT INTEGER := 109; --РПОР    Резерв по требованиям по получениям процентных доходов по сделкам ОР
  FIROLE_RESERV_RPTP            CONSTANT INTEGER := 110; --РПТП    Резерв по просроченным процентным требованиям по сделкам ОР
  FIROLE_PD_PUDP_BPP            CONSTANT INTEGER := 111; --ПД в ПУДП БПП
  FIROLE_DD_PUDP_BPP            CONSTANT INTEGER := 112; --ДД в ПУДП БПП
  FIROLE_RVPS                   CONSTANT INTEGER := 113; --РВПС
  FIROLE_RVP                    CONSTANT INTEGER := 114; --РВП
  FIROLE_BA_PPR_BPP             CONSTANT INTEGER := 115; --Базовый актив в ППР БПП
  FIROLE_PD_PDO                 CONSTANT INTEGER := 116; --ПД в ПДО
  FIROLE_PD_PKU                 CONSTANT INTEGER := 117; --ПД в ПКУ
  FIROLE_BANKROLL_BANK          CONSTANT INTEGER := 118; --Средства в банке
  FIROLE_SETTL_AGENT            CONSTANT INTEGER := 119; --Расчеты с посредником
  FIROLE_ORCB_OBLIGATION        CONSTANT INTEGER := 120; --Расчеты на ОРЦБ обязательства
  FIROLE_ORCB_REQUEST           CONSTANT INTEGER := 121; --Расчеты на ОРЦБ требования
  FIROLE_GUARANTEE              CONSTANT INTEGER := 130; --Учет гарантийного обеспечения
  FIROLE_COMPENSPAY             CONSTANT INTEGER := 131; --Компенсационный платеж
  FIROLE_CALC_COUPON            CONSTANT INTEGER := 132; --Учет купона
  FIROLE_CALC_RETIRE            CONSTANT INTEGER := 133; --Учет частичного погашения
  FIROLE_SHARE                  CONSTANT INTEGER := 134; --Акции
  FIROLE_BOND                   CONSTANT INTEGER := 135; --Облигации
  FIROLE_COM_OUT                CONSTANT INTEGER := 136; --Начисление комиссии за доср. вывод
  FIROLE_COM_MANAG              CONSTANT INTEGER := 137; --Начисление комиссии за управление
  FIROLE_COM_SUCCESS            CONSTANT INTEGER := 138; --Начисление комиссии за успех
  FIROLE_NALOG                  CONSTANT INTEGER := 139; --Обязательства по уплате налога
  FIROLE_BONUS_TP               CONSTANT INTEGER := 140; --Премия в ТП
  FIROLE_BONUS_PPR              CONSTANT INTEGER := 141; --Премия в ППР
  FIROLE_BONUS_TP_BPP           CONSTANT INTEGER := 142; --Премия в ТП БПП
  FIROLE_BONUS_PPR_BPP          CONSTANT INTEGER := 143; --Премия в ППР БПП
  FIROLE_BONUS_PUDP             CONSTANT INTEGER := 144; --Премия в ПУДП
  FIROLE_BONUS_PUDP_BPP         CONSTANT INTEGER := 145; --Премия в ПУДП БПП
  FIROLE_ODAV                   CONSTANT INTEGER := 146; --ОД после восстребования
  FIROLE_BANKROLL_FUTURES       CONSTANT INTEGER := 147; --Денежные средства фьючерс
  FIROLE_BANKROLL_OPTION        CONSTANT INTEGER := 148; --Денежные средства опцион
  FIROLE_BONUS                  CONSTANT INTEGER := 149; --Премия
  FIROLE_PRICEFI                CONSTANT INTEGER := 150; --ВЦ документа
  FIROLE_BA_NOTDV               CONSTANT INTEGER := 151; --БА не ПИ
  FIROLE_AVOIR_UC               CONSTANT INTEGER := 152; --Ц/б; учитываемые в ненадежном депозитарии
  FIROLE_OVERVALUE_SRCA_COM     CONSTANT INTEGER := 153; --Переоценка исходного актива/обязательства
  FIROLE_MONEYSOURCE_CLIENT     CONSTANT INTEGER := 154; --Источник средств - клиентские
  FIROLE_MONEYSOURCE_OWN        CONSTANT INTEGER := 155; --Источник средств - собственные
  FIROLE_RESERV_SALEREPO2_TP    CONSTANT INTEGER := 156; --Резерв; ц/б по роли РПР_ТП
  FIROLE_ORCB_OTHCLIRACC        CONSTANT INTEGER := 157; --Другой клиринговый счет в операции Расчеты на бирже (собственные ср-ва)
  FIROLE_CORACC_ACTIVE_BACK     CONSTANT INTEGER := 158; --ФИ  ктивного корреспондирующего счет  по 2 ч
  FIROLE_CORACC_PASSIVE_BACK    CONSTANT INTEGER := 159; --ФИ пассивного корреспондирующего счета по 2 ч
  FIROLE_BA_PPR_TSS_YES         CONSTANT INTEGER := 160; --базовый актив в ППР; ТСС
  FIROLE_BA_PPR_TSS_NO          CONSTANT INTEGER := 161; --базовый актив в ППР; без ТСС
  FIROLE_COMPENSDELIVERY        CONSTANT INTEGER := 162; --Компенсационная поставка
  FIROLE_COMISS_BROKER_CONTR    CONSTANT INTEGER := 163; --Комиссия посреднику от контрагента
  FIROLE_COMPENSDELIVERYCONTR   CONSTANT INTEGER := 164; --Компенсационная поставка клиенту-контрагенту
  FIROLE_RESERV_SALEREPO2_PPR   CONSTANT INTEGER := 165; --Резерв; ц/б по роли РПР_ППР
  FIROLE_RESERV_SALEREPO2_PUDP  CONSTANT INTEGER := 166; --Резерв; ц/б по роли РПР_ПУДП
  FIROLE_ORCB_OTHCLIRACC_CLIENT CONSTANT INTEGER := 167; --Другой клиринговый счет в операции Расчеты на бирже (клиентские ср-ва)
  FIROLE_ORCB_OTHCLIRACC_TRUST  CONSTANT INTEGER := 168; --Другой клиринговый счет в операции Расчеты на бирже (ДУ)
  FIROLE_MONEYSOURCE_TRUST      CONSTANT INTEGER := 169; --Источник средств - ДУ
  FIROLE_RESERV_SALEREPO2_PVO   CONSTANT INTEGER := 170; --Резерв; ц/б по роли РПР_ПВО
  FIROLE_BONUS_ACC              CONSTANT INTEGER := 171; --Начисленная премия
  FIROLE_BONUS_TP_PAID          CONSTANT INTEGER := 172; --Уплаченная премия в ТП
  FIROLE_BONUS_PPR_PAID         CONSTANT INTEGER := 173; --Уплаченная премия в ППР
  FIROLE_BONUS_PUDP_PAID        CONSTANT INTEGER := 174; --Уплаченная премия в ПУДП
  FIROLE_BONUS_TP_BPP_PAID      CONSTANT INTEGER := 175; --Уплаченная премия в ТП БПП
  FIROLE_BONUS_PPR_BPP_PAID     CONSTANT INTEGER := 176; --Уплаченная премия в ППР БПП
  FIROLE_BONUS_PUDP_BPP_PAID    CONSTANT INTEGER := 177; --Уплаченная премия в ПУДП БПП
  FIROLE_RESERV_SALEREPO2_CONTR CONSTANT INTEGER := 178; --Резерв; ц/б по роли РПР_ПКУ
  FIROLE_BA_CONTR_OVERDUE       CONSTANT INTEGER := 179; --Базовый просроченный актив в ПКУ
  FIROLE_BA_PUDP_OVERDUE        CONSTANT INTEGER := 180; --Базовый просроченный актив в ПУДП
  FIROLE_KSU                    CONSTANT INTEGER := 181; --КСУ
  FIROLE_FIRST_OVERVAL          CONSTANT INTEGER := 182; --Переоценка по первой части сделки
  FIROLE_SECOND_OVERVAL         CONSTANT INTEGER := 183; --Переоценка по второй части сделки
  FIROLE_AJUSTVALOEB            CONSTANT INTEGER := 184; --Корректировка стоимости ОЭБ
  FIROLE_CUR_CONTRAGENT         CONSTANT INTEGER := 185; --Текущий субъект для подстановки нужного субъекта при откритии счета
  FIROLE_OTHPLACEDMONEY         CONSTANT INTEGER := 186; --Прочие размещенные средства (РОРЕПО)
  FIROLE_RORPT                  CONSTANT INTEGER := 187; --Просроченная задолженность (РОРПТ)
  FIROLE_RORPTP                 CONSTANT INTEGER := 188; --Просроченные проценты (РОРПТП)
  FIROLE_RORPOR                 CONSTANT INTEGER := 189; --Расчеты по отдельным операциям и корректировки (РОРПОР)
  FIROLE_ROZ                    CONSTANT INTEGER := 190; --Расчеты по отдельным операциям и корректировки (РОЗ)
  FIROLE_ROKOR                  CONSTANT INTEGER := 191; --Расчеты по отдельным операциям и корректировки (РОКОР)
  FIROLE_WRITTENBONUS           CONSTANT INTEGER := 192; --Списанная премия
  FIROLE_DD_PDO                 CONSTANT INTEGER := 193; --ДД в ПДО
  FIROLE_ADDINCOME              CONSTANT INTEGER := 194; --Дополнительный доход;
  FIROLE_PKU                    CONSTANT INTEGER := 195; --ПКУ                  
  FIROLE_BA_SSSD_BPP            CONSTANT INTEGER := FIROLE_BA_PPR_BPP; --Базовый актив в СССД БПП
  FIROLE_BA_SSSD_BPP_OVERDUE    CONSTANT INTEGER := 196; --Просроченный базовый актив в СССД БПП
  FIROLE_BA_SSPU_BPP            CONSTANT INTEGER := 197; --Базовый актив ССПУ БПП
  FIROLE_BA_SSPU_BPP_OVERDUE    CONSTANT INTEGER := 198; --Просроченный базовый актив ССПУ БПП
  FIROLE_BA_ASCB_BPP            CONSTANT INTEGER := 199; --Базовый актив АС_ЦБ БПП
  FIROLE_BA_ASCB_BPP_OVERDUE    CONSTANT INTEGER := 200; --Просроченный базовый актив АС_ЦБ БПП
  FIROLE_BA_CONTR_BPP           CONSTANT INTEGER := 201; --Базовый актив ПКУ БПП             
  FIROLE_BA_CONTR_BPP_OVERDUE   CONSTANT INTEGER := 202; --Просроченный базовый актив ПКУ БПП         
  FIROLE_NATCOM                 CONSTANT INTEGER := 203; --Рублевые комиссии
  FIROLE_FRGCOM                 CONSTANT INTEGER := 204; --Валютные комиссии


  RATETYPE_TRADEVOLUME          CONSTANT NUMBER := 17;   --Вид курса "Объём торгов"
  RATETYPE_TRADEVOLUME_SPB      CONSTANT NUMBER := 35;   --Вид курса "(СПБ) Объём торгов"


  FUNCTION BPP_ACCOUNT_METHOD RETURN NUMBER DETERMINISTIC;
  FUNCTION PAIR_OVER_ACC RETURN NUMBER DETERMINISTIC;

  PROCEDURE AddRepError(pMessage IN VARCHAR2);

  FUNCTION GetCurrencyConvertErrorMsg(p_fiidFrom IN NUMBER, p_fiidTo IN NUMBER, p_ADate IN DATE) RETURN VARCHAR2;

  --Аналог GetRateOnDate в макросах
  FUNCTION GetRateOnDate(p_ADate IN DATE, 
                         p_FromFI IN NUMBER, 
                         p_ToFI IN NUMBER, 
                         p_NeedSayError IN NUMBER, 
                         p_RateType IN NUMBER, 
                         p_Error OUT NUMBER, 
                         p_SinceDate OUT DATE, 
                         p_MarketID IN NUMBER DEFAULT -1, 
                         p_Section IN NUMBER DEFAULT 0) RETURN NUMBER;

  --Аналог SmartConvertSum из макроса
  FUNCTION SmartConvertSum(p_sumTo OUT NUMBER, 
                           p_sumFrom IN NUMBER, 
                           p_sinceDate IN DATE, 
                           p_fiidFrom IN NUMBER,
                           p_fiidTo IN NUMBER, 
                           p_SayError IN NUMBER DEFAULT 0) RETURN NUMBER;

  FUNCTION SmartConvertSum_Ex(p_sumFrom IN NUMBER, 
                              p_sinceDate IN DATE, 
                              p_fiidFrom IN NUMBER,
                              p_fiidTo IN NUMBER, 
                              p_SayError IN NUMBER DEFAULT 0) RETURN NUMBER;


  --Аналог SmartConvertSumDbl из макроса
  FUNCTION SmartConvertSumDbl(p_SumToDbl OUT NUMBER, 
                              p_SumFromDbl IN NUMBER, 
                              p_ADate IN DATE, 
                              p_FromFI IN NUMBER, 
                              p_ToFI IN NUMBER,
                              p_NeedSayError IN NUMBER, 
                              p_RateType IN NUMBER, 
                              p_SinceDate IN OUT DATE, 
                              p_MarketID IN NUMBER, 
                              p_Section IN NUMBER, 
                              p_ErrorMes OUT VARCHAR2) RETURN NUMBER;
  
  FUNCTION SmartConvertSumDbl_Ex(  p_SumToDbl OUT NUMBER, 
                                   p_SumFromDbl IN NUMBER, 
                                   p_ADate IN DATE, 
                                   p_FromFI IN NUMBER, 
                                   p_ToFI IN NUMBER,
                                   p_NeedSayError IN NUMBER) RETURN NUMBER;

  FUNCTION SmartConvertSumDbl_Ex2( p_SumFromDbl IN NUMBER, 
                                   p_ADate IN DATE, 
                                   p_FromFI IN NUMBER, 
                                   p_ToFI IN NUMBER,
                                   p_NeedSayError IN NUMBER) RETURN NUMBER;
                                                                    
  --Аналог GetRateOnDateCrossDbl
  FUNCTION GetRateOnDateCrossDbl(p_ADate IN DATE, 
                                 p_FromFI IN NUMBER, 
                                 p_ToFI IN NUMBER,
                                 p_NeedSayError IN NUMBER, 
                                 p_RateType IN NUMBER DEFAULT 0, 
                                 p_Error OUT NUMBER, 
                                 p_SinceDate OUT DATE,
                                 p_MarketID IN NUMBER DEFAULT -1,
                                 p_Section IN NUMBER DEFAULT 0) RETURN NUMBER;

  FUNCTION GetRateOnDateCrossDbl_Ex(p_ADate IN DATE, 
                                    p_FromFI IN NUMBER, 
                                    p_ToFI IN NUMBER,
                                    p_NeedSayError IN NUMBER, 
                                    p_RateType IN NUMBER DEFAULT 0) RETURN NUMBER;

  FUNCTION GetISO_Number(p_FIID IN NUMBER) RETURN VARCHAR2 DETERMINISTIC;


  --Аналог ПолучитьКурсПостановкиНаБаланс из макроса
  FUNCTION GetBalanceRate(p_BOfficeKind IN NUMBER, p_DealID IN NUMBER, p_RQID IN NUMBER DEFAULT 0) RETURN NUMBER;


  --Аналог SP_GetCouponSumByPeriod из макроса
  FUNCTION SP_GetCouponSumByPeriod(p_FIID IN NUMBER, 
                                   p_Amount IN NUMBER, 
                                   p_DateBeg IN DATE, 
                                   p_DateEnd IN DATE, 
                                   p_IsPartial IN NUMBER, 
                                   p_ToFIID IN NUMBER, 
                                   p_CouponCount OUT NUMBER, 
                                   p_ExcludeDate IN DATE, 
                                   p_CalcByDates IN NUMBER, 
                                   p_IsClosed IN NUMBER DEFAULT 0,
                                   p_CoupRetData IN NUMBER DEFAULT 0 ) RETURN NUMBER;

  FUNCTION GetCouponSumByPeriod(p_FIID IN NUMBER, 
                                p_Amount IN NUMBER, 
                                p_DateBeg IN DATE, 
                                p_DateEnd IN DATE, 
                                p_CouponCount OUT NUMBER, 
                                p_ExcludeDate IN DATE, 
                                p_CalcByDates IN NUMBER, 
                                p_IsClosed IN NUMBER DEFAULT 0,
                                p_CoupRetData IN NUMBER DEFAULT 0) RETURN NUMBER;

  FUNCTION GetCouponSumByPeriod_Rub(p_FIID IN NUMBER, 
                                    p_Amount IN NUMBER, 
                                    p_DateBeg IN DATE, 
                                    p_DateEnd IN DATE, 
                                    p_CouponCount OUT NUMBER, 
                                    p_ExcludeDate IN DATE, 
                                    p_CalcByDates IN NUMBER, 
                                    p_IsClosed IN NUMBER DEFAULT 0,
                                    p_CoupRetData IN NUMBER DEFAULT 0) RETURN NUMBER;

  FUNCTION GetPartialSumByPeriod(p_FIID IN NUMBER, 
                                 p_Amount IN NUMBER, 
                                 p_DateBeg IN DATE, 
                                 p_DateEnd IN DATE, 
                                 p_CouponCount OUT NUMBER, 
                                 p_ExcludeDate IN DATE, 
                                 p_CalcByDates IN NUMBER, 
                                 p_IsClosed IN NUMBER DEFAULT 0) RETURN NUMBER;

  FUNCTION GetPartialSumByPeriod_Rub(p_FIID IN NUMBER, 
                                     p_Amount IN NUMBER, 
                                     p_DateBeg IN DATE, 
                                     p_DateEnd IN DATE, 
                                     p_CouponCount OUT NUMBER, 
                                     p_ExcludeDate IN DATE, 
                                     p_CalcByDates IN NUMBER, 
                                     p_IsClosed IN NUMBER DEFAULT 0) RETURN NUMBER;



  FUNCTION GetFIRoleByPortfolioBonus(p_Portfolio IN NUMBER, p_BPP IN NUMBER DEFAULT 0) RETURN NUMBER;

  FUNCTION GetFIRoleByPortfolio(p_Portfolio IN NUMBER, 
                                p_Discount  IN NUMBER DEFAULT 0, 
                                p_Percent   IN NUMBER DEFAULT 0, 
                                p_Bpp       IN NUMBER DEFAULT 0, 
                                p_IsOverdue IN NUMBER DEFAULT 0, 
                                p_Bonus     IN NUMBER DEFAULT 0
                               ) RETURN NUMBER;

  FUNCTION GetAccountID(p_DocKind   IN NUMBER,
                        p_DocID     IN NUMBER,
                        p_CatCode   IN VARCHAR2,
                        p_Date      IN DATE,
                        p_FIRole    IN NUMBER,
                        p_FIID      IN NUMBER,
                        p_Portfolio IN NUMBER,
                        p_IsBPP     IN NUMBER,
                        p_IncType   IN NUMBER DEFAULT 0,
                        p_ResType   IN NUMBER DEFAULT 0
                       ) RETURN NUMBER;


  FUNCTION GetLotCostAccountID(p_SumID IN NUMBER, p_Date IN DATE) RETURN NUMBER;
  
  FUNCTION GetRateIdByMPWithMaxTradeVolume(p_sinceDate IN DATE,
                                           p_fiidFrom  IN NUMBER,
                                           p_rateType  IN NUMBER DEFAULT 0,
                                           p_NDays     IN NUMBER DEFAULT -1,
                                           p_SayError  IN NUMBER DEFAULT 0
                                          ) RETURN NUMBER;
                                          
  FUNCTION GetCourse(p_RateId IN NUMBER, p_SinceDate IN DATE) RETURN NUMBER;
  
  FUNCTION GetCourseFI(p_RateId IN NUMBER) RETURN NUMBER;

END RSB_SPREPFUN;
/
