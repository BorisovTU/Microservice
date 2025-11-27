/**
 * Основные общие функции и константы платежей
 * Спецификация пакета
 */

CREATE OR REPLACE PACKAGE PM_COMMON
AS

  ------------------------------------------
  -- Стандартные константы для флажков: установлено/не установлено
  ------------------------------------------

  SET_CHAR   CONSTANT CHAR(1) := 'X';
  UNSET_CHAR CONSTANT CHAR(1) := CHR(0);


  PD_OR_MANUAL            CONSTANT NUMBER(5)  := 1; -- Ручной ввод (Введен пользователем вручную)
  PD_OR_AUTO              CONSTANT NUMBER(5)  := 2; -- Создан автоматически (Сформирован автоматически)
  PD_OR_SF                CONSTANT NUMBER(5)  := 3; -- Комиссия за обслуживание
  PD_OR_LOANS             CONSTANT NUMBER(5)  := 4; -- Подсистема "Кредитование"
  PD_OR_RETAIL            CONSTANT NUMBER(5)  := 5; -- Подсистема "Обслуживание физических лиц"
  PD_OR_PERCENT           CONSTANT NUMBER(5)  := 6; -- Подсистема "Проценты"
  PD_OR_INCOUNTING        CONSTANT NUMBER(5)  := 7;  -- Подсистема "Incounting" (Подсистема "Зарплата")
  PD_OR_DEPLEGPERS        CONSTANT NUMBER(5)  := 8;  -- Подсистема "Депозиты юридических лиц" (Депозиты юридических лиц)
  PD_OR_CREDDEP           CONSTANT NUMBER(5)  := 9;  -- Подсиcтема "Кредиты-Депозиты"
  PD_OR_REGCONTR          CONSTANT NUMBER(5)  := 10; -- Подсистема "Учет договоров"
  PD_OR_CF                CONSTANT NUMBER(5)  := 11; -- CognitiveForms
  PD_OR_FRB               CONSTANT NUMBER(5)  := 12; -- FineReaderBank
  PD_OR_HCSCHARGE         CONSTANT NUMBER(5)  := 13; -- Оплата по начислению ГИС ЖКХ
  PD_OR_CLB               CONSTANT NUMBER(5)  := 19; -- Клиент-Банк (Подсистема <Клиент-Банк>)
  PD_OR_CLSB              CONSTANT NUMBER(5)  := 20; -- Клиент-Сбербанк
  PD_OR_LOANS_5_50        CONSTANT NUMBER(5)  := 30; -- Подсистема "Кредитование 5.50"
  PD_OR_RETAIL_5_50       CONSTANT NUMBER(5)  := 31; -- Подсистема "Обслуживание физических лиц 5.50"
  PD_OR_MVODB_5_50        CONSTANT NUMBER(5)  := 32; -- Подсистема "МВОДБ 5.50"
  PD_OR_PAYCLAIM          CONSTANT NUMBER(5)  := 41; -- Оплата требования
  PD_OR_PROCUNKNOWNPM     CONSTANT NUMBER(5)  := 42; -- Обработка невыясненного платежа
  PD_OR_RETURNUNKNOWNPM   CONSTANT NUMBER(5)  := 43; -- Возврат невыясненного платежа
  PD_OR_REDIRECTUNKNOWNPM CONSTANT NUMBER(5)  := 44; -- Перенаправление невыясненного платежа
  PD_OR_TRANSITPAY        CONSTANT NUMBER(5)  := 45; -- Транзитный платеж
  PD_OR_NEWREQUISITPAY    CONSTANT NUMBER(5)  := 46; -- Платеж по новым реквизитам
  PD_OR_FNS               CONSTANT NUMBER(5)  := 47; -- Поручение на списание ФНС
  PD_OR_PAYEEBANK         CONSTANT NUMBER(5)  := 48; -- Выставлен банком получателя
  PD_OR_CLOSTRANS         CONSTANT NUMBER(5)  := 49; -- Перевод остатка с закрываемого счета
  PD_OR_RETSAVACCREM      CONSTANT NUMBER(5)  := 50; -- Возврат с накопительного счета с остатком
  PD_OR_MFR               CONSTANT NUMBER(5)  := 51; -- Переоформление межфилиального платежа
  PD_OR_SELCURCOLORD      CONSTANT NUMBER(5)  := 52; -- Продажа валюты по инкассовому поручению
  PD_OR_PARTAT            CONSTANT NUMBER(5)  := 53; -- Частичное исполнение платежа
  ------------------------------------------
  -- Константа для флажка doprtemp_tmp.t_Reject 
  -- Если выставлена, то отвергнутый документ закрывается
  ------------------------------------------
  FINISH_REJECT_CHAR   CONSTANT CHAR(1) := 'F';

  ------------------------------------------
  -- Константы для нулевых значений разных типов
  ------------------------------------------

  RSB_EMPTY_CHAR   CONSTANT CHAR(1)     := CHR(0);
  RSB_EMPTY_STRING CONSTANT VARCHAR2(1) := CHR(1);
  RSB_EMPTY_DATE   CONSTANT DATE        := to_date( '01010001', 'ddmmyyyy' );
  RSB_MAX_DATE     CONSTANT DATE        := to_date( '31129999', 'ddmmyyyy' );

  ------------------------------------------
  -- Типы объектов
  ------------------------------------------
  -- Субъект экономики
  OBJTYPE_PARTY        CONSTANT NUMBER(5)  :=    3;
  -- Лицевой счет
  OBJTYPE_ACCOUNT      CONSTANT NUMBER(5)  :=    4;
  -- Ценная бумага
  OBJTYPE_AVOIRISS     CONSTANT NUMBER(5)  :=   12;
  -- Узел ТС
  OBJTYPE_DEPARTMENT   CONSTANT NUMBER(5)  :=   80;
  -- Рублевый платежный документ
  OBJTYPE_PSPAYORD     CONSTANT NUMBER(5)  :=  500;
  -- Платеж
  OBJTYPE_PAYMENT      CONSTANT NUMBER(5)  :=  501;
  -- Сообщение МБР
  OBJTYPE_WLD_MES      CONSTANT NUMBER(5)  :=  502;
  -- Выписка МБР
  OBJTYPE_WLD_HEAD     CONSTANT NUMBER(5)  :=  503;
  -- Подтверждение МБР
  OBJTYPE_WLD_CONF     CONSTANT NUMBER(5)  :=  504;
  -- Запрос/Ответ МБР
  OBJTYPE_WLD_REQ      CONSTANT NUMBER(5)  :=  505;
  -- Информационное сообщение МБР
  OBJTYPE_WLD_INFO     CONSTANT NUMBER(5)  :=  506;
  -- Сеансы МБР
  OBJTYPE_WLD_SESS     CONSTANT NUMBER(5)  :=  513;
  -- МФР платеж
  OBJTYPE_MFRPAYMENT   CONSTANT NUMBER(5)  :=  523;
  -- Перевод в бюджет
  OBJTYPE_BDTRANSF     CONSTANT NUMBER(5)  :=  536;
  -- Платеж банка
  OBJTYPE_BANKPAYMENT  CONSTANT NUMBER(5)  :=  600;
  -- Требование банка
  OBJTYPE_BANKCLAIM    CONSTANT NUMBER(5)  :=  601;
  -- Валютный платеж банка
  OBJTYPE_BBANKCPORDER CONSTANT NUMBER(5)  :=  641;
  -- Валютный клиентский платеж
  OBJTYPE_PSCPORDER    CONSTANT NUMBER(5)  :=  643;
  -- объявление на взнос
  OBJTYPE_PSCASHORDERIN  CONSTANT NUMBER(5)  :=  632;
  -- чеки
  OBJTYPE_PSCASHORDEROUT CONSTANT NUMBER(5)  :=  633;
  -- мемориальный ордер
  OBJTYPE_MEMORIALORDER  CONSTANT NUMBER(5)  :=  70;
  -- мультивалютный ордер
  OBJTYPE_MULTYDOC  CONSTANT NUMBER(5)  :=  620;
  -- кассовый документ
  OBJTYPE_CASHDOCUMENT  CONSTANT NUMBER(5)  :=  630;
  -- Распоряжение на оплату
  --OBJTYPE_PMCLAIM      CONSTANT NUMBER(5)  :=  541;
  -- Виды ответных документов(по шифру операции)
  OBJTYPE_KIND_PAYMENT CONSTANT NUMBER(5)  := 2030;
  -- Статусы платежа
  OBJTYPE_PMPAYMSTATUS CONSTANT NUMBER(5)  := 2054;

  ------------------------------------------
  -- Виды документов
  ------------------------------------------
  -- Изменение статуса платежа
  DLDOC_PAYMENTSTAT      CONSTANT NUMBER(5) :=   9;
  -- Cтатус свойств платежа
  DLDOC_PAYMENTPROPSTAT  CONSTANT NUMBER(5) :=  14;
  -- Мультивалютный документ
  CB_MULTYDOC            CONSTANT NUMBER(5) :=  15;
  -- Платеж банка
  DLDOC_BANKPAYMENT      CONSTANT NUMBER(5) :=  16;
  -- Требование банка
  DLDOC_BANKCLAIM        CONSTANT NUMBER(5) :=  17;
  -- Валютный платеж банка
  BBANK_CPORDER          CONSTANT NUMBER(5) :=  27;
  -- Платеж
  DLDOC_PAYMENT          CONSTANT NUMBER(5) :=  29;
  -- Новые мемориальные ордера(и рубли и валюта)
  DLDOC_MEMORIALORDER    CONSTANT NUMBER(5) :=  70;
  -- Сводный мемориальный ордер
  DLDOC_SUMMARY_MEMORDER CONSTANT NUMBER(5) :=  74;
  -- Поручение на покупку/продажу/конверсию валюты
  PS_BUYCURORDER         CONSTANT NUMBER(5) := 200;
  -- Рублевый платежный документ клиента
  PS_PAYORDER            CONSTANT NUMBER(5) := 201;
  -- Валютный платежный документ клиента
  PS_CPORDER             CONSTANT NUMBER(5) := 202;
  -- Инкассовое поручение к валютному счету клиента (ИПВС)
  PS_INRQ                CONSTANT NUMBER(5) := 203;
  -- Клиентский платеж
  PMDOC_CLIENTPAYMENT    CONSTANT NUMBER(5) := 283;
  -- Кассовый документ
  PMDOC_CASHDOCUMENT     CONSTANT NUMBER(5) := 284;
  -- Мемориальный документ
  PMDOC_MEMORIALDOCUMENT CONSTANT NUMBER(5) := 285;
  -- Банковский ордер
  DLDOC_BANKORDER        CONSTANT NUMBER(5) := 286;
  -- Ордер по передаче ценностей
  DLDOC_VALTRORDER       CONSTANT NUMBER(5) := 288;
  -- основание для изменения состояния платежного документа
  PS_PAYORDERSTAT        CONSTANT NUMBER(5) := 290;
  -- основание для изменения состояния валютного платежного документа РКО
  PS_CPORDERSTAT         CONSTANT NUMBER(5) := 291;
  -- основание для изменения состояния валютного платежного документа ББ
  BBANK_CPORDERSTAT      CONSTANT NUMBER(5) :=  40;
  -- Начальный платеж АРМ
  RM_PMOUTDOC            CONSTANT NUMBER(5) := 310;
  -- Сводный платеж
  DLDOC_MULTYPM          CONSTANT NUMBER(5) := 311;
  -- Ответный платёж
  WL_INDOC               CONSTANT NUMBER(5) := 320;
  -- Документ выписки МБР
  WL_DOCHEAD             CONSTANT NUMBER(5) := 321;
  -- Ответный платёж, введённый вручную в АРМ
  WL_WIPM                CONSTANT NUMBER(5) := 322;
  -- Поручение банка
  DLDOC_BANKPAYORDER     CONSTANT NUMBER(5) := 330;
  -- Изменение лимита счета
  DLDOC_CHANGELIMIT      CONSTANT NUMBER(5) := 351;
  -- Использовать овердрафт для оплаты документа
  DLDOC_LIMITUSETRY      CONSTANT NUMBER(5) := 352;
  -- Использовать овердрафт для оплаты документа (интегрированный режим)
  DLDOC_LIMITUSETRY_INT  CONSTANT NUMBER(5) := 353;
  -- Создание записи использования лимита в реестре
  DLDOC_LIMITUSE         CONSTANT NUMBER(5) := 354;
  -- Использовать овердрафт для оплаты документа
  DLDOC_LIMITRESTORETRY  CONSTANT NUMBER(5) := 355;
  -- Восстановление лимита в реестре
  DLDOC_LIMITRESTORE     CONSTANT NUMBER(5) := 356;
  -- Отвержение использования лимита овердрафта
  DLDOC_REJLIMITUSE      CONSTANT NUMBER(5) := 358;
  -- Изменение статуса сообщения МБР
  WL_MESSTATUS           CONSTANT NUMBER(5) := 377;
  -- Начальный документ МБР
  WL_PMOUTDOC            CONSTANT NUMBER(5) := 383;
  -- Связь квитовок
  WL_KVITLNK             CONSTANT NUMBER(5) := 384;
  -- ID подтвеждения МБР с неопределенной корсхемой
  WL_CONFID              CONSTANT NUMBER(5) := 395;
  -- Ордер подкрепления в ББ
  CASH_BOF_ADDORDER      CONSTANT NUMBER(5) := 400;
  -- Объявление на взнос наличными в РКО
  CASH_PS_INCORDER       CONSTANT NUMBER(5) := 410;
  -- Чек в РКО
  CASH_PS_OUTORDER       CONSTANT NUMBER(5) := 420;
  -- Приходный ордер в ББ
  CASH_BOF_INCORDER      CONSTANT NUMBER(5) := 430;
  -- Расходный ордер в ББ
  CASH_BOF_OUTORDER      CONSTANT NUMBER(5) := 440;
  -- Приходно-расходный ордер в ББ
  DLDOC_INOUTORDER      CONSTANT NUMBER(5) := 445;
  -- Платежа из бэк-офиса
  DOC_BO_PAYMENT         CONSTANT NUMBER(5) := 450;
  -- Изменение реквизитов документа картотеки
  DLDOC_PMINHIST         CONSTANT NUMBER(5) := 88;

  -- Распоряжение на оплату
  DLDOC_PMCLAIM      CONSTANT NUMBER(5) := 460;
  -- Выставленное требование
  DLDOC_OUT_PMCLAIM  CONSTANT NUMBER(5) := 461;
  -- Выставленное инкассовое поручение
  DLDOC_OUT_PMCOL    CONSTANT NUMBER(5) := 462;
  -- Предъявленное требование
  DLDOC_IN_PMCLAIM   CONSTANT NUMBER(5) := 463;
  -- Предъявленное инкассовое поручение
  DLDOC_IN_PMCOL     CONSTANT NUMBER(5) := 464;

  ------------------------------------------
  -- Системные назначения платежей
  ------------------------------------------
  -- Основной по клиентской платежке
  PM_PURP_POPRIMARY   CONSTANT NUMBER(5) :=  7;
  -- платеж или требование банка
  PM_PURP_BANKPAYMENT CONSTANT NUMBER(5) := 15;
  -- Сводный платеж
  PM_PURP_MULTI       CONSTANT NUMBER(5) := 32;
  -- Рублевый банковский ордер
  PM_PURP_BANKORDER   CONSTANT NUMBER(5) := 68;
  -- Валютный банковский ордер
  PM_PURP_CBANKORDER  CONSTANT NUMBER(5) := 69;
  -- Поручение банка
  PM_PURP_BANKPAYORDER  CONSTANT NUMBER(5) := 80;

  ------------------------------------------
  -- Статусы платежа
  ------------------------------------------
  -- Отвергнут
  PM_REJECTED       CONSTANT NUMBER(5) :=   100;
  -- Готов (создана операция)
  PM_READIED        CONSTANT NUMBER(5) :=  1000;
  -- В картотеке №2
  PM_I2PLACED       CONSTANT NUMBER(5) :=  2000;
  -- помещен в картотеку ОР
  PM_IWPPLACED      CONSTANT NUMBER(5) :=  2100;
  -- Готов к отправке в МБР
  PM_READY_TO_SEND  CONSTANT NUMBER(5) :=  3000;
  -- Обрабатывается в БЭК-ОФИСЕ
  PM_KVITPROCESSING CONSTANT NUMBER(5) :=  3100;
  -- Завершенный платеж
  PM_FINISHED       CONSTANT NUMBER(5) := 32000;

  ------------------------------------------
  -- Статусы pmprop
  ------------------------------------------
  -- Готовится
  PM_PROP_PREPARING    CONSTANT NUMBER(5) :=    0;
  -- Платеж, участвовавший в слиянии
  PM_PROP_MERGED       CONSTANT NUMBER(5) :=   51;
  -- Отвергнутый платеж (обработка завершена, но неуспешно)
  PM_PROP_REJECTED     CONSTANT NUMBER(5) :=  100;
  -- Загружен (сформирован ответный по сообщению)
  PM_PROP_LOADED       CONSTANT NUMBER(5) :=  300;
  -- Принят на обработку (готовится к контролю)
  PM_PROP_UNCHECKED    CONSTANT NUMBER(5) :=  301;
  -- Проверен (проконтролирован ответный)
  PM_PROP_CHECKED      CONSTANT NUMBER(5) :=  600;
  -- Получен
  PM_PROP_RECEIVED     CONSTANT NUMBER(5) := 1000;
  -- Невыясненный
  PM_PROP_UNKNOWN      CONSTANT NUMBER(5) := 2000;
  -- Ответный несквитованный
  PM_PROP_UNKVITED     CONSTANT NUMBER(5) := 2250;
  -- Платеж, возвращенный на обработку в бэк-офис
  PM_PROP_RETURNED     CONSTANT NUMBER(5) := 2500;
  -- Готов к отправке
  PM_PROP_READY        CONSTANT NUMBER(5) := 3000;
  -- Спозиционирован
  PM_PROP_POSIT        CONSTANT NUMBER(5) := 4000;
  -- Проконтролирован
  PM_PROP_CONTROL      CONSTANT NUMBER(5) := 5000;
  -- Выгружен в Корсчета
  PM_PROP_DISCHARGED   CONSTANT NUMBER(5) := 6000;
  -- Сообщение сформировано
  PM_PROP_MESDONE      CONSTANT NUMBER(5) := 6300;
  -- Сообщение проконтролировано
  PM_PROP_MESCONTROL   CONSTANT NUMBER(5) := 6600;
  -- Сообщение отбраковано адресатом
  PM_PROP_MESDEFECT    CONSTANT NUMBER(5) := 6700;
  -- Выгружен в текстовый файл (отправлен корреспонденту)
  PM_PROP_UPLOADED     CONSTANT NUMBER(5) := 7000;
  -- Доставлен корреспонденту
  PM_PROP_DELIVERED    CONSTANT NUMBER(5) := 7150;
  -- Отвергнут у корреспондента
  PM_PROP_CORREJECTED  CONSTANT NUMBER(5) := 7200;
  -- В картотеке корсчета
  PM_PROP_CARDFILE     CONSTANT NUMBER(5) := 7300;
  -- В картотеке корсчета у корреспондента
  PM_PROP_CARDFILECORR CONSTANT NUMBER(5) := 7600;
  -- Сквитован
  PM_PROP_KVITED       CONSTANT NUMBER(5) := 8000;
  -- Начальный закрытый
  PM_PROP_CLOSED       CONSTANT NUMBER(5) := 32000;

  ------------------------------------------
  -- Виды связей платежа
  ------------------------------------------
  -- Неттинг платежей
  PMLINK_KIND_NETTING   CONSTANT NUMBER(5) :=  1;
  -- Квитовка платежей (плановых с фактическими)
  PMLINK_KIND_KVITING   CONSTANT NUMBER(5) :=  2;
  -- Отзыв части суммы платежа
  PMLINK_KIND_RECALL    CONSTANT NUMBER(5) :=  3;
  -- Пролонгация
  PMLINK_KIND_PROLONG   CONSTANT NUMBER(5) :=  4;
  -- Капитализация
  PMLINK_KIND_CAPITAL   CONSTANT NUMBER(5) :=  5;
  -- Просрочка
  PMLINK_KIND_OVERDUE   CONSTANT NUMBER(5) :=  6;
  -- Используется при возврате, перенаправлении
  PMLINK_KIND_RETREDIR  CONSTANT NUMBER(5) :=  7;
  -- Используется при перенаправлении платежа на закрытый счет
  PMLINK_KIND_CLOSACC   CONSTANT NUMBER(5) :=  8;
  -- Сводный платеж
  PMLINK_KIND_MULTYPM   CONSTANT NUMBER(5) :=  9;
  -- Обработка невыясненной суммы
  PMLINK_KIND_PROCUNKN  CONSTANT NUMBER(5) := 10;
  -- Исполнение поручения
  PMLINK_KIND_EXECORDER    CONSTANT NUMBER := 11;
  -- Частичное исполнение платежа
  PMLINK_KIND_PARTPAYMENT CONSTANT NUMBER(5):= 17;

  ------------------------------------------
  -- Некоторые статусы первичек
  ------------------------------------------
  -- Мемордер закрыт
  MEMORDER_STATUS_CLOSE CONSTANT NUMBER(5) := 3;
  -- Рублевый клиентский платеж закрыт
  PSPO_ST_CLOSED        CONSTANT NUMBER(5) := 15;
  -- Валютный платеж закрыт
  CP_ST_CLOSED          CONSTANT NUMBER(5) := 15;
  -- Кассовый ордер закрыт
  STAT_CASH_ORDER_CLOSE CONSTANT NUMBER(5) := 3;
  -- Мультивалютный документ закрыт
  MCDOC_STATUS_CLOSE    CONSTANT NUMBER(5) := 3;
  -- Банковский платеж/требование закрыто
  CB_DOC_STATE_CLOSED   CONSTANT NUMBER(5) := 50;

  ------------------------------------------
  -- Происхождения платежа
  ------------------------------------------
  -- Введен пользователем вручную
  PAYMENT_OR_MANUAL  CONSTANT NUMBER(5) := 1;
  -- Получен по электронным каналам
  PAYMENT_OR_ELECTR  CONSTANT NUMBER(5) := 2;
  -- Создан автоматически
  PAYMENT_OR_AUTO    CONSTANT NUMBER(5) := 3;

  ------------------------------------------
  -- Происхождения мем. ордеров -- Транзитный
  ------------------------------------------
  MEMORDER_FDOC_TRANZIT CONSTANT NUMBER(5) := 11;
  -- Происхождения валютных платежек -- Транзитный
  CP_OR_TRANZIT         CONSTANT NUMBER(5) := 13;

  ------------------------------------------
  -- Типы примечаний для объекта типа "Рублевый платежный документ"  (ObjectType = 500)
  ------------------------------------------
  -- Причина удаления
  NOTEKIND_PSPO_DELETEGROUND   CONSTANT NUMBER(5) :=  1;
  -- Причина невозможности выполнения отзыва
  NOTEKIND_PSPO_NOREVORDREAS  CONSTANT NUMBER(5) :=  2;
  -- Идентификатор ЗДА
  NOTEKIND_PSPO_ID_ZDA                CONSTANT NUMBER(5) :=  3;
  -- Причина отказа (требование)
  NOTEKIND_PSPO_REJECTGROUND   CONSTANT NUMBER(5) :=  4;

  ------------------------------------------
  -- Виды примечаний платежей
  ------------------------------------------
  -- Тип обслуживания
  NOTEKIND_PM_SBRFSERVICETYPE  CONSTANT NUMBER(5) := 24;
  -- Дата последней обработки
  NOTEKIND_PM_SBRFLASTDATE     CONSTANT NUMBER(5) := 25;
  -- Дата валютирования рассчитана автоматически
  NOTEKIND_PM_SBRFVLDATEAUTO   CONSTANT NUMBER(5) := 26;
  -- Резервное поле
  NOTEKIND_PM_SBRFRESERVEFLD1  CONSTANT NUMBER(5) := 27;
  -- Номер филиала получателя
  NOTEKIND_PM_SBRFRESERVEFLD2  CONSTANT NUMBER(5) := 28;
  -- Дата приема документа от клиента рассчитана автоматически
  NOTEKIND_PM_SBRFCLNTDATEAUTO CONSTANT NUMBER(5) := 29;
  -- Резерв по просроченным требованиям
  NOTEKIND_PM_RESERV           CONSTANT NUMBER(5) := 32;
  -- Номер купона
  NOTEKIND_PM_COUPON           CONSTANT NUMBER(5) := 33;
  -- Дата приостановления в картотеке
  NOTEKIND_PM_INDEX_STOPDATE   CONSTANT NUMBER(5) := 40;
  -- Основание приостановления в картотеке
  NOTEKIND_PM_INDEX_STOPGROUND CONSTANT NUMBER(5) := 41;
  -- Причина отказа (возврата) для платежа
  NOTEKIND_PM_DENIALGROUND     CONSTANT NUMBER(5) := 42;
  -- Идентификатор плательщика
  NOTEKIND_PAYM_PAYERID        CONSTANT NUMBER(5) := 57;
  -- Символ ОФР
  NOTEKIND_PM_OFRSYMBOL        CONSTANT NUMBER(5) := 67;
  -- Результат проверки реквизитов платежа ЖКХ
  NOTEKIND_PM_HCSWARNING       CONSTANT NUMBER(5) := 69;
  -- Результат проверки реквизитов платежа для ГИС ГМП
  NOTEKIND_PM_GISGMP_CHECK_RES CONSTANT NUMBER(5) := 73;
  -- Дата помещения в картотеку 2
  NOTEKIND_PM_I2PLACEDATE      CONSTANT NUMBER(5) := 99;

  ------------------------------------------
  -- Принадлежность субъекта виду
  ------------------------------------------
  --Участник БЭСП
  PTK_BESP                   CONSTANT NUMBER := 49;

  ------------------------------------------
  -- Категории субъектов
  ------------------------------------------
  --Категория Тип субъекта
  PARTY_ATTR_TYPE            CONSTANT NUMBER := 16;
  --Категория участия в БЭСП
  PARTY_ATTR_BESP            CONSTANT NUMBER := 40;
  --Отключение от БЭСП
  PARTY_ATTR_DISCONNECT_BESP CONSTANT NUMBER := 41;

  ------------------------------------------
  -- Значения категории Тип субъекта
  ------------------------------------------
  -- Нотариус
  PARTY_AT_NOTARY            CONSTANT NUMBER := 2;
  -- Адвокат
  PARTY_AT_LAWYER            CONSTANT NUMBER := 3;
  -- Ломбард
  PARTY_AT_LOMBARD           CONSTANT NUMBER := 5;
  -- КФХ( Глава крестьянского (фермерского) хозяйства )
  PARTY_AT_KFX               CONSTANT NUMBER := 6;
  -- Доверительный управляющий
  PARTY_AT_TRUSTMANAGER      CONSTANT NUMBER := 7;
  -- Управляющий товарищ инвестиционного товарищества
  PARTY_AT_MANAGPARTNER      CONSTANT NUMBER := 8;
  -- Арбитражный управляющий
  PARTY_AT_ARBITRMANAGER     CONSTANT NUMBER := 9;
    -- Иностранная структура без образования юридического лица
  PARTY_AT_FOREIGN_ENT       CONSTANT NUMBER := 13;
  -- Не предусматривает бенефициарного владельца
  PARTY_AT_FOREIGN_ENT_NOOWN CONSTANT NUMBER := 14;
  -- Не предусматривает единоличного исполнительного органа
  PARTY_AT_FOREIGN_ENT_NOADM CONSTANT NUMBER := 15;


  ------------------------------------------
  -- Виды кодов субъектов
  ------------------------------------------
  -- Код клиента оставлен для совместимости.
  PTCK_CLIENT  CONSTANT NUMBER(5) :=  1;
  -- БИК (РФ)
  PTCK_BIC     CONSTANT NUMBER(5) :=  3;
  -- Код клиринга
  PTCK_CLIRING CONSTANT NUMBER(5) :=  5;
  -- SWIFT-код
  PTCK_SWIFT   CONSTANT NUMBER(5) :=  6;
  -- Национальный код (SWIFT)
  PTCK_NATCOD  CONSTANT NUMBER(5) := 12;
  -- Код СМФР (система межфилиальных расчетов Сбербанка)
  PTCK_SMFR    CONSTANT NUMBER(5) := 14;
  -- Код абонента SBRF3
  PTCK_SBRF    CONSTANT NUMBER(5) := 15;
  -- ИНН
  PTCK_INN     CONSTANT NUMBER(5) := 16;
  -- ОГРН
  PTCK_OGRN    CONSTANT NUMBER(5) := 27;
  -- УНКГН
  PTCK_UNKGN   CONSTANT NUMBER(5) := 68;
  
  ------------------------------------------
  -- Организационные формы субъектов
  ------------------------------------------
  -- Все
  PTLEGF_ALL   CONSTANT NUMBER(5) := 0;
  -- Организации
  PTLEGF_INST  CONSTANT NUMBER(5) := 1;
  -- Физические лица
  PTLEGF_PERSN CONSTANT NUMBER(5) := 2;

  ------------------------------------------
  -- Группы платежей
  ------------------------------------------
  -- Группа не определена
  PAYMENTS_GROUP_UNDEF    CONSTANT NUMBER(5) := 0;
  -- Внешний
  PAYMENTS_GROUP_EXTERNAL CONSTANT NUMBER(5) := 1;
  -- Межфилиальный
  PAYMENTS_GROUP_BRANCH   CONSTANT NUMBER(5) := 2;
  -- Внутренний платеж
  PAYMENTS_GROUP_INTERNAL CONSTANT NUMBER := PAYMENTS_GROUP_BRANCH;

  ------------------------------------------
  -- Расходы по валютному платежу
  ------------------------------------------
  -- OUR
  PM_CHRG_OUR  CONSTANT NUMBER(5) := 0;
  -- SHA
  PM_CHRG_SHA  CONSTANT NUMBER(5) := 1;
  -- BEN
  PM_CHRG_BEN  CONSTANT NUMBER(5) := 2;

  ------------------------------------------
  -- Статусы узлов ТС
  ------------------------------------------
  -- открыт
  DEPARTMENT_STATUS_OPEN     CONSTANT NUMBER(5) :=  1;
  -- активен
  DEPARTMENT_STATUS_ACTIVE   CONSTANT NUMBER(5) :=  2;
  -- закрыт
  DEPARTMENT_STATUS_CLOSED   CONSTANT NUMBER(5) :=  3;
  -- вышестоящий
  DEPARTMENT_STATUS_SUPERIOR CONSTANT NUMBER(5) :=  4;

  ------------------------------------------
  -- Виды узлов ТС
  ------------------------------------------
  -- филиал
  DEPARTMENT_TYPE_FILIAL CONSTANT NUMBER(5) := 1;
  -- ВСП
  DEPARTMENT_TYPE_VSP    CONSTANT NUMBER(5) := 2;

  ------------------------------------------
  -- Режимы доступа для узлов ТС
  ------------------------------------------
  -- Online
  DEPARTMENT_ACCESS_ONLINE  CONSTANT NUMBER(5) := 1;
  -- Offline
  DEPARTMENT_ACCESS_OFFLINE CONSTANT NUMBER(5) := 2;
  -- Другая ЦАБС
  DEPARTMENT_ACCESS_NOTCABS CONSTANT NUMBER(5) := 3;

  /**
   * Идентификатор субъекта "Неизвестный"
   */
  UNKNOWNPARTY CONSTANT NUMBER(5) := -1;

  ------------------------------------------
  -- Сторона платежа Дебет/Кредит
  ------------------------------------------
  -- дебет
  PRT_DEBIT     CONSTANT NUMBER(5) := 0;
  -- кредит
  PRT_CREDIT    CONSTANT NUMBER(5) := 1;
  -- неопределена
  PRT_UNDEFINED CONSTANT NUMBER(5) := 2;

  ------------------------------------------
  -- Номера глав
  ------------------------------------------
  -- балансовые
  CHAPT1 CONSTANT NUMBER(5)  := 1;
  -- внебаланс
  CHAPT3 CONSTANT NUMBER(5)  := 3;

  /**
   * Идентификатор национальной валюты
   */
  NATCUR CONSTANT NUMBER(10) := 0;

  ------------------------------------------
  -- Условия акцепта для требований
  ------------------------------------------
  -- С акцептом
  PM_DEMAND_TERM_ACCEPT        CONSTANT NUMBER(5)  := 0;
  -- Без акцепта
  PM_DEMAND_TERM_WITHOUTACCEPT CONSTANT NUMBER(5)  := 1;

  /**
   * Состояния акцепта для требований
   */
  -- Не требуется
  PM_DEMAND_ACCEPT_NONE     CONSTANT NUMBER(5)  := 0;
  -- Ожидает акцепта
  PM_DEMAND_ACCEPT_WAIT     CONSTANT NUMBER(5)  := 1;
  -- Акцептовано
  PM_DEMAND_ACCEPT_ACCEPT   CONSTANT NUMBER(5)  := 2;
  -- Отказ от акцепта
  PM_DEMAND_ACCEPT_REJECTED CONSTANT NUMBER(5)  := 3;

  /**
   * Константы для регистрации вставки в шлюзе
   */
  RG_PMPAYM      CONSTANT NUMBER(5) := 55;
  RG_CREATEOBJEC CONSTANT NUMBER(5) := 1;

  /**
   * Сегмент статуса операции платежа "Направление"
   */
  OPR_PAYM_DIRECT CONSTANT NUMBER(5) := 295;
  /**
   * Сегмент статуса операции платежа "Квитовка ответного"
   */
  OPR_PAYM_IN_KVIT  CONSTANT NUMBER(5) := 3201;
  /**
   * Сегмент статуса операции платежа "Квитовка начального"
   */
  OPR_PAYM_OUT_KVIT CONSTANT NUMBER(5) := 294;

  ------------------------------------------
  -- Значения сегментов "Направление"
  ------------------------------------------
  /**
   * Внутренний
   */
  OPR_PM_ST_DIR_INTERNAL CONSTANT NUMBER(5) := 1;
  /**
   * Входящий
   */
  OPR_PM_ST_DIR_IN CONSTANT NUMBER(5) := 2;
  /**
   * Исходящий
   */
  OPR_PM_ST_DIR_OUT CONSTANT NUMBER(5) := 3;
  /**
   * Транзит
   */
  OPR_PM_ST_DIR_TRANZIT CONSTANT NUMBER(5) := 4;

  ------------------------------------------
  -- Значения сегменнов "Квитовка"
  ------------------------------------------
  -- несквитован
  OPR_PM_ST_UNKVIT CONSTANT NUMBER(5) := 1;
  -- сквитован полностью
  OPR_PM_ST_KVIT   CONSTANT NUMBER(5) := 2;
  -- отказ
  OPR_PM_ST_CANCEL CONSTANT NUMBER(5) := 3;

  ------------------------------------------
  -- Возможные значениия поля doprtemp_tmp.t_SkipDocument
  ------------------------------------------
  -- Из обработки не исключен
  SKIP_DOC_NOSKIP   CONSTANT INTEGER := 0;
  -- Временно (для выполнения какой-то процедуры) исключается из обработки
  SKIP_DOC_TMP      CONSTANT INTEGER := 2;
  -- Исключение из обработки документов, операция по которым не закончена (при закрытии операции)
  SKIP_DOC_OPRCLOSE CONSTANT INTEGER := 3;
  -- Исключается из обработки до конца прикладной части шага
  SKIP_DOC_EOS      CONSTANT INTEGER := 4;
  -- Временно исключается из обработки если не подходит для создания записи в сервисе SendDocStatusFromABS
  SKIP_DOC_SDSFABS  CONSTANT INTEGER := 5;

  ------------------------------------------
  -- Виды обслуживания субъектов
  ------------------------------------------
  -- Все
  PTSK_ALL CONSTANT NUMBER(5) := 0;
  -- РКО
  PTSK_PAY CONSTANT NUMBER(5) := 3;
  -- АРМ-позиционера
  PTSK_ARM CONSTANT NUMBER(5) := 9;
  -- Клиент - физическое лицо
  PTSK_PERSN CONSTANT NUMBER(5) := 10;

  ------------------------------------------
  -- Виды субъектов
  ------------------------------------------
  -- Все
  PTK_ALL    CONSTANT NUMBER(5) := 0;
  -- Клиенты
  PTK_CLIENT CONSTANT NUMBER(5) := 1;
  --Участник ПС БР
  PTK_PARTICIPANTS_PS_BR CONSTANT NUMBER(5) := 85;

  ------------------------------------------
  -- Виды участников расчетов
  ------------------------------------------
  -- РКЦ
  PT_KIND_PAYM_CASH_CENTRE CONSTANT NUMBER(5):= 1;

  ------------------------------------------
  -- Виды платежа
  ------------------------------------------
  -- почтой
  PAYMENT_KIND_MAIL CONSTANT CHAR(1) := 'П';
  -- телеграф
  PAYMENT_KIND_TELG CONSTANT CHAR(1) := 'T';
  -- электронно
  PAYMENT_KIND_ELEC CONSTANT CHAR(1) := 'Э';
  -- срочно
  PAYMENT_KIND_INST CONSTANT CHAR(1) := 'С';

  ------------------------------------------
  -- Виды кассовых символов
  ------------------------------------------
  -- 1 - приход
  TS_DEBET  CONSTANT NUMBER(5) := 1;
  -- 2 - расход
  TS_KREDIT CONSTANT NUMBER(5) := 2;
  -- 3 - забаланс
  TS_NOTB   CONSTANT NUMBER(5) := 3;

  ------------------------------------------
  -- вид записи в справочнике категорий учета (mccateg.dbt)
  ------------------------------------------
  -- категория учета
  MCCATEG_LEVEL_TYPE_CATEGORY CONSTANT NUMBER(5) := 1;
  -- группа категорий учета
  MCCATEG_LEVEL_TYPE_GROUP    CONSTANT NUMBER(5) := 2;

  ------------------------------------------
  -- Номера категорий учёта
  ------------------------------------------
  -- Счета учета средств по незавершенным расчетным операциям (списание)
  MCCATEG_UNFIN_OUT CONSTANT NUMBER(5) := 103;
  -- Счета учета средств по незавершенным расчетным операциям (зачисление)
  MCCATEG_UNFIN_IN  CONSTANT NUMBER(5) := 104;

  ------------------------------------------
  -- Виды позиционирования платежа
  ------------------------------------------
  -- Позиционирование системное
  PM_CORRPOS_TYPE_SYSTEM CONSTANT NUMBER(5) := 0;
  -- Позиционирование пользовательское
  PM_CORRPOS_TYPE_USER   CONSTANT NUMBER(5) := 1;

  ------------------------------------------
  -- Значения настройки 'CB\PAYMENTS\CHANGFOREDITOPER'
  ------------------------------------------
  -- сохранить изменения без смены автора
  CHFEO_SAVE                 CONSTANT NUMBER(5) := 0;
  -- сохранить изменения без смены автора, при услови, что изменения не важные
  CHFEO_SAVE_WITH_CHECK_OPER CONSTANT NUMBER(5) := 1;
  -- сохранить изменения со сменой автора
  CHFEO_SAVE_WITH_NEW_OPER   CONSTANT NUMBER(5) := 2;

  ------------------------------------------
  -- Значения настройки 'CB\PAYMENTS\DEPARTMENTALINFO\CHECKINNSUMACC'
  ------------------------------------------
  -- не проверять для синтетических
  CHISA_NOTCHECK             CONSTANT NUMBER(5) := 0;
  -- проверять счет по маске счетов физ. диц
  CHISA_PERSN_ACC_MASK_CHECK CONSTANT NUMBER(5) := 1;
  -- оба варианта
  CHISA_BOTH_CHECK           CONSTANT NUMBER(5) := 2;

  ------------------------------------------
  -- Сводный счет
  ------------------------------------------
  -- Счет не сводный 
  ACC_NOT_CONSOLIDATED       CONSTANT NUMBER(5) := 0;
  -- Счет сводный 
  ACC_CONSOLIDATED           CONSTANT NUMBER(5) := 1;

  ------------------------------------------
  -- Правила выполнения проверок для сводных счетов
  ------------------------------------------
  -- Проверка не выполняется
  CAR_NOT_CHECK               CONSTANT NUMBER(5) := 0;
  -- Проверки как для физических лиц
  CAR_PERSN_ACC_CHECK         CONSTANT NUMBER(5) := 1;
  -- Проверки как для юридических лиц
  CAR_INST_ACC_CHECK          CONSTANT NUMBER(5) := 2;

  ------------------------------------------
  -- Категория "Инициатор операции"
  ------------------------------------------
  -- клиент
  CTG_OPERINITIATOR_KLIENT CONSTANT NUMBER(5) := 1;
  -- банк
  CTG_OPERINITIATOR_BANK CONSTANT NUMBER(5) := 2;

  -- Сервис передачи изменения статуса платежного документа
  ASYNC_CALL_EVENT_DOC_STATUS CONSTANT NUMBER := 4;

  /**
   * Принадлежность счетов к РКО или к Ритейлу
   */
  -- не определено
  ACCOUNTS_OWNER_KIND_UNDEF  CONSTANT NUMBER(5) := 0;
  -- счета РКО
  ACCOUNTS_OWNER_KIND_PS     CONSTANT NUMBER(5) := 1;
  -- счета физ.лиц
  ACCOUNTS_OWNER_KIND_PERSN  CONSTANT NUMBER(5) := 2;
  -- оба вида счетов
  ACCOUNTS_OWNER_KIND_ALL    CONSTANT NUMBER(5) := 3;

  /**
   * Определение субъекта, реального владельца счета, по внутрибанковским счетам. 0 - владелец счета, 1 - филиал счета
   */
  -- Владелец счёта
  PM_BAS_CLIENT  CONSTANT NUMBER(5) := 0;
  -- Филиал счёта
  PM_BAS_DEP     CONSTANT NUMBER(5) := 1;


  /**
   * Плтаеж выполнялся иным лицом
   */
   -- нет
  PM_OTHRPRSN_NONE CONSTANT NUMBER(5) := 0;
  -- платеж за юрлицо
  PM_OTHRPRSN_INST CONSTANT NUMBER(5) := 1;
  -- платеж за физлицо
  PM_OTHRPRSN_PRSN CONSTANT NUMBER(5) := 2;

  /*
  * Типы участников операции по платежу
  */

  PART_PAYERBANK            CONSTANT NUMBER(5) := 0; 
  PART_PREVINSTRUCTINGAGENT CONSTANT NUMBER(5) := 1; 
  PART_SENDERBANK           CONSTANT NUMBER(5) := 2; 
  PART_EXECUTORBANK         CONSTANT NUMBER(5) := 3; 
  PART_INTERMEDIARYBANK     CONSTANT NUMBER(5) := 4; 
  PART_RECEIVERBANK         CONSTANT NUMBER(5) := 5; 

  /**
   * Сбросить все глобализмы пакетов
   * @since 6.20.030.51.0
   */
  PROCEDURE ResetAllGlobalsRegValues;

  /**
   * Получение текущего операционного дня
   * @since 6.20.030
   * @return DATE Текущий операционный день
   * @deprecated
   * @see RsbSessionData.curdate
   */
  FUNCTION CURDATE RETURN DATE;

  /**
   * Получение БИК субъекта, связанного с текущим филиалом
   * Инициализируется вызовом PM_COMMON.Init
   * @since 6.20.030
   * @return VARCHAR2 БИК субъекта, связанного с текущим филиалом
   */
  FUNCTION SELFBIC RETURN VARCHAR2;

  /**
   * Получение корсчета текущего филиала в РКЦ
   * Инициализируется вызовом PM_COMMON.Init
   * @since 6.20.030
   * @return Корсчет текущего филиала в РКЦ
   */
  FUNCTION SELFCORACC RETURN VARCHAR2;

  /**
   * Инициализация глобализмов  OURBANK, SELFBIC, SELFCORACC
   * @since 6.20.030
   */
  PROCEDURE Init;

  /**
  Получение ноты для объекта p_ObjectType
  */
  FUNCTION GetNoteTextStr( p_ObjectID IN NUMBER, p_ObjectType IN NUMBER, p_NoteKind IN NUMBER, p_Date IN DATE ) RETURN VARCHAR2;

  /**
   * Получение "строковой" ноты платежа на дату из dnotetext_dbt для платежей t_ObjectType = PM_COMMON.OBJTYPE_PAYMENT
   * @param p_PaymentID Идентификатор платежа
   * @param p_Kind      Примечание вида
   * @param p_Date      Дата на которую нужно получить ноту
   * @return VARCHAR2 текст примечания в случае успеха, CHR(0) в случае отсутствия примечания
   */
  FUNCTION GetPaymNoteTextStr( p_PaymentID IN NUMBER, p_Kind IN NUMBER, p_Date IN DATE ) RETURN VARCHAR2;

  /**
   * Изменение статуса платежа с привязкой к шагу операции.
   * При незаданном идентификаторе платежа меняет статус множества платежей,
   * заданных в doprtemp_tmp.
   * @since 6.20.030
   * @param p_PaymentID    Идентификатор платежа
   * @param p_PaymStatus   Новый статус платежа
   * @param p_ID_Operation Идентификатор операции
   * @param p_ID_Step      Идентификатор шага операции
   * @param p_Oper         Операционист, меняющий статус
   * @return NUMBER 0 в случае успеха, !0 в случае ошибки
   */
  FUNCTION ChangePaymStatus( p_PaymentID IN NUMBER, p_PaymStatus IN NUMBER,
                             p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER,
                             p_Oper IN NUMBER ) RETURN NUMBER;

  /**
   * Массовое изменение статуса платежей с привязкой к шагу операции.
   * Меняет статус множества платежей, заданных в doprtemp_tmp.
   * Может быть вызывана из ChangePaymStatus или отдельно.
   * @since 6.20.030
   * @param p_PaymStatus   Новый статус платежа
   * @param p_Oper         Операционист, меняющий статус
   * @return INTEGER 0 в случае успеха, !0 в случае ошибки
   */
  FUNCTION MassChangePaymStatus( p_PaymStatus IN NUMBER, p_Oper IN NUMBER DEFAULT NULL ) RETURN INTEGER;

  /**
   * Изменение статуса свойств платежа с привязкой к шагу операции.
   * При незаданном идентификаторе платежа меняет статус множества платежей,
   * заданных в doprtemp_tmp.
   * @since 6.20.030
   * @param p_PaymentID    Идентификатор платежа
   * @param p_IsSender     Сторона ( SET_CHAR - отправитель, UNSET_CHAR - получатель )
   * @param p_PropStatus   Новый статус свойств платежа
   * @param p_TpSchemID    Номер транспортной схемы. Если параметр задан, меняет статус только для платежей заданной т/с.
   * @return NUMBER 0 в случае успеха, !0 в случае ошибки
   */
  FUNCTION ChangePmPropStatus( p_PaymentID IN NUMBER, p_IsSender IN CHAR, p_PropStatus IN NUMBER,
                               p_TpSchemID IN NUMBER DEFAULT NULL ) RETURN NUMBER;

  /**
   * Проверяет, является ли счёт банковским
   * @since 6.20.030
   * @param Acc номер счёта
   * @return NUMBER 1 для внутрибанковского счёта, 0 для клиентского
   */
  FUNCTION IsOwnerAccOwnBank( Acc IN VARCHAR2 ) RETURN NUMBER;

  /**
   * Проверяет, является ли счёт корреспондентским
   * @since 6.20.030
   * @param p_FIID    Валюта счёта
   * @param p_Chapter Глава счёта
   * @param p_Account Номер счёта
   * @return BOOLEAN 1 для корреспондентского счёта, 0 в противном случае
   */
  FUNCTION IsCorrAcc( p_FIID in integer, p_Chapter in integer, p_Account in varchar2 ) RETURN BOOLEAN;

  /**
   * Проверяет, является ли счёт корреспондентским счётом ЛОРО
   * @since 6.20.030
   * @param p_Account Номер счёта
   * @param p_FIID    Валюта счёта
   * @return INTEGER 1 для ЛОРО-корсчёта, 0 в противном случае
   */
  function IsLoroAccount( p_Account in varchar2, p_FIID in number ) return integer;

  /**
   * Проверяет, является ли счёт, указанный в платеже, клиентским
   * @since 6.20.030
   * @param p_Group    Группа платежа ( PAYMENTS_GROUP_... )
   * @param p_ClientID Идентификатор контрагента в платеже
   * @param p_Account  Номер счёта в платеже
   * @return INTEGER 1 - клиентский, 0 - нет
   */
  function IsClientAccount( p_Group    in number,
                            p_ClientID in number,
                            p_Account  in varchar2 ) return integer deterministic;

  /**
   * Проверяет, является ли счёт, указанный в платеже, банковским
   * @since 6.20.030
   * @param p_Group    Группа платежа ( PAYMENTS_GROUP_... )
   * @param p_ClientID Идентификатор контрагента в платеже
   * @param p_Account  Номер счёта в платеже
   * @return INTEGER 1 - банковский, 0 - клиентский
   */
  function IsBankAccount( p_Group    in number,
                          p_ClientID in number,
                          p_Account  in varchar2 ) return integer deterministic;

  /**
   * Проверяет, является ли счет счетом МФР? А заодно и филиал-корреспондент определим...
   * @since 6.20.030
   * @param p_Account  Номер счёта
   * @param p_Currency Валюта счёта
   * @param p_Chapter  Глава счёта
   * @param v_Dep      Возвращаемый параметр - запись филиала-корреспондента
   * @return CHAR SET_CHAR - счёт МФР, UNSET_CHAR - нет
   */
  function PM_AccountIsMFR( p_Account  in  varchar2,
                            p_Currency in  number,
                            p_Chapter  in  number,
                            v_Dep      out ddp_dep_dbt%rowtype
                          ) return char;

  /**
   * Получить номер филиала для заданного операциониста
   * @since 6.20.030
   * @param p_Oper - номер операциониста
   * @return NUMBER Номер филиала
   */
  FUNCTION GetDepartmentByOper( p_Oper IN NUMBER ) RETURN NUMBER;

  /**
   * Удаление платежа
   * @since 6.20.030
   * @param p_PaymentID Идентификатор платежа
   * @return INTEGER 0 в случае успеха, !0 в случае ошибки
   */
  FUNCTION DeletePayment( p_PaymentID IN NUMBER ) RETURN INTEGER;

  /**
   * Получить субъекта-носителя кода
   * @since 6.20.030
   * @param p_CodeKind    Вид кода
   * @param p_Code        Код
   * @param p_CodeOwnerID Возвращаемый параметр. Субъект-носитель кода
   * @param p_Department  Возвращаемый параметр. Узел ТС, соответствующий субъекту (заполняется t_Code, t_AccessMode, t_NodeType)
   * @param p_Date  Дата, на которую должен быть активен код, если null, то активный сейчас
   * @return BOOLEAN true в случае успеха, false в случае ошибки (код не найден)
   */
  function GetCodeOwner( p_CodeKind in integer, p_Code in varchar2, p_CodeOwnerID in out integer, p_Department out ddp_dep_dbt%rowtype, p_Date in date default null )
  return boolean;

  /**
   * Получить структуру филиала по ID субъекта
   * @since 6.20.030
   * @param p_PartyID    ID субъекта
   * @param p_Department Возвращаемый параметр. Узел ТС, соответствующий субъекту
   * @return BOOLEAN true в случае успеха, false в случае ошибки (филиал не найден или закрыт)
   */
  function GetPartyDep( p_PartyID in integer, p_Department out ddp_dep_dbt%rowtype ) return boolean;

  /**
   * Найти актуальный код субъекта
   * @since 6.20.031
   * @param p_PartyID    ID субъекта
   * @param p_CodeKind  вид кода
   * @return dpartcode_dbt.t_Code%type в случае успеха код,  null в случае ошибки (не найден)
   */
  function FindPartCode( p_PartyID  in  integer, p_CodeKind in  integer ) return dpartcode_dbt.t_Code%type;

  /**
   * Массовое доопредление полей платёжных инструкций (ПИ) платежей, указанных в dpmproc_dbt
   * Используется при вставке платежей в пакетном режиме (BatchMode в RSL)
   * @since 6.20.030
   */
  procedure SetPIForPmMass;

  /**
   * Массовое определение филиалов для платежей, указанных в dpmproc_dbt
   * Используется при вставке платежей в пакетном режиме (BatchMode в RSL)
   * @since 6.20.030
   */
  procedure SetDepartmentsForPmMass;

  /**
   * Массовая регистрация в шлюзе вставки платежей, указанных в dpmproc_tmp
   * Используется при вставке платежей в пакетном режиме (BatchMode в RSL)
   * @since 6.20.030
   */
  procedure RegistryAllObjectInGate;

  /**
   * Получить ID операции и готового к выполнению шага для платежа
   * @since 6.20.030
   * @param p_PaymentID Идентификатор платежа
   * @param p_OperID    Возвращаемый параметр - ID операции
   * @param p_StepID    Возвращаемый параметр - ID шага
   */
  procedure GetOperIDAndRStepIDByPaymentID( p_PaymentID IN NUMBER, p_OperID OUT NUMBER, p_StepID OUT NUMBER );

  /**
   * Проверить, что субъект входит в территориальную структуру нашего банка
   * @since 6.20.030
   * @param p_PartyID Идентификатор субъекта
   * @return INTEGER 1 - входит, 0 - нет
   */
  function IsBankInTS( p_PartyID in number ) return integer deterministic;

  /**
   * Проверить, что субъект является одним из филиалов нашего банка
   * Сложная проверка, учитывающая, что не все филиалы могут быть включены в ТС
   * @since 6.20.030
   * @param p_PartyID Идентификатор субъекта
   * @return INTEGER 1 - является филиалом, 0 - нет
   */
  function IsOurFilial( p_PartyID in number ) return integer;

  /**
   * Распарсить запись о массовой вставке платежей в фискальном журнале.
   * Заполняет dmasinspm_tmp.
   * @since 6.20.030
   * @param RecID    Идентификатор записи в журнале
   * @param NumDprt  Номер подраздления
   * @param DocCount Возвращаемый параметр - количество платежей
   * @param Stat     Возвращаемый параметр - статус ( 0 в случае успеха, !0 в случае ошибки )
   */
  procedure ParseMasInsPmLog( RecID    IN  dxml2_dfisclog_dbt.t_RecID%type,
                              NumDprt  IN  dxml2_dfisclog_dbt.t_NumDprt%type,
                              DocCount OUT NUMBER,
                              Stat     OUT NUMBER
                            );

  /**
   * Массово закрыть свойство картотеки невыясненных поступлений
   * Обрабатывается множество платежей из V_PMMASSOP
   * @since 6.20.030
   */
  PROCEDURE MassCloseUnknownProp;


  /**
   * Массовое изменение полей текущих сумм, счетов и валют на указанные в dpmcarryacc_tmp
   * Меняет поля dpmpaym_dbt, если соответствующее поля dpmcarryacc_tmp не равны null
   * @since 6.20.030
   * @return 0 в случае успеха, !0 в случае ошибки
   */
  FUNCTION UpdateFutureFields RETURN NUMBER;

  /**
   * Найти ставку НДС для экземпляра комиссии, указанного в платеже
   * @since 6.20.030.42.0
   * @param p_FeeType      Вид комиссии
   * @param p_FeeID        ID комиссии
   * @param p_MultiNDSRate Возвращаемый параметр - признак наличия разных ставок НДС в ТО
   * @return               Возвращает значение ставки НДС
   */
  FUNCTION GetNDSRateForComiss( p_FeeType      in  integer,
                                p_FeeID        in  integer,
                                p_MultiNDSRate out integer ) RETURN NUMBER;

  /**
   * Проверка существования этого признака для категории
   * @since 6.20.030.54.0
   * @param p_ObjType    Вид объекта
   * @param p_ObjID      ID объекта
   * @param p_GroupID    ID категории
   * @param p_AttrID     ID признака
   * @return NUMBER 1, если признак существует, иначе 0
   */
  FUNCTION CheckObjAttrPresence( p_ObjType IN NUMBER,
                                 p_ObjID IN VARCHAR2,
                                 p_GroupID IN NUMBER,
                                 p_AttrID  IN NUMBER ) RETURN NUMBER;

   /**
   * Получение значения категории
   * @since 6.20.031.21.0
   * @param p_ObjType    Вид объекта
   * @param p_ObjID      ID объекта
   * @param p_GroupID    ID категории
   * @param p_Date       дата, за которую получаем значение
   * @return NUMBER 0, если признак не найден, иначе значение признака
   */

 --Проверка существования этого признака для категории
  FUNCTION GetObjAttrValue( p_ObjType IN NUMBER,
                            p_ObjID IN VARCHAR2,
                            p_GroupID IN NUMBER,
                            p_Date  IN date ) RETURN NUMBER;

  /**
   * ОД "Баланс" - операционный день с признаком "баланс" в календаре филиала
   * @since 6.20.031.10
   * @param p_Department - ИД филиала
   * @param p_FromDate - дата, от которой начинать поиск
   * @return DATE - ОД "Баланс"
   */
  FUNCTION PM_GetOperDay_Balance( p_Department in number,
                                  p_FromDate in date default RsbSessionData.curdate
                                ) RETURN DATE;

  /**
   * ОД "Банковское обслуж." + "Баланс" - операционный день с признаком "баланс" и видом обслуживания "банковское"
   * @since 6.20.031.10
   * @param p_Department - ИД филиала
   * @return DATE - ОД "Банковское обслуж." + "Баланс"
   */
  FUNCTION PM_GetOperDay_BankServBalance( p_Department in number ) RETURN DATE;

  /**
    * Определение категории платежа ?Инициатор операции?
    * @param p_PaymentID      ID платежа
    * @return   Возвращает инициатора операции (1 - клиент, 2 - банк, 0 - неопределён)
    */
  FUNCTION GetOperInitiatorCtg(  p_PaymentID IN INTEGER ) RETURN INTEGER;

  /**
   * Получить название узла ТС по ID
   * @since 6.20.031
   * @param p_Department Номер узла ТС
   * @return VARCHAR2 Идентификатор узла ТС (ddp_dep_dbt.t_Name)
   */
  function GetNodeName( p_Department in number ) return varchar2 deterministic;

  /**
   * Массовая установка признака "Целевое финансирование" с привязкой к шагу
   * для документов из V_PMMASSOPFOREXE
   * @since 6.20.031
   */
  PROCEDURE MassSetIsPurpose;

   /**
   * Максимальная допустимая очередность платежей
   * @return Возвращает максимальную очередность
   */
   FUNCTION PM_DefaultMaxPriority RETURN NUMBER;

  /**
   * Получить вид операции по PaymentID и DocKind
   * @return Возвращает вид операции
   */
  function GetKindOperation( p_PaymentID in number, p_DocKind in number  ) return number;

  /**
   * Процедура записи xml по статусам платежа
   * @param p_Mode откуда пришел платеж (1 - BatchMode, 0 - все остальное)
   * @param p_PaymStatus Статус платежа
   * @param p_PaymentID ID платежа, если 0 - массовый режим
   * @param p_NewStatus Особый статус, установленный вручную
   * @return 0 или код ошибки
   */
  function PaymStatusToXmlAll( p_Mode in number, p_PaymStatus in number default 0, p_PaymentID in number default 0, p_NewStatus in varchar default CHR(1) ) return number;

  /**
   * Получить ObjectType по DocKind
   * @param p_PrimDocKind PrimDocKind платежа
   * @return ObjectType платежа
   */
  function DefineObjectKind( p_PrimDocKind in NUMBER ) return NUMBER;

  /**
   * Получить Origin первичных документов
   * @param p_PaymentID ID первички
   * @param p_DocKind DocKind первички (что бы не выбирать из pmpaym)
   * @return Origin первички
   */
  function GetPrimDocOrigin( p_PaymentID in NUMBER, p_DocKind in NUMBER default 0) return NUMBER;

  /**
   * По происхождению в первичке вычисляет номер происхождения в платеже
   * @param p_DocKind вид документа (pmpaym.DocKind)
   * @param p_Origin происхождение из первички (<таблица первички>.Origin)
   * @return PrimDocOrigin платежа
   */
  function DeterminePrimDocOrigin(  p_DocKind  in NUMBER, p_Origin in NUMBER default 0) return NUMBER;

  /**
   * Проверка контрольного ключа УИН
   * @param p_ReceiverAccount  Счет получателя (pmpaym.ReceiverAccount, bdtransf.ReceiverAccount)
   * @param p_UIN  УИН (pmrmprop.UIN, bdtransf.UIN)
   * @param p_ErrMsg Текст ошибки
   * @return 0 или код ошибки
   */
  function CheckUIN
  ( p_ReceiverAccount in VARCHAR2,
    p_UIN in VARCHAR2,
    p_ErrMsg out VARCHAR2
  )
  return INTEGER;

  /**
   * Платеж является платежом на общую сумму по переводам физ.лиц?
   * @return true - является, false - не является
   */
  function IsBdTrSummaryPayment(p_PaymentID IN dpmpaym_dbt.T_PAYMENTID%type)
    return BOOLEAN;
    
  /**
   * Получить маски счетов в настройке FillINNPayer в зависимости от вида документа
   * @param p_DocKind вид документа (pmpaym.DocKind)
   * @param p_SubDocKind подвид документа
   * @return маски счетов или пустая строка
   */
  function GetFillINNPayerByDocKind( p_DocKind IN INTEGER, p_SubDocKind IN INTEGER ) return VARCHAR2;
  
  /**
   * Получить маски счетов в настройке FillINNPayer в зависимости от контекста
   * @param p_Context контекст
   * @return маски счетов или пустая строка
   */
  function GetFillINNPayerByContext( p_Context in varchar2 ) return VARCHAR2;
  
  /**
   * Получить маски счетов в настройке FillINNReceiver в зависимости от вида документа
   * @param p_DocKind вид документа (pmpaym.DocKind)
   * @param p_SubDocKind подвид документа
   * @return маски счетов или пустая строка
   */  

  function GetFillINNReceiverByDocKind( p_DocKind IN INTEGER, p_SubDocKind IN INTEGER ) return VARCHAR2;
  
  /**
   * Получить маски счетов в настройке FillINNReceiverr в зависимости от контекста
   * @param p_Context контекст
   * @return маски счетов или пустая строка
   */
  
  function GetFillINNReceiverByContext( p_Context in varchar2 ) return VARCHAR2;

  /**
   * Проверка необходимости направления документа в очереди ожидающих исполнения
   * @param p_ValueDate д т  зн чения пл теж 
   * @param p_DocKind вид документ 
   * @param p_SubDockind подвид документ 
   * @return 0 или 1 (идет или нет документ в очередь)
   */
  function CheckWaitExec( p_ValueDate in DATE, p_DocKind in NUMBER, p_SubDockind in NUMBER ) return INTEGER;

  /**
   * Проверка подходит ли счет по маску счетов физ. лиц
   * @param p_Account счет
   * @return true, если счет подходит. false если нет или не найдена настройка
   */
  function InPersnMaskCheck( p_Account in varchar2 ) return boolean;

  /**
   * Проверка является ли счет сводным
   * @param p_Account счет
   * @param p_Type_Account тип счета
   * @param p_LegalForm физ. лицо / юр. лицо
   * @return ACC_NOT_CONSOLIDATED, если не сводный, ACC_CONSOLIDATED если сводный
   */
  function IsConsolidatedAccount( p_Account in varchar2, p_Type_Account in varchar2, p_LegalForm in integer ) return integer;

  /**
   * Правила выполнения проверок для сводных счетов
   * @param p_Account счет
   * @param p_Type_Account тип счета
   * @param p_LegalForm физ. лицо / юр. лицо
   * @return правлила выполнения проверок
   */
  function ConsolidatedAccCheckRules( p_Account in varchar2, p_Type_Account in varchar2, p_LegalForm in integer ) return integer;

  /**
   * Юридическая форма субъекта - юрлицо или физлицо
   * @param p_PartyID идентификатор субъекта
   * @return PTLEGF_INST или PTLEGF_PERSN
   */
  function GetLegalForm(p_PartyID IN dparty_dbt.t_PartyID%type) 
    return dparty_dbt.t_LegalForm%type;

  /**
   * Признак индивидуального предпринимателя для субъекта
   * @param p_PartyID идентификатор субъекта
   * @return SET_CHAR/UNSET_CHAR
   */
  function GetIsEmployer(p_PartyID IN dparty_dbt.t_PartyID%type) 
    return dpersn_dbt.t_IsEmployer%type;

  /**
   * Общий алгоритм определения принадлежности счета к РКО или к Ритейлу
   * @param AccountList список номеров счетов через запятую
   * @param ClientID идентификатор клиента, может быть не задан
   * @param ClientType вид клиента (физ/юрлицо), может быть не задан
   * @param ClientINN ИНН клиента, может быть не задан
   * @return INTEGER Вид счета (константа вида ACCOUNTS_OWNER_KIND_*)
   */
  FUNCTION RSI_GetAccOwnerKind
  ( AccountList IN VARCHAR2,
    ClientID IN dparty_dbt.t_PartyID%TYPE DEFAULT 0,
    ClientType IN dparty_dbt.t_LegalForm%TYPE DEFAULT PM_COMMON.PTLEGF_ALL,
    ClientINN IN dobjcode_dbt.t_Code%TYPE DEFAULT PM_COMMON.RSB_EMPTY_STRING
  ) RETURN INTEGER;

  /**
   * Получить идентификатор субъекта для филиала
   * @param p_DprtID Идентификатор филиала
   * @return ИД субъекта
   */
  FUNCTION GetDprtPartyID(p_DprtID in ddp_dep_dbt.t_Code%type) 
    RETURN dparty_dbt.t_PartyID%type;

  /**
   * Функция проверки является ли банк УБР
   * @param p_BankID Идентификатор банка
   * @return BOOLEAN Является/не является УБР
   */
  FUNCTION IsBankUBR( p_BankID IN NUMBER ) RETURN BOOLEAN;

  /**
   * Функция проверки является ли банк УБР
   * @param p_BankID Идентификатор банка
   * @return NUMBER 1 - Является УБР / 0 - не является УБР
   */
  FUNCTION IsBankUBR_Num( p_BankID IN NUMBER ) RETURN NUMBER;

  /**
   * Сравнить номер счета с маской
   * @param p_Mask маска
   * @param p_Account номер счета
   * @return 1 - если счет подходит под маску, 0 - если нет
   */
  FUNCTION CompareAccWithMask( p_Mask in varchar2, p_Account  in varchar2 ) RETURN INTEGER DETERMINISTIC;

  FUNCTION GetOperParticipantsFromRoute ( p_PaymentID IN NUMBER ) RETURN OperParticipants_t;

  /**
   * Получить наименование субъекта-носителя кода
   * @since 6.20.030
   * @param p_CodeKind    Вид кода
   * @param p_Code        Код
   * @param p_Date  Дата, на которую должен быть активен код, если null, то активный сейчас
   * @return наименование субъекта-носителя кода в случае успеха, PM_COMMON.RSB_EMPTY_STRING в случае ошибки (наименование или код не найдены)
   */
  function GetCodeOwnerName( p_CodeKind in integer, p_Code in varchar2, p_Date in date default null ) return dparty_dbt.t_Name%type;

  /**
   * Получить номер корреспондентского счета субъекта-носителя кода
   * @since 6.20.030
   * @param p_CodeKind    Вид кода
   * @param p_Code        Код
   * @param p_Date  Дата, на которую должен быть активен код, если null, то активный сейчас
   * @return номер корреспондентского счета субъекта-носителя кода в случае успеха, PM_COMMON.RSB_EMPTY_STRING в случае ошибки (наименование или код не найдены)
   */
  function GetCodeOwnerCorAcc( p_CodeKind in integer, p_Code in varchar2, p_Date in date default null ) return varchar2;

  /**
   * Получить код субъекта на указанную дату
   * @since 6.20.030
   * @param p_PartyID    Идентификатор субъекта
   * @param p_CodeKind    Вид кода
   * @param p_Date  Дата, на которую должен быть активен код, если null, то активный сейчас
   * @return код субъекта в случае успеха, PM_COMMON.RSB_EMPTY_STRING в случае ошибки (код не найден)
   */
  function GetPartyCodeOnDateWOutOwner( p_PartyID in number, p_CodeKind in number, p_Date in date default null ) return varchar2;

END PM_COMMON;
/
