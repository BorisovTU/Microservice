CREATE OR REPLACE PACKAGE RSI_NPTXC
IS
  ---------------------------------
  -- Author  : Makarov A.G.      --
  -- Created : 08 December 2010  --
  -- Descrip : Константы для НУ НДФЛ
  ---------------------------------
/**
 *   Предельное значение НОБ для расчета по основной ставке */
       BASE_MAX_15 NUMBER := 5000000;


/**
 * Константы DNPTXLOT_DBT */
/**
 *   Типы лотов */
       NPTXLOTS_UNDEF    CONSTANT NUMBER := 0; -- не определено
       NPTXLOTS_BUY      CONSTANT NUMBER := 1; -- Покупка
       NPTXLOTS_SALE     CONSTANT NUMBER := 2; -- Продажа
       NPTXLOTS_REPO     CONSTANT NUMBER := 3; -- Репо прямое
       NPTXLOTS_BACKREPO CONSTANT NUMBER := 4; -- Репо обратное
       NPTXLOTS_LOANPUT  CONSTANT NUMBER := 5; -- Займ размещение
       NPTXLOTS_LOANGET  CONSTANT NUMBER := 6; -- Займ привлечение

/**
 *   Тип сделки */
       NPTXDEAL_UNDEF       CONSTANT NUMBER := 0; -- не определено
       NPTXDEAL_REAL        CONSTANT NUMBER := 1; -- Реальная сделка
       NPTXDEAL_MARKET      CONSTANT NUMBER := 2; -- Виртуальная рыночного типа
       NPTXDEAL_CALC        CONSTANT NUMBER := 3; -- Виртуальная расчетная сделка
       NPTXDEAL_COMP        CONSTANT NUMBER := 4; -- Посткомпенсационный лот

/**
 *   Тип происхождения */
       NPTXLOTORIGIN_DEAL   CONSTANT NUMBER := 0; --Сделка
       NPTXLOTORIGIN_GO     CONSTANT NUMBER := 1; --ГО: Глобальная конвертация
       NPTXLOTORIGIN_IN     CONSTANT NUMBER := 2; --ИН: Изменение номинала

/**
 * Константы DNPTXLNK_DBT   */
/**
 *   Тип связи */
       NPTXLNK_UNDEF       CONSTANT NUMBER := 0;  -- не определено
       NPTXLNK_DELIVER     CONSTANT NUMBER := 1;  -- Поставка
       NPTXLNK_REPO        CONSTANT NUMBER := 2;  -- Прямое Репо
       NPTXLNK_SUBSTREPO   CONSTANT NUMBER := 3;  -- Подстановка Репо
       NPTXLNK_OPPOS       CONSTANT NUMBER := 4;  -- Открытие короткой позиции
       NPTXLNK_CLPOS       CONSTANT NUMBER := 5;  -- Закрытие короткой позиции

/**
 * Константы DNPTXTS_DBT   */
/**
 *   Тип связи */
       NPTXTS_UNDEF          CONSTANT NUMBER := 0;   -- не определено
       NPTXTS_REST           CONSTANT NUMBER := 1;   -- Остатки         
       NPTXTS_INVST          CONSTANT NUMBER := 2;   -- Размещения      
       NPTXTS_SHPOS          CONSTANT NUMBER := 3;   -- Короткая позиция
       NPTXTS_BUY            CONSTANT NUMBER := 4;   -- Покупки

/**
 *   Виды периода */
       NPTXCALC_UNDEF        CONSTANT NUMBER := 0;   -- не определено
       NPTXCALC_CALCLINKS    CONSTANT NUMBER := 1;   -- Расчет связей         
       NPTXCALC_CALCNDFL     CONSTANT NUMBER := 2;   -- Расчет НДФЛ
       NPTXCALC_CLOSE        CONSTANT NUMBER := 3;   -- Закрытие
       NPTXCALC_CALCLUCRE    CONSTANT NUMBER := 4;   -- Материальная выгода

/**
 *   Резидентность субъектов */
       NPTXPARTY_UNDEF       CONSTANT NUMBER := 0; -- не определено
       NPTXPARTY_RESIDENT    CONSTANT NUMBER := 1; -- для резидентов
       NPTXPARTY_NOTRESIDENT CONSTANT NUMBER := 2; -- для нерезидентов

/**
 *   Виды сообщений: */
       MES_INF   CONSTANT NUMBER :=  0; -- информационное сообщение
       MES_ERROR CONSTANT NUMBER := 10; -- ошибка
       MES_WARN  CONSTANT NUMBER := 20; -- предупреждение (некритичная ошибка)
       MES_DEBUG CONSTANT NUMBER := 30; -- отладочное сообщение

/**
 *   статусы nptxop */
       DL_TXOP_Prep  CONSTANT NUMBER := 0; -- отложен
       DL_TXOP_Open  CONSTANT NUMBER := 1; -- открыт
       DL_TXOP_Close CONSTANT NUMBER := 2; -- закрыт

/**
 *   Типы документов по операции */
       OBJTYPE_AVOIRISS CONSTANT NUMBER := 12;   -- ФИ
       DL_SECURITYDOC   CONSTANT NUMBER := 101;  -- Сделка с ценными бумагами
       DL_RETIREMENT    CONSTANT NUMBER := 117;  -- Погашение
       DL_AVRWRT        CONSTANT NUMBER := 127;  -- Списание/зачисление ц/б
       DL_CONVAVR       CONSTANT NUMBER := 138;  -- Конвертация ц/б
       DL_NTGDOC        CONSTANT NUMBER := 140;  -- Документ неттинга
       DL_CALCNDFL      CONSTANT NUMBER := 4605; -- Операция расчета НОБ для НДФЛ
       DL_HOLDNDFL      CONSTANT NUMBER := 4608; -- Операция удержания НДФЛ

/**
 *   Используемые в НДФЛ виды DLSUM */
       DLSUM_KIND_OUTLAY        CONSTANT NUMBER := 1100;  -- Затраты на приобретение
       DLSUM_KIND_PRICEWRTTAX   CONSTANT NUMBER := 1220;  -- Цена покупки в НУ
       DLSUM_KIND_COSTWRTTAX    CONSTANT NUMBER := 1230;  -- Стоимость покупки в НУ
       DLSUM_KIND_NKDWRTTAX     CONSTANT NUMBER := 1240;  -- НКД в НУ
       DLSUM_KIND_OUTLAYWRTTAX  CONSTANT NUMBER := 1250;  -- Затраты в НУ
       DLSUM_KIND_OTHERLUCRESUM CONSTANT NUMBER := 1260;  -- Сумма мат. выгоды у др. ПУ

/**
 *   Используемые в НДФЛ номера категорий бумаги  */
       TXAVR_ATTR_TXGROUP   CONSTANT NUMBER := 37; -- Группа налогового учета для НДФЛ
       TXAVR_ATTR_CIRCULATE CONSTANT NUMBER := 38; -- Обращается на ОРЦБ для целей НДФЛ
       TXAVR_ATTR_FAVOURINCOME CONSTANT NUMBER := 54; -- Льготное налогообложение

/**
 *   Группы налогового учета для НДФЛ */
       TXGROUP_10      CONSTANT VARCHAR2(35) := '10'; -- Акции 
       TXGROUP_20      CONSTANT VARCHAR2(35) := '20'; -- Облигации обыкновенные
       TXGROUP_30      CONSTANT VARCHAR2(35) := '30'; -- Облигации с ипотечным покрытием (ставка 9%)
       TXGROUP_40      CONSTANT VARCHAR2(35) := '40'; -- Облигации льготные (ставка 0% по ПКД) 
       TXGROUP_50      CONSTANT VARCHAR2(35) := '50'; -- Облигации льготные (ставка 0% по погашению и ПКД)
       TXGROUP_60      CONSTANT VARCHAR2(35) := '60'; -- Векселя
       TXGROUP_70      CONSTANT VARCHAR2(35) := '70'; -- Сертификаты
       TXGROUP_80      CONSTANT VARCHAR2(35) := '80'; -- ФИСС фондовые
       TXGROUP_90      CONSTANT VARCHAR2(35) := '90'; -- ФИСС не фондовые
       TXGROUP_100     CONSTANT VARCHAR2(35) := '100'; -- КСУ

/**
 *   Обращаемость на ОРЦБ для НДФЛ */
       NPTX_FI_CIRCULATE     CONSTANT VARCHAR2(35) := '1'; -- Обращается
       NPTX_FI_NOCIRCULATE   CONSTANT VARCHAR2(35) := '2'; -- Не обращается
       NPTX_FI_LOSTCIRCULATE CONSTANT VARCHAR2(35) := '3'; -- Потеряла обращаемость


/**
 *   Используемые в НДФЛ виды ФИ */
       FIKIND_AVOIRISS       CONSTANT NUMBER := 2; -- Ценная бумага
       FIKIND_DERIVATIVE     CONSTANT NUMBER := 4; -- Производные инструменты (ПИ)

/**
 * Значения настроек группы "НДФЛ"   */
/**
 *   Разрешить продажу блокированных приобретений */
       NPTXREG_W1_YES   CONSTANT NUMBER := 0; -- да
       NPTXREG_W1_NO    CONSTANT NUMBER := 1; -- нет

/**
 *   Проводить перетасовку для НДФЛ */
       NPTXREG_W2_YES   CONSTANT NUMBER := 0; -- да
       NPTXREG_W2_NO    CONSTANT NUMBER := 1; -- нет

/**
 *   Статусы ошибок */
       NPTX_ERROR_20601    CONSTANT INTEGER := -20601; --Уже существует документ данного вида по клиенту за дату операции
       NPTX_ERROR_20602    CONSTANT INTEGER := -20602; --Неверная дата операции
       NPTX_ERROR_20603    CONSTANT INTEGER := -20603; --Попытка выполнить расчет НДР в закрытом налоговом периоде
       NPTX_ERROR_20604    CONSTANT INTEGER := -20604; --Уже существует операция расчета НОД для НДФЛ за указанный период
       NPTX_ERROR_20605    CONSTANT INTEGER := -20605; --Попытка выполнить повторный расчет связей за период
       NPTX_ERROR_20606    CONSTANT INTEGER := -20606; --Попытка выполнить повторный расчет НОБ для НДФЛ за период
       NPTX_ERROR_20607    CONSTANT INTEGER := -20607; --Попытка выполнить повторное закрытие налогового периода
       NPTX_ERROR_20608    CONSTANT INTEGER := -20608; --Попытка выполнить расчет связей в закрытом налоговом периоде
       NPTX_ERROR_20609    CONSTANT INTEGER := -20609; --Попытка выполнить расчет НОБ для НДФЛ в закрытом налоговом периоде
       NPTX_ERROR_20610    CONSTANT INTEGER := -20610; --Попытка откатить не последний период расчета связей
       NPTX_ERROR_20611    CONSTANT INTEGER := -20611; --Попытка откатить не последний период расчета НДФЛ
       NPTX_ERROR_20612    CONSTANT INTEGER := -20612; --Попытка откатить не последний период закрытия налогового периода
       NPTX_ERROR_20613    CONSTANT INTEGER := -20613; --Неверный период расчета связей
       NPTX_ERROR_20614    CONSTANT INTEGER := -20614; --Неверный период расчета НДФЛ
       NPTX_ERROR_20615    CONSTANT INTEGER := -20615; --Неверный период закрытия налогового периода
       NPTX_ERROR_20616    CONSTANT INTEGER := -20616; --Попытка откатить расчет связей в закрытом налоговом периоде
       NPTX_ERROR_20617    CONSTANT INTEGER := -20617; --Попытка откатить расчет НДФЛ в закрытом налоговом периоде
       NPTX_ERROR_20618    CONSTANT INTEGER := -20618; --Виртуальных сделок не должно быть в дату конвертации
       NPTX_ERROR_20619    CONSTANT INTEGER := -20619; --Виртуальных сделок не должно быть в дату конвертации
       NPTX_ERROR_20620    CONSTANT INTEGER := -20620; --Некорректная дата начала записи в таблице состояний
       NPTX_ERROR_20621    CONSTANT INTEGER := -20621; --Отрицательный остаток в записи в таблице состояний
       NPTX_ERROR_20622    CONSTANT INTEGER := -20622; --Не найден лот
       NPTX_ERROR_20623    CONSTANT INTEGER := -20623; --Некорректая дата начала записи в таблице состояний
       NPTX_ERROR_20624    CONSTANT INTEGER := -20624; --Неверный диапазон дат
       NPTX_ERROR_20625    CONSTANT INTEGER := -20625; --Попытка отката расчета связей НДФЛ в периоде, по которому были рассчитаны налоги
       NPTX_ERROR_20626    CONSTANT INTEGER := -20626; --Частичный откат связи в текущей версии не предусмотрен
       NPTX_ERROR_20627    CONSTANT INTEGER := -20627; --Остаток в записи таблицы состояний должен быть положительным
       NPTX_ERROR_20628    CONSTANT INTEGER := -20628; --Не выполнен расчет НОБ для НДФЛ в конце календарного года
       NPTX_ERROR_20629    CONSTANT INTEGER := -20629; --Уже существует операция расчета НОД для НДФЛ по материальной выгоде за указанный период
       NPTX_ERROR_20630    CONSTANT INTEGER := -20630; --Попытка выполнить повторный расчет НОБ для НДФЛ по материальной выгоде за период
       NPTX_ERROR_20631    CONSTANT INTEGER := -20631; --Попытка выполнить расчет НОБ для НДФЛ по материальной выгоде в закрытом налоговом периоде
       NPTX_ERROR_20632    CONSTANT INTEGER := -20632; --Попытка откатить не последний период расчета НОБ для НДФЛ по материальной выгоде
       NPTX_ERROR_20633    CONSTANT INTEGER := -20633; --Неверный период расчета НОБ для НДФЛ по материальной выгоде
       NPTX_ERROR_20634    CONSTANT INTEGER := -20634; --Попытка откатить расчет НОБ для НДФЛ по материальной выгоде в закрытом налоговом периоде
       NPTX_ERROR_20635    CONSTANT INTEGER := -20635; --Не выполнен расчет НОБ для НДФЛ по материальной выгоде в конце календарного года
       NPTX_ERROR_20636    CONSTANT INTEGER := -20636; --Попытка вставки объекта НДР в закрытом налоговом периоде
       NPTX_ERROR_20637    CONSTANT INTEGER := -20637; --Попытка изменения объекта НДР в закрытом налоговом периоде
       NPTX_ERROR_20638    CONSTANT INTEGER := -20638; --Попытка удаления объекта НДР в закрытом налоговом периоде
       NPTX_ERROR_20639    CONSTANT INTEGER := -20639; --Запрещено создавать пользовательский объект НДР
       NPTX_ERROR_20640    CONSTANT INTEGER := -20640; --Попытка выполнить удержание НДФЛ в закрытом налоговом периоде
       NPTX_ERROR_20641    CONSTANT INTEGER := -20641; --Попытка откатить удержание НДФЛ в закрытом налоговом периоде
       NPTX_ERROR_20642    CONSTANT INTEGER := -20642; --Ошибка при сохранении записи в истории
       NPTX_ERROR_20644    CONSTANT INTEGER := -20644; --Не найдена запись события СНОБ
       NPTX_ERROR_20645    CONSTANT INTEGER := -20645; --Ошибка при сохранении истории события СНОБ
       NPTX_ERROR_20646    CONSTANT INTEGER := -20646; --Откатываемая операция не является последней для события СНОБ
       NPTX_ERROR_20647    CONSTANT INTEGER := -20647; --Недостаточно ц/б для списания лота
       NPTX_ERROR_20648    CONSTANT INTEGER := -20648; --Не заданы значения кодов субъектов СПБ или ММВБ

/**
 *   Подвиды операции расчета НОБ для НДФЛ */
       DL_TXBASECALC_OPTYPE_ENDYEAR  CONSTANT NUMBER := 10; --Окончание года              
       DL_TXBASECALC_OPTYPE_NORMAL   CONSTANT NUMBER := 20; --Обычный расчет
       DL_TXBASECALC_OPTYPE_LUCRE    CONSTANT NUMBER := 30; --Материальная выгода
       DL_TXBASECALC_OPTYPE_CLOSE_IIS CONSTANT NUMBER := 40; -- Закрытие ИИС
       DL_TXBASECALC_OPTYPE_DIVIDEND  CONSTANT NUMBER := 50;  -- НОБ по дивидендам

/**
 *   Подвиды операции удержания НДФЛ */
       DL_TXHOLD_OPTYPE_ENDYEAR  CONSTANT NUMBER := 10; -- Окончание года
       DL_TXHOLD_OPTYPE_OUTMONEY CONSTANT NUMBER := 20; -- Вывод д/с
       DL_TXHOLD_OPTYPE_OUTAVOIR CONSTANT NUMBER := 30; -- Вывод ц/б
       DL_TXHOLD_OPTYPE_LUCRE    CONSTANT NUMBER := 40; -- Материальная выгода
       DL_TXHOLD_OPTYPE_CLOSE    CONSTANT NUMBER := 50; -- Закрытие договора


/**
 *   Типы функций, которые будут вызываться при вставке объектов */
       DL_INSERTTAXOBJECT   CONSTANT NUMBER := 1;
       DL_INSERTTAXOBJECTWD CONSTANT NUMBER := 2;

       NPTX_ENDDATE2011     CONSTANT DATE := TO_DATE('31.12.2011','DD.MM.YYYY');

/**
 *   Константы видов аналитик объектов НДР */
       TXOBJ_KIND1010 CONSTANT NUMBER := 1010; -- Сделка
       TXOBJ_KIND1020 CONSTANT NUMBER := 1020; -- Погашение
       TXOBJ_KIND1030 CONSTANT NUMBER := 1030; -- Конвертация
       TXOBJ_KIND1040 CONSTANT NUMBER := 1040; -- Расчет по ПИ
       TXOBJ_KIND1050 CONSTANT NUMBER := 1050; -- Номер купона
       TXOBJ_KIND1060 CONSTANT NUMBER := 1060; -- Номер ЧП
       TXOBJ_KIND1070 CONSTANT NUMBER := 1070; -- Зачисление/списание
       TXOBJ_KIND1080 CONSTANT NUMBER := 1080; -- Виртуальная        
       TXOBJ_KIND1090 CONSTANT NUMBER := 1090; -- Сделка ПИ
       TXOBJ_KIND1095 CONSTANT NUMBER := 1095; -- Купон в депозитарии
       TXOBJ_KIND1098 CONSTANT NUMBER := 1098; -- Дивиденды
       TXOBJ_KIND1100 CONSTANT NUMBER := 1100; -- Возврат дивидендов
       TXOBJ_KIND1110 CONSTANT NUMBER := 1110; -- Биржевая сделка ПИ
       TXOBJ_KIND1115 CONSTANT NUMBER := 1115; -- Выкуп собственных векселей банка
       TXOBJ_KIND1120 CONSTANT NUMBER := 1120; -- Мена собственных векселей банка
       TXOBJ_KIND1125 CONSTANT NUMBER := 1125; -- Зачет взаимных требований
       TXOBJ_KIND1130 CONSTANT NUMBER := 1130; -- Погашение векселя
       TXOBJ_KIND1135 CONSTANT NUMBER := 1135; -- Соглашение об урегулировании претензий
       TXOBJ_KIND2010 CONSTANT NUMBER := 2010; -- Связь
       TXOBJ_KIND2020 CONSTANT NUMBER := 2020; -- ТС
       TXOBJ_KIND2030 CONSTANT NUMBER := 2030; -- Номер купона
       TXOBJ_KIND2040 CONSTANT NUMBER := 2040; -- КБК
       TXOBJ_KIND3010 CONSTANT NUMBER := 3010; -- ЦБ
       TXOBJ_KIND3020 CONSTANT NUMBER := 3020; -- ПИ
       TXOBJ_KIND4010 CONSTANT NUMBER := 4010; -- Категория ФИ
       TXOBJ_KIND5010 CONSTANT NUMBER := 5010; -- Налоговая группа
       TXOBJ_KIND6010 CONSTANT NUMBER := 6010; -- Договор ДУ
       TXOBJ_KIND6020 CONSTANT NUMBER := 6020; -- Договор обслуживания     
       TXOBJ_KIND6030 CONSTANT NUMBER := 6030; -- Закрытие последнего ДБО   


/**
 *   Константы направлений */
      TXOBJ_DIR_ALL CONSTANT NUMBER := 0; -- Не задано
      TXOBJ_DIR_IN  CONSTANT NUMBER := 1; -- Доход 
      TXOBJ_DIR_OUT CONSTANT NUMBER := 2; -- Расход


/**
 *   Константы видов действий для истории сущностей НДФЛ */
       NPTXBC_ACTION_CREATE CONSTANT NUMBER := 0; --Создание
       NPTXBC_ACTION_UPDATE CONSTANT NUMBER := 1; --Обновление
       NPTXBC_ACTION_DELETE CONSTANT NUMBER := 2; --Удаление

/**
 *   Константы видов объектов для истории сущностей НДФЛ */
       NPTXBC_OBJKIND_LOT  CONSTANT NUMBER := 1; --Лот
       NPTXBC_OBJKIND_LNK  CONSTANT NUMBER := 2; --Связь
       NPTXBC_OBJKIND_LS   CONSTANT NUMBER := 3; --Перевешивание связи
       NPTXBC_OBJKIND_TS   CONSTANT NUMBER := 4; --Состояние лота
       NPTXBC_OBJKIND_GO   CONSTANT NUMBER := 5; --Глобальная операция
       NPTXBC_OBJKIND_GOFI CONSTANT NUMBER := 6; --Выпуск по ГО
       NPTXBC_OBJKIND_OBJ  CONSTANT NUMBER := 7; --Объект НДР

/**
 * Способы удержания НДФЛ */
      DL_TYPEGETNALOG_ACCOUNT  CONSTANT NUMBER := 10; -- "Со счета обслуживания"
      DL_TYPEGETNALOG_OTHERACC CONSTANT NUMBER := 20; -- "Со стороннего счета"


/**
 * Виды объектов НДР */
      TXOBJ_MAIN               CONSTANT NUMBER := 10;
      TXOBJ_NKD                CONSTANT NUMBER := 20;
      TXOBJ_COM                CONSTANT NUMBER := 30;
      TXOBJ_MATERIAL           CONSTANT NUMBER := 40;
      TXOBJ_DIF                CONSTANT NUMBER := 50;
      TXOBJ_VAR                CONSTANT NUMBER := 60;
      TXOBJ_PREM               CONSTANT NUMBER := 70;
      TXOBJ_PAY                CONSTANT NUMBER := 80;
      TXOBJ_PROCREPOPAY        CONSTANT NUMBER := 90;
      TXOBJ_FR_DERIV           CONSTANT NUMBER := 100;
      TXOBJ_MAINB              CONSTANT NUMBER := 110;
      TXOBJ_MAINB_0            CONSTANT NUMBER := 115;
      TXOBJ_NKDB               CONSTANT NUMBER := 120;
      TXOBJ_COMB               CONSTANT NUMBER := 130;
      TXOBJ_COMB_0             CONSTANT NUMBER := 135;
      TXOBJ_MATERIALB          CONSTANT NUMBER := 140;
      TXOBJ_TAXPM_LINK         CONSTANT NUMBER := 150;
      TXOBJ_DIFB               CONSTANT NUMBER := 160;
      TXOBJ_MAINS              CONSTANT NUMBER := 170;
      TXOBJ_MAINS_0            CONSTANT NUMBER := 175;
      TXOBJ_NKDS               CONSTANT NUMBER := 180;
      TXOBJ_COMS               CONSTANT NUMBER := 190;
      TXOBJ_COMS_0             CONSTANT NUMBER := 195;
      TXOBJ_DIFS               CONSTANT NUMBER := 200;
      TXOBJ_MAINB_TS           CONSTANT NUMBER := 210;
      TXOBJ_MAINB_TS_0         CONSTANT NUMBER := 215;
      TXOBJ_COMB_TS            CONSTANT NUMBER := 220;
      TXOBJ_COMB_TS_0          CONSTANT NUMBER := 225;
      TXOBJ_MATERIALB_TS       CONSTANT NUMBER := 230;
      TXOBJ_TAXPM_TS           CONSTANT NUMBER := 240;
      TXOBJ_DIFB_TS            CONSTANT NUMBER := 250;
      TXOBJ_MAINS_TS           CONSTANT NUMBER := 260;
      TXOBJ_MAINS_TS_0         CONSTANT NUMBER := 265;
      TXOBJ_NKDS_TS            CONSTANT NUMBER := 270;
      TXOBJ_COMS_TS            CONSTANT NUMBER := 280;
      TXOBJ_COMS_TS_0          CONSTANT NUMBER := 285;
      TXOBJ_DIFS_TS            CONSTANT NUMBER := 290;
      TXOBJ_DISCB              CONSTANT NUMBER := 300;
      TXOBJ_DISCS              CONSTANT NUMBER := 310;
      TXOBJ_DISCS_0            CONSTANT NUMBER := 315;
      TXOBJ_DISCOUNT_LINK      CONSTANT NUMBER := 320;
      TXOBJ_PROCFACT           CONSTANT NUMBER := 330;
      TXOBJ_COST               CONSTANT NUMBER := 340;
      TXOBJ_PROCNORM           CONSTANT NUMBER := 350;
      TXOBJ_PROCSEC_Link       CONSTANT NUMBER := 360;
      TXOBJ_PROCSEC_TS         CONSTANT NUMBER := 370;
      TXOBJ_PROCSEC_TS_35      CONSTANT NUMBER := 375;
      TXOBJ_FR_LINK            CONSTANT NUMBER := 380;
      TXOBJ_TAX_LINK           CONSTANT NUMBER := 385;
      TXOBJ_TAX_LINK_0         CONSTANT NUMBER := 386;
      TXOBJ_FR_TS              CONSTANT NUMBER := 390;
      TXOBJ_PROCREPOTAX        CONSTANT NUMBER := 400;
      TXOBJ_PLUSI              CONSTANT NUMBER := 402;
      TXOBJ_FR_SEC_3Y          CONSTANT NUMBER := 407;
      TXOBJ_FR_SEC             CONSTANT NUMBER := 410;
      TXOBJ_EXP_SEC            CONSTANT NUMBER := 420;
      TXOBJ_PROC_SEC           CONSTANT NUMBER := 430;
      TXOBJ_PROC_SEC_SHORT     CONSTANT NUMBER := 435;
      TXOBJ_MATERIAL_SEC       CONSTANT NUMBER := 450;
      TXOBJ_MINUS_SEC          CONSTANT NUMBER := 460;
      TXOBJ_MINUS_SEC_SHORT    CONSTANT NUMBER := 461;
      TXOBJ_PLUS_SEC           CONSTANT NUMBER := 470;
      TXOBJ_PLUS_SEC_SHORT     CONSTANT NUMBER := 471;
      TXOBJ_DIV_SEC            CONSTANT NUMBER := 480;
      TXOBJ_DIVPAY_SEC         CONSTANT NUMBER := 490;
      TXOBJ_OTHER_SEC          CONSTANT NUMBER := 510;
      TXOBJ_GENERAL            CONSTANT NUMBER := 520;
      TXOBJ_GENERAL_IIS        CONSTANT NUMBER := 525;
      TXOBJ_PREVIOUSYEAR       CONSTANT NUMBER := 530;
      TXOBJ_PLUS_SEC0          CONSTANT NUMBER := 535;
      TXOBJ_MINUS_SEC0         CONSTANT NUMBER := 536;
      TXOBJ_DIVPAY_SEC_0       CONSTANT NUMBER := 537;
      TXOBJ_EXP_GROUP          CONSTANT NUMBER := 540;
      TXOBJ_COUP0_GROUP        CONSTANT NUMBER := 550;
      TXOBJ_PLUS9_1110         CONSTANT NUMBER := 560;
      TXOBJ_PLUS9_1110_IIS     CONSTANT NUMBER := 565;
      TXOBJ_PLUSG_1110         CONSTANT NUMBER := 570;
      TXOBJ_PLUSG_1010         CONSTANT NUMBER := 580;
      TXOBJ_PLUS15_1010        CONSTANT NUMBER := 590;
      TXOBJ_PLUSG_1011         CONSTANT NUMBER := 600;
      TXOBJ_PLUSG_1011_1       CONSTANT NUMBER := 601;
      TXOBJ_PLUSG_1011_2       CONSTANT NUMBER := 602;
      TXOBJ_PLUSG_1011_1_IIS   CONSTANT NUMBER := 603;
      TXOBJ_PLUSG_1011_2_IIS   CONSTANT NUMBER := 604;
      TXOBJ_PLUSG_1011_IIS     CONSTANT NUMBER := 605;
      TXOBJ_PLUSG_1011_3       CONSTANT NUMBER := 606;
      TXOBJ_PLUSG_1011_3_IIS   CONSTANT NUMBER := 607;
      TXOBJ_PLUSG_1530         CONSTANT NUMBER := 610;
      TXOBJ_PLUSG_1544         CONSTANT NUMBER := 615;
      TXOBJ_PLUSG_1531         CONSTANT NUMBER := 620;
      TXOBJ_PLUSG_1533         CONSTANT NUMBER := 625;
      TXOBJ_PLUSG_1547         CONSTANT NUMBER := 626;
      TXOBJ_PLUSG_1545         CONSTANT NUMBER := 623;
      TXOBJ_PLUSG_1536         CONSTANT NUMBER := 630;
      TXOBJ_PLUSG_1537         CONSTANT NUMBER := 631;
      TXOBJ_PLUSG_1539         CONSTANT NUMBER := 632;
      TXOBJ_PLUSG_1549         CONSTANT NUMBER := 633;
      TXOBJ_PLUSG_1551         CONSTANT NUMBER := 634;
      TXOBJ_PLUSG_1553         CONSTANT NUMBER := 635;
      TXOBJ_PLUS35_3023        CONSTANT NUMBER := 636;
      TXOBJ_PLUSG_1532         CONSTANT NUMBER := 640;
      TXOBJ_PLUSG_1546         CONSTANT NUMBER := 645;
      TXOBJ_PLUSG_1535         CONSTANT NUMBER := 650;
      TXOBJ_PLUSG_1548         CONSTANT NUMBER := 655;
      TXOBJ_PLUSG_2640         CONSTANT NUMBER := 660;
      TXOBJ_PLUSG_2640_IIS     CONSTANT NUMBER := 665;
      TXOBJ_PLUSG_2641         CONSTANT NUMBER := 670;
      TXOBJ_PLUSG_2641_IIS     CONSTANT NUMBER := 671;
      TXOBJ_MINUSG_601         CONSTANT NUMBER := 680;
      TXOBJ_MINUS15_601        CONSTANT NUMBER := 690;
      TXOBJ_MINUSG_201         CONSTANT NUMBER := 700;
      TXOBJ_MINUSG_225         CONSTANT NUMBER := 705;
      TXOBJ_MINUSG_202         CONSTANT NUMBER := 710;
      TXOBJ_MINUSG_226         CONSTANT NUMBER := 715;
      TXOBJ_MINUSG_203         CONSTANT NUMBER := 720;
      TXOBJ_MINUSG_227         CONSTANT NUMBER := 725;
      TXOBJ_MINUSG_206         CONSTANT NUMBER := 730;
      TXOBJ_MINUSG_228         CONSTANT NUMBER := 735;
      TXOBJ_MINUSG_229         CONSTANT NUMBER := 739;
      TXOBJ_MINUSG_207         CONSTANT NUMBER := 740;
      TXOBJ_MINUSG_211         CONSTANT NUMBER := 741;
      TXOBJ_MINUSG_212         CONSTANT NUMBER := 742;
      TXOBJ_MINUSG_213         CONSTANT NUMBER := 743;
      TXOBJ_MINUSG_214         CONSTANT NUMBER := 744;
      TXOBJ_MINUSG_620_3       CONSTANT NUMBER := 745;
      TXOBJ_MINUSG_620_2       CONSTANT NUMBER := 746;
      TXOBJ_MINUSG_230         CONSTANT NUMBER := 747;
      TXOBJ_MINUSG_231         CONSTANT NUMBER := 748;
      TXOBJ_MINUSG_214_IIS     CONSTANT NUMBER := 749;
      TXOBJ_FULLMINUS_201      CONSTANT NUMBER := 750;
      TXOBJ_FULLMINUS_225      CONSTANT NUMBER := 755;
      TXOBJ_FULLMINUS_203      CONSTANT NUMBER := 760;
      TXOBJ_FULLMINUS_227      CONSTANT NUMBER := 765;
      TXOBJ_FULLMINUS_206      CONSTANT NUMBER := 770;
      TXOBJ_FULLMINUS_228      CONSTANT NUMBER := 775;
      TXOBJ_FULLMINUS_207      CONSTANT NUMBER := 780;
      TXOBJ_FULLMINUS_229      CONSTANT NUMBER := 781;
      TXOBJ_FULLMINUS_618      CONSTANT NUMBER := 785;
      TXOBJ_MINUSG_204         CONSTANT NUMBER := 790;
      TXOBJ_MINUSG_205         CONSTANT NUMBER := 800;
      TXOBJ_MINUSG_250         CONSTANT NUMBER := 805;
      TXOBJ_MINUSG_208         CONSTANT NUMBER := 810;
      TXOBJ_MINUSG_251         CONSTANT NUMBER := 815;
      TXOBJ_MINUSG_209         CONSTANT NUMBER := 820;
      TXOBJ_MINUSG_210         CONSTANT NUMBER := 821;
      TXOBJ_MINUSG_620_1       CONSTANT NUMBER := 822;
      TXOBJ_MINUSG_252         CONSTANT NUMBER := 823;
      TXOBJ_MINUSG_241         CONSTANT NUMBER := 824;
      TXOBJ_MINUSG_224         CONSTANT NUMBER := 825;
      TXOBJ_MINUSG_236         CONSTANT NUMBER := 826;
      TXOBJ_LIMIT_618          CONSTANT NUMBER := 827;
      TXOBJ_MINUSG_618         CONSTANT NUMBER := 829;
      TXOBJ_BASEMATERIAL       CONSTANT NUMBER := 830;
      TXOBJ_BASEMATERIAL_IIS   CONSTANT NUMBER := 835;
      TXOBJ_BASEGENERAL        CONSTANT NUMBER := 840;
      TXOBJ_BASEGENERAL_IIS    CONSTANT NUMBER := 845;
      TXOBJ_BASESPECIAL        CONSTANT NUMBER := 850;
      TXOBJ_BASESPECIAL_IIS    CONSTANT NUMBER := 855;
      TXOBJ_BASE_35            CONSTANT NUMBER := 856;
      TXOBJ_BASESPECIAL_DIV    CONSTANT NUMBER := 851;
      TXOBJ_PAIDMATERIAL       CONSTANT NUMBER := 860;
      TXOBJ_PAIDMATERIAL_IIS   CONSTANT NUMBER := 865;
      TXOBJ_PAIDGENERAL        CONSTANT NUMBER := 870;
      TXOBJ_PAIDGENERAL_IIS    CONSTANT NUMBER := 875;
      TXOBJ_PAIDSPECIAL        CONSTANT NUMBER := 880;
      TXOBJ_PAID_35            CONSTANT NUMBER := 881;
      TXOBJ_PAIDSPECIAL_IIS    CONSTANT NUMBER := 885;
      TXOBJ_TAXPM              CONSTANT NUMBER := 890;
      TXOBJ_MAINB_SHORT        CONSTANT NUMBER := 900;
      TXOBJ_NKDB_SHORT         CONSTANT NUMBER := 910;
      TXOBJ_COMB_SHORT         CONSTANT NUMBER := 920;
      TXOBJ_MATERIALB_SHORT    CONSTANT NUMBER := 930;
      TXOBJ_TAXPM_LINK_SHORT   CONSTANT NUMBER := 940;
      TXOBJ_DIFB_SHORT         CONSTANT NUMBER := 950;
      TXOBJ_MAINS_SHORT        CONSTANT NUMBER := 960;
      TXOBJ_NKDS_SHORT         CONSTANT NUMBER := 970;
      TXOBJ_COMS_SHORT         CONSTANT NUMBER := 980;
      TXOBJ_DIFS_SHORT         CONSTANT NUMBER := 990;
      TXOBJ_MINUSG_222         CONSTANT NUMBER := 1000;
      TXOBJ_MINUSG_239         CONSTANT NUMBER := 1005;
      TXOBJ_MINUSG_223         CONSTANT NUMBER := 1010;
      TXOBJ_MINUSG_240         CONSTANT NUMBER := 1015;
      TXOBJ_MINUSG_218         CONSTANT NUMBER := 1020;
      TXOBJ_MINUSG_233         CONSTANT NUMBER := 1025;
      TXOBJ_MINUSG_219         CONSTANT NUMBER := 1030;
      TXOBJ_MINUSG_234         CONSTANT NUMBER := 1035;
      TXOBJ_MINUS9_218         CONSTANT NUMBER := 1040;
      TXOBJ_MINUS9_233         CONSTANT NUMBER := 1045;
      TXOBJ_MINUS9_219         CONSTANT NUMBER := 1050;
      TXOBJ_MINUS9_234         CONSTANT NUMBER := 1055;
      TXOBJ_MINUSG_218_1       CONSTANT NUMBER := 1060;
      TXOBJ_MINUSG_219_1       CONSTANT NUMBER := 1070;
      TXOBJ_MINUSG_220         CONSTANT NUMBER := 1080;
      TXOBJ_MINUSG_235         CONSTANT NUMBER := 1085;
      TXOBJ_FULLMINUS_221      CONSTANT NUMBER := 1090;
      TXOBJ_FULLPLUS_1543      CONSTANT NUMBER := 1100;
      TXOBJ_MINUSG_619         CONSTANT NUMBER := 1110;
      TXOBJ_DUETAX             CONSTANT NUMBER := 1111;
      TXOBJ_NKDB_TS            CONSTANT NUMBER := 1115;
      TXOBJ_FR_TS_0            CONSTANT NUMBER := 1120;
      TXOBJ_PROCBILL           CONSTANT NUMBER := 1140;
      TXOBJ_PLUSG_2800         CONSTANT NUMBER := 1141;
      TXOBJ_BASEBILL           CONSTANT NUMBER := 1142;
      TXOBJ_PAIDBILL           CONSTANT NUMBER := 1143;
      TXOBJ_PAIDGENERAL_0      CONSTANT NUMBER := 1144;
      TXOBJ_PAIDSPECIAL_0      CONSTANT NUMBER := 1145;
      TXOBJ_PAID35_0           CONSTANT NUMBER := 1146;
      TXOBJ_PAIDGENERAL_15     CONSTANT NUMBER := 1150;
      TXOBJ_PAIDGENERAL_15_IIS CONSTANT NUMBER := 1151;
      TXOBJ_BASEG1             CONSTANT NUMBER := 1152;
      TXOBJ_BASEG2             CONSTANT NUMBER := 1153;
      TXOBJ_BASEG3             CONSTANT NUMBER := 1154;
      TXOBJ_BASEG4             CONSTANT NUMBER := 1155;
      TXOBJ_BASEG5             CONSTANT NUMBER := 1156;
      TXOBJ_BASEG6             CONSTANT NUMBER := 1157;
      TXOBJ_BASEG7             CONSTANT NUMBER := 1158;
      TXOBJ_BASEG8             CONSTANT NUMBER := 1159;
      TXOBJ_BASEG9             CONSTANT NUMBER := 1160;
      TXOBJ_PAIDGENERAL_15_1   CONSTANT NUMBER := 1161;
      TXOBJ_PAIDGENERAL_15_2   CONSTANT NUMBER := 1162;
      TXOBJ_PAIDGENERAL_15_9   CONSTANT NUMBER := 1163;
      TXOBJ_PAIDGENERAL_15_1_0 CONSTANT NUMBER := 1164;
      TXOBJ_BASESPECIAL_DIV_15 CONSTANT NUMBER := 1165;
      TXOBJ_BASEG1_13          CONSTANT NUMBER := 1166;
      TXOBJ_BASEG1_15          CONSTANT NUMBER := 1167;
      TXOBJ_PLUSG_4800         CONSTANT NUMBER := 1170;
      TXOBJ_PAIDCOMP           CONSTANT NUMBER := 1171;
      TXOBJ_PAIDCOMP_15        CONSTANT NUMBER := 1172;
      TXOBJ_FULLMINUS_233      CONSTANT NUMBER := 1173;
      TXOBJ_FULLMINUS_239      CONSTANT NUMBER := 1174;
      TXOBJ_FULLMINUS_226      CONSTANT NUMBER := 1175;
      TXOBJ_FULLMINUS_234      CONSTANT NUMBER := 1176;
      TXOBJ_FULLMINUS_240      CONSTANT NUMBER := 1177;
      TXOBJ_FULLMINUS_236      CONSTANT NUMBER := 1178;
      TXOBJ_FULLMINUS_235      CONSTANT NUMBER := 1179;
      TXOBJ_FULLMINUS_230      CONSTANT NUMBER := 1180;
      TXOBJ_FULLMINUS_231      CONSTANT NUMBER := 1181;
      TXOBJ_PAIDGENERAL_18_9   CONSTANT NUMBER := 1182;
      TXOBJ_PAIDGENERAL_20_9   CONSTANT NUMBER := 1183;
      TXOBJ_PAIDGENERAL_22_9   CONSTANT NUMBER := 1184;
      TXOBJ_PROCSEC_CHANGE     CONSTANT NUMBER := 1185;
      TXOBJ_MINUSG_517         CONSTANT NUMBER := 1186;
      TXOBJ_FULLMINUS_517      CONSTANT NUMBER := 1187;
      TXOBJ_LIMIT_517          CONSTANT NUMBER := 1188;
      TXOBJ_DUEMATERIAL        CONSTANT NUMBER := 1189;
      TXOBJ_BASECOMP           CONSTANT NUMBER := 1190;

/*
 * Статус записи в Хранилище */
   NPTXTOTALBASE_STORSTATE_CANCELED CONSTANT NUMBER := 0; --Отмененная
   NPTXTOTALBASE_STORSTATE_ACTIVE   CONSTANT NUMBER := 1; --Активная

/*
 * Статус подтверждения записи в Хранилище */
   NPTXTOTALBASE_CONFIRMSTATE_NOTCONFIRMED     CONSTANT NUMBER := 0; --Не подтверждена
   NPTXTOTALBASE_CONFIRMSTATE_CONFIRMED        CONSTANT NUMBER := 1; --Подтверждена
   NPTXTOTALBASE_CONFIRMSTATE_SHIPMENTCANCELED CONSTANT NUMBER := 2; --Отправка отменена

/*
 * Коды КБК */
   NPTXKBK_MAIN CONSTANT VARCHAR2(20) :=   '18210102010011000110';
   NPTXKBK_PROC CONSTANT VARCHAR2(20) :=   '18210102070011000110';
   NPTXKBK_INCR CONSTANT VARCHAR2(20) :=   '18210102080011000110';
   NPTXKBK_INCR18 CONSTANT VARCHAR2(20) := '18210102150011000110';
   NPTXKBK_INCR20 CONSTANT VARCHAR2(20) := '18210102160011000110';
   NPTXKBK_INCR22 CONSTANT VARCHAR2(20) := '18210102170011000110';


/*
 * Вид НОБ OBJTYPE_STB_TAXBASEKIND */
   NPTXTOTALBASE_TAXBASEKIND_DIVID CONSTANT NUMBER := 1; --ДИВИД
   NPTXTOTALBASE_TAXBASEKIND_BROK  CONSTANT NUMBER := 2; --БРОК
   NPTXTOTALBASE_TAXBASEKIND_REPO  CONSTANT NUMBER := 3; --РЕПО
   NPTXTOTALBASE_TAXBASEKIND_LOAN  CONSTANT NUMBER := 4; --ЗАЙМЦБ
   NPTXTOTALBASE_TAXBASEKIND_IIS   CONSTANT NUMBER := 5; --ИИС
   NPTXTOTALBASE_TAXBASEKIND_ONB   CONSTANT NUMBER := 6;  --ОНБ

   NPTXCIRCRECALC_STATUS_ORCB_LISTED             CONSTANT NUMBER := 1; -- Значение категории "Является обращающейся на ОРЦБ для целей НДФЛ"
   NPTXCIRCRECALC_STATUS_RUS_EXCHANGE_QUOTE      CONSTANT NUMBER := 2; -- Есть котировка рос. биржи, допущена к торгам на рос. ОРЦБ
   NPTXCIRCRECALC_STATUS_FOREIGN_EXCHANGE_QUOTE  CONSTANT NUMBER := 3; -- Есть котировка ин. биржи, допущена к торгам на рос. ОРЦБ
   NPTXCIRCRECALC_STATUS_FOREIGN_EXCHANGE_ONLY   CONSTANT NUMBER := 4; -- Есть котировка ин. биржи, не допущена к торгам на рос. ОРЦБ
   NPTXCIRCRECALC_STATUS_NO_SUITABLE_QUOTES      CONSTANT NUMBER := 5; -- Не найдены подходящие котировки

END RSI_NPTXC;
/
