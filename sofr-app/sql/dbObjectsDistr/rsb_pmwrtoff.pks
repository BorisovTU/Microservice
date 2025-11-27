CREATE OR REPLACE PACKAGE RSB_PMWRTOFF IS

   UnknownValue CONSTANT NUMBER := -1;
   UnknownParty CONSTANT NUMBER := -1;
   UnknownDate  CONSTANT DATE   := TO_DATE( '01.01.0001', 'DD.MM.YYYY' );
   UnknownTime  CONSTANT DATE   := TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS');

/**
 * Статусы ошибок */
   WRTOFF_ERROR_20200    CONSTANT INTEGER := -20200; -- Не найден лот
   WRTOFF_ERROR_20201    CONSTANT INTEGER := -20201; -- Для лота %s есть операции за более позднюю дату. см. -20220
   WRTOFF_ERROR_20202    CONSTANT INTEGER := -20202; -- Операция выполнила данное изменение более ранней датой
   WRTOFF_ERROR_20203    CONSTANT INTEGER := -20203; -- Операция уже выполнила данное изменение
   WRTOFF_ERROR_20204    CONSTANT INTEGER := -20204; -- По лоту нет архивных данных
   WRTOFF_ERROR_20205    CONSTANT INTEGER := -20205; -- Ошибка при расчете начисленного дохода.
   WRTOFF_ERROR_20206    CONSTANT INTEGER := -20206; -- Ошибка лот не поставлен.
   WRTOFF_ERROR_20207    CONSTANT INTEGER := -20207; -- Не найден метод списания для субъекта
   WRTOFF_ERROR_20208    CONSTANT INTEGER := -20208; -- Недостаточно ц/б для продажи
   WRTOFF_ERROR_20209    CONSTANT INTEGER := -20209; -- По выпуску не начисляется доход
   WRTOFF_ERROR_20210    CONSTANT INTEGER := -20210; -- По выпуску не начисляется процентный доход
   WRTOFF_ERROR_20211    CONSTANT INTEGER := -20211; -- Не указан лот по 1 ч. сделки
   WRTOFF_ERROR_20212    CONSTANT INTEGER := -20212; -- Некорректный лот по 1 ч.
   WRTOFF_ERROR_20213    CONSTANT INTEGER := -20213; -- По лоту по 1 ч. есть операции за более позднюю дату
   WRTOFF_ERROR_20214    CONSTANT INTEGER := -20214; -- Лот по 1 ч. не поставлен
   WRTOFF_ERROR_20215    CONSTANT INTEGER := -20215; -- не списан ОД
   WRTOFF_ERROR_20216    CONSTANT INTEGER := -20216; -- Неверное количество в ОД
   WRTOFF_ERROR_20217    CONSTANT INTEGER := -20217; -- Заданы неверные суммы
   WRTOFF_ERROR_20218    CONSTANT INTEGER := -20218; -- Не найден лот 2 части
   WRTOFF_ERROR_20219    CONSTANT INTEGER := -20219; -- Доход на дату уже начислен
   WRTOFF_ERROR_20220    CONSTANT INTEGER := -20220; -- Для лотов есть операции за более позднюю дату. см. -20201
   WRTOFF_ERROR_20221    CONSTANT INTEGER := -20221; -- По лотам старого выпуска есть операции за более позднюю дату
   WRTOFF_ERROR_20222    CONSTANT INTEGER := -20222; -- Появился лот старого выпуска, не занесенный в список корректируемых лотов
   WRTOFF_ERROR_20223    CONSTANT INTEGER := -20223; -- Лоты старого выпуска изменились с момента составления списка корректируемых лотов
   WRTOFF_ERROR_20224    CONSTANT INTEGER := -20224; -- В портфеле %s списываются все ц/б
   WRTOFF_ERROR_20225    CONSTANT INTEGER := -20225; -- Премия на дату уже начислена
   WRTOFF_ERROR_20226    CONSTANT INTEGER := -20226; -- Повторный перерасчет дисконта/премии при перемещении запрещен.
   WRTOFF_ERROR_20227    CONSTANT INTEGER := -20227; -- Появился непоставленный лот старого выпуска, не занесенный в список корректируемых лотов
   WRTOFF_ERROR_20228    CONSTANT INTEGER := -20228; -- Появился поставленный лот старого выпуска, не занесенный в список корректируемых лотов
   WRTOFF_ERROR_20229    CONSTANT INTEGER := -20229; -- Не выполнено начисление процентного дохода на дату списания
   WRTOFF_ERROR_20230    CONSTANT INTEGER := -20230; -- Не выполнено начисление дисконтного дохода на дату списания
   WRTOFF_ERROR_20231    CONSTANT INTEGER := -20231; -- При списании по средневзвешенной остались нераспределенные суммы
   WRTOFF_ERROR_20232    CONSTANT INTEGER := -20232; -- Неверные параметры
   WRTOFF_ERROR_20233    CONSTANT INTEGER := -20233; -- Не выполнено начисление премии на дату списания
   WRTOFF_ERROR_20234    CONSTANT INTEGER := -20234; -- Ошибка отката операции
   WRTOFF_ERROR_20235    CONSTANT INTEGER := -20235; -- Ошибка отката операции по лоту %s
   WRTOFF_ERROR_20236    CONSTANT INTEGER := -20236; -- Откат операции заданного вида не поддерживается
   WRTOFF_ERROR_20237    CONSTANT INTEGER := -20237; -- Недопустимый порядок операций для отката
   WRTOFF_ERROR_20238    CONSTANT INTEGER := -20238; -- Ошибка при распределении сумм при создании лотов 2ч прямого РЕПО
   WRTOFF_ERROR_20239    CONSTANT INTEGER := -20239; -- Неверные данные при начислении дохода
   WRTOFF_ERROR_20240    CONSTANT INTEGER := -20240; -- Откатываемая операция по лоту не является последней
   WRTOFF_ERROR_20241    CONSTANT INTEGER := -20241; -- Ошибка отката обработки лотов. Неизвестное действие с лотами
   WRTOFF_ERROR_20242    CONSTANT INTEGER := -20242; -- Списны не все лоты конвертируемого выпуска
   WRTOFF_ERROR_20243    CONSTANT INTEGER := -20243; -- Ошибка квитовки по сервисной операции БУ
   WRTOFF_ERROR_20244    CONSTANT INTEGER := -20244; -- Ошибка квитовки по операции
   WRTOFF_ERROR_20245    CONSTANT INTEGER := -20245; -- Ошибка отката обработки лотов. Откат не завершен
   WRTOFF_ERROR_20246    CONSTANT INTEGER := -20246; -- Выполнена обработка операций за более поздние даты
   WRTOFF_ERROR_20247    CONSTANT INTEGER := -20247; -- Необходимо выполнить сервисные операции БУ за более ранние даты
   WRTOFF_ERROR_20248    CONSTANT INTEGER := -20248; -- Выполнена обработка лотов по бумаге за более позднюю дату
   WRTOFF_ERROR_20249    CONSTANT INTEGER := -20249; -- Выполнено начисление за более позднюю дату
   WRTOFF_ERROR_20250    CONSTANT INTEGER := -20250; -- Некорректный лот
   WRTOFF_ERROR_20251    CONSTANT INTEGER := -20251; -- Блокированное количество на лоте не может быть меньше нуля
   WRTOFF_ERROR_20252    CONSTANT INTEGER := -20252; -- Количество на лоте не может быть меньше блокированного количества
   WRTOFF_ERROR_20253    CONSTANT INTEGER := -20253; -- Расход на дату уже начислен
   WRTOFF_ERROR_20254    CONSTANT INTEGER := -20254; -- Неверные данные
   WRTOFF_ERROR_20255    CONSTANT INTEGER := -20255; -- Недостаточно ц/б для погашения
   WRTOFF_ERROR_20256    CONSTANT INTEGER := -20256; -- По выпуску не начисляется расход/доход
   WRTOFF_ERROR_20257    CONSTANT INTEGER := -20257; -- Отсроченная разница на дату уже начислена
   WRTOFF_ERROR_20260    CONSTANT INTEGER := -20260; -- Не найдены денежные параметры РЕПО
   WRTOFF_ERROR_20261    CONSTANT INTEGER := -20261; -- Ошибка при сохранении архивных данных лота
   WRTOFF_ERROR_20262    CONSTANT INTEGER := -20262; -- Неверные параметры осреднения дисконта/премии
   WRTOFF_ERROR_20263    CONSTANT INTEGER := -20263; -- Не выполнено начисление отсроченной разницы на дату списания
   WRTOFF_ERROR_20264    CONSTANT INTEGER := -20264; -- Для выпуска ц/б не задано примечание "Процент резервирования" на дату %s
   WRTOFF_ERROR_20265    CONSTANT INTEGER := -20265; -- Для выпуска ц/б не задана категория "Категория качества"
   WRTOFF_ERROR_20266    CONSTANT INTEGER := -20266; -- Для выпуска ц/б процент резервирования не соответствует заданной категории качества
   WRTOFF_ERROR_20267    CONSTANT INTEGER := -20267; -- Для выпуска ц/б не задан курс вида "Справедливая стоимость"
   WRTOFF_ERROR_20268    CONSTANT INTEGER := -20268; -- Ошибка дублирования
   WRTOFF_ERROR_20269    CONSTANT INTEGER := -20269; -- Отсутствует дата погашения ц/б с FIID %s, дата известного купона меньше даты расчета
   WRTOFF_ERROR_20270    CONSTANT INTEGER := -20270; -- Нельзя определить дату погашения/оферты ц/б с FIID %s
   WRTOFF_ERROR_28101    CONSTANT INTEGER := -20271; -- Для сделки %s не указана категория качества
   WRTOFF_ERROR_28102    CONSTANT INTEGER := -20272; -- Для сделки %s не указан процент резерва
   WRTOFF_ERROR_28103    CONSTANT INTEGER := -20273; -- Для сделки %s указанный процент резерва не соответствует указанной категории качества

/**
 * InterestIncome  Способ начисления ПД:  Не начислять, Купонный, Процентный */
   INTERESTINCOME_NOTCALC   CONSTANT NUMBER := 0;
   INTERESTINCOME_COUPON    CONSTANT NUMBER := 1;
   INTERESTINCOME_PERCENT   CONSTANT NUMBER := 2;

/**
 * Вид лота */
   WRTSUM_KIND_U            CONSTANT NUMBER := 0;   -- Не определено
   WRTSUM_KIND_S            CONSTANT NUMBER := 10;  -- Продажа
   WRTSUM_KIND_B            CONSTANT NUMBER := 20;  -- Покупка
   WRTSUM_KIND_FS           CONSTANT NUMBER := 30;  -- Списание лота
   WRTSUM_KIND_FB           CONSTANT NUMBER := 40;  -- Зачисление лота
   WRTSUM_KIND_DI           CONSTANT NUMBER := 50;  -- Погашение выпуска
   WRTSUM_KIND_DC           CONSTANT NUMBER := 60;  -- Погашение купона
   WRTSUM_KIND_DP           CONSTANT NUMBER := 70;  -- Частичное погашение
   WRTSUM_KIND_MS           CONSTANT NUMBER := 80;  -- Списание из портфеля
   WRTSUM_KIND_MB           CONSTANT NUMBER := 90;  -- Перемещение в портфель
   WRTSUM_KIND_DS           CONSTANT NUMBER := 100; -- Списание в конвертации ДР
   WRTSUM_KIND_DB           CONSTANT NUMBER := 110; -- Зачисление в конвертации ДР
   WRTSUM_KIND_GS           CONSTANT NUMBER := 120; -- Списание в глобальной операции
   WRTSUM_KIND_GB           CONSTANT NUMBER := 130; -- Зачисление в глобальной операции
   WRTSUM_KIND_BSB          CONSTANT NUMBER := 140; -- Зачисление в покупке с обр. продажей (1 ч.)
   WRTSUM_KIND_BSS          CONSTANT NUMBER := 150; -- Списание в покупке с обр. продажей (2 ч.)
   WRTSUM_KIND_SBS          CONSTANT NUMBER := 160; -- Списание в продаже с обр. продажей (1 ч.)
   WRTSUM_KIND_SBB          CONSTANT NUMBER := 170; -- Зачисление в продаже с обр. продажей (2 ч.)
   WRTSUM_KIND_RRWAB1       CONSTANT NUMBER := 180; -- Зачисление в Репо обратном БПП (1 ч.)
   WRTSUM_KIND_RRWAS2       CONSTANT NUMBER := 190; -- Списание в Репо обратном БПП (2 ч.)
   WRTSUM_KIND_RRWAS1       CONSTANT NUMBER := 200; -- Списание в Репо прямом БПП (1 ч.)
   WRTSUM_KIND_RRWAB2       CONSTANT NUMBER := 210; -- Зачислениев Репо прямом БПП (2 ч.)
   WRTSUM_KIND_LCB          CONSTANT NUMBER := 220; -- Привлечение займа (1 ч.)
   WRTSUM_KIND_LCS          CONSTANT NUMBER := 230; -- Возврат привлеченного займа (2.ч.)
   WRTSUM_KIND_LPS          CONSTANT NUMBER := 240; -- Размещение займа (1 ч.)
   WRTSUM_KIND_LPB          CONSTANT NUMBER := 250; -- Возврат размещенного займа (2.ч.)
   WRTSUM_KIND_PB           CONSTANT NUMBER := 260; -- Получение паев
   WRTSUM_KIND_RRAB1        CONSTANT NUMBER := 270; -- Зачисление в Репо обратном ПП (1 ч.)
   WRTSUM_KIND_RRAS2        CONSTANT NUMBER := 280; -- Списание в Репо обратном ПП (2 ч.)
   WRTSUM_KIND_RRAS1        CONSTANT NUMBER := 290; -- Списание в Репо прямом ПП (1 ч.)
   WRTSUM_KIND_RRAB2        CONSTANT NUMBER := 300; -- Зачислениев Репо прямом ПП (2 ч.)
   WRTSUM_KIND_FS_TRUSTREQ  CONSTANT NUMBER := 330; -- Списание лота в ДУ по заявлению ВВК
   WRTSUM_KIND_FB_TRUSTREQ  CONSTANT NUMBER := 340; -- Зачисление лота в ДУ по заявлению ВВК
   WRTSUM_KIND_DSTR         CONSTANT NUMBER := 350; -- Размещение
   WRTSUM_KIND_RDMP         CONSTANT NUMBER := 360; -- Выкуп
   WRTSUM_KIND_RCURR        CONSTANT NUMBER := 370; -- Денежные параметры РЕПО
   WRTSUM_KIND_DAI          CONSTANT NUMBER := 380; -- Погашение доп. дохода

/**
 * Ниже приведенные константы нужно согласовывать с cbpmbfe.h
 *
 * Признак признания. Используется в лотах Репо/займах */
   PM_WRTUNADM_DEFAULT              CONSTANT NUMBER := 0;   -- 0   С признанием (== Не указано)
   PM_WRTUNADM_NOTPERM              CONSTANT NUMBER := 1;   -- 1   Без признания, продажа не разрешена
   PM_WRTUNADM_NOTPERMIT_LOTNSALE   CONSTANT NUMBER := 2;   -- 2   Без признания, продажа разрешена, лот не продан
   PM_WRTUNADM_NOTPERMIT_LOTSALE    CONSTANT NUMBER := 3;   -- 3   Без признания, продажа разрешена, лот продан

/**
 * Статус лота */
   PM_WRTSUM_NOTFORM    CONSTANT NUMBER := 0;   -- 0 Не поставлен
   PM_WRTSUM_FORM       CONSTANT NUMBER := 1;   -- 1 Поставлен
   PM_WRTSUM_CANCEL     CONSTANT NUMBER := 2;   -- 2 Аннулирован
   PM_WRTSUM_SALE_BPP   CONSTANT NUMBER := 3;   -- Продан БПП
   PM_WRTSUM_DELAY      CONSTANT NUMBER := 4;   -- Отложен
   PM_WRTSUM_PLACE_OWN  CONSTANT NUMBER := 20;  -- Размещен
   PM_WRTSUM_BUYOUT_OWN CONSTANT NUMBER := 21;  -- Выкуплен
   PM_WRTSUM_RET_OWN    CONSTANT NUMBER := 22;  -- Погашен
   PM_WRTSUM_CLOSE_OWN  CONSTANT NUMBER := 23;  -- Закрыт

/**
 * Категория лота */
   PM_WRITEOFF_SUM_UNDEF      CONSTANT NUMBER := -1; --не задан (все)
   PM_WRITEOFF_SUM_BUY        CONSTANT NUMBER := 0;  --покупка\зачисление
   PM_WRITEOFF_SUM_SALE       CONSTANT NUMBER := 1;  --продажа\списание
   PM_WRITEOFF_SUM_COUPON     CONSTANT NUMBER := 2;  --Погашение купона
   PM_WRITEOFF_SUM_PARTIAL    CONSTANT NUMBER := PM_WRITEOFF_SUM_COUPON;  --Частичное погашение
   PM_WRITEOFF_SUM_BUY_BO     CONSTANT NUMBER := 3;  --Зачисление ВО
   PM_WRITEOFF_SUM_DIVIDEND   CONSTANT NUMBER := 4;  --Получение дивидендов
   PM_WRITEOFF_SUM_PLACE      CONSTANT NUMBER := 5;  --Размещение
   PM_WRITEOFF_SUM_RET_PLACE  CONSTANT NUMBER := 6;  --Возврат из размещения
   PM_WRITEOFF_SUM_ADDINCOME  CONSTANT NUMBER := PM_WRITEOFF_SUM_COUPON;  --Погашение доп. дохода

/**
 * Вид изменения */
   PM_WRT_UPDTMODE_UNDEF          CONSTANT NUMBER := -1;  --не задан
   PM_WRT_UPDTMODE_CREATE         CONSTANT NUMBER := 0;   --Создание
   PM_WRT_UPDTMODE_DELIVERY       CONSTANT NUMBER := 10;  --Поставка
   PM_WRT_UPDTMODE_INPORTFOLIO    CONSTANT NUMBER := 20;  --Перемещение в портфеля
   PM_WRT_UPDTMODE_CONVERT        CONSTANT NUMBER := 30;  --Конвертация лота
   PM_WRT_UPDTMODE_DISCARDBD      CONSTANT NUMBER := 40;  --Списание ОД
   PM_WRT_UPDTMODE_DELIVERYBPP    CONSTANT NUMBER := 50;  --Поставка  БПП
   PM_WRT_UPDTMODE_REJECTEXEC     CONSTANT NUMBER := 200; --Отказ от исполнения
   PM_WRT_UPDTMODE_CHANGETERM     CONSTANT NUMBER := 210; --Изменение условий
   PM_WRT_UPDTMODE_CHANGEDATE     CONSTANT NUMBER := 220; --Изменение сроков исполнения
   PM_WRT_UPDTMODE_RESERV         CONSTANT NUMBER := 400; --Резервирование
   PM_WRT_UPDTMODE_OVERVALUE      CONSTANT NUMBER := 410; --переоценка
   PM_WRT_UPDTMODE_CONVOLUTION    CONSTANT NUMBER := 415; --Свертка
   PM_WRT_UPDTMODE_GLOBALCONV     CONSTANT NUMBER := 420; --глобальная конвертация
   PM_WRT_UPDTMODE_GLOBALOUT      CONSTANT NUMBER := 421; --Списание в ГО
   PM_WRT_UPDTMODE_GLOBALIN       CONSTANT NUMBER := 422; --Зачисление в ГО
   PM_WRT_UPDTMODE_GLOBALTRANSF   CONSTANT NUMBER := 430; --глобальное перемещение
   PM_WRT_UPDTMODE_CRIS_TRANSF    CONSTANT NUMBER := 435; --локальное кризисное перемещение
   PM_WRT_UPDTMODE_INCPDD         CONSTANT NUMBER := 440; --Начисление ПДД
   PM_WRT_UPDTMODE_INCPD          CONSTANT NUMBER := 441; --Начисление ПД
   PM_WRT_UPDTMODE_INCDD          CONSTANT NUMBER := 442; --Начисление ДД
   PM_WRT_UPDTMODE_PARTIAL        CONSTANT NUMBER := 450; --Учет ЧП
   PM_WRT_UPDTMODE_COUPON         CONSTANT NUMBER := 460; --Учет погашения купона
   PM_WRT_UPDTMODE_CDELIVERY      CONSTANT NUMBER := 480; --Исполнение компенсационной поставки
   PM_WRT_UPDTMODE_CDELIVERY2     CONSTANT NUMBER := 490; --Учет компенсационной поставки
   PM_WRT_UPDTMODE_CORRECT2       CONSTANT NUMBER := 510; --Корректировка лота 2ч
   PM_WRT_UPDTMODE_BLCKAMNT       CONSTANT NUMBER := 520; --Блокировка количества на лоте
   PM_WRT_UPDTMODE_RMBLCKAMNT     CONSTANT NUMBER := 530; --Снятие блокировки с количества на лоте
   PM_WRT_UPDTMODE_BUYOUT         CONSTANT NUMBER := 540; --Выкуп
   PM_WRT_UPDTMODE_SALE_BUYOUT    CONSTANT NUMBER := 550; --Продажа выкупленного
   PM_WRT_UPDTMODE_CORRECTCOSTOWN CONSTANT NUMBER := 560; --Корректировка стоимости ОЭБ
   PM_WRT_UPDTMODE_RCURR          CONSTANT NUMBER := 600; --Создание денежных параметров РЕПО
   PM_WRT_UPDTMODE_RCURR_PROFIT   CONSTANT NUMBER := 610; --Начисление по денежным параметрам РЕПО
   PM_WRT_UPDTMODE_CORRECTSUM     CONSTANT NUMBER := 4999;--Коррекция сумм
   PM_WRT_UPDTMODE_DISCARD        CONSTANT NUMBER := 5000;--списание
   PM_WRT_UPDTMODE_RETISSUE       CONSTANT NUMBER := 5010;--погашение выпуска
   PM_WRT_UPDTMODE_RETCOUPON      CONSTANT NUMBER := 5020;--погашение купона
   PM_WRT_UPDTMODE_RETPARTLY      CONSTANT NUMBER := 5030;--частичное погашение
   PM_WRT_UPDTMODE_PORTFOLIO      CONSTANT NUMBER := 5040;--Списание из портфеля
   PM_WRT_UPDTMODE_RETADDINCOME   CONSTANT NUMBER := 5050;--Погашение доп. дохода
   PM_WRT_UPDTMODE_HEDGCORR       CONSTANT NUMBER := 5060;--Корректировки хеджирования

/**
 * для средневзвешенного списания */
   PM_WRT_UPDTMODE_AVERAGE_WRTOFF CONSTANT NUMBER := 205;--Списание из портфеля по средневзвешенной
   PM_WRT_UPDTMODE_AVERAGE_DST    CONSTANT NUMBER := 206;--Распределение сумм в средневзвешенном

/**
 * Виды связей */
   PMWRTLINK_KIND_DISCARD      CONSTANT NUMBER := 10; -- списание
   PMWRTLINK_KIND_RETISSUE     CONSTANT NUMBER := 20; -- Погашение выпуска
   PMWRTLINK_KIND_RETCOUPON    CONSTANT NUMBER := 30; -- Погашение купона
   PMWRTLINK_KIND_RETPARTLY    CONSTANT NUMBER := 40; -- Частичное погашение
   PMWRTLINK_KIND_PORTFOLIO    CONSTANT NUMBER := 50; -- Списание из портфеля
   PMWRTLINK_KIND_DISCARDBD    CONSTANT NUMBER := 60; -- Списание ОД
   PMWRTLINK_KIND_BUYOUT       CONSTANT NUMBER := 70; -- Выкуп
   PMWRTLINK_KIND_SALE_BUYOUT  CONSTANT NUMBER := 80; -- Продажа выкупленного
   PMWRTLINK_KIND_RETADDINCOME CONSTANT NUMBER := 90; -- Погашение доп. дохода

/**
 * Методы списания */
   PM_WRITEOFF_LIFO    CONSTANT NUMBER := 0;
   PM_WRITEOFF_FIFO    CONSTANT NUMBER := 1;
   PM_WRITEOFF_AVERAGE CONSTANT NUMBER := 3;

/**
 * виды значений объекта - Вид портфеля (1100) */
   KINDPORT_UNDEF         CONSTANT NUMBER := -1; --        не задан
   KINDPORT_CLIENT        CONSTANT NUMBER := 0;  -- КП,    Клиентский портфель
   KINDPORT_TRADE         CONSTANT NUMBER := 1;  -- ССПУ_ЦБ, Ц/б, оцениваемые по СС через прибыль или убыток
   KINDPORT_SSPU          CONSTANT NUMBER := 1;  -- ССПУ_ЦБ, Ц/б, оцениваемые по СС через прибыль или убыток
   KINDPORT_SALE          CONSTANT NUMBER := 2;  -- ССПД_ЦБ, Ц/б, оцениваемые  по СС через прочий совокупный доход
   KINDPORT_SSSD          CONSTANT NUMBER := 2;  -- ССПД_ЦБ, Ц/б, оцениваемые  по СС через прочий совокупный доход
   KINDPORT_CONTR         CONSTANT NUMBER := 3;  -- ПКУ,   Портфель контрольного участия
   KINDPORT_PROMISSORY    CONSTANT NUMBER := 4;  -- ПДО,   Просроченные долговые обязательства
   KINDPORT_RETIRE        CONSTANT NUMBER := 5;  -- АС_ЦБ,    Ц/б, оцениваемые по АС(амортизированной стоимости)
   KINDPORT_ASCB          CONSTANT NUMBER := 5;  -- АС_ЦБ,    Ц/б, оцениваемые по АС(амортизированной стоимости)
   KINDPORT_BACK          CONSTANT NUMBER := 6;  -- ПВО,   Портфель ц/б, полученные на возвратной основе
   KINDPORT_TRUST         CONSTANT NUMBER := 7;  -- ПДУ,   Портфель ДУ
   KINDPORT_BASICDEBT     CONSTANT NUMBER := 8;  -- ОД,    Основной долг
   KINDPORT_BACK_KSU      CONSTANT NUMBER := 12; -- ПВО_КСУ, Ц/б, полученные при внесении активов в имущественный пул. РЕПО.
   KINDPORT_BACK_BPP_KSU  CONSTANT NUMBER := 13; -- ПВО_БПП_КСУ, Ц/б, полученные при внесении активов в имущественный пул и переданные БПП
   KINDPORT_AC_OWN        CONSTANT NUMBER := 40; -- АС ОЭБ
   KINDPORT_CURR_AC_BPP   CONSTANT NUMBER := 45; -- ДС по сделкам с ЦБ БПП, оцениваемые по амортизированной стоимости
   KINDPORT_CURR_CCPU_BPP CONSTANT NUMBER := 46; -- ДС по сделкам с ЦБ БПП, оцениваемые по СС через прибыль или убыток
   KINDPORT_CURR_AC_PVO   CONSTANT NUMBER := 47; -- ДС по сделкам с ЦБ, полученным по операциям, совершенным на возвратной основе (займам  и обр. РЕПО), без первоначального признания
   KINDPORT_KSU           CONSTANT NUMBER := 112;-- КСУ, Ц/б, полученные при внесении активов в имущественный пул. Зачисление.

/**
 * статусы лотов в глобальных операциях */
   SCDLPMWR_STATE_UNDEF     CONSTANT NUMBER := 0;   -- Не определен
   SCDLPMWR_STATE_NOT_READY CONSTANT NUMBER := 1;   -- Не поставлен
   SCDLPMWR_STATE_READY     CONSTANT NUMBER := 2;   -- Поставлен

/**
 * виды действий над записями реестра новых выпусков */
   SCDL_NOACTION CONSTANT NUMBER := 0;
   SCDL_INSERT   CONSTANT NUMBER := 1;
   SCDL_UPDATE   CONSTANT NUMBER := 2;
   SCDL_DELETE   CONSTANT NUMBER := 3;

/**
 * набор значений настройки 'SECUR\CHARGE_BONUS' */
   CHARGE_BONUS_NO  CONSTANT NUMBER := 0; --Значение "нет"
   CHARGE_BONUS_YES CONSTANT NUMBER := 1; --Значение "да"

/**
 * набор значений настройки 'SECUR\SORTING_LOTS_CODE' */
   SORTING_LOTS_CODE_BYINNERCODE CONSTANT NUMBER := 0; --сортировка по внутреннему коду сделки
   SORTING_LOTS_CODE_BYOUTERCODE CONSTANT NUMBER := 1; --сортировка по внешнему коду сделки
   SORTING_LOTS_CODE_BYID        CONSTANT NUMBER := 2; --сортировка по ID сделок

/**
 * набор значений настройки 'SECUR\SORTING_LOTS_AMOUNT' */
   SORTING_LOTS_AMOUNT_ASC  CONSTANT NUMBER := 0; --сортировка по возрастанию
   SORTING_LOTS_AMOUNT_DESC CONSTANT NUMBER := 1; --сортировка по убыванию

/**
 * набор значений настройки 'SECUR\CALC_DISCOUNT_WITHOUT_PARTLY' */
   DISCOUNT_WITHOUT_PARTLY_NO  CONSTANT NUMBER := 0; --Значение "нет"
   DISCOUNT_WITHOUT_PARTLY_YES CONSTANT NUMBER := 1; --Значение "да"

/**
 * набор значений настройки 'SECUR\МСФО\НАЧИСЛЕНИЕ ПРЕМИИ БЕЗ УЧЕТА ЧП' */
   BONUS_WITHOUT_PARTLY_NO  CONSTANT NUMBER := 0; --Значение "нет"
   BONUS_WITHOUT_PARTLY_YES CONSTANT NUMBER := 1; --Значение "да"

/**
 * набор значений настройки 'SECUR\МСФО\УЧЕТ ВАЛЮТНЫХ ДОЛЕВЫХ ЦБ' */
   ACCOUNTINGEXSECUR_NATCUR      CONSTANT NUMBER := 0; --в рублях
   ACCOUNTINGEXSECUR_FACEVALUEFI CONSTANT NUMBER := 1; --в валюте номинала

/**
 * набор значений настройки 'SECUR\МСФО\ЭПС ДЛЯ  ДО МЕНЬШЕ ГОДА' */
   EPSAVRLESSTHANYEAR_NO  CONSTANT NUMBER := 0; --Значение "нет"
   EPSAVRLESSTHANYEAR_YES CONSTANT NUMBER := 1; --Значение "да"

/**
 * набор значений настройки 'SECUR\МСФО\ЭПС ДЛЯ ДО С НЕСУЩЕСТВ. ОТКЛ' */
   EPSAVRNSIGNDEVACEPSACLM_NO  CONSTANT NUMBER := 0; --Значение "нет"
   EPSAVRNSIGNDEVACEPSACLM_YES CONSTANT NUMBER := 1; --Значение "да"

/**
 * набор значений настройки 'SECUR\CALC_DEFDIFF_WITHOUT_PARTLY' */
   DEFDIFF_WITHOUT_PARTLY_NO  CONSTANT NUMBER := 0; --Значение "нет"
   DEFDIFF_WITHOUT_PARTLY_YES CONSTANT NUMBER := 1; --Значение "да"

/**
 * набор значений настройки 'SECUR\МСФО\НАЧИСЛЕНИЕ ПДД ПО ДО В ССПУ' */
   CALC_PDD_BOND_SSPU_PD  CONSTANT NUMBER := 0; --Значение "купоны"
   CALC_PDD_BOND_SSPU_ALL CONSTANT NUMBER := 1; --Значение "все"

/**
 * набор значений настройки 'SECUR\ОЭБ_АМОРТИЗАЦИЯ_ДО_ОФЕРТЫ' */
   CALC_AMORTOWN_BEFOREOFFER_NO  CONSTANT NUMBER := 0; --Значение "нет"
   CALC_AMORTOWN_BEFOREOFFER_YES CONSTANT NUMBER := 1; --Значение "да"

/**
 * набор значений настройки 'SECUR\ДАТА КУРСА КОНВЕРТАЦИИ ЗАТРАТ' */
   DATE_COURSE_DELIV  CONSTANT NUMBER := 0; --использование даты поставки (дата первоначального признания)
   DATE_COURSE_COMISS CONSTANT NUMBER := 1; --использование даты оплаты комиссии

/**
 * Вид дохода при начислении во временную таблицу: Процентный, Дисконтный */
   WRTSUMTMP_KIND_PD CONSTANT NUMBER := 1;
   WRTSUMTMP_KIND_DD CONSTANT NUMBER := 2;

   ReestrValue wrt_ReestrValue := wrt_ReestrValue();

   PROCEDURE GetLastErrorMessage( ErrMes OUT VARCHAR2 );
   PROCEDURE SetError( ErrNum IN INTEGER, ErrMes IN VARCHAR2 DEFAULT NULL );

   FUNCTION GetContextDealCode RETURN VARCHAR2;
   FUNCTION GetContextAvoirisName RETURN VARCHAR2;

/**
 *
 * получить PMWRTSUM из записи, переданной из ситсемы, в виде RAW
 * @since 6.20.029 @qtest-YES
 * @param p_Sum    Структура лота, переданная из системы
 * @param pmwrtsum Входной/Возвращаемый параметр - Лот в виде RAW
 */
   PROCEDURE RSI_CopyWRTSUMtoRAW( p_Sum IN DPMWRTSUM_DBT%ROWTYPE, pmwrtsum IN OUT RAW );
   PROCEDURE RSI_CopyRAWtoWRTSUM( pmwrtsum IN RAW, p_Sum OUT DPMWRTSUM_DBT%ROWTYPE );

/**
 * Заполнение дефолтными значениями null-х полей в переданной структуре лота/истории лота/линка
 */
   PROCEDURE RSI_InsDfltIntoWRTSUM( p_Sum IN OUT DPMWRTSUM_DBT%ROWTYPE );
   PROCEDURE RSI_InsDfltIntoWRTBC( p_Sum IN OUT DPMWRTBC_DBT%ROWTYPE );
   PROCEDURE RSI_InsDfltIntoWRTLNK( p_Sum IN OUT DPMWRTLNK_DBT%ROWTYPE );

/**
 * Определяет метод списания для субъекта.
 * @since 6.20.029
 * @qtest NO
 * @param p_ClientID  Субъект (наш банк - UnknounParty)
 * @param p_ContractID Договор обслуживания
 * @return Метод списания для субъекта
 */
   FUNCTION GetAmortizationMethod( p_ClientID IN NUMBER, p_ContractID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Определяет необходимость начисления дохода на ц/б в продаже и операции начисления дохода и
 * определяет коэффициенты, требуемые для расчета. Используется в БО Ц/Б.
 * @since 6.20.029
 * @qtest NO
 * @param p_FIID ID ц/б
 * @return 1 - да, 0 - нет
 */
   FUNCTION WRTNeedChargeIncome( p_FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Определяет необходимость начисления премии на ц/б. Используется в БО Ц/Б.
 * @since 6.20.029
 * @qtest NO
 * @param p_FIID ID ц/б
 * @return 1 - да, 0 - нет
 */
   FUNCTION WRTNeedChargeBonus( p_FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Определяет необходимость начисления процентного дохода на ц/б. Используется в БО Ц/Б.
 * @since 6.20.029
 * @qtest NO
 * @param p_FIID ID ц/б
 * @return 1 - да, 0 - нет
 */
   FUNCTION WRTNeedChargeInterestIncome( p_FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление процентного дохода на купленные ц/б.
 * @since 6.20.029
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BuyDate Дата начала начисления ПД
 * @param LnkKind Вид связи списания
 * @param FIID ID ц/б
 * @param Amount Количество
 * @param NKD Сумма НКД
 * @param PrevInterestDate Дата предыдущего начисления
 * @param PrevInterest Предыдущее начисление
 * @param Method Метод списания
 * @param Coupon Номер купона
 * @param IsTrust Признак ДУ
 * @param CorrectDate Признак коррекции последней даты месяца (1 - CalcDate корректируется в соответствии с базисом расчета, 0 - CalcDate не корректируется)
 * @return Значение процентного дохода
 */
   FUNCTION WRTCalcInterestIncome( CalcDate         IN DATE,
                                   BuyDate          IN DATE,
                                   LnkKind          IN NUMBER,
                                   FIID             IN NUMBER,
                                   Amount           IN NUMBER,
                                   NKD              IN NUMBER,
                                   PrevInterestDate IN DATE,
                                   PrevInterest     IN NUMBER,
                                   Method           IN NUMBER, 
                                   Coupon           IN VARCHAR2 DEFAULT NULL,
                                   IsTrust          IN BOOLEAN DEFAULT False,
                                   CorrectDate      IN NUMBER DEFAULT 0
                                 ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление дисконтного дохода на дату.
 * @since 6.20.029
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param EndDate Дата окончания периода начисления начисления ДД
 * @param BuyDate Дата начала начисления ДД
 * @param FIID ID ц/б
 * @param Discount0 Остаток начального дисконта
 * @param InclDrwDate Включать дату погашения (1 - да, 0 - нет)
 * @param Method Метод списания
 * @return Значение дисконтного дохода на дату
 */
   FUNCTION WRTCalcDiscountIncomeOnDate( CalcDate     IN DATE,
                                         EndDate      IN DATE,
                                         BuyDate      IN DATE,
                                         FIID         IN NUMBER,
                                         Discount0    IN NUMBER,
                                         InclDrwDate  IN NUMBER,
                                         Method       IN NUMBER
                                       ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление дисконтного дохода на купленные ц/б. Используется в БО Ц/Б.
 * @since 6.20.029
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BuyDate Дата начала начисления ПД
 * @param LnkKind Вид связи списания
 * @param FIID ID ц/б
 * @param Amount Количество
 * @param Cost Стоимость
 * @param PrevDiscountDate Дата предыдущего начисления
 * @param PrevDiscount Предыдущее начисление
 * @param Discount0 Остаток начального дисконта
 * @param OldDiscount0 Остаток начального дисконта до перевода
 * @param p_Party Владелец ц/б
 * @param p_Contract Договор обслуживания клиента p_Party
 * @param RecalcDate Дата пересчета дисконта при переводе
 * @return Значение дисконтного дохода
 */
   FUNCTION WRTCalcDiscountIncome( CalcDate         IN DATE,
                                   BuyDate          IN DATE,
                                   LnkKind          IN NUMBER,
                                   FIID             IN NUMBER,
                                   Amount           IN NUMBER,
                                   Cost             IN NUMBER,
                                   PrevDiscountDate IN DATE,
                                   PrevDiscount     IN NUMBER,
                                   Discount0        IN NUMBER,
                                   OldDiscount0     IN NUMBER,
                                   p_Party          IN NUMBER,
                                   p_Contract       IN NUMBER,
                                   RecalcDate       IN DATE
                                 ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление суммы премии на дату. Используется в БО Ц/Б.
 * @since 6.20.029
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param EndDate Дата окончания периода начисления
 * @param BuyDate Дата начала начисления премии
 * @param FIID ID ц/б
 * @param Bonus0 Остаток начальной премии
 * @param Method Метод списания
 * @return Сумма премии на дату
 */
   FUNCTION WRTCalcBonusOnDate( CalcDate     IN DATE,
                                EndDate      IN DATE,
                                BuyDate      IN DATE,
                                FIID         IN NUMBER,
                                Bonus0       IN NUMBER,
                                InclDrwDate  IN NUMBER,
                                Method       IN NUMBER  
                              ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление суммы премии на дату. Используется в БО ОЭБ.
 * @since 6.20.048
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BegDate Дата начала начисления премии
 * @param FIID ID ц/б
 * @param Bonus0 Начальная премия
 * @return Сумма премии на дату
 */
   FUNCTION WRTCalcBonusOwnOnDate( CalcDate     IN DATE,
                                   BegDate      IN DATE,
                                   FIID         IN NUMBER,
                                   Bonus0       IN NUMBER
                                 ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление суммы отсроченной разницы на дату. Используется в БО ОЭБ.
 * @since 6.20.048
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BegDate Дата начала начисления отсроченной разницы
 * @param FIID ID ц/б
 * @param BegDefDiff Начальная отсроченная разница
 * @return Отсроченная разница на дату
 */
   FUNCTION WRTCalcDefDiffOwnOnDate( CalcDate     IN DATE,
                                     BegDate      IN DATE,
                                     FIID         IN NUMBER,
                                     BegDefDiff   IN NUMBER
                                   ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление суммы затрат на дату. Используется в БО ОЭБ.
 * @since 6.20.048
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BegDate Дата начала начисления затрат
 * @param FIID ID ц/б
 * @param BegWrtOutlay Начальные затраты
 * @return Затраты на дату
 */
   FUNCTION WRTCalcWrtOutlayOwnOnDate( CalcDate     IN DATE,
                                       BegDate      IN DATE,
                                       FIID         IN NUMBER,
                                       BegWrtOutlay IN NUMBER
                                     ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление суммы НДС затрат на дату. Используется в БО ОЭБ.
 * @since 6.20.048
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BegDate Дата начала начисления НДС затрат
 * @param FIID ID ц/б
 * @param BegVatOutlay Начальный НДС затрат
 * @return НДС затраты на дату
 */
   FUNCTION WRTCalcVatOutlayOwnOnDate( CalcDate     IN DATE,
                                       BegDate      IN DATE,
                                       FIID         IN NUMBER,
                                       BegVatOutlay IN NUMBER
                                     ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление суммы премии на купленные ц/б. Используется в БО Ц/Б.
 * @since 6.20.029
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BuyDate Дата начала начисления премии
 * @param FIID ID ц/б
 * @param PrevBonusDate Дата предыдущего начисления
 * @param PrevBonus Предыдущее начисление
 * @param Bonus0 Остаток начальной премии
 * @param OldBonus0 Остаток начальной премии до перевода
 * @param p_Party Владелец ц/б
 * @param p_Contract Договор обслуживания клиента p_Party
 * @param RecalcDate Дата пересчета премии при переводе
 * @return Сумма премии
 */
   FUNCTION WRTCalcBonus( CalcDate         IN DATE,
                          BuyDate          IN DATE,
                          LnkKind          IN NUMBER,
                          FIID             IN NUMBER,
                          Amount           IN NUMBER,
                          Cost             IN NUMBER, 
                          PrevBonusDate    IN DATE,
                          PrevBonus        IN NUMBER,
                          Bonus0           IN NUMBER,
                          OldBonus0        IN NUMBER,
                          p_Party          IN NUMBER,
                          p_Contract       IN NUMBER,
                          RecalcDate       IN DATE
                        ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление суммы отсроченной разницы на дату. Используется в БО Ц/Б.
 * @since 6.20.048
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BegDate Дата начала начисления премии
 * @param FIID ID ц/б
 * @param BegDefDiff Начальная отсроченная разница
 * @return Отсроченная разница на дату
 */
   FUNCTION WRTCalcDefDiffOnDate( CalcDate     IN DATE,
                                  BegDate      IN DATE,
                                  FIID         IN NUMBER,
                                  BegDefDiff    IN NUMBER
                              ) RETURN NUMBER DETERMINISTIC;

 /**
 * Вычисление суммы отсроченной разницы. Используется в БО Ц/Б.
 * @since 6.20.048
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BegDate Дата начала начисления отсроченной разницы
 * @param FIID ID ц/б
 * @param Amount Количество
 * @param PrevDefDiffDate Дата предыдущего начисления
 * @param PrevDefDiff Ранее начисленная разница
 * @param BegDefDiff Начальная отсроченная разница
 * @param Party Владелец ц/б
 * @param Contract Договор обслуживания клиента p_Party
 * @return Сумма отсроченной разницы
 */
   FUNCTION WRTCalcDefDiff( CalcDate      IN DATE,
                                BegDate         IN DATE,
                                FIID            IN NUMBER,
                                Amount          IN NUMBER,
                                PrevDefDiffDate IN DATE,
                                PrevDefDiff     IN NUMBER,
                                BegDefDiff      IN NUMBER,
                                Party           IN NUMBER,
                                Contract        IN NUMBER
                              ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление суммы премии на размещенные ц/б. Используется в БО ОЭБ.
 * @since 6.20.048
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BegDate Дата начала начисления премии
 * @param LinkKind Вид связи списания
 * @param FIID ID ц/б
 * @param PrevBonusDate Дата предыдущего начисления
 * @param PrevBonus Предыдущее начисление
 * @param Bonus0 Начальная премия
 * @return Сумма премии
 */
   FUNCTION WRTCalcBonusOwn( CalcDate         IN DATE,
                             BegDate          IN DATE,
                             LinkKind         IN NUMBER,
                             FIID             IN NUMBER,
                             PrevBonusDate    IN DATE,
                             PrevBonus        IN NUMBER,
                             Bonus0           IN NUMBER
                           ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление суммы премии на размещенные ц/б. Используется в БО ОЭБ.
 * @since 6.20.048
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BegDate Дата начала начисления отсроченной разницы
 * @param LinkKind Вид связи списания
 * @param FIID ID ц/б
 * @param PrevDefDiffDate Дата предыдущего начисления
 * @param PrevDefDiff Ранее начисленная разница
 * @param BegDefDiff Начальная отсроченная разница
 * @return Сумма отсроченной разницы
 */
   FUNCTION WRTCalcDefDiffOwn( CalcDate         IN DATE,
                               BegDate          IN DATE,
                               LinkKind         IN NUMBER,
                               FIID             IN NUMBER,
                               PrevDefDiffDate  IN DATE,
                               PrevDefDiff      IN NUMBER,
                               BegDefDiff       IN NUMBER
                             ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление суммы балансовой стоимости лота ОЭБ на дату. Используется в БО ОЭБ.
 * @since 6.20.048
 * @qtest NO
 * @param SumID Идентификатор лота
 * @param OnDate Дата расчета
 * @return Сумма балансовой стоимости
 */
   FUNCTION WRTCalcBalanceCostOwnOnDate( SumID    IN NUMBER,
                                         OnDate   IN DATE
                                       ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление суммы затрат на размещенные ц/б. Используется в БО ОЭБ.
 * @since 6.20.048
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BegDate Дата начала начисления затрат
 * @param LinkKind Вид связи списания
 * @param FIID ID ц/б
 * @param PrevWrtOutlayDate Дата предыдущего начисления
 * @param PrevWrtOutlay Ранее начисленные затраты
 * @param BegWrtOutlay Начальные затраты
 * @return Сумма затрат к начислению
 */
   FUNCTION WRTCalcWrtOutlayOwn( CalcDate          IN DATE,
                                 BegDate           IN DATE,
                                 LinkKind          IN NUMBER,
                                 FIID              IN NUMBER,
                                 PrevWrtOutlayDate IN DATE,
                                 PrevWrtOutlay     IN NUMBER,
                                 BegWrtOutlay      IN NUMBER
                               ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление суммы НДС затрат на размещенные ц/б. Используется в БО ОЭБ.
 * @since 6.20.048
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BegDate Дата начала начисления НДС затрат
 * @param LinkKind Вид связи списания
 * @param FIID ID ц/б
 * @param PrevVatOutlayDate Дата предыдущего начисления
 * @param PrevVatOutlay Ранее начисленный НДС затрат
 * @param BegVatOutlay Начальный НДС затрат
 * @return Сумма затрат к начислению
 */
   FUNCTION WRTCalcVatOutlayOwn( CalcDate          IN DATE,
                                 BegDate           IN DATE,
                                 LinkKind          IN NUMBER,
                                 FIID              IN NUMBER,
                                 PrevVatOutlayDate IN DATE,
                                 PrevVatOutlay     IN NUMBER,
                                 BegVatOutlay      IN NUMBER
                               ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление процентного расхода на размещенные ц/б.
 * @since 6.20.048
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param PlaceDate Дата начала начисления ПКД
 * @param LnkKind Вид связи списания
 * @param FIID ID ц/б
 * @param Amount Количество
 * @param NKD Сумма НКД
 * @param PrevInterestDate Дата предыдущего начисления
 * @param PrevInterest Предыдущее начисление
 * @param Coupon Номер купона
 * @param CorrectDate Признак коррекции последней даты месяца (1 - CalcDate корректируется в соответствии с базисом расчета, 0 - CalcDate не корректируется)
 * @return Значение процентного расхода
 */
   FUNCTION WRTCalcInterestExpenseOwn( CalcDate         IN DATE,
                                       PlaceDate        IN DATE,
                                       LnkKind          IN NUMBER,
                                       FIID             IN NUMBER,
                                       Amount           IN NUMBER,
                                       NKD              IN NUMBER,
                                       PrevInterestDate IN DATE,
                                       PrevInterest     IN NUMBER,
                                       Coupon           IN VARCHAR2 DEFAULT NULL,
                                       CorrectDate      IN NUMBER DEFAULT 0
                                     ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление дисконтного расхода на дату. Используется в БО ОЭБ.
 * @since 6.20.048
 * @qtest NO
 * @param EndDate Дата окончания периода начисления
 * @param BegDate Дата начала начисления ДД
 * @param FIID ID ц/б
 * @param Discount0 Начальный дисконт
 * @return Накопленный дисконт на дату
 */
   FUNCTION WRTCalcDiscountExpOwnOnDate( EndDate      IN DATE,
                                         BegDate      IN DATE,
                                         FIID         IN NUMBER,
                                         Discount0    IN NUMBER
                                       ) RETURN NUMBER DETERMINISTIC;

/**
 * Вычисление дисконтного расхода на размещенные ц/б. Используется в БО ОЭБ.
 * @since 6.20.048
 * @qtest NO
 * @param CalcDate Дата начисления
 * @param BegDate Дата начала начисления ДД
 * @param LnkKind Вид связи списания
 * @param FIID ID ц/б
 * @param PrevDiscountDate Дата предыдущего начисления
 * @param PrevDiscount Начисленный дисконт
 * @param Discount0 Начального дисконта
 * @return Значение дисконтного расхода
 */
   FUNCTION WRTCalcDiscountExpenseOwn( CalcDate         IN DATE,
                                       BegDate          IN DATE,
                                       LnkKind          IN NUMBER,
                                       FIID             IN NUMBER,
                                       PrevDiscountDate IN DATE,
                                       PrevDiscount     IN NUMBER,
                                       Discount0        IN NUMBER
                                     ) RETURN NUMBER DETERMINISTIC;

/**
 * Выполняет сохранение архивных данных лота
 * Используется, в частности, при внесении изменений в лот на шагах. Должна быть вызвана перед изменением лота
 */
   PROCEDURE RSI_WRTSaveLot( p_SumID IN NUMBER, p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER, p_ChangeDate IN DATE, p_Action IN NUMBER );

/**
 * Выполняет восстановление лота по архивным данным
 */
   PROCEDURE RSI_WRTRestoreLot( p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER, p_Action IN NUMBER, p_SumID IN NUMBER DEFAULT 0 );

/**
 * Выполняет восстановление лота по архивным данным. Используется только при откате шагов начислений.
 */
   PROCEDURE RSI_WRTRestoreLotEx( p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER, p_Action IN NUMBER, p_SumID IN NUMBER DEFAULT 0 );

/**
 * Выполняет восстановление лота по архивным данным с выдачей сообщения об ошибке для конкретного лота (аналог RSI_WRTRestoreLot)
 */
   PROCEDURE RSI_WRTRestoreLotAlt( p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER, p_Action IN NUMBER );

/**
 * Выполняет списание лота продажи/погашения выпуска по лотам покупки заданной группы списания.
 */
   PROCEDURE RSI_WRTLinkSaleToBuy( p_SaleLot          IN OUT DPMWRTSUM_DBT%ROWTYPE,
                                   p_Portfolio        IN     NUMBER, -- Портфель. Если не задан -  игнорируется.
                                   p_Group            IN     NUMBER, -- Группа списания
                                   p_StartDate        IN     DATE,   -- Дата отбора лотов покупки. Если не задана  - игнорируется.
                                   p_Method           IN     NUMBER, -- Метод списания
                                   p_CheckTime        IN     NUMBER, -- Признак контроля времени
                                   p_ID_Operation     IN     NUMBER, -- Операция и шаг, на которых выполняется списание
                                   p_ID_Step          IN     NUMBER,
                                   p_LinkKind         IN     NUMBER, -- Вид создаваемой связи
                                   p_Action           IN     NUMBER, -- Вид изменения на лоте
                                   p_CalcInterest     IN    BOOLEAN, -- Расчитывать процентный доход
                                   p_CalcDiscount     IN    BOOLEAN, -- Расчитывать дисконтный доход
                                   p_CalcBonus        IN    BOOLEAN, -- Начислять премию
                                   p_CalcDefDiff      IN    BOOLEAN, -- Начислять отсроченную разницу
                                   p_CalcCorrIntToEIR IN    BOOLEAN, -- Начислять корректировку процентов до ЭПС
                                   p_CalcReserve      IN    BOOLEAN, -- Начислять резерв
                                   p_CalcOver         IN    BOOLEAN DEFAULT false,-- Пересчитывать переоценку
                                   p_OnlyTodayBuys    IN    NUMBER DEFAULT 0 --Списывать только с сегодняшних покупок
                                 );

/**
 * Выполняет списание по лоту продажи выкупленных лотов.
 * @since 6.20.048
 * @qtest NO
 * @param p_SaleLot Буфер лота продажи
 * @param p_ID_Operation Операция и шаг, на которых выполняется списание
 * @param p_ID_Step Операция и шаг, на которых выполняется списание
 */
   PROCEDURE RSI_WRTLinkSaleToBuyOwn( p_SaleLot          IN OUT DPMWRTSUM_DBT%ROWTYPE,
                                      p_ID_Operation     IN     NUMBER,
                                      p_ID_Step          IN     NUMBER
                                    );

/**
 * Выполняет списание лота погашения выпуска по размещенным и выкупленным лотам.
 * @since 6.20.048
 * @qtest NO
 * @param p_RetLot Буфер лота погашения
 * @param p_ID_Operation Операция и шаг, на которых выполняется списание
 * @param p_ID_Step Операция и шаг, на которых выполняется списание
 * @param p_LinkKind Вид создаваемой связи
 * @param p_Action Вид изменения на лоте
 * @param p_CalcInterest Расчитывать процентный доход
 * @param p_CalcDiscount Расчитывать дисконтный доход
 * @param p_CalcBonus Начислять премию
 * @param p_CalcDefDiff Начислять отложенную разницу
 * @param p_CalcOutlay Начислять существенные затраты
 */
   PROCEDURE RSI_WRTLinkRetireLotOwn( p_RetLot           IN OUT DPMWRTSUM_DBT%ROWTYPE,
                                      p_StartDate        IN     DATE,
                                      p_ID_Operation     IN     NUMBER,
                                      p_ID_Step          IN     NUMBER,
                                      p_LinkKind         IN     NUMBER,
                                      p_Action           IN     NUMBER,
                                      p_CalcInterest     IN     BOOLEAN,
                                      p_CalcDiscount     IN     BOOLEAN,
                                      p_CalcBonus        IN     BOOLEAN,
                                      p_CalcDefDiff      IN     BOOLEAN,
                                      p_CalcOutlay       IN     BOOLEAN
                                    );

/**
 * Выполняет списание лота продажи по 2 ч. по лоту 1 ч.
 */
   PROCEDURE RSI_WRTLinkSaleToLot( p_SaleLot          IN OUT DPMWRTSUM_DBT%ROWTYPE,
                                   p_CheckTime        IN     NUMBER, -- Признак контроля времени
                                   p_ID_Operation     IN     NUMBER, -- Операция и шаг, на которых выполняется списание
                                   p_ID_Step          IN     NUMBER,
                                   p_LinkKind         IN     NUMBER, -- Вид создаваемой связи
                                   p_Action           IN     NUMBER, -- Вид изменения на лоте
                                   p_CalcInterest     IN    BOOLEAN, -- Расчитывать процентный доход
                                   p_CalcDiscount     IN    BOOLEAN, -- Расчитывать дисконтный доход
                                   p_CalcBonus        IN    BOOLEAN, -- Начислять премию
                                   p_CalcOver         IN    BOOLEAN DEFAULT false -- Пересчитывать переоценку
                                 );

/**
 * Выполняет списание лота продажи или погашения выпуска по сделке.
 */
   PROCEDURE RSI_WRTLinkSale( p_SaleID       IN NUMBER, -- Списываемый лот
                              p_G1           IN NUMBER, -- Группы списания (по приоритетам, до 5-х штук, незаданные - UnknownValue (-1))
                              p_G2           IN NUMBER,
                              p_G3           IN NUMBER,
                              p_G4           IN NUMBER,
                              p_G5           IN NUMBER,
                              p_CI1          IN NUMBER, -- Признаки необходимости начисления ПДД по соотв. группам (портфелям)
                              p_CI2          IN NUMBER,
                              p_CI3          IN NUMBER,
                              p_CI4          IN NUMBER,
                              p_CI5          IN NUMBER,
                              p_LinkToParent IN NUMBER, -- Признак необходимости связывания с лотом 1 ч.
                              p_CP           IN NUMBER, -- Признак необходимости начисления дохода по 1 ч.
                              p_RetBD        IN NUMBER, -- Признак списания ОД с лота 1 ч. (может задаваться только если установлен LinkToParent)
                              p_StartDate    IN DATE,   -- Дата отбора лотов покупки. Если не задана  - игнорируется. Используется в погашении выпуска.
                              p_ID_Operation IN NUMBER, -- Операция и шаг, на которых выполняется списание
                              p_ID_Step      IN NUMBER,
                              p_GrpID        IN NUMBER DEFAULT 0 -- Группа обработки
                            );

/**
 * Откат RSI_WRTLinkSale
 */
   PROCEDURE RSI_WRTRecoilLinkSale( p_ID_Operation IN NUMBER, -- Операция и шаг, на которых выполняется списание
                                    p_ID_Step      IN NUMBER,
                                    p_SaleID       IN NUMBER  -- Списываемый лот для которого выполняется откат
                                  );

/**
 * Выполняет списание лота погашения в сделке ОЭБ.
 * @since 6.20.048
 * @qtest NO
 * @param p_RetLotID Лот погашения
 * @param p_ID_Operation Операция, на которой выполняется списание
 * @param p_ID_Step Шаг, на котором выполняется списание
 */
   PROCEDURE RSI_WRTLinkRetireOwn( p_RetLotID     IN NUMBER,
                                   p_ID_Operation IN NUMBER,
                                   p_ID_Step      IN NUMBER
                                 );

/**
 * Выполняет связывание лота выкупа с размещенными лотами.
 * @since 6.20.048
 * @qtest NO
 * @param p_RdLot Буфер лота выкупа
 * @param p_ID_Operation Операция, на которой выполняется списание
 * @param p_ID_Step Шаг, на котором выполняется списание
 */
   PROCEDURE RSI_WRTLinkRedemptionOwn( p_RdLot         IN OUT DPMWRTSUM_DBT%ROWTYPE,
                                       p_ID_Operation  IN     NUMBER,
                                       p_ID_Step       IN     NUMBER
                                     );

/**
 * ПолучитьГруппыСписания для НашегоБанка
 */
   PROCEDURE GetWriteOffGroups( p_BOfficeKind IN NUMBER,
                                p_WrtKind IN NUMBER,
                                p_Portfolio IN NUMBER,
                                p_IsREPO IN NUMBER,
                                p_IsSALE IN NUMBER,
                                p_IsKSU IN NUMBER,
                                p_UseContr IN NUMBER,
                                p_G1 OUT NUMBER,
                                p_G2 OUT NUMBER,
                                p_G3 OUT NUMBER,
                                p_G4 OUT NUMBER,
                                p_G5 OUT NUMBER );
                                
/**
 * Получение кода для сортировки
 */
   FUNCTION WrtGetSortCode( p_DealID     IN NUMBER, 
                            p_DocKind    IN NUMBER, 
                            p_DealCode   IN VARCHAR2, 
                            p_DealCodeTS IN VARCHAR2
                          ) return VARCHAR2;

/**
 * Процедура создания лотов по второй части Прямого РЕПО
 */
   PROCEDURE RSI_WRTCreateLotsFromLink( p_SaleID       IN NUMBER,
                                        p_RQID         IN NUMBER, -- ТО по второй части
                                        p_IsOneDayREPO IN NUMBER
                                      );

/**
 * Проверяет, есть ли изменения по поставленным лотам группы списания после даты.
 * @since 6.20.029
 * @qtest NO
 * @param p_Department Филиал
 * @param FIID ID ц/б
 * @param p_Party Владелец ц/б
 * @param p_Contract Договор обслуживания клиента p_Party
 * @param p_Portfolio  Портфель. Если не задан (UnknounValue(-1)) игнорируется
 * @param p_Group  Группа списания
 * @param CalcDate Дата расчета
 * @param p_CalcTime Время расчета
 * @param p_StartDate Дата отбора лотов покупки
 * @param p_CheckCoupon Признак проверки количества с учетом погашений купонов/ЧП
 * @param p_Coupon Номер купона. Задается, только если установлен CheckCoupon
 * @param p_Partly Номер ЧП. Задается, только если установлен CheckCoupon
 * @return true - есть, false - нет
 */
   FUNCTION WRTAreChangesAfterDate( p_Department   IN NUMBER,
                                    p_FIID         IN NUMBER,
                                    p_Party        IN NUMBER,
                                    p_Contract     IN NUMBER,
                                    p_Portfolio    IN NUMBER,
                                    p_Group        IN NUMBER,
                                    p_CalcDate     IN DATE     DEFAULT UnknownDate,
                                    p_CalcTime     IN DATE     DEFAULT UnknownTime,
                                    p_StartDate    IN DATE     DEFAULT UnknownDate,
                                    p_CheckCoupon  IN NUMBER   DEFAULT 0,
                                    p_Coupon       IN VARCHAR2 DEFAULT NULL,
                                    p_Partly       IN VARCHAR2 DEFAULT NULL
                                  ) RETURN BOOLEAN DETERMINISTIC;

/**
 * Проверяет, есть ли изменения по лотам размещения после даты.
 * @since 6.20.048
 * @qtest NO
 * @param p_Department Филиал
 * @param p_FIID ID ц/б
 * @param p_CalcDate Дата расчета
 * @param p_CalcTime Время расчета
 * @param p_StartDate Дата отбора лотов размещения
 * @param p_CheckCoupon Признак проверки количества с учетом погашений купонов
 * @param p_Coupon Номер купона. Задается, только если установлен CheckCoupon
 * @return true - есть, false - нет
 */
   FUNCTION WRTAreChangesAfterDateOwn( p_Department   IN NUMBER,
                                       p_FIID         IN NUMBER,
                                       p_CalcDate     IN DATE     DEFAULT UnknownDate,
                                       p_CalcTime     IN DATE     DEFAULT UnknownTime,
                                       p_StartDate    IN DATE     DEFAULT UnknownDate,
                                       p_CheckCoupon  IN NUMBER   DEFAULT 0,
                                       p_Coupon       IN VARCHAR2 DEFAULT NULL
                                     ) RETURN BOOLEAN DETERMINISTIC;

/**
 * Определяет валюту учёта ВУ при создании лотов
 * @since 6.20.048
 * @qtest NO
 * @param p_FIID ID ц/б
 * @return валюта учета
 */
   FUNCTION WRTDetermineAccFI( p_FIID IN NUMBER
                             ) RETURN NUMBER;

/**
 * Проверяет, есть ли изменения по поставленным и проданным БПП лотам приобретенных на заданную дату.
 * @since 6.20.029
 * @qtest NO
 * @param p_Department Филиал
 * @param FIID ID ц/б
 * @param p_Party Владелец ц/б
 * @param p_Contract Договор обслуживания клиента p_Party
 * @param p_P1..p_P5 список портфелей (до 5-х штук)
 * @param p_CalcDate Дата расчета
 * @param p_EndDate Дата окончания периода отбора
 * @return true - есть, false - нет
 */
   FUNCTION WRTAreChangesAfterEndDate(
                                        p_Department   IN NUMBER,
                                        p_FIID         IN NUMBER,
                                        p_Party        IN NUMBER,
                                        p_Contract     IN NUMBER,
                                        p_P1           IN NUMBER DEFAULT UnknownValue,
                                        p_P2           IN NUMBER DEFAULT UnknownValue,
                                        p_P3           IN NUMBER DEFAULT UnknownValue,
                                        p_P4           IN NUMBER DEFAULT UnknownValue,
                                        p_P5           IN NUMBER DEFAULT UnknownValue,
                                        p_CalcDate     IN DATE,
                                        p_EndDate      IN DATE
                                     ) RETURN BOOLEAN DETERMINISTIC;

/**
 * Проверяет, есть ли изменения по всем лотам портфеля после даты в глобальных операциях.
 * @since 6.20.029
 * @qtest NO
 * @param p_Department Филиал
 * @param FIID ID ц/б
 * @param p_Party Владелец ц/б
 * @param p_Contract Договор обслуживания клиента p_Party
 * @param p_P1..p_P5 - список портфелей (до 5-х штук)
 * @param p_CalcDate Дата расчета
 * @param p_Delivered Отбирать поставленные (1 - отбирать, 0 - не отбирать)
 * @param p_NotDelivered Отбирать непоставленные (1 - отбирать, 0 - не отбирать)
 * @param p_WithoutAccept Отбирать проданные БПП (1 - отбирать, 0 - не отбирать)
 * @return > 0 - есть, 0 - нет
 */
   FUNCTION WRTAreGlobalChangesAfterDate( p_Department   IN NUMBER,
                                          p_FIID         IN NUMBER,
                                          p_Party        IN NUMBER,
                                          p_Contract     IN NUMBER,
                                          p_P1           IN NUMBER DEFAULT UnknownValue,
                                          p_P2           IN NUMBER DEFAULT UnknownValue,
                                          p_P3           IN NUMBER DEFAULT UnknownValue,
                                          p_P4           IN NUMBER DEFAULT UnknownValue,
                                          p_P5           IN NUMBER DEFAULT UnknownValue,
                                          p_CalcDate     IN DATE   DEFAULT UnknownDate,
                                          p_Delivered    IN NUMBER DEFAULT 0,
                                          p_NotDelivered IN NUMBER DEFAULT 0,
                                          p_WithoutAccept   IN NUMBER DEFAULT 0
                                        ) RETURN INTEGER DETERMINISTIC;

/**
 * Выполняет переоценку по лотам одного портфеля
 */
   PROCEDURE RSI_WRTOvervalueLots( p_OperDate       IN DATE,
                                   p_FIID           IN NUMBER,
                                   p_Department     IN NUMBER,
                                   p_ID_Operation   IN NUMBER,
                                   p_ID_Step        IN NUMBER,
                                   p_Course         IN NUMBER,
                                   p_Portfolio      IN NUMBER,-- Портфель
                                   p_BalanceCost    IN NUMBER,
                                   p_Amount         IN NUMBER,
                                   p_OldBalanceCost IN NUMBER, -- Старая балансовая стоимость
                                   p_OverAmount     IN NUMBER  --  Сумма переоценки в НацВ
                                 );
/**
 * Выполняет переоценку по лотам одного портфеля в ДУ
 */
   PROCEDURE RSI_WRTOvervalueLotsTrust( p_OperDate       IN DATE,
                                        p_FIID           IN NUMBER,
                                        p_Department     IN NUMBER,
                                        p_ID_Operation   IN NUMBER,
                                        p_ID_Step        IN NUMBER,
                                        p_Course         IN NUMBER,
                                        p_Portfolio      IN NUMBER,-- Портфель
                                        p_Contract       IN NUMBER,
                                        p_Client         IN NUMBER,
                                        p_AccountCost    IN NUMBER,
                                        p_Amount         IN NUMBER,
                                        p_OldAccountCost IN NUMBER, -- Старая балансовая стоимость
                                        p_OverAmount     IN NUMBER  --  Сумма переоценки в НацВ
                                      );
/**
 * Выполняет списание переоценки по лотам портфеля
 */
   PROCEDURE RSI_WRTAmortizeOvervalueSum( p_OperDate       IN DATE,
                                          p_FIID           IN NUMBER,
                                          p_Department     IN NUMBER,
                                          p_ID_Operation   IN NUMBER,
                                          p_ID_Step        IN NUMBER,
                                          p_Portfolio      IN NUMBER,-- Портфель
                                          p_Contract       IN NUMBER,
                                          p_Client             IN NUMBER,
                                          p_CorrectBalanceCost IN NUMBER,-- Корректировать балансовую стоимость
                                          p_Action             IN NUMBER -- Вид изменения
                                        );

/**
 * Выполняет переоценку по одного лота
 */
   PROCEDURE RSI_WRTOvervalueLot( p_OperDate       IN DATE,
                                  p_SumID          IN NUMBER,
                                  p_ID_Operation   IN NUMBER,
                                  p_ID_Step        IN NUMBER,
                                  p_BalanceCost    IN NUMBER,
                                  p_OverAmount     IN NUMBER,  --  Сумма переоценки в НацВ
                                  p_IsBD           IN NUMBER DEFAULT 1
                                );

/**
 * Выполняет переоценку по лотам
 */
   PROCEDURE RSI_WRTOvervalue( p_OperDate     IN DATE,   -- Дата
                               p_FIID         IN NUMBER, -- Выпуск
                               p_Department   IN NUMBER, -- Филиал
                               p_ID_Operation IN NUMBER, -- Операция и шаг, на которых выполняется сохранение
                               p_ID_Step      IN NUMBER,
                               p_Course       IN NUMBER, -- Курс переоценки (ТСС)

                               p_G1           IN NUMBER DEFAULT UnknownValue, -- Портфели (по приоритетам, до 5-х штук, незаданные - UnknownValue (-1) )
                               p_G2           IN NUMBER DEFAULT UnknownValue,
                               p_G3           IN NUMBER DEFAULT UnknownValue,
                               p_G4           IN NUMBER DEFAULT UnknownValue,
                               p_G5           IN NUMBER DEFAULT UnknownValue,

                               p_BC1          IN NUMBER DEFAULT 0, -- Новая балановая стоимость по портфелям (К-во * ТСС)
                               p_BC2          IN NUMBER DEFAULT 0,
                               p_BC3          IN NUMBER DEFAULT 0,
                               p_BC4          IN NUMBER DEFAULT 0,
                               p_BC5          IN NUMBER DEFAULT 0,

                               p_A1           IN NUMBER DEFAULT 0, -- Количество по портфелям для контроля
                               p_A2           IN NUMBER DEFAULT 0,
                               p_A3           IN NUMBER DEFAULT 0,
                               p_A4           IN NUMBER DEFAULT 0,
                               p_A5           IN NUMBER DEFAULT 0,

                               p_OBC1         IN NUMBER DEFAULT 0, -- Старая баласовая стоимость по портфелям (Cтек)
                               p_OBC2         IN NUMBER DEFAULT 0,
                               p_OBC3         IN NUMBER DEFAULT 0,
                               p_OBC4         IN NUMBER DEFAULT 0,
                               p_OBC5         IN NUMBER DEFAULT 0, -- Старая баласносая стоимость по группам списания  (портфелям) (Cтек)

                               p_O1           IN NUMBER DEFAULT 0, -- Сумма переоценки в НацВ по портфелям ( Д)
                               p_O2           IN NUMBER DEFAULT 0,
                               p_O3           IN NUMBER DEFAULT 0,
                               p_O4           IN NUMBER DEFAULT 0,
                               p_O5           IN NUMBER DEFAULT 0
                             );

/**
 * Выполняет переоценку по лотам во временной таблице.
 */
   PROCEDURE RSI_WRTOvervalueLotsTMP( p_OperDate   IN DATE,            -- Дата
                                      p_FIID       IN NUMBER,          -- Выпуск
                                      p_Department IN NUMBER,          -- Филиал
                                      p_ByLnk      IN NUMBER DEFAULT 0, -- Только по лотам покупки из временной таблицы связей
                                      p_RQID       IN NUMBER DEFAULT 0, -- ТО по сделке (при выбытии)
                                      p_NotIsDC    IN NUMBER DEFAULT 1 -- Погашение купона (при выбытии) /* КД*/
                                    );

/**
 * Выполняет отражение сумм переоценки на лотах по данным из временной таблицы.
 */
   PROCEDURE RSI_WRTSaveOvervalueLots( p_OperDate     IN DATE,    -- Дата
                                       p_ID_Operation IN NUMBER,  -- Операция
                                       p_ID_Step      IN NUMBER   -- Шаг операции
                                     );

/**
 * Выполняет глобальную конвертацию лотов.
 */
   PROCEDURE RSI_WRTGlobalConvert( p_DealKind     IN NUMBER,   -- Вид документа операции
                                   p_DealID       IN NUMBER,   -- Идентификатор документа операции
                                   p_OperDate     IN DATE,     -- Дата операции
                                   p_ID_Operation IN NUMBER,   -- Операция
                                   p_ID_Step      IN NUMBER   -- Шаг операции
                                 );

/**
 * Выполняет откат зачисления при глобальной конвертации лотов. Ее нет в проекте
 */
   PROCEDURE RSI_WRTRestoreAfterGlConv( p_ID_Operation IN NUMBER,   -- Операция
                                        p_ID_Step      IN NUMBER,   -- Шаг операции, где корректируются лоты
                                        p_OperDate     IN DATE,     -- Дата операции ГО
                                        p_Department   IN NUMBER    -- Филиал в операции ГО
                                      );

/**
 * Выполняет откат глобальной конвертации лотов и локального перемещения. Ее нет в проекте
 */
   PROCEDURE RSI_WRTRestoreAfterGlAct( p_DocKind      IN NUMBER,   -- Вид документа операции
                                       p_DocID        IN NUMBER,   -- Идентификатор документа операции
                                       p_ID_Operation IN NUMBER,   -- Операция
                                       p_ID_Step      IN NUMBER    -- Шаг операции, где корректируются лоты
                                     );

/**
 * Выполняет списание при глобальной конвертации лотов.
 */
   PROCEDURE RSI_WRTGlobalWriteOff( p_DealKind     IN NUMBER,   -- Вид документа операции
                                    p_DealID       IN NUMBER,   -- Идентификатор документа операции
                                    p_DealCode     IN VARCHAR2, -- Код операции
                                    p_OperDate     IN DATE,     -- Дата списания
                                    p_OldFIID      IN NUMBER,   -- Старая ц/б
                                    p_IsTrust      IN NUMBER,   -- Признак ДУ
                                    p_Department   IN NUMBER,   -- Филиал
                                    p_ID_Operation IN NUMBER,   -- Операция
                                    p_ID_Step      IN NUMBER,   -- Шаг операции
                                    p_CalcIncome   IN NUMBER    -- Расчитывать доход
                                  );

/**
 * Выполняет откат списания при глобальной конвертации лотов. Ее нет в проекте
 */
   PROCEDURE RSI_WRTRestoreAfterGlWrtOff( p_DocKind      IN NUMBER,   -- Вид документа операции
                                          p_DocID        IN NUMBER,   -- Идентификатор документа операции
                                          p_ID_Operation IN NUMBER,   -- Операция
                                          p_ID_Step      IN NUMBER    -- Шаг операции, где корректируются лоты
                                        );

/**
 * Выполняет глобальное перемещение лотов.
 */
   PROCEDURE RSI_WRTGlobalMoving( p_OperDate     IN DATE,
                                  p_FIID         IN NUMBER,
                                  p_Department   IN NUMBER,
                                  p_ID_Operation IN NUMBER,
                                  p_ID_Step      IN NUMBER,
                                  p_S1           IN NUMBER DEFAULT UnknownValue, -- Исходные портфели (до 3-х штук, незаданные - UnknownValue (-1) )
                                  p_S2           IN NUMBER DEFAULT UnknownValue,
                                  p_S3           IN NUMBER DEFAULT UnknownValue,
                                  p_G1           IN NUMBER DEFAULT UnknownValue, -- Целевые портфели (до 3-х штук, незаданные - UnknownValue (-1) )
                                  p_G2           IN NUMBER DEFAULT UnknownValue,
                                  p_G3           IN NUMBER DEFAULT UnknownValue,
                                  p_GG1          IN NUMBER DEFAULT UnknownValue, -- Целевые группы списания, соотв. портфелям (незаданные - UnknownValue (-1) )
                                  p_GG2          IN NUMBER DEFAULT UnknownValue,
                                  p_GG3          IN NUMBER DEFAULT UnknownValue,
                                  p_A1           IN NUMBER, -- Количество по портфелям для контроля
                                  p_A2           IN NUMBER,
                                  p_A3           IN NUMBER,
                                  p_R1           IN NUMBER, -- Признаки списания резерва по портфелям
                                  p_R2           IN NUMBER,
                                  p_R3           IN NUMBER,
                                  p_O1           IN NUMBER, -- Признаки списания переоценки по портфелям
                                  p_O2           IN NUMBER,
                                  p_O3           IN NUMBER,
                                  p_I1           IN NUMBER, -- Признаки списания дохода по портфелям
                                  p_I2           IN NUMBER,
                                  p_I3           IN NUMBER,
                                  p_Action        IN NUMBER,
                                  p_Delivered     IN NUMBER,
                                  p_NotDelivered  IN NUMBER,
                                  p_WithoutAccept IN NUMBER,
                                  p_RecalcDiscount0 IN NUMBER
                                );

/**
 * Выполняет глобальное перемещение лотов портфеля
 * @since 6.20.029
 * @qtest NO
 * @param p_OperDate Дата
 * @param p_FIID Выпуск
 * @param p_Department Филиал
 * @param p_ID_Operation Операция, на которой выполняется сохранение
 * @param p_ID_Step Шаг операции, на которой выполняется сохранение
 * @param p_Source Исходный портфель
 * @param p_Goal Целевой портфель
 * @param p_GoalGroup Целевая группа списания (портфель)
 * @param p_Amount Количество (для контроля)
 * @param p_WOReserv Признак списания резерва
 * @param p_WOOverv Признак списания переоценки
 * @param p_WOInterest Списывать процентный доход
 * @param p_WODiscount Списывать дисконтный доход
 * @param p_WOBonus Списывать премию
 * @param p_Action Вид действия: Глобальное перемещение, Локальное кризисное перемещение
 * @param p_Delivered Отбирать поставленные.
 * @param p_NotDelivered Отбирать непоставленные.
 * @param p_WithoutAccept Отбирать проданные БПП
 * @param p_RecalcDiscount0 Пересчитывать дисконтный доход
 */
   PROCEDURE RSI_WRTMoveLots( p_OperDate     IN DATE,
                              p_FIID         IN NUMBER,
                              p_Department   IN NUMBER,
                              p_ID_Operation IN NUMBER,
                              p_ID_Step      IN NUMBER,
                              p_Source       IN NUMBER,
                              p_Goal         IN NUMBER,
                              p_GoalGroup    IN NUMBER,
                              p_Amount       IN NUMBER,
                              p_WOReserv     IN NUMBER,
                              p_WOOverv      IN NUMBER,
                              p_WOInterest   IN NUMBER,
                              p_WODiscount   IN NUMBER,
                              p_WOBonus      IN NUMBER,
                              p_Action        IN NUMBER,
                              p_Delivered     IN NUMBER,
                              p_NotDelivered  IN NUMBER,
                              p_WithoutAccept IN NUMBER,
                              p_RecalcDiscount0 IN NUMBER
                            );

/**
 * Выполняет локальное перемещение лотов.
 */
   PROCEDURE RSI_WRTLocalMoving( p_SaleID       IN NUMBER,
                                 p_Goal         IN NUMBER, -- Целевой портфель
                                 p_GoalGroup    IN NUMBER, -- Целевая группа списания
                                 p_OperDate     IN DATE,
                                 p_ID_Operation IN NUMBER,
                                 p_ID_Step      IN NUMBER,
                                 WOOverv        IN NUMBER  -- Признак списания переоценки
                               );

/**
 * Выполняет все действия с лотами при отказе от РЕПО.
 */
   PROCEDURE RSI_WRTRejectDeal( p_SumID        IN NUMBER,   -- ID лота 2ч обратного РЕПО
                                p_OperDate     IN DATE,     -- Дата операции
                                p_Portfolio    IN NUMBER,   -- Портфель, куда зачисляем ц/б
                                p_GroupID      IN NUMBER,   -- Группа списания

                                p_Amount       IN NUMBER,   -- Кол-во в лоте 1ч
                                p_Sum          IN NUMBER,   -- Стоимость (без НКД) из панели расчетов
                                p_Currency     IN NUMBER,   -- ВЦ из панели расчетов
                                p_Cost         IN NUMBER,   -- Стоимость (без НКД) из панели расчетов, переведенная в ВН по дате отказа
                                p_NKD          IN NUMBER,   -- НКД из панели расчетов
                                p_BegDiscount  IN NUMBER,   -- Новый начальный дисконт
                                p_BegBonus     IN NUMBER,   -- Новая начальная премия

                                p_ID_Operation IN NUMBER,   -- Операция
                                p_ID_Step      IN NUMBER    -- Шаг операции
                              );

/**
 * Выполняет начисление доходов на лоты в операции начисления ПДД.
 */
   PROCEDURE RSI_WRTChargeIncomToLots( p_OperDate         IN DATE,   -- Дата
                                       p_EndDate          IN DATE,
                                       p_FIID             IN NUMBER, -- Выпуск
                                       p_Department       IN NUMBER, -- Филиал
                                       p_ID_Operation     IN NUMBER, -- Операция и шаг, на которых выполняется сохранение
                                       p_ID_Step          IN NUMBER,
                                       p_Party            IN NUMBER,
                                       p_Contract         IN NUMBER,
                                       p_CalcCorrIntToEIR IN NUMBER
                                     );

/**
 * Выполняет начисление расходов на лоты в операции начисления расхода.
 * @since 6.20.048
 * @qtest NO
 * @param p_OperDate Дата
 * @param p_EndDate Дата окончания периода начисления
 * @param p_FIID ID ц/б
 * @param p_Department Филиал
 * @param p_ID_Operation Операция, на которой выполняется сохранение
 * @param p_ID_Step Шаг, на которой выполняется сохранение
 * @param p_CalcInterest Начисление процентного расхода
 * @param p_CalcDiscount Начисление дисконтного расхода
 * @param p_CalcBonus Начисление премии
 * @param p_CalcDefDiff Начисление отсроченной разницы
 * @param p_CalcOutlay Начисление затрат по договору
 * @param p_CalcCorr Начисление корректировки доя ЭПС
 * @param p_CalcAddInc Начисление доп. дохода
 */
   PROCEDURE RSI_ChargeExpenseToOwnLotsTMP( p_OperDate     IN DATE,
                                            p_EndDate      IN DATE,
                                            p_FIID         IN NUMBER,
                                            p_Department   IN NUMBER,
                                            p_CalcInterest IN NUMBER DEFAULT 1,
                                            p_CalcDiscount IN NUMBER DEFAULT 1,
                                            p_CalcBonus    IN NUMBER DEFAULT 1,
                                            p_CalcDefDiff  IN NUMBER DEFAULT 1,
                                            p_CalcOutlay   IN NUMBER DEFAULT 1,
                                            p_CalcCorr     IN NUMBER DEFAULT 1,
                                            p_CalcAddInc   IN NUMBER DEFAULT 0
                                          );

   PROCEDURE RSI_SaveChargeExpenseToOwnLots ( p_OperDate       IN DATE,
                                              p_ID_Operation   IN NUMBER,
                                              p_ID_Step        IN NUMBER
                                            );

/**
 * Вычисляет долю суммы в ЧП.
 * @since 6.20.029
 * @qtest NO
 * @param p_FIID ID ц/б
 * @param p_Partly Номер ЧП
 * @param p_Sum0 Остаток начальной суммыго дисконта (дисконта, премии, отсроченной разницы)
 * @param p_BuyDate Дата исходной покупки
 * @return Доля суммы
 */
   FUNCTION WRTCalcSumPart( p_FIID         IN NUMBER,
                            p_Partly       IN VARCHAR2,
                            p_Sum0         IN NUMBER,
                            p_BuyDate      IN DATE
                          ) RETURN NUMBER DETERMINISTIC;

/**
 * вычисление сумм дохода по ЧП на лот
 */
   PROCEDURE RSI_WRTCalcPartialIncome( p_SumID                      IN NUMBER,    -- Лот  Репо 2 ч.
                                       p_Partly                     IN VARCHAR2,  -- Номер ЧП.
                                       p_CalcDate                   IN DATE,      -- Дата начисления
                                       p_CurSum                  IN OUT NUMBER,   -- Распределяемая сумма дохода ЧП по сделке
                                       p_CurAmount               IN OUT NUMBER,   -- Распределяемое количество по сделке
                                       p_Portfolio                 OUT NUMBER,    -- Исходный портфель (из которого зачислен лот)
                                       p_IncomeSum                 OUT NUMBER,    -- Сумма дохода по ЧП по лоту
                                       p_DiscountIncomeSum         OUT NUMBER,    -- Доля дисконта Sдоля_дисконта)
                                       p_DiscountIncomeAdd         OUT NUMBER,    -- Доначисленный ДД (SдоначДД)
                                       p_NewCost                   OUT NUMBER,    -- Новая чистая стоимость
                                       p_NewBalanceCost            OUT NUMBER,    -- Новая текущая стоимость
                                       p_NewOutlay                 OUT NUMBER,    -- Новые затраты
                                       p_NewDiscountIncome         OUT NUMBER,    -- Новый ДД
                                       p_Discount0_korr            OUT NUMBER,    -- Коррекция дисконта (использовать как слагаемое для суммы в проводке)
                                       p_NewDiscount0              OUT NUMBER,    -- Новый начальный дисконт (занести в лот)
                                       p_InterestIncomeAdd         OUT NUMBER,    -- Доначисленный ПД
                                       p_NewInterestIncome         OUT NUMBER,    -- Новый ПД
                                       p_BonusAdd                  OUT NUMBER,    -- Доначисленная премия
                                       p_NewBonus                  OUT NUMBER,    -- Новая начисленная премия
                                       p_DefDiffSum                OUT NUMBER,    -- Доля отсроченной разницы, приходящаяся на ЧП
                                       p_AccountedDefDiffAdd       OUT NUMBER,    -- Доначисленная отсроченная разница
                                       p_NewAccountedDefDiff       OUT NUMBER,    -- Новая начисленная отсроченная разница
                                       p_DefDiff0_korr             OUT NUMBER,    -- Корректировка начальной отсроченной разницы
                                       p_NewDefDiff0               OUT NUMBER,    -- Новая начальная отсроченная разница
                                       p_CorrIntToEIRAdd           OUT NUMBER,    -- Доначисленная корректировка % до ЭПС
                                       p_NewCorrIntToEIR           OUT NUMBER     -- Новое значение корректировки % до ЭПС
                                     );

/**
 * вычисление сумм дохода по погашению купона на лот
 */
   PROCEDURE RSI_WRTCalcCouponIncome( p_SumID                      IN NUMBER,    -- Лот  Репо 2 ч.
                                      p_Coupon                     IN VARCHAR2,  -- Номер купона
                                      p_CalcDate                   IN DATE,      -- Дата начисления
                                      p_CurSum                  IN OUT NUMBER,   -- Распределяемая сумма купонного дохода по сделке
                                      p_CurAmount               IN OUT NUMBER,   -- Распределяемое количество по сделке
                                      p_Portfolio                 OUT NUMBER,    -- Исходный портфель (из которого зачислен лот)
                                      p_IncomeSum                 OUT NUMBER,    -- Сумма купонного дохода по лоту
                                      p_NKD                       OUT NUMBER,    -- Сумма уплаченного НКД (НКДупл)
                                      p_InterestIncomeSum         OUT NUMBER,    -- Начисленный ПД (ПДначисл)
                                      p_InterestIncomeAdd         OUT NUMBER,    -- Доначисленный ПД SдоначПД)
                                      p_NewBalanceCost            OUT NUMBER,    -- Новая текущая стоимость
                                      p_BonusAdd                  OUT NUMBER,    -- Доначисленная премия (?(SдоначПр))
                                      p_NewBonus                  OUT NUMBER,    -- Новая премия
                                      p_NewCost                   OUT NUMBER,    -- Новая чистая стоимость
                                      p_DiscountIncomeAdd         OUT NUMBER,    -- Доначисленный дисконт
                                      p_NewDiscountIncome         OUT NUMBER,    -- Новый начисленный дисконт
                                      p_AccountedDefDiffAdd       OUT NUMBER,    -- Доначисленная отсроченная разница
                                      p_NewAccountedDefDiff       OUT NUMBER,    -- Новая начисленная отсроченная разница
                                      p_CorrIntToEIRAdd           OUT NUMBER,    -- Доначисленная корректировка % до ЭПС
                                      p_NewCorrIntToEIR           OUT NUMBER     -- Новое значение корректировки % до ЭПС
                                    );

/**
 * Процедура вычисления сумм дохода при отказе от 2ч сделки прямого РЕПО
 */
   PROCEDURE RSI_WRTCalcIncomeRejectDeal( p_SumID                     IN  NUMBER, -- Лот  Репо 2 ч.
                                          p_CalcDate                  IN  DATE,   -- Дата отказа
                                          p_Portfolio                 OUT NUMBER, -- Исходный портфель (из которого зачислен лот)
                                          p_BonusAdd                  OUT NUMBER, -- Доначисленная премия (?(SдоначПр))
                                          p_NewBonus                  OUT NUMBER, -- Новая премия
                                          p_InterestIncomeAdd         OUT NUMBER, -- Доначисленный ПД SдоначПД)
                                          p_NewInterestIncome         OUT NUMBER, -- Новый ПД
                                          p_DiscountIncomeAdd         OUT NUMBER, -- Доначисленный ДД SдоначДД)
                                          p_NewDiscountIncome         OUT NUMBER, -- Новый ДД
                                          p_NewNotCarryInterestIncome OUT NUMBER, -- Новый не отнесенный на доходы ПД
                                          p_NewNotCarryDiscountIncome OUT NUMBER, -- Новый не отнесенный на доходы ДД
                                          p_NewCost                   OUT NUMBER, -- Новая чистая стоимость
                                          p_NewBalanceCost            OUT NUMBER, -- Новая текущая стоимость
                                          p_NewNotWrtBonus            OUT NUMBER  -- Новая премия, не отнесённая на расходы
                                        );

/**
 * Расчет начисленного дохода при погашении купона/ЧП.
 */
   PROCEDURE RSI_WRTChangeIncomeOnWrt( p_Lnk              IN OUT DPMWRTLNK_DBT%ROWTYPE,
                                       p_BuyLot           IN DPMWRTSUM_DBT%ROWTYPE,
                                       p_CalcInterest     IN BOOLEAN,
                                       p_CalcDiscount     IN BOOLEAN,
                                       p_CalcBonus        IN BOOLEAN,
                                       p_CalcDefDiff      IN BOOLEAN, -- Начислять отсроченную разницу
                                       p_CalcCorrIntToEIR IN BOOLEAN, -- Начислять корректировку процентов до ЭПС
                                       p_Method           IN NUMBER,  -- метод списания
                                       p_CalcDate         IN DATE,    -- дата начисления
                                       p_Discount0_korr   OUT NUMBER, -- коррекция начального дисконта
                                       p_DefDiff0_korr    OUT NUMBER  -- коррекция начальной отсроченной разницы
                                     );

/**
 * Вычисляет списанное к-во и списанную сумму из портфеля на дату.
 * @since 6.20.029
 * @qtest NO
 * @param p_Department Филиал
 * @param p_FIID ID ц/б
 * @param p_Party Владелец ц/б
 * @param p_Contract Договор обслуживания клиента
 * @param p_Portfolio Порфель
 * @param p_CalcDate Дата расчета
 * @param p_Amount Возвращаемый параметр - количество списанных ц/б
 * @param p_OverAmount Возвращаемый параметр - Списанная сумма переоценки ц/б в количестве p_Amount
 */
   PROCEDURE WRTGetSaleOvervalue( p_Department                 IN NUMBER,
                                  p_FIID                       IN NUMBER,
                                  p_Party                      IN NUMBER,
                                  p_Contract                   IN NUMBER,
                                  p_Portfolio                  IN NUMBER,
                                  p_CalcDate                   IN DATE,
                                  p_Amount                    OUT NUMBER,
                                  p_OverAmount                OUT NUMBER

                                );
/**
 * Вычисляет суммы, измененные в сервисной операции. Применяется в операции начисления ПДД.
 * @since 6.20.029 @qtest-YES
 * @param p_ID_Operation ID операции
 * @param p_ID_Step ID шага операции
 * @param p_Action Вид изменения лота
 * @param p_Party Владелец ц/б
 * @param p_Contract Договор обслуживания клиента
 * @param p_FIID ID ц/б
 * @param p_Portfolio Портфель. Если не задан - игнорируется.
 * @param p_Delivered Отбирать поставленные лоты (1 - отбирать, 0 - не отбирать)
 * @param p_WithoutAccept Отбирать проданные БПП (1 - отбирать, 0 - не отбирать)
 * @param p_BalanceCost Возвращаемый параметр - Изменение текущей стоимости
 * @param p_NotCarryInterest Возвращаемый параметр - Изменение не отнесенного на доходы ПД
 * @param p_NotCarryDiscount Возвращаемый параметр - Изменение не отнесенного на доходы ДД
 * @param p_InterestIncome Возвращаемый параметр - Изменение ПД
 * @param p_DiscountIncome Возвращаемый параметр - Изменение ДД
 * @param p_Bonus Возвращаемый параметр - Изменение премии
 * @param p_WriteOffNotCarryIncome Возвращаемый параметр - Списание не отнесенного на доходы ПДД
 * @param p_OldInterestIncome Возвращаемый параметр - Старое значение начисленного ПД
 * @param p_OldDiscountIncome Возвращаемый параметр - Старое значение начисленного ДД
 * @param p_OldBonus Возвращаемый параметр - Старое значение начисленной премии
 * @param p_Amount Возвращаемый параметр - Количество ц/б
 * @param p_NotWrtBonus Возвращаемый параметр - Премия, списанная на расходы в рамках операции начисления ПДД
 */
   PROCEDURE WRTGetServiceFinResult( p_ID_Operation IN NUMBER,
                                     p_ID_Step      IN NUMBER,
                                     p_Action       IN NUMBER,
                                     p_Party        IN NUMBER,
                                     p_Contract     IN NUMBER,
                                     p_FIID         IN NUMBER,
                                     p_Portfolio    IN NUMBER,
                                     p_Delivered    IN NUMBER,
                                     p_WithoutAccept IN NUMBER,
                                     p_BalanceCost            OUT NUMBER,
                                     p_InterestIncome         OUT NUMBER,
                                     p_DiscountIncome         OUT NUMBER,
                                     p_Bonus                  OUT NUMBER,
                                     p_DefDiff                OUT NUMBER,
                                     p_CorrIntToEIR           OUT NUMBER,
                                     p_OldInterestIncome      OUT NUMBER,
                                     p_OldDiscountIncome      OUT NUMBER,
                                     p_OldBonus               OUT NUMBER,
                                     p_Amount                 OUT NUMBER,
                                     p_BonusAmount            OUT NUMBER,
                                     p_DiscountAmount         OUT NUMBER,
                                     p_InterestAmount         OUT NUMBER
                                   );

/**
 * Возвращаются суммы, измененные в операции глобального перемещения.
 * @since 6.20.029
 * @qtest NO
 * @param p_ID_Operation Операция, которая выполнила изменение
 * @param p_ID_Step Шаг операции, выполнившей изменение
 * @param p_Action Вид изменения лота
 * @param p_Source Исходный портфель (группа списания)
 * @param p_Goal Целевой портфель (группа списания)
 * @param p_State Статус лотов (посчитать по БПП)
 * @param p_DocKind Вид документа. Если не задан - инорируется
 * @param p_DocID Идентификатор документа Если не задан DocKind - инорируется
 * @param p_Cost Возвращаемый параметр - Перемещенная чистая стоимость (Счист)
 * @param p_NKD Возвращаемый параметр - Перемещенный НКД (НКД упл.)
 * @param p_BalanceCost Возвращаемый параметр - Перемещенная текущая стоимость (Стек)
 * @param p_InterestIncome Возвращаемый параметр - Перемещенный ПД (Sнач.ПД (зарезерв.)
 * @param p_DiscountIncome Возвращаемый параметр - Перемещенный ДД (Sнач.ДД (зарезерв.)
 * @param p_NotCarryInterest Возвращаемый параметр - Перемещенный не отнесенный на доходы ПД  (ПДне_отн)
 * @param p_NotCarryDiscount Возвращаемый параметр - Перемещенный не отнесенный на доходы ДД  (ДДне_отн)
 * @param p_OverAmount Возвращаемый параметр - Списанная сумма переоценки
 * @param p_ReservAmount Возвращаемый параметр - Списанная сумма резерва
 * @param p_IncomeReserv Возвращаемый параметр - Списанная сумма резерва ПДД
 */
   PROCEDURE WRTGetGlobalMovingFinResult( p_ID_Operation IN NUMBER,
                                          p_ID_Step      IN NUMBER,
                                          p_Action       IN NUMBER,
                                          p_Source       IN NUMBER,
                                          p_Goal         IN NUMBER,
                                          p_State        IN NUMBER,
                                          p_DocKind      IN NUMBER DEFAULT UnknownValue,
                                          p_DocID        IN NUMBER DEFAULT UnknownValue,
                                          p_Cost                   OUT NUMBER,
                                          p_NKD                    OUT NUMBER,
                                          p_BalanceCost            OUT NUMBER,
                                          p_InterestIncome         OUT NUMBER,
                                          p_DiscountIncome         OUT NUMBER,
                                          p_NotCarryInterest       OUT NUMBER,
                                          p_NotCarryDiscount       OUT NUMBER,
                                          p_OverAmount             OUT NUMBER,
                                          p_ReservAmount           OUT NUMBER,
                                          p_IncomeReserv           OUT NUMBER
                                        );

/**
 * Расчет начисленного дохода при списании.
 * @since 6.20.029
 * @qtest NO
 * @param Lnk Входной/Возвращаемый параметр - Буфер связи
 * @param BuyLot Буфер лота покупки
 * @param CalcInterest Расчитывать процентный доход (true - да, false - нет)
 * @param CalcDiscount Расчитывать дисконтный доход (true - да, false - нет)
 * @param CalcBonus Начислять премию (true - да, false - нет)
 * @param CalcDefDiff Начислять отсроченную разницу (true - да, false - нет)
 * @param CalcIntToEIR Начислять корректировку процентов до ЭПС (true - да, false - нет)
 * @param CalcReserve Начислять резерв (true - да, false - нет)
 * @param CalcOverValue Выполнять переоценку (true - да, false - нет)
 * @param Method метод списания
 * @param CalcDate дата начисления
 */
   PROCEDURE WRTChangeIncomeOnSale( Lnk           IN OUT DPMWRTLNK_DBT%ROWTYPE,
                                    BuyLot        IN DPMWRTSUM_DBT%ROWTYPE,
                                    CalcInterest  IN BOOLEAN,
                                    CalcDiscount  IN BOOLEAN,
                                    CalcBonus     IN BOOLEAN,
                                    CalcDefDiff   IN BOOLEAN,
                                    CalcIntToEIR  IN BOOLEAN,
                                    CalcReserve   IN BOOLEAN,
                                    CalcOverValue IN BOOLEAN,
                                    Method        IN NUMBER, -- метод списания
                                    CalcDate      IN DATE    -- дата начисления
                                  );

/**
 * Выполняет списание лота погашения купона или ЧП по лотам покупки заданной группы списания (Аналог RSI_WRTLinkSaleToBuy).
 */
   PROCEDURE RSI_WRTLinkWrt( p_SaleLot          IN OUT DPMWRTSUM_DBT%ROWTYPE,
                             p_Group            IN     NUMBER, -- Группа списания (портфель)
                             p_StartDate        IN     DATE,   -- Дата отбора лотов покупки. Если не задано - игнорируется.
                             p_Method           IN     NUMBER, -- Метод списания
                             p_ID_Operation     IN     NUMBER, -- Операция и шаг, на которых выполняется списание
                             p_ID_Step          IN     NUMBER, --
                             p_LinkKind         IN     NUMBER, -- Вид создаваемой связи
                             p_Action           IN     NUMBER, -- Вид изменения на лоте
                             p_CalcInterest     IN     BOOLEAN, -- Расчитывать процентный доход
                             p_CalcDiscount     IN     BOOLEAN, -- Расчитывать дисконтный доход
                             p_CalcBonus        IN     BOOLEAN, -- Начислять премию
                             p_CalcDefDiff      IN     BOOLEAN, -- Начислять отсроченную разницу
                             p_CalcCorrIntToEIR IN     BOOLEAN, -- Начислять корректировку процентов до ЭСП
                             p_GrpID            IN     NUMBER  -- Группа обработки
                           );

/**
 * Выполняет списание лота погашения купона по лотам размещения.
 * @since 6.20.048
 * @qtest NO
 * @param p_RetLot Входной/Возвращаемый параметр - Буфер лота погашения
 * @param p_ID_Operation ID операции на которой выполняется списание
 * @param p_ID_Step ID шага на котором выполняется списание
 * @param p_LinkKind Вид создаваемой связи
 * @param p_Action Вид изменения на лоте
 * @param p_CalcInterest Расчитывать процентный доход
 * @param p_CalcBonus Расчитывать премию
 * @param p_CalcDefDiff Рассчитывать отложенную разницу
 * @param p_CalcOutlay Рассчитывать существенные затраты
 */
   PROCEDURE RSI_WRTLinkWrtOwn( p_RetLot           IN OUT DPMWRTSUM_DBT%ROWTYPE,
                                p_ID_Operation     IN     NUMBER,
                                p_ID_Step          IN     NUMBER,
                                p_LinkKind         IN     NUMBER,
                                p_Action           IN     NUMBER,
                                p_CalcInterest     IN     BOOLEAN,
                                p_CalcDiscount     IN     BOOLEAN,
                                p_CalcBonus        IN     BOOLEAN,
                                p_CalcDefDiff      IN     BOOLEAN,
                                p_CalcOutlay       IN     BOOLEAN
                              );

/**
 * Возвращает остаток ц/б по портфелю (группе списания) на дату.
 * @since 6.20.029 @qtest-YES
 * @param Department Филиал. Если не задан (UnknounValue(-1)) игнорируется
 * @param FIID ID ц/б
 * @param Party Владелец
 * @param Contract Договор обслуживания
 * @param Portfolio Портфель. Если не задан (UnknounValue(-1)) игнорируется
 * @param pGroup Группа списания (портфель)
 * @param CalcDate Дата расчета
 * @param Delivered Отбирать поставленные (1 - отбирать, 0 - не отбирать)
 * @param NotDelivered Отбирать непоставленные (1 - отбирать, 0 - не отбирать)
 * @param WithoutAccept Отбирать проданные БПП (1 - отбирать, 0 - не отбирать)
 * @param IsTrust Признак ДУ. Если не задан (-1), игнорируется
 * @return Остаток ц/б по портфелю или группе списания на дату.
 */
   FUNCTION WRTGetPortfolioAmount( Department     IN NUMBER,
                                   FIID           IN NUMBER,
                                   Party          IN NUMBER,
                                   Contract       IN NUMBER,
                                   Portfolio      IN NUMBER,
                                   pGroup         IN NUMBER,
                                   CalcDate       IN DATE,
                                   Delivered      IN NUMBER,
                                   NotDelivered   IN NUMBER,
                                   WithoutAccept  IN NUMBER,
                                   IsTrust        IN NUMBER DEFAULT -1
                                 ) RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает остаток ц/б на лотах ОЭБ на дату. Обрабатывает лоты со статусом размещен и выкуплен.
 * @since 6.20.048 @qtest-NO
 * @param Department Филиал. Если не задан (UnknounValue(-1)) игнорируется
 * @param FIID ID ц/б
 * @param CalcDate Дата расчета
 * @param Placed Отбирать размещенные (1 - отбирать, 0 - не отбирать)
 * @param Redeemed Отбирать выкупленные (1 - отбирать, 0 - не отбирать)
 * @return Возвращает остаток ц/б на лотах ОЭБ на дату.
 */
   FUNCTION WRTGetAmountOwn( Department     IN NUMBER,
                             FIID           IN NUMBER,
                             CalcDate       IN DATE,
                             Placed         IN NUMBER,
                             Redeemed       IN NUMBER
                           ) RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает количество и суммы по портфелю на дату. Обрабатывает только поставленные лоты.
 * @since 6.20.029
 * @qtest NO
 * @param Department Филиал. Если не задан (UnknounValue(-1)) игнорируется
 * @param FIID ID ц/б
 * @param Party Владелец
 * @param Contract Договор обслуживания
 * @param Portfolio Портфель. Если не задан (UnknounValue(-1)) игнорируется
 * @param CalcDate Дата расчета
 * @param DealORCB Только лоты покупки по сделкам ОРЦБ (1 - только ОРЦБ, 0 - только не ОРЦБ, -1 - игнорируется)
 * @param Delivered Отбирать поставленные (1 - отбирать, 0 - не отбирать)
 * @param CalcSumm Возвращаемый параметр - выходные значения сумм
 */
   PROCEDURE WRTRestOnDate( Department     IN NUMBER,
                            FIID           IN NUMBER,
                            Party          IN NUMBER,
                            Contract       IN NUMBER,
                            Portfolio      IN NUMBER,
                            CalcDate       IN DATE,
                            DealORCB       IN NUMBER,
                            Delivered      IN NUMBER,
                            CalcSumm       OUT DPMWRTSUM_DBT%ROWTYPE
                          );

/**
 * Возвращает количество и суммы по портфелю на дату с возможностью задания даты отбора лотов покупки. Обрабатывает только поставленные лоты.
 * @since 6.20.029
 * @qtest NO
 * @param Department Филиал. Если не задан (UnknounValue(-1)) игнорируется
 * @param FIID ID ц/б
 * @param Party Владелец
 * @param Contract Договор обслуживания
 * @param Portfolio Портфель. Если не задан (UnknounValue(-1)) игнорируется
 * @param BeginDate Дата отбора лотов покупки. Если не задана - игнорируется
 * @param EndDate Дата окончания расчета
 * @param DealORCB Только лоты покупки по сделкам ОРЦБ (1 - только ОРЦБ, 0 - только не ОРЦБ, -1 - игнорируется)
 * @param Delivered Отбирать поставленные (1 - отбирать, 0 - не отбирать)
 * @param CalcSumm Возвращаемый параметр - выходные значения сумм
 */
   PROCEDURE WRTBuyForPeriod( Department     IN NUMBER,
                              FIID           IN NUMBER,
                              Party          IN NUMBER,
                              Contract       IN NUMBER,
                              Portfolio      IN NUMBER,
                              BeginDate      IN DATE,
                              EndDate        IN DATE,
                              DealORCB       IN NUMBER,
                              Delivered      IN NUMBER,
                              CalcSumm       OUT DPMWRTSUM_DBT%ROWTYPE
                            );

/**
 * Возвращает количество и суммы проданных за период ц/б по портфелю.
 * @since 6.20.029
 * @qtest NO
 * @param Department Филиал. Если не задан (UnknounValue(-1)) игнорируется
 * @param FIID ID ц/б
 * @param Party Владелец
 * @param Contract Договор обслуживания
 * @param Portfolio Портфель. Если не задан (UnknounValue(-1)) игнорируется
 * @param BeginDate Дата создания связи. Если не задана - игнорируется
 * @param EndDate Дата окончания расчета
 * @param DealORCB Только лоты покупки по сделкам ОРЦБ (1 - только ОРЦБ, 0 - только не ОРЦБ, -1 - игнорируется)
 * @param CalcSumm Возвращаемый параметр - выходные значения сумм
 */
   PROCEDURE WRTSaleForPeriod( Department     IN NUMBER,
                               FIID           IN NUMBER,
                               Party          IN NUMBER,
                               Contract       IN NUMBER,
                               Portfolio      IN NUMBER,
                               BeginDate      IN DATE,
                               EndDate        IN DATE,
                               DealORCB       IN NUMBER,
                               CalcSumm       OUT DPMWRTSUM_DBT%ROWTYPE
                             );

/**
 * Возвращает количество и суммы по портфелю (группе списания) на дату.
 * @since 6.20.029 @qtest-YES
 * @param Department Филиал. Если не задан (UnknounValue(-1)) игнорируется
 * @param FIID ID ц/б
 * @param Party Владелец
 * @param Contract Договор обслуживания
 * @param Portfolio Портфель. Если не задан (UnknounValue(-1)) игнорируется
 * @param CalcDate Дата расчета
 * @param CalcTime Время расчета. Если не задано - игнорируется.
 * @param StartDate Дата отбора лотов покупки. Если не задана - игнорируется
 * @param Delivered Отбирать поставленные (1 - отбирать, 0 - не отбирать)
 * @param WithoutAccept Отбирать проданные БПП (1 - отбирать, 0 - не отбирать)
 * @param CalcSumm Возвращаемый параметр - выходные значения сумм
 * @param IsTrust Признак ДУ. Если не задан (-1), игнорируется
 */
   PROCEDURE WRTGetPortfolioCost( Department     IN NUMBER,
                                  FIID           IN NUMBER,
                                  Party          IN NUMBER,
                                  Contract       IN NUMBER,
                                  Portfolio      IN NUMBER,
                                  CalcDate       IN DATE,
                                  CalcTime       IN DATE,
                                  StartDate      IN DATE,
                                  Delivered      IN NUMBER,
                                  WithoutAccept  IN NUMBER,
                                  CalcSumm       OUT DPMWRTSUM_DBT%ROWTYPE,
                                  IsTrust        IN NUMBER DEFAULT -1
                                );

/**
 * Возвращает к-во ц/б в наличии у клиента по операции к погашению, по которым не введены погашения купонов/ЧП/выпусков на дату.
 * @since 6.20.029
 * @qtest NO
 * @param Department Филиал. Если не задан (UnknounValue(-1)) игнорируется
 * @param FIID ID ц/б
 * @param Party Владелец
 * @param Contract Договор обслуживания
 * @param Coupon Номер купона (пустая строка, если нет купона)
 * @param Partly Номер ЧП  (пустая строка, если нет ЧП)
 * @param CalcDate Дата расчета
 * @param StartDate Дата отбора лотов покупки. Если не задана - игнорируется
 * @param DealID ID проверяемой операции погашения
 * @return Остаток ц/б
 */
   FUNCTION WRTGetWrtAmount( Department     IN NUMBER,
                             FIID           IN NUMBER,
                             Party          IN NUMBER,
                             Contract       IN NUMBER,
                             Coupon         IN VARCHAR2,
                             Partly         IN VARCHAR2,
                             CalcDate       IN DATE,
                             StartDate      IN DATE,
                             DealID         IN NUMBER
                           ) RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает к-во ц/б, по которым не выполнен учет купонов или ЧП в прямых Репо. Используется в погашениях купонов/ЧП.
 * @since 6.20.029
 * @qtest NO
 * @param Department Филиал. Если не задан (UnknounValue(-1)) игнорируется
 * @param FIID ID ц/б
 * @param Party Владелец
 * @param Contract Договор обслуживания
 * @param PlanPortfolio Портфель планируемых лотов. Если не задан - игнорируется.
 * @param IsPartial Признак ЧП
 * @param CalcDate Дата расчета
 * @return Количество ц/б
 */
   FUNCTION WRTGetDirectRepoWrtAmount( Department     IN NUMBER,  -- Филиал
                                       FIID           IN NUMBER,  -- ц/б
                                       Party          IN NUMBER,  -- Владелец
                                       Contract       IN NUMBER,  -- Договор обслуживания
                                       PlanPortfolio  IN NUMBER,  -- Портфель планируемых лотов. Если не задан - игнорируется.
                                       IsPartial      IN NUMBER,  -- Признак ЧП
                                       CalcDate       IN DATE     -- Дата расчета
                                     ) RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает списанные суммы с лота списания по группе списания (портфелю)
 * Суммы по списываемым лотам определяется в разрезе портфелей, а не групп списания (хотя списание выполнялось по группам).
 * @since 6.20.029
 * @qtest NO
 * @param SaleID Списываемый лот
 * @param Portfolio Портфель. Если не задан - игнорируется.
 * @param ParentLot Метод учета лота по 1 ч.: 0  - Все лоты, 1 - Кроме лота по 1 ч., 2 -Только лот по 1 ч
 * @param CostBuy Возвращаемый параметр - Списанная чистая стоимость (Счист)
 * @param BalanceCostBuy Возвращаемый параметр - Списанная текущая стоимость
 * @param NKDBuy Возвращаемый параметр - НКД (НКД упл.)
 * @param InterestIncomeBuy Возвращаемый параметр - Списанный начисленный ПД
 * @param DiscountIncomeBuy Возвращаемый параметр - Cписанный начисленный ДД
 * @param NotCarryInterestBuy Возвращаемый параметр - Списанный не отнесенный на доходы ПД  (ПДне_отн)
 * @param NotCarryDiscountBuy Возвращаемый параметр - Списанный не отнесенный на доходы ДД  (ДДне_отн)
 * @param InterestIncomeAdd Возвращаемый параметр - Доначисленный ПД (Sдонач.ПДi)
 * @param DiscountIncomeAdd Возвращаемый параметр - Доначисленный ДД (Sдонач.ДДi)
 * @param NotCarryInterestAdd Возвращаемый параметр - Доначисленный не отнесенный на доходы ПД  (ПДне_отн)
 * @param NotCarryDiscountAdd Возвращаемый параметр - Доначисленный не отнесенный на доходы ДД  (ДДне_отн)
 * @param InterestIncomeSum Возвращаемый параметр - Суммарный начисленный ПД (ПДначисл)
 * @param DiscountIncomeSum Возвращаемый параметр - Суммарный начисленный ДД (ДДначисл)
 * @param NotCarryInterestSum Возвращаемый параметр - Суммарный не отнесенный на доходы ПД (ПДне_отн)
 * @param NotCarryDiscountSum Возвращаемый параметр - Суммарный не отнесенный на доходы ДД (ДДне_отн)
 * @param BalanceCostSum Возвращаемый параметр - Суммарная текущая стоимость (Стек)
 * @param OutlayBuy Возвращаемый параметр - Списанные затраты
 * @param OverAmountBuy Возвращаемый параметр - Списанная сумма переоценки (ПО)
 * @param ReservAmountBuy Возвращаемый параметр - Списанная сумма резерва (Сумма созданного резерва)
 * @param ReservAmountBuy Возвращаемый параметр - Списанная сумма резерва ПДД (Сумма созданного резерва)
 * @param BalanceCostBD Возвращаемый параметр - Списанная стоимость в ОД (Часть остатка счета "- ОД", пропорциональная количеству ц/б, проданно-му из ТП и ППР)
 * @param CostSale Возвращаемый параметр - Проданная чистая стоимость (в ЧП - Sчпл)
 * @param NKDSale Возвращаемый параметр - НКД полученный (в погашении купона - SКуп_л)
 */
   PROCEDURE WRTGetSaleFinResult( SaleID               IN NUMBER,
                                  Portfolio            IN NUMBER,
                                  ParentLot            IN NUMBER,
                                  CostBuy              OUT NUMBER,
                                  BalanceCostBuy       OUT NUMBER,
                                  NKDBuy               OUT NUMBER,
                                  InterestIncomeBuy    OUT NUMBER,
                                  DiscountIncomeBuy    OUT NUMBER,
                                  NotCarryInterestBuy  OUT NUMBER,
                                  NotCarryDiscountBuy  OUT NUMBER,
                                  InterestIncomeAdd    OUT NUMBER,
                                  DiscountIncomeAdd    OUT NUMBER,
                                  NotCarryInterestAdd  OUT NUMBER,
                                  NotCarryDiscountAdd  OUT NUMBER,
                                  InterestIncomeSum    OUT NUMBER,
                                  DiscountIncomeSum    OUT NUMBER,
                                  NotCarryInterestSum  OUT NUMBER,
                                  NotCarryDiscountSum  OUT NUMBER,
                                  BalanceCostSum       OUT NUMBER,
                                  OutlayBuy            OUT NUMBER,
                                  OverAmountBuy        OUT NUMBER,
                                  ReservAmountBuy      OUT NUMBER,
                                  IncomeReservBuy      OUT NUMBER,
                                  BalanceCostBD        OUT NUMBER,
                                  CostSale             OUT NUMBER,
                                  NKDSale              OUT NUMBER
                                ) ;

/**
 * Проверяет, сделка ОРЦБ ?
 * @since 6.20.029
 * @qtest NO
 * @param DealID ID Сделки. Если не задан ( <= 0 или null ) - ф-я возвращает 1.
 * @param ORCB 1 - проверять на ОРЦБ, 0 - проверять на не ОРЦБ. Если не задан ( < 0 или null ) - ф-я возвращает 1.
 * @return 1 - да, 0 - нет
 */
   FUNCTION CheckDealORCB( DealID IN NUMBER, ORCB IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает сумму коррекции дисконта в локальном перемещении (ДДкор).
 * @since 6.20.029
 * @qtest NO
 * @param ID_Operation Операция, которая выполнила изменение
 * @param ID_Step Шаг операции, которая выполнила изменение
 * @param pAction Вид изменения
 * @return Сумма коррекции дисконта
 */
   FUNCTION WRTGetDiscountCorrection( ID_Operation IN NUMBER,
                                      ID_Step IN NUMBER,
                                      pAction IN NUMBER
                                    ) RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает списанные суммы с лота списания по группе списания (портфелю).
 * Суммы по списываемым лотам определяется в разрезе портфелей, а не групп списания (хотя списание выполнялось по группам).
 * @since 6.20.029
 * @qtest NO
 * @param SaleID Списываемый лот
 * @param Portfolio Портфель. Если не задан - игнорируется.
 * @param ParentLot Метод учета лота по 1 ч.: 0  - Все лоты, 1 - Кроме лота по 1 ч., 2 -Только лот по 1 ч
 * @param CostBuy Возвращаемый параметр - Списанная чистая стоимость ( Счист)
 * @param BalanceCostBuy Возвращаемый параметр - Списанная текущая стоимость
 * @param NKDBuy Возвращаемый параметр - НКД (НКД упл.)
 * @param InterestIncomeBuy Возвращаемый параметр - Списанный начисленный ПД
 * @param DiscountIncomeBuy Возвращаемый параметр - Списанный начисленный ДД
 * @param NotCarryInterestBuy Возвращаемый параметр - Списанный не отнесенный на доходы ПД  (ПДне_отн)
 * @param NotCarryDiscountBuy Возвращаемый параметр - Списанный не отнесенный на доходы ДД  (ДДне_отн)
 * @param InterestIncomeAdd Возвращаемый параметр - Доначисленный ПД (Sдонач.ПДi)
 * @param DiscountIncomeAdd Возвращаемый параметр - Доначисленный ДД (Sдонач.ДДi)
 * @param NotCarryInterestAdd Возвращаемый параметр - Доначисленный не отнесенный на доходы ПД  (ПДне_отн)
 * @param NotCarryDiscountAdd Возвращаемый параметр - Доначисленный не отнесенный на доходы ДД  (ДДне_отн)
 * @param InterestIncomeSum Возвращаемый параметр - Суммарный начисленный ПД (ПДначисл)
 * @param DiscountIncomeSum Возвращаемый параметр - Суммарный начисленный ДД (ДДначисл)
 * @param NotCarryInterestSum Возвращаемый параметр - Суммарный не отнесенный на доходы ПД  (ПДне_отн)
 * @param NotCarryDiscountSum Возвращаемый параметр - Суммарный не отнесенный на доходы ДД  (ДДне_отн)
 * @param BalanceCostSum Возвращаемый параметр - Суммарная текущая стоимость (Стек)
 * @param OutlayBuy Возвращаемый параметр - Списанные затраты
 * @param OverAmountBuy Возвращаемый параметр - Списанная сумма переоценки (ПО)
 * @param OverAmountAdd Возвращаемый параметр - Сумма доначисленной переоценки
 * @param OverAmountSum Возвращаемый параметр - Суммарная списываемая переоценка
 * @param ReservAmountBuy Возвращаемый параметр - Списанная сумма резерва (Сумма созданного резерва)
 * @param ReservAmountAdd Возвращаемый параметр - Доначисленный резерв
 * @param ReservAmountSum Возвращаемый параметр - Сумма списываемого резерва
 * @param BalanceCostBD Возвращаемый параметр - Списанная стоимость в ОД (Часть остатка счета "- ОД", пропорциональная количеству ц/б, проданно-му из ТП и ППР)
 * @param CostSale Возвращаемый параметр - Проданная чистая стоимость (в ЧП - Sчпл)
 * @param NKDSale Возвращаемый параметр - НКД полученный (в погашении купона - SКуп_л)
 * @param Amount Возвращаемый параметр - Списанное к-во
 * @param IncomeReservBuy Возвращаемый параметр - Списанная сумма резерва ПДД (Сумма созданного резерва)
 * @param IncomeReservAdd Возвращаемый параметр - Доначисленный резерв по ПДД
 * @param IncomeReservSum Возвращаемый параметр - Списываемая сумма резерва по ПДД
 * @param BonusAdd Возвращаемый параметр - Доначисленная премия (SдоначПрi)
 * @param CostSum Возвращаемый параметр - Суммарная списанная чистая стоимость (в продаже -  Счист, в ЧП  -  ( Счист))
 * @param StartDate Возвращаемый параметр - Дата отбора лотов покупки. Если не задана, игнорируется
 * @param AfterStartDate Возвращаемый параметр - Признак отбора ц/б, купленных после даты отбора. 0 - нет, 1 - да
 * @param BegBonus Возвращаемый параметр - Начальная прамия (Премия0_2)
 * @param OldBonus Возвращаемый параметр - Премия начисленная до перемещения (S_Пр_начисл_1)
 * @param BonusSum Возвращаемый параметр - Суммарная начисленная премия
 * @param BonusRest Возвращаемый параметр - Остаток премии (ост. премии)
 * @param NotWrtBonus Возвращаемый параметр - Сумма списания премии, не списанной на расходы
 * @param AccounttedDefDiffBuy Возвращаемый параметр - Списанная сумма отсроченной разницы
 * @param AccounttedDefDiffAdd Возвращаемый параметр - Доначисленная отсроченная разница
 * @param AccounttedDefDiffSum Возвращаемый параметр - Суммарная списанная отсроченная разница
 * @param CorrValueBuy Возвращаемый параметр - Списанная сумма корректировки СС
 * @param CorrIntToEIRBuy Возвращаемый параметр - Списанная сумма корректировки % до ЭПС
 * @param CorrIntToEIRAdd Возвращаемый параметр - Добавленная корректировка % до ЭСП
 * @param CorrIntToEIRSum Возвращаемый параметр - Суммарная списываемая корректировка % до ЭПС
 * @param EstReserveBuy Возвращаемый параметр - Списанная сумма отсроченного резерва
 * @param EstReserveAdd Возвращаемый параметр - Доначисленный оценочный резерв
 * @param EstReserveSum Возвращаемый параметр - Суммарный списываемый оценочный резерв
 * @param CorrEstReserveBuy Возвращаемый параметр - Списанная сумма корректировки отсроченного резерва
 * @param CorrEstReserveAdd Возвращаемый параметр - Доначисленная корректировка оценочного резерва
 * @param CorrEstReserveSum Возвращаемый параметр - Суммарная списываемая корректировка оценочного резерва
 */
   PROCEDURE WRTGetSaleFinResult_EX( SaleID               IN NUMBER,
                                     Portfolio            IN NUMBER,
                                     ParentLot            IN NUMBER,
                                     CostBuy              OUT NUMBER,
                                     BalanceCostBuy       OUT NUMBER,
                                     NKDBuy               OUT NUMBER,
                                     InterestIncomeBuy    OUT NUMBER,
                                     DiscountIncomeBuy    OUT NUMBER,
                                     NotCarryInterestBuy  OUT NUMBER,
                                     NotCarryDiscountBuy  OUT NUMBER,
                                     InterestIncomeAdd    OUT NUMBER,
                                     DiscountIncomeAdd    OUT NUMBER,
                                     NotCarryInterestAdd  OUT NUMBER,
                                     NotCarryDiscountAdd  OUT NUMBER,
                                     InterestIncomeSum    OUT NUMBER,
                                     DiscountIncomeSum    OUT NUMBER,
                                     NotCarryInterestSum  OUT NUMBER,
                                     NotCarryDiscountSum  OUT NUMBER,
                                     BalanceCostSum       OUT NUMBER,
                                     OutlayBuy            OUT NUMBER,
                                     OverAmountBuy        OUT NUMBER,
                                     OverAmountAdd        OUT NUMBER,
                                     OverAmountSum        OUT NUMBER,
                                     ReservAmountBuy      OUT NUMBER,
                                     ReservAmountAdd      OUT NUMBER,
                                     ReservAmountSum      OUT NUMBER,
                                     BalanceCostBD        OUT NUMBER,
                                     CostSale             OUT NUMBER,
                                     NKDSale              OUT NUMBER,
                                     Amount               OUT NUMBER,
                                     IncomeReservBuy      OUT NUMBER,
                                     IncomeReservAdd      OUT NUMBER,
                                     IncomeReservSum      OUT NUMBER,
                                     BonusAdd             OUT NUMBER,
                                     CostSum              OUT NUMBER,
                                     StartDate            IN DATE   DEFAULT UnknownDate,
                                     AfterStartDate       IN NUMBER DEFAULT 0,
                                     BegBonus             OUT NUMBER,
                                     OldBonus             OUT NUMBER,
                                     BonusSum             OUT NUMBER,
                                     BonusRest            OUT NUMBER,
                                     NotWrtBonus          OUT NUMBER,
                                     AccounttedDefDiffBuy OUT NUMBER,
                                     AccounttedDefDiffAdd OUT NUMBER,
                                     AccounttedDefDiffSum OUT NUMBER,
                                     CorrValueBuy         OUT NUMBER,
                                     CorrIntToEIRBuy      OUT NUMBER,
                                     CorrIntToEIRAdd      OUT NUMBER,
                                     CorrIntToEIRSum      OUT NUMBER,
                                     EstReserveBuy        OUT NUMBER,
                                     EstReserveAdd        OUT NUMBER,
                                     EstReserveSum        OUT NUMBER,
                                     CorrEstReserveBuy    OUT NUMBER,
                                     CorrEstReserveAdd    OUT NUMBER,
                                     CorrEstReserveSum    OUT NUMBER,
                                     HedgCorrSum          OUT NUMBER,
                                     AmortHedgCorrSum     OUT NUMBER
                                   );

/**
 * Выполняет резервирование по одному лоту.
 * @since 6.20.029
 * @qtest NO
 * @param p_OperDate Дата
 * @param p_SumID Лот
 * @param p_ID_Operation Операция, на которой выполняется сохранение
 * @param p_ID_Step Шаг операции, на которой выполняется сохранение
 * @param p_ReservAmount Новый резерв
 * @param p_SetIncomeReserv Признак установки резерва по ПДД
 * @param p_IncomeReserv Новый резерв ПДД
 */
    PROCEDURE RSI_WRTReserveLot( p_OperDate        IN DATE,
                                 p_SumID           IN NUMBER,
                                 p_ID_Operation    IN NUMBER,
                                 p_ID_Step         IN NUMBER,
                                 p_ReservAmount    IN NUMBER,
                                 p_SetIncomeReserv IN NUMBER,
                                 p_IncomeReserv    IN NUMBER
                               );

/**
 * Выполняет резервирование по одному лоту во временную таблицу
 */
   PROCEDURE RSI_WRTReservePortfolioLotsTMP( p_CalcDate         IN DATE,
                                             p_FIID             IN NUMBER,
                                             p_Department       IN NUMBER,
                                             p_Portfolio        IN NUMBER, -- Портфель
                                             p_State            IN NUMBER, -- Статус
                                             p_CalcIncomeReserv IN NUMBER, -- Признак расчета резерва по ПДД
                                             p_ByLnk            IN NUMBER  -- Признак: только по лотам из связи DPMWRTLNK_TMP
                                           );

/**
 * Выполняет резервирование по одному лоту во временную таблицу
 * @param p_CalcDate Дата
 * @param p_FIID Идентификатор ц/б
 * @param p_Department Новый резерв
 * @param p_CalcIncomeReserv Признак расчета резерва по ПДД
 * @param p_ByLnk Признак: только по лотам из связи DPMWRTLNK_TMP. По умолчанию = 0
 */
    PROCEDURE RSI_WRTReserveLotsTMP(p_CalcDate           IN DATE,
                                    p_FIID               IN NUMBER,
                                    p_Department         IN NUMBER,
                                    p_CalcIncomeReserv   IN NUMBER,
                                    p_ByLnk              IN NUMBER DEFAULT 0
                                   );

/**
 * Выполняет формирование оценочного резерва по всем подходящим лотам
 * @param p_CalcDate Дата
 * @param p_FIID Ценная бумага
 * @param p_Department Филиал
 * @param p_ByLnk Признак: только по лотам из связи DPMWRTLNK_TMP. По умолчанию = 0
 */
    PROCEDURE RSI_WRTEstReserveLotsTMP(p_CalcDate    IN DATE,
                                       p_FIID        IN NUMBER,
                                       p_Department  IN NUMBER,
                                       p_ByLnk       IN NUMBER DEFAULT 0
                                      );

/**
 * Выполняет отражение сумм резерва на лотах по данным из временной таблицы
 * @param p_OperDate Дата
 * @param p_ID_Operation Операция, на которой выполняется сохранение
 * @param p_ID_Step Шаг, на котором выполняется сохранение
 */
    PROCEDURE RSI_WRTSaveReserveLots(p_OperDate     DATE,
                                     p_ID_Operation NUMBER,
                                     p_ID_Step      NUMBER);

/**
 * Проверяет, является ли субъект надежным.
 * @since 6.20.029
 * @qtest NO
 * @param PartyID ID Субъекта
 * @param OperDate Дата, на которую определяется надежность субъекта
 * @return 1 - является, 0 - не является
 */
    FUNCTION PT_IsResponsible( PartyID IN NUMBER, OperDate IN DATE ) RETURN NUMBER DETERMINISTIC;

/**
 * Выполняет компенсационную поставку по РЕПО
 * Предпологается что все действия выполняются на одном шаге операции поэтому у нас ID платежа неизвестен
 * будем считать что на этом шаге может быть вставлен только один платеж с видом PM_PURP_COMPENS_BAi поэтому будем брать
 * платеж с максимальным ID и не закрытый
 */
    PROCEDURE RSI_WRTCompensWrt( p_LotS     IN OUT DPMWRTSUM_DBT%ROWTYPE,   -- Вид документа операции
                             p_ID_Operation IN NUMBER,   -- Операция
                             p_ID_Step      IN NUMBER,   -- Шаг операции
                             p_Lot2SumID    IN NUMBER,
                             p_SaleRepoAmount IN NUMBER DEFAULT 0
                           );

/**
 * Кооректировка лота при средневзвешенном списании
 */
    PROCEDURE RSI_WRTAverageRedistrSum( p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER,
                                        p_SaleLot IN DPMWRTSUM_DBT%ROWTYPE,
                                        p_Portfolio IN NUMBER, p_Group IN NUMBER, p_CalcOver IN BOOLEAN );

/**
 * Выполняет откат списания по средневзвешенной по старому алгоритму. Для совместимости со старой реализацией до #165720
 */
    PROCEDURE RSI_WRTRecoilOldRedistrSum( p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER );

/**
 * Исправляется возможная ошибка расчета чистой и балансовой стоимости (выполняется макрос обновления Check_Cost по #157676)
 * @since 6.20.029
 * @qtest NO
 * @param pSumID Лот
 * @param pInstance Состояние лота
 * @param pCost Чистая стоимость лота
 * @param pBalanceCost Балансовая стоимость лота
 * @param pAmount Количество ц/б в лоте
 */
    PROCEDURE SetCostToHist (pSumID IN NUMBER, pInstance IN NUMBER, pCost IN NUMBER, pBalanceCost IN NUMBER, pAmount IN NUMBER);

    PROCEDURE RSI_SetCostToLink( pBuyID IN NUMBER, pAmount IN NUMBER, pPortfolio IN NUMBER, pID_Operation IN NUMBER,
                             pID_Step IN NUMBER, pAction IN NUMBER,
                             pCost IN OUT NUMBER, pBalanceCost IN OUT NUMBER);
    PROCEDURE RSI_SetCostToMovedLot( pBuyID IN NUMBER, pSaleID IN NUMBER, pAction IN NUMBER, pPortfolio IN NUMBER,
                                 pCost IN NUMBER, pBalanceCost IN NUMBER, pOver IN NUMBER );

/**
 * Возвращает значение настройки 'SECUR\CHARGE_BONUS'
 * @since 6.20.030
 * @qtest NO
 * @return значение настройки 'SECUR\CHARGE_BONUS'
 */
    FUNCTION CHARGE_BONUS RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает значение настройки 'SECUR\ДАТА НАЧАЛА НОВОГО БУ РЕПО'
 * @since 6.20.031
 * @qtest NO
 * @return значение настройки 'SECUR\ДАТА НАЧАЛА НОВОГО БУ РЕПО'
 */
    FUNCTION NewRepoDate RETURN DATE DETERMINISTIC;

/**
 * Возвращает значение настройки 'SECUR\SORTING_LOTS_CODE'
 * @since 6.20.031
 * @qtest NO
 * @return значение настройки 'SECUR\SORTING_LOTS_CODE'
 */
    FUNCTION SortingLotsCode RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает значение настройки 'SECUR\SORTING_LOTS_AMOUNT'
 * @since 6.20.031
 * @qtest NO
 * @return значение настройки 'SECUR\SORTING_LOTS_AMOUNT'
 */
    FUNCTION SortingLotsAmount RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает значение настройки 'SECUR\CALC_DISCOUNT_WITHOUT_PARTLY'
 * @since 6.20.031
 * @qtest NO
 * @return значение настройки 'SECUR\CALC_DISCOUNT_WITHOUT_PARTLY'
 */
    FUNCTION CALC_DISCOUNT_WITHOUT_PARTLY RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает значение настройки 'SECUR\МСФО\НАЧИСЛЕНИЕ ПРЕМИИ БЕЗ УЧЕТА ЧП'
 * @since 6.20.031
 * @qtest NO
 * @return значение настройки 'SECUR\МСФО\НАЧИСЛЕНИЕ ПРЕМИИ БЕЗ УЧЕТА ЧП'
 */
    FUNCTION CALC_BONUS_WITHOUT_PARTLY RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает значение настройки 'SECUR\МСФО\УЧЕТ ВАЛЮТНЫХ ДОЛЕВЫХ ЦБ'
 * @since 6.20.048
 * @qtest NO
 * @return значение настройки 'SECUR\МСФО\УЧЕТ ВАЛЮТНЫХ ДОЛЕВЫХ ЦБ'
 */
    FUNCTION AccountingExSecur RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает значение настройки 'SECUR\МСФО\ЭПС ДЛЯ  ДО МЕНЬШЕ ГОДА'
 * @since 6.20.048
 * @qtest NO
 * @return значение настройки 'SECUR\МСФО\ЭПС ДЛЯ  ДО МЕНЬШЕ ГОДА'
 */
    FUNCTION EPSAvrLessThanYear RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает значение настройки 'SECUR\МСФО\ЭПС ДЛЯ ДО С НЕСУЩЕСТВ. ОТКЛ'
 * @since 6.20.048
 * @qtest NO
 * @return значение настройки 'SECUR\МСФО\ЭПС ДЛЯ ДО С НЕСУЩЕСТВ. ОТКЛ'
 */
    FUNCTION EPSAvrNSignDevACEPSACLM RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает значение настройки 'SECUR\CALC_DEFDIFF_WITHOUT_PARTLY'
 * @since 6.20.031
 * @qtest NO
 * @return значение настройки 'SECUR\CALC_DEFDIFF_WITHOUT_PARTLY'
 */
    FUNCTION CALC_DEFDIFF_WITHOUT_PARTLY RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает значение настройки 'SECUR\МСФО\НАЧИСЛЕНИЕ ПДД ПО ДО В ССПУ'
 * @since 6.20.031
 * @qtest NO
 * @return значение настройки 'SECUR\МСФО\НАЧИСЛЕНИЕ ПДД ПО ДО В ССПУ'
 */
    FUNCTION CALC_PDD_BOND_SSPU RETURN NUMBER DETERMINISTIC;


/**
 * Возвращает значение категории для ц/б "Наблюдаемые исходные данные"
 * @since 6.20.031.49
 * @qtest NO
 * @param FIID Идентификатор ц/б
 * @return значение категории для ц/б "Наблюдаемые исходные данные"
 */
    FUNCTION RSI_AvoirAttrObsBaseData( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает значение категории для ц/б "Уровень исходных данных иерархии СС МСФО 13"
 * @since 6.20.031.49
 * @qtest NO
 * @param FIID Идентификатор ц/б
 * @return значение категории для ц/б "Уровень исходных данных иерархии СС МСФО 13"
 */
    FUNCTION RSI_AvoirAttrHSDataLevel( FIID IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Синхронизация реестра новых выпусков
 */
    PROCEDURE RSI_SCDLFISync( pDealKind IN NUMBER, pDealID IN NUMBER, pMode IN NUMBER);

/**
 * Проверяет, заполнен ли в операции реестр новых выпусков.
 * @since 6.20.030
 * @qtest NO
 * @param pDealKind Вид документа операции
 * @param pDealID ID документа операции
 * @param pNum Порядковый номер выпуска в списке новых выпусков по операциии. Если не задан ( передан 0 ) - игнорируется.
 * @param pNewFIID ID ц/б нового выпуска. Если не задан ( передан 0 ) - игнорируется.
 * @return 1 - есть, 0 - нет
 */
    FUNCTION ExistSCDLFI( pDealKind IN NUMBER, pDealID IN NUMBER, pNum IN NUMBER, pNewFIID IN NUMBER) RETURN NUMBER DETERMINISTIC;
/**
 * Промаркировать записи в dscdlfi_tmp к удалению
 */
    PROCEDURE RSI_MarkForDeleteSCDLFI( pDealKind IN NUMBER, pDealID IN NUMBER);
/**
 * синхронизация лотов по операции ГО
 */
    PROCEDURE RSI_SCDLPMWRSync( pDealKind IN NUMBER, pDealID IN NUMBER, pMode IN NUMBER);
/**
 * Удаление из временной таблицы лотов по операции ГО
 */
    PROCEDURE RSI_MarkForDeleteSCDLPMWR( pDealKind IN NUMBER, pDealID IN NUMBER);
/**
 * Заполнить лоты по операции ГО
 */
    PROCEDURE RSI_FillScdlpmwrByDlcomm( pDealKind IN NUMBER, pDealID IN NUMBER, pFIID IN NUMBER,
                                        pCommDate IN DATE, pBegDate IN DATE, pEndDate IN DATE,
                                        pFlag3 IN NUMBER, pDepartment IN NUMBER, pError OUT NUMBER, pOperSubKind IN NUMBER);

/**
 * Проверяет, существуют ли лоты по операции из ddl_comm_dbt
 * @since 6.20.030
 * @qtest NO
 * @param pDealKind Вид документа операции
 * @param pDealID ID документа операции
 * @return 1 - есть, 0 - нет
 */
    FUNCTION ExistLotsByDlComm( pDealKind IN NUMBER, pDealID IN NUMBER) RETURN NUMBER DETERMINISTIC;

/**
 * Заполнить лоты по операции ГО - пользовательская ф-я (обертка внутренней RSI_FillScdlpmwrByDlcomm)
 * @since 6.20.030
 * @qtest NO
 * @param pDealKind Вид документа операции
 * @param pDealID ID документа операции
 * @param pFIID Старый выпуск ц/б
 * @param pCommDate Дата операции
 * @param pBegDate Дата начала начисления ПДД (используется если взведен pFlag3)
 * @param pEndDate Дата - не используется
 * @param pFlag3 Признак необходимости расчета ПДД (1- рассчитать, 0 - не рассчитывать)
 * @param pDepartment Филиал
 * @param pError Возвращающий параметр - Номер ошибки
 * @param pOperSubKind - Подвид ГО
 */
    PROCEDURE FillScdlpmwrByDlcomm( pDealKind IN NUMBER, pDealID IN NUMBER, pFIID IN NUMBER,
                                    pCommDate IN DATE, pBegDate IN DATE, pEndDate IN DATE,
                                    pFlag3 IN NUMBER, pDepartment IN NUMBER, pError OUT NUMBER, pOperSubKind IN NUMBER);

/**
 * Выполняет начисление доходов на лоты во временную таблицу в операции начисления ПДД, погашения, либо при продаже*/
    PROCEDURE RSI_WRTChargeIncomToLotsTMP(p_IsWrtSale          IN BOOLEAN, -- Признак вызова при продаже\погашении или ПДД (true-списание\погашение, false-начисление ПДД)
                                          p_OperDate           IN DATE,   -- Дата
                                          p_EndDate            IN DATE,
                                          p_FIID               IN NUMBER, -- Выпуск
                                          p_Department         IN NUMBER, -- Филиал
                                          p_P1                 IN NUMBER DEFAULT UnknownValue, -- Портфели (по приоритетам, до 5-х штук, незаданные
                                          p_P2                 IN NUMBER DEFAULT UnknownValue,
                                          p_P3                 IN NUMBER DEFAULT UnknownValue,
                                          p_P4                 IN NUMBER DEFAULT UnknownValue,
                                          p_P5                 IN NUMBER DEFAULT UnknownValue,
                                          p_Party              IN NUMBER,
                                          p_Contract           IN NUMBER,
                                          p_CalcInterest       IN BOOLEAN,--начислять ПД
                                          p_CalcDiscount       IN BOOLEAN,--начислять ДД
                                          p_CalcBonus          IN BOOLEAN,--начислять премию
                                          p_CalcDefDiff        IN BOOLEAN, --начисление отсроченной разницы
                                          p_CalcCorrIntToEIR   IN BOOLEAN, --начисление корректировки процентов до ЭПС
                                          p_AmortizationMethod IN NUMBER,--метод списания
                                          p_IsTrust            IN BOOLEAN DEFAULT FALSE,
                                          p_LnkKind            IN NUMBER DEFAULT 0, -- Вид связи списания
                                          p_Coupon             IN VARCHAR2 DEFAULT NULL -- номер купона
                                         );

/**
 * Выполняет начисление расходов на лоты во временную таблицу в операции начисления ПДД, погашения, либо при продаже
 * @since 6.20.048
 * @qtest NO
 * @param p_IsRet        Признак вызова при возвращении ц/б (выкуп, погашение, зачисление) (true-выкуп/зачисление/погашение, false-начисление расхода)
 * @param p_OperDate     Дата
 * @param p_EndDate      Дата окончания периода начисления
 * @param p_FIID         Выпуск
 * @param p_Department   Филиал
 * @param p_CalcInterest начислять ПД
 * @param p_CalcDiscount начислять ДД
 * @param p_CalcBonus    начислять премию
 * @param p_CalcDefDiff  Начисление отсроченной разницы
 * @param p_CalcOutLay   Начисление затрат по сделкам
 * @param p_LnkKind      Вид связи списания
 * @param p_Coupon       номер купона
 * @param p_CalcAddInc   Начислять доп. доход по БИО
 */
    PROCEDURE RSI_WRTChargeExpToOwnLotsTMP(p_IsRet              IN BOOLEAN,
                                           p_OperDate           IN DATE,
                                           p_EndDate            IN DATE,
                                           p_FIID               IN NUMBER,
                                           p_Department         IN NUMBER,
                                           p_CalcInterest       IN BOOLEAN,
                                           p_CalcDiscount       IN BOOLEAN,
                                           p_CalcBonus          IN BOOLEAN,
                                           p_CalcDefDiff        IN BOOLEAN,
                                           p_CalcOutLay         IN BOOLEAN,
                                           p_CalcCorr           IN BOOLEAN,
                                           p_LnkKind            IN NUMBER DEFAULT 0,
                                           p_Coupon             IN VARCHAR2 DEFAULT NULL,
                                           p_CalcAddInc         IN BOOLEAN DEFAULT FALSE
                                          );

/**
 * Выполняет вычисление сумм доходов по лоту покупки через временную таблицу*/
   PROCEDURE RSI_WRTCalcIncomeOnLotTMP( p_SumID              IN NUMBER, -- лот
                                        p_CalcDate           IN DATE,   -- Дата расчетов
                                        p_CalcInterest       IN BOOLEAN, -- Начисление процентного дохода
                                        p_CalcDiscount       IN BOOLEAN, -- Начисление дисконтного дохода
                                        p_CalcBonus          IN BOOLEAN, -- Начисление премии
                                        p_CalcDefDiff        IN BOOLEAN, -- Начисление отсроченной разницы
                                        p_CalcCorrIntToEIR   IN BOOLEAN, -- Начисление корректировки % до ЭПС
                                        p_LinkKind           IN NUMBER DEFAULT 0, -- Константа вида расчёта
                                        p_InterestAdd        OUT NUMBER, -- доначисленный ПД
                                        p_DiscountAdd        OUT NUMBER, -- доначисленный ДД
                                        p_BonusAdd           OUT NUMBER, -- доначисленная премия
                                        p_DefDiffAdd         OUT NUMBER, -- доначисленная отсроченная разница
                                        p_CorrIntToEIRAdd    OUT NUMBER  -- доначисленная корректировка % до ЭПС
                                      );

/**
 * Выполняет вычисление сумм расходов по лоту размещения через временную таблицу
 * @since 6.20.048
 * @qtest NO
 * @param p_SumID        лот
 * @param p_FIID        Идентификатор ц/б
 * @param p_Department        Филиал
 * @param p_CalcDate     Дата расчетов
 * @param p_CalcInterest начислять ПД
 * @param p_CalcDiscount начислять ДД
 * @param p_CalcBonus    начислять премию
 * @param p_CalcDefDiff  начислять отложенную разницу
 * @param p_CalcOutlay   начислять существенные затраты
 * @param p_LinkKind     Константа вида расчёта
 * @param p_InterestAdd  доначисленный ПД
 * @param p_DiscountAdd  доначисленный ДД
 * @param p_BonusAdd     доначисленная премия
 * @param p_DefDiffAdd   доначисленная отложенная разница
 * @param p_OutlayAdd    доначисленные существенные затраты
 * @param p_VatOutlayAdd доначисленный НДС по существенным затратам
 */
   PROCEDURE RSI_WRTCalcExpenseOnOwnLotTMP( p_SumID              IN NUMBER, -- лот. Если указано 0, то считаем для всех размещенных лотов
                                            p_FIID               IN NUMBER, --Идентификатор ц/б
                                            p_Department         IN NUMBER, --Филиал
                                            p_CalcDate           IN DATE,   -- Дата расчетов
                                            p_CalcInterest       IN NUMBER, -- Начисление процентного дохода
                                            p_CalcDiscount       IN NUMBER, -- Начисление дисконтного дохода
                                            p_CalcBonus          IN NUMBER, -- Начисление премии
                                            p_CalcDefDiff        IN NUMBER, -- Начисление отложенной разницы
                                            p_CalcOutlay         IN NUMBER, -- Начисление существенных затрат
                                            p_LinkKind           IN NUMBER DEFAULT 0, -- Константа вида расчёта
                                            p_Coupon             IN VARCHAR2 DEFAULT NULL, -- номер купона
                                            p_InterestAdd        OUT NUMBER, -- доначисленный ПД
                                            p_DiscountAdd        OUT NUMBER, -- доначисленный ДД
                                            p_BonusAdd           OUT NUMBER, -- доначисленная премия
                                            p_DefDiffAdd         OUT NUMBER, -- доначисленная отложенная разница
                                            p_OutlayAdd          OUT NUMBER, -- доначисленные существенные затраты
                                            p_VatOutlayAdd       OUT NUMBER  -- доначисленный НДС по существенным затратам
                                          );

/**
  * Выполняет обработку клиентского лота в сервисной операции БУ, либо на шаге операции (ГО, ИН, конвертация акций в ДР).
  */
    PROCEDURE RSI_WRTSetClientLot( p_FIID IN NUMBER,
                                   p_Department IN NUMBER,
                                   p_Party IN NUMBER,
                                   p_Contract IN NUMBER,
                                   p_Amount IN NUMBER,
                                   p_FactDate IN DATE,
                                   p_ID_Operation IN NUMBER,
                                   p_ID_Step IN NUMBER,
                                   p_CheckRest IN NUMBER);

/**
  * Выполняет удаление нулевых лотов, объединяет лоты с равными датами и количествами, проверяет отрицательные остатки.
  */
    PROCEDURE RSI_WRTDelAndCheckClientLots(p_FIID IN NUMBER,
                                           p_Department IN NUMBER,
                                           p_Party IN NUMBER,
                                           p_Contract IN NUMBER,
                                           p_ID_Operation IN NUMBER,
                                           p_ID_Step IN NUMBER,
                                           p_CheckRest IN NUMBER);



/**
  * Выполняет квитовку клиентских лотов  в сервисной операции БУ.
  */
    PROCEDURE RSI_WRTLinkClientLots(p_DocumentID IN NUMBER,
                                    p_Date IN DATE,
                                    p_GrpID IN NUMBER
                                   );

/**
  * Выполняет откат квитовки клиентских лотов в сервисной операции БУ.
  */
    PROCEDURE RSI_WRTRecoilLinkClientLots(p_DocumentID IN NUMBER,
                                          p_GrpID IN NUMBER
                                         );

/**
  * Выполняет квитовку лотов Нашего Банка в сервисной операции БУ.
  */
    PROCEDURE RSI_WRTLinkOurLots(p_DocumentID IN NUMBER,
                                 p_OperDate IN DATE,
                                 p_GrpID IN NUMBER,
                                 p_UseContr IN NUMBER
                                 );

/**
 * Выполняет обработку лотов Нашего Банка по сделкам ОЭБ в сервисной операции БУ.
 * @since 6.20.048
 * @qtest NO
 * @param p_DocumentID   операция БУ
 * @param p_OperDate     дата операции БУ
 * @param p_GrpID        группа обработки
 */
    PROCEDURE RSI_WRTLinkOurLotsOwn( p_DocumentID IN NUMBER,
                                     p_OperDate IN DATE,
                                     p_GrpID IN NUMBER
                                   );

/**
  * Выполняет откат обработки лотов Нашего Банка в сервисной операции БУ, в том числе и лотов ОЭБ.
  */
    PROCEDURE RSI_WRTRecoilLinkOurLots(p_DocumentID IN NUMBER,
                                       p_GrpID IN NUMBER
                                      );

/**
  * создание scdlpmwr на шаге операции.
  */
    PROCEDURE RSI_CreateSCDLPMWR(RecDlpmwr IN RAW);

/**
  * откат создание scdlpmwr на шаге операции
  */
    PROCEDURE RSI_RecoilSCDLPMWR(DealKind IN NUMBER, DealID IN NUMBER, SumID IN NUMBER, NewFIID IN NUMBER, Party IN NUMBER);

/**
  * Возвращает остаток ц/б по клиенту на дату. Возможен расчёт по всем клиентам/договорам
  */
    FUNCTION WRTGetAmountByClient( Department     IN NUMBER,
                                   FIID           IN NUMBER,
                                   Party          IN NUMBER,
                                   Contract       IN NUMBER,
                                   CalcDate       IN DATE
                                 ) RETURN NUMBER DETERMINISTIC;

/**
  * Возвращает остаток ц/б по клиенту на дату, в обратном РЕПО. Возможен расчёт по всем клиентам/договорам
  */
    FUNCTION WRTGetAmountByClientInBackRepo( Department     IN NUMBER,
                                             FIID           IN NUMBER,
                                             Party          IN NUMBER,
                                             Contract       IN NUMBER,
                                             CalcDate       IN DATE
                                           ) RETURN NUMBER DETERMINISTIC;

/**
  * Возвращает количество ц/б по клиенту на дату по всем ведённым операциям погашения со схожии параметрами. Возможен расчёт по всем клиентам/договорам.
  */
    FUNCTION WRTGetAmountFromRetire( Department     IN NUMBER,
                                     FIID           IN NUMBER,
                                     Party          IN NUMBER,
                                     Contract       IN NUMBER,
                                     Coupon         IN VARCHAR2,
                                     Partly         IN VARCHAR2,
                                     DealID         IN NUMBER,
                                     Registrar      IN NUMBER DEFAULT 0,
                                     OnlyExecAcc    IN NUMBER DEFAULT 0
                                   ) RETURN NUMBER DETERMINISTIC;

/**
 * Возвращает количество ц/б на дату по всем ведённым операциям погашения ОЭБ со схожии параметрами.
 * @since 6.20.048 @qtest-NO
 * @param Department Филиал. Если не задан (UnknounValue(-1)) игнорируется
 * @param FIID ID ц/б
 * @param Coupon Номер купона. Если не задан игнорируется
 * @param DealID  Если не задан (UnknounValue(-1)) игнорируется
 * @param OnlyExecAcc Только с исполенным БУ
 * @param RetAddIncome Признак отбора операций погашения допю дохода
 * @return Количество ц/б на дату.
 */
    FUNCTION WRTGetAmountFromRetireOwn( Department     IN NUMBER,
                                        FIID           IN NUMBER,
                                        Coupon         IN VARCHAR2,
                                        DealID         IN NUMBER,
                                        OnlyExecAcc    IN NUMBER DEFAULT 0,
                                        RetAddIncome   IN NUMBER DEFAULT 0
                                      ) RETURN NUMBER DETERMINISTIC;

/**
 * Получить группу списания по портфелю
 * @since 6.20.031
 * @qtest NO
 * @param p_Portfolio Портфель
 * @return Группа списания
 */
   FUNCTION RSI_GetWrtoffGroupByPortfolio(p_Portfolio IN NUMBER, p_IsAvrWrt IN NUMBER DEFAULT 0) RETURN NUMBER;

/**
 * Начислить доход по лотам во временную таблицу для последующего извлечения сумм по лотам для отчета "Оценка портфеля"
 * @since 6.20.031
 * @qtest NO
 * @param p_CalcDate     Дата
 * @param p_FIID         Выпуск
 * @param p_Department   Филиал
 * @param p_P1           Портфель
 * @param p_P2           Портфель
 * @param p_P3           Портфель
 * @param p_P4           Портфель
 * @param p_P5           Портфель
 * @param p_Party        Владелец
 * @param p_Contract     Договор с владельцем
 * @param p_CalcInterest начислять ПД
 * @param p_CalcDiscount начислять ДД
 * @param p_CalcBonus    начислять премию
 */
   PROCEDURE WRTChargeIncomToLotsTMP_Rep( p_CalcDate      IN DATE,
                                          p_FIID          IN NUMBER,
                                          p_Department    IN NUMBER,
                                          p_P1            IN NUMBER DEFAULT UnknownValue,
                                          p_P2            IN NUMBER DEFAULT UnknownValue,
                                          p_P3            IN NUMBER DEFAULT UnknownValue,
                                          p_P4            IN NUMBER DEFAULT UnknownValue,
                                          p_P5            IN NUMBER DEFAULT UnknownValue,
                                          p_Party         IN NUMBER,
                                          p_Contract      IN NUMBER,
                                          p_CalcInterest  IN INTEGER,
                                          p_CalcDiscount  IN INTEGER,
                                          p_CalcBonus     IN INTEGER
                                        );



/**
 * Функция вычисления суммы начального дисконта или премии по лоту
 * @since 6.20.031
 * @qtest NO
 * @param p_Leg           Транш сделки по 1ч
 * @param p_Cost          Чистая стоимость Cчист
 * @param p_Date          Дата поставки
 * @param p_CostPFI       ССПФИ
 * @param p_FaceValueFI   Валюта номинала
 * @return Начальная премия/дисконт для лота
 */
   FUNCTION RSI_WRTCalcBegDiscountOrBonus( p_Leg IN ddl_leg_dbt%ROWTYPE,
                                           p_Cost IN NUMBER,
                                           p_Date IN DATE,
                                           p_CostPFI IN NUMBER,
                                           p_FaceValueFI IN NUMBER
                                         ) RETURN NUMBER;



/**
 * Откат создания денежных параметров сделки ПРЕПО
 */
   PROCEDURE RSI_WRTRestoreCurrParmREPO( p_DealID       IN NUMBER,
                                                 p_AttrID       IN NUMBER,
                                                 p_ID_Operation IN NUMBER,
                                                 p_ID_Step      IN NUMBER
                                               );

/**
 * Создание денежных параметров сделки ПРЕПО
 */
   PROCEDURE RSI_WRTCreateCurrParmREPO( p_DealID        IN NUMBER,
                                        p_ID_Operation  IN NUMBER,
                                        p_ID_Step       IN NUMBER,
                                        p_AmortCalcKind IN NUMBER DEFAULT 0,
                                        p_TestAttrID    IN NUMBER DEFAULT 0,
                                        p_FairValueRub  IN NUMBER DEFAULT UnknownValue
                                      );

/**
 * Создание денежных параметров сделки ПРЕПО при массовом выполнении
 */
   PROCEDURE RSI_Mass_CreateCurrParmREPO;

/**
 * Выполняет начисление по сделке РЕПО на шаге операции начисления расходов/доходов
 * @since 6.20.031.48
 * @qtest NO
 * @param p_OperDate Дата операции
 * @param p_EndDate  Дата окончания периода начисления
 * @param p_DealID   Идентификатор сделки
 * @param p_SumPerc  Сумма доначисленных процентов в дату EndDate в НацВ
 */
   PROCEDURE RSI_WRTCalcDealParmREPO( p_OperDate IN DATE,
                                      p_EndDate  IN DATE,
                                      p_DealID   IN NUMBER,
                                      p_SumPerc  IN NUMBER
                                    );

/**
 * Выполняет отражение начисления в денежных параметрах РЕПО в операции начисления доходов/расходов
 */
   PROCEDURE RSI_WRTSetDealParmREPO( p_OperDate     IN DATE,
                                     p_EndDate      IN DATE,
                                     p_DealID       IN NUMBER,
                                     p_ID_Operation IN NUMBER,
                                     p_ID_Step      IN NUMBER
                                   );

/**
 * Выполняет начисление по сделке РЕПО во временную таблицу при выполнении СОБУ
 * @since 6.20.031.48
 * @qtest NO
 * @param p_IsWrt    Признак полного списания (1 - да, 0 - нет)
 * @param p_SumPerc  Сумма доначисленных процентов в дату EndDate в НацВ
 * @param p_EndDate  Дата окончания периода начисления
 * @param p_DealID   Идентификатор сделки
 * @param p_GrDealID Идентифкатор строки графика
 */
   PROCEDURE RSI_WRTAccrueParmREPOTmp( p_IsWrt    IN NUMBER,
                                       p_SumPerc  IN NUMBER,
                                       p_EndDate  IN DATE,
                                       p_DealID   IN NUMBER,
                                       p_GrDealID IN NUMBER DEFAULT 0
                                     );

/**
 * Выполняет откат начисления в денежных параметрах РЕПО в СОБУ
 */
   PROCEDURE RSI_WRTRestoreParmREPOAcc( p_DocumentID IN NUMBER,
                                        p_GrpID      IN NUMBER
                                      );

/**
 * Выполняет отражение начисления в денежных параметрах РЕПО в СОБУ
 */
   PROCEDURE RSI_WRTSaveParmREPOAcc( p_OperDate     IN DATE,
                                     p_GrpID        IN NUMBER,
                                     p_ID_Operation IN NUMBER,
                                     p_ID_Step      IN NUMBER
                                   );

/**
 * Получить последнее учтенное значение СС ПФИ по сделке
 */
   FUNCTION RSI_GetLastAccountedFrVal( DealID IN NUMBER, BofficeKind IN NUMBER, OnDate IN DATE ) RETURN NUMBER DETERMINISTIC;

/**
 * Выполняет перемещение лотов портфеля
 * @since 6.20.031.55
 * @qtest NO
 * @param p_DocumentID Идентификатор сервисной операции
 * @param p_ID_Operation Операция, на которой выполняется сохранение
 * @param p_ID_Step Шаг операции, на которой выполняется сохранение
 */
   PROCEDURE RSI_WRTExecMoving( p_DocumentID   IN NUMBER,
                                p_ID_Operation IN NUMBER,
                                p_ID_Step      IN NUMBER
                              );


/**
 * Получить сумму по объекту из dlsum
 * @since 6.20.031.57
 * @qtest NO
 * @param p_DocKind Вид объекта
 * @param p_DocID Идентификатор объекта
 * @param p_SumKind Вид суммы
 * @param p_ByDate Даза, за которую искать (точное совпадение)
 */
   FUNCTION GetDealSum(p_DocKind NUMBER, p_DocID NUMBER, p_SumKind IN NUMBER, p_ByDate IN DATE DEFAULT UnknownDate) RETURN NUMBER;

/**
 * Выполнить наполнение таблицы данных для скроллинга лотов на дату
 * @since 6.20.031.64
 * @qtest NO
 * @param p_FIID Бумага
 * @param p_Date Дата, по состоянию на конец которой отбираем данные
 */
   PROCEDURE RSI_WRTFillTmpByFIID( p_FIID IN NUMBER,
                                   p_Date IN DATE
                                 );

/**
 * Получить идентификатор сделки по лоту
 * @since 6.20.031.64
 * @qtest NO
 * @param p_SumID Идентификатор лота
 */
   FUNCTION GetDealByLot(p_SumID IN NUMBER) RETURN NUMBER;

/**
  * Процедура расчета сумм доначисления комиссий по собственным облигациям во временную таблицу
  */
  PROCEDURE RSI_СalcOwnFiComTMP(p_FIID IN NUMBER, p_OperDate IN DATE, p_CalcDate IN DATE, p_FullWrt IN NUMBER );

/**
 * Процедура начисления корректировки хеджирования по лотам во временную таблицу
 * @since 6.20.031.74
 * @qtest NO
 * @param p_OperDate Дата операции
 * @param p_FIID Идентифиуатор ц/б
 * @param p_Department Филиал
 * @param p_AddHedgCorr Сумма добавленной корректировки хеджирования
 * @param p_CorrCurrecy Валюта добавленной суммы
 */
  PROCEDURE RSI_AddHedgCorrToLotsTMP(p_OperDate IN DATE,
                                     p_FIID IN NUMBER,
                                     p_Department IN NUMBER,
                                     p_AddHedgCorr IN NUMBER,
                                     p_CorrCurrency IN NUMBER
                                    );

/**
 * Процедура амортизации корректировки хеджирования по лотам во временную таблицу
 * @since 6.20.031.74
 * @qtest NO
 * @param p_OperDate Дата операции
 * @param p_FIID Идентифиуатор ц/б
 * @param p_Department Филиал
 * @param p_WrtAmortHedgCorr Сумма списания корректировки хеджирования к амортизации
 * @param p_CorrCurrecy Валюта добавленной суммы
 */
  PROCEDURE RSI_WrtAmortHedgCorrToLotsTMP(p_OperDate IN DATE,
                                          p_FIID IN NUMBER,
                                          p_Department IN NUMBER,
                                          p_WrtAmortHedgCorr IN NUMBER,
                                          p_CorrCurrency IN NUMBER
                                         );

/**
 * Процедура сохранения корректировок хеджирования по лотам из временной таблицы
 * @since 6.20.031.74
 * @qtest NO
 * @param p_OperDate Дата операции
 * @param p_ID_Operation Идентифиуатор операции
 * @param p_ID_Step Идентификатор шага
 */
  PROCEDURE RSI_WRTSaveHedgCorrLots( p_OperDate     IN DATE,  
                                     p_ID_Operation IN NUMBER,
                                     p_ID_Step      IN NUMBER 
                                   );

/**
  * Получить пол лоту идентификатор исходного лота приобретения с учетом множетсвенных продаж БПП 
  * @since 6.20.031.78
  * @qtest NO
  * @param p_SumID     Идентификатор лота, для которого ищем исходный лот приобретения
  * @return Идентификатор исходного лота приобретения
  */
  FUNCTION GetFirstSumID(p_SumID IN NUMBER) RETURN NUMBER; 


END RSB_PMWRTOFF;
/
