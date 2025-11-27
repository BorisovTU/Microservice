/**
 * Пакет RSB_BILL для получения основных параметров состояния векселя в различных модулях БО */
CREATE OR REPLACE PACKAGE RSB_BILL AS

/** Продажа */
  VSORDLNK_K_SALE CONSTANT NUMBER := 0;
/** Покупка */
  VSORDLNK_K_BUY  CONSTANT NUMBER := 1;

/** Изменить учтенный вексель*/
  DL_VSBNRBCK                  CONSTANT NUMBER := 191;
/** Договор продажи векселей*/
  DL_VSSALE                    CONSTANT NUMBER := 124;
/** Договор хранения векселей*/
  DL_VSSTORAGEORDER            CONSTANT NUMBER := 114;
/** Договор мены векселей*/
  DL_VSBARTERORDER             CONSTANT NUMBER := 113;
/** Базис расчета Act/Act*/
  BASIS_ACT_ACT                CONSTANT NUMBER := 4;
/** Базис расчета 365/Act*/
  BASIS_365_ACT                CONSTANT NUMBER := 8;

/** Вид сделки с векселями - Сделка с учтенными векселями */
  DL_VEKSELACCOUNTED CONSTANT NUMBER := 141;
/** Вид сделки с векселями - Списание/зачисление учтенных векселей */
  DL_VAENWR CONSTANT NUMBER          := 144;
/** Вид сделки с векселями - Сделка с учтенными векселями в ДУ */
  DL_VATRUST CONSTANT NUMBER         := 147;
/** Вид сделки с векселями - Заявление на ввод каритала */
  TS_DOC_DECLAR CONSTANT NUMBER      := 906;

/** Формулировка вексельного срока - На определенный день */
  VS_TERMF_FIXEDDAY CONSTANT NUMBER := 10;
/** Формулировка вексельного срока - Во столько-то времени от составления */
  VS_TERMF_INATIME  CONSTANT NUMBER := 15;
/** Формулировка вексельного срока - По предъявлении */
  VS_TERMF_ATSIGHT  CONSTANT NUMBER := 20;
/** Формулировка вексельного срока - Во столько-то времени предъявления */
  VS_TERMF_DURING   CONSTANT NUMBER := 30;

/** Ценовые условия - Условия/транш сделки */
  LEG_KIND_DL_TICK       CONSTANT NUMBER := 0;
/** Ценовые условия - Ценовые условия векселя */
  LEG_KIND_VSBANNER      CONSTANT NUMBER := 1;
/** Ценовые условия - Условия/транш для обратной сделки */
  LEG_KIND_DL_TICK_BACK  CONSTANT NUMBER := 2;
/** Ценовые условия - Ценовые условия депозитного сертификата */
  LEG_KIND_DL_DEPOCERT   CONSTANT NUMBER := 3;

/** Статус учтенного векселя - Введен */
  VABANNER_STATUS_INPUT   CONSTANT NUMBER :=  0;
/** Статус учтенного векселя - Учтен */
  VABANNER_STATUS_ACCOUNT CONSTANT NUMBER :=  100;
/** Статус учтенного векселя - Погашен */
  VABANNER_STATUS_ENDED   CONSTANT NUMBER :=  200;

/** Статус собственного векселя - Отложен */
  VSBANNER_STATUS_PREP   CONSTANT NUMBER :=  0;
/** Статус собственного векселя - Оплачен */
  VSBANNER_STATUS_PAYED  CONSTANT NUMBER :=  5;
/** Статус собственного векселя - Оформлен */
  VSBANNER_STATUS_FORMED CONSTANT NUMBER :=  10;

/** Вид анкеты векселя - Просто анкета векселя */
  VSBANNER_FORMKIND_SIMPLE   CONSTANT NUMBER := 1;
/** Вид анкеты векселя - Анкета сертификата */
  VSBANNER_FORMKIND_CERT     CONSTANT NUMBER := 2;
/** Вид анкеты векселя - Анкета векселя для инвентарной карточки */
  VSBANNER_FORMKIND_INVCARD  CONSTANT NUMBER := 20;
/** Вид анкеты векселя - Обеспечение кредита */
  VSBANNER_FORMKIND_SECURITY CONSTANT NUMBER := 21;
/** Вид анкеты векселя - Анкета для описи сертификата */
  VSBANNER_FORMKIND_CERTMOVE CONSTANT NUMBER := 22;

/** Тип записи для формы 711 - Все */
  BILLKIND_ALL       CONSTANT NUMBER := 0;
/** Тип записи для формы 711 - Учтенные */
  BILLKIND_ACCOUNTED CONSTANT NUMBER := 1;
/** Тип записи для формы 711 - Собственные */
  BILLKIND_OUR       CONSTANT NUMBER := 2;
/** Тип записи для формы 711 - Прочие */
  BILLKIND_OTHER     CONSTANT NUMBER := 3;

/** Тип векселя - Простой */
  VS_TYPE_SIMPLE   CONSTANT NUMBER := 0;
/** Тип векселя - Переводной */
  VS_TYPE_TRANSFER CONSTANT NUMBER := 1;

/** Роль агента обращения - Авалист */
  IRCAG_ROLE_AVALIST    CONSTANT NUMBER :=  1;
/** Роль агента обращения - Авалист */
  IRCAG_ROLE_ACCEPTANT  CONSTANT NUMBER :=  2;
/** Роль агента обращения - Индоссант */
  IRCAG_ROLE_INDOSSANT  CONSTANT NUMBER :=  3;
/** Роль агента обращения - Индоссат */
  IRCAG_ROLE_INDOSSAT   CONSTANT NUMBER :=  4;
/** Роль агента обращения - Цедент */
  IRCAG_ROLE_CEDENT     CONSTANT NUMBER :=  5;
/** Роль агента обращения - Цессионарий */
  IRCAG_ROLE_CESSION    CONSTANT NUMBER :=  6;
/** Роль агента обращения - Залогодатель */
  IRCAG_ROLE_PAWNER     CONSTANT NUMBER :=  7;
/** Роль агента обращения - Оценщик */
  IRCAG_ROLE_ESTIMATOR  CONSTANT NUMBER :=  8;
/** Роль агента обращения - Залогодержатель закладной */
  IRCAG_ROLE_PAWNKEEPER CONSTANT NUMBER :=  9;
/** Роль агента обращения - Законный владелец */
  IRCAG_ROLE_OWNER      CONSTANT NUMBER :=  10;
/** Роль агента обращения - Законный владелец */
  IRCAG_ROLE_TRUSTER    CONSTANT NUMBER :=  11;
/** Роль агента обращения - Номинальный держатель */
  IRCAG_ROLE_HOLDER     CONSTANT NUMBER :=  12;
/** Роль агента обращения - Регистратор органа регистрации */
  IRCAG_ROLE_REGISTER   CONSTANT NUMBER :=  13;
/** Роль агента обращения - Другое */
  IRCAG_ROLE_OTHER      CONSTANT NUMBER :=  14;
/** Роль агента обращения - Депозитарий */
  IRCAG_ROLE_DEPOSITARY CONSTANT NUMBER :=  15;

/** Вид записи о доходе по векселям (dvsincome_dbt.t_IncomeType) - Начисление ПДД */
  VSINCOMETYPE_CHARGE       CONSTANT NUMBER := 0;
/** Вид записи о доходе по векселям (dvsincome_dbt.t_IncomeType) - Переоценка НВПИ */
  VSINCOMETYPE_OVERVALUE    CONSTANT NUMBER := 1;
/** Вид записи о доходе по векселям (dvsincome_dbt.t_IncomeType) - Движение по счету "Учтенные векселя" */
  VSINCOMETYPE_ACCOUNTEDSUM CONSTANT NUMBER := 2;
/** Вид записи о доходе по векселям (dvsincome_dbt.t_IncomeType) - Движение по счету "Премия вексель" */
  VSINCOMETYPE_BONUSSUM     CONSTANT NUMBER := 3;
/** Вид записи о доходе по векселям (dvsincome_dbt.t_IncomeType) - корректировка ПДД по ЭПС */
  VSINCOMETYPE_EPRPERC      CONSTANT NUMBER := 5;
/** Вид записи о доходе по векселям (dvsincome_dbt.t_IncomeType) - Отсроченная разница */
  VSINCOMETYPE_DEFDIF       CONSTANT NUMBER := 8;
/** Вид записи о доходе по векселям (dvsincome_dbt.t_IncomeType) - Начальный дисконт */
  VSINCOMETYPE_FIRSTDISC    CONSTANT NUMBER := 9;

/** Действие с записями во временном файле связей - Ничего не делать */
  VSORDLNKACTION_NOACTION   constant number := 0;
/** Действие с записями во временном файле связей - Новая запись */
  VSORDLNKACTION_INSERT     constant number := 1;
/** Действие с записями во временном файле связей - Запись для обновления */
  VSORDLNKACTION_UPDATE     constant number := 2;
/** Действие с записями во временном файле связей - Запись для удаления */
  VSORDLNKACTION_DELETE     constant number := 3;

/** Статус собственного векселя - выдан*/
  VSBANNER_STATUS_SENDED    constant number := 20;

/** Дата начала начислений согласно разъяснения ЦБ РФ*/
  DL_INCOMEDATETYPE_CBR     constant number := 1;

/** Номер первичного документа - вексельный договор*/
  DL_VEKSELORDER            constant number := 109;

/** Вид ценной бумаги - вексель*/
  AVOIRISSKIND_BILL         constant number := 5;

/** форма дохода - дисконт*/
  VS_FORMULA_DISCOUNT CONSTANT NUMBER := 0; 
  
  VSBANNER_DOCKIND_MCACCDOC CONSTANT NUMBER := 164; --Вид документа в mcaccdoc
  FIROLE_BANNER CONSTANT NUMBER := 5; --роль фин. инструмента - вексель


/**
 * Возвращает наш ли вексель
 * @param  p_IssuerID Эмитент
 * @return BOOLEAN
 */
  FUNCTION IsOurBanner(p_IssuerID   IN NUMBER)
                                RETURN BOOLEAN;

/**
 * Возвращает статус векселя на дату
 * @since  6.20.030.00.0 @qtest-YES
 * @param  BCID Идентификатор учтенного векселя
 * @param  OnDate Дата, на которую определяется статус векселя
 * @return dvsbnrbck_dbt.t_NewABCStatus%TYPE Статус векселя на дату
 */
  FUNCTION GetVABnrStatusOnDate(BCID   IN dvsbnrbck_dbt.t_BCID%TYPE,
                                OnDate IN dvsbnrbck_dbt.t_ChangeDate%TYPE)
                                RETURN dvsbnrbck_dbt.t_NewABCStatus%TYPE;

/**
 * Возвращает статус векселя на дату
 * @qtest  NO
 * @since  6.20.030.00.0
 * @param  BCID Идентификатор учтенного векселя
 * @param  OnDate Дата, на которую определяется статус векселя
 * @return dvsbnrbck_dbt.t_NewBCState%TYPE Статус векселя на дату
 */
  FUNCTION GetVABnrStateOnDate(BCID   IN dvsbnrbck_dbt.t_BCID%TYPE,
                               OnDate IN dvsbnrbck_dbt.t_ChangeDate%TYPE)
                               RETURN dvsbnrbck_dbt.t_NewBCState%TYPE;

/**
 * Возвращает символ БО, в котором вексель был учтен на дату
 * @qtest  NO
 * @since  6.20.030.00.0
 * @param  BCID Идентификатор учтенного векселя
 * @param  OnDate Дата, на которую определяется символ БО, в котором вексель был учтен
 * @return d711bill_tmp.t_BO%TYPE Символ БО, в котором вексель был учтен на дату
 */
  FUNCTION GetVABnrBOOnDate(BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                            OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                            RETURN d711bill_tmp.t_BO%TYPE;

/**
 * Возвращает статус векселя на дату (для собственных векселей)
 * @qtest  NO
 * @since  6.20.030.00.0
 * @param  BCID Идентификатор собственного векселя
 * @param  OnDate Дата, на которую определяется статус векселя
 * @return dvsbnrbck_dbt.t_NewABCStatus%TYPE Статус векселя на дату
 */
  FUNCTION GetVSBnrStatusOnDate(BCID   IN dvsbnrbck_dbt.t_BCID%TYPE,
                                OnDate IN dvsbnrbck_dbt.t_ChangeDate%TYPE)
                                RETURN dvsbnrbck_dbt.t_NewABCStatus%TYPE;

/**
 * Получить ID клиента, для которого был куплен вексель на дату
 * @qtest  NO
 * @since  6.20.030.00.0
 * @param  BCID Идентификатор учтенного векселя
 * @param  OnDate Дата, на которую определяется ID клиента, для которого был куплен вексель
 * @return NUMBER ID клиента, для которого был куплен вексель на дату
 */
  FUNCTION GetVABnrClientOnDate(BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                                RETURN NUMBER;

/**
 * Получить договор обслуживания клиента, для которого был куплен вексель на дату
 * @qtest  NO
 * @since  6.20.030.00.0
 * @param  BCID Идентификатор учтенного векселя
 * @param  OnDate Дата, на которую определяется договор обслуживания клиента, для которого был куплен вексель
 * @return NUMBER Идентификатор договора обслуживания клиента, для которого был куплен вексель на дату
 */
  FUNCTION GetVABnrClientContrIDOnDate(BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                       OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                                       RETURN NUMBER;

/**
 * Определеить валюту учета ц/б (кроме собственных)
 * @qtest  NO
 * @since  6.20.031.05.0
 * @param  BCID Идентификатор учтенного векселя
 * @param  IsTrust Признак доверительного управления
 * @param  IsTemp Признак работы по временным таблицам
 * @return NUMBER Код валюты учета ц/б
 */
  FUNCTION GetVABnrPayFIID(BCID    IN dvsbanner_dbt.t_BCID%TYPE,
                           IsTrust IN NUMBER DEFAULT 0,
                           IsTemp  IN NUMBER DEFAULT 0)
                           RETURN NUMBER;

/**
 * Определеить валюту учета ц/б для собственных векселей
 * @qtest  NO
 * @since  6.20.031.27.0
 * @param  BCID Идентификатор собственного векселя
 * @param  IsTemp Признак работы по временным таблицам
 * @return NUMBER Код валюты учета ц/б
 */
  FUNCTION GetVSBnrPayFIID(BCID    IN dvsbanner_dbt.t_BCID%TYPE,
                           IsTemp  IN NUMBER DEFAULT 0)
                          RETURN NUMBER;

/**
 * Получить цену продажи на дату СВ
 * @qtest  NO
 * @since  6.20.031.27.0
 * @param  p_LegId Идентификатор сделки
 * @param  p_CalcDate Дата расчёта
 * @param  p_LastSalePrice Цена продажи в ВН
 * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
      */
  FUNCTION GetLastBnrSalePrice (p_LegId          IN     NUMBER,
                                p_CalcDate       IN     DATE,
                                p_LastSalePrice      OUT NUMBER)
                                RETURN NUMBER;

/**
 * Получить данные постановки векселя на баланс (УВ)
 * @since  6.20.031.05.0 @qtest-YES
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_BoundDate Предельное значение даты постановки учтенного векселя на баланс
 * @param  p_ClientContrID Идентификатор договора обслуживания клиента
 * @param  p_SetDealID Идентификатор сделки
 * @param  p_BalanceDate Даты постановки учтенного векселя на баланс
 * @param  p_DealID Идентификатор сделки
 * @param  p_Cost Цена учтенного векселя
 * @param  p_CostFI Код валюты цены учтенного векселя
 * @param  p_DealType Вид операции
 * @param  p_DealCode Номер сделки в системе учета
 * @param  p_BOfficeKind Вид сделки
 */
  PROCEDURE GetVABnrBalancePrm(p_BCID               IN  dvsbanner_dbt.t_BCID%TYPE,
                               p_BoundDate          IN  DATE,
                               p_ClientContrID      IN  ddl_tick_dbt.t_ClientContrID%TYPE,
                               p_SetDealID          IN  ddl_tick_dbt.t_DealID%TYPE,
                               p_BalanceDate        OUT DATE,
                               p_DealID             OUT ddl_tick_dbt.t_DealID%TYPE,
                               p_Cost               OUT dvsordlnk_dbt.t_BCCost%TYPE,
                               p_CostFI             OUT dvsordlnk_dbt.t_BCCFI%TYPE,
                               p_DealType           OUT ddl_tick_dbt.t_DealType%TYPE,
                               p_DealCode           OUT ddl_tick_dbt.t_DealCode%TYPE,
                               p_BOfficeKind        OUT ddl_tick_dbt.t_BOfficeKind%TYPE,
                               p_ClientID           IN  ddl_tick_dbt.t_ClientID%TYPE DEFAULT -1,
                               p_SaleID             IN  ddl_tick_dbt.t_DealID%TYPE DEFAULT -1
                              );

/**
 * Получить ID сделки постановки векселя на баланс на дату
 * @qtest  NO
 * @since  6.20.031.05.0
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, на которую определяется ID сделки постановки векселя на баланс
 * @return NUMBER ID сделки постановки векселя на баланс на дату
 */
  FUNCTION GetVABnrBalanceDealID(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                 p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                                 RETURN NUMBER;

/**
 * Получить сумму учета по векселю в ВН
 * @since  6.20.031.05.0 @qtest-YES
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, на определяется сумма учета по векселю в ВН
 * @return NUMBER Сумма учета по векселю в ВН
 */
  FUNCTION GetVABnrAccountedCostPFI(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                    p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                                    RETURN NUMBER;

/**
 * Получить сумму учета по векселю в ВУ
 * @qtest  NO
 * @since  6.20.031.05.0
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, на определяется сумма учета по векселю в ВУ
 * @return NUMBER Сумма учета по векселю в ВУ
 */
  FUNCTION GetVABnrAccountedCostAcc(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                    p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                                    RETURN NUMBER;

/**
 * Получить стоимость поставки векселя (в ВН) - оплаченная стоимость в ВР, переведенная в ВН на дату поставки
 * @since  6.20.031.05.0 @qtest-YES
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, на которую определяется стоимость поставки векселя
 * @return NUMBER Стоимость поставки векселя (в ВН) - оплаченная стоимость в ВР, переведенная в ВН на дату поставки
 */
  FUNCTION GetVABnrCostPFI(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                           p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                           RETURN NUMBER;

/**
 * Получить сумму начальной премии по векселю в ВН
 * @qtest  NO
 * @since  6.20.031.05.0
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, на которую определяется сумма начальной премии по векселю в ВН
 * @return NUMBER Сумма начальной премии по векселю в ВН
 */
  FUNCTION GetVABnrStartBonusPFI(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                 p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                                 RETURN NUMBER;

/**
 * Получить сумму начальной премии по векселю в ВУ
 * @qtest  NO
 * @since  6.20.031.05.0
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, на которую определяется сумма начальной премии по векселю в ВУ
 * @return NUMBER Сумма начальной премии по векселю в ВУ
 */
  FUNCTION GetVABnrStartBonusAcc(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                 p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                                 RETURN NUMBER;

/**
 * ID записи в истории по векселю, в результате которой статус векселя изменился на "Учтен"
 * @since  6.20.031.05.0 @qtest-YES
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, на которую определяется ID записи в истории по векселю
 * @param  p_BalanceDealID Идентификатор документа
 * @param  p_BalanceDocKind Вид первичного документа
 * @return NUMBER ID записи в истории по векселю, в результате которой статус векселя изменился на "Учтен"
 */
  FUNCTION GetVABnrLastAccountedEnrolID(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                        p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                        p_BalanceDealID IN NUMBER DEFAULT 0,
                                        p_BalanceDocKind IN NUMBER DEFAULT 0)
                                        RETURN NUMBER;

/**
 * Получить дату учета векселя
 * @since  6.20.031.05.0 @qtest-YES
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, не позднее которой был учтен вескель
 * @return DATE Дата учета векселя
 */
  FUNCTION GetVABnrLastBalanceDate(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                   p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'))
                                   RETURN DATE;

/**
 * Получить остаток не начисленной премии по векселю на дату в ВН
 * @qtest  NO
 * @since  6.20.031.05.0
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, на которую определяется остаток не начисленной премии по векселю в ВН
 * @return NUMBER Остаток не начисленной премии по векселю на дату в ВН
 */
  FUNCTION GetVABnrRestBonusPFI(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                                RETURN NUMBER;
/**
 * Получить остаток не начисленной премии по векселю на дату в ВУ
 * @qtest  NO
 * @since  6.20.031.06.0
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, на которую определяется остаток не начисленной премии по векселю в ВУ
 * @return NUMBER Остаток не начисленной премии по векселю на дату в ВУ
 */
  FUNCTION GetVABnrRestBonusAcc(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                                RETURN NUMBER;

/**
 * Синхронизация связей ц/б и сделки
 * @since  6.20.031.00.0 @qtest-NO
 * @param  p_dockind Вид первичного документа
 * @param  p_contractid Идентификатор договора/сделки
 * @return number 0 - в случае успеха, не 0 в случае неудачи
 */
  FUNCTION VSORDLNKSync(p_DocKind IN NUMBER, p_ContractID IN NUMBER) RETURN NUMBER;

/**
 * Получить процентную ставку по векселю на дату
 * @since  6.20.031.32.5 @qtest-NO
 * @param  p_BCID Идентификатор векселя
 * @param  p_OnDate Дата поиска ставки
 * @return number Значение ставки
 */
  FUNCTION GetBnrRateOnDate(p_BCID IN NUMBER, p_OnDate IN DATE) RETURN NUMBER;

/**
 * Проверить, находится ли сделка на внебалансе
 * @since  6.20.031.33.0 @qtest-NO
 * @param  p_DealID Идентификатор сделки
 * @return number 0 - в случае успеха, не 0 в случае неудачи
 */
  FUNCTION VADealIsOffBalance(p_DealID IN NUMBER) RETURN NUMBER;

/**
 * Получить справедливую стоимость по векселю на дату
 * @since  6.20.031.33.0 @qtest-NO
 * @param  p_BCID Идентификатор векселя
 * @param  p_OnDate Дата поиска
 * @param  p_Accounted Искать учтенную или неучтенную СС
 * @return number Величина СС
 */
  FUNCTION GetVABnrFrValueOnDate(p_BCID IN NUMBER, p_ContractID IN NUMBER, p_OnDate IN DATE, p_Accounted IN CHAR) RETURN NUMBER;

/**
 * Проверить, есть ли учтенные или неучтенные записи в истории СС
 * @since  6.20.031.33.0 @qtest-NO
 * @param  p_BCID Идентификатор векселя
 * @param  p_OnDate Дата поиска
 * @param  p_Accounted Искать учтенную или неучтенную СС
 * @return number Число найденных записей
 */
  FUNCTION ExistsVABnrFrValueOnDate(p_BCID IN NUMBER, p_ContractID IN NUMBER, p_OnDate IN DATE, p_Accounted IN CHAR) RETURN NUMBER;


/**
 * Изменить учёт СС по векселю
 * @since  6.20.031.33.0 @qtest-NO
 * @param  p_BCID Идентификатор векселя
 * @param  p_ContractID Идентификатор сделки
 * @param  p_BegDate Дата СС
 * @param  p_Accounted Признак учета СС
 */
 PROCEDURE ChangeAccountedForBnrFrVal(p_BCID IN NUMBER, p_ContractID IN NUMBER, p_BegDate IN DATE, p_Accounted IN CHAR);

/**
 * Получить отсроченную разницу в НацВ
 * @qtest  NO
 * @since  6.20.031.05.0
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, на которую определяется отсроченная разница в НацВ
 * @return NUMBER отсроченная разница в НацВ
 */
  FUNCTION GetVABnrStartDefDif(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                               p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                               RETURN NUMBER;
/**
 * Получить корректировку ПДД по ЭПС
 * @qtest  NO
 * @since  6.20.031.05.0
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, на которую определяется корректировка ПДД по ЭПС
 * @return NUMBER корректировка ПДД по ЭПС
 */
  FUNCTION GetVABnrAdjustmentEIR(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                  p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE,
                                  p_АdjustmentEIR OUT NUMBER,
                                  p_AccАdjustmentEIR OUT NUMBER)
  RETURN INTEGER;

  /**
 * Получить начальный дисконт
 * @qtest  NO
 * @since  6.20.031.05.0
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, на которую определяется начальный дисконт
 * @return NUMBER начальный дисконт
 */
  FUNCTION GetBNRFirstDiscount(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                              p_DealId IN NUMBER,
                              p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE,
                              p_FirstDiscount OUT NUMBER,
                              p_AccFirstDiscount OUT NUMBER)
  RETURN INTEGER;

  /**
 * Проверка на существенность отклонения с параметром int
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
 * @param p_S0VA Учтённая цена векселя для УВ
 * @param p_IsEssential Да/нет, 1/0
 * @param p_RateKind Наименование ставки используемой в дальнейшем
 * @param p_RateVal Значение ставки
 * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
    FUNCTION GetEssentialDevInt(p_LevelEssential IN NUMBER,
                                p_Portfolio IN NUMBER,
                                p_CalcDate IN DATE,
                                p_DoCompare IN NUMBER,
                                p_ToCompare1 IN NUMBER,
                                p_ToCompare2 IN NUMBER,
                                p_ObjectKind IN NUMBER,
                                p_ObjectID IN NUMBER,
                                p_S0VA IN NUMBER,
                                p_IsEssential OUT NUMBER,
                                p_RateKind OUT NUMBER,
                                p_RateVal OUT NUMBER
                          )
    return NUMBER;

/**
 * Получить дату оплаты векселя
 * @since  6.20.031.05.0 @qtest-YES
 * @param  p_BCID Идентификатор учтенного векселя
 * @param  p_OnDate Дата, не позднее которой был оплачен вескель
 * @return DATE Дата оплаты векселя
 */
  FUNCTION GetVSBnrLastPayedDate(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                   p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'))
                                   RETURN DATE;

/**
 * ID записи в истории по векселю, в результате которой статус собственного векселя изменился на "Оплачен"
 * @since  6.20.031.05.0 @qtest-YES
 * @param  p_BCID Идентификатор собственного векселя
 * @param  p_OnDate Дата, на которую определяется ID записи в истории по векселю
 * @param  p_BalanceDealID Идентификатор документа
 * @param  p_BalanceDocKind Вид первичного документа
 * @return NUMBER ID записи в истории по векселю, в результате которой статус векселя изменился на "Оплачен"
 */
  FUNCTION GetVSBnrLastPayedEnrolID(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                        p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                        p_BalanceDealID IN NUMBER DEFAULT 0,
                                        p_BalanceDocKind IN NUMBER DEFAULT 0)
                                        RETURN NUMBER;

/**
 * Получить отсроченную разницу СВ в НацВ
 * @qtest  NO
 * @since  6.20.031.05.0
 * @param  p_BCID Идентификатор собственного векселя
 * @param  p_OnDate Дата, на которую определяется отсроченная разница в НацВ
 * @return NUMBER отсроченная разница в НацВ
 */
  FUNCTION GetVSBnrStartDefDif(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                               p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                               RETURN NUMBER;

/**
 * Получить не отнесённую отсроченную разницу СВ
 * @qtest  NO
 * @since  6.20.031.05.0
 * @param  p_BCID Идентификатор собственного векселя
 * @param  p_OnDate Дата, на которую определяется отсроченная разница в НацВ
 * @return NUMBER отсроченная разница в НацВ
 */
  FUNCTION GetVSBnrNotWrittenDefDif(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                               p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE)
                               RETURN NUMBER;

/**
 * Получить корректировку ПДД по ЭПС СВ
 * @qtest  NO
 * @since  6.20.031.05.0
 * @param  p_BCID Идентификатор собственного векселя
 * @param  p_OnDate Дата, на которую определяется корректировка ПДД по ЭПС
 * @return NUMBER корректировка ПДД по ЭПС
 */
  FUNCTION GetVSBnrAdjustmentEIR(p_BCID   IN dvsbanner_dbt.t_BCID%TYPE,
                                  p_OnDate IN dvsbanner_dbt.t_IssueDate%TYPE,
                                  p_АdjustmentEIR OUT NUMBER,
                                  p_AccАdjustmentEIR OUT NUMBER)
  RETURN INTEGER;

/**
 * Получить дату выдачи/продажи СВ
 * @qtest  NO
 * @since  6.20.031.048
 * @param  p_BCID Идентификатор собственного векселя
 * @param  p_StartDate Дата, выходной параметр
 * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
  FUNCTION GetVSStartDate (p_LegId IN NUMBER, p_StartDate OUT DATE)
     RETURN INTEGER;

/**
 * Получить кол-во дней в году по базису СВ
 * @qtest  NO
 * @since  6.20.031.048
 * @param  p_Basis Базис векселя
 * @param  p_dateC Дата начала действия векселя
 * @param  p_asPeriod Дата как период
 * @param  p_DayInYear Кол-во дней в году
 * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
  FUNCTION GetDaysInYearByBasis (p_Basis       IN     NUMBER,
                                 p_dateC       IN     DATE,
                                 p_asPeriod    IN     BOOLEAN,
                                 p_DayInYear      OUT NUMBER)
     RETURN INTEGER;

/**
 * Плановая дата погашения
 * @qtest  NO
 * @since  6.20.031.048
 * @param  p_LegId Идентификатор сделки
 * @param  p_PlanRepayDate Плановая дата погашения векселя
  * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
  FUNCTION GetBnrPlanRepayDate (p_LegId           IN     NUMBER,
                                  p_PlanRepayDate      OUT DATE)
     RETURN INTEGER;

/**
 * Плановая дата погашения
 * @qtest  NO
 * @since  6.20.031.048
 * @param  p_LegId Идентификатор сделки
  * @return рез-т вызова ф-ции Плановая дата погашения векселя
 */
  FUNCTION GetBnrNOPlanRepayDate(p_LegId           IN     NUMBER)
     RETURN DATE;

/**
 * Посчитать ЭПС СВ
 * @qtest  NO
 * @since  6.20.031.048
 * @param  p_LegId Идентификатор сделки
 * @param  p_EIR ЭПС
  * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
   FUNCTION CalcVSEIR (p_LegId IN NUMBER, p_d0 in DATE, p_EIR OUT NUMBER)
      RETURN INTEGER;

/**
 * Посчитать АС ЭПС СВ
 * @qtest  NO
 * @since  6.20.031.048
 * @param  p_LegId Идентификатор сделки
 * @param  p_EIR Расчитанный ЭПС
 * @param  p_CalcDate Дата расчёта
 * @param  p_ASEIR АС ЭПС
  * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
  FUNCTION CalcVSAS_EIR (p_LegId      IN     NUMBER,
                        p_EIR        IN     NUMBER,
                        p_CalcDate   IN     DATE,
                        p_ASEIR         OUT NUMBER)
     RETURN INTEGER;

/**
 * Посчитать Корректировка%_ЭПС СВ
 * @qtest  NO
 * @since  6.20.031.048
 * @param  p_LegId Идентификатор сделки
 * @param  p_CalcDate Дата расчёта
 * @param  p_InterestIncomeAdd Сумма доначисленных процентов
 * @param  p_BonusAdd Суммы доначисленной премии
 * @param  p_DiscountIncomeAdd Суммы доначисленного дисконта
 * @param  p_FairValue Справедливая стоимость
 * @param  p_EIR ЭПС
 * @param  p_Ret Возвращаемое значение
  * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
  FUNCTION CalcVSPersentEIR (p_LegId               IN     NUMBER,
                            p_CalcDate            IN     DATE,
                            p_InterestIncomeAdd   IN     NUMBER,
                            p_BonusAdd            IN     NUMBER,
                            p_DiscountIncomeAdd   IN     NUMBER,
                            p_FairValue           IN     NUMBER,
                            p_EIR                 IN     NUMBER,
                            p_Ret                    OUT NUMBER)
     RETURN INTEGER;

/**
 * Посчитать ЭПС УВ
 * @qtest  NO
 * @since  6.20.031.048
 * @param  p_LegId Идентификатор сделки
 * @param  p_S0 Учтенная цена
 * @param  p_EIR ЭПС
  * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
   FUNCTION CalcVAEIR (p_LegId IN NUMBER,  p_d0 in DATE, p_S0 in NUMBER, p_EIR OUT NUMBER)
      RETURN INTEGER;

/**
 * Посчитать АС ЭПС УВ
 * @qtest  NO
 * @since  6.20.031.048
 * @param  p_LegId Идентификатор сделки
 * @param  p_EIR Расчитанный ЭПС
 * @param  p_CalcDate Дата расчёта
 * @param  p_ASEIR АС ЭПС
  * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
  FUNCTION CalcVAAS_EIR (p_LegId      IN     NUMBER,
                        p_EIR        IN     NUMBER,
                        p_CalcDate   IN     DATE,
                        p_ASEIR         OUT NUMBER)
     RETURN INTEGER;

/**
 * Посчитать Корректировка%_ЭПС УВ
 * @qtest  NO
 * @since  6.20.031.048
 * @param  p_LegId Идентификатор сделки
 * @param  p_CalcDate Дата расчёта
 * @param  p_InterestIncomeAdd Сумма доначисленных процентов
 * @param  p_BonusAdd Суммы доначисленной премии
 * @param  p_DiscountIncomeAdd Суммы доначисленного дисконта
 * @param  p_FairValue Справедливая стоимость
 * @param  p_EIR ЭПС
 * @param  p_Ret Возвращаемое значение
  * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
  FUNCTION CalcVAPersentEIR (p_LegId               IN     NUMBER,
                            p_CalcDate            IN     DATE,
                            p_InterestIncomeAdd   IN     NUMBER,
                            p_BonusAdd            IN     NUMBER,
                            p_DiscountIncomeAdd   IN     NUMBER,
                            p_FairValue           IN     NUMBER,
                            p_EIR                 IN     NUMBER,
                            p_Ret                    OUT NUMBER)
     RETURN INTEGER;

/**
 * ID контракта, соответствующего макс. дата подписания для СВ */
  Function GetContractID_MaxDateBnr(BCID IN dvsbanner_dbt.t_BCID%TYPE)
  return ddl_order_dbt.t_ContractID%TYPE;

/**
 * Последняя дата векселя в нужном состоянии и не больше дня операции */
  FUNCTION GetLastDateStateVS(p_BCID    IN NUMBER,
                              p_State   IN VARCHAR2,
                              p_Date    IN DATE)
      RETURN DATE;

  /**
      * Получить дисконт на дату УВ
      * @qtest  NO
      * @since  6.20.031.053
      * @param  p_LegId Идентификатор сделки
      * @param  p_CalcDate Дата расчёта
      * @param  p_CalcDiscount Сумма дисконта на дату
       * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
      */
  FUNCTION GetDiscountOnDateVA (p_LegId          IN     NUMBER,
                                p_DealID         IN     NUMBER,
                                p_CalcDate       IN     DATE,
                                p_CalcDiscount      OUT NUMBER)
     RETURN NUMBER;

  /**
      * Получить премию на дату УВ
      * @qtest  NO
      * @since  6.20.031.053
      * @param  p_LegId Идентификатор сделки
      * @param  p_CalcDate Дата расчёта
      * @param  p_CalcDiscount Сумма премии к списанию на дату
       * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
      */
  FUNCTION GetBonusOnDateVA (p_LegId       IN     NUMBER,
                             p_CalcDate    IN     DATE,
                             p_CalcBonus      OUT NUMBER)
     RETURN NUMBER;

  /**
      * Получить процент на дату УВ
      * @qtest  NO
      * @since  6.20.031.053
      * @param  p_LegId Идентификатор сделки
      * @param  p_CalcDate Дата расчёта
      * @param  p_CalcDiscount Сумма процента на дату
       * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
      */
  FUNCTION GetPrecentOnDate (p_LegId         IN     NUMBER,
                               p_CalcDate      IN     DATE,
                               p_CalcPrecent      OUT NUMBER)
     RETURN NUMBER;

  /**
      * Расчитать АС ЛН УВ
      * @qtest  NO
      * @since  6.20.031.053
      * @param  p_LegId Идентификатор сделки
      * @param  p_CalcDate Дата расчёта
      * @param  p_ACt АС ЛН
       * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
      */
  FUNCTION CalcVAAS_LN (p_LegId          IN     NUMBER,
                        p_CalcDate       IN     DATE,
                        p_ACt            OUT    NUMBER)
     RETURN NUMBER;

  /**
      * Получить дисконт на дату СВ
      * @qtest  NO
      * @since  6.20.031.053
      * @param  p_LegId Идентификатор сделки
      * @param  p_CalcDate Дата расчёта
      * @param  p_CalcDiscount Сумма дисконта на дату
       * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
      */
  FUNCTION GetDiscountOnDateVS (p_LegId          IN     NUMBER,
                                p_DealId         IN     NUMBER,
                                p_CalcDate       IN     DATE,
                                p_CalcDiscount      OUT NUMBER)
     RETURN NUMBER;

  /**
      * Расчитать АС ЛН СВ
      * @qtest  NO
      * @since  6.20.031.053
      * @param  p_LegId Идентификатор сделки
      * @param  p_CalcDate Дата расчёта
      * @param  p_ACt АС ЛН
       * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
      */
  FUNCTION CalcVSAS_LN (p_LegId          IN     NUMBER,
                        p_CalcDate       IN     DATE,
                        p_ACt            OUT    NUMBER)
     RETURN NUMBER;

  /**
      * Получить ID сделка по векселю и дате
      * @qtest  NO
      * @since  6.20.031.053
      * @param  p_BCID Идентификатор векселя
      * @param  p_CalcDate Дата расчёта
      * @param  p_DealId Ид сделки
       * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
      */
  FUNCTION GetVSDealId (p_BCID           IN     NUMBER,
                        p_CalcDate       IN     DATE,
                        p_DealId         OUT    NUMBER)
     RETURN NUMBER;

  /**
      * Получить ID сделка по векселю и дате
      * @qtest  NO
      * @since  6.20.031.053
      * @param  p_BCID Идентификатор векселя
      * @param  p_CalcDate Дата расчёта
      * @param  p_DealId Ид сделки
       * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
      */
  FUNCTION GetVADealId (p_BCID           IN     NUMBER,
                        p_CalcDate       IN     DATE,
                        p_DealId         OUT    NUMBER)
     RETURN NUMBER;

  FUNCTION GetBNRFirstDate (p_BCID           IN     NUMBER,
                            p_DealId         IN     NUMBER)
     RETURN DATE;

  /**
      * Получить ID сделка по векселю и дате
      * @qtest  NO
      * @since  6.20.031.053
      * @param  p_BCID Идентификатор векселя
      * @param  p_CalcDate Дата расчёта
       * @return рез-т вызова ф-ции Ид сделки
      */
  FUNCTION GetVANODealId (p_BCID           IN     NUMBER,
                        p_CalcDate       IN     DATE)
     RETURN NUMBER;

  /**
      * Получить ID сделка по векселю и дате
      * @qtest  NO
      * @since  6.20.031.053
      * @param  p_BCID Идентификатор векселя
      * @param  p_CalcDate Дата расчёта
       * @return рез-т вызова ф-ции Ид сделки
      */
  FUNCTION GetVSNODealId (p_BCID           IN     NUMBER,
                        p_CalcDate       IN     DATE)
     RETURN NUMBER;

/**
 * Получить дату выдачи/продажи СВ
 * @qtest  NO
 * @since  6.20.031.048
 * @param  p_BCID Идентификатор собственного векселя
 * @param  p_StartDate Дата, выходной параметр
 * @return рез-т вызова ф-ции (0-успешно, 1-неуспешно)
 */
  FUNCTION GetVSNOStartDate (p_LegId IN NUMBER)
     RETURN DATE;

  PROCEDURE PrcBillRestList(ContractID IN NUMBER, BeginDate IN DATE, EndDate IN DATE);

/**
 * ID сделки, соответствующая макс. дата для векселя */
  Function GetDealID_MaxDateBnr(BCID IN dvsbanner_dbt.t_BCID%TYPE)
  return ddl_tick_dbt.t_DealID%TYPE;

/**
 * Сумма по виду налога для сделки, исключая текущую сделку. Выводится на панели погашения/выкупа СВ */
  FUNCTION VSGetSumNPTXKindClientInDate(ClientID IN NUMBER,
                                DateFrom IN DATE, DateTo in DATE, ContrID IN NUMBER)
        RETURN NUMBER;

/**
  * Проверка остатка на счетах хеджирования для СВ */
  FUNCTION VSGetRestOnAcc( BCID IN NUMBER,
                           CommDate IN DATE)
        RETURN NUMBER;
/**
  * Проверка остатка на счетах хеджирования для УВ */
  FUNCTION VAGetRestOnAcc( BCID IN NUMBER,
                           CommDate IN DATE)
        RETURN NUMBER;
/**
  * Поиск последней даты корректировки СВ*/
  FUNCTION VSGetDateCorrLast( BCID IN NUMBER)
        RETURN DATE;
/**
  * Поиск последней даты корректировки УВ*/
  FUNCTION VAGetDateCorrLast( BCID IN NUMBER)
        RETURN DATE;
/**
  * Ставка по векселю. так же, считает и ставку дисконта*/       
  FUNCTION GetBnrRate(p_bcid in dvsbanner_dbt.t_BCID%TYPE) 
        RETURN number;
/**
  * Поиск даты погашения по формуле рсхб*/   
  FUNCTION GetBnrPlanRepayDateVS (p_LegId IN NUMBER)
        RETURN DATE;

  function payment_due_date (
    p_bctermformula  dvsbanner_dbt.t_bctermformula%type,
    p_maturity       ddl_leg_dbt.t_maturity%type,
    p_expiry         ddl_leg_dbt.t_expiry%type
  ) return varchar2;
END RSB_BILL;
/
