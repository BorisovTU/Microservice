CREATE OR REPLACE PACKAGE UINTEGRATION
IS

    --Вида кода субъекта, в котором хранится номер кода клиента в Новой Афине
    CLIENT_CODE_NA constant integer := 101;

    --Номер справочника, в котором перечислены категории и примечания, подлежащие выгрузке
    SPRAV_NUMBER   constant integer := 5001;

    --Константа для функции psb_GetFortsCode
    PrefForts CONSTANT VARCHAR2(10) := 'C900';


    /* Новая Афина.   Вставка комиссии брокера */
/*
    FUNCTION InsertComisBroker(p_docid        IN V_PTIBR_CALCFXMMVBTRANSACTION.T_DOCID%TYPE,
                               p_docdate      IN V_PTIBR_CALCFXMMVBTRANSACTION.T_DOCDATE%TYPE,
                               p_agreementnum IN V_PTIBR_CALCFXMMVBTRANSACTION.T_AGREEMENTNUM%TYPE,
                               p_amounttypeid IN V_PTIBR_CALCFXMMVBTRANSACTION.T_AMOUNTTYPEID%TYPE,
                               p_amountlabel  IN V_PTIBR_CALCFXMMVBTRANSACTION.T_AMOUNTLABEL%TYPE,
                               p_amount       IN V_PTIBR_CALCFXMMVBTRANSACTION.T_AMOUNT%TYPE,
                               p_curr         IN V_PTIBR_CALCFXMMVBTRANSACTION.T_CURR%TYPE,
                               p_paydate      IN V_PTIBR_CALCFXMMVBTRANSACTION.T_PAYDATE%TYPE)
    RETURN INTEGER;
*/

    /* Функция определяет: курс по валюте или по ценной бумаге */
    FUNCTION WhatKindOfRate(p_fiid1 varchar2,
                            p_fiid2 varchar2)
    RETURN integer;


    /* Informatica. Вставка курса */
    FUNCTION ImportOneCourse(p_BaseFIID      IN dratedef_dbt.T_OTHERFI%TYPE,
                             p_OtherFIID     IN dratedef_dbt.T_FIID%TYPE,
                             p_RateKind      IN dratedef_dbt.T_TYPE%TYPE,
                             p_SinceDate     IN dratedef_dbt.T_SINCEDATE%TYPE,
                             --p_MarketCode IN varchar2,
                             --p_MarketCodeKind IN integer,
                             p_MarketPlace   IN dratedef_dbt.T_MARKET_PLACE%TYPE,
                             p_MarketSection IN dratedef_dbt.T_SECTION%TYPE,
                             p_Rate          IN dratedef_dbt.T_RATE%TYPE,
                             p_Scale         IN dratedef_dbt.T_SCALE%TYPE,
                             p_Point         IN dratedef_dbt.T_POINT%TYPE,
                             --p_BoardID IN varchar2,
                             p_IsRelative    IN dratedef_dbt.T_ISRELATIVE%TYPE default null,
                             p_IsDominant    IN dratedef_dbt.T_ISDOMINANT%TYPE default chr(0),
                             p_IsInverse     IN dratedef_dbt.T_ISINVERSE%TYPE default chr(0),
                             p_Oper          IN dratedef_dbt.T_OPER%TYPE default 0,
                             Err             OUT VARCHAR2)
    RETURN INTEGER;


    /* Вспомогательная функция функция для использования внутри пакета */
/*
    FUNCTION PaymentXml(p_paymentid IN dpmpaym_dbt.t_paymentid%type)
    RETURN clob;
*/

    /* Вспомогательная функция функция для использования внутри пакета */
/*
    FUNCTION AccTrnXml(p_acctrnid IN dacctrn_dbt.t_acctrnid%type)
    RETURN clob;
*/

    /* Вспомогательная функция функция для использования внутри пакета */
/*
    FUNCTION AccountXml(p_accountid IN dacctrn_dbt.t_acctrnid%type)
    RETURN clob;
*/

    /* Informatica - Новая Афина.   Генерация XML по невыгруженным объектам в таблице синхронизации */
/*
    PROCEDURE uGenXMLSynchObj(p_objkind  IN intgr_synch_obj.t_objectkind%type default 0,
                              p_count    IN number                            default 0,
                              p_datefrom IN intgr_synch_obj.t_synch_time%type default null,
                              p_dateto   IN intgr_synch_obj.t_synch_time%type default null);
*/

    /* Вспомогательная функция функция для использования внутри пакета */
    FUNCTION GetBalanceByNumPlan(account_id IN daccbalance_dbt.t_accountid%type,
                                 num_plan   IN daccbalance_dbt.t_numplan%type)
    RETURN daccbalance_dbt.t_balance%type;


    /* Вспомогательная функция функция для использования внутри пакета */
    FUNCTION GetIsoCodeByFIID(fiid IN dfininstr_dbt.t_fiid%type)
    RETURN dfininstr_dbt.t_ccy%type;


    /* Вспомогательная функция функция для использования внутри пакета */
    FUNCTION GetNearestWorkDate(p_date IN DATE)
    RETURN DATE;


    /* ПСБ-Ритэйл.   Интерфейс Справочника НКД */
/*
    PROCEDURE CalcNKD(p_date IN DATE);
*/

    /* Получить код договора на срочном рынке */
    FUNCTION psb_GetFortsCode(InvestCode IN INTEGER)
    RETURN VARCHAR2;
    
    --Подготавливает данные по счетам клиента для передачи из СОФР в ДБО ФЛ
/*
    PROCEDURE rshb_prepDataAccounts(clientid IN INTEGER,
                                    p_date IN DATE);
*/
END;
/

