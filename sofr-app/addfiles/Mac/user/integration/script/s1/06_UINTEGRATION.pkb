CREATE OR REPLACE PACKAGE BODY UINTEGRATION
IS
/* TODO: в процедуру CalcNKD: сделать ограничение по выборке ЦБ - только те, у которых есть признак (возможно это категория) "БО-лайт"*/


    /**
    * Вставка комиссии брокера
    *    p_docid        Внутренний идентификатор комиссии внешней системы
    *    p_docdate      Дата расчета комиссии
    *    p_agreementnum Номер брокерского договора
    *    p_amounttypeid Тип комиссии
    *    p_amountlabel  Наименование типа комиссии
    *    p_amount       Сумма комиссии
    *    p_curr         Валюта комиссии
    *    p_paydate      Дата платежа комиссии
    */
/*
    FUNCTION InsertComisBroker(p_docid        IN V_PTIBR_CALCFXMMVBTRANSACTION.T_DOCID%TYPE,
                               p_docdate      IN V_PTIBR_CALCFXMMVBTRANSACTION.T_DOCDATE%TYPE,
                               p_agreementnum IN V_PTIBR_CALCFXMMVBTRANSACTION.T_AGREEMENTNUM%TYPE,
                               p_amounttypeid IN V_PTIBR_CALCFXMMVBTRANSACTION.T_AMOUNTTYPEID%TYPE,
                               p_amountlabel  IN V_PTIBR_CALCFXMMVBTRANSACTION.T_AMOUNTLABEL%TYPE,
                               p_amount       IN V_PTIBR_CALCFXMMVBTRANSACTION.T_AMOUNT%TYPE,
                               p_curr         IN V_PTIBR_CALCFXMMVBTRANSACTION.T_CURR%TYPE,
                               p_paydate      IN V_PTIBR_CALCFXMMVBTRANSACTION.T_PAYDATE%TYPE )
    RETURN INTEGER
    IS
    BEGIN

        begin
            MERGE INTO v_ptibr_calcfxmmvbtransaction t1
                 USING (SELECT p_docid        t_docid,
                               p_docdate      t_docdate,
                               p_agreementnum t_agreementnum,
                               p_amounttypeid t_amounttypeid,
                               p_amountlabel  t_amountlabel,
                               p_amount       t_amount,
                               p_curr         t_curr,
                               p_paydate      t_paydate
                          FROM DUAL) t2
                    ON (t1.t_docid = t2.t_docid)
                 WHEN MATCHED
                 THEN
                  UPDATE SET t1.t_docdate = t2.t_docdate,
                             t1.t_agreementnum = t2.t_agreementnum,
                             t1.t_amounttypeid = t2.t_amounttypeid,
                             t1.t_amountlabel = t2.t_amountlabel,
                             t1.t_amount = t2.t_amount,
                             t1.t_curr = t2.t_curr,
                             t1.t_paydate = t2.t_paydate
                 WHEN NOT MATCHED
                 THEN
                  INSERT (t1.t_docid,
                          t1.t_docdate,
                          t1.t_agreementnum,
                          t1.t_amounttypeid,
                          t1.t_amountlabel,
                          t1.t_amount,
                          t1.t_curr,
                          t1.t_paydate)
                    VALUES (t2.t_docid,
                            t2.t_docdate,
                            t2.t_agreementnum,
                            t2.t_amounttypeid,
                            t2.t_amountlabel,
                            t2.t_amount,
                            t2.t_curr,
                            t2.t_paydate);

            EXCEPTION WHEN OTHERS THEN
                RETURN -1;
        end;

        COMMIT;
        RETURN 0;

    END InsertComisBroker;
*/

    /**
    *  Вспомогательная функция для интерфейса "Вставка курса/котировки".
     * Определяет, какой курс, и возвращает: 1 - курс по валюте
     *                                       2 - курс по ценной бумаге
     *                                       0 - не удалось определить
    */
    FUNCTION WhatKindOfRate(p_fiid1 varchar2,
                            p_fiid2 varchar2)
    RETURN integer
    IS
        find1 integer;
        find2 integer;
    BEGIN

        --проверка что валюта1 и валюта2 - это денежные валюты

        --если код валюты1 буквенный типа 'RUB'
        if (NVL(LENGTH(LTRIM(p_fiid1, '0123456789')), 0) > 0) then
            begin
                select count(1) into find1 from dfininstr_dbt where t_fi_kind = 1 and t_ccy = p_fiid1 and length(t_ccy) = 3;
                exception when no_data_found then
                    find1 := 0;
            end;

        else --код валюты1 цифровой типа '643'
            begin
                select count(1) into find1 from dfininstr_dbt where t_fi_kind = 1 and t_iso_number = p_fiid1 and length(t_ccy) = 3;
                exception when no_data_found then
                    find1 := 0;
            end;
        end if;

        --если код валюты2 буквенный типа 'RUB'
        if (NVL(LENGTH(LTRIM(p_fiid2, '0123456789')), 0) > 0) then
            begin
                select count(1) into find2 from dfininstr_dbt where t_fi_kind = 1 and t_ccy = p_fiid2 and length(t_ccy) = 3;
                exception when no_data_found then
                    find2 := 0;
            end;

        else --код валюты2 цифровой типа '643'
            begin
                select count(1) into find2 from dfininstr_dbt where t_fi_kind = 1 and t_iso_number = p_fiid2 and length(t_ccy) = 3;
                exception when no_data_found then
                    find2 := 0;
            end;
        end if;

        if (find1 + find2 >= 2) then
            return 1;
        end if;

        --проверка что валюта1 и валюта2 - это ценные бумаги
        begin
            select count(1) into find1 from davrkinds_dbt where t_fi_kind = 2 and to_char(t_avoirkind) = p_fiid1;
            exception when no_data_found then
                find1 := 0;
        end;

        begin
            select count(1) into find2 from davrkinds_dbt where t_fi_kind = 2 and to_char(t_avoirkind) = p_fiid2;
            exception when no_data_found then
                find2 := 0;
        end;

        if (find1 + find2 >= 2) then
            return 2;
        end if;

        return 3;

    END WhatKindOfRate;


    /**
    *  Вставка курса
    *    p_BaseFIID       Код ФИ (Базовый ФИ)
    *    p_OtherFIID      Код валюты котировки (FICK_...)
    *    p_RateKind       Вид курса
    *    p_SinceDate      Дата установки курса
    *    p_MarketCode     Код торговой площадки
    *    p_MarketCodeKind Вид кода субъекта торговой площадки (PTCK_...)
    *    p_MarketPlace    Торговая площадка
    *    p_MarketSection  Сектор торговой площадки
    *    p_Rate           Значение курса
    *    p_Scale          Масштаб курса
    *    p_Point          Количество значащих цифр
    *    p_BoardID        Режим торгов
    *    p_IsRelative     Признак относительной котировки (облигации - true, для остальных - false)
    *    p_IsDominant     Признак основного курса false
    *    p_IsInverse      Признак обратной котировки false
    *    p_Oper           Номер пользователя
    *    Err              Возвращаемый параметр - текст ошибки
    */
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
                             Err             OUT VARCHAR2) return integer
    /* Шпаргалка:
        Торговая площадка = select * from dparty_dbt where t_partyid = (select t_market_place from Dratedef_Dbt where t_rateid = ВидКурса);
        Секция торговой площадки = select t_officeid from dptoffice_dbt where t_partyid = (select t_market_place from Dratedef_Dbt where t_rateid = ВидКурса) and t_officeid = (select t_section from Dratedef_Dbt where t_rateid = ВидКурса);
    */
    IS
        Ratedef_Rec       Dratedef_Dbt%ROWTYPE;
        Cnt               NUMBER;
        First_Save        NUMBER(1) := 0;
        Bad_Value_Rate    EXCEPTION;
        Bad_Currency      EXCEPTION;
        Bad_Date_Rate_Set EXCEPTION;
        l_IsRelative      dratedef_dbt.T_ISRELATIVE%TYPE;
        l_IsDominant      dratedef_dbt.T_ISDOMINANT%TYPE;
        l_IsInverse       dratedef_dbt.T_ISINVERSE%TYPE;
        IsCurrencyRate    char(1);
    BEGIN

        -- Признак относительной котировки по умолчанию для облигации - true, для остальных - false
        if (p_IsRelative is null) then
            select case when exists (select 1
                                       from (select t_avoirkind from dfininstr_dbt where t_fiid in (p_BaseFIID, p_OtherFIID)) t1,
                                            (select t_avoirkind from davrkinds_dbt where t_fi_kind = 2 and t_numlist like '2%') t2
                                      where t1.t_avoirkind = t2.t_avoirkind)
                              then chr(88)
                              else chr(0)
                      end into l_IsRelative
            from dual;
        else
            l_IsRelative := p_IsRelative;
        end if;

        if ((p_IsDominant != chr(88) and p_IsDominant != chr(0)) or (p_IsDominant is null)) then
            l_IsDominant := chr(0);
        else
            l_IsDominant := p_IsDominant;
        end if;

        if ((p_IsInverse != chr(88) and p_IsInverse != chr(0)) or (p_IsInverse is null)) then
            l_IsInverse := chr(0);
        else
            l_IsInverse := p_IsInverse;
        end if;

        Dbms_Output.Put_Line(To_Char(p_SinceDate, 'dd.mm.yyyy hh24:mi:ss'));
        SAVEPOINT Saverate;
        -- блокируем строку для данной валюты и вида курса daratedef_dbt
        BEGIN
           SELECT *
             INTO Ratedef_Rec
             FROM Dratedef_Dbt Rd
            WHERE Rd.t_Fiid = p_OtherFIID
              AND Rd.t_Otherfi = p_BaseFIID
              AND Rd.t_Type = p_RateKind
              FOR UPDATE NOWAIT;
        EXCEPTION
           WHEN No_Data_Found THEN
              First_Save := 1;
        END;
        BEGIN
           SELECT COUNT(*)
             INTO Cnt
             FROM Dratehist_Dbt
            WHERE t_Rateid = Ratedef_Rec.t_Rateid
              AND t_Sincedate = Ratedef_Rec.t_Sincedate;
        END;
        IF (First_Save = 0) THEN
           IF (Ratedef_Rec.t_Sincedate > p_SinceDate) THEN
              ROLLBACK TO SAVEPOINT Saverate;
              RAISE Bad_Date_Rate_Set;
           ELSE
              IF (Ratedef_Rec.t_Sincedate <= p_SinceDate) THEN
                 IF (Cnt > 0) THEN
                    UPDATE Dratehist_Dbt
                       SET t_Rate = Ratedef_Rec.t_Rate,
                              t_IsManualInput = CHR(0)
                     WHERE t_Rateid = Ratedef_Rec.t_Rateid
                       AND t_Sincedate = Ratedef_Rec.t_Sincedate;
                 ELSE
                    INSERT INTO Dratehist_Dbt
                    VALUES
                       (Ratedef_Rec.t_Rateid
                       ,Ratedef_Rec.t_Isinverse
                       ,Ratedef_Rec.t_Rate
                       ,Ratedef_Rec.t_Scale
                       ,Ratedef_Rec.t_Point
                       ,Ratedef_Rec.t_Inputdate
                       ,Ratedef_Rec.t_Inputtime
                       ,Ratedef_Rec.t_Oper
                       ,Ratedef_Rec.t_Sincedate
                       ,null);
                 END IF;

              END IF;

              UPDATE Dratedef_Dbt Rd
                 SET Rd.t_Rate      = p_Rate
                    ,Rd.t_Scale     = p_Scale
                    ,Rd.t_Point     = p_Point
                    ,Rd.t_Inputdate = Trunc(SYSDATE)
                    ,Rd.t_Inputtime = To_Date('01010001' ||
                                              To_Char(SYSDATE, 'HH24MISS')
                                             ,'DDMMYYYYHH24MISS')
                    ,Rd.t_Oper      = p_Oper
                    ,Rd.t_Sincedate = p_SinceDate
                    ,Rd.t_IsManualInput = CHR(0)
               WHERE Rd.t_Fiid = p_OtherFIID
                 AND Rd.t_Otherfi = p_BaseFIID
                 AND Rd.t_Type = p_RateKind;
           END IF;
        ELSE
           -- вводим впервые
           INSERT INTO Dratedef_Dbt
              (t_Rateid
              ,t_Fiid
              ,t_Otherfi
              ,t_Name
              ,t_Definition
              ,t_Type
              ,t_IsDominant
              ,t_IsRelative
              ,t_Informator
              ,t_Market_Place
              ,t_IsInverse
              ,t_Rate
              ,t_Scale
              ,t_Point
              ,t_Inputdate
              ,t_Inputtime
              ,t_Oper
              ,t_Sincedate
              ,t_Section
              ,t_Version
              ,t_IsManualInput)
           VALUES
              (0
              ,p_OtherFIID
              ,p_BaseFIID
              ,Chr(1)
              ,Chr(1)
              ,p_RateKind
              ,l_IsDominant
              ,l_IsRelative
              ,0
              ,p_MarketPlace
              ,l_IsInverse
              ,p_Rate
              ,p_Scale
              ,p_Point
              ,Trunc(SYSDATE)
              ,To_Date('01010001' || To_Char(SYSDATE, 'HH24MISS'),'DDMMYYYYHH24MISS')
              ,p_Oper
              ,p_SinceDate
              ,p_MarketSection
              ,NULL
              ,CHR(0));
        END IF;
        COMMIT;
        RETURN 0;
    EXCEPTION
        WHEN Bad_Date_Rate_Set THEN
           Err := 'Дата установки курса меньше даты действующего курса';
           RETURN 1;
        WHEN OTHERS THEN
           ROLLBACK TO SAVEPOINT Saverate;
           Err := SQLERRM;
           RETURN 1;

    END ImportOneCourse;


    /**
    *  Собирает XML по одному платежу и возвращает его в виде CLOB.
    *    paymentid - ID платежа
    *    operation - вид действия с объектом в системе (I - insert, U - update, D - delete)
    *  (функция для использования внутри пакета)
    */
/*
    FUNCTION PaymentXml(p_paymentid IN dpmpaym_dbt.t_paymentid%type)
    RETURN clob
    IS
        pm_rec           dpmpaym_dbt%rowtype;
        rm_rec           dpmrmprop_dbt%rowtype;
        PayerBankCode    dobjcode_dbt.t_code%type;
        ReceiverBankCode dobjcode_dbt.t_code%type;
        xml_obj          clob;
        OBJ_KIND         constant intgr_synch_obj.t_objectkind%type := 501;
    BEGIN

        --если платежа в базе нет, формируется xml из одного тега (id объекта)
        begin
            select * into pm_rec from dpmpaym_dbt where t_paymentid = p_paymentid;

            exception when no_data_found then
                select XMLSerialize(DOCUMENT XMLRoot(XMLFOREST(p_paymentid paymentid),VERSION '1.0'))
                   into xml_obj
                 from dual;

                return xml_obj;
        end;

        --собираем свойства платежа
        begin
            select * into rm_rec
              from dpmrmprop_dbt
            where t_paymentid = p_paymentid;

            exception when no_data_found then
                rm_rec.t_PayerBankName := null;
                rm_rec.t_PayerName := null;
                rm_rec.t_PayerINN := null;
                rm_rec.t_ReceiverBankName := null;
                rm_rec.t_ReceiverName := null;
                rm_rec.t_ReceiverINN := null;
                rm_rec.t_Ground := null;
        end;

        begin
            select pBankCode.t_bankcode,
                   rBankCode.t_bankcode
              into PayerBankCode,
                   ReceiverBankCode
             from (select decode(count(*), 0, NULL, max(t_bankcode)) t_bankcode from dpmprop_dbt where t_paymentid = p_paymentid and t_issender = chr(88)) pBankCode,
                  (select decode(count(*), 0, NULL, max(t_bankcode)) t_bankcode from dpmprop_dbt where t_paymentid = p_paymentid and t_issender = chr(0)) rBankCode;

            exception when no_data_found then
                PayerBankCode := null;
                ReceiverBankCode := null;
        end;

        --собираем XML по платежу + категории + примечания
        select xmltype('<?xml version="1.0" encoding="UTF-8"?>' ||
                 xmlelement("payment",
                 --платеж (начало):
                 (select xmlagg(xmlforest(
                    pm_rec.t_Paymentid                                                                                                 Paymentid,
                    decode(rm_rec.t_Ground, chr(1), null, chr(0), null, rm_rec.t_Ground)                           Ground,
                    decode(GetIsoCodeByFIID(pm_rec.t_FIID), chr(1), null, chr(0), null, GetIsoCodeByFIID(pm_rec.t_FIID))            FIID,
                    nvl(pm_rec.t_Amount, 0)                                                                                          Amount,
                    decode(GetIsoCodeByFIID(pm_rec.t_PayFIID), chr(1), null, chr(0), null, GetIsoCodeByFIID(pm_rec.t_PayFIID))  PayFIID,
                    decode(rm_rec.t_PayerName, chr(1), null, chr(0), null, rm_rec.t_PayerName)              PayerName,
                    decode(rm_rec.t_PayerINN, chr(1), null, chr(0), null, rm_rec.t_PayerINN)                     PayerINN,
                    decode(PayerBankCode, chr(1), null, chr(0), null, PayerBankCode)                               PayerBankCode,
                    decode(rm_rec.t_PayerBankName, chr(1), null, chr(0), null, rm_rec.t_PayerBankName) PayerBankName,
                    decode(pm_rec.t_PayerAccount, chr(1), null, chr(0), null, pm_rec.t_PayerAccount)       PayerAccount,
                    decode(rm_rec.t_ReceiverName, chr(1), null, chr(0), null, rm_rec.t_ReceiverName)      ReceiverName,
                    decode(rm_rec.t_ReceiverINN, chr(1), null, chr(0), null, rm_rec.t_ReceiverINN)            ReceiverINN,
                    decode(ReceiverBankCode, chr(1), null, chr(0), null, ReceiverBankCode)                      ReceiverBankCode,
                    decode(rm_rec.t_ReceiverBankName, chr(1), null, chr(0), null, rm_rec.t_ReceiverBankName) ReceiverBankName,
                    decode(pm_rec.t_ReceiverAccount, chr(1), null, chr(0), null, pm_rec.t_ReceiverAccount) ReceiverAccount,
                    nvl(pm_rec.t_ValueDate, to_date('01010001','ddmmyyyy'))                                         ValueDate,
                    nvl(pm_rec.t_PaymStatus, 0)                                                                                   PaymStatus,
                    decode(pm_rec.t_Netting, chr(0), null, pm_rec.t_Netting)                                           Netting,
                    pm_rec.t_NumberPack                                                                                            NumberPack,
                    decode(pm_rec.t_Isplanpaym, chr(0), null, chr(1), null, pm_rec.t_Isplanpaym)             Isplanpaym,
                    decode(pm_rec.t_Isfactpaym, chr(0), null, chr(1), null, pm_rec.t_Isfactpaym)              Isfactpaym,
                    nvl(pm_rec.t_Payamount, 0)                                                                                    Payamount,
                    pm_rec.t_Ratetype                                                                                                 Ratetype,
                    decode(pm_rec.t_Isinverse, chr(0), null, chr(1), null, pm_rec.t_Isinverse)                   Isinverse,
                    pm_rec.t_Scale                                                                                                     Scale,
                    pm_rec.t_Point                                                                                                      Point,
                    decode(pm_rec.t_Isfixamount, chr(0), null, chr(1), null, pm_rec.t_Isfixamount)           Isfixamount,
                    pm_rec.t_Rate                                                                                                      Rate,
                    nvl(pm_rec.t_BaseAmount, 0)                                                                                 BaseAmount,
                    decode(GetIsoCodeByFIID(pm_rec.t_BaseFIID), chr(1), null, chr(0), null, GetIsoCodeByFIID(pm_rec.t_BaseFIID))  BaseFIID,
                    pm_rec.t_RateDate                                                                                                RateDate,
                    pm_rec.t_BaseRateType                                                                                         BaseRateType,
                    pm_rec.t_BaseRate                                                                                                BaseRate,
                    pm_rec.t_BasePoint                                                                                               BasePoint,
                    pm_rec.t_BaseScale                                                                                              BaseScale,
                    decode(pm_rec.t_IsBaseInverse, chr(0), null, chr(1), null, pm_rec.t_IsBaseInverse)    IsBaseInverse,
                    pm_rec.t_BaseRateDate                                                                                         BaseRateDate,
                    pm_rec.t_PayerDpBlock                                                                                          PayerDpBlock,
                    pm_rec.t_ReceiverDpBlock                                                                                      ReceiverDpBlock,
                    pm_rec.t_I2PlaceDate                                                                                            I2PlaceDate,
                    nvl(pm_rec.t_PayerBankenterDate, to_date('01010001','ddmmyyyy'))                         PayerBankenterDate,
                    nvl(pm_rec.t_Oper, 0)                                                                                           Oper,
                    pm_rec.t_Closedate                                                                                              Closedate,
                    decode(pm_rec.t_Userfield1, chr(1), null, chr(0), null, pm_rec.t_Userfield1)               Userfield1,
                    decode(pm_rec.t_Userfield2, chr(1), null, chr(0), null, pm_rec.t_Userfield2)               Userfield2,
                    decode(pm_rec.t_Userfield3, chr(1), null, chr(0), null, pm_rec.t_Userfield3)               Userfield3,
                    decode(pm_rec.t_Userfield4, chr(1), null, chr(0), null, pm_rec.t_Userfield4)               Userfield4,
                    pm_rec.t_PayType                                                                                                PayType,
                    nvl(pm_rec.t_Creationdate, to_date('01010001','ddmmyyyy'))                                   Creationdate,
                    nvl(pm_rec.t_Creationtime, to_date('01010001','ddmmyyyy'))                                   Creationtime))
                 from dual),
                 --платеж (конец)

                 --пример добавления вложенных xmlagg: (select xmlelement("categories", (select xmlagg(xmlelement(x, rownum)) from dual connect by level <3)) from dual)

                 --категории платежа (начало):
                 (select xmlelement("categories", (select xmlagg(xmlelement(evalname(v.t_code), o.t_attrid))
                                                                   from dobjatcor_dbt o,
                                                                        dllvalues_dbt v
                                                                  where o.t_objecttype = OBJ_KIND
                                                                    and o.t_groupid = v.t_flag
                                                                    and o.t_object = lpad(p_paymentid, 10, chr(48))
                                                                    and v.t_list = SPRAV_NUMBER
                                                                    and upper(v.t_name) like '%PAYMENT%CATEGORY%'))
                 from dual),
                 --категории платежа (конец)

                 --примечания платежа (начало):
                 -- справка по типам данных: select * from dnamealg_dbt where t_itypealg = 3404 (т.е. где посмотреть расшифровку для k.t_notetype)
                 (select xmlelement("notes", (select xmlagg(xmlelement(evalname(v.t_code),
                                                case k.t_notetype
                                                   when 0 then to_char(rsb_struct.getInt(n.t_text),'0d0')
                                                   when 1 then to_char(rsb_struct.getLong(n.t_text),'0d0')
                                                   when 2 then rtrim(trim(to_char(rsb_struct.getDouble(n.t_text), '9999999999999990D09999999')), '0')
                                                   when 3 then rtrim(trim(to_char(rsb_struct.getDouble(n.t_text), '9999999999999990D09999999')), '0')
                                                   when 4 then rtrim(trim(to_char(rsb_struct.getDouble(n.t_text), '9999999999999990D09999999')), '0')
                                                   when 7 then to_char(regexp_replace(rsb_struct.getString(n.t_text), '[[:cntrl:]]'))
                                                   when 9 then to_char(rsb_struct.getDate(n.t_text),'YYYY-MM-DD')
                                                   when 10 then to_char(rsb_struct.getTime(n.t_text),'HH24:MI:SS')
                                                   when 12 then to_char(regexp_replace(rsb_struct.getChar(n.t_text), '[[:cntrl:]]'))
                                                   when 25 then rtrim(trim(to_char(rsb_struct.getMoney(n.t_text), '9999999999999990D09999999')), '0')
                                                   else ''
                                                end))
                                                from dnotetext_dbt n,
                                                       dnotekind_dbt k,
                                                       dllvalues_dbt v
                                               where n.t_objecttype = OBJ_KIND
                                                 and n.t_notekind = v.t_flag
                                                 and n.t_objecttype = k.t_objecttype
                                                 and n.t_notekind = k.t_notekind
                                                 and n.t_documentid = lpad(p_paymentid, 10, chr(48))
                                                 and v.t_list = SPRAV_NUMBER
                                                 and upper(v.t_name) like '%PAYMENT%NOTE%'))
                 from dual)
                 --примечания платежа (конец)

            )).getclobval() --полученная XML конвертируется в CLOB
            into xml_obj   --и сохраняем результат
        from dual;

        return xml_obj;

    END PaymentXml;
*/

    /**
    *  Собирает XML по одной проводке и возвращает его в виде CLOB.
    *    acctrnid - ID проводки
    *    operation - вид действия с объектом в системе (I - insert, U - update, D - delete)
    *  (функция для использования внутри пакета)
    */
/*
    FUNCTION AccTrnXml(p_acctrnid IN dacctrn_dbt.t_acctrnid%type)
    RETURN clob
    IS
        acctrn_rec dacctrn_dbt%rowtype;
        xml_obj    clob;
        OBJ_KIND   constant intgr_synch_obj.t_objectkind%type := 1;
    BEGIN

        --если проводки в базе нет, формируется xml из одного тега (id объекта)
        begin
            select * into acctrn_rec from dacctrn_dbt where t_acctrnid = p_acctrnid;

            exception when no_data_found then
                select XMLSerialize(DOCUMENT XMLRoot(XMLFOREST(p_acctrnid acctrnid),VERSION '1.0'))
                   into xml_obj
                 from dual;

                return xml_obj;
        end;

        --собираем XML по проводке + категории
        select xmltype('<?xml version="1.0" encoding="UTF-8"?>' ||
                 xmlelement("transaction",
                 --проводка (начало):
                 (select xmlagg(xmlforest(
                    acctrn_rec.T_ACCTRNID                                                                                                                   ACCTRNID,
                    acctrn_rec.T_STATE                                                                                                                        STATE,
                    acctrn_rec.T_CHAPTER                                                                                                                    CHAPTER,
                    nvl(acctrn_rec.T_DATE_CARRY, to_date('01010001','ddmmyyyy'))                                                        DATE_CARRY,
                    acctrn_rec.T_DATE_RATE                                                                                                                 DATE_RATE,
                    decode(GetIsoCodeByFIID(acctrn_rec.T_FIID_PAYER), chr(1), null, chr(0), null, GetIsoCodeByFIID(acctrn_rec.T_FIID_PAYER))           FIID_PAYER,
                    decode(GetIsoCodeByFIID(acctrn_rec.T_FIID_RECEIVER), chr(1), null, chr(0), null, GetIsoCodeByFIID(acctrn_rec.T_FIID_RECEIVER)) FIID_RECEIVER,
                    acctrn_rec.T_ACCOUNTID_PAYER                                                                                                      ACCOUNTID_PAYER,
                    acctrn_rec.T_ACCOUNTID_RECEIVER                                                                                                 ACCOUNTID_RECEIVER,
                    decode(acctrn_rec.T_ACCOUNT_PAYER, chr(1), null, chr(0), null, acctrn_rec.T_ACCOUNT_PAYER)          ACCOUNT_PAYER,
                    decode(acctrn_rec.T_ACCOUNT_RECEIVER, chr(1), null, chr(0), null, acctrn_rec.T_ACCOUNT_RECEIVER) ACCOUNT_RECEIVER,
                    acctrn_rec.T_SUM_NATCUR                                                                                                              SUM_NATCUR,
                    nvl(acctrn_rec.T_SUM_PAYER, 0)                                                                                                      SUM_PAYER,
                    nvl(acctrn_rec.T_SUM_RECEIVER, 0)                                                                                                 SUM_RECEIVER,
                    decode(GetIsoCodeByFIID(acctrn_rec.T_FIIDEQ_PAYER), chr(1), null, chr(0), null, GetIsoCodeByFIID(acctrn_rec.T_FIIDEQ_PAYER))   FIIDEQ_PAYER,
                    decode(GetIsoCodeByFIID(acctrn_rec.T_FIIDEQ_RECEIVER), chr(1), null, chr(0), null, GetIsoCodeByFIID(acctrn_rec.T_FIIDEQ_RECEIVER)) FIIDEQ_RECEIVER,
                    decode(acctrn_rec.T_SKIPRESTEQCHANGE, chr(0), null, chr(1), null, acctrn_rec.T_SKIPRESTEQCHANGE) SKIPRESTEQCHANGE,
                    acctrn_rec.T_SUMEQ_PAYER                                                                                                            SUMEQ_PAYER,
                    acctrn_rec.T_SUMEQ_RECEIVER                                                                                                       SUMEQ_RECEIVER,
                    acctrn_rec.T_RESULT_CARRY                                                                                                           RESULT_CARRY,
                    acctrn_rec.T_NUMBER_PACK                                                                                                             NUMBER_PACK,
                    acctrn_rec.T_OPER                                                                                                                          OPER,
                    acctrn_rec.T_DEPARTMENT                                                                                                              DEPARTMENT,
                    acctrn_rec.T_BRANCH                                                                                                                      BRANCH,
                    decode(acctrn_rec.T_NUMB_DOCUMENT, chr(1), null, chr(0), null, acctrn_rec.T_NUMB_DOCUMENT)       NUMB_DOCUMENT,
                    decode(acctrn_rec.T_GROUND, chr(1), null, chr(0), null, acctrn_rec.T_GROUND)                                   GROUND,
                    decode(acctrn_rec.T_SHIFR_OPER, chr(1), null, chr(0), null, acctrn_rec.T_SHIFR_OPER)                       SHIFR_OPER,
                    decode(acctrn_rec.T_KIND_OPER, chr(1), null, chr(0), null, acctrn_rec.T_KIND_OPER)                           KIND_OPER,
                    decode(acctrn_rec.T_TYPEDOCUMENT, chr(1), null, chr(0), null, acctrn_rec.T_TYPEDOCUMENT)            TYPEDOCUMENT,
                    decode(acctrn_rec.T_USERTYPEDOCUMENT, chr(1), null, chr(0), null, acctrn_rec.T_USERTYPEDOCUMENT) USERTYPEDOCUMENT,
                    acctrn_rec.T_PRIORITY                                                                                                                     PRIORITY,
                    acctrn_rec.T_MINPHASE                                                                                                                   MINPHASE,
                    acctrn_rec.T_MAXPHASE                                                                                                                  MAXPHASE,
                    acctrn_rec.T_SYSTEMDATE                                                                                                              SYSTEMDATE,
                    acctrn_rec.T_SYSTEMTIME                                                                                                              SYSTEMTIME,
                    decode(acctrn_rec.T_CHECKSUM, chr(1), null, chr(0), null, acctrn_rec.T_CHECKSUM)                           CHECKSUM,
                    acctrn_rec.T_EXRATEACCTRNID                                                                                                       EXRATEACCTRNID,
                    acctrn_rec.T_PARENTACCTRNID                                                                                                       PARENTACCTRNID,
                    acctrn_rec.T_CLAIMID                                                                                                                     CLAIMID,
                    acctrn_rec.T_METHODID                                                                                                                  METHODID,
                    decode(acctrn_rec.T_MINIMIZATIONTURN, chr(0), null, chr(1), null, acctrn_rec.T_MINIMIZATIONTURN)                  MINIMIZATIONTURN,
                    decode(acctrn_rec.T_EXRATEACCPLUSDEBET, chr(1), null, chr(0), null, acctrn_rec.T_EXRATEACCPLUSDEBET)        EXRATEACCPLUSDEBET,
                    decode(acctrn_rec.T_EXRATEACCPLUSCREDIT, chr(1), null, chr(0), null, acctrn_rec.T_EXRATEACCPLUSCREDIT)     EXRATEACCPLUSCREDIT,
                    decode(acctrn_rec.T_EXRATEACCMINUSDEBET, chr(1), null, chr(0), null, acctrn_rec.T_EXRATEACCMINUSDEBET)    EXRATEACCMINUSDEBET,
                    decode(acctrn_rec.T_EXRATEACCMINUSCREDIT, chr(1), null, chr(0), null, acctrn_rec.T_EXRATEACCMINUSCREDIT) EXRATEACCMINUSCREDIT,
                    decode(acctrn_rec.T_SKIPRECALCSUMNATCUR, chr(0), null, chr(0), null, acctrn_rec.T_SKIPRECALCSUMNATCUR)    SKIPRECALCSUMNATCUR,
                    acctrn_rec.T_FLAGRECALCSUM                                                                                                        FLAGRECALCSUM,
                    acctrn_rec.T_RATE                                                                                                                         RATE,
                    acctrn_rec.T_SCALE                                                                                                                       SCALE,
                    acctrn_rec.T_POINT                                                                                                                        POINT,
                    decode(acctrn_rec.T_ISINVERSE, chr(0), null, chr(1), null, acctrn_rec.T_ISINVERSE)                            ISINVERSE,
                    decode(acctrn_rec.T_USERFIELD1, chr(1), null, chr(0), null, acctrn_rec.T_USERFIELD1)                        USERFIELD1,
                    decode(acctrn_rec.T_USERFIELD2, chr(1), null, chr(0), null, acctrn_rec.T_USERFIELD2)                        USERFIELD2,
                    decode(acctrn_rec.T_USERFIELD3, chr(1), null, chr(0), null, acctrn_rec.T_USERFIELD3)                        USERFIELD3,
                    decode(acctrn_rec.T_USERFIELD4, chr(1), null, chr(0), null, acctrn_rec.T_USERFIELD4)                        USERFIELD4,
                    acctrn_rec.T_NU_STATUS                                                                                                               NU_STATUS,
                    acctrn_rec.T_NU_KIND                                                                                                                    NU_KIND,
                    acctrn_rec.T_NU_STARTDATE                                                                                                         NU_STARTDATE,
                    acctrn_rec.T_NU_ENDDATE                                                                                                             NU_ENDDATE,
                    acctrn_rec.T_NU_ACKDATE                                                                                                             NU_ACKDATE,
                    acctrn_rec.T_FU_ACCTRNID                                                                                                            FU_ACCTRNID,
                    acctrn_rec.T_SIDETRANSACTION                                                                                                    SIDETRANSACTION,
                    acctrn_rec.T_EXRATEEXTRA                                                                                                           EXRATEEXTRA,
                    acctrn_rec.T_USERGROUPID                                                                                                           USERGROUPID,
                    acctrn_rec.T_OFRRECID                                                                                                                 OFRRECID))
                 from dual),
                 --проводка (конец)

                 --категории проводки (начало):
                 (select xmlelement("categories", (select xmlagg(xmlelement(evalname(v.t_code), o.t_attrid))
                                                     from dobjatcor_dbt o,
                                                             dllvalues_dbt v
                                                    where o.t_objecttype = OBJ_KIND
                                                      and o.t_groupid = v.t_flag
                                                      and o.t_object like '%' || p_acctrnid || '%'
                                                      and v.t_list = SPRAV_NUMBER
                                                      and upper(v.t_name) like '%ACCTRN%CATEGORY%'))
                    from dual)
                 --категории проводки (конец)

                 --примечаний у проводки нет (не поддерживается)

            )).getclobval() --полученная XML конвертируется в CLOB
            into xml_obj   --и сохраняем результат
        from dual;

        return xml_obj;

    END AccTrnXml;
*/

    /**
    *  Собирает XML по одному лицевому счету и возвращает его в виде CLOB.
    *    accountid - ID лицевого счета
    *    operation - вид действия с объектом в системе (I - insert, U - update, D - delete)
    *  (функция для использования внутри пакета)
    */
/*
    FUNCTION AccountXml(p_accountid IN dacctrn_dbt.t_acctrnid%type)
    RETURN clob
    IS
        account_rec daccount_dbt%rowtype;
        xml_obj     clob;
        clientcode  dobjcode_dbt.t_code%type;
        OBJ_KIND    constant intgr_synch_obj.t_objectkind%type := 4;

    BEGIN

        --если счета в базе нет, формируется xml из одного тега (id объекта)
        begin
            select * into account_rec from daccount_dbt where t_accountid = p_accountid;

            exception when no_data_found then
                select XMLSerialize(DOCUMENT XMLRoot(XMLFOREST(p_accountid accountid),VERSION '1.0'))
                   into xml_obj
                 from dual;

                return xml_obj;
        end;

        --код клиента в Новой Афине хранится в RS-Bank'e в коде вида CLIENT_CODE_NA
        begin
            select t_code
              into clientcode
              from dobjcode_dbt
             where t_objecttype = 3
               and t_codekind = CLIENT_CODE_NA
               and t_objectid = account_rec.t_client;

            exception when no_data_found then
                clientcode := null;
        end;

        --собираем XML по счету + категории + примечания
        select xmltype('<?xml version="1.0" encoding="UTF-8"?>' ||
                 xmlelement("account",
                 --счет (начало):
                 (select xmlagg(xmlforest(
                    account_rec.t_AccountId                                                                                                          AccountId,
                    account_rec.t_Chapter                                                                                                             Chapter,
                    decode(GetIsoCodeByFIID(account_rec.t_code_currency), chr(1), null, chr(0), null, GetIsoCodeByFIID(account_rec.t_code_currency)) Code_currency,
                    decode(clientcode, chr(1), null, clientcode )                                                                               Client,
                    101                                                                                                                                        ClientCodeKind,
                    decode(upper(account_rec.t_Open_Close), 'З', 'З', 'О')                                                                Open_Close,
                    decode(account_rec.t_Account, chr(1), null, chr(0), null, account_rec.t_Account)                           Account,
                    account_rec.t_Oper                                                                                                                  Oper,
                    decode(account_rec.t_Balance, chr(1), null, chr(0), null, account_rec.t_Balance)                            Balance,
                    decode(account_rec.t_Index2, chr(0), null, chr(1), null, account_rec.t_Index2)                               Index2,
                    decode(account_rec.t_Index3, chr(0), null, chr(1), null, account_rec.t_Index3)                               Index3,
                    decode(account_rec.t_Kind_Account, chr(1), null, chr(0), null, account_rec.t_Kind_Account)            Kind_Account,
                    decode(account_rec.t_Type_Account, chr(1), null, chr(0), null, account_rec.t_Type_Account)          Type_Account,
                    decode(account_rec.t_UserTypeAccount, chr(1), null, chr(0), null, account_rec.t_UserTypeAccount) UserTypeAccount,
                    decode(account_rec.t_EType_Account, chr(1), null, chr(0), null, account_rec.t_EType_Account)       EType_Account,
                    --account_rec.t_LimitDate                                                                                                             LimitDate,
                    nvl(account_rec.t_Open_Date, to_date('01010001','ddmmyyyy'))                                                   Open_Date,
                    account_rec.t_Close_Date                                                                                                         Close_Date,
                    decode(account_rec.t_NameAccount, chr(1), null, account_rec.t_NameAccount)                             NameAccount,
                    account_rec.t_Change_Date                                                                                                      Change_Date,
                    account_rec.t_Change_DatePrev                                                                                                Change_DatePrev,
                    decode(account_rec.t_PairAccount, chr(1), null, chr(0), null, account_rec.t_PairAccount)                 PairAccount,
                    decode(account_rec.t_UserField1, chr(1), null, chr(0), null, account_rec.t_UserField1)                    UserField1,
                    decode(account_rec.t_UserField2, chr(1), null, chr(0), null, account_rec.t_UserField2)                    UserField2,
                    decode(account_rec.t_UserField3, chr(1), null, chr(0), null, account_rec.t_UserField3)                    UserField3,
                    decode(account_rec.t_UserField4, chr(1), null, chr(0), null, account_rec.t_UserField4)                    UserField4,
                    account_rec.t_OperationDate                                                                                                    OperationDate,
                    nvl(account_rec.t_DaysToEnd, 0)                                                                                              DaysToEnd,
                    nvl(account_rec.t_ORScheme, 0)                                                                                              ORScheme,
                    GetBalanceByNumPlan(account_rec.t_accountid, 1)                                                                     Balance1,
                    GetBalanceByNumPlan(account_rec.t_accountid, 2)                                                                     Balance2,
                    GetBalanceByNumPlan(account_rec.t_accountid, 3)                                                                     Balance3,
                    GetBalanceByNumPlan(account_rec.t_accountid, 4)                                                                     Balance4,
                    GetBalanceByNumPlan(account_rec.t_accountid, 5)                                                                     Balance5,
                    GetBalanceByNumPlan(account_rec.t_accountid, 6)                                                                     Balance6,
                    GetBalanceByNumPlan(account_rec.t_accountid, 7)                                                                     Balance7,
                    GetBalanceByNumPlan(account_rec.t_accountid, 8)                                                                     Balance8,
                    GetBalanceByNumPlan(account_rec.t_accountid, 9)                                                                     Balance9,
                    GetBalanceByNumPlan(account_rec.t_accountid, 10)                                                                   Balance10,
                    GetBalanceByNumPlan(account_rec.t_accountid, 11)                                                                   Balance11))
                 from dual),
                 --счет (конец)

                 --категории счета (начало):
                 (select xmlelement("categories", (select xmlagg(xmlelement(evalname(v.t_code), a.t_name))
                                                     from dobjatcor_dbt t,
                                                          dobjattr_dbt a,
                                                          dllvalues_dbt v
                                                    where t.t_objecttype = OBJ_KIND
                                                      and a.t_objecttype = t.t_objecttype
                                                      and a.t_groupid = t.t_groupid
                                                      and a.t_attrid = t.t_attrid
                                                      and t.t_groupid = v.t_flag
                                                      and substr(t.t_object, -20) = account_rec.t_account
                                                      and v.t_list = SPRAV_NUMBER
                                                      and upper(v.t_name) like '%ACCOUNT%CATEGORY%'))
                 from dual),
                 --категории счета (конец)

                 --примечания счета (начало):
                 (select xmlelement("notes", (select xmlagg(xmlelement(evalname(v.t_code),
                                                case k.t_notetype
                                                   when 0 then to_char(rsb_struct.getInt(n.t_text),'0d0')
                                                   when 1 then to_char(rsb_struct.getLong(n.t_text),'0d0')
                                                   when 2 then rtrim(trim(to_char(rsb_struct.getDouble(n.t_text), '9999999999999990D09999999')), '0')
                                                   when 3 then rtrim(trim(to_char(rsb_struct.getDouble(n.t_text), '9999999999999990D09999999')), '0')
                                                   when 4 then rtrim(trim(to_char(rsb_struct.getDouble(n.t_text), '9999999999999990D09999999')), '0')
                                                   when 7 then to_char(regexp_replace(rsb_struct.getString(n.t_text), '[[:cntrl:]]'))
                                                   when 9 then to_char(rsb_struct.getDate(n.t_text),'YYYY-MM-DD')
                                                   when 10 then to_char(rsb_struct.getTime(n.t_text),'HH24:MI:SS')
                                                   when 12 then to_char(regexp_replace(rsb_struct.getChar(n.t_text), '[[:cntrl:]]'))
                                                   when 25 then rtrim(trim(to_char(rsb_struct.getMoney(n.t_text), '9999999999999990D09999999')), '0')
                                                   else ''
                                                end))
                                                from dnotetext_dbt n,
                                                       dnotekind_dbt k,
                                                       dllvalues_dbt v
                                              where n.t_objecttype = OBJ_KIND
                                                 and n.t_notekind = v.t_flag
                                                 and n.t_objecttype = k.t_objecttype
                                                 and n.t_notekind = k.t_notekind
                                                 and substr(n.t_documentid, -20) = account_rec.t_account
                                                 and v.t_list = SPRAV_NUMBER
                                                 and upper(v.t_name) like '%ACCOUNT%NOTE%'))
                 from dual)
                 --примечания счета (конец)

            )).getclobval() --полученная XML конвертируется в CLOB
            into xml_obj   --и сохраняем результат
        from dual;

        return xml_obj;

    END AccountXml;
*/

    /**
     * Собирает XML по всем объектам в таблице синхронизации, которые внешняя система ещё не забрала.
     * Необязательные входные параметры:
     *      p_objkind  -  Тип объекта (по умолчанию: 0 = все)
     *      p_count    -  Максимальное количество объектов ((по умолчанию: 0 = все))
     *      p_datefrom -  Период объектов синхронизации (дата [+время])
     *      p_dateto      (по умолчанию - все записи, без учета ограничения по дате/времени)
    */
/*
    PROCEDURE uGenXMLSynchObj(p_objkind  IN intgr_synch_obj.t_objectkind%type default 0,
                              p_count    IN number                            default 0,
                              p_datefrom IN intgr_synch_obj.t_synch_time%type default null,
                              p_dateto   IN intgr_synch_obj.t_synch_time%type default null)
    IS
        FLAG_NEED_SYNCH constant intgr_synch_obj.t_synch_flag%type := 1; -- требуется синхронизация
        FLAG_PROCESSING constant intgr_synch_obj.t_synch_flag%type := 2; -- в обработке
        FLAG_PASSED     constant intgr_synch_obj.t_synch_flag%type := 3; -- xml создан
        PAYMENT         constant integer                           := 501; --платеж
        ACCTRN          constant integer                           := 1;   --проводка
        ACOUNT          constant integer                           := 4;   --счет
        --SECUR           constant integer                           := 12;  --ЦБ
        objkind_from    intgr_synch_obj.t_objectkind%type;
        objkind_to      intgr_synch_obj.t_objectkind%type;
        date_from       intgr_synch_obj.t_synch_time%type;
        date_to         intgr_synch_obj.t_synch_time%type;
        rownum_from     long;
        rownum_to       long;
        xml_object      clob;

    BEGIN
        if (p_objkind = 0) then
            objkind_from := 0;
            begin
                select max(t_objecttype) into objkind_to from dobjects_dbt;
                exception when no_data_found then
                    objkind_to := 32767;
            end;
        else
            objkind_from := p_objkind;
            objkind_to := p_objkind;
        end if;

        if (p_count = 0) then
            rownum_from := 0;
            begin
                select count(*) into rownum_to
                  from intgr_synch_obj;
                exception when no_data_found then
                    rownum_to := 0;
            end;
        else
            rownum_from := p_count;
            rownum_to := p_count;
        end if;

        if (p_datefrom is null) then
            date_from := to_date('01.01.0001','dd.mm.yyyy');
        else
            date_from := p_datefrom;
        end if;

        if (p_dateto is null) then
            date_to := to_date('31.12.9999','dd.mm.yyyy');
        else
            date_to := p_dateto;
        end if;

        --список невыгруженных объектов
        for rec in (select rowid, t.*
                      from intgr_synch_obj t
                     where t_synch_flag = FLAG_NEED_SYNCH
                       and t_objectkind between objkind_from and objkind_to
                       and t_synch_time between date_from and date_to
                       and rownum between rownum_from and rownum_to
                     order by t_id)
        loop
            update intgr_synch_obj s set t_synch_flag = FLAG_PROCESSING
            where s.rowid = rec.rowid;
            commit;

            xml_object := null;

            case rec.t_objectkind
                when PAYMENT then xml_object := PaymentXml(rec.t_objectid);
                when ACCTRN then xml_object := AccTrnXml(rec.t_objectid);
                when ACOUNT then xml_object := AccountXml(rec.t_objectid);
            end case;

            if (xml_object is not null) then

                merge into intgr_synch_obj_xml t2
                 using (select * from intgr_synch_obj where t_id = rec.t_id) t1
                    on (t2.t_recordid = t1.t_id)
                when matched then update set t2.t_object = xml_object,
                                                             t2.t_start_time = systimestamp
                                                   where t2.t_recordid = t1.t_id
                when not matched then insert (t2.t_recordid, t2.t_object, t2.t_start_time, t2.t_delete)
                                                  values (t1.t_id, xml_object, systimestamp, 0);

                update intgr_synch_obj s set t_synch_flag = FLAG_PASSED
                where s.rowid = rec.rowid;

                commit;
            else
                --???
                null;
            end if;

        end loop;

    END uGenXMLSynchObj;
*/

    /**
    *  Возвращает балансовый счет по accountID и номеру плана счетов.
    *  (функция для использования внутри пакета)
    */
    FUNCTION GetBalanceByNumPlan(account_id IN daccbalance_dbt.t_accountid%type,
                                 num_plan   IN daccbalance_dbt.t_numplan%type)
    RETURN daccbalance_dbt.t_balance%type
    IS
        balance daccbalance_dbt.t_balance%type;

    BEGIN

        begin
            select t_balance into balance
              from daccbalance_dbt
             where t_accountid = account_id
               and t_numplan = num_plan;

            exception when others then
                balance := null;
        end;

        return balance;

    END GetBalanceByNumPlan;


    /**
    *  Возвращает буквенный ISO-код валюты (например, RUB) по FIID.
    *  (функция для использования внутри пакета)
    */
    FUNCTION GetIsoCodeByFIID(fiid IN dfininstr_dbt.t_fiid%type)
    RETURN dfininstr_dbt.t_ccy%type
    IS
        currency dfininstr_dbt.t_ccy%type;

    BEGIN

        begin
            select t_ccy into currency
              from dfininstr_dbt
             where t_fiid = fiid;

            exception when others then
                currency := null;
        end;

        return currency;

    END GetIsoCodeByFIID;


    /**
    *  Возвращает ближайший рабочий день по системному календарю.
    *  (функция для использования внутри пакета)
    */
    FUNCTION GetNearestWorkDate(p_date IN DATE)
    RETURN DATE
    IS
        v_date DATE;
    BEGIN

        v_date := p_date;

        while (RSI_RsbCalendar.IsWorkDay(v_date)= 0) loop
            v_date := v_date + 1;
        end loop;

        return v_date;

    END GetNearestWorkDate;


    /**
    *  Интерфейс Справочника НКД. Процедура выполняет расчет НКД,
    *   результат записывает в UACCCOUPON4RETAIL_TMP.
    *  (функция для использования внутри пакета)
    *       p_date - дата, относитетельно которой выполняется расчет
    */
/*
    PROCEDURE CalcNKD (p_date IN DATE)
    IS
        v_datet0 DATE;
        v_datet1 DATE;
    BEGIN
        v_datet0 := GetNearestWorkDate(p_date);
        v_datet1 := GetNearestWorkDate(v_datet0+1);

        BEGIN
            DELETE FROM UACCCOUPON4RETAIL_TMP;

            INSERT INTO UACCCOUPON4RETAIL_TMP (T_DATE,
                                               T_DATET0,
                                               T_DATET1,
                                               T_SECURITYID,
                                               T_COUPONT0,
                                               T_COUPONT1)
            SELECT trunc(p_date),
                   trunc(v_datet0),
                   trunc(v_datet1),
                   NVL(s.t_isin, chr(1)),
                   RSB_FIInstr.CalcNKD_Ex(f.t_fiid, v_datet0, 1, 0 ),
                   RSB_FIInstr.CalcNKD_Ex(f.t_fiid, v_datet1, 1, 0 )
              FROM dfininstr_dbt f,
                   davrkinds_dbt a,
                   davoiriss_dbt s
             WHERE f.t_avoirkind = a.t_avoirkind
               AND f.t_fi_kind = a.t_fi_kind
               AND f.t_fiid = s.t_fiid
               AND a.t_root = 17--только облигации;
        -- TODO: сделать ограничение по выборке ЦБ - только те, у которых есть признак (возможно это категория) "БО-лайт"

        COMMIT;

        EXCEPTION WHEN others THEN
            RETURN;
    END;

    END CalcNKD;
*/

    -- Получить код договора на срочном рынке
    FUNCTION psb_GetFortsCode( InvestCode IN INTEGER )
    RETURN VARCHAR2 IS
        N1 INTEGER;
        N2 INTEGER;
        N3 INTEGER;
        N4 INTEGER;
        CodeForts Varchar2(255);
        v_InvestCode  INTEGER;
    BEGIN
        /*Так по коду банка*/
        v_InvestCode:= InvestCode - 3500;

        N1:= floor(v_InvestCode/(35*26*36));
        N2:= floor((v_InvestCode - N1*35*26*36)/(26*36));
        N3:= floor((v_InvestCode - N1*35*26*36 - N2*26*36)/36);
        N4:= v_InvestCode -N1*35*26*36 - N2*26*36 - N3*36;

        -- Первый и последний разряды могут быть как с цифрами так и с буквами, а средний только буквы
        N2:= n2+48;
        N3:= n3+58+7;
        n4:= n4+48;

        If (N4>57) THEN -- Если не число а буква, то надо перешагнуть всякие символы
          N4:= N4+7;
        End If;

        If (N2>57) then
          N2:= N2+7;
        end if;

        If(N2>67) then --А тут надо исключить букву D
           N2:= N2+1;
        end if;

        CodeForts:= PrefForts||CHR(N2)||CHR(N3)||CHR(n4);
        Return CodeForts;

    END psb_GetFortsCode;

END;
/

