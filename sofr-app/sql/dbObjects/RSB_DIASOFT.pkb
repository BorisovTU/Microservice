CREATE OR REPLACE package body RSB_DIASOFT is

 /**
 @file RSB_DIASOFT.pkb
   @brief Работа по обмену сообщениями DIASOFT. BIQ-13198
     
   # changeLog
   |date       |author              |tasks                                                     |note                                                        
   |-----------|--------------------|----------------------------------------------------------|-------------------------------------------------------------
   |2025.03.05 |Велигжанин А.В.     |BOSS-7143_BOSS-8146                                       | correctNDR(), заполнение в dnptxop_dbt
   |           |                    |                                                          | полей t_PayMedical, t_Receiver, t_PayPurpose
   |2024.06.21 |Гераськина Т.В.     |BOSS-3366                                                 | Добавлен разбор новых параметров в структуру SendDepoPaymentInfoReq
   |2024.05.21 |Мирошниченко Е.А.   |DEF-62673                                                 | Из процедуры MakeTaxObjByDepoInfo вырезано удаление объектов НДР и добавлено создание отрицательных НДР согласно ТЗ
   |2023.12.27 |Сенников И.В.       |DEF-59114                                                 | Исправление возвращения нескольких строк при получении договора
   |2023.12.26 |Никоноров И.В.      |DEF-59114                                                 | Правки по созданию НДР
   |2023.12.25 |Сенников И.В.       |DEF-58586                                                 | Сортировка обработки
   |2023.11.15 |Сенников И.В.       |BIQ-13198                                                 | Создание                  
   |           |                    |                                                          | Изменена дата получения признака обращаемости бумаги
   |2025.08.22 |Каргаполов Р.И.     |DEF-96962                                                 | Из senddepopaymentinforeq убрана обработка INTR после вставки сообщения, так как она вынесена в  планировщик
   |2025.08.22 |Каргаполов Р.И.     |DEF-96962                                                 | добавлена MakeTaxObjForINTR для обработки всех INTR из планировщика
   |2025.08.25 |Каргаполов Р.И.     |DEF-96962                                                 | Переписана MakeTaxObjByDepoInfo по ТЗ DEF-96962

  */

  type STATUS_LIST_TYPE is table of VARCHAR2(30) ;
  c_StatList STATUS_LIST_TYPE := STATUS_LIST_TYPE('отменена','активна');
  
  vMICEX_CODE VARCHAR2(2000) := RSB_Common.GetRegStrValue('SECUR\MICEX_CODE');
 
  FUNCTION GetMicexID RETURN  NUMBER DETERMINISTIC
  IS
     vMicexID number(10):=0;
  BEGIN
     BEGIN
        select t_objectid into vMicexID from dobjcode_dbt where t_objecttype = 3 and t_codekind = 1 and t_state = 0 and  t_code = vMICEX_CODE;
     EXCEPTION
        WHEN no_data_found THEN NULL;
     END;
     RETURN vMicexID;
  END;

  PROCEDURE SetErrorStatus(p_RecID IN NUMBER, p_Status IN NUMBER, p_ErrCode IN NUMBER, p_Error IN VARCHAR2)
  IS

  BEGIN
    UPDATE DCDRECORDS_DBT
       SET T_STATUS = p_Status,
           T_ERROR  = p_Error
     WHERE T_ID = p_RecID;

    IF p_ErrCode <> 0 THEN
      raise_application_error(p_ErrCode, p_Error);
    END IF; 
  END;

  procedure SetSOFR_IDs(p_ID IN NUMBER)
  as
    rec DCDRECORDS_DBT%ROWTYPE;
    err NUMBER := 0;
  begin
    
    SELECT * INTO rec FROM DCDRECORDS_DBT WHERE t_ID = p_ID;
    
    IF rec.T_PARTYID = -1 AND rec.T_CLIENTID_OBJECTID IS NOT NULL AND rec.T_CLIENTID_OBJECTID <> CHR(1) THEN
      rec.T_PARTYID    := IT_DIASOFT.GetPartyIDByCFT(rec.T_CLIENTID_OBJECTID);
      
      IF rec.T_PARTYID = -1 THEN
        rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
        rec.T_ERROR  := 'Не найден клиент с кодом '||rec.T_CLIENTID_OBJECTID;
        err := 1;
      END IF;
    END IF; 

    IF err = 0 THEN
      IF rec.T_CONTRACTID <= 0 AND rec.T_AGREEMENTNUMBER IS NOT NULL AND rec.T_AGREEMENTNUMBER <> CHR(1) THEN
        rec.T_CONTRACTID := IT_DIASOFT.CheckDBO(rec.T_PARTYID, rec.T_AGREEMENTNUMBER, TO_DATE('01010001','DDMMYYYY'), TO_DATE('31129999','DDMMYYYY'));
      
        IF rec.T_CONTRACTID <= 0 THEN
          rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
          rec.T_ERROR  := 'Не найден договор брокерского обслуживания с номером '||rec.T_AGREEMENTNUMBER;
          err := 1;
        END IF;
      END IF;
    END IF;

    IF err = 0 THEN
      IF rec.T_FIID <= 0 AND rec.T_ISINREGISTRATIONNUMBER IS NOT NULL AND rec.T_ISINREGISTRATIONNUMBER <> CHR(1) THEN
        rec.T_FIID       := IT_DIASOFT.GetAvoirFIID(rec.T_ISINREGISTRATIONNUMBER);

        IF rec.T_FIID <= 0 THEN
          rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
          rec.T_ERROR  := 'Не найдена ценная бумага с ISIN '||rec.T_ISINREGISTRATIONNUMBER;
          rec.T_PROCRESULT := 'ERROR';
          err := 1;
        END IF;
      END IF;
    END IF;

    UPDATE DCDRECORDS_DBT
       SET T_PARTYID = rec.T_PARTYID,
           T_CONTRACTID = rec.T_CONTRACTID,
           T_FIID = rec.T_FIID,
           T_STATUS = rec.t_STATUS,
           T_ERROR = rec.T_ERROR,
           T_PROCRESULT = rec.T_PROCRESULT
     WHERE T_ID = p_ID;
  end;
  
  /**  
    @brief correctNDR - корректировка с минусом НДР + создание операции расчета НОБ 
    @param[in] p_TS_positiveNDR - время положительного НДР 
  */
  procedure correctNDR(p_rec in DCDRECORDS_DBT%ROWTYPE, 
                       txNegativeCoupon in out DNPTXOBJ_DBT%ROWTYPE, 
                       p_TS_positiveNDR in DATE
                       ) is
    txNegativeCouponLink  DCDNPTXOBDC_DBT%ROWTYPE;
    txCorrectRecalc       DNPTXOP_DBT%ROWTYPE;
    v_LastOperationDate   DATE;
    v_NumberOfOperation   NUMBER(10);

    v_NoRecordExists NUMBER := 0;
  begin
    txNegativeCouponLink.T_RECID  := p_rec.T_ID;
    
    txNegativeCoupon.t_ObjID := 0;
    txNegativeCoupon.t_Sum   := -1 * txNegativeCoupon.t_Sum;
    txNegativeCoupon.t_Sum0  := -1 * txNegativeCoupon.t_Sum0;

    INSERT INTO DNPTXOBJ_DBT
    VALUES txNegativeCoupon RETURNING T_OBJID INTO txNegativeCouponLink.T_OBJID;
    
    INSERT INTO DCDNPTXOBDC_DBT
    VALUES txNegativeCouponLink;
    
    BEGIN
        SELECT T_OPERDATE 
        INTO v_LastOperationDate
        FROM (SELECT * 
                FROM DNPTXOP_DBT 
               WHERE T_CLIENT = txNegativeCoupon.T_CLIENT
                 AND T_KIND_OPERATION = 2035 
                 AND T_OPERDATE  BETWEEN p_TS_positiveNDR AND p_rec.t_requestdate
                 AND T_SUBKIND_OPERATION IN (20, 10)
                 AND  ((T_RECALC <> CHR(88)) OR (T_RECALC is null)) 
               ORDER BY T_OPERDATE DESC)
        WHERE ROWNUM = 1;
      
    EXCEPTION WHEN NO_DATA_FOUND 
        THEN v_NoRecordExists := 1;
    END;
    
    IF (v_NoRecordExists = 0)
    THEN 
      v_NumberOfOperation := 0;
      SELECT COUNT(T_CODE) INTO v_NumberOfOperation FROM DNPTXOP_DBT WHERE T_CODE LIKE '62673_%_'||txNegativeCoupon.T_CLIENT;
    
      txCorrectRecalc.T_CODE              := (N'62673_'||TO_CHAR(v_NumberOfOperation+1)||'_'||txNegativeCoupon.T_CLIENT);
      txCorrectRecalc.t_ID                := dnptxop_dbt_seq.NEXTVAL;
      txCorrectRecalc.t_DocKind           := RSI_NPTXC.DL_CALCNDFL;
      txCorrectRecalc.t_OperDate          := v_LastOperationDate;
      txCorrectRecalc.t_Kind_Operation    := 2035;
      txCorrectRecalc.t_Client            := txNegativeCoupon.T_CLIENT;
      txCorrectRecalc.t_Department        := 1;
      txCorrectRecalc.t_Oper              := 1;
      txCorrectRecalc.t_Status            := RSI_NPTXC.DL_TXOP_Prep;
      txCorrectRecalc.t_SubKind_Operation := 20;
      txCorrectRecalc.t_IIS               := CNST.UNSET_CHAR;
      txCorrectRecalc.t_Account           := RSI_RsbOperation.ZERO_STR;
      txCorrectRecalc.t_AccountTax        := RSI_RsbOperation.ZERO_STR;
      txCorrectRecalc.t_BegRecalcDate     := TO_DATE('01.01.'||TO_CHAR(EXTRACT(YEAR FROM v_LastOperationDate)), 'dd.mm.yyyy');
      txCorrectRecalc.t_CalcNDFL          := CNST.SET_CHAR;
      txCorrectRecalc.t_Contract          := 0;
      txCorrectRecalc.t_Currency          := 0;
      txCorrectRecalc.t_CurrencySum       := 0;
      txCorrectRecalc.t_CurrentYear_Sum   := 0;
      txCorrectRecalc.t_EndRecalcDate     := v_LastOperationDate;
      txCorrectRecalc.t_FIID              := -1;
      txCorrectRecalc.t_FlagTax           := CNST.UNSET_CHAR;
      txCorrectRecalc.t_LimitStatus       := 0;
      txCorrectRecalc.t_MarketPlace       := 0;
      txCorrectRecalc.t_MarketPlace2      := 0;
      txCorrectRecalc.t_MarketSector      := 0;
      txCorrectRecalc.t_MarketSector2     := 0;
      txCorrectRecalc.t_Method            := 0;
      txCorrectRecalc.t_OutCost           := 0;
      txCorrectRecalc.t_OutSum            := 0;
      txCorrectRecalc.t_Partial           := CNST.UNSET_CHAR;
      txCorrectRecalc.t_Place             := 0;
      txCorrectRecalc.t_Place2            := 0;
      txCorrectRecalc.t_PlaceKind         := 0;
      txCorrectRecalc.t_PlaceKind2        := 0;
      txCorrectRecalc.t_PrevDate          := txNegativeCoupon.T_DATE;
      txCorrectRecalc.t_PrevTaxSum        := 0;
      txCorrectRecalc.t_Recalc            := CNST.SET_CHAR;
      txCorrectRecalc.t_Tax               := 0;
      txCorrectRecalc.t_TaxBase           := 0;
      txCorrectRecalc.t_TaxSum            := 0;
      txCorrectRecalc.t_TaxSum2           := 0;
      txCorrectRecalc.t_TaxToPay          := 0;
      txCorrectRecalc.t_Time              := NPTAX.UnknownTime;
      txCorrectRecalc.t_TotalTaxSum       := 0;
      txCorrectRecalc.t_TOUT              := 0;
      txCorrectRecalc.t_TaxDp             := 0;
      txCorrectRecalc.t_IIS               := CNST.UNSET_CHAR;
      txCorrectRecalc.t_PayMedical        := CNST.UNSET_CHAR;       -- BOSS-7143_BOSS-8146
      txCorrectRecalc.t_Receiver          := -1;
      txCorrectRecalc.t_PayPurpose        := 0;
    
      INSERT INTO DNPTXOP_DBT
      VALUES txCorrectRecalc;
      
    END IF;
  end correctNDR;
  
  /**  
    @brief createNDR - создание положительного НДР 
  */
  procedure createNDR(p_rec in DCDRECORDS_DBT%ROWTYPE) is
    txObj                 DNPTXOBJ_DBT%ROWTYPE;
    txObjObDc             DCDNPTXOBDC_DBT%ROWTYPE;
    v_Fiid                NUMBER(10);
    v_SfcontrId           NUMBER(10);
    v_Cur                 NUMBER(10);
    v_ClientId            NUMBER(10);
    v_micexID             NUMBER(10) := GetMicexID();
  begin
    txObj.T_OUTSYSTCODE := 'ДЕПО';
    txObj.T_OUTOBJID := p_rec.T_RecordPaymentID;
    --txObj.T_SOURCEOBJID := p_rec.T_RECORDPAYMENTQTYID;
    
    txObj.T_ANALITICKIND1 := 0;
    txObj.T_ANALITIC1 := -1;
    txObj.T_ANALITICKIND2 := 0;
    txObj.T_ANALITIC2 := -1;
    txObj.T_ANALITICKIND3 := 0;
    txObj.T_ANALITIC3 := -1;
    txObj.T_ANALITICKIND4 := 0;
    txObj.T_ANALITIC4 := -1;
    txObj.T_ANALITICKIND5 := 0;
    txObj.T_ANALITIC5 := -1;
    txObj.T_ANALITICKIND6 := 0;
    txObj.T_ANALITIC6 := -1;
    txObj.T_DATE := p_rec.T_PAYMENTDATE;
    txObj.T_CUR := RSI_RSB_FIInstr.NATCUR;
    
    txObjObDc.T_RECID := p_rec.T_ID;
    
    begin
      select T_FIID into v_Fiid from DAVOIRISS_DBT where T_ISIN = p_rec.T_ISINREGISTRATIONNUMBER;
      txObj.T_ANALITIC3 := v_Fiid;
      txObj.T_ANALITICKIND3 := RSI_NPTXC.TXOBJ_KIND3010;
    
      txObj.T_ANALITIC4 := RSI_NPTO.Market3date(v_Fiid, p_rec.t_PayReceivedDate);
      txObj.T_ANALITICKIND4 := RSI_NPTXC.TXOBJ_KIND4010;
    
      txObj.T_ANALITIC5 := npto.GetPaperTaxGroupNPTX(v_Fiid);
      txObj.T_ANALITICKIND5 := RSI_NPTXC.TXOBJ_KIND5010;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN 
        SetErrorStatus(p_rec.T_ID, RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR, -20001, 'Не найдена запись ЦБ в таблице DAVOIRISS_DBT по ISIN "'||p_rec.T_ISINREGISTRATIONNUMBER||'"');
    end;
    
    begin
      select q.T_ID, q.T_PARTYID
             into v_SfcontrId, v_ClientId
        from (select sf_mp.T_ID, sf_mp.T_PARTYID 
                from DSFCONTR_DBT sf, ddlcontr_dbt dlc, ddlcontrmp_dbt mp, dsfcontr_dbt sf_mp 
               where sf.T_NUMBER = p_rec.T_AGREEMENTNUMBER
                 and dlc.t_SfContrID = sf.t_ID
                 and mp.t_DlContrID = dlc.t_DlContrID
                 and sf_mp.t_ID = mp.t_SfContrID
                 and sf_mp.t_ServKind = 1 
                 and mp.T_MARKETID = v_micexID
                 and sf_mp.t_DateBegin <= p_rec.T_PAYMENTDATE
               order by (case when sf_mp.t_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY') then TO_DATE('31.12.9999','DD.MM.YYYY') else sf_mp.t_DateClose end) DESC
             ) q
      where rownum = 1;
      
      
      txObj.T_ANALITIC6 := v_SfcontrId;
      txObj.T_ANALITICKIND6 := RSI_NPTXC.TXOBJ_KIND6020;
      txObj.T_CLIENT := v_ClientId;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        SetErrorStatus(p_rec.T_ID, RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR, -20002, 'Не найден субдоговор фондового рынка для ДБО с номером "'||p_rec.T_AGREEMENTNUMBER||'"');
    end;
    
    begin
      select T_FIID into v_Cur from DFININSTR_DBT where T_ISO_NUMBER = CASE WHEN p_rec.T_ISSUERCURRENCY = '810' THEN '643' ELSE p_rec.T_ISSUERCURRENCY END;
      txObj.T_CUR := v_Cur;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        SetErrorStatus(p_rec.T_ID, RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR, -20003, 'Не найдена запись валюты в таблице DFININSTR_DBT по T_ISO_NUMBER "'||p_rec.T_ISSUERCURRENCY||'"');
    end;
    
    if (p_rec.T_COUPONNUMBER > 0) then
      txObj.T_ANALITICKIND2 := RSI_NPTXC.TXOBJ_KIND2030;
      txObj.T_ANALITIC2 := p_rec.T_COUPONNUMBER;
    end if;
    
    txObj.T_LEVEL := 2;
    txObj.T_USER := CHR(0);
    txObj.T_TECHNICAL := CHR(0);
    txObj.T_KIND := RSI_NPTXC.TXOBJ_PROCSEC_TS;
    txObj.T_DIRECTION := RSI_NPTXC.TXOBJ_DIR_IN;
    txObj.T_FROMOUTSYST := CHR(0);
    txObj.T_SUM := p_rec.t_TaxBase;
    txObj.T_SUM0 := RSI_RSB_FIInstr.ConvSum( txObj.T_SUM, txObj.T_CUR, RSI_RSB_FIInstr.NATCUR, txObj.T_DATE, 1 );
    
    insert into DNPTXOBJ_DBT values txObj RETURNING T_OBJID into txObjObDc.T_OBJID;
    -- Связь между выплатой и созданным объектом НДР сохраняется в таблицу dcdnptxobdc_dbt
    insert into DCDNPTXOBDC_DBT values txObjObDc;
  end createNDR;

  /**
    @brief MakeTaxObjByDepoInfo - основные действия 
    @param[in] p_rec - запись по одну из двух блоков Payment XML
  */
  procedure MakeTaxObjByDepoInfo(p_rec DCDRECORDS_DBT%ROWTYPE)
  as
    v_ObjID NUMBER := 0;
    v_ID     NUMBER := 0;
    v_obj     DNPTXOBJ_DBT%ROWTYPE;
    v_CDRecords DCDRECORDS_DBT%ROWTYPE;

    v_RequestDate DATE;
    v_RequestTime DATE;
  begin
  
    IF (LOWER(TRIM(p_rec.t_OperationStatus)) = 'активна' AND p_rec.t_ISGETTAX <> 'X' AND p_rec.t_ACCOUNTNUMBER LIKE '306%') THEN
        --Сценарий 1.
        --если имеется новая активная запись, и есть такая же предыдущая активная запись, но между ними не было отмены. Надо сформировать отриц.НДР по пред. активной записи как будто была отмена. 
        BEGIN
          SELECT t_ID, t_ObjID
            INTO v_ID, v_ObjID
            FROM (SELECT RECR.T_ID, OBJC.t_ObjID
                    FROM DCDNPTXOBDC_DBT    LINK,
                         DNPTXOBJ_DBT       OBJC,
                         DCDRECORDS_DBT     RECR
                   WHERE RECR.T_RECORDPAYMENTQTYID = p_rec.T_RECORDPAYMENTQTYID
                    AND RECR.T_RECORDPAYMENTID = p_rec.T_RECORDPAYMENTID
                    AND (RECR.T_REQUESTDATE + (RECR.T_REQUESTTIME - TRUNC(RECR.T_REQUESTTIME))) <= (p_rec.T_REQUESTDATE + (p_rec.T_REQUESTTIME - TRUNC(p_rec.T_REQUESTTIME)))
                    AND RECR.T_ID < p_rec.T_ID -- при условии дат "<=" запрос захватывает и текущую запись p_rec, правда у нее нет НДР и она не должна попадать. На всякий случай это условие ее отсекает
                    AND LINK.T_RECID = RECR.T_ID
                    AND LINK.T_OBJID = OBJC.T_OBJID
                ORDER BY recr.t_RequestDate DESC, recr.t_RequestTime DESC, RECR.T_ID DESC)
          WHERE ROWNUM = 1;

        EXCEPTION WHEN NO_DATA_FOUND 
            THEN
                v_ObjID := 0;
        END;

        IF v_ObjID > 0 THEN    --запись с НДР имеется
            SELECT * INTO v_obj FROM dnptxobj_dbt WHERE t_ObjID = v_ObjID;
            --за счет запроса сразу со связью с НДР и сортировкой даты+время по убыванию, мы получаем тут либо отмену (Sum<0), либо активную запись (Sum>0)
            IF v_obj.t_Sum0 > 0 THEN
                -- отмененной записи нет, это предыдушая активная, формируем на нее отрицательный НДР
                SELECT * INTO v_CDRecords FROM DCDRECORDS_DBT WHERE t_ID = v_ID; -- предыдущая активная
                correctNDR(p_rec, v_obj, v_CDRecords.t_RequestDate);
            END IF;
        END IF;

        --обработка активной записи, то создать положительный НДР
        createNDR(p_rec);
        --на обработанной записи статус "Обработано". очищает поле с описание ошибки, если было заполнено
        UPDATE DCDRECORDS_DBT SET T_PROCRESULT = 'OK', T_STATUS = RSB_DIASOFT.CDRECORDS_STATUS_PROCESSED, T_ERROR = '' WHERE T_ID = p_rec.T_ID;
        
    ELSIF LOWER(TRIM(p_rec.t_OperationStatus)) = 'отменена' THEN
        --Сценарий 2.
        --найти предыдущую отмененную запись с самой "старой"
        BEGIN
            SELECT T_REQUESTDATE, T_REQUESTTIME INTO v_RequestDate, v_RequestTime 
            FROM DCDRECORDS_DBT
            WHERE LOWER(TRIM(t_OperationStatus)) = 'отменена'
                AND T_RECORDPAYMENTQTYID = p_rec.T_RECORDPAYMENTQTYID
                AND (T_REQUESTDATE + (T_REQUESTTIME - TRUNC(T_REQUESTTIME))) < (p_rec.T_REQUESTDATE + (p_rec.T_REQUESTTIME - TRUNC(p_rec.T_REQUESTTIME)))
                AND LOWER(TRIM(t_CorporateActionType)) IN ('intr')
            ORDER BY t_RequestDate DESC, t_RequestTime DESC, t_ID DESC
            FETCH FIRST 1 ROWS ONLY;
          
            --предыдущая отмена найдена
            --Для отобранной "отмененной" записи сначала найти "активную" запись, к которой относится отмена
            BEGIN
                SELECT * INTO v_CDRecords
                    FROM DCDRECORDS_DBT
                    WHERE LOWER(TRIM(t_OperationStatus)) = 'активна'
                        AND T_RECORDPAYMENTQTYID = p_rec.T_RECORDPAYMENTQTYID
                        AND (v_RequestDate + (v_RequestTime - TRUNC(v_RequestTime))) <= (T_REQUESTDATE + (T_REQUESTTIME - TRUNC(T_REQUESTTIME)))
                        AND (T_REQUESTDATE + (T_REQUESTTIME - TRUNC(T_REQUESTTIME))) <  (p_rec.T_REQUESTDATE + (p_rec.T_REQUESTTIME - TRUNC(p_rec.T_REQUESTTIME))) --ищем текущую между двумя отменами
                        AND LOWER(TRIM(t_CorporateActionType)) IN ('intr')
                        AND t_ACCOUNTNUMBER LIKE '306%'
                    ORDER BY t_RequestDate DESC, t_RequestTime DESC, t_ID DESC
                    FETCH FIRST 1 ROWS ONLY;
                    
            EXCEPTION WHEN NO_DATA_FOUND 
                THEN
                --Если ни одна активная запись с 306% счетом не найдена, то объект НДР не формируется, на обрабатываемой отменной записи в DCDRECORDS_DBT проставляет статус  = Обработано
                    UPDATE DCDRECORDS_DBT SET T_PROCRESULT = 'OK', T_STATUS = RSB_DIASOFT.CDRECORDS_STATUS_PROCESSED, T_ERROR = '' WHERE T_ID = p_rec.T_ID;
                    RETURN; -- Текущая запись обработана. Прерываем выполнение
            END;
            
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
            --предыдущая отмена НЕ найдена - даты берем из текущей отмены
            BEGIN
                SELECT * INTO v_CDRecords
                    FROM DCDRECORDS_DBT
                    WHERE LOWER(TRIM(t_OperationStatus)) = 'активна'
                        AND T_RECORDPAYMENTQTYID = p_rec.T_RECORDPAYMENTQTYID
                        AND (T_REQUESTDATE + (T_REQUESTTIME - TRUNC(T_REQUESTTIME))) < (p_rec.T_REQUESTDATE + (p_rec.T_REQUESTTIME - TRUNC(p_rec.T_REQUESTTIME)))
                        AND LOWER(TRIM(t_CorporateActionType)) IN ('intr')
                        AND t_ACCOUNTNUMBER LIKE '306%'
                    ORDER BY t_RequestDate DESC, t_RequestTime DESC, t_ID DESC
                    FETCH FIRST 1 ROWS ONLY;
                    
            EXCEPTION WHEN NO_DATA_FOUND 
                THEN
                --Если ни одна активная запись с 306% счетом не найдена, то объект НДР не формируется, на обрабатываемой отменной записи в DCDRECORDS_DBT проставляет статус  = Обработано
                    UPDATE DCDRECORDS_DBT SET T_PROCRESULT = 'OK', T_STATUS = RSB_DIASOFT.CDRECORDS_STATUS_PROCESSED, T_ERROR = '' WHERE T_ID = p_rec.T_ID;
                    RETURN; -- Текущая запись обработана. Прерываем выполнение
            END;
        END;
            
        IF (v_CDRecords.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_PROCESSED) THEN
            --Если найдена активная запись по условию с 306 счетом, но статус <> Обработано
            UPDATE DCDRECORDS_DBT SET T_PROCRESULT = 'OK', T_STATUS = RSB_DIASOFT.CDRECORDS_STATUS_PROCESSING_ERROR, T_ERROR = 'Отменяемая выплата не обработана ' || p_rec.T_RECORDPAYMENTQTYID WHERE T_ID = p_rec.T_ID;
            
        ELSIF (v_CDRecords.T_STATUS = RSB_DIASOFT.CDRECORDS_STATUS_PROCESSED) THEN
            --найдена активная запись по условию и статус = Обработано
            --получаем link
            SELECT t_ObjID INTO v_ObjID FROM DCDNPTXOBDC_DBT WHERE T_RECID = v_CDRecords.t_id AND ROWNUM = 1;
             
             IF v_ObjID > 0 THEN
                  SELECT * INTO v_obj FROM dnptxobj_dbt WHERE t_ObjID = v_ObjID;
                
                IF v_obj.t_Sum0 > 0 THEN  --Если это положительный объект
                    --Такой объект нужно отменить
                    correctNDR(p_rec, v_obj, v_CDRecords.t_RequestDate);
                    --на обработанной записи статус "Обработано"
                    UPDATE DCDRECORDS_DBT SET T_PROCRESULT = 'OK', T_STATUS = RSB_DIASOFT.CDRECORDS_STATUS_PROCESSED, T_ERROR = '' WHERE T_ID = p_rec.T_ID;
                END IF;
            ELSE 
                --не найден link
                UPDATE DCDRECORDS_DBT SET T_PROCRESULT = 'OK', T_STATUS = RSB_DIASOFT.CDRECORDS_STATUS_PROCESSING_ERROR, T_ERROR = 'Не найдена связь с НДР в таблице DCDNPTXOBDC_DBT' WHERE T_ID = p_rec.T_ID;
            END IF;
        END IF;
            
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- если createNDR или updateNDR установили на записи ошибку и сгенерировали исключение мы его игнорируем так как это обработка в цикле по записям
        NULL;
  END;

  PROCEDURE InitCDRECORDS(p_cdrec IN OUT DCDRECORDS_DBT%ROWTYPE)
  AS
  BEGIN
    p_cdrec.T_ID                     := 0;
    p_cdrec.T_GUID                   := CHR(1);
    p_cdrec.T_REQUESTTIME            := TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS');
    p_cdrec.T_SHORTNAME              := CHR(1);
    p_cdrec.T_FULLNAME               := CHR(1);
    p_cdrec.T_AGREEMENTNUMBER        := CHR(1);
    p_cdrec.T_ISIIS                  := CHR(0);
    p_cdrec.T_AGREEMENTOPENDATE      := TO_DATE('01.01.0001','DD.MM.YYYY');
    p_cdrec.T_AGREEMENTCLOSEDATE     := TO_DATE('01.01.0001','DD.MM.YYYY');
    p_cdrec.T_CORPORATEACTIONTYPE    := CHR(1);
    p_cdrec.T_PAYMENTTYPE            := CHR(1);
    p_cdrec.T_RECORDPAYMENTID        := 0;
    p_cdrec.T_RECORDPAYMENTQTYID     := 0;
    p_cdrec.T_OPERATIONSTATUS        := CHR(1);
    p_cdrec.T_PAYMENTDATE            := TO_DATE('01.01.0001','DD.MM.YYYY');
    p_cdrec.T_CLIENTID_OBJECTID      := CHR(1);
    p_cdrec.T_CLIENTID_SYSTEMID      := CHR(1);
    p_cdrec.T_CLIENTID_SYSTEMNODEID  := CHR(1);
    p_cdrec.T_FINANCIALNAME          := CHR(1);
    p_cdrec.T_ISINREGISTRATIONNUMBER := CHR(1);
    p_cdrec.T_RECORDSNOBID           := CHR(1);
    p_cdrec.T_TAXRATE                := 0;
    p_cdrec.T_TAXBASE                := 0;
    p_cdrec.T_ISSUERCURRENCY         := CHR(1);
    p_cdrec.T_ISSUERSUM              := 0;
    p_cdrec.T_CLIENTSUM              := 0;
    p_cdrec.T_CLIENTCURRENCY         := CHR(1);
    p_cdrec.T_KBK                    := CHR(1);
    p_cdrec.T_RETURNTAX              := 0;
    p_cdrec.T_INDIVIDUALTAX          := 0;
    p_cdrec.T_TAXREDUCTIONSUM        := 0;
    p_cdrec.T_SUMD1                  := 0;
    p_cdrec.T_SUMD2                  := 0;
    p_cdrec.T_OFFSETTAX              := 0;
    p_cdrec.T_ACCOUNTNUMBER          := CHR(1);
    p_cdrec.T_OPERATIONDATE          := TO_DATE('01.01.0001','DD.MM.YYYY');
    p_cdrec.T_ISGETTAX               := CHR(0);
    p_cdrec.T_COUPONNUMBER           := 0;
    p_cdrec.T_COUPONSTARTDATE        := TO_DATE('01.01.0001','DD.MM.YYYY');
    p_cdrec.T_COUPONENDDATE          := TO_DATE('01.01.0001','DD.MM.YYYY');
    p_cdrec.T_PROCRESULT             := CHR(1);
    p_cdrec.T_PAYRECEIVEDDATE        := TO_DATE('01.01.0001','DD.MM.YYYY');
    p_cdrec.T_FIXINGDATE             := TO_DATE('01.01.0001','DD.MM.YYYY');
    p_cdrec.T_QUANTITY               := 0;
    p_cdrec.T_REQUESTDATE            := TO_DATE('01.01.0001','DD.MM.YYYY');
    p_cdrec.T_STATUS                 := RSB_DIASOFT.CDRECORDS_STATUS_NEW;
    p_cdrec.T_ERROR                  := CHR(1);
    p_cdrec.T_PARTYID                := -1;
    p_cdrec.T_CONTRACTID             := 0;
    p_cdrec.T_FIID                   := -1;
  END;

  PROCEDURE InsDfltIntoCDRECORDS( p_cdrec IN OUT DCDRECORDS_DBT%ROWTYPE )
  IS
  BEGIN
    p_cdrec.T_ID                     := NVL(p_cdrec.T_ID                    , 0);
    p_cdrec.T_GUID                   := NVL(p_cdrec.T_GUID                  , CHR(1));
    p_cdrec.T_REQUESTTIME            := NVL(p_cdrec.T_REQUESTTIME           , TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS'));
    p_cdrec.T_SHORTNAME              := NVL(p_cdrec.T_SHORTNAME             , CHR(1));
    p_cdrec.T_FULLNAME               := NVL(p_cdrec.T_FULLNAME              , CHR(1));
    p_cdrec.T_AGREEMENTNUMBER        := NVL(p_cdrec.T_AGREEMENTNUMBER       , CHR(1));
    p_cdrec.T_ISIIS                  := NVL(p_cdrec.T_ISIIS                 , CHR(0));
    p_cdrec.T_AGREEMENTOPENDATE      := NVL(p_cdrec.T_AGREEMENTOPENDATE     , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_cdrec.T_AGREEMENTCLOSEDATE     := NVL(p_cdrec.T_AGREEMENTCLOSEDATE    , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_cdrec.T_CORPORATEACTIONTYPE    := NVL(p_cdrec.T_CORPORATEACTIONTYPE   , CHR(1));
    p_cdrec.T_PAYMENTTYPE            := NVL(p_cdrec.T_PAYMENTTYPE           , CHR(1));
    p_cdrec.T_RECORDPAYMENTID        := NVL(p_cdrec.T_RECORDPAYMENTID       , 0);
    p_cdrec.T_RECORDPAYMENTQTYID     := NVL(p_cdrec.T_RECORDPAYMENTQTYID    , 0);
    p_cdrec.T_OPERATIONSTATUS        := NVL(p_cdrec.T_OPERATIONSTATUS       , CHR(1));
    p_cdrec.T_PAYMENTDATE            := NVL(p_cdrec.T_PAYMENTDATE           , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_cdrec.T_CLIENTID_OBJECTID      := NVL(p_cdrec.T_CLIENTID_OBJECTID     , CHR(1));
    p_cdrec.T_CLIENTID_SYSTEMID      := NVL(p_cdrec.T_CLIENTID_SYSTEMID     , CHR(1));
    p_cdrec.T_CLIENTID_SYSTEMNODEID  := NVL(p_cdrec.T_CLIENTID_SYSTEMNODEID , CHR(1));
    p_cdrec.T_FINANCIALNAME          := NVL(p_cdrec.T_FINANCIALNAME         , CHR(1));
    p_cdrec.T_ISINREGISTRATIONNUMBER := NVL(p_cdrec.T_ISINREGISTRATIONNUMBER, CHR(1));
    p_cdrec.T_RECORDSNOBID           := NVL(p_cdrec.T_RECORDSNOBID          , CHR(1));
    p_cdrec.T_TAXRATE                := NVL(p_cdrec.T_TAXRATE               , 0);
    p_cdrec.T_TAXBASE                := NVL(p_cdrec.T_TAXBASE               , 0);
    p_cdrec.T_ISSUERCURRENCY         := NVL(p_cdrec.T_ISSUERCURRENCY        , CHR(1));
    p_cdrec.T_ISSUERSUM              := NVL(p_cdrec.T_ISSUERSUM             , 0);
    p_cdrec.T_CLIENTSUM              := NVL(p_cdrec.T_CLIENTSUM             , 0);
    p_cdrec.T_CLIENTCURRENCY         := NVL(p_cdrec.T_CLIENTCURRENCY        , CHR(1));
    p_cdrec.T_KBK                    := NVL(p_cdrec.T_KBK                   , CHR(1));
    p_cdrec.T_RETURNTAX              := NVL(p_cdrec.T_RETURNTAX             , 0);
    p_cdrec.T_INDIVIDUALTAX          := NVL(p_cdrec.T_INDIVIDUALTAX         , 0);
    p_cdrec.T_TAXREDUCTIONSUM        := NVL(p_cdrec.T_TAXREDUCTIONSUM       , 0);
    p_cdrec.T_SUMD1                  := NVL(p_cdrec.T_SUMD1                 , 0);
    p_cdrec.T_SUMD2                  := NVL(p_cdrec.T_SUMD2                 , 0);
    p_cdrec.T_OFFSETTAX              := NVL(p_cdrec.T_OFFSETTAX             , 0);
    p_cdrec.T_ACCOUNTNUMBER          := NVL(p_cdrec.T_ACCOUNTNUMBER         , CHR(1));
    p_cdrec.T_OPERATIONDATE          := NVL(p_cdrec.T_OPERATIONDATE         , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_cdrec.T_ISGETTAX               := NVL(p_cdrec.T_ISGETTAX              , CHR(0));
    p_cdrec.T_COUPONNUMBER           := NVL(p_cdrec.T_COUPONNUMBER          , 0);
    p_cdrec.T_COUPONSTARTDATE        := NVL(p_cdrec.T_COUPONSTARTDATE       , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_cdrec.T_COUPONENDDATE          := NVL(p_cdrec.T_COUPONENDDATE         , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_cdrec.T_PROCRESULT             := NVL(p_cdrec.T_PROCRESULT            , CHR(1));
    p_cdrec.T_PAYRECEIVEDDATE        := NVL(p_cdrec.T_PAYRECEIVEDDATE       , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_cdrec.T_FIXINGDATE             := NVL(p_cdrec.T_FIXINGDATE            , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_cdrec.T_QUANTITY               := NVL(p_cdrec.T_QUANTITY              , 0);
    p_cdrec.T_REQUESTDATE            := NVL(p_cdrec.T_REQUESTDATE           , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_cdrec.T_STATUS                 := NVL(p_cdrec.T_STATUS                , RSB_DIASOFT.CDRECORDS_STATUS_NEW);
    p_cdrec.T_ERROR                  := NVL(p_cdrec.T_ERROR                 , CHR(1));
    p_cdrec.T_PARTYID                := NVL(p_cdrec.T_PARTYID               , -1);
    p_cdrec.T_CONTRACTID             := NVL(p_cdrec.T_CONTRACTID            , 0);
    p_cdrec.T_FIID                   := NVL(p_cdrec.T_FIID                  , -1);
  END;


  --  Обработчик SendDepoPaymentInfoReq
  procedure SendDepoPaymentInfoReq(p_worklogid integer
                                  ,p_messbody  clob
                                  ,p_messmeta  xmltype
                                  ,o_msgid     out varchar2
                                  ,o_MSGCode   out integer
                                  ,o_MSGText   out varchar2
                                  ,o_messbody  out clob
                                  ,o_messmeta  out xmltype)
  as
    v_xml_in xmltype;
    rec DCDRECORDS_DBT%ROWTYPE;
    recId NUMBER(10);
    v_testValue VARCHAR2(500);
    v_namespace varchar2(128):= it_kafka.get_namespace(p_system_name => 'DIASOFT', p_rootelement => 'SendDepoPaymentInfoReq');
    v_errtxt varchar2(4000);

    v_ExistTag NUMBER := 0;

  begin
    v_xml_in := it_xml.Clob_to_xml(p_messbody);

    for PaymentElem in (select VALUE(t) as elem from table(XMLSEQUENCE(EXTRACT(v_xml_in, '//SendDepoPaymentInfoReq/PaymentList/Payment', v_namespace))) t) loop
      rec := NULL;

      InitCDRECORDS(rec);

      select                                          EXTRACTVALUE(v_xml_in, '//SendDepoPaymentInfoReq/GUID', v_namespace),  /*xs:string*/
             to_date(to_char(it_xml.char_to_timestamp(EXTRACTVALUE(v_xml_in, '//SendDepoPaymentInfoReq/RequestTime', v_namespace)),'dd.mm.yyyy'),'dd.mm.yyyy'),
             to_date(to_char(it_xml.char_to_timestamp(EXTRACTVALUE(v_xml_in, '//SendDepoPaymentInfoReq/RequestTime', v_namespace)),'"01.01.0001" hh24:mi:ss'),'dd.mm.yyyy hh24:mi:ss')
        into rec.T_GUID,
             rec.T_REQUESTDATE,
             rec.T_REQUESTTIME
        from dual;


      select             EXTRACTVALUE(PaymentElem.elem, '/Payment/ShortName', v_namespace), /*xs:string*/
                         EXTRACTVALUE(PaymentElem.elem, '/Payment/FullName', v_namespace), /*xs:string*/
                         EXTRACTVALUE(PaymentElem.elem, '/Payment/AgreementNumber', v_namespace), /*xs:string*/
         CASE WHEN LOWER(EXTRACTVALUE(PaymentElem.elem, '/Payment/IsIIS', v_namespace)) = '1' THEN CHR(88) ELSE CHR(0) END, /*xs:boolean*/
     it_xml.char_to_date(EXTRACTVALUE(PaymentElem.elem, '/Payment/AgreementOpenDate', v_namespace)), /*xs:date*/
     it_xml.char_to_date(EXTRACTVALUE(PaymentElem.elem, '/Payment/AgreementCloseDate', v_namespace)), /*xs:date*/
                         EXTRACTVALUE(PaymentElem.elem, '/Payment/CorporateActionType', v_namespace), /*xs:string*/
                         EXTRACTVALUE(PaymentElem.elem, '/Payment/PaymentType', v_namespace), /*xs:string*/
               to_number(EXTRACTVALUE(PaymentElem.elem, '/Payment/RecordPaymentId', v_namespace)), /*xs:integer*/
     it_xml.char_to_date(EXTRACTVALUE(PaymentElem.elem, '/Payment/PaymentDate', v_namespace)),  /*xs:date*/
               to_number(EXTRACTVALUE(PaymentElem.elem, '/Payment/CouponNumber', v_namespace) default null on CONVERSION ERROR), /*xs:integer*/
     it_xml.char_to_date(EXTRACTVALUE(PaymentElem.elem, '/Payment/CouponStartDate', v_namespace)), /*xs:date*/
     it_xml.char_to_date(EXTRACTVALUE(PaymentElem.elem, '/Payment/CouponEndDate', v_namespace)), /*xs:date*/
                         EXTRACTVALUE(PaymentElem.elem, '/Payment/FinancialName', v_namespace), /*xs:string*/
                         EXTRACTVALUE(PaymentElem.elem, '/Payment/ISINRegistrationNumber', v_namespace) /*xs:string*/
                         , it_xml.char_to_date(EXTRACTVALUE(PaymentElem.elem, '/Payment/PayReceivedDate', v_namespace))  /*xs:date*/
                         , it_xml.char_to_date(EXTRACTVALUE(PaymentElem.elem, '/Payment/FixingDate', v_namespace))  /*xs:date*/
                         ,it_xml.char_to_number(EXTRACTVALUE(PaymentElem.elem, '/Payment/PaymentSecuritiesQuantity', v_namespace)) /*xs:decimal*/
        into rec.T_SHORTNAME,
             rec.T_FULLNAME,
             rec.T_AGREEMENTNUMBER,
             rec.T_ISIIS,
             rec.T_AGREEMENTOPENDATE,
             rec.T_AGREEMENTCLOSEDATE,
             rec.T_CORPORATEACTIONTYPE,
             rec.T_PAYMENTTYPE,
             rec.T_RECORDPAYMENTID,
             rec.T_PAYMENTDATE,
             rec.T_COUPONNUMBER,
             rec.T_COUPONSTARTDATE,
             rec.T_COUPONENDDATE,
             rec.T_FINANCIALNAME,
             rec.T_ISINREGISTRATIONNUMBER,
             rec.T_PAYRECEIVEDDATE,
             rec.T_FIXINGDATE,
             rec.T_QUANTITY
        from dual;

      select EXTRACTVALUE(PaymentElem.elem, '/Payment/ClientId/ObjectId', v_namespace), /*xs:string*/
             EXTRACTVALUE(PaymentElem.elem, '/Payment/ClientId/SystemId', v_namespace), /*xs:string*/
             EXTRACTVALUE(PaymentElem.elem, '/Payment/ClientId/SystemNodeId', v_namespace) /*xs:string*/
        into rec.T_CLIENTID_OBJECTID,
             rec.T_CLIENTID_SYSTEMID,
             rec.T_CLIENTID_SYSTEMNODEID
        from dual;


      FOR i IN c_StatList.FIRST .. c_StatList.LAST LOOP
        for PaymentQty in (select VALUE(t) as elem from table(XMLSEQUENCE(EXTRACT(PaymentElem.elem, '/Payment/PaymentQtyList/PaymentQty', v_namespace))) t) loop
          select it_xml.char_to_number(EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/RecordPaymentQtyId', v_namespace)), /*xs:integer*/
                           EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/OperationStatus', v_namespace), /*xs:string*/
                           EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/RecordSNOBId', v_namespace), /*xs:string*/
                 it_xml.char_to_number(EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/TaxRate', v_namespace)), /*xs:integer*/
                 it_xml.char_to_number(EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/TaxBase', v_namespace)), /*xs:decimal*/
                           EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/IssuerCurrency', v_namespace), /*xs:string*/
                 it_xml.char_to_number(EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/IssuerSum', v_namespace)), /*xs:decimal*/
                 it_xml.char_to_number(EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/ClientSum', v_namespace)), /*xs:decimal*/
                           EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/ClientCurrency', v_namespace), /*xs:string*/
                           EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/KBK', v_namespace), /*xs:integer*/
                 it_xml.char_to_number(EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/ReturnTax', v_namespace)), /*xs:decimal*/
                 it_xml.char_to_number(EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/IndividualTax', v_namespace)), /*xs:decimal*/
                 it_xml.char_to_number(EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/TaxReductionSum', v_namespace)), /*xs:decimal*/
                 it_xml.char_to_number(EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/SumD1', v_namespace)), /*xs:decimal*/
                 it_xml.char_to_number(EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/SumD2', v_namespace)), /*xs:decimal*/
                 it_xml.char_to_number(EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/OffsetTax', v_namespace) ), /*xs:decimal*/
                           EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/AccountNumber', v_namespace), /*xs:string*/
       it_xml.char_to_date(EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/OperationDate', v_namespace)), /*xs:date*/
           CASE WHEN LOWER(EXTRACTVALUE(PaymentQty.elem, '/PaymentQty/IsGetTax', v_namespace)) = '1' THEN CHR(88) ELSE CHR(0) END /*xs:boolean*/
            into rec.T_RECORDPAYMENTQTYID,
                 rec.T_OPERATIONSTATUS,
                 rec.T_RECORDSNOBID,
                 rec.T_TAXRATE,
                 rec.T_TAXBASE,
                 rec.T_ISSUERCURRENCY,
                 rec.T_ISSUERSUM,
                 rec.T_CLIENTSUM,
                 rec.T_CLIENTCURRENCY,
                 rec.T_KBK,
                 rec.T_RETURNTAX,
                 rec.T_INDIVIDUALTAX,
                 rec.T_TAXREDUCTIONSUM,
                 rec.T_SUMD1,
                 rec.T_SUMD2,
                 rec.T_OFFSETTAX,
                 rec.T_ACCOUNTNUMBER,
                 rec.T_OPERATIONDATE,
                 rec.T_ISGETTAX
            from dual;

            rec.T_ID := 0;
            
            --обрабатываем записи только по текущему статусу
            if (LOWER(rec.T_OPERATIONSTATUS) = c_StatList(i)) then
              
              rec.T_STATUS     := RSB_DIASOFT.CDRECORDS_STATUS_NEW;
              rec.T_PROCRESULT := 'OK';

              SELECT (CASE WHEN EXTRACT(v_xml_in, '//SendDepoPaymentInfoReq/GUID', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр GUID';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(v_xml_in, '//SendDepoPaymentInfoReq/RequestTime', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр RequestTime';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentElem.elem, '/Payment/ShortName', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр ShortName';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentElem.elem, '/Payment/FullName', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр FullName';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentElem.elem, '/Payment/CorporateActionType', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр CorporateActionType';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentElem.elem, '/Payment/RecordPaymentId', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр RecordPaymentId';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentElem.elem, '/Payment/PaymentDate', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр PaymentDate';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentElem.elem, '/Payment/ISINRegistrationNumber', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр ISINRegistrationNumber';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentElem.elem, '/Payment/FinancialName', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр FinancialName';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentElem.elem, '/Payment/PayReceivedDate', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр PayReceivedDate';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentElem.elem, '/Payment/FixingDate', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр FixingDate';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentElem.elem, '/Payment/PaymentSecuritiesQuantity', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр PaymentSecuritiesQuantity';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentElem.elem, '/Payment/ClientId', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр ClientId';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentQty.elem, '/PaymentQty/RecordPaymentQtyId', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр RecordPaymentQtyId';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentQty.elem, '/PaymentQty/OperationStatus', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр OperationStatus';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentQty.elem, '/PaymentQty/TaxBase', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр TaxBase';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentQty.elem, '/PaymentQty/IssuerCurrency', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр IssuerCurrency';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentQty.elem, '/PaymentQty/IssuerSum', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр IssuerSum';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentQty.elem, '/PaymentQty/ClientSum', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр ClientSum';
                rec.T_PROCRESULT := 'ERROR';
              END IF;
              
              SELECT (CASE WHEN EXTRACT(PaymentQty.elem, '/PaymentQty/ClientCurrency', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр ClientCurrency';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentQty.elem, '/PaymentQty/AccountNumber', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр AccountNumber';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentQty.elem, '/PaymentQty/OperationDate', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр OperationDate';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              SELECT (CASE WHEN EXTRACT(PaymentQty.elem, '/PaymentQty/IsGetTax', v_namespace) IS NULL THEN 0 ELSE 1 END) INTO v_ExistTag FROM DUAL;
              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR AND v_ExistTag = 0 THEN
                rec.T_STATUS := RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR;
                rec.T_ERROR  := 'В сообщении не задан обязательный параметр IsGetTax';
                rec.T_PROCRESULT := 'ERROR';
              END IF;

              InsDfltIntoCDRECORDS(rec);

              insert into DCDRECORDS_DBT values rec RETURNING T_ID INTO rec.T_ID;

              IF rec.T_STATUS <> RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR THEN
                SetSOFR_IDs(rec.T_ID);
              END IF;

              SELECT * INTO rec FROM DCDRECORDS_DBT WHERE t_ID = rec.T_ID;
              
              IF rec.T_STATUS = RSB_DIASOFT.CDRECORDS_STATUS_NEW THEN
                BEGIN
                  IF rec.T_CORPORATEACTIONTYPE <> 'REDM' AND rec.T_CORPORATEACTIONTYPE <> 'BPUT' AND 
                     NOT (rec.T_CORPORATEACTIONTYPE = 'INTR' AND rec.T_ISGETTAX <> 'X' /*инверсия условия обработки INTR: IF (rec.T_CORPORATEACTIONTYPE = 'INTR' AND rec.T_ISGETTAX <> 'X'), т.е статус проставляем только для необрабатываемых INTR*/) THEN
                    UPDATE DCDRECORDS_DBT SET T_PROCRESULT = 'OK', T_STATUS = RSB_DIASOFT.CDRECORDS_STATUS_PROCESSED WHERE T_ID = rec.T_ID;
                  END IF;
                EXCEPTION
                  WHEN OTHERS THEN 
                    v_errtxt := sqlerrm || ';' || sys.dbms_utility.format_error_backtrace;
                    UPDATE DCDRECORDS_DBT SET T_PROCRESULT = v_errtxt, T_STATUS = RSB_DIASOFT.CDRECORDS_STATUS_VALIDATION_ERROR, T_ERROR = v_errtxt WHERE T_ID = rec.T_ID;
                END;
              END IF;
            end if;
        end loop;
      end loop;
    end loop;
  end;

  /*
    @brief MakeTaxObjForINTR - ХП запускается из планировщика cdrecords_sheduler для обработки всех выплат по купонам INTR
    @param[in] p_term - срок обработки выплат по погашению
  */
  PROCEDURE MakeTaxObjForINTR( p_term INTEGER /*срок обработки выплат по погашению*/ )
  AS
    TYPE t_records_table IS TABLE OF DCDRECORDS_DBT%ROWTYPE;
    v_records t_records_table;

    CURSOR cur_records IS
        SELECT *
        FROM DCDRECORDS_DBT
        WHERE t_RequestDate >= trunc(sysdate) - p_term
          AND LOWER(TRIM(t_CorporateActionType)) IN ('intr')
          AND ( (LOWER(TRIM(t_OperationStatus)) = 'активна' AND t_ISGETTAX <> 'X' AND t_ACCOUNTNUMBER LIKE '306%') OR (LOWER(TRIM(t_OperationStatus)) = 'отменена') )
          AND t_Status IN (RSB_DIASOFT.CDRECORDS_STATUS_NEW, RSB_DIASOFT.CDRECORDS_STATUS_PROCESSING_ERROR)
          ORDER BY t_RequestDate ASC, t_RequestTime ASC, t_ID ASC;

    rec cur_records%ROWTYPE;
  BEGIN
    OPEN cur_records;
    LOOP
        FETCH cur_records BULK COLLECT INTO v_records LIMIT 1000;
        EXIT WHEN v_records.COUNT = 0;

        FOR i IN 1..v_records.COUNT LOOP
            -- Обработка каждой записи
            RSB_DIASOFT.MakeTaxObjByDepoInfo(v_records(i));
        END LOOP;
    END LOOP;
    CLOSE cur_records;
  END;

end RSB_DIASOFT;
