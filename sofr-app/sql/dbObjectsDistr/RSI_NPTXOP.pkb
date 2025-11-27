CREATE OR REPLACE PACKAGE BODY RSI_NPTXOP
IS
  PARALLEL_LEVEL CONSTANT NUMBER(5) := 10; --количество потоков

  OBJTYPE_NPTXCALC CONSTANT NUMBER(5) := 132;
  REFOBJ_NPTXCALC CONSTANT NUMBER(5) := 1;

  MESTYPE_OK      CONSTANT NUMBER(5) := 0;
  MESTYPE_ERROR   CONSTANT NUMBER(5) := 1;
  MESTYPE_NODEALS CONSTANT NUMBER(5) := 2;

  TYPE ListClient_t IS TABLE OF DPARTY_DBT.T_PARTYID%TYPE;
  TYPE ListNPTXOP_t IS TABLE OF DNPTXOP_DBT%ROWTYPE;
  TYPE ListTaxMes_t IS TABLE OF DSCTAXMES_TMP%ROWTYPE;
  TYPE ListMassProtocol_t IS TABLE OF DNPTXMASSPROT_DBT%ROWTYPE;

  PROCEDURE AddTechnical (pID DNPTXOP_DBT.T_ID%TYPE) 
  IS
  BEGIN
    INSERT INTO DOBJATCOR_DBT (T_OBJECTTYPE,
                               T_GROUPID,
                               T_ATTRID,
                               T_OBJECT,
                               T_GENERAL,
                               T_VALIDFROMDATE,
                               T_OPER,
                               T_VALIDTODATE,
                               T_ISAUTO)
    VALUES (OBJTYPE_NPTXCALC,
            1,
            1,
            LPAD(pID, 34, '0'),
            'X',
            RSBSESSIONDATA.CURDATE,
            RSBSESSIONDATA.OPER,
            TO_DATE('31.12.9999', 'DD.MM.YYYY'),
            'X');
  END;

  --Получить параметры NPTXOP из записи, переданной в виде Raw
  PROCEDURE CopyRAWtoNPTXOP( pRawNPTXOP IN RAW, pNPTXOP OUT DNPTXOP_DBT%ROWTYPE )
  IS
  BEGIN
    rsb_struct.readStruct('DNPTXOP_DBT');

    pNPTXOP.T_ID                :=   rsb_struct.getLong('T_ID',                pRawNPTXOP);
    pNPTXOP.T_DOCKIND           :=    rsb_struct.getInt('T_DOCKIND',           pRawNPTXOP);
    pNPTXOP.T_KIND_OPERATION    :=    rsb_struct.getInt('T_KIND_OPERATION',    pRawNPTXOP);
    pNPTXOP.T_SUBKIND_OPERATION :=    rsb_struct.getInt('T_SUBKIND_OPERATION', pRawNPTXOP);
    pNPTXOP.T_CODE              := rsb_struct.getString('T_CODE',              pRawNPTXOP);
    pNPTXOP.T_OPERDATE          :=   rsb_struct.getDate('T_OPERDATE',          pRawNPTXOP);
    pNPTXOP.T_CLIENT            :=   rsb_struct.getLong('T_CLIENT',            pRawNPTXOP);
    pNPTXOP.T_CONTRACT          :=   rsb_struct.getLong('T_CONTRACT',          pRawNPTXOP);
    pNPTXOP.T_PREVDATE          :=   rsb_struct.getDate('T_PREVDATE',          pRawNPTXOP);
    pNPTXOP.T_PLACEKIND         :=    rsb_struct.getInt('T_PLACEKIND',         pRawNPTXOP);
    pNPTXOP.T_PLACE             :=   rsb_struct.getLong('T_PLACE',             pRawNPTXOP);
    pNPTXOP.T_TAXBASE           :=  rsb_struct.getMoney('T_TAXBASE',           pRawNPTXOP);
    pNPTXOP.T_OUTSUM            :=  rsb_struct.getMoney('T_OUTSUM',            pRawNPTXOP);
    pNPTXOP.T_OUTCOST           :=  rsb_struct.getMoney('T_OUTCOST',           pRawNPTXOP);
    pNPTXOP.T_TOUT              :=  rsb_struct.getMoney('T_TOUT',              pRawNPTXOP);
    pNPTXOP.T_TOTALTAXSUM       :=  rsb_struct.getMoney('T_TOTALTAXSUM',       pRawNPTXOP);
    pNPTXOP.T_PREVTAXSUM        :=  rsb_struct.getMoney('T_PREVTAXSUM',        pRawNPTXOP);
    pNPTXOP.T_TAXSUM            :=  rsb_struct.getMoney('T_TAXSUM',            pRawNPTXOP);
    pNPTXOP.T_TAX               :=  rsb_struct.getMoney('T_TAX',               pRawNPTXOP);
    pNPTXOP.T_METHOD            :=    rsb_struct.getInt('T_METHOD',            pRawNPTXOP);
    pNPTXOP.T_ACCOUNT           := rsb_struct.getString('T_ACCOUNT',           pRawNPTXOP);
    pNPTXOP.T_CURRENCY          :=   rsb_struct.getLong('T_CURRENCY',          pRawNPTXOP);
    pNPTXOP.T_STATUS            :=    rsb_struct.getInt('T_STATUS',            pRawNPTXOP);
    pNPTXOP.T_OPER              :=    rsb_struct.getInt('T_OPER',              pRawNPTXOP);
    pNPTXOP.T_DEPARTMENT        :=    rsb_struct.getInt('T_DEPARTMENT',        pRawNPTXOP);
    pNPTXOP.T_IIS               :=   rsb_struct.getChar('T_IIS',               pRawNPTXOP);
    pNPTXOP.T_TAXTOPAY          :=  rsb_struct.getMoney('T_TAXTOPAY',          pRawNPTXOP);
    pNPTXOP.T_CALCNDFL          :=   rsb_struct.getChar('T_CALCNDFL',          pRawNPTXOP);
    pNPTXOP.T_RECALC            :=   rsb_struct.getChar('T_RECALC',            pRawNPTXOP);
    pNPTXOP.T_BEGRECALCDATE     :=   rsb_struct.getDate('T_BEGRECALCDATE',     pRawNPTXOP);
    pNPTXOP.T_ENDRECALCDATE     :=   rsb_struct.getDate('T_ENDRECALCDATE',     pRawNPTXOP);
    pNPTXOP.T_TIME              :=   rsb_struct.getTime('T_TIME',              pRawNPTXOP);
    pNPTXOP.T_CURRENTYEAR_SUM   :=  rsb_struct.getMoney('T_CURRENTYEAR_SUM',   pRawNPTXOP);
    pNPTXOP.T_CURRENCYSUM       :=   rsb_struct.getLong('T_CURRENCYSUM',       pRawNPTXOP);
    pNPTXOP.T_FLAGTAX           :=   rsb_struct.getChar('T_FLAGTAX',           pRawNPTXOP);
    pNPTXOP.T_PARTIAL           :=   rsb_struct.getChar('T_PARTIAL',           pRawNPTXOP);
    pNPTXOP.T_CLOSECONTR        :=   rsb_struct.getChar('T_CLOSECONTR',        pRawNPTXOP);
    pNPTXOP.T_ACCOUNTTAX        := rsb_struct.getString('T_ACCOUNTTAX',        pRawNPTXOP);
    pNPTXOP.T_TAXSUM2           :=  rsb_struct.getMoney('T_TAXSUM2',           pRawNPTXOP);
    pNPTXOP.T_FIID              :=   rsb_struct.getLong('T_FIID',              pRawNPTXOP);
    pNPTXOP.T_LIMITSTATUS       :=    rsb_struct.getInt('T_LIMITSTATUS',       pRawNPTXOP);
    pNPTXOP.T_PLACEKIND2        :=    rsb_struct.getInt('T_PLACEKIND2',        pRawNPTXOP);
    pNPTXOP.T_PLACE2            :=   rsb_struct.getLong('T_PLACE2',            pRawNPTXOP);
    pNPTXOP.T_MARKETPLACE       :=   rsb_struct.getLong('T_MARKETPLACE',       pRawNPTXOP);
    pNPTXOP.T_MARKETPLACE2      :=   rsb_struct.getLong('T_MARKETPLACE2',      pRawNPTXOP);
    pNPTXOP.T_MARKETSECTOR      :=   rsb_struct.getLong('T_MARKETSECTOR',      pRawNPTXOP);
    pNPTXOP.T_MARKETSECTOR2     :=   rsb_struct.getLong('T_MARKETSECTOR2',     pRawNPTXOP);
    pNPTXOP.T_TAXDP             :=  rsb_struct.getMoney('T_TAXDP',             pRawNPTXOP);
    -- BOSS-4644, Формирование поручения на перевод денежных средств с ИИС-3 на оплату дорогостоящего лечения в медицинское учреждение
    pNPTXOP.T_PAYMEDICAL        :=   rsb_struct.getChar('T_PAYMEDICAL',        pRawNPTXOP);
    pNPTXOP.T_RECEIVER          :=   rsb_struct.getLong('T_RECEIVER',          pRawNPTXOP);
    -- BOSS-7143, Перевод другому брокеру, цель списания
    pNPTXOP.T_PAYPURPOSE        :=   rsb_struct.getInt('T_PAYPURPOSE',         pRawNPTXOP);
    pNPTXOP.T_TECHNICAL         :=   rsb_struct.getChar('T_TECHNICAL',         pRawNPTXOP);
  END;

  --Получить параметры Raw из записи NPTXOP
  PROCEDURE CopyNPTXOPtoRAW( pNPTXOP IN DNPTXOP_DBT%ROWTYPE, pRawNPTXOP IN OUT RAW )
  IS
  BEGIN
    rsb_struct.readStruct('DNPTXOP_DBT');

    pRawNPTXOP :=   rsb_struct.putLong('T_ID',                pRawNPTXOP, NVL(pNPTXOP.T_ID, 0));
    pRawNPTXOP :=    rsb_struct.putInt('T_DOCKIND',           pRawNPTXOP, NVL(pNPTXOP.T_DOCKIND, 0));
    pRawNPTXOP :=    rsb_struct.putInt('T_KIND_OPERATION',    pRawNPTXOP, NVL(pNPTXOP.T_KIND_OPERATION, 0));
    pRawNPTXOP :=    rsb_struct.putInt('T_SUBKIND_OPERATION', pRawNPTXOP, NVL(pNPTXOP.T_SUBKIND_OPERATION, 0));
    pRawNPTXOP := rsb_struct.putString('T_CODE',              pRawNPTXOP, NVL(pNPTXOP.T_CODE, RSI_RsbOperation.ZERO_STR));
    pRawNPTXOP :=   rsb_struct.putDate('T_OPERDATE',          pRawNPTXOP, NVL(pNPTXOP.T_OPERDATE, NPTAX.UnknownDate));
    pRawNPTXOP :=   rsb_struct.putLong('T_CLIENT',            pRawNPTXOP, NVL(pNPTXOP.T_CLIENT, -1));
    pRawNPTXOP :=   rsb_struct.putLong('T_CONTRACT',          pRawNPTXOP, NVL(pNPTXOP.T_CONTRACT, 0));
    pRawNPTXOP :=   rsb_struct.putDate('T_PREVDATE',          pRawNPTXOP, NVL(pNPTXOP.T_PREVDATE, NPTAX.UnknownDate));
    pRawNPTXOP :=    rsb_struct.putInt('T_PLACEKIND',         pRawNPTXOP, NVL(pNPTXOP.T_PLACEKIND, 0));
    pRawNPTXOP :=   rsb_struct.putLong('T_PLACE',             pRawNPTXOP, NVL(pNPTXOP.T_PLACE, 0));
    pRawNPTXOP :=  rsb_struct.putMoney('T_TAXBASE',           pRawNPTXOP, NVL(pNPTXOP.T_TAXBASE, 0));
    pRawNPTXOP :=  rsb_struct.putMoney('T_OUTSUM',            pRawNPTXOP, NVL(pNPTXOP.T_OUTSUM, 0));
    pRawNPTXOP :=  rsb_struct.putMoney('T_OUTCOST',           pRawNPTXOP, NVL(pNPTXOP.T_OUTCOST, 0));
    pRawNPTXOP :=  rsb_struct.putMoney('T_TOUT',              pRawNPTXOP, NVL(pNPTXOP.T_TOUT, 0));
    pRawNPTXOP :=  rsb_struct.putMoney('T_TOTALTAXSUM',       pRawNPTXOP, NVL(pNPTXOP.T_TOTALTAXSUM, 0));
    pRawNPTXOP :=  rsb_struct.putMoney('T_PREVTAXSUM',        pRawNPTXOP, NVL(pNPTXOP.T_PREVTAXSUM, 0));
    pRawNPTXOP :=  rsb_struct.putMoney('T_TAXSUM',            pRawNPTXOP, NVL(pNPTXOP.T_TAXSUM, 0));
    pRawNPTXOP :=  rsb_struct.putMoney('T_TAX',               pRawNPTXOP, NVL(pNPTXOP.T_TAX, 0));
    pRawNPTXOP :=    rsb_struct.putInt('T_METHOD',            pRawNPTXOP, NVL(pNPTXOP.T_METHOD, 0));
    pRawNPTXOP := rsb_struct.putString('T_ACCOUNT',           pRawNPTXOP, NVL(pNPTXOP.T_ACCOUNT, RSI_RsbOperation.ZERO_STR));
    pRawNPTXOP :=   rsb_struct.putLong('T_CURRENCY',          pRawNPTXOP, NVL(pNPTXOP.T_CURRENCY, 0));
    pRawNPTXOP :=    rsb_struct.putInt('T_STATUS',            pRawNPTXOP, NVL(pNPTXOP.T_STATUS, 0));
    pRawNPTXOP :=    rsb_struct.putInt('T_OPER',              pRawNPTXOP, NVL(pNPTXOP.T_OPER, 0));
    pRawNPTXOP :=    rsb_struct.putInt('T_DEPARTMENT',        pRawNPTXOP, NVL(pNPTXOP.T_DEPARTMENT, 0));
    pRawNPTXOP :=   rsb_struct.putChar('T_IIS',               pRawNPTXOP, NVL(pNPTXOP.T_IIS, CNST.UNSET_CHAR));
    pRawNPTXOP :=  rsb_struct.putMoney('T_TAXTOPAY',          pRawNPTXOP, NVL(pNPTXOP.T_TAXTOPAY, 0));
    pRawNPTXOP :=   rsb_struct.putChar('T_CALCNDFL',          pRawNPTXOP, NVL(pNPTXOP.T_CALCNDFL, CNST.UNSET_CHAR));
    pRawNPTXOP :=   rsb_struct.putChar('T_RECALC',            pRawNPTXOP, NVL(pNPTXOP.T_RECALC, CNST.UNSET_CHAR));
    pRawNPTXOP :=   rsb_struct.putDate('T_BEGRECALCDATE',     pRawNPTXOP, NVL(pNPTXOP.T_BEGRECALCDATE, NPTAX.UnknownDate));
    pRawNPTXOP :=   rsb_struct.putDate('T_ENDRECALCDATE',     pRawNPTXOP, NVL(pNPTXOP.T_ENDRECALCDATE, NPTAX.UnknownDate));
    pRawNPTXOP :=   rsb_struct.putTime('T_TIME',              pRawNPTXOP, NVL(pNPTXOP.T_TIME, NPTAX.UnknownDate));
    pRawNPTXOP :=  rsb_struct.putMoney('T_CURRENTYEAR_SUM',   pRawNPTXOP, NVL(pNPTXOP.T_CURRENTYEAR_SUM, 0));
    pRawNPTXOP :=   rsb_struct.putLong('T_CURRENCYSUM',       pRawNPTXOP, NVL(pNPTXOP.T_CURRENCYSUM, 0));
    pRawNPTXOP :=   rsb_struct.putChar('T_FLAGTAX',           pRawNPTXOP, NVL(pNPTXOP.T_FLAGTAX, CNST.UNSET_CHAR));
    pRawNPTXOP :=   rsb_struct.putChar('T_PARTIAL',           pRawNPTXOP, NVL(pNPTXOP.T_PARTIAL, CNST.UNSET_CHAR));
    pRawNPTXOP :=   rsb_struct.putChar('T_CLOSECONTR',        pRawNPTXOP, NVL(pNPTXOP.T_CLOSECONTR, CNST.UNSET_CHAR));
    pRawNPTXOP := rsb_struct.putString('T_ACCOUNTTAX',        pRawNPTXOP, NVL(pNPTXOP.T_ACCOUNTTAX, RSI_RsbOperation.ZERO_STR));
    pRawNPTXOP :=  rsb_struct.putMoney('T_TAXSUM2',           pRawNPTXOP, NVL(pNPTXOP.T_TAXSUM2, 0));
    pRawNPTXOP :=   rsb_struct.putLong('T_FIID',              pRawNPTXOP, NVL(pNPTXOP.T_FIID, -1));
    pRawNPTXOP :=    rsb_struct.putInt('T_LIMITSTATUS',       pRawNPTXOP, NVL(pNPTXOP.T_LIMITSTATUS, 0));
    pRawNPTXOP :=    rsb_struct.putInt('T_PLACEKIND2',        pRawNPTXOP, NVL(pNPTXOP.T_PLACEKIND2, 0));
    pRawNPTXOP :=   rsb_struct.putLong('T_PLACE2',            pRawNPTXOP, NVL(pNPTXOP.T_PLACE2, 0));
    pRawNPTXOP :=   rsb_struct.putLong('T_MARKETPLACE',       pRawNPTXOP, NVL(pNPTXOP.T_MARKETPLACE, 0));
    pRawNPTXOP :=   rsb_struct.putLong('T_MARKETPLACE2',      pRawNPTXOP, NVL(pNPTXOP.T_MARKETPLACE2, 0));
    pRawNPTXOP :=   rsb_struct.putLong('T_MARKETSECTOR',      pRawNPTXOP, NVL(pNPTXOP.T_MARKETSECTOR, 0));
    pRawNPTXOP :=   rsb_struct.putLong('T_MARKETSECTOR2',     pRawNPTXOP, NVL(pNPTXOP.T_MARKETSECTOR2, 0));
    pRawNPTXOP :=  rsb_struct.putMoney('T_TAXDP',             pRawNPTXOP, NVL(pNPTXOP.T_TAXDP, 0));
    -- BOSS-4644, Формирование поручения на перевод денежных средств с ИИС-3 на оплату дорогостоящего лечения в медицинское учреждение
    pRawNPTXOP :=   rsb_struct.putChar('T_PAYMEDICAL',        pRawNPTXOP, NVL(pNPTXOP.T_PAYMEDICAL, CNST.UNSET_CHAR));
    pRawNPTXOP :=   rsb_struct.putLong('T_RECEIVER',          pRawNPTXOP, NVL(pNPTXOP.T_RECEIVER, -1));
    -- BOSS-7143, Перевод другому брокеру, цель списания
    pRawNPTXOP :=    rsb_struct.putInt('T_PAYPURPOSE',        pRawNPTXOP, NVL(pNPTXOP.T_PAYPURPOSE, 0));
    pRawNPTXOP :=   rsb_struct.putChar('T_TECHNICAL',         pRawNPTXOP, NVL(pNPTXOP.T_TECHNICAL, CNST.UNSET_CHAR));
  END;

  FUNCTION GetSQLClients( pCreateDate IN DATE, pIIS IN CHAR, pAddSelectFlds IN VARCHAR2 DEFAULT NULL, pClientGroup IN NUMBER DEFAULT 0, pClientIdType IN NUMBER DEFAULT 0, pSubKind IN NUMBER DEFAULT 0)
    RETURN VARCHAR2
  IS
    v_sql      VARCHAR2(1500);
  BEGIN
    v_sql := 'SELECT '|| pAddSelectFlds || ' party.t_PartyID T_ID ' ||
              ' FROM dparty_dbt party ' ||
             ' WHERE EXISTS ' ||  --являются клиентами Фондового диллинга или Срочных контрактов на дату операции
                      ' (SELECT client.t_PartyID ' ||
                         ' FROM dclient_dbt client ' ||
                        ' WHERE client.t_PartyID = party.t_PartyID ' ||
                          ' AND client.t_ServiceKind IN (' || RSI_NPTO.PTSK_STOCKDL || ', ' || RSI_NPTO.PTSK_DV || ') ' ||
                          ' AND client.t_StartDate <= TO_DATE('''|| TO_CHAR(pCreateDate, 'DD.MM.YYYY') || ''', ''DD.MM.YYYY'') ' ||
                          ' AND (   client.t_FinishDate = TO_DATE(''01.01.0001'', ''DD.MM.YYYY'') ' ||
                               ' OR client.t_FinishDate > TO_DATE('''|| TO_CHAR(pCreateDate, 'DD.MM.YYYY') || ''', ''DD.MM.YYYY''))) ';

    IF pIIS = CNST.UNSET_CHAR THEN
      IF (pSubkind<>0 AND pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_NORMAL) THEN
        v_sql := v_sql || ' AND RSI_NPTO.ExistNotIISContr(party.t_PartyID, TO_DATE('''|| TO_CHAR(pCreateDate, 'DD.MM.YYYY') || ''', ''DD.MM.YYYY'')) = 1 ';
      ELSIF (pSubkind<>0 AND pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_ENDYEAR) THEN
        v_sql := v_sql || ' AND (RSI_NPTO.ExistNotIISContr(party.t_PartyID, TO_DATE('''|| TO_CHAR(pCreateDate, 'DD.MM.YYYY') || ''', ''DD.MM.YYYY'')) = 1 OR RSI_NPTO.ExistNotIISContr(party.t_PartyID, TO_DATE(''01.01.''||TO_CHAR( EXTRACT( YEAR FROM TO_DATE('''||TO_CHAR(pCreateDate, 'dd.mm.yyyy')||''', ''dd.mm.yyyy''))), ''dd.mm.yyyy'')) = 1)';
      ELSE
        v_sql := v_sql || ' AND RSI_NPTO.ExistNotIISContr(party.t_PartyID, TO_DATE('''|| TO_CHAR(pCreateDate, 'DD.MM.YYYY') || ''', ''DD.MM.YYYY'')) = 1 ';
      END IF;
    END IF;

    IF pClientGroup = 2 THEN
      v_sql := v_sql || ' AND TO_CHAR(party.T_PARTYID) IN (SELECT TO_CHAR(T_CLIENTID) FROM DNPTXOPMASSLIST_TMP)';
    ELSE
      v_sql := v_sql || ' AND party.t_LegalForm = ' || TO_CHAR(PM_COMMON.PTLEGF_PERSN);
    END IF;

    RETURN v_sql;
  END;

  --количество клиентов к обработке
  FUNCTION GetCountClients( pCreateDate IN DATE, pIIS IN CHAR ) RETURN NUMBER
  IS
    v_cnt NUMBER(10);
  BEGIN
    EXECUTE IMMEDIATE 'SELECT COUNT(1) FROM ('|| GetSQLClients(pCreateDate, pIIS) ||')' INTO v_cnt;
    RETURN v_cnt;
  END;

  FUNCTION GetKindOperation( pDocKind IN NUMBER ) RETURN NUMBER
  IS
    v_Kind_Operation NUMBER(10);
  BEGIN
    SELECT t_Kind_Operation INTO v_Kind_Operation
      FROM doprkoper_dbt
     WHERE t_DocKind = pDocKind
       AND t_NotInUse = CHR(0);
    RETURN v_Kind_Operation;
  EXCEPTION WHEN OTHERS THEN RETURN 0;
  END;

  PROCEDURE AddMsg( pListTaxMes IN OUT NOCOPY ListTaxMes_t, pMesType IN NUMBER, pMessage IN VARCHAR2, pID IN NUMBER DEFAULT 0 )
  IS
    v_Mes DSCTAXMES_TMP%ROWTYPE;
  BEGIN
    v_Mes.T_ID := pID; --решил записать сюда (для выделения созданных операций)
    v_Mes.T_DEALID := 0;
    v_Mes.T_FIID := 0;
    v_Mes.T_TYPE := pMesType;
    v_Mes.T_MESSAGE := pMessage;
    v_Mes.T_MESTIME := TO_DATE('01.01.0001 ' || TO_CHAR(SYSDATE(), 'HH24:MI:SS'), 'DD.MM.YYYY HH24:MI:SS');

    pListTaxMes.Extend();
    pListTaxMes(pListTaxMes.last) := v_Mes;
  END;

  PROCEDURE InsertTaxMes(pListTaxMes IN OUT NOCOPY ListTaxMes_t)
  IS
  BEGIN
    IF pListTaxMes.COUNT > 0 THEN
      FORALL i IN pListTaxMes.FIRST .. pListTaxMes.LAST
        INSERT INTO DSCTAXMES_TMP VALUES pListTaxMes(i);
      pListTaxMes.DELETE;
    END IF;
  END;

  FUNCTION GenerateOperNum(pOperNum IN VARCHAR2, pNum IN NUMBER) RETURN VARCHAR2
  IS
    v_Code VARCHAR2(25);
    v_StrNumInEnd VARCHAR2(20);
    v_IdxNumInEnd NUMBER(10);
  BEGIN
    v_StrNumInEnd := REGEXP_SUBSTR(pOperNum, '(\d*)$');
    v_IdxNumInEnd := NVL(INSTR(pOperNum, v_StrNumInEnd, -1), 0);

    IF v_IdxNumInEnd = 0 THEN
      v_Code := pOperNum || TO_CHAR(pNum);
    ELSE
      v_Code := SUBSTR(pOperNum, 1, v_IdxNumInEnd-1) ||
                LPAD(TO_CHAR(TO_NUMBER(v_StrNumInEnd)+pNum), LENGTH(v_StrNumInEnd), '0');
    END IF;

    RETURN v_Code;
  END;

  FUNCTION NptxCalcTaxPrevDateByKind( p_Client IN NUMBER, p_IIS IN CHAR, p_OperDate IN DATE, pSubKind IN NUMBER, pDlContrID IN NUMBER, pCorrectDate IN NUMBER DEFAULT 0, pAddDay IN NUMBER DEFAULT 0) RETURN DATE
  IS
     v_Date DATE;
     v_MinBegDate DATE;
  BEGIN

     IF p_IIS = CNST.SET_CHAR THEN
       v_MinBegDate := RSI_NPTO.GetFirstDateIIS(p_Client, pDlContrID);
     ELSE
       v_MinBegDate := TO_DATE('01.01.'||(TO_CHAR( p_OperDate, 'YYYY')),'DD.MM.YYYY');
     END IF;

     SELECT NVL(MAX(T_OPERDATE), TO_DATE('01.01.0001','DD.MM.YYYY'))
       INTO v_Date
       FROM DNPTXOP_DBT
      WHERE T_DOCKIND = RSI_NPTXC.DL_CALCNDFL
        AND T_RECALC = CHR(0)
        AND T_CLIENT = p_Client
        AND T_CONTRACT = pDlContrID
        AND T_IIS    = p_IIS
        AND T_OPERDATE <= p_OperDate
        AND T_OPERDATE >= v_MinBegDate
        AND T_STATUS <> RSI_NPTXC.DL_TXOP_Prep
        AND ((pSubKind = 0 and T_SUBKIND_OPERATION <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_NORMAL and T_SUBKIND_OPERATION <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE) OR T_SUBKIND_OPERATION = pSubKind);

     IF pCorrectDate <> 0 AND v_Date = TO_DATE('01.01.0001','DD.MM.YYYY') THEN
       v_Date := v_MinBegDate;
     ELSIF pCorrectDate <> 0 THEN
       v_Date := v_Date + pAddDay;
     END IF;
        
     RETURN v_Date;
  END;

  FUNCTION CheckExistsLucreData(pBegDate   IN DATE,
                                pEndDate   IN DATE,
                                pClient    IN NUMBER) RETURN NUMBER
  IS
    v_ExistsLucreData NUMBER := 0;

    v_TaxGroup  NUMBER;

    v_StartDate DATE;
  BEGIN

    IF EXTRACT(YEAR FROM pEndDate) < RSI_NPTO.GetLucreStartTaxPeriod() THEN
      RETURN 0;
    END IF;

    v_StartDate := to_date(RSB_COMMON.GetRegStrValue('COMMON\НДФЛ\ДАТА_ЛЬГОТЫ_ПО_МАТВЫГОДЕ'), 'dd.mm.yyyy');

    FOR one_rec IN (SELECT q.*
                      FROM ( 
                              WITH sf AS (SELECT t_ID, t_PartyID 
                                            FROM dsfcontr_dbt 
                                           WHERE t_PartyID = pClient
                                             AND t_ServKind = RSI_NPTO.PTSK_STOCKDL
                                         )
                              SELECT RQ.t_FactDate AvFactDate,  
                                     (CASE WHEN RQ.t_FactDate < PM.t_FactDate THEN PM.t_FactDate ELSE RQ.t_FactDate END) t_FactDate, 
                                     RQ.t_DocKind, Leg.t_CFI, Leg.t_PFI, Leg.t_Price, Leg.t_Principal, Leg.t_RelativePrice, 
                                     Fin.t_FaceValueFI, Tick.t_DealID, Tick.t_DealDate, 
                                     Tick.t_ClientContrID, Tick.t_IsPartyClient, Tick.t_PartyContrID,
                                     Opr.IsRepo, Opr.IsLoan   
                              FROM sf, ddl_tick_dbt Tick, ddlrq_dbt RQ, ddl_leg_dbt Leg, dfininstr_dbt Fin, 
                                   (SELECT t_Kind_Operation, 
                                           Rsb_Secur.IsBuy(rsb_secur.get_OperationGroup(t_SysTypes)) IsBuy,
                                           Rsb_Secur.IsSale(rsb_secur.get_OperationGroup(t_SysTypes)) IsSale,
                                           Rsb_Secur.IsRepo(rsb_secur.get_OperationGroup(t_SysTypes)) IsRepo,
                                           Rsb_Secur.IsLoan(rsb_secur.get_OperationGroup(t_SysTypes)) IsLoan 
                                    FROM doprkoper_dbt WHERE t_DocKind = RSB_SECUR.DL_SECURITYDOC) Opr, ddlrq_dbt PM 
                              WHERE Tick.t_BOfficeKind = RSB_SECUR.DL_SECURITYDOC
                                AND Tick.t_DealDate >= TO_DATE('01.01.2016','DD.MM.YYYY')
                                AND Tick.t_DealDate <= pEndDate
                                AND Tick.t_ClientID = sf.t_PartyID AND Tick.t_ClientContrID = sf.t_ID AND Opr.IsBuy = 1 
                                AND RQ.t_DocKind    = Tick.t_BOfficeKind
                                AND RQ.t_DocID      = Tick.t_DealID
                                AND RQ.t_State      = RSI_DLRQ.DLRQ_STATE_EXEC 
                                AND RQ.t_Type       = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                                AND RQ.t_DealPart   = 1 
                                AND Opr.t_Kind_Operation = Tick.t_DealType 
                                AND Leg.t_DealID    = Tick.t_DealID 
                                AND Leg.t_LegID     = 0 
                                AND Leg.t_LegKind   = 0 
                                AND Fin.t_FIID      = Leg.t_PFI 
                                AND PM.t_DocID      = Tick.t_DealID 
                                AND PM.t_DocKind    = Tick.t_BOfficeKind 
                                AND PM.t_State      = RSI_DLRQ.DLRQ_STATE_EXEC
                                AND PM.t_Type       = RSI_DLRQ.DLRQ_TYPE_PAYMENT 
                                AND PM.t_DealPart   = 1 
                                AND (RQ.t_FactDate BETWEEN pBegDate AND pEndDate OR PM.t_FactDate BETWEEN pBegDate AND pEndDate)
                                AND (CASE WHEN RQ.t_FactDate < PM.t_FactDate THEN PM.t_FactDate ELSE RQ.t_FactDate END) BETWEEN pBegDate AND pEndDate
                           ) q
                      WHERE q.t_FactDate > v_StartDate
                   ) 
    LOOP

      v_TaxGroup := npto.GetPaperTaxGroupNPTX(one_rec.t_PFI);

      IF  ((one_rec.IsRepo = 0 AND one_rec.IsLoan = 0) OR  
           RSI_NPTX.CheckCateg(RSB_SECUR.OBJTYPE_SECDEAL, 23, LPAD(one_rec.t_DealID, 34, '0'), 2)=1  --категория "Является налоговым Репо" на сделке DDL_TICK.T_DEALID задана и равна False
          ) AND
          ( 
             v_TaxGroup <> 40 
             OR RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(one_rec.t_DealID, 34, '0'), 53, one_rec.t_DealDate) <> 2 --категория "Первичное размещение"
          ) THEN

          v_ExistsLucreData := 1;
          EXIT;
      END IF;
    END LOOP;

    IF v_ExistsLucreData = 0 THEN
      FOR one_rec IN (SELECT q.*
                        FROM ( 
                                WITH sf AS (SELECT t_ID, t_PartyID 
                                              FROM dsfcontr_dbt 
                                             WHERE t_PartyID = pClient
                                               AND t_ServKind = RSI_NPTO.PTSK_STOCKDL
                                           )
                                SELECT RQ.t_FactDate AvFactDate,  
                                       (CASE WHEN RQ.t_FactDate < PM.t_FactDate THEN PM.t_FactDate ELSE RQ.t_FactDate END) t_FactDate, 
                                       RQ.t_DocKind, Leg.t_CFI, Leg.t_PFI, Leg.t_Price, Leg.t_Principal, Leg.t_RelativePrice, 
                                       Fin.t_FaceValueFI, Tick.t_DealID, Tick.t_DealDate, 
                                       Tick.t_ClientContrID, Tick.t_IsPartyClient, Tick.t_PartyContrID,
                                       Opr.IsRepo, Opr.IsLoan   
                                FROM sf, ddl_tick_dbt Tick, ddlrq_dbt RQ, ddl_leg_dbt Leg, dfininstr_dbt Fin, 
                                     (SELECT t_Kind_Operation, 
                                             Rsb_Secur.IsBuy(rsb_secur.get_OperationGroup(t_SysTypes)) IsBuy,
                                             Rsb_Secur.IsSale(rsb_secur.get_OperationGroup(t_SysTypes)) IsSale,
                                             Rsb_Secur.IsRepo(rsb_secur.get_OperationGroup(t_SysTypes)) IsRepo,
                                             Rsb_Secur.IsLoan(rsb_secur.get_OperationGroup(t_SysTypes)) IsLoan 
                                      FROM doprkoper_dbt WHERE t_DocKind = RSB_SECUR.DL_SECURITYDOC) Opr, ddlrq_dbt PM 
                                WHERE Tick.t_BOfficeKind = RSB_SECUR.DL_SECURITYDOC
                                  AND Tick.t_DealDate >= TO_DATE('01.01.2016','DD.MM.YYYY')
                                  AND Tick.t_DealDate <= pEndDate
                                  AND Tick.t_PartyID = sf.t_PartyID AND Tick.t_IsPartyClient = 'X' AND Tick.t_PartyContrID = sf.t_ID AND Opr.IsSale = 1  
                                  AND RQ.t_DocKind    = Tick.t_BOfficeKind
                                  AND RQ.t_DocID      = Tick.t_DealID
                                  AND RQ.t_State      = RSI_DLRQ.DLRQ_STATE_EXEC 
                                  AND RQ.t_Type       = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                                  AND RQ.t_DealPart   = 1 
                                  AND Opr.t_Kind_Operation = Tick.t_DealType 
                                  AND Leg.t_DealID    = Tick.t_DealID 
                                  AND Leg.t_LegID     = 0 
                                  AND Leg.t_LegKind   = 0 
                                  AND Fin.t_FIID      = Leg.t_PFI 
                                  AND PM.t_DocID      = Tick.t_DealID 
                                  AND PM.t_DocKind    = Tick.t_BOfficeKind 
                                  AND PM.t_State      = RSI_DLRQ.DLRQ_STATE_EXEC
                                  AND PM.t_Type       = RSI_DLRQ.DLRQ_TYPE_PAYMENT 
                                  AND PM.t_DealPart   = 1 
                                  AND (RQ.t_FactDate BETWEEN pBegDate AND pEndDate OR PM.t_FactDate BETWEEN pBegDate AND pEndDate)
                                  AND (CASE WHEN RQ.t_FactDate < PM.t_FactDate THEN PM.t_FactDate ELSE RQ.t_FactDate END) BETWEEN pBegDate AND pEndDate
                             ) q
                        WHERE q.t_FactDate > v_StartDate
                     ) 
      LOOP

        v_TaxGroup := npto.GetPaperTaxGroupNPTX(one_rec.t_PFI);

        IF  ((one_rec.IsRepo = 0 AND one_rec.IsLoan = 0) OR  
             RSI_NPTX.CheckCateg(RSB_SECUR.OBJTYPE_SECDEAL, 23, LPAD(one_rec.t_DealID, 34, '0'), 2)=1  --категория "Является налоговым Репо" на сделке DDL_TICK.T_DEALID задана и равна False
            ) AND
            ( 
               v_TaxGroup <> 40 
               OR RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(one_rec.t_DealID, 34, '0'), 53, one_rec.t_DealDate) <> 2 --категория "Первичное размещение"
            ) THEN


            v_ExistsLucreData := 1;
            EXIT;
        END IF;
      END LOOP;

    END IF;

    IF v_ExistsLucreData = 0 THEN
      FOR one_rec IN (SELECT *
                        FROM (
                                WITH sf AS (SELECT t_ID, t_PartyID 
                                              FROM dsfcontr_dbt 
                                             WHERE t_PartyID = pClient
                                               AND t_ServKind = RSI_NPTO.PTSK_STOCKDL
                                           )
                                SELECT RQ.t_FactDate, RQ.t_DocKind, Leg.t_CFI, Leg.t_PFI, Leg.t_Principal, Leg.t_RelativePrice, 
                                       Fin.t_FaceValueFI, Tick.t_DealID, Tick.t_DealDate, 
                                       (CASE WHEN Tick.t_Flag3 <> 'X' THEN DECODE( Leg.t_Start, TO_DATE('01.01.0001','DD.MM.YYYY'), Tick.t_DealDate, Leg.t_Start )
                                             ELSE npto.GetDateFromAvrWrtIn( Tick.t_DealID, Leg.t_Start, Tick.t_DealDate ) END) BuyDate,
                                       NVL((SELECT t_Sum
                                              FROM ddlsum_dbt
                                             WHERE t_DocKind = Tick.t_BOfficeKind
                                               AND t_DocID   = Tick.t_DealID
                                               AND t_Kind    = RSI_NPTXC.DLSUM_KIND_OTHERLUCRESUM
                                               AND ROWNUM    = 1
                                           ), 0) as OtherLucreSum 
                                FROM sf, ddl_tick_dbt Tick, ddlrq_dbt RQ, ddl_leg_dbt Leg, dfininstr_dbt Fin, 
                                     (SELECT t_Kind_Operation
                                        FROM doprkoper_dbt 
                                       WHERE t_DocKind = RSB_SECUR.DL_AVRWRT 
                                         AND Rsb_Secur.IsAvrWrtIn(rsb_secur.get_OperationGroup(t_SysTypes)) = 1) Opr 
                                WHERE Tick.t_BOfficeKind   = RSB_SECUR.DL_AVRWRT
                                  AND Tick.t_ClientID      = sf.t_PartyID
                                  AND Tick.t_ClientContrID = sf.t_ID
                                  AND Tick.t_Flag2         = 'X'
                                  AND RQ.t_DocKind         = Tick.t_BOfficeKind
                                  AND RQ.t_DocID           = Tick.t_DealID
                                  AND RQ.t_State           = RSI_DLRQ.DLRQ_STATE_EXEC
                                  AND RQ.t_Type            = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                                  AND RQ.t_DealPart        = 1
                                  AND Opr.t_Kind_Operation = Tick.t_DealType
                                  AND Leg.t_DealID         = Tick.t_DealID 
                                  AND Leg.t_LegID          = 0 
                                  AND Leg.t_LegKind        = 0 
                                  AND Fin.t_FIID           = Leg.t_PFI 
                                  AND RQ.t_FactDate >= pBegDate 
                                  AND RQ.t_FactDate <= pEndDate 
                                  AND Tick.t_Flag3 = 'X' /*учитывать в налоговом учете*/ 
                             ) q
                             WHERE q.BuyDate > v_StartDate
                     )
      LOOP

        v_TaxGroup := npto.GetPaperTaxGroupNPTX(one_rec.t_PFI);

        IF one_rec.OtherLucreSum <> 0 THEN
          v_ExistsLucreData := 1;
          EXIT;
        ELSIF  v_TaxGroup <> 100 AND 
            (v_TaxGroup <> 40 
             or RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(one_rec.t_DealID, 34, '0'), 53, one_rec.t_DealDate) <> 2 --категория "Первичное размещение"
            ) THEN

            v_ExistsLucreData := 1;
            EXIT;
        END IF;
      END LOOP;

    END IF;

    IF v_ExistsLucreData = 0 THEN
      FOR one_rec IN (SELECT D.t_ID 
                        FROM DDVNDEAL_DBT D, DDVNFI_DBT NFI, DFININSTR_DBT FIN 
                       WHERE D.t_Client = pClient 
                         AND D.t_Type = RSB_DERIVATIVES.ALG_DV_BUY 
                         AND D.t_Date >= pBegDate 
                         AND D.t_Date <= pEndDate 
                         AND D.t_Date > v_StartDate
                         AND ((D.t_DVKind = RSB_DERIVATIVES.DV_FORWARD AND D.t_ISPFI = 'X') or D.t_DVKind = RSB_DERIVATIVES.DV_OPTION) 
                         AND NFI.t_DealID = D.t_ID
                         AND NFI.t_Type = (CASE WHEN D.t_Forvard = 'X' THEN 1 /*DV_NFIType_Forward*/ ELSE 0 /*DV_NFIType_BaseActiv*/ END)
                         AND FIN.T_FIID = NFI.t_FIID
                         AND ROWNUM = 1
                     )
      LOOP
          v_ExistsLucreData := 1;
          EXIT;

      END LOOP;

    END IF;

    RETURN v_ExistsLucreData;

  END;

  PROCEDURE CreateNptxOp( pPackNum IN NUMBER, pEndID IN NUMBER, pGUID IN VARCHAR2, pOperDprt IN NUMBER, pOper IN NUMBER, 
                          pCreateDate IN DATE,
                          pSubKind IN NUMBER,
                          pOperNum IN VARCHAR2,
                          pIIS IN CHAR,
                          pWarnLaterOper IN CHAR,
                          pNoFormNoDeals IN CHAR,
                          pRecalc IN CHAR DEFAULT CHR(0),
                          pCalcNDFL IN CHAR DEFAULT CHR(0),
                          pOpPrefix IN VARCHAR2 DEFAULT CHR(0),
                          pIsTechnical IN CHAR DEFAULT CHR(0),
                          pClientIdType IN NUMBER DEFAULT 0
                        )
  IS
    v_stat NUMBER(10);
    v_RefID NUMBER (10);

    v_ListClient ListClient_t;
    v_PrevCalcDate DATE;
    v_PrevCloseDate DATE;

    v_TaxPeriod NUMBER := EXTRACT(YEAR FROM pCreateDate);

    v_EndDatePrevYear DATE := TO_DATE('31.12.' || TO_CHAR(v_TaxPeriod-1), 'DD.MM.YYYY');

    v_IIS NUMBER(5) := CASE WHEN pIIS = CNST.SET_CHAR THEN 1 ELSE 0 END;
    v_Kind_Operation NUMBER(10) := GetKindOperation(RSI_NPTXC.DL_CALCNDFL);

    v_nptxop DNPTXOP_DBT%ROWTYPE;
    v_ListNPTXOP ListNPTXOP_t := ListNPTXOP_t();

    v_UseReference BOOLEAN := CASE WHEN NVL(pOperNum, CHR(0)) = CHR(0) THEN TRUE ELSE FALSE END;
    v_Num NUMBER(10) := 0;

    v_dnptxmassprot DNPTXMASSPROT_DBT%ROWTYPE;
    v_ListMassProtocol ListMassProtocol_t := ListMassProtocol_t();
    v_MassComment VARCHAR(1024) := '';
    v_protocolWriten NUMBER(1) := 0;

    v_SubKindName DNAMEALG_DBT.T_SZNAMEALG%TYPE;

    v_PrevDate DATE;
    v_ErrMsg VARCHAR2(1024);

  BEGIN

    FOR cData IN ( SELECT party.t_PartyID, party.t_ShortName, mas.t_DlContrID,
                          NVL((SELECT sf.t_Number FROM ddlcontr_dbt dlc, dsfcontr_dbt sf WHERE dlc.t_DlContrID = mas.t_DlContrID AND sf.t_ID = dlc.t_SfContrID), CHR(1)) as ContrNum,
                          count(1) over() CurrentPackSize
                     FROM DNPTXOPMASS_DBT mas, DPARTY_DBT party
                    WHERE mas.t_GUID = pGUID
                      AND mas.t_PackNum = pPackNum
                      AND party.t_PartyID = mas.t_ClientID
                 )
    LOOP
      v_stat := 0;

      IF v_protocolWriten != 0 THEN 
        v_ListMassProtocol.Extend();
        v_ListMassProtocol(v_ListMassProtocol.last) := v_dnptxmassprot;
      END IF;

      IF v_protocolWriten = 0 THEN 
        v_protocolWriten := 1; 
      END IF;


      v_dnptxmassprot.t_Comment    := CHR(1);
      v_dnptxmassprot.t_nptxnumber := CHR(1);
      v_dnptxmassprot.t_ClientID   := cData.t_PartyID;
      v_dnptxmassprot.t_ContrNum   := cData.ContrNum;

      IF pIIS = CNST.UNSET_CHAR THEN
        v_PrevCalcDate := RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CALCNDFL, cData.t_PartyID, pIIS );
        v_PrevCloseDate := RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CLOSE, cData.t_PartyID, pIIS );
      END IF;

      IF pIIS = CNST.UNSET_CHAR AND 
         v_PrevCalcDate < v_EndDatePrevYear AND
         v_PrevCloseDate < v_EndDatePrevYear AND
         ((pSubKind <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND RSI_NPTO.CheckExistDataForPeriod( TO_DATE('01.01.0001', 'DD.MM.YYYY'), v_EndDatePrevYear, cData.t_PartyID, v_IIS ) > 0)
          OR
          (pSubKind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND RSI_NPTXOP.CheckExistsLucreData( v_PrevCalcDate+1, v_EndDatePrevYear, cData.t_PartyID) > 0)
         )
      THEN
         v_dnptxmassprot.t_Comment := 'Пропущен период расчета НОБ';
      ELSE

        BEGIN
          v_SubKindName := CHR(1);

          IF pRecalc = CHR(0) THEN
            SELECT na.t_szNameAlg
              INTO v_SubKindName
              FROM dnptxop_dbt op, dnamealg_dbt na
             WHERE op.t_DocKind = RSI_NPTXC.DL_CALCNDFL
               AND op.t_Client = cData.t_PartyID
               AND op.t_Contract = cData.t_DlContrID
               AND op.t_OperDate BETWEEN TO_DATE('01.01.'||TO_CHAR(v_TaxPeriod),'DD.MM.YYYY') AND TO_DATE('31.12.'||TO_CHAR(v_TaxPeriod),'DD.MM.YYYY')
               AND op.t_IIS = (CASE WHEN v_IIS = 1 THEN 'X' ELSE CHR(0) END)
               AND (   op.t_SubKind_Operation = RSI_NPTXC.DL_TXBASECALC_OPTYPE_ENDYEAR  
                    OR (op.t_SubKind_Operation = RSI_NPTXC.DL_TXBASECALC_OPTYPE_CLOSE_IIS AND RSB_SECUR.GetMainObjAttr(132 /*OBJTYPE_NPTXCALC*/, LPAD(op.t_ID, 34, '0'), 1, op.t_OperDate) = 0)
                   )
               AND op.t_Status IN (RSI_NPTXC.DL_TXOP_Open, RSI_NPTXC.DL_TXOP_Close)
               AND na.t_iTypeAlg = 7337
               AND na.t_iNumberAlg = op.t_SubKind_Operation
               AND ROWNUM = 1;
          ELSE
            SELECT na.t_szNameAlg
              INTO v_SubKindName
              FROM dnptxop_dbt op, dnamealg_dbt na
             WHERE op.t_DocKind = RSI_NPTXC.DL_CALCNDFL
               AND op.t_Client = cData.t_PartyID
               AND op.t_Contract = cData.t_DlContrID
               AND op.t_OperDate BETWEEN TO_DATE('01.01.'||TO_CHAR(v_TaxPeriod),'DD.MM.YYYY') AND TO_DATE('31.12.'||TO_CHAR(v_TaxPeriod),'DD.MM.YYYY')
               AND op.t_IIS = (CASE WHEN v_IIS = 1 THEN 'X' ELSE CHR(0) END)
               AND (   op.t_SubKind_Operation = RSI_NPTXC.DL_TXBASECALC_OPTYPE_ENDYEAR 
                    OR (op.t_SubKind_Operation = RSI_NPTXC.DL_TXBASECALC_OPTYPE_CLOSE_IIS AND RSB_SECUR.GetMainObjAttr(132 /*OBJTYPE_NPTXCALC*/, LPAD(op.t_ID, 34, '0'), 1, op.t_OperDate) = 0)
                   )
               AND op.t_SubKind_Operation <> pSubKind
               AND op.t_Status IN (RSI_NPTXC.DL_TXOP_Open, RSI_NPTXC.DL_TXOP_Close)
               AND na.t_iTypeAlg = 7337
               AND na.t_iNumberAlg = op.t_SubKind_Operation
               AND ROWNUM = 1;
          END IF;

          v_dnptxmassprot.t_Comment := 'За заданный период имеются операции расчета с типом '||TO_CHAR(v_SubKindName);
          v_stat := 1;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
        END;

        IF v_stat = 0 THEN

          IF v_UseReference THEN
            v_stat := RSI_RSB_REFER.GetReferenceIDByType(OBJTYPE_NPTXCALC, REFOBJ_NPTXCALC, v_RefID);
            IF (v_stat = 0) THEN
              v_stat:= RSI_RSB_REFER.WldGenerateReference(v_nptxop.t_Code, v_RefID, OBJTYPE_NPTXCALC, 0, NULL, NULL, CHR(0), CHR(88), pOperDprt);
              v_nptxop.t_Code := CASE WHEN pOpPrefix  <> CNST.UNSET_CHAR THEN pOpPrefix||v_nptxop.t_Code ELSE v_nptxop.t_Code END;                                                                                               
            END IF;
          ELSE
            v_nptxop.t_Code := GenerateOperNum( pOperNum, v_Num+(pPackNum-1)*cData.CurrentPackSize ); --номера будут последовательные только в рамках pPackNum :(
            v_nptxop.t_Code := CASE WHEN pOpPrefix  <> CNST.UNSET_CHAR THEN pOpPrefix||v_nptxop.t_Code ELSE v_nptxop.t_Code END;                                          
            v_Num := v_Num + 1;
          END IF;
          
          --заполнение буфера операции
          v_nptxop.t_ID := dnptxop_dbt_seq.nextval;
          v_nptxop.t_DocKind := RSI_NPTXC.DL_CALCNDFL;
          v_nptxop.t_OperDate := pCreateDate;
          v_nptxop.t_Kind_Operation := v_Kind_Operation;
          v_nptxop.t_Client := cData.t_PartyID;
          v_nptxop.t_Contract := cData.t_DlContrID;
          v_nptxop.t_Department := pOperDprt;
          v_nptxop.t_Oper := pOper;
          v_nptxop.t_Status := RSI_NPTXC.DL_TXOP_Prep;
          v_nptxop.t_SubKind_Operation := pSubKind;
          v_nptxop.t_IIS := CASE WHEN v_IIS = 1 THEN CNST.SET_CHAR ELSE  CNST.UNSET_CHAR END;
          v_nptxop.t_Recalc := CASE WHEN pRecalc = CNST.SET_CHAR THEN CNST.SET_CHAR ELSE  CNST.UNSET_CHAR END;
          v_nptxop.t_CalcNDFL := CASE WHEN pCalcNDFL = CNST.SET_CHAR THEN CNST.SET_CHAR ELSE  CNST.UNSET_CHAR END;
          
          IF pSubKind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE THEN
            v_nptxop.t_CalcNDFL := CNST.UNSET_CHAR;
          END IF;

          IF(pRecalc = CNST.SET_CHAR) THEN
            v_PrevDate := NptxCalcTaxPrevDateByKind(cData.t_PartyID, (CASE WHEN v_IIS = 1 THEN CNST.SET_CHAR ELSE CNST.UNSET_CHAR END), pCreateDate, pSubKind, cData.t_DlContrID);

            v_nptxop.t_PrevDate := v_PrevDate;
            v_nptxop.t_EndRecalcDate := v_nptxop.t_PrevDate;

            IF pIIS = CNST.SET_CHAR THEN
              v_nptxop.t_BegRecalcDate := RSI_NPTO.GetFirstDateIIS(v_nptxop.t_Client, v_nptxop.t_Contract);
            ELSE
              v_nptxop.t_BegRecalcDate := TO_DATE('01.01.'||(TO_CHAR(v_nptxop.t_EndRecalcDate, 'YYYY')),'DD.MM.YYYY');
            END IF;

            IF v_nptxop.t_PrevDate <> TO_DATE('01.01.0001','DD.MM.YYYY') THEN
              v_nptxop.t_OperDate := v_nptxop.t_PrevDate;
            END IF;

            IF(v_nptxop.t_PrevDate = NPTAX.UnknownDate) THEN
              v_dnptxmassprot.t_Comment := 'У клиента нет операций для пересчета';
              CONTINUE;
            END IF;

          ELSE
            v_nptxop.t_BegRecalcDate := NPTAX.UnknownDate;
            v_nptxop.t_EndRecalcDate := CASE WHEN v_IIS = 1 THEN TRUNC(SYSDATE) ELSE NPTAX.UnknownDate END;

            v_PrevDate := NptxCalcTaxPrevDateByKind(cData.t_PartyID, (CASE WHEN v_IIS = 1 THEN CNST.SET_CHAR ELSE CNST.UNSET_CHAR END), pCreateDate, 0, cData.t_DlContrID);

            v_nptxop.t_PrevDate      := v_PrevDate;
          
          END IF;
          --ЗПУ
          v_nptxop.t_Account := RSI_RsbOperation.ZERO_STR;
          v_nptxop.t_AccountTax := RSI_RsbOperation.ZERO_STR;
          v_nptxop.t_Currency := 0;
          v_nptxop.t_CurrencySum := -1;
          v_nptxop.t_CurrentYear_Sum := 0;
          v_nptxop.t_FIID := -1;
          v_nptxop.t_FlagTax := CNST.UNSET_CHAR;
          v_nptxop.t_LimitStatus := 0;
          v_nptxop.t_MarketPlace := 0;
          v_nptxop.t_MarketPlace2 := 0;
          v_nptxop.t_MarketSector := 0;
          v_nptxop.t_MarketSector2 := 0;
          v_nptxop.t_Method := 0;
          v_nptxop.t_OutCost := 0;
          v_nptxop.t_OutSum := 0;
          v_nptxop.t_Partial := CNST.UNSET_CHAR;
          v_nptxop.t_Place := -1;
          v_nptxop.t_Place2 := 0;
          v_nptxop.t_PlaceKind := 0;
          v_nptxop.t_PlaceKind2 := 0;
          v_nptxop.t_PrevTaxSum := 0;
          v_nptxop.t_Tax := 0;
          v_nptxop.t_TaxBase := 0;
          v_nptxop.t_TaxSum := 0;
          v_nptxop.t_TaxSum2 := 0;
          v_nptxop.t_TaxToPay := 0;
          v_nptxop.t_Time := NPTAX.UnknownTime;
          v_nptxop.t_TotalTaxSum := 0;
          v_nptxop.t_TOUT := 0;
          v_nptxop.t_TaxDp := 0;
          v_nptxop.t_PayMedical := CHR(0);
          v_nptxop.t_Receiver := -1;
          v_nptxop.t_PayPurpose := 0;

          IF pIIS = CNST.SET_CHAR THEN
            v_nptxop.t_Technical := pIsTechnical;
          END IF;

          v_stat := NPTX.Check_Document(3, v_nptxop, NULL); --Пользовательские проверки

          IF v_stat = 0 THEN
            v_ListNPTXOP.Extend();
            v_ListNPTXOP(v_ListNPTXOP.last) := v_nptxop;

            IF pIsTechnical = CNST.SET_CHAR THEN
              AddTechnical (v_nptxop.t_ID);
            END IF;

            v_dnptxmassprot.t_nptxnumber := v_nptxop.t_Code;
          ELSE
            v_dnptxmassprot.t_Comment := ' Польз. ф-я проверки вернула stat='||TO_CHAR(v_stat);
          END IF;
        END IF;
      END IF;
    END LOOP;

    v_ListMassProtocol.Extend();
    v_ListMassProtocol(v_ListMassProtocol.last) := v_dnptxmassprot;

    --Вставки
    IF v_ListNPTXOP.COUNT > 0 THEN
      FORALL i IN v_ListNPTXOP.FIRST .. v_ListNPTXOP.LAST
        INSERT INTO DNPTXOP_DBT VALUES v_ListNPTXOP(i);
      
      FORALL i IN v_ListNPTXOP.FIRST .. v_ListNPTXOP.LAST
        INSERT INTO DSCTAXMES_TMP (T_ID, T_DEALID, T_FIID, T_TYPE, T_MESSAGE, T_MESTIME) VALUES(v_ListNPTXOP(i).t_ID, 0, 0, MESTYPE_OK, CHR(1), TO_DATE('01.01.0001 ' || TO_CHAR(SYSDATE(), 'HH24:MI:SS'), 'DD.MM.YYYY HH24:MI:SS'));

      v_ListNPTXOP.DELETE;
    END IF;
    
    IF v_ListMassProtocol.COUNT > 0 THEN
      FORALL i IN v_ListMassProtocol.FIRST .. v_ListMassProtocol.LAST
        INSERT INTO DNPTXMASSPROT_DBT VALUES v_ListMassProtocol(i);
      v_ListMassProtocol.DELETE;
    END IF;


  EXCEPTION WHEN OTHERS THEN

    v_ErrMsg := SUBSTR(sqlerrm, 1, 1024);

    INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
    SELECT TO_CHAR(v_dnptxmassprot.t_ClientID), CHR(1), v_dnptxmassprot.t_ContrNum, v_ErrMsg
      FROM DUAL;
  END;


  PROCEDURE MasCreateNptxOp( pCreateDate IN DATE,
                             pSubKind IN NUMBER,
                             pOperNum IN VARCHAR2,
                             pIIS IN CHAR,
                             pWarnLaterOper IN CHAR,
                             pNoFormNoDeals IN CHAR,
                             pGUID IN VARCHAR2,
                             pExecPackSize IN NUMBER,
                             pRecalc IN CHAR DEFAULT CHR(0),
                             pCalcNDFL IN CHAR DEFAULT CHR(0),
                             pClientGroup IN NUMBER DEFAULT 0,
                             pClientIdType IN NUMBER DEFAULT 0,
                             pFIID IN NUMBER DEFAULT 0,
                             pTaxPeriod IN NUMBER DEFAULT 0,
                             pPeriodFrom IN DATE DEFAULT TO_DATE('01.01.0001', 'dd.mm.yyyy'),
                             pPeriodTo IN DATE DEFAULT TO_DATE('01.01.0001', 'dd.mm.yyyy'),
                             pOpPrefix IN VARCHAR2 DEFAULT CHR(0),
                             pIsTechnical IN CHAR DEFAULT CHR(0))
  IS
    v_pIIS CHAR;
    v_pWarnLaterOper CHAR;
    v_pNoFormNoDeals CHAR;
    v_pRecalc CHAR;
    v_pCalcNDFL CHAR;
    
    v_task_name VARCHAR2(300);
    v_sql_chunks CLOB;
    v_sql_process VARCHAR2(400);
    v_try NUMBER(5) := 0;
    v_status NUMBER;

    TYPE masexec_t IS TABLE OF DNPTXOPMASS_DBT%ROWTYPE INDEX BY BINARY_INTEGER;
    v_masexec masexec_t;
    v_MaxPackNum NUMBER(10);
    v_dboSqlClientsQuery VARCHAR(5000);

    v_sql      VARCHAR2(1500);
    
    v_PeriodFrom DATE;
    v_PeriodTo DATE;
  BEGIN
    v_pIIS := pIIS;
    IF v_pIIS <> CNST.SET_CHAR THEN
       v_pIIS := CNST.UNSET_CHAR;
    END IF;
    v_pWarnLaterOper := pWarnLaterOper;
    IF v_pWarnLaterOper <> CNST.SET_CHAR THEN
       v_pWarnLaterOper := CNST.UNSET_CHAR;
    END IF;
    v_pNoFormNoDeals := pNoFormNoDeals;
    IF v_pNoFormNoDeals <> CNST.SET_CHAR THEN
       v_pNoFormNoDeals := CNST.UNSET_CHAR;
    END IF;
    v_pRecalc := pRecalc;
    IF v_pRecalc <> CNST.SET_CHAR THEN
       v_pRecalc := CNST.UNSET_CHAR;
    END IF;
    v_pCalcNDFL := pCalcNDFL;
    IF v_pCalcNDFL <> CNST.SET_CHAR THEN
       v_pCalcNDFL := CNST.UNSET_CHAR;
    END IF;
   

    v_PeriodFrom := TRUNC(pPeriodFrom);
    IF v_PeriodFrom IS NULL THEN
       v_PeriodFrom := TO_DATE('01.01.0001','DD.MM.YYYY');
    END IF;

    v_PeriodTo := TRUNC(pPeriodTo);
    IF v_PeriodTo IS NULL THEN
       v_PeriodTo := TO_DATE('01.01.0001','DD.MM.YYYY');
    END IF;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE DNPTXMASSPROT_DBT';

    IF pClientGroup = 2 THEN --Если клиенты загружаются из списка
      --Заполним во временной таблице ID клиента в СОФР, чтобы дальше только по нему работать
      IF pClientIdType = 1 THEN
        
        UPDATE DNPTXOPMASSLIST_TMP TMP
           SET TMP.T_CLIENTID = TO_NUMBER(TMP.T_IN_CLIENTID)
         WHERE EXISTS(SELECT 1 FROM DPARTY_DBT PT WHERE PT.T_PARTYID = TO_NUMBER(TMP.T_IN_CLIENTID));

      ELSIF pClientIdType = 2 THEN
        
        UPDATE DNPTXOPMASSLIST_TMP TMP
           SET T_CLIENTID = (SELECT CODE.T_OBJECTID 
                               FROM DOBJCODE_DBT CODE, DPARTY_DBT PT 
                              WHERE PT.T_PARTYID = CODE.T_OBJECTID AND CODE.T_CODE = TMP.T_IN_CLIENTID AND CODE.T_OBJECTTYPE = 3 AND CODE.T_CODEKIND = 101 AND PT.T_ISPROBDOUBLER != 'X' AND PT.T_ISDOUBLER != 'X')
         WHERE EXISTS(SELECT CODE.T_OBJECTID 
                        FROM DOBJCODE_DBT CODE, DPARTY_DBT PT 
                       WHERE PT.T_PARTYID = CODE.T_OBJECTID AND CODE.T_CODE = TMP.T_IN_CLIENTID AND CODE.T_OBJECTTYPE = 3 AND CODE.T_CODEKIND = 101 AND PT.T_ISPROBDOUBLER != 'X' AND PT.T_ISDOUBLER != 'X');

      END IF;

      --Для всех клиентов из списка, по которым не удалось найти ID в СОФР, заносим ошибку в протокол
      INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
      SELECT DECODE(pClientIdType, 1, T_IN_CLIENTID, CHR(1)), DECODE(pClientIdType, 2, T_IN_CLIENTID, CHR(1)), T_CONTRNUM, 'Не корректный ID клиента'
        FROM DNPTXOPMASSLIST_TMP
       WHERE T_CLIENTID <= 0;

      DELETE FROM DNPTXOPMASSLIST_TMP WHERE T_CLIENTID <= 0;

      --Занести в протокол сообщение, если субъект не является физлицом
      INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
      SELECT TO_CHAR(T_CLIENTID), DECODE(pClientIdType, 2, T_IN_CLIENTID, CHR(1)), T_CONTRNUM, 'NOT_SHOW_CONTRNUMНедопустимая форма субъекта'
        FROM DNPTXOPMASSLIST_TMP TMP
       WHERE EXISTS(SELECT 1 FROM DPARTY_DBT PT WHERE PT.T_PARTYID = TMP.T_CLIENTID AND PT.T_LEGALFORM <> 2);

      --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
      DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);

      INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
      SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'У клиента в анкете заполнена дата смерти'
        FROM DNPTXOPMASSLIST_TMP tmp
       WHERE EXISTS (select 1 from dpersn_dbt pr where pr.t_personid = tmp.T_CLIENTID and pr.t_Death <> to_date('01010001','ddmmyyyy'));

      DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);

      IF v_pIIS = CNST.SET_CHAR THEN --Если генерим для ИИС
        
        --Для всех записей из списка, по которым не заполнен номер ДБО, заносим ошибку в протокол
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(T_CLIENTID), DECODE(pClientIdType, 2, T_IN_CLIENTID, CHR(1)), T_CONTRNUM, 'Не корректный договор ИИС'
          FROM DNPTXOPMASSLIST_TMP
         WHERE (T_CONTRNUM = CHR(1) OR T_CONTRNUM IS NULL);

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);

        --Для всех записей из списка, по которым заполнен номер ДБО, но такого ДБО в принципе нет в базе, заносим ошибку в протокол
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'Не корректный договор ИИС'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE NOT EXISTS(SELECT 1 FROM DSFCONTR_DBT WHERE T_NUMBER = TMP.T_CONTRNUM AND T_SERVKIND = 0);

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);


        --Для всех записей из списка, по которым указан договор, он есть в базе, но он не является ИИС, заносим ошибку в протокол (добавим в текст маркер NOT_SHOW_CONTRNUM, чтобы потом по этой строке номер договора не выводить в протокол)
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'NOT_SHOW_CONTRNUMНе корректный договор ИИС'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE NOT EXISTS(SELECT 1 FROM DSFCONTR_DBT SF, DDLCONTR_DBT DLC WHERE SF.T_NUMBER = TMP.T_CONTRNUM AND SF.T_PARTYID = TMP.T_CLIENTID AND SF.T_SERVKIND = 0 AND DLC.T_SFCONTRID = SF.T_ID AND DLC.T_IIS = 'X');

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);

        --Заполним во временной таблице ID ДБО по загруженному номеру ДБО
        UPDATE DNPTXOPMASSLIST_TMP TMP
           SET TMP.T_DLCONTRID = NVL((SELECT dlc.t_DlContrID
                                        FROM dsfcontr_dbt sf_dlc, ddlcontr_dbt dlc
                                       WHERE sf_dlc.t_Number = TMP.T_CONTRNUM
                                         AND sf_dlc.t_PartyID = TMP.T_CLIENTID
                                         AND sf_dlc.t_DateBegin <= pCreateDate
                                         AND 1 = (CASE WHEN pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_NORMAL AND (sf_dlc.t_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY') OR sf_dlc.t_DateClose > pCreateDate) THEN 1
                                                       WHEN pSubkind <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_NORMAL AND (sf_dlc.t_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY') OR EXTRACT(YEAR FROM sf_dlc.t_DateClose) = pTaxPeriod) THEN 1
                                                       ELSE 0 END)
                                         AND dlc.t_SfContrID = sf_dlc.t_ID
                                         AND dlc.t_IIS = CNST.SET_CHAR
                                     ), 0)
        WHERE TMP.T_DLCONTRID <= 0;

        --Для всех записей из списка, по которым не удалось найти ID ДБО, заносим ошибку в протокол
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(T_CLIENTID), DECODE(pClientIdType, 2, T_IN_CLIENTID, CHR(1)), T_CONTRNUM, 'У клиента нет открытого договора ИИС из списка'
          FROM DNPTXOPMASSLIST_TMP
         WHERE T_DLCONTRID <= 0;

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);

        --Для всех записей из списка, по которым есть дублирующая запись, заносим ошибку в протокол
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'По клиенту '||TO_CHAR(TMP.T_CLIENTID)||' и Договору '||TMP.T_CONTRNUM||' существует дублирующая запись'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE (SELECT COUNT(1) FROM DNPTXOPMASSLIST_TMP TMP1 WHERE TMP1.T_CLIENTID = TMP.T_CLIENTID AND TMP1.T_CONTRNUM = TMP.T_CONTRNUM) > 1;

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);

        --Если пересчет, то проверяем, есть ли у клиентов операции расчета
        IF v_pRecalc = CNST.SET_CHAR THEN
          --Для всех записей из списка c ДБО > 0, по которым не удалось найти существующих операций расчета НОБ, заносим ошибку в протокол
          INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
          SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'За заданный налоговый период нет операций расчета НОБ. Пересчет не сформирован'
            FROM DNPTXOPMASSLIST_TMP TMP
           WHERE TMP.T_DLCONTRID > 0
             AND NOT EXISTS(SELECT 1
                              FROM DNPTXOP_DBT OP
                             WHERE OP.t_DocKind = RSI_NPTXC.DL_CALCNDFL
                               AND OP.t_Client = TMP.t_ClientID
                               AND OP.t_Contract = TMP.T_DLCONTRID
                               AND OP.t_IIS = CNST.SET_CHAR
                               AND OP.t_SubKind_Operation = pSubkind
                               AND OP.t_OperDate BETWEEN TO_DATE('01.01.'||TO_CHAR(pTaxPeriod),'DD.MM.YYYY') AND TO_DATE('31.12.'||TO_CHAR(pTaxPeriod),'DD.MM.YYYY')
                               AND OP.t_Status IN (RSI_NPTXC.DL_TXOP_Open, RSI_NPTXC.DL_TXOP_Close)
                           );

        END IF;

      ELSE --Не ИИС
        --Занести в протокол сообщение, если у клиента не найден ДБО
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(T_CLIENTID), DECODE(pClientIdType, 2, T_IN_CLIENTID, CHR(1)), T_CONTRNUM, 'У клиента нет договора ДБО'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE 0 = (CASE WHEN pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_NORMAL AND RSI_NPTO.ExistNotIISContr(TMP.t_ClientID, pCreateDate) = 1 THEN 1
                         WHEN pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_ENDYEAR AND (RSI_NPTO.ExistNotIISContr(TMP.t_ClientID, pCreateDate) = 1 OR RSI_NPTO.ExistNotIISContr(TMP.t_ClientID, TO_DATE('01.01.'||TO_CHAR(EXTRACT(YEAR FROM pCreateDate)), 'DD.MM.YYYY')) = 1) THEN 1
                         WHEN RSI_NPTO.ExistNotIISContr(TMP.t_ClientID, pCreateDate) = 1 THEN 1
                         ELSE 0 END); 

      END IF;

      --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
      DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);
    
    ELSE --По всем субъектам
      --Отобрать клиентов
      --Занесем их во временную таблицу

      DELETE FROM DNPTXOPMASSLIST_TMP;

      IF v_pIIS = CNST.UNSET_CHAR THEN --Не ИИС

        IF pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE THEN
          INSERT INTO DNPTXOPMASSLIST_TMP(T_IN_CLIENTID, T_CLIENTID, T_CONTRNUM, T_DLCONTRID)
          SELECT TO_CHAR(party.t_PartyID), party.t_PartyID, CHR(1), 0
            FROM dparty_dbt party
           WHERE EXISTS (SELECT client.t_PartyID  --являются клиентами Фондового диллинга или Срочных контрактов на дату операции
                           FROM dclient_dbt client 
                          WHERE client.t_PartyID = party.t_PartyID 
                            AND client.t_ServiceKind IN (RSI_NPTO.PTSK_STOCKDL, RSI_NPTO.PTSK_DV) 
                            AND client.t_StartDate <= pCreateDate
                            AND (   client.t_FinishDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') 
                                 OR client.t_FinishDate > pCreateDate)
                        )
             AND party.t_LegalForm = PM_COMMON.PTLEGF_PERSN
             AND RSI_NPTO.ExistNotIISContr(party.t_PartyID, pCreateDate) = 1
             AND RSI_NPTXOP.CheckExistsLucreData(RSI_NPTXOP.NptxCalcTaxPrevDateByKind(party.t_PartyID, CNST.UNSET_CHAR, pCreateDate, pSubkind, 0, 1, 1), v_PeriodTo, party.t_PartyID) = 1;
        ELSE

          INSERT INTO DNPTXOPMASSLIST_TMP(T_IN_CLIENTID, T_CLIENTID, T_CONTRNUM, T_DLCONTRID)
          SELECT TO_CHAR(party.t_PartyID), party.t_PartyID, CHR(1), 0
            FROM dparty_dbt party
           WHERE EXISTS (SELECT client.t_PartyID  --являются клиентами Фондового диллинга или Срочных контрактов на дату операции
                           FROM dclient_dbt client 
                          WHERE client.t_PartyID = party.t_PartyID 
                            AND client.t_ServiceKind IN (RSI_NPTO.PTSK_STOCKDL, RSI_NPTO.PTSK_DV) 
                            AND client.t_StartDate <= pCreateDate
                            AND (   client.t_FinishDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') 
                                 OR client.t_FinishDate > pCreateDate)
                        )
             AND party.t_LegalForm = PM_COMMON.PTLEGF_PERSN
             AND 1 = (CASE WHEN pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_NORMAL AND RSI_NPTO.ExistNotIISContr(party.t_PartyID, pCreateDate) = 1 THEN 1
                           WHEN pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_ENDYEAR AND (RSI_NPTO.ExistNotIISContr(party.t_PartyID, pCreateDate) = 1 OR RSI_NPTO.ExistNotIISContr(party.t_PartyID, TO_DATE('01.01.'||TO_CHAR(EXTRACT(YEAR FROM pCreateDate)), 'DD.MM.YYYY')) = 1) THEN 1
                           WHEN RSI_NPTO.ExistNotIISContr(party.t_PartyID, pCreateDate) = 1 THEN 1
                           ELSE 0 END);
        END IF;

        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'У клиента в анкете заполнена дата смерти'
          FROM DNPTXOPMASSLIST_TMP tmp
         WHERE EXISTS (select 1 from dpersn_dbt pr where pr.t_personid = tmp.T_CLIENTID and pr.t_Death <> to_date('01010001','ddmmyyyy'));

        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);

      ELSE --ИИС

        --Сначала добавим всех подходящих субъектов, без заполения ДБО (укажем ДБО = -1, чтобы потом отделить эти записи)
        INSERT INTO DNPTXOPMASSLIST_TMP(T_IN_CLIENTID, T_CLIENTID, T_CONTRNUM, T_DLCONTRID)
        SELECT TO_CHAR(party.t_PartyID), party.t_PartyID, CHR(1), -1
          FROM dparty_dbt party
         WHERE EXISTS (SELECT client.t_PartyID  --являются клиентами Фондового диллинга или Срочных контрактов на дату операции
                         FROM dclient_dbt client 
                        WHERE client.t_PartyID = party.t_PartyID 
                          AND client.t_ServiceKind IN (RSI_NPTO.PTSK_STOCKDL, RSI_NPTO.PTSK_DV) 
                          AND client.t_StartDate <= pCreateDate
                          AND (   client.t_FinishDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') 
                               OR client.t_FinishDate > pCreateDate)
                      )
           AND party.t_LegalForm = PM_COMMON.PTLEGF_PERSN;

        --Для каждой добавленной записи с ДБО = -1 найдем все подходящие ДБО ИИС и создадим записи с ними
        INSERT INTO DNPTXOPMASSLIST_TMP(T_IN_CLIENTID, T_CLIENTID, T_CONTRNUM, T_DLCONTRID)
        SELECT TO_CHAR(TMP.T_CLIENTID), TMP.t_ClientID, sf_dlc.t_Number, dlc.t_DlContrID
          FROM DNPTXOPMASSLIST_TMP TMP, dsfcontr_dbt sf_dlc, ddlcontr_dbt dlc
         WHERE TMP.t_DlContrID = -1
           AND sf_dlc.t_PartyID = TMP.t_ClientID
           AND sf_dlc.t_ServKind = 0
           AND 1 = (CASE WHEN pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_NORMAL AND (sf_dlc.t_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY') OR sf_dlc.t_DateClose > pCreateDate) THEN 1
                         WHEN pSubkind <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_NORMAL AND (sf_dlc.t_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY') OR EXTRACT(YEAR FROM sf_dlc.t_DateClose) = EXTRACT(YEAR FROM pCreateDate)) THEN 1
                         ELSE 0 END)
           AND dlc.t_SfContrID = sf_dlc.t_ID
           AND dlc.t_IIS = CNST.SET_CHAR;

        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'У клиента в анкете заполнена дата смерти'
          FROM DNPTXOPMASSLIST_TMP tmp
         WHERE EXISTS (select 1 from dpersn_dbt pr where pr.t_personid = tmp.T_CLIENTID and pr.t_Death <> to_date('01010001','ddmmyyyy'));

        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);

        --Для всех записей из списка c ДБО = -1, по которым не удалось создать записей в найденным ДБО, заносим ошибку в протокол
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), TMP.T_CONTRNUM, 'У клиента нет открытого договора ИИС'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE TMP.T_DLCONTRID = -1
           AND NOT EXISTS(SELECT 1
                            FROM DNPTXOPMASSLIST_TMP TMP1
                           WHERE TMP1.t_ClientID = TMP.t_ClientID
                             AND TMP1.t_DlContrID > 0
                         );

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE TMP.t_DlContrID = -1 AND EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);

        --Если пересчет, то проверяем, есть ли у клиентов операции расчета
        IF v_pRecalc = CNST.SET_CHAR THEN
          --Для всех записей из списка c ДБО = -1, по которым не удалось найти существующих операций расчета НОБ, заносим ошибку в протокол
          INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CONTRNUM, T_COMMENT)
          SELECT TO_CHAR(TMP.T_CLIENTID), TMP.T_CONTRNUM, 'За заданный налоговый период нет операций расчета НОБ. Пересчет не сформирован'
            FROM DNPTXOPMASSLIST_TMP TMP
           WHERE TMP.T_DLCONTRID > 0
             AND NOT EXISTS(SELECT 1
                              FROM DNPTXOP_DBT OP
                             WHERE OP.t_DocKind = RSI_NPTXC.DL_CALCNDFL
                               AND OP.t_Client = TMP.t_ClientID
                               AND OP.t_Contract = TMP.t_DlContrID
                               AND OP.t_IIS = CNST.SET_CHAR
                               AND OP.t_SubKind_Operation = pSubkind
                               AND OP.t_OperDate BETWEEN TO_DATE('01.01.'||TO_CHAR(pTaxPeriod),'DD.MM.YYYY') AND TO_DATE('31.12.'||TO_CHAR(pTaxPeriod),'DD.MM.YYYY')
                               AND OP.t_Status IN (RSI_NPTXC.DL_TXOP_Open, RSI_NPTXC.DL_TXOP_Close)
                           );

          --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
          DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE TMP.T_DLCONTRID > 0 AND EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);
        END IF;


        --Удаляем из временной таблицы записи c незаполненным ДБО
        DELETE FROM DNPTXOPMASSLIST_TMP WHERE t_DlContrID <= 0;
      END IF;

    END IF;

    IF v_pIIS = CNST.UNSET_CHAR THEN

      IF v_pRecalc = CNST.UNSET_CHAR THEN
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'Есть более поздние операции расчета НОБ'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE EXISTS(SELECT 1
                       FROM DNPTXOP_DBT OP
                      WHERE OP.t_DocKind = RSI_NPTXC.DL_CALCNDFL
                        AND OP.t_Client = TMP.t_ClientID
                        AND OP.t_IIS = CNST.UNSET_CHAR
                        AND (  (pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND OP.t_SubKind_Operation = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE) 
                            OR (pSubkind <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND OP.t_SubKind_Operation <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE)
                            )
                        AND 1 = (CASE WHEN OP.t_Status IN (RSI_NPTXC.DL_TXOP_Open, RSI_NPTXC.DL_TXOP_Close) AND OP.t_OperDate >= pCreateDate THEN 1
                                      WHEN OP.t_Status IN (RSI_NPTXC.DL_TXOP_Prep) AND OP.t_OperDate = pCreateDate THEN 1
                                      ELSE 0 END)
                     );

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);
      END IF;

      IF v_pRecalc = CNST.SET_CHAR THEN

        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'Есть более поздние операции расчета НОБ'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE EXISTS(SELECT 1
                       FROM DNPTXOP_DBT OP
                      WHERE OP.t_DocKind = RSI_NPTXC.DL_CALCNDFL
                        AND OP.t_Client = TMP.t_ClientID
                        AND OP.t_IIS = CNST.UNSET_CHAR
                        AND (  (pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND OP.t_SubKind_Operation = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE) 
                            OR (pSubkind <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND OP.t_SubKind_Operation <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE)
                            )
                        AND 1 = (CASE WHEN OP.t_Status IN (RSI_NPTXC.DL_TXOP_Open, RSI_NPTXC.DL_TXOP_Close) AND OP.t_OperDate > pCreateDate THEN 1
                                      ELSE 0 END)
                        AND EXTRACT(YEAR FROM OP.T_OPERDATE) = pTaxPeriod
                     );

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);
        
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), TMP.T_CONTRNUM, 'За заданный налоговый период нет операций расчета НОБ. Пересчет не сформирован'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE NOT EXISTS(SELECT 1
                            FROM DNPTXOP_DBT OP
                           WHERE OP.t_DocKind = RSI_NPTXC.DL_CALCNDFL
                             AND OP.t_Client = TMP.t_ClientID
                             AND OP.t_IIS = CNST.UNSET_CHAR
                             AND OP.t_SubKind_Operation = pSubkind
                             AND OP.t_OperDate BETWEEN TO_DATE('01.01.'||TO_CHAR(pTaxPeriod),'DD.MM.YYYY') AND TO_DATE('31.12.'||TO_CHAR(pTaxPeriod),'DD.MM.YYYY')
                             AND OP.t_Status IN (RSI_NPTXC.DL_TXOP_Open, RSI_NPTXC.DL_TXOP_Close)
                         );

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);
      END IF;

      --11.3.     "Тип расчета НОБ" = "Обычный расчет" или "Окончание года", признак "Пересчет" НЕ установлен, признак "Не формировать при отсутствии сделок" установлен - должна выполняться проверка для сочетания (Клиент + Договор), 
      --на наличие торговых операций у клиента по соответствующему договору за период ограниченный параметрами "за период от" и "за период до" (оба параметра включительно), 
      --если таковые не найдены, то операция расчета НОБ для НДФЛ не создается, в Протокол с ошибками выводится сообщение "Нет сделок в период расчета НОБ";
      IF (pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_NORMAL OR pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_ENDYEAR) AND v_pRecalc = CNST.UNSET_CHAR AND v_pNoFormNoDeals = CNST.SET_CHAR THEN
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'Нет сделок в период расчета НОБ'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE RSI_NPTO.CheckExistDataForPeriod(NptxCalcTaxPrevDateByKind(TMP.T_CLIENTID, CNST.UNSET_CHAR, pCreateDate, 0, TMP.t_DlContrID, 1), v_PeriodTo, TMP.t_ClientID, 0, -1, 1, TMP.t_DlContrID) = 0;

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);

      END IF;

      IF pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND pClientGroup = 2 /*Клиенты из списка*/ THEN
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'Нет сделок, подходящих под условия отбора'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE RSI_NPTXOP.CheckExistsLucreData(RSI_NPTXOP.NptxCalcTaxPrevDateByKind(TMP.T_CLIENTID, CNST.UNSET_CHAR, pCreateDate, pSubkind, TMP.t_DlContrID, 1, 1), v_PeriodTo, TMP.t_ClientID) = 0;

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);
      END IF;

    ELSE --ИИС

      --11.2.     "Тип расчета НОБ" = "Обычный расчет" или "Окончание года", признак "Пересчет" НЕ установлен - должна выполняться проверка для сочетания (Клиент + Договор), 
      --на наличие у клиента расчета НОБ с типом "Закрытие ИИС" (признак технического расчета = НЕТ)  по соответствующему договору ИИС (период и статус операции не имеют значения) 
      --и/или дата окончания договора != пусто, если таковая найдена, и/или дата окончания договора != пусто, то операция расчета НОБ для НДФЛ не создается, 
      --в Протокол с ошибками выводится сообщение "По договору <№ договора ИИС> выполнен расчет НОБ с типом "Закрытие ИИС""
      IF (pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_NORMAL OR pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_ENDYEAR OR pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_CLOSE_IIS) AND v_pRecalc = CNST.UNSET_CHAR THEN
        
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'По договору '||TO_CHAR(TMP.T_CONTRNUM)||' выполнен расчет НОБ с типом "Закрытие ИИС"'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE EXISTS(SELECT 1
                       FROM DNPTXOP_DBT OP
                      WHERE OP.t_DocKind = RSI_NPTXC.DL_CALCNDFL
                        AND OP.t_Client = TMP.t_ClientID
                        AND OP.t_Contract = TMP.t_DlContrID
                        AND OP.t_IIS = CNST.SET_CHAR
                        AND OP.t_SubKind_Operation = RSI_NPTXC.DL_TXBASECALC_OPTYPE_CLOSE_IIS
                        AND OP.t_Technical = CNST.UNSET_CHAR
                     );

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);

        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'Договор ИИС является закрытым'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE EXISTS(SELECT 1
                       FROM DSFCONTR_DBT SF, DDLCONTR_DBT DLC
                      WHERE DLC.T_DLCONTRID = TMP.t_DlContrID
                        AND SF.T_ID = DLC.T_SFCONTRID
                        AND SF.T_DATECLOSE > TO_DATE('01.01.0001','DD.MM.YYYY')
                     );

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);

      END IF;
      
      --11.1.     "Тип расчета НОБ" = "Обычный расчет" или "Окончание года", признак "Пересчет" НЕ установлен - должна выполняться проверка для сочетания (Клиент + Договор), 
      --на наличие выполненной операции расчета НОБ для НДФЛ с более поздними параметрами дата/время соответствующего типа расчета НОБ, если таковая найдена, операция расчета НОБ для НДФЛ не создается, 
      --в Протокол с ошибками выводится сообщение "Есть более поздние операции расчета НОБ";
      IF (pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_NORMAL OR pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_ENDYEAR OR pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_CLOSE_IIS) AND v_pRecalc = CNST.UNSET_CHAR THEN
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'Есть более поздние операции расчета НОБ'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE EXISTS(SELECT 1
                       FROM DNPTXOP_DBT OP
                      WHERE OP.t_DocKind = RSI_NPTXC.DL_CALCNDFL
                        AND OP.t_Client = TMP.t_ClientID
                        AND OP.t_Contract = TMP.t_DlContrID
                        AND OP.t_IIS = CNST.SET_CHAR
                        AND OP.t_OperDate > pCreateDate
                     );

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);
      END IF;


      --11.3.     "Тип расчета НОБ" = "Обычный расчет" или "Окончание года", признак "Пересчет" НЕ установлен, признак "Не формировать при отсутствии сделок" установлен - должна выполняться проверка для сочетания (Клиент + Договор), 
      --на наличие торговых операций у клиента по соответствующему договору за период ограниченный параметрами "за период от" и "за период до" (оба параметра включительно), 
      --если таковые не найдены, то операция расчета НОБ для НДФЛ не создается, в Протокол с ошибками выводится сообщение "Нет сделок в период расчета НОБ";
      IF (pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_NORMAL OR pSubkind = RSI_NPTXC.DL_TXBASECALC_OPTYPE_ENDYEAR) AND v_pRecalc = CNST.UNSET_CHAR AND v_pNoFormNoDeals = CNST.SET_CHAR THEN
        INSERT INTO DNPTXMASSPROT_DBT(T_CLIENTID, T_CLIENTCODE, T_CONTRNUM, T_COMMENT)
        SELECT TO_CHAR(TMP.T_CLIENTID), DECODE(pClientIdType, 2, TMP.T_IN_CLIENTID, CHR(1)), TMP.T_CONTRNUM, 'Нет сделок в период расчета НОБ'
          FROM DNPTXOPMASSLIST_TMP TMP
         WHERE RSI_NPTO.CheckExistDataForPeriod(NptxCalcTaxPrevDateByKind(TMP.T_CLIENTID, CNST.SET_CHAR, pCreateDate, 0, TMP.t_DlContrID, 1), v_PeriodTo, TMP.t_ClientID, 1, -1, 1, TMP.t_DlContrID) = 0;

        --Удаляем из временной таблицы с загруженным списком клиентов записи, по которым что-то добавляли в протокол
        DELETE FROM DNPTXOPMASSLIST_TMP TMP WHERE EXISTS(SELECT 1 FROM DNPTXMASSPROT_DBT PROT WHERE PROT.T_CLIENTID = TO_CHAR(TMP.T_CLIENTID) AND PROT.T_CONTRNUM = TMP.T_CONTRNUM);

      END IF;
      
    END IF;

    SELECT pGUID,
           TO_NUMBER(ROWNUM),
           t_ClientID,
           t_DlContrID
      BULK COLLECT INTO v_masexec
      FROM DNPTXOPMASSLIST_TMP;

    IF v_masexec.COUNT > 0 THEN
      FORALL i IN v_masexec.FIRST .. v_masexec.LAST
        INSERT INTO DNPTXOPMASS_DBT VALUES v_masexec(i);

      v_MaxPackNum := v_masexec(v_masexec.LAST).t_PackNum;
      v_masexec.DELETE;

      IF  1 <> 1 THEN --Выполняем без распараллеливания

        FOR one_rec IN (SELECT DISTINCT t_PackNum FROM DNPTXOPMASS_DBT WHERE T_GUID = pGUID ORDER BY t_PackNum ASC)
        LOOP
          RSI_NPTXOP.CreateNptxOp(one_rec.t_PackNum, 0,
                                  pGUID, 
                                  RsbSessionData.OperDprt,
                                  RsbSessionData.Oper,
                                  pCreateDate, 
                                  pSubKind,
                                  pOperNum,
                                  v_pIIS,
                                  v_pWarnLaterOper,
                                  v_pNoFormNoDeals,
                                  v_pRecalc,
                                  v_pCalcNDFL,
                                  pOpPrefix,
                                  pIsTechnical,
                                  pClientIdType);

        END LOOP;
      ELSE

        v_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;
        DBMS_PARALLEL_EXECUTE.create_task (task_name => v_task_name);

        v_sql_chunks := 'SELECT level, 0 FROM DUAL CONNECT BY LEVEL <= '||TO_CHAR(v_MaxPackNum);

        DBMS_PARALLEL_EXECUTE.create_chunks_by_sql(task_name => v_task_name,
                                                   sql_stmt  => v_sql_chunks,
                                                   by_rowid  => FALSE);

        v_sql_process := 'CALL RSI_NPTXOP.CreateNptxOp(:start_id, :end_id, '||
                                                       '''' || pGUID || ''', ' ||
                                                       TO_CHAR(RsbSessionData.OperDprt) || ', ' ||
                                                       TO_CHAR(RsbSessionData.Oper) || ', ' ||
                                                       'TO_DATE('''||TO_CHAR(pCreateDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY''), ' ||
                                                       TO_CHAR(pSubKind) || ', ' ||
                                                       '''' || pOperNum || ''', ' ||
                                                       '''' || (CASE  WHEN v_pIIS = CHR(88) THEN CHR(88) ELSE CHR(0) END )|| ''', ' ||
                                                       '''' || v_pWarnLaterOper || ''', ' ||
                                                       '''' || v_pNoFormNoDeals || ''', ' ||
                                                       '''' || v_pRecalc || ''', ' ||
                                                       '''' || v_pCalcNDFL || ''', ' ||
                                                       '''' || pOpPrefix || ''', ' ||
                                                       '''' || pIsTechnical || ''', ' ||
                                                       '''' || pClientIdType || ''')';

        DBMS_PARALLEL_EXECUTE.run_task(task_name => v_task_name,
                                       sql_stmt => v_sql_process,
                                       language_flag => DBMS_SQL.NATIVE,
                                       parallel_level => PARALLEL_LEVEL);

        v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
        WHILE(v_try < 2 and v_status != DBMS_PARALLEL_EXECUTE.FINISHED)
        LOOP
          v_try := v_try + 1;
          DBMS_PARALLEL_EXECUTE.resume_task(v_task_name);
          v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
        END LOOP;

        DBMS_PARALLEL_EXECUTE.drop_task(v_task_name);
      END IF;

      --DELETE FROM DNPTXOPMASSLIST_TMP;
    END IF;
  END;

  FUNCTION GetStrMes (pNPTXOPID IN NUMBER)
    RETURN VARCHAR2
  IS
    v_mes   VARCHAR2 (3799);
    v_cnt   NUMBER (10);
  BEGIN
    v_mes := CHR (1);

    SELECT COUNT (1)
      INTO v_cnt
      FROM dnptxop_dbt
     WHERE T_ID = pNPTXOPID AND t_status = 1;

    IF (v_cnt > 0) THEN
      SELECT COUNT (1)
        INTO v_cnt
        FROM DNPTXMES_DBT
       WHERE ((t_Type = 10) OR (t_Type = 20)) AND t_DocID = pNPTXOPID;

      IF (v_cnt > 0) THEN
        DECLARE
          E_OBJECT_TOOBIG   EXCEPTION;
          PRAGMA EXCEPTION_INIT (E_OBJECT_TOOBIG, -1489);
        BEGIN
          SELECT LISTAGG (t_message, '; ') WITHIN GROUP (ORDER BY t_message DESC)
            INTO v_mes
            FROM DNPTXMES_DBT
           WHERE ((t_type = 10) OR (t_type = 20)) AND t_docid = pNPTXOPID AND ROWNUM < 16;
        EXCEPTION
          WHEN E_OBJECT_TOOBIG THEN
            RETURN 'Ошибка формирования строки вывода';
        END;

        IF (v_cnt > 15) THEN
          v_mes :=
               v_mes
            || '; Множественные ошибки. См. Протокол по F7';
        END IF;
      END IF;
    END IF;

    RETURN v_mes;
  END;

  FUNCTION GetCntMes (pNPTXOPID IN NUMBER)
    RETURN NUMBER
  IS
    v_cnt      NUMBER (10);
    v_cntErr   NUMBER (10);
  BEGIN
    v_cntErr := 0;

    SELECT COUNT (1)
      INTO v_cnt
      FROM dnptxop_dbt
     WHERE T_ID = pNPTXOPID AND t_status = 1;

    IF (v_cnt > 0) THEN
      SELECT COUNT (1)
        INTO v_cntErr
        FROM DNPTXMES_DBT
       WHERE ((t_Type = 10) OR (t_Type = 20)) AND t_DocID = pNPTXOPID;
    END IF;

    RETURN v_cntErr;
  END;

END RSI_NPTXOP;
/