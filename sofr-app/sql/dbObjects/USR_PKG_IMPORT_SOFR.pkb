CREATE OR REPLACE PACKAGE BODY USR_PKG_IMPORT_SOFR
AS


 --заменяем не цифровые символы строки кодом ASCII
 FUNCTION ASCIISymNonDigitToCode( p_StrSource IN VARCHAR2, p_CntSymbCorrect IN INTEGER DEFAULT 0, p_isReverse INTEGER DEFAULT 0) RETURN VARCHAR2
 IS
   v_StrBuf         VARCHAR2(200);
   v_Symb           VARCHAR2(1);
   v_StrSrcLength   INTEGER;
   v_i              INTEGER;
   v_Column_Length  INTEGER;

  BEGIN
   BEGIN

     v_StrSrcLength := LENGTH(p_StrSource) ;
     v_StrBuf := '';

     FOR v_i IN 1..v_StrSrcLength
     LOOP

      v_Symb := SUBSTR(p_StrSource, v_i, 1);

      IF( INSTR( '0123456789', v_Symb) <> 0 ) THEN
       v_StrBuf := v_StrBuf || v_Symb;
      ELSE
       v_StrBuf := v_StrBuf || TO_CHAR( ASCII(v_Symb)/*:o:3*/ );
      END IF;


     END LOOP;


     SELECT (CHAR_LENGTH - /*7*/ p_CntSymbCorrect) INTO v_Column_Length --ограничение ЦФТ до 8 символов
     FROM user_tab_columns WHERE TABLE_NAME = UPPER('dacctrn_dbt')
      AND COLUMN_NAME = UPPER('T_NUMB_DOCUMENT');

     IF v_Column_Length >= LENGTH(v_StrBuf) THEN
      RETURN v_StrBuf;
     ELSIF p_isReverse = 1 THEN
      RETURN SUBSTR(v_StrBuf, LENGTH(v_StrBuf) - v_Column_Length + 1);
     ELSE
      RETURN SUBSTR(v_StrBuf, 1, v_Column_Length);
     END IF;


   EXCEPTION
    WHEN OTHERS THEN
    BEGIN

     RETURN p_StrSource;

    END;
   END;

 END;



 --получить строку ошибки события
 FUNCTION GetStrEventErr( p_Mode IN INTEGER, p_Objecttype IN NUMBER, p_ObjectId IN NUMBER ) RETURN VARCHAR2
 IS                                                                                          

  v_StrErr   VARCHAR2(1000);
  v_ErrCode  INTEGER; 

 BEGIN
  BEGIN

    IF p_Mode = 0 THEN --из СОФР в ЦФТ Отсутствует в СОФР

     SELECT NVL( ( SELECT D.T_STATUS FROM  utableprocessevent_dbt D                  
                   WHERE D.T_OBJECTID = p_ObjectId
                    AND D.T_OBJECTTYPE = p_Objecttype
                    AND D.T_TYPE IN( 1, 2 )
--                    AND D.T_STATUS IN(4, 5)                                 
--                    AND D.T_MESSAGEID IS NOT NULL                           
--                    AND D.T_MESSAGEID > 0                                   
--                    AND D.T_RESULTCODE = 0                                
                    AND NOT EXISTS( SELECT 1 FROM utableprocessevent_dbt I  
                                    WHERE I.T_OBJECTID = D.T_OBJECTID       
                                     AND I.T_OBJECTTYPE = D.T_OBJECTTYPE    
                                     AND I.T_TYPE = 3                       
                                     AND I.T_RECID >  D.T_RECID ) ) , -1 ) INTO v_ErrCode FROM DUAL;

      SELECT ( CASE WHEN v_ErrCode = -1 OR v_ErrCode = 4 THEN 'Из СОФР в ЦФТ. Отсутствует в СОФР' 
                    WHEN v_ErrCode = 5 OR v_ErrCode = 2 OR v_ErrCode = 1 THEN 'Из СОФР в ЦФТ. Ошибка выгрузки в ЦФТ' 
                    ELSE 'Из СОФР в ЦФТ. Отсутствует в СОФР' END )
      INTO v_StrErr FROM DUAL;



    ELSIF p_Mode = 1 THEN  --из СОФР в ЦФТ Отсутствует в ЦФТ

     SELECT NVL( ( SELECT D.T_STATUS FROM  utableprocessevent_dbt D                  
                   WHERE D.T_OBJECTID = p_ObjectId
                    AND D.T_OBJECTTYPE = p_Objecttype
                    AND D.T_TYPE IN( 1, 2 )
--                    AND D.T_STATUS IN(4, 5)                                 
--                    AND D.T_MESSAGEID IS NOT NULL                           
--                    AND D.T_MESSAGEID > 0                                   
--                    AND D.T_RESULTCODE = 0                                
                    AND NOT EXISTS( SELECT 1 FROM utableprocessevent_dbt I  
                                    WHERE I.T_OBJECTID = D.T_OBJECTID       
                                     AND I.T_OBJECTTYPE = D.T_OBJECTTYPE    
                                     AND I.T_TYPE = 3                       
                                     AND I.T_RECID >  D.T_RECID ) ) , -1 ) INTO v_ErrCode FROM DUAL;

      SELECT ( CASE WHEN v_ErrCode = -1 OR v_ErrCode = 4 THEN 'Из СОФР в ЦФТ. Присутствует в СОФР, Отсутствует в ЦФТ' 
                    WHEN v_ErrCode = 5 OR v_ErrCode = 2 OR v_ErrCode = 1 THEN 'Из СОФР в ЦФТ. Ошибка выгрузки в ЦФТ' 
                    ELSE 'Из СОФР в ЦФТ. Присутствует в СОФР, Отсутствует в ЦФТ' END )
      INTO v_StrErr FROM DUAL;


    ELSIF p_Mode = 2 THEN  --из ЦФТ в СОФР


      SELECT NVL( ( SELECT 1 FROM upmbiscotto_dbt V 
                    WHERE V.T_BISCOTTOID = TO_CHAR( p_ObjectId )
                     AND V.T_DOCKIND IN(322, 320) ) , -1 ) INTO v_ErrCode FROM DUAL;

      SELECT ( CASE WHEN v_ErrCode = -1 THEN 'Из ЦФТ в СОФР. Отсутствует в СОФР' 
                    ELSE 'Из ЦФТ в СОФР. Отсутствует в СОФР' END )
      INTO v_StrErr FROM DUAL;

    ELSIF p_Mode = 3 THEN  --из ЦФТ в СОФР


      SELECT NVL( ( SELECT 1 FROM upmbiscotto_dbt V 
                    WHERE V.T_PMID = p_ObjectId
                     AND V.T_DOCKIND IN(322, 320) ) , -1 ) INTO v_ErrCode FROM DUAL;

      SELECT ( CASE WHEN v_ErrCode = -1 THEN 'Из ЦФТ в СОФР. Присутствует в СОФР, Отсутствует в ЦФТ' 
                    ELSE 'Из ЦФТ в СОФР. Присутствует в СОФР, Отсутствует в ЦФТ' END )
      INTO v_StrErr FROM DUAL;


    ELSE

      v_StrErr := '';

    END IF;

    RETURN v_StrErr;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN '';

   END;
  END;

 END;


 --получить строку расхождений 
 FUNCTION GetDifferenceStrErr( p_Mode IN INTEGER, p_AccD_BISQUIT IN VARCHAR2, p_AccC_BISQUIT IN VARCHAR2, p_AmCur_BISQUIT IN NUMBER,
   p_AmNatCur_BISQUIT IN NUMBER, p_Cur_BISQUIT IN VARCHAR2, p_Ground_BISQUIT IN VARCHAR2, p_DocNum_BISQUIT IN VARCHAR2,
   p_AcctrnId IN NUMBER, p_Id_BISQUIT IN VARCHAR2, p_ReqId_BISQUIT IN VARCHAR2 ) RETURN VARCHAR2
 IS                                                                                          

  v_StrErr   VARCHAR2(1000);

 BEGIN
  BEGIN

    IF p_Mode = 0 THEN

      --кроме полу-проводок
      SELECT CASE WHEN D.T_ACCOUNT_PAYER <> p_AccD_BISQUIT --C.T_ACCT_DB
                       AND ( SELECT COUNT(1) FROM dbrokacc_dbt E
                             WHERE E.T_ACCOUNT = p_AccD_BISQUIT /*C.T_ACCT_DB*/ ) = 0
                  THEN 'Счет Дебет;' END ||
             CASE WHEN D.T_ACCOUNT_RECEIVER <> p_AccC_BISQUIT --C.T_ACCT_CR
                       AND ( SELECT COUNT(1) FROM dbrokacc_dbt E
                             WHERE E.T_ACCOUNT =  p_AccC_BISQUIT /*C.T_ACCT_CR*/ ) = 0
                  THEN 'Счет Кредит;' END ||
             CASE WHEN ( CASE WHEN D.T_FIID_PAYER = 0 AND D.T_FIID_RECEIVER = 0 OR p_AmCur_BISQUIT = 0 THEN 0 ELSE D.T_SUM_PAYER END ) <> p_AmCur_BISQUIT --C.T_AMTCUR
                       OR ( CASE WHEN D.T_FIID_PAYER = 0 AND D.T_FIID_RECEIVER = 0 OR p_AmCur_BISQUIT = 0 THEN 0 ELSE D.T_SUM_RECEIVER END ) <> p_AmCur_BISQUIT --C.T_AMTCUR
                  THEN 'Сумма в валюте счета;' END ||
             CASE WHEN D.T_SUM_NATCUR <> p_AmNatCur_BISQUIT --C.T_AMTRUB
                  THEN 'Сумма в нац. валюте;' END ||
/*поле валюта не сверяем C.T_CURRENCY
             CASE WHEN  p_Cur_BISQUIT <> ( SELECT F.T_CODEINACCOUNT FROM dfininstr_dbt F
                                                            WHERE F.T_FIID = D.T_FIID_PAYER )
                  THEN 'Валюта;' END ||
*/
      /*
             CASE WHEN D.T_GROUND <> p_Ground_BISQUIT --C.T_DETAILS
                  THEN 'Назначение платежа;' END ||
      */
             CASE WHEN D.T_NUMB_DOCUMENT <> p_DocNum_BISQUIT --C.T_DOCNUM 
                  THEN 'Номер документа;' END INTO v_StrErr
      FROM dacctrn_dbt D
      WHERE D.T_ACCTRNID = p_AcctrnId; --C.T_ACCTRNID

    ELSIF p_Mode = 2 THEN

      --платежи
      SELECT CASE WHEN D.T_PAYERACCOUNT <> p_AccD_BISQUIT --C.T_ACCT_DB
                       AND D.T_FUTUREPAYERACCOUNT <> p_AccD_BISQUIT --C.T_ACCT_DB
                       AND ( SELECT COUNT(1) FROM dbrokacc_dbt E
                             WHERE E.T_ACCOUNT = p_AccD_BISQUIT /*C.T_ACCT_DB*/ ) = 0
                  THEN 'Счет Дебет;' END ||
             CASE WHEN D.T_RECEIVERACCOUNT <> p_AccC_BISQUIT --C.T_ACCT_CR
                       AND D.T_FUTURERECEIVERACCOUNT <> p_AccC_BISQUIT --C.T_ACCT_CR
                       AND ( SELECT COUNT(1) FROM dbrokacc_dbt E
                             WHERE E.T_ACCOUNT =  p_AccC_BISQUIT /*C.T_ACCT_CR*/ ) = 0
                  THEN 'Счет Кредит;' END ||
             CASE WHEN ( CASE WHEN D.T_FIID = 0 AND D.T_PAYFIID = 0 THEN 0 ELSE D.T_AMOUNT END ) <> p_AmCur_BISQUIT --C.T_AMTCUR
                  THEN 'Сумма в валюте счета;' END /* || */
/*             CASE WHEN D.T_SUM_NATCUR <> p_AmNatCur_BISQUIT --C.T_AMTRUB
                  THEN 'Сумма в нац. валюте;' END ||*/
/*поле валюта не сверяем C.T_CURRENCY
             CASE WHEN  p_Cur_BISQUIT <> ( SELECT F.T_CODEINACCOUNT FROM dfininstr_dbt F
                                                            WHERE F.T_FIID = D.T_FIID )
                  THEN 'Валюта;' END || 
*/
      /*
             CASE WHEN D.T_GROUND <> p_Ground_BISQUIT --C.T_DETAILS
                  THEN 'Назначение платежа;' END ||
      */
/*
             CASE WHEN 'paym' || TO_CHAR(D.T_PAYMENTID) <> p_DocNum_BISQUIT --C.T_DOCNUM 
                  THEN 'Номер документа;' END */ INTO v_StrErr
      FROM dpmpaym_dbt D
      WHERE D.T_PAYMENTID = p_AcctrnId * -1; --C.T_ACCTRNID

    ELSE

      --только полу-проводки
      SELECT CASE WHEN D.T_ACCOUNT_PAYER <> /*C.T_ACCT_DB*/
                        NVL( p_AccD_BISQUIT /*C.T_ACCT_DB*/, ( SELECT G.T_ACCT_DB FROM uloadentforcompare_dbt G
                                                               WHERE G.T_IDBISCOTTO = p_Id_BISQUIT  --C.T_IDBISCOTTO
                                                                AND G.T_REQID = p_ReqId_BISQUIT  --C.T_REQID
                                                                AND G.T_ACCT_CR IS NULL AND G.T_ACCT_DB IS NOT NULL )  )
                  THEN 'Счет Дебет;' END ||
             CASE WHEN D.T_ACCOUNT_RECEIVER <> /*C.T_ACCT_CR*/
                        NVL ( p_AccC_BISQUIT /*C.T_ACCT_CR*/, ( SELECT G.T_ACCT_CR FROM uloadentforcompare_dbt G
                                                                WHERE G.T_IDBISCOTTO = p_Id_BISQUIT --C.T_IDBISCOTTO
                                                                  AND G.T_REQID = p_ReqId_BISQUIT --C.T_REQID
                                                                  AND G.T_ACCT_DB IS NULL AND G.T_ACCT_CR IS NOT NULL )  )
                  THEN 'Счет Кредит;' END ||
             CASE WHEN D.T_SUM_PAYER <> p_AmCur_BISQUIT --C.T_AMTCUR
                    OR D.T_SUM_RECEIVER <> p_AmCur_BISQUIT --C.T_AMTCUR
                  THEN 'Сумма в валюте счета;' END ||
             CASE WHEN D.T_SUM_NATCUR <> p_AmNatCur_BISQUIT --C.T_AMTRUB
                  THEN 'Сумма в нац. валюте;' END ||
/*поле валюта не сверяем C.T_CURRENCY
             CASE WHEN p_Cur_BISQUIT <> ( SELECT F.T_CODEINACCOUNT FROM dfininstr_dbt F
                                                           WHERE F.T_FIID = D.T_FIID_PAYER )
                  THEN 'Валюта;' END ||
*/
      /*
             CASE WHEN D.T_GROUND <> p_Ground_BISQUIT  --C.T_DETAILS
                  THEN 'Назначение платежа;' END ||
      */
             CASE WHEN D.T_NUMB_DOCUMENT <> p_DocNum_BISQUIT  --C.T_DOCNUM
                  THEN 'Номер документа;' END INTO v_StrErr
      FROM dacctrn_dbt D
      WHERE D.T_ACCTRNID = p_AcctrnId;  --C.T_ACCTRNID


    END IF;

    RETURN v_StrErr;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN '';

   END;
  END;

 END;



 --проверим наличие кода ЦФТ к платежу, 0 - не найден, 1 - найден, -1 - ошибка
 FUNCTION CheckBiscottoId( p_BiscottoId IN VARCHAR2 ) RETURN NUMBER
 IS

  v_Cnt   INTEGER;

 BEGIN
  BEGIN


    SELECT COUNT(1) INTO v_Cnt FROM upmbiscotto_dbt
    WHERE T_BISCOTTOID = p_BiscottoId;

    IF v_Cnt > 0 THEN

     RETURN 1;

    ELSE

     RETURN 0;

    END IF;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN -1;

   END;
  END;

 END;

function FindSofrIdByBiscotto( p_BiscottoId IN VARCHAR2 ) RETURN NUMBER
 IS
  v_pmid number(10);
 BEGIN
    v_pmid := 0;
    BEGIN
        SELECT t_pmid INTO v_pmid FROM upmbiscotto_dbt
         WHERE T_BISCOTTOID = p_BiscottoId;
    EXCEPTION
        when no_data_found then v_pmid := 0;
        when others then v_pmid := -1;
    END;
    return  v_pmid;
 END;


 --добавляем код ЦФТ к платежу, 0 - успешно, 1 - ошибка
 FUNCTION AddBiscottoId( p_PmId IN NUMBER, p_DocKind IN NUMBER, p_BiscottoId IN VARCHAR2 ) RETURN NUMBER
 IS
  v_DocKind       dnptxop_dbt.T_DOCKIND%TYPE;

 BEGIN
  BEGIN


    IF p_DocKind < 0 THEN --для nptxop подтягиваем вид документа

     SELECT T_DOCKIND INTO v_DocKind FROM dnptxop_dbt
     WHERE T_ID = p_PmId;

    ELSE

     v_DocKind := p_DocKind;

    END IF;

    INSERT INTO upmbiscotto_dbt
    VALUES( p_PmId, v_DocKind, p_BiscottoId );


    RETURN 0;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;

 END;



 --добавляем оферту для фин. инструмента, 0 - успешно, 1 - ошибка
 FUNCTION AddOffer( p_SOFR_FintoolId IN VARCHAR2, p_Fiid IN NUMBER ) RETURN NUMBER
 IS
 BEGIN
  BEGIN


  INSERT INTO doffers_dbt( T_FIID, T_OFFERNUM, T_BEGINDATEOFFER, T_ENDFATEOFFER, T_DATEREDEMPTION )
   ( SELECT p_Fiid AS T_FIID, NVL( C.ID_OFFER, 0 ) AS T_OFFERNUM, 
  --    NVL( C.OFFER_DATE, TO_DATE('01010001', 'ddmmyyyy')) AS T_BEGINDATEOFFER, 
      NVL( C.beg_order, TO_DATE('01010001', 'ddmmyyyy')) AS T_BEGINDATEOFFER,
  --    NVL( C.END_OFFER_DATE, TO_DATE('01010001', 'ddmmyyyy') ) AS T_ENDFATEOFFER,
      NVL( C.end_order, TO_DATE('01010001', 'ddmmyyyy') ) AS  T_ENDFATEOFFER,
  --    RSI_RSBCALENDAR.GetDateAfterWorkDay( NVL( C.END_OFFER_DATE, TO_DATE('01010001', 'ddmmyyyy') ), 1, NULL ) AS T_DATEREDEMPTION
      NVL( C.offer_Date, TO_DATE('01010001', 'ddmmyyyy') ) AS T_DATEREDEMPTION
     FROM sofr_bond_offers C 
     WHERE NOT EXISTS( SELECT 1 FROM doffers_dbt D
                       WHERE D.T_FIID = p_Fiid
                        AND D.T_OFFERNUM = NVL( C.ID_OFFER, 0 ) )
       
      AND C.ID_FINTOOL = p_SOFR_FintoolId );


   RETURN 0;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;

 END;




 --установить дату ограничения действия категории, добавить категорию, если имеются с более поздней датой действия 0 - успешно (ограничили или не нашли),
 -- 1 - успешно ( добавили категорию с ограничениями дат ), 2 - ошибка
 FUNCTION IsAttrIdDisconnectedSmart( p_ObjType IN NUMBER, p_GroupId IN NUMBER, p_PartyId IN NUMBER, p_AttrId IN NUMBER, p_ParentAttrId IN NUMBER,
   p_DateRating IN DATE ) RETURN NUMBER
 IS
  v_IdMinValidFromDt         dobjatcor_dbt.T_ID%TYPE;
  v_IdMaxValidFromDt         dobjatcor_dbt.T_ID%TYPE;
  v_MinValidFromDt           dobjatcor_dbt.T_VALIDFROMDATE%TYPE;
  v_MaxValidFromDt           dobjatcor_dbt.T_VALIDFROMDATE%TYPE;
  v_state                    INTEGER;
  

 BEGIN
  BEGIN

    v_state := 0;

    --категория раньше даты рейтинга с минимальной T_VALIDFROMDATE
    FOR objatcor_rec IN(
--    BEGIN

     SELECT A.T_ID, A.T_VALIDFROMDATE, A.T_VALIDTODATE --INTO v_IdMaxValidFromDt, v_MaxValidFromDt
     FROM dobjatcor_dbt A                                
     WHERE A.T_OBJECTTYPE = p_ObjType                     
      AND A.T_GROUPID = p_GroupId                         
      AND  A.T_OBJECT =  LPAD(TO_CHAR(p_PartyId), 10, '0')
--      AND A.T_ATTRID = p_AttrId                           

      AND A.T_ATTRID IN( SELECT C.T_ATTRID FROM dobjattr_dbt C
                         WHERE C.T_OBJECTTYPE = A.T_OBJECTTYPE
                          AND C.T_GROUPID = A.T_GROUPID
                          AND C.T_PARENTID = p_ParentAttrId )

      AND A.T_VALIDFROMDATE =
      ( SELECT MAX(B.T_VALIDFROMDATE)             
        FROM dobjatcor_dbt B
        WHERE B.T_OBJECTTYPE = A.T_OBJECTTYPE
          AND B.T_GROUPID = A.T_GROUPID
          AND  B.T_OBJECT =  A.T_OBJECT
--          AND B.T_ATTRID = A.T_ATTRID

          AND B.T_ATTRID IN( SELECT C.T_ATTRID FROM dobjattr_dbt C
                             WHERE C.T_OBJECTTYPE = A.T_OBJECTTYPE
                              AND C.T_GROUPID = A.T_GROUPID
                              AND C.T_PARENTID = p_ParentAttrId )


          AND B.T_VALIDFROMDATE < p_DateRating )--;
)
    LOOP
/*
    EXCEPTION
     WHEN NO_DATA_FOUND THEN
     BEGIN

      v_IdMaxValidFromDt := -1;
      
     END;
    END;
*/

     --T_VALIDTODATE

      UPDATE dobjatcor_dbt
      SET T_VALIDTODATE = p_DateRating - 1
      WHERE T_ID = objatcor_rec.T_ID
       AND T_VALIDTODATE > p_DateRating - 1;


    END LOOP;

    --категория позже (равен) даты рейтинга с минимальной T_VALIDFROMDATE
--    BEGIN

    FOR objatcor_rec IN(

     SELECT A.T_ID, A.T_VALIDFROMDATE, A.T_VALIDTODATE --INTO v_IdMinValidFromDt, v_MinValidFromDt
     FROM dobjatcor_dbt A                                
     WHERE A.T_OBJECTTYPE = p_ObjType                     
      AND A.T_GROUPID = p_GroupId                         
      AND  A.T_OBJECT =  LPAD(TO_CHAR(p_PartyId), 10, '0')
--      AND A.T_ATTRID = p_AttrId                           

      AND A.T_ATTRID IN( SELECT C.T_ATTRID FROM dobjattr_dbt C
                         WHERE C.T_OBJECTTYPE = A.T_OBJECTTYPE
                          AND C.T_GROUPID = A.T_GROUPID
                          AND C.T_PARENTID = p_ParentAttrId )

      AND A.T_VALIDFROMDATE =
      ( SELECT MIN(B.T_VALIDFROMDATE)             
        FROM dobjatcor_dbt B                                
        WHERE B.T_OBJECTTYPE = A.T_OBJECTTYPE                     
          AND B.T_GROUPID = A.T_GROUPID                         
          AND  B.T_OBJECT =  A.T_OBJECT
--          AND B.T_ATTRID = A.T_ATTRID                           
          AND B.T_ATTRID IN( SELECT C.T_ATTRID FROM dobjattr_dbt C
                             WHERE C.T_OBJECTTYPE = A.T_OBJECTTYPE
                              AND C.T_GROUPID = A.T_GROUPID
                              AND C.T_PARENTID = p_ParentAttrId )
          AND B.T_VALIDFROMDATE >= p_DateRating ) --;
    )
    LOOP

     --T_VALIDTODATE

      INSERT INTO dobjatcor_dbt( T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_ISAUTO )
      VALUES( p_ObjType, p_GroupId, p_AttrId, LPAD(TO_CHAR(p_PartyId), 10, '0'), CHR(0), p_DateRating, RSBSESSIONDATA.OPER,
         (objatcor_rec.T_VALIDFROMDATE /*v_MinValidFromDt*/ - 1), CHR(88) );

      v_state := 1;


/*
    EXCEPTION
     WHEN NO_DATA_FOUND THEN
     BEGIN

       v_IdMinValidFromDt := -1;
      
     END;
    END;
*/

    END LOOP;

/*    
    IF v_IdMaxValidFromDt <> -1 THEN --T_VALIDTODATE

      UPDATE dobjatcor_dbt
      SET T_VALIDTODATE = p_DateRating - 1
      WHERE T_ID = v_IdMaxValidFromDt
       AND T_VALIDTODATE > p_DateRating - 1;

    END IF;
*/
    
/*
    IF v_IdMinValidFromDt <> -1 THEN --T_VALIDTODATE

      INSERT INTO dobjatcor_dbt( T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_ISAUTO )
      VALUES( p_ObjType, p_GroupId, p_AttrId, LPAD(TO_CHAR(p_PartyId), 10, '0'), CHR(0), p_DateRating, RSBSESSIONDATA.OPER,
         (v_MinValidFromDt - 1), CHR(88) );

      v_state := 1;

    END IF;
*/


    RETURN v_state;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 2;

   END;
  END;

 END;


 --установить дату ограничения действия категории, добавить категорию, если имеются с более поздней датой действия 0 - успешно (ограничили или не нашли),
 -- 1 - успешно ( добавили категорию с ограничениями дат ), 2 - ошибка
 -- Для Объекта "СТРАНА"
 FUNCTION IsAttrIdDisconnectedSmart2( p_ObjType IN NUMBER, p_GroupId IN NUMBER, p_PartyId IN VARCHAR2, p_AttrId IN NUMBER, p_ParentAttrId IN NUMBER,
   p_DateRating IN DATE ) RETURN NUMBER
 IS
  v_IdMinValidFromDt         dobjatcor_dbt.T_ID%TYPE;
  v_IdMaxValidFromDt         dobjatcor_dbt.T_ID%TYPE;
  v_MinValidFromDt           dobjatcor_dbt.T_VALIDFROMDATE%TYPE;
  v_MaxValidFromDt           dobjatcor_dbt.T_VALIDFROMDATE%TYPE;
  v_state                    INTEGER;
  

 BEGIN
  BEGIN

    v_state := 0;

    --категория раньше даты рейтинга с минимальной T_VALIDFROMDATE
    FOR objatcor_rec IN(
--    BEGIN

     SELECT A.T_ID, A.T_VALIDFROMDATE, A.T_VALIDTODATE --INTO v_IdMaxValidFromDt, v_MaxValidFromDt
     FROM dobjatcor_dbt A                                
     WHERE A.T_OBJECTTYPE = p_ObjType                     
      AND A.T_GROUPID = p_GroupId                         
      AND  A.T_OBJECT = TO_CHAR(p_PartyId)
--      AND A.T_ATTRID = p_AttrId                           

      AND A.T_ATTRID IN( SELECT C.T_ATTRID FROM dobjattr_dbt C
                         WHERE C.T_OBJECTTYPE = A.T_OBJECTTYPE
                          AND C.T_GROUPID = A.T_GROUPID
                          AND C.T_PARENTID = p_ParentAttrId )

      AND A.T_VALIDFROMDATE =
      ( SELECT MAX(B.T_VALIDFROMDATE)             
        FROM dobjatcor_dbt B
        WHERE B.T_OBJECTTYPE = A.T_OBJECTTYPE
          AND B.T_GROUPID = A.T_GROUPID
          AND  B.T_OBJECT =  A.T_OBJECT
--          AND B.T_ATTRID = A.T_ATTRID

          AND B.T_ATTRID IN( SELECT C.T_ATTRID FROM dobjattr_dbt C
                             WHERE C.T_OBJECTTYPE = A.T_OBJECTTYPE
                              AND C.T_GROUPID = A.T_GROUPID
                              AND C.T_PARENTID = p_ParentAttrId )


          AND B.T_VALIDFROMDATE < p_DateRating )--;
)
    LOOP
/*
    EXCEPTION
     WHEN NO_DATA_FOUND THEN
     BEGIN

      v_IdMaxValidFromDt := -1;
      
     END;
    END;
*/

     --T_VALIDTODATE

      UPDATE dobjatcor_dbt
      SET T_VALIDTODATE = p_DateRating - 1
      WHERE T_ID = objatcor_rec.T_ID
       AND T_VALIDTODATE > p_DateRating - 1;


    END LOOP;

    --категория позже (равен) даты рейтинга с минимальной T_VALIDFROMDATE
--    BEGIN

    FOR objatcor_rec IN(

     SELECT A.T_ID, A.T_VALIDFROMDATE, A.T_VALIDTODATE --INTO v_IdMinValidFromDt, v_MinValidFromDt
     FROM dobjatcor_dbt A                                
     WHERE A.T_OBJECTTYPE = p_ObjType                     
      AND A.T_GROUPID = p_GroupId                         
      AND  A.T_OBJECT =  TO_CHAR(p_PartyId)
--      AND A.T_ATTRID = p_AttrId                           

      AND A.T_ATTRID IN( SELECT C.T_ATTRID FROM dobjattr_dbt C
                         WHERE C.T_OBJECTTYPE = A.T_OBJECTTYPE
                          AND C.T_GROUPID = A.T_GROUPID
                          AND C.T_PARENTID = p_ParentAttrId )

      AND A.T_VALIDFROMDATE =
      ( SELECT MIN(B.T_VALIDFROMDATE)             
        FROM dobjatcor_dbt B                                
        WHERE B.T_OBJECTTYPE = A.T_OBJECTTYPE                     
          AND B.T_GROUPID = A.T_GROUPID                         
          AND  B.T_OBJECT =  A.T_OBJECT
--          AND B.T_ATTRID = A.T_ATTRID                           
          AND B.T_ATTRID IN( SELECT C.T_ATTRID FROM dobjattr_dbt C
                             WHERE C.T_OBJECTTYPE = A.T_OBJECTTYPE
                              AND C.T_GROUPID = A.T_GROUPID
                              AND C.T_PARENTID = p_ParentAttrId )
          AND B.T_VALIDFROMDATE >= p_DateRating ) --;
    )
    LOOP

     --T_VALIDTODATE

      INSERT INTO dobjatcor_dbt( T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_ISAUTO )
      VALUES( p_ObjType, p_GroupId, p_AttrId, TO_CHAR(p_PartyId), CHR(0), p_DateRating, RSBSESSIONDATA.OPER,
         (objatcor_rec.T_VALIDFROMDATE /*v_MinValidFromDt*/ - 1), CHR(88) );

      v_state := 1;

    END LOOP;

    

    RETURN v_state;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 2;

   END;
  END;

 END;




 --добавить код FIID 0 - успешно, 1 - ошибка
 FUNCTION AddCodeFIID( p_ObjType IN NUMBER, p_CodeKind IN NUMBER, p_ObjId IN NUMBER, p_Code IN VARCHAR2, p_Mode IN INTEGER ) RETURN NUMBER
 IS
  v_Autokey         dobjcode_dbt.T_AUTOKEY%TYPE;
  v_Code            dobjcode_dbt.T_CODE%TYPE;
  v_Cnt             INTEGER;
 BEGIN
  BEGIN

    SELECT COUNT(1) INTO v_Cnt
    FROM dobjcode_dbt C
    WHERE C.T_OBJECTTYPE = p_ObjType
     AND C.T_CODEKIND = p_CodeKind
     AND C.T_STATE = 0
     AND C.T_OBJECTID = p_ObjId; 

    IF v_Cnt > 0 THEN

      SELECT C.T_AUTOKEY, C.T_CODE INTO v_Autokey, v_Code
      FROM dobjcode_dbt C
      WHERE C.T_OBJECTTYPE = p_ObjType
       AND C.T_CODEKIND = p_CodeKind
       AND C.T_STATE = 0
       AND C.T_OBJECTID = p_ObjId; 

    ELSE

      v_Autokey := -1;
      
    END IF;


    IF v_Autokey = -1 THEN --код не найден

     INSERT INTO dobjcode_dbt(  T_OBJECTTYPE, T_CODEKIND, T_OBJECTID, T_CODE, T_STATE, T_BANKDATE, T_SYSDATE, T_SYSTIME, T_USERID,
      T_BRANCH, T_NUMSESSION,T_UNIQUE, T_BANKCLOSEDATE )
     VALUES( p_ObjType, p_CodeKind, p_ObjId, p_Code, 0,
       RSBSESSIONDATA.CURDATE,
       TRUNC(SYSDATE), TO_DATE('01.01.0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD.MM.YYYY:HH24:MI:SS'),
       RSBSESSIONDATA.OPER, 0, 0, CHR(88) ,TO_DATE('01.01.0001', 'DD.MM.YYYY'));

    ELSIF (v_Code <> p_Code) AND ( p_Mode = 1 ) THEN

     UPDATE dobjcode_dbt
     SET T_BANKCLOSEDATE = RSBSESSIONDATA.CURDATE, T_STATE = 1
     WHERE T_AUTOKEY = v_Autokey;

     INSERT INTO dobjcode_dbt(  T_OBJECTTYPE, T_CODEKIND, T_OBJECTID, T_CODE, T_STATE, T_BANKDATE, T_SYSDATE, T_SYSTIME, T_USERID,
      T_BRANCH, T_NUMSESSION,T_UNIQUE, T_BANKCLOSEDATE )
     VALUES( p_ObjType, p_CodeKind, p_ObjId, p_Code, 0,
       (RSBSESSIONDATA.CURDATE + 1),
       TRUNC(SYSDATE), TO_DATE('01.01.0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD.MM.YYYY:HH24:MI:SS'),
       RSBSESSIONDATA.OPER, 0, 0, CHR(88) ,TO_DATE('01.01.0001', 'DD.MM.YYYY'));

    ELSIF (v_Code <> p_Code) AND ( p_Mode = 0 ) THEN

     UPDATE dobjcode_dbt
        SET T_BANKCLOSEDATE = RSBSESSIONDATA.CURDATE, T_STATE = 1
      WHERE T_AUTOKEY = v_Autokey;

     INSERT INTO dobjcode_dbt( T_OBJECTTYPE, T_CODEKIND, T_OBJECTID, T_CODE, T_STATE, 
                               T_BANKDATE, 
                               T_SYSDATE, T_SYSTIME, 
                               T_USERID, T_BRANCH, T_NUMSESSION, T_UNIQUE, T_BANKCLOSEDATE )
     VALUES( p_ObjType, p_CodeKind, p_ObjId, p_Code, 0,
             RSBSESSIONDATA.CURDATE,
             TRUNC(SYSDATE), TO_DATE('01.01.0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD.MM.YYYY:HH24:MI:SS'),
             RSBSESSIONDATA.OPER, 0, 0, CHR(88) ,TO_DATE('01.01.0001', 'DD.MM.YYYY'));
    END IF;


   RETURN 0;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;

 END;




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



 --получим наименование субъекта-банк по коду 3 и 6
 FUNCTION GetFullNameFromTwoCode( p_Code_1 IN VARCHAR2, p_Code_2 IN VARCHAR2,
   p_CodeKind_1 IN NUMBER, p_CodeKind_2 IN NUMBER, p_Date IN DATE ) RETURN VARCHAR2
 IS
  v_PartyId         dparty_dbt.T_PARTYID%TYPE;
  v_FullName        dparty_dbt.T_NAME%TYPE;
  v_Cnt             INTEGER;
  v_LastDateClose   DATE;
 BEGIN
  BEGIN

   v_FullName := '_';

   v_PartyId := USR_PKG_IMPORT_SOFR.GetPtIdActORLastClosedByCode( p_Code_1, p_CodeKind_1, p_Date );


   IF v_PartyId < 0 THEN

    v_PartyId := USR_PKG_IMPORT_SOFR.GetPtIdActORLastClosedByCode( p_Code_2, p_CodeKind_2, p_Date );

   END IF;


   IF v_PartyId > 0 THEN

    v_FullName := RSI_RSBPARTY.GetFullBankName( v_PartyId );

   END IF;

   RETURN v_FullName;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    v_FullName := '_';
    
    RETURN v_FullName;
   END;
  END;

 END;



 --получим Id субъекта-банк с закрытым кодом максимальной датой закрытия, -1 - не найден
 FUNCTION GetPtIdActORLastClosedByCode( p_Code IN VARCHAR2, p_CodeKind IN NUMBER, p_Date IN DATE ) RETURN NUMBER
 IS
  v_PartyId         dparty_dbt.T_PARTYID%TYPE;
  v_Cnt             INTEGER;
  v_LastDateClose   DATE;
 BEGIN
  BEGIN

   SELECT COUNT(A.T_OBJECTID) INTO v_Cnt
   FROM dobjcode_dbt A
   WHERE A.T_OBJECTTYPE = 3
    AND A.T_CODEKIND = p_CodeKind
    AND ( A.T_CODE = p_Code 
     OR A.T_CODE LIKE p_Code || '/%' )
    AND A.T_STATE = 0
    AND A.T_BANKDATE <= p_Date;

   IF v_Cnt > 0 THEN

    SELECT A.T_OBJECTID INTO v_PartyId
    FROM dobjcode_dbt A
    WHERE A.T_OBJECTTYPE = 3
     AND A.T_CODEKIND = p_CodeKind
     AND ( A.T_CODE = p_Code
      OR A.T_CODE LIKE p_Code || '/%' )
     AND A.T_STATE = 0
     AND A.T_BANKDATE <= p_Date
     AND ROWNUM = 1;

   ELSE

    SELECT MAX(A.T_BANKCLOSEDATE) INTO v_LastDateClose
    FROM dobjcode_dbt A
    WHERE A.T_OBJECTTYPE = 3
     AND A.T_CODEKIND = p_CodeKind
     AND ( A.T_CODE = p_Code
      OR A.T_CODE LIKE p_Code || '/%' )
     AND A.T_STATE = 1;

    SELECT A.T_OBJECTID INTO v_PartyId
    FROM dobjcode_dbt A
    WHERE A.T_OBJECTTYPE = 3
     AND A.T_CODEKIND = p_CodeKind
     AND ( A.T_CODE = p_Code
      OR A.T_CODE LIKE p_Code || '/%' )
     AND A.T_STATE = 1
     AND A.T_BANKCLOSEDATE = v_LastDateClose
     AND ROWNUM = 1;

   END IF;


   RETURN v_PartyId;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN
    
    RETURN -1;
   END;
  END;

 END;



 --получим ветку реестра содержит Id Пользовательского справочника объектов очеререди заданий
 FUNCTION GetREG_OBJECT_SYNCH RETURN VARCHAR2
 IS
 BEGIN
  BEGIN

   RETURN REG_OBJECT_SYNCH;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN
    RETURN '';
   END;
  END;

 END;


 --получим ветку реестра содержит Id Пользовательского справочника DocKind pmpaym и oproper
 FUNCTION GetREG_PAYM_OPROPER RETURN VARCHAR2
 IS
 BEGIN
  BEGIN

   RETURN REG_PAYM_OPROPER;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN
    RETURN '';
   END;
  END;

 END;


 --получим Id Пользовательского справочника объектов очеререди заданий
 FUNCTION GetOBJECT_SYNCH RETURN NUMBER
 IS
  v_RegValue   NUMBER(10);
 BEGIN
  BEGIN

   BEGIN

    v_RegValue := RSB_COMMON.GetRegIntValue(GetREG_OBJECT_SYNCH);


   EXCEPTION
    WHEN OTHERS THEN
    BEGIN
     v_RegValue := 0;
    END;
   END;


   RETURN v_RegValue;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN
    RETURN NULL;
   END;
  END;

 END;


 --получим Id Пользовательского справочника DocKind pmpaym и oproper
 FUNCTION GetPAYM_OPROPER RETURN NUMBER
 IS
  v_RegValue   NUMBER(10);
 BEGIN
  BEGIN

   BEGIN

    v_RegValue := RSB_COMMON.GetRegIntValue(GetREG_PAYM_OPROPER);


   EXCEPTION
    WHEN OTHERS THEN
    BEGIN
     v_RegValue := 0;
    END;
   END;


   RETURN v_RegValue;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN
    RETURN NULL;
   END;
  END;

 END;




 --добавим LOB запись для выгрузки в файл 0 - успешно, 1 - ошибка
 FUNCTION AddLOBToTMP( p_Mode IN INTEGER, p_IsDel IN INTEGER, p_FileName IN VARCHAR2 ) RETURN INTEGER
 IS
  v_state           INTEGER;
  v_Str_CLOB        CLOB;
  v_Str             VARCHAR2(6000);
 BEGIN

  BEGIN

   v_state := 0;

   IF p_Mode = 0 THEN

    IF p_IsDel = 1 THEN

     DELETE ulob_txt_tmp
     WHERE T_FILENAME = p_FileName || '.lim';

    END IF;


    --.lim
    FOR file_cursor IN ( SELECT T_LIMIT_TYPE, T_FIRM_ID, NULL AS T_SECCODE, T_TAG, T_CURR_CODE, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_LIMIT,
                          T_LEVERAGE, NULL AS T_TRDACCID, NULL AS T_WA_POSITION_PRICE, T_LIMIT_KIND    
                         FROM udl_lmtcashstock_exch_dbt
                         WHERE 1=1
                         UNION ALL
                         SELECT T_LIMIT_TYPE, T_FIRM_ID, T_SECCODE, NULL AS T_TAG, NULL AS T_CURR_CODE, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_LIMIT,
                          NULL AS  T_LEVERAGE, T_TRDACCID, T_WA_POSITION_PRICE, T_LIMIT_KIND
                         FROM udl_lmtsecuritest_exch_dbt
                         ORDER BY T_LIMIT_TYPE
    )
    LOOP

     IF v_state = 0 THEN
  
      INSERT INTO ulob_txt_tmp
      VALUES( p_FileName || '.lim', EMPTY_CLOB())
      RETURNING T_FILE INTO v_Str_CLOB;

      v_state := 1;

     END IF;

     SELECT (CASE WHEN file_cursor.T_LIMIT_TYPE IS NOT NULL THEN file_cursor.T_LIMIT_TYPE || ':  ' 
             ELSE NULL END) || 
            (CASE WHEN file_cursor.T_FIRM_ID IS NOT NULL THEN 'FIRM_ID = ' || file_cursor.T_FIRM_ID || '; ' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_SECCODE IS NOT NULL THEN 'SECCODE = ' || file_cursor.T_SECCODE || '; ' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_TAG IS NOT NULL THEN 'TAG = ' || file_cursor.T_TAG || '; ' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_CURR_CODE IS NOT NULL THEN 'CURR_CODE = ' || file_cursor.T_CURR_CODE || '; ' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_CLIENT_CODE IS NOT NULL THEN 'CLIENT_CODE = ' || file_cursor.T_CLIENT_CODE || '; ' 
             ELSE NULL END) ||
/*                                           (CASE WHEN file_cursor.T_CLIENT_CODE IS NOT NULL THEN 'CLIENT_CODE=' || file_cursor.T_CLIENT_CODE || ';' 
             ELSE NULL END) ||*/
            (CASE WHEN file_cursor.T_LIMIT_KIND IS NOT NULL THEN 'LIMIT_KIND = ' || file_cursor.T_LIMIT_KIND || '; ' 
             ELSE NULL END)  ||
            (CASE WHEN file_cursor.T_OPEN_BALANCE IS NOT NULL THEN 'OPEN_BALANCE = ' || file_cursor.T_OPEN_BALANCE || '; ' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_OPEN_LIMIT IS NOT NULL THEN 'OPEN_LIMIT = ' || file_cursor.T_OPEN_LIMIT || '; ' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_LEVERAGE IS NOT NULL THEN 'LEVERAGE = ' || file_cursor.T_LEVERAGE || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_TRDACCID IS NOT NULL THEN 'TRDACCID = ' || REPLACE( file_cursor.T_TRDACCID, CHR(0) ) || ';' 
             ELSE NULL END) || CHR(10)INTO v_Str FROM DUAL;

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB( v_Str ) );
                                           
    END LOOP;

    IF v_state = 1 THEN

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB(p_FileName || '.lim' || CHR(10))); --боремся с рудиментами

     UPDATE ulob_txt_tmp
     SET T_FILE = v_Str_CLOB
     WHERE T_FILENAME = p_FileName || '.lim';

     v_state := 0;

    END IF;



    --.fli
    IF p_IsDel = 1 THEN

     DELETE ulob_txt_tmp
     WHERE T_FILENAME = p_FileName || '.fli';

    END IF;

    FOR file_cursor IN ( SELECT T_CLASS_CODE, T_ACCOUNT, T_VOLUMEMN, T_VOLUMEPL, T_KFL, T_KGO, T_USE_KGO, T_FIRM_ID, T_SECCODE
                         FROM udl_lmtfuturmark_exch_dbt
                         WHERE 1=1
    )
    LOOP

     IF v_state = 0 THEN

      INSERT INTO ulob_txt_tmp
      VALUES( p_FileName || '.fli', EMPTY_CLOB())
      RETURNING T_FILE INTO v_Str_CLOB;

      v_state := 1;

     END IF;

     SELECT (CASE WHEN file_cursor.T_CLASS_CODE IS NOT NULL THEN 'CLASS_CODE=' || file_cursor.T_CLASS_CODE || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_ACCOUNT IS NOT NULL THEN 'ACCOUNT=' || file_cursor.T_ACCOUNT || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_VOLUMEMN IS NOT NULL THEN 'VOLUMEMN=' || file_cursor.T_VOLUMEMN || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_VOLUMEPL IS NOT NULL THEN 'VOLUMEPL=' || file_cursor.T_VOLUMEPL || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_KFL IS NOT NULL THEN 'KFL=' || file_cursor.T_KFL || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_KGO IS NOT NULL THEN 'KGO=' || file_cursor.T_KGO || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_USE_KGO IS NOT NULL THEN 'USE_KGO=' || file_cursor.T_USE_KGO || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_FIRM_ID IS NOT NULL THEN 'FIRM_ID=' || file_cursor.T_FIRM_ID || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_SECCODE IS NOT NULL THEN 'SECCODE=' || file_cursor.T_SECCODE || ';' 
             ELSE NULL END) || CHR(10)INTO v_Str FROM DUAL;

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB( v_Str ) );
    END LOOP;

    IF v_state = 1 THEN

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB(p_FileName || '.fli' || CHR(10))); --боремся с рудиментами

     UPDATE ulob_txt_tmp
     SET T_FILE = v_Str_CLOB
     WHERE T_FILENAME = p_FileName || '.fli';

     v_state := 0;

    END IF;

   ELSIF p_Mode = 1 THEN

    --.lci
    IF p_IsDel = 1 THEN

     DELETE ulob_txt_tmp
     WHERE T_FILENAME = p_FileName || '.lci';

    END IF;

    FOR file_cursor IN ( SELECT T_LIMIT_TYPE, T_LIMIT_ID, T_FIRM_ID, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_BALANCE, T_CURRENT_LIMIT, 
                          T_LIMIT_OPERATION, T_TRDACCID, T_SECCODE, T_TAG, T_CURR_CODE, T_LIMIT_KIND, T_LEVERAGE, T_WA_POSITION_PRICE
                         FROM udl_dl_lmtadjust_exch_dbt
                         WHERE 1=1
                         ORDER BY T_LIMIT_TYPE
    )
    LOOP

     IF v_state = 0 THEN

      INSERT INTO ulob_txt_tmp
      VALUES( p_FileName || '.lci', EMPTY_CLOB())
      RETURNING T_FILE INTO v_Str_CLOB;

      v_state := 1;

     END IF;

     SELECT (CASE WHEN file_cursor.T_LIMIT_TYPE IS NOT NULL THEN 'LIMIT_TYPE=' || file_cursor.T_LIMIT_TYPE || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_LIMIT_ID IS NOT NULL THEN 'LIMIT_ID=' || file_cursor.T_LIMIT_ID || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_FIRM_ID IS NOT NULL THEN 'FIRM_ID=' || file_cursor.T_FIRM_ID || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_CLIENT_CODE IS NOT NULL THEN 'CLIENT_CODE=' || file_cursor.T_CLIENT_CODE || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_OPEN_BALANCE IS NOT NULL THEN 'OPEN_BALANCE=' || file_cursor.T_OPEN_BALANCE || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_OPEN_LIMIT IS NOT NULL THEN 'OPEN_LIMIT=' || file_cursor.T_OPEN_LIMIT || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_CURRENT_BALANCE IS NOT NULL THEN 'CURRENT_BALANCE=' || file_cursor.T_CURRENT_BALANCE || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_CURRENT_LIMIT IS NOT NULL THEN 'CURRENT_LIMIT=' || file_cursor.T_CURRENT_LIMIT || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_LIMIT_OPERATION IS NOT NULL THEN 'LIMIT_OPERATION=' || file_cursor.T_LIMIT_OPERATION || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_TRDACCID IS NOT NULL THEN 'TRDACCID=' || file_cursor.T_TRDACCID || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_SECCODE IS NOT NULL THEN 'SECCODE=' || file_cursor.T_SECCODE || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_TAG IS NOT NULL THEN 'TAG=' || file_cursor.T_TAG || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_CURR_CODE IS NOT NULL THEN 'CURR_CODE=' || file_cursor.T_CURR_CODE || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_LIMIT_KIND IS NOT NULL THEN 'LIMIT_KIND=' || file_cursor.T_LIMIT_KIND || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_LEVERAGE IS NOT NULL THEN 'LEVERAGE=' || file_cursor.T_LEVERAGE || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_WA_POSITION_PRICE IS NOT NULL THEN 'WA_POSITION_PRICE=' || file_cursor.T_WA_POSITION_PRICE || ';' 
             ELSE NULL END) || CHR(10) INTO v_Str FROM DUAL;

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB( v_Str ) );
    END LOOP;

    IF v_state = 1 THEN

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB(p_FileName || '.lci' || CHR(10))); --боремся с рудиментами

     UPDATE ulob_txt_tmp
     SET T_FILE = v_Str_CLOB
     WHERE T_FILENAME = p_FileName || '.lci';

     v_state := 0;

    END IF;

/*!!!!!!!!!!!!!!пока не используем до выяснения - это входящий файл
    --.lco       !!!!!!!!!!!!!!!!!!!!!!этот файл не понятно откуда выгружать и что это
    IF p_IsDel = 1 THEN

     DELETE ulob_txt_tmp
     WHERE T_FILENAME = p_FileName || '.lco';

    END IF;

    FOR file_cursor IN ( SELECT T_CLASS_CODE, T_ACCOUNT, T_VOLUMEMN, T_VOLUMEPL, T_KFL, T_KGO, T_USE_KGO, T_FIRM_ID, T_SECCODE
                         FROM udl_lmtfuturmark_exch_dbt
                         WHERE 1=1
    )
    LOOP

     IF v_state = 0 THEN

      INSERT INTO ulob_txt_tmp
      VALUES( p_FileName || '.lco', EMPTY_CLOB())
      RETURNING T_FILE INTO v_Str_CLOB;

      v_state := 1;

     END IF;

     SELECT (CASE WHEN file_cursor.T_CLASS_CODE IS NOT NULL THEN 'CLASS_CODE=' || file_cursor.T_CLASS_CODE || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_ACCOUNT IS NOT NULL THEN 'ACCOUNT=' || file_cursor.T_ACCOUNT || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_VOLUMEMN IS NOT NULL THEN 'VOLUMEMN=' || file_cursor.T_VOLUMEMN || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_VOLUMEPL IS NOT NULL THEN 'VOLUMEPL=' || file_cursor.T_VOLUMEPL || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_KFL IS NOT NULL THEN 'KFL=' || file_cursor.T_KFL || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_KGO IS NOT NULL THEN 'KGO=' || file_cursor.T_KGO || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_USE_KGO IS NOT NULL THEN 'USE_KGO=' || file_cursor.T_USE_KGO || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_FIRM_ID IS NOT NULL THEN 'FIRM_ID=' || file_cursor.T_FIRM_ID || ';' 
             ELSE NULL END) ||
            (CASE WHEN file_cursor.T_SECCODE IS NOT NULL THEN 'SECCODE=' || file_cursor.T_SECCODE || ';' 
             ELSE NULL END) || CHR(10)INTO v_Str FROM DUAL;

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB( v_Str ) );
    END LOOP;

    IF v_state = 1 THEN

     DBMS_LOB.APPEND( v_Str_CLOB, TO_CLOB(p_FileName || '.lco' || CHR(10))); --боремся с рудиментами

     UPDATE ulob_txt_tmp
     SET T_FILE = v_Str_CLOB
     WHERE T_FILENAME = p_FileName || '.lco';

     v_state := 0;

    END IF;
*/
   END IF;

   RETURN v_state;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;



 --добавим записи в буферную таблицу udl_lmtcashstock_exch_dbt 0 - успешно, 1 - ошибка
 FUNCTION AddInudl_lmtcashstock_exch RETURN INTEGER
 IS
  v_state           INTEGER;
 BEGIN

  BEGIN

   v_state := 0;

   DELETE udl_lmtcashstock_exch_dbt;

   INSERT INTO udl_lmtcashstock_exch_dbt( T_LIMIT_TYPE, T_FIRM_ID, T_TAG, T_CURR_CODE, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_LIMIT,
     T_LEVERAGE, T_LIMIT_KIND )
    SELECT 'MONEY',T_FIRM_ID, T_TAG, T_CURR_CODE, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_LIMIT, T_LEVERAGE, T_LIMIT_KIND
    FROM DDL_LIMITCASHSTOCK_DBT
    WHERE T_MARKET = 1; -- могут добавиться условия!!!!!!!


   RETURN v_state;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;



 --добавим записи в буферную таблицу udl_lmtsecuritest_exch_dbt 0 - успешно, 1 - ошибка
 FUNCTION AddInudl_lmtsecuritest_exch RETURN INTEGER
 IS
  v_state           INTEGER;
 BEGIN

  BEGIN

   v_state := 0;

   DELETE udl_lmtsecuritest_exch_dbt;

   INSERT INTO udl_lmtsecuritest_exch_dbt( T_LIMIT_TYPE, T_FIRM_ID, T_SECCODE, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_LIMIT,
     T_TRDACCID, T_WA_POSITION_PRICE, T_LIMIT_KIND )
    SELECT 'DEPO',T_FIRM_ID, T_SECCODE, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, T_CURRENT_LIMIT, T_TRDACCID, T_WA_POSITION_PRICE, T_LIMIT_KIND
    FROM DDL_LIMITSECURITES_DBT
    WHERE T_MARKET = 1; -- могут добавиться условия!!!!!!!


   RETURN v_state;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;



 --добавим записи в буферную таблицу udl_lmtfuturmark_exch_dbt 0 - успешно, 1 - ошибка
 FUNCTION AddInudl_lmtfuturmark_exch RETURN INTEGER
 IS
  v_state           INTEGER;
 BEGIN

  BEGIN

   v_state := 0;

   DELETE udl_lmtfuturmark_exch_dbt;

   INSERT INTO udl_lmtfuturmark_exch_dbt( T_CLASS_CODE, T_ACCOUNT, T_VOLUMEMN, T_VOLUMEPL, T_KFL, T_KGO, T_USE_KGO, T_FIRM_ID, T_SECCODE )
    SELECT T_CLASS_CODE, T_ACCOUNT, T_VOLUMEMN, T_VOLUMEPL, T_KFL, T_KGO, T_USE_KGO, T_FIRM_ID, T_SECCODE
    FROM DDL_LIMITFUTURMARK_DBT
    WHERE 1 = 1; -- могут добавиться условия!!!!!!!


   RETURN v_state;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;



 --добавим записи в буферную таблицу udl_dl_lmtadjust_exch_dbt 0 - успешно, 1 - ошибка
 FUNCTION AddInudl_dl_lmtadjust_exch RETURN INTEGER
 IS
  v_state           INTEGER;
 BEGIN

  BEGIN

   v_state := 0;

   DELETE udl_dl_lmtadjust_exch_dbt;

   INSERT INTO udl_dl_lmtadjust_exch_dbt( T_LIMIT_TYPE, T_LIMIT_ID, T_FIRM_ID, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, /*T_CURRENT_BALANCE,*/ 
    T_CURRENT_LIMIT, T_LIMIT_OPERATION, T_TRDACCID, T_SECCODE, T_TAG, T_CURR_CODE, T_LIMIT_KIND, T_LEVERAGE /*, T_WA_POSITION_PRICE*/ )
     SELECT T_LIMIT_TYPE, T_LIMITID, T_FIRM_ID, T_CLIENT_CODE, T_OPEN_BALANCE, T_OPEN_LIMIT, /*,???!!!Соответствие отсутствует*/
      T_CURRENT_LIMIT, T_LIMIT_OPERATION, T_TRDACCID, T_SECCODE, T_TAG, T_CURR_CODE, T_LIMIT_KIND, T_LEVERAGE /*,???!!!Соответствие отсутствует*/
     FROM DDL_LIMITADJUST_DBT
     WHERE 1 = 1; -- могут добавиться условия!!!!!!!
     
      
   RETURN v_state;
   
  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;





 --добавим СПИ субъекта 0 - успешно, 1 - ошибка
 FUNCTION AddSfSiForParty( p_PartyId IN NUMBER, p_ServiceKind IN NUMBER, p_KindOper IN NUMBER, p_FiKind IN NUMBER, p_FiCode IN VARCHAR,
  p_Account IN VARCHAR, p_BankId IN NUMBER, p_BankDate IN DATE ) RETURN INTEGER
 IS
  v_CodeKindBank           dobjcode_dbt.T_CODEKIND%TYPE;
  v_CodeKindClient         dobjcode_dbt.T_CODEKIND%TYPE;
  v_ObjectType             dobjcode_dbt.T_OBJECTTYPE%TYPE;
  v_SetAccId               dsettacc_dbt.T_SETTACCID%TYPE;
  v_Order                  dsettacc_dbt.T_ORDER%TYPE;
 BEGIN

  BEGIN

  if  ( p_FiCode = '810' ) THEN 
     v_CodeKindBank := 3;
   else
     v_CodeKindBank := 6;
   end if;


   v_CodeKindClient := 1;
   v_ObjectType := 3;


   SELECT TO_CHAR( NVL( ( SELECT MAX( TO_NUMBER( T_ORDER ) ) + 1 
                          FROM dsettacc_dbt
                          WHERE T_PARTYID = p_PartyId
    ), 1) ) INTO v_Order FROM DUAL;

   INSERT INTO dsettacc_dbt( T_PARTYID, T_BANKID, T_FIID, T_CHAPTER, T_ACCOUNT, T_RECNAME, T_BANKCODEKIND, T_BANKCODE, T_BANKNAME, T_BANKCORRID,
    T_BANKCORRCODEKIND, T_BANKCORRCODE, T_BANKCORRNAME, T_FIKIND, T_BENEFICIARYID, T_CODEKIND, T_CODE, T_ORDER, T_SHORTNAME )
   VALUES( p_PartyId, p_BankId,
     ( SELECT T_FIID FROM dfininstr_dbt
       WHERE T_FI_KIND = p_FiKind
        AND T_FI_CODE = p_FiCode ),
     ( SELECT T_CHAPTER FROM dbalance_dbt B
       WHERE T_INUMPLAN = 0
        AND T_BALANCE = SUBSTR( p_Account, 1, 5 ) ),
     p_Account,
     ( SELECT T_NAME FROM dparty_dbt
       WHERE T_PARTYID = p_PartyId ),
     v_CodeKindBank,
     ( SELECT T_CODE FROM dobjcode_dbt
       WHERE  T_OBJECTTYPE = v_ObjectType
        AND T_CODEKIND = v_CodeKindBank
        AND T_OBJECTID = p_BankId
        AND T_STATE = 0
        AND T_BANKDATE <= p_BankDate ),
     ( SELECT T_NAME FROM dparty_dbt
       WHERE T_PARTYID = p_BankId ),
     ( SELECT T_OBJECTID FROM dobjcode_dbt
       WHERE T_OBJECTTYPE = v_ObjectType
        AND T_CODEKIND = v_CodeKindBank
        AND T_CODE = ( SELECT T_BIC_RCC FROM dbankdprt_dbt
                       WHERE T_PARTYID = p_BankId )
        AND T_STATE = 0 ),
     v_CodeKindBank,
     ( SELECT T_BIC_RCC FROM dbankdprt_dbt
       WHERE T_PARTYID = p_BankId ),
     ( SELECT A.T_NAME FROM dparty_dbt A
       WHERE A.T_PARTYID = ( SELECT B.T_OBJECTID FROM dobjcode_dbt B
                             WHERE B.T_OBJECTTYPE = v_ObjectType
                              AND B.T_CODEKIND = v_CodeKindBank
                              AND B.T_CODE = ( SELECT C.T_BIC_RCC FROM dbankdprt_dbt C
                                             WHERE C.T_PARTYID = p_BankId )
                              AND B.T_STATE = 0 ) ),
     p_FiKind, p_PartyId, v_CodeKindClient,
     ( SELECT T_CODE FROM dobjcode_dbt
       WHERE  T_OBJECTTYPE = v_ObjectType
        AND T_CODEKIND = v_CodeKindClient
        AND T_OBJECTID = p_PartyId
        AND T_STATE = 0
        AND T_BANKDATE <= p_BankDate ),
     /*'1'*/ v_Order, 'СПИ операции ' || TO_CHAR( p_KindOper ) )
    RETURNING T_SETTACCID INTO v_SetAccId;
    
   INSERT INTO dpmautoac_dbt( T_PARTYID, T_FIID, T_KINDOPER, T_PURPOSE, T_SETTACCID, T_FIKIND, T_SERVICEKIND, T_ORDER, T_ACCOUNT )
    SELECT p_PartyId,
     ( SELECT T_FIID FROM dfininstr_dbt
       WHERE T_FI_KIND = p_FiKind
        AND T_FI_CODE = p_FiCode ),
     p_KindOper, 0, v_SetAccId, p_FiKind, p_ServiceKind, /*'1'*/ v_Order, CHR(1)
    FROM DUAL;

   RETURN 0;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    IF v_SetAccId > 0 THEN

     ROLLBACK;

    END IF;

    RETURN 1;

   END;
  END;


 END;



 --получим счет из СПИ субъекта и PartyId банка из СПИ субъекта 0 - успешно, 1 - ошибка
 FUNCTION GetSfSiAccountAndBankPartyId( p_PartyId IN NUMBER, p_ServiceKind IN NUMBER, p_KindOper IN NUMBER, p_FiKind IN NUMBER, p_FiCode IN VARCHAR, p_Account IN VARCHAR, p_AccountResult OUT VARCHAR,
    p_BankId OUT NUMBER ) RETURN INTEGER
 IS
  v_PartyId          dparty_dbt.T_PARTYID%TYPE;
 BEGIN

  BEGIN

   SELECT T_ACCOUNT, T_BANKID INTO p_AccountResult, p_BankId FROM dsettacc_dbt sett1
   WHERE T_SETTACCID IN( SELECT A.T_SETTACCID FROM
                          (SELECT T_SETTACCID FROM dpmautoac_dbt
                          WHERE T_PARTYID = p_PartyId
                           AND T_SERVICEKIND = p_ServiceKind
                           AND T_KINDOPER = p_KindOper
                           AND T_FIID = (SELECT T_FIID FROM dfininstr_dbt B
                                         WHERE B.T_FI_KIND = p_FiKind
                                          AND T_FI_CODE = p_FiCode)
                           ORDER BY  T_ORDER) A)
    AND T_ACCOUNT = p_Account
    AND t_order = (select min(t_order) from dsettacc_dbt sett2 
                   where sett2.T_PARTYID = sett1.T_PARTYID 
                    and sett2.T_FIKIND = sett1.t_fikind and sett2.T_FIID = sett1.t_fiid and sett2.t_account = p_Account );

   RETURN 0;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    p_AccountResult := '';
    p_BankId := -1;
    RETURN 1;

   END;
  END;


 END;


 --получим Id субъект и счет ДО по номеру открытого ДО 0 - успешно, 1 - ошибка
 FUNCTION GetPropFromContrNum( p_LegalForm IN NUMBER, p_ContrNumber IN VARCHAR, p_ObjectType IN NUMBER, p_Account IN VARCHAR, p_ObjType IN NUMBER,
  p_FiKind IN NUMBER, p_FiCode IN VARCHAR, p_PartyId OUT NUMBER, p_ContrId OUT NUMBER, p_AccountContr OUT VARCHAR, p_ContrAccountId OUT NUMBER, p_ServKind OUT NUMBER ) RETURN INTEGER
 IS
  v_PartyId          dparty_dbt.T_PARTYID%TYPE;
 BEGIN

  BEGIN

   IF p_LegalForm = 1 THEN  --для юр.лиц

/*
    SELECT A.T_PARTYID, A.T_OBJECT, A.T_ID, A.T_ACCOUNTID INTO p_PartyId, p_AccountContr, p_ContrId, p_ContrAccountId
    FROM ( SELECT A.T_PARTYID, A.T_OBJECT, D.T_ID, F.T_ACCOUNTID
           FROM dsfcontr_dbt A, ddlcontr_dbt B, ddlcontrmp_dbt C, dsfcontr_dbt D, dsettacc_dbt E, daccount_dbt F, dsfssi_dbt G 
           WHERE A.T_NUMBER = p_ContrNumber
            AND A.T_OBJECTTYPE = p_ObjectType
            AND A.T_DATECLOSE = TO_DATE('01010001','ddmmyyyy')
            AND B.T_SFCONTRID = A.T_ID
            AND C.T_DLCONTRID = B.T_DLCONTRID
            AND D.T_ID = C.T_SFCONTRID
            AND E.T_ACCOUNT = p_Account
            AND E.T_SETTACCID = G.T_SETACCID
            AND G.T_OBJECTID = LPAD( D.T_ID, 10, '0')
            AND G.T_OBJECTTYPE = p_ObjType
            AND G.T_FIKIND = p_FiKind
            AND G.T_FIID = (SELECT H.T_FIID FROM dfininstr_dbt H    
                                     WHERE H.T_FI_KIND = G.T_FIKIND         
                                      AND H.T_FI_CODE = p_FiCode)             
            AND F.T_ACCOUNT = E.T_ACCOUNT                                                
            AND F.T_CHAPTER = E.T_CHAPTER                                                
            AND F.T_CODE_CURRENCY = E.T_FIID
            ORDER BY E.T_ORDER ) A
    WHERE ROWNUM = 1; 
*/

    SELECT /*D.T_ACCOUNT*/ C.T_PARTYID, C.T_OBJECT, C.T_ID, D.T_ACCOUNTID, C.T_SERVKIND INTO p_PartyId, p_AccountContr, p_ContrId, p_ContrAccountId, p_ServKind
    FROM dmcaccdoc_dbt A, dmccateg_dbt B, dsfcontr_dbt C, daccount_dbt D
    WHERE A.T_ACCOUNT = p_Account
     AND A.T_ISCOMMON = 'X'                                         
     AND B.T_ID = A.T_CATID                                         
     AND B.T_CODE = 'ДС клиента, ц/б'                              
      AND SUBSTR(A.T_ACCOUNT, 1, 3) = '306'                         
     AND C.T_ID = A.T_CLIENTCONTRID                                 
     AND D.T_ACCOUNT = A.T_ACCOUNT                                                
     AND D.T_CHAPTER = A.T_CHAPTER                                                
     AND D.T_CODE_CURRENCY = A.T_CURRENCY;


   ELSE --для физ.лиц

    SELECT A.T_PARTYID, A.T_OBJECT, D.T_ID, F.T_ACCOUNTID, D.T_SERVKIND INTO p_PartyId, p_AccountContr, p_ContrId, p_ContrAccountId, p_ServKind
    FROM dbrokacc_dbt H, dsfcontr_dbt A, ddlcontr_dbt B, ddlcontrmp_dbt C, dsfcontr_dbt D, dsfssi_dbt G, dsettacc_dbt E, daccount_dbt F  
    WHERE H.T_ACCOUNT = p_Account
     AND A.T_NUMBER = p_ContrNumber
     AND A.T_OBJECTTYPE = p_ObjectType
     AND A.T_DATECLOSE = TO_DATE('01010001','ddmmyyyy')
     AND B.T_SFCONTRID = A.T_ID
     AND C.T_DLCONTRID = B.T_DLCONTRID
     AND D.T_ID = C.T_SFCONTRID
     AND D.T_SERVKIND = case when H.T_SERVKIND = 0 then 1 else  H.T_SERVKIND end
     AND D.T_SERVKINDSUB = case when H.T_SERVKINDSUB = 0 then 8 else H.T_SERVKINDSUB end
     AND G.T_OBJECTID = LPAD( D.T_ID, 10, '0')
     AND G.T_OBJECTTYPE = p_ObjType
     AND G.T_FIKIND = p_FiKind
     AND G.T_FIID = H.T_CURRENCY
     AND E.T_SETTACCID = G.T_SETACCID
     AND F.T_ACCOUNT = E.T_ACCOUNT                                                
     AND F.T_CHAPTER = E.T_CHAPTER                                                
     AND F.T_CODE_CURRENCY = E.T_FIID
     AND G.T_ORDER = ( SELECT MIN(I.T_ORDER)
                       FROM dsfssi_dbt I
                       WHERE I.T_OBJECTID = G.T_OBJECTID
                        AND I.T_OBJECTTYPE = G.T_OBJECTTYPE
                        AND I.T_FIKIND = G.T_FIKIND
                        AND I.T_FIID = G.T_FIID)
    ORDER BY C.T_MARKETID
     FETCH FIRST 1 ROWS ONLY;
     
   END IF;
 
   RETURN 0;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;



 --вставка CLOB-XML в лог 0 - успешно, 1 - ошибка
 FUNCTION AddRecXMLToLOG( p_Cnum IN NUMBER ) RETURN INTEGER
 IS
  v_Cnt          INTEGER;

 BEGIN

  BEGIN

   v_Cnt := 0;

   SELECT COUNT(1) INTO v_Cnt FROM uclientRegMB_Log_dbt A
   WHERE A.T_SESSIONID = p_Cnum
    AND A.T_FILENAME = ( SELECT B.T_FILENAME
                         FROM uclientRegMB_LogTmp_dbt B
                         WHERE B.T_SESSIONID = p_Cnum
                          AND ROWNUM = 1 );

   IF v_Cnt > 0 THEN

    DELETE uclientRegMB_Log_dbt A
    WHERE A.T_SESSIONID = p_Cnum
     AND A.T_FILENAME = ( SELECT B.T_FILENAME
                          FROM uclientRegMB_LogTmp_dbt B
                          WHERE B.T_SESSIONID = p_Cnum
                           AND ROWNUM = 1 );

   END IF;


   INSERT INTO uclientRegMB_Log_dbt( T_SESSIONID, T_FILENAME, T_STATUS, T_OPER, T_XML_MESS)
    (SELECT p_Cnum AS T_SESSIONID, T_FILENAME, 0 AS T_STATUS,  T_OPER, T_XML_MESS
     FROM uclientRegMB_LogTmp_dbt
     WHERE T_SESSIONID = p_Cnum
      AND ROWNUM = 1);

   DELETE uclientRegMB_LogTmp_dbt WHERE T_SESSIONID = p_Cnum;

   RETURN 0;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    RETURN 1;

   END;
  END;


 END;



 --обработать загруженный CLOB-XML регистрация клиентов на МБ 0 - успешно, 1 - ошибка
 FUNCTION ProcwssObjAttrib(p_ObjectType IN NUMBER, p_CodeKind IN NUMBER, p_GroupId IN NUMBER, p_SessionId IN NUMBER,
  p_FileName IN VARCHAR, p_Oper IN NUMBER) RETURN INTEGER
 IS

  v_Cnt          INTEGER;
  v_CntValid     INTEGER;
  v_RecID        uclientRegMB_Log_dbt.T_RECID%TYPE;
  v_Status       uclientRegMB_Log_dbt.T_STATUS%TYPE;
  v_ErrCode      uclientRegMB_Log_dbt.T_ERRCODE%TYPE;
  v_ErrText      uclientRegMB_Log_dbt.T_ERRTEXT%TYPE;

 BEGIN

  BEGIN

   v_Cnt := 0;
   v_CntValid := 0;
   v_RecID := -1;
   v_Status := 0;
   v_ErrCode := 0;
   v_ErrText := '';

   FOR clnt_regMB_XML_rec IN (
    SELECT A.CLIENT_CODE, A.MARKETID, A.RESCODE, A.RESMESSAGE, 
     (SELECT B.T_OBJECTID FROM dobjcode_dbt B
      WHERE B.T_OBJECTTYPE  = p_ObjectType
       AND B.T_CODEKIND = p_CodeKind
       AND B.T_CODE = A.CLIENT_CODE 
       AND T_STATE = 0
       AND  ROWNUM = 1 ) AS PARTYID,
     A.T_RECID
    FROM
     (WITH XML_MESS AS ( SELECT T_RECID, XMLTYPE(T_XML_MESS) AS MESS 
                        FROM  uclientRegMB_Log_dbt
                        WHERE T_SESSIONID = p_SessionId
                         AND T_FILENAME = p_FileName
                         AND T_STATUS = 0)
      SELECT MEMBER.COLUMN_VALUE.EXTRACT ('//CLIENT_CODE/CLIENT_MARKETS/@ClientCode').getStringVal() AS CLIENT_CODE,
       MEMBER.COLUMN_VALUE.EXTRACT ('//CLIENT_CODE/CLIENT_MARKETS/@MarketId').getStringVal() AS MARKETID,
       MEMBER.COLUMN_VALUE.EXTRACT ('//CLIENT_CODE/CLIENT_MARKETS/@ResCode').getStringVal() AS RESCODE,
       MEMBER.COLUMN_VALUE.EXTRACT ('//CLIENT_CODE/CLIENT_MARKETS/@ResMessage').getStringVal() AS RESMESSAGE,
       T_RECID
      FROM XML_MESS XML_MESS_TBL, TABLE( XMLSEQUENCE( XML_MESS_TBL.MESS.EXTRACT( 'MICEX_DOC/CLIENTS/CLIENT' ) ) ) MEMBER) A )
   LOOP

    BEGIN
     v_RecID := clnt_regMB_XML_rec.T_RECID;

     IF clnt_regMB_XML_rec.PARTYID IS NOT NULL THEN
   
      RSI_RSB_CATEGORY.SetPartyAtCor( clnt_regMB_XML_rec.PARTYID, p_GroupId, ( CASE WHEN clnt_regMB_XML_rec.RESCODE = '0' THEN 1 ELSE 2 END ), p_Oper );

      IF clnt_regMB_XML_rec.RESCODE <> '0' THEN
       --субъект не зарегистрирован с clnt_regMB_XML_rec.CLIENT_CODE в ErrText код возврата -3
       --накапливаем данные, по которым не установлена категория, сваливаем все это в ErrText
 
       v_ErrText := v_ErrText || '/n' || 'Файл ' || p_FileName || ' Отрицательный результат регистрации субъекта с кодом ' ||
        clnt_regMB_XML_rec.CLIENT_CODE || ', Id ' || clnt_regMB_XML_rec.PARTYID;
       v_ErrCode := -3; --отрицательный результат регистрации субъекта

      END IF;

      v_CntValid := v_CntValid + 1;

     ELSE
      --не найден субъект с clnt_regMB_XML_rec.CLIENT_CODE в ErrText код возврата -1
      --накапливаем данные, по которым не установлена категория, сваливаем все это в ErrText
      v_ErrText := v_ErrText || '/n' || 'Файл ' || p_FileName || ' Не найден субъект с кодом ' || clnt_regMB_XML_rec.CLIENT_CODE;
      v_ErrCode := -1; --не все записи файла обработаны
      
     END IF;


    EXCEPTION
     WHEN OTHERS THEN
     BEGIN
      --не установлена категория субъекта с кодом clnt_regMB_XML_rec.CLIENT_CODE, clnt_regMB_XML_rec.PARTYID в ErrText код возврата -1
      --накапливаем данные, по которым не установлена категория, сваливаем все это в ErrText
      v_ErrText := v_ErrText || '/n' || 'Файл ' || p_FileName || ' Не установлена категория субъекта с кодом ' || clnt_regMB_XML_rec.CLIENT_CODE ||
         ', Id ' || clnt_regMB_XML_rec.PARTYID;
      v_ErrCode := -1; --не все записи файла обработаны

     END;
    END;

    v_Cnt := v_Cnt + 1;

   END LOOP;

   v_Status := 1;


   IF v_Cnt = 0 THEN

    v_ErrText := v_ErrText || '/n' || 'Файл ' || p_FileName || ' Не обработано ни одной записи';
    v_ErrCode := -2; --записи файла не обработаны
    v_Status := 2;

   ELSE

    v_ErrText := v_ErrText || '/n' || 'Файл ' || p_FileName || ' Записей ' || TO_CHAR(v_Cnt) || ' Обработано записей успешно ' || TO_CHAR(v_CntValid) ||
     ' Обработано записей с ошибкой ' || TO_CHAR(v_Cnt - v_CntValid);

   END IF;


   --пишем ErrText!!!!!!!!!!!!
   UPDATE uclientRegMB_Log_dbt
   SET T_ERRTEXT = v_ErrText, T_STATUS = v_Status, T_ERRCODE = v_ErrCode
   WHERE T_RECID = v_RecID;


   RETURN 0;

  EXCEPTION
   WHEN OTHERS THEN
   BEGIN

    UPDATE uclientRegMB_Log_dbt
    SET T_ERRTEXT = v_ErrText, T_STATUS = 2, T_ERRCODE = -1000
    WHERE T_RECID = v_RecID;

    RETURN 1;

   END;
  END;


 END;


    /* Функция вставки задания в очередь для пользовательских объектов */
    FUNCTION InsertSequenceJobUsrObj(p_object_id IN NUMBER,
                                     p_object_type IN NUMBER,
                                     p_process_type IN NUMBER,
                                     p_obj_type_code IN VARCHAR2) RETURN INTEGER
    IS

        v_ret   NUMBER(5) := 0;
        v_q_ins LONG;

    BEGIN

        v_q_ins := 'INSERT INTO dfuncobj_dbt( T_OBJECTTYPE, T_OBJECTID, T_FUNCID, T_PARAM ) ';
        v_q_ins := v_q_ins || ' (SELECT B.T_ELEMENT, :p_ObjectId_1, A.T_FUNCID, ';
        v_q_ins := v_q_ins || '         TO_CHAR(:p_ObjectId_2) || '';'' || TO_CHAR(:p_ObjectType) || '';'' || TO_CHAR(:p_ProcessType) ';
        v_q_ins := v_q_ins || '    FROM dfunc_dbt A, dllvalues_dbt B ';
        v_q_ins := v_q_ins || '   WHERE A.T_FUNCID = B.T_FLAG ';
        v_q_ins := v_q_ins || '     AND B.T_LIST = USR_PKG_IMPORT_SOFR.GetOBJECT_SYNCH ';
        v_q_ins := v_q_ins || '     AND B.T_CODE = :p_ObjectTypeCode ';
        v_q_ins := v_q_ins || '     AND B.T_ELEMENT >= 5001) ';

        EXECUTE IMMEDIATE v_q_ins USING IN p_object_id, IN p_object_id, IN p_object_type, IN p_process_type, IN p_obj_type_code;

        RETURN v_ret;

    EXCEPTION
        WHEN OTHERS
        THEN
            v_ret := 1;
            RETURN v_ret;
    END;


    /**
     @brief    		Функция для обновления статуса процесса.
     @param[in]    	p_RecID    		ID записи
     @param[in]    	p_Status    		статус, который нужно установить
     @param[in]    	p_CheckConcurent	флаг проверки конкурентов (0 -- не проверять, 1 -- проверять)
     @param[in]    	p_TblName    		имя таблицы (utableprocessevent_dbt, utableprocessout_dbt, utableprocessin_dbt)
     @return                            	0 -- обновлено успешно, 1 -- не обновлено, -1 -- нет записи
    */
    FUNCTION UpdateProcess( p_RecID IN NUMBER, p_Status IN NUMBER, p_CheckConcurent IN NUMBER, p_TblName IN VARCHAR2 ) RETURN number
    IS
      x_Sql VARCHAR2(2000);
      x_Ret NUMBER := 1;      -- по-умолчанию, не обновлено
      x_ObjType NUMBER;
      pragma autonomous_transaction;
    BEGIN
     -- поиск записи
     BEGIN
       x_Sql := 'SELECT t_objecttype FROM '||p_TblName||' WHERE t_recid = :R';
       EXECUTE IMMEDIATE x_Sql INTO x_ObjType USING p_RecID;
     EXCEPTION WHEN others THEN
       RETURN -1;
     END;

     IF(p_CheckConcurent = 0) THEN
        -- без проверки конкурентов
        x_Sql := 'UPDATE '||p_TblName||' r SET t_status = :S WHERE r.t_recid = :R ';
        EXECUTE IMMEDIATE x_Sql USING p_Status, p_RecID;
     ELSE
        -- с проверкой конкурентов
        x_Sql := 'UPDATE '||p_TblName||' r SET t_status = :S WHERE r.t_recid = :R AND NOT EXISTS '
                 ||'(SELECT 1 FROM '||p_TblName||' WHERE t_objecttype = :O and t_status = :S and t_recid != :R)'
        ;
        EXECUTE IMMEDIATE x_Sql USING p_Status, p_RecID, x_ObjType, p_Status, p_RecID;
     END IF;
     IF( SQL%ROWCOUNT = 1 ) THEN
       x_Ret := 0;  -- успешное обновление, конкурентов нет
     END IF;
     COMMIT;
     RETURN x_Ret;
    EXCEPTION
      WHEN others THEN
        ROLLBACK;
        RETURN x_Ret;
    END;

    /**
     @brief    		Функция для запуска процесса.
     @param[in]    	p_RecID    		ID записи
     @param[in]    	p_TblName    		имя таблицы (utableprocessevent_dbt, utableprocessout_dbt, utableprocessin_dbt)
     @param[in]    	p_SleepTime    		кол-во секунд, на которые процесс засыпает при наличии конкурентов
     @param[in]    	p_TimeOut    		тайм-аут, по истечении которого процесс останавливается
     @return                            	0 -- запуск произведен успешно, 1 -- ошибка запуска, -1 -- завершение по тайм-ауту
    */
    FUNCTION StartProcess( p_RecID IN NUMBER, p_TblName IN VARCHAR2, p_SleepTime IN number, p_TimeOut IN NUMBER ) RETURN number
    IS
      x_Sql VARCHAR2(2000);
      x_Ret NUMBER := 1;      -- по-умолчанию, не обновлено
      x_Started NUMBER;
      x_Stop number := 0;
      x_Delay number := 0;
    BEGIN
      WHILE (x_Stop = 0) LOOP
        x_Started := USR_PKG_IMPORT_SOFR.UpdateProcess( p_RecID, 3, 1, p_TblName );
        IF(x_Started = 0) THEN
          x_Ret := 0; 		-- все хорошо, процесс запущен,
          x_Stop := 1;          -- останавливаемся
        ELSIF(x_Started = -1) THEN
          x_Ret := 1; 		-- нет записи, 
          x_Stop := 1;          -- останавливаемся
        ELSIF(x_Delay > p_TimeOut) THEN
          x_Ret := -1; 		-- завершение по тайм-ауту,
          x_Stop := 1;          -- останавливаемся
        ELSE
          -- ожидаем завершения конкурирующего процесса
          x_Delay := x_Delay + p_SleepTime;
          dbms_lock.sleep(p_SleepTime);
        END IF;
      END LOOP;
      RETURN x_Ret;
    EXCEPTION
      WHEN others THEN
        RETURN x_Ret;
    END;

    /* Функция обновления статуса записи в указанной таблице (utableprocessevent_dbt; utableprocessout_dbt, utableprocessin_dbt) */
    FUNCTION UpdateEvntProcessTable(p_recid IN NUMBER, p_status IN NUMBER, p_tbl_name IN VARCHAR2) RETURN INTEGER
    IS

        v_ret   NUMBER(5) := 0;
        v_q_upd LONG;

    BEGIN

        v_q_upd :=           'UPDATE ' || p_tbl_name;
        v_q_upd := v_q_upd || '  SET t_status = :p_status ';
        v_q_upd := v_q_upd || 'WHERE t_recid = :p_recid ';

        EXECUTE IMMEDIATE v_q_upd USING IN p_status, IN p_recid;

        RETURN v_ret;

    EXCEPTION
        WHEN OTHERS
        THEN
            v_ret := 1;
            RETURN v_ret;
    END;


    /* Функция для начала обработки процесса с созданием задачи в dfuncobj_dbt */
    FUNCTION MakeStartProcessWTask(p_recid IN NUMBER, p_status IN NUMBER, p_type IN VARCHAR2, p_tbl_name IN VARCHAR2) RETURN INTEGER
    IS

        v_ret   NUMBER(5) := 0;
        v_q_sel LONG;

        v_recid NUMBER(10);
        v_objecttype NUMBER(10);
        v_status NUMBER(10);
        v_obj_type_code dllvalues_dbt.t_code%TYPE;

    BEGIN

        v_ret := UpdateEvntProcessTable(p_recid, p_status, p_tbl_name);

        v_q_sel :=            'SELECT tbl_proc.t_recid, tbl_proc.t_objecttype, tbl_proc.t_status, ';
        v_q_sel := v_q_sel || '       (SELECT tbl_llval.t_code FROM dllvalues_dbt tbl_llval ';
        v_q_sel := v_q_sel || '         WHERE tbl_llval.t_list = USR_PKG_IMPORT_SOFR.GetOBJECT_SYNCH ';
        v_q_sel := v_q_sel || '           AND tbl_llval.t_code = TO_CHAR( tbl_proc.t_objecttype ) ) AS t_code ';
        v_q_sel := v_q_sel || '  FROM ' || p_tbl_name || ' tbl_proc ';
        v_q_sel := v_q_sel || ' WHERE tbl_proc.t_recid = :p_recid ';

        EXECUTE IMMEDIATE v_q_sel INTO v_recid, v_objecttype, v_status, v_obj_type_code USING IN p_recid;

        IF v_ret = 0
        THEN
            v_ret := InsertSequenceJobUsrObj(v_recid, v_objecttype, p_type, v_obj_type_code);
        END IF;

        IF v_ret = 0
        THEN
            COMMIT;
        ELSE
            ROLLBACK;
        END IF;

        RETURN v_ret;

    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            v_ret := 1;
            RETURN v_ret;
    END;



    FUNCTION ImportOneCourse_g(p_BaseFIID      IN dratedef_dbt.T_OTHERFI%TYPE,
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
            WHERE Rd.t_Fiid = p_BaseFIID
              AND Rd.t_Otherfi = p_OtherFIID
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
           IF (p_SinceDate > Ratedef_Rec.t_Sincedate) THEN
                -- сохранение текущего значения
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
                           ,Ratedef_Rec.t_IsManualInput);
                END IF;
              -- обновление текущего значения         
              UPDATE Dratedef_Dbt Rd
                 SET Rd.t_Rate   = p_Rate
                    ,Rd.t_Scale    = p_Scale
                    ,Rd.t_Point     = p_Point
                    ,Rd.t_Inputdate = Trunc(SYSDATE)
                    ,Rd.t_Inputtime = To_Date('01010001' ||
                                              To_Char(SYSDATE, 'HH24MISS')
                                             ,'DDMMYYYYHH24MISS')
                    ,Rd.t_Oper      = p_Oper
                    ,Rd.t_Sincedate = p_SinceDate
                    ,Rd.t_IsManualInput = CHR(0)
               WHERE Rd.t_rateid = Ratedef_Rec.t_rateid;
                                        
           ELSIF (p_SinceDate = Ratedef_Rec.t_Sincedate) THEN
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
               WHERE Rd.t_rateid = Ratedef_Rec.t_rateid;          
           ELSE
                SELECT COUNT(*)
                   INTO Cnt
                   FROM Dratehist_Dbt
                 WHERE t_Rateid = Ratedef_Rec.t_Rateid
                     AND t_Sincedate = p_SinceDate;
                 IF (Cnt > 0) THEN
                   UPDATE Dratehist_Dbt
                        SET t_Rate      = p_Rate
                              ,t_Scale    = p_Scale
                              ,t_Point     = p_Point
                              ,t_Inputdate = Trunc(SYSDATE)
                              ,t_Inputtime = To_Date('01010001' ||
                                              To_Char(SYSDATE, 'HH24MISS')
                                             ,'DDMMYYYYHH24MISS')
                              ,t_Oper      = p_Oper
                              ,t_IsManualInput = CHR(0)
                    WHERE t_Rateid = Ratedef_Rec.t_Rateid
                        AND t_Sincedate = p_SinceDate;
                 ELSE
                    INSERT INTO Dratehist_Dbt
                    VALUES
                       (Ratedef_Rec.t_Rateid
                       ,Ratedef_Rec.t_Isinverse
                       ,p_Rate
                       ,p_Scale
                       ,p_Point
                       ,Trunc(SYSDATE)
                       ,To_Date('01010001' ||
                                              To_Char(SYSDATE, 'HH24MISS')
                                             ,'DDMMYYYYHH24MISS')
                       ,p_Oper
                       ,p_SinceDate
                       ,CHR(0));
                 END IF;
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
              ,p_BaseFIID
              ,p_OtherFIID
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

    END ImportOneCourse_g;

    
/*----------------------------*/
/* Функция генерации СМС-кода */
/*----------------------------*/
    FUNCTION f_make_sms_code(p_type IN NUMBER,
                             p_client_id IN NUMBER,
                             p_num_of_try IN NUMBER,
                             p_sms_code OUT VARCHAR2) RETURN NUMBER
    IS

        v_stat_is_active  NUMBER := 1; /* Статус "Код активен" */
        v_init_accept_try NUMBER := 0; /* Начальное значение количества подтверждения */

        v_init_val NUMBER;
        v_sms_code NUMBER;
        v_code_id  NUMBER;
        v_err_flag NUMBER; /* 1 - продолжаем работу цикла; -1 - выход из цикла по ошибке */
        v_err_cntr NUMBER; /* Счетчик количества итераций цикла */
 
    BEGIN

        SELECT TO_NUMBER(TO_CHAR(SYSTIMESTAMP, 'HH24MISSFF2')) INTO v_init_val FROM DUAL;
        
        DBMS_RANDOM.INITIALIZE(v_init_val);
        
        v_err_flag := 1;
        v_err_cntr := 0;

        WHILE v_err_flag = 1
        LOOP
            BEGIN
                v_sms_code := ROUND(dbms_random.value(100000, 999999));

                INSERT INTO usr_clnt_sms_code_dbt
                       (t_id, t_type, t_client, t_sms_code, t_date, t_status, t_create_ts, t_accept_try)
                VALUES (0, p_type, p_client_id, v_sms_code, TRUNC(SYSDATE), v_stat_is_active, SYSTIMESTAMP, v_init_accept_try)
                RETURNING t_id
                INTO v_code_id;
                
                v_err_flag := 0;

            EXCEPTION
                -- Проверка уникальности кода будет работать если на таблице usr_clnt_sms_code_dbt есть уникальный индекс по T_TYPE, T_DATE, T_SMS_CODE
                WHEN DUP_VAL_ON_INDEX
                THEN
                    IF v_err_cntr < p_num_of_try
                    THEN
                        v_err_flag := 1;
                        v_err_cntr := v_err_cntr + 1;
                    ELSE
                        v_err_flag := -1;
                        v_code_id := 0;
                    END IF;
                WHEN OTHERS
                THEN
                    v_err_flag := -1;
                    v_code_id := 0;
            END;
        END LOOP;
        
        DBMS_RANDOM.TERMINATE;
        
        p_sms_code := TO_CHAR(v_sms_code);

        /*-------------------------------------------------------------*/
        /* ВНИМАНИЕ! Будут затронуты ВСЕ изменения, сделанные до этого */
        /*-------------------------------------------------------------*/
        IF v_err_flag = 0
        THEN
            COMMIT;
        ELSE
            ROLLBACK;
        END IF;

        RETURN v_code_id;

    END f_make_sms_code;


/*---------------------------------------------------------------------------*/
/* Функция генерации СМС-кода с обеспечением его исключительности по статусу */
/*---------------------------------------------------------------------------*/
    FUNCTION f_make_only_one_code(p_type IN NUMBER,
                                  p_client_id IN NUMBER,
                                  p_num_of_try IN NUMBER,
                                  p_sms_code OUT VARCHAR2) RETURN NUMBER
    IS
        v_stat_is_active  NUMBER := 1; /* Статус "Код активен" */
        v_stat_not_active NUMBER := 3; /* Статус "Код не активен" */
        v_stat_was_sent   NUMBER := 6; /* Статус "Код отправлен" */

        v_sms_code NUMBER;
        v_code_id  NUMBER;
    BEGIN
        BEGIN
            /*  Делаем не активными коды со статусами "Код активен" и "Код отправлен" */
            /* (действующий код заданного типа по заданному клиенту будет только один) */
            /* Уточнение: */
            /*  По информации от Банка единомоментно должен быть только один действующий */
            /* код для заданного клиента (вне зависимости от типа кода) */
            UPDATE usr_clnt_sms_code_dbt
               SET t_status = v_stat_not_active
--             WHERE t_type = p_type /* см. Уточнение выше */
             WHERE t_client = p_client_id
               AND t_status IN (v_stat_is_active, v_stat_was_sent);

            /* COMMIT и ROLLBACK внутри */
            v_code_id := f_make_sms_code(p_type, p_client_id, p_num_of_try, v_sms_code);

            p_sms_code := v_sms_code;

        EXCEPTION
            WHEN OTHERS
            THEN
                v_code_id := 0;
        END;

        IF v_code_id = 0
        THEN
            ROLLBACK;
        END IF;

        RETURN v_code_id;

    END f_make_only_one_code;


/*------------------------------------------------------*/
/* Функция сохранения кодового слова в буферной таблице */
/*------------------------------------------------------*/
    FUNCTION f_save_code_word_buf(p_cw_client IN NUMBER,
                                  p_code_word IN VARCHAR2,
                                  p_code_type IN NUMBER,
                                  p_code_stat IN NUMBER,
                                  p_ret_txt OUT VARCHAR2) RETURN NUMBER
    IS
        v_ret     NUMBER(10) := 0;
        v_ret_txt VARCHAR2(1900 Char) := 'OK';
        /* Кодовое слово для Брокерского обслуживания */
        v_cw_proc_type  usr_clnt_cdwrd_buf_dbt.t_cw_proc_type%TYPE;
        /* Запись вставляется со статусом "Поступило из неавторизованной зоны ДБО ФЛ (новое)" */
        v_cw_accept_try usr_clnt_cdwrd_buf_dbt.t_cw_accept_try%TYPE := 0;
    BEGIN
        BEGIN
            /* Определяем было ли до этого зарегистрировано кодовое слово для БО у Клиента */
            SELECT DECODE(COUNT(*), 0, 1, 2) INTO v_cw_proc_type
              FROM usr_clnt_cdwrd_hist_dbt
             WHERE t_cw_type = p_code_type
               AND t_cw_client = p_cw_client;

            /* В Буферной таблице храним только одну запись по Типу кодового слова (t_cw_type) */
            DELETE FROM usr_clnt_cdwrd_buf_dbt
             WHERE t_cw_type = p_code_type AND t_cw_client = p_cw_client;

            /* Сохраняем кодовое слово в буферной таблице */
            INSERT INTO usr_clnt_cdwrd_buf_dbt
                   (T_CW_TYPE, T_CW_CLIENT, T_CW_PROC_TYPE, T_CODE_WORD,
                    T_CW_STATUS, T_CW_CREATE_TS, T_CW_ACCEPT_TRY)
            VALUES (p_code_type, p_cw_client, v_cw_proc_type, p_code_word,
                    p_code_stat, SYSTIMESTAMP, v_cw_accept_try);
            COMMIT;

        EXCEPTION
            WHEN OTHERS
            THEN
                v_ret := SQLCODE;
                v_ret_txt := SUBSTR(SQLERRM, 1, 1500);
                ROLLBACK;
        END;

        p_ret_txt := v_ret_txt;

        RETURN v_ret;

    END f_save_code_word_buf;


/*--------------------------------------------------------------------------------------------------------*/
/* Функция переноса кодового слова из буферной таблицы в таблицу текущих значений (с изменением статусов) */
/*--------------------------------------------------------------------------------------------------------*/
    FUNCTION f_save_accepted_code_word(p_cw_type IN NUMBER,
                                       p_client IN NUMBER,
                                       p_stat_cw_chng IN NUMBER,
                                       p_stat_cw_accepted IN NUMBER,
                                       p_ret_txt OUT VARCHAR2) RETURN NUMBER
    IS
        v_ret     NUMBER(10) := 0;
        v_ret_txt VARCHAR2(1900 Char) := 'OK';
        v_def_id  usr_clnt_cdwrd_hist_dbt.t_id%TYPE := 0;
        v_cw_prev usr_clnt_cdwrd_hist_dbt.t_cw_prev%TYPE := NULL;
        v_cw_not_active usr_clnt_cdwrd_hist_dbt.t_cw_is_active%TYPE := chr(0);

    BEGIN
        BEGIN
            /* Поиск предыдущего Кодового слова */
                     SELECT cw_1.t_id
                       INTO v_cw_prev
                       FROM usr_clnt_cdwrd_hist_dbt cw_1
            LEFT OUTER JOIN usr_clnt_cdwrd_hist_dbt cw_2 ON cw_2.t_cw_client = cw_1.t_cw_client
                                                        AND cw_2.t_cw_type = cw_1.t_cw_type
                                                        AND cw_2.t_cw_create_ts > cw_1.t_cw_create_ts
                      WHERE cw_1.t_cw_client = p_client
                        AND cw_1.t_cw_type = p_cw_type
                        AND cw_1.t_cw_status = p_stat_cw_accepted
                        AND cw_2.t_cw_create_ts IS NULL;

            /* Обновляем статус старой записи (если она есть) */
            /* В таблице должна быть только одна запись с действующим кодом */
            UPDATE usr_clnt_cdwrd_hist_dbt
               SET t_cw_status = p_stat_cw_chng,
                   t_cw_is_active = v_cw_not_active
             WHERE t_cw_client = p_client
               AND t_cw_type = p_cw_type
               AND t_cw_status = p_stat_cw_accepted;

        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                v_ret := 0; /* Все нормально - продолжаем */

            WHEN OTHERS
            THEN
                v_ret := SQLCODE;
                v_ret_txt := SUBSTR(SQLERRM, 1, 1500);
                ROLLBACK;
        END;

        BEGIN
            IF v_ret = 0
            THEN
                /* Переносим новую запись из буферной таблицы */
                INSERT INTO usr_clnt_cdwrd_hist_dbt
                       (T_ID, T_CW_PREV, T_CW_TYPE, T_CW_CLIENT,
                        T_CW_PROC_TYPE, T_CODE_WORD, T_CW_STATUS,
                        T_CW_CREATE_TS, T_CW_ACCEPT_TS, T_CW_ACCEPT_TRY)
                SELECT v_def_id, v_cw_prev, t_cw_type, t_cw_client,
                       t_cw_proc_type, t_code_word, p_stat_cw_accepted,
                       t_cw_create_ts, SYSTIMESTAMP, (t_cw_accept_try + 1)
                  FROM usr_clnt_cdwrd_buf_dbt
                 WHERE t_cw_type = p_cw_type
                   AND t_cw_client = p_client;

                /* Удаляем запись из буферной таблицы */
                DELETE FROM usr_clnt_cdwrd_buf_dbt
                 WHERE t_cw_type = p_cw_type
                   AND t_cw_client = p_client;

                COMMIT;
            END IF;

        EXCEPTION
            WHEN OTHERS
            THEN
                v_ret := SQLCODE;
                v_ret_txt := SUBSTR(SQLERRM, 1, 1500);
                ROLLBACK;
        END;

        p_ret_txt := v_ret_txt;

        RETURN v_ret;
    
    END f_save_accepted_code_word;


/*-----------------------------------*/
/* Функция блокировки кодового слова */
/*-----------------------------------*/
    FUNCTION f_block_code_word(p_cw_id IN NUMBER,
                               p_change_oper IN NUMBER) RETURN NUMBER
    IS
        v_ret           NUMBER(10) := 0;
        v_cw_not_active usr_clnt_cdwrd_hist_dbt.t_cw_is_active%TYPE := chr(0);
        v_cw_is_active  usr_clnt_cdwrd_hist_dbt.t_cw_is_active%TYPE;
        v_max_id        usr_clnt_cdwrd_hist_dbt.t_id%TYPE;
    BEGIN
        BEGIN
            WITH to_block AS (SELECT * FROM usr_clnt_cdwrd_hist_dbt
                              WHERE t_id = p_cw_id)
                     SELECT to_block.t_cw_is_active, cw_1.t_id
                       INTO v_cw_is_active, v_max_id
                       FROM to_block
                  LEFT JOIN usr_clnt_cdwrd_hist_dbt cw_1 ON cw_1.t_cw_client = to_block.t_cw_client
                                                        AND cw_1.t_cw_type = to_block.t_cw_type
            LEFT OUTER JOIN usr_clnt_cdwrd_hist_dbt cw_2 ON cw_2.t_cw_client = cw_1.t_cw_client
                                                        AND cw_2.t_cw_type = cw_1.t_cw_type
                                                        AND cw_2.t_cw_create_ts > cw_1.t_cw_create_ts
                      WHERE cw_2.t_cw_create_ts IS NULL;

            /* Проверка на is_active */
            IF v_cw_is_active != chr(88)
            THEN
                v_ret := 1;
            END IF;

            /* Проверка является ли блокируемая запись самой актуальной */
            IF ((v_ret = 0) AND (p_cw_id != v_max_id))
            THEN
                v_ret := 2;
            END IF;

            /* Обновление is_active */
            IF v_ret = 0
            THEN
                UPDATE usr_clnt_cdwrd_hist_dbt
                   SET t_cw_is_active = v_cw_not_active,
                       t_change_oper = p_change_oper,
                       t_oper_chng_ts = SYSTIMESTAMP
                 WHERE t_id = p_cw_id;

                COMMIT;
            END IF;

        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                v_ret := 3;

            WHEN OTHERS
            THEN
                v_ret := 4;
        END;

        RETURN v_ret;

    END f_block_code_word;


/*-----------------------------------*/
/* Функция добавления кодового слова */
/*-----------------------------------*/
    FUNCTION f_add_code_word(p_cw_type IN NUMBER,
                             p_cw_client IN NUMBER,
                             p_code_word IN VARCHAR2,
                             p_man_oper IN NUMBER) RETURN NUMBER
    IS
        v_ret              NUMBER(10) := 0;
        v_def_id           usr_clnt_cdwrd_hist_dbt.t_id%TYPE := 0;
        v_cw_prev          usr_clnt_cdwrd_hist_dbt.t_cw_prev%TYPE := NULL;
        v_cw_not_active    usr_clnt_cdwrd_hist_dbt.t_cw_is_active%TYPE := chr(0);
        v_stat_cw_accepted usr_clnt_cdwrd_hist_dbt.t_cw_status%TYPE := 4;
        v_stat_cw_chng     usr_clnt_cdwrd_hist_dbt.t_cw_status%TYPE := 5;
        v_cw_proc_type     usr_clnt_cdwrd_buf_dbt.t_cw_proc_type%TYPE := 1;
    BEGIN
        BEGIN
            /* Поиск предыдущего Кодового слова */
                     SELECT cw_1.t_id
                       INTO v_cw_prev
                       FROM usr_clnt_cdwrd_hist_dbt cw_1
            LEFT OUTER JOIN usr_clnt_cdwrd_hist_dbt cw_2 ON cw_2.t_cw_client = cw_1.t_cw_client
                                                        AND cw_2.t_cw_type = cw_1.t_cw_type
                                                        AND cw_2.t_cw_create_ts > cw_1.t_cw_create_ts
                      WHERE cw_1.t_cw_client = p_cw_client
                        AND cw_1.t_cw_type = p_cw_type
                        AND cw_1.t_cw_status = v_stat_cw_accepted
                        AND cw_2.t_cw_create_ts IS NULL;

            /* Если нашли предыдущую запись, значит процесс "Изменение" */
            v_cw_proc_type := 2;

            /* Обновляем статус старой записи (если она есть) */
            /* В таблице должна быть только одна запись с действующим кодом */
            UPDATE usr_clnt_cdwrd_hist_dbt
               SET t_cw_status = v_stat_cw_chng,
                   t_cw_is_active = v_cw_not_active
             WHERE t_cw_client = p_cw_client
               AND t_cw_type = p_cw_type
               AND t_cw_status = v_stat_cw_accepted;

        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                v_ret := 0; /* Все нормально - продолжаем */

            WHEN OTHERS
            THEN
                v_ret := SQLCODE;
                ROLLBACK;
        END;

        BEGIN
            IF v_ret = 0
            THEN
                /* Переносим новую запись из буферной таблицы */
                INSERT INTO usr_clnt_cdwrd_hist_dbt
                       (T_ID, T_CW_PREV, T_CW_TYPE, T_CW_CLIENT,
                        T_CW_PROC_TYPE, T_CODE_WORD, T_CW_STATUS,
                        T_CW_CREATE_TS, T_CW_ACCEPT_TS, T_CW_ACCEPT_TRY,
                        T_MANUAL_OPER)
                VALUES (v_def_id, v_cw_prev, p_cw_type, p_cw_client,
                       v_cw_proc_type, p_code_word, v_stat_cw_accepted,
                       SYSTIMESTAMP, SYSTIMESTAMP, 0,
                       p_man_oper);

                COMMIT;
            END IF;

        EXCEPTION
            WHEN OTHERS
            THEN
                v_ret := SQLCODE;
                ROLLBACK;
        END;

        RETURN v_ret;

    END f_add_code_word;

    /* Добавление категории к объекту.
     * 0 - категория успешно добавлена
     * 1 -значение категории уже заполнено. Ничего не редактируется
     * 2 - неизвестная ошибка
     */
    FUNCTION ConnectAttr (p_ObjType IN NUMBER,
                          p_GroupId IN NUMBER,
                          p_ObjId IN VARCHAR2,
                          p_AttrId IN NUMBER,
                          p_ValidFromDate IN DATE ) RETURN NUMBER
    IS
        v_state        INTEGER;
        v_ValidToDate  dobjatcor_dbt.T_VALIDTODATE%TYPE := to_date('31.12.9999', 'dd.mm.yyyy');
    BEGIN
        
        v_state := 0;
            
        BEGIN
            SELECT 1 INTO v_state FROM DOBJATCOR_DBT WHERE T_OBJECTTYPE = p_ObjType AND T_GROUPID = p_GroupId AND T_OBJECT = p_ObjId;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_state := 0;
          WHEN TOO_MANY_ROWS THEN
            v_state := 1;
          WHEN OTHERS THEN
            v_state := 2;
        END;
        
        IF v_state = 0 THEN
            INSERT INTO dobjatcor_dbt( T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_ISAUTO )
            VALUES( p_ObjType, p_GroupId, p_AttrId, p_ObjId, CHR(88), p_ValidFromDate, RSBSESSIONDATA.OPER, v_ValidToDate, CHR(88) );
        END IF;
        
        RETURN v_state;

      EXCEPTION
       WHEN OTHERS THEN
         RETURN 2;
    END ConnectAttr;
    
END USR_PKG_IMPORT_SOFR;
/
