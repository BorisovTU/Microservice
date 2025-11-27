--п.3.6 ТЗ
DECLARE
  logID VARCHAR2(32) := 'BOSS-4040';
  v_Cnt NUMBER := 0;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;

BEGIN
  LogIt('Создание отсутствующих РОВУ по неторговым операциям');
  INSERT INTO DDLINACC_DBT (T_BOFFICE, 
                            T_OPERTYPE, 
                            T_DOCUMENTKIND, 
                            T_DOCUMENTID, 
                            T_DATE, 
                            T_CHAPTER, 
                            T_DEBACC, 
                            T_KREDACC, 
                            T_FIKIND, 
                            T_FIID, 
                            T_SUM, 
                            T_RATESUM, 
                            T_GROUNDKIND, 
                            T_GROUNDNUM, 
                            T_DEPARTMENT, 
                            T_OPER, 
                            T_DEALKIND, 
                            T_ACCTRNID, 
                            T_PLACEID, 
                            T_SERVDOCKIND, 
                            T_SERVDOCID, 
                            T_PARTYID, 
                            T_PARTYCONTRID, 
                            T_MARKETOFFICEID) 
  SELECT /*T_BOFFICE*/       DECODE(sf.t_ServKind, 1, 5, 8),
         /*T_OPERTYPE*/      CASE WHEN acctrn.t_Ground LIKE 'Ком%' THEN 8 WHEN op.t_SubKind_Operation = 10 THEN 6 WHEN op.t_SubKind_Operation = 20 AND acctrn.t_Ground LIKE 'Уде%' THEN 17 ELSE 7 END,
         /*T_DOCUMENTKIND*/  op.t_DocKind,
         /*T_DOCUMENTID*/    op.t_ID,
         /*T_DATE*/          acctrn.t_Date_Carry,
         /*T_CHAPTER*/       acctrn.t_Chapter,
         /*T_DEBACC*/        acctrn.t_Account_Payer,
         /*T_KREDACC*/       acctrn.t_Account_Receiver,   
         /*T_FIKIND*/        1,
         /*T_FIID*/          op.t_FIID,
         /*T_SUM*/           acctrn.t_Sum_Payer,    
         /*T_RATESUM*/       0,   
         /*T_GROUNDKIND*/    NVL((SELECT grnd.t_Kind FROM DSPGROUND_DBT grnd WHERE grnd.t_SpGroundID = (SELECT MIN(doc.t_SpGroundID) FROM DSPGRDOC_DBT doc WHERE doc.t_SourceDocKind = op.t_DocKind AND doc.t_SourceDocID = op.t_ID)), 0), --создадим РОВУ, даже если документ так и нет                                                                        
         /*T_GROUNDNUM*/     NVL((SELECT grnd.t_AltXld FROM DSPGROUND_DBT grnd WHERE grnd.t_SpGroundID = (SELECT MIN(doc.t_SpGroundID) FROM DSPGRDOC_DBT doc WHERE doc.t_SourceDocKind = op.t_DocKind AND doc.t_SourceDocID = op.t_ID)), CHR(1)),                                                                         
         /*T_DEPARTMENT*/    acctrn.t_Department,
         /*T_OPER*/          acctrn.t_Oper,
         /*T_DEALKIND*/      op.t_DocKind,
         /*T_ACCTRNID*/      acctrn.t_AccTrnID, 
         /*T_PLACEID*/       1,
         /*T_SERVDOCKIND*/   0,
         /*T_SERVDOCID*/     0,
         /*T_PARTYID*/       0,
         /*T_PARTYCONTRID*/  -1,
         /*T_MARKETOFFICEID*/0
    FROM DNPTXOP_DBT op, DOPROPER_DBT oproper, DOPRDOCS_DBT oprd, DACCTRN_DBT acctrn, DSFCONTR_DBT sf, DPARTY_DBT pt 
   WHERE op.t_DocKind = 4607                                                                            
     AND oproper.t_Kind_Operation = op.t_Kind_Operation                                                 
     AND oproper.t_DocumentID = op.t_ID                                                                 
     AND oprd.t_ID_Operation = oproper.t_ID_Operation                                                   
     AND oprd.t_DocKind = 1                                                                             
     AND acctrn.t_AccTrnID = oprd.t_AccTrnID                                                            
     AND acctrn.t_Chapter = 21
     AND sf.t_ID = op.t_Contract 
     AND pt.t_PartyID = op.t_Client
     AND pt.t_LegalForm = 2 --ЮЛ не трогаем
     AND NOT EXISTS (SELECT 1 FROM DDLINACC_DBT inacc WHERE inacc.t_AccTrnID = acctrn.t_AccTrnID);          
                   
  v_Cnt := SQL%ROWCOUNT;
  LogIt('Создано '||to_char(v_Cnt)||' РОВУ по неторговым операциям');

EXCEPTION WHEN OTHERS THEN 
  LogIt('Ошибка при создании РОВУ по неторговым операциям');
END;
/