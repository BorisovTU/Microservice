--Фондовый рынок
DECLARE
   v_spgroundid NUMBER := 0;
BEGIN
   --Исправляем заявки по сделкам с ID 3202635, 3203608
   INSERT INTO dspground_dbt (T_SPGROUNDID, T_DOCLOG, T_KIND, T_DIRECTION, T_XLD, T_REGISTRDATE, T_REGISTRTIME, T_PARTY, T_ALTXLD, T_SIGNEDDATE, T_SIGNEDTIME, T_PROXY, T_DIVISION, T_REFERENCES, T_RECEPTIONIST, T_COPIES, T_SENT, T_DELIVERYKIND,
                              T_BACKOFFICE, T_COMMENT, T_SOURCEDOCID, T_SOURCEDOCKIND, T_DOCTEMPLATE, T_TERMINATEDATE, T_PARTYNAME, T_PARTYCODE, T_BEGINNINGDATE, T_SENTDATE, T_SENTTIME, T_DEPARTMENT, T_BRANCH, T_PARENT, T_USERLOG, T_VERSION,  
                              T_ISMAKEAUTO, T_TECHAUTODOC, T_DEPONENT, T_HAVESUBJLIST, T_SUBJECTID, T_REGISTERID, T_DEPOACNTID, T_MSGNUMBER, T_MSGDATE, T_MSGTIME, T_METHODAPPLIC)                                                                 
                      VALUES (0, 513, 251, 1, '0005.22041415122091', 
                              TO_DATE('04/14/2022 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 15:12:20', 'MM/DD/YYYY HH24:MI:SS'), 153197, '0005.22041415122091', TO_DATE('04/14/2022 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 
                              TO_DATE('01/01/0001 15:12:20', 'MM/DD/YYYY HH24:MI:SS'), 0, 0, 2, 1, 
                              0, '', 0, 'Ъ', 'Поручение клиента на сделку по заявке 31018562104', 
                              0, 0, 0, TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 'Битюкова Галина Александровна', 
                              '296051346794', TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 1, 
                              1, 0, 0, 2, 'X', 
                              0, 0, '', 0, 0, 
                              0, '', TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 1) RETURNING t_spgroundid INTO v_spgroundid;
       
   UPDATE dspgrdoc_dbt
      SET t_spgroundid = v_spgroundid
    WHERE t_spgroundid = 6419165
      AND ((t_sourcedockind = 350 AND t_sourcedocid = 6418292) OR (t_sourcedockind = 101 AND t_sourcedocid = 3203608));

   --Исправляем заявки по сделкам с ID 3202979, 3203095
   INSERT INTO dspground_dbt (T_SPGROUNDID, T_DOCLOG, T_KIND, T_DIRECTION, T_XLD, T_REGISTRDATE, T_REGISTRTIME, T_PARTY, T_ALTXLD, T_SIGNEDDATE, T_SIGNEDTIME, T_PROXY, T_DIVISION, T_REFERENCES, T_RECEPTIONIST, T_COPIES, T_SENT, T_DELIVERYKIND,
                              T_BACKOFFICE, T_COMMENT, T_SOURCEDOCID, T_SOURCEDOCKIND, T_DOCTEMPLATE, T_TERMINATEDATE, T_PARTYNAME, T_PARTYCODE, T_BEGINNINGDATE, T_SENTDATE, T_SENTTIME, T_DEPARTMENT, T_BRANCH, T_PARENT, T_USERLOG, T_VERSION,  
                              T_ISMAKEAUTO, T_TECHAUTODOC, T_DEPONENT, T_HAVESUBJLIST, T_SUBJECTID, T_REGISTERID, T_DEPOACNTID, T_MSGNUMBER, T_MSGDATE, T_MSGTIME, T_METHODAPPLIC)                                                                 
                      VALUES (0, 513, 251, 1, '0005.220414151220141', 
                              TO_DATE('04/14/2022 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 15:12:20', 'MM/DD/YYYY HH24:MI:SS'), 164458, '0005.220414151220141', TO_DATE('04/14/2022 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 
                              TO_DATE('01/01/0001 15:12:20', 'MM/DD/YYYY HH24:MI:SS'), 0, 0, 2, 1, 
                              0, '', 0, 'Ъ', 'Поручение клиента на сделку по заявке 31018562130', 
                              0, 0, 0, TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 'Ихсанов Марат Фаилевич', 
                              '526230853075', TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 1, 
                              1, 0, 0, 2, 'X', 
                              0, 0, '', 0, 0, 
                              0, '', TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 1) RETURNING t_spgroundid INTO v_spgroundid;
       
   UPDATE dspgrdoc_dbt
      SET t_spgroundid = v_spgroundid
    WHERE t_spgroundid = 6419236
      AND ((t_sourcedockind = 350 AND t_sourcedocid = 6418362) OR (t_sourcedockind = 101 AND t_sourcedocid = 3202979));

   --Исправляем заявки по сделкам с ID 3710423, 3710427 
   DELETE FROM dspgrdoc_dbt
    WHERE t_sourcedocid IN (3710423, 3710427) 
      AND t_sourcedockind = 101
      AND t_spgroundid = 6539713;
     
   --Исправляем заявки по сделкам с ID 9098675
   DELETE FROM dspgrdoc_dbt
    WHERE t_sourcedocid = 9098675
      AND t_sourcedockind = 101
      AND t_spgroundid = 9783052;
   
   --Исправляем заявки по сделкам с ID 10040839, 10040148
   INSERT INTO dspground_dbt (T_SPGROUNDID, T_DOCLOG, T_KIND, T_DIRECTION, T_XLD, T_REGISTRDATE, T_REGISTRTIME, T_PARTY, T_ALTXLD, T_SIGNEDDATE, T_SIGNEDTIME, T_PROXY, T_DIVISION, T_REFERENCES, T_RECEPTIONIST, T_COPIES, T_SENT, T_DELIVERYKIND,
                              T_BACKOFFICE, T_COMMENT, T_SOURCEDOCID, T_SOURCEDOCKIND, T_DOCTEMPLATE, T_TERMINATEDATE, T_PARTYNAME, T_PARTYCODE, T_BEGINNINGDATE, T_SENTDATE, T_SENTTIME, T_DEPARTMENT, T_BRANCH, T_PARENT, T_USERLOG, T_VERSION,  
                              T_ISMAKEAUTO, T_TECHAUTODOC, T_DEPONENT, T_HAVESUBJLIST, T_SUBJECTID, T_REGISTERID, T_DEPOACNTID, T_MSGNUMBER, T_MSGDATE, T_MSGTIME, T_METHODAPPLIC)                                                                 
                      VALUES (0, 513, 251, 1, '10232.2301271740391', 
                              TO_DATE('01/27/2023 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 17:40:39', 'MM/DD/YYYY HH24:MI:SS'), 167709, '10232.2301271740391', TO_DATE('01/27/2023 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 
                              TO_DATE('01/01/0001 17:40:39', 'MM/DD/YYYY HH24:MI:SS'), 0, 0, 2, 1, 
                              0, '', 0, 'Ъ', 'Поручение клиента на сделку по заявке 52203054396', 
                              0, 0, 0, TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 'Пятковский Алексей Владимирович', 
                              '121423192225', TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 1, 
                              1, 0, 0, 2, 'X', 
                              0, 0, '', 0, 0, 
                              0, '', TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 1) RETURNING t_spgroundid INTO v_spgroundid;
       
   UPDATE dspgrdoc_dbt
      SET t_spgroundid = v_spgroundid
    WHERE t_spgroundid = 9801826
      AND ((t_sourcedockind = 350 AND t_sourcedocid = 9633830) OR (t_sourcedockind = 101 AND t_sourcedocid = 10040148));
      
   --Исправляем заявки по сделкам с ID 9495727, 9495619, 10750952
   DELETE FROM dspgrdoc_dbt
         WHERE t_sourcedocid in (9170528, 10330024)
           AND t_sourcedockind = 350;

   DELETE FROM ddl_req_dbt
         WHERE t_id in (9170528, 10330024);
 
   COMMIT;

EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/


--Валютный рынок
DECLARE
   v_spgroundid NUMBER := 0;
BEGIN
   --Исправляем заявки по сделкам с ID 757258, 791377, 7580496, 1199238, 1211899
   DELETE FROM dspgrdoc_dbt
         WHERE t_sourcedocid IN (7166508, 7449633, 7543434, 10000502, 10114983) 
           AND t_sourcedockind = 350;

   DELETE FROM ddl_req_dbt
         WHERE t_id IN (7166508, 7449633, 7543434, 10000502, 10114983);

   --Исправляем заявки по сделкам с ID 910972, 911461
   UPDATE ddl_req_dbt
      SET t_sourcekind = 199
    WHERE t_id = 8484965;
 
   INSERT INTO dspground_dbt (T_SPGROUNDID, T_DOCLOG, T_KIND, T_DIRECTION, T_XLD, T_REGISTRDATE, T_REGISTRTIME, T_PARTY, T_ALTXLD, T_SIGNEDDATE, T_SIGNEDTIME, T_PROXY, T_DIVISION, T_REFERENCES, T_RECEPTIONIST, T_COPIES, T_SENT, T_DELIVERYKIND, 
                              T_BACKOFFICE, T_COMMENT, T_SOURCEDOCID, T_SOURCEDOCKIND, T_DOCTEMPLATE, T_TERMINATEDATE, T_PARTYNAME, T_PARTYCODE, T_BEGINNINGDATE, T_SENTDATE, T_SENTTIME, T_DEPARTMENT, T_BRANCH, T_PARENT, T_USERLOG, T_VERSION, 
                              T_ISMAKEAUTO, T_TECHAUTODOC, T_DEPONENT, T_HAVESUBJLIST, T_SUBJECTID, T_REGISTERID, T_DEPOACNTID, T_MSGNUMBER, T_MSGDATE, T_MSGTIME, T_METHODAPPLIC) 
                      VALUES (0, 250, 251, 1, '0005.2209271654341', 
                              TO_DATE('09/27/2022 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 16:54:34', 'MM/DD/YYYY HH24:MI:SS'), 245279, '0005.2209271654341', TO_DATE('09/27/2022 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 
                              TO_DATE('01/01/0001 16:54:34', 'MM/DD/YYYY HH24:MI:SS'), 0, 0, 2, 1, 
                              0, '', 0, 'Ъ', 'Поручение клиента на сделку по заявке 25519860895', 
                              0, 0, 0, TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 'Исавнин Ярослав Александрович', 
                              '569970253684', TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 1, 
                              1, 0, 0, 2, 'X', 
                              0, 0, '', 0, 0, 
                              0, '', TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), 1) RETURNING t_spgroundid INTO v_spgroundid;
       
   UPDATE dspgrdoc_dbt
      SET t_spgroundid = v_spgroundid
    WHERE t_spgroundid = 8579849
      AND ((t_sourcedockind = 350 AND t_sourcedocid = 8484981) OR (t_sourcedockind = 4813 AND t_sourcedocid = 910972));

   COMMIT;

EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/


--Срочный рынок
DECLARE
   v_spgroundid NUMBER := 0;
BEGIN
   --Исправляем заявки по сделкам с ID 657170, 657201, 694418
   DELETE FROM dspgrdoc_dbt
         WHERE t_sourcedocid in (9965783, 10230917)
           AND t_sourcedockind = 350;

   DELETE FROM ddl_req_dbt
         WHERE t_id in (9965783, 10230917);

   UPDATE dspground_dbt
      SET t_doclog = 250, 
          t_party = 169207, 
          t_partyname = 'Ласточкин Андрей Николаевич', 
          t_partycode = '524801406913', 
          t_comment = 'Поручение клиента на сделку по заявке 1892948226588375300'
    WHERE t_spgroundid = 10149074;

   UPDATE dspground_dbt
      SET t_doclog = 250, 
          t_party = 136093, 
          t_partyname = 'Царьков Дмитрий Владимирович', 
          t_partycode = '00#P#11470805', 
          t_comment = 'Поручение клиента на сделку по заявке 2012575190474629748'
    WHERE t_spgroundid = 10425817;

   --Исправляем заявки по сделкам с ID 277706, 277865, 277873, 278031, 279148, 279168, 279204, 279205, 279242, 279278, 279460, 279521, 279723, 279795, 279801, 279871, 279989, 280031, 280032, 280067, 317456, 317735, 362629, 362878, 362880, 363149, 
   --374259, 374324, 396264, 396276, 396348, 396631, 436629, 437216, 442908, 443481, 446093, 446265, 446293, 446476, 446578, 446612, 450018, 450062, 477294, 478089, 491823, 492967, 495299, 495410, 546955, 546967, 568743, 569166, 571868, 575311, 591741, 
   --592296, 592344, 592819, 596034, 596038, 596096, 596158, 622176, 622184, 636617, 636626, 636680, 636729, 636905, 637143, 637637, 637728, 637760, 638259, 638583, 638836, 638943, 639049, 639555, 639779, 639916, 640394, 640560, 640630, 657170, 657201
   FOR rec IN (SELECT tk.t_ID t_GoodDealID, tk1.t_ID t_BadDealID, req.t_ID t_GoodReqID, req1.t_ID t_BadReqID, req1.t_Codets t_BadReqCode, ground.*
                FROM DDVDEAL_DBT tk,                 
                     dspgrdoc_dbt dealdoc,                 
                     DDVDEAL_DBT tk1,                 
                     dspgrdoc_dbt dealdoc1,
                     dspground_dbt ground,
                     dspgrdoc_dbt reqdoc,
                     ddl_req_dbt req,
                     dspgrdoc_dbt reqdoc1,
                     ddl_req_dbt req1
               WHERE tk.t_Date >= TO_DATE ('01.01.2022', 'dd.mm.yyyy')
                     AND dealdoc.t_sourcedocid = tk.t_ID
                     AND dealdoc.t_sourcedockind = 192
                     AND ground.t_spgroundid = dealdoc.t_spgroundid
                     AND dealdoc1.t_sourcedocid = tk1.t_ID
                     AND dealdoc1.t_sourcedockind = 192
                     AND ground.t_spgroundid = dealdoc1.t_spgroundid
                     AND dealdoc1.t_sourcedocid != reqdoc1.t_sourcedocid
                     AND dealdoc1.t_sourcedockind != reqdoc1.t_sourcedockind
                     AND dealdoc.t_sourcedocid < dealdoc1.t_sourcedocid
                     AND ground.t_spgroundid = reqdoc.t_spgroundid
                     AND dealdoc.t_sourcedocid != reqdoc.t_sourcedocid
                     AND dealdoc.t_sourcedockind != reqdoc.t_sourcedockind
                     AND reqdoc.t_sourcedocid = req.t_id
                     AND reqdoc.t_sourcedockind = req.t_kind
                     AND ground.t_spgroundid = reqdoc1.t_spgroundid
                     AND dealdoc.t_sourcedocid != reqdoc1.t_sourcedocid
                     AND dealdoc.t_sourcedockind != reqdoc1.t_sourcedockind
                     AND reqdoc.t_sourcedocid < reqdoc1.t_sourcedocid
                     AND reqdoc1.t_sourcedocid = req1.t_id
                     AND reqdoc1.t_sourcedockind = req1.t_kind)
   LOOP
      v_spgroundid := 0;
      INSERT INTO dspground_dbt (T_SPGROUNDID, T_DOCLOG, T_KIND, T_DIRECTION, T_XLD, T_REGISTRDATE, T_REGISTRTIME, T_PARTY, T_ALTXLD, T_SIGNEDDATE, T_SIGNEDTIME, T_PROXY, T_DIVISION, T_REFERENCES, T_RECEPTIONIST, T_COPIES, T_SENT, T_DELIVERYKIND,
                                 T_BACKOFFICE, T_COMMENT, T_SOURCEDOCID, T_SOURCEDOCKIND, T_DOCTEMPLATE, T_TERMINATEDATE, T_PARTYNAME, T_PARTYCODE, T_BEGINNINGDATE, T_SENTDATE, T_SENTTIME, T_DEPARTMENT, T_BRANCH, T_PARENT, T_USERLOG, T_VERSION,  
                                 T_ISMAKEAUTO, T_TECHAUTODOC, T_DEPONENT, T_HAVESUBJLIST, T_SUBJECTID, T_REGISTERID, T_DEPOACNTID, T_MSGNUMBER, T_MSGDATE, T_MSGTIME, T_METHODAPPLIC)                                                                 
                          VALUES (0, 250, 251, rec.T_DIRECTION, rec.T_XLD||'99', 
                                  rec.T_REGISTRDATE, rec.T_REGISTRTIME, rec.T_PARTY, rec.T_ALTXLD||'99', rec.T_SIGNEDDATE, rec.T_SIGNEDTIME, rec.T_PROXY, rec.T_DIVISION, rec.T_REFERENCES, rec.T_RECEPTIONIST, 
                                  rec.T_COPIES, rec.T_SENT, rec.T_DELIVERYKIND, rec.T_BACKOFFICE, 'Поручение клиента на сделку по заявке '||rec.t_BadReqCode, 
                                  rec.T_SOURCEDOCID, rec.T_SOURCEDOCKIND, rec.T_DOCTEMPLATE, rec.T_TERMINATEDATE, rec.T_PARTYNAME, rec.T_PARTYCODE, rec.T_BEGINNINGDATE, rec.T_SENTDATE, rec.T_SENTTIME, 
                                  rec.T_DEPARTMENT, rec.T_BRANCH, rec.T_PARENT, rec.T_USERLOG, rec.T_VERSION, rec.T_ISMAKEAUTO, rec.T_TECHAUTODOC, rec.T_DEPONENT, rec.T_HAVESUBJLIST, rec.T_SUBJECTID, 
                                  rec.T_REGISTERID, rec.T_DEPOACNTID, rec.T_MSGNUMBER, rec.T_MSGDATE, rec.T_MSGTIME, rec.T_METHODAPPLIC) RETURNING t_spgroundid INTO v_spgroundid;
       
      UPDATE dspgrdoc_dbt
         SET t_spgroundid = v_spgroundid
       WHERE t_spgroundid = rec.t_spgroundid
         AND ((t_sourcedockind = 350 AND t_sourcedocid = rec.t_BadReqID) OR (t_sourcedockind = 192 AND t_sourcedocid = rec.t_BadDealID));
   END LOOP;

   COMMIT;

EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/               