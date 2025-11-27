CREATE OR REPLACE PACKAGE BODY RSB_PAYMENTS_API
IS
   PROCEDURE InsertGENERALINF (p_InterMesID in integer, p_Root in varchar2, p_MesType in varchar2, p_Count out integer, p_XMLProduct in varchar2, p_ProductType in varchar2, p_Text out varchar2, p_SRSType in varchar2);
   PROCEDURE GetInsertGENERALINF (p_MesType in varchar2, p_SRSType in varchar2, p_Insert out varchar2, p_Select out varchar2, p_From out varchar2);
   PROCEDURE GetTypeMessage (p_Root out varchar2, p_MesType out varchar2, p_Generate out char);
   FUNCTION GetTypeSRS (p_XMLProduct in varchar2) return varchar2;
   FUNCTION CheckXMLProduct return varchar2;
   FUNCTION CheckElementProductType return integer;
   FUNCTION CheckProductType (p_SRSType in varchar2, p_Reporting out char) return varchar2;
   FUNCTION CheckElementSRS (p_SRSType in varchar2, p_Element out varchar2) return integer;
   FUNCTION CheckAmendment return integer;
   PROCEDURE GetTextServiceAttrSRS (p_MesType in varchar2, p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2);
   PROCEDURE GetTextCommonAttrSRS (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2);
   PROCEDURE GetTextGeneralAttr (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2);
   PROCEDURE GetTextGeneralAttrRM001 (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2);
   PROCEDURE GetTextGeneralAttrRM002 (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2);
   PROCEDURE GetTextGeneralAttrRM003 (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2);
   PROCEDURE GetTextGeneralAttrRM005 (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2, p_Amendment in integer);
   PROCEDURE GetTextGeneralAttrRM006 (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2);
   PROCEDURE GetTextGeneralAttrParty (p_Party in varchar2, p_SRSType in varchar2, p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2);
   PROCEDURE GetTextGeneralAttrClient (p_Client in varchar2, p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2);
   PROCEDURE InsertTableParty (p_InterMesID in integer, l_SRSType in varchar2);
   FUNCTION InsertTableParty083 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableDifferences (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM021 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM022 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM023 (p_InterMesID in integer, p_ProductType in varchar2, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM032 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM041 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM042 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM043 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM044 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM045 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM046 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM047 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM048 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM051 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM053 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM092 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM093 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM094 (p_InterMesID in integer, p_Text out varchar2) return integer;
   FUNCTION InsertTableCM083 (p_InterMesID in integer, p_Text out varchar2, p_IDParty in integer, p_CounterpartyID in varchar2, p_CounterpartyNum in integer) return integer;
   FUNCTION CheckReportReferences return integer;
   PROCEDURE UpdateProductID (p_InterMesID in integer);
   PROCEDURE InsertInstrument (SRS_ID IN NUMBER, p_Code IN VARCHAR2, p_Name IN VARCHAR2);
   FUNCTION InsertInstrument083 (SRS_ID IN NUMBER, Code IN VARCHAR2) return integer;
   PROCEDURE InsertDlGrDoc(p_op_id IN NUMBER, p_InterMesID IN NUMBER, p_GrID IN NUMBER);
--   PROCEDURE InsertLogDate(pDocID IN NUMBER, pDocKind IN NUMBER, pType IN NUMBER,  pOper IN INTEGER, pLogData IN VARCHAR2);
   FUNCTION Get_RowProtocol (p_MESTYPE in varchar2, p_Product in varchar2, p_ErrorText in varchar2) return clob;
   FUNCTION Get_RowProtocolProc (p_MESSAGETYPE in varchar2, p_MESSAGEID in varchar2, p_INREPLYTO in varchar2, p_TRADEID_UTIGENERATINGPARTY in varchar2, p_XMLPRODUCT in varchar2, p_ErrorText in varchar2) return clob;

   PROCEDURE DeleteSRS (p_InterMesID in integer);
--   PROCEDURE InsertHeaderLog(p_DocId IN NUMBER, p_DocKind IN NUMBER, p_oper IN NUMBER, p_LogData IN varchar2);

   PROCEDURE InsertHeaderLog(p_DocId IN NUMBER, p_DocKind IN NUMBER, p_oper IN NUMBER, p_LogData IN varchar2)
   AS
   BEGIN
      RSI_DLLOG.InsertHeader( p_DocId, p_DocKind, p_oper, p_LogData );
   END;

   PROCEDURE InsertLogDate(pDocID IN NUMBER, pDocKind IN NUMBER, pType IN NUMBER,  pOper IN INTEGER, pLogData IN VARCHAR2)
   AS
   BEGIN
      RSI_DLLOG.InsertDL_LOGDATA(pDocID , pDocKind, pType , 0, 0, pOper , pLogData);
   END;


   FUNCTION  InsertOP_SRS ( ID_OP OUT integer,ID_PERSON OUT integer) RETURN NUMBER
   AS
--     ID_OP INTEGER;
     refer varchar2(30);
     stat integer;
     refID integer;
     ID_Oper integer;
     ID_DEP integer;
     ID_DEPNODE integer;
   BEGIN
     stat:=0;
     ID_Oper :=0;
     ID_Oper := Rsb_Common.GetRegIntValue('РЕПОЗИТАРИЙ\ИСПОЛН.ПРИ АВТОМ.ОБР-КЕ СООБЩ', 0);
  IF  ID_Oper is null then /*DAN */
    id_oper := 1;
  end if;

     ID_PERSON:= ID_Oper;
     IF(ID_Oper <> 0) THEN
       begin
         select pers.t_codedepart into ID_DEPNODE from dperson_dbt pers where pers.t_oper = ID_Oper;
         exception
           WHEN NO_DATA_FOUND THEN
         stat:=1;
       end;

       IF (stat = 0) THEN
         begin
           select q.t_code INTO ID_DEP from (
              select level lv, t.* from ddp_dep_dbt t
              CONNECT BY Prior  t.t_parentcode = t.t_code
              and prior t.t_nodetype <> 1   --не равен филиалу
              start With t.t_code = ID_DEPNODE
              order by level desc) q
               where rownum = 1;
         exception
           WHEN NO_DATA_FOUND THEN
           ID_DEP:=ID_DEPNODE;
         end;
       END IF;



       IF(stat = 0) then
          RsbSessionData.SetCurdate(TRUNC(SYSDATE));
          RsbSessionData.SetOperDprt(ID_DEP);-- департамент
          RsbSessionData.SetOperDprtNode(ID_DEPNODE);-- подразделение
          stat := RSI_RSB_REFER.GetReferenceIDByType (4028,3, refID);
          if (stat = 0)
            then
             stat:= RSI_RSB_REFER.WldGenerateReference(refer,refID ,4028, 0, TRUNC(SYSDATE));
          end if;
          refer := 'AUTO-'||refer;
          INSERT INTO DIR_OP_DBT (T_DOCKIND, T_DATE, T_STATUS, T_OPERATOR,T_CODEOP, T_PROTOCOL ,T_INBOUNDPROCESS, T_RECONCILIATION)
                       VALUES (REPOS_INBOUNDMSG, TRUNC(SYSDATE) , 2 ,ID_Oper ,refer,chr(88) ,chr(88) ,chr(88) )
                       RETURNING DIR_OP_DBT.T_ID INTO ID_OP;
       END IF;

     ELSE
           stat:=1;
     END IF;

     IF(NOT (ID_OP >0) ) THEN
           stat:=1;
     END IF;

     if(stat = 0) then
        RSI_DLLOG.InsertDL_LOG(ID_OP,Rsb_Secur.REPOS_INBOUNDMSG,ID_Oper );
        RSI_DLLOG.InsertHeader( ID_OP, Rsb_Secur.REPOS_INBOUNDMSG, ID_Oper, LOGHEADER );
     end if;

     return stat;
   END InsertOP_SRS;

   PROCEDURE InsertDlGrDoc(p_op_id IN NUMBER, p_InterMesID IN NUMBER, p_GrID IN NUMBER)
   IS
   BEGIN
       INSERT INTO ddlgrdoc_dbt (t_ID, t_GRDEALID, T_DOCKIND, T_DOCID, T_SERVDOCKIND, T_SERVDOCID, T_GRPID, T_SOURCETYPE)
       VALUES (0,p_GrID,REPOS_GENERALINF,p_InterMesID,REPOS_INBOUNDMSG,p_op_id,0,0);
   END InsertDlGrDoc;

   PROCEDURE InsertInstrument (SRS_ID IN NUMBER, p_Code IN VARCHAR2, p_Name IN VARCHAR2)
   IS
     FIID INTEGER DEFAULT -1;
   BEGIN
     IF p_Code IS NOT NULL THEN
       BEGIN
         SELECT T_FIID INTO FIID FROM davoiriss_dbt
          WHERE t_ISIN = p_Code;
       EXCEPTION WHEN OTHERS THEN
         BEGIN
           SELECT T_FIID INTO FIID FROM davoiriss_dbt
            WHERE t_LSIN = p_Code;
         EXCEPTION WHEN OTHERS THEN
           BEGIN
             SELECT T_FIID INTO FIID FROM dfininstr_dbt
              WHERE t_FI_CODE = p_Code;
           EXCEPTION WHEN OTHERS THEN NULL;
           END;
         END;
       END;
     END IF;
     -- Save text as received
     INSERT INTO dir_instruments_dbt
                 (T_INTERNALMESSAGEID, T_FIID, T_INSTRUMENTCODE, T_INSTRUMENTNAME)
          VALUES (SRS_ID, FIID, NVL (p_Code, CHR (1)), NVL (p_Name, CHR (1)));
   END InsertInstrument;

   FUNCTION InsertInstrument083 (SRS_ID IN NUMBER, Code IN VARCHAR2) return Integer
   IS
   FIID INTEGER;
   FI_CODE VARCHAR2(15);
   ID_INSTR INTEGER;
   FI_NAME VARCHAR2(50) := chr(1);
   BEGIN
     begin
       SELECT fi.T_FiID, fi.t_fi_code, fi.T_NAME into FIID, FI_CODE, FI_NAME
                FROM dfininstr_dbt fi
               WHERE  fi.t_fi_code = Code
               AND ROWNUM = 1;
      exception
      WHEN NO_DATA_FOUND THEN
        FIID := -1;
        FI_CODE := chr(1);
     end;

     ID_INSTR:=0;
      INSERT INTO dir_instruments_dbt (T_ID,
                                       T_INTERNALMESSAGEID,
                                       T_FIID,
                                       T_INSTRUMENTCODE,
                                       T_INSTRUMENTNAME)
           VALUES (0,
                   SRS_ID,
                   FIID,
                   FI_CODE,
                   FI_NAME)
           RETURNING dir_instruments_dbt.T_ID INTO ID_INSTR;
      return ID_INSTR;
   END InsertInstrument083;


   PROCEDURE SetStatusSRS (SRS_ID IN NUMBER, V_Status IN NUMBER DEFAULT 0)
   IS
      Status       INTEGER;
      StatusName   VARCHAR2 (40);
   BEGIN
      -- тут установка статуса
      IF (V_Status = 0)
      THEN
         Status := RSTATUS_UNLOADED;
      ELSE
         Status := RSTATUS_ER_UNLOADED;
      END IF;

      SELECT NA.T_SZNAMEALG
        INTO StatusName
        FROM DNAMEALG_DBT NA
       WHERE NA.T_ITYPEALG = ALG_GENINF_RSTATUS AND NA.T_INUMBERALG = Status;

       UPDATE DIR_GENERALINF_DBT SET T_RSTATUS = Status ,T_RSTATUSNAME = StatusName
       WHERE T_INTERNALMESSAGEID = SRS_ID;

   END SetStatusSRS;

   FUNCTION GenerationSRSFromInput (p_Text OUT VARCHAR2, p_InterMesID out integer, p_op_id IN INTEGER, p_Protocol out varchar2) return INTEGER
   AS
   /*Процедура генерации СРС из полученного сообщения:
       p_Content - текст полученного сообщения
       p_Date    - дата-время получения сообщения в Payments-е
   */
     l_Root        VARCHAR2(80);
     l_MesType     VARCHAR2(5);
     l_Generate    CHAR(1);
     l_Count       INTEGER;
     l_XMLProduct  VARCHAR2(35);
     l_ProductType VARCHAR2(80);
     l_Reporting   CHAR(1);
     l_SRSType     VARCHAR2(5);
     l_Element     VARCHAR2(256);
   BEGIN
     GetTypeMessage(l_Root, l_MesType, l_Generate);
     IF nvl(l_Generate, chr(0)) = chr(0) THEN
       p_Text := 'В системе не реализована обработка такого вида сообщений. Необходимо выполнить обработку сообщения вручную.';
       p_Protocol := Get_RowProtocol(l_MesType, l_XMLProduct, p_Text);
       Return 2;
     END IF;
     IF l_MesType NOT IN('RM002', 'RM003', 'RM006') THEN
       l_XMLProduct := CheckXMLProduct;
       IF l_XMLProduct = chr(1) THEN
         p_Text := 'В системе не реализована обработка продукта XML из загружаемого сообщения. Необходимо выполнить обработку сообщения вручную.';
         p_Protocol := Get_RowProtocol(l_MesType, l_XMLProduct, p_Text);
         Return 2;
       END IF;
       l_SRSType := GetTypeSRS(p_XMLProduct => l_XMLProduct);
       IF l_SRSType IS NULL THEN
         p_Text := 'Не удалось определить тип СРС.';
         p_Protocol := Get_RowProtocol(l_MesType, l_XMLProduct, p_Text);
         Return 2;
       END IF;
       IF CheckElementProductType = 1 THEN
         l_ProductType := CheckProductType(l_SRSType, l_Reporting);
         IF nvl(l_Reporting, chr(0)) <> chr(88) THEN
           p_Text := 'Обработка типа продукта "'||l_ProductType||'" не реализована в системе. Необходимо выполнить обработку сообщения вручную.';
           p_Protocol := Get_RowProtocol(l_MesType, l_XMLProduct, p_Text);
           Return 2;
         END IF;
       END IF;
       IF CheckElementSRS(p_SRSType => l_SRSType,
                          p_Element => l_Element) > 0 THEN
         p_Text := 'В загружаемом сообщении есть элемент, обработка которого в системе не реализована: '||l_Element||'. Необходимо выполнить обработку сообщения вручную.';
         p_Protocol := Get_RowProtocol(l_MesType, l_XMLProduct, p_Text);
         Return 2;
       END IF;
     END IF;
     p_InterMesID := DIR_GENERALINF_DBT_SEQ.NEXTVAL;
     InsertGENERALINF (p_InterMesID, l_Root, l_MesType, l_Count, l_XMLProduct, l_ProductType, p_Text, l_SRSType);
     IF l_Count <> 1 THEN
       p_Text := 'Ошибка создания СРС '||l_MesType||
        --' p_InterMesID='||p_InterMesID||' l_Root='||l_Root||' l_Count='||l_Count||' l_XMLProduct='||l_XMLProduct||' l_ProductType='||
        --  l_ProductType||' l_SRSType='||l_SRSType||
       ': '||p_Text||'.';
       p_Protocol := Get_RowProtocol(l_MesType, l_XMLProduct, p_Text);
       Return 1;
     END IF;
     IF l_SRSType IS NOT NULL /*<> 'CM083'*/ OR l_MesType IN('RM002', 'RM003', 'RM006') THEN
       InsertTableParty (p_InterMesID, l_SRSType);
     END IF;
     l_Count := 0;
     IF (l_MesType NOT IN('RM002', 'RM003')) THEN
       IF l_MesType = 'RM006' THEN
         l_Count := InsertTableDifferences(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM010' THEN /*Karlov*/
         l_Count := 0;
       ELSIF l_SRSType = 'CM021' THEN
         l_Count := InsertTableCM021(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM022' THEN
         l_Count := InsertTableCM022(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM023' THEN
         l_Count := InsertTableCM023(p_InterMesID, l_ProductType, p_Text);
       ELSIF l_SRSType = 'CM032' THEN
         l_Count := InsertTableCM032(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM041' THEN
         l_Count := InsertTableCM041(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM042' THEN
         l_Count := InsertTableCM042(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM043' THEN
         l_Count := InsertTableCM043(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM044' THEN
         l_Count := InsertTableCM044(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM045' THEN
         l_Count := InsertTableCM045(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM046' THEN
         l_Count := InsertTableCM046(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM047' THEN
         l_Count := InsertTableCM047(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM048' THEN
         l_Count := InsertTableCM048(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM051' THEN
         l_Count := InsertTableCM051(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM053' THEN
         l_Count := InsertTableCM053(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM083' THEN
         l_Count := InsertTableParty083 (p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM093' THEN
         l_Count := InsertTableCM093(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM094' THEN
         l_Count := InsertTableCM094(p_InterMesID, p_Text);
       ELSIF l_SRSType = 'CM092' THEN
         l_Count := InsertTableCM092(p_InterMesID, p_Text);
       END IF;
       IF l_Count <> 0 THEN
         p_Protocol := Get_RowProtocol(l_MesType, l_XMLProduct, p_Text);
         Return 2;
       END IF;
     END IF;
     InsertDlGrDoc(p_op_id, p_InterMesID, 0);
     p_Text := 'Создано СРС '||l_MesType||', ИД = '||p_InterMesID||'.';
     p_Protocol := Get_RowProtocol(l_MesType, l_XMLProduct, null);/*Karlov*/
     Return 0;
   end GenerationSRSFromInput;

   PROCEDURE GetTypeMessage (p_Root out varchar2, p_MesType out varchar2, p_Generate out char) IS
   /*Определение типа входящего СРС*/
   begin
     SELECT decode(d.t_messagecode,
                   'RM006', 'nsdext:'||x.column_value.getRootElement(),
                   x.column_value.getRootElement()),
            d.t_messagecode,
            d.t_generate_report
       INTO p_Root,
            p_MesType,
            p_Generate
       FROM DIR_TYPESMESSAGE_DBT d,
            DIR_SRS_TMP t,
            table(xmlsequence(xmltype(t.t_fmtclobdata_xxxx))) x
      WHERE d.t_xmlname = x.column_value.getRootElement()
        AND d.t_xmlproduct = chr(1)
        AND d.t_messagecode like 'RM%';
   exception
     WHEN NO_DATA_FOUND THEN
       NULL;
   end GetTypeMessage;

   FUNCTION GetTypeSRS (p_XMLProduct in varchar2) return varchar2 AS
   /*Определение типа исходящего СРС*/
     l_SRSType VARCHAR2(5);
   begin
     IF p_XMLProduct = 'fxSwap' THEN
       l_SRSType := 'CM021';
     ELSIF p_XMLProduct = 'fxSingleLeg' THEN
       l_SRSType := 'CM022';
     ELSIF p_XMLProduct = 'fxOption' THEN
       l_SRSType := 'CM023';
     ELSIF p_XMLProduct = 'swap' THEN
       l_SRSType := 'CM032';
     ELSIF p_XMLProduct = 'repo' THEN
       l_SRSType := 'CM041';
     ELSIF p_XMLProduct = 'bondSimpleTransaction' THEN
       l_SRSType := 'CM042';
     ELSIF p_XMLProduct = 'bondForward' THEN
       l_SRSType := 'CM043';
     ELSIF p_XMLProduct = 'bondOption' THEN
       l_SRSType := 'CM044';
     ELSIF p_XMLProduct = 'bondBasketOption' THEN
       l_SRSType := 'CM045';
     ELSIF p_XMLProduct = 'equitySimpleTransaction' THEN
       l_SRSType := 'CM046';
     ELSIF p_XMLProduct = 'equityForward' THEN
       l_SRSType := 'CM047';
     ELSIF p_XMLProduct = 'equityOption' THEN
       l_SRSType := 'CM048';
     ELSIF p_XMLProduct = 'commodityForward' THEN
       l_SRSType := 'CM051';
     ELSIF p_XMLProduct = 'commoditySwap' THEN
       l_SRSType := 'CM053';
     ELSIF p_XMLProduct = 'executionStatus' THEN
       l_SRSType := 'CM093';
     ELSIF p_XMLProduct = 'markToMarketValuation' THEN
       l_SRSType := 'CM094';
     ELSIF p_XMLProduct = 'transfersAndExecution' THEN
       l_SRSType := 'CM092';
     ELSIF p_XMLProduct = 'repoBulkReport' THEN
       l_SRSType := 'CM083';
     ELSIF p_XMLProduct = 'masterAgreementTerms' THEN
       l_SRSType := 'CM010';
     END IF;
     Return l_SRSType;
   end GetTypeSRS;

   PROCEDURE InsertGENERALINF (p_InterMesID in integer, p_Root in varchar2, p_MesType in varchar2, p_Count out integer, p_XMLProduct in varchar2, p_ProductType in varchar2, p_Text out varchar2, p_SRSType in varchar2) IS
   /*Генерация входящего СРС*/
     l_Insert VARCHAR2(16000);
     l_Select VARCHAR2(16000);
     l_From   VARCHAR2(16000);
     l_Query  VARCHAR2(32000);
   begin
     GetInsertGENERALINF (p_MesType, p_SRSType, l_Insert, l_Select, l_From);
     l_Query := l_Insert||chr(13)||l_Select||chr(13)||
                'FROM DIR_SRS_TMP t,
                      XMLTABLE (
                        xmlnamespaces (
                           DEFAULT ''http://www.fpml.org/FpML-5/recordkeeping'',
                           ''http://www.fpml.org/FpML-5/recordkeeping/nsd-ext'' AS "nsdext",
                           ''http://www.fpml.org/FpML-5/ext'' AS "fpmlext"),
                        ''//'||p_Root||'''
                        PASSING xmltype(t.t_fmtclobdata_xxxx)
                        COLUMNS '||l_From;
    --dbms_output.put_line(l_Query);
     IF p_MesType in ('RM001', 'RM005') THEN
      EXECUTE IMMEDIATE l_Query USING p_InterMesID, p_MesType, p_XMLProduct, p_ProductType, p_ProductType, p_SRSType, p_ProductType, p_SRSType, p_SRSType, p_SRSType;
     ELSE
      -- В RM002 RM003 RM006 передаваемых переменных нужно меньше DEF-40207
      EXECUTE IMMEDIATE l_Query USING p_InterMesID, p_MesType, p_XMLProduct, p_ProductType, p_ProductType, p_SRSType;
     END IF;
     p_Count := SQL%ROWCOUNT;
   exception
     WHEN OTHERS THEN
       p_Count := 0;
       p_Text := SQLERRM;
   end InsertGENERALINF;

   PROCEDURE GetInsertGENERALINF (p_MesType in varchar2, p_SRSType in varchar2, p_Insert out varchar2, p_Select out varchar2, p_From out varchar2) IS
   /*Генерация входящего СРС*/
     l_Insert     VARCHAR2(16000);
     l_Select     VARCHAR2(16000);
     l_From       VARCHAR2(16000);
     l_FromParty  VARCHAR2(16000);
     l_FromClient VARCHAR2(16000);
     l_Amendment  INTEGER;
     l_Count      INTEGER;
   begin
     GetTextServiceAttrSRS(p_MesType, l_Insert, l_Select, l_From);
     GetTextCommonAttrSRS (l_Insert, l_Select, l_From);
     GetTextGeneralAttr (l_Insert, l_Select, l_From);
     IF p_MesType = 'RM001' THEN
       GetTextGeneralAttrRM001 (l_Insert, l_Select, l_From);
     ELSIF p_MesType = 'RM002' THEN
       GetTextGeneralAttrRM002 (l_Insert, l_Select, l_From);
     ELSIF p_MesType = 'RM003' THEN
       GetTextGeneralAttrRM003 (l_Insert, l_Select, l_From);
     ELSIF p_MesType = 'RM005' THEN
       l_Amendment := CheckAmendment;
       GetTextGeneralAttrRM005 (l_Insert, l_Select, l_From, l_Amendment);
     ELSIF p_MesType = 'RM006' THEN
       GetTextGeneralAttrRM006 (l_Insert, l_Select, l_From);
     END IF;
     SELECT count(*)
       INTO l_Count
       FROM DIR_SRS_TMP t,
            XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                   'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                   'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                    '//partyTradeIdentifier'
                    PASSING xmltype(t.t_fmtclobdata_xxxx)
                    COLUMNS partyTradeIdentifier XMLTYPE PATH '/*') x;
     IF l_Count > 0 THEN
       GetTextGeneralAttrParty ('TradeRepository', p_SRSType, l_Insert, l_Select, l_FromParty);
       GetTextGeneralAttrParty ('Party1', p_SRSType, l_Insert, l_Select, l_FromParty);
       GetTextGeneralAttrParty ('Party2', p_SRSType, l_Insert, l_Select, l_FromParty);
       GetTextGeneralAttrParty ('UTIGeneratingParty', p_SRSType, l_Insert, l_Select, l_FromParty);
     END IF;
     FOR c IN (SELECT x.* FROM DIR_SRS_TMP t,
        XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
        'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
        'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
        '//nsdext:clientDetails'
        PASSING xmltype(t.t_fmtclobdata_xxxx)
        COLUMNS Party PATH '/*/nsdext:servicingParty/@href') x) 
     LOOP
        GetTextGeneralAttrClient (c.party, l_Insert, l_Select, l_FromClient);
     END LOOP;
     p_Insert := 'INSERT INTO DIR_GENERALINF_DBT ('||l_Insert||')';
     p_Select := 'SELECT '||l_Select;
     p_From := l_From||') x'||
               l_FromParty||
               l_FromClient;
   end GetInsertGENERALINF;

   FUNCTION CheckXMLProduct return varchar2 as
   /*Проверка продукта XML*/
     l_xmlproduct VARCHAR2(35);
   begin
     SELECT x.product.GetRootElement()
       INTO l_xmlproduct
       FROM DIR_TYPESMESSAGE_DBT tm,
            DIR_SRS_TMP t,
            XMLTABLE (
               xmlnamespaces (
                  DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                          'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                          'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                    '//*/child::node()'
                    PASSING xmltype(t.t_fmtclobdata_xxxx)
                    COLUMNS product XMLTYPE PATH '/*') x
      WHERE upper(tm.t_xmlproduct) = upper(x.product.GetRootElement())
        AND tm.t_generate_report = 'X';
     Return l_xmlproduct;
   exception
     WHEN NO_DATA_FOUND THEN
       Return chr(1);
   end CheckXMLProduct;

   FUNCTION CheckElementProductType return integer as
     l_Count INTEGER;
   begin
     SELECT count(*)
       INTO l_Count
       FROM DIR_SRS_TMP t,
            XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                   'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                   'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                    '//productType'
                    PASSING xmltype(t.t_fmtclobdata_xxxx)
                    COLUMNS productType XMLTYPE PATH '/*') x;
     Return l_Count;
   end CheckElementProductType;

   FUNCTION CheckProductType (p_SRSType in varchar2, p_Reporting out char) return varchar2 as
   /*Проверки типа продукта*/
     l_ProductType VARCHAR2(80);
   begin
     SELECT x.productType, max(nvl(sm.t_reporting, chr(1)))
       INTO l_ProductType, p_Reporting
       FROM (select t_id, t_producttype
               from DIR_TYPEPRODUCT_DBT
              where t_messagecode = p_SRSType) tr,
            DIR_SETUPMESSAGE_DBT sm,
            DIR_SRS_TMP t,
            XMLTABLE (
               xmlnamespaces (
                  DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                          'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                          'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                    '//productType'
                    PASSING xmltype(t.t_fmtclobdata_xxxx)
                    COLUMNS productType PATH '/*') x
      WHERE x.productType = tr.t_producttype(+)
        AND tr.t_id = sm.t_typeproduct(+)
      GROUP BY x.productType;
     Return l_ProductType;
   exception
     WHEN NO_DATA_FOUND THEN
       Return chr(0);
   end CheckProductType;

   FUNCTION CheckElementSRS (p_SRSType in varchar2, p_Element out varchar2) return integer as
     l_sql   VARCHAR2(1024);
     l_Count INTEGER;
   begin
     FOR c IN(SELECT substr(t.t_pathmessage, instr(t.t_pathmessage, 'trade')) elementSRS, t.t_name
                FROM DIR_TEMPLATEMESSAGE_DBT t
               WHERE t.t_attribute = 'N'
                 AND t.t_pathmessage <> chr(1)
                 AND t.t_messagetype = p_SRSType) LOOP
       l_sql := 'SELECT count(*)
                   FROM DIR_SRS_TMP t,
                        XMLTABLE (
                            xmlnamespaces (
                              DEFAULT ''http://www.fpml.org/FpML-5/recordkeeping'',
                                      ''http://www.fpml.org/FpML-5/recordkeeping/nsd-ext'' AS "nsdext",
                                      ''http://www.fpml.org/FpML-5/ext'' AS "fpmlext",
                                      ''http://www.fpml.org/FpML-5/recordkeeping'' AS "fpml"),
                            ''//'||c.elementSRS||'''
                            PASSING xmltype(t.t_fmtclobdata_xxxx)
                            COLUMNS elementSRS XMLTYPE PATH ''/*'') x';
       EXECUTE IMMEDIATE l_sql INTO l_Count;
       IF l_Count > 0 THEN
         p_Element := c.t_name;
         EXIT;
       END IF;
     END LOOP;
     Return l_Count;
   end CheckElementSRS;

   FUNCTION CheckAmendment return integer as
     l_Cnt INTEGER;
   begin
     SELECT count(*)
       INTO l_Cnt
       FROM DIR_SRS_TMP t,
            XMLTABLE (
               xmlnamespaces (
                  DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                          'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                          'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                    'nonpublicExecutionReport/child::node()'
                    PASSING xmltype(t.t_fmtclobdata_xxxx)
                    COLUMNS teg XMLTYPE PATH '/*') x
      WHERE x.teg.getRootElement() = 'amendment';
     Return l_Cnt;
   end CheckAmendment;

   FUNCTION FindOriginalMessage (p_Content in clob, p_Root in varchar2) return integer as
     l_RefInterMesID INTEGER;
   begin
     execute immediate('
       SELECT gen.t_internalmessageid
         FROM DIR_GENERALINF_DBT gen,
              XMLTABLE (
                 xmlnamespaces (
                    DEFAULT ''http://www.fpml.org/FpML-5/recordkeeping'',
                            ''http://www.fpml.org/FpML-5/recordkeeping/nsd-ext'' AS "nsdext"),
                      ''//'||p_Root||'''
                      PASSING xmltype('''||p_Content||''')
                      COLUMNS Receiver  PATH ''header/sendTo'',
                              MessageId PATH ''header/messageId'') x
        WHERE gen.t_sendby = x.Receiver
          AND gen.t_messageid = x.RefMessageID
     ') into l_RefInterMesID;
     Return l_RefInterMesID;
   exception
     WHEN NO_DATA_FOUND THEN
       Return NULL;
   end FindOriginalMessage;

   PROCEDURE GetTextServiceAttrSRS (p_MesType in varchar2, p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2) IS
   /*Генерация входящего СРС*/
   begin
     p_Insert := 'T_INTERNALMESSAGEID,
                  T_RSTATUS,
                  T_MESSAGETYPE,
                  T_PRDATE,
                  T_PRTIME,
                  T_AMENDMENT,
                  T_LOADDATE,
                  T_LOADTIME,
                  '||CASE WHEN p_MesType IN('RM003')
                          THEN 'T_JOURNALDATE,'
                     END||'
                  '||CASE WHEN p_MesType = 'RM005'
                          THEN 'T_REGISTRATIONDATE,'
                     END||'
                  T_XMLPRODUCT,
                  T_PRODUCTTYPE,
                  T_PRODUCTTYPEID,';
     p_Select := ':InterMesID,
                  '||RSTATUS_UPLOADED||',
                  :MesType,
                  TRUNC(SYSDATE),
                  TO_DATE(''01.01.0001 ''||TO_CHAR(SYSDATE, ''HH24:MI:SS''),
                          ''DD.MM.YYYY HH24:MI:SS''),
                  decode('||CheckAmendment||', 0, CHR(0), ''X''),
                  trunc(to_date(t.t_loaddatetime, ''dd.mm.yy hh24:mi:ss'')),
                  TO_DATE(''01.01.0001 ''||TO_CHAR(t.t_loaddatetime, ''HH24:MI:SS''), ''DD.MM.YYYY HH24:MI:SS''),
                  '||CASE WHEN p_MesType IN('RM003')
                          THEN 'NULL,'
                     END||'
                  '||CASE WHEN p_MesType = 'RM005'
                          THEN 'NULL,'
                     END||'
                  :XMLProduct,
                  :ProductType,
                  (select nvl((select prt.t_ID
                    from dir_typeproduct_dbt prt
                                where LOWER(prt.t_producttype) = LOWER(:ProductType)
                                  and prt.t_messagecode = :SRSType), 0) from dual) ,';
     p_From := 'AgrDate DATE PATH ''amendment/agreementDate''';
   end GetTextServiceAttrSRS;
   PROCEDURE GetTextCommonAttrSRS (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2) IS
   /*Генерация входящего СРС*/
   begin
     p_Insert := p_Insert||'
                  T_MESSAGEID,
                  T_INREPLYTO,
                  T_SENDBY,
                  T_SENDBYID,
                  T_SENDTO,
                  T_SENDTOID,
                  T_CREATIONDATE,
                  T_CREATIONTIME,
                  T_VERSION';
     p_Select := p_Select||'
                  x.MessageId,
                  x.RefMessageID,
                  x.Sender,
                  (select obj.t_objectID from dobjcode_dbt obj where obj.t_objectType =' || OBJTYPE_PARTY || ' and obj.t_codekind =' || PTCK_RIC || ' and obj.t_code = x.Sender and obj.t_State = 0 ),
                  x.Receiver,
                  (select obj.t_objectID from dobjcode_dbt obj where obj.t_objectType =' || OBJTYPE_PARTY || ' and obj.t_codekind =' || PTCK_RIC || ' and obj.t_code = x.Receiver and obj.t_State = 0),
                  TRUNC(TO_DATE (REPLACE (x.DateCreatMessage, ''T'', '' ''),
                                 ''YYYY-MM-DD HH24:MI:SS'')),
                  TO_DATE(''01.01.0001 ''||TO_CHAR (TO_DATE (REPLACE (x.DateCreatMessage, ''T'', '' ''),
                                                             ''YYYY-MM-DD HH24:MI:SS''),
                                                    ''HH24:MI:SS''),
                          ''DD.MM.YYYY HH24:MI:SS''),
                  x.Version';
     p_From := p_From||',
                MessageId        PATH ''header/messageId'',
                RefMessageID     PATH ''header/inReplyTo'',
                Sender           PATH ''header/sentBy'',
                Receiver         PATH ''header/sendTo'',
                DateCreatMessage PATH ''header/creationTimestamp'',
                Version          PATH ''header/implementationSpecification/version''';
   end GetTextCommonAttrSRS;

   PROCEDURE GetTextGeneralAttr (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2) IS
   /*Генерация входящего СРС*/
   begin
     p_Insert := p_Insert||',
                  T_CORRELATIONID';
     p_Select := p_Select||',
                  x.correlationId';
     p_From := p_From||',
                correlationId PATH ''correlationId''';
   end GetTextGeneralAttr;

   PROCEDURE GetTextGeneralAttrRM001 (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2) IS
   /*Генерация входящего СРС*/
   begin    
     p_Insert := p_Insert||',
                  T_OPERDATE,
                  T_ASOFDATE,
                  T_AGREEMENTDATE,
                  T_EFFECTIVEDATE,
                  t_reportingregime,
                  t_nonstandardterms,
                  t_executionvenuetype,
                  t_tradedate,
                  t_margintype,
                  t_margintypecode,
                  t_colaterallform,
                  t_colaterallformcode,
                  t_included,
                  t_excluded,
                  t_brokerid,
                  t_cleared,
                  T_IDCLEARSETTLEMENTTYPE,
                  t_clearsettlementtype,
                  t_clearingorgcode,
                  t_cleareddate,
                  t_clearingdate,
                  t_clearingtime,
                  t_centralcounterparty,
                  t_clearingmember,
                  T_IDRECONTYPE,
                  t_recontype,
                  T_IDCLEARSETTLMETHOD,
                  t_clearsettlmethod,
                  T_IDCONFMETHOD,
                  t_confmethod,
                  t_automaticexecution,
                  t_affparties,
                  T_TRADEREGTYPE,
                  t_traderegtypecode,
                  t_startagreementdate,
                  t_endagreementdate,
                  t_specificcode,
                  t_relcode,
                  t_hedgeInfo,
                  t_partyref_traderepository3,
                  t_traderepository_reportid,
                  t_obligationstatus,
                  t_spotlegseyttldate,
                  t_forwardlegseyttldate';
     p_Select := p_Select||',
                  x.regdate,
                  x.asOfDate,
                  x.aggdate,
                  x.effdate,
                  x.reportingregime,
                  x.nonstandardterms,
                  x.executionvenuetype,
                  x.tradedate,
                  nvl((select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_PROVISION_TYPE||'
                     and objtype.T_CODE = x.margintype), 0), 
                  decode((select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_PROVISION_TYPE||'
                     and objtype.T_CODE = x.margintype), NULL, chr(1), x.margintype),
                  nvl((select objform.T_ELEMENT
                     from DLLVALUES_DBT objform
                    where objform.T_LIST = '||OBJTYPE_PROVISION_FORM||'
                      and objform.T_CODE = x.collateralform), 0),
                  decode((select objform.T_ELEMENT
                     from DLLVALUES_DBT objform
                    where objform.T_LIST = '||OBJTYPE_PROVISION_FORM||'
                      and objform.T_CODE = x.collateralform), NULL, chr(1), x.collateralform),
                  x.included,
                  x.excluded,
                  x.brokerid,
                  decode( LOWER(x.cleared), ''n'', chr(0), chr(88)),
                  nvl((select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_CALCTYPE||'
                     and objtype.T_CODE = x.clearsettlementtype), 0),
                  decode((select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_CALCTYPE||'
                     and objtype.T_CODE = x.clearsettlementtype), NULL, chr(1), x.clearsettlementtype),
                  x.clearingorgcode,
                  x.cleareddate,
                  TRUNC(TO_DATE (REPLACE (x.ClearingDateTime, ''T'', '' ''),
                                 ''YYYY-MM-DD HH24:MI:SS'')),
                  TO_DATE(''01.01.0001 ''||TO_CHAR (TO_DATE (REPLACE (x.ClearingDateTime, ''T'', '' ''),
                                                             ''YYYY-MM-DD HH24:MI:SS''),
                                                    ''HH24:MI:SS''),
                          ''DD.MM.YYYY HH24:MI:SS''),
                  x.centralcounterparty,
                  x.clearingmember,
                  nvl((select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_REVISE_TYPE||'
                     and objtype.T_CODE = x.recontype), 0),
                  decode((select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_REVISE_TYPE||'
                     and objtype.T_CODE = x.recontype), NULL, chr(1), x.recontype),
                  nvl((select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_CALCMETHOD||'
                     and objtype.T_CODE = x.clearsettlmethod), 0),
                  decode((select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_CALCMETHOD||'
                     and objtype.T_CODE = x.clearsettlmethod), NULL, chr(1), x.clearsettlmethod),
                  nvl((select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_METHOD_TWO_WAY||'
                     and objtype.T_CODE = x.confmethod), 0),
                  decode((select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_METHOD_TWO_WAY||'
                     and objtype.T_CODE = x.confmethod), NULL, chr(1), x.confmethod),
                  decode( LOWER(x.automaticexecution), ''n'', chr(0), chr(88)),
                  decode( LOWER(x.affparties), ''n'', chr(0), chr(88)),
                  nvl((select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_REGDEALTYPE||'
                     and objtype.T_CODE = x.traderegtype), 0),
                  decode((select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_REGDEALTYPE||'
                     and objtype.T_CODE = x.traderegtype), NULL, chr(1), x.traderegtype),
                  x.startagreementdate,
                  x.endagreementdate,
                  x.specificcode,
                  x.relcode,
                  CASE WHEN (select t.t_typecode from dir_typeproduct_dbt t where t.t_producttype = :ProductType and t.t_messagecode = :SRSType) <> ''R''
                       THEN CASE WHEN ((x.hedgeInfo = ''H1'' OR x.hedgeInfo = ''H2'') AND :SRSType BETWEEN ''CM021'' AND ''CM081'')
                                 THEN x.hedgeInfo
                                 WHEN (x.hedgeInfo = ''H3'' AND :SRSType BETWEEN ''CM083'' AND ''CM085'')
                                 THEN x.hedgeInfo
                                 ELSE chr(1) 
                            END
                       ELSE chr(1) 
                  END,
                  x.PartyReference3,
                  x.tradeId_Party3,
                  x.obligationstatus,
                  x.spotlegsettldate,
                  x.forwardlegsettldate';
     p_From := p_From||',
                regdate             DATE PATH ''originalMessage/nsdext:registeredInformation/nsdext:operDay'',
                aggdate             DATE PATH ''amendment/agreementDate'',
                effdate             DATE PATH ''amendment/effectiveDate'',
                asOfDate            DATE PATH ''asOfDate'',
                reportingregime          PATH ''originalMessage/nsdext:registeredInformation/trade/tradeHeader/partyTradeInformation/reportingRegime/name'',
                nonstandardterms         PATH ''originalMessage/nsdext:registeredInformation/trade/tradeHeader/nonStandardTerms'',
                executionvenuetype       PATH ''originalMessage/nsdext:registeredInformation/trade/tradeHeader/executionVenueType'',
                tradedate           DATE PATH ''originalMessage/nsdext:registeredInformation/trade/tradeHeader/tradeDate'',
                margintype               PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:collateral/nsdext:marginType'',
                collateralform           PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:collateral/nsdext:collateralForm'',
                included            DATE PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:collateral/nsdext:dateTradeIncludedIntoPortfolio'',
                excluded            DATE PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:collateral/nsdext:dateTradeExcludedFromPortfolio'',
                brokerid                 PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:reportingBrokerID'',
                cleared                  PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:cleared'',
                clearsettlementtype      PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:clearSettlementType'',
                clearingorgcode          PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:clearingOrganizationCode'',
                cleareddate         DATE PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:clearedDate'',
                ClearingDateTime         PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:clearingDateTime'',
                centralcounterparty      PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:clearingCentralCounterpartyCode'',
                clearingmember           PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:clearingMemberID'',
                recontype                PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:reconciliationType'',
                clearsettlmethod         PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:clearSettlementMethod'',
                confmethod               PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:confirmationMethod'',
                automaticexecution       PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:automaticExecution'',
                affparties               PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:partiesAreAffiliated'',
                traderegtype             PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:regulatoryStatus'',
                startagreementdate  DATE PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:startAgreementDate'',
                endagreementdate    DATE PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:endAgreementDate'',
                specificcode             PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:productSpecificCodes/nsdext:productGeneralCodes/nsdext:classificationCode/nsdext:specificCode'',
                relcode                  PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:productSpecificCodes/nsdext:productGeneralCodes/nsdext:classificationCode/nsdext:code'',
                hedgeInfo                PATH ''originalMessage/nsdext:registeredInformation/trade/nsdext:nsdSpecificTradeFields/nsdext:hedgeInfo'',
                PartyReference3          PATH ''nsdext:executionStatus/nsdext:repositoryMessageIdentifier/partyReference/@href'',
                tradeId_Party3           PATH ''nsdext:executionStatus/nsdext:repositoryMessageIdentifier/tradeId'',
                obligationstatus         PATH ''repoBulkReport/tradesObligationStatus'',
                spotlegsettldate    DATE PATH ''repoBulkReport/spotLegSettlDate'',
                forwardlegsettldate DATE PATH ''repoBulkReport/forwardLegSettlDate''';
   end GetTextGeneralAttrRM001;

   PROCEDURE GetTextGeneralAttrRM002 (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2) IS
   DescLen Integer := 0;
   begin
      select data_length into DescLen from user_tab_columns 
        where table_name='DIR_GENERALINF_DBT' and column_name='T_REASONDESCRIPTION';
     p_Insert := p_Insert||',
                  T_REASONCODE,
                  T_REASONDESCRIPTION,
                  T_JOURNALDATE';
     p_Select := p_Select||',
                  x.ErrorCode,
                  substr(x.Descrip, 1, ' || DescLen || '),
                  x.Journaldate';
     p_From := p_From||',
                ErrorCode PATH ''reason/reasonCode'',
                Descrip   PATH ''reason/description'',
                Journaldate DATE PATH ''additionalData/originalMessage/nsdext:rejectionInformation/nsdext:operDay''';
   end GetTextGeneralAttrRM002;

   PROCEDURE GetTextGeneralAttrRM003 (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2) IS
   /*Генерация входящего СРС*/
   begin
     /*p_Insert := p_Insert||',
                  T_STATUSCODE';
     p_Select := p_Select||',
                  x.eventCode';
     p_From := p_From||',
                eventCode  PATH ''statusItem/status''';*/ /* DAN */
      p_Insert := p_Insert||',
                  T_STATUSCODE, T_PARTYREF_TRADEREPOSITORY, T_TRADEID_TRADEREPOSITORY';
      p_Select := p_Select||',
                  x.eventCode, ''TradeRepository'', x.TRADEIDRP';
      p_From := p_From||',
                eventCode PATH ''statusItem/status'',
                TRADEIDRP PATH ''statusItem/eventIdentifier/tradeIdentifier/tradeId''';
   end GetTextGeneralAttrRM003;

   PROCEDURE GetTextGeneralAttrRM005 (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2, p_Amendment in integer) IS
   /*Генерация входящего СРС*/
   begin
     p_Insert := p_Insert||',
                  T_ASOFDATE,
                  T_AGREEMENTDATE,
                  T_EFFECTIVEDATE,
                  t_reportingregime,
                  t_nonstandardterms,
                  t_executionvenuetype,
                  t_tradedate,
                  t_margintype,
                  t_margintypecode,
                  t_colaterallform,
                  t_colaterallformcode,
                  t_included,
                  t_excluded,
                  t_brokerid,
                  t_cleared,
                  T_IDCLEARSETTLEMENTTYPE,
                  t_clearsettlementtype,
                  t_clearingorgcode,
                  t_cleareddate,
                  t_clearingdate,
                  t_clearingtime,
                  t_centralcounterparty,
                  t_clearingmember,
                  T_IDRECONTYPE,
                  t_recontype,
                  T_IDCLEARSETTLMETHOD,
                  t_clearsettlmethod,
                  T_IDCONFMETHOD,
                  t_confmethod,
                  t_automaticexecution,
                  t_affparties,
                  T_TRADEREGTYPE,
                  t_traderegtypecode,
                  t_startagreementdate,
                  t_endagreementdate,
                  t_specificcode,
                  t_relcode,
                  t_hedgeInfo,
                  t_partyref_traderepository3,
                  t_traderepository_reportid';
     p_Select := p_Select||',
                  x.asOfDate,
                  x.aggdate,
                  x.effdate,
                  x.reportingregime,
                  x.nonstandardterms,
                  x.executionvenuetype,
                  x.tradedate,
                  (select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_PROVISION_TYPE||'
                     and objtype.T_CODE = x.margintype),
                  x.margintype,
                  (select objform.T_ELEMENT
                     from DLLVALUES_DBT objform
                    where objform.T_LIST = '||OBJTYPE_PROVISION_FORM||'
                      and objform.T_CODE = x.collateralform),
                  x.collateralform,
                  x.included,
                  x.excluded,
                  x.brokerid,
                  decode( LOWER(x.cleared), ''n'', chr(0), chr(88)),
                  (select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_CALCTYPE||'
                     and objtype.T_CODE = x.clearsettlementtype),
                  x.clearsettlementtype,
                  x.clearingorgcode,
                  x.cleareddate,
                  TRUNC(TO_DATE (REPLACE (x.ClearingDateTime, ''T'', '' ''),
                                 ''YYYY-MM-DD HH24:MI:SS'')),
                  TO_DATE(''01.01.0001 ''||TO_CHAR (TO_DATE (REPLACE (x.ClearingDateTime, ''T'', '' ''),
                                                             ''YYYY-MM-DD HH24:MI:SS''),
                                                    ''HH24:MI:SS''),
                          ''DD.MM.YYYY HH24:MI:SS''),
                  x.centralcounterparty,
                  x.clearingmember,
                  (select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_REVISE_TYPE||'
                     and objtype.T_CODE = x.recontype),
                  x.recontype,
                  (select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_CALCMETHOD||'
                     and objtype.T_CODE = x.clearsettlmethod),
                  x.clearsettlmethod,
                  (select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_METHOD_TWO_WAY||'
                     and objtype.T_CODE = x.confmethod),
                  x.confmethod,
                  decode( LOWER(x.automaticexecution), ''n'', chr(0), chr(88)),
                  decode( LOWER(x.affparties), ''n'', chr(0), chr(88)),
                  (select objtype.T_ELEMENT
                     from DLLVALUES_DBT objtype
                    where objtype.T_LIST = '||OBJTYPE_REGDEALTYPE||'
                     and objtype.T_CODE = x.traderegtype),
                  x.traderegtype,
                  x.startagreementdate,
                  x.endagreementdate,
                  x.specificcode,
                  x.relcode,
                  CASE WHEN (select t.t_typecode from dir_typeproduct_dbt t where t.t_producttype = :ProductType and t.t_messagecode = :SRSType) <> ''R''
                       THEN CASE WHEN ((x.hedgeInfo = ''H1'' OR x.hedgeInfo = ''H2'') AND :SRSType BETWEEN ''CM021'' AND ''CM081'')
                                 THEN x.hedgeInfo
                                 WHEN (x.hedgeInfo = ''H3'' AND :SRSType BETWEEN ''CM083'' AND ''CM085'')
                                 THEN x.hedgeInfo
                                 ELSE chr(1) 
                            END
                       ELSE chr(1) 
                  END,
                  x.PartyReference3,
                  x.tradeId_Party3';
     IF p_Amendment = 0 THEN
       p_From := p_From||',
                  asOfDate            DATE PATH ''asOfDate'',
                  aggdate             DATE PATH ''agreementDate'',
                  effdate             DATE PATH ''effectiveDate'',
                  reportingregime          PATH ''trade/tradeHeader/partyTradeInformation/reportingRegime/name'',
                  nonstandardterms         PATH ''trade/tradeHeader/nonStandardTerms'',
                  executionvenuetype       PATH ''trade/tradeHeader/executionVenueType'',
                  tradedate           DATE PATH ''trade/tradeHeader/tradeDate'',
                  margintype               PATH ''trade/nsdext:collateral/nsdext:marginType'',
                  collateralform           PATH ''trade/nsdext:collateral/nsdext:collateralForm'',
                  included            DATE PATH ''trade/nsdext:collateral/nsdext:dateTradeIncludedIntoPortfolio'',
                  excluded            DATE PATH ''trade/nsdext:collateral/nsdext:dateTradeExcludedFromPortfolio'',
                  brokerid                 PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:reportingBrokerID'',
                  cleared                  PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:cleared'',
                  clearsettlementtype      PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:clearSettlementType'',
                  clearingorgcode          PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:clearingOrganizationCode'',
                  cleareddate         DATE PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:clearedDate'',
                  ClearingDateTime         PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:clearingDateTime'',
                  centralcounterparty      PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:clearingCentralCounterpartyCode'',
                  clearingmember           PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:clearingMemberID'',
                  recontype                PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:reconciliationType'',
                  clearsettlmethod         PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:clearSettlementMethod'',
                  confmethod               PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:confirmationMethod'',
                  automaticexecution       PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:automaticExecution'',
                  affparties               PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:partiesAreAffiliated'',
                  traderegtype             PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:regulatoryStatus'',
                  startagreementdate  DATE PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:startAgreementDate'',
                  endagreementdate    DATE PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:endAgreementDate'',
                  specificcode             PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:productSpecificCodes/nsdext:productGeneralCodes/nsdext:classificationCode/nsdext:specificCode'',
                  relcode                  PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:productSpecificCodes/nsdext:productGeneralCodes/nsdext:classificationCode/nsdext:code'',
                  hedgeInfo                PATH ''trade/nsdext:nsdSpecificTradeFields/nsdext:hedgeInfo'',
                  PartyReference3          PATH ''nsdext:executionStatus/nsdext:repositoryMessageIdentifier/partyReference/@href'',
                  tradeId_Party3           PATH ''nsdext:executionStatus/nsdext:repositoryMessageIdentifier/tradeId''';
     ELSE
       p_From := p_From||',
                  asOfDate            DATE PATH ''asOfDate'',
                  aggdate             DATE PATH ''amendment/agreementDate'',
                  effdate             DATE PATH ''amendment/effectiveDate'',
                  reportingregime          PATH ''amendment/trade/tradeHeader/partyTradeInformation/reportingRegime/name'',
                  nonstandardterms         PATH ''amendment/trade/tradeHeader/nonStandardTerms'',
                  executionvenuetype       PATH ''amendment/trade/tradeHeader/executionVenueType'',
                  tradedate           DATE PATH ''amendment/trade/tradeHeader/tradeDate'',
                  margintype               PATH ''amendment/trade/nsdext:collateral/nsdext:marginType'',
                  collateralform           PATH ''amendment/trade/nsdext:collateral/nsdext:collateralForm'',
                  included            DATE PATH ''amendment/trade/nsdext:collateral/nsdext:dateTradeIncludedIntoPortfolio'',
                  excluded            DATE PATH ''amendment/trade/nsdext:collateral/nsdext:dateTradeExcludedFromPortfolio'',
                  brokerid                 PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:reportingBrokerID'',
                  cleared                  PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:cleared'',
                  clearsettlementtype      PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:clearSettlementType'',
                  clearingorgcode          PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:clearingOrganizationCode'',
                  cleareddate         DATE PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:clearedDate'',
                  ClearingDateTime         PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:clearingDateTime'',
                  centralcounterparty      PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:clearingCentralCounterpartyCode'',
                  clearingmember           PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:clearingMemberID'',
                  recontype                PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:reconciliationType'',
                  clearsettlmethod         PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:clearSettlementMethod'',
                  confmethod               PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:confirmationMethod'',
                  automaticexecution       PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:automaticExecution'',
                  affparties               PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:partiesAreAffiliated'',
                  traderegtype             PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:regulatoryStatus'',
                  startagreementdate  DATE PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:startAgreementDate'',
                  endagreementdate    DATE PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:endAgreementDate'',
                  specificcode             PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:productSpecificCodes/nsdext:productGeneralCodes/nsdext:classificationCode/nsdext:specificCode'',
                  relcode                  PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:productSpecificCodes/nsdext:productGeneralCodes/nsdext:classificationCode/nsdext:code'',
                  hedgeInfo                PATH ''amendment/trade/nsdext:nsdSpecificTradeFields/nsdext:hedgeInfo'',
                  PartyReference3          PATH ''nsdext:executionStatus/nsdext:repositoryMessageIdentifier/partyReference/@href'',
                  tradeId_Party3           PATH ''nsdext:executionStatus/nsdext:repositoryMessageIdentifier/tradeId''';
     END IF;
   end GetTextGeneralAttrRM005;

   PROCEDURE GetTextGeneralAttrRM006 (p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2) IS
   /*Генерация входящего СРС*/
   begin
     p_Insert := p_Insert||',
                  T_COUNTERPARTYMESSAGEID';
     p_Select := p_Select||',
                  x.counterPartyMessageId';
     p_From := p_From||',
                counterPartyMessageId  PATH ''nsdext:counterPartyMessageId''';
   end GetTextGeneralAttrRM006;

   PROCEDURE GetTextGeneralAttrParty (p_Party in varchar2, p_SRSType in varchar2, p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2) IS
   /*Генерация входящего СРС*/
   begin
     p_Insert := p_Insert||','||
                 CASE p_Party WHEN 'TradeRepository'
                              THEN 't_PartyRef_TradeRepository,
                                    t_TradeId_TradeRepository,
                                    T_PARTYREFID_TRADEREPOSITORY,
                                    t_LinkID,
                                    t_OriginID_TradeRepository,
                                    T_PARTYREF_ORIGINID_TRADEREP'
                              WHEN 'Party1'
                              THEN 't_PartyRef_Party1,
                                    t_TradeId_Party1,
                                    T_PARTYREF_IDPARTY1,
                                    t_OriginatingID_Party1,
                                    T_PARTYREF_ORIGINID_PARTY1'
                              WHEN 'Party2'
                              THEN 't_PartyRef_Party2,
                                    t_TradeId_Party2,
                                    T_PARTYREF_IDPARTY2,
                                    t_OriginatingID_Party2,
                                    T_PARTYREF_ORIGINID_PARTY2'
                              WHEN 'UTIGeneratingParty'
                              THEN 't_PartyRef_UTIGeneratingParty,
                                    t_TradeId_UTIGeneratingParty,
                                    T_PARTYREF_IDUTIGENPARTY'
                 END;
     p_Select := p_Select||', '||
                 CASE WHEN p_SRSType = 'CM083' AND p_Party = 'UTIGeneratingParty'
                      THEN 'chr(1), chr(1), 0' 
                 ELSE
                    substr(p_Party, 1, 6)||'.party_'||p_Party||', '||
                    substr(p_Party, 1, 6)||'.tradeId_'||p_Party||
                    CASE WHEN p_Party = 'TradeRepository'
                          THEN ', '|| IR_PARTY_KIND_TRADEREPOSITORY ||', '||substr(p_Party, 1, 6)||'.linkId_'||p_Party
                          WHEN p_Party = 'Party1'
                          THEN ', '|| IR_PARTY_KIND_PARTY1
                          WHEN p_Party = 'Party2'
                          THEN ', '|| IR_PARTY_KIND_PARTY2
                          WHEN p_Party = 'UTIGeneratingParty'
                          THEN ', '|| IR_PARTY_KIND_UTIGENPARTY
                    END||
                    CASE WHEN p_Party <> 'UTIGeneratingParty'
                          THEN ', '||substr(p_Party, 1, 6)||'.origTradeId_'||p_Party||', '||substr(p_Party, 1, 6)||'.origParty_'||p_Party
                    END
                 END;
     p_From := p_From||
                 CASE WHEN p_SRSType = 'CM083' AND p_Party = 'UTIGeneratingParty'
                      THEN ''   -- ничего не надо
                 ELSE
                    ', (select y.*
                      from DIR_SRS_TMP t,
                            XMLTABLE (
                              xmlnamespaces (
                                  DEFAULT ''http://www.fpml.org/FpML-5/recordkeeping'',
                                          ''http://www.fpml.org/FpML-5/recordkeeping/nsd-ext'' AS "nsdext"),
                                  ''//partyTradeIdentifier''
                                  PASSING xmltype(t.t_fmtclobdata_xxxx)
                                  COLUMNS party_'||p_Party||'       PATH ''partyReference/@href'',
                                          tradeId_'||p_Party||'     PATH ''tradeId'',
                                          linkId_'||p_Party||'      PATH ''linkId'',
                                          origTradeId_'||p_Party||' PATH ''originatingTradeId/tradeId'',
                                          origParty_'||p_Party||'   PATH ''originatingTradeId/partyReference/@href'') y
                      where y.party_'||p_Party||' = '''||p_Party||''') '||substr(p_Party, 1, 6)
                END;
   end GetTextGeneralAttrParty;

   PROCEDURE GetTextGeneralAttrClient (p_Client in varchar2, p_Insert in out varchar2, p_Select in out varchar2, p_From in out varchar2) IS
   /*Генерация входящего СРС*/
   begin
     p_Insert := p_Insert||','||
                 CASE p_Client WHEN 'Party1'
                               THEN 't_ref_clientparty1,
                                     t_noinf1,
                                     t_owntrade1,
                                     t_clienttype1,
                                     t_clientident1,
                                     t_clientname1,
                                     t_clientcountry1'
                               WHEN 'Party2'
                               THEN 't_refclientparty2,
                                     t_noinf2,
                                     t_owntrade2,
                                     t_clienttype2,
                                     t_clientident2,
                                     t_clientname2,
                                     t_clientcountry2'
                 END;
     p_Select := p_Select||', '||
                 p_Client||'_1.ref_client_'||p_Client||',
                 decode('||p_Client||'_1.noinf_'||p_Client||', ''true'', ''X'', chr(0)),
                 decode('||p_Client||'_1.owntrade_'||p_Client||', ''true'', ''X'', chr(0)), '||
                 p_Client||'_1.clienttype_'||p_Client||', '||
                 p_Client||'_1.clientident_'||p_Client||', '||
                 p_Client||'_1.clientname_'||p_Client||', '||
                 p_Client||'_1.clientcountry_'||p_Client;
     p_From := p_From||',
                (select y.*
                   from DIR_SRS_TMP t,
                        XMLTABLE (
                           xmlnamespaces (
                              DEFAULT ''http://www.fpml.org/FpML-5/recordkeeping'',
                                      ''http://www.fpml.org/FpML-5/recordkeeping/nsd-ext'' AS "nsdext"),
                              ''//nsdext:clientDetails''
                              PASSING xmltype(t.t_fmtclobdata_xxxx)
                              COLUMNS ref_client_'||p_Client||'    PATH ''nsdext:servicingParty/@href'',
                                      noinf_'||p_Client||'         PATH ''nsdext:noInformation'',
                                      owntrade_'||p_Client||'      PATH ''nsdext:ownTrade'',
                                      clienttype_'||p_Client||'    PATH ''nsdext:type'',
                                      clientident_'||p_Client||'   PATH ''nsdext:id'',
                                      clientname_'||p_Client||'    PATH ''nsdext:name'',
                                      clientcountry_'||p_Client||' PATH ''nsdext:country'') y
                  where y.ref_client_'||p_Client||' = '''||p_Client||''') '||p_Client||'_1';
   end GetTextGeneralAttrClient;

   PROCEDURE InsertTableParty (p_InterMesID in integer, l_SRSType in varchar2) is
     l_Insert     VARCHAR2(2000);
     l_Update     VARCHAR2(2000);
     l_NameColumn VARCHAR2(2000);
     l_WhereCond  VARCHAR2(2000);
     l_ID      VARCHAR2(64) := ' ';
     l_count   INTEGER := 0;
     l_kind    INTEGER := 0;
     l_partyID INTEGER := 0;
     l_classID INTEGER := 0;
   begin
     FOR c IN(SELECT x.id, x.partyName, x.classification, x.country, x.organizationType, y.*
                FROM DIR_SRS_TMP t,
                     XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                            'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                            'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                              '//party'
                              PASSING xmltype(t.t_fmtclobdata_xxxx)
                              COLUMNS id               PATH '@id',
                                      partyId  XMLTYPE PATH 'partyId',
                                      partyName        PATH 'partyName',
                                      classification   PATH 'classification',
                                      country          PATH 'country',
                                      organizationType PATH 'organizationType') x,
                     XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                                    'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                                    'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                              'partyId'
                              PASSING x.partyId
                              COLUMNS partyId PATH '/*') y) LOOP
       IF c.id <> l_ID THEN
         IF l_Insert IS NOT NULL THEN
           IF l_count = 2 THEN
             l_Insert := l_Insert||')';
           elsif l_count =1 THEN
             l_Insert := l_Insert||', chr(1) )';
           end if;
           execute immediate l_Insert using p_InterMesID;
           IF l_kind <> 0 THEN
             execute immediate l_Update using p_InterMesID;
           END IF;
           l_Insert := NULL;
         END IF;
         
         IF c.id = 'TradeRepository' THEN
            l_kind := IR_PARTY_KIND_TRADEREPOSITORY;
            l_NameColumn := 'T_PARTYREFID_TRADEREPOSITORY = ' || l_kind; 
            l_WhereCond  := ' AND T_PARTYREFID_TRADEREPOSITORY is NULL ';
         ELSIF c.id = 'Party1' THEN
            l_kind := IR_PARTY_KIND_PARTY1;
            l_NameColumn := 'T_PARTYREF_IDPARTY1 = ' || l_kind;
            l_WhereCond := ' AND T_PARTYREF_IDPARTY1 is NULL ';
         ELSIF c.id = 'Party2' THEN
            l_kind := IR_PARTY_KIND_PARTY2;
            l_NameColumn := 'T_PARTYREF_IDPARTY2 = ' || l_kind;
            l_WhereCond  := ' AND T_PARTYREF_IDPARTY2 is NULL ';
         ELSIF c.id = 'UTIGeneratingParty' AND l_SRSType <> 'CM083' THEN
            l_kind := IR_PARTY_KIND_UTIGENPARTY;
            l_NameColumn := 'T_PARTYREF_IDUTIGENPARTY = ' || l_kind;
            l_WhereCond  := ' AND T_PARTYREF_IDUTIGENPARTY is NULL ';
         ELSIF c.id = 'Sender' THEN
            l_kind := IR_PARTY_KIND_SENDER;
            l_NameColumn := 'T_PARTYREF_IDSender = ' || l_kind;
            l_WhereCond  := ' AND T_PARTYREF_IDSender is NULL ';
         ELSIF c.id = 'Receiver' AND l_SRSType <> 'CM083' THEN
            l_kind := IR_PARTY_KIND_COUNTERPARTY;
            l_NameColumn := 'T_PARTYREF_IDSender = ' || l_kind;
            l_WhereCond  := ' AND T_PARTYREF_IDSender is NULL ';
         ELSE
            l_kind := 0;
         END IF;
         IF l_kind <> 0 THEN
            l_Update := 'UPDATE DIR_GENERALINF_DBT SET ' || l_NameColumn || ' WHERE T_INTERNALMESSAGEID = :l ' || l_WhereCond;
         END IF;
         BEGIN
           select obj.t_objectID INTO l_partyID  from dobjcode_dbt obj where obj.t_objectType = OBJTYPE_PARTY and obj.t_codekind = PTCK_RIC  and obj.t_code = c.partyid and obj.t_State = 0;
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
           l_partyID := 0;
         END;
         BEGIN
           Select obj.t_attrID INTO l_classID from dobjattr_dbt obj
            Where obj.t_objecttype = OBJTYPE_PARTY
              and obj.t_groupid = 80
              and LOWER(obj.t_nameobject) = LOWER(c.classification);
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
           l_classID := 0;
         END;
         IF l_kind <> 0 THEN
            l_Insert := 'INSERT INTO dir_party_dbt(t_internalmessageid, t_partytype, t_partyID, t_partyKind, t_partyname, t_partyid1, t_classification, T_CLASSIFICATIONID, t_country, t_organizationtype, t_partyid2)
    /*DAN*/           VALUES(:1, '''||c.id||''', '''||l_partyID||''', '''||l_kind||''', q''~'||replace(c.partyname,'''')||'~'', '''||c.partyid||''', '''||c.classification||''','''||l_classID||''', '''||c.country||''', '''||c.organizationtype||'''';
    /*DAN*/           --VALUES(:1, '''||c.id||''', '''||l_partyID||''', '''||l_kind||''', '''||replace(c.partyname,'''')||''', '''||c.partyid||''', '''||c.classification||''','''||l_classID||''', '''||c.country||''', '''||c.organizationtype||'''';
            l_count := 1;
         END IF;
       ELSIF l_Insert IS NOT NULL THEN
         l_Insert := l_Insert||', '''||c.partyid||'''';
         l_count := 2;
       END IF;
       l_ID := c.id;
     END LOOP;

     IF l_Insert IS NOT NULL THEN
       IF l_count = 2 THEN
         l_Insert := l_Insert||')';
       elsif l_count =1 THEN
         l_Insert := l_Insert||', chr(1) )';
       end if;
       execute immediate l_Insert using p_InterMesID;
       IF l_kind <> 0 THEN
         execute immediate l_Update using p_InterMesID;
       END IF;
     END IF;
   end InsertTableParty;

   FUNCTION InsertTableParty083 (p_InterMesID in integer, p_Text out varchar2) return integer as
     l_Insert VARCHAR2(2000);
     l_Name   VARCHAR2(100) := ' ';
     l_count   INTEGER := 0;
     l_partyID INTEGER := 0;
     l_classID INTEGER := 0;
     l_PartyNum INTEGER := 1;
     l_IDParty INTEGER;
   begin
     FOR c IN(SELECT t.Counterparty.getrootelement() Counterparty,
                     x.CounterpartyID,
                     x.partyName,
                     x.classification,
                     x.country,
                     x.organizationType,
                     y.*
                FROM (select x.*
                        from DIR_SRS_TMP t,
                             XMLTABLE(xmlnamespaces(default 'http://www.fpml.org/FpML-5/recordkeeping',
                                                   'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' as "nsdext",
                                                   'http://www.fpml.org/FpML-5/ext' as "fpmlext"),
                             '//nsdext:repos/*' PASSING xmltype(t.t_fmtclobdata_xxxx)
                             COLUMNS Counterparty XMLTYPE PATH '/*') x
                       where lower(x.Counterparty.getrootelement()) like ('counterparty%')) t,
                      XMLTABLE(xmlnamespaces(default 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' as "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' as "fpmlext"),
                               '/*' PASSING Counterparty
                               COLUMNS CounterpartyID   PATH '@id',
                                       partyId  XMLTYPE PATH 'partyId',
                                       partyName        PATH 'partyName',
                                       classification   PATH 'classification',
                                       country          PATH 'country',
                                       organizationType PATH 'organizationType') x,
                      XMLTABLE(xmlnamespaces(default 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' as "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                               'partyId' PASSING x.partyId
                               COLUMNS partyId PATH '/*') y) LOOP
       IF c.CounterpartyID <> l_Name THEN
         IF l_Insert IS NOT NULL THEN
           IF l_count = 2 THEN
             l_Insert := l_Insert||')';
           ELSIF l_count = 1 THEN
             l_Insert := l_Insert||', chr(1) )';
           END IF;
           execute immediate l_Insert||' RETURNING t_id INTO :l_IDParty' using p_InterMesID, out l_IDParty;
           l_Count := InsertTableCM083(p_InterMesID, p_Text, l_IDParty, l_Name, l_PartyNum);
           l_PartyNum := l_PartyNum + 1;
         END IF;
           BEGIN
           SELECT obj.t_objectID
             INTO l_partyID
             FROM dobjcode_dbt obj
            WHERE obj.t_objectType = OBJTYPE_PARTY
             AND obj.t_codekind = PTCK_RIC
             AND obj.t_State = 0             
             AND obj.t_code = c.partyid;
           EXCEPTION
           WHEN NO_DATA_FOUND THEN
             l_partyID := 0;
           END;
           BEGIN
           SELECT obj.t_attrID
             INTO l_classID
             FROM dobjattr_dbt obj
            WHERE obj.t_objecttype = OBJTYPE_PARTY
              AND obj.t_groupid = 80
              AND LOWER(obj.t_nameobject) = LOWER(c.classification);
           EXCEPTION
           WHEN NO_DATA_FOUND THEN
             l_classID := 0;
           END;
         l_Insert := 'INSERT INTO dir_party_dbt (t_internalmessageid, t_partytype, t_partyID, t_partyKind, t_partyname, t_partyid1, t_classification, 
                      T_CLASSIFICATIONID, t_country, t_organizationtype, t_partyid2) 
                      VALUES (:InterMesID, '''|| 'Counterparty'||TO_CHAR(l_PartyNum) ||''', '''||l_partyID||''', '''||IR_PARTY_KIND_COUNTERPARTY||''', q''~'||c.partyname||'~'', '''||c.partyid||''', '''||
                      c.classification||''','''||l_classID||''', '''||c.country||''', '''||c.organizationtype||'''';
         l_count := 1;
       ELSE
         l_Insert := l_Insert||', '''||c.partyid||'''';
         l_count := 2;
       END IF;
       l_Name := c.CounterpartyID;
     END LOOP;
     IF l_count = 2 THEN
       l_Insert := l_Insert||')';
     elsif l_count =1 THEN
       l_Insert := l_Insert||', chr(1) )';
     end if;
     execute immediate l_Insert||' RETURNING t_id INTO :l_IDParty' using p_InterMesID, out l_IDParty;
     l_Count := InsertTableCM083(p_InterMesID, p_Text, l_IDParty, l_Name, l_PartyNum);
     Return l_Count;
   end InsertTableParty083;

   FUNCTION InsertTableDifferences (p_InterMesID in integer, p_Text out varchar2) return integer as
   begin
     INSERT INTO dir_differences_dbt(t_internalmessageid,
                                     T_DIFFERENCETYPE,
                                     T_ELEMENTNAME,
                                     T_BASEPATH,
                                     T_BASEVALUE,
                                     T_OTHERVALUE,
                                     T_DIFFERENCEDESCR)
       SELECT p_InterMesID,
              x.differenceType, x.element, x.basePath, x.baseValue, x.otherValue, x.message
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//nsdext:difference'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS differenceType PATH 'differenceType',
                               element        PATH 'element',
                               basePath       PATH 'basePath',
                               baseValue      PATH 'baseValue',
                               otherValue     PATH 'otherValue',
                               message        PATH 'message') x;
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableDifferences;

   FUNCTION InsertTableCM021 (p_InterMesID in integer, p_Text out varchar2) return integer as
   begin
     INSERT INTO dir_cm021_dbt(t_internalmessageid,
                               T_PAYERCURR1_PART1,
                               T_RECEIVERCURR1_PART1,
                               T_CURRENCY1_PART1,
                               T_AMOUNTCURR1_PART1,
                               T_PAYERCURR2_PART1,
                               T_RECEIVERCURR2_PART1,
                               T_CURRENCY2_PART1,
                               T_AMOUNTCURR2_PART1,
                               T_TRADECURR_PART1,
                               T_VALDATE_PART1,
                               T_QUOTED_CURR1_PART1,
                               T_QUOTED_CURR2_PART1,
                               T_QBASIS_PART1,
                               T_RATE_PART1,
                               T_SETTLCURR_PART1,
                               T_PAYERCURR1_PART1ID,
                               T_RECEIVERCURR1_PART1ID,
                               T_CURRENCY1_PART1ID,
                               T_PAYERCURR2_PART1ID,
                               T_RECEIVERCURR2_PART1ID,
                               T_CURRENCY2_PART1ID,
                               T_TRADECURR_PART1ID,
                               T_QUOTED_CURR1_PART1ID,
                               T_QUOTED_CURR2_PART1ID,
                               T_SETTLCURR_PART1ID,
                               T_PAYERCURR1_PART2,
                               T_RECEIVERCURR1_PART2,
                               T_CURRENCY1_PART2,
                               T_AMOUNTCURR1_PART2,
                               T_PAYERCURR2_PART2,
                               T_RECEIVERCURR2_PART2,
                               T_CURRENCY2_PART2,
                               T_AMOUNTCURR2_PART2,
                               T_TRADECURR_PART2,
                               T_VALDATE_PART2,
                               T_QUOTED_CURR1_PART2,
                               T_QUOTED_CURR2_PART2,
                               T_QBASIS_PART2,
                               T_RATE_PART2,
                               T_SETTLCURR_PART2,
                               T_PAYERCURR1_PART2ID,
                               T_RECEIVERCURR1_PART2ID,
                               T_CURRENCY1_PART2ID,
                               T_PAYERCURR2_PART2ID,
                               T_RECEIVERCURR2_PART2ID,
                               T_CURRENCY2_PART2ID,
                               T_TRADECURR_PART2ID,
                               T_QUOTED_CURR1_PART2ID,
                               T_QUOTED_CURR2_PART2ID,
                               T_SETTLCURR_PART2ID)
       SELECT p_InterMesID,
              x.payerCurr1Party1, x.receiverCurr1Party1, x.currency1Part1, to_number(x.amountCurr1Part1, '999999999999999999999.99999999'),
              x.payerCurr2Party1, x.receiverCurr2Party1, x.currency2Part1, to_number(x.amountCurr2Part1, '999999999999999999999.99999999'),
              x.tradeCurrPart1, x.valDatePart1, x.quotedCurr1Part1, x.quotedCurr2Part1, x.qBasisPart1, to_number(x.ratePart1, '999999999999999999999.99999999'), x.settlCurrPart1,
              DECODE(lower(x.payerCurr1Party1),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.receiverCurr1Party1),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              (select fi11.t_fiid from dfininstr_dbt fi11 Where fi11.t_fi_kind = 1 and LOWER(fi11.t_ccy) = LOWER(x.currency1Part1)),
              DECODE(lower(x.payerCurr2Party1),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.receiverCurr2Party1),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              (select fi21.t_fiid from dfininstr_dbt fi21 Where fi21.t_fi_kind = 1 and LOWER(fi21.t_ccy) = LOWER(x.currency2Part1)),
              (select fit1.t_fiid from dfininstr_dbt fit1 Where fit1.t_fi_kind = 1 and LOWER(fit1.t_ccy) = LOWER(x.tradeCurrPart1)),
              (select fiq11.t_fiid from dfininstr_dbt fiq11 Where fiq11.t_fi_kind = 1 and LOWER(fiq11.t_ccy) = LOWER(x.quotedCurr1Part1)),
              (select fiq21.t_fiid from dfininstr_dbt fiq21 Where fiq21.t_fi_kind = 1 and LOWER(fiq21.t_ccy) = LOWER(x.quotedCurr2Part1)),
              (select fis1.t_fiid from dfininstr_dbt fis1 Where fis1.t_fi_kind = 1 and LOWER(fis1.t_ccy) = LOWER(x.settlCurrPart1)),
              x.payerCurr1Party2, x.receiverCurr1Party2, x.currency1Part2, to_number(x.amountCurr1Part2, '999999999999999999999.99999999'),
              x.payerCurr2Party2, x.receiverCurr2Party2, x.currency2Part2, to_number(x.amountCurr2Part2, '999999999999999999999.99999999'),
              x.tradeCurrPart2, x.valDatePart2, x.quotedCurr1Part2, x.quotedCurr2Part2, x.qBasisPart2, to_number(x.ratePart2, '999999999999999999999.99999999'), x.settlCurrPart2,
              DECODE(lower(x.payerCurr1Party2),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.receiverCurr1Party2),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              (select fi12.t_fiid from dfininstr_dbt fi12 Where fi12.t_fi_kind = 1 and LOWER(fi12.t_ccy) = LOWER(x.currency1Part2)),
              DECODE(lower(x.payerCurr2Party2),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.receiverCurr2Party2),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              (select fi22.t_fiid from dfininstr_dbt fi22 Where fi22.t_fi_kind = 1 and LOWER(fi22.t_ccy) = LOWER(x.currency2Part2)),
              (select fit2.t_fiid from dfininstr_dbt fit2 Where fit2.t_fi_kind = 1 and LOWER(fit2.t_ccy) = LOWER(x.tradeCurrPart2)),
              (select fiq12.t_fiid from dfininstr_dbt fiq12 Where fiq12.t_fi_kind = 1 and LOWER(fiq12.t_ccy) = LOWER(x.quotedCurr1Part2)),
              (select fiq22.t_fiid from dfininstr_dbt fiq22 Where fiq22.t_fi_kind = 1 and LOWER(fiq22.t_ccy) = LOWER(x.quotedCurr2Part2)),
              (select fis2.t_fiid from dfininstr_dbt fis2 Where fis2.t_fi_kind = 1 and LOWER(fis2.t_ccy) = LOWER(x.settlCurrPart2))
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//fxSwap'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS payerCurr1Party1    PATH 'nearLeg/exchangedCurrency1/payerPartyReference/@href',
                               receiverCurr1Party1 PATH 'nearLeg/exchangedCurrency1/receiverPartyReference//@href',
                               currency1Part1      PATH 'nearLeg/exchangedCurrency1/paymentAmount/currency',
                               amountCurr1Part1    PATH 'nearLeg/exchangedCurrency1/paymentAmount/amount',
                               payerCurr2Party1    PATH 'nearLeg/exchangedCurrency2/payerPartyReference/@href',
                               receiverCurr2Party1 PATH 'nearLeg/exchangedCurrency2/receiverPartyReference//@href',
                               currency2Part1      PATH 'nearLeg/exchangedCurrency2/paymentAmount/currency',
                               amountCurr2Part1    PATH 'nearLeg/exchangedCurrency2/paymentAmount/amount',
                               tradeCurrPart1      PATH 'nearLeg/dealtCurrency',
                               valDatePart1   DATE PATH 'nearLeg/valueDate',
                               quotedCurr1Part1    PATH 'nearLeg/exchangeRate/quotedCurrencyPair/currency1',
                               quotedCurr2Part1    PATH 'nearLeg/exchangeRate/quotedCurrencyPair/currency2',
                               qBasisPart1         PATH 'nearLeg/exchangeRate/quotedCurrencyPair/quoteBasis',
                               ratePart1           PATH 'nearLeg/exchangeRate/rate',
                               settlCurrPart1      PATH 'nearLeg/nonDeliverableSettlement/settlementCurrency',
                               payerCurr1Party2    PATH 'farLeg/exchangedCurrency1/payerPartyReference/@href',
                               receiverCurr1Party2 PATH 'farLeg/exchangedCurrency1/receiverPartyReference//@href',
                               currency1Part2      PATH 'farLeg/exchangedCurrency1/paymentAmount/currency',
                               amountCurr1Part2    PATH 'farLeg/exchangedCurrency1/paymentAmount/amount',
                               payerCurr2Party2    PATH 'farLeg/exchangedCurrency2/payerPartyReference/@href',
                               receiverCurr2Party2 PATH 'farLeg/exchangedCurrency2/receiverPartyReference//@href',
                               currency2Part2      PATH 'farLeg/exchangedCurrency2/paymentAmount/currency',
                               amountCurr2Part2    PATH 'farLeg/exchangedCurrency2/paymentAmount/amount',
                               tradeCurrPart2      PATH 'farLeg/dealtCurrency',
                               valDatePart2   DATE PATH 'farLeg/valueDate',
                               quotedCurr1Part2    PATH 'farLeg/exchangeRate/quotedCurrencyPair/currency1',
                               quotedCurr2Part2    PATH 'farLeg/exchangeRate/quotedCurrencyPair/currency2',
                               qBasisPart2         PATH 'farLeg/exchangeRate/quotedCurrencyPair/quoteBasis',
                               ratePart2           PATH 'farLeg/exchangeRate/rate',
                               settlCurrPart2      PATH 'farLeg/nonDeliverableSettlement/settlementCurrency') x;
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM021;

   FUNCTION InsertTableCM022 (p_InterMesID in integer, p_Text out varchar2) return integer as
   begin
     INSERT INTO dir_cm022_dbt(t_internalmessageid,
                               t_payerpartyreference1,
                               t_receiverpartyreference1,
                               T_PAYERPARTYREFERENCE1ID,
                               T_RECEIVERPARTYREFERENCE1ID,
                               T_CURRENCY1ID,
                               t_currency1,
                               t_amountcurr1,
                               t_payerpartyreference2,
                               t_receiverpartyreference2,
                               T_PAYERPARTYREFERENCE2ID,
                               T_RECEIVERPARTYREFERENCE2ID,
                               T_CURRENCY2ID,
                               t_currency2,
                               t_amountcurr2,
                               T_TRADECURRID,
                               t_tradecurr,
                               t_valdate,
                               t_quotedcurrency1,
                               t_quotedcurrency2,
                               t_qbasis,
                               t_rate,
                               t_settlcurr)
       SELECT p_InterMesID, x.payerParty1, x.receiverParty1,
              DECODE(lower(x.payerParty1),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.receiverParty1),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.part1curr) ),
              x.part1curr, to_number(x.part1amount, '999999999999999999999.99999999'),
              x.payerParty2, x.receiverParty2,
              DECODE(lower(x.payerParty2),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.receiverParty2),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.part2curr) ),
              x.part2curr, to_number(x.part2amount, '999999999999999999999.99999999'),
              decode(lower(x.dealtCurr),'exchangedcurrency1',
                                       (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.part1curr) ),
                                       (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.part2curr) ) ),
              x.dealtCurr,
              x.valueDate, x.currency1, x.currency2, x.quoteBasis, to_number(x.rate, '999999999999999999999.99999999'), x.settlCurr
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//fxSingleLeg'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS payerParty1    PATH 'exchangedCurrency1/payerPartyReference/@href',
                               receiverParty1 PATH 'exchangedCurrency1/receiverPartyReference/@href',
                               part1curr      PATH 'exchangedCurrency1/paymentAmount/currency',
                               part1amount    PATH 'exchangedCurrency1/paymentAmount/amount',
                               payerParty2    PATH 'exchangedCurrency2/payerPartyReference/@href',
                               receiverParty2 PATH 'exchangedCurrency2/receiverPartyReference/@href',
                               part2curr      PATH 'exchangedCurrency2/paymentAmount/currency',
                               part2amount    PATH 'exchangedCurrency2/paymentAmount/amount',
                               dealtCurr      PATH 'dealtCurrency',
                               valueDate DATE PATH 'valueDate',
                               currency1      PATH 'exchangeRate/quotedCurrencyPair/currency1',
                               currency2      PATH 'exchangeRate/quotedCurrencyPair/currency2',
                               quoteBasis     PATH 'exchangeRate/quotedCurrencyPair/quoteBasis',
                               rate           PATH 'exchangeRate/rate',
                               settlCurr      PATH 'exchangeRate/nonDeliverableSettlement/settlementCurrency') x;
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM022;

   FUNCTION InsertTableCM023 (p_InterMesID in integer, p_ProductType in varchar2, p_Text out varchar2) return integer as
   begin
     INSERT INTO dir_cm023_dbt(t_internalmessageid,
                               t_buyer,
                               t_seller,
                               T_BUYERID,
                               T_SELLERID,
                               T_OPTEXERID,
                               t_optexer,
                               t_opendate,
                               t_expirydate,
                               T_PUTCURRID,
                               t_putcurr,
                               t_putcurramount,
                               T_CALLCURRID,
                               t_callcurr,
                               t_callcurramount,
                               t_soldas,
                               t_optrate,
                               t_strikebasis,
                               t_premiumpayerparty,
                               t_premiumreceiverparty,
                               T_PREMIUMPAYERPARTYID,
                               T_PREMIUMRECEIVERPARTYID,
                               T_PREMIUMCURRENCYID,
                               t_premiumcurrency,
                               t_premiumamount,
                               t_settlcurrency)
       SELECT p_InterMesID, x.Buyer, x.Seller,
              DECODE(lower(x.Buyer),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.Seller),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              CASE WHEN p_ProductType LIKE '%American%' THEN DVSTYLE_AMERICAN
                   WHEN p_ProductType LIKE '%European%' THEN DVSTYLE_EUROPE
              END,
              CASE WHEN p_ProductType LIKE '%American%' THEN 'A'
                   WHEN p_ProductType LIKE '%European%' THEN 'E'
              END,
              x.OpenDate, decode(x.ExpiryDateA, null, x.ExpiryDateE, x.ExpiryDateA),
              (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.PutCurr) ),
              x.PutCurr, to_number(x.PutCurrAmount, '999999999999999999999.99999999'),
              (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.CallCurr) ),
              x.CallCurr, to_number(x.CallCurrAmount, '999999999999999999999.99999999'),
              x.soldAs, to_number(x.OptRate, '999999999999999999999.99999999'), x.StrikeBasis,
              x.PremiumPayerParty, x.PremiumReceiverParty,
              DECODE(lower(x.PremiumPayerParty),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.PremiumReceiverParty),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.PremiumCurrency) ),
              x.PremiumCurrency,
              to_number(x.PremiumAmount, '999999999999999999999.99999999'), x.SettlCurrency
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//fxOption'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS Buyer                PATH 'buyerPartyReference/@href',
                               Seller               PATH 'sellerPartyReference/@href',
                               OpenDate        DATE PATH 'americanExercise/commencementDate/adjustableDate/unadjustedDate',
                               ExpiryDateA     DATE PATH 'americanExercise/expiryDate',
                               ExpiryDateE     DATE PATH 'europeanExercise/expiryDate',
                               PutCurrAmount        PATH 'putCurrencyAmount/amount',
                               PutCurr              PATH 'putCurrencyAmount/currency',
                               CallCurrAmount       PATH 'callCurrencyAmount/amount',
                               CallCurr             PATH 'callCurrencyAmount/currency',
                               soldAs               PATH 'soldAs',
                               OptRate              PATH 'strike/rate',
                               StrikeBasis          PATH 'strike/strikeQuoteBasis',
                               PremiumPayerParty    PATH 'premium/payerPartyReference/@href',
                               PremiumReceiverParty PATH 'premium/receiverPartyReference/@href',
                               PremiumCurrency      PATH 'premium/paymentAmount/currency',
                               PremiumAmount        PATH 'premium/paymentAmount/amount',
                               SettlCurrency        PATH 'cashSettlement/settlementCurrency') x;
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM023;

   FUNCTION InsertTableCM032 (p_InterMesID in integer, p_Text out varchar2) return integer as
   begin
     INSERT INTO dir_cm032_dbt(t_internalmessageid,
                               t_payerparty_o1,
                               t_receiverparty_o1,
                               t_effectivedate,
                               t_terminationdate,
                               t_periodmultiplier_o1,
                               t_periodtype_o1,
                               t_nomvalue_o1,
                               t_nomcurrency_o1,
                               t_fixrate_o1,
                               t_floatrate_o1,
                               t_rate_o1,
                               t_floatingrateindex_o1,
                               t_FloatPeriodMultiplier_O1, 
                               t_FloatPeriodType_O1, 
                               t_spread_o1,
                               t_floatingrate_o1,
                               t_daycountfraction_o1,
                               t_initialexchange_o1,
                               t_finalexchange_o1,
                               t_intermediateexchange_o1,
                               t_payerparty_o1id,
                               t_receiverparty_o1id,
                               t_periodtype_o1id,
                               t_FloatPeriodType_O1id,
                               t_nomcurrency_o1id,
                               t_floatingrateindex_o1id,
                               t_daycountfraction_o1id,
                               t_payerparty_o2,
                               t_receiverparty_o2,
                               t_effectivedate_o2,
                               t_terminationdate_o2,
                               t_periodmultiplier_o2,
                               t_periodtype_o2,
                               t_nomvalue_o2,
                               t_nomcurrency_o2,
                               t_fixrate_o2,
                               t_floatrate_o2,
                               t_rate_o2,
                               t_floatingrateindex_o2,
                               t_FloatPeriodMultiplier_O2, 
                               t_FloatPeriodType_O2, 
                               t_spread_o2,
                               t_floatingrate_o2,
                               t_daycountfraction_o2,
                               t_initialexchange_o2,
                               t_finalexchange_o2,
                               t_intermediateexchange_o2,
                               t_payerparty_o2id,
                               t_receiverparty_o2id,
                               t_periodtype_o2id,
                               t_FloatPeriodType_O2id,
                               t_nomcurrency_o2id,
                               t_floatingrateindex_o2id,
                               t_daycountfraction_o2id)
       SELECT p_InterMesID,
              Stream1.payerparty_o1, 
              Stream1.receiverparty_o1, 
              Stream1.effectivedate, 
              Stream1.terminationdate, 
              Stream1.periodmultiplier_o1, 
              Stream1.periodtype_o1, 
              NVL(ROUND(to_number(Stream1.nomvalue_o1, '999999999999999999999.99999999'), 4), 0),  
              Stream1.nomcurrency_o1, 
              decode(Stream1.rate_o1, null, 'N', 'Y'),
              decode(Stream1.rate_o1, null, 'Y', 'N'), 
              NVL(to_number(Stream1.rate_o1, '999999999999999999999.99999999'), 0),
              NVL(Stream1.floatingrateindex_o1, CHR(1)),
              NVL(Stream1.FloatPeriodMultiplier_O1, 0),
              NVL(Stream1.FloatPeriodType_O1, CHR(1)), 
              NVL(to_number(Stream1.spread_o1, '999999999999999999999.99999999'), 0),
              NVL(to_number(Stream1.floatingrate_o1, '999999999999999999999.99999999'), 0),
              Stream1.daycountfraction_o1,
              decode(Stream1.initialexchange_o1, 'true', 'X', ' '),
              decode(Stream1.finalexchange_o1, 'true', 'X', ' '),
              decode(Stream1.intermediateexchange_o1, 'true', 'X', ' '),
              DECODE(lower(Stream1.payerparty_o1),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(Stream1.receiverparty_o1),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              NVL((select pt1.t_inumberalg from dnamealg_dbt pt1 where pt1.t_itypealg = ALG_DV_PAYMPERIODKIND and pt1.t_sznamealg = Stream1.periodtype_o1), 0),
              NVL((select pt1.t_inumberalg from dnamealg_dbt pt1 where pt1.t_itypealg = ALG_DV_PAYMPERIODKIND and pt1.t_sznamealg = Stream1.FloatPeriodType_O1), 0),
              (select fi1.t_fiid from dfininstr_dbt fi1 Where fi1.t_fi_kind = 1 and LOWER(fi1.t_ccy) = LOWER(Stream1.nomcurrency_o1)),
              NVL((select ind1.t_element from dllvalues_dbt ind1 where ind1.t_list = OBJTYPE_INDEX_FLOATRATE and ind1.t_name = Stream1.floatingrateindex_o1), 0),
              (select dayc1.t_inumberalg from dnamealg_dbt dayc1 where dayc1.t_itypealg = ALG_GENINF_DAYCOUNTCALC and dayc1.t_sznamealg = Stream1.daycountfraction_o1),
              Stream2.payerparty_o2, 
              Stream2.receiverparty_o2, 
              Stream2.effectivedate_o2, 
              Stream2.terminationdate_o2, 
              Stream2.periodmultiplier_o2,
              Stream2.periodtype_o2, 
              NVL(ROUND(to_number(Stream2.nomvalue_o2, '999999999999999999999.99999999'), 4), 0),  
              Stream2.nomcurrency_o2, 
              decode(Stream2.rate_o2, null, 'N', 'Y'),
              decode(Stream2.rate_o2, null, 'Y', 'N'), 
              NVL(to_number(Stream2.rate_o2, '999999999999999999999.99999999'), 0),
              NVL(Stream2.floatingrateindex_o2, CHR(1)),
              NVL(Stream2.FloatPeriodMultiplier_O2, 0),
              NVL(Stream2.FloatPeriodType_O2, CHR(1)),
              NVL(to_number(Stream2.spread_o2, '999999999999999999999.99999999'), 0),
              NVL(to_number(Stream2.floatingrate_o2, '999999999999999999999.99999999'), 0),
              Stream2.daycountfraction_o2,
              decode(Stream2.initialexchange_o2, 'true', 'X', ' '),
              decode(Stream2.finalexchange_o2, 'true', 'X', ' '),
              decode(Stream2.intermediateexchange_o2, 'true', 'X', ' '),
              DECODE(lower(Stream2.payerparty_o2),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(Stream2.receiverparty_o2),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              NVL((select pt2.t_inumberalg from dnamealg_dbt pt2 where pt2.t_itypealg = ALG_DV_PAYMPERIODKIND and pt2.t_sznamealg = Stream2.periodtype_o2), 0),
              NVL((select pt1.t_inumberalg from dnamealg_dbt pt1 where pt1.t_itypealg = ALG_DV_PAYMPERIODKIND and pt1.t_sznamealg = Stream2.FloatPeriodType_O2), 0),
              (select fi2.t_fiid from dfininstr_dbt fi2 Where fi2.t_fi_kind = 1 and LOWER(fi2.t_ccy) = LOWER(Stream2.nomcurrency_o2)),
              NVL((select ind2.t_element from dllvalues_dbt ind2 where ind2.t_list = OBJTYPE_INDEX_FLOATRATE and ind2.t_name = Stream2.floatingrateindex_o2), 0),
              (select dayc2.t_inumberalg from dnamealg_dbt dayc2 where dayc2.t_itypealg = ALG_GENINF_DAYCOUNTCALC and dayc2.t_sznamealg = Stream2.daycountfraction_o2)
         FROM (SELECT Stream.*
                 FROM (SELECT rownum rn, x.swapStream
                         FROM DIR_SRS_TMP t,
                              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                                     'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                                     'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                                       '//swap/swapStream'
                                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                                       COLUMNS swapStream XMLTYPE PATH '*') x) swap,
                      XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                               '/*'
                               PASSING xmltype('<swapStream>'||swap.swapStream||'</swapStream>')
                               COLUMNS payerparty_o1            PATH 'payerPartyReference/@href',
                                       receiverparty_o1         PATH 'receiverPartyReference/@href',
                                       effectivedate      DATE  PATH 'calculationPeriodDates/effectiveDate/unadjustedDate',
                                       terminationdate    DATE  PATH 'calculationPeriodDates/terminationDate/unadjustedDate',
                                       periodmultiplier_o1      PATH 'paymentDates/paymentFrequency/periodMultiplier',
                                       periodtype_o1            PATH 'paymentDates/paymentFrequency/period',
                                       nomvalue_o1              PATH 'calculationPeriodAmount/calculation/notionalSchedule/notionalStepSchedule/initialValue',
                                       nomcurrency_o1           PATH 'calculationPeriodAmount/calculation/notionalSchedule/notionalStepSchedule/currency',
                                       rate_o1                  PATH 'calculationPeriodAmount/calculation/fixedRateSchedule/initialValue',
                                       floatingrateindex_o1     PATH 'calculationPeriodAmount/calculation/floatingRateCalculation/floatingRateIndex',
                                       FloatPeriodMultiplier_O1 PATH 'calculationPeriodAmount/calculation/floatingRateCalculation/indexTenor/periodMultiplier',
                                       FloatPeriodType_O1       PATH 'calculationPeriodAmount/calculation/floatingRateCalculation/indexTenor/period',
                                       spread_o1                PATH 'calculationPeriodAmount/calculation/floatingRateCalculation/spreadSchedule/initialValue',
                                       floatingrate_o1          PATH 'calculationPeriodAmount/calculation/floatingRateCalculation/initialRate',
                                       daycountfraction_o1      PATH 'calculationPeriodAmount/calculation/dayCountFraction',
                                       initialexchange_o1       PATH 'principalExchanges/initialExchange',
                                       finalexchange_o1         PATH 'principalExchanges/finalExchange',
                                       intermediateexchange_o1  PATH 'principalExchanges/intermediateExchange') Stream
                WHERE swap.rn = 1) Stream1,
              (SELECT Stream.*
                 FROM (SELECT rownum rn, x.swapStream
                         FROM DIR_SRS_TMP t,
                              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                                     'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                                     'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                                       '//swap/swapStream'
                                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                                       COLUMNS swapStream XMLTYPE PATH '*') x) swap,
                      XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                               '/*'
                               PASSING xmltype('<swapStream>'||swap.swapStream||'</swapStream>')
                               COLUMNS payerparty_o2            PATH 'payerPartyReference/@href',
                                       receiverparty_o2         PATH 'receiverPartyReference/@href',
                                       effectivedate_o2   DATE  PATH 'calculationPeriodDates/effectiveDate/unadjustedDate',
                                       terminationdate_o2 DATE  PATH 'calculationPeriodDates/terminationDate/unadjustedDate',
                                       periodmultiplier_o2      PATH 'paymentDates/paymentFrequency/periodMultiplier',
                                       periodtype_o2            PATH 'paymentDates/paymentFrequency/period',
                                       nomvalue_o2              PATH 'calculationPeriodAmount/calculation/notionalSchedule/notionalStepSchedule/initialValue',
                                       nomcurrency_o2           PATH 'calculationPeriodAmount/calculation/notionalSchedule/notionalStepSchedule/currency',
                                       rate_o2                  PATH 'calculationPeriodAmount/calculation/fixedRateSchedule/initialValue',
                                       floatingrateindex_o2     PATH 'calculationPeriodAmount/calculation/floatingRateCalculation/floatingRateIndex',
                                       FloatPeriodMultiplier_O2 PATH 'calculationPeriodAmount/calculation/floatingRateCalculation/indexTenor/periodMultiplier',
                                       FloatPeriodType_O2       PATH 'calculationPeriodAmount/calculation/floatingRateCalculation/indexTenor/period',
                                       spread_o2                PATH 'calculationPeriodAmount/calculation/floatingRateCalculation/spreadSchedule/initialValue',
                                       floatingrate_o2          PATH 'calculationPeriodAmount/calculation/floatingRateCalculation/initialRate',
                                       daycountfraction_o2      PATH 'calculationPeriodAmount/calculation/dayCountFraction',
                                       initialexchange_o2       PATH 'principalExchanges/initialExchange',
                                       finalexchange_o2         PATH 'principalExchanges/finalExchange',
                                       intermediateexchange_o2  PATH 'principalExchanges/intermediateExchange'/*,
                                       SettlCurr_O2             PATH 'settlementProvision/settlementCurrency' */) Stream
                WHERE swap.rn = 2) Stream2;
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM032;

   FUNCTION InsertTableCM041 (p_InterMesID in integer, p_Text out varchar2) return integer as
     l_Count        INTEGER;
     l_InstrumentCode VARCHAR2(20);
   begin
     SELECT count(*)
       INTO l_Count
       FROM DIR_SRS_TMP t,
            XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                   'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                   'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                     '//fpmlext:repo'
                     PASSING xmltype(t.t_fmtclobdata_xxxx)
                     COLUMNS initialValue PATH 'fpmlext:fixedRateSchedule/initialValue') x;
     IF l_Count = 0 THEN
       p_Text := 'В загружаемом сообщении есть данные о плавающей ставке РЕПО, обработка которых в системе не реализована.';
       Return 9;
     END IF;
     INSERT INTO dir_cm041_dbt(t_internalmessageid,
                               t_initialrate,
                               t_ratetype,
                               t_daycountcalcID,
                               t_durationtype,
                               t_buyerpart1,
                               t_sellerpart1,
                               t_part1settldate,
                               t_part1curr,
                               t_part1amount,
                               t_signbonds,
                               t_collatcurr,
                               t_collatnomamount,
                               t_netprice,
                               t_accrual,
                               t_drtprice,
                               t_numberofunits,
                               t_signbond,
                               t_unitpricecurr,
                               t_unitpriceamount,
                               t_haircutvaluer,
                               t_deliverymethodpart1,
                               t_deliverydatepart1,
                               t_buyerpart2,
                               t_sellerpart2,
                               t_part2settldate,
                               t_currencypart2,
                               t_amountpart2,
                               t_settlmethodpart2,
                               t_deliverydatepart2, 
                               t_daycountcalc)
       SELECT p_InterMesID, 
              to_number(x.initialValue, '999999999999999999999.99999999'), 'FX',
              (select na.t_inumberalg
                 from dnamealg_dbt na
                where na.t_itypealg = ALG_GENINF_DAYCOUNTCALC and na.t_sznamealg = x.dayCount),
              x.durationType, x.buyerpart1, x.sellerpart1,
              x.part1settldate, x.part1curr, to_number(x.part1amount, '999999999999999999999.99999999'), decode(x.bonds, null, chr(0), 'X'),
              x.collatcurr, to_number(x.collatnomamount, '999999999999999999999.99999999'), to_number(x.netprice, '999999999999999999999.99999999'),
              to_number(x.accrual, '999999999999999999999.99999999'), to_number(x.drtprice, '999999999999999999999.99999999'), x.numberofunits,
              decode(x.equite, null, chr(0), 'X'), x.unitpricecurr, to_number(x.unitpriceamount, '999999999999999999999.99999999'),
              to_number(x.haircutvaluer, '999999999999999999999.99999999'), x.deliverymethodpart1, nvl(x.deliverydatepart1, x.part1settldate), x.buyerpart2, x.sellerpart2,
              x.part2settldate, x.currencypart2, to_number(x.amountpart2, '999999999999999999999.99999999'), x.settlmethodpart2, nvl(x.deliverydatepart2,x.part2settldate), 
              x.dayCount 
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//fpmlext:repo'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS initialValue           PATH 'fpmlext:fixedRateSchedule/initialValue',
                               dayCount               PATH 'fpmlext:dayCountFraction',
                               durationType           PATH 'fpmlext:duration',
                               buyerpart1             PATH 'fpmlext:spotLeg/buyerPartyReference/@href',
                               sellerpart1            PATH 'fpmlext:spotLeg/sellerPartyReference/@href',
                               part1settldate    DATE PATH 'fpmlext:spotLeg/fpmlext:settlementDate/adjustableDate/unadjustedDate',
                               part1curr              PATH 'fpmlext:spotLeg/settlementAmount/currency',
                               part1amount            PATH 'fpmlext:spotLeg/settlementAmount/amount',
                               collatcurr             PATH 'fpmlext:spotLeg/fpmlext:collateral/fpmlext:nominalAmount/currency',
                               collatnomamount        PATH 'fpmlext:spotLeg/fpmlext:collateral/fpmlext:nominalAmount/amount',
                               netprice               PATH 'fpmlext:spotLeg/fpmlext:collateral/fpmlext:cleanPrice',
                               accrual                PATH 'fpmlext:spotLeg/fpmlext:collateral/fpmlext:accruals',
                               drtprice               PATH 'fpmlext:spotLeg/fpmlext:collateral/fpmlext:dirtyPrice',
                               numberofunits          PATH 'fpmlext:spotLeg/fpmlext:collateral/fpmlext:numberOfUnits',
                               unitpricecurr          PATH 'fpmlext:spotLeg/fpmlext:collateral/fpmlext:unitPrice/currency',
                               unitpriceamount        PATH 'fpmlext:spotLeg/fpmlext:collateral/fpmlext:unitPrice/amount',
                               haircutvaluer          PATH 'fpmlext:spotLeg/fpmlext:collateral/nsdext:securityHaircut/nsdext:haircutValue',
                               deliverymethodpart1    PATH 'fpmlext:spotLeg/nsdext:deliveryMethod',
                               deliverydatepart1 DATE PATH 'fpmlext:spotLeg/nsdext:deliveryDate/adjustableDate/unadjustedDate',
                               buyerpart2             PATH 'fpmlext:forwardLeg/buyerPartyReference/@href',
                               sellerpart2            PATH 'fpmlext:forwardLeg/sellerPartyReference/@href',
                               part2settldate    DATE PATH 'fpmlext:forwardLeg/fpmlext:settlementDate/adjustableDate/unadjustedDate',
                               currencypart2          PATH 'fpmlext:forwardLeg/settlementAmount/currency',
                               amountpart2            PATH 'fpmlext:forwardLeg/settlementAmount/amount',
                               settlmethodpart2       PATH 'fpmlext:forwardLeg/nsdext:deliveryMethod',
                               deliverydatepart2 DATE PATH 'fpmlext:forwardLeg/nsdext:deliveryDate/adjustableDate/unadjustedDate',
                               equite                 PATH 'equity/@id',
                               bonds                  PATH 'bond/@id') x;
       SELECT SUBSTR (NVL (equity, bond), 1, 20)
         INTO l_InstrumentCode
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//fpmlext:repo'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS bond   PATH 'bond/instrumentId',
                               equity PATH 'equity/instrumentId') x;
     BEGIN
       InsertInstrument (p_InterMesID, l_InstrumentCode, null);
     EXCEPTION
       WHEN OTHERS THEN
         NULL;
     END;
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM041;

   FUNCTION InsertTableCM042 (p_InterMesID in integer, p_Text out varchar2) return integer as
     l_InstrumentCode VARCHAR2(20);
     l_InstrumentName VARCHAR2(50);
   begin
     INSERT INTO dir_cm042_dbt(t_internalmessageid,
                               t_buyerid,
                               t_buyer,
                               t_sellerid,
                               t_seller,
                               t_nominalcurrencyid,
                               t_nominalcurrency,
                               t_nominalamount,
                               t_cleanprice,
                               t_accruals,
                               t_dirtyprice,
                               t_periodmultiplier,
                               t_periodid,
                               t_period,
                               t_deliverymethodid,
                               t_deliverymethod,
                               t_settlmentdate,
                               t_deliverydate,
                               t_numberofunits
                               )
       SELECT p_InterMesID,
              DECODE(lower(x.buyer),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              x.buyer,
              DECODE(lower(x.seller),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              x.seller,
              (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.nominalcurrency)),
              x.nominalcurrency, to_number(x.nominalamount, '999999999999999999999.99999999'),
              to_number(x.cleanprice, '999999999999999999999.99999999'), to_number(x.accruals, '999999999999999999999.99999999'),
              to_number(x.dirtyprice, '999999999999999999999.99999999'), x.periodmultiplier,
              (select pt.t_inumberalg from dnamealg_dbt pt where pt.t_itypealg = ALG_DV_PAYMPERIODKIND and pt.t_sznamealg = x.period),
              x.period,
              (select pt.t_inumberalg from dnamealg_dbt pt where pt.t_itypealg = ALG_GENINF_SETTLMETHOD and pt.t_sznamealg = x.deliverymethod),
              x.deliverymethod,
              x.settlmentdate,
              x.deliverydate,
              to_number(x.numberofunits, '999999999999999999999.99999999')
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//nsdext:bondSimpleTransaction'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS buyer              PATH 'buyerPartyReference/@href',
                               seller             PATH 'sellerPartyReference/@href',
                               nominalcurrency    PATH 'fpmlext:notionalAmount/currency',
                               nominalamount      PATH 'fpmlext:notionalAmount/amount',
                               cleanprice         PATH 'fpmlext:price/fpmlext:cleanPrice',
                               accruals           PATH 'fpmlext:price/fpmlext:accruals',
                               dirtyprice         PATH 'fpmlext:price/fpmlext:dirtyPrice',
                               periodmultiplier   PATH 'nsdext:term/periodMultiplier',
                               period             PATH 'nsdext:term/period',
                               deliverymethod     PATH 'deliveryMethod',
                               settlmentdate DATE PATH 'nsdext:settlementDate/adjustableDate/unadjustedDate',
                               deliverydate  DATE PATH 'nsdext:deliveryDate/adjustableDate/unadjustedDate',
                               numberofunits      PATH 'nsdext:numberOfUnits'
                               ) x;
     SELECT SUBSTR (x.bond, 1, 20), SUBSTR (x.bondName, 1, 50)
       INTO l_InstrumentCode, l_InstrumentName
       FROM DIR_SRS_TMP t,
            XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                           'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                           'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                     '//nsdext:bondSimpleTransaction'
                     PASSING xmltype(t.t_fmtclobdata_xxxx)
                     COLUMNS bond     PATH 'fpmlext:bond/instrumentId',
                             bondName PATH 'fpmlext:bond/description') x;
     BEGIN
       InsertInstrument (p_InterMesID, l_InstrumentCode, l_InstrumentName);
     EXCEPTION
       WHEN OTHERS THEN
         NULL;
     END;
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM042;

   FUNCTION InsertTableCM043 (p_InterMesID in integer, p_Text out varchar2) return integer as
     l_InstrumentCode VARCHAR2(20);
     l_InstrumentName VARCHAR2(50);
   begin
     INSERT INTO dir_cm043_dbt(t_internalmessageid,
                               t_buyerid,
                               t_buyer,
                               t_sellerid,
                               t_seller,
                               t_underlyertype,
                               t_openunits,
                               t_amountcurrencyid,
                               t_amountcurrency,
                               t_amount,
                               t_settlmentdate,
                               t_settlcurrencyid,
                               t_settlcurrency,
                               t_pricesource,
                               t_partialdelivery,
                               t_pricecurrencyid,
                               t_pricecurrency,
                               t_price)
       SELECT p_InterMesID,
              DECODE(lower(x.buyer),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),      
              x.buyer,
              DECODE(lower(x.seller),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              x.seller, 
              decode(x.bond, NULL, 'Index', 'Bond'),
              x.openunits,
              (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.amountcurrency)),
              x.amountcurrency,
              to_number(x.amount, '999999999999999999999.99999999'),
              x.settlmentdate,
              (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.settlcurrency)),
              x.settlcurrency,
              x.pricesource,
              decode(x.partialdelivery, 'true', 'X', chr(0)),
              (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.pricecurrency)),
              x.pricecurrency,
              to_number(x.price, '999999999999999999999.99999999')
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//nsdext:bondForward'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS buyer              PATH 'buyerPartyReference/@href',
                               seller             PATH 'sellerPartyReference/@href',
                               openunits          PATH 'nsdext:underlyer/singleUnderlyer/openUnits',
                               amountcurrency     PATH 'nsdext:notionalAmount/currency',
                               amount             PATH 'nsdext:notionalAmount/amount',
                               settlmentdate DATE PATH 'nsdext:settlementDate/adjustableDate/unadjustedDate',
                               settlcurrency      PATH 'nsdext:settlementCurrency',
                               pricesource        PATH 'nsdext:settlementPriceSource',
                               partialdelivery    PATH 'nsdext:partialDelivery',
                               pricecurrency      PATH 'nsdext:forwardPrice/nsdext:forwardPricePerBond/currency',
                               price              PATH 'nsdext:forwardPrice/nsdext:forwardPricePerBond/amount',
                               bond               PATH 'nsdext:underlyer/singleUnderlyer/bond/@id',
                               indx               PATH 'nsdext:underlyer/singleUnderlyer/index/@id') x;
       SELECT SUBSTR (NVL (x.indx, x.bond), 1, 20),
              SUBSTR (decode(x.indx, null, x.bondName, x.indxName), 1, 50)
         INTO l_InstrumentCode, l_InstrumentName
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//singleUnderlyer'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS bond PATH 'bond/instrumentId',
                               bondName PATH 'bond/description',
                               indx PATH 'index/instrumentId',
                               indxName PATH 'index/description') x;
     BEGIN
       InsertInstrument (p_InterMesID, l_InstrumentCode, l_InstrumentName);
     EXCEPTION
       WHEN OTHERS THEN
         NULL;
     END;
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM043;

   FUNCTION InsertTableCM044 (p_InterMesID in integer, p_Text out varchar2) return integer as
     l_InstrumentCode VARCHAR2(20);
     l_InstrumentName VARCHAR2(50);
   begin
     INSERT INTO dir_cm044_dbt(t_internalmessageid,
                               t_buyer,
                               t_seller,
                               t_optiontype,
                               t_premiumpayer,
                               t_premiumreceiver,
                               t_premiumcurrency,
                               t_premiumamount,
                               t_optionstyle,
                               T_OPTIONSTYLEID,
                               t_commencementdate,
                               t_expirationdate,
                               t_nominalcurrency,
                               t_nominalamount,
                               t_optionentitlement,
                               t_numberofoptions,
                               t_settlementtype,
                               t_strike,
                               t_strikecurrency,
                               T_BUYERID,
                               T_SELLERID,
                               T_PREMIUMPAYERID,
                               T_PREMIUMRECEIVERID,
                               T_PREMIUMCURRENCYID,
                               T_NOMINALCURRENCYID,
                               T_STRIKECURRENCYID,
                               T_OPTIONTYPEID,
                               T_SETTLEMENTTYPEID)
       SELECT p_InterMesID, x.buyer, x.seller, x.optiontype, x.premiumpayer, x.premiumreceiver, x.premiumcurrency,
              to_number(x.premiumamount, '999999999999999999999.99999999'),
              decode(x.expirationdate_e, null, 'A', 'E'),
              decode(x.expirationdate_e, null, DVSTYLE_AMERICAN, DVSTYLE_EUROPE),
              NVL(x.commencementdate, TO_DATE('01.01.0001','DD.MM.YYYY')),
              decode(x.expirationdate_e, null, NVL(x.expirationdate_a,TO_DATE('01.01.0001','DD.MM.YYYY')), x.expirationdate_e), x.nominalcurrency,
              to_number(x.nominalamount, '999999999999999999999.99999999'),
              to_number(x.optionentitlement, '999999999999999999999.99999999'),
              to_number(x.numberofoptions, '999999999999999999999.99999999'), x.settlementtype,
              to_number(x.strike, '999999999999999999999.99999999'), x.strikecurrency,
              DECODE(lower(x.buyer),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.seller),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.premiumpayer),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.premiumreceiver),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              (select fi1.t_fiid from dfininstr_dbt fi1 Where fi1.t_fi_kind = 1 and LOWER(fi1.t_ccy) = LOWER(x.premiumcurrency)),
              (select fi2.t_fiid from dfininstr_dbt fi2 Where fi2.t_fi_kind = 1 and LOWER(fi2.t_ccy) = LOWER(x.nominalcurrency)),
              (select fi3.t_fiid from dfininstr_dbt fi3 Where fi3.t_fi_kind = 1 and LOWER(fi3.t_ccy) = LOWER(x.strikecurrency)),
              (select pt1.t_inumberalg from dnamealg_dbt pt1 where pt1.t_itypealg = ALG_FI_DVTYPE and pt1.t_sznamealg = x.optiontype),
              (select pt3.t_inumberalg from dnamealg_dbt pt3 where pt3.t_itypealg = ALG_DV_OPTION_PAYKIND and pt3.t_sznamealg = x.settlementtype)
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//bondOption'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS buyer                 PATH 'buyerPartyReference/@href',
                               seller                PATH 'sellerPartyReference/@href',
                               optiontype            PATH 'optionType',
                               premiumpayer          PATH 'premium/payerPartyReference/@href',
                               premiumreceiver       PATH 'premium/receiverPartyReference/@href',
                               premiumcurrency       PATH 'premium/paymentAmount/currency',
                               premiumamount         PATH 'premium/paymentAmount/amount',
                               commencementdate DATE PATH 'americanExercise/commencementDate/adjustableDate/unadjustedDate',
                               expirationdate_a DATE PATH 'americanExercise/expirationDate/adjustableDate/unadjustedDate',
                               expirationdate_e DATE PATH 'europeanExercise/expirationDate/adjustableDate/unadjustedDate',
                               nominalcurrency       PATH 'notionalAmount/currency',
                               nominalamount         PATH 'notionalAmount/amount',
                               optionentitlement     PATH 'optionEntitlement',
                               numberofoptions       PATH 'numberOfOptions',
                               settlementtype        PATH 'settlementType',
                               strike                PATH 'strike/price/strikePrice',
                               strikecurrency        PATH 'strike/price/currency') x;
     SELECT SUBSTR (x.bond, 1, 20), SUBSTR (x.bondName, 1, 50)
       INTO l_InstrumentCode, l_InstrumentName
       FROM DIR_SRS_TMP t,
            XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                           'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                           'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                     '//bond'
                     PASSING xmltype(t.t_fmtclobdata_xxxx)
                     COLUMNS bond     PATH 'instrumentId',
                             bondName PATH 'description') x;
     BEGIN
       InsertInstrument (p_InterMesID, l_InstrumentCode, l_InstrumentName);
     EXCEPTION
       WHEN OTHERS THEN
         NULL;
     END;
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM044;

   FUNCTION InsertTableCM045 (p_InterMesID in integer, p_Text out varchar2) return integer as
     l_InstrumentCode VARCHAR2(20);
     l_InstrumentName VARCHAR2(50);
   begin
     INSERT INTO dir_cm045_dbt(t_internalmessageid,
                               t_buyer,
                               t_seller,
                               t_optiontype,
                               t_premiumpayer,
                               t_premiumreceiver,
                               t_premiumcurrency,
                               t_premiumamount,
                               t_optionstyle,
                               T_OPTIONSTYLEID,
                               t_commencementdate,
                               t_expirationdate,
                               t_nominalcurrency,
                               t_nominalamount,
                               t_optionentitlement,
                               t_numberofoptions,
                               t_settlementtype,
                               t_strike,
                               t_strikecurrency,
                               T_BUYERID,
                               T_SELLERID,
                               T_PREMIUMPAYERID,
                               T_PREMIUMRECEIVERID,
                               T_PREMIUMCURRENCYID,
                               T_NOMINALCURRENCYID,
                               T_STRIKECURRENCYID,
                               T_OPTIONTYPEID,
                               T_SETTLEMENTTYPEID)
       SELECT p_InterMesID, x.buyer, x.seller, x.optiontype, x.premiumpayer, x.premiumreceiver, x.premiumcurrency,
              to_number(x.premiumamount, '999999999999999999999.99999999'),
              decode(x.expirationdate_e, null, 'A', 'E'),
              decode(x.expirationdate_e, null, DVSTYLE_AMERICAN, DVSTYLE_EUROPE),
              NVL(x.commencementdate, TO_DATE('01.01.0001','DD.MM.YYYY')),
              decode(x.expirationdate_e, null, NVL(x.expirationdate_a,TO_DATE('01.01.0001','DD.MM.YYYY')), x.expirationdate_e), x.nominalcurrency,
              to_number(x.nominalamount, '999999999999999999999.99999999'),
              to_number(x.optionentitlement, '999999999999999999999.99999999'),
              to_number(x.numberofoptions, '999999999999999999999.99999999'), x.settlementtype,
              to_number(x.strike, '999999999999999999999.99999999'), x.strikecurrency,
              DECODE(lower(x.buyer),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.seller),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.premiumpayer),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.premiumreceiver),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              (select fi1.t_fiid from dfininstr_dbt fi1 Where fi1.t_fi_kind = 1 and LOWER(fi1.t_ccy) = LOWER(x.premiumcurrency)),
              (select fi2.t_fiid from dfininstr_dbt fi2 Where fi2.t_fi_kind = 1 and LOWER(fi2.t_ccy) = LOWER(x.nominalcurrency)),
              (select fi3.t_fiid from dfininstr_dbt fi3 Where fi3.t_fi_kind = 1 and LOWER(fi3.t_ccy) = LOWER(x.strikecurrency)),
              (select pt1.t_inumberalg from dnamealg_dbt pt1 where pt1.t_itypealg = ALG_FI_DVTYPE and pt1.t_sznamealg = x.optiontype),
              (select pt3.t_inumberalg from dnamealg_dbt pt3 where pt3.t_itypealg = ALG_DV_OPTION_PAYKIND and pt3.t_sznamealg = x.settlementtype)
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//nsdext:bondBasketOption'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS buyer                 PATH 'buyerPartyReference/@href',
                               seller                PATH 'sellerPartyReference/@href',
                               optiontype            PATH 'optionType',
                               premiumpayer          PATH 'premium/payerPartyReference/@href',
                               premiumreceiver       PATH 'premium/receiverPartyReference/@href',
                               premiumcurrency       PATH 'premium/paymentAmount/currency',
                               premiumamount         PATH 'premium/paymentAmount/amount',
                               commencementdate DATE PATH 'americanExercise/commencementDate/adjustableDate/unadjustedDate',
                               expirationdate_a DATE PATH 'americanExercise/expirationDate/adjustableDate/unadjustedDate',
                               expirationdate_e DATE PATH 'europeanExercise/expirationDate/adjustableDate/unadjustedDate',
                               nominalcurrency       PATH 'notionalAmount/currency',
                               nominalamount         PATH 'notionalAmount/amount',
                               optionentitlement     PATH 'optionEntitlement',
                               numberofoptions       PATH 'numberOfOptions',
                               settlementtype        PATH 'settlementType',
                               strike                PATH 'strike/price/strikePrice',
                               strikecurrency        PATH 'strike/price/currency') x;
     SELECT SUBSTR (x.indx, 1, 20), SUBSTR (x.indxName, 1, 50)
       INTO l_InstrumentCode, l_InstrumentName
       FROM DIR_SRS_TMP t,
            XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                           'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                           'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                     '//index'
                     PASSING xmltype(t.t_fmtclobdata_xxxx)
                     COLUMNS indx     PATH 'instrumentId',
                             indxName PATH 'description') x;
     BEGIN
       InsertInstrument (p_InterMesID, l_InstrumentCode, l_InstrumentName);
     EXCEPTION
       WHEN OTHERS THEN
         NULL;
     END;
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM045;

   FUNCTION InsertTableCM046 (p_InterMesID in integer, p_Text out varchar2) return integer as
     l_InstrumentCode VARCHAR2(20);
     l_InstrumentName VARCHAR2(50);
   begin
     INSERT INTO dir_cm046_dbt(t_internalmessageid,
                               t_buyerid,
                               t_buyer,
                               t_sellerid,
                               t_seller,
                               t_numberofunits,
                               t_pricecurrencyid,
                               t_pricecurrency,
                               t_price,
                               t_periodmultiplier,
                               t_periodid,
                               t_period,
                               t_deliverymethodid,
                               t_deliverymethod,
                               t_settlmentdate,
                               t_deliverydate)
       SELECT p_InterMesID,
              DECODE(lower(x.buyer),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              x.buyer, 
              DECODE(lower(x.seller),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              x.seller, x.numberofunits,
              (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.pricecurrency)),
              x.pricecurrency, to_number(x.price, '999999999999999999999.99999999'),
              x.periodmultiplier,
              (select pt.t_inumberalg from dnamealg_dbt pt where pt.t_itypealg = ALG_DV_PAYMPERIODKIND and pt.t_sznamealg = x.period),
              x.period,
              (select pt.t_inumberalg from dnamealg_dbt pt where pt.t_itypealg = ALG_GENINF_SETTLMETHOD and pt.t_sznamealg = x.deliverymethod),
              x.deliverymethod, x.settlmentdate, x.deliverydate
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//nsdext:equitySimpleTransaction'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS buyer              PATH 'buyerPartyReference/@href',
                               seller             PATH 'sellerPartyReference/@href',
                               numberofunits      PATH 'fpmlext:numberOfUnits',
                               pricecurrency      PATH 'fpmlext:unitPrice/currency',
                               price              PATH 'fpmlext:unitPrice/amount',
                               periodmultiplier   PATH 'nsdext:term/periodMultiplier',
                               period             PATH 'nsdext:term/period',
                               deliverymethod     PATH 'deliveryMethod',
                               settlmentdate DATE PATH 'nsdext:settlementDate/adjustableDate/unadjustedDate',
                               deliverydate  DATE PATH 'nsdext:deliveryDate/adjustableDate/unadjustedDate') x;
     SELECT SUBSTR (x.equity, 1, 20), SUBSTR (x.equityName, 1, 50)
       INTO l_InstrumentCode, l_InstrumentName
       FROM DIR_SRS_TMP t,
            XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                           'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                           'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                     '//nsdext:equitySimpleTransaction'
                     PASSING xmltype(t.t_fmtclobdata_xxxx)
                     COLUMNS equity     PATH 'fpmlext:equity/instrumentId',
                             equityName PATH 'fpmlext:equity/description') x;
     BEGIN
       InsertInstrument (p_InterMesID, l_InstrumentCode, l_InstrumentName);
     EXCEPTION
       WHEN OTHERS THEN
         NULL;
     END;
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM046;

   FUNCTION InsertTableCM047 (p_InterMesID in integer, p_Text out varchar2) return integer as
     l_InstrumentCode VARCHAR2(20);
     l_InstrumentName VARCHAR2(50);
   begin
     INSERT INTO dir_cm047_dbt(t_internalmessageid,
                               t_buyerid,
                               t_buyer,
                               t_sellerid,
                               t_seller,
                               t_openunits,
                               t_settlmentdate,
                               t_settlcurrencyid,
                               t_settlcurrency,
                               t_settlementtypeid,
                               t_settlementtype,
                               t_pricecurrencyid,
                               t_pricecurrency,
                               t_price)
       SELECT p_InterMesID,
              DECODE(lower(x.buyer),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              x.buyer,
              DECODE(lower(x.seller),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              x.seller,
              x.openunits,
              x.settlmentdate,
              (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.settlcurrency)),
              x.settlcurrency,
              (select pt.t_inumberalg from dnamealg_dbt pt where pt.t_itypealg = ALG_DV_OPTION_PAYKIND and pt.t_sznamealg = x.settlementtype),
              x.settlementtype,
              (select fi.t_fiid from dfininstr_dbt fi Where fi.t_fi_kind = 1 and LOWER(fi.t_ccy) = LOWER(x.pricecurrency)),
              x.pricecurrency,
              to_number(x.price, '999999999999999999999.99999999')
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//equityForward'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS buyer              PATH 'buyerPartyReference/@href',
                               seller             PATH 'sellerPartyReference/@href',
                               openunits          PATH 'underlyer/singleUnderlyer/openUnits',
                               settlmentdate DATE PATH 'equityExercise/equityEuropeanExercise/expirationDate/adjustableDate/unadjustedDate',
                               settlcurrency      PATH 'equityExercise/settlementCurrency',
                               settlementtype     PATH 'equityExercise/settlementType',
                               pricecurrency      PATH 'forwardPrice/currency',
                               price              PATH 'forwardPrice/amount') x;
       SELECT SUBSTR (x.equity, 1, 20), SUBSTR (x.equityName, 1, 50)
         INTO l_InstrumentCode, l_InstrumentName
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//singleUnderlyer'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS equity     PATH 'equity/instrumentId',
                               equityName PATH 'equity/description') x;
     BEGIN
       InsertInstrument (p_InterMesID, l_InstrumentCode, l_InstrumentName);
     EXCEPTION
       WHEN OTHERS THEN
         NULL;
     END;
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM047;

   FUNCTION InsertTableCM048 (p_InterMesID in integer, p_Text out varchar2) return integer as
     l_InstrumentCode VARCHAR2(20);
     l_InstrumentName VARCHAR2(50);
   begin
     INSERT INTO dir_cm048_dbt(t_internalmessageid,
                               t_buyer,
                               t_seller,
                               t_optiontype,
                               t_openunit,
                               t_optionstyle,
                               T_OPTIONSTYLEID,
                               t_commencementdate,
                               t_expirationdate,
                               t_settlementcurrency,
                               t_settlementtype,
                               t_strike,
                               t_strikecurrency,
                               t_numberofoptions,
                               t_optionentitlement,
                               t_premiumpayer,
                               t_premiumreceiver,
                               t_premiumcurrency,
                               t_premiumamount,
                               T_BUYERID,
                               T_SELLERID,
                               T_PREMIUMPAYERID,
                               T_PREMIUMRECEIVERID,
                               T_PREMIUMCURRENCYID,
                               T_SETTLEMENTCURRENCYID,
                               T_STRIKECURRENCYID,
                               T_OPTIONTYPEID,
                               T_SETTLEMENTTYPEID)
       SELECT p_InterMesID, x.buyer, x.seller, x.optiontype, to_number(NVL(x.openunit,0), '999999999999999999999.99999999'),
              decode(x.expirationdate_e, null, 'A', 'E'),
              decode(x.expirationdate_e, null, DVSTYLE_AMERICAN, DVSTYLE_EUROPE),
              NVL(x.commencementdate, TO_DATE('01.01.0001','DD.MM.YYYY')),
              decode(x.expirationdate_e, null, NVL(x.expirationdate_a,TO_DATE('01.01.0001','DD.MM.YYYY')), x.expirationdate_e),
              x.settlementcurrency, x.settlementtype,
              to_number(x.strike, '999999999999999999999.99999999'), x.strikecurrency,
              to_number(x.numberofoptions, '999999999999999999999.99999999'),
              to_number(x.optionentitlement, '999999999999999999999.99999999'),
              x.premiumpayer, x.premiumreceiver, x.premiumcurrency,
              to_number(x.premiumamount, '999999999999999999999.99999999'),
              DECODE(lower(x.buyer),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.seller),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.premiumpayer),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              DECODE(lower(x.premiumreceiver),'party1',IR_PARTY_KIND_PARTY1, IR_PARTY_KIND_PARTY2),
              (select fi1.t_fiid from dfininstr_dbt fi1 Where fi1.t_fi_kind = 1 and LOWER(fi1.t_ccy) = LOWER(x.premiumcurrency)),
              (select fi2.t_fiid from dfininstr_dbt fi2 Where fi2.t_fi_kind = 1 and LOWER(fi2.t_ccy) = LOWER(x.settlementcurrency)),
              (select fi3.t_fiid from dfininstr_dbt fi3 Where fi3.t_fi_kind = 1 and LOWER(fi3.t_ccy) = LOWER(x.strikecurrency)),
              (select pt1.t_inumberalg from dnamealg_dbt pt1 where pt1.t_itypealg = ALG_FI_DVTYPE and pt1.t_sznamealg = x.optiontype),
              (select pt3.t_inumberalg from dnamealg_dbt pt3 where pt3.t_itypealg = ALG_DV_OPTION_PAYKIND and pt3.t_sznamealg = x.settlementtype)
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//equityOption'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS buyer                 PATH 'buyerPartyReference/@href',
                               seller                PATH 'sellerPartyReference/@href',
                               optiontype            PATH 'optionType',
                               openunit              PATH 'underlyer/singleUnderlyer/basket/basketConstituent/constituentWeight/openUnits',
                               commencementdate DATE PATH 'equityExercise/equityAmericanExercise/commencementDate/adjustableDate/unadjustedDate',
                               expirationdate_a DATE PATH 'equityExercise/equityAmericanExercise/expirationDate/adjustableDate/unadjustedDate',
                               expirationdate_e DATE PATH 'equityExercise/equityEuropeanExercise/expirationDate/adjustableDate/unadjustedDate',
                               settlementcurrency    PATH 'equityExercise/settlementCurrency',
                               settlementtype        PATH 'equityExercise/settlementType',
                               strike                PATH 'strike/strikePrice',
                               strikecurrency        PATH 'strike/currency',
                               numberofoptions       PATH 'numberOfOptions',
                               optionentitlement     PATH 'optionEntitlement',
                               premiumpayer          PATH 'equityPremium/payerPartyReference/@href',
                               premiumreceiver       PATH 'equityPremium/receiverPartyReference/@href',
                               premiumcurrency       PATH 'equityPremium/paymentAmount/currency',
                               premiumamount         PATH 'equityPremium/paymentAmount/amount') x;
     SELECT SUBSTR (x.equity, 1, 20), SUBSTR (x.equityName, 1, 50)
       INTO l_InstrumentCode, l_InstrumentName
       FROM DIR_SRS_TMP t,
            XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                           'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                           'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                     '//underlyer/singleUnderlyer'
                     PASSING xmltype(t.t_fmtclobdata_xxxx)
                     COLUMNS equity     PATH 'equity/instrumentId',
                             equityName PATH 'equity/description') x;
     BEGIN
       InsertInstrument (p_InterMesID, l_InstrumentCode, l_InstrumentName);
     EXCEPTION
       WHEN OTHERS THEN
         NULL;
     END;
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM048;
   
   FUNCTION InsertTableCM051 (p_InterMesID IN INTEGER, p_Text OUT VARCHAR2) RETURN INTEGER AS
   BEGIN
     INSERT INTO dir_cm051_dbt (T_INTERNALMESSAGEID,
                                T_PAYERPARTYREF_FXLEG,
                                T_PAYERPARTYREF_FXLEGID,
                                T_RECEIVERPARTYREF_FXLEG,
                                T_RECEIVERPARTYREF_FXLEGID,
                                T_PRICE,
                                T_PRICECURRENCY,
                                T_PRICEUNIT,
                                T_AMOUNT,
                                T_AMOUNTCURRENCY,
                                T_PAYMENTDATE_FXLEG,
                                T_EXTYPE,
                                T_PAYERPARTYREFERENCE,
                                T_PAYERPARTYREFERENCEID,
                                T_RECEIVERPARTYREFERENCE,
                                T_RECEIVERPARTYREFERENCEID,
                                T_INSTRUMENTID,
                                T_COMMODITY,
                                T_COMUNIT,
                                T_COMPRICECURRENCY,
                                T_DELIVERYDATE,
                                T_QUANTITY,
                                T_QUANTITYUNIT,
                                T_CONVERSIONFACTOR,
                                T_PAYMENTDATE)
                         SELECT p_InterMesID,
                                x.payerpartyref_fxleg,
                                DECODE (LOWER (x.payerpartyref_fxleg),
                                        'party1', IR_PARTY_KIND_PARTY1,
                                        IR_PARTY_KIND_PARTY2),
                                x.receiverpartyref_fxleg,
                                DECODE (LOWER (x.receiverpartyref_fxleg),
                                        'party1', IR_PARTY_KIND_PARTY1,
                                        IR_PARTY_KIND_PARTY2),
                                to_number(x.price, '999999999999999999999.99999999'),
                                x.pricecurrency,
                                x.priceunit,
                                to_number(x.amount, '999999999999999999999.99999999'),
                                x.amountcurrency,
                                x.paymentdate_fxleg,
                                DECODE (x.payerpartyreference_p, NULL, 'F', 'P'),
                                DECODE (x.payerpartyreference_p,
                                        NULL, x.payerpartyreference_f,
                                        x.payerpartyreference_p),
                                DECODE (
                                   LOWER (
                                      DECODE (x.payerpartyreference_p,
                                              NULL, x.payerpartyreference_f,
                                              x.payerpartyreference_p)),
                                   'party1', IR_PARTY_KIND_PARTY1,
                                   IR_PARTY_KIND_PARTY2),
                                DECODE (x.receiverpartyreference_p,
                                        NULL, x.receiverpartyreference_f,
                                        x.receiverpartyreference_p),
                                DECODE (
                                   LOWER (
                                      DECODE (x.receiverpartyreference_p,
                                              NULL, x.receiverpartyreference_f,
                                              x.receiverpartyreference_p)),
                                   'party1', IR_PARTY_KIND_PARTY1,
                                   IR_PARTY_KIND_PARTY2),
                                DECODE (x.instrumentid_p,
                                        NULL, x.instrumentid_f,
                                        x.instrumentid_p),
                                DECODE (x.commodity_p, NULL, x.commodity_f, x.commodity_p),
                                DECODE (x.comunit_p, NULL, x.comunit_f, x.comunit_p),
                                DECODE (x.compricecurrency_p,
                                        NULL, x.compricecurrency_f,
                                        x.compricecurrency_p),
                                x.deliverydate,
                                DECODE (to_number(x.quantity_p, '999999999999999999999.99999999'), NULL, 
                                        to_number(x.quantity_f, '999999999999999999999.99999999'), 
                                        to_number(x.quantity_p, '999999999999999999999.99999999')),
                                x.quantityunit,
                                to_number(x.conversionfactor, '999999999999999999999.99999999'),
                                x.paymentdate
                           FROM DIR_SRS_TMP t,
                                XMLTABLE (
                                   xmlnamespaces (
                                      DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                      'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                      'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                                   '//commodityForward'
                                   PASSING xmltype (t.t_fmtclobdata_xxxx)
                                   COLUMNS payerpartyref_fxleg PATH 'fixedLeg/payerPartyReference/@href',
                                           receiverpartyref_fxleg PATH 'fixedLeg/receiverPartyReference/@href',
                                           price PATH 'fixedLeg/fixedPrice/price',
                                           pricecurrency PATH 'fixedLeg/fixedPrice/priceCurrency',
                                           priceunit PATH 'fixedLeg/fixedPrice/priceUnit',
                                           amountcurrency PATH 'fixedLeg/totalPrice/currency',
                                           amount PATH 'fixedLeg/totalPrice/amount',
                                           paymentdate_fxleg DATE
                                              PATH 'fixedLeg/paymentDates/adjustableDates/unadjustedDate',
                                           payerpartyreference_p PATH 'nsdext:commodityForwardPhysicalLeg/payerPartyReference/@href',
                                           payerpartyreference_f PATH 'nsdext:floatingForwardLeg/payerPartyReference/@href',
                                           receiverpartyreference_p PATH 'nsdext:commodityForwardPhysicalLeg/receiverPartyReference/@href',
                                           receiverpartyreference_f PATH 'nsdext:floatingForwardLeg/receiverPartyReference/@href',
                                           instrumentid_p PATH 'nsdext:commodityForwardPhysicalLeg/nsdext:commodity/instrumentId',
                                           instrumentid_f PATH 'nsdext:floatingForwardLeg/nsdext:commodity/instrumentId',
                                           commodity_p PATH 'nsdext:commodityForwardPhysicalLeg/nsdext:commodity/description',
                                           commodity_f PATH 'nsdext:floatingForwardLeg/nsdext:commodity/description',
                                           comunit_p PATH 'nsdext:commodityForwardPhysicalLeg/nsdext:commodity/unit',
                                           comunit_f PATH 'nsdext:floatingForwardLeg/nsdext:commodity/unit',
                                           compricecurrency_p PATH 'nsdext:commodityForwardPhysicalLeg/nsdext:commodity/currency',
                                           compricecurrency_f PATH 'nsdext:floatingForwardLeg/nsdext:commodity/currency',
                                           deliverydate DATE
                                              PATH 'nsdext:commodityForwardPhysicalLeg/nsdext:deliveryPeriods/periods/unadjustedDate',
                                           quantity_p PATH 'nsdext:commodityForwardPhysicalLeg/nsdext:deliveryQuantity/totalPhysicalQuantity/quantity',
                                           quantity_f PATH 'nsdext:floatingForwardLeg/totalNotionalQuantity',
                                           quantityunit PATH 'nsdext:commodityForwardPhysicalLeg/nsdext:deliveryQuantity/totalPhysicalQuantity/quantityUnit',
                                           conversionfactor PATH 'nsdext:commodityForwardPhysicalLeg/nsdext:conversionFactor',
                                           paymentdate DATE
                                              PATH 'nsdext:floatingForwardLeg/paymentDates/adjustableDates/unadjustedDate') x;

     UpdateProductID (p_InterMesID);
     RETURN 0;
   EXCEPTION
     WHEN OTHERS
   THEN
     DeleteSRS (p_InterMesID);
     p_Text := SQLERRM;
     RETURN 1;
   END InsertTableCM051;

   FUNCTION InsertTableCM053 (p_InterMesID in integer, p_Text out varchar2) return integer as
      v_effectiveDate DATE;
      v_TerminationDate DATE;
      v_settlementcurrency VARCHAR2(3);
      v_payerpartyref_leg1 VARCHAR2(6);
      v_receiverpartyref_leg1 VARCHAR2(6);
      v_periodMultiplier_Leg1 NUMBER;
      v_period_Leg1 CHAR;
      v_amountcurrency_leg1 VARCHAR2(3);
      v_amount_Leg1 NUMBER;
      v_Quantity_Leg1 NUMBER;
      v_PaymentDate_Leg1 DATE;
      v_payerPartyRef_Leg2 VARCHAR2(6);
      v_receiverPartyRef_Leg2 VARCHAR2(6);
      v_periodMultiplier_Leg2 NUMBER;
      v_period_Leg2 CHAR;
      v_amountCurrency_Leg2 VARCHAR2(3);
      v_amount_Leg2 NUMBER;
      v_Quantity_Leg2 NUMBER;
      v_PaymentDate_Leg2 DATE;
      v_instrumentid VARCHAR2(40);
      v_commodity VARCHAR2(40);
      v_priceunit VARCHAR2(24);
      v_pricecurrency VARCHAR2(3);
      v_payerpartyreference VARCHAR2(6);
      v_receiverpartyreference VARCHAR2(6);
      v_deliverydate DATE;
      v_quantityunit VARCHAR2(24);
      v_quantity NUMBER(32, 12);
      v_conversionfactor NUMBER(32, 12);
      v_calculationdate_leg1 DATE;
      v_calculationdate_leg2 DATE;
   BEGIN
/*DAN*/SELECT x.effectiveDate, x.TerminationDate, x.settlementCurrency, x.instrumentid, x.commodity, x.priceunit, x.pricecurrency, x.payerpartyreference, x.receiverpartyreference, x.deliverydate, x.quantityunit, to_number(x.quantity, '999999999999999999999.99999999'), to_number(x.conversionfactor, '999999999999999999999.99999999')
         INTO v_effectiveDate, v_TerminationDate, v_settlementCurrency, v_instrumentid, v_commodity, v_priceunit, v_pricecurrency, v_payerpartyreference, v_receiverpartyreference, v_deliverydate, v_quantityunit, v_quantity, v_conversionfactor
         FROM DIR_SRS_TMP t,
                  XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                                'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                                'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                           '//commoditySwap'
                           PASSING xmltype(t.t_fmtclobdata_xxxx)
                           COLUMNS effectiveDate     DATE PATH 'effectiveDate/adjustableDate/unadjustedDate',
                                 TerminationDate   DATE PATH 'terminationDate/adjustableDate/unadjustedDate',
                                 settlementCurrency     PATH 'settlementCurrency',
                                 instrumentid PATH 'nsdext:commoditySwapPhysicalLeg/nsdext:commodity/instrumentId',
                                 commodity PATH 'nsdext:commoditySwapPhysicalLeg/nsdext:commodity/description',
                                 priceunit PATH 'nsdext:commoditySwapPhysicalLeg/nsdext:commodity/unit',
                                 pricecurrency PATH 'nsdext:commoditySwapPhysicalLeg/nsdext:commodity/currency',
                                 payerpartyreference PATH 'nsdext:commoditySwapPhysicalLeg/payerPartyReference/@href',
                                 receiverpartyreference PATH 'nsdext:commoditySwapPhysicalLeg/receiverPartyReference/@href',
                                 deliverydate DATE PATH 'nsdext:commoditySwapPhysicalLeg/nsdext:deliveryPeriods/periods/unadjustedDate',
                                 quantityunit PATH 'nsdext:commoditySwapPhysicalLeg/nsdext:deliveryQuantity/totalPhysicalQuantity/quantityUnit',
                                 quantity PATH 'nsdext:commoditySwapPhysicalLeg/nsdext:deliveryQuantity/totalPhysicalQuantity/quantity',
                                 conversionfactor PATH 'nsdext:commoditySwapPhysicalLeg/nsdext:conversionFactor'
                                 ) x; 
   
/*DAN*/ FOR c IN(SELECT x.payerPartyReference, x.receiverPartyReference, x.periodMultiplier, x.period, x.currency, to_number(x.amount, '999999999999999999999.99999999') amount, to_number(x.totalNotionalQuantity, '999999999999999999999.99999999') totalNotionalQuantity, x.paymentDates, x.calculationDates, ROWNUM as numleg
               FROM DIR_SRS_TMP t,
                        XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                                         'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                                         'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                                    '//fixedLeg'
                                    PASSING xmltype(t.t_fmtclobdata_xxxx)
                                    COLUMNS payerPartyReference    PATH 'payerPartyReference/@href',
                                          receiverPartyReference PATH 'receiverPartyReference/@href',
                                          periodMultiplier       PATH 'calculationPeriodsSchedule/periodMultiplier',
                                          period                 PATH 'calculationPeriodsSchedule/period',
                                          currency               PATH 'totalPrice/currency',
                                          amount                 PATH 'totalPrice/amount',
                                          totalNotionalQuantity  PATH 'totalNotionalQuantity',
                                          paymentDates      DATE PATH 'paymentDates/adjustableDates/unadjustedDate',
                                          calculationDates  DATE PATH 'calculationDates/unadjustedDate'
                                          ) x) LOOP
         IF(c.numleg = 1) THEN
            v_payerpartyref_leg1 := c.payerPartyReference;
            v_receiverpartyref_leg1 := c.receiverPartyReference;
            v_periodMultiplier_Leg1 := c.periodMultiplier;
            v_period_Leg1 := c.period;
            v_amountcurrency_leg1 := c.currency;
            v_amount_Leg1 := to_number(c.amount, '999999999999999999999.99999999');
            v_Quantity_Leg1 := c.totalNotionalQuantity;
            v_PaymentDate_Leg1 := c.paymentDates;
            v_calculationdate_leg1 := c.calculationDates;
         ELSE
            v_payerPartyRef_Leg2 :=  c.payerPartyReference;
            v_receiverPartyRef_Leg2 :=  c.receiverPartyReference;
            v_periodMultiplier_Leg2 :=  c.periodMultiplier;
            v_period_Leg2 :=  c.period;
            v_amountCurrency_Leg2 :=  c.currency;
            v_amount_Leg2 :=  to_number(c.amount, '999999999999999999999.99999999');
            v_Quantity_Leg2 :=  c.totalNotionalQuantity;
            v_PaymentDate_Leg2 :=  c.paymentDates;
            v_calculationdate_leg2 := c.calculationDates;
         END IF;
      END LOOP;

      INSERT INTO DIR_CM053_DBT(T_INTERNALMESSAGEID, 
                                T_STRUCTURETYPE,
                                T_effectiveDate,
                                T_TerminationDate,
                                T_settlementCurrency,
                                T_payerpartyref_leg1ID,
                                T_payerpartyref_leg1,
                                T_receiverpartyref_leg1ID,
                                T_receiverpartyref_leg1,
                                T_periodMultiplier_Leg1,
                                T_period_Leg1,
                                T_amountcurrency_leg1,
                                T_amount_Leg1, 
                                T_Quantity_Leg1,
                                T_PaymentDate_Leg1,
                                t_payerpartyref_leg2id,
                                T_payerPartyRef_Leg2,
                                T_receiverPartyRef_Leg2ID,
                                T_receiverPartyRef_Leg2,
                                T_periodMultiplier_Leg2,
                                T_period_Leg2,
                                T_amountCurrency_Leg2,
                                T_amount_Leg2, T_Quantity_Leg2,
                                T_PaymentDate_Leg2,
                                T_instrumentid,
                                T_commodity,
                                T_priceunit,
                                T_pricecurrency,
                                T_payerpartyreferenceID,
                                T_payerpartyreference,
                                T_receiverpartyreferenceID,
                                T_receiverpartyreference,
                                T_deliverydate,
                                T_quantityunit,
                                T_quantity,
                                T_conversionfactor,
                                t_calculationdate_leg1,
                                t_calculationdate_leg2)
      SELECT p_InterMesID,
             0,
             v_effectiveDate,
             v_TerminationDate,
             v_settlementCurrency,
             (CASE WHEN LOWER(v_payerPartyRef_Leg1) = 'Party2' THEN RSB_PAYMENTS_API.IR_PARTY_KIND_PARTY2
                   ELSE RSB_PAYMENTS_API.IR_PARTY_KIND_PARTY1 END) as payerPartyRef_Leg1ID,
             v_payerpartyref_leg1,
             (CASE WHEN LOWER(v_receiverpartyref_leg1) = 'Party2' THEN RSB_PAYMENTS_API.IR_PARTY_KIND_PARTY2
                   ELSE RSB_PAYMENTS_API.IR_PARTY_KIND_PARTY1 END) as receiverPartyRef_Leg1ID,
             v_receiverpartyref_leg1,
             v_periodMultiplier_Leg1,
             v_period_Leg1,
             v_amountcurrency_leg1,
             v_amount_Leg1,
             v_Quantity_Leg1,
             v_PaymentDate_Leg1,
             (CASE WHEN LOWER(v_payerPartyRef_Leg2) = 'Party2' THEN RSB_PAYMENTS_API.IR_PARTY_KIND_PARTY2
                   ELSE RSB_PAYMENTS_API.IR_PARTY_KIND_PARTY1 END) as payerPartyRef_Leg2ID,
             v_payerPartyRef_Leg2,
             (CASE WHEN LOWER(v_receiverPartyRef_Leg2) = 'Party2' THEN RSB_PAYMENTS_API.IR_PARTY_KIND_PARTY2
                   ELSE RSB_PAYMENTS_API.IR_PARTY_KIND_PARTY1 END) as receiverPartyRef_Leg2ID,
             v_receiverPartyRef_Leg2,
             v_periodMultiplier_Leg2,
             v_period_Leg2,
             v_amountCurrency_Leg2,
             v_amount_Leg2,
             v_Quantity_Leg2,
             v_PaymentDate_Leg2,
             v_instrumentid,
             v_commodity,
             v_priceunit,
             v_pricecurrency,
             (CASE WHEN LOWER(v_payerpartyreference) = 'Party2' THEN RSB_PAYMENTS_API.IR_PARTY_KIND_PARTY2
                   ELSE RSB_PAYMENTS_API.IR_PARTY_KIND_PARTY1 END) as payerpartyreferenceID,
             v_payerpartyreference,
             (CASE WHEN LOWER(v_receiverpartyreference) = 'Party2' THEN RSB_PAYMENTS_API.IR_PARTY_KIND_PARTY2
                   ELSE RSB_PAYMENTS_API.IR_PARTY_KIND_PARTY1 END) as receiverpartyreferenceID,
             v_receiverpartyreference,
             v_deliverydate,
             v_quantityunit,
             v_quantity,
             v_conversionfactor,
             v_calculationdate_leg1,
             v_calculationdate_leg2
      FROM DUAL;

      UpdateProductID(p_InterMesID);
      Return 0;
   EXCEPTION
      WHEN OTHERS THEN
         DeleteSRS (p_InterMesID);
         p_Text := SQLERRM;
         Return 1;
   END InsertTableCM053;

   FUNCTION InsertTableCM093 (p_InterMesID in integer, p_Text out varchar2) return integer as
     l_ResCheck INTEGER;
   begin
     l_ResCheck := CheckReportReferences;
     IF l_ResCheck > 0 THEN
       p_Text := 'В загружаемом сообщении есть элементы <allTrades> и <masterAgreementID>, обработка которых в системе не реализована.';
       Return 12;
     END IF;
     UPDATE dir_generalinf_dbt gen
        SET gen.t_tradeschoice = 'TradesList'
      WHERE gen.t_internalmessageid = p_InterMesID;
     INSERT INTO dir_cm093_dbt(t_internalmessageid,
                               t_tradeident,
                               t_tradeoblstatus
                               )
       SELECT p_InterMesID, y.tradeident, x.tradeoblstatus
         FROM DIR_SRS_TMP t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//nsdext:tradesWithStatus'
                       PASSING xmltype(t.t_fmtclobdata_xxxx)
                       COLUMNS reportReferences XMLTYPE PATH 'nsdext:reportReferences',
                               tradeoblstatus           PATH 'nsdext:tradeObligationStatus') x,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//nsdext:tradeId'
                       PASSING x.reportReferences
                       COLUMNS tradeident PATH '/*') y
                            ;
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM093;


   FUNCTION InsertTableCM094 (p_InterMesID in integer, p_Text out varchar2) return integer as
     l_ResCheck         INTEGER;
     l_reportParty      VARCHAR2(6);
     l_valuationMethod  CHAR;
     l_mtmidentifier    VARCHAR2(34);
     l_prevDate         DATE;
     l_094ID            INTEGER;
     l_Repository       VARCHAR2(32) :='';
     l_Party1           VARCHAR2(30) :='';
     l_Party2           VARCHAR2(30) :='';
     
   begin
      SELECT x.reportParty, x.valuationMethod, nvl(x.mtmidentifier, '')
        INTO l_reportParty, l_valuationMethod,l_mtmidentifier
        FROM DIR_SRS_TMP t,
          XMLTABLE (
          xmlnamespaces (
          DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
          'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
          'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
          '//nsdext:markToMarketValuation'
          PASSING xmltype (t.t_fmtclobdata_xxxx)
          COLUMNS reportParty  PATH 'nsdext:reportParty',
          valuationMethod      PATH 'nsdext:valuationMethod',
          mtmidentifier        PATH 'nsdext:mtmidentifier') x;

   FOR trade IN ( 
        SELECT x.*
          FROM (      SELECT x.*
                        FROM DIR_SRS_TMP t,
                             XMLTABLE (
                               xmlnamespaces (
                                 DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                 'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                 'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                               '//nsdext:markToMarketValuation/*'
                               PASSING xmltype (t.t_fmtclobdata_xxxx)
                               COLUMNS Counterparty XMLTYPE PATH '/*') x
                       WHERE x.Counterparty.getrootelement () LIKE
                               ('reportIdentifier%')) t,
               XMLTABLE (
                 xmlnamespaces (
                   DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                   'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                   'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                 '/*'
                 PASSING Counterparty
                 COLUMNS partyReference  PATH 'partyReference/@href',
                         tradeId PATH 'tradeId') x ) LOOP
      
        IF(trade.partyReference = 'TradeRepository') THEN
          l_Repository:=  trade.tradeID;              
        ELSIF(trade.partyReference = 'Party1') THEN
          l_Party1:=  trade.tradeID;              
        ELSIF(trade.partyReference = 'Party2') THEN
          l_Party2:=  trade.tradeID;              
        END IF;                   
      END LOOP;        
      
   
     UPDATE dir_generalinf_dbt gen
        SET gen.T_PARTYREF_TRADEREPOSITORY = 'TradeRepository',
            gen.T_TRADEID_TRADEREPOSITORY = l_Repository,
            gen.T_PARTYREF_PARTY1 = 'Party1',
            gen.T_TRADEID_PARTY1 = l_Party1,
            gen.T_PARTYREF_PARTY2 = 'Party2',
            gen.T_TRADEID_PARTY2 = l_Party2,
            gen.T_REPORTPARTY = l_reportParty
      WHERE gen.t_internalmessageid = p_InterMesID;
                           
                 
     l_prevdate := TO_DATE('01.01.0001','DD.MM.YYYY');                       
     FOR c IN( SELECT x.valuationDate, y.* ,(select fis.t_fiid from dfininstr_dbt fis Where fis.t_fi_kind = 1 and LOWER(fis.t_ccy) = LOWER(y.currency)) CurID
           FROM (  SELECT x.*
                     FROM DIR_SRS_TMP t,
                      XMLTABLE (
                       xmlnamespaces (
                        DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                       'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                       'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//nsdext:markToMarketValuation/*'
                        PASSING xmltype (t.t_fmtclobdata_xxxx)
                        COLUMNS markToMarketDetails XMLTYPE PATH '/*') x
                       WHERE x.markToMarketDetails.getrootelement () LIKE
                       ('markToMarketDetails%')) t,
                XMLTABLE (
                  xmlnamespaces (
                    DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                    'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                    'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                  '/*'
                  PASSING markToMarketDetails
                  COLUMNS markToMarketInformation XMLTYPE
                            PATH 'nsdext:markToMarketInformation',
                          valuationDate DATE PATH 'nsdext:valuationDate') x,
                XMLTABLE (
                  xmlnamespaces (
                    DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                    'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                    'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                  'nsdext:markToMarketInformation'
                  PASSING x.markToMarketInformation
                  COLUMNS tradeId PATH 'nsdext:tradeId',
                amount PATH 'nsdext:amount',
                currency PATH 'nsdext:currency') y) LOOP
      IF(c.valuationDate <> l_prevdate) THEN
        Insert into dir_cm094_dbt (T_INTERNALMESSAGEID, T_MTMIDENTIFIER, T_VALUATIONMETHOD, T_VALUATIONDATE )
             values (p_InterMesID, l_mtmidentifier, l_valuationMethod, c.valuationDate )
           returning T_ID into l_094ID;
        l_prevdate := c.valuationDate;
      END IF;
        Insert into dir_trades_dbt (T_INTERNALMESSAGEID, T_VALUATIONDATE, T_TRADEID, T_AMOUNT, T_CURRENCY, T_CODE_CURRENCY )
             values (p_InterMesID,  c.valuationDate , c.tradeID, to_number(c.Amount, '999999999999999999999.99999999'), c.CurID, c.currency );
                            
      END LOOP;
                           
     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM094;

   FUNCTION InsertTableCM092 (p_InterMesID in integer, p_Text out varchar2) return integer as
     l_ResCheck         INTEGER;
     l_reportPartyId    NUMBER (5);
     l_reportParty      dir_generalinf_dbt.T_REPORTPARTY%TYPE;
     l_prevDate         DATE;
     l_092ID            INTEGER;
     l_Repository       dir_generalinf_dbt.T_TRADEID_TRADEREPOSITORY%TYPE;
     l_Party1           dir_generalinf_dbt.T_TRADEID_PARTY1%TYPE;
     l_Party2           dir_generalinf_dbt.T_TRADEID_PARTY2%TYPE;
     l_OriginRepository dir_generalinf_dbt.T_ORIGINID_TRADEREPOSITORY%TYPE;
     l_OriginParty1     dir_generalinf_dbt.T_ORIGINATINGID_PARTY1%TYPE; 
     l_OriginParty2     dir_generalinf_dbt.T_ORIGINATINGID_PARTY2%TYPE;
     l_RepositoryLinkId dir_generalinf_dbt.T_LINKID%TYPE;
   begin
      SELECT x.reportParty 
        INTO l_reportParty 
        FROM DIR_SRS_TMP t,
          XMLTABLE (
          xmlnamespaces (
          DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
          'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
          'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
          '//nsdext:transfersAndExecution'
          PASSING xmltype (t.t_fmtclobdata_xxxx)
          COLUMNS reportParty         PATH 'nsdext:reportParty') x;

   IF(upper(l_reportParty) = upper('All')) THEN
     l_reportPartyId := 3;
   ELSIF(upper(l_reportParty) = upper('Party1')) THEN
     l_reportPartyId := 1;
   ELSIF(upper(l_reportParty) = upper('Party2')) THEN
     l_reportPartyId := 2;
   ELSE
     l_reportPartyId := 0;
   END IF;

   FOR trade IN (
        SELECT x.partyReference, x.tradeId, x.linkId, y.*
          FROM (      SELECT x.*
                        FROM DIR_SRS_TMP t,
                             XMLTABLE (
                               xmlnamespaces (
                                 DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                 'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                 'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                               '//nsdext:transfersAndExecution/*'
                               PASSING xmltype (t.t_fmtclobdata_xxxx)
                               COLUMNS Counterparty XMLTYPE PATH '/*') x
                       WHERE x.Counterparty.getrootelement () LIKE
                               ('reportIdentifier%')) t,
                XMLTABLE (
                  xmlnamespaces (
                    DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                    'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                    'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                  '/*'
                  PASSING Counterparty
                  COLUMNS originatingTradeId XMLTYPE 
                            PATH 'originatingTradeId',
                          partyReference  PATH 'partyReference/@href',
                          tradeId PATH 'tradeId',
                          linkId PATH 'linkId') x,
                XMLTABLE (
                  xmlnamespaces (
                    DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                    'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                    'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                  'originatingTradeId'
                  PASSING x.originatingTradeId
                  COLUMNS originPartyReference  PATH 'partyReference/@href',
                          originTradeId PATH 'tradeId') y ) LOOP

        IF(trade.partyReference = 'TradeRepository') THEN
          l_Repository:=  trade.tradeID;
          l_RepositoryLinkId:= trade.LinkId;
        ELSIF(trade.partyReference = 'Party1') THEN
          l_Party1:=  trade.tradeID;
        ELSIF(trade.partyReference = 'Party2') THEN
          l_Party2:=  trade.tradeID;
        END IF;
        IF(trade.originPartyReference = 'TradeRepository') THEN
          l_OriginRepository:=  trade.originTradeId;
        ELSIF(trade.originPartyReference = 'Party1') THEN
          l_OriginParty1:=  trade.originTradeId;
        ELSIF(trade.originPartyReference = 'Party2') THEN
          l_OriginParty2:=  trade.originTradeId;
        END IF;
      END LOOP;

     UPDATE dir_generalinf_dbt gen
        SET gen.T_PARTYREF_TRADEREPOSITORY = 'TradeRepository',
            gen.T_TRADEID_TRADEREPOSITORY = l_Repository,
            gen.T_LINKID = l_RepositoryLinkId,
            gen.T_PARTYREF_ORIGINID_TRADEREP = 'TradeRepository',
            gen.T_ORIGINID_TRADEREPOSITORY = l_OriginRepository,
            gen.T_PARTYREF_PARTY1 = 'Party1',
            gen.T_TRADEID_PARTY1 = l_Party1,
            gen.T_PARTYREF_ORIGINID_PARTY1 = 'Party1',
            gen.T_ORIGINATINGID_PARTY1 =  l_OriginParty1,
            gen.T_PARTYREF_PARTY2 = 'Party2',
            gen.T_TRADEID_PARTY2 = l_Party2,
            gen.T_PARTYREF_ORIGINID_PARTY2 = 'Party2',
            gen.T_ORIGINATINGID_PARTY2 = l_OriginParty2,
            gen.T_REPORTPARTY = l_reportParty
      WHERE gen.t_internalmessageid = p_InterMesID;

     l_prevdate := TO_DATE('01.01.0001','DD.MM.YYYY');
     FOR c IN( SELECT x.valuationDate, 
                      credSupA.*, 
                      (select fis.t_fiid from dfininstr_dbt fis Where fis.t_fi_kind = 1 and LOWER(fis.t_ccy) = LOWER(credSupA.credSupA_Currency)) credSupA_CurID,
                      credSupB.*, 
                      (select fis.t_fiid from dfininstr_dbt fis Where fis.t_fi_kind = 1 and LOWER(fis.t_ccy) = LOWER(credSupB.credSupB_Currency)) credSupB_CurID,
                      indepA.*, 
                      (select fis.t_fiid from dfininstr_dbt fis Where fis.t_fi_kind = 1 and LOWER(fis.t_ccy) = LOWER(indepA.indepA_Currency)) indepA_CurID,
                      credSupO.*, 
                      (select fis.t_fiid from dfininstr_dbt fis Where fis.t_fi_kind = 1 and LOWER(fis.t_ccy) = LOWER(credSupO.credSupO_Currency)) credSupO_CurID
           FROM (  SELECT x.*
                     FROM DIR_SRS_TMP t,
                      XMLTABLE (
                       xmlnamespaces (
                        DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                       'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                       'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//nsdext:transfersAndExecution/*'
                        PASSING xmltype (t.t_fmtclobdata_xxxx)
                        COLUMNS creditSupportInformation XMLTYPE PATH '/*') x
                       WHERE x.creditSupportInformation.getrootelement() LIKE
                       ('creditSupportInformation%')) t,
                XMLTABLE (
                  xmlnamespaces (
                    DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                    'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                    'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                  '/*'
                  PASSING creditSupportInformation
                  COLUMNS creditSupportAmount XMLTYPE
                            PATH 'nsdext:creditSupportAmount',
                          creditSupportBalance XMLTYPE
                            PATH 'nsdext:creditSupportBalance',
                          independentAmount XMLTYPE
                            PATH 'nsdext:independentAmount',
                          creditSupportObligations XMLTYPE
                            PATH 'nsdext:creditSupportObligations',
                          valuationDate DATE PATH 'nsdext:valuationDate') x,
                XMLTABLE (
                  xmlnamespaces (
                    DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                    'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                    'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                  'nsdext:creditSupportAmount'
                  PASSING x.creditSupportAmount
                  COLUMNS credSupA_transorReference PATH 'nsdext:transferorReference/@href',
                          credSupA_transeeReference PATH 'nsdext:transfereeReference/@href',
                          credSupA_Amount PATH 'nsdext:amount',
                          credSupA_Currency PATH 'nsdext:currency')(+) credSupA,
                XMLTABLE (
                  xmlnamespaces (
                    DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                    'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                    'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                  'nsdext:creditSupportBalance'
                  PASSING x.creditSupportBalance
                  COLUMNS credSupB_transorReference PATH 'nsdext:transferorReference/@href',
                          credSupB_transeeReference PATH 'nsdext:transfereeReference/@href',
                          credSupB_Amount PATH 'nsdext:amount',
                          credSupB_Currency PATH 'nsdext:currency') credSupB,
                XMLTABLE (
                  xmlnamespaces (
                    DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                    'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                    'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                  'nsdext:independentAmount'
                  PASSING x.independentAmount
                  COLUMNS indepA_transorReference PATH 'nsdext:transferorReference/@href',
                          indepA_transeeReference PATH 'nsdext:transfereeReference/@href',
                          indepA_Amount PATH 'nsdext:amount',
                          indepA_Currency PATH 'nsdext:currency') indepA,
                XMLTABLE (
                  xmlnamespaces (
                    DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                    'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                    'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                  'nsdext:creditSupportObligations'
                  PASSING x.creditSupportObligations 
                  COLUMNS credSupO_transorReference PATH 'nsdext:transferorReference/@href',
                          credSupO_transeeReference PATH 'nsdext:transfereeReference/@href',
                          credSupO_Amount PATH 'nsdext:amount',
                          credSupO_Currency PATH 'nsdext:currency') credSupO) LOOP
      Insert into dir_cm092_dbt (
                                 T_REPORTPARTYID,
                                 T_REPORTPARTY,
                                 T_INTERNALMESSAGEID, 
                                 T_VALUATIONDATE, 
                                 T_TRANSORREFID_CSAMOUNT,
                                 T_TRANSORREF_CSAMOUNT,
                                 T_TRANSEEREFID_CSAMOUNT,
                                 T_TRANSEEREF_CSAMOUNT,
                                 T_CSAMOUNT,
                                 T_CSAMOUNTCURRENCYID,
                                 T_CSAMOUNTCURRENCY,
                                 T_TRANSORREFID_CSBALANCE,
                                 T_TRANSORREF_CSBALANCE,
                                 T_TRANSEEREFID_CSBALANCE,
                                 T_TRANSEEREF_CSBALANCE,
                                 T_CSBALANCE,
                                 T_CSBALANCECURRENCYID,
                                 T_CSBALANCECURRENCY,
                                 T_TRANSORREFID_INDEPAMOUNT,
                                 T_TRANSORREF_INDEPAMOUNT,
                                 T_TRANSEEREFID_INDEPAMOUNT,
                                 T_TRANSEETEF_INDEPAMOUNT,
                                 T_INDEPAMOUNT,
                                 T_INDEPAMOUNTCURRENCYID,
                                 T_INDEPAMOUNTCURRENCY,
                                 T_TRANSORREFID_CSOBLIGATIONS,
                                 T_TRANSORREF_CSOBLIGATIONS,
                                 T_TRANSEEREFID_CSOBLIGATIONS,
                                 T_TRANSEEREF_CSOBLIGATIONS,
                                 T_CSOBLIGATIONS,
                                 T_CSOBLIGATIONSCURRENCYID,
                                 T_CSOBLIGATIONSCURRECNY)
           select  l_reportPartyId,
                   l_reportParty,
                   p_InterMesID, 
                   c.valuationDate, 
                   CASE WHEN upper(c.credSupA_transorReference)='PARTY1' THEN 1 
                        WHEN upper(c.credSupA_transorReference)='PARTY2' THEN 2 
                        ELSE 0 END,
                   c.credSupA_transorReference,
                   CASE WHEN upper(c.credSupA_transeeReference)='PARTY1' THEN 1 
                        WHEN upper(c.credSupA_transeeReference)='PARTY2' THEN 2 
                        ELSE 0 END,
                   c.credSupA_transeeReference,
                   to_number(c.credSupA_Amount, '999999999999999999999.99999999'),
                   c.credSupA_CurID,
                   c.credSupA_Currency,
                   CASE WHEN upper(c.credSupB_transorReference)='PARTY1' THEN 1 
                        WHEN upper(c.credSupB_transorReference)='PARTY2' THEN 2 
                        ELSE 0 END,
                   c.credSupB_transorReference,
                   CASE WHEN upper(c.credSupB_transeeReference)='PARTY1' THEN 1 
                        WHEN upper(c.credSupB_transeeReference)='PARTY2' THEN 2 
                        ELSE 0 END,
                   c.credSupB_transeeReference,
                   to_number(c.credSupB_Amount, '999999999999999999999.99999999'),
                   c.credSupB_CurID,
                   c.credSupB_Currency,
                   CASE WHEN upper(c.indepA_transorReference)='PARTY1' THEN 1 
                        WHEN upper(c.indepA_transorReference)='PARTY2' THEN 2 
                        ELSE 0 END,
                   c.indepA_transorReference,
                   CASE WHEN upper(c.indepA_transeeReference)='PARTY1' THEN 1 
                        WHEN upper(c.indepA_transeeReference)='PARTY2' THEN 2 
                        ELSE 0 END,
                   c.indepA_transeeReference,
                   to_number(c.indepA_Amount, '999999999999999999999.99999999'),
                   c.indepA_CurID,
                   c.indepA_Currency,
                   CASE WHEN upper(c.credSupO_transorReference)='PARTY1' THEN 1 
                        WHEN upper(c.credSupO_transorReference)='PARTY2' THEN 2 
                        ELSE 0 END,
                   c.credSupO_transorReference,
                   CASE WHEN upper(c.credSupO_transeeReference)='PARTY1' THEN 1 
                        WHEN upper(c.credSupO_transeeReference)='PARTY2' THEN 2 
                        ELSE 0 END,
                   c.credSupO_transeeReference,
                   to_number(c.credSupO_Amount, '999999999999999999999.99999999'),
                   c.credSupO_CurID,
                   c.credSupO_Currency
             from  dual;
      END LOOP;

     UpdateProductID(p_InterMesID);
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM092;

   FUNCTION InsertTableCM083 (p_InterMesID in integer, p_Text out varchar2, p_IDParty in integer, p_CounterpartyID in varchar2, p_CounterpartyNum in integer) return integer as
   begin
     INSERT INTO dir_cm083_dbt(t_internalmessageid, t_internalpartyid, t_partytype,
                               t_r_tradeid, t_u_tradeid, t_pid_tradeid, t_p_tradeId,
                               t_side, t_rate,
                               t_a_spot, t_c_spot,
                               t_a_forward, t_c_forward,
                               t_sign_bonds, t_id_bond, t_n_bond, t_c_bond,
                               t_sign_equity, t_id_equity, t_n_equity, t_p_equity, t_c_equity,
                               t_i_client, t_n_client, t_t_client, t_c_client,
                               t_clientdeal,
                               T_DEALID)
       SELECT p_InterMesID, p_IDParty, p_CounterpartyNum,
              x.tradeId_r, x.tradeId_u, x.tradeId_pid, x.tradeId_p,
              x.side, to_number(nvl(x.rate, 0), '999999999999999999999.99999999'),
              to_number(x.spotLeg_a, '999999999999999999999.99999999'), x.spotLeg_c,
              to_number(x.forwardLeg_a, '999999999999999999999.99999999'), x.forwardLeg_c,
              decode(x.bond_id, null, chr(0), 'X'), nvl(x.bond_id, chr(1)), to_number(nvl(x.bond_n, 0), '999999999999999999999.99999999'), nvl(x.bond_c, chr(1)),
              decode(x.equity_id, null, chr(0), 'X'), 
              nvl(x.equity_id, chr(1)), 
              to_number(nvl(x.equity_n, 0), '999999999999999999999.99999999'), 
              to_number(nvl(x.equity_p, 0), '999999999999999999999.99999999'), 
              nvl(x.equity_c, chr(1)),
              nvl(z.client_id, chr(1)), nvl(z.client_n, chr(1)), nvl(z.client_t, chr(0)), nvl(z.client_c, chr(1)),
              decode(z.client_id, null, chr(0), 'X'),
              nvl((select rd.t_docID from ddl_repozdeal_dbt rd where rd.T_UTI_CONTRACT = x.tradeId_u and rd.t_docKind = DL_SECURITYDOC), -1)
         FROM (select x.*
                 from DIR_SRS_TMP t,
                      XMLTABLE(xmlnamespaces(default 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' as "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' as "fpmlext"),
                               '//nsdext:repoBulkReport/*' PASSING xmltype(t.t_fmtclobdata_xxxx)
                               COLUMNS repos XMLTYPE PATH '/*',
                                       Counterparty  PATH '//nsdext:counterparty/@id') x
                where x.repos.getrootelement() like ('repos')
                  and lower(x.repos) like ('%counterparty%')
                  and x.Counterparty = p_CounterpartyID ) t,
              XMLTABLE(xmlnamespaces(default 'http://www.fpml.org/FpML-5/recordkeeping',
                                     'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' as "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '//nsdext:repoDetails' PASSING repos
                       COLUMNS tradeId_r    PATH 'nsdext:tradeId/@r',
                               tradeId_p    PATH 'nsdext:tradeId/@p',
                               tradeId_u    PATH 'nsdext:tradeId/@u',
                               tradeId_pid  PATH 'nsdext:tradeId/@pid',
                               side         PATH 'nsdext:side',
                               rate         PATH 'nsdext:rate',
                               spotLeg_a    PATH 'nsdext:spot/@a',
                               spotLeg_c    PATH 'nsdext:spot/@c',
                               forwardLeg_a PATH 'nsdext:forward/@a',
                               forwardLeg_c PATH 'nsdext:forward/@c',
                               equity_id    PATH 'nsdext:equity/@id',
                               equity_n     PATH 'nsdext:equity/@n',
                               equity_p     PATH 'nsdext:equity/@p',
                               equity_c     PATH 'nsdext:equity/@c',
                               bond_id      PATH 'nsdext:bond/@id',
                               bond_n       PATH 'nsdext:bond/@n',
                               bond_c       PATH 'nsdext:bond/@c',
                               clients XMLTYPE PATH 'nsdext:client') x FULL OUTER JOIN
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                             'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                             'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                       '/*' PASSING clients 
                       COLUMNS client_id    PATH '@i',
                               client_t     PATH '@t',
                               client_n     PATH '@n',
                               client_c     PATH '@c'
                              ) z on x.clients is null or x.clients is not null;     
     Return 0;
   exception
     WHEN OTHERS THEN
       DeleteSRS (p_InterMesID);
       p_Text := SQLERRM;
       Return 1;
   end InsertTableCM083;

   FUNCTION CheckReportReferences return integer as
   /*Проверка наличия элементов*/
     l_Count INTEGER;
   begin
     SELECT count(*)
       INTO l_Count
       FROM DIR_TYPESMESSAGE_DBT tm,
            DIR_SRS_TMP t,
            XMLTABLE (
               xmlnamespaces (
                  DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                          'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                          'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                    '/*/child::node()'
                    PASSING xmltype(t.t_fmtclobdata_xxxx)
                    COLUMNS product XMLTYPE PATH '/*') x
                    WHERE x.product.GetRootElement() IN('allTrades', 'masterAgreementID');
     Return l_Count;
   end CheckReportReferences;

   PROCEDURE UpdateProductID (p_InterMesID in integer) IS
   begin
     UPDATE dir_generalinf_dbt gen
        SET gen.t_productcode = (select x.productId
                                   from DIR_SRS_TMP t,
                                        XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                                                               'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                                                               'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                                                 '//productId'
                                                 PASSING xmltype(t.t_fmtclobdata_xxxx)
                                                 COLUMNS productId PATH '/*') x)
      WHERE gen.t_internalmessageid = p_InterMesID;
   end UpdateProductID;


   FUNCTION ProcInboundSRS (p_Protocol IN OUT CLOB, p_ID_OP IN integer)  return number AS
   cursor c1 is(
       SELECT msg.*
         FROM dir_generalinf_dbt msg
        WHERE msg.t_Rstatus in(RSTATUS_UPLOADED, RSTATUS_ER_PROCESSED));
   l_count    integer;
   l_TypeOld  varchar2(5);
   l_Update   varchar2(16000);
   l_UpdateACC varchar2(16000);
   l_Side     integer;
   l_id       integer;
   l_docID    integer;
   l_docKind  integer;
   err        integer;
   l_MesID    integer;
   l_c         SYS_REFCURSOR;
   l_IDGR     integer;
   l_count093 integer;
   l_ID093    integer;
   l_Kind093  integer;
   l_GR093    integer;
   c2         SYS_REFCURSOR;
   c83        SYS_REFCURSOR;
   c93        SYS_REFCURSOR;
   cTrades    SYS_REFCURSOR;
   c01_02     SYS_REFCURSOR;
   L_ID_TRADE           NUMBER(10);
   L_ID_CM093           NUMBER(10);
   L_TRADEIDENT         VARCHAR2(52);
   L_TRADEOBLSTATUS     VARCHAR2(2);
   l_x        integer;
   l_y        integer;
   p_ErrorText varchar2(1000);
   l_UTI083    VARCHAR2(52);
   l_ErrProc  integer;
   l_RdID     integer;
   l_DealID   integer;
   l_GrID     integer;
   l_Report   char(1);
   l_Amendment char(1);
   l_IsCorrection char(1);
   l_CorrectRegisteredInfo char(1);
   l_TRADEID083 VARCHAR2(50);
   TemplNum   integer;
   l_IDInstr  INTEGER := 0;
   l_IDEquity VARCHAR2(15) := ' ';
   l_IDBond   VARCHAR2(15) := ' ';
   l_NUM083   INTEGER := 0;
   l_ID83     INTEGER :=0;
   l_Side1    INTEGER :=0;  

  begin

   FOR SRS IN c1 LOOP
     err:= 0;
     p_ErrorText:= '';
     savepoint Svp;

     IF(SRS.t_messagetype IN ('RM001', 'RM002', 'RM003', 'RM006')) THEN
       l_ErrProc:= 0;
       begin
         select rownum, T_MESSAGETYPE, T_INTERNALMESSAGEID, T_AMENDMENT, T_ISCORRECTION, T_CorrectRegisteredInfo 
            into l_count, l_TypeOld, l_MesID, l_Amendment, l_IsCorrection, l_CorrectRegisteredInfo from dir_generalinf_dbt
            where t_messageid = SRS.T_INREPLYTO
            AND ROWNUM = 1; --если есть несколько исходящих с одним MessageID (чего не долно быть, это ошибка в базе!), то берем первое
         /*select rownum, T_MESSAGETYPE, T_INTERNALMESSAGEID, T_AMENDMENT, T_ISCORRECTION, T_TRADEID_PARTY1, T_ORIGINATINGID_PARTY1, 
          T_TRADEID_PARTY2, T_ORIGINATINGID_PARTY2 into 
            l_count, l_TypeOld ,l_MesID, l_Amendment, l_IsCorrection, l_TradeId_Party1, l_OrigID_Party1, l_TradeId_Party1, l_OrigID_Party1 from dir_generalinf_dbt */

         /*if( SRS.t_messagetype = 'RM006' and (SRS.T_TRADEID_PARTY1='' or SRS.T_TRADEID_PARTY1 is null) and 
          (SRS.T_ORIGINATINGID_PARTY1='' or SRS.T_ORIGINATINGID_PARTY1 is null) and
          (SRS.T_TRADEID_PARTY2='' or SRS.T_TRADEID_PARTY2 is null) and (SRS.T_ORIGINATINGID_PARTY2='' or SRS.T_ORIGINATINGID_PARTY2 is null) ) then
            update dir_generalinf_dbt
            set T_TRADEID_PARTY1 = l_TradeId_Party1,
                T_ORIGINATINGID_PARTY1 = l_OrigID_Party1, 
                T_TRADEID_PARTY2 = l_TradeId_Party2,
                T_ORIGINATINGID_PARTY2 = l_OrigID_Party2 
            where T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID;          
          end if; */

       EXCEPTION
       WHEN NO_DATA_FOUND THEN
         l_count:= 0;
         p_ErrorText:= 'Не найдено исходное сообщение, в ответ на которое отправлено входящее сообщение' ;
         l_ErrProc:=1;
 WHEN TOO_MANY_ROWS THEN /*DAN*/
 continue;
       end;
/*DAN  i-support 520583*/
      if (l_count > 0) and (SRS.t_messagetype IN ( 'RM003'))  then
        if length(SRS.T_TradeID_TRADEREPOSITORY) = 12  then
           update dir_generalinf_dbt t
             set t.T_TRADEREPOSITORY_REPORTID = SRS.T_TradeID_TRADEREPOSITORY
             where t.t_messageid = SRS.T_INREPLYTO;
         UPDATE ddl_repozdeal_dbt rd
            SET rd.t_contract_ir = SRS.T_TradeID_TRADEREPOSITORY
          WHERE rd.T_UTI_CONTRACT =
                   (SELECT gi.T_TRADEID_UTIGENERATINGPARTY
                      FROM dir_generalinf_dbt gi
                     WHERE gi.t_messageid = SRS.T_INREPLYTO AND ROWNUM = 1)
               AND rd.t_contract_ir = chr(1);
        end if;
      end if;
 /*DAN*/
       IF(l_count > 0) THEN
         IF(SRS.T_MESSAGETYPE = 'RM001') THEN
            IF(l_TypeOld = 'CM093') THEN

             Open c93 for select cm093.T_ID, cm093.T_TRADEIDENT, cm093.T_TRADEOBLSTATUS from dir_cm093_dbt cm093 where cm093.T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID ;
               LOOP
                 EXIT WHEN l_ErrProc > 0;

                 FETCH c93 INTO L_ID_CM093, L_TRADEIDENT, L_TRADEOBLSTATUS;
                 EXIT WHEN c93%NOTFOUND;

                 begin
                 select rd.t_DocID, rd.t_DocKind, GR.T_ID ,rd.T_REPORTING
                 into l_ID093, l_Kind093, l_GR093, l_Report
                           from ddl_repozdeal_dbt rd, dv_irdeal td , ddlgrdeal_dbt gr
                           WHERE  rd.T_CONTRACT_IR = L_TRADEIDENT
                            AND GR.T_DOCID = rd.t_DocID
                            AND GR.t_DocKind = rd.t_DocKind
                            and gr.t_templnum = (case WHEN L_TRADEOBLSTATUS = 'T'  THEN DLGR_TEMPL_CLOSECONTR
                                                      WHEN L_TRADEOBLSTATUS = 'C'  THEN DLGR_TEMPL_EXECDELAYMSG
                                                      WHEN L_TRADEOBLSTATUS = 'P'  THEN DLGR_TEMPL_EXECHOLDMSG
                                                      WHEN L_TRADEOBLSTATUS = 'TD' THEN DLGR_TEMPL_EARLYEXECMSG
                                                      WHEN L_TRADEOBLSTATUS = 'D'  THEN DLGR_TEMPL_REJECTIONMSG
                                                      WHEN L_TRADEOBLSTATUS = 'SO' THEN DLGR_TEMPL_NETTINGSUSP
                                                      END)
                            AND ROWNUM = 1;--на случай дубликата

                  EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                    err:= 1;
                    p_ErrorText:= 'Не найдены репозитарные параметры сделки';
                    l_ErrProc:=1;
                    rollback to Svp;

                 end;

                 if(err = 0)then

                   IF(l_Report<>'X') THEN
                     err:= 1;
                     l_ErrProc:=1;
                     p_ErrorText:= 'В исходной сделке указан признак отказа от репортинга';
                     rollback to Svp;

                   ELSE

                     UPDATE dir_cm093_dbt set
                     T_DEALID = l_ID093,
                     T_BOFFICEKIND =l_Kind093,
                     T_TEMPLNUM = l_GR093
                     Where T_ID = L_ID_CM093;

                     InsertDlGrDoc(p_ID_OP, SRS.T_INTERNALMESSAGEID, l_GR093);

                   end if;

                 end if;

               END LOOP;
              Close c93;

              begin
              
                Select ga.t_Side1 INTO l_Side1 from ddvndeal_dbt deal, ddl_genagr_dbt ga Where deal.T_ID = l_ID093 AND deal.T_DocKind = l_Kind093 And ga.t_genagrID = deal.t_genagrID;
              exception
              WHEN NO_DATA_FOUND THEN
                  l_Side1 := 0;  
 WHEN TOO_MANY_ROWS THEN  /*DAN*/
 continue;
              end;


              if(err = 0)then
                update dir_generalinf_dbt
                set T_TRADEREPOSITORY_REPORTID = SRS.T_TradeID_TRADEREPOSITORY,
                    T_STATUSCODE = 'REGISTERED',
                    T_REGISTRATIONDATE = SRS.T_OPERDATE,
                    T_COUNTERPARTY_REPORTID = DECODE(l_Side1, 1, SRS.T_TRADEID_PARTY1, 2, SRS.T_TRADEID_PARTY2, '')
                where t_messageid = SRS.T_INREPLYTO;
              end if;

            ELSIF(l_TypeOld = 'CM094') THEN
             Open cTrades for select trades.T_ID, trades.T_TRADEID from dir_trades_dbt trades where trades.T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID ;
               LOOP

                 EXIT WHEN l_ErrProc > 0;

                 FETCH cTrades INTO L_ID_TRADE, L_TRADEIDENT;
                 EXIT WHEN cTrades%NOTFOUND;

                 begin
                 select rd.t_DocID, rd.t_DocKind, GR.T_ID ,rd.T_REPORTING, rd.t_ID
                 into l_ID093, l_Kind093, l_GR093, l_Report, l_RdID
                           from ddl_repozdeal_dbt rd, dv_irdeal td , ddlgrdeal_dbt gr
                           WHERE  rd.T_UTI_CONTRACT = L_TRADEIDENT
                            AND GR.T_DOCID = rd.t_DocID
                            AND GR.t_DocKind = rd.t_DocKind
                            and gr.t_templnum = DLGR_TEMPL_FAIRVALUE
                            AND ROWNUM = 1;--на случай дубликата

                  EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                    err:= 1;
                    p_ErrorText:= 'Не найдены репозитарные параметры сделки';
                    l_ErrProc:=1;
                    rollback to Svp;

                 end;

                 if(err = 0)then

                   IF(l_Report<>'X') THEN
                     err:= 1;
                     l_ErrProc:=1;
                     p_ErrorText:= 'В исходной сделке указан признак отказа от репортинга';
                     rollback to Svp;

                   ELSE

                     UPDATE dir_trades_dbt set
                     T_DEALID = l_ID093,
                     T_BOFFICEKIND =l_Kind093,
                     T_TEMPLNUM = l_GR093
                     Where T_ID = L_ID_CM093;

                     InsertDlGrDoc(p_ID_OP, SRS.T_INTERNALMESSAGEID, l_GR093);

                   end if;

                 end if;

               END LOOP;
              Close cTrades;
              begin
              
                Select ga.t_Side1 INTO l_Side1 from ddvndeal_dbt deal, ddl_genagr_dbt ga Where deal.T_ID = l_ID093 AND deal.T_DocKind = l_Kind093 And ga.t_genagrID = deal.t_genagrID;
              exception
              WHEN NO_DATA_FOUND THEN
                  l_Side1 := 0;  
 WHEN TOO_MANY_ROWS THEN
 continue;
              end;                      

              if(err = 0)then
                update dir_generalinf_dbt
                set T_TRADEREPOSITORY_REPORTID = SRS.T_TradeID_TRADEREPOSITORY,
                    T_STATUSCODE = 'REGISTERED',
                    T_REGISTRATIONDATE = SRS.T_OPERDATE,
                    T_COUNTERPARTY_REPORTID = DECODE(l_Side1, 1, SRS.T_TRADEID_PARTY1, 2, SRS.T_TRADEID_PARTY2, '')
                where t_messageid = SRS.T_INREPLYTO;
              end if;

            ELSIF (l_TypeOld = 'CM092') THEN
   
              select T_DEALID, T_BOFFICEKIND into l_ID093, l_Kind093 from dir_cm092_dbt where T_INTERNALMESSAGEID = (select T_INTERNALMESSAGEID from dir_generalinf_dbt where t_messageid = SRS.T_INREPLYTO) and rownum = 1;
              
              begin
                Select ga.t_Side1 INTO l_Side1 from DDVCSA_DBT CSA, ddl_genagr_dbt ga Where CSA.T_CSAID = l_ID093 And rsb_secur.DV_CSA = l_Kind093 and ga.t_genagrID = CSA.t_genagrID;
              exception
              WHEN NO_DATA_FOUND THEN
                  l_Side1 := 0;
 WHEN TOO_MANY_ROWS THEN /*DAN*/
 continue;
              end;                                      

              if(err = 0)then
                update dir_generalinf_dbt
                set T_TRADEREPOSITORY_REPORTID = SRS.T_TradeID_TRADEREPOSITORY,
                    T_STATUSCODE = 'REGISTERED',
                    T_REGISTRATIONDATE = SRS.T_OPERDATE,
                    T_COUNTERPARTY_REPORTID = DECODE(l_Side1, 1, SRS.T_TRADEID_PARTY1, 2, SRS.T_TRADEID_PARTY2, '')
                where t_messageid = SRS.T_INREPLYTO;
              end if;

            ELSIF (l_TypeOld = 'CM083') THEN
              l_IDInstr   := 0;
              l_IDEquity  := ' ';
              l_IDBond    := ' ';
              l_NUM083    := 0;

              Open c83 for select cm083.T_U_TRADEID, cm083.T_R_TRADEID, cm083.T_ID_Bond, cm083.T_ID_EQUITY, cm083.T_ID from dir_cm083_dbt cm083 where cm083.T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID ;
                LOOP

                 FETCH c83 INTO l_UTI083,l_TRADEID083, l_IDBond, l_IDEquity, l_ID83;
                 EXIT WHEN c83%NOTFOUND or l_ErrProc > 0;

                 IF l_IDEquity = ' ' THEN
                    l_IDEquity := l_IDBond;
                 END IF;
                 l_GrID := 0;
                 begin
                 select rd.t_ID, rd.T_REPORTING, rd.T_DocID into l_RdID, l_Report, l_DealID
                           from ddl_repozdeal_dbt rd
                           WHERE  rd.T_UTI_CONTRACT = l_UTI083;
                   EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                   err:= 1;
                   l_ErrProc:=1;
                   p_ErrorText:= 'Не найдена исходная сделка, к которой относится входящее сообщение';
 WHEN TOO_MANY_ROWS THEN /*DAN*/
 continue;
                 end;

                 IF(err = 0) THEN
                    IF(l_Report<>'X') THEN
                      err:= 1;
                      l_ErrProc:=1;
                      p_ErrorText:= 'В исходной сделке указан признак отказа от репортинга';
                    ELSE

                      update ddl_repozdeal_dbt rd
                      set   rd.T_STATUSCONTRACTID = (SELECT SC.T_ID FROM DIR_STATUSCONTRACT_DBT sc Where sc.T_STATUSCONTRACT = 'REGISTERED'),
                            rd.T_STATUSCONTRACT = 'REGISTERED',
                            rd.T_CONTRACT_IR = NVL(SRS.T_TRADEID_TRADEREPOSITORY,'NONREF')||'/'||l_TRADEID083
                      Where T_ID = l_RdID;

                      l_IDInstr:= InsertInstrument083 (SRS.T_INTERNALMESSAGEID,l_IDEquity);
                      l_NUM083 := l_NUM083 + 1 ;

                      update dir_cm083_dbt
                      set T_INSTRUMENTSID = l_IDInstr,
                          T_PARTYTYPE = l_NUM083
                      Where t_ID = l_ID83;

                      begin
                         select gr.t_ID into l_GrID from ddlgrdeal_dbt gr
                         where gr.t_DocID = l_DealID
                           AND gr.t_DocKind = DL_SECURITYDOC
                           AND gr.t_TemplNum = DLGR_TEMPL_BULKREPORT;
                        exception
                        WHEN NO_DATA_FOUND THEN
                          err:=1;
                          l_ErrProc:= 1;
                          p_ErrorText:= 'Не найдена строка графика для сделки по сводному отчету';
WHEN TOO_MANY_ROWS THEN /*DAN*/
 continue;
                      end;

                      if(l_ErrProc = 0) then
                        InsertDlGrDoc(p_ID_OP, SRS.T_INTERNALMESSAGEID, l_GrID);
                      end if;
                    END IF;
                   END IF;

               END LOOP;
              Close c83;

              IF(l_ErrProc= 0) THEN
                update dir_generalinf_dbt
                set T_TRADEREPOSITORY_REPORTID = SRS.T_TradeID_TRADEREPOSITORY,
                    T_STATUSCODE = 'REGISTERED',
                    T_REGISTRATIONDATE = SRS.T_OPERDATE
                where t_messageid = SRS.T_INREPLYTO;
              ELSE
                rollback to Svp;
              END IF;

            ELSIF (l_TypeOld = 'CM010') THEN

              begin
                SELECT ga.T_GenagrID, ga.T_Side1, ga.T_Reporting INTO l_id, l_Side1, l_Report FROM ddl_genagr_dbt ga
                  WHERE ga.t_UTI_Code = SRS.T_TRADEID_UTIGENERATINGPARTY 
                    AND (ga.T_Code=SRS.T_TradeId_Party1 OR ga.T_Code=SRS.T_TradeId_Party2) 
                  AND rownum = 1;
              EXCEPTION
              WHEN NO_DATA_FOUND THEN
                err:=1;
                l_ErrProc:= 1;
                p_ErrorText:= 'Не найдено исходное ГС, к которому относится входящее сообщение';
 WHEN TOO_MANY_ROWS THEN /*DAN*/
 continue;
              END;

              IF(l_ErrProc = 0) THEN

                IF(l_Report<>'X') THEN
                  err:= 1;
                  l_ErrProc:=1;
                  p_ErrorText:= 'В исходной сделке указан признак отказа от репортинга';
                  rollback to Svp;

                ELSIF(l_IsCorrection <> 'X') THEN     -- все, кроме "заменяющее                
                  update dir_generalinf_dbt set T_STATUSCODE = 'REGISTERED', T_REGISTRATIONDATE = SRS.T_OPERDATE
                    where t_messageid = SRS.T_INREPLYTO;

                  IF(l_Amendment <> 'X'AND l_CorrectRegisteredInfo <> 'X') THEN   -- исходное сообщение - первичное 
                    select data_length into l_count from user_tab_columns where table_name='DDL_GENAGR_DBT' and column_name='T_NUMREPOZ';
                    l_Update := 'UPDATE ddl_genagr_dbt ga SET ga.T_STATE_GS=(SELECT sc.T_ID FROM dir_statuscontract_dbt sc WHERE sc.T_STATUSCONTRACT = ''REGISTERED''),
                        ga.T_DateRepoz = TO_DATE('''||TO_CHAR(SRS.T_OPERDATE, 'DD.MM.YYYY')||''', ''DD.MM.YYYY''), ga.T_NumContr = ''';
                    IF(l_Side1 = 2) THEN  --контрагент
                      l_Update:= l_Update || SRS.T_TRADEID_PARTY1;
                    ELSE
                      l_Update:= l_Update || SRS.T_TRADEID_PARTY2;
                    END IF;              
                    l_Update:= l_Update || ''' ,  ga.T_NumRepoz = substr('''||SRS.T_TRADEID_TRADEREPOSITORY||
                      ''', 1, ' || l_count || ') WHERE ga.t_GenagrID = ' || l_id ;   
                    --dbms_output.put_line(l_Update);
                    EXECUTE IMMEDIATE l_Update;

                    update ddl_repozdeal_dbt rd
                    set   rd.T_STATUSCONTRACTID = (SELECT SC.T_ID FROM DIR_STATUSCONTRACT_DBT sc Where sc.T_STATUSCONTRACT = 'REGISTERED'),
                          rd.T_STATUSCONTRACT = 'REGISTERED',
                          rd.T_CONTRACT_IR = NVL(SRS.T_TRADEID_TRADEREPOSITORY,'NONREF'), 
                          rd.T_CONTRACT_CONTR = DECODE(l_Side1, 2, SRS.T_TRADEID_PARTY1, SRS.T_TRADEID_PARTY2) --2=контрагент
                    Where T_DocID = l_id and t_DocKind = 4812; -- ГС
                  END IF;              
                END IF;              

              ELSE
                rollback to Svp;
              END IF;

            ELSIF (l_TypeOld IN ('CM041','CM021','CM022','CM023','CM032','CM042','CM043','CM044','CM045','CM046','CM047','CM048','CM051', 'CM053') ) THEN

              IF(SRS.T_AMENDMENT='X') THEN
                TemplNum:= DLGR_TEMPL_CHANGEMSG;
              ELSE
                TemplNum:= DLGR_TEMPL_MAKEDEAL;
              END IF;

              update dir_generalinf_dbt
              set-- T_TRADEREPOSITORY_REPORTID = T_PARTYREFID_TRADEREPOSITORY,
                    T_TRADEREPOSITORY_REPORTID = SRS.T_TRADEID_TRADEREPOSITORY, /*DAN */
                  T_STATUSCODE = 'REGISTERED',
              --    T_RSTATUS = RSTATUS_PROCESSED,
                  T_REGISTRATIONDATE = SRS.T_OPERDATE
              where t_messageid = SRS.T_INREPLYTO;

              -- определяем(обновляем) исходную сделку
              begin
                SELECT rd.t_ID, rd.t_DocID,rd.t_DocKind, GENAGR.T_SIDE1, gr.T_ID
                INTO l_id, l_docID, l_docKind, l_side,l_GrID
                FROM ddl_repozdeal_dbt rd, ddl_genagr_dbt GenAgr, dv_irdeal td, ddlgrdeal_dbt gr
                WHERE     rd.t_DocKind = td.t_DocKind
                      AND rd.t_DocID = td.t_DocID
                      AND GenAgr.t_GenAgrID(+) = td.t_GenAgrID
                      AND rd.T_UTI_CONTRACT = SRS.T_TRADEID_UTIGENERATINGPARTY
                      AND gr.t_docID = rd.t_DocID
                      AND gr.t_DocKind = rd.t_DocKind
                      and gr.t_templNum = TemplNum
                      AND rownum =1;
              EXCEPTION
              WHEN NO_DATA_FOUND THEN
                err:=1;
                l_ErrProc:= 1;

                p_ErrorText:= 'Не найдена исходная сделка, к которой относится входящее сообщение';
              end;

              IF(l_ErrProc=0) THEN
                l_Update := 'update ddl_repozdeal_dbt rd
                 set rd.T_STATUSCONTRACTID = (SELECT SC.T_ID FROM DIR_STATUSCONTRACT_DBT sc Where sc.T_STATUSCONTRACT = ''REGISTERED''),
                     rd.T_STATUSCONTRACT = ''REGISTERED'',
                     rd.T_CONTRACT_CONTR = ''';
                IF(l_side = 2) THEN  --контрагент
                  l_Update:= l_Update|| SRS.T_TRADEID_PARTY1;
                ELSE
                  l_Update:= l_Update|| SRS.T_TRADEID_PARTY2;
                END IF;

                l_Update:= l_Update|| ''' ,  rd.T_CONTRACT_IR = '''||  SRS.T_TRADEID_TRADEREPOSITORY
                               ||''' Where rd.t_ID ='|| l_id ;
                EXECUTE IMMEDIATE l_Update;
                  InsertDlGrDoc(p_ID_OP, SRS.T_INTERNALMESSAGEID, l_GrID);
                  /*
                INSERT INTO ddlgrdoc_dbt (t_ID, t_GRDEALID, T_DOCKIND, T_DOCID, T_SERVDOCKIND, T_SERVDOCID, T_GRPID, T_SOURCETYPE)
                     VALUES (0,l_GrID,REPOS_GENERALINF,SRS.T_INTERNALMESSAGEID,REPOS_INBOUNDMSG,0,0,0);
                    */

              END IF;
            END IF;
         ELSIF(SRS.T_MESSAGETYPE IN ('RM002', 'RM003', 'RM006')) THEN
           IF(SRS.T_MESSAGETYPE = 'RM002') THEN

             update dir_generalinf_dbt
             set T_STATUSCODE = 'RJCT',
                 T_REASONCODE = SRS.T_REASONCODE,
                 T_REASONDESCRIPTION = SRS.T_REASONDESCRIPTION,
                 T_REJECTDATE = SRS.T_CREATIONDATE
             where t_messageid = SRS.T_INREPLYTO;
             IF (l_Amendment<>'X'AND l_IsCorrection <> 'X') THEN

                IF( l_TypeOld IN ('CM041','CM021','CM022','CM023','CM032','CM042','CM043','CM044','CM045','CM046','CM047','CM048','CM051','CM053','CM083') )THEN
                  update ddl_repozdeal_dbt rd
                    set rd.T_STATUSCONTRACTID = (SELECT SC.T_ID FROM DIR_STATUSCONTRACT_DBT sc Where sc.T_STATUSCONTRACT = 'RJCT'),
                        rd.T_STATUSCONTRACT = 'RJCT'
                    where rd.t_ID in(   select rd1.t_id
                        from ddlgrdoc_dbt doc, ddlgrdeal_dbt gr, ddl_repozdeal_dbt rd1
                        Where  doc.t_docID =  l_MesID
                        AND doc.t_docKind = REPOS_GENERALINF
                        and doc.t_grdealID = gr.t_ID
                        and rd1.t_DocID = gr.t_DocID
                        and rd1.t_DocKind = gr.t_DocKind );
                ELSIF(l_TypeOld = 'CM010') THEN       --обновить genagr.t_STATE_GS
                  update ddl_genagr_dbt ga
                    set ga.t_STATE_GS = (SELECT SC.T_ID FROM DIR_STATUSCONTRACT_DBT sc Where sc.T_STATUSCONTRACT = 'RJCT')
                    WHERE ga.t_UTI_Code = (SELECT T_TRADEID_UTIGENERATINGPARTY FROM dir_generalinf_dbt WHERE T_INTERNALMESSAGEID = l_MesID);

                END IF;

              END IF;

           ELSE
             IF(SRS.T_STATUSCODE = 'INCOMING_DOC') THEN
               update dir_generalinf_dbt
               set T_STATUSCODE = SRS.T_STATUSCODE,
                   T_JOURNALDATE = SRS.T_CREATIONDATE,/*DAN*/
                   T_TRADEREPOSITORY_REPORTID = SRS.T_TRADEID_TRADEREPOSITORY
               where t_messageid = SRS.T_INREPLYTO and T_STATUSCODE != 'REGISTERED'/*GAA:Скорее всегопо 518393,DAN*/;

             ELSE
               update dir_generalinf_dbt
               set T_STATUSCODE = SRS.T_STATUSCODE
               where t_messageid = SRS.T_INREPLYTO;

             END IF;

             IF (l_Amendment<>'X'AND l_IsCorrection <> 'X') THEN

                IF( l_TypeOld IN ('CM041','CM021','CM022','CM023','CM032','CM042','CM043','CM044','CM045','CM046','CM047','CM048','CM051','CM053','CM083') )THEN

                update ddl_repozdeal_dbt rd
                    set rd.T_STATUSCONTRACTID = (SELECT SC.T_ID FROM DIR_STATUSCONTRACT_DBT sc Where sc.T_STATUSCONTRACT = SRS.T_STATUSCODE),
                        rd.T_STATUSCONTRACT = SRS.T_STATUSCODE
                    where rd.t_ID in(   select rd1.t_id
                        from ddlgrdoc_dbt doc, ddlgrdeal_dbt gr, ddl_repozdeal_dbt rd1
                        Where  doc.t_docID =  l_MesID
                        AND doc.t_docKind = REPOS_GENERALINF
                        and doc.t_grdealID = gr.t_ID
                        and rd1.t_DocID = gr.t_DocID
                        and rd1.t_DocKind = gr.t_DocKind );
                ELSIF(l_TypeOld = 'CM010') THEN       --обновить genagr.t_STATE_GS
                  update ddl_genagr_dbt ga
                    set ga.t_STATE_GS = (SELECT SC.T_ID FROM DIR_STATUSCONTRACT_DBT sc Where sc.T_STATUSCONTRACT = SRS.T_STATUSCODE)
                    WHERE ga.t_UTI_Code = (SELECT T_TRADEID_UTIGENERATINGPARTY FROM dir_generalinf_dbt WHERE T_INTERNALMESSAGEID = l_MesID);

                END IF;

             END IF;

           END IF;

           update dir_generalinf_dbt
              set T_RSTATUS = RSTATUS_PROCESSED,
                  T_RSTATUSNAME = ( SELECT NA.T_SZNAMEALG
                                      FROM DNAMEALG_DBT NA
                                     WHERE NA.T_ITYPEALG = ALG_GENINF_RSTATUS
                                       AND NA.T_INUMBERALG = RSTATUS_PROCESSED ),
                  T_PRDATE = trunc(SYSDATE),
                  T_PRTIME = TO_DATE('01.01.0001'||TO_CHAR(SYSDATE, 'HH24:MI:SS'),'DD.MM.YYYY HH24:MI:SS') 
            where T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID;

              ------
             Open c01_02 for select gr.t_id,rd1.T_REPORTING
                     from ddlgrdoc_dbt doc, ddlgrdeal_dbt gr, ddl_repozdeal_dbt rd1
                     Where  doc.t_docID =  l_MesID
                     AND doc.t_docKind = REPOS_GENERALINF
                     and doc.t_grdealID = gr.t_ID
                     and rd1.t_DocID = gr.t_DocID
                     and rd1.t_DocKind = gr.t_DocKind ;
               LOOP

                 FETCH c01_02 INTO l_GrID,l_Report;
                 EXIT WHEN c01_02%NOTFOUND or l_ErrProc > 0;
                    IF(l_Report<>'X') THEN
                      err:= 1;
                      l_ErrProc:=1;
                      p_ErrorText:= 'В исходной сделке указан признак отказа от репортинга';
                    ELSE
                       InsertDlGrDoc(p_ID_OP, SRS.T_INTERNALMESSAGEID, l_GrID);
                    END IF;

               END LOOP;

             Close c01_02;

             if(not(l_ErrProc=0))then
                rollback to Svp;
             end if;
              ------

         END IF;
          /*
         IF(l_ErrProc != 0 and (l_TypeOld = 'CM094' OR l_TypeOld = 'CM093')) THEN
            update dir_generalinf_dbt
               set T_RSTATUS = RSTATUS_ER_PROCESSED,
                  T_RSTATUSNAME = ( SELECT NA.T_SZNAMEALG
                                      FROM DNAMEALG_DBT NA
                                     WHERE NA.T_ITYPEALG = ALG_GENINF_RSTATUS
                                       AND NA.T_INUMBERALG = RSTATUS_ER_PROCESSED ),
                  T_PRDATE = trunc(SYSDATE),
                  T_PRTIME = TO_DATE('01.01.0001'||TO_CHAR(SYSDATE, 'HH24:MI:SS'),'DD.MM.YYYY HH24:MI:SS')
            where T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID;
           ELSE
            update dir_generalinf_dbt
               set T_RSTATUS = RSTATUS_PROCESSED,
                   T_RSTATUSNAME = ( SELECT NA.T_SZNAMEALG
                                       FROM DNAMEALG_DBT NA
                                      WHERE NA.T_ITYPEALG = ALG_GENINF_RSTATUS
                                        AND NA.T_INUMBERALG = RSTATUS_PROCESSED ),
                   T_PRDATE = trunc(SYSDATE),
                   T_PRTIME = TO_DATE('01.01.0001'||TO_CHAR(SYSDATE, 'HH24:MI:SS'),'DD.MM.YYYY HH24:MI:SS')

            where T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID;
           END IF;
              */
         IF(l_ErrProc != 0) THEN
          l_ErrProc := RSTATUS_ER_PROCESSED;
         ELSE
          l_ErrProc := RSTATUS_PROCESSED;
         END IF;
         update dir_generalinf_dbt
            set T_RSTATUS = l_ErrProc,
                T_RSTATUSNAME = ( SELECT NA.T_SZNAMEALG
                                    FROM DNAMEALG_DBT NA
                                  WHERE NA.T_ITYPEALG = ALG_GENINF_RSTATUS
                                    AND NA.T_INUMBERALG = l_ErrProc ),
                T_PRDATE = trunc(SYSDATE),
                T_PRTIME = TO_DATE('01.01.0001'||TO_CHAR(SYSDATE, 'HH24:MI:SS'),'DD.MM.YYYY HH24:MI:SS')
         where T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID;

       ELSIF (l_count = 0) THEN
         update dir_generalinf_dbt
            set T_RSTATUS = RSTATUS_ER_PROCESSED,
                T_RSTATUSNAME = ( SELECT NA.T_SZNAMEALG
                                    FROM DNAMEALG_DBT NA
                                   WHERE NA.T_ITYPEALG = ALG_GENINF_RSTATUS
                                     AND NA.T_INUMBERALG = RSTATUS_ER_PROCESSED )
          where T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID;
       END IF;

     ELSIF(SRS.t_messagetype = 'RM005') THEN
        IF(SRS.T_XMLPRODUCT = 'executionStatus') THEN
          l_x:= 0; -- всего сделок
          l_y:= 0; -- кол-во сделок, которые есть репоз. парам.

          begin
             SELECT COUNT (1)
             INTO l_x
             FROM dir_cm093_dbt cm093
             WHERE cm093.T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID  ;
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
            err:= 1;

          end;

          begin
             SELECT COUNT (1)
             INTO l_y
             FROM dir_cm093_dbt cm093, ddl_repozdeal_dbt rd
             WHERE cm093.T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID
             AND cm093.T_TRADEIDENT = rd.T_CONTRACT_IR
             AND rd.t_reporting = chr(88);
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
            err:= 1;

          end;
          l_count093:= l_x - l_y;
          IF(l_count093 <>0) THEN
            err:=1;
            p_ErrorText:= 'В исходной сделке указан признак отказа от репортинга';

          END IF;
          --если l_count093 <>0 , тогда вставка ошибки
          IF (err = 0 ) THEN
          Open c2 for select cm093.T_ID, cm093.T_TRADEIDENT, cm093.T_TRADEOBLSTATUS from dir_cm093_dbt cm093 where cm093.T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID ;
          LOOP
            EXIT WHEN c2%NOTFOUND or l_ErrProc > 0; /*Karlov*/
            FETCH c2 INTO L_ID_CM093, L_TRADEIDENT, L_TRADEOBLSTATUS;
--            EXIT WHEN c2%NOTFOUND or l_ErrProc > 0;

            begin
              select rd.t_DocID, rd.t_DocKind, GR.T_ID into l_ID093, l_Kind093, l_GR093
                           from ddl_repozdeal_dbt rd, dv_irdeal td , ddlgrdeal_dbt gr
                           WHERE     rd.t_DocKind = td.t_DocKind
                            AND rd.t_DocID = td.t_DocID
                            AND rd.T_CONTRACT_IR = L_TRADEIDENT
                            AND GR.T_DOCID = rd.t_DocID
                            AND GR.t_DocKind = rd.t_DocKind
                            and gr.t_templnum = (case WHEN L_TRADEOBLSTATUS = 'T' THEN DLGR_TEMPL_CLOSECONTRREQUEST
                                                      WHEN L_TRADEOBLSTATUS = 'C' THEN DLGR_TEMPL_EXECDELAYMSGREQUEST
                                                      WHEN L_TRADEOBLSTATUS = 'P' THEN DLGR_TEMPL_EXECHOLDMSGREQUEST
                                                      WHEN L_TRADEOBLSTATUS = 'TD' THEN DLGR_TEMPL_EARLYEXECMSGREQUEST
                                                      WHEN L_TRADEOBLSTATUS = 'D' THEN DLGR_TEMPL_REJECTIONMSGREQUEST
                                                      WHEN L_TRADEOBLSTATUS = 'SO' THEN DLGR_TEMPL_NETTINGSUSPREQUEST
                                                      END);
                EXCEPTION
               WHEN NO_DATA_FOUND THEN
                err:= 1;
                p_ErrorText:= 'Не найдены репозитарные параметры сделки';
                l_ErrProc:=1;
                rollback to Svp;


            end;

            if(err = 0) THEN
               UPDATE dir_cm093_dbt set
               T_DEALID = l_ID093,
               T_BOFFICEKIND =l_Kind093,
               T_TEMPLNUM = l_GR093
               Where T_ID = L_ID_CM093;
                  InsertDlGrDoc(p_ID_OP, SRS.T_INTERNALMESSAGEID, l_GR093);
                /*
               INSERT INTO ddlgrdoc_dbt (t_ID, t_GRDEALID, T_DOCKIND, T_DOCID, T_SERVDOCKIND, T_SERVDOCID, T_GRPID, T_SOURCETYPE)
               VALUES (0,l_GrID,REPOS_GENERALINF,SRS.T_INTERNALMESSAGEID,REPOS_INBOUNDMSG,0,0,0);
                */

            end if;

          END LOOP;
          Close c2;
          l_UpdateACC:= ' Update ddlgracc_dbt set t_state = ' || DLGRACC_STATE_NOTNEED ||
                       ' Where T_GRDEALID  IN
                       (select cm093.T_TEMPLNUM
                        from DIR_CM093_DBT cm093
                        Where cm093.T_INTERNALMESSAGEID = :ID_SRS ) ' ;
           EXECUTE IMMEDIATE l_UpdateACC USING SRS.T_INTERNALMESSAGEID ;

            OPEN l_c FOR
                      select cm093.T_DEALID, cm093.T_BOFFICEKIND from DIR_CM093_DBT cm093
                       Where cm093.T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID;

            LOOP
            FETCH l_c INTO l_docID, l_docKind;
            EXIT WHEN l_c%NOTFOUND;
               l_IDGR :=0;

               begin /*DAN*/
              Insert into ddlgrdeal_dbt (T_DOCID, T_DOCKIND, T_TEMPLNUM, T_PLANDATE, T_FIID )
                        values (l_docID, l_docKind, DLGR_TEMPL_RECONCILIATION, SRS.T_PRDATE , -1)
                        returning T_ID into l_IDGR;
               exception when others then /*DAN*/
                l_IDGR :=0;
               end;

              IF(l_IDGR <> 0) THEN
              Insert into ddlgrdoc_dbt (T_GRDEALID, T_DOCID, T_DOCKIND)
                       values (l_IDGR, SRS.T_INTERNALMESSAGEID, REPOS_RECONCILIATION);
              ELSE
                err:= 1;
                p_ErrorText:= 'Не получилось вставить строку графика';

              END IF;
            END LOOP;
          CLOSE l_c;

          END IF;
        ELSE          
          IF(SRS.t_XMLProduct = 'masterAgreementTerms') THEN   -- сообщение по ГС
            IF(SRS.T_AMENDMENT='X') THEN
                TemplNum:= DLGR_TEMPL_CHANGEMSGWTHREQUEST;    -- 49 Сообщение по изменению условий с ожиданием запроса
            ELSE
                TemplNum:= DLGR_TEMPL_MAKECONTRACTWTHREQUEST;  -- 64 Заключение договора с ож. запроса
            END IF;
            begin
              SELECT  rownum, rd.t_ID, rd.t_DocID, rd.t_DocKind, gr.t_ID, rd.T_REPORTING, ga.T_Side1
              INTO  l_count, l_id, l_docID, l_docKind, l_GrID, l_Report, l_Side1
              FROM  ddl_repozdeal_dbt rd, ddlgrdeal_dbt gr, ddl_genagr_dbt ga
              WHERE   rd.T_UTI_CONTRACT = SRS.T_TRADEID_UTIGENERATINGPARTY    
                  AND gr.t_docID = rd.t_DocID
                  AND gr.t_DocKind = rd.t_DocKind
                  AND gr.t_templNum = TemplNum
                  AND ga.T_GenagrID = rd.t_DocID  
                  ;                
              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                  l_count:= 0;     
                WHEN TOO_MANY_ROWS THEN
                  l_count:= 2;                        
            end;           
            /* Если ГС с таким кодом UTI не найдено, выполняется поиск ГС с совпадающими сторонами  */          
            IF(l_count = 0) THEN
              err := 1;   -- в автоматическом режиме эту сделку все равно не привязываем
              begin
                SELECT  rownum, rd.t_ID, rd.t_DocID, rd.t_DocKind, gr.t_ID, ga.T_REPORTING, ga.T_Side1
                INTO  l_count, l_id, l_docID, l_docKind, l_GrID, l_Report, l_Side1
                FROM  ddl_repozdeal_dbt rd, ddlgrdeal_dbt gr, ddl_genagr_dbt ga
                WHERE ga.t_Date_Genagr = SRS.T_TradeDate                           -- дата заключения ГС = дата заключения из СРС
                  AND ga.t_PartyID = (SELECT t_PartyID FROM dir_party_dbt     -- выбираем ГС, в которых контрагент из СРС = сторона1 или сторона2, в зависимости от ГС 
                                    WHERE t_InternalMessageID = SRS.t_InternalMessageID AND t_PartyKind = DECODE(ga.t_Initiator, 1, 2, 3))
                  AND gr.t_docID = rd.t_DocID
                  AND gr.t_DocKind = rd.t_DocKind
                  AND gr.t_templNum = TemplNum
                  AND ga.t_GenagrID = rd.t_DocID
                  AND ga.t_DocKind = rd.t_DocKind
                  ;
                EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                    l_count:= 0;        
                  WHEN TOO_MANY_ROWS THEN
                    l_count:= 2;                        
              end;
            END IF;

            IF(l_count = 0) THEN
                err:= 1;
                p_ErrorText:= 'Не найдено исходное ГС, к которому относится входящее сообщение';
            ELSIF (l_count = 2) THEN
                err:= 1;
                p_ErrorText:= 'Найдено несколько подходящих ГС. Невозможно выбрать ГС, к которому относится входящее сообщение, в автоматическом режиме';
            ELSE
                IF( err = 1) THEN
                    p_ErrorText:= 'Найдено ГС, к которой предположительно относится входящее сообщение, по совпадению параметров ГС и сообщения';--. Необходимо подтвердить выбор ГС в ручном режиме';
                    err := 0; /* т.к. нет привязки ГС вручную, то сбросываем ошибку и привяжем ГС автоматом */
                END IF;
                --ELSE  -- 
                SELECT data_length INTO l_count FROM user_tab_columns WHERE table_name='DDL_GENAGR_DBT' AND column_name='T_NUMREPOZ';
                UPDATE ddl_genagr_dbt ga 
                  SET ga.T_UTI_CODE = SRS.T_TRADEID_UTIGENERATINGPARTY,
                      ga.T_NumContr = DECODE(l_Side1, 1, SRS.T_TRADEID_PARTY1, SRS.T_TRADEID_PARTY2),  --2=контрагент
                      ga.T_NumRepoz = substr(SRS.T_TRADEID_TRADEREPOSITORY, 1, l_count)
                WHERE ga.t_GenagrID = l_docID;   

                UPDATE ddl_repozdeal_dbt rd
                SET   rd.T_UTI_CONTRACT = SRS.T_TRADEID_UTIGENERATINGPARTY,
                      rd.T_CONTRACT_IR = SRS.T_TRADEID_TRADEREPOSITORY, 
                      rd.T_CONTRACT_CONTR = DECODE(l_Side1, 1, SRS.T_TRADEID_PARTY1, SRS.T_TRADEID_PARTY2) --2=контрагент
                WHERE T_ID = l_id;
                --END IF;
            END IF;

          ELSE    -- сообщение по сделке
            IF(SRS.T_AMENDMENT='X') THEN
                TemplNum:= DLGR_TEMPL_CHANGEMSGWTHREQUEST;  -- 49 Сообщение по изменению условий с ожиданием запроса
            ELSE
                TemplNum:= DLGR_TEMPL_MAKEDEALWTHREQUEST;   -- 48 Заключение сделки с ожиданием запроса
            END IF;

            begin
              SELECT  rownum, rd.t_ID, rd.t_DocID, rd.t_DocKind, gr.t_ID, rd.T_REPORTING
              INTO  l_count, l_id, l_docID, l_docKind, l_GrID, l_Report
              FROM ddl_repozdeal_dbt rd, dv_irdeal td, ddlgrdeal_dbt gr
              WHERE   rd.t_DocKind = td.t_DocKind
                  AND rd.t_DocID = td.t_DocID
                  AND rd.T_UTI_CONTRACT = SRS.T_TRADEID_UTIGENERATINGPARTY    
                  AND gr.t_docID = rd.t_DocID
                  AND gr.t_DocKind = rd.t_DocKind
                  AND gr.t_templNum = TemplNum
                  ;
              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                  l_count:= 0;     
                WHEN TOO_MANY_ROWS THEN
                  l_count:= 2;                        
            end;           
            /* Если сделка с таким кодом UTI не найдена, выполняется поиск сделки с совпадающими сторонами  */          
            IF(l_count = 0) THEN
              err := 1;   -- в автоматическом режиме эту сделку все равно не привязываем
              begin
                select rownum, repoz.t_ID, repoz.t_DocID, repoz.t_DocKind, gr.t_ID, repoz.T_REPORTING
                INTO  l_count, l_id, l_docID, l_docKind, l_GrID, l_Report
                  from DDL_REPOZDEAL_DBT repoz, ddlgrdeal_dbt gr 
                  where repoz.T_CODECLASSPFI = SRS.T_PRODUCTCODE
                  and repoz.T_TYPEPRODUCT = SRS.T_PRODUCTTYPEID
                  and repoz.T_CREATOR_UTI = 2          -- контрагент
                  and repoz.T_DATE_START = SRS.T_STARTAGREEMENTDATE
                  AND repoz.T_DATE_END = SRS.T_ENDAGREEMENTDATE
                  AND gr.t_docID = repoz.t_DocID
                  AND gr.t_DocKind = repoz.t_DocKind
                  AND gr.t_templNum = TemplNum        -- 48 или 49 Заключение сделки (по изменению условий) с ожиданием запроса
                  ;
                EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                    l_count:= 0;        
                  WHEN TOO_MANY_ROWS THEN
                    l_count:= 2;                        
              end;
            END IF;

            IF(l_count = 0) THEN
                err:= 1;
                p_ErrorText:= 'Не найдена исходная сделка, к которой относится входящее сообщение';
            ELSE
                IF (l_count = 2) THEN
                  p_ErrorText:= 'Найдено несколько сделок, к которой предположительно относится входящее сообщение, по совпадению параметров сделки и сообщения. Необходимо выбрать сделку в списке для ручной сверки сообщений RM005';
                  err:= 1;
                ELSIF (l_count = 1 AND err = 1) THEN -- если было совпадение по UTI, то err == 0 
                  p_ErrorText:= 'Найдена сделка, к которой предположительно относится входящее сообщение, по совпадению параметров сделки и сообщения. Необходимо подтвердить выбор сделки в списке для ручной сверки сообщений RM005';
                END IF;
                IF(err = 0) THEN
                    Update DDL_REPOZDEAL_DBT repoz 
                    set repoz.T_UTI_CONTRACT=SRS.T_TRADEID_UTIGENERATINGPARTY,
                        repoz.T_CONTRACT_IR = SRS.T_TRADEID_TRADEREPOSITORY,
                        repoz.T_CONTRACT_CONTR = SRS.T_TRADEID_PARTY1
                    where repoz.t_ID=l_id;    
                END IF;
            END IF;
          END IF;   -- /сообщение по сделке

          IF(err = 0) THEN
              IF(l_Report <> 'X') THEN
                  err:= 1;
                  l_ErrProc:=1;
                  p_ErrorText:= 'В исходной сделке указан признак отказа от репортинга';
              ELSE
                  l_UpdateACC:= ' Update ddlgracc_dbt set t_state = ' || DLGRACC_STATE_NOTNEED ||
                  ' Where T_ID  IN
                  (select gracc.t_ID
                    from ddlgracc_dbt gracc, ddlgrdeal_dbt gr
                    Where gracc.t_grdealId = gr.t_id
                      and gr.t_docid =  :l_docID
                      and gr.t_DocKind =  :l_docKind
                      and gr.t_templnum in ( '
                      || TemplNum ||' )  and gracc.t_accnum = ' || DLGR_ACCKIND_REPOSITORY ||
                          ' and gracc.t_state  = ' || DLGRACC_STATE_PLAN ||' )';
                  EXECUTE IMMEDIATE l_UpdateACC USING l_docID, l_docKind ;

                  l_IDGR :=0;
                  Insert into ddlgrdeal_dbt (T_DOCID, T_DOCKIND, T_TEMPLNUM, T_PLANDATE, T_FIID )
                        values (l_docID, l_docKind, DLGR_TEMPL_RECONCILIATION, SRS.T_PRDATE , -1)
                        returning T_ID into l_IDGR;

                  IF(l_IDGR <> 0) THEN
                      Insert into ddlgrdoc_dbt (T_GRDEALID, T_DOCID, T_DOCKIND)
                          values (l_IDGR, SRS.T_INTERNALMESSAGEID, REPOS_RECONCILIATION);
                      -- InsertDlGrDoc(p_ID_OP, SRS.T_INTERNALMESSAGEID, l_GrID);
                  ELSE
                      err:= 1;
                      p_ErrorText:= 'Не получилось вставить строку графика';
                  END IF;

                  InsertDlGrDoc(p_ID_OP, SRS.T_INTERNALMESSAGEID, l_GrID);                      
                  /*INSERT INTO ddlgrdoc_dbt (t_ID, t_GRDEALID, T_DOCKIND, T_DOCID, T_SERVDOCKIND, T_SERVDOCID, T_GRPID, T_SOURCETYPE)
                      VALUES (0,l_GrID,REPOS_GENERALINF,SRS.T_INTERNALMESSAGEID,REPOS_INBOUNDMSG,0,0,0); */                      
              END IF;
            ELSE
                err := 1;
            END IF;

        END IF;

          IF (err = 0) THEN
            update dir_generalinf_dbt
            set T_RSTATUS = RSTATUS_READY_REV,
                T_RSTATUSNAME = ( SELECT NA.T_SZNAMEALG
                                    FROM DNAMEALG_DBT NA
                                   WHERE NA.T_ITYPEALG = ALG_GENINF_RSTATUS
                                     AND NA.T_INUMBERALG = RSTATUS_READY_REV )
            where T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID;
          ELSE
            l_DealID := RSTATUS_ER_PROCESSED;
            IF (p_ErrorText = 'В исходной сделке указан признак отказа от репортинга') THEN
                l_DealID := RSTATUS_NOT_PROCES;
            END IF;
            update dir_generalinf_dbt
            set T_RSTATUS = l_DealID,
                T_RSTATUSNAME = ( SELECT NA.T_SZNAMEALG
                                    FROM DNAMEALG_DBT NA
                                   WHERE NA.T_ITYPEALG = ALG_GENINF_RSTATUS
                                     AND NA.T_INUMBERALG = l_DealID )
            where T_INTERNALMESSAGEID = SRS.T_INTERNALMESSAGEID;
          END IF;

      END IF;
      p_Protocol:= p_Protocol ||  Get_RowProtocolProc (SRS.T_MESSAGETYPE, SRS.T_MESSAGEID, nvl(SRS.T_INREPLYTO,' '), nvl(SRS.T_TRADEID_UTIGENERATINGPARTY, ' '), nvl(SRS.T_XMLPRODUCT, ' ' ), p_ErrorText) ;

   END LOOP;
    
   return 0;
   end ProcInboundSRS;

   function Get_RowProtocol (p_MESTYPE in varchar2, p_Product in varchar2, p_ErrorText in varchar2) return clob as
     l_Report CLOB;
   begin
     SELECT xmlroot(
              xmlelement("Property",
                xmlattributes(p_MESTYPE as "MESTYPE",
                              x.MESCODEIN as "MESCODEIN",
                              nvl(x.MESCODEOUT, ' ') as "MESCODEOUT",
                              y.tradeId as "CODEUTI",
                              REGEXP_REPLACE(p_Product, '[[:cntrl:]]', ' ') as "MESPRODUCT",
                              p_ErrorText as "CAUSEERR")),  version '1.0').GetClobVal()
       INTO l_Report
       FROM DIR_SRS_TMP t,
            XMLTABLE (
               xmlnamespaces (
                  DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                          'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                          'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                    '/*'
                    PASSING xmltype(t.t_fmtclobdata_xxxx)
                    COLUMNS MESCODEIN PATH '/*/header/messageId',
                            MESCODEOUT PATH '/*/header/inReplyTo',
                            tradeHeader XMLTYPE PATH '//trade/tradeHeader') x,
            XMLTABLE (
               xmlnamespaces (
                  DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                          'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                          'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                    '//partyTradeIdentifier'
                    PASSING x.tradeHeader
                    COLUMNS tradeId PATH 'tradeId',
                            href    PATH 'partyReference/@href') y
      WHERE y.href = 'UTIGeneratingParty';
     Return substr(l_Report, instr(l_Report, '<Property'));
   exception
     WHEN NO_DATA_FOUND THEN
       SELECT xmlroot(
              xmlelement("Property",
                xmlattributes(p_MESTYPE as "MESTYPE",
                              x.MESCODEIN as "MESCODEIN",
                              nvl(x.MESCODEOUT,' ') as "MESCODEOUT",
                              ' ' as "CODEUTI",
                              REGEXP_REPLACE(p_Product, '[[:cntrl:]]', ' ') as "MESPRODUCT",
                              p_ErrorText as "CAUSEERR")),  version '1.0').GetClobVal()
       INTO l_Report
       FROM DIR_SRS_TMP t,
            XMLTABLE (
               xmlnamespaces (
                  DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping',
                          'http://www.fpml.org/FpML-5/recordkeeping/nsd-ext' AS "nsdext",
                          'http://www.fpml.org/FpML-5/ext' AS "fpmlext"),
                    '/*'
                    PASSING xmltype(t.t_fmtclobdata_xxxx)
                    COLUMNS MESCODEIN PATH '/*/header/messageId',
                            MESCODEOUT PATH '/*/header/inReplyTo') x;
     Return substr(l_Report, instr(l_Report, '<Property'));
   end Get_RowProtocol;

   function Get_RowProtocolProc (p_MESSAGETYPE in varchar2, p_MESSAGEID in varchar2, p_INREPLYTO in varchar2, p_TRADEID_UTIGENERATINGPARTY in varchar2, p_XMLPRODUCT in varchar2, p_ErrorText in varchar2) return clob as
     l_Report CLOB;
   begin
     SELECT xmlroot(
              xmlelement("Property",
                xmlattributes(p_MESSAGETYPE as "MESTYPE",
                              p_MESSAGEID as "MESCODEIN",
                              REGEXP_REPLACE(p_INREPLYTO, '[[:cntrl:]]', ' ') as "MESCODEOUT",
                              REGEXP_REPLACE(p_TRADEID_UTIGENERATINGPARTY, '[[:cntrl:]]', ' ') as "CODEUTI",
                              REGEXP_REPLACE(p_XMLPRODUCT, '[[:cntrl:]]', ' ') as "MESPRODUCT",
                              p_ErrorText as "CAUSEERR")),  version '1.0').GetClobVal()
       INTO l_Report
       FROM dual;
     Return substr(l_Report, instr(l_Report, '<Property'));
/*        exception
     WHEN NO_DATA_FOUND THEN
     SELECT xmlroot(
              xmlelement("Property",
                xmlattributes(p_MESSAGETYPE as "MESTYPE",
                              p_MESSAGEID as "MESCODEIN",
                              p_INREPLYTO as "MESCODEOUT",
                              '' as "CODEUTI",
                              p_XMLPRODUCT as "MESPRODUCT",
                              p_ErrorText as "CAUSEERR")),  version '1.0').GetClobVal()
       INTO l_Report
       FROM dual;*/

   end Get_RowProtocolProc;

   PROCEDURE DeleteSRS (p_InterMesID in integer) is
   begin
     DELETE FROM dir_generalinf_dbt t WHERE t.t_internalmessageid = p_InterMesID;
   end DeleteSRS;

   PROCEDURE InsertProtocol (p_Protocol in clob) is
   begin
     insert into DDL_LOGDATA_DBT(T_LOGID, T_ID_OPERATION, T_ID_STEP, T_TYPE, T_LOGDATA)
       values(2912, 0, 0, 1, p_Protocol);
   end InsertProtocol;

   
   /* процедура вставки/изменения параметров подтверждения сделки
    * p_DocKind  Вид сделки
    * p_DocId    ID сделки
    * p_Status    Статус подтверждения
    * p_Condition Состояние подтверждения
    Если запись с ключом p_DocKind/p_DocID уже существует, то происходит ее обновление заданными p_Status/p_Condition
    Если р_Status <= 0, то T_STATUS := p_Condition.    */
   PROCEDURE InsertDLMesParams(p_DocKind IN INTEGER, p_DocID IN INTEGER, p_Status IN INTEGER, p_Condition IN INTEGER) 
   IS      
   BEGIN
      RSB_SECUR.InsertDLMes(p_DocKind, p_DocID, p_Status, p_Condition);
   END InsertDLMesParams;
 
   PROCEDURE InsertEmailNotify (p_EmailGroup in integer, p_Head in varchar2, p_Text in clob) is
     PRAGMA AUTONOMOUS_TRANSACTION; 
   BEGIN 
     execute immediate 'INSERT INTO DEMAIL_NOTIFY_DBT(T_DATEADD,  
                                                      T_EMAIL,  
                                                      T_HEAD,  
                                                      T_TEXT) 
                          SELECT SYSDATE, 
                                 uea.t_email, 
                                 :p_Head, 
                                 :p_Text
                            FROM usr_email_addr_dbt uea
                           WHERE uea.t_group = :p_EmailGroup' USING p_Head, p_Text, p_EmailGroup;
     COMMIT; 
   END InsertEmailNotify;

END RSB_PAYMENTS_API;
/