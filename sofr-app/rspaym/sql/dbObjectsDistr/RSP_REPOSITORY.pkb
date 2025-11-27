CREATE OR REPLACE PACKAGE BODY RSP_REPOSITORY as
 g_MessageID INTEGER;
 g_KindMsg VARCHAR2(3);
 g_XMLProduct VARCHAR2(35);
 g_Amendment CHAR(1);
 function GENMSG4REPO (P_DRAFTID in integer,P_OPER in integer,P_DEPARTMENT in integer,P_TEXT in out varchar2) return INTEGER as
 /*Функция инициирует генерацию сообщения по указанному поручению в Payments*/
 l_KindMsg VARCHAR2(5);
 l_ResCrt INTEGER;
 l_Cnt INTEGER;
 begin
 RSP_COMMON.INS_LOG(1, 'Вызвана функция GENMSG4REPO: ID = '||P_DRAFTID);
 g_MessageID := P_DRAFTID;
 RSP_ADM.SET_USERID(USERID => to_char(p_Oper));
 p_Text := chr(1);
 SELECT gen.t_messagetype,
 gen.t_xmlproduct,
 gen.t_amendment
 INTO l_KindMsg,
 g_XMLProduct,
 g_Amendment
 FROM IR_GENERALINF gen
 WHERE gen.t_internalmessageid = p_DraftID;
 g_KindMsg := substr(l_KindMsg, 3);
 --Проверка наличия сообщений
 SELECT count(p.id_paym)
 INTO l_Cnt
 FROM paym p,
 payass a,
 nform n,
 msgrepo m
 WHERE p.id_paym = a.id_paym
 AND a.associate = to_char(p_DraftID)
 AND a.bankid = g_KindMsg
 AND p.id_nform = n.id_nform
 AND n.code = RSP_SETUP.GET_VALUE(P_PATH => 'Репозитарий\Формы\СРС')
 AND p.id_paym = m.id_paym
 AND m.typ = l_KindMsg
 AND p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
 P_CODE => RSP_SETUP.GET_VALUE(P_PATH => 'Репозитарий\Состояния\Откат'));
 IF l_Cnt <> 0 THEN
 p_Text := 'По поручению ID='||p_DraftID||' уже есть СРС';
 RSP_COMMON.INS_LOG(1, 'Завершение функции GENMSG4REPO: '||p_Text);
 Return 1;
 END IF;
 l_ResCrt := RSP_REPOSITORY.Crt_SRS(P_DRAFTID => p_DraftID,
 P_KINDMSG => l_KindMsg,
 P_TEXT => p_Text);
 RSP_COMMON.INS_LOG(1, 'Завершение функции GENMSG4REPO: '||p_Text);
 Return l_ResCrt;
 end GenMsg4Repo;
 function CRT_SRS (P_DRAFTID in integer,P_KINDMSG in varchar2,P_TEXT out varchar2) return INTEGER as
 /*Функция создает платеж с сообщением по указанному поручению.
 p_DraftID - идентификатор поручения,
 p_KindMsg - тип сообщения,
 p_Text - информация о создании платежа.
 Возвращает код результата выполнения.
 */
 l_IDPaym INTEGER;
 l_Text VARCHAR2(32000);
 l_Msg CLOB;
 l_IDKart INTEGER;
 l_Form INTEGER;
 l_IDPack INTEGER;
 begin
 --Создание платежа
 l_IDPaym := RSP_REPOSITORY.CRT_PAYM(P_DRAFTID => p_DraftID,
 P_KINDMSG => p_KindMsg,
 P_TEXT => l_Text);
 IF l_IDPaym IS NULL THEN
 p_Text := substr('Ошибка создания платежа по сделке '||p_DraftID||': '||l_Text, 1, 4000);
 Return 2;
 END IF;
 --Получение сообщения
 l_Msg := RSP_REPOSITORY.GenXMLMessage(p_MessageID => p_DraftID,
 P_TEXT => l_Text);
 IF l_Msg IS NULL THEN
 p_Text := substr('Ошибка создания СРС по сделке '||p_DraftID||': '||l_Text, 1, 4000);
 Return 2;
 END IF;
 --Заполнение "хвостовой" таблицы
 RSP_PAYM_API.Crt_MsgREPO(P_IDPAYM => l_IDPAYM,
 P_MSG => l_Msg,
 P_KINDMSG => p_KindMsg);
 --Создание ассоциации платежа с поручением
 RSP_PAYM_API.Crt_PaymAssociate(P_ASS => p_DraftID,
 P_IDPAYM => l_IDPaym,
 P_IDTYPASS => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SASS',
 P_CODE => RSP_SETUP.GET_VALUE(P_PATH => 'Репозитарий\Ассоциация')),
 P_BIC => g_KindMsg);
 --Создание пакета
 SELECT n.id_nkart INTO l_IDKart
 FROM nkart n
 WHERE n.namekart = 'Ценные бумаги';
 SELECT f.code INTO l_Form
 FROM nform f, ntable n
 WHERE f.id_ntable = n.id_ntable AND n.name = 'IPSPCK';
 l_IDPack := RSP_PAYM_API.PackCreation(P_IDPAYM => l_IDPaym,
 P_REFER => RSP_REF.MakeRefer(P_FORMREF => l_Form,
 P_TYPEREF => NULL),
 P_IDSTATE => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTPCK',
 P_CODE => RSP_SETUP.GET_VALUE(P_PATH => 'Репозитарий\Состояния пакета\Сформирован запрос')),
 P_NUMREFER => null,
 P_IDKART => l_IDKart);
 IF l_IDPack IS NOT NULL THEN
 p_Text := p_Text||' Создан пакет '||l_IDPack||'.';
 END IF;
 --Результат
 Return 0;
 end Crt_SRS;
 function CRT_PAYM (P_DRAFTID in integer,P_KINDMSG in varchar2,P_TEXT out varchar2) return INTEGER as
 /*Создание платежа для сообщения.
 p_DraftID - идентификатор поручения,
 p_KindMsg - тип сообщения,
 p_Text - информация о создании платежа.
 Возвращает идентификатор созданного платежа.
 */
 l_TypBIC INTEGER;
 l_DealSum NUMBER;
 l_BIC VARCHAR2(100);
 l_IDFi INTEGER;
 l_NumRef VARCHAR2(30);
 l_NKorr INTEGER;
 l_Nks INTEGER;
 l_IDPaym INTEGER;
 f1 INTEGER;
 l_sql_stmt VARCHAR2(1024);
 begin
 --Определение типа БИК-а
 l_TypBIC := RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'STBIC',
 P_CODE => 40);
 --Определение получателя
 l_sql_stmt := 'SELECT distinct nvl(v.t_sendTo, chr(1)),
 sf.id_sfi,
 nvl(RSP_COMMON.EraseChr1(v.t_dealcode), v.t_messageid)
 FROM IR_GENERALINF v,
 '||CASE WHEN g_KindMsg NOT IN('001', '002', '010', '011')
 THEN 'IR_'||p_KindMsg||' cm,'
 END||'
 sfi sf
 WHERE v.t_internalmessageid = :DraftID
 '||CASE WHEN g_KindMsg NOT IN('001', '002', '010', '011')
 THEN 'AND v.t_internalmessageid = cm.t_internalmessageid'
 END||'
 AND sf.strcode = nvl('||CASE WHEN g_KindMsg = '022'
 THEN 'replace(cm.t_settlcurr, ''RUR'', ''RUB'')'
 WHEN g_KindMsg = '023'
 THEN 'replace(cm.t_settlcurrency, ''RUR'', ''RUB'')'
 WHEN g_KindMsg = '041'
 THEN 'replace(cm.t_part1curr, ''RUR'', ''RUB'')'
 WHEN g_KindMsg = '083'
 THEN 'replace(cm.t_c_spot, ''RUR'', ''RUB'')'
 ELSE ''''''
 END||', ''RUB'')';
 EXECUTE IMMEDIATE l_sql_stmt INTO l_BIC, l_IDFi, l_NumRef USING p_DraftID;
 IF l_BIC = chr(1) THEN
 p_Text := 'Не задан репозитарный идентификационный код получателя.';
 Return NULL;
 END IF;
 BEGIN
 --Определение корсхемы
 SELECT n.id_nkorr, k.id_nks INTO l_NKorr, l_Nks
 FROM nkorr n, nks k, nroute r
 WHERE r.bic = l_BIC AND n.id_nkorr = r.id_nkorr AND k.id_nkorr = n.id_nkorr AND k.code = 1;
 EXCEPTION
 WHEN NO_DATA_FOUND THEN
  IF l_NKorr IS NULL THEN
 --Создание корсхемы
 RSP_PAYM_API.Crt_KorrKs(P_CODEKORR => /*NULL*/ 0,
 P_NAMEKORR => 'Клиент '||l_BIC,
 P_PRIOR => 10,
 P_BIC => l_BIC,
 P_TYPBIC => l_TypBIC,
 P_IDFI => l_IDFi,
 P_CODEKS => 1,
 P_NAMEKS => 'Расчеты по ц/б.(репозитарий)',
 P_FRNSYS => NULL,
 P_LORO => 1,
 P_IDKORR => l_NKorr,
 P_IDKS => l_NKs);
 END IF;
 END;
 --Создание платежа
 INSERT INTO paym(nodeid, id_nform, id_spclass, id_sfi, amount, payerbankid, id_stbicpayer, payeraccount,
 receiverbankid, id_stbicreceiver, id_sstate, numreferdoc, numpack, datereferdoc, valuedate,
 currstatedate, inputdate, id_nkorroutput, id_nksoutput, id_sfioutput, dateoutput, priority)
 VALUES(RSP_SETUP.GET_VALUE(P_PATH => 'Настройки инициализации\OWNNODE'),
 RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'NFORM',
 P_CODE => RSP_SETUP.Get_Value(P_PATH => 'Репозитарий\Формы\СРС')),
 RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SPCLASS',
 P_CODE => 1),
 l_IDFi,
 l_DealSum,
 (select r.bic
 from nroute r
 where r.id_nkorr is null and r.id_stbic = l_TypBIC),
 l_TypBIC,
 (select r.accmask
 from nroute r
 where r.id_nkorr is null and r.id_stbic = l_TypBIC),
 l_BIC, l_TypBIC,
 RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTATE',
 P_CODE => RSP_SETUP.Get_Value(P_PATH => 'Репозитарий\Состояния\Отправляется')),
 l_NumRef,
 8888, trunc(sysdate, 'DD'), trunc(sysdate, 'DD'), sysdate,
 sysdate, l_NKorr, l_Nks, l_IDFi, RSP_COMMON.Get_DateOperDay, 6)
 RETURNING id_paym INTO l_IDPaym;
 --Создание истории платежа
 DELETE FROM tmp_paym_pack;
 INSERT INTO tmp_paym_pack(id_paym, id_sstate)
 SELECT l_IDPaym, id_sstate
 FROM paym
 WHERE id_paym = l_IDPaym;
 IF SQL%ROWCOUNT > 0 THEN
 f1 := RSP_GATE.MASSCHANGESTATE(P_TBLNAME => 'TMP_PAYM_PACK',
 P_UPD => 1);
 IF f1 > 0 THEN
 p_Text := 'Создан платеж '||l_IDPaym||'.';
 Return l_IDPaym;
 END IF;
 ELSE
 Return NULL;
 END IF;
 exception
 WHEN OTHERS THEN
 p_Text := SQLERRM;
 rollback;
 Return null;
 end Crt_Paym;
 function GenXMLMessage (p_MessageID in integer, P_TEXT out varchar2) return CLOB as
 /*Функция генерирует текст сообщением по указанному поручению в Payments.
 p_MessageID - идентификатор поручения,
 Возвращает текст сообщения.
 */
 l_sql_stmt CLOB;
 l_Msg CLOB;
 begin
 RSP_COMMON.INS_LOG(1, 'Вызвана функция GenXMLMessage');
 l_sql_stmt := 'SELECT xmlroot(xmlelement("'||CASE WHEN g_KindMsg = '001'
 THEN 'nonpublicExecutionReportAcknowledgement'
 WHEN g_KindMsg = '002'
 THEN 'nonpublicExecutionReportException'
 ELSE 'nonpublicExecutionReport'
 END||'",
 xmlattributes(''http://www.fpml.org/FpML-5/recordkeeping'' as "xmlns",
 ''http://www.fpml.org/FpML-5/recordkeeping/nsd-ext'' as "xmlns:nsdext",
 ''http://www.w3.org/2001/XMLSchema-instance'' as "xmlns:xsi",
 ''http://www.fpml.org/FpML-5/ext'' as "xmlns:fpmlext",
 ''http://www.fpml.org/FpML-5/recordkeeping ../fpml-recordkeeping-merged-schema.xsd http://www.fpml.org/FpML-5/recordkeeping/nsd-ext ../nsd-ext-merged-schema.xsd'' as "xsi:schemaLocation",
 ''5-4'' as "fpmlVersion",
 ''5'' as "actualBuild"),
 '||CASE WHEN g_KindMsg = '001'
 THEN RSP_REPOSITORY.GetXMLCM001
 WHEN g_KindMsg = '002'
 THEN RSP_REPOSITORY.GetXMLCM002
 WHEN g_KindMsg = '010'
 THEN RSHB_RSPAYM_SEC.RSP_REPOSITORY.GetXMLCM010
 WHEN g_KindMsg = '011'
 THEN RSP_REPOSITORY.GetXMLCM011
 WHEN g_KindMsg = '021'
 THEN RSP_REPOSITORY.GetXMLCM021
 WHEN g_KindMsg = '022'
 THEN RSP_REPOSITORY.GetXMLCM022
 WHEN g_KindMsg = '023'
 THEN RSP_REPOSITORY.GetXMLCM023
 WHEN g_KindMsg = '032'
 THEN RSP_REPOSITORY.GetXMLCM032
 WHEN g_KindMsg = '041'
 THEN RSP_REPOSITORY.GetXMLCM041
 WHEN g_KindMsg = '042'
 THEN RSP_REPOSITORY.GetXMLCM042
 WHEN g_KindMsg = '043'
 THEN RSP_REPOSITORY.GetXMLCM043
 WHEN g_KindMsg = '044'
 THEN RSP_REPOSITORY.GetXMLCM044
 WHEN g_KindMsg = '045'
 THEN RSP_REPOSITORY.GetXMLCM045
 WHEN g_KindMsg = '046'
 THEN RSP_REPOSITORY.GetXMLCM046
 WHEN g_KindMsg = '047'
 THEN RSP_REPOSITORY.GetXMLCM047
 WHEN g_KindMsg = '048'
 THEN RSP_REPOSITORY.GetXMLCM048
 WHEN g_KindMsg = '051'
 THEN RSP_REPOSITORY.GetXMLCM051
 WHEN g_KindMsg = '053'
 THEN RSP_REPOSITORY.GetXMLCM053
 WHEN g_KindMsg = '083'
 THEN RSP_REPOSITORY.GetXMLCM083
 WHEN g_KindMsg = '092'
 THEN RSP_REPOSITORY.GetXMLCM092
 WHEN g_KindMsg = '093'
 THEN RSP_REPOSITORY.GetXMLCM093
 WHEN g_KindMsg = '094'
 THEN RSP_REPOSITORY.GetXMLCM094
 END||'), version ''1.0" encoding="windows-1251'').getclobval()
 FROM IR_GENERALINF gen
 WHERE gen.t_internalmessageid = :MessageID';
 dbms_output.put_line(substr(l_sql_stmt, 1, 4000));
 dbms_output.put_line(substr(l_sql_stmt, 4001, 4000));
 dbms_output.put_line(substr(l_sql_stmt, 8001, 4000));
 dbms_output.put_line(substr(l_sql_stmt, 12001, 4000));
 dbms_output.put_line(substr(l_sql_stmt, 16001, 4000));
 dbms_output.put_line(substr(l_sql_stmt, 20001, 4000));
 dbms_output.put_line(substr(l_sql_stmt, 24001, 4000));
 dbms_output.put_line(substr(l_sql_stmt, 28001, 4000));
 dbms_output.put_line(substr(l_sql_stmt, 32001, 4000));
 dbms_output.put_line(substr(l_sql_stmt, 36001, 4000)||' '||p_MessageID);

 EXECUTE IMMEDIATE l_sql_stmt INTO l_Msg USING p_MessageID ;
 Return l_Msg;
 exception
 WHEN OTHERS THEN
 p_Text := SQLERRM;
 rollback;
 Return null;
 end GenXMLMessage;

 function GetXMLCM001 return clob as
 begin
 Return RSP_REPOSITORY.GetBlockHeader||',
 xmlelement("parentCorrelationId",
 xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"), RSP_COMMON.EraseChr1(gen.T_PARENTCORRELATIONID)),
 xmlelement("correlationId",
 xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"), RSP_COMMON.EraseChr1(gen.T_CORRELATIONID)),
 '||RSP_REPOSITORY.GetBlockOriginalMessage||',
 '||RSP_REPOSITORY.GetBlockPartyQuest(p_PartyType => 'Sender');
 end GetXMLCM001;

 function GetXMLCM002 return clob as
 begin
 Return RSP_REPOSITORY.GetBlockHeader||',
 xmlelement("correlationId ",
 xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"), RSP_COMMON.EraseChr1(gen.T_CORRELATIONID)),
 xmlelement("reason",
 xmlelement("reasonCode", RSP_COMMON.EraseChr1(gen.t_reasoncode)),
 xmlelement("description", RSP_COMMON.EraseChr1(gen.t_reasondescription))),
 xmlelement("additionalData",
 xmlelement("originalMessage",
 xmlelement("nsdext:partiesInformation",
 '||RSP_REPOSITORY.GetBlockPartyQuest(p_PartyType => 'TradeRepository')||',
 '||RSP_REPOSITORY.GetBlockPartyQuest(p_PartyType => 'Sender')||')))';
 end GetXMLCM002;

 function GetXMLCM010 return clob as
 begin
 Return RSP_REPOSITORY.GetBlockHeader||',
 xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
 CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
 THEN xmlelement("parentCorrelationId",
 xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"), gen.t_parentcorrelationid)
 END,
 CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
 THEN xmlelement("correlationId",
 xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"), gen.t_correlationid)
 END,
 xmlelement("asOfDate", gen.t_asofdate),
 xmlelement("trade",
 '||RSP_REPOSITORY.GetBlockTradeHeader(p_KindMsg => g_KindMsg)||',
 '||RSP_REPOSITORY.GetBlockMasterAgrTerms||'),
 '||RSP_REPOSITORY.GetBlockParty;
 end GetXMLCM010;

 function GetXMLCM011 return clob as
 begin
 Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            '||RSP_REPOSITORY.GetBlockMasterAgrTermination||',
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM011;

   function GetXMLCM021 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"), gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"), gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockFxSwap||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM021;

   function GetXMLCM022 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockFxSingleLeg||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM022;

   function GetXMLCM023 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockFxOption||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM023;

   function GetXMLCM032 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockSwap||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM032;

   function GetXMLCM041 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            CASE WHEN gen.t_Amendment = ''Y''
                 THEN xmlelement("agreementDate", gen.t_AgreementDate)
            END,
            CASE WHEN gen.t_Amendment = ''Y''
                 THEN xmlelement("effectiveDate", gen.t_EffectiveDate)
            END,
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockRepo||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM041;

   function GetXMLCM042 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            CASE WHEN gen.t_Amendment = ''Y''
                 THEN xmlelement("agreementDate", gen.t_AgreementDate)
            END,
            CASE WHEN gen.t_Amendment = ''Y''
                 THEN xmlelement("effectiveDate", gen.t_EffectiveDate)
            END,
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockBondSimpleTransaction||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM042;

   function GetXMLCM043 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            '||RSP_REPOSITORY.GetBlockGeneralElement||',
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockBondForward||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM043;

   function GetXMLCM044 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            CASE WHEN gen.t_Amendment = ''Y''
                 THEN xmlelement("agreementDate", gen.t_AgreementDate)
            END,
            CASE WHEN gen.t_Amendment = ''Y''
                 THEN xmlelement("effectiveDate", gen.t_EffectiveDate)
            END,
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockBondOption||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM044;

   function GetXMLCM045 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            CASE WHEN gen.t_Amendment = ''Y''
                 THEN xmlelement("agreementDate", gen.t_AgreementDate)
            END,
            CASE WHEN gen.t_Amendment = ''Y''
                 THEN xmlelement("effectiveDate", gen.t_EffectiveDate)
            END,
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockBondBasketOption||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM045;

   function GetXMLCM046 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            CASE WHEN gen.t_Amendment = ''Y''
                 THEN xmlelement("agreementDate", gen.t_AgreementDate)
            END,
            CASE WHEN gen.t_Amendment = ''Y''
                 THEN xmlelement("effectiveDate", gen.t_EffectiveDate)
            END,
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockEquitySimplTransaction||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM046;

   function GetXMLCM047 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            '||RSP_REPOSITORY.GetBlockGeneralElement||',
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockEquityForward||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM047;

   function GetXMLCM048 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            CASE WHEN gen.t_Amendment = ''Y''
                 THEN xmlelement("agreementDate", gen.t_AgreementDate)
            END,
            CASE WHEN gen.t_Amendment = ''Y''
                 THEN xmlelement("effectiveDate", gen.t_EffectiveDate)
            END,
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockEquityOption||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM048;

   function GetXMLCM051 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockCommodityForward||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM051;

   function GetXMLCM053 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader||',
              '||RSP_REPOSITORY.GetBlockCommoditySwap||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM053;

   function GetXMLCM083 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            xmlelement("trade",
              xmlattributes(''nsdext:TradeNsd'' as "xsi:type"),
              '||RSP_REPOSITORY.GetBlockTradeHeader(p_KindMsg => '083')||',
              '||RSP_REPOSITORY.GetBlockRepoBulkReport||',
              CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1) OR nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                        OR nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'') OR nvl(gen.t_excluded, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                   THEN '||RSP_REPOSITORY.GetBlockCollateral(p_KindMsg => '083')||'
              END,
              '||RSP_REPOSITORY.GetBlockSpecificTradeFields083||'),
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM083;

   function GetXMLCM092 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"), gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            '||RSP_REPOSITORY.GetBlockTransfersAndExecution||',
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM092;

   function GetXMLCM093 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                 THEN xmlelement("parentCorrelationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
            END,
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"), gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            '||RSP_REPOSITORY.GetBlockExecutionStatus||',
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM093;

   function GetXMLCM094 return clob as
   begin
     Return RSP_REPOSITORY.GetBlockHeader||',
            xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
            CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                 THEN xmlelement("correlationId",
                        xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"), gen.t_correlationid)
            END,
            xmlelement("asOfDate", gen.t_asofdate),
            '||RSP_REPOSITORY.GetBlockMarkToMarketValuation||',
            '||RSP_REPOSITORY.GetBlockParty;
   end GetXMLCM094;

   function GetBlockHeader return clob as
   begin
     Return 'xmlelement("header",
               xmlelement("messageId",
                 xmlattributes(''http://repository.nsd.ru/coding-scheme/messageid(nsdrus)'' as "messageIdScheme"), RSP_COMMON.EraseChr1(gen.t_messageid)),
               '||CASE WHEN g_KindMsg = '001'
                       THEN 'xmlelement("inReplyTo",
                               xmlattributes(''http://repository.nsd.ru/coding-scheme/messageid(nsdrus)'' as "messageIdScheme"), RSP_COMMON.EraseChr1(gen.t_InReplyTo)),'
                  END||'
               xmlelement("sentBy", RSP_COMMON.EraseChr1(gen.t_sendby)),
               xmlelement("sendTo", RSP_COMMON.EraseChr1(gen.t_sendto)),
               '||CASE WHEN g_KindMsg <> '001'
                       THEN 'xmlelement("creationTimestamp", to_char(gen.t_creationdate, ''YYYY-MM-DD'')||''T''||to_char(gen.t_creationtime, ''hh24:mi:ss'')),'
                  END||'
               xmlelement("implementationSpecification",
                 xmlelement("version", RSP_COMMON.EraseChr1(gen.t_version))))';
   end GetBlockHeader;

   function GetBlockGeneralElement return clob as
   begin
     Return 'xmlelement("isCorrection", decode(gen.t_iscorrection, ''X'', ''true'', ''false'')),
               CASE WHEN nvl(gen.t_parentcorrelationid, chr(1)) <> chr(1)
                    THEN xmlelement("parentCorrelationId",
                           xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_parentcorrelationid)
               END,
               CASE WHEN nvl(gen.t_correlationid, chr(1)) <> chr(1)
                    THEN xmlelement("correlationId",
                           xmlattributes(''http://repository.nsd.ru/coding-scheme/correlationId(nsdrus)'' as "correlationIdScheme"),  gen.t_correlationid)
               END,
               xmlelement("asOfDate", gen.t_asofdate),
               CASE WHEN gen.t_Amendment = ''Y''
                    THEN xmlelement("agreementDate", gen.t_AgreementDate)
               END,
               CASE WHEN gen.t_Amendment = ''Y''
                    THEN xmlelement("effectiveDate", gen.t_EffectiveDate)
               END';
   end GetBlockGeneralElement;

   function GetBlockOriginalMessage return clob as
   begin
     Return 'xmlelement("originalMessage",
               '||CASE WHEN g_Amendment = 'X'
                       THEN 'xmlelement("nsdext:registeredInformation",
                               xmlelement("nsdext:operDay", gen.T_OPERDATE),
                               xmlelement("amendment",
                                 '||RSP_REPOSITORY.GetBlockInformation||'))))'
                        ELSE 'xmlelement("nsdext:agreedInformation",
                                '||RSP_REPOSITORY.GetBlockInformation||')))'
                  END;
   end GetBlockOriginalMessage;

   function GetBlockInformation return clob as
   begin
     Return CASE WHEN g_XMLProduct = 'executionStatus'
                 THEN RSP_REPOSITORY.GetBlockExecutionStatus
                 ELSE 'xmlelement("trade",
                         '||RSP_REPOSITORY.GetBlockTradeHeader(p_KindMsg => g_KindMsg)||',
                         '||CASE WHEN g_XMLProduct = 'repo'
                                 THEN RSP_REPOSITORY.GetBlockRepo
                                 WHEN g_XMLProduct = 'fxSingleLeg'
                                 THEN RSP_REPOSITORY.GetBlockFxSingleLeg
                                 WHEN g_XMLProduct = 'fxOption'
                                 THEN RSP_REPOSITORY.GetBlockFxOption
                                 WHEN g_XMLProduct = 'fxSwap'
                                 THEN RSP_REPOSITORY.GetBlockFxSwap
                            END
            END||',
            '||RSP_REPOSITORY.GetBlockCollateral||',
            '||RSP_REPOSITORY.GetBlockSpecificTradeFields||',
            '||RSP_REPOSITORY.GetBlockParty;
   end GetBlockInformation;

   function GetBlockTradeHeader (p_KindMsg in varchar2 default null) return clob as
   begin
     Return 'xmlelement("tradeHeader",
               '||RSP_REPOSITORY.GetBlockPartyTradeIdentifier(p_PartyType => 'TradeRepository',
                                                              p_KindMsg   => p_KindMsg)||'
               '||RSP_REPOSITORY.GetBlockPartyTradeIdentifier(p_PartyType => 'Party1',
                                                              p_KindMsg   => p_KindMsg)||'
               '||RSP_REPOSITORY.GetBlockPartyTradeIdentifier(p_PartyType => 'Party2',
                                                              p_KindMsg   => p_KindMsg)||'
               '||CASE WHEN nvl(p_KindMsg, '0') <> '083'
                       THEN RSP_REPOSITORY.GetBlockPartyTradeIdentifier(p_PartyType => 'UTIGeneratingParty')
                  END||'
               '||RSP_REPOSITORY.GetBlockPartyTradeInformation||',
               xmlelement("tradeDate", gen.t_TradeDate))';
   end GetBlockTradeHeader;

   function GetBlockPartyTradeIdentifier (p_PartyType in varchar2, p_KindMsg in varchar2 default null) return clob as
   begin
     Return 'xmlelement("partyTradeIdentifier",
               xmlelement("partyReference",
                 xmlattributes('''||p_PartyType||''' as "href")),
               CASE WHEN nvl(gen.t_PartyRef_'||p_PartyType||', chr(1)) <> chr(1)
                    THEN xmlelement("tradeId", decode('''||p_PartyType||''', gen.t_PartyRef_TradeRepository, RSP_COMMON.EraseChr1(gen.t_TradeId_TradeRepository),
                                                                             gen.t_PartyRef_Party1, RSP_COMMON.EraseChr1(gen.t_TradeId_Party1),
                                                                             gen.t_PartyRef_Party2, RSP_COMMON.EraseChr1(gen.t_TradeId_Party2),
                                                                             gen.t_PartyRef_UTIGeneratingParty, RSP_COMMON.EraseChr1(gen.t_TradeId_UTIGeneratingParty)))
               END
               '||CASE WHEN nvl(p_KindMsg, '0') NOT IN('010', '083') AND p_PartyType = 'TradeRepository'
                       THEN ', CASE WHEN nvl(gen.t_LinkID, chr(1)) <> chr(1)
                                    THEN xmlelement("linkId",
                                           xmlattributes(''http://repository.nsd.ru/coding-scheme/linkid(nsdrus)'' as "linkIdScheme"), gen.t_linkid)
                               END,
                               CASE WHEN nvl(gen.t_OriginID_TradeRepository, chr(1)) <> chr(1)
                                    THEN xmlelement("originatingTradeId",
                                           xmlelement("partyReference",
                                             xmlattributes('''||p_PartyType||''' as "href")),
                                           xmlelement("tradeId", gen.t_OriginID_TradeRepository))
                               END'
                  END||'
               '||CASE WHEN nvl(p_KindMsg, '0') <> '083' AND p_PartyType = 'Party1'
                       THEN ', CASE WHEN nvl(gen.t_OriginatingID_Party1, chr(1)) <> chr(1)
                                    THEN xmlelement("originatingTradeId",
                                           xmlelement("partyReference",
                                             xmlattributes('''||p_PartyType||''' as "href")),
                                           xmlelement("tradeId", gen.t_OriginatingID_Party1))
                               END'
                  END||'
               '||CASE WHEN nvl(p_KindMsg, '0') <> '083' AND p_PartyType = 'Party2'
                       THEN ', CASE WHEN nvl(gen.t_OriginatingID_Party2, chr(1)) <> chr(1)
                                    THEN xmlelement("originatingTradeId",
                                           xmlelement("partyReference",
                                             xmlattributes('''||p_PartyType||''' as "href")),
                                           xmlelement("tradeId", gen.t_OriginatingID_Party2))
                               END'
                  END||'),';
   end GetBlockPartyTradeIdentifier;

   function GetBlockPartyTradeInformation return clob as
   begin
     Return 'xmlelement("partyTradeInformation",
               xmlelement("partyReference",
                 xmlattributes(gen.t_PartyRef_TradeRepository as "href")),
               xmlelement("reportingRegime",
                 xmlelement("name",
                   xmlattributes(''http://www.fpml.org/coding-scheme/reporting-regime'' as "reportingRegimeNameScheme"), RSP_COMMON.EraseChr1(gen.t_ReportingRegime))),
               CASE WHEN gen.t_nonStandardTerms = ''X''
                    THEN xmlelement("nonStandardTerms", ''true'')
               END)';
   end GetBlockPartyTradeInformation;

   function GetBlockCollateral (p_KindMsg in varchar2 default null) return clob as
   begin
     Return 'xmlelement("nsdext:collateral",
               CASE WHEN nvl(gen.t_margintypecode, chr(1)) <> chr(1)
                    THEN xmlelement("nsdext:marginType", RSP_COMMON.EraseChr1(gen.t_margintypecode))
               END,
               CASE WHEN nvl(gen.t_colaterallformcode, chr(1)) <> chr(1)
                    THEN xmlelement("nsdext:collateralForm", RSP_COMMON.EraseChr1(gen.t_colaterallformcode))
               END'||
               CASE WHEN nvl(p_KindMsg, '0') <> '083'
                    THEN ', CASE WHEN nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                                 THEN xmlelement("nsdext:dateTradeIncludedIntoPortfolio", gen.t_included)
                            END'
               END||''||
               CASE WHEN nvl(p_KindMsg, '0') <> '083'
                    THEN ', CASE WHEN nvl(gen.t_included, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                                 THEN xmlelement("nsdext:dateTradeExcludedFromPortfolio", gen.t_excluded)
                            END'
               END||')';
   end GetBlockCollateral;

   function GetBlockSpecificTradeFields return clob as
   begin
     Return 'xmlelement("nsdext:nsdSpecificTradeFields",
               CASE WHEN nvl(gen.t_BrokerID, chr(1)) <> chr(1)
                    THEN xmlelement("nsdext:reportingBrokerID", gen.t_BrokerID)
               END,
               xmlelement("nsdext:cleared", decode(gen.t_cleared, ''X'', ''Y'', ''N'')),
               CASE WHEN nvl(gen.t_ClearingOrgCode, chr(1)) <> chr(1)
                    THEN xmlelement("nsdext:clearingOrganizationCode", gen.t_ClearingOrgCode)
               END,
               CASE WHEN nvl(gen.t_ClearedDate, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                    THEN xmlelement("nsdext:clearedDate", gen.t_ClearedDate)
               END,
               CASE WHEN nvl(gen.t_ClearingDate, to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                    THEN xmlelement("nsdext:clearingDateTime", gen.t_ClearingDate||''T''||gen.t_clearingtime)
               END,
               CASE WHEN nvl(gen.t_CentralCounterparty, chr(1)) <> chr(1)
                    THEN xmlelement("nsdext:clearingCentralCounterpartyCode", gen.t_CentralCounterparty)
               END,
               CASE WHEN nvl(gen.t_ClearingMember, chr(1)) <> chr(1)
                    THEN xmlelement("nsdext:clearingMemberID", gen.t_ClearingMember)
               END,
               xmlelement("nsdext:reconciliationType", nvl(RSP_COMMON.EraseChr1(gen.t_ReconType), ''GENF'')),
               xmlelement("nsdext:clearSettlementType",
                 xmlattributes(''http://repository.nsd.ru/coding-scheme/clear-settlement-type'' as "clearSettlementTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ClearSettlementType)),
               xmlelement("nsdext:clearSettlementMethod",
                 xmlattributes(''http://repository.nsd.ru/coding-scheme/clear-settlement-method'' as "clearSettlementMethodScheme"), RSP_COMMON.EraseChr1(gen.t_clearSettlMethod)),
               xmlelement("nsdext:confirmationMethod", nvl(RSP_COMMON.EraseChr1(gen.t_ConfMethod), ''MXME'')),
/*
               xmlelement("nsdext:automaticExecution", decode(gen.t_automaticExecution, ''X'', ''Y'', ''N'')),
*/
               CASE WHEN gen.t_nonStandardTerms = ''X''
                    THEN xmlelement("nsdext:partiesAreAffiliated", decode(gen.t_AffParties, ''X'', ''Y'', ''N''))
               END,
               CASE WHEN nvl(gen.t_TradeRegTypeCode, chr(1)) <> chr(1)
                    THEN xmlelement("nsdext:regulatoryStatus",
                           xmlattributes(''http://repository.nsd.ru/coding-scheme/regulatory-status'' as "regulatoryAdviceScheme"), gen.t_TradeRegTypeCode)
               END,
               CASE WHEN nvl(trunc(gen.t_StartAgreementDate), to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                    THEN xmlelement("nsdext:startAgreementDate", gen.t_StartAgreementDate)
               END,
               CASE WHEN nvl(trunc(gen.t_EndAgreementDate), to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                    THEN xmlelement("nsdext:endAgreementDate", gen.t_EndAgreementDate)
               END,
               CASE WHEN RSP_COMMON.EraseChr1(gen.t_specificCode) IS NOT NULL
                         AND '||g_KindMsg||' NOT IN(''021'', ''022'', ''023'', ''053'')
                    THEN xmlelement("nsdext:productSpecificCodes",
                           xmlelement("nsdext:productGeneralCodes",
                             xmlelement("nsdext:classificationCode",
                               xmlelement("nsdext:specificCode", gen.t_specificCode),
                               CASE WHEN RSP_COMMON.EraseChr1(gen.t_RelCode) IS NOT NULL
                                    THEN xmlelement("nsdext:code", gen.t_RelCode)
                               END)))
               END,
               '||RSP_REPOSITORY.GetBlockClientDetails(p_PartyType => 'Party1')||',
               '||RSP_REPOSITORY.GetBlockClientDetails(p_PartyType => 'Party2')||')';
   end GetBlockSpecificTradeFields;

   function GetBlockSpecificTradeFields083 return clob as
   begin
     Return 'xmlelement("nsdext:nsdSpecificTradeFields",
               xmlelement("nsdext:cleared", RSP_COMMON.EraseChr1(gen.t_cleared)),
               xmlelement("nsdext:reconciliationType", RSP_COMMON.EraseChr1(gen.t_ReconType)),
               xmlelement("nsdext:clearSettlementType", RSP_COMMON.EraseChr1(gen.t_ClearSettlementType)),
               xmlelement("nsdext:clearSettlementMethod",
                 xmlattributes(''http://repository.nsd.ru/coding-scheme/clear-settlement-method'' as "clearSettlementMethodScheme"), RSP_COMMON.EraseChr1(gen.t_clearSettlMethod)),
               xmlelement("nsdext:confirmationMethod", nvl(RSP_COMMON.EraseChr1(gen.t_ConfMethod), ''MXME'')),
               xmlelement("nsdext:partiesAreAffiliated", decode(gen.t_AffParties, ''X'', ''Y'', ''N'')),
               CASE WHEN nvl(gen.t_RStatusName, chr(1)) <> chr(1)
                    THEN xmlelement("nsdext:regulatoryStatus",
                           xmlattributes(''http://repository.nsd.ru/coding-scheme/regulatory-status'' as "regulatoryAdviceScheme"), gen.t_RStatusName)
               END,
               CASE WHEN nvl(trunc(gen.t_StartAgreementDate), to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                    THEN xmlelement("nsdext:startAgreementDate", gen.t_StartAgreementDate)
               END,
               CASE WHEN nvl(trunc(gen.t_EndAgreementDate), to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                    THEN xmlelement("nsdext:endAgreementDate", gen.t_EndAgreementDate)
               END)';
   end GetBlockSpecificTradeFields083;

   function GetBlockSpecificTradeFields010 return clob as
   begin
     Return 'xmlelement("nsdext:nsdSpecificTradeFields",
               xmlelement("nsdext:confirmationMethod", nvl(RSP_COMMON.EraseChr1(gen.t_ConfMethod), ''MXME'')),
               xmlelement("nsdext:partiesAreAffiliated", decode(gen.t_AffParties, ''X'', ''Y'', ''N'')),
               xmlelement("nsdext:code", RSP_COMMON.EraseChr1(gen.t_RelCode)))';
   end GetBlockSpecificTradeFields010;

   function GetBlockMasterAgrTerms return clob as
   begin
     Return 'xmlelement("nsdext:masterAgreementTerms",
               xmlelement("nsdext:masterAgreementType",
                 xmlattributes(''http://www.fpml.org/coding-scheme/master-agreement-type'' as "masterAgreementTypeScheme"), RSP_COMMON.EraseChr1(gen.T_MASTERAGREEMENTTYPECODE)),
               xmlelement("nsdext:masterAgreementVersion",
                 xmlattributes(''http://www.fpml.org/coding-scheme/master-agreement-version'' as "masterAgreementVersionScheme"), RSP_COMMON.EraseChr1(gen.T_MASTERAGREEMENTVERSIONCODE)),
               CASE WHEN RSP_COMMON.EraseChr1(gen.T_MASTERAGREEMENTORGCODE) IS NOT NULL
                    THEN xmlelement("nsdext:masterAgreementOrganization", gen.T_MASTERAGREEMENTORGCODE)
               END,
               xmlelement("nsdext:confirmationMethod", nvl(RSP_COMMON.EraseChr1(gen.t_ConfMethod), ''MXME'')),
               CASE WHEN RSP_COMMON.EraseChr1(gen.T_NARRATIVEDESCRIPTION) IS NOT NULL
                    THEN xmlelement("nsdext:narrativeDescription", gen.T_NARRATIVEDESCRIPTION)
               END,
               xmlelement("nsdext:partiesAreAffiliated", decode(gen.T_AFFPARTIES, ''X'', ''Y'', ''N'')))';
   end GetBlockMasterAgrTerms;

   function GetBlockMasterAgrTermination return clob as
   begin
     Return 'xmlelement("nsdext:masterAgreementTermination",
               xmlelement("nsdext:masterAgreementId", RSP_COMMON.EraseChr1(gen.t_masteragreementid)),
               xmlelement("nsdext:maTerminatingReason", RSP_COMMON.EraseChr1(gen.t_materminatingreason)))';
   end GetBlockMasterAgrTermination;

   function GetBlockClientDetails (p_PartyType in varchar2) return clob as
   begin
     Return 'xmlelement("nsdext:clientDetails",
               xmlelement("nsdext:servicingParty",
                 xmlattributes('''||p_PartyType||''' as "href")),
               CASE WHEN '''||p_PartyType||''' = ''Party1''
                    THEN CASE WHEN gen.t_OwnTrade1 = ''X''
                              THEN xmlelement("nsdext:ownTrade", ''true'')
                              ELSE CASE WHEN gen.t_NoInf1 = ''X''
                                        THEN xmlelement("nsdext:noInformation", ''true'')
                                        ELSE CASE WHEN RSP_COMMON.EraseChr1(gen.t_ClientType1) IS NOT NULL
                                                  THEN xmlelement("nsdext:type", gen.t_ClientType1)
                                                  WHEN RSP_COMMON.EraseChr1(gen.t_ClientIdent1) IS NOT NULL
                                                  THEN xmlelement("nsdext:id", gen.t_ClientIdent1)
                                                  WHEN RSP_COMMON.EraseChr1(gen.t_ClientName1) IS NOT NULL
                                                  THEN xmlelement("nsdext:name", gen.t_ClientName1)
                                                  WHEN RSP_COMMON.EraseChr1(gen.t_ClientCountry1) IS NOT NULL
                                                  THEN xmlelement("nsdext:country",
                                                         xmlattributes(''http://www.fpml.org/ext/iso3166'' as "countryScheme"), gen.t_ClientCountry1)
                                             END
                                   END
                         END
                    ELSE CASE WHEN '''||p_PartyType||''' = ''Party2''
                              THEN CASE WHEN gen.t_OwnTrade2 = ''X''
                                        THEN xmlelement("nsdext:ownTrade", ''true'')
                                        ELSE CASE WHEN gen.t_NoInf2 = ''X''
                                                  THEN xmlelement("nsdext:noInformation", ''true'')
                                                  ELSE CASE WHEN RSP_COMMON.EraseChr1(gen.t_ClientType2) IS NOT NULL
                                                            THEN xmlelement("nsdext:type", gen.t_ClientType2)
                                                            WHEN RSP_COMMON.EraseChr1(gen.t_ClientIdent2) IS NOT NULL
                                                            THEN xmlelement("nsdext:id", gen.t_ClientIdent2)
                                                            WHEN RSP_COMMON.EraseChr1(gen.t_ClientName2) IS NOT NULL
                                                            THEN xmlelement("nsdext:name", gen.t_ClientName2)
                                                            WHEN RSP_COMMON.EraseChr1(gen.t_ClientCountry2) IS NOT NULL
                                                            THEN xmlelement("nsdext:country",
                                                                   xmlattributes(''http://www.fpml.org/ext/iso3166'' as "countryScheme"), gen.t_ClientCountry2)
                                                       END
                                             END
                                   END
                         END
               END)';
   end GetBlockClientDetails;

   function GetBlockParty return clob as
   begin
     Return '(select xmlagg(xmlelement("party",
                              xmlattributes(RSP_COMMON.EraseChr1(p.t_partytype) as "id"),
                              xmlelement("partyId", RSP_COMMON.EraseChr1(p.t_partyid1)),
                              xmlelement("partyId", RSP_COMMON.EraseChr1(p.t_partyid2)),
                              CASE WHEN nvl(p.t_partyname, chr(1)) <> chr(1)
                                   THEN xmlelement("partyName", p.t_partyname)
                              END,
                              CASE WHEN p.t_partytype IN(''Party1'', ''Party2'')
                                   THEN xmlelement("classification", RSP_COMMON.EraseChr1(p.t_classification))
                              END,
                              CASE WHEN p.t_partytype IN(''Party1'', ''Party2'')
                                   THEN xmlelement("country", RSP_COMMON.EraseChr1(p.t_country))
                              END,
                              CASE WHEN p.t_partytype IN(''Party1'', ''Party2'')
                                   THEN xmlelement("organizationType", RSP_COMMON.EraseChr1(p.t_organizationtype))
                              END))
                from IR_PARTY p
               where p.t_internalmessageid = gen.t_internalmessageid)';
--                 and p.t_partytype in(''Party1'', ''Party2'', ''TradeRepository'', ''UTIGeneratingParty'')
   end GetBlockParty;

   function GetBlockPartyQuest (p_PartyType in varchar2) return clob as
   begin
     Return '(select xmlelement("party",
                       xmlattributes('''||p_PartyType||''' as "id"),
                       xmlelement("partyId", p.T_PARTYID1),
                       xmlelement("partyId", p.T_PARTYID2),
                       xmlelement("partyName", p.T_PARTYNAME))
                from IR_PARTY p
               where p.t_internalmessageid = gen.t_internalmessageid
                 and p.t_partytype = '''||p_PartyType||''')';
   end GetBlockPartyQuest;

   function GetBlockFxSwap return clob as
   begin
     Return '(select xmlelement("fxSwap",
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       xmlelement("nearLeg",
                         xmlelement("exchangedCurrency1",
                           xmlelement("payerPartyReference",
                             xmlattributes(RSP_COMMON.EraseChr1(cm.T_PAYERCURR1_PART1) as "href")),
                           xmlelement("receiverPartyReference",
                             xmlattributes(RSP_COMMON.EraseChr1(cm.T_RECEIVERCURR1_PART1) as "href")),
                           xmlelement("paymentAmount",
                             xmlelement("currency",
                               xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_CURRENCY1_PART1)),
                             xmlelement("amount", replace(ltrim(to_char(cm.T_AMOUNTCURR1_PART1, ''99999999999999999990D00''), '' ''), '','', ''.'')))),
                         xmlelement("exchangedCurrency2",
                           xmlelement("payerPartyReference",
                             xmlattributes(RSP_COMMON.EraseChr1(cm.T_PAYERCURR2_PART1) as "href")),
                           xmlelement("receiverPartyReference",
                             xmlattributes(RSP_COMMON.EraseChr1(cm.T_RECEIVERCURR2_PART1) as "href")),
                           xmlelement("paymentAmount",
                             xmlelement("currency",
                               xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_CURRENCY2_PART1)),
                             xmlelement("amount", replace(ltrim(to_char(cm.T_AMOUNTCURR2_PART1, ''99999999999999999990D00''), '' ''), '','', ''.'')))),
                         xmlelement("dealtCurrency", cm.T_TRADECURR_PART1),
                         xmlelement("valueDate", cm.T_VALDATE_PART1),
                         xmlelement("exchangeRate",
                           xmlelement("quotedCurrencyPair",
                             xmlelement("currency1",
                               xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_QUOTED_CURR1_PART1)),
                             xmlelement("currency2",
                               xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_QUOTED_CURR2_PART1)),
                             xmlelement("quoteBasis", cm.T_QBASIS_PART1)),
                           xmlelement("rate", replace(cm.T_RATE_PART1, '','', ''.'')))
/*,
                         CASE WHEN RSP_COMMON.EraseChr1(cm.T_SETTLCURR_PART1) IS NOT NULL
                              THEN xmlelement("nonDeliverableSettlement",
                                     xmlelement("settlementCurrency",
                                       xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_SETTLCURR_PART1)))
                         END
*/
),
                       xmlelement("farLeg",
                         xmlelement("exchangedCurrency1",
                           xmlelement("payerPartyReference",
                             xmlattributes(RSP_COMMON.EraseChr1(cm.T_PAYERCURR1_PART2) as "href")),
                           xmlelement("receiverPartyReference",
                             xmlattributes(RSP_COMMON.EraseChr1(cm.T_RECEIVERCURR1_PART2) as "href")),
                           xmlelement("paymentAmount",
                             xmlelement("currency",
                               xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_CURRENCY1_PART2)),
                             xmlelement("amount", replace(ltrim(to_char(cm.T_AMOUNTCURR1_PART2, ''99999999999999999990D00''), '' ''), '','', ''.'')))),
                         xmlelement("exchangedCurrency2",
                           xmlelement("payerPartyReference",
                             xmlattributes(RSP_COMMON.EraseChr1(cm.T_PAYERCURR2_PART2) as "href")),
                           xmlelement("receiverPartyReference",
                             xmlattributes(RSP_COMMON.EraseChr1(cm.T_RECEIVERCURR2_PART2) as "href")),
                           xmlelement("paymentAmount",
                             xmlelement("currency",
                               xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_CURRENCY2_PART2)),
                             xmlelement("amount", replace(ltrim(to_char(cm.T_AMOUNTCURR2_PART2, ''99999999999999999990D00''), '' ''), '','', ''.'')))),
                         xmlelement("dealtCurrency", cm.T_TRADECURR_PART2),
                         xmlelement("valueDate", cm.T_VALDATE_PART2),
                         xmlelement("exchangeRate",
                           xmlelement("quotedCurrencyPair",
                             xmlelement("currency1",
                               xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_QUOTED_CURR1_PART2)),
                             xmlelement("currency2",
                               xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_QUOTED_CURR2_PART2)),
                             xmlelement("quoteBasis", cm.T_QBASIS_PART2)),
                           xmlelement("rate", replace(cm.T_RATE_PART2, '','', ''.'')))
/*,
                         CASE WHEN RSP_COMMON.EraseChr1(cm.T_SETTLCURR_PART2) IS NOT NULL
                              THEN xmlelement("nonDeliverableSettlement",
                                     xmlelement("settlementCurrency",
                                       xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_SETTLCURR_PART2)))
                         END
*/
))
                 from IR_CM021 cm
                where cm.T_INTERNALMESSAGEID = gen.t_internalmessageid)';
   end GetBlockFxSwap;

   function GetBlockFxOption return clob as
   begin
     Return '(select xmlelement("fxOption",
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       xmlelement("buyerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.t_Buyer) as "href")),
                       xmlelement("sellerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.t_Seller) as "href")),
                       CASE WHEN cm.t_OptExer = ''A''
                            THEN xmlelement("americanExercise",
                                   xmlelement("commencementDate",
                                     xmlelement("adjustableDate",
                                       xmlelement("unadjustedDate", cm.t_opendate))),
                                   xmlelement("expiryDate", cm.t_expirydate))
                            WHEN cm.t_OptExer = ''E''
                            THEN xmlelement("europeanExercise",
                                   xmlelement("expiryDate", cm.t_expirydate))
                       END,
                       xmlelement("putCurrencyAmount",
                         xmlelement("currency",
                           xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_putcurr)),
                         xmlelement("amount", replace(ltrim(to_char(cm.t_PutCurrAmount, ''99999999999999999990D00''), '' ''), '','', ''.''))),
                       xmlelement("callCurrencyAmount",
                         xmlelement("currency",
                           xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_CallCurr)),
                         xmlelement("amount", replace(ltrim(to_char(cm.t_CallCurrAmount, ''99999999999999999990D00''), '' ''), '','', ''.''))),
                       xmlelement("soldAs", RSP_COMMON.EraseChr1(cm.t_soldAs)),
                       xmlelement("strike",
                         xmlelement("rate", replace(cm.t_OptRate, '','', ''.'')),
                         xmlelement("strikeQuoteBasis", RSP_COMMON.EraseChr1(cm.t_StrikeBasis))),
                       xmlelement("premium",
                         xmlelement("payerPartyReference",
                           xmlattributes(RSP_COMMON.EraseChr1(cm.t_PremiumPayerParty) as "href")),
                         xmlelement("receiverPartyReference",
                           xmlattributes(RSP_COMMON.EraseChr1(cm.t_PremiumReceiverParty) as "href")),
                         xmlelement("paymentAmount",
                           xmlelement("currency",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_PremiumCurrency)),
                           xmlelement("amount", replace(ltrim(to_char(cm.t_PremiumAmount, ''99999999999999999990D00''), '' ''), '','', ''.''))))
/*,
                       xmlelement("cashSettlement",
                         xmlelement("settlementCurrency",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_SettlCurrency)))
*/
)
                 from IR_CM023 cm
                where cm.t_internalmessageid = gen.t_internalmessageid)';
   end GetBlockFxOption;

   function GetBlockFxSingleLeg return clob as
   begin
     Return '(select xmlelement("fxSingleLeg",
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       xmlelement("exchangedCurrency1",
                         xmlelement("payerPartyReference",
                           xmlattributes(RSP_COMMON.EraseChr1(cm.t_PayerPartyReference1) as "href")),
                         xmlelement("receiverPartyReference",
                           xmlattributes(RSP_COMMON.EraseChr1(cm.t_ReceiverPartyReference1) as "href")),
                         xmlelement("paymentAmount",
                           xmlelement("currency",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_Currency1)),
                           xmlelement("amount", replace(ltrim(to_char(cm.t_AmountCurr1, ''99999999999999999990D00''), '' ''), '','', ''.'')))),
                       xmlelement("exchangedCurrency2",
                         xmlelement("payerPartyReference",
                           xmlattributes(RSP_COMMON.EraseChr1(cm.t_PayerPartyReference2) as "href")),
                         xmlelement("receiverPartyReference",
                           xmlattributes(RSP_COMMON.EraseChr1(cm.t_ReceiverPartyReference2) as "href")),
                         xmlelement("paymentAmount",
                           xmlelement("currency",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_Currency2)),
                           xmlelement("amount", replace(ltrim(to_char(cm.t_AmountCurr2, ''99999999999999999990D00''), '' ''), '','', ''.'')))),
                       xmlelement("dealtCurrency", RSP_COMMON.EraseChr1(cm.t_TradeCurr)),
                       xmlelement("valueDate", cm.t_ValDate),
                       xmlelement("exchangeRate",
                         xmlelement("quotedCurrencyPair",
                           xmlelement("currency1",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_QuotedCurrency1)),
                           xmlelement("currency2",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_QuotedCurrency2)),
                           xmlelement("quoteBasis", RSP_COMMON.EraseChr1(cm.t_QBasis))),
                         xmlelement("rate", replace(cm.t_Rate, '','', ''.'')))
/*,
                       CASE WHEN RSP_COMMON.EraseChr1(cm.t_SettlCurr) IS NOT NULL
                            THEN xmlelement("nonDeliverableSettlement",
                                   xmlelement("settlementCurrency",
                                     xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_SettlCurr)))
                       END
*/
)
                from IR_CM022 cm
               where cm.t_internalmessageid = gen.t_internalmessageid)';
   end GetBlockFxSingleLeg;

   function GetBlockSwap return clob as
   begin
     Return '(select xmlelement("swap",
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       xmlelement("swapStream",
                         xmlelement("payerPartyReference",
                           xmlattributes(RSP_COMMON.EraseChr1(cm.T_PAYERPARTY_O1) as "href")),
                         xmlelement("receiverPartyReference",
                           xmlattributes(RSP_COMMON.EraseChr1(cm.T_RECEIVERPARTY_O1) as "href")),
                         xmlelement("calculationPeriodDates",
                           xmlelement("effectiveDate",
                             xmlelement("unadjustedDate", cm.T_EFFECTIVEDATE)),
                           xmlelement("terminationDate",
                             xmlelement("unadjustedDate", cm.T_TERMINATIONDATE))),
                         xmlelement("paymentDates",
                           xmlelement("paymentFrequency",
                             xmlelement("periodMultiplier", cm.T_PERIODMULTIPLIER_O1),
                             xmlelement("period", RSP_COMMON.EraseChr1(cm.T_PERIODTYPE_O1)))),
                         xmlelement("calculationPeriodAmount",
                           xmlelement("calculation",
                             xmlelement("notionalSchedule",
                               xmlelement("notionalStepSchedule",
                                 xmlelement("initialValue", cm.T_NOMVALUE_O1),
                                 xmlelement("currency",
                                   xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_NOMCURRENCY_O1)))),
                             CASE WHEN cm.T_FixRate_O1 = ''Y''
                                  THEN xmlelement("fixedRateSchedule",
                                         xmlelement("initialValue", replace(rtrim(ltrim(to_char(cm.T_RATE_O1, ''99999999999999999990D0000000''), '' ''), ''0''), '','', ''.'')))
                                  ELSE xmlelement("floatingRateCalculation",
                                         xmlelement("floatingRateIndex", RSP_COMMON.EraseChr1(cm.T_FLOATINGRATEINDEX_O1)),
                                         xmlelement("indexTenor",
                                           xmlelement("periodMultiplier", cm.T_FLOATPERIODMULTIPLIER_O1),
                                           xmlelement("period", cm.T_FLOATPERIODTYPE_O1)),
                                         xmlelement("spreadSchedule",
                                           xmlelement("initialValue", replace(rtrim(ltrim(to_char(cm.T_SPREAD_O1, ''99999999999999999990D0000000''), '' ''), '' ''), '','', ''.''))),
                                         xmlelement("initialRate", replace(rtrim(ltrim(to_char(cm.T_FLOATINGRATE_O1, ''99999999999999999990D0000000''), '' ''), '' ''), '','', ''.'')))
                             END,
                             xmlelement("dayCountFraction", decode(cm.T_FixRate_O1, ''Y'', cm.T_DAYCOUNTFRACTION_O2, cm.T_DAYCOUNTFRACTION_O1)))),
                           CASE WHEN nvl(cm.T_INITIALEXCHANGE_O1, chr(0)) <> chr(0) OR
                                     nvl(cm.T_FINALEXCHANGE_O1, chr(0)) <> chr(0) OR
                                     nvl(cm.T_INTERMEDIATEEXCHANGE_O1, chr(0)) <> chr(0)
                                THEN xmlelement("principalExchanges",
                                       xmlelement("initialExchange", decode(nvl(cm.T_INITIALEXCHANGE_O1, chr(0)), chr(0), ''N'', cm.T_INITIALEXCHANGE_O1)),
                                       xmlelement("finalExchange", decode(nvl(cm.T_FINALEXCHANGE_O1, chr(0)), chr(0), ''N'', cm.T_FINALEXCHANGE_O1)),
                                       xmlelement("intermediateExchange", decode(nvl(cm.T_INTERMEDIATEEXCHANGE_O1, chr(0)), chr(0), ''N'', cm.T_INTERMEDIATEEXCHANGE_O1)))
                           END),
                       xmlelement("swapStream",
                         xmlelement("payerPartyReference",
                           xmlattributes(RSP_COMMON.EraseChr1(cm.T_PAYERPARTY_O2) as "href")),
                         xmlelement("receiverPartyReference",
                           xmlattributes(RSP_COMMON.EraseChr1(cm.T_RECEIVERPARTY_O2) as "href")),
                         xmlelement("calculationPeriodDates",
                           xmlelement("effectiveDate",
                             xmlelement("unadjustedDate", cm.T_EFFECTIVEDATE_O2)),
                           xmlelement("terminationDate",
                             xmlelement("unadjustedDate", cm.T_TERMINATIONDATE_O2))),
                         xmlelement("paymentDates",
                           xmlelement("paymentFrequency",
                             xmlelement("periodMultiplier", cm.T_PERIODMULTIPLIER_O2),
                             xmlelement("period", RSP_COMMON.EraseChr1(cm.T_PERIODTYPE_O2)))),
                         xmlelement("calculationPeriodAmount",
                           xmlelement("calculation",
                             xmlelement("notionalSchedule",
                               xmlelement("notionalStepSchedule",
                                 xmlelement("initialValue", cm.T_NOMVALUE_O2),
                                 xmlelement("currency",
                                   xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_NOMCURRENCY_O2)))),
                             CASE WHEN cm.T_FixRate_O2 = ''Y''
                                  THEN xmlelement("fixedRateSchedule",
                                         xmlelement("initialValue", replace(rtrim(ltrim(to_char(cm.T_RATE_O2, ''99999999999999999990D0000000''), '' ''), '' ''), '','', ''.'')))
                                  ELSE xmlelement("floatingRateCalculation",
                                         xmlelement("floatingRateIndex", RSP_COMMON.EraseChr1(cm.T_FLOATINGRATEINDEX_O2)),
                                         xmlelement("indexTenor",
                                           xmlelement("periodMultiplier", cm.T_FLOATPERIODMULTIPLIER_O2),
                                           xmlelement("period", cm.T_FLOATPERIODTYPE_O2)),
                                         xmlelement("spreadSchedule",
                                           xmlelement("initialValue", replace(rtrim(ltrim(to_char(cm.T_SPREAD_O2, ''99999999999999999990D0000000''), '' ''), '' ''), '','', ''.''))),
                                         xmlelement("initialRate", replace(rtrim(ltrim(to_char(cm.T_FLOATINGRATE_O2, ''99999999999999999990D0000000''), '' ''), '' ''), '','', ''.'')))
                             END,
                             xmlelement("dayCountFraction", decode(cm.T_FixRate_O2, ''Y'', cm.T_DAYCOUNTFRACTION_O1, cm.T_DAYCOUNTFRACTION_O2)))),
                           CASE WHEN nvl(cm.T_INITIALEXCHANGE_O2, chr(0)) <> chr(0) OR
                                     nvl(cm.T_FINALEXCHANGE_O2, chr(0)) <> chr(0) OR
                                     nvl(cm.T_INTERMEDIATEEXCHANGE_O2, chr(0)) <> chr(0)
                                THEN xmlelement("principalExchanges",
                                       xmlelement("initialExchange", decode(nvl(cm.T_INITIALEXCHANGE_O2, chr(0)), chr(0), ''N'', cm.T_INITIALEXCHANGE_O2)),
                                       xmlelement("finalExchange", decode(nvl(cm.T_FINALEXCHANGE_O2, chr(0)), chr(0), ''N'', cm.T_FINALEXCHANGE_O2)),
                                       xmlelement("intermediateExchange", decode(nvl(cm.T_INTERMEDIATEEXCHANGE_O2, chr(0)), chr(0), ''N'', cm.T_INTERMEDIATEEXCHANGE_O2)))
                           END))
                 from IR_CM032 cm
                where cm.T_INTERNALMESSAGEID = gen.t_internalmessageid)';
   end GetBlockSwap;

   function GetBlockRepo return clob as
   begin
     Return '(select xmlelement("fpmlext:repo",
                       xmlattributes(''nsdext:RepoNsd'' as "xsi:type"),
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       CASE WHEN cm.t_RateType = ''FX''
                            THEN xmlelement("fpmlext:fixedRateSchedule",
                                   xmlelement("initialValue", cm.t_initialRate))
                       END,
                       CASE WHEN RSP_COMMON.EraseChr1(cm.t_DayCountCalc) IS NOT NULL
                            THEN xmlelement("fpmlext:dayCountFraction",
                                   xmlattributes(''http://www.fpml.org/coding-scheme/day-count-fraction'' as "dayCountFractionScheme"), cm.t_DayCountCalc)
                       END,
                       CASE WHEN RSP_COMMON.EraseChr1(cm.t_durationType) IS NOT NULL
                            THEN xmlelement("fpmlext:duration", cm.t_durationType)
                       END,
                       '||RSP_REPOSITORY.GetBlockSpotLeg||',
                       '||RSP_REPOSITORY.GetBlockForwardLeg||',
                       CASE WHEN cm.t_SignBonds = ''X''
                            THEN xmlelement("bond",
                                     xmlattributes(RSP_COMMON.EraseChr1(ins.t_InstrumentCode) as "id"),
                                   xmlelement("instrumentId",
                                     xmlattributes(''https://www.nsd.ru/ru/info/good/sec_ru/'' as "instrumentIdScheme"), RSP_COMMON.EraseChr1(ins.t_InstrumentCode)))
                            WHEN cm.t_SignBond = ''X''
                            THEN xmlelement("equity",
                                     xmlattributes(RSP_COMMON.EraseChr1(ins.t_InstrumentCode) as "id"),
                                   xmlelement("instrumentId",
                                     xmlattributes(''https://www.nsd.ru/ru/info/good/sec_ru/'' as "instrumentIdScheme"), RSP_COMMON.EraseChr1(ins.t_InstrumentCode)))
                       END)
                from IR_CM041 cm,
                     IR_INSTRUMENTS ins
               where cm.t_internalmessageid = gen.t_internalmessageid
                 and ins.t_internalmessageid = cm.t_internalmessageid)';
   end GetBlockRepo;

   function GetBlockBondSimpleTransaction return clob as
   begin
     Return '(select xmlelement("nsdext:bondSimpleTransaction",
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       xmlelement("buyerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_BUYER) as "href")),
                       xmlelement("sellerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_SELLER) as "href")),
                       xmlelement("fpmlext:notionalAmount",
                         xmlelement("currency",
                           xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_NOMINALCURRENCY)),
                         xmlelement("amount", replace(ltrim(to_char(cm.T_NOMINALAMOUNT, ''99999999999999999990D00''), '' ''), '','', ''.''))),
                       xmlelement("fpmlext:price",
                         xmlelement("fpmlext:cleanPrice", replace(ltrim(to_char(cm.T_CLEANPRICE, ''99999999999999999990D00''), '' ''), '','', ''.'')),
                         xmlelement("fpmlext:accruals", replace(ltrim(to_char(cm.T_ACCRUALS, ''99999999999999999990D00''), '' ''), '','', ''.'')),
                         xmlelement("fpmlext:dirtyPrice", replace(ltrim(to_char(cm.T_DIRTYPRICE, ''99999999999999999990D00''), '' ''), '','', ''.''))),
                       xmlelement("fpmlext:bond",
                         xmlattributes(RSP_COMMON.EraseChr1(ins.t_InstrumentCode) as "id"),
                         xmlelement("instrumentId",
                           xmlattributes(''https://www.nsd.ru/ru/info/good/sec_ru/'' as "instrumentIdScheme"), RSP_COMMON.EraseChr1(ins.t_InstrumentCode)),
                         xmlelement("description", ins.t_InstrumentName)),
                       CASE WHEN RSP_COMMON.EraseChr1(cm.T_PERIOD) IS NOT NULL
                            THEN xmlelement("nsdext:term",
                                   xmlelement("periodMultiplier", cm.T_PERIODMULTIPLIER),
                                   xmlelement("period", cm.T_PERIOD))
                       END,
                       xmlelement("nsdext:deliveryMethod", cm.T_DELIVERYMETHOD),
                       xmlelement("nsdext:settlementDate",
                         xmlelement("adjustableDate",
                           xmlelement("unadjustedDate", cm.T_SETTLMENTDATE))),
                       xmlelement("nsdext:deliveryDate",
                         xmlelement("adjustableDate",
                           xmlelement("unadjustedDate", cm.T_DELIVERYDATE))),
                       xmlelement("nsdext:numberOfUnits", replace(ltrim(to_char(cm.T_NUMBEROFUNITS, ''99999999999999999990D00''), '' ''), '','', ''.'')))
                from IR_CM042 cm,
                     IR_INSTRUMENTS ins
               where cm.t_internalmessageid = gen.t_internalmessageid
                 and ins.t_internalmessageid = cm.t_internalmessageid)';
   end GetBlockBondSimpleTransaction;

   function GetBlockBondForward return clob as
   begin
     Return '(select xmlelement("nsdext:bondForward",
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       xmlelement("buyerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_BUYER) as "href")),
                       xmlelement("sellerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_SELLER) as "href")),
                       xmlelement("nsdext:underlyer",
                         xmlelement("singleUnderlyer",
                           CASE WHEN cm.T_UNDERLYERTYPE = ''Bond''
                                THEN xmlelement("bond",
                                       xmlattributes(ins.t_InstrumentCode as "id"),
                                       xmlelement("instrumentId",
                                         xmlattributes(''https://www.nsd.ru/ru/info/good/sec_ru/'' as "instrumentIdScheme"), RSP_COMMON.EraseChr1(ins.t_InstrumentCode)),
                                       xmlelement("description", ins.t_InstrumentName))
                                WHEN cm.T_UNDERLYERTYPE = ''Index''
                                THEN xmlelement("index",
                                       xmlattributes(ins.t_InstrumentCode as "id"),
                                       xmlelement("instrumentId",
                                         xmlattributes(''https://www.nsd.ru/ru/info/good/sec_ru/'' as "instrumentIdScheme"), RSP_COMMON.EraseChr1(ins.t_InstrumentCode)),
                                       xmlelement("description", ins.t_InstrumentName))
                           END,
                           xmlelement("openUnits", cm.T_OPENUNITS))),
                       xmlelement("nsdext:notionalAmount",
                         xmlelement("currency",
                           xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_AMOUNTCURRENCY)),
                         xmlelement("amount", replace(ltrim(to_char(cm.T_AMOUNT, ''99999999999999999990D0000000''), '' ''), '','', ''.''))),
                       xmlelement("nsdext:settlementDate",
                         xmlelement("adjustableDate",
                           xmlelement("unadjustedDate", cm.T_SETTLMENTDATE))),
                       CASE WHEN RSP_COMMON.EraseChr1(cm.T_SETTLCURRENCY) IS NOT NULL
                            THEN xmlelement("nsdext:settlementCurrency",
                                   xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), cm.T_SETTLCURRENCY)
                       END,
                       CASE WHEN RSP_COMMON.EraseChr1(cm.T_PRICESOURCE) IS NOT NULL
                            THEN xmlelement("nsdext:settlementPriceSource", cm.T_PRICESOURCE)
                       END,
                       CASE WHEN cm.T_PARTIALDELIVERY = ''X''
                            THEN xmlelement("nsdext:partialDelivery", ''true'')
                       END,
                       xmlelement("nsdext:forwardPrice",
                          xmlelement("nsdext:forwardPricePerBond",
                            xmlelement("currency",
                              xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_PRICECURRENCY)),
                            xmlelement("amount", replace(ltrim(to_char(cm.T_PRICE, ''99999999999999999990D0000000''), '' ''), '','', ''.'')))))
                from IR_CM043 cm,
                     IR_INSTRUMENTS ins
               where cm.t_internalmessageid = gen.t_internalmessageid
                 and ins.t_internalmessageid = cm.t_internalmessageid)';
   end GetBlockBondForward;

   function GetBlockBondOption return clob as
   begin
     Return '(select xmlelement("bondOption",
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       xmlelement("buyerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_BUYER) as "href")),
                       xmlelement("sellerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_SELLER) as "href")),
                       xmlelement("optionType", cm.T_OPTIONTYPE),
                       xmlelement("premium",
                         xmlelement("payerPartyReference",
                           xmlattributes(cm.T_PREMIUMPAYER as "href")),
                         xmlelement("receiverPartyReference",
                           xmlattributes(cm.T_PREMIUMRECEIVER as "href")),
                         xmlelement("paymentAmount",
                           xmlelement("currency",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_PREMIUMCURRENCY)),
                           xmlelement("amount", replace(ltrim(to_char(cm.T_PREMIUMAMOUNT, ''99999999999999999990D00''), '' ''), '','', ''.'')))),
                       CASE WHEN cm.T_OPTIONSTYLE = ''A''
                            THEN xmlelement("americanExercise",
                                   xmlelement("commencementDate",
                                     xmlelement("adjustableDate",
                                       xmlelement("unadjustedDate", cm.T_COMMENCEMENTDATE))),
                                   xmlelement("expirationDate",
                                     xmlelement("adjustableDate",
                                       xmlelement("unadjustedDate", cm.T_EXPIRATIONDATE))))
                            WHEN cm.T_OPTIONSTYLE = ''E''
                            THEN xmlelement("europeanExercise",
                                   xmlelement("expirationDate",
                                     xmlelement("adjustableDate",
                                       xmlelement("unadjustedDate", cm.T_EXPIRATIONDATE))))
                       END,
                       xmlelement("notionalAmount",
                         xmlelement("currency",
                           xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_NOMINALCURRENCY)),
                         xmlelement("amount", replace(ltrim(to_char(cm.T_NOMINALAMOUNT, ''99999999999999999990D00''), '' ''), '','', ''.''))),
                       xmlelement("optionEntitlement", replace(ltrim(to_char(cm.T_OPTIONENTITLEMENT, ''99999999999999999990D0000000''), '' ''), '','', ''.'')),
                       CASE WHEN cm.T_NUMBEROFOPTIONS <> 0
                            THEN xmlelement("numberOfOptions", cm.T_NUMBEROFOPTIONS)
                       END,
                       xmlelement("settlementType", cm.T_SETTLEMENTTYPE),
                       xmlelement("strike",
                         xmlelement("price",
                           xmlelement("strikePrice", cm.T_STRIKE),
                           xmlelement("currency",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_STRIKECURRENCY)))),
                       xmlelement("bond",
                         xmlattributes(RSP_COMMON.EraseChr1(ins.t_InstrumentCode) as "id"),
                         xmlelement("instrumentId",
                           xmlattributes(''https://www.nsd.ru/ru/info/good/sec_ru/'' as "instrumentIdScheme"), RSP_COMMON.EraseChr1(ins.t_InstrumentCode)),
                         xmlelement("description", ins.t_InstrumentName)))
                from IR_CM044 cm,
                     IR_INSTRUMENTS ins
               where cm.t_internalmessageid = gen.t_internalmessageid
                 and ins.t_internalmessageid = cm.t_internalmessageid)';
   end GetBlockBondOption;

   function GetBlockBondBasketOption return clob as
   begin
     Return '(select xmlelement("nsdext:bondBasketOption",
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       xmlelement("buyerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_BUYER) as "href")),
                       xmlelement("sellerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_SELLER) as "href")),
                       xmlelement("optionType", cm.T_OPTIONTYPE),
                       xmlelement("premium",
                         xmlelement("payerPartyReference",
                           xmlattributes(cm.T_PREMIUMPAYER as "href")),
                         xmlelement("receiverPartyReference",
                           xmlattributes(cm.T_PREMIUMRECEIVER as "href")),
                         xmlelement("paymentAmount",
                           xmlelement("currency",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_PREMIUMCURRENCY)),
                           xmlelement("amount", replace(ltrim(to_char(cm.T_PREMIUMAMOUNT, ''99999999999999999990D00''), '' ''), '','', ''.'')))),
                       CASE WHEN cm.T_OPTIONSTYLE = ''A''
                            THEN xmlelement("americanExercise",
                                   xmlelement("commencementDate",
                                     xmlelement("adjustableDate",
                                       xmlelement("unadjustedDate", cm.T_COMMENCEMENTDATE))),
                                   xmlelement("expirationDate",
                                     xmlelement("adjustableDate",
                                       xmlelement("unadjustedDate", cm.T_EXPIRATIONDATE))))
                            WHEN cm.T_OPTIONSTYLE = ''E''
                            THEN xmlelement("europeanExercise",
                                   xmlelement("expirationDate",
                                     xmlelement("adjustableDate",
                                       xmlelement("unadjustedDate", cm.T_EXPIRATIONDATE))))
                       END,
                       xmlelement("notionalAmount",
                         xmlelement("currency",
                           xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_NOMINALCURRENCY)),
                         xmlelement("amount", replace(ltrim(to_char(cm.T_NOMINALAMOUNT, ''99999999999999999990D00''), '' ''), '','', ''.''))),
                       xmlelement("optionEntitlement", replace(ltrim(to_char(cm.T_OPTIONENTITLEMENT, ''99999999999999999990D0000000''), '' ''), '','', ''.'')),
                       CASE WHEN cm.T_NUMBEROFOPTIONS <> 0
                            THEN xmlelement("numberOfOptions", cm.T_NUMBEROFOPTIONS)
                       END,
                       xmlelement("settlementType", cm.T_SETTLEMENTTYPE),
                       xmlelement("nsdext:strike",
                         xmlelement("price",
                           xmlelement("strikePrice", cm.T_STRIKE),
                           xmlelement("currency",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_STRIKECURRENCY)))),
                       xmlelement("index",
                         xmlattributes(RSP_COMMON.EraseChr1(ins.t_InstrumentCode) as "id"),
                         xmlelement("instrumentId",
                           xmlattributes(''https://www.nsd.ru/ru/info/good/sec_ru/'' as "instrumentIdScheme"), RSP_COMMON.EraseChr1(ins.t_InstrumentCode)),
                         xmlelement("description", ins.t_InstrumentName)))
                from IR_CM045 cm,
                     IR_INSTRUMENTS ins
               where cm.t_internalmessageid = gen.t_internalmessageid
                 and ins.t_internalmessageid = cm.t_internalmessageid)';
   end GetBlockBondBasketOption;

   function GetBlockEquitySimplTransaction return clob as
   begin
     Return '(select xmlelement("nsdext:equitySimpleTransaction",
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       xmlelement("buyerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_BUYER) as "href")),
                       xmlelement("sellerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_SELLER) as "href")),
                       xmlelement("fpmlext:numberOfUnits", replace(ltrim(to_char(cm.T_NUMBEROFUNITS, ''99999999999999999990''), '' ''), '','', ''.'')),
                       xmlelement("fpmlext:unitPrice",
                         xmlelement("currency",
                           xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_PRICECURRENCY)),
                         xmlelement("amount", replace(ltrim(to_char(cm.T_PRICE, ''99999999999999999990D0000000''), '' ''), '','', ''.''))),
                       xmlelement("fpmlext:equity",
                         xmlattributes(RSP_COMMON.EraseChr1(ins.t_InstrumentCode) as "id"),
                         xmlelement("instrumentId",
                           xmlattributes(''https://www.nsd.ru/ru/info/good/sec_ru/'' as "instrumentIdScheme"), RSP_COMMON.EraseChr1(ins.t_InstrumentCode)),
                         xmlelement("description", ins.t_InstrumentName)),
                       CASE WHEN RSP_COMMON.EraseChr1(cm.T_PERIOD) IS NOT NULL
                            THEN xmlelement("nsdext:term",
                                   xmlelement("periodMultiplier", cm.T_PERIODMULTIPLIER),
                                   xmlelement("period", cm.T_PERIOD))
                       END,
                       xmlelement("nsdext:deliveryMethod", cm.T_DELIVERYMETHOD),
                       xmlelement("nsdext:settlementDate",
                         xmlelement("adjustableDate",
                           xmlelement("unadjustedDate", cm.T_SETTLMENTDATE))),
                       xmlelement("nsdext:deliveryDate",
                         xmlelement("adjustableDate",
                           xmlelement("unadjustedDate", cm.T_DELIVERYDATE))))
                from IR_CM046 cm,
                     IR_INSTRUMENTS ins
               where cm.t_internalmessageid = gen.t_internalmessageid
                 and ins.t_internalmessageid = cm.t_internalmessageid)';
   end GetBlockEquitySimplTransaction;

   function GetBlockEquityForward return clob as
   begin
     Return '(select xmlelement("equityForward",
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       xmlelement("buyerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_BUYER) as "href")),
                       xmlelement("sellerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_SELLER) as "href")),
                       xmlelement("underlyer",
                         xmlelement("singleUnderlyer",
                           xmlelement("equity",
                             xmlattributes(ins.t_InstrumentCode as "id"),
                               xmlelement("instrumentId",
                                 xmlattributes(''https://www.nsd.ru/ru/info/good/sec_ru/'' as "instrumentIdScheme"), RSP_COMMON.EraseChr1(ins.t_InstrumentCode)),
                               xmlelement("description", ins.t_InstrumentName)),
                           xmlelement("openUnits", cm.T_OPENUNITS))),
                       xmlelement("equityExercise",
                         xmlelement("equityEuropeanExercise",
                           xmlelement("expirationDate",
                             xmlelement("adjustableDate",
                               xmlelement("unadjustedDate", cm.T_SETTLMENTDATE)))),
                         CASE WHEN RSP_COMMON.EraseChr1(cm.T_SETTLCURRENCY) IS NOT NULL
                              THEN xmlelement("settlementCurrency",
                                     xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), cm.T_SETTLCURRENCY)
                         END,
                         xmlelement("settlementType", cm.T_SETTLEMENTTYPE)),
                       xmlelement("forwardPrice",
                          xmlelement("currency",
                            xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_PRICECURRENCY)),
                          xmlelement("amount", replace(ltrim(to_char(cm.T_PRICE, ''99999999999999999990D0000000''), '' ''), '','', ''.''))))
                from IR_CM047 cm,
                     IR_INSTRUMENTS ins
               where cm.t_internalmessageid = gen.t_internalmessageid
                 and ins.t_internalmessageid = cm.t_internalmessageid)';
   end GetBlockEquityForward;

   function GetBlockEquityOption return clob as
   begin
     Return '(select xmlelement("equityOption",
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       xmlelement("buyerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_BUYER) as "href")),
                       xmlelement("sellerPartyReference",
                         xmlattributes(RSP_COMMON.EraseChr1(cm.T_SELLER) as "href")),
                       CASE WHEN RSP_COMMON.EraseChr1(cm.T_OPTIONTYPE) IS NOT NULL
                            THEN xmlelement("optionType", cm.T_OPTIONTYPE)
                       END,
                       xmlelement("underlyer",
                         xmlelement("singleUnderlyer",
                           xmlelement("equity",
                             xmlattributes(RSP_COMMON.EraseChr1(ins.t_InstrumentCode) as "id"),
                             xmlelement("instrumentId",
                               xmlattributes(''https://www.nsd.ru/ru/info/good/sec_ru/'' as "instrumentIdScheme"), RSP_COMMON.EraseChr1(ins.t_InstrumentCode)),
                             xmlelement("description", ins.t_InstrumentName)),
                           xmlelement("openUnits", cm.T_OPENUNIT))),
                       xmlelement("equityExercise",
                         CASE WHEN cm.T_OPTIONSTYLE = ''A''
                              THEN xmlelement("equityAmericanExercise",
                                     xmlelement("commencementDate",
                                       xmlelement("adjustableDate",
                                         xmlelement("unadjustedDate", cm.T_COMMENCEMENTDATE))),
                                     xmlelement("expirationDate",
                                       xmlelement("adjustableDate",
                                         xmlelement("unadjustedDate", cm.T_EXPIRATIONDATE))))
                              WHEN cm.T_OPTIONSTYLE = ''E''
                              THEN xmlelement("equityEuropeanExercise",
                                     xmlelement("expirationDate",
                                       xmlelement("adjustableDate",
                                         xmlelement("unadjustedDate", cm.T_EXPIRATIONDATE))))
                         END,
                         xmlelement("settlementCurrency",
                           xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_SETTLEMENTCURRENCY)),
                         xmlelement("settlementType", cm.T_SETTLEMENTTYPE)),
                       xmlelement("strike",
                         xmlelement("strikePrice", cm.T_STRIKE),
                         xmlelement("currency",
                           xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_STRIKECURRENCY))),
                       xmlelement("numberOfOptions", cm.T_NUMBEROFOPTIONS),
                       xmlelement("optionEntitlement", cm.T_OPTIONENTITLEMENT),
                       xmlelement("equityPremium",
                         xmlelement("payerPartyReference",
                           xmlattributes(cm.T_PREMIUMPAYER as "href")),
                         xmlelement("receiverPartyReference",
                           xmlattributes(cm.T_PREMIUMRECEIVER as "href")),
                         xmlelement("paymentAmount",
                           xmlelement("currency",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_PREMIUMCURRENCY)),
                           xmlelement("amount", replace(ltrim(to_char(cm.T_PREMIUMAMOUNT, ''99999999999999999990D00''), '' ''), '','', ''.'')))))
                from IR_CM048 cm,
                     IR_INSTRUMENTS ins
               where cm.t_internalmessageid = gen.t_internalmessageid
                 and ins.t_internalmessageid = cm.t_internalmessageid)';
   end GetBlockEquityOption;

   function GetBlockCommodityForward return clob as
   begin
     Return '(select xmlelement("commodityForward",
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       '||RSP_REPOSITORY.GetBlockFixedLeg051||',
                       CASE WHEN cm.t_ExType = ''P''
                            THEN '||RSP_REPOSITORY.GetBlockForwardPhysicalLeg||'
                            ELSE '||RSP_REPOSITORY.GetBlockFloatingForwardLeg||'
                       END)
                from IR_CM051 cm
               where cm.t_internalmessageid = gen.t_internalmessageid)';
   end GetBlockCommodityForward;

   function GetBlockCommoditySwap return clob as
   begin
     Return '(select xmlelement("commoditySwap",
                       xmlelement("productType",
                         xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
                       xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
                       xmlelement("effectiveDate",
                         xmlelement("adjustableDate",
                           xmlelement("unadjustedDate", cm.T_EFFECTIVEDATE))),
                       xmlelement("terminationDate",
                         xmlelement("adjustableDate",
                           xmlelement("unadjustedDate", cm.T_TERMINATIONDATE))),
                       xmlelement("settlementCurrency",
                         xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_SETTLEMENTCURRENCY)),
                       CASE WHEN gen.t_ProductType = ''Commodity:Swap:Physical''
                                 AND cm.T_STRUCTURETYPE = ''1''
                            THEN '||RSP_REPOSITORY.GetBlockFixedLeg053(p_Number => 1)||'
                       END,
                       CASE WHEN gen.t_ProductType = ''Commodity:Swap:Physical''
                                 AND cm.T_STRUCTURETYPE = ''1''
                            THEN '||RSP_REPOSITORY.GetBlockSwapPhysicalLeg||'
                       END,
                       CASE WHEN gen.t_ProductType = ''Commodity:Swap:Physical''
                                 AND cm.T_STRUCTURETYPE = ''1''
                            THEN '||RSP_REPOSITORY.GetBlockFixedLeg053(p_Number => 2)||'
                       END,
                       CASE WHEN gen.t_ProductType = ''Commodity:Swap:Physical''
                                 AND cm.T_STRUCTURETYPE = ''2''
                            THEN '||RSP_REPOSITORY.GetBlockSwapPhysicalLeg||'
                       END,
                       CASE WHEN gen.t_ProductType = ''Commodity:Swap:Physical''
                                 AND cm.T_STRUCTURETYPE = ''2''
                            THEN '||RSP_REPOSITORY.GetBlockFixedLeg053(p_Number => 1)||'
                       END,
                       CASE WHEN gen.t_ProductType = ''Commodity:Swap:Physical''
                                 AND cm.T_STRUCTURETYPE = ''2''
                            THEN '||RSP_REPOSITORY.GetBlockFixedLeg053(p_Number => 2)||'
                       END,
                       CASE WHEN gen.t_ProductType = ''Commodity:Swap:Cash''
                            THEN '||RSP_REPOSITORY.GetBlockFixedLeg053(p_Number => 1)||'
                       END,
                       CASE WHEN gen.t_ProductType = ''Commodity:Swap:Cash''
                            THEN '||RSP_REPOSITORY.GetBlockFixedLeg053(p_Number => 2)||'
                       END)
                from IR_CM053 cm
               where cm.t_internalmessageid = gen.t_internalmessageid)';
   end GetBlockCommoditySwap;

   function GetBlockSpotLeg return clob as
   begin
     Return 'xmlelement("fpmlext:spotLeg",
               xmlattributes(''nsdext:RepoTransactionLegNsd'' as "xsi:type"),
               xmlelement("buyerPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.t_BuyerPart1) as "href")),
               xmlelement("sellerPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.t_SellerPart1) as "href")),
               xmlelement("fpmlext:settlementDate",
                 xmlelement("adjustableDate",
                   xmlelement("unadjustedDate", cm.t_Part1SettlDate))),
               xmlelement("settlementAmount",
                 xmlelement("currency",
                   xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_Part1Curr)),
                 xmlelement("amount", replace(ltrim(to_char(cm.t_Part1Amount, ''99999999999999999990D00''), '' ''), '','', ''.''))),
               '||RSP_REPOSITORY.GetBlockCollateral041||',
               CASE WHEN cm.t_durationType <> ''Open''
                    THEN xmlelement("nsdext:deliveryMethod", RSP_COMMON.EraseChr1(cm.t_deliveryMethodPart1))
               END,
               CASE WHEN nvl(trunc(cm.t_DeliveryDatePart1), to_date(''01.01.0001'', ''DD.MM.YYYY'')) <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                    THEN '||RSP_REPOSITORY.GetBlockDeliveryDate||'
               END)';
   end GetBlockSpotLeg;

   function GetBlockFixedLeg051 return clob as
   begin
     Return 'xmlelement("fixedLeg",
               xmlelement("payerPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.T_PAYERPARTYREF_FXLEG) as "href")),
               xmlelement("receiverPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.T_RECEIVERPARTYREF_FXLEG) as "href")),
               xmlelement("fixedPrice",
                 xmlelement("price", cm.T_PRICE),
                 xmlelement("priceCurrency",
                   xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_PRICECURRENCY)),
                 xmlelement("priceUnit",
                   xmlattributes(''http://www.fpml.org/coding-scheme/price-quote-units'' as "quantityUnitScheme"), cm.T_PRICEUNIT)),
               xmlelement("totalPrice",
                 xmlelement("currency",
                   xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_AMOUNTCURRENCY)),
                 xmlelement("amount", replace(ltrim(to_char(cm.T_AMOUNT, ''99999999999999999990D00''), '' ''), '','', ''.''))),
               xmlelement("paymentDates",
                 xmlelement("adjustableDates",
                   xmlelement("unadjustedDate", cm.T_PAYMENTDATE_FXLEG))))';
   end GetBlockFixedLeg051;

   function GetBlockFixedLeg053(p_Number in integer) return clob as
   begin
     Return 'xmlelement("fixedLeg",
               xmlelement("payerPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.T_PAYERPARTYREF_LEG'||p_Number||') as "href")),
               xmlelement("receiverPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.T_RECEIVERPARTYREF_LEG'||p_Number||') as "href")),
               xmlelement("calculationDates",
                 xmlelement("unadjustedDate", cm.T_PAYMENTDATE_LEG'||p_Number||')),
/*
               xmlelement("calculationPeriodsSchedule",
                 xmlelement("periodMultiplier", cm.T_PERIODMULTIPLIER_LEG'||p_Number||'),
                 xmlelement("period", cm.T_PERIOD_LEG'||p_Number||')),
*/
               xmlelement("totalPrice",
                 xmlelement("currency",
                   xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_AMOUNTCURRENCY_LEG'||p_Number||')),
                 xmlelement("amount", replace(ltrim(to_char(cm.T_AMOUNT_LEG'||p_Number||', ''99999999999999999990D00''), '' ''), '','', ''.''))),
               xmlelement("totalNotionalQuantity", cm.T_QUANTITY_LEG'||p_Number||'),
               xmlelement("paymentDates",
                 xmlelement("adjustableDates",
                   xmlelement("unadjustedDate", cm.T_PAYMENTDATE_LEG'||p_Number||'))))';
   end GetBlockFixedLeg053;

   function GetBlockForwardPhysicalLeg return clob as
   begin
     Return 'xmlelement("nsdext:commodityForwardPhysicalLeg",
               xmlelement("payerPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.T_PAYERPARTYREFERENCE) as "href")),
               xmlelement("receiverPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.T_RECEIVERPARTYREFERENCE) as "href")),
               xmlelement("nsdext:commodity",
                 xmlattributes(''nsdext:CommodityBasket'' as "xsi:type"),
                 xmlelement("instrumentId",
                   xmlattributes(cm.T_INSTRUMENTID as "instrumentIdScheme"), cm.T_INSTRUMENTID),
                 xmlelement("description", cm.T_COMMODITY),
                 xmlelement("unit",
                   xmlattributes(''http://www.fpml.org/coding-scheme/price-quote-units''as "quantityUnitScheme"), cm.T_COMUNIT),
                 xmlelement("currency",
                   xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_COMPRICECURRENCY))),
               xmlelement("nsdext:deliveryPeriods",
                 xmlelement("periods",
                   xmlelement("unadjustedDate", cm.T_DELIVERYDATE))),
               xmlelement("nsdext:deliveryQuantity",
                 xmlelement("totalPhysicalQuantity",
                   xmlelement("quantityUnit",
                     xmlattributes(''http://www.fpml.org/coding-scheme/price-quote-units'' as "quantityUnitScheme"), cm.T_QUANTITYUNIT),
                   xmlelement("quantity", cm.T_QUANTITY))))';
   end GetBlockForwardPhysicalLeg;

   function GetBlockFloatingForwardLeg return clob as
   begin
     Return 'xmlelement("nsdext:floatingForwardLeg",
               xmlelement("payerPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.T_PAYERPARTYREFERENCE) as "href")),
               xmlelement("receiverPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.T_RECEIVERPARTYREFERENCE) as "href")),
               xmlelement("nsdext:commodity",
                 xmlattributes(''nsdext:CommodityBasket'' as "xsi:type"),
                 xmlelement("instrumentId",
                   xmlattributes(cm.T_INSTRUMENTID as "instrumentIdScheme"), cm.T_INSTRUMENTID),
                 xmlelement("description", cm.T_COMMODITY),
                 xmlelement("unit",
                   xmlattributes(''http://www.fpml.org/coding-scheme/price-quote-units''as "quantityUnitScheme"), cm.T_COMUNIT),
                 xmlelement("currency",
                   xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_COMPRICECURRENCY))),
               xmlelement("nsdext:calculation",
                 xmlelement("pricingDates", null)),
               xmlelement("nsdext:totalNotionalQuantity", cm.T_QUANTITY),
               xmlelement("nsdext:paymentDates",
                 xmlelement("adjustableDates",
                   xmlelement("unadjustedDate", cm.T_PAYMENTDATE))))';
   end GetBlockFloatingForwardLeg;

   function GetBlockSwapPhysicalLeg return clob as
   begin
     Return 'xmlelement("nsdext:commoditySwapPhysicalLeg",
               xmlelement("nsdext:commodity",
                 xmlattributes(''nsdext:CommodityBasket'' as "xsi:type"),
                 xmlelement("instrumentId",
                   xmlattributes(cm.T_INSTRUMENTID as "instrumentIdScheme"), cm.T_INSTRUMENTID),
                 xmlelement("description", cm.T_COMMODITY),
                 xmlelement("unit",
                   xmlattributes(''http://www.fpml.org/coding-scheme/price-quote-units''as "quantityUnitScheme"), cm.T_PRICEUNIT),
                 xmlelement("currency",
                   xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_PRICECURRENCY))),
               xmlelement("payerPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.T_PAYERPARTYREFERENCE) as "href")),
               xmlelement("receiverPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.T_RECEIVERPARTYREFERENCE) as "href")),
               xmlelement("nsdext:deliveryPeriods",
                 xmlelement("periods",
                   xmlelement("unadjustedDate", cm.T_DELIVERYDATE))),
               xmlelement("nsdext:deliveryQuantity",
                 xmlelement("totalPhysicalQuantity",
                   xmlelement("quantityUnit",
                     xmlattributes(''http://www.fpml.org/coding-scheme/price-quote-units'' as "quantityUnitScheme"), cm.T_QUANTITYUNIT),
                   xmlelement("quantity", cm.T_QUANTITY))),
               CASE WHEN cm.T_CONVERSIONFACTOR > 0
                    THEN xmlelement("nsdext:conversionFactor", cm.T_CONVERSIONFACTOR)
               END)';
   end GetBlockSwapPhysicalLeg;

   function GetBlockCollateral041 return clob as
   begin
     Return 'xmlelement("fpmlext:collateral",
               xmlattributes(''nsdext:CollateralValuationNsd'' as "xsi:type"),
               CASE WHEN cm.t_SignBonds = ''X''
                    THEN xmlelement("fpmlext:nominalAmount",
                           xmlelement("currency",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_CollatCurr)),
                           xmlelement("amount", replace(ltrim(to_char(cm.t_CollatNomAmount, ''99999999999999999990D00''), '' ''), '','', ''.'')))
               END,
               CASE WHEN cm.t_SignBonds = ''X''
                    THEN xmlelement("fpmlext:cleanPrice", cm.t_NetPrice)
               END,
               CASE WHEN cm.t_SignBonds = ''X''
                    THEN xmlelement("fpmlext:accruals", replace(ltrim(to_char(cm.t_Accrual, ''99999999999999999990D00''), '' ''), '','', ''.''))
               END,
               CASE WHEN cm.t_SignBonds = ''X''
                    THEN xmlelement("fpmlext:dirtyPrice", replace(ltrim(to_char(cm.t_DrtPrice, ''99999999999999999990D00''), '' ''), '','', ''.''))
               END,
               CASE WHEN cm.t_SignBonds = ''X''
                    THEN xmlelement("fpmlext:assetReference",
                           xmlattributes(RSP_COMMON.EraseChr1(ins.t_InstrumentCode) as "href"))
               END,
               CASE WHEN cm.t_SignBonds = ''X''
                    THEN xmlelement("nsdext:securityHaircut",
                           xmlelement("nsdext:haircutValue", replace(ltrim(to_char(cm.t_haircutvaluer, ''99999999999999999990D00''), '' ''), '','', ''.'')))
               END,
               CASE WHEN cm.t_SignBond = ''X''
                    THEN xmlelement("fpmlext:numberOfUnits",
                           xmlelement("fpmlext:numberOfUnits", cm.t_NumberOfUnits),
                           xmlelement("fpmlext:unitPrice",
                             xmlelement("currency",
                               xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_UnitPriceCurr)),
                             xmlelement("amount", replace(ltrim(to_char(cm.t_UnitPriceAmount, ''99999999999999999990D00''), '' ''), '','', ''.''))))
               END,
               CASE WHEN nvl(cm.t_haircutValuer, 0) <> 0
                    THEN xmlelement("nsdext:securityHaircut",
                           xmlelement("nsdext:haircutValue", replace(ltrim(to_char(cm.t_haircutValuer, ''99999999999999999990D00''), '' ''), '','', ''.'')))
               END)';
   end GetBlockCollateral041;

   function GetBlockDeliveryDate return clob as
   begin
     Return 'xmlelement("nsdext:deliveryDate",
               xmlelement("adjustableDate",
                 xmlelement("unadjustedDate", cm.t_DeliveryDatePart1)))';
   end GetBlockDeliveryDate;

   function GetBlockForwardLeg return clob as
   begin
     Return 'xmlelement("fpmlext:forwardLeg",
               xmlattributes(''nsdext:ForwardRepoTransactionLegNsd'' as "xsi:type"),
               xmlelement("buyerPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.t_BuyerPart2) as "href")),
               xmlelement("sellerPartyReference",
                 xmlattributes(RSP_COMMON.EraseChr1(cm.t_SellerPart2) as "href")),
               xmlelement("fpmlext:settlementDate",
                 xmlelement("adjustableDate",
                   xmlelement("unadjustedDate", cm.t_Part2SettlDate))),
               CASE WHEN cm.t_RateType = ''FX''
                    THEN xmlelement("settlementAmount",
                           xmlelement("currency",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_CurrencyPart2)),
                             xmlelement("amount", replace(ltrim(to_char(cm.t_AmountPart2, ''99999999999999999990D00''), '' ''), '','', ''.'')))
                    ELSE xmlelement("settlementCurrency ",
                           xmlelement("currency",
                             xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.t_CurrencyPart2)))
               END,
               xmlelement("nsdext:deliveryMethod", RSP_COMMON.EraseChr1(cm.t_SettlMethodPart2)),
               '||RSP_REPOSITORY.GetBlockDeliveryDate||')';
   end GetBlockForwardLeg;

   function GetBlockRepoBulkReport return clob as
   begin
     Return 'xmlelement("repoBulkReport",
               xmlelement("productType",
                 xmlattributes(''http://www.fpml.org/coding-scheme/product-taxonomy'' as "productTypeScheme"), RSP_COMMON.EraseChr1(gen.t_ProductType)),
               xmlelement("productId", nvl(RSP_COMMON.EraseChr1(gen.t_ProductCode), ''UNKN'')),
               xmlelement("tradesObligationStatus", RSP_COMMON.EraseChr1(gen.t_ObligationStatus)),
               xmlelement("spotLegSettlDate", gen.t_spotLegSeyttlDate),
               xmlelement("forwardLegSettlDate", gen.t_forwardLegSeyttlDate),
               '||RSP_REPOSITORY.GetBlockRepos1||')';
   end GetBlockRepoBulkReport;

   function GetBlockRepos return clob as
   begin
     Return '(select xmlagg(xmlelement("repos",
                              xmlelement("nsdext:counterpartyN",
                                xmlelement("partyId", RSP_COMMON.EraseChr1(p.t_PartyId1)),
                                xmlelement("partyId", RSP_COMMON.EraseChr1(p.t_PartyId2)),
                                xmlelement("partyName", RSP_COMMON.EraseChr1(p.t_PartyName)),
                                xmlelement("classification", RSP_COMMON.EraseChr1(p.t_Classification)),
                                xmlelement("country", RSP_COMMON.EraseChr1(p.t_Country)),
                                xmlelement("organizationType", RSP_COMMON.EraseChr1(p.t_organizationType))),
                                '||RSP_REPOSITORY.GetBlockRepoDetails||'))
                from IR_PARTY p
               where p.t_InternalMessageID = gen.t_InternalMessageID
               order by p.t_PartyType)';
   end GetBlockRepos;

   function GetBlockRepos1 return clob as
     l_XML CLOB;
   begin
     FOR c IN(SELECT p.*
                FROM IR_PARTY p
               WHERE p.t_InternalMessageID = g_MessageID
               ORDER BY p.t_partytype) LOOP
       IF c.t_partytype <> chr(1) THEN
         l_XML := l_XML||chr(13)||'xmlelement("repos",
                          xmlelement("'||RSP_COMMON.EraseChr1(c.t_partytype)||'",
                            xmlelement("partyId", '''||RSP_COMMON.EraseChr1(c.t_partyid1)||'''),
                            xmlelement("partyId", '''||RSP_COMMON.EraseChr1(c.t_PartyId2)||'''),
                            xmlelement("partyName", '''||RSP_COMMON.EraseChr1(c.t_PartyName)||'''),
                            xmlelement("classification", '''||RSP_COMMON.EraseChr1(c.t_classification)||'''),
                            xmlelement("country", '''||RSP_COMMON.EraseChr1(c.t_country)||'''),
                            xmlelement("organizationType", '''||RSP_COMMON.EraseChr1(c.t_organizationtype)||''')),
                            '||RSP_REPOSITORY.GetBlockRepoDetails(c.t_ID)||'),';
       END IF;
     END LOOP;
     Return ltrim(rtrim(l_XML, ','), chr(13));
   end GetBlockRepos1;

   function GetBlockRepoDetails (p_InternalpartyID in varchar2 default null) return clob as
   begin
     Return '(select xmlagg(xmlelement("repoDetails",
                              xmlelement("tradeId",
                                xmlattributes(RSP_COMMON.EraseChr1(cm.t_r_TradeId) as "r",
                                              RSP_COMMON.EraseChr1(cm.t_p_tradeId) as "p",
                                              RSP_COMMON.EraseChr1(cm.t_u_tradeId) as "u",
                                              RSP_COMMON.EraseChr1(cm.t_pid_tradeId) as "pid")),
                              xmlelement("side", RSP_COMMON.EraseChr1(cm.t_side)),
                              xmlelement("rate", cm.t_rate),
                              xmlelement("spotLeg",
                                xmlattributes(RSP_COMMON.EraseChr1(cm.t_a_spot) as "a",
                                              RSP_COMMON.EraseChr1(cm.t_c_spot) as "c")),
                              xmlelement("forwardLeg",
                                xmlattributes(RSP_COMMON.EraseChr1(cm.t_a_forward) as "a",
                                              RSP_COMMON.EraseChr1(cm.t_c_forward) as "c")),
                              CASE WHEN cm.t_Sign_Equity = ''X''
                                   THEN xmlelement("equity",
                                          xmlattributes(RSP_COMMON.EraseChr1(cm.t_id_Equity) as "id",
                                                        cm.t_n_Equity as "n",
                                                        cm.t_p_Equity as "p",
                                                        RSP_COMMON.EraseChr1(cm.t_c_Equity) as "c"))
                                   WHEN cm.t_Sign_Bonds = ''X''
                                   THEN xmlelement("bond",
                                          xmlattributes(RSP_COMMON.EraseChr1(cm.t_id_Bond) as "id",
                                                        cm.t_n_Bond as "n",
                                                        RSP_COMMON.EraseChr1(cm.t_c_Bond) as "p"))
                              END,
                              xmlelement("client",
                                xmlattributes(RSP_COMMON.EraseChr1(cm.t_i_Client) as "id",
                                              RSP_COMMON.EraseChr1(cm.t_t_Client) as "t",
                                              RSP_COMMON.EraseChr1(cm.t_n_Client) as "n",
                                              RSP_COMMON.EraseChr1(cm.t_c_Client) as "c"))))
                from IR_CM083 cm
               where cm.t_InternalMessageID = gen.t_InternalMessageID
                 and cm.t_InternalpartyID = '||p_InternalpartyID||')';
   end GetBlockRepoDetails;

   function GetBlockTransfersAndExecution return clob as
   begin
     Return 'xmlelement("nsdext:transfersAndExecution",
               xmlelement("nsdext:reportIdentifier",
                 xmlelement("partyReference",
                   xmlattributes(gen.t_PartyRef_TradeRepository as "href")),
                 xmlelement("tradeId", RSP_COMMON.EraseChr1(gen.t_TradeId_TradeRepository)),
                 xmlelement("linkId",
                   xmlattributes(''http://repository.nsd.ru/coding-scheme/linkid(nsdrus)'' as "linkIdScheme"), gen.t_linkid),
                 xmlelement("originatingTradeId",
                   xmlelement("partyReference",
                     xmlattributes(gen.t_PartyRef_TradeRepository as "href")),
                   xmlelement("tradeId", RSP_COMMON.EraseChr1(gen.t_OriginId_TradeRepository)))),
               xmlelement("nsdext:reportIdentifier",
                 xmlelement("partyReference",
                   xmlattributes(gen.t_PartyRef_Party1 as "href")),
                 xmlelement("tradeId", RSP_COMMON.EraseChr1(gen.t_TradeId_Party1)),
                 xmlelement("originatingTradeId",
                   xmlelement("partyReference",
                     xmlattributes(gen.t_PartyRef_Party1 as "href")),
                   xmlelement("tradeId", RSP_COMMON.EraseChr1(gen.t_originatingid_party1)))),
               xmlelement("nsdext:reportIdentifier",
                 xmlelement("partyReference",
                   xmlattributes(gen.t_PartyRef_Party2 as "href")),
                 xmlelement("tradeId", RSP_COMMON.EraseChr1(gen.t_TradeId_Party2)),
                 xmlelement("originatingTradeId",
                   xmlelement("partyReference",
                     xmlattributes(gen.t_PartyRef_Party2 as "href")),
                   xmlelement("tradeId", RSP_COMMON.EraseChr1(gen.t_originatingid_party2)))),
               xmlelement("nsdext:reportParty", gen.t_reportParty),
               '||RSP_REPOSITORY.GetBlockCreditSupportInfo||')';
  end GetBlockTransfersAndExecution;

   function GetBlockExecutionStatus return clob as
   begin
     Return 'xmlelement("nsdext:executionStatus",
               xmlelement("nsdext:repositoryMessageIdentifier",
                 xmlelement("partyReference",
                   xmlattributes(gen.t_PartyRef_TradeRepository3 as "href")),
                 xmlelement("tradeId", gen.T_TRADEREPOSITORY_REPORTID)),
               '||RSP_REPOSITORY.GetBlockTradesWithStatus||')';
   end GetBlockExecutionStatus;

   function GetBlockMarkToMarketValuation return clob as
   begin
     Return 'xmlelement("nsdext:markToMarketValuation",
               xmlelement("nsdext:reportIdentifier",
                 xmlelement("partyReference",
                   xmlattributes(gen.t_PartyRef_TradeRepository as "href")),
                 xmlelement("tradeId", RSP_COMMON.EraseChr1(gen.t_TradeId_TradeRepository))),
               xmlelement("nsdext:reportIdentifier",
                 xmlelement("partyReference",
                   xmlattributes(gen.t_PartyRef_Party1 as "href")),
                   xmlelement("tradeId", RSP_COMMON.EraseChr1(gen.t_TradeId_Party1))),
               xmlelement("nsdext:reportIdentifier",
                 xmlelement("partyReference",
                   xmlattributes(gen.t_PartyRef_Party2 as "href")),
                   xmlelement("tradeId", RSP_COMMON.EraseChr1(gen.t_TradeId_Party2))),
                 '||RSP_REPOSITORY.GetBlockMarkToMarketDetails||')';
   end GetBlockMarkToMarketValuation;

   function GetBlockCreditSupportInfo return clob as
   begin
     Return '(select xmlagg(xmlelement("nsdext:creditSupportInformation",
                              xmlelement("nsdext:valuationDate", cm.T_VALUATIONDATE),
                              CASE WHEN RSP_COMMON.EraseChr1(cm.T_TRANSORREF_CSAMOUNT) IS NOT NULL
                                    AND RSP_COMMON.EraseChr1(cm.T_TRANSEEREF_CSAMOUNT) IS NOT NULL
                                   THEN xmlelement("nsdext:creditSupportAmount",
                                          xmlelement("nsdext:transferorReference",
                                            xmlattributes(RSP_COMMON.EraseChr1(cm.T_TRANSORREF_CSAMOUNT) as "href")),
                                          xmlelement("nsdext:transfereeReference",
                                            xmlattributes(RSP_COMMON.EraseChr1(cm.T_TRANSEEREF_CSAMOUNT) as "href")),
                                          xmlelement("nsdext:amount", replace(ltrim(to_char(cm.T_CSAMOUNT, ''99999999999999999990D00''), '' ''), '','', ''.'')),
                                            xmlelement("nsdext:currency",
                                              xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_CSAMOUNTCURRENCY)))
                              END,
                              xmlelement("nsdext:creditSupportBalance",
                                xmlelement("nsdext:transferorReference",
                                  xmlattributes(RSP_COMMON.EraseChr1(cm.T_TRANSORREF_CSBALANCE) as "href")),
                                xmlelement("nsdext:transfereeReference",
                                  xmlattributes(RSP_COMMON.EraseChr1(cm.T_TRANSEEREF_CSBALANCE) as "href")),
                                xmlelement("nsdext:amount", replace(ltrim(to_char(cm.T_CSBALANCE, ''99999999999999999990D00''), '' ''), '','', ''.'')),
                                  xmlelement("nsdext:currency",
                                    xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_CSBALANCECURRENCY))),
                              xmlelement("nsdext:independentAmount",
                                xmlelement("nsdext:transferorReference",
                                  xmlattributes(RSP_COMMON.EraseChr1(cm.T_TRANSORREF_INDEPAMOUNT) as "href")),
                                xmlelement("nsdext:transfereeReference",
                                  xmlattributes(RSP_COMMON.EraseChr1(cm.T_TRANSEETEF_INDEPAMOUNT) as "href")),
                                xmlelement("nsdext:amount", replace(ltrim(to_char(cm.T_INDEPAMOUNT, ''99999999999999999990D00''), '' ''), '','', ''.'')),
                                  xmlelement("nsdext:currency",
                                    xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_INDEPAMOUNTCURRENCY))),
                              xmlelement("nsdext:creditSupportObligations",
                                xmlelement("nsdext:transferorReference",
                                  xmlattributes(RSP_COMMON.EraseChr1(cm.T_TRANSORREF_CSOBLIGATIONS) as "href")),
                                xmlelement("nsdext:transfereeReference",
                                  xmlattributes(RSP_COMMON.EraseChr1(cm.T_TRANSEEREF_CSOBLIGATIONS) as "href")),
                                xmlelement("nsdext:amount", replace(ltrim(to_char(cm.T_CSOBLIGATIONS, ''99999999999999999990D00''), '' ''), '','', ''.'')),
                                  xmlelement("nsdext:currency",
                                    xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(cm.T_CSOBLIGATIONSCURRECNY)))))
                from IR_CM092 cm
               where cm.t_internalmessageid = gen.t_InternalMessageID)';
  end GetBlockCreditSupportInfo;

  function GetBlockTradesWithStatus return clob as
  begin
    Return '(select xmlagg(xmlelement("nsdext:tradesWithStatus",
                             (select xmlelement("nsdext:reportReferences",
                                       xmlagg(xmlelement("nsdext:tradeId", RSP_COMMON.EraseChr1(cm1.t_tradeident))))
                                from IR_CM093 cm1
                               where cm1.t_tradeoblstatus = cm2.t_tradeoblstatus
                                 and cm1.t_internalmessageid = gen.t_InternalMessageID),
                             xmlelement("nsdext:tradeObligationStatus", cm2.t_tradeoblstatus)))
               from (select cm.t_tradeoblstatus
                       from IR_CM093 cm
                      where cm.t_internalmessageid = gen.t_InternalMessageID
                      group by cm.t_tradeoblstatus) cm2)';
  end GetBlockTradesWithStatus;

  function GetBlockMarkToMarketDetails return clob as
  begin
    Return '(select CASE WHEN RSP_COMMON.EraseChr1(cm.T_MTMIDENTIFIER) IS NOT NULL
                         THEN xmlelement("nsdext:MTMIdentifier", RSP_COMMON.EraseChr1(cm.T_MTMIDENTIFIER))
                    END
               from IR_CM094 cm
              where cm.T_INTERNALMESSAGEID = gen.t_InternalMessageID
                and rownum = 1),
            (select xmlelement("nsdext:valuationMethod", cm.T_VALUATIONMETHOD)
               from IR_CM094 cm
              where cm.T_INTERNALMESSAGEID = gen.t_InternalMessageID
                and rownum = 1),
            xmlelement("nsdext:reportParty", gen.T_REPORTPARTY),
            (select xmlagg(xmlelement("nsdext:markToMarketDetails",
                             xmlelement("nsdext:valuationDate", cm.T_VALUATIONDATE),
                             (select xmlagg(xmlelement("nsdext:markToMarketInformation",
                                              xmlelement("nsdext:tradeId", tr.T_TRADEID),
                                              xmlelement("nsdext:amount", replace(ltrim(to_char(tr.T_AMOUNT, ''99999999999999999990D00''), '' ''), '','', ''.'')),
                                              xmlelement("nsdext:currency",
                                                xmlattributes(''http://www.fpml.org/ext/iso4217-2001-08-15'' as "currencyScheme"), RSP_COMMON.EraseChr1(tr.t_code_currency))))
                                from IR_TRADES tr
                               where tr.T_INTERNALMESSAGEID = cm.t_internalmessageid
                                 and tr.t_valuationdate = cm.t_valuationdate)))
              from IR_CM094 cm
             where cm.T_INTERNALMESSAGEID = gen.t_internalmessageid)';
  end GetBlockMarkToMarketDetails;

  function CHECKUNDO (P_DRAFTID in integer,P_OPER in integer,P_TEXT out varchar2) return INTEGER as
   /*Функция выполняет откат всех шагов обработки сообщения, если оно ещё не было отправлено, и возвращет уведомление об откате. */
     l_c         SYS_REFCURSOR;
     l_IDPaym    INTEGER;
     l_IDState   INTEGER;
     l_VR        INTEGER;
     l_CodeState INTEGER;
     f1          INTEGER;
     l_IDPack    INTEGER;
     l_VRPack    INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция CHECKUNDO: ID = '||P_DRAFTID);
     p_Text := chr(1);
   --Получение и блокировка списка платеже
     OPEN l_c FOR SELECT p.id_paym, p.id_sstate, p.versionrec
                    FROM paym p
                   WHERE p.id_paym IN(select max(id_paym)
                                        from payass
                                       where associate = to_char(p_DraftID)
                                         and id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                                                      P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Репозитарий\Ассоциация'))
                                       group by associate)
                     AND p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                                       P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Репозитарий\Состояния\Откат')) FOR UPDATE NOWAIT;
       LOOP
         FETCH l_c INTO l_IDPaym, l_IDState, l_VR;
       --Если список пустой
         IF l_IDPaym IS NULL THEN
           Return 1;
         END IF;
         EXIT WHEN l_c%NOTFOUND;
       --Блокировка пакета
         SELECT i.id_ipspck, i.versionrec INTO l_IDPack, l_VRPack
           FROM ipspck i
          WHERE i.id_paym = l_IDPaym FOR UPDATE NOWAIT;
       --Определение кода состояния
         SELECT code
           INTO l_CodeState
           FROM sstate
          WHERE id_sstate = l_IDState;
       --Анализ состояния
         IF l_CodeState = RSP_SETUP.GET_VALUE(P_PATH => 'Репозитарий\Состояния\Отправлен') THEN
           p_Text := 'Откат невозможен: инструкция отправлена';
           Return 2;
         ELSIF RSP_SETUP.GET_VALUE(P_PATH => 'Репозитарий\Состояния\Для отката') LIKE('%'||l_CodeState||'%') THEN
         --Формирование пачки платежей
           INSERT INTO tmp_paym_pack(id_paym, id_sstate, varlen, vr)
             VALUES(l_IDPaym, RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                             P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Репозитарий\Состояния\Откат')), NULL, l_VR);
           p_Text := 'Откат платежа '||l_IDPaym||'.';
         ELSE
           p_Text := 'Состояние платежа не допускает отката инструкции';
           Return 2;
         END IF;
       END LOOP;
     CLOSE l_c;
     RSP_ADM.SET_USERID(USERID => to_char(p_Oper));
   --Изменение состояния платежей
     f1 := RSP_GATE.MASSCHANGESTATE(P_TBLNAME => 'TMP_PAYM_PACK');
     IF f1 > 0 THEN
     --Изменение состояния пакета
       f1 := RSP_PAYM_API.PackChangeState(P_IDPACK  => l_IDPack,
                                          P_VR      => l_VRPack,
                                          P_IDSTATE => RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTPCK',
                                                                                      P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния пакета\Завершенный')),
                                          P_VARLEN  => 'Отозвана инструкция');
       IF f1 > 0 THEN
         p_Text := p_Text||' Пакет '||l_IDPack||' завершен.';
         Return 0;
       END IF;
       Return 1;
     END IF;
   end CheckUndo;

   function CheckStatePaym (P_DRAFTID in integer) return INTEGER as
   /*Функция определяет состояние платежа для возножных действий с ним.*/
     l_Cnt INTEGER;
   begin
     SELECT 1
       INTO l_Cnt
       FROM paym p
      WHERE p.id_paym IN(select max(id_paym)
                           from payass
                          where associate = to_char(p_DraftID)
                            and id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                                         P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Репозитарий\Ассоциация'))
                          group by associate)
        AND p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                          P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Репозитарий\Состояния\Откат')) FOR UPDATE NOWAIT;
     Return 0;
   exception
     WHEN NO_DATA_FOUND THEN
       Return 1;
   end CheckStatePaym;

   function LOADPAYM(p_NameFile in varchar2) return integer as
     l_Node    INTEGER := RSP_SETUP.Get_Value(P_PATH => 'Настройки инициализации\OWNNODE');
     l_PClass  INTEGER := RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SPCLASS',
                                                         P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Репозитарий\Класс платежа'));
     l_FormSRS INTEGER := RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'NFORM',
                                                         P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Репозитарий\Формы\СРС'));
     l_IDState INTEGER := RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTATE',
                                                         P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Репозитарий\Состояния\Получен'));
     l_IDPaym  INTEGER := Sequence_Paym.Nextval;
     l_Res     INTEGER;
   begin
   --Вставка платежа
     INSERT INTO paym(id_paym, nodeid, nodepaymentid, id_spclass, isinf, id_nform, id_sstate, id_sfi, currstatedate,
                      inputdate, numreferdoc, datereferdoc, payerbankid, receiverbankid,
                      dc, numpack, priority, id_nkorrinput, id_nksinput, id_sfiinput, id_nkorrcurramount,
                      id_nkscurramount, id_sficurramount, valuedate, dateinput)
       SELECT l_IDPaym, l_Node, l_IDPaym, l_PClass, 0, l_FormSRS, l_IDState, n.id_sfi, sysdate,
              sysdate, substr(x.numreferdoc, 20), TO_DATE (REPLACE (x.datereferdoc, 'T', ' '),
                                                           'YYYY-MM-DD HH24:MI:SS'), x.payerbankid, x.receiverbankid,
              0, 3333, 6, k.id_nkorr, n.id_nks, n.id_sfi, k.id_nkorr,
              n.id_nks, n.id_sfi, RSP_COMMON.Get_DateOperDay, sysdate
         FROM nkorr k, nks n, nroute r, TMP_XML t,
              XMLTABLE(xmlnamespaces(DEFAULT 'http://www.fpml.org/FpML-5/recordkeeping'),
                       '/*/header' PASSING t.valxml
                       COLUMNS payerbankid    PATH 'sentBy',
                               receiverbankid PATH 'sendTo',
                               numreferdoc    PATH 'messageId',
                               datereferdoc   PATH 'creationTimestamp') x
        WHERE k.id_nkorr = n.id_nkorr(+)
          AND x.payerbankid = r.bic(+)
          AND r.id_nkorr = k.id_nkorr(+);
     INSERT INTO msgrepo(id_paym, typ, sendbyrik, sendtorik, str)
       SELECT p.id_paym, RSP_REPOSITORY.Get_TYPE_MESSAGE_SRS(x.column_value.getRootElement()), p.payerbankid, p.receiverbankid, t.valxml.GetClobVal()
         FROM paym p,
              TMP_XML t,
              table(xmlsequence(t.valxml)) x
        WHERE p.id_paym = l_IDPaym;
     INSERT INTO tmp_paym_pack(id_paym, id_sstate, varlen, vr)
       VALUES(l_IDPaym, l_IDState, 'Получен файл '||p_NameFile, 1);
     l_Res := RSP_GATE.MASSCHANGESTATE('TMP_PAYM_PACK', 1, 1);
     Return l_IDPaym;
   end LoadPAYM;

   function Get_TYPE_MESSAGE_SRS (p_Root in varchar2) return varchar2 as
   begin
     Return CASE p_Root WHEN 'nonpublicExecutionReportAcknowledgement' THEN 'RM001'
                        WHEN 'nonpublicExecutionReportException' THEN 'RM002'
                        WHEN 'eventStatusResponse' THEN 'RM003'
                        WHEN 'statementReport' THEN 'RM004'
                        WHEN 'nonpublicExecutionReport' THEN 'RM005'
                        WHEN 'reportDifference' THEN 'RM006'
                        WHEN 'pendingMessagesReport' THEN 'RM007'
                        WHEN 'IncomingMessagesStatementReport' THEN 'RM009'
                        WHEN 'repositoryAgreemntTerminationNotification' THEN 'RM010'
            END;
   end Get_Type_Message_SRS;

   function GetIncomingSRS (p_date IN DATE, p_op_id IN INTEGER, p_op_Kind IN INTEGER, p_oper IN INTEGER, p_LogHeader IN varchar2) return number as
   cursor c1 is(
       SELECT m.str,
              p.inputdate,
              p.id_paym
         FROM paym p, msgrepo m, tmp_stepdoc t
        WHERE p.id_paym = m.id_paym
          AND t.id_paym = p.id_paym
          AND t.vr = p.versionrec);
   ret        integer;
   InterMesID integer;
   Text       varchar2(1000);
   Protocol   varchar2(32000);
   Property   varchar2(1000);
   Property32 varchar2(32000);

   begin
     execute immediate 'begin RSB_PAYMENTS_API.InsertHeaderLog(:DocID, :DocKind, :oper, :LogData); end;'
       using  in p_op_id, in p_op_Kind, in p_oper, in p_LogHeader;

     INSERT INTO tmp_stepdoc(id_paym,
                             vr,
                             id_sstate,
                             frndate)
       SELECT p.id_paym, p.versionrec,
              RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                             P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Репозитарий\Состояния\Выгружен')),
              sysdate
         FROM paym p
        WHERE p.id_sstate = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                           P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Репозитарий\Состояния\Получен'));
   Ret:=0;
   Protocol:='<ReportNumber Number="0">';

   FOR SRS IN c1 LOOP
     execute immediate 'delete from IR_SRS_TMP';
     execute immediate 'insert into IR_SRS_TMP (t_fmtclobdata_xxxx, t_loaddatetime) values(:str, :inputdate)' using SRS.str, SRS.inputdate;

     execute immediate 'begin :Ret := RSB_PAYMENTS_API.GenerationSRSFromInput(:Text, :InterMesID, :op_id, :Property); end;'
       using out Ret, out Text, out InterMesID, in p_op_id, out Property;
     IF Ret = 0 THEN
       UPDATE tmp_stepdoc t
          SET t.varlen = Text
        WHERE t.id_paym = SRS.id_paym;
     ELSIF(Ret = 1) THEN
       UPDATE tmp_stepdoc t
          SET t.varlen = Text,
              t.id_sstate = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                           P_CODE  => 7915)
        WHERE t.id_paym = SRS.id_paym;
     ELSE
       UPDATE tmp_stepdoc t
          SET t.varlen = Text,
              t.id_sstate = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                           P_CODE  => 7914)
        WHERE t.id_paym = SRS.id_paym;
     END IF;

     Protocol:= Protocol || Property ;

   END LOOP;
     Protocol:= Protocol || '</ReportNumber>';

     execute immediate 'begin RSB_PAYMENTS_API.InsertLogDate(:DocID, :DocKind, 1, :oper, :LogData); end;'
       using  in p_op_id, in p_op_Kind, in p_oper, in Protocol;

   Ret := RSP_GATE.MASSCHANGESTATE('TMP_STEPDOC');

   Protocol:='<ReportNumber Number="0">';

   execute immediate 'begin :Ret := RSB_PAYMENTS_API.ProcInboundSRS(:Property, :OP_ID); end;' using out Ret, in out Property32, in p_op_id;
   Protocol:= Protocol || Property32 || '</ReportNumber>';

   execute immediate 'begin RSB_PAYMENTS_API.InsertLogDate(:DocID, :DocKind, 4, :oper, :LogData); end;'
     using  in p_op_id, in p_op_Kind, in p_oper, in Protocol;

   return Ret;

   end;

end RSP_REPOSITORY;
/

