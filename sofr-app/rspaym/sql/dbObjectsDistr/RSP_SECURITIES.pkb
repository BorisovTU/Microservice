CREATE OR REPLACE package body RSP_SECURITIES as
 G_CATKORR INTEGER :=0;
 G_KINDMSG INTEGER;
 G_DRAFTID INTEGER;
 G_TRANSPORT VARCHAR2(25);
 G_DRAFTKIND INTEGER;
 function GENMSG4DRAFT (P_DRAFTKIND in integer,P_DRAFTID in integer,P_KINDMSG in varchar2,P_OPER in integer,P_DEPARTMENT in integer,P_TEXT in out varchar2,P_NUMREFER out varchar2,P_DATEREFER out date,P_TRANSPORT in varchar2 default 'SWIFT') return INTEGER as
 /*Функция инициирует генерацию сообщения по указанному поручению в Payments*/
 l_ResCrt INTEGER;
 begin
 RSP_COMMON.INS_LOG(1, 'Вызвана функция GENMSG4DRAFT: '||P_DRAFTKIND||' '||P_DRAFTID||' '||P_KINDMSG||' '||P_TRANSPORT);
 g_KindMsg := substr(p_KindMsg, 3);
 g_DraftID := p_DraftID;
 G_TRANSPORT := p_TRANSPORT;
 G_DRAFTKIND := P_DRAFTKIND;
 BEGIN
 --Получение категории учетной организации
 SELECT sc.code
 INTO g_CatKorr
 FROM DRAFTSINFO v,
 nroute nr,
 lnkrct lc,
 scat sc
 WHERE sc.id_scat = lc.id_scat
 AND lc.id_nkorr = nr.id_nkorr
 AND nr.bic = v.t_receiverbic
 AND v.t_DraftID = p_DraftID;
 EXCEPTION
 WHEN NO_DATA_FOUND THEN
 NULL;
 END;
 RSP_ADM.SET_USERID(USERID => to_char(p_Oper));
 p_Text := chr(1);
 IF p_DraftKind IN(1, 5, 6, 8, 9) THEN
 l_ResCrt := RSP_SECURITIES.Crt_MT54x(P_DRAFTKIND => p_DraftKind,
 P_DRAFTID => p_DraftID,
 P_KINDMSG => g_KindMsg,
 P_TEXT => p_Text,
 p_NumRefer => p_NumRefer,
 p_DateRefer => p_DateRefer);
 END IF;
 IF p_DraftKind = 2 THEN
 l_ResCrt := RSP_SECURITIES.Crt_MT53x(P_DRAFTID => p_DraftID,
 P_KINDMSG => g_KindMsg,
 P_TEXT => p_Text,
 p_NumRefer => p_NumRefer,
 p_DateRefer => p_DateRefer);
 END IF;
 Return l_ResCrt;
 end GenMsg4Draft;
 function GENMSG4DRAFTSEC (P_DRAFTID in integer,P_KINDMSG in varchar2,P_OPER in integer,P_DEPARTMENT in integer,P_TEXT in out varchar2,P_NUMREFER out varchar2,P_DATEREFER out date,P_TRANSPORT in varchar2 default 'SWIFT',P_DRAFTKIND in integer default 0) return INTEGER as
 /*Функция инициирует генерацию сообщения по указанному поручению в Payments*/
 l_ResCrt INTEGER;
 begin
 RSP_COMMON.INS_LOG(1, 'Вызвана функция GENMSG4DRAFTSEC: '||P_DRAFTID||' '||P_KINDMSG);
 RSP_ADM.SET_USERID(USERID => to_char(p_Oper));
 p_Text := chr(1);
 g_KindMsg := substr(p_KindMsg, 3);
 g_DraftID := p_DraftID;
 G_TRANSPORT := p_TRANSPORT;
 g_DraftKind := nvl(p_DraftKind, g_KindMsg);
 IF g_KindMsg = '518' or g_KindMsg = '599' or g_KindMsg = '540' or g_KindMsg = '541' or g_KindMsg = '542' or g_KindMsg = '543' THEN
 g_DraftKind := 101;
 END IF;
 IF g_KindMsg = '518' THEN
 l_ResCrt := RSP_SECURITIES.Crt_MT518(P_DRAFTID => p_DraftID,
 P_KINDMSG => g_KindMsg,
 P_TEXT => p_Text,
 p_NumRefer => p_NumRefer,
 p_DateRefer => p_DateRefer);
 END IF;
 IF g_KindMsg = '599' THEN
 l_ResCrt := RSP_SECURITIES.Crt_MT599(P_DRAFTID => p_DraftID,
 P_KINDMSG => g_KindMsg,
 P_TEXT => p_Text,
 p_NumRefer => p_NumRefer,
 p_DateRefer => p_DateRefer);
 END IF;
 Return l_ResCrt;
 end GenMsg4DraftSEC;
 function GENMSG4DRAFTSECNETT (P_DRAFTID in integer,P_KINDMSG in varchar2,P_OPER in integer,P_DEPARTMENT in integer,P_TEXT in out varchar2,P_DRAFTKIND in integer) return INTEGER as
 /*Функция инициирует генерацию сообщения по указанному поручению в Payments*/
 l_ResCrt INTEGER;
 begin
 RSP_COMMON.INS_LOG(1, 'Вызвана функция GENMSG4DRAFTSECNETT: '||P_DRAFTID||' '||P_KINDMSG);
 RSP_ADM.SET_USERID(USERID => to_char(p_Oper));
 g_KindMsg := substr(p_KindMsg, 3);
 g_DraftID := p_DraftID;
 g_DraftKind := nvl(p_DraftKind, g_KindMsg);
 p_Text := chr(1);
 l_ResCrt := RSHB_RSPAYM_SEC.RSP_SECURITIES.Crt_MT599Nett(P_DRAFTID => p_DraftID,
 P_KINDMSG => g_KindMsg,
 P_TEXT => p_Text,
 P_ROWNUM => 1);
 Return l_ResCrt;
 end GenMsg4DraftSECNett;
 function GENMSG4DRAFTSECNETTCONS (P_BANKID in varchar2,P_OPER in integer,P_DEPARTMENT in integer,P_RESULT out T_REFMTNETT) return INTEGER as
 /*Функция инициирует генерацию сводного сообщения по указанному контрагенту*/
 l_IDPaym INTEGER;
 l_NKorr INTEGER;
 l_Nks INTEGER;
 f1 INTEGER;
 l_NumRefer VARCHAR2(20);
 l_DateRefer DATE;
 l_Text VARCHAR2(128);
 i INTEGER := 0;
 l_IDKart INTEGER;
 l_Form INTEGER;
 l_IDPack INTEGER;
 l_Msg CLOB;
 begin
 RSP_COMMON.INS_LOG(1, 'Вызвана функция GENMSG4DRAFTSECNETTCONS: '||P_BANKID);
 RSP_ADM.SET_USERID(USERID => to_char(p_Oper));
 --Группировка МТ599
 FOR c IN(SELECT p.nodeid,
 p.id_nform,
 p.id_spclass,
 p.id_sfi,
 p.payerbankid,
 p.id_stbicpayer,
 p.receiverbankid,
 p.id_stbicreceiver,
 sum(p.amount) amount,
 m.addinfo,
 m.client
 FROM paym p,
 mtn9x m
 WHERE m.id_paym = p.id_paym
 AND p.receiverbankid = p_BankID
 AND p.valuedate = RSP_COMMON.Get_DateOperDay
 AND p.id_sstate = RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTATE',
                                                                  P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Состояния отправки/приема\Отложен'))
               GROUP BY p.nodeid,
                        p.id_nform,
                        p.id_spclass,
                        p.id_sfi,
                        p.payerbankid,
                        p.id_stbicpayer,
                        p.receiverbankid,
                        p.id_stbicreceiver,
                        m.addinfo,
                        m.client) LOOP
       l_Msg := NULL;
       FOR s IN(SELECT to_clob(substr(m.text, instr(m.text , 'PLEASE CONFIRM'))||chr(13)) msg
                  FROM paym p,
                       mtn9x m
                 WHERE m.id_paym = p.id_paym
                   AND p.receiverbankid = p_BankID
                   AND p.valuedate = RSP_COMMON.Get_DateOperDay
                   AND m.addinfo = c.addinfo
                   AND m.client = c.client
                   AND p.id_sstate = RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTATE',
                                                                    P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Состояния отправки/приема\Отложен'))) LOOP
         l_Msg := l_Msg||s.msg;
       END LOOP;
       BEGIN
       --Определение корсхемы
         SELECT n.id_nkorr, k.id_nks
           INTO l_NKorr, l_Nks
           FROM nkorr n, nks k, nroute r
          WHERE r.bic = p_BankID
            AND n.id_nkorr = r.id_nkorr
            AND k.id_nkorr = n.id_nkorr
            AND k.code = 1
            AND k.id_sfi = c.id_sfi;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN
         --Создание корсхемы
           RSP_PAYM_API.Crt_KorrKs(P_CODEKORR => 0,
                                   P_NAMEKORR => 'Клиент '||p_BankID,
                                   P_PRIOR    => 10,
                                   P_BIC      => p_BankID,
                                   P_TYPBIC   => c.id_stbicreceiver,
                                   P_IDFI     => c.id_sfi,
                                   P_CODEKS   => 1,
                                   P_NAMEKS   => 'Расчеты с клиентом в ц/б.',
                                   P_FRNSYS   => 'SWIFTRUS9',
                                   P_LORO     => 1,
                                   P_IDKORR   => l_NKorr,
                                   P_IDKS     => l_NKs);
       END;
     --Создание платежа
       INSERT INTO paym(nodeid,
                        id_nform,
                        id_spclass,
                        id_sfi,
                        payerbankid,
                        id_stbicpayer,
                        receiverbankid,
                        id_stbicreceiver,
                        valuedate,
                        amount,
                        numreferdoc,
                        datereferdoc,
                        numpack,
                        id_sstate,
                        currstatedate,
                        inputdate,
                        priority,
                        id_nkorroutput,
                        id_nksoutput,
                        id_sfioutput,
                        dateoutput)
         VALUES(c.nodeid,
                c.id_nform,
                c.id_spclass,
                c.id_sfi,
                c.payerbankid,
                c.id_stbicpayer,
                c.receiverbankid,
                c.id_stbicreceiver,
                RSP_COMMON.Get_DateOperDay,
                c.amount,
                to_char(RSP_REF.MakeRefer(P_FORMREF => 599,
                                          P_TYPEREF => NULL)),
                trunc(sysdate),
                RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Пачка сводного'),
                RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTATE',
                                               P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Состояния отправки/приема\Отправляется')),
                SYSDATE,
                SYSDATE,
                6,
                l_NKorr,
                l_NKs,
                c.id_sfi,
                trunc(sysdate))
       RETURNING id_paym, numreferdoc, datereferdoc INTO l_IDPaym, l_NumRefer, l_DateRefer;
     --Заполнение хвостовой таблицы
       RSP_PAYM_API.CRT_MTN9X(P_IDPAYM    => l_IDPaym,
                              P_MSG       => ':20C::SEME//'||l_NumRefer||chr(13)||':79:'||l_Msg,
                              P_KINDMSG   => 599,
                              p_Attribute => c.addinfo);
     --Создание истории платежа
       DELETE FROM tmp_paym_pack;
       INSERT INTO tmp_paym_pack(id_paym, id_sstate)
         SELECT l_IDPaym, id_sstate
           FROM paym
          WHERE id_paym = l_IDPaym;
       f1 := RSP_GATE.MASSCHANGESTATE(P_TBLNAME => 'TMP_PAYM_PACK',
                                      P_UPD => 1);
       IF f1 > 0 THEN
         l_Text := 'Создан платеж '||l_IDPaym||': № '||l_NumRefer||' от '||to_char(l_DateRefer, 'dd.mm.yyyy')||'.';
       END IF;
       i := i + 1;
     --Создание массива референсов созданных платежей
       p_Result(i).NumRef  := l_NumRefer;
       p_Result(i).DateRef := l_DateRefer;
       p_Result(i).IDPaym  := l_IDPaym;
       p_Result(i).text    := l_Text;
     --Создание пакета
       SELECT n.id_nkart
         INTO l_IDKart
         FROM nkart n
        WHERE n.namekart = 'Ценные бумаги';
       SELECT f.code
         INTO l_Form
         FROM nform f, ntable n
        WHERE f.id_ntable = n.id_ntable AND n.name = 'IPSPCK';
       l_IDPack := RSP_PAYM_API.PackCreation(P_IDPAYM  => l_IDPaym,
                                             P_REFER   => RSP_REF.MakeRefer(P_FORMREF => l_Form,
                                                                            P_TYPEREF => NULL),
                                             P_IDSTATE => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTPCK',
                                                                                         P_CODE => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния пакета\Сформирован запрос')),
                                             P_NUMREFER => null,
                                             P_IDKART     => l_IDKart);
       INSERT INTO pckpay(id_ipspck, id_paym)
         SELECT l_IDPack, p.id_paym
           FROM paym p, mtn9x m
          WHERE m.id_paym = p.id_paym
            AND p.receiverbankid = p_BankID
            AND p.valuedate = RSP_COMMON.Get_DateOperDay
            AND nvl(m.addinfo, ' ') = nvl(c.addinfo, ' ')
            AND m.client = c.client
            AND p.id_sstate = RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTATE',
                                                             P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Состояния отправки/приема\Отложен'));
     END LOOP;
     IF i = 0 THEN
       Return 1;
     END IF;
     DELETE FROM tmp_paym_pack;
     INSERT INTO tmp_paym_pack(id_paym,
                               id_sstate,
                               vr,
                               varlen)
       SELECT p.id_paym,
              RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTATE',
                                             P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Состояния отправки/приема\Добавлен в сводное')),
              p.versionrec,
              'Включено в сводный платеж ID='||l_IDPaym
         FROM paym p
        WHERE p.receiverbankid = p_BankID
          AND p.valuedate = RSP_COMMON.Get_DateOperDay
          AND p.id_sstate = RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTATE',
                                                           P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Состояния отправки/приема\Отложен'));
     f1 := RSP_GATE.MASSCHANGESTATE(P_TBLNAME => 'TMP_PAYM_PACK',
                                    P_UPD     => 0);
     Return 0;
   end GenMsg4DraftSECNettCons;
   function CREATEPAYM4SWIFT (P_KINDMSG in varchar2,P_DRAFTID in integer,P_MSG in clob,P_RECEIVBIC in varchar2,P_TEXT out varchar2,P_TRANSPORT in varchar2 default 'SWIFT',P_DRAFTKIND in integer) return INTEGER as
   /*Функция создает платеж с сообщением типа MT3xx в Payments.
      p_KindMsg - тип сообщения,
      p_Msg     - текст сообщения,
      p_Text    - информация о создании платежа.
      Возвращает код результата выполнения.
   */
     l_IDPaym INTEGER;
     l_Form   INTEGER;
     l_IDPack INTEGER;
     l_VRPack INTEGER;
     f1       INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция CREATEPAYM4SWIFT: '||P_KINDMSG||' '||P_DRAFTID||' '||P_MSG||' '||P_RECEIVBIC||' '||P_TRANSPORT||' '||P_DRAFTKIND);
     g_KindMsg := substr(p_KindMsg, 3);
     g_DraftID := p_DraftID;
     G_TRANSPORT := p_TRANSPORT;
     g_DraftKind := nvl(p_DraftKind, g_KindMsg);
   --Поиск отправленного сообщений
     SELECT nvl(MAX(a.id_paym), 0)
       INTO l_IDPaym
       FROM paym p, payass a, mtrec m
      WHERE p.id_paym = a.id_paym
        AND a.associate = to_char(p_DraftID)
        AND (a.bankid = to_char(g_DraftKind) or a.bankid = to_char(g_KindMsg))
        AND p.id_paym = m.id_paym
        AND m.type = g_KindMsg
        AND p.numreferdoc = RSP_SECURITIES.GETVALUEFIELD(P_MSG   => p_Msg,
                                                         P_FIELD => ':20:')
        AND p.id_sstate = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                         P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отправки/приема\Отправлен'));
     IF l_IDPaym > 0 THEN
       p_Text := 'Сообщение '||p_KindMsg||' ID='||p_DraftID||' уже отправлено в SWIFT (ID платежа '||l_IDPaym||').';
       Return 1;
     END IF;
   --Проверка наличия сообщений
     BEGIN
       FOR c IN(SELECT p.id_paym, p.versionrec
                  FROM paym p, payass a, mtrec m
                 WHERE p.id_paym = a.id_paym
                   AND a.associate = TO_CHAR(p_DraftID)
                   AND (a.bankid = TO_CHAR(g_DraftKind) or a.bankid = TO_CHAR(g_KindMsg))
                   AND p.id_paym = m.id_paym
                   AND m.type = g_KindMsg
                   AND p.numreferdoc = RSP_SECURITIES.GETVALUEFIELD(P_MSG   => p_Msg,
                                                                    P_FIELD => ':20:')
                   AND p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                                     P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат'))) LOOP
       --Создание истории платежа
         DELETE FROM tmp_paym_pack;
         INSERT INTO tmp_paym_pack(id_paym,
                                   vr,
                                   varlen,
                                   id_sstate)
           VALUES(c.id_paym,
                  c.versionrec,
                  'Получен новый текст сообщения',
                  RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                 P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат')));
         f1 := RSP_GATE.MASSCHANGESTATE(P_TBLNAME => 'TMP_PAYM_PACK');
         IF G_KINDMSG = '600' THEN
           SELECT pc.id_ipspck, pc.versionrec
             INTO l_IDPack, l_VRPack
             FROM ipspck pc, pckpay pp
            WHERE pc.id_ipspck = pp.id_ipspck AND pp.id_paym = l_IDPaym;
           f1 := RSP_PAYM_API.PACKCHANGESTATE(P_IDPACK  => l_IDPack,
                                              P_VR      => l_VRPack,
                                              P_IDSTATE => RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTPCK',
                                                                                          P_CODE  => 9),
                                              P_VARLEN  => 'Откат платежа');
         END IF;
       END LOOP;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         NULL;
     END;
   --Создание платежа
     l_IDPaym := RSP_SECURITIES.CRT_PAYMSWIFT(P_KINDMSG   => g_KindMsg,
                                              P_MSG       => replace(p_Msg, chr(1)),
                                              P_RECEIVBIC => P_RECEIVBIC,
                                              P_TEXT      => p_Text);
     IF l_IDPaym IS NULL THEN
       Return 2;
     END IF;
   --Заполнение "хвостовой" таблицы
     RSP_PAYM_API.Crt_MTRec(P_IDPAYM  => l_IDPAYM,
                            P_MSG     => p_Msg,
                            P_KINDMSG => g_KindMsg);
   --Создание ассоциации платежа с поручением
     RSP_PAYM_API.Crt_PaymAssociate(P_ASS      => p_DraftID,
                                    P_IDPAYM   => l_IDPaym,
                                    P_IDTYPASS => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SASS',
                                                                                 P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Ассоциация')),
                                    P_BIC      => g_DraftKind);
     IF g_KindMsg IN (300, 600, 604, 605) THEN
     --Создание пакета
       SELECT f.code
         INTO l_Form
         FROM nform f, ntable n
        WHERE f.id_ntable = n.id_ntable AND n.name = 'IPSPCK';
       l_IDPack := RSP_PAYM_API.PackCreation(P_IDPAYM   => l_IDPaym,
                                             P_REFER    => RSP_SECURITIES.GETVALUEFIELD(P_MSG   => p_Msg,
                                                                                        P_FIELD => CASE WHEN g_KindMsg = 600 THEN ':22:'
                                                                                                        WHEN g_KindMsg = 300 THEN ':22C:'
                                                                                                        ELSE ':21:'
                                                                                                   END),
                                             P_IDSTATE  => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTPCK',
                                                                                          P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния пакета\Сформирован запрос')),
                                             P_NUMREFER => RSP_SECURITIES.GETVALUEFIELD(P_MSG   => p_Msg,
                                                                                        P_FIELD => ':20:'),
                                             P_IDKART   => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'NKART',
                                                                                          P_CODE  => 2));
     END IF;
     Return 0;
   end CreatePaym4SWIFT;
   function CREATEPAYMFROMSEC (P_KINDMSG in varchar2,P_DRAFTID in integer,P_MSG in clob,P_RECEIVBIC in varchar2,P_TEXT out varchar2,P_TRANSPORT in varchar2 default 'SWIFT',P_DRAFTKIND in integer) return INTEGER as
   /*Функция создает платеж с сообщением типа MT3xx в Payments.
     p_KindMsg - тип сообщения,
     p_Msg     - текст сообщения,
     p_Text    - информация о создании платежа.
     Возвращает код результата выполнения.
   */
     l_IDPaym INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция CREATEPAYMFROMSEC: '||P_KINDMSG||' '||P_DRAFTID||' '||P_MSG||' '||P_RECEIVBIC||' '||P_TRANSPORT);
     g_KindMsg := SUBSTR (p_KindMsg, 3);
     g_DraftID := p_DraftID;
     G_TRANSPORT := p_TRANSPORT;
     g_DraftKind := nvl(p_DraftKind, g_KindMsg);
   --Создание платежа
     l_IDPaym := RSP_SECURITIES.CRT_PAYMSWIFT (P_KINDMSG   => g_KindMsg,
                                               P_MSG       => REPLACE (p_Msg, CHR (1)),
                                               P_RECEIVBIC => P_RECEIVBIC,
                                               P_TEXT      => p_Text);
     IF l_IDPaym IS NULL THEN
       RETURN 2;
     END IF;
   --Заполнение "хвостовой" таблицы
       IF g_KindMsg = 399 THEN
       RSP_PAYM_API.Crt_MTn9x (P_IDPAYM  => l_IDPAYM,
                               P_MSG     => p_Msg,
                               P_KINDMSG => g_KindMsg);
     ELSE
       RSP_PAYM_API.Crt_MTRec (P_IDPAYM  => l_IDPAYM,
                               P_MSG     => p_Msg,
                               P_KINDMSG => g_KindMsg);
     END IF;
   --Создание ассоциации платежа с поручением
     RSP_PAYM_API.Crt_PaymAssociate (P_ASS      => p_DraftID,
                                     P_IDPAYM   => l_IDPaym,
                                     P_IDTYPASS => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SASS',
                                                                                  P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Ассоциация')),
                                     P_BIC      => g_DraftKind);
   /*
     IF g_KindMsg IN(300, 320) THEN
       l_GeneralRef := RSP_SECURITIES.GETVALUEFIELD(P_MSG => p_Msg,
                                                    P_FIELD => ':22C:');
     ELSIF g_KindMsg IN(600) THEN
       l_GeneralRef := RSP_SECURITIES.GETVALUEFIELD(P_MSG => p_Msg,
                                                    P_FIELD => ':22:');
     ELSE
       l_GeneralRef := RSP_SECURITIES.GETVALUEFIELD(P_MSG => p_Msg,
                                                    P_FIELD => ':21:');
     END IF;
     RSP_PAYM_API.Crt_PaymAssociate(P_ASS      => l_GeneralRef,
                                    P_IDPAYM   => l_IDPaym,
                                    P_IDTYPASS => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE   => 'SASS',
                                                                                 P_CODE    => 5),
                                    P_BIC      => RTRIM (P_RECEIVBIC, 'XXX'));
   */
     RETURN 0;
   end CreatePaymFromSec;
   function GETRELATEDREFERENCE (P_KINDMSG in varchar2,P_DRAFTID in integer,P_DRAFTKIND in integer) return VARCHAR2 as
     l_Refer VARCHAR2(50);
   begin
     SELECT p.numreferdoc
       INTO l_Refer
       FROM paym p
      WHERE p.id_paym = (select max(a.id_paym)
                           from payass a
                          where a.associate = to_char(p_DraftID)
                            and (a.bankid = to_char(p_DraftKind) or a.bankid = substr(p_KindMsg, 3))
                            and a.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT('SASS', 3));
     Return l_Refer;
   exception
     WHEN NO_DATA_FOUND THEN
       Return NULL;
   end GetRelatedReference;
   function GETCOUNTMSG (P_KINDMSG in varchar2,P_DRAFTID in integer,P_DRAFTKIND in integer) return INTEGER as
     l_Count INTEGER;
   begin
     SELECT count(*)
       INTO l_Count
       FROM payass a, mtrec m
      WHERE a.associate = to_char(p_DraftID)
        AND (a.bankid = to_char(p_DraftKind) or a.bankid = substr(p_KindMsg, 3))
        AND a.id_paym = m.id_paym
        AND m.type = substr(p_KindMsg, 3)
        AND a.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT('SASS', 3);
     Return l_Count;
   end GetCountMsg;
   function GETCOUNTMSG (P_KINDMSG in varchar2,P_DRAFTID in integer) return INTEGER as
     l_Count INTEGER;
   begin
     SELECT count(*)
       INTO l_Count
       FROM payass a, mtrec m
      WHERE a.associate = to_char(p_DraftID)
        AND a.bankid = substr(p_KindMsg, 3)
        AND a.id_paym = m.id_paym
        AND m.type = substr(p_KindMsg, 3)
        AND a.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT('SASS', 3);
     Return l_Count;
   end GetCountMsg;
   function CREATEMTN92 (P_DRAFTID in integer,P_MSG in clob,P_RECEIVBIC in varchar2,P_TEXT out varchar2,P_TRANSPORT in varchar2 default 'SWIFT',P_DRAFTKIND in integer) return INTEGER as
   /*Функция создает платеж в Payments.
     p_KindMsg - тип сообщения,
     p_Msg     - текст сообщения,
     p_Text    - информация о создании платежа.
     Возвращает код результата выполнения.
   */
      l_IDPaym INTEGER;
   BEGIN
     RSP_COMMON.INS_LOG(1, 'Вызвана функция CREATEMTN92: '||P_DRAFTID||' '||P_MSG||' '||P_RECEIVBIC||' '||P_TRANSPORT);
     g_KindMsg := 392;
     g_DraftID := p_DraftID;
     G_TRANSPORT := p_TRANSPORT;
     g_DraftKind := nvl(p_DraftKind, g_KindMsg);
   --Создание платежа
     l_IDPaym := RSP_SECURITIES.CRT_PAYMSWIFT(P_KINDMSG   => g_KindMsg,
                                              P_MSG       => REPLACE(p_Msg, CHR(1)),
                                              P_RECEIVBIC => P_RECEIVBIC,
                                              P_TEXT      => p_Text);
     IF l_IDPaym IS NULL THEN
       RETURN 2;
     END IF;
   --Заполнение "хвостовой" таблицы
     RSP_PAYM_API.Crt_MTRec(P_IDPAYM  => l_IDPAYM,
                            P_MSG     => p_Msg,
                            P_KINDMSG => g_KindMsg);
   --Создание ассоциации платежа с поручением
     RSP_PAYM_API.Crt_PaymAssociate(P_ASS      => p_DraftID,
                                    P_IDPAYM   => l_IDPaym,
                                    P_IDTYPASS => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SASS',
                                                                                 P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Ассоциация')),
                                    P_BIC      => p_DraftKind);
     RETURN 0;
   end CreateMTn92;
   function GETMSGFORMTN92 (P_KINDMSG in varchar2,P_DRAFTID in integer,P_MSG out clob,P_TEXT out varchar2,P_NUMREFER out varchar2,P_DATEREFER out varchar2,P_DRAFTKIND in integer) return INTEGER as
   /*Функция возвращает текст отменяемого сообщения.
     p_KindMsg - тип сообщения,
     p_DraftId - ИД сделки в АБС
     p_Msg     - текст найденного сообщения
     p_Text    - информация о поиске платежа.
     Возвращает код результата выполнения.
   */
     l_IDPaym INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GETMSGFORMTN92: '||P_KINDMSG||' '||P_DRAFTID);
     g_DraftKind := p_DraftKind;
   --Поиск отменяемого сообщения
     SELECT Id_Paym, Str, Refer, DateRefer
       INTO l_IDPaym,
            p_Msg,
            p_NumRefer,
            p_DateRefer
       FROM (select p.id_paym Id_Paym,
                    m.str Str,
                    p.numreferdoc Refer,
                    TO_CHAR(p.datereferdoc, 'YYMMDD') DateRefer
               from paym p, payass a, mtrec m
              where p.id_paym = a.id_paym
                and a.associate = TO_CHAR(p_DraftID)
                and (a.bankid = TO_CHAR(g_DraftKind) or a.bankid = substr(p_KindMsg, 3))
                and p.id_paym = m.id_paym
                and m.type = substr(p_KindMsg, 3)
                and p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                                  P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат'))
              order by 1 desc)
      WHERE rownum = 1;
     RETURN 0;
   exception
     WHEN NO_DATA_FOUND THEN
       p_Text := 'Для сделки ID='||p_DraftID||' нет отменяемого сообщения.';
       RETURN 1;
   end GetMsgForMTn92;
   function CRT_MT54X (P_DRAFTKIND in integer,P_DRAFTID in integer,P_KINDMSG in varchar2,P_TEXT out varchar2,P_NUMREFER out varchar2,P_DATEREFER out date) return INTEGER as
   /*Функция создает платеж с сообщением типа MT54x по указанному поручению в Payments.
     p_DraftID - идентификатор поручения,
     p_KindMsg - тип сообщения,
     p_Text    - информация о создании платежа.
     Возвращает код результата выполнения.
   */
     l_Cnt     INTEGER;
     l_FuncMsg VARCHAR2 (4);
     l_IDPaym  INTEGER;
     l_Text    VARCHAR2 (8000);
     l_Msg     CLOB;
     l_IDKart  INTEGER;
     l_Form    INTEGER;
     l_IDPack  INTEGER;
   begin
   --Проверка наличия сообщений
     SELECT COUNT (p.id_paym)
       INTO l_Cnt
       FROM paym p,
            payass a,
            nform n,
            mtrec m
      WHERE p.id_paym = a.id_paym
        AND a.associate = TO_CHAR (p_DraftID)
        AND a.bankid IN (nvl(p_DraftKind, 1), nvl(p_DraftKind, 8), nvl(p_DraftKind, 9))
        AND p.id_nform = n.id_nform
        AND n.code = RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Формы\Инструкция')
        AND p.id_paym = m.id_paym
        AND m.type = p_KindMsg
        AND p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                          P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат'));
     IF l_cnt = 0 THEN
       l_FuncMsg := 'NEWM';
    --Создание платежа
       l_IDPaym := RSP_SECURITIES.CRT_PAYM(P_DRAFTID   => p_DraftID,
                                           P_KINDMSG   => g_KindMsg,
                                           P_TEXT      => p_Text,
                                           p_NumRefer  => p_NumRefer,
                                           p_DateRefer => p_DateRefer);
       IF l_IDPaym IS NULL THEN
         Return 2;
       END IF;
     --Получение сообщения
       l_Msg := RSP_SECURITIES.GENMESSAGE54X(P_DRAFTID => p_DraftID,
                                             P_IDPAYM  => l_IDPaym,
                                             P_KINDMSG => p_KindMsg,
                                             P_FUNCMSG => l_FuncMsg,
                                             P_PAGE    => NULL,
                                             P_TEXT    => l_Text);
       IF l_Msg IS NULL THEN
         p_Text := 'Неверные параметры поручения '||p_DraftID||': '||l_Text;
         Return 2;
       END IF;
     --Заполнение "хвостовой" таблицы
       RSP_PAYM_API.Crt_MTRec(P_IDPAYM  => l_IDPAYM,
                              P_MSG     => l_Msg,
                              P_KINDMSG => p_KindMsg);
     --Создание ассоциации платежа с поручением
       RSP_PAYM_API.Crt_PaymAssociate(P_ASS      => p_DraftID,
                                      P_IDPAYM   => l_IDPaym,
                                      P_IDTYPASS => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE   => 'SASS',
                                                                                   P_CODE    => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Ассоциация')),
                                      P_BIC      => p_DraftKind);
     --Создание пакета
       SELECT n.id_nkart
         INTO l_IDKart
         FROM nkart n
        WHERE n.namekart = 'Ценные бумаги';
       SELECT f.code
         INTO l_Form
         FROM nform f, ntable n
        WHERE f.id_ntable = n.id_ntable AND n.name = 'IPSPCK';
       l_IDPack := RSP_PAYM_API.PackCreation(P_IDPAYM  => l_IDPaym,
                                             P_REFER   => RSP_REF.MakeRefer(P_FORMREF => l_Form,
                                                                            P_TYPEREF => NULL),
                                             P_IDSTATE  => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTPCK',
                                                                                          P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния пакета\Сформирован запрос')),
                                             P_NUMREFER => NULL,
                                              P_IDKART   => l_IDKart);
       IF l_IDPack IS NOT NULL THEN
         p_Text := p_Text||' Создан пакет '||l_IDPack||'.';
       END IF;
     --Результат
       Return 0;
     ELSE
       p_Text := 'По поручению ID='||p_DraftID||' уже есть инструкция';
       Return 1;
     END IF;
   end Crt_MT54x;
   function CRT_MT53X (P_DRAFTID in integer,P_KINDMSG in varchar2,P_TEXT out varchar2,P_NUMREFER out varchar2,P_DATEREFER out date) return INTEGER as
   /*Функция создает платеж с сообщением типа MT54x по указанному поручению в Payments.
     p_DraftID - идентификатор поручения,
     p_KindMsg - тип сообщения,
     p_Text    - информация о создании платежа.
     Возвращает код результата выполнения.
   */
     l_FuncMsg VARCHAR2(4);
     l_IDPaym  INTEGER;
     l_Text    VARCHAR2(8000);
     l_Msg     CLOB;
   begin
     l_FuncMsg := 'NEWM';
   --Определение размера сообщения
     IF p_KindMsg = '535' THEN
       RSP_SECURITIES.GetPagesMT535;
     ELSIF p_KindMsg = '536' THEN
       RSP_SECURITIES.GetPagesMT536;
     END IF;
   --Постраничное формирование выписок
     FOR l_c IN(SELECT *
                  FROM tmp_mt53x) LOOP
     --Создание платежа
       l_IDPaym := RSP_SECURITIES.Crt_Paym(P_DRAFTID   => p_DraftID,
                                           P_KINDMSG   => g_KindMsg,
                                           P_TEXT      => p_Text,
                                           p_NumRefer  => p_NumRefer,
                                           p_DateRefer => p_DateRefer);
       p_Text := p_Text||' '||l_Text;
       IF l_IDPaym IS NULL THEN
         Return 2;
       END IF;
     --Получение сообщения
       l_Msg := RSP_SECURITIES.GenMessage53X(P_DRAFTID => p_DraftID,
                                             P_IDPAYM  => l_IDPaym,
                                             P_KINDMSG => p_KindMsg,
                                             P_FUNCMSG => l_FuncMsg,
                                             P_PAGE    => l_c.f_28e,
                                             P_IDTMP   => l_c.id,
                                             P_TEXT    => l_Text);
       IF l_Msg IS NULL THEN
         p_Text := 'Неверные параметры поручения '||p_DraftID||': '||l_Text;
         Return 2;
       END IF;
     --Заполнение "хвостовой" таблицы
       RSP_PAYM_API.Crt_MTRec(P_IDPAYM  => l_IDPAYM,
                              P_MSG     => l_Msg,
                              P_KINDMSG => p_KindMsg);
     --Создание ассоциации платежа с поручением
       RSP_PAYM_API.Crt_PaymAssociate(P_ASS      => p_DraftID,
                                      P_IDPAYM   => l_IDPaym,
                                      P_IDTYPASS => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SASS',
                                                                                   P_CODE => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Ассоциация')),
                                      P_BIC      => 2);
     END LOOP;
   --Результат
     Return 0;
   end Crt_MT53x;
   function CRT_MT518 (P_DRAFTID in integer,P_KINDMSG in varchar2,P_TEXT out varchar2,P_NUMREFER out varchar2,P_DATEREFER out date) return INTEGER as
   /*Функция создает платеж с сообщением типа MT518 по указанному поручению в Payments.
     p_DraftID - идентификатор поручения,
     p_KindMsg - тип сообщения,
     p_Text    - информация о создании платежа.
     Возвращает код результата выполнения.
   */
     l_Cnt       INTEGER;
     l_FuncMsg   VARCHAR2 (4);
     l_IDPaym    INTEGER;
     l_Text      VARCHAR2 (8000);
     l_Msg       CLOB;
     l_IDKart    INTEGER;
     l_Form      INTEGER;
     l_IDPack    INTEGER;
   begin
   --Проверка наличия сообщений
     SELECT count(p.id_paym)
       INTO l_Cnt
       FROM paym p,
            payass a,
            nform n,
            mtrec m
      WHERE p.id_paym = a.id_paym
        AND a.associate = TO_CHAR (p_DraftID)
        AND a.bankid = nvl(g_DraftKind, 3)
        AND p.id_nform = n.id_nform
        AND n.code = RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Формы\MT518')
        AND p.id_paym = m.id_paym
        AND m.type = p_KindMsg
        AND p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE   => 'SSTATE',
                                                          P_CODE    => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат'));
     IF l_cnt = 0 THEN
       l_FuncMsg := 'NEWM';
     --Создание платежа
       l_IDPaym := RSP_SECURITIES.CRT_PAYM(P_DRAFTID   => p_DraftID,
                                           P_KINDMSG   => p_KindMsg,
                                           P_TEXT      => p_Text,
                                           p_NumRefer  => p_NumRefer,
                                           p_DateRefer => p_DateRefer);
       p_Text := p_Text||' '||l_Text;
       IF l_IDPaym IS NULL THEN
         Return 2;
       END IF;
     --Получение сообщения
       l_Msg := RSP_SECURITIES.GENMESSAGE518(P_DRAFTID => p_DraftID,
                                             P_IDPAYM  => l_IDPaym,
                                             P_KINDMSG => p_KindMsg,
                                             P_FUNCMSG => l_FuncMsg,
                                             P_PAGE    => NULL,
                                             P_TEXT    => l_Text);
       IF l_Msg IS NULL THEN
         p_Text := 'Неверные параметры поручения '||p_DraftID||': '||l_Text;
         Return 2;
       END IF;
     --Заполнение "хвостовой" таблицы
       RSP_PAYM_API.Crt_MTRec(P_IDPAYM  => l_IDPAYM,
                              P_MSG     => l_Msg,
                              P_KINDMSG => p_KindMsg);
     --Создание ассоциации платежа с поручением
       RSP_PAYM_API.Crt_PaymAssociate(P_ASS      => p_DraftID,
                                      P_IDPAYM   => l_IDPaym,
                                      P_IDTYPASS => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SASS',
                                                                                   P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Ассоциация')),
                                      P_BIC      => nvl(g_DraftKind, 3));
     --Создание пакета
       SELECT n.id_nkart
         INTO l_IDKart
         FROM nkart n
        WHERE n.namekart = 'Ценные бумаги';
       SELECT f.code
         INTO l_Form
         FROM nform f, ntable n
        WHERE f.id_ntable = n.id_ntable AND n.name = 'IPSPCK';
       l_IDPack := RSP_PAYM_API.PackCreation(P_IDPAYM   => l_IDPaym,
                                             P_REFER    => RSP_REF.MakeRefer(P_FORMREF => l_Form,
                                                                             P_TYPEREF => NULL),
                                             P_IDSTATE  => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTPCK',
                                                                                          P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния пакета\Сформирован запрос')),
                                             P_NUMREFER => null,
                                             P_IDKART   => l_IDKart);
       IF l_IDPack IS NOT NULL THEN
         p_Text := p_Text||' Создан пакет '||l_IDPack||'.';
       END IF;
     --Результат
       Return 0;
     ELSE
       p_Text := 'По сделке ID='||p_DraftID||' уже есть инструкция';
       Return 1;
     END IF;
   end Crt_MT518;
   function CRT_MT599 (P_DRAFTID in integer,P_KINDMSG in varchar2,P_TEXT out varchar2,P_NUMREFER out varchar2,P_DATEREFER out date) return INTEGER as
   /*Функция создает платеж с сообщением типа MT599 по указанному поручению в Payments.
     p_DraftID - идентификатор поручения,
     p_KindMsg - тип сообщения,
     p_Text    - информация о создании платежа.
     Возвращает код результата выполнения.
   */
     l_Cnt       INTEGER;
     l_IDPaym    INTEGER;
     l_Text      VARCHAR2(8000);
     l_Msg       CLOB;
     l_IDKart    INTEGER;
     l_Form      INTEGER;
     l_IDPack    INTEGER;
     l_Attribute VARCHAR2(20);
     l_Client    VARCHAR2(4000);
   begin
   --Проверка наличия сообщений
     SELECT COUNT (p.id_paym)
       INTO l_Cnt
       FROM paym p,
            payass a,
            nform n,
            mtn9x m
      WHERE p.id_paym = a.id_paym
        AND a.associate = TO_CHAR (p_DraftID)
        AND a.bankid = nvl(g_DraftKind, 4)
        AND p.id_nform = n.id_nform
        AND n.code = RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Формы\MT599')
        AND p.id_paym = m.id_paym
        AND m.TYPE = p_KindMsg
        AND p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                          P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат'));
     IF l_cnt = 0 THEN
     --Создание платежа
       l_IDPaym := RSP_SECURITIES.CRT_PAYM(P_DRAFTID   => p_DraftID,
                                           P_KINDMSG   => p_KindMsg,
                                           P_TEXT      => p_Text,
                                           p_NumRefer  => p_NumRefer,
                                           p_DateRefer => p_DateRefer);
       p_Text := p_Text||' '||l_Text;
       IF l_IDPaym IS NULL THEN
         Return 2;
       END IF;
     --Получение сообщения
       l_Msg := RSP_SECURITIES.GENMESSAGE599(P_DRAFTID   => p_DraftID,
                                             P_IDPAYM    => l_IDPaym,
                                             P_NETT      => 0,
                                             p_Attribute => l_Attribute,
                                             p_Client    => l_Client);
       IF l_Msg IS NULL THEN
         p_Text := 'Неверные параметры поручения '||p_DraftID||': '||l_Text;
         Return 2;
       END IF;
     --Заполнение "хвостовой" таблицы
       RSP_PAYM_API.Crt_MTn9x(P_IDPAYM  => l_IDPAYM,
                              P_MSG     => l_Msg,
                              P_KINDMSG => p_KindMsg);
     --Создание ассоциации платежа с поручением
       RSP_PAYM_API.Crt_PaymAssociate(P_ASS      => p_DraftID,
                                      P_IDPAYM   => l_IDPaym,
                                      P_IDTYPASS => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE   => 'SASS',
                                                                                   P_CODE    => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Ассоциация')),
                                      P_BIC      => nvl(g_DraftKind, 4));
     --Создание пакета
       SELECT n.id_nkart
         INTO l_IDKart
         FROM nkart n
        WHERE n.namekart = 'Ценные бумаги';
       SELECT f.code
         INTO l_Form
         FROM nform f, ntable n
        WHERE f.id_ntable = n.id_ntable AND n.name = 'IPSPCK';
       l_IDPack := RSP_PAYM_API.PackCreation(P_IDPAYM   => l_IDPaym,
                                             P_REFER    => RSP_REF.MakeRefer(P_FORMREF => l_Form,
                                                                             P_TYPEREF => NULL),
                                             P_IDSTATE  => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTPCK',
                                                                                          P_CODE => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния пакета\Сформирован запрос')),
                                             P_NUMREFER => null,
                                             P_IDKART   => l_IDKart);
       IF l_IDPack IS NOT NULL THEN
         p_Text := p_Text||' Создан пакет '||l_IDPack||'.';
       END IF;
     --Результат
       Return 0;
     ELSE
       p_Text := 'По сделке ID='||p_DraftID||' уже есть инструкция';
       Return 1;
     END IF;
   end Crt_MT599;
   function CRT_MT599NETT (P_DRAFTID in integer,P_KINDMSG in varchar2,P_TEXT out varchar2,P_ROWNUM in integer) return INTEGER as
   /*Функция создает платеж с сообщением типа MT599 по указанному поручению в Payments.
     p_DraftID - идентификатор поручения,
     p_KindMsg - тип сообщения,
     p_Text    - информация о создании платежа.
     Возвращает код результата выполнения.
   */
     l_IDPaym    INTEGER;
     l_Text      VARCHAR2(8000);
     l_Msg       CLOB;
     l_Attribute VARCHAR2(20);
     l_Client    VARCHAR2(4000);
   begin
   --Создание платежа
     l_IDPaym := RSP_SECURITIES.CRT_PAYMNETT(P_DRAFTID => p_DraftID,
                                             P_TEXT    => p_Text,
                                             P_ROWNUM  => p_RowNum);
     p_Text := p_Text||' '||l_Text;
     IF l_IDPaym IS NULL THEN
       Return 2;
     END IF;
   --Получение сообщения
     l_Msg := RSP_SECURITIES.GENMESSAGE599(P_DRAFTID   => p_DraftID,
                                           P_IDPAYM    => l_IDPaym,
                                           P_NETT      => 1,
                                           p_Attribute => l_Attribute,
                                           p_Client    => l_Client,
                                           P_RN        => p_RowNum);
     IF l_Msg IS NULL THEN
       p_Text := 'Неверные параметры поручения '||p_DraftID||': '||l_Text;
       Return 2;
     END IF;
   --Заполнение "хвостовой" таблицы
     RSP_PAYM_API.Crt_MTn9x(P_IDPAYM    => l_IDPAYM,
                            P_MSG       => l_Msg,
                            P_KINDMSG   => p_KindMsg,
                            p_Attribute => l_Attribute,
                            p_Client    => l_Client);
   --Создание ассоциации платежа с поручением
     RSP_PAYM_API.Crt_PaymAssociate(P_ASS      => p_DraftID,
                                    P_IDPAYM   => l_IDPaym,
                                    P_IDTYPASS => RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SASS',
                                                                                 P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Ассоциация')),
                                    P_BIC      => nvl(g_DraftKind, 7));
   --Результат
     Return 0;
   end Crt_MT599Nett;
   function CRT_PAYM (P_DRAFTID in integer,P_KINDMSG in varchar2,P_TEXT out varchar2,P_NUMREFER out varchar2,P_DATEREFER out date) return INTEGER as
   /*Создание платежа для сообщения.
     p_DraftID - идентификатор поручения,
     p_KindMsg - тип сообщения,
     p_Text    - информация о создании платежа.
     Возвращает идентификатор созданного платежа.
   */
     l_TypBIC    INTEGER;
     l_DealSum   NUMBER;
     l_Refer     VARCHAR2(30);
     l_sql_stmt  VARCHAR2(1024);
     l_BIC       VARCHAR2(100);
     l_IDFi      INTEGER;
     l_NKorr     INTEGER;
     l_Nks       INTEGER;
     l_IDPaym    INTEGER;
     f1          INTEGER;
     l_State     INTEGER := CASE g_Transport WHEN 'SWIFT' THEN 7902
                                             WHEN 'СПФС' THEN 7908
                                             WHEN 'TELEX' THEN 7909
                            END;
   begin
   --Определение типа БИК-а
     l_TypBIC := RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'STBIC',
                                                P_CODE  => 20);
     --Определение получателя
       l_sql_stmt := 'SELECT nvl(v.t_ReceiverBIC, chr(1)),
                             sf.id_sfi,
                             '||CASE WHEN p_KindMsg IN ('535', '536') THEN 'v.t_numrep'
                                     ELSE 'substr(v.t_DealID, instr(v.t_DealID, ''/'') + 1)'
                                END||'
                        FROM '||CASE WHEN p_KindMsg = '535' THEN 'AccBalance'
                                     WHEN p_KindMsg = '536' THEN 'AccTransactions'
                                     WHEN p_KindMsg = '518' THEN 'DRAFTSINFO1'
                                     WHEN p_KindMsg = '599' THEN 'DRAFTSINFO1'
                                     ELSE 'DRAFTSINFO'
                                END||' v,
                             sfi sf
                       WHERE v.t_DraftID = :DraftID
                         AND sf.strcode = replace(v.t_issuenomcurrency, ''RUR'', ''RUB'')
                        '||CASE WHEN p_KindMsg IN(518, 599)
                                 THEN ' AND v.t_rownum = 1'
                            END;
       EXECUTE IMMEDIATE l_sql_stmt INTO l_BIC, l_IDFi, l_Refer USING p_DraftID;
       IF l_BIC = chr(1) THEN
         p_Text := 'Не задан BIC ISO (SWIFT) получателя.';
          Return NULL;
       END IF;
     BEGIN
     --Определение корсхемы
       SELECT n.id_nkorr, k.id_nks
         INTO l_NKorr, l_Nks
         FROM nkorr n,
              (select k.id_nkorr, k.id_nks
                 from nks k
                where k.code = 1
                  and k.id_sfi = l_IDFi) k,
              nroute r
        WHERE r.bic = l_BIC
          AND n.id_nkorr = r.id_nkorr
          AND k.id_nkorr(+) = n.id_nkorr
          AND rownum = 1;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         IF l_NKorr IS NULL THEN
         --Создание корсхемы
           RSP_PAYM_API.Crt_KorrKs(P_CODEKORR => 0,
                                 P_NAMEKORR => 'Клиент '||l_BIC,
                                   P_PRIOR    => 10,
                                 P_BIC      => l_BIC,
                                   P_TYPBIC   => l_TypBIC,
                                   P_IDFI     => l_IDFi,
                                   P_CODEKS   => 1,
                                   P_NAMEKS   => CASE WHEN p_KindMsg IN('535', '536', '518', '599')
                                                      THEN 'Расчеты с клиентом в ц/б.'
                                                      ELSE 'Расчеты по ц/б.'
                                                 END,
                                   P_FRNSYS   => 'SWIFTRUS9',
                                   P_LORO     => CASE WHEN p_KindMsg IN('535', '536', '518', '599')
                                                      THEN 1
                                                      ELSE 0
                                                 END,
                                   P_IDKORR   => l_NKorr,
                                   P_IDKS     => l_NKs);
         END IF;
     END;
   --Определение суммы
     l_sql_stmt := 'SELECT v.t_DealSum
                      FROM '||CASE WHEN p_KindMsg = '518' THEN 'DRAFTSINFO1'
                                   WHEN p_KindMsg = '599' THEN 'DRAFTSINFO1'
                                   ELSE 'DRAFTSINFO'
                              END||' v
                     WHERE v.t_DraftID = :DraftID
                           '||CASE WHEN p_KindMsg IN(518, 599)
                                   THEN ' AND v.t_rownum = 1'
                              END;
     EXECUTE IMMEDIATE l_sql_stmt INTO l_DealSum USING p_DraftID;
   --Создание платежа
     INSERT INTO paym(nodeid,
                      id_nform,
                      id_spclass,
                      id_sfi,
                      amount,
                      payerbankid,
                      id_stbicpayer,
                      receiverbankid,
                      id_stbicreceiver,
                      id_sstate,
                      numreferdoc,
                      numpack,
                      datereferdoc,
                      valuedate,
                      currstatedate,
                      inputdate,
                      id_nkorroutput,
                      id_nksoutput,
                      id_sfioutput,
                      dateoutput,
                      priority)
       VALUES(nvl(RSP_SETUP.GET_VALUE(P_PATH => 'Настройки инициализации\OWNNODE'), 100), -- dan необходимо разобраться
              RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'NFORM',
                                             P_CODE  => CASE WHEN p_KindMsg IN ('535', '536') THEN RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Формы\Выписка')
                                                             WHEN p_KindMsg = '518' THEN RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Формы\MT518')
                                                             WHEN p_KindMsg = '599' THEN RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Формы\MT599')
                                                             ELSE RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Формы\Инструкция')
                                                        END),
              RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SPCLASS',
                                             P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Класс платежа')),
              l_IDFi,
              l_DealSum,
              (SELECT r.bic
                 FROM nroute r
                WHERE r.id_nkorr IS NULL AND r.id_stbic = l_TypBIC),
              l_TypBIC,
              l_BIC,
              l_TypBIC,
              RSP_COMMON.GET_ID_BY_CODE_DICT('SSTATE', l_State),
              l_Refer,
              8888,
              trunc(sysdate, 'DD'),
              sysdate,
              sysdate,
              sysdate,
              l_NKorr,
              l_Nks,
              l_IDFi,
              sysdate,
              6)
     RETURNING id_paym, numreferdoc, datereferdoc INTO l_IDPaym, p_NumRefer, p_DateRefer;
   --Создание истории платежа
     DELETE FROM tmp_paym_pack;
     INSERT INTO tmp_paym_pack(id_paym, id_sstate)
       SELECT l_IDPaym, id_sstate
         FROM paym
        WHERE id_paym = l_IDPaym;
     IF SQL%ROWCOUNT > 0 THEN
       f1 := RSP_GATE.MASSCHANGESTATE(P_TBLNAME => 'TMP_PAYM_PACK',
                                      P_UPD     => 1);
       IF f1 > 0 THEN
         p_Text := 'Создан платеж '||l_IDPaym||': № '||p_NumRefer||' от '||to_char(p_DateRefer, 'dd.mm.yyyy')||'.';
       --Передача состояния в АБС
         RSB_PAYMENTS_API.InsertDLMesParams(p_DocKind   => g_DraftKind,
                                            p_DocID     => p_DraftID,
                                            p_Status    => 0,
                                            p_Condition => l_State);
         Return l_IDPaym;
       END IF;
     ELSE
       Return NULL;
     END IF;
   end Crt_Paym;
   function CRT_PAYMNETT (P_DRAFTID in integer,P_TEXT out varchar2,P_ROWNUM in integer) return INTEGER as
   /*Создание платежа для сообщения.
     p_DraftID - идентификатор поручения,
     p_Text    - информация о создании платежа.
     Возвращает идентификатор созданного платежа.
   */
     l_TypBIC    INTEGER;
     l_DealSum   NUMBER;
     l_BIC       VARCHAR2(100);
     l_IDFi      INTEGER;
     l_IDPaym    INTEGER;
     f1          INTEGER;
     l_NumRefer  VARCHAR2(20);
     l_DateRefer DATE;
   begin
   --Определение типа БИК-а
     l_TypBIC := RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'STBIC',
                                                P_CODE  => 20);
   --Определение получателя
     SELECT nvl(v.t_ReceiverBIC, chr(1)),
            sf.id_sfi
       INTO l_BIC, l_IDFi
       FROM DRAFTSINFO1 v,
            nkorr n,
            nroute r,
            sfi sf
      WHERE v.t_DraftID = p_DraftID
        AND v.t_rownum = p_RowNum
        AND r.bic(+) = v.t_receiverbic
        AND n.id_nkorr(+) = r.id_nkorr
        AND sf.strcode = replace(v.t_issuenomcurrency, 'RUR', 'RUB');
     IF l_BIC = chr(1) THEN
       p_Text := 'Не задан БИК получателя.';
       Return NULL;
     END IF;
   --Определение суммы
     SELECT v.t_DealSum
       INTO l_DealSum
       FROM DRAFTSINFO1 v
      WHERE v.t_DraftID = p_DraftID
        AND v.t_rownum = p_RowNum;
   --Создание платежа
     INSERT INTO paym(nodeid,
                      id_nform,
                      id_spclass,
                      id_sfi,
                      amount,
                      payerbankid,
                      id_stbicpayer,
                      receiverbankid,
                      id_stbicreceiver,
                      id_sstate,
                      numreferdoc,
                      numpack,
                      datereferdoc,
                      valuedate,
                      currstatedate,
                      inputdate,
                      priority)
       VALUES(RSP_SETUP.GET_VALUE(P_PATH => 'Настройки инициализации\OWNNODE'),
              RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'NFORM',
                                             P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Формы\MT599')),
              RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SPCLASS',
                                             P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Класс платежа')),
              l_IDFi,
              l_DealSum,
              (SELECT r.bic
                 FROM nroute r
                WHERE r.id_nkorr IS NULL AND r.id_stbic = l_TypBIC),
              l_TypBIC,
              RTRIM(l_BIC, 'XXX'),
              l_TypBIC,
              RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE   => 'SSTATE',
                                             P_CODE    => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Состояния отправки/приема\Отложен')),
              TO_CHAR(RSP_REF.MakeRefer(P_FORMREF => 599, P_TYPEREF => NULL)),
              8888,
              TRUNC(SYSDATE, 'DD'),
              SYSDATE,
              SYSDATE,
              SYSDATE,
              6)
     RETURNING id_paym, numreferdoc, datereferdoc INTO l_IDPaym, l_NumRefer, l_DateRefer;
   --Создание истории платежа
     DELETE FROM tmp_paym_pack;
     INSERT INTO tmp_paym_pack (id_paym, id_sstate)
       SELECT l_IDPaym, id_sstate
         FROM paym
        WHERE id_paym = l_IDPaym;
     IF SQL%ROWCOUNT > 0 THEN
       f1 := RSP_GATE.MASSCHANGESTATE(P_TBLNAME => 'TMP_PAYM_PACK',
                                      P_UPD     => 1);
       IF f1 > 0 THEN
         p_Text := 'Создан платеж ID='||l_IDPaym||': № '||l_NumRefer||' от '||to_char(l_DateRefer, 'dd.mm.yyyy')||'.';
         Return l_IDPaym;
       END IF;
     ELSE
       Return NULL;
     END IF;
   end Crt_PaymNett;
function CRT_PAYMSWIFT (P_KINDMSG in varchar2,P_MSG in clob,P_RECEIVBIC in varchar2,P_TEXT out varchar2) return INTEGER as
   /*Создание платежа для сообщения.
     p_DraftID - идентификатор поручения,
     p_KindMsg - тип сообщения,
     p_Text    - информация о создании платежа.
     Возвращает идентификатор созданного платежа.
   */
     l_TypBIC    INTEGER;
     l_BIC       VARCHAR2(100);
     l_IDFi      INTEGER;
     l_NKorr     INTEGER;
     l_Nks       INTEGER;
     l_IDPaym    INTEGER;
     l_NumRefer  VARCHAR2(20);
     l_DateRefer DATE;
     f1          INTEGER;
     l_State     INTEGER := CASE g_Transport WHEN 'SWIFT' THEN 7902
                                             WHEN 'СПФС' THEN 7908
                                             WHEN 'TELEX' THEN 7909
                            END;
   begin
   --Определение типа БИК-а
     l_TypBIC := RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'STBIC',
                                                P_CODE  => 20);
     IF p_KindMsg = 392 THEN
       g_KindMsg := RSP_SECURITIES.GETVALUEFIELD (P_MSG => p_Msg, P_FIELD => ':11S:');
     END IF;
     IF g_KindMsg IN('300', '320', '360', '361') THEN
       SELECT sf.id_sfi
         INTO l_IDFi
         FROM sfi sf
        WHERE sf.strcode = substr(RSP_SECURITIES.GETVALUEFIELD (P_MSG => p_Msg, P_FIELD => ':32B:'),1,3);
     ELSIF g_KindMsg IN('362') THEN
       SELECT sf.id_sfi
         INTO l_IDFi
         FROM sfi sf
        WHERE sf.strcode = substr(RSP_SECURITIES.GETVALUEFIELD (P_MSG => p_Msg, P_FIELD => ':33F:'),1,3);
     END IF;
   --Определение получателя
     IF P_RECEIVBIC IS NULL THEN
       p_Text := 'Не задан BIC ISO (SWIFT) получателя.';
       Return NULL;
     ELSE
       l_BIC := P_RECEIVBIC;
     END IF;
     BEGIN
     --Определение корсхемы
       SELECT n.id_nkorr, k.id_nks
         INTO l_NKorr, l_Nks
         FROM nkorr n,
              (select k.id_nkorr, k.id_nks
                 from nks k
                where k.code = 1
                  and k.id_sfi = nvl(l_IDFi, 1)) k,
              nroute r
        WHERE r.bic = l_BIC
          AND n.id_nkorr = r.id_nkorr
          AND k.id_nkorr(+) = n.id_nkorr
          AND rownum = 1;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         IF l_NKorr IS NULL OR l_Nks IS NULL THEN
         --Создание корсхемы
           RSP_PAYM_API.Crt_KorrKs(P_CODEKORR => 0,
                                   P_NAMEKORR => 'Клиент '||l_BIC,
                                   P_PRIOR    => 10,
                                   P_BIC      => l_BIC,
                                   P_TYPBIC   => l_TypBIC,
                                   P_IDFI     => nvl(l_IDFi, 1),
                                   P_CODEKS   => 1,
                                   P_NAMEKS   => 'Подтверждения сделки',
                                   P_FRNSYS   => 'SWIFTRUS9',
                                   P_LORO     => 0,
                                   P_IDKORR   => l_NKorr,
                                   P_IDKS     => l_NKs);
         END IF;
     END;
   --Создание платежа
     INSERT INTO paym(nodeid,
                      id_nform,
                      id_spclass,
                      payerbankid,
                      id_stbicpayer,
                      receiverbankid,
                      id_stbicreceiver,
                      id_sstate,
                      numreferdoc,
                      numpack,
                      datereferdoc,
                      valuedate,
                      currstatedate,
                      inputdate,
                      id_nkorroutput,
                      id_nksoutput,
                      id_sfioutput,
                      dateoutput,
                      priority,
                      id_sfi)
       VALUES(RSP_SETUP.GET_VALUE(P_PATH => 'Настройки инициализации\OWNNODE'),
              CASE WHEN P_KINDMSG LIKE '%99'
                   THEN RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'NFORM',
                                                       P_CODE  => 6000)
                   WHEN P_KINDMSG LIKE '3%'
                   THEN RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'NFORM',
                                                       P_CODE  => 1300)
                   WHEN P_KINDMSG LIKE '6%'
                   THEN RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'NFORM',
                                                       P_CODE  => 1600)
              END,
              RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SPCLASS',
                                             P_CODE  => 3),
              (SELECT r.bic
                 FROM nroute r
                WHERE r.id_nkorr IS NULL
                  AND r.id_stbic = l_TypBIC),
              l_TypBIC,
              l_BIC,
              l_TypBIC,
              RSP_COMMON.GET_ID_BY_CODE_DICT('SSTATE', l_State),
              RSP_SECURITIES.GETVALUEFIELD(P_MSG   => p_Msg,
                                           P_FIELD => ':20:'),
              8888,
              TRUNC(SYSDATE, 'DD'),
              SYSDATE,
              SYSDATE,
              SYSDATE,
              l_NKorr,
              l_Nks,
              l_IDFi,
              SYSDATE,
              6,
              l_IDFi)
       RETURNING id_paym, numreferdoc, datereferdoc INTO l_IDPaym, l_NumRefer, l_DateRefer;
     --Создание истории платежа
     DELETE FROM tmp_paym_pack;
     INSERT INTO tmp_paym_pack (id_paym, id_sstate)
        SELECT l_IDPaym, id_sstate
          FROM paym
         WHERE id_paym = l_IDPaym;
     IF SQL%ROWCOUNT > 0 THEN
       f1 := RSP_GATE.MASSCHANGESTATE(P_TBLNAME => 'TMP_PAYM_PACK',
                                      P_UPD     => 1);
       IF f1 > 0 THEN
         p_Text := 'Создано подтверждение ID:'||l_IDPaym||': № '||l_NumRefer||' от '||TO_CHAR(l_DateRefer, 'dd.mm.yyyy')||'.';
      --Передача состояния в АБС
        RSB_PAYMENTS_API.InsertDLMesParams(p_DocKind   => g_DraftKind,
                                           p_DocID     => g_DraftID,
                                           p_Status    => 0,
                                           p_Condition => CASE WHEN p_KindMsg = 392
                                                               THEN 0
                                                               ELSE l_State
                                                          END);
         Return l_IDPaym;
       END IF;
     ELSE
       Return NULL;
     END IF;
   end Crt_PaymSWIFT;
   function GENMESSAGE518 (P_DRAFTID in integer,P_IDPAYM in integer,P_KINDMSG in varchar2,P_FUNCMSG in varchar2,P_PAGE in varchar2,P_TEXT out varchar2) return CLOB as
   /*Функция генерирует текст сообщением по указанному поручению в Payments.
     p_DraftID - идентификатор поручения,
     p_IDPaym  - идентификатор платежа,
     p_KindMsg - тип сообщения,
     p_FuncMsg - функция сообщения,
     p_Page    - номер/код страницы сообщения,
     p_Text    - информация о создании платежа.
     Возвращает текст сообщения.
   */
     l_sql_stmt VARCHAR2(32000);
     l_Genl     CLOB;
     l_Setd     CLOB;
     l_Conf     CLOB;
     l_Text     VARCHAR2(4000);
     l_Msg      CLOB;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENMESSAGE');
     l_Genl := RSP_SECURITIES.GenBlocGENL(P_DRAFTID => p_DraftID,
                                          P_IDPAYM  => p_IDPaym,
                                          P_KINDMSG => p_KindMsg,
                                          P_FUNCMSG => p_FuncMsg,
                                          P_PAGE    => p_Page,
                                          P_TEXT    => l_Text);
     IF l_Genl IS NULL THEN
       p_Text := p_Text||' '||l_Text;
       Return NULL;
     END IF;
     l_Conf := RSP_SECURITIES.GenBlocCONFDET(P_DRAFTID => p_DraftID,
                                             P_TEXT    => l_Text);
     IF l_Conf IS NULL THEN
       p_Text := p_Text||' '||l_Text;
       Return NULL;
     END IF;
     l_Setd := RSP_SECURITIES.GenBlocSETDET518(P_DRAFTID => p_DraftID,
                                               P_TEXT    => l_Text);
     p_Text := p_Text||' '||l_Text;
   --Создание сообщения
     l_sql_stmt := 'SELECT '||l_Genl||'||chr(13)||'||chr(13)
                            ||l_Conf||'||chr(13)||'||chr(13)
                            ||CASE WHEN l_Setd IS NOT NULL
                                   THEN l_Setd
                              END
                            ||'decode(v.t_Repo, 1, '||RSP_SECURITIES.GenBlocREPO||')
                      FROM DRAFTSINFO1 v, paym p
                     WHERE v.t_DraftID = :DraftID
                       AND p.id_paym = :IDPaym
                       AND v.t_rownum = 1';
   dbms_output.put_line(substr(l_sql_stmt, 1, 4000));
   dbms_output.put_line(substr(l_sql_stmt, 4001, 4000));
   dbms_output.put_line(substr(l_sql_stmt, 8001, 4000));
   dbms_output.put_line(substr(l_sql_stmt, 12001, 4000));
   dbms_output.put_line(substr(l_sql_stmt, 16001, 4000));
     EXECUTE IMMEDIATE l_sql_stmt INTO l_Msg USING p_DraftID, p_IDPaym;
     Return l_Msg;
   end GenMessage518;
   function GENMESSAGE53X (P_DRAFTID in integer,P_IDPAYM in integer,P_KINDMSG in varchar2,P_FUNCMSG in varchar2,P_PAGE in varchar2,P_IDTMP in integer,P_TEXT out varchar2) return CLOB as
   /*Функция генерирует текст сообщением по указанному поручению в Payments.
     p_DraftID - идентификатор поручения,
     p_IDPaym  - идентификатор платежа,
     p_KindMsg - тип сообщения,
     p_FuncMsg - функция сообщения,
     p_Page    - номер/код страницы сообщения,
     p_IDTMP   - идентификатор строки временной таблицы,
     p_Text    - информация о создании платежа.
     Возвращает текст сообщения.
   */
     l_sql_stmt VARCHAR2(32000);
     l_Genl     CLOB;
     l_Text     VARCHAR2(4000);
     l_Msg      CLOB;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENMESSAGE');
     l_Genl := RSP_SECURITIES.GenBlocGENL(P_DRAFTID => p_DraftID,
                                          P_IDPAYM  => p_IDPaym,
                                          P_KINDMSG => p_KindMsg,
                                          P_FUNCMSG => p_FuncMsg,
                                          P_PAGE    => p_Page,
                                          P_TEXT    => l_Text);
     p_Text := p_Text||' '||l_Text;
   --Создание сообщения
     l_sql_stmt := 'SELECT '||l_Genl||'||
                           decode(v.t_ExtActi, ''Y'', t.subsafe)
                      FROM '||CASE WHEN p_KindMsg = '535'
                                   THEN 'AccBalance'
                                   ELSE 'AccTransactions'
                              END||' v, paym p, tmp_mt53x t
                      WHERE p.id_paym = :IDPaym AND t.id = :IDTMP';
   --dbms_output.put_line(l_sql_stmt);
     EXECUTE IMMEDIATE l_sql_stmt INTO l_Msg USING p_IDPaym, p_IDTMP;
     Return l_Msg;
   end GenMessage53X;
   function GENMESSAGE54X (P_DRAFTID in integer,P_IDPAYM in integer,P_KINDMSG in varchar2,P_FUNCMSG in varchar2,P_PAGE in varchar2,P_TEXT out varchar2) return CLOB as
   /*Функция генерирует текст сообщением по указанному поручению в Payments.
     p_DraftID - идентификатор поручения,
     p_IDPaym  - идентификатор платежа,
     p_KindMsg - тип сообщения,
     p_FuncMsg - функция сообщения,
     p_Page    - номер/код страницы сообщения,
     p_Text    - информация о создании платежа.
     Возвращает текст сообщения.
   */
     l_sql_stmt VARCHAR2(32000);
     l_Genl     CLOB;
     l_Trad     CLOB;
     l_Fiac     CLOB;
     l_Setd     CLOB;
     l_Text     VARCHAR2(4000);
     l_Msg      CLOB;
     l_Qual     VARCHAR2(4);
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENMESSAGE');
     l_Genl := RSP_SECURITIES.GenBlocGENL(P_DRAFTID => p_DraftID,
                                          P_IDPAYM  => p_IDPaym,
                                          P_KINDMSG => p_KindMsg,
                                          P_FUNCMSG => p_FuncMsg,
                                          P_PAGE    => p_Page,
                                          P_TEXT    => l_Text);
     IF l_Genl IS NULL THEN
       p_Text := p_Text||' '||l_Text;
       Return NULL;
     END IF;
     l_Trad := RSP_SECURITIES.GenBlocTRADDET(P_DRAFTID => p_DraftID,
                                             P_TEXT    => l_Text);
     p_Text := p_Text||' '||l_Text;
     IF P_KINDMSG IN('540', '541') THEN
       l_Qual := 'REAG';
     ELSIF P_KINDMSG IN('542', '543') THEN
       l_Qual := 'DEAG';
     END IF;
     l_Fiac := RSP_SECURITIES.GenBlocFIAC(P_DRAFTID => p_DraftID,
                                          P_QUAL    => l_Qual,
                                          P_TEXT    => l_Text);
     p_Text := p_Text||' '||l_Text;
     l_Setd := RSP_SECURITIES.GenBlocSETDET(P_DRAFTID => p_DraftID,
                                            P_KINDMSG => p_KindMsg,
                                            P_TEXT    => l_Text);
     p_Text := p_Text||' '||l_Text;
     l_sql_stmt := 'SELECT '||l_Genl||'||chr(13)||'
                           ||l_Trad||
                           CASE WHEN l_Fiac IS NOT NULL
                                THEN '||chr(13)||'||l_Fiac
                           END||
                           CASE WHEN l_Setd IS NOT NULL
                                THEN '||chr(13)||'||l_Setd
                           END||'
                      FROM DRAFTSINFO v,
                           paym p
                     WHERE v.t_DraftID = :DraftID
                       AND p.id_paym = :IDPaym';
   dbms_output.put_line(substr(l_sql_stmt, 1, 4000));
   dbms_output.put_line(substr(l_sql_stmt, 4001, 4000));
   dbms_output.put_line(substr(l_sql_stmt, 8001, 4000));
   dbms_output.put_line(substr(l_sql_stmt, 12001, 4000));
   dbms_output.put_line(substr(l_sql_stmt, 16001, 4000));
     EXECUTE IMMEDIATE l_sql_stmt INTO l_Msg USING p_DraftID, p_IDPaym;
     Return l_Msg;
   end GenMessage54X;
   function GENMESSAGE599 (P_DRAFTID in integer,P_IDPAYM in integer,P_NETT in integer default 0,P_ATTRIBUTE out varchar2,P_CLIENT out varchar2,P_RN in integer default 1) return CLOB as
   /*Функция генерирует текст сообщением по указанному поручению в Payments.
     p_DraftID - идентификатор поручения,
     p_IDPaym  - идентификатор платежа,
     p_Text    - информация о создании платежа.
     Возвращает текст сообщения.
   */
     l_sql_stmt VARCHAR2(32000);
     l_Fl79     CLOB;
     l_Msg      CLOB;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENMESSAGE');
     IF p_Nett = 0 THEN
       l_Fl79 := RSP_SECURITIES.GENFIELD79;
     ELSE
       l_Fl79 := RSP_SECURITIES.GENFIELD79Nett;
     END IF;
   --Создание сообщения
     l_sql_stmt := 'SELECT '':20C:''||p.numreferdoc||chr(13)||
                           '''||l_Fl79||''',
                           v.t_Attribute,
                           v.t_Client
                      FROM DRAFTSINFO1 v, paym p
                     WHERE v.t_DraftID = :DraftID
                       AND p.id_paym = :IDPaym
                       AND v.t_rownum = :RN';
   dbms_output.put_line(substr(l_sql_stmt, 1, 4000));
   dbms_output.put_line(substr(l_sql_stmt, 4001, 4000));
   dbms_output.put_line(substr(l_sql_stmt, 8001, 4000));
   dbms_output.put_line(substr(l_sql_stmt, 12001, 4000));
   dbms_output.put_line(substr(l_sql_stmt, 16001, 4000));
     EXECUTE IMMEDIATE l_sql_stmt INTO l_Msg, p_Attribute, p_Client USING p_DraftID, p_IDPaym, p_RN;
     Return l_Msg;
   end GenMessage599;
   function GENBLOCGENL (P_DRAFTID in integer,P_IDPAYM in integer,P_KINDMSG in varchar2,P_FUNCMSG in varchar2,P_PAGE in varchar2,P_TEXT out varchar2) return CLOB as
   /*Функция генерирует текст конструкции SELECT.
     p_DraftID - идентификатор поручения,
     p_IDPaym  - идентификатор платежа,
     p_KindMsg - тип сообщения,
     p_FuncMsg - функция сообщения,
     p_Page    - номер/код страницы сообщения,
     p_Text    - информация о создании платежа.
     Возвращает текст конструкции SELECT для генерации блока GENL.
   */
     l_cnt INTEGER;
     l_Msg CLOB;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCGENL');
     IF p_FuncMsg NOT IN('NEWM', 'CANC', 'PREA') THEN
       p_Text := 'неправильная "Функция сообщения"';
     END IF;
   --Проверка наличия референса
     SELECT count(*) INTO l_cnt
       FROM paym p
       WHERE p.id_paym = p_IDPaym;
     IF l_cnt = 0 THEN
       p_Text := p_Text||' отсутствует "Референс сообщения"';
     END IF;
     IF p_KindMsg IN('541', '543') AND g_CatKorr <> 50 THEN
     --Проверка регистрационного номера сделки
       SELECT count(*)
         INTO l_cnt
         FROM DRAFTSINFO v
        WHERE v.t_DealID <> chr(1)
          AND v.t_DraftID = p_DraftID;
       IF l_cnt = 0 THEN
         p_Text := p_Text||' отсутствует "Внешний номер сделки в БО ЦБ"';
       END IF;
     END IF;
     IF p_Text IS NOT NULL THEN
       Return Null;
     ELSE
     --Получение блока GENL
       l_Msg := ''':16R:GENL''||chr(13)||'||
                       CASE WHEN p_KindMsg IN('535', '536')
                            THEN ''':28E:'||p_Page||'''||chr(13)||
                                    CASE WHEN length(v.t_NumRep) > 3 THEN '':13J:''
                                         WHEN length(v.t_NumRep) > 0 THEN '':13A:''
                                    END||'':STAT//''||v.t_NumRep||chr(13)||'
                       END||'
                     '':20C::SEME//''||(substr(v.t_DealID,instr(v.t_DealID,''/'')+1))||''''||chr(13)||
                     '':23G:'||p_FuncMsg||'''||chr(13)||'||
                       CASE WHEN p_KindMsg IN('535', '536')
                            THEN 'decode(length(v.t_PrepDate), 8, '':98A::PREP//''||to_char(v.t_PrepDate, ''YYYYMMDD''),
                                                                  '':98C::PREP//''||to_char(v.t_PrepDate, ''YYYYMMDDHH24MISS''))||chr(13)||'||
                                 CASE WHEN p_KindMsg = '535'
                                      THEN 'decode(length(v.t_StatDate), 8, '':98A::STAT//''||to_char(v.t_StatDate, ''YYYYMMDD''),
                                                                            '':98C::STAT//''||to_char(v.t_StatDate, ''YYYYMMDDHH24MISS''))||chr(13)||'
                                 END||
                                 CASE WHEN p_KindMsg = '536'
                                      THEN 'decode(length(v.t_StatDate), 8, '':69B::STAT//''||to_char(v.t_StatDate, ''YYYYMMDD'')||''/''||
                                                                                              to_char(v.t_EndStatDate, ''YYYYMMDD''),
                                                                            '':69C::STAT//''||to_char(v.t_StatDate, ''YYYYMMDDHH24MISS'')||''/''||
                                                                                              to_char(v.t_EndStatDate, ''YYYYMMDDHH24MISS''))||chr(13)||'
                                 END||'
                                 '':22F::CODE//''||v.t_ReportSize||chr(13)||
                                 '':22F::SFRE//''||v.t_ReportPeriod||chr(13)||'||
                                 CASE WHEN p_KindMsg = '535'
                                      THEN ''':22F::STBA//SETT''||chr(13)||
                                            '':22F::SITY//''||v.t_ExtrType||chr(13)||'
                                 END
                            ELSE CASE WHEN g_CatKorr <> 50
                                      THEN ''':98C::PREP//''||to_char(p.inputdate, ''YYYYMMDDHH24MISS'')||chr(13)||'
                                 END
                       END||
                       CASE WHEN p_KindMsg IN('518')
                            THEN ''':22F:TRTR//TRAD''||chr(13)||'
                       END||
                       CASE WHEN p_KindMsg IN('541', '543') AND g_CatKorr <> 50
                            THEN ''':16R:LINK''||chr(13)||
                                      '':20C::SEME//TRRF/''||(substr(v.t_DealID,instr(v.t_DealID,''/'')+1))||chr(13)||
                                  '':16S:LINK''||chr(13)||'
                       END||
                       CASE WHEN p_KindMsg IN('535', '536')
                            THEN ''':97A::SAFE//''||v.t_SafeDepo||chr(13)||
                                  '':17B::ACTI//''||v.t_ExtActi||chr(13)||
                                  '':17B::CONS//''||v.t_ExtCons||chr(13)||'
                       END||'
                 '':16S:GENL''';
       Return l_Msg;
     END IF;
   end GenBlocGENL;
   function GENBLOCTRADDET (P_DRAFTID in integer,P_TEXT out varchar2) return CLOB as
   /*Функция генерирует текст конструкции SELECT.
     p_DraftID - идентификатор поручения.
     Возвращает текст конструкции SELECT для генерации блока TRADDET.
   */
     l_cnt INTEGER;
     l_Msg CLOB;
   begin
     /*
     SELECT count(*)
       INTO l_cnt
       FROM DRAFTSINFO v
      WHERE nvl(decode(v.T_ISUNIVOPER, 0, v.t_SettDate, v.t_SettDateOff), '01.01.0001') <> '01.01.0001'
        AND v.t_DraftID = p_DraftID;
     IF l_cnt = 0 THEN
       p_Text := 'отсутствует "Дата/время расчетов"';
     END IF;
     */
     SELECT count(*)
       INTO l_cnt
       FROM DRAFTSINFO v
      WHERE nvl(v.t_IssueName, chr(1)) = chr(1)
        AND nvl(v.t_ISIN, chr(1)) = chr(1)
        AND nvl(v.t_LSIN, chr(1)) = chr(1)
        AND nvl(v.t_IssueOthrCode, chr(1)) = chr(1)
        AND v.t_DraftID = p_DraftID;
     IF l_cnt > 0 THEN
       p_Text := p_Text||' отсутствует "Определение финансового инструмента"';
     END IF;
     IF p_Text IS NOT NULL THEN
       Return Null;
     ELSE
     --Получение блока TRADDET
       IF g_CatKorr = 50 THEN
         l_Msg := ''':16R:TRADDET''||chr(13)||
                          CASE WHEN v.T_ISUNIVOPER = 0
                               THEN '':98A::SETT//''||to_char(v.t_SettDate, ''YYYYMMDD'')||chr(13)
                               ELSE '||CASE WHEN g_KindMsg IN(540, 542)
                                            THEN ''':98A::SETT//''||to_char(v.t_SettDateOFF, ''YYYYMMDD'')||chr(13)'
                                            ELSE 'NULL'
                                       END||'
                          END||
                          CASE WHEN v.T_ISUNIVOPER = 0
                               THEN CASE WHEN v.t_DealDate <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                                         THEN '':98A::TRAD//''||to_char(v.t_DealDate, ''YYYYMMDD'')||chr(13)
                                    END
                               ELSE '||CASE WHEN g_KindMsg IN(540, 542)
                                            THEN 'CASE WHEN v.t_DealDateOFF <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                                                       THEN '':98A::TRAD//''||to_char(v.t_DealDateOFF, ''YYYYMMDD'')||chr(13)
                                                  END'
                                            ELSE 'NULL'
                                       END||'
                          END||'||
                          RSP_SECURITIES.GenField35B||
                          case when g_kindMSG in(540,541)
                               then ''':11A::FXIS//''||replace(v.t_issuenomcurrency,''RUR'',''RUB'')||chr(13)||
                                     '':16S:TRADDET'''
                               else ''':11A::FXIB//''||replace(v.t_issuenomcurrency,''RUR'',''RUB'')||chr(13)||
                                    '':16S:TRADDET'''
                          end;
       else
         l_Msg := ''':16R:TRADDET''||chr(13)||
                          CASE WHEN (length(v.t_SettDate) = 8 OR length(v.t_SettDateOFF) = 8)
                               THEN CASE WHEN nvl(v.t_DealID, chr(1)) <> chr(1)
                                         THEN '':98A::SETT//''||to_char(v.t_SettDateOFF, ''YYYYMMDD'')||chr(13)
                                         ELSE '||CASE WHEN g_KindMsg IN(540, 542)
                                                      THEN ''':98A::SETT//''||to_char(v.t_SettDate, ''YYYYMMDD'')||chr(13)'
                                                      ELSE 'NULL'
                                                 END||'
                                    END
                               ELSE '':98C::SETT//''||to_char(v.t_SettDate, ''YYYYMMDDHH24MISS'')||chr(13)
                          END||
                          CASE WHEN v.t_TradType <> chr(1)
                               THEN '':94B::TRAD//''||v.t_TradType||CASE WHEN v.t_MIC <>  chr(1)
                                                                         THEN ''/''||v.t_MIC
                                                                    END||chr(13)
                          END||
                          CASE WHEN v.T_ISUNIVOPER = 0
                               THEN CASE WHEN v.t_DealTime = to_date(''01.01.0001 00:00:00'', ''DD.MM.YYYY HH24:MI:SS'')
                                         THEN CASE WHEN v.t_DealDate = to_date(''01.01.0001'', ''DD.MM.YYYY'')
                                                   THEN '':98A::TRAD//''||to_char(v.t_DealDate, ''YYYYMMDD'')||chr(13)
                                              END
                                         ELSE '':98C::TRAD//''||to_char(v.t_DealDate, ''YYYYMMDD'')||to_char(v.t_DealTime,''HH24MISS'')||chr(13)
                                    END
                               ELSE  '||CASE WHEN g_KindMsg IN(540, 542)
                                             THEN 'CASE WHEN v.t_DealDateOFF <> to_date(''01.01.0001'', ''DD.MM.YYYY'')
                                                        THEN '':98A::TRAD//''||to_char(v.t_DealDateOFF, ''YYYYMMDD'')||chr(13)
                                                   END'
                                             ELSE 'NULL'
                                        END||'
                          END||'||
                          RSP_SECURITIES.GenField35B||
                          'CASE WHEN v.t_Blocked <> chr(1)
                                THEN '':16R:FIA''||chr(13)||
                                         '':70E::FIAN//''||substr(v.t_Blocked, 1, 24)||''''||chr(13)||
                                           decode(substr(v.t_Blocked, 25, 35), NULL, NULL, substr(v.t_Blocked, 25, 35)||chr(13))||
                                           decode(substr(v.t_Blocked, 61, 35), NULL, NULL, substr(v.t_Blocked, 61, 35)||chr(13))||
                                           decode(substr(v.t_Blocked, 97, 35), NULL, NULL, substr(v.t_Blocked, 97, 35)||chr(13))||
                                           decode(substr(v.t_Blocked, 133, 35), NULL, NULL, substr(v.t_Blocked, 133, 35)||chr(13))||
                                           decode(substr(v.t_Blocked, 169, 35), NULL, NULL, substr(v.t_Blocked, 169, 35)||chr(13))||
                                           decode(substr(v.t_Blocked, 205, 35), NULL, NULL, substr(v.t_Blocked, 205, 35)||chr(13))||
                                           decode(substr(v.t_Blocked, 241, 35), NULL, NULL, substr(v.t_Blocked, 241, 35)||chr(13))||
                                           decode(substr(v.t_Blocked, 277, 35), NULL, NULL, substr(v.t_Blocked, 277, 35)||chr(13))||
                                           decode(substr(v.t_Blocked, 313, 35), NULL, NULL, substr(v.t_Blocked, 313, 35)||chr(13))||
                                     '':16S:FIA''||chr(13)
                          END||
                          CASE WHEN v.t_NeedMach <> chr(1)
                               THEN '':70E::SPRO//XX/CORP/''||substr(v.t_ReceiverBIC, 1, 4)||''/''||v.t_NeedMach||''''||chr(13)
                          END||
                   '':16S:TRADDET''';
       END IF;
       Return l_Msg;
     END IF;
   end GenBlocTRADDET;
   function GENBLOCCONFDET (P_DRAFTID in integer,P_TEXT out varchar2) return CLOB as
   /*Функция генерирует текст конструкции SELECT.
     p_DraftID - идентификатор поручения.
     Возвращает текст конструкции SELECT для генерации блока CONFDET.
   */
     l_cnt INTEGER;
     l_Msg CLOB;
   begin
     SELECT count(*)
       INTO l_cnt
       FROM DRAFTSINFO1 v
      WHERE nvl(v.t_SettDate, '01.01.0001') = '01.01.0001'
        AND v.t_DraftID = p_DraftID;
     IF l_cnt > 0 THEN
       p_Text := 'отсутствует "Дата/время расчетов"';
     END IF;
     SELECT count(*)
       INTO l_cnt
       FROM DRAFTSINFO1 v
      WHERE nvl(v.t_IssueName, chr(1)) = chr(1)
        AND nvl(v.t_ISIN, chr(1)) = chr(1)
        AND nvl(v.t_LSIN, chr(1)) = chr(1)
        AND nvl(v.t_IssueOthrCode, chr(1)) = chr(1)
        AND v.t_DraftID = p_DraftID;
     IF l_cnt > 0 THEN
       p_Text := p_Text||' отсутствует "Определение финансового инструмента"';
     END IF;
     IF p_Text IS NOT NULL THEN
       Return Null;
     ELSE
     --Получение блока CONFDET
       l_Msg := ''':16R:CONFDET''||chr(13)||
                        '':98A::TRAD//''||to_char(v.t_DealDate, ''YYYYMMDD'')||chr(13)||
                        '':98A::SETT//''||to_char(v.t_SettDate, ''YYYYMMDD'')||chr(13)||
                        '':90A::DEAL//PRCT/''||decode(sign(v.t_PricePRC), -1, ''-'')||v.t_PricePRC||chr(13)||
                        CASE WHEN v.t_day <> chr(1)
                             THEN '':99A::DAAC//''||decode(rtrim(v.t_day, ''.''), ''365 дней в году / в месяце по календарю'', ''А005'',
                                                                                  ''360 дней в году / 30 дней в месяце'', ''А001'',
                                                                                  ''в году по календарю / в мес. по календ'', ''А006'',
                                                                                  ''360 дней в году / в месяце по календарю'', ''А004'')||chr(13)
                        END||
                        CASE WHEN v.t_tradtype <> chr(1)
                             THEN '':94B::TRAD//''||v.t_tradtype||chr(13)
                        END||
                        '':22H::BUSE//''||decode(v.t_direction, 1, ''BUYI'',
                                                                2, ''SELL'')||chr(13)||
                        '':22H::PAYM//''||decode(v.t_dvp, 1, ''APMT'',
                                                             ''FREE'')||chr(13)||
                        '':22F::TTCO//BTEX''||chr(13)||
                        '':22F::RPOR//TRRE''||chr(13)||
                        '||RSP_SECURITIES.GenBlocCONFPRTY(P_QUAL => 'BUYR')||'
                        '||RSP_SECURITIES.GenBlocCONFPRTY(P_QUAL => 'SELL')||'
                        '':36B::CONF//FAMT/''||v.t_Amount * v.t_IssueNominal||chr(13)||
                        '||RSP_SECURITIES.GenField35B||'
                        '||RSP_SECURITIES.GenBlocFIA||'
                 '':16S:CONFDET''';
       Return l_Msg;
     END IF;
   end GenBlocCONFDET;
   function GENFIELD35B return CLOB as
   /*Функция генерирует текст конструкции SELECT.
     Возвращает текст конструкции SELECT для генерации поля "35B" блока TRADDET.
   */
   begin
     Return 'CASE WHEN v.t_isin <> chr(1)
                  THEN '':35B:ISIN ''||v.t_ISIN||chr(13)
                  ELSE CASE WHEN v.t_IssueOthrCode <> chr(1)
                            THEN '':35B:/XX/''||v.t_IssueOthrCodeKind||''/''||v.t_IssueOthrCode||chr(13)||
                                 CASE WHEN v.t_LSIN <> chr(1)
                                      THEN ''/RU/''||v.t_LSIN||chr(13)
                                 END||
                                 CASE WHEN v.t_IssueName <> chr(1)
                                      THEN ''/NAME/''||substr(v.t_IssueName, 1, 29)||''''||chr(13)||
                                           decode(substr(v.t_IssueName, 30, 35), NULL, NULL, substr(v.t_IssueName, 30, 35)||chr(13))
                                 END
                            ELSE CASE WHEN v.t_LSIN <> chr(1)
                                      THEN '':35B:/RU/''||v.t_LSIN||chr(13)||
                                           CASE WHEN v.t_IssueName <> chr(1)
                                                THEN ''/NAME/''||substr(v.t_IssueName, 1, 29)||''''||chr(13)||
                                                     decode(substr(v.t_IssueName, 30, 35), NULL, NULL, substr(v.t_IssueName, 30, 35)||chr(13))||
                                                     decode(substr(v.t_IssueName, 66, 35), NULL, NULL, substr(v.t_IssueName, 66, 35)||chr(13))
                                           END
                                      ELSE CASE WHEN v.t_IssueName <> chr(1)
                                                THEN '':35B:/NAME/''||substr(v.t_IssueName, 1, 24)||''''||chr(13)||
                                                     decode(substr(v.t_IssueName, 25, 35), NULL, NULL, substr(v.t_IssueName, 25, 35)||chr(13))||
                                                     decode(substr(v.t_IssueName, 61, 35), NULL, NULL, substr(v.t_IssueName, 61, 35)||chr(13))||
                                                     decode(substr(v.t_IssueName, 97, 35), NULL, NULL, substr(v.t_IssueName, 97, 35)||chr(13))
                                           END
                                 END
                       END
             END||';
   end GenField35B;
   function GENFIELD79 return CLOB as
   /*Функция генерирует текст конструкции SELECT.
     Возвращает текст конструкции для генерации поля "79".
   */
   begin
     Return replace(':79:WE CONFIRM THE FOLLOWING OF THE REPO''||chr(13)||
                   ''DEAL TRADED''||chr(13)||
                   ''TRADE DATE: ''||to_char(v.t_DealDate, ''dd.mm.yyyy'')||chr(13)||
                   ''SELLER: ''||decode(nvl(v.t_SellBIC, chr(1)), chr(1), null, v.t_SellBIC||'' '')||v.t_SellName||chr(13)||
                   ''BUYER: ''||decode(nvl(v.t_BuyrBIC, chr(1)), chr(1), NULL, v.t_BuyrBIC||'' '')||v.t_BuyrName||chr(13)||
                   ''SECURITIES ISIN: ''||v.t_ISIN||chr(13)||
                   ''QUANTITY: ''||v.t_Amount||chr(13)||
                   ''REPO RATE: ''||v.t_Rate||chr(13)||
                   ''FIRST PART OF DEAL:''||chr(13)||
                   ''1.PRICE: ''||v.t_PricePrc||chr(13)||
                   ''2.PRINCIPAL AMOUNT: ''||v.t_SumNCCurrency||v.t_SumNC||chr(13)||
                   ''3.ACCURED INTEREREST: ''||v.t_CoupCurrency||v.t_Coup||chr(13)||
                   ''4.TOTAL AMOUNT: ''||v.t_SumCurrency||v.t_Sum||chr(13)||
                   CASE WHEN v.t_EXCH <> 0
                        THEN v.t_Sum * v.t_EXCH||chr(13)
                   END||
                   ''5.SETTLEMENT DATE ''||decode(to_char(v.t_SettDate, ''dd.mm.yyyy''), ''01.01.0001'', NULL, to_char(v.t_SettDate, ''dd.mm.yyyy''))||chr(13)||
                   ''6.SETTLEMENT CONDITIONS: ''||decode(v.t_DVP, 0, null, v.t_dvp)||chr(13)||
                   CASE WHEN v.t_Repo = 1
                        THEN '''||RSP_SECURITIES.GenField79Repo||'''||chr(13)
                   END||
                   ''OUR SAFE DETAILS: ''||chr(13)||
                   CASE WHEN nvl(v.t_BuyrName, chr(1)) = v.t_Client
                        THEN decode(v.t_BuyrBIC, chr(1), NULL, v.t_BuyrBIC||'' '')||v.t_BuyrName||chr(13)
                        ELSE decode(v.t_SellBIC, chr(1), NULL, v.t_SellBIC||'' '')||v.t_SellName||chr(13)
                   END||
                   CASE WHEN nvl(v.t_SellName, chr(1)) = v.t_Client
                        THEN decode(nvl(v.t_DeagName, chr(1)),
                                    null,
                                    decode(v.t_DeagBIC, chr(1),
                                           NULL,
                                           v.t_DeagBIC||'' '')||v.t_DeagName||decode(v.t_DeagDepoAcc, chr(1),
                                                                                     NULL,
                                                                                     '' ''||v.t_DeagDepoAcc)||chr(13))
                        ELSE decode(nvl(v.t_ReagName, chr(1)),
                                    null,
                                    decode(v.t_ReagBIC, chr(1),
                                           NULL,
                                           v.t_ReagBIC||'' '')||v.t_ReagName||decode(v.t_ReagDepoAcc, chr(1),
                                                                                     NULL,
                                                                                     '' ''||v.t_ReagDepoAcc)||chr(13))
                   END||
                   ''OUR CASH DETAILS: ''||chr(13)||
                   ''ACCOUNT ''||CASE WHEN nvl(v.t_PayeCash, chr(1)) = v.t_Client
                                      THEN v.t_PayeCash||chr(13)
                                      ELSE v.t_BenmCash||chr(13)
                                 END||
                   ''WITH ''||RSP_SETUP.GET_VALUE(P_PATH => ''Настройки инициализации\OWNNAME'')||chr(13)||
                   ''C/A ''||decode(v.t_CA, chr(1), NULL, v.t_CA)||chr(13)||
                   ''BIC ''||(SELECT nr.bic as bic
                                FROM nroute nr, stbic tb
                               WHERE nr.id_nkorr is null
                                 AND nr.id_stbic = tb.id_stbic
                                 AND tb.code = 20)||chr(13)||
                   ''YOUR DETAILS TO YOUR INSTRUCTION''||chr(13)||
                   ''BEST REGARDS, BACK-OFFICE', '  ', ' ');
   end GenField79;
   function GENFIELD79REPO return CLOB as
     l_79Repo CLOB;
   begin
     SELECT 'SECOND PART OF DEAL:'||chr(13)||
            '1.PRICE '||v.T_PRICEPRC||chr(13)||
            '2.PRINCIPAL AMOUNT: '||v.t_SumNCCurrency||v.t_SumNC||chr(13)||
            '3.ACCURED INTEREREST: '||v.t_CoupCurrency||v.t_Coup||chr(13)||
            '4.TOTAL AMOUNT: '||v.t_SumCurrency||v.t_Sum||chr(13)||
             CASE WHEN v.t_EXCH1 <> 0
                  THEN v.t_Sum * v.t_EXCH1||chr(13)
             END||
            '5.SETTLEMENT DATE '||decode(to_char(v.t_REPO2, 'dd.mm.yyyy'), '01.01.0001', NULL, to_char(v.t_REPO2, 'dd.mm.yyyy'))||chr(13)||
            '6.SETTLEMENT CONDITIONS: '||decode(v.t_DVP, 0, null, v.t_dvp)
       INTO l_79Repo
       FROM DRAFTSINFO1 v
      WHERE v.t_rownum = 2;
     Return l_79Repo;
   exception
     WHEN NO_DATA_FOUND THEN
       Return NULL;
   end GENFIELD79Repo;
   function GENFIELD79NETT return CLOB as
   /*Функция генерирует текст конструкции SELECT.
     Возвращает текст конструкции для генерации поля "79".
   */
   begin
     Return replace(
              replace(':79:PLEASE CONFIRM YOUR AGREEMENT FOR''||chr(13)||
                     ''THE DEAL PAIR-OFF''||chr(13)||
                     ''WE HAVE DEALS FOR''||chr(13)||
                     ''VALUE DATE: ''||to_char(v.t_SettDateNet, ''dd.mm.yyyy'')||chr(13)||
                     '''||RSP_SECURITIES.GENFIELD79NETTPART||'''||
                     CASE WHEN v.t_SumNet > 0
                          THEN CASE WHEN v.t_DirNet = ''Т''
                                    THEN ''AS A RESULT CONTRPARTY PAY/DELIVERY: ''
                                    WHEN v.t_DirNet = ''О''
                                    THEN ''AS A RESULT WE PAY/DELIVERY: ''
                               END||replace(v.t_NaNet, ''RUR'', ''RUB'')||ABS(v.t_SumNet)||chr(13)
                     END||
                     CASE WHEN nvl(v.t_SumNet, null) = 0
                          THEN CASE WHEN v.t_InsNet = ''Ц''
                                    THEN ''NO BONDS MOVEMENTS''||chr(13)
                                    WHEN v.t_InsNet = ''Д''
                                    THEN ''NO CASH MOVEMENTS''||chr(13)
                               END
                     END||
                     CASE WHEN v.t_InsNet = ''Ц''
                          THEN ''OUR SAFE DETAILS''||chr(13)||
                               CASE WHEN nvl(v.t_BuyrName, chr(1)) = v.t_Client
                                    THEN decode(v.t_BuyrBIC, chr(1), NULL, v.t_BuyrBIC||'' '')||RSP_COMMON.EraseChr1(v.t_BuyrName)||chr(13)
                                    ELSE decode(v.t_SellBIC, chr(1), NULL, v.t_SellBIC||'' '')||RSP_COMMON.EraseChr1(v.t_SellName)||chr(13)
                               END||
                               CASE WHEN nvl(v.t_SellName, chr(1)) = v.t_Client
                                    THEN decode(v.t_DeagBIC, chr(1),
                                                NULL,
                                                v.t_DeagBIC||'' '')||RSP_COMMON.EraseChr1(v.t_DeagName)||decode(v.t_DeagDepoAcc, chr(1),
                                                                                                                NULL,
                                                                                                                '' ''||v.t_DeagDepoAcc)||chr(13)
                                    ELSE decode(v.t_ReagBIC, chr(1),
                                                NULL,
                                                v.t_ReagBIC||'' '')||RSP_COMMON.EraseChr1(v.t_ReagName)||decode(v.t_ReagDepoAcc, chr(1),
                                                                                                                NULL,
                                                                                                                '' ''||v.t_ReagDepoAcc)||chr(13)
                               END||
                               ''DEPO AGREEMENT: ''||chr(13)||
                               ''YOUR SAFE DETAILS''||chr(13)||
                               CASE WHEN nvl(v.t_BuyrName, chr(1)) <> v.t_Client
                                    THEN decode(v.t_BuyrBIC, chr(1), NULL, v.t_BuyrBIC||'' '')||RSP_COMMON.EraseChr1(v.t_BuyrName)||chr(13)
                                    ELSE decode(v.t_SellBIC, chr(1), NULL, v.t_SellBIC||'' '')||RSP_COMMON.EraseChr1(v.t_SellName)||chr(13)
                               END||
                               CASE WHEN nvl(v.t_SellName, chr(1)) <> v.t_Client
                                    THEN ''WITH: ''||decode(v.t_DeagBIC, chr(1),
                                                            NULL,
                                                            v.t_DeagBIC||'' '')||RSP_COMMON.EraseChr1(v.t_DeagName)||decode(v.t_DeagDepoAcc, chr(1),
                                                                                                                            NULL,
                                                                                                                            '' ''||v.t_DeagDepoAcc)||chr(13)
                                    ELSE ''WITH: ''||decode(v.t_ReagBIC, chr(1),
                                                            NULL,
                                                            v.t_ReagBIC||'' '')||RSP_COMMON.EraseChr1(v.t_ReagName)||decode(v.t_ReagDepoAcc, chr(1),
                                                                                                                            NULL,
                                                                                                                            '' ''||v.t_ReagDepoAcc)||chr(13)
                               END||
                               ''DEPO AGREEMENT: ''||chr(13)
                     END||
                     CASE WHEN v.t_InsNet = ''Д''
                          THEN ''CASH DETAILS OF RECEIVING PARTY''||chr(13)||
                               ''ACCOUNT: ''||decode(v.t_DirNet, ''О'', decode(v.t_PayeName, v.t_Client, v.t_BenmCash, v.t_PayeCash),
                                                                 ''Т'', decode(v.t_PayeName, v.t_Client, v.t_PayeCash, v.t_BenmCash))||chr(13)||
                               ''WITH: ''||decode(v.t_DirNet, ''О'', decode(v.t_PayeName, v.t_Client, v.t_AccwBIC||'' ''||v.t_AccwName,
                                                                            v.t_PayeAccwBIC||'' ''||v.t_PayeAccwName),
                                                              ''Т'', decode(v.t_PayeName, v.t_Client, v.t_PayeAccwBIC||'' ''||v.t_PayeAccwName,
                                                                            v.t_AccwBIC||'' ''||v.t_AccwName))||chr(13)||
                               ''C/A: ''||decode(v.t_DirNet, ''О'', decode(v.t_PayeName, v.t_Client, RSP_COMMON.EraseChr1(v.t_AccwCash),
                                                                                                     RSP_COMMON.EraseChr1(v.t_PayeAccwCash)),
                                                             ''Т'', decode(v.t_PayeName, v.t_Client, RSP_COMMON.EraseChr1(v.t_PayeAccwCash),
                                                                                                     RSP_COMMON.EraseChr1(v.t_AccwCash)))||chr(13)||
                               ''WITH: ''||decode(v.t_DirNet, ''О'', decode(v.t_PayeName, v.t_Client, RSP_COMMON.EraseChr1(v.t_AccwCashWith),
                                                                                                      RSP_COMMON.EraseChr1(v.t_PayeAccwCashWith)),
                                                              ''Т'', decode(v.t_PayeName, v.t_Client, RSP_COMMON.EraseChr1(v.t_PayeAccwCashWith),
                                                                                                      RSP_COMMON.EraseChr1(v.t_AccwCashWith)))||chr(13)||
                               ''BIC: ''||decode(v.t_DirNet, ''О'', decode(v.t_PayeName, v.t_Client, RSP_COMMON.EraseChr1(v.t_AccwCashWithBIC),
                                                                                                     RSP_COMMON.EraseChr1(v.t_PayeAccwCashWithBIC)),
                                                             ''Т'', decode(v.t_PayeName, v.t_Client, RSP_COMMON.EraseChr1(v.t_PayeAccwCashWithBIC),
                                                                                                     RSP_COMMON.EraseChr1(v.t_AccwCashWithBIC)))||chr(13)||
                               ''INN: ''||decode(v.t_DirNet, ''О'', decode(v.t_PayeName, v.t_Client, RSP_COMMON.EraseChr1(v.t_AccwCashWithINN),
                                                                                                     RSP_COMMON.EraseChr1(v.t_PayeAccwCashWithINN)),
                                                             ''Т'', decode(v.t_PayeName, v.t_Client, RSP_COMMON.EraseChr1(v.t_PayeAccwCashWithINN),
                                                                                                     RSP_COMMON.EraseChr1(v.t_AccwCashWithINN)))||chr(13)
                     END||
                     ''CONTACT DETAILS: +7(495)777-10-20 EXT.77-0202''||chr(13)||
                     ''BEST REGARDS, BACK-OFFICE',
                     '  ', ' '),
                   chr(13)||chr(13), chr(13));
   end GenField79Nett;
   function GENFIELD79NETTPART return CLOB as
   /*Функция генерирует текст конструкции SELECT.
     Возвращает текст конструкции для генерации поля "79".
   */
     l_Msg CLOB;
   begin
     FOR c IN(SELECT v.*
                FROM DRAFTSINFO1 v) LOOP
       l_Msg := l_Msg||
                'WE '||CASE c.t_direction WHEN 1 THEN 'BUY'
                                          WHEN 2 THEN 'SELL'
                       END||chr(13)||
                '1.SECURITIES ISIN: '||RSP_COMMON.EraseChr1(c.t_ISIN)||chr(13)||
                '2.QUANTITY: '||c.t_Amount||chr(13)||
                '3.DEAL NUMBER: '||RSP_COMMON.EraseChr1(c.t_DealID)||chr(13)||
                '4.DEAL DATE: '||to_char(c.t_DealDate, 'dd.mm.yyyy')||chr(13)||
                '5.AMOUNT: '||replace(c.t_SumCurrency, 'RUR', 'RUB')||ABS(c.t_Sum)||chr(13);
     END LOOP;
     Return l_Msg;
   end GenField79NettPart;
   function GENBLOCFIAC (P_DRAFTID in integer,P_QUAL in varchar2,P_TEXT out varchar2) return CLOB as
     l_cnt INTEGER;
     l_Msg CLOB;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCFIAC');
     SELECT count(*)
       INTO l_cnt
       FROM DRAFTSINFO v
      WHERE nvl(v.t_Amount, 0) = 0
        AND v.t_DraftID = p_DraftID;
     IF l_cnt > 0 THEN
       p_Text := p_Text||' отсутствует "Количество финансового инструмента"';
     END IF;
     SELECT count(*)
       INTO l_cnt
       FROM DRAFTSINFO v
      WHERE decode(v.T_ISUNIVOPER, 0, decode(p_Qual, 'REAG', v.t_ReagDepoAcc,
                                                     'DEAG', v.t_DeagDepoAcc),
                                      decode(p_Qual, 'REAG', v.t_ReagDepoAccOFF,
                                                     'DEAG', v.t_DeagDepoAccOFF)) = chr(1)
        AND v.t_DraftID = p_DraftID;
     IF l_cnt > 0 THEN
       p_Text := p_Text||' отсутствует "Счет/Счет депо"';
     END IF;
     IF p_Text IS NOT NULL THEN
       Return Null;
     ELSE
     --Получение блока FIAC
       l_Msg := ''':16R:FIAC''||chr(13)||
                     '':36B::SETT//''||CASE WHEN v.t_issueView IN(''Еврооблигация'', ''Облигация'', ''Варрант'',''Корпоративная еврооблигация'', ''Иностранная государственная'', ''ОВВЗ'')
                                            THEN ''FAMT/''||v.t_Amount * v.t_IssueNominal||'',''
                                            ELSE ''UNIT/''||v.t_Amount||'',''
                                       END||chr(13)||
                       CASE WHEN v.T_ISUNIVOPER = 0
                            THEN '':97A::SAFE//''||v.t_'||p_Qual||'DepoAcc||CASE WHEN nvl(v.t_'||p_Qual||'DepoPart, chr(1)) <> chr(1)
                                                                                 THEN ''/KRZD/''||v.t_'||p_Qual||'DepoPart
                                                                            END
                            ELSE '||CASE WHEN g_KindMsg IN(540, 542)
                                         THEN ''':97A::SAFE//''||v.t_'||p_Qual||'DepoAccOFF||CASE WHEN nvl(v.t_'||p_Qual||'DepoPartOFF, chr(1)) <> chr(1)
                                                                                                  THEN ''/KRZD/''||v.t_'||p_Qual||'DepoPartOFF
                                                                                             END'
                                         ELSE 'NULL'
                                    END||'
                       END||chr(13)||
                 '':16S:FIAC''';
       Return l_Msg;
     END IF;
   end GenBlocFIAC;
   function GENBLOCSETDET (P_DRAFTID in integer,P_KINDMSG in varchar2,P_TEXT out varchar2) return CLOB as
     l_cnt INTEGER;
     l_Msg CLOB;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCSETDET');
     SELECT count(*) INTO l_cnt
       FROM DRAFTSINFO v
       WHERE v.t_OperType = chr(1) AND v.t_DraftID = p_DraftID;
     IF l_cnt > 0 THEN
       p_Text := p_Text||' отсутствует "Признак типа расчётной операции"';
     END IF;
     SELECT count(*)
       INTO l_cnt
       FROM DRAFTSINFO v
      WHERE v.t_isunivoper = 0
        AND v.t_PSETBIC = chr(1)
        AND v.t_PSETName = chr(1)
        AND v.t_DraftID = p_DraftID;
     IF l_cnt > 0 THEN
       p_Text := p_Text||' отсутствует "Место проведения расчётов"';
     END IF;
     /*
     SELECT count(*)
       INTO l_cnt
       FROM DRAFTSINFO v
      WHERE v.t_isunivoper = 0
        AND v.t_DeagBIC = chr(1)
        AND v.t_DeagName = chr(1)
        AND v.t_DraftID = p_DraftID;
     IF l_cnt > 0 THEN
       p_Text := p_Text||' отсутствует "Агент по поставке"';
     END IF;
     */
     IF p_Text IS NOT NULL THEN
       Return Null;
   --Получение блока SETDET
     ELSE
       l_Msg := ''':16R:SETDET''||chr(13)||
                     '':22F::SETR//''||v.t_OperType||chr(13)||
                     CASE WHEN v.t_issueView IN(''Еврооблигация'', ''Облигация'', v.t_issueView)
                          THEN '':22F::RTGS//YRTG''||chr(13)
                     END||
                     '||CASE WHEN g_CatKorr = 50
                             THEN CASE WHEN g_KindMsg IN(540, 541)
                                       THEN RSP_SECURITIES.GenBlocSETPRTYClear(P_QUAL => 'DEAG')
                                  END||
                                  RSP_SECURITIES.GenBlocSETPRTYClear(P_QUAL => 'PSET')||
                                  CASE WHEN g_KindMsg IN(542, 543)
                                       THEN RSP_SECURITIES.GenBlocSETPRTYClear(P_QUAL => 'REAG')
                                  END||
                                  CASE WHEN g_KindMsg IN(540)
                                       THEN RSP_SECURITIES.GenBlocSETPRTYClear(P_QUAL => 'SELL')
                                  END/*||
                                  CASE WHEN g_KindMsg IN(542)
                                       THEN RSP_SECURITIES.GenBlocSETPRTYClear(P_QUAL => 'BUYR')
                                  END*/
                             ELSE 'CASE WHEN v.t_SellBIC <> chr(1)
                                        THEN '||RSP_SECURITIES.GenBlocSETPRTY(P_QUAL => 'SELL')||'
                                   END||
                                   CASE WHEN v.t_DeagBIC <> chr(1) OR v.t_DeagBICOFF <> chr(1) OR v.t_DeagNameOFF <> chr(1)
                                        THEN '||RSP_SECURITIES.GenBlocSETPRTY(P_QUAL => 'DEAG')||'
                                   END||
                                   CASE WHEN v.t_PSETBIC <> chr(1) OR v.t_PSETBICOFF1 <> chr(1)
                                        THEN '||RSP_SECURITIES.GenBlocSETPRTY(P_QUAL => 'PSET')||'
                                   END||
                                   CASE WHEN v.t_ReagBIC <> chr(1) OR v.t_ReagBICOFF <> chr(1) OR v.t_ReagNameOFF <> chr(1)
                                        THEN '||RSP_SECURITIES.GenBlocSETPRTY(P_QUAL => 'REAG')||'
                                   END||
                                   CASE WHEN v.t_BuyrBIC <> chr(1)
                                        THEN '||RSP_SECURITIES.GenBlocSETPRTY(P_QUAL => 'BUYR')||'
                                   END||'
                        END||'
                     '||CASE WHEN p_KindMsg IN('541', '543')
                             THEN RSP_SECURITIES.GenBlocCSHPRTY(P_QUAL => 'PAYE')||
                                  RSP_SECURITIES.GenBlocCSHPRTY(P_QUAL => 'ACCW')||
                                  RSP_SECURITIES.GenBlocCSHPRTY(P_QUAL => 'BENM')
                        END||
                        CASE WHEN g_KindMsg IN(541, 543)
                             THEN RSP_SECURITIES.GenBlocSETDETAMT
                        END||'
                 '':16S:SETDET''';
       Return l_Msg;
     END IF;
   end GenBlocSETDET;
   function GENBLOCSETDET518 (P_DRAFTID in integer,P_TEXT out varchar2) return CLOB as
     l_cnt INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCSETDET');
     SELECT count(*)
       INTO l_cnt
       FROM DRAFTSINFO1 v
      WHERE nvl(v.t_OperType, chr(1)) = chr(1)
        AND v.t_DraftID = p_DraftID;
     IF l_cnt > 0 THEN
       p_Text := p_Text||' отсутствует "Признак типа расчётной операции"';
       Return Null;
     END IF;
   --Получение блока SETDET
     Return (''':16R:SETDET''||chr(13)||
              '':22F::SETR//''||v.t_OperType||chr(13)||
                decode(v.t_BuyrBIC, chr(1), NULL, '||RSP_SECURITIES.GenBlocSETPRTY518(P_QUAL => 'BUYR')||')||
                decode(v.t_ReagBIC, chr(1), NULL, '||RSP_SECURITIES.GenBlocSETPRTY518(P_QUAL => 'RECU')||')||
                decode(v.t_PSETBIC1, chr(1), NULL, '||RSP_SECURITIES.GenBlocSETPRTY518(P_QUAL => 'REAG')||')||
                decode(v.t_PSETBIC1, chr(1), NULL, '||RSP_SECURITIES.GenBlocSETPRTY518(P_QUAL => 'PSET')||')||
                decode(v.t_SellBIC, chr(1), NULL, '||RSP_SECURITIES.GenBlocSETPRTY518(P_QUAL => 'SELL')||')||
                decode(v.t_PSETBIC, chr(1), NULL, '||RSP_SECURITIES.GenBlocSETPRTY518(P_QUAL => 'DEAG')||')||
                '||RSP_SECURITIES.GenBlocCSHPRTY518(P_QUAL => 'PAYE')||'
                '||RSP_SECURITIES.GenBlocCSHPRTY518(P_QUAL => 'BENM')||'
                '||RSP_SECURITIES.GenBlocCSHPRTY518(P_QUAL => 'ACCW')||'
                '||RSP_SECURITIES.GenBlocAMT(P_QUAL => 'DEAL')||'
                '||RSP_SECURITIES.GenBlocAMT(P_QUAL => 'ACRU')||'
                '||RSP_SECURITIES.GenBlocAMT(P_QUAL => 'SETT')||'
              '':16S:SETDET''||chr(13)||');
   end GenBlocSETDET518;
   function GENBLOCSETPRTY (P_QUAL in varchar2) return VARCHAR2 as
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCSETPRTY: '||P_QUAL);
     Return ('CASE WHEN v.T_ISUNIVOPER = 0
                   THEN '':16R:SETPRTY''||chr(13)||
                         '||CASE WHEN p_Qual = 'REAG'
                                 THEN 'rtrim('':95R::'||p_Qual||'/''||v.t_'||p_Qual||'NameOFF||decode(v.t_'||p_Qual||'DepoAccOFF, chr(1), NULL,
                                                                                                      ''/''||v.t_'||p_Qual||'DepoAccOFF)||
                                                                                                      decode(v.t_'||p_Qual||'DepoPartOFF, chr(1), NULL,
                                                                                                             ''/KRZD/''||v.t_'||p_Qual||'DepoPartOFF), ''/'')||chr(13)||'
                                 ELSE 'CASE WHEN v.t_'||p_Qual||'BIC = chr(1)
                                            THEN decode(v.t_'||p_Qual||'Name, chr(1), NULL,
                                                                                 '':95Q::'||p_Qual||'//''||v.t_'||p_Qual||'Name||chr(13))
                                            ELSE '':95P::'||p_Qual||'//''||v.t_'||p_Qual||'BIC||chr(13)
                                       END||'
                            END||'
                         '||CASE WHEN p_Qual <> 'PSET'
                                 THEN 'CASE WHEN length(v.t_'||p_Qual||'DepoAcc||v.t_'||p_Qual||'DepoPart) <= 29
                                            THEN decode(v.t_'||p_Qual||'DepoAcc, chr(1), NULL,
                                                                                         '':97A::SAFE//''||v.t_'||p_Qual||'DepoAcc||
                                                                                           decode(v.t_'||p_Qual||'DepoPart, chr(1), NULL,
    ''/KRZD/''||v.t_'||p_Qual||'DepoPart)||chr(13))
                                       END||'
                            END||'
                        '':16S:SETPRTY''||chr(13)
                   ELSE '||CASE WHEN p_Qual IN('REAG', 'DEAG')
                                THEN ''':16R:SETPRTY''||chr(13)||
                                            rtrim('':95R::'||p_Qual||'/''||v.t_'||p_Qual||'NameOFF||decode(v.t_'||p_Qual||'DepoAccOFF, chr(1), NULL,
                                                                                                           ''/''||v.t_'||p_Qual||'DepoAccOFF)||
                                                   decode(v.t_'||p_Qual||'DepoPartOFF, chr(1), NULL,
                                                          ''/KRZD/''||v.t_'||p_Qual||'DepoPartOFF), ''/'')||chr(13)||
                                      '':16S:SETPRTY''||chr(13)'
                                 WHEN p_Qual IN('PSET')
                                 THEN ''':16R:SETPRTY''||chr(13)||
                                           '':95P::'||p_Qual||'/''||v.t_'||p_Qual||'BicOFF1||chr(13)||
                                      '':16S:SETPRTY''||chr(13)'
                                 ELSE 'NULL'
                            END||'
              END');
   end GenBlocSETPRTY;
   function GENBLOCSETPRTYCLEAR (P_QUAL in varchar2) return VARCHAR2 as
     l_SETPRTY VARCHAR2(4000);
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCSETPRTYCLEAR');
     IF p_Qual IN('PSET') THEN
       l_SETPRTY := 'CASE WHEN v.T_ISUNIVOPER = 0
                          THEN CASE WHEN v.t_'||p_Qual||'BIC <> chr(1)
                                    THEN '':16R:SETPRTY''||chr(13)||
                                             '':95P::'||p_Qual||'//''||v.t_'||p_Qual||'BIC||chr(13)||
                                         '':16S:SETPRTY''||chr(13)
                               END
                          ELSE CASE WHEN v.t_'||p_Qual||'BICOFF1 <> chr(1)
                                    THEN '':16R:SETPRTY''||chr(13)||
                                             '':95P::'||p_Qual||'//''||v.t_'||p_Qual||'BICOFF1||chr(13)||
                                         '':16S:SETPRTY''||chr(13)
                                    ELSE CASE WHEN v.t_'||p_Qual||'BICOFF <> chr(1)
                                              THEN '':16R:SETPRTY''||chr(13)||
                                                       '':95P::'||p_Qual||'//''||v.t_'||p_Qual||'BICOFF||chr(13)||
                                                   '':16S:SETPRTY''||chr(13)
                                         END
                               END
                          END||';
     ELSIF p_Qual IN('BUYR') THEN
       l_SETPRTY := 'CASE WHEN v.T_ISUNIVOPER = 0
                          THEN CASE WHEN v.t_'||p_Qual||'BIC <> chr(1)
                                    THEN '':16R:SETPRTY''||chr(13)||
                                             '':95P::'||p_Qual||'//''||v.t_'||p_Qual||'BIC||chr(13)||
                                         '':16S:SETPRTY''||chr(13)
                               END
                          ELSE CASE WHEN v.t_'||p_Qual||'BICOFF <> chr(1)
                                    THEN '':16R:SETPRTY''||chr(13)||
                                             '':95P::'||p_Qual||'//''||v.t_'||p_Qual||'BICOFF||chr(13)||
                                         '':16S:SETPRTY''||chr(13)
                               END
                          END||';
     ELSIF p_Qual IN('DEAG', 'REAG') THEN
       l_SETPRTY := 'CASE WHEN v.T_ISUNIVOPER = 0
                          THEN '||
                          CASE WHEN G_DRAFTKIND = 9
                               THEN ''':16R:SETPRTY''||chr(13)||
                                         '':95P::'||p_Qual||'//''||v.t_'||p_Qual||'BIC||chr(13)||
                                         '':97A::SAFE//''||v.t_'||p_Qual||'DepoAcc||decode(v.t_'||p_Qual||'DepoPart, chr(1), NULL,
                                                                                           ''/KRZD/''||v.t_'||p_Qual||'DepoPart)||chr(13)||
                                     '':16S:SETPRTY''||chr(13)'
                               ELSE ''':16R:SETPRTY''||chr(13)||
                                         rtrim('':95R::'||p_Qual||'/''||v.t_'||p_Qual||'Name||decode(v.t_'||p_Qual||'DepoAcc, chr(1), NULL,
                                                                                                     ''/''||v.t_'||p_Qual||'DepoAcc)||
                                               decode(v.t_'||p_Qual||'DepoPart, chr(1), NULL,
                                                      ''/KRZD/''||v.t_'||p_Qual||'DepoPart), ''/'')||chr(13)||
                                   '':16S:SETPRTY''||chr(13)'
                          END||'
                          ELSE CASE WHEN v.t_IssueName LIKE ''OFZ%'' OR v.t_IssueName LIKE ''ОФЗ%''
                                    THEN '':16R:SETPRTY''||chr(13)||
                                             '':95P::'||p_Qual||'//''||v.t_'||p_Qual||'BICOFF||chr(1)||
                                             '':97A::SAFE//''||v.t_'||p_Qual||'DepoAccOFF||decode(v.t_'||p_Qual||'DepoPartOFF, chr(1), NULL,
                                                                                                 ''/KRZD/''||v.t_'||p_Qual||'DepoPartOFF)||chr(13)||
                                         '':16S:SETPRTY''||chr(13)
                                    ELSE '':16R:SETPRTY''||chr(13)||
                                               rtrim('':95R::'||p_Qual||'/''||CASE WHEN instr(v.t_'||p_Qual||'nameoff, ''/'') = 9
                                                                                   THEN concat(substr(v.t_'||p_Qual||'NameOFF, 1, 4),
                                                                                               substr(v.t_'||p_Qual||'NameOFF,
                                                                                                      instr(v.t_'||p_Qual||'NameOFF, ''/'')))
                                                                                   ELSE v.t_'||p_Qual||'nameoff
                                                                              END||
                                                                              decode(v.t_'||p_Qual||'DepoAccOFF, chr(1), NULL,
                                                                                     ''/''||v.t_'||p_Qual||'DepoAccOFF)||
                                                                                     decode(v.t_'||p_Qual||'DepoPartOFF, chr(1), NULL,
                                                                                            ''/KRZD/''||v.t_'||p_Qual||'DepoPartOFF), ''/'')||chr(13)||
                                         '':16S:SETPRTY''||chr(13)
                               END
                     END||';
     ELSIF p_Qual = 'SELL' THEN
       l_SETPRTY := 'CASE WHEN v.t_BuyrBICOFF <> chr(1)
                          THEN decode(v.T_ISUNIVOPER, 1, '':16R:SETPRTY''||chr(13)||
                                                             '':95P::'||p_Qual||'//''||v.t_BuyrBICOFF||chr(13)||
                                                         '':16S:SETPRTY''||chr(13))
                     END||';
     END IF;
     Return l_SETPRTY;
   end GenBlocSETPRTYClear;
   function GENBLOCSETDETAMT return VARCHAR2 as
     l_AMT VARCHAR2(4000);
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GenBlocSETDETAMT');
     l_AMT := ''':16R:AMT''||chr(13)||
                   '':19A::SETT//''||decode(sign(v.t_DealSum), -1, ''-'')||v.t_DealCurrency||replace(trim(to_char(ABS(v.t_DealSum),''9999999999999D99'')),''.'','','')||chr(13)||
               '':16S:AMT''||chr(13)||';
     Return l_AMT;
   end GenBlocSETDETAMT;
   function GENBLOCSETPRTY518 (P_QUAL in varchar2) return VARCHAR2 as
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCSETPRTY518');
     Return (CASE WHEN p_Qual = 'BUYR'
                  THEN 'CASE WHEN nvl(v.t_'||p_Qual||'BIC, chr(1)) <> chr(1)
                              THEN '':16R:SETPRTY''||chr(13)||
                                       '':95P::'||p_Qual||'//''||v.t_'||p_Qual||'BIC||chr(13)||
                                   '':16S:SETPRTY''||chr(13)
                              ELSE decode(nvl(v.t_'||p_Qual||'NAME, chr(1)), chr(1), NULL,
                                          '':16R:SETPRTY''||chr(13)||
                                              '':95Q::'||p_Qual||'//''||v.t_'||p_Qual||'NAME||chr(13)||
                                          '':16S:SETPRTY''||chr(13))
                              END'
                  WHEN p_Qual = 'SELL'
                  THEN 'CASE WHEN nvl(v.t_'||p_Qual||'BIC, chr(1)) <> chr(1)
                             THEN '':16R:SETPRTY''||chr(13)||
                                      '':95P::'||p_Qual||'//''||v.t_'||p_Qual||'BIC||chr(13)
                             ELSE decode(nvl(v.t_'||p_Qual||'NAME, chr(1)), chr(1), NULL,
                                        '':16R:SETPRTY''||chr(13)||
                                            '':95Q::'||p_Qual||'//''||v.t_'||p_Qual||'NAME||chr(13))
                             END||decode(v.t_SellDepoAcc, chr(1), NULL,
                                         '':97A::SAFE//''||v.t_SellDepoAcc||chr(13))||
                                 '':16S:SETPRTY''||chr(13)'
                  WHEN p_Qual = 'RECU'
                  THEN 'CASE WHEN nvl(v.t_ReagBIC, chr(1)) <> chr(1)
                             THEN '':16R:SETPRTY''||chr(13)||
                                      '':95P::RECU//''||v.t_ReagBIC||chr(13)
                             ELSE decode(nvl(v.t_ReagNAME, chr(1)), chr(1), NULL,
                                        '':16R:SETPRTY''||chr(13)||
                                            '':95Q::RECU//''||v.t_ReagNAME||chr(13))
                             END||decode(v.t_BuyrDepoAcc, chr(1), NULL,
                                         '':97A::SAFE//''||v.t_BuyrDepoAcc||chr(13))||
                                 '':16S:SETPRTY''||chr(13)'
                  WHEN p_Qual IN('REAG', 'PSET')
                  THEN 'CASE WHEN nvl(v.t_PSETBIC1, chr(1)) <> chr(1)
                             THEN '':16R:SETPRTY''||chr(13)||
                                      '':95P::'||p_Qual||'//''||v.t_PSETBIC1||chr(13)||
                                  '':16S:SETPRTY''||chr(13)
                             ELSE decode(nvl(v.t_PSETNAME1, chr(1)), chr(1), NULL,
                                         '':16R:SETPRTY''||chr(13)||
                                             '':95Q::'||p_Qual||'//''||v.t_PSETNAME1||chr(13)||
                                         '':16S:SETPRTY''||chr(13))
                             END'
                  WHEN p_Qual = 'DEAG'
                  THEN 'CASE WHEN nvl(v.t_PSETBIC, chr(1)) <> chr(1)
                             THEN '':16R:SETPRTY''||chr(13)||
                                      '':95P::'||p_Qual||'//''||v.t_PSETBIC||chr(13)||
                                  '':16S:SETPRTY''||chr(13)
                             ELSE decode(nvl(v.t_PSETNAME, chr(1)), chr(1), NULL,
                                         '':16R:SETPRTY''||chr(13)||
                                             '':95Q::'||p_Qual||'//''||v.t_PSETNAME||chr(13)||
                                         '':16S:SETPRTY''||chr(13))
                             END'
                  END);
   end GenBlocSETPRTY518;
   function GENBLOCCSHPRTY (P_QUAL in varchar2) return VARCHAR2 as
     l_Bloc VARCHAR2(4000);
     l_Sql  VARCHAR2(4000);
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCCSHPRTY '||p_Qual);
     l_Sql := 'SELECT CASE WHEN v.t_'||p_Qual||'BIC <> chr(1)
                           THEN '':95P::'||p_Qual||'//''||v.t_'||p_Qual||'BIC||chr(13)
                           ELSE decode(nvl(v.t_'||p_Qual||'Name, chr(1)), chr(1), NULL,
                                                                                  '':95Q::'||p_Qual||'//''||v.t_'||p_Qual||'Name||chr(13))
                      END||decode(nvl(v.t_'||p_Qual||'Cash, chr(1)), chr(1), NULL,
                                                                             '':97A::CASH//''||v.t_'||p_Qual||'Cash||chr(13))
                 FROM DRAFTSINFO v
                WHERE v.t_DraftID = :DraftID';
     EXECUTE IMMEDIATE l_Sql INTO l_Bloc USING g_DraftID;
     IF l_Bloc IS NOT NULL THEN
       l_Bloc := ''':16R:CSHPRTY''||chr(13)||
                    '''||l_Bloc||'''||
                  '':16S:CSHPRTY''||chr(13)||';
     END IF;
     Return l_Bloc;
   end GenBlocCSHPRTY;
   function GENBLOCCSHPRTY518 (P_QUAL in varchar2) return VARCHAR2 as
     l_Bloc VARCHAR2(4000);
     l_Sql  VARCHAR2(4000);
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCCSHPRTY518 '||p_Qual);
     l_Sql := 'SELECT CASE WHEN v.t_'||p_Qual||'BIC <> chr(1)
                           THEN '':95P::'||p_Qual||'//''||v.t_'||p_Qual||'BIC||chr(13)
                           ELSE decode(nvl(v.t_'||p_Qual||'Name, chr(1)), chr(1), NULL,
                                                                                  '':95Q::'||p_Qual||'//''||v.t_'||p_Qual||'Name||chr(13))
                      END||decode(nvl(v.t_'||p_Qual||'Cash, chr(1)), chr(1), NULL,
                                                                             '':97A::CASH//''||v.t_'||p_Qual||'Cash||chr(13))
                 FROM DRAFTSINFO1 v
                WHERE v.t_DraftID = :DraftID
                  AND v.t_rownum = 1';
     EXECUTE IMMEDIATE l_Sql INTO l_Bloc USING g_DraftID;
     IF l_Bloc IS NOT NULL THEN
       l_Bloc := ''':16R:CSHPRTY''||chr(13)||
                    '''||l_Bloc||'''||
                  '':16S:CSHPRTY''||chr(13)||';
     END IF;
     Return l_Bloc;
   end GenBlocCSHPRTY518;
   function GENBLOCCONFPRTY (P_QUAL in varchar2) return VARCHAR2 as
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCCONFPRTY');
     Return (''':16R:CONFPRTY''||chr(13)||
                     CASE WHEN nvl(v.t_'||p_Qual||'BIC, chr(1)) <> chr(1)
                          THEN '':95P::'||p_Qual||'//''||v.t_'||p_Qual||'BIC||chr(13)
                          ELSE '':95Q::'||p_Qual||'//''||v.t_'||p_Qual||'NAME||chr(13)
                     END||
                     CASE WHEN '''||p_Qual||''' = ''SELL''
                          THEN '':20C::PROC//''||p.numreferdoc||chr(13)||
                                 decode(v.t_Gsett, chr(1), NULL,
                                                           '':70C::PACO//''||substr(v.t_Gsett, 1, 24)||''''||chr(13)||
                                                           decode(substr(v.t_Gsett, 25, 35), NULL, NULL, substr(v.t_Gsett, 25, 35)||chr(13))||
                                                           decode(substr(v.t_Gsett, 61, 35), NULL, NULL, substr(v.t_Gsett, 61, 35)||chr(13))||
                                                           decode(substr(v.t_Gsett, 97, 35), NULL, NULL, substr(v.t_Gsett, 97, 35)||chr(13)))
                     END||
              '':16S:CONFPRTY''||chr(13)||');
   end GenBlocCONFPRTY;
   function GENBLOCFIA return VARCHAR2 as
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCFIA');
     Return (''':16R:FIA''||chr(13)||
                  '':12C::CLAS//DTFFB''||chr(13)||
                  CASE WHEN v.t_IssueNomCurrency <> chr(1)
                       THEN '':11A::DENO ''||v.t_IssueNomCurrency||chr(13)
                  END||
                  CASE WHEN nvl(v.t_Maturity, ''01.01.0001'') <> ''01.01.0001''
                       THEN '':98A::MATU//''||to_char(v.t_Maturity, ''YYYYMMDD'')||chr(13)
                  END||
                  CASE WHEN v.t_INTR <> 0
                       THEN '':92A::INTR//''||replace(v.t_INTR, '','', ''.'')||chr(13)
                  END||
              '':16S:FIA''||chr(13)||');
   end GenBlocFIA;
   function GENBLOCAMT (P_QUAL in varchar2) return VARCHAR2 as
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCAMT');
     Return (''':16R:AMT''||chr(13)||
                    '||CASE WHEN p_Qual = 'DEAL'
                            THEN ''':19A::DEAL//''||decode(sign(v.t_SumNC), -1, ''-'')||replace(v.t_SumNCCurrency, ''RUR'', ''RUB'')||ABS(v.t_SumNC)||chr(13)||
                                    CASE WHEN nvl(v.t_EXCHCur, chr(1)) <> chr(1) AND nvl(v.t_IssueNomCurrency, chr(1)) <> nvl(v.t_EXCHCur, chr(1))
                                         THEN '':19А::RESU//''||decode(sign(v.t_SumNC), -1, ''-'')||replace(v.t_EXCHCur, ''RUR'', ''RUB'')||ABS(v.t_SumNC) * v.t_EXCH||chr(13)
                                    END||'
                       END||'
                    '||CASE WHEN p_Qual = 'ACRU'
                            THEN ''':19A::ACRU//''||decode(sign(v.t_Coup), -1, ''-'')||replace(v.t_CoupCurrency, ''RUR'', ''RUB'')||ABS(v.t_Coup)||''''||chr(13)||
                                    CASE WHEN nvl(v.t_EXCHCur, chr(1)) <> chr(1) AND nvl(v.t_IssueNomCurrency, chr(1)) <> nvl(v.t_EXCHCur, chr(1))
                                         THEN '':19А::RESU//''||decode(sign(v.t_Coup), -1, ''-'')||replace(v.t_EXCHCur, ''RUR'', ''RUB'')||ABS(v.t_Coup) * v.t_EXCH||''''||chr(13)
                                    END||'
                       END||'
                    '||CASE WHEN p_Qual = 'SETT'
                            THEN ''':19A::SETT//''||decode(sign(v.t_Sum), -1, ''-'')||replace(v.t_SumCurrency, ''RUR'', ''RUB'')||ABS(v.t_Sum)||''''||chr(13)||
                                    CASE WHEN nvl(v.t_EXCHCur, chr(1)) <> chr(1) AND nvl(v.t_IssueNomCurrency, chr(1)) <> nvl(v.t_EXCHCur, chr(1))
                                         THEN '':19А::RESU//''||decode(sign(v.t_Sum), -1, ''-'')||replace(v.t_EXCHCur, ''RUR'', ''RUB'')||ABS(v.t_Sum) * v.t_EXCH||''''||chr(13)
                                    END||'
                       END||'
                 '':98A::VALU//''||to_char(v.t_SettDate, ''YYYYMMDD'')||chr(13)||
                   CASE WHEN nvl(v.t_EXCHCur, chr(1)) <> chr(1)
                        THEN decode(nvl(v.t_IssueNomCurrency, chr(1)), nvl(v.t_EXCHCur, chr(1)),
                                        NULL,
                                        '':92B::EXCH//''||replace(v.t_EXCHCur, ''RUR'', ''RUB'')||v.t_EXCH)||chr(13)
                   END||
             '':16S:AMT''||chr(13)||');
   end GenBlocAMT;
   function GENBLOCREPO return CLOB as
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCREPO');
     Return (''':16R:REPO''||chr(13)||
                     CASE WHEN nvl(v.t_REPO2, ''01.01.0001'') <> ''01.01.0001''
                          THEN '':98A::TERM//''||to_char(v.t_REPO2, ''YYYYMMDD'')||chr(13)
                          ELSE '':98B::TERM//OPEN''||chr(13)
                     END||
                     CASE WHEN nvl(v.t_day, chr(1)) <> chr(1)
                          THEN '':22F::MICO//''||decode(v.t_day, ''365 дней в году / в месяце по календарю'', ''А005'',
                                                                 ''360 дней в году / 30 дней в месяце'', ''А001'',
                                                                 ''в году по кален-дарю / в мес. по календ'', ''А006'',
                                                                 ''360 дней в году / в месяце по календарю'', ''А004'')||chr(13)
                     END||
                  '':22F::RERT//FIXE''||chr(13)||
                    decode(v.t_PricePRC, 0, NULL, '':92A::SHAI//''||decode(sign(v.t_PricePRC), -1, ''-'')||replace(v.t_PricePRC, '','', ''.'')||'',''||chr(13))||
                    decode(v.t_Rate, 0, NULL, '':92A::PRIC//''||decode(sign(v.t_Rate), -1, ''-'')||v.t_Rate||'',''||chr(13))||
                    decode(v.t_Coup, 0, NULL, '':19А::ACRU//''||decode(sign(v.t_Coup), -1, ''-'')||replace(replace(v.t_CoupCurrency, chr(1)), ''RUR'', ''RUB'')||v.t_Coup * v.t_EXCH1||chr(13))||
                    decode(v.t_SumNC, 0, NULL, '':19А::DEAL//''||decode(sign(v.t_SumNC), -1, ''-'')||replace(replace(v.t_SumNCCurrency, chr(1)), ''RUR'', ''RUB'')||v.t_SumNC * v.t_EXCH1||chr(13))||
                    decode(v.t_Sum, 0, NULL, '':19А::TRTE//''||decode(sign(v.t_Sum), -1, ''-'')||replace(replace(v.t_SumCurrency, chr(1)), ''RUR'', ''RUB'')||v.t_Sum * v.t_EXCH1||chr(13))||
                    decode(v.t_Interest, 0, NULL, '':19А::REPP//''||decode(sign(v.t_Interest), -1, ''-'')||replace(replace(v.t_EXCHCur, chr(1)), ''RUR'', ''RUB'')||v.t_Interest * v.t_EXCH1||chr(13))||
                    decode(v.t_Gsett, chr(1), NULL, '':70C::SECO//''||substr(v.t_Gsett, 1, 24)||''''||chr(13)||
                                                      decode(substr(v.t_Gsett, 25, 35), NULL, NULL, substr(v.t_Gsett, 25, 35)||chr(13))||
                                                      decode(substr(v.t_Gsett, 61, 35), NULL, NULL, substr(v.t_Gsett, 61, 35)||chr(13))||
                                                      decode(substr(v.t_Gsett, 97, 35), NULL, NULL, substr(v.t_Gsett, 97, 35)||chr(13)))||
             '':16S:REPO''');
   end GenBlocREPO;
   function GENBLOCSUBSAFE (P_CONS in char,P_SUBSAFE in clob,P_DEPO in varchar2,P_ACTI in char,P_FIN in clob) return CLOB as
   begin
     Return CASE p_Cons WHEN 'Y' THEN p_SUBSAFE||chr(13)||':16R:SUBSAFE'||chr(13)||
                                                          ':97A::SAFE//'||p_Depo||chr(13)||
                                                          ':17B::ACTI//'||p_Acti||chr(13)||
                                                            p_FIN||
                                                          ':16S:SUBSAFE'
                        WHEN 'N' THEN p_SUBSAFE||p_FIN
            END;
   end GenBlocSUBSAFE;
   function GENBLOCFIN (P_KINDMSG in varchar2) return VARCHAR2 as
   /*Функция генерирует текст конструкции SELECT.
     p_KindMsg - тип сообщения.
     Возвращает текст конструкции SELECT для генерации блока FIN.
   */
     l_Msg CLOB;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GENBLOCFIN');
   --Получение блока FIN
     IF p_KindMsg = '535' THEN
       l_Msg := ''':16R:FIN''||chr(13)||
                     '||RSP_SECURITIES.GENFIELD35B||'||
                     '':93B::AGGR//UNIT/''||a.quantity||decode(length(abs(-a.quantity -trunc(-a.quantity))) -1, 0, '','')||chr(13)||
                             CASE WHEN b.t_ExtCons = upper(''N'')
                                  THEN LISTAGG('':93B::''||r.t_ResType||''//UNIT/''||r.t_Quantity||decode(length(abs(-r.t_Quantity -trunc(-r.t_Quantity))) -1, 0, '','')||chr(13))
                                               WITHIN GROUP (ORDER BY v.t_FIID) OVER(PARTITION BY v.t_FIID)
                             END||
                 '':16S:FIN''';
     ELSIF p_KindMsg = '536' THEN
       l_Msg := ''':16R:FIN''||chr(13)||
                     '||RSP_SECURITIES.GENFIELD35B;
     END IF;
     Return l_Msg;
   end GenBlocFIN;
   procedure GENBLOCTRANSDET as
   /*Процедура формирует список транзакций (блоки TRANSDET)
     по каждому финансовому инструменту для выписки MT536.
   */
     l_FI       INTEGER := 0;
     l_Safe     VARCHAR2(35);
     l_sql_stmt VARCHAR2(4000);
     l_SumLen   INTEGER := 0;
     l_TRAN     CLOB;
     l_FIN      CLOB;
     l_FINDEPO  CLOB;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана процедура GENBLOCTRANSDET');
   --Формирование списка всех транзакций по счету
     FOR l_c IN(SELECT length(tran) len, tran, t_safedepopart, t_fiid
                  FROM (select ':16R:TRANSDET'||chr(13)||
                               ':36В::PSTA//UNIT/'||r.t_PstaQuant||decode(length(abs(-r.t_PstaQuant -trunc(-r.t_PstaQuant))) -1, 0, ',')||chr(13)||
                                 decode(r.t_PstaSumm, 0, NULL,
                                        ':19A::PSTA//'||v.t_IssueNomCurrency||r.t_PstaSumm||decode(length(abs(-r.t_PstaSumm -trunc(-r.t_PstaSumm))) -1, 0, ',')||chr(13))||
                               ':22F::TRAN/'||r.t_KindOper||chr(13)||
                               ':22H::PAYM/'||r.t_PaymSign||chr(13)||
                               ':22H::REDE/'||r.t_DealType||chr(13)||
                                 decode(to_char(r.t_EsetTime, 'HH24MISS'), '000000',
                                        ':98A::ESET//'||to_char(r.t_EsetDate, 'YYYYMMDD'),
                                        ':98C::ESET//'||to_char(r.t_EsetDate, 'YYYYMMDD')||to_char(r.t_EsetTime, 'HH24MISS'))||chr(13)||
                                 decode(to_char(r.t_SettTime, 'HH24MISS'), '000000',
                                        ':98A::SETT//'||to_char(r.t_SettDate, 'YYYYMMDD'),
                                        ':98C::SETT//'||to_char(r.t_SettDate, 'YYYYMMDD')||to_char(r.t_SettTime, 'HH24MISS'))||chr(13)||
                               ':16S:TRANSDET' tran, v.t_safedepopart, v.t_fiid
                          from AccTransactionsOP r, AccTransactionsFI v
                          where r.t_fiid = v.t_fiid)
                ORDER BY t_safedepopart, t_fiid) LOOP
     --Для "нового" ФИ
       IF l_FI <> l_c.t_fiid THEN
         IF l_TRAN IS NOT NULL THEN
           l_FINDEPO := l_FINDEPO||l_FIN||l_TRAN||chr(13)||':16S:TRAN'||chr(13)||':16S:FIN'||chr(13);
         END IF;
         IF nvl(l_Safe, l_c.t_safedepopart) <> l_c.t_safedepopart THEN
           IF l_TRAN IS NOT NULL THEN
             l_FINDEPO := l_FINDEPO;
           --Сохранение блока TRAN "старого" ФИ
             RSP_SECURITIES.InsBlocFIN(P_FIN => l_FINDEPO,
                                       P_FI  => l_FI);
             l_FINDEPO := NULL;
           --Обнуление счетчика
             l_SumLen := 0;
           END IF;
         END IF;
       --Получение "нового" счета депо
         l_Safe := l_c.t_safedepopart;
       --Получение ИД "нового" ФИ
         l_FI := l_c.t_fiid;
       --Получение описания ФИ
         l_sql_stmt := 'SELECT '||RSP_SECURITIES.GENBLOCFIN('536')||'||'':16R:TRAN''
                          FROM AccTransactionsFI v
                          WHERE v.t_fiid = :FI';
         EXECUTE IMMEDIATE l_sql_stmt INTO l_FIN USING l_FI;
       --Обнуление списка транзакций ФИ
         l_TRAN := NULL;
       END IF;
     --Для "текущего" ФИ
       IF l_FI = l_c.t_fiid THEN
       --Увеличение счетчика
         l_SumLen := l_SumLen + l_c.len;
         IF l_SumLen <= 9000 THEN
         --Формирование блока TRANSDET
           l_TRAN := l_TRAN||chr(13)||l_c.tran;
         ELSE
           l_FINDEPO := l_FINDEPO||l_FIN||l_TRAN||chr(13)||':16S:TRAN'||chr(13)||':16S:FIN'||chr(13);
         --Сохранение блока TRAN "текущего" ФИ
           RSP_SECURITIES.InsBlocFIN(P_FIN => l_FINDEPO,
                                     P_FI  => l_FI);
           l_FINDEPO := NULL;
         --Новое значение счетчика
           l_SumLen := l_c.len;
         --Формирование нового блока TRANSDET для "текущего" ФИ
           l_TRAN := chr(13)||l_c.tran;
         END IF;
       END IF;
     END LOOP;
   --Сохранение последнего блока TRAN
     l_FINDEPO := l_FINDEPO||l_FIN||l_TRAN||chr(13)||':16S:TRAN'||chr(13)||':16S:FIN'||chr(13);
     RSP_SECURITIES.InsBlocFIN(P_FIN  => l_FINDEPO,
                               P_FI   => l_FI);
   end GenBlocTRANSDET;
   procedure GETPAGESMT535 as
   /*Процедура определяет количество страниц в выписке и
     для каждой страницы формирует набор блоков FIN.
   */
     l_sql_stmt CLOB;
     l_c        SYS_REFCURSOR;
     l_FIN      CLOB;
     l_Len      INTEGER;
     l_Cons     VARCHAR2(1);
     l_SumLen   INTEGER;
     l_Depo     VARCHAR2(35 CHAR);
     l_Acti     VARCHAR2(1);
     l_SUBSAFE  CLOB;
     l_NumPage  INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GETPAGESMT53X');
   --Формирование блока(ов) SUBSAFE
     l_sql_stmt := 'SELECT fin, length(fin), t_SafeDepoPart, t_PartActi, t_ExtCons
                      FROM (select REPLACE(REPLACE(XMLAGG(XMLELEMENT("A",to_clob(fin)) ORDER BY 1).getClobVal(), ''<A>'',''''), ''</A>'') as fin,
                                   t_ExtCons, t_SafeDepoPart, t_PartActi
                              from (select distinct t.fin, t.t_ExtCons, t.t_SafeDepoPart, t.t_PartActi
                                      from (select '||RSP_SECURITIES.GENBLOCFIN(P_KINDMSG => '535')||'||chr(13) as fin,
                                                    b.t_ExtCons, v.t_SafeDepoPart, p.t_PartActi
                                              from AccBalance b, AccBalanceFI v, AccBalanceRest r, AccBalancePartition p,
                                                   (select r.t_FIID, sum(r.t_Quantity) as quantity
                                                      from AccBalanceRest r, AccBalancePartition p
                                                      where p.t_PartActi = upper(''Y'') and r.t_SafeDepoPart = p.t_SafeDepoPart
                                                    group by r.t_FIID) a
                                              where v.t_FIID = r.t_FIID and p.t_PartActi = upper(''Y'')
                                                and v.t_safedepopart = p.t_safedepopart and v.t_FIID = a.t_FIID) t)
                            group by t_ExtCons, t_SafeDepoPart, t_PartActi)';
     OPEN l_c FOR l_sql_stmt;
       LOOP
         FETCH l_c INTO l_FIN, l_Len, l_Depo, l_Acti, l_Cons;
         EXIT WHEN l_c%NOTFOUND;
         RSP_SECURITIES.LOOPPAGEMT53X(P_FIN     => l_FIN,
                                      P_LEN     => l_Len,
                                      P_DEPO    => l_Depo,
                                      P_ACTI    => l_Acti,
                                      P_CONS    => l_Cons,
                                      P_NUMPAGE => l_NumPage,
                                      P_SUMLEN  => l_SumLen,
                                      P_SUBSAFE => l_SUBSAFE);
       END LOOP;
     CLOSE l_c;
     --Сохранение "последнего" блока SUBSAFE
       RSP_SECURITIES.InsBlocSUBSAFE(P_CONS    => l_Cons,
                                     P_SUBSAFE => l_SUBSAFE,
                                     P_NUMPAGE => nvl(l_NumPage, 1));
   end GetPagesMT535;
   procedure GETPAGESMT536 as
   /*Процедура определяет количество страниц в выписке и
     для каждой страницы формирует набор блоков FIN.
   */
     l_c        SYS_REFCURSOR;
     l_FIN      CLOB;
     l_Len      INTEGER;
     l_Cons     VARCHAR2(1);
     l_SumLen   INTEGER;
     l_Depo     VARCHAR2(35 CHAR);
     l_Acti     VARCHAR2(1);
     l_SUBSAFE  CLOB;
     l_NumPage  INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GETPAGESMT536');
   --Формирование блоков TRANSDET для каждого ФИ
     RSP_SECURITIES.GenBlocTRANSDET;
   --Формирование блока(ов) SUBSAFE
     OPEN l_c FOR SELECT t.tran, length(t.tran), v.t_safedepopart, p.t_partacti,
                         (select b.t_ExtCons
                            from AccTransactions b) as t_ExtCons
                    FROM tmp_transdet t, AccTransactionsFI v, AccTransactionsPartition p
                    WHERE t.id = v.t_fiid AND p.t_safedepopart = v.t_safedepopart
                  ORDER BY v.t_safedepopart;
       LOOP
         FETCH l_c INTO l_FIN, l_Len, l_Depo, l_Acti, l_Cons;
         EXIT WHEN l_c%NOTFOUND;
         RSP_SECURITIES.LOOPPAGEMT53X(P_FIN     => l_FIN,
                                      P_LEN     => l_Len,
                                      P_DEPO    => l_Depo,
                                      P_ACTI    => l_Acti,
                                      P_CONS    => l_Cons,
                                      P_NUMPAGE => l_NumPage,
                                      P_SUMLEN  => l_SumLen,
                                      P_SUBSAFE => l_SUBSAFE);
       END LOOP;
     CLOSE l_c;
   --Сохранение "последнего" блока SUBSAFE
     RSP_SECURITIES.InsBlocSUBSAFE(P_CONS    => l_Cons,
                                   P_SUBSAFE => l_SUBSAFE,
                                   P_NUMPAGE => nvl(l_NumPage, 1));
   end GetPagesMT536;
   procedure LOOPPAGEMT53X (P_FIN in clob,P_LEN in integer,P_DEPO in varchar2,P_ACTI in char,P_CONS in char,P_NUMPAGE in out integer,P_SUMLEN in out integer,P_SUBSAFE in out clob) as
   /*Процедура формирует блоки SUBSAFE и вставляет их во временную таблицу.
     p_FIN     - блок FIN,
     p_Len     - размер блока FIN,
     p_Depo    - раздел счета депо,
     p_Acti    - признак наличия остатков или транзакций по разделу счета депо,
     p_Cons    - признак формирования консолидированной выписки,
     p_NumPage - номер страницы сообщения,
     p_SumLen  - суммарный размер блоков FIN,
     p_SUBSAFE - сформированный блок SUBSAFE.*/
   begin
     p_SumLen := nvl(p_SumLen, 0) + p_Len;
     IF p_SumLen <= 9300 THEN
       p_SUBSAFE := RSP_SECURITIES.GenBlocSUBSAFE(P_CONS    => p_Cons,
                                                  P_SUBSAFE => p_SUBSAFE,
                                                  P_DEPO    => p_Depo,
                                                  P_ACTI    => p_Acti,
                                                  P_FIN     => p_FIN);
     ELSE
       RSP_SECURITIES.InsBlocSUBSAFE(P_CONS     => p_Cons,
                                     P_SUBSAFE  => p_SUBSAFE,
                                     P_NUMPAGE  => nvl(p_NumPage, 1),
                                     P_CODEPAGE => 'MORE');
       p_NumPage := nvl(p_NumPage, 1) + 1;
       p_SumLen := p_Len;
       p_SUBSAFE := RSP_SECURITIES.GenBlocSUBSAFE(P_CONS    => p_Cons,
                                                  P_SUBSAFE => NULL,
                                                  P_DEPO    => p_Depo,
                                                  P_ACTI    => p_Acti,
                                                  P_FIN     => p_FIN);
     END IF;
   end LoopPageMT53x;
   procedure INSBLOCFIN (P_FIN in clob,P_FI in integer) as
   /*Процедура заполняет временную таблицу блоками FIN.
     p_FIN - блок FIN,
     p_FI  - идентификатор финансового инструмента.
   */
   begin
     INSERT INTO tmp_transdet(tran, id)
       VALUES(p_FIN, p_FI);
   end InsBlocFIN;
   procedure INSBLOCSUBSAFE (P_CONS in char,P_SUBSAFE in clob,P_NUMPAGE in integer,P_CODEPAGE in char default null) as
   /*Процедура заполняет временную таблицу блоками SUBSAFE.
     p_Cons     - признак формирования консолидированной выписки,
     p_SUBSAFE  - блок SUBSAFE,
     p_NumPage  - номер страницы сообщения,
     p_CodePage - код страницы сообщения,
   */
   begin
     INSERT INTO tmp_mt53x(subsafe, f_28e, id)
       VALUES(CASE p_Cons WHEN 'Y' THEN p_SUBSAFE
                          WHEN 'N' THEN chr(13)||':16R:SUBSAFE'||chr(13)||p_SUBSAFE||':16S:SUBSAFE'
              END, p_NumPage||'/'||nvl(p_CodePage, decode(p_NumPage,  1, 'ONLY', 'LAST')), p_NumPage);
   end InsBlocSUBSAFE;
   function CHECKUNDO (P_DRAFTKIND in integer,P_DRAFTID in integer,P_OPER in integer,P_DEPARTMENT in integer,P_TEXT out varchar2) return INTEGER as
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
     p_Text := chr(1);
   --Получение и блокировка списка платеже
     OPEN l_c FOR SELECT p.id_paym, p.id_sstate, p.versionrec
                    FROM paym p
                    WHERE p.id_paym IN(select decode(p_DraftKind, 1, max(id_paym),
                                                                  5, max(id_paym),
                                                                  6, max(id_paym),
                                                                  id_paym)
                                         from payass
                                         where associate = to_char(p_DraftID) and bankid = to_char(p_DraftKind)
                                       group by id_paym)
                      AND p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                                        P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат')) FOR UPDATE NOWAIT;
       LOOP
         FETCH l_c INTO l_IDPaym, l_IDState, l_VR;
       --Если список пустой
         IF l_IDPaym IS NULL THEN
           Return 1;
         END IF;
         EXIT WHEN l_c%NOTFOUND;
       --Блокировка пакета
         IF p_DraftKind IN(1, 5, 6) THEN
           SELECT i.id_ipspck, i.versionrec INTO l_IDPack, l_VRPack
             FROM ipspck i
             WHERE i.id_paym = l_IDPaym FOR UPDATE NOWAIT;
         END IF;
       --Определение кода состояния
         SELECT code INTO l_CodeState
           FROM sstate
           WHERE id_sstate = l_IDState;
       --Анализ состояния
         IF l_CodeState = RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отправки/приема\Отправлен') THEN
           p_Text := 'Откат невозможен: инструкция отправлена';
           Return 2;
         ELSIF RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Для отката') LIKE('%'||l_CodeState||'%') THEN
         --Формирование пачки платежей
           INSERT INTO tmp_paym_pack(id_paym, id_sstate, varlen, vr)
             VALUES(l_IDPaym, RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                             P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат')), NULL, l_VR);
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
       IF p_DraftKind IN(1, 5, 6) THEN
         f1 := RSP_PAYM_API.PackChangeState(P_IDPACK  => l_IDPack,
                                            P_VR      => l_VRPack,
                                            P_IDSTATE => RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTPCK',
                                                                                        P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния пакета\Завершенный')),
                                            P_VARLEN  => 'Отозвана инструкция');
         IF f1 > 0 THEN
           p_Text := p_Text||' Пакет '||l_IDPack||' завершен.';
           Return 0;
         END IF;
       END IF;
     --Передача состояния в АБС
       RSB_PAYMENTS_API.InsertDLMesParams(p_DocKind   => g_KindMsg,
                                          p_DocID     => p_DraftID,
                                          p_Status    => 0,
                                          p_Condition => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат'));
       Return 0;
     END IF;
   end CheckUndo;
   function CHECKUNDONETTCONS (P_BANKID in varchar2,P_OPER in integer,P_DEPARTMENT in integer,P_RESULT out T_REFMTNETT) return INTEGER as
   /*Функция выполняет откат всех шагов сводного сообщения, если оно ещё не было отправлено. */
     l_Cnt    INTEGER;
     f1       INTEGER;
     l_Text   VARCHAR2(128);
     i        INTEGER := 0;
     l_IDPack INTEGER;
     l_VRPack INTEGER;
   begin
     RSP_ADM.SET_USERID(USERID => to_char(p_Oper));
   --Проверка наличия сводных сообщений по контрагенту
     SELECT count(*)
       INTO l_Cnt
       FROM paym p
      WHERE p.receiverbankid = p_BankID
        AND p.numpack = RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Пачка сводного')
        AND p.valuedate = RSP_COMMON.Get_DateOperDay;
     IF l_Cnt = 0 THEN
       Return 1;
     END IF;
   --Отбор сводных сообщений
     FOR c IN(SELECT p.id_paym, p.numreferdoc, p.datereferdoc, p.id_sstate, p.versionrec
                FROM paym p,
                     sstate s
               WHERE p.receiverbankid = p_BankID
                 AND s.id_sstate = p.id_sstate
                 AND p.numpack = RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Пачка сводного')
                 AND p.valuedate = RSP_COMMON.Get_DateOperDay
                 AND RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Для отката') LIKE('%'||s.code||'%')) LOOP
     --Изменение истории платежа
       DELETE FROM tmp_paym_pack;
       INSERT INTO tmp_paym_pack(id_paym, id_sstate, vr, varlen)
         VALUES(c.id_paym, RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTATE',
                                                          P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат')),
                c.versionrec, 'Расформирован сводный платеж');
       f1 := RSP_GATE.MASSCHANGESTATE(P_TBLNAME => 'TMP_PAYM_PACK');
       IF f1 > 0 THEN
         l_Text := 'Расформирован сводный платеж '||c.id_paym||': № '||c.numreferdoc||' от '||to_char(c.datereferdoc, 'dd.mm.yyyy')||'.';
         i := i + 1;
       --Создание массива откаченных платежей
         p_Result(i).NumRef  := c.numreferdoc;
         p_Result(i).DateRef := c.datereferdoc;
         p_Result(i).IDPaym  := c.id_paym;
         p_Result(i).text    := l_Text;
       --Изменение истории первичных платежей
         DELETE FROM tmp_paym_pack;
         INSERT INTO tmp_paym_pack(id_paym, id_sstate, vr, varlen)
           SELECT pp.id_paym, RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTATE',
                                                             P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отправки/приема\Отложен')),
                  p.versionrec, 'Расформирован сводный платеж ID='||c.id_paym
                    FROM pckpay pp,
                         ipspck ip,
                         paym p
                   WHERE ip.id_paym = c.id_paym
                     AND pp.id_ipspck = ip.id_ipspck
                     AND p.id_paym = pp.id_paym
                     AND p.id_sstate = RSP_COMMON.Get_ID_By_Code_Dict(P_TABLE => 'SSTATE',
                                                                      P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Состояния отправки/приема\Добавлен в сводное'));
         f1 := RSP_GATE.MASSCHANGESTATE(P_TBLNAME => 'TMP_PAYM_PACK');
       --Изменение состояния пакета
         SELECT i.id_ipspck, i.versionrec
           INTO l_IDPack, l_VRPack
           FROM ipspck i
          WHERE i.id_paym = c.id_paym;
         f1 := RSP_PAYM_API.PackChangeState(P_IDPACK  => l_IDPack,
                                            P_VR      => l_VRPack,
                                            P_IDSTATE => RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTPCK',
                                                                                        P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния пакета\Завершенный')),
                                            P_VARLEN  => 'Отозвана инструкция');
       END IF;
     END LOOP;
     IF i = 0 THEN
       Return 2;
     END IF;
     Return 0;
   end CheckUndoNettCons;
   function GETMSGTEXT (P_DRAFTKIND in integer,P_DRAFTID in integer,P_MSG out clob,P_KINDMSG in integer default 0) return INTEGER as
   /*Функция возвращает текст отправленных сообщений,
     сформированных по заданному поручению*/
     l_ID        INTEGER;
     l_DraftKind INTEGER := ltrim(P_DRAFTKIND, '-');
   begin
     IF P_DRAFTKIND LIKE '-%' THEN
       p_Msg := 'По данному сообщению в транспортной системе выполнен откат ';
       Return 0;
     END IF;
     IF l_DraftKind IN(4) THEN
       SELECT m.text as msg
         INTO p_Msg
         FROM (select max(p.id_paym) as id_paym
                 from payass p
                where p.associate = to_char(p_DraftID) and p.bankid = to_char(l_DraftKind)
                  and p.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                                 P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Ассоциация'))
                group by p.associate, p.bankid) p, mtn9x m
        WHERE m.id_paym = p.id_paym;
     ELSIF l_DraftKind IN(2, 8) THEN
       SELECT rtrim(replace(replace(xmlagg(xmlelement("A",
                                                      to_clob(replace(m.str, chr(1)))) order by m.id_paym).getClobVal(),
                                    '<A>', ''),
                            '</A>', chr(13)||chr(13)),
                    chr(13)) as msg INTO p_Msg
         FROM (select max(p.id_paym) as id_paym
                 from payass p
                 where p.associate = to_char(p_DraftID) and p.bankid = to_char(l_DraftKind)
                   and p.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                                  P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Ассоциация'))
               group by p.associate, p.bankid) p, mtrec m
         WHERE m.id_paym = p.id_paym;
     ELSIF l_DraftKind IN(7) THEN
       l_ID := RSP_SECURITIES.GET_CONSIDBYDRAFTID(P_DRAFTID => p_DraftID);
       SELECT m.text as msg
         INTO p_Msg
         FROM mtn9x m
        WHERE m.id_paym = l_ID;
     ELSE
       IF p_KindMsg IN(600, 604, 605) THEN
         SELECT o.str msg
           INTO p_Msg
           FROM (select max(pa.id_paym) as id_paym
                   from payass pa, paym pm, mtrec mt
                  where pa.id_paym = pm.id_paym
                    and pm.id_sstate = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                                      P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Состояния отправки/приема\Отправлен'))
                    and pa.associate = to_char(p_DraftID)
                    and pa.bankid = to_char(p_DraftKind)
                    and mt.id_paym = pm.id_paym
                    and mt.type = p_KindMsg
                    and pa.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                                    P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Ассоциация'))
                  group by pa.associate, pa.bankid) p, origin o, pexp pex
          WHERE o.id_pexp = pex.id_pexp
            AND pex.id_paym = p.id_paym;
       ELSE
          SELECT m.str as msg INTO p_Msg
            FROM (select max(p.id_paym) as id_paym
                    from payass p, mtrec mt
                   where p.associate = to_char(p_DraftID)
                     and p.bankid = to_char(p_DraftKind)
                     and mt.id_paym = p.id_paym
                     and mt.type = p_KindMsg
                     and p.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                                    P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Ассоциация'))
                   group by p.associate, p.bankid) p, mtrec m
           WHERE m.id_paym = p.id_paym;
       END IF;
     END IF;
     Return 0;
   exception
     WHEN NO_DATA_FOUND THEN
       Return 1;
   end GetMsgText;
   function GETMSGTEXT_SWP (P_DRAFTKIND in integer,P_DRAFTID in integer,P_PARTSWP in integer,P_MSG out clob,P_KINDMSG in integer) return INTEGER as
   /*Функция возвращает текст отправленных сообщений,
     сформированных по заданному поручению*/
     l_DraftKind INTEGER := ltrim(P_DRAFTKIND, '-');
   begin
     IF l_DraftKind = 600 THEN
       SELECT o.str msg
         INTO p_Msg
         FROM (select decode(p_PartSWP, 1, min(pa.id_paym), max(pa.id_paym)) as id_paym
                 from payass pa, paym pm, mtrec mt
                where pa.id_paym = pm.id_paym
                  and pm.id_sstate = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                                    P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Состояния отправки/приема\Отправлен'))
                  and pa.associate = to_char(p_DraftID)
                  and pa.bankid = to_char(l_DraftKind)
                  and mt.id_paym = pa.id_paym
                  and mt.type = p_KindMsg
                  and pa.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                                  P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Ассоциация'))
                group by pa.associate, pa.bankid) p, origin o, pexp pex
        WHERE o.id_pexp = pex.id_pexp
          AND pex.id_paym = p.id_paym;
     END IF;
     Return 0;
   exception
     WHEN NO_DATA_FOUND THEN
       Return 1;
   end GetMsgText_SWP;
   function GETCONFIRMPARAMS (P_DRAFTKIND in integer,P_DRAFTID in integer,P_NUM out varchar2,P_DATE out varchar2,P_TYPEIN out varchar2,P_STATUS out varchar2,P_REASON out varchar2,P_COMMENT out varchar2) return INTEGER as
   /*Функция возвращает параметры подтверждения, сквитованного с исходным сообщением по поручению*/
     l_IDPaym   INTEGER;
     l_Stat     VARCHAR2(10);
     l_Func23G  VARCHAR2(10);
   begin
     p_Num    := chr(1);
     p_Date   := to_date('01.01.0001', 'DD.MM.YYYY');
   --Определение ID подтверждения
     l_IDPaym := RSP_SECURITIES.GetConfirmID(P_DRAFTKIND => p_DraftKind,
                                             P_DRAFTID   => p_DraftID,
                                             p_TypeIN    => p_TypeIN);
     IF l_IDPaym IS NULL THEN
       Return 1;
     ELSIF l_IDPaym = 0 THEN
       l_IDPaym := RSP_SECURITIES.GetRepStatusID(P_DRAFTKIND => p_DraftKind,
                                                 P_DRAFTID   => p_DraftID);
       IF l_IDPaym = 0 THEN
         Return 2;
       ELSE
       --Получение параметров статуса
         WITH ttabl AS (select to_char(ltrim(substr(txt,
                                                    instr(txt, chr(10), 1, level) + 1,
                                                    instr(txt, chr(10), 1, level + 1) - instr(txt, chr(10), 1, level) - 1))) str,
                               to_char(replace(substr(substr(txt, instr(txt, ':70D::'),
                                                                  instr(substr(txt, instr(txt, ':70D::')),
                                                                        chr(10)||':')),
                                                      13),
                                               chr(10))) f70D,
                               typ
                          from (select chr(10)||str||chr(10) txt, typ
                                  from (select str, type typ
                                          from mtrec
                                          where id_paym = l_IDPaym))
                        connect by level <= length(txt) - length(replace(txt, chr(10))) - 1)
           SELECT p.numreferdoc p_Num,
                  (select to_char(to_date(substr(str, 13, 14), 'yyyy.mm.dd hh24:mi:ss'), 'dd.mm.yyyy hh24:mi:ss')
                     from ttabl
                    where str like (':98_::PREP//%')) p_Date,
                  (select substr(str, 7, 10)
                     from ttabl
                    where str like (':25D::%') and rownum = 1) l_Stat,
                  (select substr(str, 7, 10)
                     from ttabl
                    where str like (':24B::%') and rownum = 1) p_Reason,
                  (select f70D
                     from ttabl
                    where str like (':70D::%')) p_Comment,
                  (select distinct typ
                     from ttabl) p_TypeIN
             INTO p_Num,
                  p_Date,
                  l_Stat,
                  p_Reason,
                  p_Comment,
                  p_TypeIN
             FROM paym p
            WHERE p.id_paym = l_IDPaym;
         --p_Status := RSP_SECURITIES.GetCodeStatus(P_25D => l_Stat);
         p_Status := l_Stat;
       END IF;
       p_TypeIN := 'MT'||p_TypeIN;
       Return 0;
     ELSE
     --Получение выходных параметров
       WITH ttabl AS (select to_char(ltrim(substr(txt,
                                                  instr(txt, chr(10), 1, level) + 1,
                                                  instr(txt, chr(10), 1, level + 1) - instr(txt, chr(10), 1, level) - 1))) str
                        from (select chr(10)||str||chr(10) txt
                                from (select str
                                        from mtrec
                                        where id_paym = l_IDPaym))
                      connect by level <= length(txt) - length(replace(txt, chr(10))) - 1)
       SELECT p.numreferdoc,
              (select to_char(to_date(substr(str, 13, 14), 'yyyy.mm.dd hh24:mi:ss'), 'dd.mm.yyyy hh24:mi:ss')
                 from ttabl
                 where str like(':98_::ESET//%') or str like(':98_::PREP//%')),
              (select substr(str, 6, 4)
                 from ttabl
                 where str like(':23G:%')) status INTO p_Num, p_Date, l_Func23G
         FROM paym p
         WHERE p.id_paym = l_IDPaym;
       p_TypeIN := 'MT'||p_TypeIN;
       IF l_Func23G = 'NEWM' THEN
         p_Status := 0;
       ELSE
         p_Status := 1;
       END IF;
       Return 0;
     END IF;
   end GetConfirmParams;
   function GETCONFIRMPARAMS_EXT (P_DRAFTKIND in integer,P_DRAFTID in integer,P_NUM out varchar2,P_DATE out varchar2,P_TYPEIN out varchar2,P_STATUS out varchar2,P_REASON out varchar2,P_COMMENT out varchar2,P_AMOUNT out varchar2) return INTEGER as
   /*Функция возвращает параметры подтверждения, сквитованного с исходным сообщением по поручению*/
     l_IDPaym   INTEGER;
     l_Stat     VARCHAR2(10);
     l_Func23G  VARCHAR2(10);
   begin
     p_Num    := chr(1);
     p_Date   := to_date('01.01.0001', 'DD.MM.YYYY');
   --Определение ID подтверждения
     l_IDPaym := RSP_SECURITIES.GetConfirmID_ext(P_DRAFTKIND => p_DraftKind,
                                                 P_DRAFTID   => p_DraftID,
                                                 p_TypeIN    => p_TypeIN);
     IF l_IDPaym IS NULL THEN
       Return 1;
     ELSIF l_IDPaym <> 0 THEN
   --    l_IDPaym := RSP_SECURITIES.GetRepStatusID(P_DRAFTKIND => p_DraftKind,
   --                                              P_DRAFTID   => p_DraftID);
       IF l_IDPaym = 0 THEN
         Return 2;
       ELSE
       --Получение параметров статуса
         WITH ttabl AS (select to_char(ltrim(substr(txt,
                                                    instr(txt, chr(10), 1, level) + 1,
                                                    instr(txt, chr(10), 1, level + 1) - instr(txt, chr(10), 1, level) - 1))) str,
                               to_char(replace(substr(substr(txt, instr(txt, ':70D::'),
                                                                  instr(substr(txt, instr(txt, ':70D::')),
                                                                        chr(10)||':')),
                                                      13),
                                               chr(10))) f70D,
                               typ
                          from (select chr(10)||str||chr(10) txt, typ
                                  from (select str, type typ
                                          from mtrec
                                          where id_paym = l_IDPaym))
                        connect by level <= length(txt) - length(replace(txt, chr(10))) - 1)
         SELECT p.numreferdoc p_Num,
                (select to_char(to_date(substr(str, 13, 14), 'yyyy.mm.dd hh24:mi:ss'), 'dd.mm.yyyy hh24:mi:ss')
                   from ttabl
                  where str like(':98_::PREP//%')) p_Date,
                (select substr(str, 7, 10)
                   from ttabl
                  where str like(':25D::IPRC%') and rownum = 1) l_Stat,
                (select substr(str, 7, 10)
                   from ttabl
                  where str like(':24B::%') and rownum = 1) p_Reason,
                (select f70D
                   from ttabl
                  where str like(':70D::%')) p_Comment,
                replace((select substr(str, 7)
                   from ttabl
                  where str like(':19A::%')),',','.') p_Amount,
                (select distinct typ
                   from ttabl) p_TypeIN
           INTO p_Num, p_Date, l_Stat, p_Reason, p_Comment, p_Amount, p_TypeIN
           FROM paym p
          WHERE p.id_paym = l_IDPaym;
         --p_Status := RSP_SECURITIES.GetCodeStatus(P_25D => l_Stat);
         p_Status :=  l_Stat;
       END IF;
       p_TypeIN := 'MT'||p_TypeIN;
       Return 0;
     ELSE
     --Получение выходных параметров
       WITH ttabl AS (select to_char(ltrim(substr(txt,
                                                  instr(txt, chr(10), 1, level) + 1,
                                                  instr(txt, chr(10), 1, level + 1) - instr(txt, chr(10), 1, level) - 1))) str
                        from (select chr(10)||str||chr(10) txt
                                from (select str
                                        from mtrec
                                        where id_paym = l_IDPaym))
                      connect by level <= length(txt) - length(replace(txt, chr(10))) - 1)
       SELECT p.numreferdoc,
              (select to_char(to_date(substr(str, 13, 14), 'yyyy.mm.dd hh24:mi:ss'), 'dd.mm.yyyy hh24:mi:ss')
                 from ttabl
                 where str like(':98_::ESET//%') or str like(':98_::PREP//%')),
              (select substr(str, 6, 4)
                 from ttabl
                 where str like(':23G:%')) status INTO p_Num, p_Date, l_Func23G
         FROM paym p
         WHERE p.id_paym = l_IDPaym;
       p_TypeIN := 'MT'||p_TypeIN;
       IF l_Func23G = 'NEWM' THEN
         p_Status := 0;
       ELSE
         p_Status := 1;
       END IF;
       Return 0;
     END IF;
   end GetConfirmParams_ext;
   function GETCONFIRMPARAMSSEC (P_DRAFTID in integer,P_NUM out varchar2,P_DATE out varchar2,P_TYPEIN out varchar2,P_STATE out integer) return INTEGER as
   /*Функция возвращает параметры подтверждения, сквитованного с исходным сообщением по поручению*/
     l_IDPaym INTEGER;
     l_TypeIN INTEGER;
   begin
     p_Num    := chr(1);
     p_Date   := to_char(to_date('01.01.0001', 'DD.MM.YYYY'));
   --Определение ID подтверждения
     l_IDPaym := RSP_SECURITIES.GetConfirmIDSEC(P_DRAFTID   => p_DraftID,
                                                p_TypeIN    => l_TypeIN);
     p_TypeIN := 'MT'||l_TypeIN;
     IF l_IDPaym IS NULL THEN
       Return 1;
     ELSIF l_IDPaym = 0 THEN
       Return 2;
     ELSE
     --Получение выходных параметров
       SELECT p.numreferdoc,
              nvl(to_char(p.datereferdoc, 'dd.mm.yyyy hh24:mi:ss'), '01.01.0001'),
              decode(p.id_sstate, RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                                 P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отправки/приема\Сквитован')),
                                  0, 1)
         INTO p_Num, p_Date, p_State
         FROM paym p
        WHERE p.id_paym = l_IDPaym;
       Return 0;
     END IF;
   end GetConfirmParamsSEC;
   function GETCONFIRMPARAMSSECNETT (P_DRAFTID in integer,P_NUM out varchar2,P_DATE out varchar2,P_TYPEIN out varchar2,P_STATE out integer) return INTEGER as
   /*Функция возвращает параметры подтверждения, сквитованного с исходным сообщением по поручению*/
     l_IDPaym INTEGER;
     l_TypeIN INTEGER;
   begin
     p_Num    := chr(1);
     p_Date   := to_date('01.01.0001', 'DD.MM.YYYY');
   --Определение ID подтверждения
     l_IDPaym := RSP_SECURITIES.GetConfirmIDSECNETT(P_DRAFTID => p_DraftID,
                                                    p_TypeIN  => l_TypeIN);
     p_TypeIN := 'MT'||l_TypeIN;
     IF l_IDPaym IS NULL THEN
       Return 1;
     ELSIF l_IDPaym = 0 THEN
       Return 2;
     ELSE
     --Получение выходных параметров
       SELECT p.numreferdoc,
              to_char(p.datereferdoc, 'dd.mm.yyyy hh24:mi:ss'),
              decode(p.id_sstate,
                     RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                    P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отправки/приема\Сквитован')),
                     0,
                     1)
         INTO p_Num, p_Date, p_State
         FROM paym p
         WHERE p.id_paym = l_IDPaym;
       Return 0;
     END IF;
   end GetConfirmParamsSECNETT;
   function GETCONFIRMPARAMSAL return T_REFMTTABLE PIPELINED as
   /*Функция возвращает список входящих сообщений, не ассоциированных с исходящими*/
     l_c     SYS_REFCURSOR;
     l_Table T_TYPMTTABLE;
   begin
     OPEN l_c FOR SELECT m.type||mt.type as MT,
                         trunc(p.datereferdoc) as DateDoc,
                         to_char(p.datereferdoc, 'HH24:MI:SS') as TimeDoc,
                         m.sender||mt.sender as Sender,
                         m.receiver||mt.receiver as Receiver,
                         m.id_paym||mt.id_paym as IDPaym
                    FROM paym p, mtrec m, mtn9x mt
                   WHERE p.id_paym = m.id_paym(+) and p.id_paym = mt.id_paym(+)
                     AND (m.type = 518 or mt.type = 599)
                     AND p.id_sstate = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                                      P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отправки/приема\Обработан с ошибками'));
     LOOP
       FETCH l_c INTO l_Table;
       EXIT WHEN l_c%NOTFOUND;
       PIPE ROW (l_Table);
     END LOOP;
     CLOSE l_c;
     RETURN;
   end GetConfirmParamsAL;
   function GETCONFIRMTEXT (P_DRAFTKIND in integer,P_DRAFTID in integer,P_MSG out clob) return INTEGER as
   /*Функция возвращает текст подтверждения, сквитованного с исходным сообщением по поручению*/
     l_IDPaym    INTEGER;
     l_TypeIN    INTEGER;
     l_DraftKind INTEGER := ltrim(P_DRAFTKIND, '-');
   begin
   --Определение ID подтверждения
     IF l_DraftKind IN(7) THEN
       l_IDPaym := RSP_SECURITIES.GetConfirmIDSecNett(P_DRAFTID => p_DraftID,
                                                      p_TypeIN  => l_TypeIN);
     ELSE
       l_IDPaym := RSP_SECURITIES.GetConfirmID(P_DRAFTKIND => l_DraftKind,
                                               P_DRAFTID   => p_DraftID,
                                               p_TypeIN    => l_TypeIN);
     END IF;
     IF l_IDPaym IS NULL THEN
       Return 1;
     ELSIF l_IDPaym = 0 THEN
       Return 2;
     ELSE
     --Получение текста подтверждения
       IF l_DraftKind IN(600, 604, 605) THEN
         SELECT o.str
           INTO p_Msg
           FROM origin o, pexp pex, paym p
          WHERE p.id_paym = l_IDPaym
            AND p.id_paym = pex.id_paym
            AND pex.id_pexp = o.id_pexp;
         Return 0;
       ELSE
          SELECT m.str||mt.text
            INTO p_Msg
            FROM mtrec m, mtn9x mt, paym p
           WHERE p.id_paym = l_IDPaym
             AND p.id_paym = m.id_paym(+)
             AND p.id_paym = mt.id_paym(+);
          Return 0;
       END IF;
     END IF;
   end GetConfirmText;
   function GETCONFIRMTEXT_SWP (P_DRAFTKIND in integer,P_DRAFTID in integer,P_PARTSWP in integer,P_MSG out clob) return INTEGER as
   /*Функция возвращает текст подтверждения, сквитованного с исходным сообщением по поручению*/
     l_IDPaym INTEGER;
     l_TypeIN INTEGER;
     l_DraftKind INTEGER := ltrim(P_DRAFTKIND, '-');
   begin
   --Определение ID подтверждения
     l_IDPaym := RSP_SECURITIES.GetConfirmID_SWP(P_DRAFTKIND => l_DraftKind,
                                                 P_DRAFTID   => p_DraftID,
                                                 p_PartSWP   => p_PartSWP,
                                                 p_TypeIN    => l_TypeIN);
     IF l_IDPaym IS NULL THEN
       Return 1;
     ELSIF l_IDPaym = 0 THEN
       Return 2;
     ELSE
     --Получение текста подтверждения
       IF l_DraftKind = 600 THEN
         SELECT o.str
           INTO p_Msg
           FROM origin o, pexp pex, paym p
          WHERE p.id_paym = l_IDPaym
            AND p.id_paym = pex.id_paym
            AND pex.id_pexp = o.id_pexp;
         Return 0;
       ELSE
         SELECT m.str||mt.text
           INTO p_Msg
           FROM mtrec m, mtn9x mt, paym p
          WHERE p.id_paym = l_IDPaym
            AND p.id_paym = m.id_paym(+)
           AND p.id_paym = mt.id_paym(+);
         Return 0;
       END IF;
     END IF;
   end GetConfirmText_SWP;
   function GETCONFIRMID (P_DRAFTKIND in integer,P_DRAFTID in integer,P_TYPEIN out integer) return INTEGER as
   /*Определение ИД и типа ответного платежа (подтверждения)*/
     l_IDPaym  INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GETCONFIRMID');
   --Определение формы сообщения
     BEGIN
       SELECT CASE P_DRAFTKIND WHEN 8
                               THEN CASE m.TYPE WHEN 540 THEN 548
                                                WHEN 541 THEN 548
                                                WHEN 542 THEN 548
                                                WHEN 543 THEN 548
                                                WHEN 604 THEN 605
                                                WHEN 605 THEN 604
                                                ELSE TO_NUMBER(m.TYPE||mt.TYPE)
                                    END
                               ELSE CASE m.TYPE WHEN 540 THEN 544
                                                WHEN 541 THEN 545
                                                WHEN 542 THEN 546
                                                WHEN 543 THEN 547
                                                WHEN 604 THEN 605
                                                WHEN 605 THEN 604
                                                ELSE TO_NUMBER(m.TYPE||mt.TYPE)
                                    END
              END
         INTO p_TypeIN
         FROM mtrec m, paym p, mtn9x mt
        WHERE p.id_paym = (select max(id_paym)
                             from payass
                            where associate = to_char(P_DRAFTID)
                              and bankid = P_DRAFTKIND
                              and id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                                           P_CODE  => 3))
          AND m.id_paym(+) = p.id_paym
          AND mt.id_paym(+) = p.id_paym;
   --       AND p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
   --                                                         P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат'));
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         Return NULL;
     END;
   --Определение идентификатора подтверждения
     BEGIN
       SELECT nvl(max(pp.id_paym), max(a.id_paym))
         INTO l_IDPaym
         FROM pckpay pp, ipspck mp, payass a, mtrec m, payass a1, mtn9x mt
        WHERE pp.id_ipspck(+) = mp.id_ipspck
          AND mp.id_paym(+) = a.id_paym
          AND a1.id_paym = m.id_paym(+)
          AND a1.id_paym = mt.id_paym(+)
          AND a.associate = to_char(P_DRAFTID)
          AND a.bankid = to_char(nvl(P_DRAFTKIND, decode(m.type, 518, 3,
                                                                 599, 4,
                                                                 P_DRAFTKIND)))
          AND a1.id_paym = pp.id_paym
          AND a1.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                          P_CODE  => 1)
          AND m.type||mt.type = p_TypeIN
        GROUP BY m.type||mt.type;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         BEGIN
           SELECT nvl(max(pp.id_paym), 0)
             INTO l_IDPaym
             FROM pckpay pp, ipspck mp, payass a--, mtrec m, payass a1, mtn9x mt
            WHERE pp.id_ipspck(+) = mp.id_ipspck
              AND mp.id_paym(+) = a.id_paym
   --           AND a1.id_paym = m.id_paym(+)
   --           AND a1.id_paym = mt.id_paym(+)
              AND a.associate = to_char(P_DRAFTID);
         EXCEPTION
           WHEN NO_DATA_FOUND THEN
             Return 0;
         END;
     END;
     Return l_IDPaym;
   end GetConfirmID;
   function GETCONFIRMID_EXT (P_DRAFTKIND in integer,P_DRAFTID in integer,P_TYPEIN out integer) return INTEGER as
   /*Определение ИД и типа ответного платежа (подтверждения)*/
     l_IDPaym  INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GETCONFIRMID');
   --Определение формы сообщения
     BEGIN
       SELECT CASE m.type WHEN 540 THEN 544
                          WHEN 541 THEN 545
                          WHEN 542 THEN 546
                          WHEN 543 THEN 547
                          WHEN 604 THEN 605
                          WHEN 605 THEN 604
                          ELSE to_number(m.type||mt.type)
              END
         INTO p_TypeIN
         FROM mtrec m, paym p, mtn9x mt
        WHERE p.id_paym = (select max(id_paym)
                             from payass
                            where associate = to_char(P_DRAFTID)
                              and bankid = to_char(P_DRAFTKIND)
                              and id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                                           P_CODE  => 3))
          AND m.id_paym(+) = p.id_paym
          AND mt.id_paym(+) = p.id_paym;
   --       AND p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
   --                                                         P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат'));
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         Return NULL;
     END;
   --Определение идентификатора подтверждения
     BEGIN
       SELECT nvl(max(pp.id_paym), max(a.id_paym))
         INTO l_IDPaym
         FROM pckpay pp, ipspck mp, payass a, mtrec m, payass a1, mtn9x mt
        WHERE pp.id_ipspck(+) = mp.id_ipspck
          AND mp.id_paym(+) = a.id_paym
          AND a1.id_paym = m.id_paym(+)
          AND a1.id_paym = mt.id_paym(+)
          AND a.associate = to_char(P_DRAFTID)
          AND a.bankid = to_char(nvl(P_DRAFTKIND, decode(m.type, 518, 3,
                                                                 599, 4,
                                                                 P_DRAFTKIND)))
          AND a1.id_paym = pp.id_paym
          AND a1.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                          P_CODE  => 1)
          AND m.type||mt.type = p_TypeIN
        GROUP BY m.type||mt.type;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         BEGIN
           SELECT nvl(max(pp.id_paym), 0)
             INTO l_IDPaym
             FROM pckpay pp, ipspck mp, payass a--, mtrec m, payass a1, mtn9x mt
            WHERE pp.id_ipspck(+) = mp.id_ipspck
              AND mp.id_paym(+) = a.id_paym
   --           AND a1.id_paym = m.id_paym(+)
   --           AND a1.id_paym = mt.id_paym(+)
              AND a.associate = to_char(P_DRAFTID);
         EXCEPTION
           WHEN NO_DATA_FOUND THEN
             Return 0;
         END;
     END;
     Return l_IDPaym;
   end GetConfirmID_ext;
   function GETCONFIRMIDSEC (P_DRAFTID in integer,P_TYPEIN out integer) return INTEGER as
   /*Определение ИД и типа ответного платежа (подтверждения)*/
     l_IDPaym INTEGER;
     l_ID     INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GETCONFIRMID');
     BEGIN
       SELECT max(a.id_paym)
         INTO l_ID
         FROM payass a
        WHERE a.associate = to_char(p_DraftID)
          AND a.bankid IN(3, 4)
          AND a.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                         P_CODE  => 3);
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         Return NULL;
     END;
   --Определение формы сообщения
     SELECT m.type||mt.type, m.id_paym||mt.id_paym
       INTO p_TypeIN, l_IDPaym
       FROM mtrec m, mtn9x mt, paym p, payass a
      WHERE m.id_paym||mt.id_paym = (select max(pp.id_paym)
                                       from pckpay pp, pckpay pc
                                      where pp.id_ipspck = pc.id_ipspck
                                        and pc.id_paym = l_ID)
        AND m.id_paym(+) = p.id_paym
        AND mt.id_paym(+) = p.id_paym
        AND a.id_paym = p.id_paym
        AND a.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                       P_CODE  => 1)
        AND p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                          P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отправки/приема\Обработан с ошибками'));
     Return l_IDPaym;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       Return 0;
   end GetConfirmIDSEC;
   function GETCONFIRMIDSECNETT (P_DRAFTID in integer,P_TYPEIN out integer) return INTEGER as
   /*Определение ИД и типа ответного платежа (подтверждения)*/
     l_IDPaym INTEGER;
     l_ID     INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GETCONFIRMIDSECNETT');
     BEGIN
       SELECT max(a.id_paym)
         INTO l_ID
         FROM payass a
        WHERE a.associate = to_char(p_DraftID)
          AND a.bankid IN(7)
          AND a.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                         P_CODE  => 3);
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         Return NULL;
     END;
   --Определение формы сообщения
     SELECT mt.type, mt.id_paym
       INTO p_TypeIN, l_IDPaym
       FROM paym p, pckpay pp, payass pa, mtn9x mt
      WHERE p.id_paym = pp.id_paym
        AND pp.id_ipspck IN (select pp.id_ipspck
                              from pckpay pp
                             where pp.id_paym = l_ID)
        AND pa.id_paym = p.id_paym
        AND mt.id_paym = p.id_paym
        AND pa.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                        P_CODE  => 1)
        AND p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                          P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отправки/приема\Обработан с ошибками'));
     Return l_IDPaym;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       Return 0;
   end GetConfirmIDSECNETT;
   function GETCONFIRMID_SWP (P_DRAFTKIND in integer,P_DRAFTID in integer,P_PARTSWP in integer,P_TYPEIN out integer) return INTEGER as
   /*Определение ИД и типа ответного платежа (подтверждения)*/
     l_IDPaym  INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GETCONFIRMID');
   --Определение формы сообщения
     BEGIN
       SELECT CASE m.type WHEN 540 THEN 544
                          WHEN 541 THEN 545
                          WHEN 542 THEN 546
                          WHEN 543 THEN 547
                          ELSE to_number(m.type||mt.type)
              END
         INTO p_TypeIN
         FROM mtrec m, paym p, mtn9x mt
        WHERE p.id_paym = (select max(id_paym)
                             from payass
                            where associate = to_char(P_DRAFTID)
                              and bankid = to_char(P_DRAFTKIND)
                              and id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                                           P_CODE  => 3))
          AND m.id_paym(+) = p.id_paym
          AND mt.id_paym(+) = p.id_paym;
   --       AND p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
   --                                                         P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат'));
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         Return NULL;
     END;
   --Определение идентификатора подтверждения
     BEGIN
       SELECT nvl(decode(p_PartSWP, 1, min(pp.id_paym),
                                    2, max(pp.id_paym)),
                  decode(p_PartSWP, 1, min(a.id_paym),
                                    2, max(a.id_paym)))
         INTO l_IDPaym
         FROM pckpay pp, ipspck mp, payass a, mtrec m, payass a1, mtn9x mt
        WHERE pp.id_ipspck(+) = mp.id_ipspck
          AND mp.id_paym(+) = a.id_paym
          AND a1.id_paym = m.id_paym(+)
          AND a1.id_paym = mt.id_paym(+)
          AND a.associate = to_char(P_DRAFTID)
          AND a.bankid = to_char(nvl(P_DRAFTKIND, decode(m.type, 518, 3,
                                                                 599, 4,
                                                                 P_DRAFTKIND)))
          AND a1.id_paym = pp.id_paym
          AND a1.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                          P_CODE  => 1)
          AND m.type||mt.type = p_TypeIN
        GROUP BY m.type||mt.type;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         Return 0;
     END;
     Return l_IDPaym;
   end GetConfirmID_SWP;
   function GETREPSTATUSID (P_DRAFTKIND in integer,P_DRAFTID in integer) return INTEGER as
     l_IDPaym  INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GETREPSTATUSID');
   --Определение идентификатора отчета о статусе
     SELECT max(pp.id_paym) INTO l_IDPaym
       FROM pckpay pp, ipspck mp, payass a, mtrec m, payass a1
      WHERE pp.id_ipspck = mp.id_ipspck AND mp.id_paym = a.id_paym AND pp.id_paym = m.id_paym
        AND a.associate = to_char(p_DraftID) AND a.bankid = to_char(nvl(p_DraftKind, decode(m.type, 518, 3,
                                                                                                    599, 4)))
        AND a1.id_paym = pp.id_paym
        AND a1.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                        P_CODE  => 1);
     Return nvl(l_IDPaym, 0);
   end GetRepStatusID;
   function GETCODESTATUS (P_25D in varchar2) return INTEGER as
   /*Функция возвращает код текущего актуального статуса из отчета МТ548,
     сквитованного с исходным сообщением по поручению*/
   begin
     IF p_25D = 'IPRC//PACK' THEN
       Return 2;
     ELSIF p_25D = 'IPRC//REJT' THEN
       Return 3;
     ELSIF p_25D = 'IPRC//PPRC' THEN
       Return 4;
     ELSIF p_25D = 'MTCH//MACH' THEN
       Return 5;
     ELSIF p_25D = 'MTCH//NMAT' THEN
       Return 6;
     ELSIF p_25D = 'IPRC//CAND' THEN
       Return 7;
     ELSE
       Return NULL;
     END IF;
   end GetCodeStatus;
   function GETCONFIRMPARAMSPAYM (P_IDPAYM in integer,P_NUM out varchar2,P_DATE out varchar2,P_TYPEIN out varchar2,P_STATE out integer) return INTEGER as
   /*Функция возвращает параметры подтверждения, сквитованного с исходным сообщением по поручению*/
   begin
     p_Num    := chr(1);
     p_Date   := '01.01.0001 00:00:00';
   --Получение выходных параметров
     SELECT p.numreferdoc,
            nvl(to_char(p.datereferdoc, 'dd.mm.yyyy hh24:mi:ss'), '01.01.0001'),
            decode(p.id_sstate, RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                               P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отправки/приема\Сквитован')),
                   0, 1),
            m.type||mt.type
       INTO p_Num, p_Date, p_State, p_TypeIN
       FROM mtrec m, mtn9x mt, paym p
      WHERE p.id_paym = p_IDPaym
        AND p.id_paym = m.id_paym(+)
        AND p.id_paym = mt.id_paym(+);
     Return 0;
   end GetConfirmParamsPaym;
   function GETCONFIRMTEXTAL (P_PAYMID in integer,P_MSG out clob) return INTEGER as
   /*Функция возвращает сообщения*/
   begin
   --Получение текста сообщения
     SELECT m.str||mt.text
       INTO p_Msg
       FROM mtrec m,
            mtn9x mt,
            paym p
      WHERE p.id_paym = p_PaymID
        AND p.id_paym = m.id_paym(+)
        AND p.id_paym = mt.id_paym(+);
     IF p_Msg IS NULL THEN
       Return 1;
     ELSE
       Return 0;
     END IF;
   end GetConfirmTextAL;
   function HANDCONFIRM (P_DRAFTID in integer,P_PAYMID in integer,P_KINDMSG in varchar2,P_NETT in integer,P_NUM out varchar2,P_DATE out varchar2,P_TYPEIN out varchar2,P_STATE out integer) return integer as
     f1           INTEGER;
     l_Kvit       INTEGER;
     l_IDPaym     INTEGER;
     l_IDPack     INTEGER;
     l_VR         INTEGER;
     l_IDStatePck INTEGER;
     l_State      INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция HANDCONFIRM');
     IF p_Nett = 1 THEN
       f1 := RSP_SECURITIES.HANDCONFIRMNETT(P_PAYMID   => p_DraftID,
                                            P_PAYMIDIN => p_PaymID,
                                            P_NUM      => p_Num,
                                            P_DATE     => p_Date,
                                            P_TYPEIN   => p_TypeIn,
                                            P_STATE    => p_State);
       Return f1;
     ELSE
       BEGIN
         SELECT max(a.id_paym)
           INTO l_IDPaym
           FROM payass a
          WHERE a.associate = p_DraftID
            AND a.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                           P_CODE  => 3);
         SELECT st.code
           INTO l_State
           FROM paym p,
                sstate st
          WHERE p.id_paym = l_IDPaym
            AND st.id_sstate = p.id_sstate;
         IF RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Для отката') LIKE('%'||l_State||'%') THEN
           Return 2;
         END IF;
       --Поиск пакета
         SELECT p.id_ipspck INTO l_IDPack
           FROM pckpay p
          WHERE p.id_paym = l_IDPaym;
       --Блокировка пакета
         SELECT i.versionrec INTO l_VR
           FROM ipspck i
          WHERE i.id_ipspck = l_IDPack FOR UPDATE NOWAIT;
       --Получение нового состояния пакета
         SELECT s.id_sstpck INTO l_IDStatePck
           FROM sstpck s
          WHERE s.code = RSP_SETUP.GET_VALUE('Депозитарий\Состояния пакета\Получен ответ');
       --Помещение ответного платежа в пакет
         f1 := RSP_PAYM_API.PackAddPaym(l_IDPack, l_VR, p_PaymID, l_IDStatePck, 'Получен ответ ID='||p_PaymID);
       EXCEPTION
         WHEN NO_DATA_FOUND THEN
           Return 1;
       END;
     --Создание истории платежа
       DELETE FROM tmp_paym_pack;
       INSERT INTO tmp_paym_pack(id_paym, id_sstate, varlen, vr)
         VALUES(p_PaymID, RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                         P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отправки/приема\Обработан')),
                'ручное ассоциирование', (select p.versionrec
                                            from paym p
                                           where p.id_paym = p_PaymID));
       IF SQL%ROWCOUNT > 0 THEN
         f1 := RSP_GATE.MASSCHANGESTATE(P_TBLNAME => 'TMP_PAYM_PACK');
         IF f1 = 1 THEN
           RSP_PAYM_API.KvitSWIFT_CB(p_IDPaymIN  => p_PaymID,
                                     p_IDPaymOUT => l_IDPaym);
           l_Kvit := RSP_SECURITIES.GetConfirmParamsPaym(P_IDPAYM => p_PaymID,
                                                         P_NUM    => p_Num,
                                                         P_DATE   => p_Date,
                                                         P_TYPEIN => p_TypeIN,
                                                         p_State  => p_State);
           Return l_Kvit;
         ELSE
           Return 1;
         END IF;
       ELSE
         Return 1;
       END IF;
     END IF;
   end HandConfirm;
   function HANDCONFIRMNETT (P_PAYMID in integer,P_PAYMIDIN in integer,P_NUM out varchar2,P_DATE out varchar2,P_TYPEIN out varchar2,P_STATE out integer) return integer as
     f1           INTEGER;
     l_Kvit       INTEGER;
     l_IDPack     INTEGER;
     l_VR         INTEGER;
     l_IDStatePck INTEGER;
     l_State      INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция HANDCONFIRMNETT');
     BEGIN
       SELECT st.code
         INTO l_State
         FROM paym p,
              sstate st
        WHERE p.id_paym = p_PaymID
          AND st.id_sstate = p.id_sstate;
       IF RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Для отката') LIKE('%'||l_State||'%') THEN
         Return 2;
       END IF;
     --Поиск пакета
       SELECT p.id_ipspck INTO l_IDPack
         FROM pckpay p
        WHERE p.id_paym = p_PaymID;
     --Блокировка пакета
       SELECT i.versionrec INTO l_VR
         FROM ipspck i
        WHERE i.id_ipspck = l_IDPack FOR UPDATE NOWAIT;
     --Получение нового состояния пакета
       SELECT s.id_sstpck INTO l_IDStatePck
         FROM sstpck s
        WHERE s.code = RSP_SETUP.GET_VALUE('Депозитарий\Состояния пакета\Получен ответ');
     --Помещение ответного платежа в пакет
       f1 := RSP_PAYM_API.PackAddPaym(l_IDPack, l_VR, p_PaymIDIn, l_IDStatePck, 'Получен ответ ID='||p_PaymIDIn);
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         Return 1;
     END;
   --Создание истории платежа
     DELETE FROM tmp_paym_pack;
     INSERT INTO tmp_paym_pack(id_paym, id_sstate, varlen, vr)
       VALUES(p_PaymIDIn, RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                         P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отправки/приема\Обработан')),
              'ручное ассоциирование', (select p.versionrec
                                          from paym p
                                         where p.id_paym = p_PaymIDIn));
     IF SQL%ROWCOUNT > 0 THEN
       f1 := RSP_GATE.MASSCHANGESTATE(P_TBLNAME => 'TMP_PAYM_PACK');
       IF f1 = 1 THEN
         RSP_PAYM_API.KvitSWIFT_CB(p_IDPaymIN  => p_PaymIDIn,
                                   p_IDPaymOUT => p_PaymID);
         l_Kvit := RSP_SECURITIES.GetConfirmParamsPaym(P_IDPAYM => p_PaymIDIn,
                                                       P_NUM    => p_Num,
                                                       P_DATE   => p_Date,
                                                       P_TYPEIN => p_TypeIN,
                                                       p_State  => p_State);
         Return l_Kvit;
       ELSE
         Return 1;
       END IF;
     ELSE
       Return 1;
     END IF;
   end HandConfirmNett;
   function GETTEXT599 (P_DRAFTID in integer,P_ACTION in integer,P_DRAFTKIND in integer,P_TEXT in out clob) return INTEGER as
   /*Функция возвращает текст сообщения MT599,
     сформированных по заданному поручению и
     записыват изменения
     p_Action = 1 - получить текст
                2 - записать текст*/
     l_IDPaym INTEGER;
   begin
     RSP_COMMON.INS_LOG(1, 'Вызвана функция GETTEXT599');
     IF P_DRAFTKIND = 7 THEN
       l_IDPaym := RSP_SECURITIES.GET_CONSIDBYDRAFTID(P_DRAFTID => p_DraftID);
       IF l_IDPaym IS NULL THEN
         Return 1;
       END IF;
     ELSIF P_DRAFTKIND = 4 THEN
       BEGIN
         SELECT m.id_paym
           INTO l_IDPaym
           FROM mtn9x m,
                paym p
          WHERE p.id_paym = (select max(ps.id_paym) as id_paym
                               from payass ps
                              where ps.associate = to_char(p_DraftID) and ps.bankid = 4
                                and ps.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                                                P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Ассоциация'))
                              group by ps.associate, ps.bankid)
            AND m.id_paym = p.id_paym
            AND p.id_sstate = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                             P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отправки/приема\Отправляется'));-- FOR UPDATE NOWAIT;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN
           Return 1;
       END;
     END IF;
     IF p_Action = 1 THEN
       SELECT substr(decode(p_DraftKind, 4, m.text,
                                         7, replace(m.text, ':79:')), decode(p_DraftKind, 4, instr(m.text, ':79:') + 4,
                                                                                          7, instr(m.text, ':20:') + 27))
         INTO p_Text
         FROM mtn9x m
        WHERE m.id_paym = l_IDPaym;
       IF p_Text IS NULL THEN
         Return 1;
       END IF;
     ELSIF p_Action = 2 THEN
       IF P_DRAFTKIND = 7 THEN
         UPDATE mtn9x m
            SET m.text = substr(m.text, 1, decode(p_DraftKind, 4, instr(m.text, ':79:') + 3,
                                                               7, instr(m.text, ':20:') + 26))||
                                replace(p_Text, 'PLEASE CONFIRM YOUR AGREEMENT FOR THE DEAL PAIR-OFF',
                                        ':79:PLEASE CONFIRM YOUR AGREEMENT FOR THE DEAL PAIR-OFF')
          WHERE m.id_paym = RSP_SECURITIES.GET_CONSIDBYDRAFTID(P_DRAFTID => p_DraftID);
       ELSIF P_DRAFTKIND = 4 THEN
         UPDATE mtn9x m
            SET m.text = substr(m.text, 1, instr(m.text, ':79:') + 3)||p_Text
          WHERE m.id_paym = (select max(p.id_paym) as id_paym
                               from payass p
                              where p.associate = to_char(p_DraftID) and p.bankid = 4
                                and p.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                                               P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Ассоциация'))
                              group by p.associate, p.bankid)
            AND m.id_paym IN(select py.id_paym
                               from paym py
                              where py.id_sstate = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                                                  P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отправки/приема\Отправляется')));
       END IF;
       IF SQL%ROWCOUNT = 0 THEN
         Return 2;
       END IF;
     END IF;
     Return 0;
   end GetText599;
   function GET_DRAFTKIND (P_DRAFTID in integer,P_KINDMSG in varchar2) return INTEGER as
     l_DraftKind INTEGER;
     l_State     INTEGER;
   begin
     SELECT pa.bankid, p.id_sstate
       INTO l_DraftKind, l_State
       FROM payass pa, paym p, mtrec m, mtn9x mt
      WHERE pa.id_paym = p.id_paym
        AND p.id_paym = m.id_paym(+)
        AND p.id_paym = mt.id_paym(+)
        AND pa.associate = to_char(P_DRAFTID)
        AND m.type||mt.type = substr(P_KINDMSG, 3)
        AND pa.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                        P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Ассоциация'))
        AND p.id_paym = (select max(pas.id_paym)
                           from payass pas
                          where pas.associate = to_char(P_DRAFTID));
     IF l_State = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                 P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат')) THEN
       Return '-'||l_DraftKind;
     ELSE
       Return l_DraftKind;
     END IF;
   exception
     WHEN NO_DATA_FOUND THEN
       Return NULL;
   end Get_DraftKind;
   function GET_CONSIDBYDRAFTID (P_DRAFTID in integer) return INTEGER as
     l_ConsId INTEGER;
   begin
     SELECT max(ip.id_paym)
       INTO l_ConsId
       FROM ipspck ip,
            pckpay pp
      WHERE ip.id_ipspck = pp.id_ipspck
        AND pp.id_paym IN(select pa.id_paym
                            from payass pa,
                                 paym p
                           where pa.id_paym = p.id_paym
                             and p.id_sstate <> RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                                               P_CODE  => RSP_SETUP.GET_VALUE(P_PATH => 'Депозитарий\Состояния отката\Откат'))
                             and pa.associate = p_DraftID
                             and pa.bankid = 7
                             and pa.id_sass = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SASS',
                                                                             P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Ассоциация')));
     Return l_ConsId;
   exception
     WHEN NO_DATA_FOUND THEN
       Return NULL;
   end Get_ConsIdByDraftId;
   function GET_ASSIDBYCONSID (P_CONSID in integer) return T_REFINSID PIPELINED as
     l_c     SYS_REFCURSOR;
     l_InsId VARCHAR2(4000);
   begin
     OPEN l_c FOR SELECT distinct to_number(pa.associate)
                    FROM pckpay pp,
                         ipspck ip,
                         paym p,
                         payass pa
                   WHERE ip.id_paym = p_ConsId
                     AND pp.id_ipspck = ip.id_ipspck
                     AND pa.id_paym = p.id_paym
                     AND p.id_paym = pp.id_paym;
     LOOP
       FETCH l_c INTO l_InsId;
       EXIT WHEN l_c%NOTFOUND;
       PIPE ROW(l_InsId);
     END LOOP;
     CLOSE l_c;
     RETURN;
   end Get_AssIdByConsId;
   function GETACCEUROCLEAR (P_NUMREP out varchar2,P_STATEDATE out date,P_FINISH out varchar2,P_SAFEDEPO out varchar2,P_EXTACTI out char) return INTEGER as
   /*Получение данных выписки из EuroClear*/
     l_IDPaym INTEGER;
     l_SAFE   VARCHAR2(25);
     l_ISIN   VARCHAR2(25);
     l_LSIN   VARCHAR2(25);
     l_AGGR   VARCHAR2(25);
   begin
     BEGIN
       SELECT max(p.id_paym)
         INTO l_IDPaym
         FROM paym p,
              nroute r
        WHERE p.id_nform = RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'NFORM',
                                                          P_CODE  => RSP_SETUP.Get_Value(P_PATH => 'Депозитарий\Формы\Выписка'))
          AND p.valuedate = RSP_COMMON.GET_DATEOPERDAY
          AND r.id_nkorr = p.id_nkorrinput
          AND P.ID_SSTATE IN(RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                            P_CODE => 5016),
                             RSP_COMMON.GET_ID_BY_CODE_DICT(P_TABLE => 'SSTATE',
                                                            P_CODE => 5019))      -- состоянме 5016 Обработан (SWIFT) / 5019 - несквитован
          AND ( r.bic = 'MGTCBEBEE' OR r.bic = 'MGTCBEBEMECL') ;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         Return 1;
     END;
     WITH ttabl AS (select to_char(ltrim(substr(txt,
                                                instr(txt, chr(10), 1, level) + 1,
                                                instr(txt, chr(10), 1, level + 1) - instr(txt, chr(10), 1, level) - 1))) str
                      from (select chr(10)||str||chr(10) txt
                              from (select str
                                      from mtrec
                                     where id_paym = l_IDPaym))
                     connect by level <= length(txt) - length(replace(txt, chr(10))) - 1)
       SELECT p.numreferdoc, p.datereferdoc,
              (select substr(str, 13)
                 from ttabl
                where str like(':22F::CODE//%')),
              (select substr(str, 13)
                 from ttabl
                where str like(':97A::SAFE//%') and rownum = 1),
              (select substr(str, 13)
                 from ttabl
                where str like(':17B::ACTI//%') and rownum = 1)
         INTO p_NumRep, p_StateDate, p_Finish, p_SafeDepo, p_ExtActi
         FROM paym p
        WHERE p.id_paym = l_IDPaym;
     IF p_ExtActi = 'Y' THEN
       FOR c IN(SELECT to_char(ltrim(substr(txt,
                                            instr(txt, chr(10), 1, level) + 1,
                                            instr(txt, chr(10), 1, level + 1) - instr(txt, chr(10), 1, level) - 1))) str
                  FROM (select chr(10)||str||chr(10) txt
                          from (select str
                                  from mtrec
                                 where id_paym = l_IDPaym))
                        connect by level <= length(txt) - length(replace(txt, chr(10))) - 1) LOOP
         IF c.str LIKE ':97A::SAFE//%/KRZD/%' THEN
           l_Safe := substr(c.str, instr(c.str, 'KRZD/') + 5);
         END IF;
         IF c.str LIKE ':35B:ISIN%' THEN
           l_ISIN := trim(substr(c.str, 10));
         END IF;
         IF c.str LIKE '/RU/%' THEN
           l_LSIN := substr(c.str, 5);
         END IF;
         IF c.str LIKE ':93B::AGGR//UNIT/%' THEN
           l_AGGR := rtrim(substr(c.str, 18), ',');
         END IF;
         IF c.str LIKE ':93B::AGGR//FAMT/%' THEN
           l_AGGR := rtrim(substr(c.str, 18), ',');
         END IF;
         IF c.str LIKE ':16S:FIN%' THEN
           execute immediate 'INSERT INTO DACCREPORTEUROCLEAR_TMP(T_SAFEDEPOPART, T_ISIN, T_LSIN, T_QUANTITY)
                                VALUES(:Safe, :ISIN, :LSIN, :AGGR)' using l_Safe, l_ISIN, l_LSIN, l_AGGR;
         END IF;
       END LOOP;
     END IF;
     Return 0;
   end GetAccEuroClear;
   function CHECKLINKPAYM return INTEGER as
   begin
     Return 0;
   end CheckLinkPaym;
   function GETVALUEFIELD (P_MSG in clob,P_FIELD in varchar2) return VARCHAR2 as
     l_Val VARCHAR2(1024);
   begin
     FOR c IN(SELECT to_char(ltrim(substr(txt,
                                          instr(txt, chr(10), 1, level) + 1,
                                          instr(txt, chr(10), 1, level + 1) - instr(txt, chr(10), 1, level) - 1))) str
                FROM (select chr(10)||p_Msg||chr(10) txt
                        from dual)
                      connect by level <= length(txt) - length(replace(txt, chr(10))) - 1) LOOP
       IF c.str LIKE p_Field||'%' THEN
         IF p_Field IN (':33G:', ':32B:') THEN
           l_Val := SUBSTR(c.str, INSTR(c.str, p_Field) + 5, 3);
           EXIT;
         ELSIF length(p_Field) = 5 THEN
           l_Val := SUBSTR(c.str, INSTR(c.str, p_Field) + 5);
           EXIT;
         ELSE
           l_Val := SUBSTR(c.str, INSTR(c.str, p_Field) + 4);
           EXIT;
         END IF;
       END IF;
     END LOOP;
     Return l_Val;
   end GetValueField;
end RSP_SECURITIES;
/
