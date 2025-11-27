CREATE OR REPLACE package body RSP_CURRMARKET as
   function LoadRequisites RETURN INTEGER as
   /*Обработка отчета МБ.
   */
   begin
     INSERT INTO tmp_mb_requisites(doc_date, doc_time, 
                                   doc_no, doc_type_id, sender_id, sender_name, receiver_id, remarks)
       SELECT to_date(x.DOC_DATE, 'YYYY-MM-DD'), to_date('0001-01-01'||' '||x.DOC_TIME, 'YYYY-MM-DD HH24:MI:SS'),
              x.DOC_NO, x.DOC_TYPE_ID, x.SENDER_ID, x.SENDER_NAME, x.RECEIVER_ID, x.REMARKS
         FROM tmp_xml t,
              XMLTABLE('//DOC_REQUISITES' PASSING valxml
                       COLUMNS DOC_DATE    PATH '@DOC_DATE',
                               DOC_TIME    PATH '@DOC_TIME',
                               DOC_NO      PATH '@DOC_NO',
                               DOC_TYPE_ID PATH '@DOC_TYPE_ID',
                               SENDER_ID   PATH '@SENDER_ID',
                               SENDER_NAME PATH '@SENDER_NAME',
                               RECEIVER_ID PATH '@RECEIVER_ID',
                               REMARKS     PATH '@REMARKS') x;
     Return SQL%ROWCOUNT;
   end LoadRequisites;

   function LoadCUX23 return INTEGER as
   /*Обработка отчета CUX23.
   */
   begin
     INSERT INTO tmp_cm_cux23(reportdate, firmid, firmname, firmnameen, clearingfirmid, clearingfirmname, clearingfirmnameen, extsettlecode, 
                              exttradecode, exttradecodetype, addsession, sessionname, sessionnameen, currencyid, currencyname, currencynameen, cocurrencyid,
                              cocurrencyname, cocurrencynameen, securityid, secshortname, facevalue, 
                              settledate, tradegroup, mainsecurityid, mainsecshortname, tradeno, buysell, orderno, tradederiv,
                              tradetime, tradetype, decimals, price, 
                              quantity, value, cpfirmid, period, settlecode, userid, 
                              userexchangeid, brokerref, extref, exchcomm, itscomm, 
                              clrcomm, sumcomm, trdaccid, clientcode, details, subdetails, 
                              repotradeno, boardid, boardname, boardnameen, algoorderno, isactualmm)
       SELECT to_date(x.reportdate, 'YYYY-MM-DD'), x.firmid, x.firmname, x.firmnameen, y.clearingfirmid, y.clearingfirmname, y.clearingfirmnameen, z.extsettlecode, 
              v.exttradecode, v.exttradecodetype, w.addsession, w.sessionname, w.sessionnameen, s.currencyid, s.currencyname, s.currencynameen, s.cocurrencyid,
              s.cocurrencyname, s.cocurrencynameen, r.securityid, r.secshortname, to_number(r.facevalue, '99999999999999999999.999999'), 
              to_date(p.settledate, 'YYYY-MM-DD'), o.tradegroup, n.mainsecurityid, n.mainsecshortname, u.tradeno, u.buysell, u.orderno, u.tradederiv,
              to_date('0001-01-01'||' '||u.tradetime, 'YYYY-MM-DD HH24:MI:SS'), u.tradetype, u.decimals, to_number(u.price, '99999999999999999999.999999'), 
              to_number(u.quantity, '99999999999999999999.99'), to_number(u.value, '99999999999999999999.99'), u.cpfirmid, u.period, u.settlecode, u.userid, 
              u.userexchangeid, u.brokerref, u.extref, to_number(u.exchcomm, '99999999999999999999.99'), to_number(u.itscomm, '99999999999999999999.99'), 
              to_number(u.clrcomm, '99999999999999999999.99'), to_number(u.sumcomm, '99999999999999999999.99'), u.trdaccid, u.clientcode, u.details, u.subdetails, 
              u.repotradeno, u.boardid, u.boardname, u.boardnameen, u.AlgoOrderNo, u.IsActualMM
         FROM tmp_xml t,
              XMLTABLE('//CUX23' PASSING valxml
                       COLUMNS ReportDate        PATH '@ReportDate',
                               FirmID            PATH '@FirmID',
                               FirmName          PATH '@FirmName',
                               FirmNameEN        PATH '@FirmNameEN',
                               CLEARPART XMLTYPE PATH '/*') x,
              XMLTABLE('//CLEARPART' PASSING x.CLEARPART
                       COLUMNS ClearingFirmId     PATH '@ClearingFirmId',
                               ClearingFirmName   PATH '@ClearingFirmName',
                               ClearingFirmNameEN PATH '@ClearingFirmNameEN',
                               SETTLE     XMLTYPE PATH '/*') y,
              XMLTABLE('//SETTLE' PASSING y.SETTLE
                       COLUMNS ExtSettleCode    PATH '@ExtSettleCode',
                               TRADEACC XMLTYPE PATH '/*') z,
              XMLTABLE('//TRADEACC' PASSING z.TRADEACC
                       COLUMNS ExtTradeCode     PATH '@ExtTradeCode',
                               ExtTradeCodeType PATH '@ExtTradeCodeType',
                               SESION   XMLTYPE PATH '/*') v,
              XMLTABLE('//SESSION' PASSING v.SESION
                       COLUMNS AddSession       PATH '@AddSession',
                               SessionName      PATH '@SessionName',
                               SessionNameEN    PATH '@SessionNameEN',
                               CURRPAIR XMLTYPE PATH '/*') w,
              XMLTABLE('//CURRPAIR' PASSING w.CURRPAIR
                       COLUMNS CurrencyId       PATH '@CurrencyId',
                               CurrencyName     PATH '@CurrencyName',
                               CurrencyNameEN   PATH '@CurrencyNameEN',
                               CoCurrencyId     PATH '@CoCurrencyId',
                               CoCurrencyName   PATH '@CoCurrencyName',
                               CoCurrencyNameEN PATH '@CoCurrencyNameEN',
                               SECURITY XMLTYPE PATH '/*') s,
              XMLTABLE('//SECURITY' PASSING s.SECURITY
                       COLUMNS SecurityId         PATH '@SecurityId',
                               SecShortName       PATH '@SecShortName',
                               FaceValue          PATH '@FaceValue',
                               SETTLEDATE XMLTYPE PATH '/*') r,
              XMLTABLE('//SETTLEDATE' PASSING r.SETTLEDATE
                       COLUMNS SettleDate  PATH '@SettleDate',
                               GRP XMLTYPE PATH '/*') p,
              XMLTABLE('//GROUP' PASSING p.GRP
                       COLUMNS TradeGroup      PATH '@TradeGroup',
                               MAINSEC XMLTYPE PATH '/*') o,
              XMLTABLE('//MAINSEC' PASSING o.MAINSEC
                       COLUMNS MainSecurityId   PATH '@MainSecurityId',
                               MainSecShortName PATH '@MainSecShortName',
                               RECORDS  XMLTYPE PATH '/*') n,
              XMLTABLE('//RECORDS' PASSING n.RECORDS
                       COLUMNS TradeNo           PATH '@TradeNo',
                               BuySell           PATH '@BuySell',
                               OrderNo           PATH '@OrderNo',
                               TradeDeriv        PATH '@TradeDeriv',
                               TradeTime         PATH '@TradeTime',
                               TradeType         PATH '@TradeType',
                               Decimals          PATH '@Decimals',
                               Price             PATH '@Price',
                               Quantity          PATH '@Quantity',
                               Value             PATH '@Value',
                               CPFirmId          PATH '@CPFirmId',
                               Period            PATH '@Period',
                               SettleCode        PATH '@SettleCode',
                               UserId            PATH '@UserId',
                               UserExchangeId    PATH '@UserExchangeId',
                               BrokerRef         PATH '@BrokerRef',
                               ExtRef            PATH '@ExtRef',
                               ExchComm          PATH '@ExchComm',
                               ITSComm           PATH '@ITSComm',
                               ClrComm           PATH '@ClrComm',
                               SumComm           PATH '@SumComm',
                               TrdAccId          PATH '@TrdAccId',
                               ClientCode        PATH '@ClientCode',
                               Details           PATH '@Details',
                               SubDetails        PATH '@SubDetails',
                               RepoTradeNo       PATH '@RepoTradeNo',
                               BoardId           PATH '@BoardId',
                               BoardName         PATH '@BoardName',
                               BoardNameEN       PATH '@BoardNameEN',
                               AlgoOrderNo       PATH '@AlgoOrderNo',
                               IsActualMM        PATH '@IsActualMM') u;
     Return SQL%ROWCOUNT;
   end LoadCUX23;

   function LoadCCX17 return INTEGER as
   /*Обработка отчета CCX17.
   */
   begin
     INSERT INTO tmp_cm_ccx17(reportdate, reporttype, firmid, firmname, firmnameen, extsettlecode, extsettlecodeunifiedpool, currencyid, 
                              currencyname, currencynameen, exttradecode, settledate, settleprice, 
                              tradedate, tradetime, tradeno, 
                              price, tradegroup, buysell, quantity, 
                              value, varm, trdaccid,
                              termquantity, termvalue)
       SELECT to_date(x.reportdate, 'YYYY-MM-DD'), x.reporttype, x.firmid, x.firmname, x.firmnameen, y.extsettlecode, y.extsettlecodeunifiedpool, z.currencyid, 
              z.currencyname, z.currencynameen, v.exttradecode, to_date(w.settledate, 'YYYY-MM-DD'), to_number(w.settleprice, '99999999999999999999.999999'), 
              to_date(u.tradedate, 'YYYY-MM-DD'), to_date('0001-01-01'||' '||u.tradetime, 'YYYY-MM-DD HH24:MI:SS'), u.tradeno, 
              to_number(u.price, '99999999999999999999.999999'), u.tradegroup, u.buysell, to_number(u.quantity, '99999999999999999999.99'), 
              to_number(u.value, '99999999999999999999.99'), to_number(u.varm, '99999999999999999999.99'), u.trdaccid,
              to_number(u.termquantity, '99999999999999999999.99'), to_number(u.termvalue, '99999999999999999999.99')
         FROM tmp_xml t,
              XMLTABLE('//CCX17' PASSING valxml
                       COLUMNS ReportDate     PATH '@ReportDate',
                               ReportType     PATH '@ReportType',
                               FirmID         PATH '@FirmID',
                               FirmName       PATH '@FirmName',
                               FirmNameEN     PATH '@FirmNameEN',
                               SETTLE XMLTYPE PATH '/*') x,
              XMLTABLE('//SETTLE' PASSING x.SETTLE
                       COLUMNS ExtSettleCode            PATH '@ExtSettleCode',
                               ExtSettleCodeUnifiedPool PATH '@ExtSettleCodeUnifiedPool',
                               CURRENCY         XMLTYPE PATH '/*') y,
              XMLTABLE('//CURRENCY' PASSING y.CURRENCY
                       COLUMNS CurrencyId     PATH '@CurrencyId',
                               CurrencyName   PATH '@CurrencyName',
                               CurrencyNameEN PATH '@CurrencyNameEN',
                               TRADE  XMLTYPE PATH '/*') z,
              XMLTABLE('//TRADE' PASSING z.TRADE
                       COLUMNS ExtTradeCode       PATH '@ExtTradeCode',
                               SETTLEDATE XMLTYPE PATH '/*') v,
              XMLTABLE('//SETTLEDATE' PASSING v.SETTLEDATE
                       COLUMNS SettleDate      PATH '@SettleDate',
                               SettlePrice     PATH '@SettlePrice',
                               RECORDS XMLTYPE PATH '/*') w,
              XMLTABLE('//RECORDS' PASSING w.RECORDS
                       COLUMNS TradeDate    PATH '@TradeDate',
                               TradeTime    PATH '@TradeTime',
                               TradeNo      PATH '@TradeNo',
                               Price        PATH '@Price',
                               TradeGroup   PATH '@TradeGroup',
                               BuySell      PATH '@BuySell',
                               Quantity     PATH '@Quantity',
                               Value        PATH '@Value',
                               Varm         PATH '@Varm',
                               TrdAccId     PATH '@TrdAccId',
                               TermQuantity PATH '@TermQuantity',
                               TermValue    PATH '@TermValue') u;
     Return SQL%ROWCOUNT;
   end LoadCCX17;

   function LoadCCX10 return INTEGER as
   /*Обработка отчета CCX10.
   */
   begin
     INSERT INTO tmp_cm_ccx10(reportdate, reporttype, clearingfirmid, clearingfirmname, clearingfirmnameen, extsettlecode,
                              extsettlecodeunifiedpool, comtype, commistype, commisname, commisnameen, datefrom,
                              dateto, comm, itsvat,
                              settledate, extsettlecode2, comm2)
       SELECT to_date(x.reportdate, 'YYYY-MM-DD'), x.reporttype, x.clearingfirmid, x.clearingfirmname, x.clearingfirmnameen, y.extsettlecode, 
              y.extsettlecodeunifiedpool, z.ComType, u.commistype, u.commisname, u.commisnameen, to_date(u.datefrom, 'YYYY-MM-DD'), 
              to_date(u.dateto, 'YYYY-MM-DD'), to_number(u.comm, '99999999999999999999.99'), to_number(u.itsvat, '99999999999999999999.99'), 
              to_date(u.settledate, 'YYYY-MM-DD'), v.extsettlecode2, to_number(v.comm2, '99999999999999999999.99')
         FROM tmp_xml t,
              XMLTABLE('//CCX10' PASSING valxml
                       COLUMNS ReportDate         PATH '@ReportDate',
                               ReportType         PATH '@ReportType',
                               ClearingFirmId     PATH '@ClearingFirmId',
                               ClearingFirmName   PATH '@ClearingFirmName',
                               ClearingFirmNameEN PATH '@ClearingFirmNameEN',
                               SETTLE1    XMLTYPE PATH '/*') x,
              XMLTABLE('//SETTLE1' PASSING x.SETTLE1
                       COLUMNS ExtSettleCode            PATH '@ExtSettleCode',
                               ExtSettleCodeUnifiedPool PATH '@ExtSettleCodeUnifiedPool',
                               TYPE             XMLTYPE PATH '/*') y,
              XMLTABLE('//TYPE' PASSING y.TYPE
                       COLUMNS ComType         PATH '@ComType',
                               RECORDS XMLTYPE PATH '/*') z,
              XMLTABLE('//RECORDS' PASSING z.RECORDS
                       COLUMNS CommisType      PATH '@CommisType',
                               CommisName      PATH '@CommisName',
                               CommisNameEN    PATH '@CommisNameEN',
                               DateFrom        PATH '@DateFrom',
                               DateTo          PATH '@DateTo',
                               Comm            PATH '@Comm',
                               ITSVAT          PATH '@ITSVAT',
                               SettleDate      PATH '@SettleDate',
                               SETTLE2 XMLTYPE PATH '/*') u,
              XMLTABLE('//SETTLE2' PASSING u.SETTLE2
                       COLUMNS ExtSettleCode2 PATH '@ExtSettleCode',
                               Comm2          PATH '@Comm')(+) v;
     Return SQL%ROWCOUNT;
   end LoadCCX10;

   function LoadCCX4 return INTEGER as
   /*Обработка отчетов CCX04, CCX4P.
   */
   begin
     INSERT INTO tmp_cm_ccx04(reportdate, clearingfirmid, clearingfirmname, clearingfirmnameen, extsettlecode,
                              extsettlecodeunifiedpool, currencyid, nccrealaccount, nettosum, datatype,
                              debit, credit)
       SELECT to_date(x.reportdate, 'YYYY-MM-DD'), x.clearingfirmid, x.clearingfirmname, x.clearingfirmnameen, y.extsettlecode, 
              y.extsettlecodeunifiedpool, z.CurrencyId, z.NccRealAccount, to_number(z.NettoSum, '99999999999999999999.99'), u.DataType, 
              to_number(u.Debit, '99999999999999999999.99'), to_number(u.Credit, '99999999999999999999.99')
         FROM tmp_xml t,
              XMLTABLE('//CCX04' PASSING valxml
                       COLUMNS ReportDate         PATH '@ReportDate',
                               ClearingFirmID     PATH '@ClearingFirmId',
                               ClearingFirmName   PATH '@ClearingFirmName',
                               ClearingFirmNameEN PATH '@ClearingFirmNameEN',
                               SETTLE     XMLTYPE PATH '/*') x,
              XMLTABLE('//SETTLE' PASSING x.SETTLE
                       COLUMNS ExtSettleCode            PATH '@ExtSettleCode',
                               ExtSettleCodeUnifiedPool PATH '@ExtSettleCodeUnifiedPool',
                               CURRENCY         XMLTYPE PATH '/*') y,
              XMLTABLE('//CURRENCY' PASSING y.CURRENCY
                       COLUMNS CurrencyId      PATH '@CurrencyId',
                               NccRealAccount  PATH '@NccRealAccount',
                               NettoSum        PATH '@NettoSum',
                               RECORDS XMLTYPE PATH '/*') z,
              XMLTABLE('//RECORDS' PASSING z.RECORDS
                       COLUMNS DataType PATH '@DataType',
                               Debit    PATH '@Debit',
                               Credit   PATH '@Credit') u;
     Return SQL%ROWCOUNT;
   end LoadCCX4;

   function LoadCCX99 return INTEGER as
   /*Обработка отчетов CCX99.
   */
   begin
     INSERT INTO tmp_cm_ccx99(firmpurpose_payment, trade_date, regcode, date_from, date_to,
                              report_type, id, account, currency, opening_balance,
                              closing_balance, previous_date,
                              date_opening_balance, date_closing_balance,
                              debit_sum, credit_sum,
                              purpose_payment_acc, type, codetype, trantype, transkind, number_docum, reference, pay_number, acc_doc_date,
                              pay_acc, pay_inn, pay_kpp, pay_name, pay_bic, pay_bank, pay_coracc, rec_acc, rec_inn, rec_kpp, rec_name, rec_bic,
                              rec_bank, cor_acc, purpose_payment, debit,
                              credit, date_transaction,
                              time_transaction, bic_order, party_id_order, nameaddr_order, bic_inter,
                              party_id_inter, nameaddr_inter, bic_account, party_id_account, nameaddr_account, bic_benef, party_id_benef, nameaddr_benef)
       SELECT x.firmpurpose_payment, to_date(x.trade_date, 'YYYY-MM-DD'), x.regcode, to_date(x.date_from, 'YYYY-MM-DD'), to_date(x.date_to, 'YYYY-MM-DD'), 
              x.report_type, y.id, z.account, z.currency, to_number(z.opening_balance, '99999999999999999999999999999999.9999'), 
              to_number(z.closing_balance, '99999999999999999999999999999999.9999'), to_date(z.previous_date, 'YYYY-MM-DD'), 
              to_date(z.date_opening_balance, 'YYYY-MM-DD'), to_date(z.date_closing_balance, 'YYYY-MM-DD'),
              to_number(z.debit_sum, '99999999999999999999999999999999.9999'), to_number(z.credit_sum, '99999999999999999999999999999999.9999'),
              z.purpose_payment_acc, z.type, u.codetype, u.trantype, u.transkind, u.number_docum, u.reference, u.pay_number, to_date(u.acc_doc_date, 'YYYY-MM-DD'), 
              u.pay_acc, u.pay_inn, u.pay_kpp, u.pay_name, u.pay_bic, u.pay_bank, u.pay_coracc, u.rec_acc, u.rec_inn, u.rec_kpp, u.rec_name, u.rec_bic, 
              u.rec_bank, u.cor_acc, u.purpose_payment, to_number(u.debit, '99999999999999999999999999999999.9999'),
              to_number(u.credit, '99999999999999999999999999999999.9999'), to_date(u.date_transaction, 'YYYY-MM-DD'), 
              to_date('0001-01-01'||' '||u.time_transaction, 'YYYY-MM-DD HH24:MI:SS'), u.bic_order, u.party_id_order, u.nameaddr_order, u.bic_inter,
              u.party_id_inter, u.nameaddr_inter, u.bic_account, u.party_id_account, u.nameaddr_account, u.bic_benef, u.party_id_benef, u.nameaddr_benef
         FROM tmp_xml t,
              XMLTABLE('//CCX99' PASSING valxml
                       COLUMNS FIRMPURPOSE_PAYMENT   PATH '@FIRMPURPOSE_PAYMENT',
                               TRADE_DATE            PATH '@TRADE_DATE',
                               REGCODE               PATH '@REGCODE',
                               DATE_FROM             PATH '@DATE_FROM',
                               DATE_TO               PATH '@DATE_TO',
                               REPORT_TYPE           PATH '@REPORT_TYPE',
                               EXTSETTLECODE XMLTYPE PATH '/*') x,
              XMLTABLE('//EXTSETTLECODE' PASSING x.EXTSETTLECODE
                       COLUMNS ID                PATH '@ID',
                               STATEMENT XMLTYPE PATH '/*') y,
              XMLTABLE('//STATEMENT' PASSING y.STATEMENT
                       COLUMNS ACCOUNT              PATH '@ACCOUNT',
                               CURRENCY             PATH '@CURRENCY',
                               OPENING_BALANCE      PATH '@OPENING_BALANCE',
                               CLOSING_BALANCE      PATH '@CLOSING_BALANCE',
                               PREVIOUS_DATE        PATH '@PREVIOUS_DATE',
                               DATE_OPENING_BALANCE PATH '@DATE_OPENING_BALANCE',
                               DATE_CLOSING_BALANCE PATH '@DATE_CLOSING_BALANCE',
                               DEBIT_SUM            PATH '@DEBIT_SUM',
                               CREDIT_SUM           PATH '@CREDIT_SUM',
                               PURPOSE_PAYMENT_ACC  PATH '@PURPOSE_PAYMENT_ACC',
                               TYPE                 PATH '@TYPE',
                               ENTRY        XMLTYPE PATH '/*') z,
              XMLTABLE('//ENTRY' PASSING z.ENTRY
                       COLUMNS CODETYPE         PATH '@CODETYPE',
                               TRANTYPE         PATH '@TRANTYPE',
                               TRANSKIND        PATH '@TRANSKIND',
                               NUMBER_DOCUM     PATH '@NUMBER',
                               REFERENCE        PATH '@REFERENCE',
                               PAY_NUMBER       PATH '@PAY_NUMBER',
                               ACC_DOC_DATE     PATH '@ACC_DOC_DATE',
                               PAY_ACC          PATH '@PAY_ACC',
                               PAY_INN          PATH '@PAY_INN',
                               PAY_KPP          PATH '@PAY_KPP',
                               PAY_NAME         PATH '@PAY_NAME',
                               PAY_BIC          PATH '@PAY_BIC',
                               PAY_BANK         PATH '@PAY_BANK',
                               PAY_CORACC       PATH '@PAY_CORACC',
                               REC_ACC          PATH '@REC_ACC',
                               REC_INN          PATH '@REC_INN',
                               REC_KPP          PATH '@REC_KPP',
                               REC_NAME         PATH '@REC_NAME',
                               REC_BIC          PATH '@REC_BIC',
                               REC_BANK         PATH '@REC_BANK',
                               COR_ACC          PATH '@COR_ACC',
                               PURPOSE_PAYMENT  PATH '@PURPOSE_PAYMENT',
                               DEBIT            PATH '@DEBIT',
                               CREDIT           PATH '@CREDIT',
                               DATE_TRANSACTION PATH '@DATE',
                               TIME_TRANSACTION PATH '@TIME',
                               BIC_ORDER        PATH 'OrderingParty/@BIC',
                               PARTY_ID_ORDER   PATH 'OrderingParty/@PARTY_ID',
                               NAMEADDR_ORDER   PATH 'OrderingParty/@NAMEADDR',
                               BIC_INTER        PATH 'Intermediary/@BIC',
                               PARTY_ID_INTER   PATH 'Intermediary/@PARTY_ID',
                               NAMEADDR_INTER   PATH 'Intermediary/@NAMEADDR',
                               BIC_ACCOUNT      PATH 'ACCOUNTWITHINSTITUTION/@BIC',
                               PARTY_ID_ACCOUNT PATH 'ACCOUNTWITHINSTITUTION/@PARTY_ID',
                               NAMEADDR_ACCOUNT PATH 'ACCOUNTWITHINSTITUTION/@NAMEADDR',
                               BIC_BENEF        PATH 'BENEFICIARY/@BIC',
                               PARTY_ID_BENEF   PATH 'BENEFICIARY/@PARTY_ID',
                               NAMEADDR_BENEF   PATH 'BENEFICIARY/@NAMEADDR') u;
     Return SQL%ROWCOUNT;
   end LoadCCX99;

   function LoadCUX22 return INTEGER as
   /*Обработка отчета CUX22.
   */
   begin
     INSERT INTO tmp_cm_cux22(reportdate, firmid, firmname, firmnameen, clearingfirmid, clearingfirmname, clearingfirmnameen, 
                              extsettlecode, exttradecode, exttradecodetype, addsession, sessionname, sessionnameen, currencyid, currencyname, currencynameen, 
                              cocurrencyid, cocurrencyname, cocurrencynameen, securityid, secshortname, facevalue, 
                              settledate, FixingDate, tradegroup, orderno, 
                              AlgoOrderNo, IsActualMM, userid, asp, EntryTime, 
                              buysell, OrderType, BasePRICE, quantity, 
                              QuantityHidden, decimals, price, Status, 
                              AmendTime, Balance, cpfirmid, trdaccid, clientcode, 
                              details, subdetails, boardid, boardname, boardnameen)
       SELECT to_date(x.reportdate, 'YYYY-MM-DD'), x.firmid, x.firmname, x.firmnameen, y.clearingfirmid, y.clearingfirmname, y.clearingfirmnameen, 
              z.extsettlecode, v.exttradecode, v.exttradecodetype, w.addsession, w.sessionname, w.sessionnameen, s.currencyid, s.currencyname, s.currencynameen, 
              s.cocurrencyid, s.cocurrencyname, s.cocurrencynameen, r.securityid, r.secshortname, to_number(r.facevalue, '99999999999999999999.999999'),
              to_date(p.settledate, 'YYYY-MM-DD'), to_date(p.FixingDate, 'YYYY-MM-DD'), o.tradegroup, to_number(u.orderno, '99999999999999999999'), 
              to_number(u.AlgoOrderNo, '99999999999999999999'), u.IsActualMM, u.userid, u.asp, to_date('0001-01-01 '||u.EntryTime, 'YYYY-MM-DD HH24:MI:SS'), 
              u.buysell, u.OrderType, to_number(u.BasePRICE, '99999999999999999999.999999'), to_number(u.quantity, '99999999999999999999.99'), 
              to_number(u.quantityHidden, '99999999999999999999'), u.decimals, to_number(u.price, '99999999999999999999.999999'), u.status, 
              to_date('0001-01-01 '||u.amendtime, 'YYYY-MM-DD HH24:MI:SS'), to_number(u.balance,'99999999999999999999.99'), u.cpfirmid, u.trdaccid, u.clientcode, 
              u.details, u.subdetails, u.boardid, u.boardname, u.boardnameen
         FROM tmp_xml t,
            XMLTABLE('//CUX22' PASSING valxml
                      COLUMNS ReportDate        PATH '@ReportDate',
                              FirmID            PATH '@FirmID',
                              FirmName          PATH '@FirmName',
                              FirmNameEN        PATH '@FirmNameEN',
                              CLEARPART XMLTYPE PATH '/*') x,
            XMLTABLE('//CLEARPART' PASSING x.CLEARPART
                      COLUMNS ClearingFirmId     PATH '@ClearingFirmId',
                              ClearingFirmName   PATH '@ClearingFirmName',
                              ClearingFirmNameEN PATH '@ClearingFirmNameEN',
                              SETTLE     XMLTYPE PATH '/*') y,
            XMLTABLE('//SETTLE' PASSING y.SETTLE
                      COLUMNS ExtSettleCode    PATH '@ExtSettleCode',
                              TRADEACC XMLTYPE PATH '/*') z,
            XMLTABLE('//TRADEACC' PASSING z.TRADEACC
                      COLUMNS ExtTradeCode     PATH '@ExtTradeCode',
                              ExtTradeCodeType PATH '@ExtTradeCodeType',
                              SESION   XMLTYPE PATH '/*') v,
            XMLTABLE('//SESSION' PASSING v.SESION
                      COLUMNS AddSession       PATH '@AddSession',
                              SessionName      PATH '@SessionName',
                              SessionNameEN    PATH '@SessionNameEN',
                              CURRPAIR XMLTYPE PATH '/*') w,
            XMLTABLE('//CURRPAIR' PASSING w.CURRPAIR
                      COLUMNS CurrencyId       PATH '@CurrencyId',
                              CurrencyName     PATH '@CurrencyName',
                              CurrencyNameEN   PATH '@CurrencyNameEN',
                              CoCurrencyId     PATH '@CoCurrencyId',
                              CoCurrencyName   PATH '@CoCurrencyName',
                              CoCurrencyNameEN PATH '@CoCurrencyNameEN',
                              SECURITY XMLTYPE PATH '/*') s,
            XMLTABLE('//SECURITY' PASSING s.SECURITY
                      COLUMNS SecurityId         PATH '@SecurityId',
                              SecShortName       PATH '@SecShortName',
                              FaceValue          PATH '@FaceValue',
                              SETTLEDATE XMLTYPE PATH '/*') r,
            XMLTABLE('//SETTLEDATE' PASSING r.SETTLEDATE
                      COLUMNS SettleDate  PATH '@SettleDate',
                              FixingDate  PATH '@FixingDate',
                              GRP XMLTYPE PATH '/*') p,
            XMLTABLE('//GROUP' PASSING p.GRP
                      COLUMNS TradeGroup      PATH '@TradeGroup',
                              RECORDS XMLTYPE PATH '/*') o,
            XMLTABLE('//RECORDS' PASSING o.RECORDS
                      COLUMNS OrderNo           PATH '@OrderNo',
                              AlgoOrderNo       PATH '@AlgoOrderNo',
                              IsActualMM        PATH '@IsActualMM', 
                              UserId            PATH '@UserId',
                              ASP               PATH '@ASP',
                              EntryTime         PATH '@EntryTime',
                              BuySell           PATH '@BuySell',
                              OrderType         PATH '@OrderType',
                              BasePRICE         PATH '@BasePRICE',
                              Quantity          PATH '@Quantity',
                              QuantityHidden    PATH '@QuantityHidden',
                              Decimals          PATH '@Decimals',
                              Price             PATH '@Price',
                              Status            PATH '@Status',
                              AmendTime         PATH '@AmendTime',
                              Balance           PATH '@Balance',
                              CPFirmId          PATH '@CPFirmId',
                              TrdAccId          PATH '@TrdAccId',
                              ClientCode        PATH '@ClientCode',
                              Details           PATH '@Details',
                              SubDetails        PATH '@SubDetails',
                              BoardId           PATH '@BoardId',
                              BoardName         PATH '@BoardName',
                              BoardNameEN       PATH '@BoardNameEN') u;
     Return SQL%ROWCOUNT;
   end LoadCUX22;

   function Load_File (p_FILENAME in varchar2, p_FileType in varchar2, p_Msg out varchar2) return INTEGER as
   /*Основная функция загрузки полученных отчетов МБ.
     p_FileName - имя принятого файла.
     Возвращает информацию о результате загрузки и обработки.
   */
     l_Rows INTEGER;
   begin
     p_Msg := 'Прием файла Валютного рынка МБ '||p_FILENAME||': ';
     l_Rows := RSP_CURRMARKET.LOADREQUISITES; 
     IF l_Rows = 0 THEN
       p_Msg := p_Msg||'ошибка разбора формата Валютного рынка '||p_FileType||'.';
       Return 0;
     END IF;
     IF p_FileType = 'CUX23' THEN
       l_Rows := RSP_CURRMARKET.LoadCUX23;
     END IF;
     IF p_FileType = 'CCX17' THEN
       l_Rows := RSP_CURRMARKET.LoadCCX17;
     END IF;
     IF p_FileType = 'CCX10' THEN
       l_Rows := RSP_CURRMARKET.LoadCCX10;
     END IF;
     IF p_FileType = 'CUX22' THEN
       l_Rows := RSP_CURRMARKET.LoadCUX22;
     END IF;
     IF p_FileType IN('CCX04', 'CCX4P') THEN
       l_Rows := RSP_CURRMARKET.LoadCCX4;
     END IF;
     IF p_FileType = 'CCX99' THEN
       l_Rows := RSP_CURRMARKET.LoadCCX99;
     END IF;
     IF l_Rows = 0 THEN
       p_Msg := p_Msg||'в отчете '||p_FileType||' отсутствуют сделки.';
     END IF;
     Return l_Rows;
   end Load_File;

   function Insert_Requisites (p_FILENAME in varchar2, p_IDProcLog in integer, p_IDNFORM in integer, p_Date in date default null) RETURN INTEGER as
   /*Обработка отчета МБ.
     p_FileName - имя файла отчета.
   */
     l_IDReq INTEGER;
   begin
     l_IDReq := Sequence_Mb_Requisites.Nextval();
     INSERT INTO mb_requisites(id_mb_requisites, id_processing_log, id_nform, doc_date, doc_time, doc_no, doc_type_id, sender_id, sender_name, 
                 receiver_id, remarks, file_name)
       SELECT l_IDReq, p_IDProcLog, p_IDNFORM, nvl(x.DOC_DATE, p_Date), x.DOC_TIME, x.DOC_NO, x.DOC_TYPE_ID, x.SENDER_ID, x.SENDER_NAME, 
              x.RECEIVER_ID, x.REMARKS, p_FILENAME
         FROM tmp_mb_requisites x;
     Return l_IDReq;
   end Insert_Requisites;

   function Insert_CUX23 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета CUX23.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO cm_cux23_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO cm_cux23(id_mb_requisites, id_processing_log, reportdate, firmid, firmname, firmnameen, clearingfirmid, clearingfirmname, clearingfirmnameen, 
                          extsettlecode, exttradecode, exttradecodetype, addsession, sessionname, sessionnameen, currencyid, currencyname, currencynameen, cocurrencyid,
                          cocurrencyname, cocurrencynameen, securityid, secshortname, facevalue, settledate, tradegroup, mainsecurityid, mainsecshortname, tradeno,
                          buysell, orderno, tradederiv, tradetime, tradetype, decimals, price, quantity, value, cpfirmid, period, settlecode, userid, userexchangeid,
                          brokerref, extref, exchcomm, itscomm, clrcomm, sumcomm, trdaccid, clientcode, details, subdetails, repotradeno, boardid, boardname, boardnameen,
                          algoorderno, isactualmm)
       SELECT p_IDReg, p_IDProcLog, reportdate, firmid, firmname, firmnameen, clearingfirmid, clearingfirmname, clearingfirmnameen, 
              extsettlecode, exttradecode, exttradecodetype, addsession, sessionname, sessionnameen, currencyid, currencyname, currencynameen, cocurrencyid,
              cocurrencyname, cocurrencynameen, securityid, secshortname, facevalue, settledate, tradegroup, mainsecurityid, mainsecshortname, tradeno,
              buysell, orderno, tradederiv, tradetime, tradetype, decimals, price, quantity, value, cpfirmid, period, settlecode, userid, userexchangeid,
              brokerref, extref, exchcomm, itscomm, clrcomm, sumcomm, trdaccid, clientcode, details, subdetails, repotradeno, boardid, boardname, boardnameen,
              algoorderno, isactualmm
         FROM tmp_cm_cux23;
     Return SQL%ROWCOUNT;
   end Insert_CUX23;

   function Insert_CCX17 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета CCX17.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO cm_ccx17_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO cm_ccx17(id_mb_requisites, id_processing_log, reportdate, reporttype, firmid, firmname, firmnameen, extsettlecode, extsettlecodeunifiedpool,
                          currencyid, currencyname, currencynameen, exttradecode, settledate, settleprice, tradedate, tradetime, tradeno, price, tradegroup,
                          buysell, quantity, value, varm, trdaccid, termquantity, termvalue)
       SELECT p_IDReg, p_IDProcLog, t.reportdate, t.reporttype, t.firmid, t.firmname, t.firmnameen, t.extsettlecode, t.extsettlecodeunifiedpool,
              t.currencyid, t.currencyname, t.currencynameen, t.exttradecode, t.settledate, t.settleprice, t.tradedate, t.tradetime, t.tradeno, t.price, t.tradegroup,
              t.buysell, t.quantity, t.value, t.varm, t.trdaccid, t.termquantity, t.termvalue
         FROM tmp_cm_ccx17 t;
     Return SQL%ROWCOUNT;
   end Insert_CCX17;

   function Insert_CCX10 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета CCX10.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO cm_ccx10_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO cm_ccx10(id_mb_requisites, id_processing_log, reportdate, reporttype, clearingfirmid, clearingfirmname, clearingfirmnameen, extsettlecode,
                          extsettlecodeunifiedpool, comtype, commistype, commisname, commisnameen, datefrom, dateto, comm, itsvat,
                          settledate, extsettlecode2, comm2)
       SELECT p_IDReg, p_IDProcLog, t.reportdate, t.reporttype, t.clearingfirmid, t.clearingfirmname, t.clearingfirmnameen, t.extsettlecode,
              t.extsettlecodeunifiedpool, t.comtype, t.commistype, t.commisname, t.commisnameen, t.datefrom, t.dateto, t.comm, t.itsvat,
              t.settledate, t.extsettlecode2, t.comm2
         FROM tmp_cm_ccx10 t;
     Return SQL%ROWCOUNT;
   end Insert_CCX10;

   function Insert_CCX04 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета CCX04.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO cm_ccx04_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO cm_ccx04(id_mb_requisites, id_processing_log, reportdate, clearingfirmid, clearingfirmname, clearingfirmnameen, extsettlecode,
                         extsettlecodeunifiedpool, currencyid, nccrealaccount, nettosum, datatype, debit, credit)
       SELECT p_IDReg, p_IDProcLog, t.reportdate, t.clearingfirmid, t.clearingfirmname, t.clearingfirmnameen, t.extsettlecode,
              t.extsettlecodeunifiedpool, t.currencyid, t.nccrealaccount, t.nettosum, t.datatype, t.debit, t.credit
         FROM tmp_cm_ccx04 t;
     Return SQL%ROWCOUNT;
   end Insert_CCX04;

   function Insert_CCX4P (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета CCX4P.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO cm_ccx4p_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO cm_ccx4p(id_mb_requisites, id_processing_log, reportdate, clearingfirmid, clearingfirmname, clearingfirmnameen, extsettlecode,
                          extsettlecodeunifiedpool, currencyid, nccrealaccount, nettosum, datatype, debit, credit)
       SELECT p_IDReg, p_IDProcLog, t.reportdate, t.clearingfirmid, t.clearingfirmname, t.clearingfirmnameen, t.extsettlecode,
              t.extsettlecodeunifiedpool, t.currencyid, t.nccrealaccount, t.nettosum, t.datatype, t.debit, t.credit
         FROM tmp_cm_ccx04 t;
     Return SQL%ROWCOUNT;
   end Insert_CCX4P;

   function Insert_CCX99 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета CCX99.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO cm_ccx99_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO cm_ccx99(id_mb_requisites, id_processing_log, firmpurpose_payment, trade_date, regcode, date_from, date_to, report_type, id, account,
                          currency, opening_balance, closing_balance, previous_date, date_opening_balance, date_closing_balance, debit_sum,
                          credit_sum, purpose_payment_acc, type, codetype, trantype, transkind, number_docum, reference, pay_number, acc_doc_date,
                          pay_acc, pay_inn, pay_kpp, pay_name, pay_bic, pay_bank, pay_coracc, rec_acc, rec_inn, rec_kpp, rec_name, rec_bic,
                          rec_bank, cor_acc, purpose_payment, debit, credit, date_transaction, time_transaction, bic_order, party_id_order,
                          nameaddr_order, bic_inter, party_id_inter, nameaddr_inter, bic_account, party_id_account, nameaddr_account, bic_benef,
                          party_id_benef, nameaddr_benef)
       SELECT p_IDReg, p_IDProcLog, t.firmpurpose_payment, t.trade_date, t.regcode, t.date_from, t.date_to, t.report_type, t.id, t.account,
              t.currency, t.opening_balance, t.closing_balance, t.previous_date, t.date_opening_balance, t.date_closing_balance,  t.debit_sum,
              t.credit_sum, t.purpose_payment_acc, t.type, t.codetype, t.trantype, t.transkind, t.number_docum, t.reference, t.pay_number, t.acc_doc_date,
              t.pay_acc, t.pay_inn, t.pay_kpp, t.pay_name, t.pay_bic, t.pay_bank, t.pay_coracc, t.rec_acc, t.rec_inn, t.rec_kpp, t.rec_name, t.rec_bic,
              t.rec_bank, t.cor_acc, t.purpose_payment, t.debit, t.credit, t.date_transaction, t.time_transaction, t.bic_order, t.party_id_order,
              t.nameaddr_order, t.bic_inter, t.party_id_inter, t.nameaddr_inter, t.bic_account, t.party_id_account, t.nameaddr_account, t.bic_benef,
              t.party_id_benef, t.nameaddr_benef
         FROM tmp_cm_ccx99 t;
     Return SQL%ROWCOUNT;
   end Insert_CCX99;

   function Insert_CUX22 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета CUX22.
     p_IDReg - ссылка на отчет    */
   begin
     INSERT INTO cm_cux22_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO cm_cux22(id_mb_requisites, id_processing_log, reportdate, firmid, firmname, firmnameen, clearingfirmid, clearingfirmname, clearingfirmnameen, 
                          extsettlecode, exttradecode, exttradecodetype, addsession, sessionname, sessionnameen, currencyid, currencyname, currencynameen, 
                          cocurrencyid, cocurrencyname, cocurrencynameen, securityid, secshortname, facevalue, settledate, FixingDate, tradegroup, orderno, 
                          AlgoOrderNo, IsActualMM, userid, asp, EntryTime, buysell, OrderType, BasePRICE, quantity, QuantityHidden, decimals, price, Status, 
                          AmendTime, Balance, cpfirmid, trdaccid, clientcode, details, subdetails, boardid, boardname, boardnameen)
        SELECT p_IDReg, p_IDProcLog, reportdate, firmid, firmname, firmnameen, clearingfirmid, clearingfirmname, clearingfirmnameen, 
               extsettlecode, exttradecode, exttradecodetype, addsession, sessionname, sessionnameen, currencyid, currencyname, currencynameen, 
               cocurrencyid, cocurrencyname, cocurrencynameen, securityid, secshortname, facevalue, settledate, FixingDate, tradegroup, orderno, 
               AlgoOrderNo, IsActualMM, userid, asp, EntryTime, buysell, OrderType, BasePRICE, quantity, QuantityHidden, decimals, price, Status, 
               AmendTime, Balance, cpfirmid, trdaccid, clientcode, details, subdetails, boardid, boardname, boardnameen
          FROM tmp_cm_cux22;
     Return SQL%ROWCOUNT;
   end Insert_CUX22;

  function Insert_Table (p_FILENAME in varchar2, p_FileType in varchar2, p_IDProcLog in integer) return INTEGER as
  /*Основная функция загрузки полученных отчетов МБ.
    p_FileName - имя принятого файла,
    p_FileType - тип файла.
    Возвращает информацию о результате загрузки и обработки.
  */
    l_IDReq   INTEGER;
    l_IdNForm INTEGER;
  begin
    IF p_FileType = 'CUX23' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Валютный рынок\Формы\CUX23'));
    ELSIF p_FileType = 'CCX17' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Валютный рынок\Формы\CCX17'));
    ELSIF p_FileType = 'CCX10' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Валютный рынок\Формы\CCX10'));
    ELSIF p_FileType = 'CUX22' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Валютный рынок\Формы\CUX22'));
    ELSIF p_FileType = 'CCX04' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Валютный рынок\Формы\CCX04'));
    ELSIF p_FileType = 'CCX4P' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Валютный рынок\Формы\CCX4P'));
    ELSIF p_FileType = 'CCX99' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Валютный рынок\Формы\CCX99'));
    END IF;
    l_IDReq := RSP_CURRMARKET.Insert_Requisites(p_FILENAME, p_IDProcLog, l_IdNForm); 
    IF p_FileType = 'CUX23' THEN
      Return RSP_CURRMARKET.Insert_CUX23(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'CCX17' THEN
      Return RSP_CURRMARKET.Insert_CCX17(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'CCX10' THEN
      Return RSP_CURRMARKET.Insert_CCX10(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'CUX22' THEN
      Return RSP_CURRMARKET.Insert_CUX22(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'CCX04' THEN
      Return RSP_CURRMARKET.Insert_CCX04(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'CCX4P' THEN
      Return RSP_CURRMARKET.Insert_CCX4P(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'CCX99' THEN
      Return RSP_CURRMARKET.Insert_CCX99(l_IDReq, p_IDProcLog);
    END IF;
  end Insert_Table;

  function Load_CurrMarket (p_FILENAME in varchar2, p_Content in clob) return INTEGER as
    l_IDSession    INTEGER;  
    l_IDProcAct    INTEGER;
    l_IDProcLog    INTEGER;
    l_RowsReated   INTEGER;
    l_RowsCreated  INTEGER;
    l_Duration     INTEGER := 0;
    l_Systimestamp TIMESTAMP(3) := systimestamp;
    l_Msg          VARCHAR2(255);
    l_FileParams   RSP_MB.T_FILEPARAMS;
    e_Error        EXCEPTION;
  begin
    l_IDSession := RSP_MB.LogSession_Insert(p_USERNICK => SYS_CONTEXT ('RSP_ADM_CTX','USERID'),
                                            p_FileName => p_FileName);
    RSP_MB.LogData_Insert(l_IDSession, 1, null, 'BEGIN. Начало обработки файла', l_Duration);
    
    --1. Сохранение файла во временную таблицу
    INSERT INTO tmp_xml(VALXML) 
      VALUES(XMLTYPE(p_Content));
    
    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_MB.LogData_Insert(l_IDSession, 2, 1, 'Успешно загружено содержимое файла', l_Duration);
    
    --2. Проверяем надо ли обрабатывать файл (нет ли уже успешно загруженного того же типа с тем же хешем)
    l_FileParams := RSP_MB.Get_ParamFile;
    
    l_FileParams.OrdNum := to_number(regexp_replace(regexp_substr(p_FILENAME, '(.*?)(\_|$)', 1, 3, NULL, 1),'[^[:digit:]]' ,''));
    l_FileParams.OrdNumStr := regexp_substr(p_FILENAME, '(.*?)(\_|$)', 1, 3, NULL, 1);
    l_IDProcAct := RSP_MB.Get_ActualProcessing(l_FileParams);

    IF l_IDProcAct IS NOT NULL AND RSP_MB.Check_UniqueFile(p_IDProcAct => l_IDProcAct,
                                                           p_FileHash  => l_FileParams.FileHash,
                                                           p_FileDate  => to_date(l_FileParams.DOC_DATE||' '||to_char(l_FileParams.DOC_TIME, 'HH24:MI:SS'),
                                                                                  'DD.MM.YYYY HH24:MI:SS')) THEN
      --инициализация ошибки
      RAISE e_Error;
      Return 1;
    END IF;

    l_IDProcLog := RSP_MB.Processing_Log_Insert(l_FileParams, p_FILENAME);
    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_MB.LogData_Insert(l_IDSession, 3, 1, 'Успешно создана запись процессинга', l_Duration);
    l_Systimestamp := systimestamp;

    --3. Загружаем данные из XML во временные таблицы
    l_RowsReated := RSP_CURRMARKET.LOAD_File(p_FILENAME => p_FILENAME,
                                             p_FileType => l_FileParams.doc_type_id,
                                             P_MSG      => l_Msg);

    IF l_RowsReated = 0 THEN
      --логируем ошибку
      l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
      RSP_MB.LogData_Insert(l_IDSession, 3, l_RowsReated, l_Msg, l_Duration);
      RSP_MB.Processing_Log_Update(p_IDProcLog   => l_IDProcLog,
                                   p_RowsReaded  => NULL,
                                   p_RowsCreated => NULL,
                                   p_RRCreated   => NULL,
                                   p_ResNum      => NULL,
                                   p_ResText     => l_Msg);
      l_RowsCreated := RSP_CURRMARKET.Insert_Table(p_FILENAME  => p_FILENAME,
                                                   p_FileType  => l_FileParams.doc_type_id,
                                                   p_IDProcLog => l_IDProcLog);
      Return -1;
    END IF;

    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_MB.LogData_Insert(l_IDSession, 4, l_RowsReated, 'Успешно загружены данные из файла во временные таблицы', l_Duration);
    l_Systimestamp := systimestamp;

    --4. Переливаем данные в постоянные таблицы
    l_RowsCreated := RSP_CURRMARKET.Insert_Table(p_FILENAME  => p_FILENAME,
                                                 p_FileType  => l_FileParams.doc_type_id,
                                                 p_IDProcLog => l_IDProcLog);
    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_MB.LogData_Insert(l_IDSession, 5, l_RowsCreated, 'Успешно загружены данные в постоянные таблицы', l_Duration);
    l_Systimestamp := systimestamp;

    --5. Актуализируем данные в системных таблицах
    RSP_MB.Processing_Log_Update(p_IDProcLog   => l_IDProcLog,
                                 p_RowsReaded  => l_RowsReated,
                                 p_RowsCreated => l_RowsCreated,
                                 p_RRCreated   => NULL,
                                 p_ResNum      => 0,
                                 p_ResText     => l_Msg);

    UPDATE processing_actual
       SET id_processing_log = l_IDProcLog,
           ID_MB_REQUISITES = RSP_MB.GetRequisitesID(l_IDProcLog)
     WHERE trade_date = l_FileParams.TradeDate
       AND file_type  = l_FileParams.doc_type_id
       AND ord_num_str = l_FileParams.OrdNumStr;
     
    IF SQL%ROWCOUNT = 0 THEN         -- Если для этого ключа это первая запись
      INSERT INTO processing_actual(trade_date, file_type, ord_num, id_processing_log, id_mb_requisites, ord_num_str)
        VALUES(l_FileParams.TradeDate, l_FileParams.doc_type_id, l_FileParams.OrdNum, l_IDProcLog, RSP_MB.GetRequisitesID(l_IDProcLog), l_FileParams.OrdNumStr);
    END IF;

    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_MB.LogData_Insert(l_IDSession, 6, NULL, 'END. Успешно обновили системные таблицы', l_Duration);

    Return 0;
  exception
    WHEN e_Error THEN
      l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
      RSP_MB.LogData_Insert(l_IDSession, 3, NULL, 'Внимание! Данный файл уже был обработан. Повторная обработка не требуется', l_Duration);
      RSP_MB.LogSession_Update(p_IDSession => l_IDSession,
                               p_ErrCode   => '-20000',
                               p_ErrText   => 'Внимание! Данный файл уже был обработан. Повторная обработка не требуется');
      Return 1;
    WHEN OTHERS THEN
      l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
      RSP_MB.LogData_Insert(l_IDSession, -1, NULL, 'END. Обработка файла закончилась с ошибкой', l_Duration);
      RSP_MB.LogSession_Update(p_IDSession => l_IDSession,
                               p_ErrCode   => SQLCODE,
                               p_ErrText   => SQLERRM);
    RAISE;
  end Load_CurrMarket;
end RSP_CURRMARKET;
/