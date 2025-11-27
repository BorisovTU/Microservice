CREATE OR REPLACE package body RSP_SPB as /*Тело пакета RSP_SPB*/
   function LoadRequisites (p_FILENAME IN VARCHAR2, p_FileType IN VARCHAR2, p_TEXT out varchar2) RETURN INTEGER as
   /*Обработка отчета СПБ.
     p_FILENAME - имя файла
     p_FileType - тип файла
     p_Text - информация о результате обработки.
   */
   begin
     IF p_FILENAME LIKE '%.txt' THEN
       INSERT INTO tmp_mb_requisites(doc_date,  
                                     doc_no, doc_type_id, 
                                     sender_id, sender_name, receiver_id)
         VALUES(to_date(substr(p_FILENAME, 21, 10), 'YYYY-MM-DD'),
                1, p_FileType, 
                const_SENDER_ID, const_SENDER_NAME, const_RECEIVER_ID);
     ELSE
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
     END IF;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка разбора формата СПБ';
       Return 0;
     END IF;
     Return 1;
   end LoadRequisites;

   function LoadSPB03 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета SPB03.
     p_Text - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_spb_spb03(reportdate, volume, volumetotal, reportnumber, reportlang, reportcode, clracccode, subclracccode, currencyid, currencyname,
                               boardid, boardname, settledate, securityid, secshortname, isin, regnumber,
                               facevalue, seccurrencyid, securitytype, pricetype, recno, tradeno, tradenoextra,
                               tradedate, tradetime, tradeperiod, specialperiod,
                               primaryorderid, orderid, ordertype, userid, commentar, ismm, buysell, settlecode, tradetype, tradeinstrumenttype, trademodeid,
                               trademodename, decimals, price, quantity,
                               value, amount, balance,
                               exchcomm, clrcomm, clientcode, clientdetails, ccpcode,
                               cpfirmid, cpfirmshortname, otccodeinitiator, otccodeconfirmator,
                               accint, price2, 
                               reporate, repopart, repoperiod, type, internaltrademodeid)
       SELECT to_date(x.ReportDate, 'YYYY-MM-DD'), x.Volume, x.VolumeTotal, x.ReportNumber, x.ReportLang, x.ReportCode, y.ClrAccCode, z.SubClrAccCode, v.CurrencyId, v.CurrencyName, 
              w.BoardId, w.BoardName, to_date(t.SettleDate, 'YYYY-MM-DD'), s.SecurityId, s.SecShortName, s.ISIN, s.RegNumber, 
              to_number(s.FaceValue, '99999999999999999999.99999999'), s.SecCurrencyId, s.SecurityType, s.PriceType, u.RecNo, u.TradeNo, u.TradeNoExtra, 
              to_date(u.TradeDate, 'YYYY-MM-DD'), to_date('0001-01-01'||' '||u.TradeTime, 'YYYY-MM-DD HH24:MI:SS'), u.TradePeriod, u.SpecialPeriod, 
              u.PrimaryOrderID, u.OrderID, u.OrderType, u.UserId, u.Commentar, u.IsMM, u.BuySell, u.SettleCode, u.TradeType, u.TradeInstrumentType, u.TradeModeId, 
              u.TradeModeName, u.Decimals, 
              to_number(u.Price, '99999999999999999999.99999999'), to_number(u.Quantity, '99999999999999999999.99999999'), 
              to_number(u.Val, '99999999999999999999.99999999'), to_number(u.Amount, '99999999999999999999.99999999'), to_number(u.Balance, '99999999999999999999.99999999'), 
              to_number(u.ExchComm, '99999999999999999999.99999999'), to_number(u.ClrComm, '99999999999999999999.99999999'), u.ClientCode, u.ClientDetails, u.CcpCode, 
              u.CPFirmId, u.CPFirmShortName, u.OtcCodeInitiator, u.OtcCodeConfirmator,               
              to_number(u.AccInt, '99999999999999999999.99999999'), to_number(u.Price2, '99999999999999999999.99999999'), 
              to_number(u.RepoRate, '999999999999999999999999999999.99999999'), u.RepoPart, u.RepoPeriod, u.Type, u.InternalTradeModeID
         FROM tmp_xml t,
              XMLTABLE('//REPORT' PASSING valxml
                       COLUMNS DOC_INFO XMLTYPE PATH '/*',
                               CLRACC   XMLTYPE PATH '/*') n,
              XMLTABLE('//DOC_INFO' PASSING n.DOC_INFO
                       COLUMNS ReportDate   PATH '@ReportDate',
                               Volume       PATH '@Volume',
                               VolumeTotal  PATH '@VolumeTotal',
                               ReportNumber PATH '@ReportNumber',
                               ReportLang   PATH '@ReportLang',
                               ReportCode   PATH '@ReportCode') x,
              XMLTABLE('//CLRACC' PASSING n.CLRACC
                       COLUMNS ClrAccCode        PATH '@ClrAccCode',
                               SUBCLRACC XMLTYPE PATH '/*') y,
              XMLTABLE('//SUBCLRACC' PASSING y.SUBCLRACC
                       COLUMNS SubClrAccCode    PATH '@SubClrAccCode',
                               CURRENCY XMLTYPE PATH '/*') z,
              XMLTABLE('//CURRENCY' PASSING z.CURRENCY
                       COLUMNS CurrencyId    PATH '@CurrencyId',
                               CurrencyName  PATH '@CurrencyName',
                               BOARD XMLTYPE PATH '/*') v,
              XMLTABLE('//BOARD' PASSING v.BOARD
                       COLUMNS BoardId    PATH '@BoardId',
                               BoardName  PATH '@BoardName',
                               SETTLEDATE XMLTYPE PATH '/*') w,
              XMLTABLE('//SETTLEDATE' PASSING w.SETTLEDATE
                       COLUMNS SettleDate       PATH '@SettleDate',
                               SECURITY XMLTYPE PATH '/*') t,
              XMLTABLE('//SECURITY' PASSING t.SECURITY
                       COLUMNS SecurityId      PATH '@SecurityId',
                               SecShortName    PATH '@SecShortName',
                               ISIN            PATH '@ISIN',
                               RegNumber       PATH '@RegNumber',
                               FaceValue       PATH '@FaceValue',
                               SecCurrencyId   PATH '@SecCurrencyId',
                               SecurityType    PATH '@SecurityType',
                               PriceType       PATH '@PriceType',
                               RECORDS XMLTYPE PATH '/*') s,
              XMLTABLE('//RECORDS' PASSING s.RECORDS
                       COLUMNS RecNo               PATH '@RecNo',
                               TradeNo             PATH '@TradeNo',
                               TradeNoExtra        PATH '@TradeNoExtra',
                               TradeDate           PATH '@TradeDate',
                               TradeTime           PATH '@TradeTime',
                               TradePeriod         PATH '@TradePeriod',
                               SpecialPeriod       PATH '@SpecialPeriod',
                               PrimaryOrderID      PATH '@PrimaryOrderID',
                               OrderID             PATH '@OrderID',
                               OrderType           PATH '@OrderType',
                               UserId              PATH '@UserId',
                               Commentar           PATH '@Comment',
                               IsMM                PATH '@IsMM',
                               BuySell             PATH '@BuySell',
                               SettleCode          PATH '@SettleCode',
                               TradeType           PATH '@TradeType',
                               TradeInstrumentType PATH '@TradeInstrumentType',
                               TradeModeId         PATH '@TradeModeId',
                               TradeModeName       PATH '@TradeModeName',
                               Decimals            PATH '@Decimals',
                               Price               PATH '@Price',
                               Quantity            PATH '@Quantity',
                               Val                 PATH '@Value',
                               Amount              PATH '@Amount',
                               Balance             PATH '@Balance',
                               ExchComm            PATH '@ExchComm',
                               ClrComm             PATH '@ClrComm',
                               ClientCode          PATH '@ClientCode',
                               ClientDetails       PATH '@ClientDetails',
                               CcpCode             PATH '@CcpCode',
                               CPFirmId            PATH '@CPFirmId',
                               CPFirmShortName     PATH '@CPFirmShortName',
                               OtcCodeInitiator    PATH '@OtcCodeInitiator',
                               OtcCodeConfirmator  PATH '@OtcCodeConfirmator',
                               AccInt              PATH '@AccInt',
                               Price2              PATH '@Price2',
                               RepoRate            PATH '@RepoRate',
                               RepoPart            PATH '@RepoPart',
                               RepoPeriod          PATH '@RepoPeriod',
                               Type                PATH '@Type',
                               InternalTradeModeID PATH '@InternalTradeModeID') u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета СПБ.';
       Return 0;
     ELSE 
       Return SQL%ROWCOUNT;
     END IF;
   end LoadSPB03;

   function LoadSPB21 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета SPB21.
     p_Text - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_spb_spb21(tradedate, volume, volumetotal, reportnumber, reportlang, reportcode,
                               boardid, boardname, securityid, secshortname, isin, regnumber,
                               facevalue, seccurrencyid, securitytype, issuername, issuerdetails, quotelist, decim,
                               currencyid, currencyname, accruedinterest, tradeperiod, setttype_market, trademode_market,
                               periodtotalamount, periodtotalvolume,
                               periodtotalcount, periodopenprice,
                               periodopenvolume, periodlastprice,
                               periodlastvolume, periodcurrentprice,
                               periodmaxdealprice, periodmindealprice,
                               periodwaprice, setttype_address, trademode_address, addressperiodtotalamount,
                               addressperiodtotalvolume, addressperiodtotalcount,
                               addressperiodopenprice, addressperiodopenvolume,
                               addressperiodlastprice, addressperiodlastvolume,
                               addressperiodcurrentprice, addressperiodmaxdealprice,
                               addressperiodmindealprice, addressperiodwaprice,
                               totalamount, totalvolume,
                               totaldealcount, maxdealprice,
                               mindealprice, closeprice,
                               prevclose, trendclose,
                               waprice, currentprice,
                               admittedquote, admittedquotevolume,
                               marketprice2, mp2volume,
                               marketprice3, mp3volume,
                               clearingprice)
       SELECT to_date(x.TradeDate, 'YYYY-MM-DD'), x.Volume, x.VolumeTotal, x.ReportNumber, x.ReportLang, x.ReportCode,
              y.BoardId, y.BoardName, s.SecurityId,  s.SecShortName, s.ISIN, s.RegNumber, 
              to_number(s.FaceValue, '99999999999999999999.99999999'), s.SecCurrencyId, s.SecurityType, s.IssuerName, s.IssuerDetails, s.QuoteList, s.Decim,
              s.CurrencyId, s.CurrencyName, to_number(s.AccruedInterest, '99999999999999999999.99999999'), z.TradePeriod, v.SettType, v.TradeMode,
              to_number(v.PeriodTotalAmount, '99999999999999999999.99999999'), to_number(v.PeriodTotalVolume, '99999999999999999999.99999999'), 
              to_number(v.PeriodTotalCount, '99999999999999999999.99999999'), to_number(v.PeriodOpenPrice, '99999999999999999999.99999999'),
              to_number(v.PeriodOpenVolume, '99999999999999999999.99999999'), to_number(v.PeriodLastPrice, '99999999999999999999.99999999'),
              to_number(v.PeriodLastVolume, '99999999999999999999.99999999'), to_number(v.PeriodCurrentPrice, '99999999999999999999.99999999'),
              to_number(v.PeriodMaxDealPrice, '99999999999999999999.99999999'), to_number(v.PeriodMinDealPrice, '99999999999999999999.99999999'),
              to_number(v.PeriodWAPrice, '99999999999999999999.99999999'), z.SettType, z.TradeMode, to_number(z.AddressPeriodTotalAmount, '99999999999999999999.99999999'), 
              to_number(z.AddressPeriodTotalVolume, '99999999999999999999.99999999'), to_number(z.AddressPeriodTotalCount, '99999999999999999999.99999999'), 
              to_number(z.AddressPeriodOpenPrice, '99999999999999999999.99999999'), to_number(z.AddressPeriodOpenVolume, '99999999999999999999.99999999'), 
              to_number(z.AddressPeriodLastPrice, '99999999999999999999.99999999'), to_number(z.AddressPeriodLastVolume, '99999999999999999999.99999999'), 
              to_number(z.AddressPeriodCurrentPrice, '99999999999999999999.99999999'), to_number(z.AddressPeriodMaxDealPrice, '99999999999999999999.99999999'), 
              to_number(z.AddressPeriodMinDealPrice, '99999999999999999999.99999999'), to_number(z.AddressPeriodWAPrice, '99999999999999999999.99999999'),
              to_number(u.TotalAmount, '99999999999999999999.99999999'), to_number(u.TotalVolume, '99999999999999999999.99999999'),
              to_number(u.TotalDealCount, '99999999999999999999.99999999'), to_number(u.MaxDealPrice, '99999999999999999999.99999999'),
              to_number(u.MinDealPrice, '99999999999999999999.99999999'), to_number(u.ClosePrice, '99999999999999999999.99999999'),
              to_number(u.PrevClose, '99999999999999999999.99999999'), to_number(u.TrendClose, '99999999999999999999.99999999'),
              to_number(u.WAPrice, '99999999999999999999.99999999'), to_number(u.CurrentPrice, '99999999999999999999.99999999'),
              to_number(u.AdmittedQuote, '99999999999999999999.99999999'), to_number(u.AdmittedQuoteVolume, '99999999999999999999.99999999'),
              to_number(u.MarketPrice2, '99999999999999999999.99999999'), to_number(u.MP2Volume, '99999999999999999999.99999999'),
              to_number(u.MarketPrice3, '99999999999999999999.99999999'), to_number(u.MP3Volume, '99999999999999999999.99999999'),
              to_number(u.ClearingPrice, '99999999999999999999.99999999')
         FROM tmp_xml t,
              XMLTABLE('//REPORT' PASSING valxml
                       COLUMNS DOC_INFO XMLTYPE PATH '/*',
                               BOARD    XMLTYPE PATH '/*') n,
              XMLTABLE('//DOC_INFO' PASSING n.DOC_INFO
                       COLUMNS TradeDate    PATH '@TradeDate',
                               Volume       PATH '@Volume',
                               VolumeTotal  PATH '@VolumeTotal',
                               ReportNumber PATH '@ReportNumber',
                               ReportLang   PATH '@ReportLang',
                               ReportCode   PATH '@ReportCode') x,
              XMLTABLE('//BOARD' PASSING n.BOARD
                       COLUMNS BoardId          PATH '@BoardId',
                               BoardName        PATH '@BoardName',
                               SECURITY XMLTYPE PATH '/*') y,
              XMLTABLE('//SECURITY' PASSING y.SECURITY
                       COLUMNS SecurityId           PATH '@SecurityId',
                               SecShortName         PATH '@SecShortName',
                               ISIN                 PATH '@ISIN',
                               RegNumber            PATH '@RegNumber',
                               FaceValue            PATH '@FaceValue',
                               SecCurrencyId        PATH '@SecCurrencyId',
                               SecurityType         PATH '@SecurityType',
                               IssuerName           PATH '@IssuerName',
                               IssuerDetails        PATH '@IssuerDetails',
                               QuoteList            PATH '@QuoteList',
                               Decim                PATH '@Decimals',
                               CurrencyId           PATH '@CurrencyId',
                               CurrencyName         PATH '@CurrencyName',
                               AccruedInterest      PATH '@AccruedInterest',
                               TRADE_PERIOD XMLTYPE PATH '/*',
                               RESULT       XMLTYPE PATH '/*') s,
              XMLTABLE('//TRADE_PERIOD' PASSING s.TRADE_PERIOD
                       COLUMNS TradePeriod           PATH '@TradePeriod',
                               SettType                  PATH 'ADDRESS_TRADE[1]/@SettType',
                               TradeMode                 PATH 'ADDRESS_TRADE[1]/@TradeMode',
                               AddressPeriodTotalAmount  PATH 'ADDRESS_TRADE[1]/@AddressPeriodTotalAmount ',
                               AddressPeriodTotalVolume  PATH 'ADDRESS_TRADE[1]/@AddressPeriodTotalVolume ',
                               AddressPeriodTotalCount   PATH 'ADDRESS_TRADE[1]/@AddressPeriodTotalCount ',
                               AddressPeriodOpenPrice    PATH 'ADDRESS_TRADE[1]/@AddressPeriodOpenPrice',
                               AddressPeriodOpenVolume   PATH 'ADDRESS_TRADE[1]/@AddressPeriodOpenVolume',
                               AddressPeriodLastPrice    PATH 'ADDRESS_TRADE[1]/@AddressPeriodLastPrice',
                               AddressPeriodLastVolume   PATH 'ADDRESS_TRADE[1]/@AddressPeriodLastVolume',
                               AddressPeriodCurrentPrice PATH 'ADDRESS_TRADE[1]/@AddressPeriodCurrentPrice',
                               AddressPeriodMaxDealPrice PATH 'ADDRESS_TRADE[1]/@AddressPeriodMaxDealPrice',
                               AddressPeriodMinDealPrice PATH 'ADDRESS_TRADE[1]/@AddressPeriodMinDealPrice',
                               AddressPeriodWAPrice      PATH 'ADDRESS_TRADE[1]/@AddressPeriodWAPrice',
                               MARKET_TRADE  XMLTYPE PATH '/*'/*,
                               ADDRESS_TRADE XMLTYPE PATH '/*'*/) z,
              XMLTABLE('//MARKET_TRADE' PASSING z.MARKET_TRADE
                       COLUMNS SettType           PATH '@SettType',
                               TradeMode          PATH '@TradeMode',
                               PeriodTotalAmount  PATH '@PeriodTotalAmount',
                               PeriodTotalVolume  PATH '@PeriodTotalVolume',
                               PeriodTotalCount   PATH '@PeriodTotalCount',
                               PeriodOpenPrice    PATH '@PeriodOpenPrice',
                               PeriodOpenVolume   PATH '@PeriodOpenVolume',
                               PeriodLastPrice    PATH '@PeriodLastPrice',
                               PeriodLastVolume   PATH '@PeriodLastVolume',
                               PeriodCurrentPrice PATH '@PeriodCurrentPrice',
                               PeriodMaxDealPrice PATH '@PeriodMaxDealPrice',
                               PeriodMinDealPrice PATH '@PeriodMinDealPrice',
                               PeriodWAPrice      PATH '@PeriodWAPrice') v,
               /*XMLTABLE('//ADDRESS_TRADE' PASSING z.ADDRESS_TRADE 
                       COLUMNS SettType                  PATH '@SettType',
                               TradeMode                 PATH '@TradeMode',
                               AddressPeriodTotalAmount  PATH '@AddressPeriodTotalAmount ',
                               AddressPeriodTotalVolume  PATH '@AddressPeriodTotalVolume ',
                               AddressPeriodTotalCount   PATH '@AddressPeriodTotalCount ',
                               AddressPeriodOpenPrice    PATH '@AddressPeriodOpenPrice',
                               AddressPeriodOpenVolume   PATH '@AddressPeriodOpenVolume',
                               AddressPeriodLastPrice    PATH '@AddressPeriodLastPrice',
                               AddressPeriodLastVolume   PATH '@AddressPeriodLastVolume',
                               AddressPeriodCurrentPrice PATH '@AddressPeriodCurrentPrice',
                               AddressPeriodMaxDealPrice PATH '@AddressPeriodMaxDealPrice',
                               AddressPeriodMinDealPrice PATH '@AddressPeriodMinDealPrice',
                               AddressPeriodWAPrice      PATH '@AddressPeriodWAPrice') w,*/
              XMLTABLE('//RESULT' PASSING s.RESULT
                       COLUMNS TotalAmount         PATH '@TotalAmount',
                               TotalVolume         PATH '@TotalVolume',
                               TotalDealCount      PATH '@TotalDealCount',
                               MaxDealPrice        PATH '@MaxDealPrice',
                               MinDealPrice        PATH '@MinDealPrice',
                               ClosePrice          PATH '@ClosePrice',
                               PrevClose           PATH '@PrevClose',
                               TrendClose          PATH '@TrendClose',
                               WAPrice             PATH '@WAPrice',
                               CurrentPrice        PATH '@CurrentPrice',
                               AdmittedQuote       PATH '@AdmittedQuote',
                               AdmittedQuoteVolume PATH '@AdmittedQuoteVolume',
                               MarketPrice2        PATH '@MarketPrice2',
                               MP2Volume           PATH '@MP2Volume',
                               MarketPrice3        PATH '@MarketPrice3',
                               MP3Volume           PATH '@MP3Volume',
                               ClearingPrice       PATH '@ClearingPrice') u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета СПБ.';
       Return 0;
     ELSE 
       Return SQL%ROWCOUNT;
     END IF;
   end LoadSPB21;

   function LoadMFB06C (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета MFB06C.
     p_Text - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_spb_mfb06c(reportdate, volume, volumetotal, reportnumber, reportlang, reportcode, firmid, clientcode, clientdetails, 
                                currencyid, currencyname, inftype, clearingtype, 
                                clearingtime, 
                                settledate, boardid, boardname, secshortname, securityid, isin, basesecuritycode, 
                                facevalue, securitytype, seccurrencyid, pricetype, pricecurrencyid, recno, tradeno, 
                                tradenoextra, tradedate, tradetime, primaryorderid, orderid, 
                                commentar, tradeplace, buysell, settlecode, tradetype, tradeinstrumenttype, tradeperiod, trademodeid, decimals, 
                                price, quantity, value, 
                                amount, balance, exchcomm, 
                                clrcomm, corpevent, liccomm, clracccode, subaccclrcode, 
                                ccpcode, cpfirmid, cpfirmshortname, duedate, 
                                accint, price2, repopart, repoperiod, 
                                reporate, type, finedebit, 
                                finecredit, repositorynumber)
       SELECT to_date(x.ReportDate, 'YYYY-MM-DD'), x.Volume, x.VolumeTotal, x.ReportNumber, x.ReportLang, x.ReportCode, y.FirmID, z.ClientCode, z.ClientDetails, 
              v.CurrencyId, v.CurrencyName, w.InfType, t.ClearingType, 
              CASE WHEN length(s.ClearingTime) > 8 THEN to_timestamp(s.ClearingTime,'YYYY-MM-DD HH24:MI:SS.FF')
                   WHEN length(s.ClearingTime) = 8 THEN to_timestamp(to_date(m.SettleDate, 'YYYY-MM-DD')||' '||s.ClearingTime, 'DD.MM.YY HH24:MI:SS.FF')
              END, 
              to_date(m.SettleDate, 'YYYY-MM-DD'), r.BoardId, r.BoardName, p.SecShortName, p.SecurityId, p.ISIN, p.BaseSecurityCode, 
              to_number(p.FaceValue, '99999999999999999999.99999999'), p.SecurityType, p.SecCurrencyId, p.PriceType, p.PriceCurrencyId, u.RecNo, u.TradeNo, 
              u.TradeNoExtra, to_date(u.TradeDate, 'YYYY-MM-DD'), to_date('0001-01-01'||' '||u.TradeTime, 'YYYY-MM-DD HH24:MI:SS'), u.PrimaryOrderID, u.OrderID, 
              u.Commentar, u.TradePlace, u.BuySell, u.SettleCode, u.TradeType, u.TradeInstrumentType, u.TradePeriod, u.TradeModeId, u.Decimals, 
              to_number(u.Price, '99999999999999999999.99999999'), to_number(u.Quantity, '99999999999999999999.99999999'), to_number(u.Val, '99999999999999999999.99999999'), 
              to_number(u.Amount, '99999999999999999999.99999999'), to_number(u.Balance, '99999999999999999999.99999999'), to_number(u.ExchComm, '99999999999999999999.99999999'), 
              to_number(u.ClrComm, '99999999999999999999.99999999'), u.CorpEvent, to_number(u.LicComm, '99999999999999999999.99999999'), u.ClrAccCode, u.SubAccClrCode, 
              u.CcpCode, u.CPFirmId, u.CPFirmShortName, to_date(u.DueDate, 'YYYY-MM-DD'), 
              to_number(u.AccInt, '99999999999999999999.99999999'), to_number(u.Price2, '99999999999999999999.99999999'), u.RepoPart, u.RepoPeriod, 
              to_number(u.RepoRate, '99999999999999999999.99999999'), u.Type, to_number(u.FineDebit, '99999999999999999999.99999999'), 
              to_number(u.FineCredit, '99999999999999999999.99999999'), u.RepositoryNumber
         FROM tmp_xml t,
              XMLTABLE('//REPORT' PASSING valxml
                       COLUMNS DOC_INFO XMLTYPE PATH '/*',
                               FIRM     XMLTYPE PATH '/*') n,
              XMLTABLE('//DOC_INFO' PASSING n.DOC_INFO
                       COLUMNS ReportDate   PATH '@ReportDate',
                               Volume       PATH '@Volume',
                               VolumeTotal  PATH '@VolumeTotal',
                               ReportNumber PATH '@ReportNumber',
                               ReportLang   PATH '@ReportLang',
                               ReportCode   PATH '@ReportCode') x,
              XMLTABLE('//FIRM' PASSING n.FIRM
                       COLUMNS FirmID         PATH '@FirmID',
                               CLIENT XMLTYPE PATH '/*') y,
              XMLTABLE('//CLIENT' PASSING y.CLIENT
                       COLUMNS ClientCode       PATH '@ClientCode',
                               ClientDetails    PATH '@ClientDetails',
                               CURRENCY XMLTYPE PATH '/*') z,
              XMLTABLE('//CURRENCY' PASSING z.CURRENCY
                       COLUMNS CurrencyId      PATH '@CurrencyId',
                               CurrencyName    PATH '@CurrencyName',
                               INFTYPE XMLTYPE PATH '/*') v,
              XMLTABLE('//INFTYPE' PASSING v.INFTYPE
                       COLUMNS InfType              PATH '@InfType',
                               CLEARINGTYPE XMLTYPE PATH '/*') w,
              XMLTABLE('//CLEARINGTYPE' PASSING w.CLEARINGTYPE
                       COLUMNS ClearingType    PATH '@ClearingType',
                               SESION XMLTYPE PATH '/*') t,
              XMLTABLE('//SESSION' PASSING t.SESION
                       COLUMNS ClearingTime       PATH '@ClearingTime',
                               SETTLEDATE XMLTYPE PATH '/*') s,
              XMLTABLE('//SETTLEDATE' PASSING s.SETTLEDATE
                       COLUMNS SettleDate      PATH '@SettleDate',
                               BOARD XMLTYPE PATH '/*') m,
              XMLTABLE('//BOARD' PASSING m.BOARD
                       COLUMNS BoardName        PATH '@BoardName',
                               BoardID          PATH '@BoardID',
                               SECURITY XMLTYPE PATH '/*') r,
              XMLTABLE('//SECURITY' PASSING r.SECURITY
                       COLUMNS SecurityId       PATH '@SecurityId',
                               BaseSecurityCode PATH '@BaseSecurityCode',
                               ISIN             PATH '@ISIN',
                               SecShortName     PATH '@SecShortName',
                               FaceValue        PATH '@FaceValue',
                               SecCurrencyId    PATH '@SecCurrencyId',
                               PriceCurrencyId  PATH '@PriceCurrencyId',
                               SecurityType     PATH '@SecurityType',
                               PriceType        PATH '@PriceType',
                               RECORDS  XMLTYPE PATH '/*') p,
              XMLTABLE('//RECORDS' PASSING p.RECORDS
                       COLUMNS RecNo               PATH '@RecNo',
                               TradeNo             PATH '@TradeNo',
                               TradeNoExtra        PATH '@TradeNoExtra',
                               TradeDate           PATH '@TradeDate',
                               TradeTime           PATH '@TradeTime',
                               PrimaryOrderID      PATH '@PrimaryOrderID',
                               OrderID             PATH '@OrderID',
                               Commentar           PATH '@Comment',
                               TradePlace          PATH '@TradePlace',
                               BuySell             PATH '@BuySell',
                               SettleCode          PATH '@SettleCode',
                               TradePeriod         PATH '@TradePeriod',
                               TradeType           PATH '@TradeType',
                               TradeModeId         PATH '@TradeModeId',
                               TradeInstrumentType PATH '@TradeInstrumentType',
                               Decimals            PATH '@Decimals',
                               Price               PATH '@Price',
                               Quantity            PATH '@Quantity',
                               Val                 PATH '@Value',
                               Amount              PATH '@Amount',
                               Balance             PATH '@Balance',
                               CorpEvent           PATH '@CorpEvent',
                               ExchComm            PATH '@ExchComm',
                               ClrComm             PATH '@ClrComm',
                               LicComm             PATH '@LicComm',
                               ClrAccCode          PATH '@ClrAccCode',
                               SubAccClrCode       PATH '@SubAccClrCode',
                               CcpCode             PATH '@CcpCode',
                               CPFirmId            PATH '@CPFirmId',
                               CPFirmShortName     PATH '@CPFirmShortName',
                               DueDate             PATH '@DueDate',
                               AccInt              PATH '@AccInt',
                               Price2              PATH '@Price2',
                               RepoPart            PATH '@RepoPart',
                               RepoPeriod          PATH '@RepoPeriod',
                               RepoRate            PATH '@RepoRate',
                               Type                PATH '@Type',
                               FineDebit           PATH '@FineDebit',
                               FineCredit          PATH '@FineCredit',
                               RepositoryNumber    PATH '@RepositoryNumber') u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета СПБ.';
       Return 0;
     ELSE 
       Return SQL%ROWCOUNT;
     END IF;
   end LoadMFB06C;

   function LoadMFB13 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета MFB13.
     p_Text - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_spb_mfb13(reportdate, volume, volumetotal, reportnumber, reportlang, reportcode, firmid, clearingtype,
                               clearingtime, finalobligations, clracccode, postype, bankacccode, guardepunitid,
                               currencyid, currencyname, securityid, secshortname, debit,
                               credit, shortage)
       SELECT to_date(x.ReportDate, 'YYYY-MM-DD'), x.Volume, x.VolumeTotal, x.ReportNumber, x.ReportLang, x.ReportCode, y.FirmID, t.ClearingType, 
              to_date(s.ClearingTime, 'YYYY-MM-DD HH24:MI:SS'), s.FinalObligations, m.ClrAccCode, r.PosType, p.BankAccCode, p.GuarDepUnitId,
              u.CurrencyId, u.CurrencyName, u.SecurityId, u.SecShortName, to_number(u.Debit, '99999999999999999999.99999999'), 
              to_number(u.Credit, '99999999999999999999.99999999'), to_number(u.Shortage, '99999999999999999999.99999999')
         FROM tmp_xml t,
              XMLTABLE('//REPORT' PASSING valxml
                       COLUMNS DOC_INFO XMLTYPE PATH '/*',
                               FIRM     XMLTYPE PATH '/*') n,
              XMLTABLE('//DOC_INFO' PASSING n.DOC_INFO
                       COLUMNS ReportDate   PATH '@ReportDate',
                               Volume       PATH '@Volume',
                               VolumeTotal  PATH '@VolumeTotal',
                               ReportNumber PATH '@ReportNumber',
                               ReportLang   PATH '@ReportLang',
                               ReportCode   PATH '@ReportCode') x,
              XMLTABLE('//FIRM' PASSING n.FIRM
                       COLUMNS FirmID               PATH '@FirmID',
                               CLEARINGTYPE XMLTYPE PATH '/*') y,
              XMLTABLE('//CLEARINGTYPE' PASSING y.CLEARINGTYPE
                       COLUMNS ClearingType   PATH '@ClearingType',
                               SESION XMLTYPE PATH '/*') t,
              XMLTABLE('//SESSION' PASSING t.SESION
                       COLUMNS ClearingTime     PATH '@ClearingTime',
                               FinalObligations PATH '@FinalObligations',
                               SETTLE   XMLTYPE PATH '/*') s,
              XMLTABLE('//SETTLE' PASSING s.SETTLE
                       COLUMNS ClrAccCode       PATH '@ClrAccCode',
                               POSTYPES XMLTYPE PATH '/*') m,
              XMLTABLE('//POSTYPES' PASSING m.POSTYPES
                       COLUMNS PosType        PATH '@PosType',
                               GROUPP XMLTYPE PATH '/*') r,
              XMLTABLE('//GROUP' PASSING r.GROUPP
                       COLUMNS BankAccCode     PATH '@BankAccCode',
                               GuarDepUnitId   PATH '@GuarDepUnitId',
                               RECORDS XMLTYPE PATH '/*') p,
              XMLTABLE('//RECORDS' PASSING p.RECORDS
                       COLUMNS CurrencyId   PATH '@CurrencyId',
                               CurrencyName PATH '@CurrencyName',
                               SecurityId   PATH '@SecurityId',
                               SecShortName PATH '@SecShortName',
                               Debit        PATH '@Debit',
                               Credit       PATH '@Credit',
                               Shortage     PATH '@Shortage') u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета СПБ.';
       Return 0;
     ELSE 
       Return SQL%ROWCOUNT;
     END IF;
   end LoadMFB13;

   function LoadMFB98 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета MFB98.
     p_Text - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_spb_mfb98(reportdate, volume, volumetotal, reportnumber, reportlang, reportcode, firmid, clracccode, bankacccode, guardepunitid,
                               tradeno, tradenootc, operationno, clientcode, tradedate, settledate1,
                               settledate2, debitcredit, tradespecialtype, securityid, secshortname, isin, 
                               quantity, type, recorddate, paymentdate,
                               obligationdate, fullnetto, newsecurityid, newsecshortname, newisin, currencyid, currencyname,
                               price, taxrate,
                               sum)
       SELECT to_date(x.ReportDate, 'YYYY-MM-DD'), x.Volume, x.VolumeTotal, x.ReportNumber, x.ReportLang, x.ReportCode, y.FirmID, m.ClrAccCode, p.BankAccCode, p.GuarDepUnitId,
              u.TradeNo, u.TradeNoOtc, u.OperationNo, u.ClientCode, to_date(u.TradeDate, 'YYYY-MM-DD HH24:MI:SS'), to_date(u.SettleDate1, 'YYYY-MM-DD HH24:MI:SS'),
              to_date(u.SettleDate2, 'YYYY-MM-DD HH24:MI:SS'), u.DebitCredit, u.TradeSpecialType, u.SecurityId, u.SecShortName, u.ISIN, 
              to_number(u.Quantity, '99999999999999999999.99999999'), u.Type, to_date(u.RecordDate, 'YYYY-MM-DD HH24:MI:SS'), to_date(u.PaymentDate, 'YYYY-MM-DD HH24:MI:SS'), 
              to_date(u.ObligationDate, 'YYYY-MM-DD HH24:MI:SS'), u.FullNetto, u.NewSecurityId, u.NewSecShortName, u.NewISIN, u.CurrencyId, u.CurrencyName,
              to_number(u.Price, '99999999999999999999.99999999'), to_number(u.TaxRate, '99999999999999999999.99999999'), 
              to_number(u.Sum, '99999999999999999999.99999999')
         FROM tmp_xml t,
              XMLTABLE('//REPORT' PASSING valxml
                       COLUMNS DOC_INFO XMLTYPE PATH '/*',
                               FIRM     XMLTYPE PATH '/*') n,
              XMLTABLE('//DOC_INFO' PASSING n.DOC_INFO
                       COLUMNS ReportDate   PATH '@ReportDate',
                               Volume       PATH '@Volume',
                               VolumeTotal  PATH '@VolumeTotal',
                               ReportNumber PATH '@ReportNumber',
                               ReportLang   PATH '@ReportLang',
                               ReportCode   PATH '@ReportCode') x,
              XMLTABLE('//FIRM' PASSING n.FIRM
                       COLUMNS FirmID         PATH '@FirmID',
                               SETTLE XMLTYPE PATH '/*') y,
              XMLTABLE('//SETTLE' PASSING y.SETTLE
                       COLUMNS ClrAccCode     PATH '@ClrAccCode',
                               GROUPP XMLTYPE PATH '/*') m,
              XMLTABLE('//GROUP' PASSING m.GROUPP
                       COLUMNS BankAccCode     PATH '@BankAccCode',
                               GuarDepUnitId   PATH '@GuarDepUnitId',
                               RECORDS XMLTYPE PATH '/*') p,
              XMLTABLE('//RECORDS' PASSING p.RECORDS
                       COLUMNS TradeNo          PATH '@TradeNo',
                               TradeNoOtc       PATH '@TradeNoOtc',
                               OperationNo      PATH '@OperationNo',
                               ClientCode       PATH '@ClientCode',
                               TradeDate        PATH '@TradeDate',
                               SettleDate1      PATH '@SettleDate1',
                               SettleDate2      PATH '@SettleDate2',
                               DebitCredit      PATH '@DebitCredit',
                               TradeSpecialType PATH '@TradeSpecialType',
                               SecurityId       PATH '@SecurityId',
                               SecShortName     PATH '@SecShortName',
                               ISIN             PATH '@ISIN',
                               Quantity         PATH '@Quantity',
                               Type             PATH '@Type',
                               RecordDate       PATH '@RecordDate',
                               PaymentDate      PATH '@PaymentDate',
                               ObligationDate   PATH '@ObligationDate',
                               FullNetto        PATH '@FullNetto',
                               NewSecurityId    PATH '@NewSecurityId',
                               NewSecShortName  PATH '@NewSecShortName',
                               NewISIN          PATH '@NewISIN',
                               CurrencyId       PATH '@CurrencyId',
                               CurrencyName     PATH '@CurrencyName',
                               Price            PATH '@Price',
                               TaxRate          PATH '@TaxRate',
                               Sum              PATH '@Sum') u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета СПБ.';
       Return 0;
     ELSE 
       Return SQL%ROWCOUNT;
     END IF;
   end LoadMFB98;

   function LoadMFB99 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета MFB99.
     p_Text - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_spb_mfb99(reportdate, volume, volumetotal, reportnumber, reportlang, reportcode, firmid, clracccode, guaranteefund, postype, bankacccode,
                               guardepunitid, currencyid, currencyname, securityid, isin, secshortname,
                               openingbalance, closingbalance,
                               openingdebtssum, debtssum,
                               debitsum, creditsum, operationcode,
                               purpose, operationtime, docno, customerno,
                               debit, credit, clientcode)
       SELECT to_date(x.ReportDate, 'YYYY-MM-DD'), x.Volume, x.VolumeTotal, x.ReportNumber, x.ReportLang, x.ReportCode, y.FirmID, m.ClrAccCode, m.GuaranteeFund, r.PosType, p.BankAccCode, 
              p.GuarDepUnitId, u.CurrencyId, u.CurrencyName, u.SecurityId, u.ISIN, u.SecShortName, 
              to_number(u.OpeningBalance, '9999999999999999999999999999.99999999'), to_number(u.ClosingBalance, '9999999999999999999999999999.99999999'), 
              to_number(u.OpeningDebtsSum, '9999999999999999999999999999.99999999'), to_number(u.DebtsSum, '9999999999999999999999999999.99999999'), 
              to_number(u.DebitSum, '9999999999999999999999999999.99999999'), to_number(u.CreditSum, '9999999999999999999999999999.99999999'), s.OperationCode, 
              s.Purpose, to_date('0001-01-01'||' '||s.OperationTime, 'YYYY-MM-DD HH24:MI:SS'), s.DocNo, s.CustomerNo, 
              to_number(s.Debit, '9999999999999999999999999999.99999999'), to_number(s.Credit, '9999999999999999999999999999.99999999'), s.ClientCode
         FROM tmp_xml t,
              XMLTABLE('//REPORT' PASSING valxml
                       COLUMNS DOC_INFO XMLTYPE PATH '/*',
                               FIRM     XMLTYPE PATH '/*') n,
              XMLTABLE('//DOC_INFO' PASSING n.DOC_INFO
                       COLUMNS ReportDate   PATH '@ReportDate',
                               Volume       PATH '@Volume',
                               VolumeTotal  PATH '@VolumeTotal',
                               ReportNumber PATH '@ReportNumber',
                               ReportLang   PATH '@ReportLang',
                               ReportCode   PATH '@ReportCode') x,
              XMLTABLE('//FIRM' PASSING n.FIRM
                       COLUMNS FirmID         PATH '@FirmID',
                               SETTLE XMLTYPE PATH '/*') y,
              XMLTABLE('//SETTLE' PASSING y.SETTLE
                       COLUMNS ClrAccCode     PATH '@ClrAccCode',
                               GuaranteeFund  PATH '@GuaranteeFund',
                               POSTYPES XMLTYPE PATH '/*') m,
              XMLTABLE('//POSTYPES' PASSING m.POSTYPES
                       COLUMNS PosType         PATH '@PosType',
                               GROUPP XMLTYPE  PATH '/*') r,
              XMLTABLE('//GROUP' PASSING r.GROUPP
                       COLUMNS BankAccCode     PATH '@BankAccCode',
                               GuarDepUnitId   PATH '@GuarDepUnitId',
                               RECORDS XMLTYPE PATH '/*') p,
              XMLTABLE('//RECORDS' PASSING p.RECORDS
                       COLUMNS CurrencyId      PATH '@CurrencyId',
                               CurrencyName    PATH '@CurrencyName',
                               SecurityId      PATH '@SecurityId',
                               ISIN            PATH '@ISIN',
                               SecShortName    PATH '@SecShortName',
                               OpeningBalance  PATH '@OpeningBalance',
                               ClosingBalance  PATH '@ClosingBalance',
                               OpeningDebtsSum PATH '@OpeningDebtsSum',
                               DebtsSum        PATH '@DebtsSum',
                               DebitSum        PATH '@DebitSum',
                               CreditSum       PATH '@CreditSum',
                               ENTRY   XMLTYPE PATH '/*') u,
              XMLTABLE('//ENTRY' PASSING u.ENTRY
                       COLUMNS OperationCode PATH '@OperationCode',
                               Purpose       PATH '@Purpose',
                               OperationTime PATH '@OperationTime',
                               DocNo         PATH '@DocNo',
                               CustomerNo    PATH '@CustomerNo',
                               Debit         PATH '@Debit',
                               Credit        PATH '@Credit',
                               ClientCode    PATH '@ClientCode')(+) s;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета СПБ.';
       Return 0;
     ELSE 
       Return SQL%ROWCOUNT;
     END IF;
   end LoadMFB99;

   function LoadORDERS (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета ORDERS.
     p_Text - информация о результате обработки.
   */
     l_Str   CLOB;
     l_Value RSP_COMMON.t_Value := RSP_COMMON.t_Value();
     l_Cnt   INTEGER;
     l_Strv  VARCHAR2(32000);
     l_len   INTEGER;
     l_tmp   INTEGER := 1;
     l_pos   INTEGER;
   begin
     SELECT t.valclob||chr(10), LENGTH(t.valclob)
       INTO l_Str, l_len
       FROM tmp_xml t;

     l_pos:= INSTR(l_str, chr(10));

     WHILE l_pos > 0 LOOP
       l_strv := SUBSTR(l_str, l_tmp, l_pos - l_tmp);
       l_tmp:= l_pos+1;
       l_Value := RSP_COMMON.ParseStr(p_Str       => l_strv,
                                      p_Separator => chr(9));
       l_pos:= INSTR(l_str, chr(10), l_tmp);
       INSERT INTO TMP_SPB_ORDERS(ACTION, ORDER_NO, REG_NO, ENTRY_DATE, ENTRY_TIME, FIRMID, FIRMCODE,
                                  CLIENT_CODEID, CLIENT_ALIAS, CLIENT_COUNTRY, LOGIN, TRD_ACCID, CP_FIRMID,
                                  CP_FIRMCODE, ADDRESS_CODE, CP_ADDRESS_CODE, BUY_SELL, ORDER_TYPE, IS_ADDRESS,
                                  IS_MM, SECURITYID, TRADE_MODE, TRADE_PERIOD, QUOTE_CURRENCY, CURRENCY, PRICE,
                                  REPO_PRICE, QUANTITY, QUANTITY_LOT, QUANTITY_REST, QUANTITY_LOT_REST, VOLUME,
                                  STATUS, STATUS_REASON, AMEND_DATE, AMEND_TIME, ISSUE_DATE, ISSUE_TIME,
                                  MATCH_REF, NUM_TRADES, SETT_TYPE, SPECIAL_PERIOD, CL_ORDERID, EXTRA_REF,
                                  COMMENTAR, TIME_IN_FORCE, EXTRA_ORDER_NO)
         VALUES(l_Value(1), l_Value(2), l_Value(3), to_date(l_Value(4), 'DD.MM.YYYY'), to_timestamp('01.01.0001'||' '||l_Value(5), 'DD.MM.YYYY HH24:MI:SS.FF'), l_Value(6), l_Value(7),
                l_Value(8), l_Value(9), l_Value(10), l_Value(11), l_Value(12), l_Value(13), l_Value(14), l_Value(15), l_Value(16),
                l_Value(17), l_Value(18), l_Value(19), l_Value(20), l_Value(21), l_Value(22), l_Value(23), l_Value(24),
                l_Value(25), to_number(l_Value(26), '99999999999999999999999999.99999999'), to_number(l_Value(27), '99999999999999999999999999.99999999'), l_Value(28), l_Value(29), 
                l_Value(30), l_Value(31), to_number(l_Value(32), '99999999999999999999999999.99999999'), l_Value(33), l_Value(34), to_date(l_Value(35), 'DD.MM.YYYY'), 
                to_timestamp('01.01.0001'||' '||l_Value(36), 'DD.MM.YYYY HH24:MI:SS.FF'), to_date(l_Value(37), 'DD.MM.YYYY'), to_timestamp('01.01.0001'||' '||l_Value(38), 'DD.MM.YYYY HH24:MI:SS.FF'),
                l_Value(39), l_Value(40), l_Value(41), l_Value(42), l_Value(43), l_Value(44), l_Value(45), l_Value(46), l_Value(47));
       l_Value.DELETE;
     END LOOP;
     SELECT count(*)
       INTO l_Cnt
       FROM TMP_SPB_ORDERS;
     IF l_Cnt = 0 THEN
       p_Text := 'ошибка обработки элементов отчета СПБ.';
       Return 0;
     ELSE 
       Return l_Cnt;
     END IF;
   end LoadORDERS;

   function Load_SPBFile (p_FILENAME in varchar2, p_FileType in varchar2, p_Msg out varchar2) return INTEGER as
   /*Основная функция загрузки полученных отчетов МБ.
     p_FileName - имя принятого файла.
     Возвращает информацию о результате загрузки и обработки.
   */
     l_Rows INTEGER;
     l_Text VARCHAR2(4000);
   begin
     p_Msg := 'Прием файла СПБ '||p_FILENAME||': ';
     l_Rows := RSP_SPB.LOADREQUISITES(p_FILENAME, p_FileType, l_Text);
     IF p_FileType = 'SPB03' THEN
       l_Rows := RSP_SPB.LoadSPB03(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'SPB21' THEN
       l_Rows := RSP_SPB.LoadSPB21(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'MFB06C' THEN
       l_Rows := RSP_SPB.LoadMFB06C(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'MFB13' THEN
       l_Rows := RSP_SPB.LoadMFB13(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'MFB98' THEN
       l_Rows := RSP_SPB.LoadMFB98(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'MFB99' THEN
       l_Rows := RSP_SPB.LoadMFB99(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'ORDERS' THEN
       l_Rows := RSP_SPB.LoadORDERS(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
   end Load_SPBFile;

   function Insert_Requisites (p_FILENAME in varchar2, p_IDProcLog in integer, p_IDNFORM in integer) RETURN INTEGER as
   /*Обработка отчета МБ.
     p_FileName - имя файла отчета.
   */
     l_IDReq INTEGER;
   begin
     l_IDReq := Sequence_Mb_Requisites.Nextval();
     INSERT INTO mb_requisites(id_mb_requisites, id_processing_log, id_nform, doc_date, doc_time, doc_no, doc_type_id, sender_id, sender_name, receiver_id, remarks, file_name)
       SELECT l_IDReq, p_IDProcLog, p_IDNFORM, x.DOC_DATE, x.DOC_TIME, x.DOC_NO, x.DOC_TYPE_ID, x.SENDER_ID, x.SENDER_NAME, x.RECEIVER_ID, x.REMARKS, p_FILENAME
         FROM tmp_mb_requisites x;
     Return l_IDReq;
   end Insert_Requisites;

   function Insert_SPB03 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета SPB03.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO spb_spb03_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO spb_spb03(id_mb_requisites, id_processing_log, reportdate, volume, volumetotal, reportnumber, reportlang, reportcode, clracccode, subclracccode, currencyid, 
                           currencyname, boardid, boardname, settledate, securityid, secshortname, isin, regnumber, facevalue, seccurrencyid, 
                           securitytype, pricetype, recno, tradeno, tradenoextra, tradedate, tradetime, tradeperiod, specialperiod, primaryorderid, 
                           orderid, ordertype, userid, commentar, ismm, buysell, settlecode, tradetype, tradeinstrumenttype, trademodeid, trademodename, 
                           decimals, price, quantity, value, amount, balance, exchcomm, clrcomm, clientcode, clientdetails, ccpcode, 
                           cpfirmid, cpfirmshortname, otccodeinitiator, otccodeconfirmator, accint, price2, reporate, repopart, 
                           repoperiod, type, internaltrademodeid)
       SELECT p_IDReg, p_IDProcLog, u.reportdate, u.volume, u.volumetotal, u.reportnumber, u.reportlang, u.reportcode, u.clracccode, u.subclracccode, u.currencyid,
              u.currencyname, u.boardid, u.boardname, u.settledate, u.securityid, u.secshortname, u.isin, u.regnumber, u.facevalue, u.seccurrencyid,
              u.securitytype, u.pricetype, u.recno, u.tradeno, u.tradenoextra, u.tradedate, u.tradetime, u.tradeperiod, u.specialperiod, u.primaryorderid, 
              u.orderid, u.ordertype, u.userid, u.commentar, u.ismm, u.buysell, u.settlecode, u.tradetype, u.tradeinstrumenttype, u.trademodeid, u.trademodename,
              u.decimals, u.price, u.quantity, u.value, u.amount, u.balance, u.exchcomm, u.clrcomm, u.clientcode, u.clientdetails, u.ccpcode,
              u.cpfirmid, u.cpfirmshortname, u.otccodeinitiator, u.otccodeconfirmator, u.accint, u.price2, u.reporate, u.repopart,
              u.repoperiod, u.type, u.internaltrademodeid
         FROM tmp_spb_spb03 u;
     Return SQL%ROWCOUNT;
   end Insert_SPB03;

   function Insert_SPB21 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета SPB21.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO spb_spb21_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO spb_spb21(id_mb_requisites, id_processing_log, tradedate, volume, volumetotal, reportnumber, reportlang, reportcode, 
                           boardid, boardname, securityid, secshortname, isin, regnumber, facevalue,
                           seccurrencyid, securitytype, issuername, issuerdetails, quotelist, decim, currencyid, currencyname, accruedinterest,
                           tradeperiod, setttype_market, trademode_market, periodtotalamount, periodtotalvolume, periodtotalcount, periodopenprice,
                           periodopenvolume, periodlastprice, periodlastvolume, periodcurrentprice, periodmaxdealprice, periodmindealprice, periodwaprice,
                           setttype_address, trademode_address, addressperiodtotalamount, addressperiodtotalvolume, addressperiodtotalcount,
                           addressperiodopenprice, addressperiodopenvolume, addressperiodlastprice, addressperiodlastvolume, addressperiodcurrentprice,
                           addressperiodmaxdealprice, addressperiodmindealprice, addressperiodwaprice, totalamount, totalvolume, totaldealcount,
                           maxdealprice, mindealprice, closeprice, prevclose, trendclose, waprice, currentprice, admittedquote, admittedquotevolume,
                           marketprice2, mp2volume, marketprice3, mp3volume, clearingprice)
       SELECT p_IDReg, p_IDProcLog, u.tradedate, u.volume, u.volumetotal, u.reportnumber, u.reportlang, u.reportcode, 
              u.boardid, u.boardname, u.securityid, u.secshortname, u.isin, u.regnumber, u.facevalue,
              u.seccurrencyid, u.securitytype, u.issuername, u.issuerdetails, u.quotelist, u.decim, u.currencyid, u.currencyname, u.accruedinterest,
              u.tradeperiod, u.setttype_market, u.trademode_market, u.periodtotalamount, u.periodtotalvolume, u.periodtotalcount, u.periodopenprice,
              u.periodopenvolume, u.periodlastprice, u.periodlastvolume, u.periodcurrentprice, u.periodmaxdealprice, u.periodmindealprice, u.periodwaprice,
              u.setttype_address, u.trademode_address, u.addressperiodtotalamount, u.addressperiodtotalvolume, u.addressperiodtotalcount,
              u.addressperiodopenprice, u.addressperiodopenvolume, u.addressperiodlastprice, u.addressperiodlastvolume, u.addressperiodcurrentprice,
              u.addressperiodmaxdealprice, u.addressperiodmindealprice, u.addressperiodwaprice, u.totalamount, u.totalvolume, u.totaldealcount,
              u.maxdealprice, u.mindealprice, u.closeprice, u.prevclose, u.trendclose, u.waprice, u.currentprice, u.admittedquote, u.admittedquotevolume,
              u.marketprice2, u.mp2volume, u.marketprice3, u.mp3volume, u.clearingprice
         FROM tmp_spb_spb21 u;
     Return SQL%ROWCOUNT;
   end Insert_SPB21;

   function Insert_MFB06C (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета MFB06C.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO spb_mfb06c_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO spb_mfb06c(id_mb_requisites, id_processing_log, reportdate, volume, volumetotal, reportnumber, reportlang, reportcode, firmid, clientcode, clientdetails,
                            currencyid, currencyname, inftype, clearingtype, clearingtime, settledate, boardname, boardid, securityid, basesecuritycode, isin,
                            secshortname, facevalue, seccurrencyid, pricecurrencyid, securitytype, pricetype, recno, tradeno, tradenoextra, tradedate, tradetime,
                            primaryorderid, orderid, commentar, tradeplace, buysell, settlecode, tradeperiod, tradetype, trademodeid, tradeinstrumenttype,
                            decimals, price, quantity, value, amount, balance, corpevent, exchcomm, clrcomm, liccomm, clracccode, subaccclrcode,
                            ccpcode, cpfirmid, cpfirmshortname, duedate, accint, price2, repopart, repoperiod,
                            reporate, type, finedebit, finecredit, repositorynumber)
       SELECT p_IDReg, p_IDProcLog, u.reportdate, u.volume, u.volumetotal, u.reportnumber, u.reportlang, u.reportcode, u.firmid, u.clientcode, u.clientdetails, u.currencyid,
              u.currencyname, u.inftype, u.clearingtype, u.clearingtime, u.settledate, u.boardname, u.boardid, u.securityid, u.basesecuritycode, u.isin, 
              u.secshortname, u.facevalue, u.seccurrencyid, u.pricecurrencyid, u.securitytype, u.pricetype, u.recno, u.tradeno, u.tradenoextra, u.tradedate,
              u.tradetime, u.primaryorderid, u.orderid, u.commentar, u.tradeplace, u.buysell, u.settlecode, u.tradeperiod, u.tradetype, u.trademodeid,
              u.tradeinstrumenttype, u.decimals, u.price, u.quantity, u.value, u.amount, u.balance, u.corpevent, u.exchcomm, u.clrcomm, u.liccomm, u.clracccode,
              u.subaccclrcode, u.ccpcode, u.cpfirmid, u.cpfirmshortname, u.duedate, u.accint, 
              u.price2, u.repopart, u.repoperiod, u.reporate, u.type, u.finedebit, u.finecredit, u.repositorynumber
         FROM tmp_spb_mfb06c u;
     Return SQL%ROWCOUNT;
   end Insert_MFB06C;

   function Insert_MFB13 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета MFB13.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO spb_mfb13_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO spb_mfb13(id_mb_requisites, id_processing_log, reportdate, volume, volumetotal, reportnumber, reportlang, reportcode, firmid, clearingtype, clearingtime,
                           finalobligations, clracccode, postype, bankacccode, guardepunitid, currencyid, currencyname, securityid, secshortname,
                           debit, credit, shortage)
       SELECT p_IDReg, p_IDProcLog, u.reportdate, u.volume, u.volumetotal, u.reportnumber, u.reportlang, u.reportcode, u.firmid, u.clearingtype, u.clearingtime,
              u.finalobligations, u.clracccode, u.postype, u.bankacccode, u.guardepunitid, u.currencyid, u.currencyname, u.securityid, u.secshortname,
              u.debit, u.credit, u.shortage
         FROM tmp_spb_mfb13 u;
     Return SQL%ROWCOUNT;
   end Insert_MFB13;

   function Insert_MFB98 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета MFB98.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO spb_mfb98_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO spb_mfb98(id_mb_requisites, id_processing_log, reportdate, volume, volumetotal, reportnumber, reportlang, reportcode, firmid, clracccode, bankacccode, guardepunitid,
                           tradeno, tradenootc, operationno, clientcode, tradedate, settledate1, settledate2, debitcredit, tradespecialtype, securityid,
                           secshortname, isin, quantity, type, recorddate, paymentdate, obligationdate, fullnetto, newsecurityid, newsecshortname, 
                           newisin, currencyid, currencyname, price, taxrate, sum)
       SELECT p_IDReg, p_IDProcLog, u.reportdate, u.volume, u.volumetotal, u.reportnumber, u.reportlang, u.reportcode, u.firmid, u.clracccode, u.bankacccode, u.guardepunitid,
              u.tradeno, u.tradenootc, u.operationno, u.clientcode, u.tradedate, u.settledate1, u.settledate2, u.debitcredit, u.tradespecialtype, u.securityid,
              u.secshortname, u.isin, u.quantity, u.type, u.recorddate, u.paymentdate, u.obligationdate, u.fullnetto, u.newsecurityid, u.newsecshortname, 
              u.newisin, u.currencyid, u.currencyname, u.price, u.taxrate, u.sum
         FROM tmp_spb_mfb98 u;
     Return SQL%ROWCOUNT;
   end Insert_MFB98;

   function Insert_MFB99 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета MFB99.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO spb_mfb99_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO spb_mfb99(id_mb_requisites, id_processing_log, reportdate, volume, volumetotal, reportnumber, reportlang, reportcode, firmid, clracccode, guaranteefund, postype,
                           bankacccode, guardepunitid, currencyid, currencyname, securityid, isin, secshortname, openingbalance, closingbalance, 
                           openingdebtssum, debtssum, debitsum, creditsum, operationcode, operationtime, docno, customerno, purpose, debit, credit, 
                           clientcode)
       SELECT p_IDReg, p_IDProcLog, u.reportdate, u.volume, u.volumetotal, u.reportnumber, u.reportlang, u.reportcode, u.firmid, u.clracccode, u.guaranteefund, u.postype,
              u.bankacccode, u.guardepunitid, u.currencyid, u.currencyname, u.securityid, u.isin, u.secshortname, u.openingbalance, u.closingbalance,
              u.openingdebtssum, u.debtssum, u.debitsum, u.creditsum, u.operationcode, u.operationtime, u.docno, u.customerno, u.purpose, u.debit, u.credit,
              u.clientcode
         FROM tmp_spb_mfb99 u;
     Return SQL%ROWCOUNT;
   end Insert_MFB99;

   function Insert_ORDERS (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета ORDERS.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO spb_orders_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO SPB_ORDERS(ID_MB_REQUISITES, ID_PROCESSING_LOG, ACTION, ORDER_NO, REG_NO, ENTRY_DATE,
                            ENTRY_TIME, FIRMID, FIRMCODE, CLIENT_CODEID, CLIENT_ALIAS, CLIENT_COUNTRY,
                            LOGIN, TRD_ACCID, CP_FIRMID, CP_FIRMCODE, ADDRESS_CODE, CP_ADDRESS_CODE, BUY_SELL,
                            ORDER_TYPE, IS_ADDRESS, IS_MM, SECURITYID, TRADE_MODE, TRADE_PERIOD, QUOTE_CURRENCY,
                            CURRENCY, PRICE, REPO_PRICE, QUANTITY, QUANTITY_LOT, QUANTITY_REST, QUANTITY_LOT_REST,
                            VOLUME, STATUS, STATUS_REASON, AMEND_DATE, AMEND_TIME, ISSUE_DATE, ISSUE_TIME,
                            MATCH_REF, NUM_TRADES, SETT_TYPE, SPECIAL_PERIOD, CL_ORDERID, EXTRA_REF, COMMENTAR,
                            TIME_IN_FORCE, EXTRA_ORDER_NO)
       SELECT p_IDReg, p_IDProcLog, t.action, t.order_no, t.reg_no, t.entry_date,
              t.entry_time, t.firmid, t.firmcode, t.client_codeid, t.client_alias, t.client_country,
              t.login, t.trd_accid, t.cp_firmid, t.cp_firmcode, t.address_code, t.cp_address_code, t.buy_sell,
              t.order_type, t.is_address, t.is_mm, t.securityid, t.trade_mode, t.trade_period, t.quote_currency,
              t.currency, t.price, t.repo_price, t.quantity, t.quantity_lot, t.quantity_rest, t.quantity_lot_rest,
              t.volume, t.status, t.status_reason, t.amend_date, t.amend_time, t.issue_date, t.issue_time,
              t.match_ref, t.num_trades, t.sett_type, t.special_period, t.cl_orderid, t.extra_ref, t.commentar,
              t.time_in_force, t.extra_order_no
         FROM TMP_SPB_ORDERS t;
     Return SQL%ROWCOUNT;
   end Insert_ORDERS;

   function Insert_TableSPB (p_FILENAME in varchar2, p_FileType in varchar2, p_IDProcLog in integer) return INTEGER as
  /*Основная функция загрузки полученных отчетов МБ.
    p_FileName - имя принятого файла,
    p_FileType - тип файла.
    Возвращает информацию о результате загрузки и обработки.
  */
    l_IDReq   INTEGER;
    l_IdNForm INTEGER;
    l_Rows    INTEGER;
  begin
    IF p_FileType = 'SPB03' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'СПБ\Формы\SPB03'));
    ELSIF p_FileType = 'SPB21' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'СПБ\Формы\SPB21'));
    ELSIF p_FileType = 'MFB06C' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'СПБ\Формы\MFB06C'));
    ELSIF p_FileType = 'MFB13' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'СПБ\Формы\MFB13'));
    ELSIF p_FileType = 'MFB98' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'СПБ\Формы\MFB98'));
    ELSIF p_FileType = 'MFB99' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'СПБ\Формы\MFB99'));
    ELSIF p_FileType = 'ORDERS' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'СПБ\Формы\ORDERS'));
    END IF;
    l_IDReq := RSP_SPB.Insert_Requisites(p_FILENAME, p_IDProcLog, l_IdNForm);
    IF p_FileType = 'SPB03' THEN
      l_Rows := RSP_SPB.Insert_SPB03(l_IDReq, p_IDProcLog);
      Return l_Rows;
    END IF;
    IF p_FileType = 'SPB21' THEN
      l_Rows := RSP_SPB.Insert_SPB21(l_IDReq, p_IDProcLog);
      Return l_Rows;
    END IF;
    IF p_FileType = 'MFB06C' THEN
      l_Rows := RSP_SPB.Insert_MFB06C(l_IDReq, p_IDProcLog);
      Return l_Rows;
    END IF;
    IF p_FileType = 'MFB13' THEN
      l_Rows := RSP_SPB.Insert_MFB13(l_IDReq, p_IDProcLog);
      Return l_Rows;
    END IF;
    IF p_FileType = 'MFB98' THEN
      l_Rows := RSP_SPB.Insert_MFB98(l_IDReq, p_IDProcLog);
      Return l_Rows;
    END IF;
    IF p_FileType = 'MFB99' THEN
      l_Rows := RSP_SPB.Insert_MFB99(l_IDReq, p_IDProcLog);
      Return l_Rows;
    END IF;
    IF p_FileType = 'ORDERS' THEN
      l_Rows := RSP_SPB.Insert_ORDERS(l_IDReq, p_IDProcLog);
      Return l_Rows;
    END IF;
  end Insert_TableSPB;

  function Get_ParamFile return T_FILEPARAMS as
    l_FileParams T_FILEPARAMS;
  begin
    SELECT to_date(nvl(x.ReportDate, x.TradeDate), 'YYYY-MM-DD'), 
           to_date(x.DOC_DATE, 'YYYY-MM-DD'), to_date('0001-01-01'||' '||x.DOC_TIME, 'YYYY-MM-DD HH24:MI:SS'),
           x.ReportCode, DBMS_CRYPTO.HASH(t.valxml.GetClobVal(), 2)
      INTO l_FileParams.TradeDate, l_FileParams.DOC_DATE, l_FileParams.DOC_TIME, l_FileParams.doc_type_id, l_FileParams.FileHash
      FROM tmp_xml t,
           XMLTABLE('//RTS_DOC' PASSING valxml
                       COLUMNS ReportCode PATH 'REPORT/DOC_INFO/@ReportCode',
                               DOC_DATE   PATH 'DOC_REQUISITES/@DOC_DATE',
                               DOC_TIME   PATH 'DOC_REQUISITES/@DOC_TIME',
                               ReportDate PATH 'REPORT/DOC_INFO/@ReportDate',
                               TradeDate  PATH 'REPORT/DOC_INFO/@TradeDate') x;
    Return l_FileParams;
  end Get_ParamFile;

  function Get_ParamFile_Txt (p_FILENAME IN VARCHAR2) return T_FILEPARAMS as
    l_FileParams T_FILEPARAMS;
  begin
    SELECT to_date(substr(p_FILENAME, 21, 10), 'YYYY-MM-DD'), 
           to_date(substr(p_FILENAME, 21, 10), 'YYYY-MM-DD'), 
           to_date('0001-01-01'||' '||'00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
           substr(p_FILENAME, instr(p_FILENAME, '-') + 1, instr(p_FILENAME, '_') - instr(p_FILENAME, '-') - 1),
           DBMS_CRYPTO.HASH(t.valclob, 2)
      INTO l_FileParams.TradeDate, l_FileParams.DOC_DATE, l_FileParams.DOC_TIME, l_FileParams.doc_type_id, l_FileParams.FileHash
      FROM tmp_xml t;
    Return l_FileParams;
  end Get_ParamFile_Txt;

  function Check_UniqueFile (p_IDProcAct IN INTEGER, p_FileHash IN VARCHAR2, p_FileDate IN DATE) return BOOLEAN as
    l_FileDate DATE;
    l_FileHash VARCHAR2(100);
  begin
    SELECT to_date(r.doc_date||' '||to_char(r.doc_time, 'HH24:MI:SS'), 'DD.MM.YYYY HH24:MI:SS'), p.file_hash
      INTO l_FileDate, l_FileHash
      FROM mb_requisites r,
           processing_log p,
           processing_actual a
      WHERE p.id_processing_log = r.id_processing_log
        AND p.id_processing_log = a.id_processing_log
        AND a.id_processing_actual = p_IDProcAct;
    IF l_FileDate < p_FileDate AND l_FileHash <> p_FileHash THEN
      Return false;
    ELSE
      Return true;
    END IF;
  exception
    WHEN NO_DATA_FOUND THEN
      Return false;
  end Check_UniqueFile;

  function Get_ActualProcessing (p_FileParams IN T_FILEPARAMS) return INTEGER as
    l_IDProcAct INTEGER;
  begin
     SELECT pa.id_processing_actual
       INTO l_IDProcAct
       FROM processing_actual pa,
            processing_log pl
      WHERE pl.id_processing_log = pa.id_processing_log
        AND pa.trade_date  = p_FileParams.TradeDate
        AND pa.ord_num_str = p_FileParams.OrdNumStr
        AND pa.file_type   = p_FileParams.doc_type_id
        AND pl.file_hash   = p_FileParams.FileHash;
    Return l_IDProcAct;
  exception
    WHEN NO_DATA_FOUND THEN
      Return NULL;
  end Get_ActualProcessing;

  function GetRequisitesID(p_IDProcLog IN INTEGER) return INTEGER as
    l_IDRequisites INTEGER;
  begin
    SELECT r.id_mb_requisites
      INTO l_IDRequisites
      FROM mb_requisites r
     WHERE r.id_processing_log = p_IDProcLog;
    Return l_IDRequisites;
  exception
    WHEN NO_DATA_FOUND THEN
      Return NULL;
  end GetRequisitesID;

  function Processing_Log_Insert(p_FileParams IN T_FILEPARAMS, p_FILENAME in varchar2) return INTEGER as
    l_IDProcLog INTEGER;
  begin
    INSERT INTO processing_log(file_type, file_name, file_hash, beg_dt, ord_num, result_num, ord_num_str)
      VALUES(p_FileParams.doc_type_id, p_FileName, p_FileParams.FileHash, sysdate, p_FileParams.OrdNum, -1, p_FileParams.OrdNumStr)
    RETURNING id_processing_log INTO l_IDProcLog;
    Return l_IDProcLog;
  end Processing_Log_Insert;

  procedure Processing_Log_Update(p_IDProcLog IN INTEGER, p_RowsReaded IN INTEGER, p_RowsCreated IN INTEGER, p_RRCreated IN INTEGER, p_ResNum IN INTEGER, p_ResText IN VARCHAR2) as
  begin
    UPDATE processing_log l
       SET end_dt       = sysdate,
           rows_readed  = p_RowsReaded,
           rows_created = p_RowsCreated,
           rr_created   = p_RRCreated,
           result_num   = p_ResNum,
           result_text  = p_ResText
     WHERE id_processing_log = p_IDProcLog;
  end Processing_Log_Update;

  function LogSession_Insert(p_USERNICK IN VARCHAR2, p_FileName IN VARCHAR2) return integer is
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_IDSession    INTEGER;  
  begin
    INSERT INTO logsession(USERNICK, object_name)
      VALUES(p_USERNICK, p_FileName)
    RETURNING id_logsession INTO l_IDSession;
    commit;
    Return l_IDSession;
  end LogSession_Insert;

  procedure LogSession_Update(p_IDSession IN INTEGER, p_ErrCode IN VARCHAR2, p_ErrText IN VARCHAR2) as
    PRAGMA AUTONOMOUS_TRANSACTION;
  begin
    UPDATE logsession l
       SET l.errorcode = p_ErrCode,
           l.errortext = substr(p_ErrText, 1, 4000)
     WHERE l.id_logsession = p_IDSession;
    commit;
  end LogSession_Update;

  procedure LogData_Insert(p_IDSession IN INTEGER, p_Ext_Num IN INTEGER, p_Rows IN INTEGER, p_Info IN VARCHAR2, p_Duration IN INTEGER) as
    PRAGMA AUTONOMOUS_TRANSACTION;
  begin
    INSERT INTO logdata(id_logsession, step_dt, ext_num, countrows, info, duration_ms)
      VALUES(p_IDSession, sysdate, p_Ext_Num, p_Rows, p_Info, p_Duration);
    commit;
  end LogData_Insert;

  function Load_SPB (p_FILENAME in varchar2, p_Content in clob) return INTEGER as
    l_IDSession    INTEGER;  
    l_IDProcAct    INTEGER;
    l_IDProcLog    INTEGER;
    l_RowsReated   INTEGER;
    l_RowsCreated  INTEGER;
    l_Duration     INTEGER := 0;
    l_Systimestamp TIMESTAMP(3) := systimestamp;
    l_Msg          VARCHAR2(255);
    l_FileParams   T_FILEPARAMS;
    e_Error        EXCEPTION;
  begin
    l_IDSession := RSP_SPB.LogSession_Insert(p_USERNICK => SYS_CONTEXT ('RSP_ADM_CTX','USERID'),
                                             p_FileName => p_FileName);
    RSP_SPB.LogData_Insert(l_IDSession, 1, null, 'BEGIN. Начало обработки файла', l_Duration);

    --1. Сохранение файла во временную таблицу
    IF p_FILENAME LIKE '%.txt' THEN
      INSERT INTO tmp_xml(VALCLOB) 
        VALUES(rtrim(p_Content, chr(13)||chr(10)));
    ELSE
      INSERT INTO tmp_xml(VALXML) 
        VALUES(XMLTYPE(p_Content));
    END IF;
  
    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_SPB.LogData_Insert(l_IDSession, 2, 1, 'Успешно загружено содержимое файла', l_Duration);

    --2. Проверяем надо ли обрабатывать файл (нет ли уже успешно загруженного того же типа с тем же хешем)
    IF p_FILENAME LIKE '%.txt' THEN
      l_FileParams := RSP_SPB.Get_ParamFile_Txt(p_FILENAME);
    ELSE
      l_FileParams := RSP_SPB.Get_ParamFile;
    END IF;
    
    l_FileParams.OrdNum := 0;
    l_FileParams.OrdNumStr := '0';

    l_IDProcAct := RSP_SPB.Get_ActualProcessing(l_FileParams);

    IF l_IDProcAct IS NOT NULL AND RSP_SPB.Check_UniqueFile(p_IDProcAct => l_IDProcAct,
                                                            p_FileHash  => l_FileParams.FileHash,
                                                            p_FileDate  => to_date(l_FileParams.DOC_DATE||' '||to_char(l_FileParams.DOC_TIME, 'HH24:MI:SS'),
                                                                                   'DD.MM.YYYY HH24:MI:SS')) THEN
      --инициализация ошибки
      RAISE e_Error;
      Return 1;
    END IF;

    l_IDProcLog := RSP_SPB.Processing_Log_Insert(l_FileParams, p_FILENAME);
    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_SPB.LogData_Insert(l_IDSession, 3, 1, 'Успешно создана запись процессинга', l_Duration);
    l_Systimestamp := systimestamp;

    --3. Загружаем данные из XML во временные таблицы
    l_RowsReated := RSP_SPB.Load_SPBFile(p_FILENAME => p_FILENAME,
                                         p_FileType => l_FileParams.doc_type_id,
                                         P_MSG      => l_Msg);

    IF l_RowsReated = 0 THEN
      --логируем ошибку
      l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
      RSP_SPB.LogData_Insert(l_IDSession, 4, l_RowsReated, l_Msg, l_Duration);
      RSP_SPB.Processing_Log_Update(p_IDProcLog   => l_IDProcLog,
                                    p_RowsReaded  => NULL,
                                    p_RowsCreated => NULL,
                                    p_RRCreated   => NULL,
                                    p_ResNum      => NULL,
                                    p_ResText     => l_Msg);
      l_RowsCreated := RSP_SPB.Insert_TableSPB(p_FILENAME  => p_FILENAME,
                                               p_FileType  => l_FileParams.doc_type_id,
                                               p_IDProcLog => l_IDProcLog);
      Return -1;
    END IF;

    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_SPB.LogData_Insert(l_IDSession, 4, l_RowsReated, 'Успешно загружены данные из файла во временные таблицы', l_Duration);
    l_Systimestamp := systimestamp;

    --4. Переливаем данные в постоянные таблицы
    l_RowsCreated := RSP_SPB.Insert_TableSPB(p_FILENAME  => p_FILENAME,
                                             p_FileType  => l_FileParams.doc_type_id,
                                             p_IDProcLog => l_IDProcLog);
    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_SPB.LogData_Insert(l_IDSession, 5, l_RowsCreated, 'Успешно загружены данные в постоянные таблицы', l_Duration);
    l_Systimestamp := systimestamp;

    --5. Актуализируем данные в системных таблицах
    RSP_SPB.Processing_Log_Update(p_IDProcLog   => l_IDProcLog,
                                  p_RowsReaded  => l_RowsReated,
                                  p_RowsCreated => l_RowsCreated,
                                  p_RRCreated   => NULL,
                                  p_ResNum      => 0,
                                  p_ResText     => l_Msg);

    UPDATE processing_actual
       SET id_processing_log = l_IDProcLog,
           ID_MB_REQUISITES = RSP_SPB.GetRequisitesID(l_IDProcLog)
     WHERE trade_date  = l_FileParams.TradeDate
       AND file_type   = l_FileParams.doc_type_id
       AND ord_num_str = l_FileParams.OrdNumStr;
     
    IF SQL%ROWCOUNT = 0 THEN         -- Если для этого ключа это первая запись
      INSERT INTO processing_actual(trade_date, file_type, ord_num, id_processing_log, id_mb_requisites, ord_num_str)
        VALUES(l_FileParams.TradeDate, l_FileParams.doc_type_id, l_FileParams.OrdNum, l_IDProcLog, RSP_SPB.GetRequisitesID(l_IDProcLog), l_FileParams.OrdNumStr);
    END IF;

    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_SPB.LogData_Insert(l_IDSession, 6, NULL, 'END. Успешно обновили системные таблицы', l_Duration);

    Return 0;
  exception
    WHEN e_Error THEN
      l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
      RSP_SPB.LogData_Insert(l_IDSession, 3, NULL, 'Внимание! Данный файл уже был обработан. Повторная обработка не требуется', l_Duration);
      RSP_SPB.LogSession_Update(p_IDSession => l_IDSession,
                                p_ErrCode   => '-20000',
                                p_ErrText   => 'Внимание! Данный файл уже был обработан. Повторная обработка не требуется');
      Return 1;
    WHEN OTHERS THEN
      l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
      RSP_SPB.LogData_Insert(l_IDSession, -1, NULL, 'END. Обработка файла закончилась с ошибкой', l_Duration);
      RSP_SPB.LogSession_Update(p_IDSession => l_IDSession,
                                p_ErrCode   => SQLCODE,
                                p_ErrText   => SQLERRM);
    RAISE;
  end Load_SPB;
end RSP_SPB;
/
