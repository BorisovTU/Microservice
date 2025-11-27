CREATE OR REPLACE package body RSP_MB as
   function LoadRequisites (p_FILENAME IN VARCHAR2, p_FileType IN VARCHAR2, p_TEXT out varchar2) RETURN INTEGER as
   /*Обработка отчета МБ.
     p_FILENAME - имя файла
     p_FileType - тип файла
     p_Text - информация о результате обработки.
   */
   begin
     IF p_FILENAME LIKE '%.csv' THEN
       INSERT INTO tmp_mb_requisites(doc_type_id, sender_id, sender_name, receiver_id)
         VALUES(p_FileType, const_SENDER_ID, const_SENDER_NAME, const_RECEIVER_ID);
     ELSE
       INSERT INTO tmp_mb_requisites(doc_date, doc_time,
                                     doc_no, doc_type_id, sender_id, sender_name, receiver_id, remarks)
         SELECT to_date(x.DOC_DATE, 'YYYY-MM-DD'), to_date('0001-01-01'||' '||x.DOC_TIME, 'YYYY-MM-DD HH24:MI:SS'),
                x.DOC_NO, nvl(x.DOC_TYPE_ID, p_FileType), x.SENDER_ID, x.SENDER_NAME, x.RECEIVER_ID, x.REMARKS
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
       p_Text := 'ошибка разбора формата МБ';
       Return 0;
     END IF;
     Return 1;
   end LoadRequisites;

   function LoadSEM02 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета SEM02.
     p_Text  - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_mb_sem02(tradedate, tradesessiondate, sessionno, firmid, boardid, boardname, activationdate,
                          recno, orderno, transno, status, ordtype, ordtypecode, buysell, securityid, secshortname, secsetid, secsetshortname,
                          pricetype, currencyid, decimals, price, initialprice,
                          quantity, quantityvisible, value, clearingcenterid, trdaccid,
                          entrytime, acttime,
                          amendtime, settlecode, accint, cpfirmid,
                          cpfirmshortname, cpfirminn, ratetype, benchmark, reporate, repovalue,
                          repoperiod, discount, lowerdiscount,
                          upperdiscount, userid, asp, clientcode, details, subdetails, refundrate,
                          matchref, brokerref, clearingfirmid, isactualmm, liqsource)
       SELECT to_date(x.TradeDate, 'YYYY-MM-DD'), to_date(x.TradeSessionDate, 'YYYY-MM-DD'), y.SessionNo, y.FirmID, z.BoardId, z.BoardName, to_date(v.ActivationDate, 'YYYY-MM-DD'),
              u.recno, u.orderno, u.transno, u.status, u.ordtype, u.ordtypecode, u.buysell, u.securityid, u.secshortname, u.secsetid, u.secsetshortname,
              u.pricetype, u.currencyid, u.decimals, to_number(u.price, '99999999999999999999.9999999'), to_number(u.initialprice, '99999999999999999999.999999'),
              u.quantity, u.quantityvisible, to_number(u.value, '99999999999999999999.99'), u.clearingcenterid, u.trdaccid,
              to_date('0001-01-01'||' '||u.entrytime, 'YYYY-MM-DD HH24:MI:SS'), to_date('0001-01-01'||' '||u.acttime, 'YYYY-MM-DD HH24:MI:SS'),
              to_date('0001-01-01'||' '||u.amendtime, 'YYYY-MM-DD HH24:MI:SS'), u.settlecode, to_number(u.accint, '99999999999999999999.99'), u.cpfirmid,
              u.cpfirmshortname, u.cpfirminn, u.ratetype, u.benchmark, to_number(u.reporate, '99999999999999999999.999999'), to_number(u.repovalue, '99999999999999999999.99'),
              u.repoperiod, to_number(u.discount, '99999999999999999999.999999'), to_number(u.lowerdiscount, '99999999999999999999.999999'),
              to_number(u.upperdiscount, '99999999999999999999.999999'), u.userid, u.asp, u.clientcode, u.details, u.subdetails, to_number(u.refundrate, '99999999999999999999.99'),
              u.matchref, u.brokerref, u.clearingfirmid, u.isactualmm, u.liqsource
         FROM tmp_xml t,
              XMLTABLE('//SEM02' PASSING valxml
                       COLUMNS TradeDate    PATH '@TradeDate',
                               TradeSessionDate PATH '@TradeSessionDate',
                               SESS XMLTYPE PATH '/*') x,
              XMLTABLE('//SESSION' PASSING x.SESS
                       COLUMNS SessionNo     PATH '@SessionNo',
                               FirmID        PATH 'FIRM/@FirmID',
                               BOARD XMLTYPE PATH '/*') y,
              XMLTABLE('//BOARD' PASSING y.BOARD
                       COLUMNS BoardId PATH '@BoardId',
                               BoardName              PATH '@BoardName',
                               ACTIVATIONDATE XMLTYPE PATH '/*') z,
              XMLTABLE('//ACTIVATIONDATE' PASSING z.ACTIVATIONDATE
                       COLUMNS ActivationDate  PATH '@ActivationDate',
                               RECORDS XMLTYPE PATH '/*') v,
              XMLTABLE('//RECORDS' PASSING v.RECORDS
                       COLUMNS RecNo            PATH '@RecNo',
                               OrderNo          PATH '@OrderNo',
                               TransNo          PATH '@TransNo',
                               Status           PATH '@Status',
                               OrdType          PATH '@OrdType',
                               OrdTypeCode      PATH '@OrdTypeCode',
                               BuySell          PATH '@BuySell',
                               SecurityId       PATH '@SecurityId',
                               SecShortName     PATH '@SecShortName',
                               SecSetId         PATH '@SecSetId',
                               SecSetShortName  PATH '@SecSetShortName',
                               PriceType        PATH '@PriceType',
                               CurrencyId       PATH '@CurrencyId',
                               Decimals         PATH '@Decimals',
                               Price            PATH '@Price',
                               InitialPrice     PATH '@InitialPrice',
                               Quantity         PATH '@Quantity',
                               QuantityVisible  PATH '@QuantityVisible',
                               Value            PATH '@Value',
                               ClearingCenterId PATH '@ClearingCenterId',
                               TrdAccId         PATH '@TrdAccId',
                               EntryTime        PATH '@EntryTime',
                               ActTime          PATH '@ActTime',
                               AmendTime        PATH '@AmendTime',
                               SettleCode       PATH '@SettleCode',
                               AccInt           PATH '@AccInt',
                               CPFirmId         PATH '@CPFirmId',
                               CPFirmShortName  PATH '@CPFirmShortName',
                               CPfirmINN        PATH '@CPfirmINN',
                               RateType         PATH '@RateType',
                               Benchmark        PATH '@Benchmark',
                               RepoRate         PATH '@RepoRate',
                               RepoValue        PATH '@RepoValue',
                               RepoPeriod       PATH '@RepoPeriod',
                               Discount         PATH '@Discount',
                               LowerDiscount    PATH '@LowerDiscount',
                               UpperDiscount    PATH '@UpperDiscount',
                               UserId           PATH '@UserId',
                               ASP              PATH '@ASP',
                               ClientCode       PATH '@ClientCode',
                               Details          PATH '@Details',
                               SubDetails       PATH '@SubDetails',
                               RefundRate       PATH '@RefundRate',
                               MatchRef         PATH '@MatchRef',
                               BrokerRef        PATH '@BrokerRef',
                               ClearingFirmID   PATH '@ClearingFirmID',
                               IsActualMM       PATH '@IsActualMM',
                               LiqSource        PATH '@LiqSource') u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета МБ.';
       Return 0;
     ELSE
       Return SQL%ROWCOUNT;
     END IF;
   end LoadSEM02;

   function LoadSEM03 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета SEM03.
     p_Text  - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_mb_sem03(tradedate, tradesessiondate, sessionno, firmid, currencyid, boardid, boardname, settledate, securityid, secshortname, secname, securitytype,
                              initialfacevalue, facevalue, seccurrencyid, pricetype, trdaccid, clearingcenterid,
                              recno, secsetid, secsetshortname, tradeno,
                              tradetime, buysell, settlecode, decimals, price, quantity, value, amount, exchcomm, faceamount, orderno,
                              accint, cpfirmid, cpfirmshortname, cpfirminn, cptrdaccid, repovalue, repoperiod, ratetype, benchmark, reporate,
                              outstandingreturnvalue, discount, lowerdiscount, upperdiscount, tradetype, cancelorder, iscancel, userid, yield, period, extref,
                              price2, accint2, clientcode, details, subdetails, refundrate, matchref, brokerref, systemref, clearingfirmid, ishidden, isactualmm,
                              liqsource)
       SELECT to_date(x.TradeDate, 'YYYY-MM-DD'), to_date(x.TradeSessionDate, 'YYYY-MM-DD'), y.SessionNo, w.FirmID, n.CurrencyId, z.BoardId, z.BoardName, to_date(s.SettleDate, 'YYYY-MM-DD'),
              v.SecurityId, v.SecShortName, v.SecName, v.SecurityType, to_number(v.InitialFaceValue, '99999999999999999999.999999'),
              to_number(v.FaceValue, '99999999999999999999.999999'), v.SecCurrencyId, v.PriceType, r.TrdAccId, r.ClearingCenterId,
              u.RecNo, u.SecSetId, u.SecSetShortName, u.TradeNo, to_date('0001-01-01'||' '||u.TradeTime, 'YYYY-MM-DD HH24:MI:SS'), u.buysell, u.SettleCode,
              u.decimals, to_number(u.price, '99999999999999999999.9999999'), u.quantity, to_number(u.value, '99999999999999999999.99'),
              to_number(u.Amount, '99999999999999999999.99'), to_number(u.ExchComm, '99999999999999999999.99'), to_number(u.FaceAmount, '99999999999999999999.99'),
              u.OrderNo, to_number(u.accint, '99999999999999999999.99'), u.cpfirmid, u.cpfirmshortname, u.cpfirminn, u.CPTrdAccId,
              to_number(u.RepoValue, '99999999999999999999.99'), u.RepoPeriod, u.RateType, u.benchmark, to_number(u.RepoRate, '99999999999999999999.999999'),
              to_number(u.OutStandingReturnValue, '99999999999999999999.99'), to_number(u.discount, '99999999999999999999.999999'),
              to_number(u.lowerdiscount, '99999999999999999999.999999'), to_number(u.upperdiscount, '99999999999999999999.999999'), u.TradeType, u.CancelOrder,
              u.IsCancel, u.UserId, to_number(u.Yield, '99999999999999999999.99'), u.Period, u.ExtRef, to_number(u.Price2, '99999999999999999999.999999'),
              to_number(u.AccInt2, '99999999999999999999.99'), u.ClientCode, u.Details, u.SubDetails, to_number(u.RefundRate, '99999999999999999999.99'),
              u.MatchRef, u.BrokerRef, u.SystemRef, u.ClearingFirmID, u.IsHidden, u.IsActualMM, u.LiqSource
         FROM tmp_xml t,
              XMLTABLE('//SEM03' PASSING valxml
                       COLUMNS TradeDate    PATH '@TradeDate',
                               TradeSessionDate PATH '@TradeSessionDate',
                               SESS XMLTYPE PATH '/*') x,
              XMLTABLE('//SESSION' PASSING x.SESS
                       COLUMNS SessionNo     PATH '@SessionNo',
                               FIRM  XMLTYPE PATH '/*')(+) y,
              XMLTABLE('//FIRM' PASSING y.FIRM
                       COLUMNS FirmID           PATH '@FirmID',
                               CURRENCY XMLTYPE PATH '/*')(+) w,
              XMLTABLE('//CURRENCY' PASSING w.CURRENCY
                       COLUMNS CurrencyId    PATH '@CurrencyId',
                               BOARD XMLTYPE PATH '/*')(+) n,
              XMLTABLE('//BOARD' PASSING n.BOARD
                       COLUMNS BoardId            PATH '@BoardId',
                               BoardName          PATH '@BoardName',
                               SETTLEDATE XMLTYPE PATH '/*')(+) z,
              XMLTABLE('//SETTLEDATE' PASSING z.SETTLEDATE
                       COLUMNS SettleDate  PATH '@SettleDate',
                               SECURITY XMLTYPE PATH '/*')(+) s,
              XMLTABLE('//SECURITY' PASSING s.SECURITY
                       COLUMNS SecurityId       PATH '@SecurityId',
                               SecShortName     PATH '@SecShortName',
                               SecName          PATH '@SecName',
                               SecurityType     PATH '@SecurityType',
                               InitialFaceValue PATH '@InitialFaceValue',
                               FaceValue        PATH '@FaceValue',
                               SecCurrencyId    PATH '@SecCurrencyId',
                               PriceType        PATH '@PriceType',
                               TRDACC  XMLTYPE PATH '/*')(+) v,
              XMLTABLE('//TRDACC' PASSING v.TRDACC
                       COLUMNS TrdAccId         PATH '@TrdAccId',
                               ClearingCenterId PATH '@ClearingCenterId',
                               RECORDS  XMLTYPE PATH '/*')(+) r,
              XMLTABLE('//RECORDS' PASSING r.RECORDS
                       COLUMNS RecNo                  PATH '@RecNo',
                               SecSetId               PATH '@SecSetId',
                               SecSetShortName        PATH '@SecSetShortName',
                               TradeNo                PATH '@TradeNo',
                               TradeTime              PATH '@TradeTime',
                               BuySell                PATH '@BuySell',
                               SettleCode             PATH '@SettleCode',
                               Decimals               PATH '@Decimals',
                               Price                  PATH '@Price',
                               Quantity               PATH '@Quantity',
                               Value                  PATH '@Value',
                               Amount                 PATH '@Amount',
                               ExchComm               PATH '@ExchComm',
                               FaceAmount             PATH '@FaceAmount',
                               OrderNo                PATH '@OrderNo',
                               AccInt                 PATH '@AccInt',
                               CPFirmId               PATH '@CPFirmId',
                               CPFirmShortName        PATH '@CPFirmShortName',
                               CPfirmINN              PATH '@CPfirmINN',
                               CPTrdAccId             PATH '@CPTrdAccId',
                               RepoValue              PATH '@RepoValue',
                               RepoPeriod             PATH '@RepoPeriod',
                               RateType               PATH '@RateType',
                               Benchmark              PATH '@Benchmark',
                               RepoRate               PATH '@RepoRate',
                               OutStandingReturnValue PATH '@OutStandingReturnValue',
                               Discount               PATH '@Discount',
                               LowerDiscount          PATH '@LowerDiscount',
                               UpperDiscount          PATH '@UpperDiscount',
                               TradeType              PATH '@TradeType',
                               CancelOrder            PATH '@CancelOrder',
                               IsCancel               PATH '@IsCancel',
                               UserId                 PATH '@UserId',
                               Yield                  PATH '@Yield',
                               Period                 PATH '@Period',
                               ExtRef                 PATH '@ExtRef',
                               Price2                 PATH '@Price2',
                               AccInt2                PATH '@AccInt2',
                               ClientCode             PATH '@ClientCode',
                               Details                PATH '@Details',
                               SubDetails             PATH '@SubDetails',
                               RefundRate             PATH '@RefundRate',
                               MatchRef               PATH '@MatchRef',
                               BrokerRef              PATH '@BrokerRef',
                               SystemRef              PATH '@SystemRef',
                               ClearingFirmID         PATH '@ClearingFirmID',
                               IsHidden               PATH '@IsHidden',
                               IsActualMM             PATH '@IsActualMM',
                               LiqSource              PATH '@LiqSource')(+) u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета МБ.';
       Return 0;
     ELSE
       Return SQL%ROWCOUNT;
     END IF;
   end LoadSEM03;

   function LoadSEM21 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета SEM21.
     p_Text  - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_mb_sem21(tradedate, tradesessiondate, sessionno, boardid, boardname, engboardname, boardtype, securityid, secshortname, securitytype,
                              pricetype, engname, engtype, regnumber, decimals, facevalue, faceunit,
                              volume, value, currencyid,
                              openperiod, open,
                              low, high,
                              close, lastprice,
                              lowoffer, highbid,
                              waprice, closeauction,
                              closeperiod, trendclose,
                              trendwap, bid,
                              offer, prev,
                              yieldatwap, yieldclose,
                              accint, accintface,
                              marketprice, numtrades, issuesize,
                              trendclspr, trendwappr,
                              matdate, marketprice2,
                              marketprice3, marketprice3cur,
                              admittedquote, listname, prevlegalcloseprice,
                              legalopenprice, legalcloseprice,
                              openval, closeval, duration,
                              mpvaltrd, mp2valtrd,
                              mp3valtrd, mp3valtrdcur)
       SELECT to_date(x.tradedate, 'YYYY-MM-DD'), to_date(x.tradesessiondate, 'YYYY-MM-DD'), w.sessionno, n.boardid, n.boardname, n.engboardname, n.boardtype, u.securityid, u.secshortname, u.securitytype,
              u.pricetype, u.engname, u.engtype, u.regnumber, u.decimals, to_number(u.facevalue, '99999999999999999999.99999999'), u.faceunit,
              to_number(u.volume, '99999999999999999999.'), to_number(u.value, '99999999999999999999.99999999'), u.currencyid,
              to_number(u.openperiod, '99999999999999999999.99999999'), to_number(u.open, '99999999999999999999.99999999'),
              to_number(u.low, '99999999999999999999.99999999'), to_number(u.high, '99999999999999999999.99999999'),
              to_number(u.close, '99999999999999999999.99999999'), to_number(u.lastprice, '99999999999999999999.99999999'),
              to_number(u.lowoffer, '99999999999999999999.99999999'), to_number(u.highbid, '99999999999999999999.99999999'),
              to_number(u.waprice, '99999999999999999999.99999999'), to_number(u.closeauction, '99999999999999999999.99999999'),
              to_number(u.closeperiod, '99999999999999999999.99999999'), to_number(u.trendclose, '99999999999999999999.99999999'),
              to_number(u.trendwap, '99999999999999999999.99999999'), to_number(u.bid, '99999999999999999999.99999999'),
              to_number(u.offer, '99999999999999999999.99999999'), to_number(u.prev, '99999999999999999999.99999999'),
              to_number(u.yieldatwap, '99999999999999999999.99999999'), to_number(u.yieldclose, '99999999999999999999.99999999'),
              to_number(u.accint, '99999999999999999999.99999999'), to_number(u.accintface, '99999999999999999999.99999999'),
              to_number(u.marketprice, '99999999999999999999.99999999'), u.numtrades, to_number(u.issuesize, '99999999999999999999.'),
              to_number(u.trendclspr, '99999999999999999999.99999999'), to_number(u.trendwappr, '99999999999999999999.99999999'),
              to_date(u.matdate, 'YYYY-MM-DD'), to_number(u.marketprice2, '99999999999999999999.99999999'),
              to_number(u.marketprice3, '99999999999999999999.99999999'), to_number(u.marketprice3cur, '99999999999999999999.99999999'),
              to_number(u.admittedquote, '99999999999999999999.99999999'), u.listname, to_number(u.prevlegalcloseprice, '99999999999999999999.99999999'),
              to_number(u.legalopenprice, '99999999999999999999.99999999'), to_number(u.legalcloseprice, '99999999999999999999.99999999'),
              to_number(u.openval, '99999999999999999999.99999999'), to_number(u.closeval, '99999999999999999999.99999999'), u.duration,
              to_number(u.mpvaltrd, '99999999999999999999.99999999'), to_number(u.mp2valtrd, '99999999999999999999.99999999'),
              to_number(u.mp3valtrd, '99999999999999999999.99999999'), to_number(u.mp3valtrdcur, '99999999999999999999.99999999')
         FROM tmp_xml t,
              XMLTABLE('//SEM21' PASSING valxml
                       COLUMNS TradeDate      PATH '@TradeDate',
                               TradeSessionDate PATH '@TradeSessionDate',
                               SESION XMLTYPE PATH '/*') x,
              XMLTABLE('//SESSION' PASSING x.SESION
                       COLUMNS SessionNo     PATH '@SessionNo',
                               BOARD XMLTYPE PATH '/*') w,
              XMLTABLE('//BOARD' PASSING w.BOARD
                       COLUMNS BoardId         PATH '@BoardId',
                               BoardName       PATH '@BoardName',
                               EngBoardName    PATH '@EngBoardName',
                               BoardType       PATH '@BoardType',
                               RECORDS XMLTYPE PATH '/*') n,
              XMLTABLE('//RECORDS' PASSING n.RECORDS
                       COLUMNS SecurityId          PATH '@SecurityId',
                               SecShortName        PATH '@SecShortName',
                               SecurityType        PATH '@SecurityType',
                               PriceType           PATH '@PriceType',
                               EngName             PATH '@EngName',
                               EngType             PATH '@EngType',
                               RegNumber           PATH '@RegNumber',
                               Decimals            PATH '@Decimals',
                               FaceValue           PATH '@FaceValue',
                               FaceUnit            PATH '@FaceUnit',
                               Volume              PATH '@Volume',
                               Value               PATH '@Value',
                               CurrencyId          PATH '@CurrencyId',
                               OpenPeriod          PATH '@OpenPeriod',
                               Open                PATH '@Open',
                               Low                 PATH '@Low',
                               High                PATH '@High',
                               Close               PATH '@Close',
                               LastPrice           PATH '@LastPrice',
                               LowOffer            PATH '@LowOffer',
                               HighBid             PATH '@HighBid',
                               WAPrice             PATH '@WAPrice',
                               CloseAuction        PATH '@CloseAuction',
                               ClosePeriod         PATH '@ClosePeriod',
                               TrendClose          PATH '@TrendClose',
                               TrendWAP            PATH '@TrendWAP',
                               Bid                 PATH '@Bid',
                               Offer               PATH '@Offer',
                               Prev                PATH '@Prev',
                               YieldAtWAP          PATH '@YieldAtWAP',
                               YieldClose          PATH '@YieldClose',
                               AccInt              PATH '@AccInt',
                               AccIntFace          PATH '@AccIntFace',
                               MarketPrice         PATH '@MarketPrice',
                               NumTrades           PATH '@NumTrades',
                               IssueSize           PATH '@IssueSize',
                               TrendClsPr          PATH '@TrendClsPr',
                               TrendWapPr          PATH '@TrendWapPr',
                               MatDate             PATH '@MatDate',
                               MarketPrice2        PATH '@MarketPrice2',
                               MarketPrice3        PATH '@MarketPrice3',
                               MarketPrice3Cur     PATH '@MarketPrice3Cur',
                               AdmittedQuote       PATH '@AdmittedQuote ',
                               ListName            PATH '@ListName',
                               PrevLegalClosePrice PATH '@PrevLegalClosePrice',
                               LegalOpenPrice      PATH '@LegalOpenPrice',
                               LegalClosePrice     PATH '@LegalClosePrice',
                               OpenVal             PATH '@OpenVal',
                               CloseVal            PATH '@CloseVal',
                               Duration            PATH '@Duration',
                               MPValTrd            PATH '@MPValTrd',
                               MP2ValTrd           PATH '@MP2ValTrd',
                               MP3ValTrd           PATH '@MP3ValTrd',
                               MP3ValTrdCur        PATH '@MP3ValTrdCur') u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета МБ.';
       Return 0;
     ELSE
       Return SQL%ROWCOUNT;
     END IF;
   end LoadSEM21;

   function LoadSEM25 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета SEM25.
     p_Text  - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_mb_sem25(reportdate, firmid, currencyid, currencyname, securityid, secname,
                              recno, repotradeno, repotradedate, settledate, direction, decimals,
                              discount, lowerdiscount,
                              upperdiscount, outstandingdeficit,
                              repothreshold, value, quantity,
                              outstandingdiscount, outstandingrepovalue,
                              outstandingquantity, outstandingprice2,
                              outstandingreturnvalue, trdaccid, clientdetails, cpfirmid, cpfirmshortname)
       SELECT to_date(x.ReportDate, 'YYYY-MM-DD'), w.FirmID, n.CurrencyId, n.CurrencyName, z.SecurityId, z.SecName,
              u.RecNo, u.RepoTradeNo, to_date(u.RepoTradeDate, 'YYYY-MM-DD'), to_date(u.SettleDate, 'YYYY-MM-DD'), u.Direction, u.decimals,
              to_number(u.Discount, '99999999999999999999.999999'), to_number(u.LowerDiscount, '99999999999999999999.999999'),
              to_number(u.UpperDiscount, '99999999999999999999.999999'), to_number(u.OutStandingDeficit, '99999999999999999999.99'),
              to_number(u.RepoThresHold, '99999999999999999999.99'), to_number(u.Value, '99999999999999999999.99'), u.Quantity,
              to_number(u.OutStandingDiscount, '99999999999999999999.999999'), to_number(u.OutStandingRepoValue, '99999999999999999999.99'),
              u.OutStandingQuantity, to_number(u.OutStandingPrice2, '99999999999999999999.999999'),
              to_number(u.OutStandingReturnValue, '99999999999999999999.99'), u.TrdAccId, u.ClientDetails, u.CPFirmId, u.CPFirmShortName
         FROM tmp_xml t,
              XMLTABLE('//SEM25' PASSING valxml
                       COLUMNS ReportDate   PATH '@ReportDate',
                               FIRM XMLTYPE PATH '/*') x,
              XMLTABLE('//FIRM' PASSING x.FIRM
                       COLUMNS FirmID           PATH '@FirmID',
                               CURRENCY XMLTYPE PATH '/*') w,
              XMLTABLE('//CURRENCY' PASSING w.CURRENCY
                       COLUMNS CurrencyId       PATH '@CurrencyId',
                               CurrencyName     PATH '@CurrencyName',
                               SECURITY XMLTYPE PATH '/*') n,
              XMLTABLE('//SECURITY' PASSING n.SECURITY
                       COLUMNS SecurityId      PATH '@SecurityId',
                               SecName         PATH '@SecName',
                               RECORDS XMLTYPE PATH '/*') z,
              XMLTABLE('//RECORDS' PASSING z.RECORDS
                       COLUMNS RecNo                  PATH '@RecNo',
                               RepoTradeNo            PATH '@RepoTradeNo',
                               RepoTradeDate          PATH '@RepoTradeDate',
                               SettleDate             PATH '@SettleDate',
                               Direction              PATH '@Direction',
                               Decimals               PATH '@Decimals',
                               Discount               PATH '@Discount',
                               LowerDiscount          PATH '@LowerDiscount',
                               UpperDiscount          PATH '@UpperDiscount',
                               OutStandingDeficit     PATH '@OutStandingDeficit',
                               RepoThresHold          PATH '@RepoThresHold',
                               Value                  PATH '@Value',
                               Quantity               PATH '@Quantity',
                               OutStandingDiscount    PATH '@OutStandingDiscount',
                               OutStandingRepoValue   PATH '@OutStandingRepoValue',
                               OutStandingQuantity    PATH '@OutStandingQuantity',
                               OutStandingPrice2      PATH '@OutStandingPrice2',
                               OutStandingReturnValue PATH '@OutStandingReturnValue',
                               TrdAccId               PATH '@TrdAccId',
                               ClientDetails          PATH '@ClientDetails',
                               CPFirmId               PATH '@CPFirmId',
                               CPFirmShortName        PATH '@CPFirmShortName') u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета МБ.';
       Return 0;
     ELSE
       Return SQL%ROWCOUNT;
     END IF;
   end LoadSEM25;

   function LoadSEM26 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета SEM26.
     p_Text  - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_mb_sem26(reportdate, firmid, currencyid, currencyname, securityid, recno, repotradeno,
                              repotradedate, repovaluedate,  principalvalue,
                              couponvalue, repovalue,
                              returnvalue, outstandingrepovalue,
                              outstandingreturnvalue, mcvalue,
                              outstandingrepovalue2, outstandingreturnvalue2,
                              paymentvalue, paymentcurrency, clientdetails)
       SELECT to_date(x.ReportDate, 'YYYY-MM-DD'), w.FirmID, n.CurrencyId, n.CurrencyName, z.SecurityId, u.RecNo, u.RepoTradeNo,
              to_date(u.RepoTradeDate, 'YYYY-MM-DD'), to_date(u.RepoValueDate, 'YYYY-MM-DD'), to_number(u.PrincipalValue, '99999999999999999999.99999999'),
              to_number(u.CouponValue, '99999999999999999999.99999999'), to_number(u.RepoValue, '99999999999999999999.99999999'),
              to_number(u.ReturnValue, '99999999999999999999.99999999'), to_number(u.OutStandingRepoValue, '99999999999999999999.99999999'),
              to_number(u.OutStandingReturnValue, '99999999999999999999.99999999'), to_number(u.MCValue, '99999999999999999999.99999999'),
              to_number(u.OutStandingRepoValue2, '99999999999999999999.99999999'), to_number(u.OutStandingReturnValue2, '99999999999999999999.99999999'),
              to_number(u.PaymentValue, '99999999999999999999.99999999'), u.PaymentCurrency, u.ClientDetails
         FROM tmp_xml t,
              XMLTABLE('//SEM26' PASSING valxml
                       COLUMNS ReportDate   PATH '@ReportDate',
                               FIRM XMLTYPE PATH '/*') x,
              XMLTABLE('//FIRM' PASSING x.FIRM
                       COLUMNS FirmID           PATH '@FirmID',
                               CURRENCY XMLTYPE PATH '/*') w,
              XMLTABLE('//CURRENCY' PASSING w.CURRENCY
                       COLUMNS CurrencyId       PATH '@CurrencyId',
                               CurrencyName     PATH '@CurrencyName',
                               SECURITY XMLTYPE PATH '/*') n,
              XMLTABLE('//SECURITY' PASSING n.SECURITY
                       COLUMNS SecurityId      PATH '@SecurityId',
                               RECORDS XMLTYPE PATH '/*') z,
              XMLTABLE('//RECORDS' PASSING z.RECORDS
                       COLUMNS RecNo                   PATH '@RecNo',
                               RepoTradeNo             PATH '@RepoTradeNo',
                               RepoTradeDate           PATH '@RepoTradeDate',
                               RepoValueDate           PATH '@RepoValueDate',
                               PrincipalValue          PATH '@PrincipalValue',
                               CouponValue             PATH '@CouponValue',
                               RepoValue               PATH '@RepoValue',
                               ReturnValue             PATH '@ReturnValue',
                               OutStandingRepoValue    PATH '@OutStandingRepoValue',
                               OutStandingReturnValue  PATH '@OutStandingReturnValue',
                               MCValue                 PATH '@MCValue',
                               OutStandingRepoValue2   PATH '@OutStandingRepoValue2',
                               OutStandingReturnValue2 PATH '@OutStandingReturnValue2',
                               PaymentValue            PATH '@PaymentValue',
                               PaymentCurrency         PATH '@PaymentCurrency',
                               ClientDetails           PATH '@ClientDetails') u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета МБ.';
       Return 0;
     ELSE
       Return SQL%ROWCOUNT;
     END IF;
   end LoadSEM26;

   function LoadEQM06 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета EQM06.
     p_Text  - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_mb_eqm06(reportdate, firmid, extsettlecode, currencyid, inftype, clearingtype, sessionnum,
                              settledate, instrtype, boardid, boardname, securityid, isin, secshortname, pricetype, recno, tradeno,
                              tradedate, tradesessiondate, tradetime, buysell, settlecode, decimals,
                              price, quantity, value,
                              faceamount, deporate,
                              accint, amount, balance,
                              sum1, sum2, exchcomm,
                              clrcomm, trdaccid, clientdetails, cpfirmid, cpfirmshortname,
                              price2, payoff, repopart, repoperiod, reportno,
                              reporttime, settletime, clientcode,
                              duedate, type, systemref, earlysettlestatus, trademergeno, reporate,
                              ratetype, benchmark, benchmarkrate, curreporate,
                              reposum, interestamount)
       SELECT to_date(x.reportdate, 'YYYY-MM-DD'), y.firmid, z.extsettlecode, v.currencyid, w.inftype, m.clearingtype, s.sessionnum,
              to_date(r.settledate, 'YYYY-MM-DD'), p.instrtype, o.boardid, o.boardname, n.securityid, n.isin, n.secshortname, n.pricetype, u.recno, u.tradeno,
              to_date(u.tradedate, 'YYYY-MM-DD'), to_date(u.tradesessiondate, 'YYYY-MM-DD'), to_date('0001-01-01'||' '||u.tradetime, 'YYYY-MM-DD HH24:MI:SS'), u.buysell, u.settlecode, u.decimals,
              to_number(u.price, '99999999999999999999.9999999'), u.quantity, to_number(u.value, '99999999999999999999.99'),
              to_number(u.faceamount, '99999999999999999999.99'), to_number(u.deporate, '9999999999999999999.9999999'),
              to_number(u.accint, '99999999999999999999.99'), to_number(u.amount, '99999999999999999999.99'), u.balance,
              to_number(u.sum1, '99999999999999999999.99'), to_number(u.sum2, '99999999999999999999.99'), to_number(u.exchcomm, '99999999999999999999.99'),
              to_number(u.clrcomm, '99999999999999999999.99'), u.trdaccid, u.clientdetails, u.cpfirmid, u.cpfirmshortname,
              to_number(u.price2, '99999999999999999999.9999999'), to_number(u.payoff, '99999999999999999999.999999'), u.repopart, u.repoperiod, u.reportno,
              to_date('0001-01-01'||' '||u.reporttime, 'YYYY-MM-DD HH24:MI:SS'), to_date('0001-01-01'||' '||u.settletime, 'YYYY-MM-DD HH24:MI:SS'), u.clientcode,
              to_date(u.duedate, 'YYYY-MM-DD'), u.type, u.systemref, u.earlysettlestatus, u.trademergeno, to_number(u.reporate, '99999999999999999999.999999'),
              u.ratetype, u.benchmark, to_number(u.benchmarkrate, '99999999999999999999.999999'), to_number(u.curreporate, '99999999999999999999.999999'),
              to_number(u.reposum, '99999999999999999999.99'), to_number(u.interestamount, '99999999999999999999.99')
         FROM tmp_xml t,
              XMLTABLE('//EQM06' PASSING valxml
                       COLUMNS ReportDate   PATH '@ReportDate',
                               FIRM XMLTYPE PATH '/*') x,
              XMLTABLE('//FIRM' PASSING x.FIRM
                       COLUMNS FirmID         PATH '@FirmID',
                               SETTLE XMLTYPE PATH '/*')(+) y,
              XMLTABLE('//SETTLE' PASSING y.SETTLE
                       COLUMNS ExtSettleCode    PATH '@ExtSettleCode',
                               CURRENCY XMLTYPE PATH '/*')(+) z,
              XMLTABLE('//CURRENCY' PASSING z.CURRENCY
                       COLUMNS CurrencyId      PATH '@CurrencyId',
                               INFTYPE XMLTYPE PATH '/*')(+) v,
              XMLTABLE('//INFTYPE' PASSING v.INFTYPE
                       COLUMNS InfType              PATH '@InfType',
                               CLEARINGTYPE XMLTYPE PATH '/*')(+) w,
              XMLTABLE('//CLEARINGTYPE' PASSING w.CLEARINGTYPE
                       COLUMNS ClearingType   PATH '@ClearingType',
                               SESION XMLTYPE PATH '/*')(+) m,
              XMLTABLE('//SESSION' PASSING m.SESION
                       COLUMNS SessionNum         PATH '@Session',
                               SETTLEDATE XMLTYPE PATH '/*')(+) s,
              XMLTABLE('//SETTLEDATE' PASSING s.SETTLEDATE
                       COLUMNS SettleDate         PATH '@SettleDate',
                               INSTRTRADE XMLTYPE PATH '/*')(+) r,
              XMLTABLE('//INSTRTRADE' PASSING r.INSTRTRADE
                       COLUMNS InstrType    PATH '@InstrType',
                               BOARD XMLTYPE PATH '/*')(+) p,
              XMLTABLE('//BOARD' PASSING p.BOARD
                       COLUMNS BoardId          PATH '@BoardId',
                               BoardName        PATH '@BoardName',
                               SECURITY XMLTYPE PATH '/*') o,
              XMLTABLE('//SECURITY' PASSING o.SECURITY
                       COLUMNS SecurityId      PATH '@SecurityId',
                               ISIN            PATH '@ISIN',
                               SecShortName    PATH '@SecShortName',
                               PriceType       PATH '@PriceType',
                               RECORDS XMLTYPE PATH '/*') n,
              XMLTABLE('//RECORDS' PASSING n.RECORDS
                       COLUMNS RecNo             PATH '@RecNo',
                               TradeNo           PATH '@TradeNo',
                               TradeDate         PATH '@TradeDate',
                               TradeSessionDate  PATH '@TradeSessionDate',
                               TradeTime         PATH '@TradeTime',
                               BuySell           PATH '@BuySell',
                               SettleCode        PATH '@SettleCode',
                               Decimals          PATH '@Decimals',
                               Price             PATH '@Price',
                               Quantity          PATH '@Quantity',
                               Value             PATH '@Value',
                               FaceAmount        PATH '@FaceAmount',
                               DepoRate          PATH '@DepoRate',
                               AccInt            PATH '@AccInt',
                               Amount            PATH '@Amount',
                               Balance           PATH '@Balance',
                               Sum1              PATH '@Sum1',
                               Sum2              PATH '@Sum2',
                               ExchComm          PATH '@ExchComm',
                               ClrComm           PATH '@ClrComm',
                               TrdAccId          PATH '@TrdAccId',
                               ClientDetails     PATH '@ClientDetails',
                               CPFirmId          PATH '@CPFirmId',
                               CPFirmShortName   PATH '@CPFirmShortName',
                               Price2            PATH '@Price2',
                               Payoff            PATH '@Payoff',
                               RepoPart          PATH '@RepoPart',
                               RepoPeriod        PATH '@RepoPeriod',
                               ReportNo          PATH '@ReportNo',
                               ReportTime        PATH '@ReportTime',
                               SettleTime        PATH '@SettleTime',
                               ClientCode        PATH '@ClientCode',
                               DueDate           PATH '@DueDate',
                               Type              PATH '@Type',
                               SystemRef         PATH '@SystemRef',
                               EarlySettleStatus PATH '@EarlySettleStatus',
                               TradeMergeNo      PATH '@TradeMergeNo',
                               RepoRate          PATH '@RepoRate',
                               RateType          PATH '@RateType',
                               Benchmark         PATH '@Benchmark',
                               BenchmarkRate     PATH '@BenchmarkRate',
                               CurRepoRate       PATH '@CurRepoRate',
                               RepoSum           PATH '@RepoSum',
                               InterestAmount    PATH '@InterestAmount') u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета МБ.';
       Return 0;
     ELSE
       Return SQL%ROWCOUNT;
     END IF;
   end LoadEQM06;

   function LoadEQM6C (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета EQM6C.
     p_Text  - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_mb_eqm6c(reportdate, firmid, clientcode, clientdetails, currencyid, currencyname, inftype, clearingtype, sessionid,
                              settledate, instrtype, boardid, boardname, securityid, isin, secshortname, pricetype, recno, tradeno,
                              tradedate, tradesessiondate, tradetime, buysell, settlecode, decimals,
                              price, quantity,
                              value, faceamount,
                              deporate, accint,
                              amount, balance, sum1,
                              sum2, exchcomm,
                              clrcomm, trdaccid, cpfirmid, cpfirmshortname,
                              price2, payoff, repopart, repoperiod, reportno,
                              reporttime, settletime,
                              duedate, type, systemref, earlysettlestatus, trademergeno, reporate,
                              ratetype, benchmark, benchmarkrate, curreporate,
                              reposum)
       SELECT to_date(x.reportdate, 'YYYY-MM-DD'), y.firmid, z.clientcode, z.clientdetails, v.currencyid, v.CurrencyName, w.inftype, m.clearingtype, s.sessionid,
              to_date(r.settledate, 'YYYY-MM-DD'), p.instrtype, o.boardid, o.boardname, n.securityid, n.isin, n.secshortname, n.pricetype, u.recno, u.tradeno,
              to_date(u.tradedate, 'YYYY-MM-DD'), to_date(u.tradesessiondate, 'YYYY-MM-DD'), to_date('0001-01-01'||' '||u.tradetime, 'YYYY-MM-DD HH24:MI:SS'), u.buysell, u.settlecode, u.decimals,
              to_number(u.price, '99999999999999999999.99999999'), to_number(u.quantity, '99999999999999999999.'),
              to_number(u.value, '99999999999999999999.99999999'), to_number(u.faceamount, '99999999999999999999.99999999'),
              to_number(u.deporate, '9999999999999999999.99999999'), to_number(u.accint, '99999999999999999999.99999999'),
              to_number(u.amount, '99999999999999999999.99999999'), u.balance, to_number(u.sum1, '99999999999999999999.99999999'),
              to_number(u.sum2, '99999999999999999999.99999999'), to_number(u.exchcomm, '99999999999999999999.99999999'),
              to_number(u.clrcomm, '99999999999999999999.99999999'), u.trdaccid, u.cpfirmid, u.cpfirmshortname,
              to_number(u.price2, '99999999999999999999.99999999'), to_number(u.payoff, '99999999999999999999.99999999'), u.repopart, u.repoperiod, u.reportno,
              to_date('0001-01-01'||' '||u.reporttime, 'YYYY-MM-DD HH24:MI:SS'), to_date('0001-01-01'||' '||u.settletime, 'YYYY-MM-DD HH24:MI:SS'),
              to_date(u.duedate, 'YYYY-MM-DD'), u.type, u.systemref, u.earlysettlestatus, u.trademergeno, to_number(u.reporate, '99999999999999999999.99999999'),
              u.ratetype, u.benchmark, to_number(u.benchmarkrate, '99999999999999999999.99999999'), to_number(u.curreporate, '99999999999999999999.99999999'),
              to_number(u.reposum, '99999999999999999999.99999999')
         FROM tmp_xml t,
              XMLTABLE('//EQM6C' PASSING valxml
                       COLUMNS ReportDate   PATH '@ReportDate',
                               FIRM XMLTYPE PATH '/*') x,
              XMLTABLE('//FIRM' PASSING x.FIRM
                       COLUMNS FirmID         PATH '@FirmID',
                               CLIENT XMLTYPE PATH '/*')(+) y,
              XMLTABLE('//CLIENT' PASSING y.CLIENT
                       COLUMNS ClientCode    PATH '@ClientCode',
                               ClientDetails PATH '@ClientDetails',
                               CURRENCY XMLTYPE PATH '/*')(+) z,
              XMLTABLE('//CURRENCY' PASSING z.CURRENCY
                       COLUMNS CurrencyId      PATH '@CurrencyId',
                               CurrencyName    PATH '@CurrencyName',
                               INFTYPE XMLTYPE PATH '/*')(+) v,
              XMLTABLE('//INFTYPE' PASSING v.INFTYPE
                       COLUMNS InfType              PATH '@InfType',
                               CLEARINGTYPE XMLTYPE PATH '/*')(+) w,
              XMLTABLE('//CLEARINGTYPE' PASSING w.CLEARINGTYPE
                       COLUMNS ClearingType   PATH '@ClearingType',
                               SESION XMLTYPE PATH '/*')(+) m,
              XMLTABLE('//SESSION' PASSING m.SESION
                       COLUMNS SessionId          PATH '@Session',
                               SETTLEDATE XMLTYPE PATH '/*')(+) s,
              XMLTABLE('//SETTLEDATE' PASSING s.SETTLEDATE
                       COLUMNS SettleDate         PATH '@SettleDate',
                               INSTRTRADE XMLTYPE PATH '/*')(+) r,
              XMLTABLE('//INSTRTRADE' PASSING r.INSTRTRADE
                       COLUMNS InstrType    PATH '@InstrType',
                               BOARD XMLTYPE PATH '/*')(+) p,
              XMLTABLE('//BOARD' PASSING p.BOARD
                       COLUMNS BoardId          PATH '@BoardId',
                               BoardName        PATH '@BoardName',
                               SECURITY XMLTYPE PATH '/*') o,
              XMLTABLE('//SECURITY' PASSING o.SECURITY
                       COLUMNS SecurityId      PATH '@SecurityId',
                               ISIN            PATH '@ISIN',
                               SecShortName    PATH '@SecShortName',
                               PriceType       PATH '@PriceType',
                               RECORDS XMLTYPE PATH '/*') n,
              XMLTABLE('//RECORDS' PASSING n.RECORDS
                       COLUMNS RecNo             PATH '@RecNo',
                               TradeNo           PATH '@TradeNo',
                               TradeDate         PATH '@TradeDate',
                               TradeSessionDate  PATH '@TradeSessionDate',
                               TradeTime         PATH '@TradeTime',
                               BuySell           PATH '@BuySell',
                               SettleCode        PATH '@SettleCode',
                               Decimals          PATH '@Decimals',
                               Price             PATH '@Price',
                               Quantity          PATH '@Quantity',
                               Value             PATH '@Value',
                               FaceAmount        PATH '@FaceAmount',
                               DepoRate          PATH '@DepoRate',
                               AccInt            PATH '@AccInt',
                               Amount            PATH '@Amount',
                               Balance           PATH '@Balance',
                               Sum1              PATH '@Sum1',
                               Sum2              PATH '@Sum2',
                               ExchComm          PATH '@ExchComm',
                               ClrComm           PATH '@ClrComm',
                               TrdAccId          PATH '@TrdAccId',
                               CPFirmId          PATH '@CPFirmId',
                               CPFirmShortName   PATH '@CPFirmShortName',
                               Price2            PATH '@Price2',
                               Payoff            PATH '@Payoff',
                               RepoPart          PATH '@RepoPart',
                               RepoPeriod        PATH '@RepoPeriod',
                               ReportNo          PATH '@ReportNo',
                               ReportTime        PATH '@ReportTime',
                               SettleTime        PATH '@SettleTime',
                               DueDate           PATH '@DueDate',
                               Type              PATH '@Type',
                               SystemRef         PATH '@SystemRef',
                               EarlySettleStatus PATH '@EarlySettleStatus',
                               TradeMergeNo      PATH '@TradeMergeNo',
                               RepoRate          PATH '@RepoRate',
                               RateType          PATH '@RateType',
                               Benchmark         PATH '@Benchmark',
                               BenchmarkRate     PATH '@BenchmarkRate',
                               CurRepoRate       PATH '@CurRepoRate',
                               RepoSum           PATH '@RepoSum') u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета МБ.';
       Return 0;
     ELSE
       Return SQL%ROWCOUNT;
     END IF;
   end LoadEQM6C;

   function LoadEQM13 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета EQM13.
     p_Text  - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_mb_eqm13(reportdate, sessionnum, firmid, extsettlecode, postype, bankaccid, guardepunitid, trdaccid,
                              currencyid, currencyname, isin, securityid, secshortname, nettosum, datatype,
                              debit, credit)
       SELECT to_date(x.reportdate, 'YYYY-MM-DD'), x.SessionNum, y.firmid, z.ExtSettleCode, v.PosType, w.BankAccId, w.GuarDepUnitId, w.TrdAccId,
              m.CurrencyId, m.CurrencyName, m.ISIN, m.SecurityId, m.SecShortName, m.NettoSum, u.DataType,
              to_number(u.Debit, '99999999999999999999.99999999'), to_number(u.Credit, '99999999999999999999.99999999')
         FROM tmp_xml t,
              XMLTABLE('//EQM13' PASSING valxml
                       COLUMNS ReportDate   PATH '@ReportDate',
                               SessionNum   PATH '@Session',
                               FIRM XMLTYPE PATH '/*') x,
              XMLTABLE('//FIRM' PASSING x.FIRM
                       COLUMNS FirmID         PATH '@FirmID',
                               SETTLE XMLTYPE PATH '/*')(+) y,
              XMLTABLE('//SETTLE' PASSING y.SETTLE
                       COLUMNS ExtSettleCode    PATH '@ExtSettleCode',
                               POSTYPES XMLTYPE PATH '/*')(+) z,
              XMLTABLE('//POSTYPES' PASSING z.POSTYPES
                       COLUMNS PosType        PATH '@PosType',
                               GROUPE XMLTYPE PATH '/*')(+) v,
              XMLTABLE('//GROUP' PASSING v.GROUPE
                       COLUMNS BankAccId        PATH '@BankAccId',
                               GuarDepUnitId    PATH '@GuarDepUnitId',
                               TrdAccId         PATH '@TrdAccId',
                               CURRENCY XMLTYPE PATH '/*')(+) w,
              XMLTABLE('//CURRENCY' PASSING w.CURRENCY
                       COLUMNS CurrencyId   PATH '@CurrencyId',
                               CurrencyName    PATH '@CurrencyName',
                               ISIN            PATH '@ISIN',
                               SecurityId      PATH '@SecurityId',
                               SecShortName    PATH '@SecShortName',
                               NettoSum        PATH 'NettoSum',
                               RECORDS XMLTYPE PATH '/*')(+) m,
              XMLTABLE('//RECORDS' PASSING m.RECORDS
                       COLUMNS DataType PATH '@DataType',
                               Debit    PATH '@Debit',
                               Credit   PATH '@Credit') u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета МБ.';
       Return 0;
     ELSE
       Return SQL%ROWCOUNT;
     END IF;
   end LoadEQM13;

   function LoadEQM98 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета EQM98.
     p_Text  - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_mb_eqm98(reportdate, reporttype, mainfirmid, firmname, firmid, bankaccid, postype, realaccount, depunitid,
                              trdaccid, currencyid, currencyname, tradeno, operationno, settledate,
                              settletime, clientcode, tradedate, tradetype,
                              settledate1, settledate2, recorddate, debitcredit, securityid,
                              secshortname, isin, quantity, price, d1,
                              d2, tax,
                              fee, type, sum)
       SELECT to_date(x.reportdate, 'YYYY-MM-DD'), x.ReportType, x.MainFirmId, x.FirmName, y.firmid, z.BankAccId, v.PosType, w.RealAccount, w.DepUnitId,
              w.TrdAccId, m.CurrencyId, m.CurrencyName, u.TradeNo, u.OperationNo, to_date(u.SettleDate, 'YYYY-MM-DD'),
              to_date('0001-01-01'||' '||u.SettleTime, 'YYYY-MM-DD HH24:MI:SS'), u.ClientCode, to_date(u.TradeDate, 'YYYY-MM-DD'), u.TradeType,
              to_date(u.SettleDate1, 'YYYY-MM-DD'), to_date(u.SettleDate2, 'YYYY-MM-DD'), to_date(u.RecordDate, 'YYYY-MM-DD'), u.DebitCredit, u.SecurityId,
              u.SecShortName, u.ISIN, u.Quantity, to_number(u.Price, '99999999999999999999.99999999'), to_number(u.D1, '99999999999999999999.99999999'),
              to_number(u.D2, '99999999999999999999.99999999'), to_number(u.Tax, '99999999999999999999.99999999'),
              to_number(u.Fee, '99999999999999999999.99999999'), u.Type, to_number(u.Sum, '99999999999999999999.99999999')
         FROM tmp_xml t,
              XMLTABLE('//EQM98' PASSING valxml
                       COLUMNS ReportDate   PATH '@ReportDate',
                               ReportType   PATH '@ReportType',
                               MainFirmId   PATH '@MainFirmId',
                               FirmName     PATH '@FirmName',
                               FIRM XMLTYPE PATH '/*') x,
              XMLTABLE('//FIRM' PASSING x.FIRM
                       COLUMNS FirmID          PATH '@FirmID',
                               BANKACC XMLTYPE PATH '/*')(+) y,
              XMLTABLE('//BANKACC' PASSING y.BANKACC
                       COLUMNS BankAccId        PATH '@BankAccId',
                               POSTYPES XMLTYPE PATH '/*')(+) z,
              XMLTABLE('//POSTYPES' PASSING z.POSTYPES
                       COLUMNS PosType        PATH '@PosType',
                               GROUPE XMLTYPE PATH '/*')(+) v,
              XMLTABLE('//GROUP' PASSING v.GROUPE
                       COLUMNS RealAccount      PATH '@RealAccount',
                               DepUnitId        PATH '@DepUnitId',
                               TrdAccId         PATH '@TrdAccId',
                               CURRENCY XMLTYPE PATH '/*')(+) w,
              XMLTABLE('//CURRENCY' PASSING w.CURRENCY
                       COLUMNS CurrencyId      PATH '@CurrencyId',
                               CurrencyName    PATH '@CurrencyName',
                               RECORDS XMLTYPE PATH '/*')(+) m,
              XMLTABLE('//RECORDS' PASSING m.RECORDS
                       COLUMNS TradeNo      PATH '@TradeNo',
                               OperationNo  PATH '@OperationNo',
                               SettleDate   PATH '@SettleDate',
                               SettleTime   PATH '@SettleTime',
                               ClientCode   PATH '@ClientCode',
                               TradeDate    PATH '@TradeDate',
                               TradeType    PATH '@TradeType',
                               SettleDate1  PATH '@SettleDate1',
                               SettleDate2  PATH '@SettleDate2',
                               RecordDate   PATH '@RecordDate',
                               DebitCredit  PATH '@DebitCredit',
                               SecurityId   PATH '@SecurityId',
                               SecShortName PATH '@SecShortName',
                               ISIN         PATH '@ISIN',
                               Quantity     PATH '@Quantity',
                               Price        PATH '@Price',
                               D1           PATH '@D1',
                               D2           PATH '@D2',
                               Tax          PATH '@Tax',
                               Fee          PATH '@Fee',
                               Type         PATH '@Type',
                               Sum          PATH '@Sum') u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета МБ.';
       Return 0;
     ELSE
       Return SQL%ROWCOUNT;
     END IF;
   end LoadEQM98;

   function LoadEQM99 (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета EQM99.
     p_Text  - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_mb_eqm99(reportdate, reporttype, mainfirmid, firmname, firmid, bankaccid, bankaccunifiedpool, levelinfo,
                              parentbankaccid, postype, realaccount, depunitid, trdaccid, unifiedpooltrdacc, currencyid, currencyname, securityid, isin,
                              secshortname, openingbalance, closingbalance,
                              debitsum, creditsum, nettosum, operationcode, operationname,
                              operationtime, docno, traninfo, sessionnum, clientcode,
                              debit, credit,
                              netto)
       SELECT to_date(x.reportdate, 'YYYY-MM-DD'), x.ReportType, x.MainFirmId, x.FirmName, y.firmid, z.BankAccId, z.BankAccUnifiedPool, z.LevelInfo,
              z.ParentBankAccId, v.PosType, w.RealAccount, w.DepUnitId, w.TrdAccId, w.UnifiedPoolTrdAcc, u.CurrencyId, u.CurrencyName, u.SecurityId, u.ISIN,
              u.SecShortName, to_number(u.OpeningBalance, '99999999999999999999.99999999'), to_number(u.ClosingBalance, '99999999999999999999.99999999'),
              to_number(u.DebitSum, '99999999999999999999.99999999'), to_number(u.CreditSum, '99999999999999999999.99999999'),
              to_number(u.NettoSum, '99999999999999999999.99999999'), m.OperationCode, m.OperationName,
              to_date('0001-01-01'||' '||m.OperationTime, 'YYYY-MM-DD HH24:MI:SS'), m.DocNo, m.Traninfo, m.SessionNum, m.ClientCode,
              to_number(m.Debit, '99999999999999999999.99999999'), to_number(m.Credit, '99999999999999999999.99999999'),
              to_number(m.Netto, '99999999999999999999.99999999')
         FROM tmp_xml t,
              XMLTABLE('//EQM99' PASSING valxml
                       COLUMNS ReportDate   PATH '@ReportDate',
                               ReportType   PATH '@ReportType',
                               MainFirmId   PATH '@MainFirmId',
                               FirmName     PATH '@FirmName',
                               FIRM XMLTYPE PATH '/*') x,
              XMLTABLE('//FIRM' PASSING x.FIRM
                       COLUMNS FirmID          PATH '@FirmID',
                               BANKACC XMLTYPE PATH '/*')(+) y,
              XMLTABLE('//BANKACC' PASSING y.BANKACC
                       COLUMNS BankAccId          PATH '@BankAccId',
                               BankAccUnifiedPool PATH '@BankAccUnifiedPool',
                               LevelInfo          PATH '@LevelInfo',
                               ParentBankAccId    PATH '@ParentBankAccId',
                               POSTYPES   XMLTYPE PATH '/*')(+) z,
              XMLTABLE('//POSTYPES' PASSING z.POSTYPES
                       COLUMNS PosType        PATH '@PosType',
                               GROUPE XMLTYPE PATH '/*')(+) v,
              XMLTABLE('//GROUP' PASSING v.GROUPE
                       COLUMNS RealAccount       PATH '@RealAccount',
                               DepUnitId         PATH '@DepUnitId',
                               TrdAccId          PATH '@TrdAccId',
                               UnifiedPoolTrdAcc PATH '@UnifiedPoolTrdAcc',
                               RECORDS   XMLTYPE PATH '/*')(+) w,
              XMLTABLE('//RECORDS' PASSING w.RECORDS
                       COLUMNS CurrencyId     PATH '@CurrencyId',
                               CurrencyName   PATH '@CurrencyName',
                               SecurityId     PATH '@SecurityId',
                               ISIN           PATH '@ISIN',
                               SecShortName   PATH '@SecShortName',
                               OpeningBalance PATH '@OpeningBalance',
                               ClosingBalance PATH '@ClosingBalance',
                               DebitSum       PATH '@DebitSum',
                               CreditSum      PATH '@CreditSum',
                               NettoSum       PATH '@NettoSum',
                               ENTRY  XMLTYPE PATH '/*')(+) u,
              XMLTABLE('//ENTRY' PASSING u.ENTRY
                       COLUMNS OperationCode PATH '@OperationCode',
                               OperationName PATH '@OperationName',
                               OperationTime PATH '@OperationTime',
                               DocNo         PATH '@DocNo',
                               Traninfo      PATH '@Traninfo',
                               SessionNum    PATH '@Session',
                               ClientCode    PATH '@ClientCode',
                               Debit         PATH '@Debit',
                               Credit        PATH '@Credit',
                               Netto         PATH '@Netto')(+) m;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета МБ.';
       Return 0;
     ELSE
       Return SQL%ROWCOUNT;
     END IF;
   end LoadEQM99;

   function LoadEQM3T (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета EQM3T.
     p_Text  - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_mb_eqm3t(tradedate, mainfirmid, firmname, firmid, currencyid, boardid, boardname, settledate,
                              securityid, secshortname, initialfacevalue, facevalue,
                              seccurrencyid, pricetype, trdaccid, recno, tradeno, tradetime, buysell,
                              settlecode, decimals, price, quantity, value,
                              amount, faceamount,
                              clrcomm, orderno, ordtypecode, accint,
                              cpfirmid, cpfirmshortname, cptrdaccid, tradetype, userid, yield, period, extref,
                              clientcode, details, subdetails, matchref, brokerref)
       SELECT to_date(x.tradedate, 'YYYY-MM-DD'), x.mainfirmid, x.firmname, y.firmid, m.currencyid, z.boardid, z.boardname, to_date(v.settledate, 'YYYY-MM-DD'),
              w.securityid, w.secshortname, to_number(w.initialfacevalue, '99999999999999999999.99999999'), to_number(w.facevalue, '99999999999999999999.99999999'),
              w.seccurrencyid, w.pricetype, p.trdaccid, u.recno, u.tradeno, to_date('0001-01-01'||' '||u.tradetime, 'YYYY-MM-DD HH24:MI:SS'), u.buysell,
              u.settlecode, u.decimals, to_number(u.price, '99999999999999999999.99999999'), u.quantity, to_number(u.value, '99999999999999999999.99999999'),
              to_number(u.amount, '99999999999999999999.99999999'), to_number(u.faceamount, '99999999999999999999.99999999'),
              to_number(u.clrcomm, '99999999999999999999.99999999'), u.orderno, u.ordtypecode, to_number(u.accint, '99999999999999999999.99999999'),
              u.cpfirmid, u.cpfirmshortname, u.cptrdaccid, u.tradetype, u.userid, to_number(u.yield, '99999999999999999999.99999999'), u.period, u.extref,
              u.clientcode, u.details, u.subdetails, u.matchref, u.brokerref
         FROM tmp_xml t,
              XMLTABLE('//EQM3T' PASSING valxml
                       COLUMNS TradeDate    PATH '@TradeDate',
                               MainFirmId   PATH '@MainFirmId',
                               FirmName     PATH '@FirmName',
                               FIRM XMLTYPE PATH '/*') x,
              XMLTABLE('//FIRM' PASSING x.FIRM
                       COLUMNS FirmID           PATH '@FirmID',
                               CURRENCY XMLTYPE PATH '/*')(+) y,
              XMLTABLE('//CURRENCY' PASSING y.CURRENCY
                       COLUMNS CurrencyId      PATH '@CurrencyId',
                               BOARD XMLTYPE PATH '/*')(+) m,
              XMLTABLE('//BOARD' PASSING m.BOARD
                       COLUMNS BoardId            PATH '@BoardId',
                               BoardName          PATH '@BoardName',
                               SETTLEDATE XMLTYPE PATH '/*')(+) z,
              XMLTABLE('//SETTLEDATE' PASSING z.SETTLEDATE
                       COLUMNS SettleDate       PATH '@SettleDate',
                               SECURITY XMLTYPE PATH '/*')(+) v,
              XMLTABLE('//SECURITY' PASSING v.SECURITY
                       COLUMNS SecurityId       PATH '@SecurityId',
                               SecShortName     PATH '@SecShortName',
                               InitialFaceValue PATH '@InitialFaceValue',
                               FaceValue        PATH '@FaceValue',
                               SecCurrencyId    PATH '@SecCurrencyId',
                               PriceType        PATH '@PriceType',
                               TRDACC   XMLTYPE PATH '/*')(+) w,
              XMLTABLE('//TRDACC' PASSING w.TRDACC
                       COLUMNS TrdAccId       PATH '@TrdAccId',
                               RECORDS XMLTYPE PATH '/*') (+) p,
              XMLTABLE('//RECORDS' PASSING p.RECORDS
                       COLUMNS RecNo           PATH '@RecNo',
                               TradeNo         PATH '@TradeNo',
                               TradeTime       PATH '@TradeTime',
                               BuySell         PATH '@BuySell',
                               SettleCode      PATH '@SettleCode',
                               Decimals        PATH '@Decimals',
                               Price           PATH '@Price',
                               Quantity        PATH '@Quantity',
                               Value           PATH '@Value',
                               Amount          PATH '@Amount',
                               FaceAmount      PATH '@FaceAmount',
                               ClrComm         PATH '@ClrComm',
                               OrderNo         PATH '@OrderNo',
                               OrdTypeCode     PATH '@OrdTypeCode',
                               AccInt          PATH '@AccInt',
                               CPFirmId        PATH '@CPFirmId',
                               CPFirmShortName PATH '@CPFirmShortName',
                               CPTrdAccId      PATH '@CPTrdAccId',
                               TradeType       PATH '@TradeType',
                               UserId          PATH '@UserId',
                               Yield           PATH '@Yield',
                               Period          PATH '@Period',
                               ExtRef          PATH '@ExtRef',
                               ClientCode      PATH '@ClientCode',
                               Details         PATH '@Details',
                               SubDetails      PATH '@SubDetails',
                               MatchRef        PATH '@MatchRef',
                               BrokerRef       PATH '@BrokerRef')(+) u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета МБ.';
       Return 0;
     ELSE
       Return SQL%ROWCOUNT;
     END IF;
   end LoadEQM3T;

   function LoadEQM2T (P_TEXT out varchar2) return INTEGER as
   /*Обработка отчета EQM2T.
     p_Text  - информация о результате обработки.
   */
   begin
     INSERT INTO tmp_mb_eqm2t(tradedate, mainfirmid, firmname, firmid, boardid, boardname, recno, orderno, status, ordtypecode,
                              buysell, securityid, secshortname, pricetype, currencyid, decimals, price,
                              quantity, value, trdaccid, entrytime,
                              amendtime, settlecode, accint,
                              cpfirmid, cpfirmshortname, userid, asp, clientcode, details, subdetails, matchref, brokerref)
       SELECT to_date(x.tradedate, 'YYYY-MM-DD'), x.mainfirmid, x.firmname, y.firmid, z.boardid, z.boardname, u.recno, u.OrderNo, u.Status, u.OrdTypeCode,
              u.buysell, u.securityid, u.secshortname, u.pricetype, u.currencyid, u.decimals, to_number(u.price, '99999999999999999999.9999999'),
              u.quantity, to_number(u.value, '99999999999999999999.99'), u.TrdAccId, to_date('0001-01-01'||' '||u.EntryTime, 'YYYY-MM-DD HH24:MI:SS'),
              to_date('0001-01-01'||' '||u.AmendTime, 'YYYY-MM-DD HH24:MI:SS'), u.settlecode, to_number(u.accint, '99999999999999999999.99'),
              u.cpfirmid, u.cpfirmshortname, u.userid, u.ASP, u.clientcode, u.details, u.subdetails, u.matchref, u.brokerref
         FROM tmp_xml t,
              XMLTABLE('//EQM2T' PASSING valxml
                       COLUMNS TradeDate    PATH '@TradeDate',
                               MainFirmId   PATH '@MainFirmId',
                               FirmName     PATH '@FirmName',
                               FIRM XMLTYPE PATH '/*') x,
              XMLTABLE('//FIRM' PASSING x.FIRM
                       COLUMNS FirmID           PATH '@FirmID',
                               BOARD XMLTYPE PATH '/*')(+) y,
              XMLTABLE('//BOARD' PASSING y.BOARD
                       COLUMNS BoardId            PATH '@BoardId',
                               BoardName          PATH '@BoardName',
                               RECORDS XMLTYPE PATH '/*')(+) z,
              XMLTABLE('//RECORDS' PASSING z.RECORDS
                       COLUMNS RecNo           PATH '@RecNo',
                               OrderNo         PATH '@OrderNo',
                               Status          PATH '@Status',
                               OrdTypeCode     PATH '@OrdTypeCode',
                               BuySell         PATH '@BuySell',
                               SecurityId      PATH '@SecurityId',
                               SecShortName    PATH '@SecShortName',
                               PriceType       PATH '@PriceType',
                               CurrencyId      PATH '@CurrencyId',
                               Decimals        PATH '@Decimals',
                               Price           PATH '@Price',
                               Quantity        PATH '@Quantity',
                               Value           PATH '@Value',
                               TrdAccId        PATH '@TrdAccId',
                               EntryTime       PATH '@EntryTime',
                               AmendTime       PATH '@AmendTime',
                               SettleCode      PATH '@SettleCode',
                               AccInt          PATH '@AccInt',
                               CPFirmId        PATH '@CPFirmId',
                               CPFirmShortName PATH '@CPFirmShortName',
                               UserId          PATH '@UserId',
                               ASP             PATH '@ASP',
                               ClientCode      PATH '@ClientCode',
                               Details         PATH '@Details',
                               SubDetails      PATH '@SubDetails',
                               MatchRef        PATH '@MatchRef',
                               BrokerRef       PATH '@BrokerRef')(+) u;
     IF SQL%ROWCOUNT = 0 THEN
       p_Text := 'ошибка обработки элементов отчета МБ.';
       Return 0;
     ELSE
       Return SQL%ROWCOUNT;
     END IF;
   end LoadEQM2T;

   function Load_File (p_FILENAME in varchar2, p_FileType in varchar2, p_Msg out varchar2) return INTEGER as
   /*Основная функция загрузки полученных отчетов МБ.
     p_FileName - имя принятого файла.
     Возвращает информацию о результате загрузки и обработки.
   */
     l_Rows INTEGER;
     l_Text VARCHAR2(4000);
   begin
     p_Msg := 'Прием файла МБ '||p_FILENAME||': ';
     l_Rows := RSP_MB.LOADREQUISITES(p_FILENAME, p_FileType, l_Text);
     IF p_FileType = 'SEM02' THEN
       l_Rows := RSP_MB.LoadSEM02(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'SEM03' THEN
       l_Rows := RSP_MB.LoadSEM03(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'SEM21' THEN
       l_Rows := RSP_MB.LoadSEM21(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'SEM25' THEN
       l_Rows := RSP_MB.LoadSEM25(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'SEM26' THEN
       l_Rows := RSP_MB.LoadSEM26(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'EQM06' THEN
       l_Rows := RSP_MB.LoadEQM06(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'EQM6C' THEN
       l_Rows := RSP_MB.LoadEQM6C(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'EQM13' THEN
       l_Rows := RSP_MB.LoadEQM13(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'EQM98' THEN
       l_Rows := RSP_MB.LoadEQM98(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'EQM99' THEN
       l_Rows := RSP_MB.LoadEQM99(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'EQM3T' THEN
       l_Rows := RSP_MB.LoadEQM3T(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     IF p_FileType = 'EQM2T' THEN
       l_Rows := RSP_MB.LoadEQM2T(l_Text);
       p_Msg := p_Msg||l_Text;
       Return l_Rows;
     END IF;
     Return NULL;
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

   function Insert_SEM02 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета SEM02.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO mb_sem02_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO mb_sem02(id_mb_requisites, id_processing_log, tradedate, tradesessiondate, sessionno, firmid, boardid, boardname, activationdate,
                          recno, orderno, transno, status, ordtype, ordtypecode, buysell, securityid, secshortname, secsetid, secsetshortname,
                          pricetype, currencyid, decimals, price, initialprice,
                          quantity, quantityvisible, value, clearingcenterid, trdaccid,
                          entrytime, acttime,
                          amendtime, settlecode, accint, cpfirmid,
                          cpfirmshortname, cpfirminn, ratetype, benchmark, reporate, repovalue,
                          repoperiod, discount, lowerdiscount,
                          upperdiscount, userid, asp, clientcode, details, subdetails, refundrate,
                          matchref, brokerref, clearingfirmid, isactualmm, liqsource)
       SELECT p_IDReg, p_IDProcLog, u.TradeDate, u.TradeSessionDate, u.SessionNo, u.FirmID, u.BoardId, u.BoardName, u.ActivationDate,
              u.recno, u.orderno, u.transno, u.status, u.ordtype, u.ordtypecode, u.buysell, u.securityid, u.secshortname, u.secsetid, u.secsetshortname,
              u.pricetype, u.currencyid, u.decimals, u.price, u.initialprice,
              u.quantity, u.quantityvisible, u.value, u.clearingcenterid, u.trdaccid,
              u.entrytime, u.acttime,
              u.amendtime, u.settlecode, u.accint, u.cpfirmid,
              u.cpfirmshortname, u.cpfirminn, u.ratetype, u.benchmark, u.reporate, u.repovalue,
              u.repoperiod, u.discount, u.lowerdiscount,
              u.upperdiscount, u.userid, u.asp, u.clientcode, u.details, u.subdetails, u.refundrate,
              u.matchref, u.brokerref, u.clearingfirmid, u.isactualmm, u.liqsource
         FROM tmp_mb_sem02 u;
     Return SQL%ROWCOUNT;
   end Insert_SEM02;

   function Insert_SEM03 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета SEM03.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO mb_sem03_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO mb_sem03(id_mb_requisites, id_processing_log, tradedate, tradesessiondate, sessionno, firmid, currencyid, boardid, boardname, settledate, securityid, secshortname,
                          secname, securitytype, initialfacevalue, facevalue, seccurrencyid, pricetype, trdaccid, clearingcenterid, recno, secsetid,
                          secsetshortname, tradeno, tradetime, buysell, settlecode, decimals, price, quantity, value, amount, exchcomm, faceamount,
                          orderno, accint, cpfirmid, cpfirmshortname, cpfirminn, cptrdaccid, repovalue, repoperiod, ratetype,
                          benchmark, reporate, outstandingreturnvalue, discount, lowerdiscount, upperdiscount, tradetype, cancelorder, iscancel, userid,
                          yield, period, extref, price2, accint2, clientcode, details, subdetails, refundrate, matchref, brokerref, systemref,
                          clearingfirmid, ishidden, isactualmm, liqsource)
       SELECT p_IDReg, p_IDProcLog, u.tradedate, u.tradesessiondate, u.sessionno, u.firmid, u.currencyid, u.boardid, u.boardname, u.settledate, u.securityid, u.secshortname,
              u.secname, u.securitytype, u.initialfacevalue, u.facevalue, u.seccurrencyid, u.pricetype, u.trdaccid, u.clearingcenterid, u.recno, u.secsetid,
              u.secsetshortname, u.tradeno, u.tradetime, u.buysell, u.settlecode, u.decimals, u.price, u.quantity, u.value, u.amount, u.exchcomm, u.faceamount,
              u.orderno, u.accint, u.cpfirmid, u.cpfirmshortname, u.cpfirminn, u.cptrdaccid, u.repovalue, u.repoperiod, u.ratetype,
              u.benchmark, u.reporate, u.outstandingreturnvalue, u.discount, u.lowerdiscount, u.upperdiscount, u.tradetype, u.cancelorder, u.iscancel, u.userid,
              u.yield, u.period, u.extref, u.price2, u.accint2, u.clientcode, u.details, u.subdetails, u.refundrate, u.matchref, u.brokerref, u.systemref,
              u.clearingfirmid, u.ishidden, u.isactualmm, u.liqsource
         FROM tmp_mb_sem03 u;
     Return SQL%ROWCOUNT;
   end Insert_SEM03;

   function Insert_SEM21 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета SEM21.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO mb_sem21_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO mb_sem21(id_mb_requisites, id_processing_log, tradedate, tradesessiondate, sessionno, boardid, boardname, engboardname, boardtype, securityid, secshortname,
                          securitytype, pricetype, engname, engtype, regnumber, decimals, facevalue, faceunit, volume, value, currencyid,
                          openperiod, open, low, high, close, lastprice, lowoffer, highbid, waprice, closeauction, closeperiod, trendclose,
                          trendwap, bid, offer, prev, yieldatwap, yieldclose, accint, accintface, marketprice, numtrades, issuesize,
                          trendclspr, trendwappr, matdate, marketprice2, marketprice3, marketprice3cur, admittedquote, listname, prevlegalcloseprice,
                          legalopenprice, legalcloseprice, openval, closeval, duration, mpvaltrd, mp2valtrd, mp3valtrd, mp3valtrdcur)
       SELECT p_IDReg, p_IDProcLog, u.tradedate, u.tradesessiondate, u.sessionno, u.boardid, u.boardname, u.engboardname, u.boardtype, u.securityid, u.secshortname,
              u.securitytype, u.pricetype, u.engname, u.engtype, u.regnumber, u.decimals, u.facevalue, u.faceunit, u.volume, u.value, u.currencyid,
              u.openperiod, u.open, u.low, u.high, u.close, u.lastprice, u.lowoffer, u.highbid, u.waprice, u.closeauction, u.closeperiod, u.trendclose,
              u.trendwap, u.bid, u.offer, u.prev, u.yieldatwap, u.yieldclose, u.accint, u.accintface, u.marketprice, u.numtrades, u.issuesize,
              u.trendclspr, u.trendwappr, u.matdate, u.marketprice2, u.marketprice3, u.marketprice3cur, u.admittedquote, u.listname, u.prevlegalcloseprice,
              u.legalopenprice, u.legalcloseprice, u.openval, u.closeval, u.duration, u.mpvaltrd, u.mp2valtrd, u.mp3valtrd, u.mp3valtrdcur
         FROM tmp_mb_sem21 u;
     Return SQL%ROWCOUNT;
   end Insert_SEM21;

   function Insert_SEM25 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета SEM25.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO mb_sem25_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO mb_sem25(id_mb_requisites, id_processing_log, reportdate, firmid, currencyid, currencyname,
                          secshortname, securityid, secname, recno, repotradeno, repotradedate, settledate, direction, decimals, discount, lowerdiscount,
                          upperdiscount, outstandingdeficit, repothreshold, value, quantity, outstandingdiscount, outstandingrepovalue, outstandingquantity,
                          outstandingprice2, outstandingreturnvalue, trdaccid, clientdetails, cpfirmid, cpfirmshortname)
       SELECT p_IDReg, p_IDProcLog, u.reportdate, u.firmid, u.currencyid, u.currencyname, u.secshortname,
              u.securityid, u.secname, u.recno, u.repotradeno, u.repotradedate, u.settledate, u.direction, u.decimals, u.discount, u.lowerdiscount,
              u.upperdiscount, u.outstandingdeficit, u.repothreshold, u.value, u.quantity, u.outstandingdiscount, u.outstandingrepovalue,
              u.outstandingquantity, u.outstandingprice2, u.outstandingreturnvalue, u.trdaccid, u.clientdetails, u.cpfirmid, u.cpfirmshortname
         FROM tmp_mb_sem25 u;
     Return SQL%ROWCOUNT;
   end Insert_SEM25;

   function Insert_SEM26 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета SEM26.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO mb_sem26_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO mb_sem26(id_mb_requisites, id_processing_log, reportdate, firmid, currencyid, currencyname, securityid, recno, repotradeno, repotradedate,
                          repovaluedate, principalvalue, couponvalue, repovalue, returnvalue, outstandingrepovalue, outstandingreturnvalue, mcvalue,
                          outstandingrepovalue2, outstandingreturnvalue2, paymentvalue, paymentcurrency, clientdetails)
       SELECT p_IDReg, p_IDProcLog, u.reportdate, u.firmid, u.currencyid, u.currencyname, u.securityid, u.recno, u.repotradeno, u.repotradedate,
              u.repovaluedate, u.principalvalue, u.couponvalue, u.repovalue, u.returnvalue, u.outstandingrepovalue, u.outstandingreturnvalue, u.mcvalue,
              u.outstandingrepovalue2, u.outstandingreturnvalue2, u.paymentvalue, u.paymentcurrency, u.clientdetails
         FROM tmp_mb_sem26 u;
     Return SQL%ROWCOUNT;
   end Insert_SEM26;

   function Insert_EQM06 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета EQM06.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO mb_eqm06_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO mb_eqm06(id_mb_requisites, id_processing_log, reportdate, firmid, extsettlecode, currencyid, inftype, clearingtype, sessionnum, settledate,
                          instrtype, boardid, boardname, securityid, isin, secshortname, pricetype, recno, tradeno, tradedate, tradesessiondate, tradetime, buysell, settlecode,
                          decimals, price, quantity, value, faceamount, deporate, accint, amount, balance, sum1, sum2, exchcomm, clrcomm, trdaccid, clientdetails,
                          cpfirmid, cpfirmshortname, price2, payoff, repopart, repoperiod, reportno, reporttime, settletime, clientcode, duedate, type,
                          systemref, earlysettlestatus, trademergeno, reporate, ratetype, benchmark, benchmarkrate, curreporate, reposum, interestamount)
       SELECT p_IDReg, p_IDProcLog, reportdate, firmid, extsettlecode, currencyid, inftype, clearingtype, sessionnum, settledate,
              instrtype, boardid, boardname, securityid, isin, secshortname, pricetype, recno, tradeno, tradedate, tradesessiondate, tradetime, buysell, settlecode,
              decimals, price, quantity, value, faceamount, deporate, accint, amount, balance, sum1, sum2, exchcomm, clrcomm, trdaccid, clientdetails,
              cpfirmid, cpfirmshortname, price2, payoff, repopart, repoperiod, reportno, reporttime, settletime, clientcode, duedate, type,
              systemref, earlysettlestatus, trademergeno, reporate, ratetype, benchmark, benchmarkrate, curreporate, reposum, interestamount
         FROM tmp_mb_eqm06 u;
     Return SQL%ROWCOUNT;
   end Insert_EQM06;

   function Insert_EQM6C (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета EQM6C.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO mb_eqm6c_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO mb_eqm6c(id_mb_requisites, id_processing_log, reportdate, firmid, clientcode, clientdetails, currencyid, currencyname, inftype, clearingtype,
                          sessionid, settledate, instrtype, boardid, boardname, securityid, isin, secshortname, pricetype, recno, tradeno,
                          tradedate, tradesessiondate, tradetime, buysell, settlecode, decimals, price, quantity, value, faceamount, deporate, accint, amount,
                          balance, sum1, sum2, exchcomm, clrcomm, trdaccid, cpfirmid, cpfirmshortname, price2, payoff, repopart, repoperiod,
                          reportno, reporttime, settletime, duedate, type, systemref, earlysettlestatus, trademergeno, reporate, ratetype,
                          benchmark, benchmarkrate, curreporate, reposum)
       SELECT p_IDReg, p_IDProcLog, u.reportdate, u.firmid, u.clientcode, u.clientdetails, u.currencyid, u.currencyname, u.inftype, u.clearingtype,
              u.sessionid, u.settledate, u.instrtype, u.boardid, u.boardname, u.securityid, u.isin, u.secshortname, u.pricetype, u.recno, u.tradeno,
              u.tradedate, u.tradesessiondate, u.tradetime, u.buysell, u.settlecode, u.decimals, u.price, u.quantity, u.value, u.faceamount, u.deporate, u.accint, u.amount,
              u.balance, u.sum1, u.sum2, u.exchcomm, u.clrcomm, u.trdaccid, u.cpfirmid, u.cpfirmshortname, u.price2, u.payoff, u.repopart, u.repoperiod,
              u.reportno, u.reporttime, u.settletime, u.duedate, u.type, u.systemref, u.earlysettlestatus, u.trademergeno, u.reporate, u.ratetype,
              u.benchmark, u.benchmarkrate, u.curreporate, u.reposum
         FROM tmp_mb_eqm6c u;
     Return SQL%ROWCOUNT;
   end Insert_EQM6C;

   function Insert_EQM13 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета EQM13.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO mb_eqm13_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO mb_eqm13(id_mb_requisites, id_processing_log, reportdate, sessionnum, firmid, extsettlecode, postype, bankaccid, guardepunitid,
                          trdaccid, currencyid, currencyname, isin, securityid, secshortname, nettosum, datatype, debit, credit)
       SELECT p_IDReg, p_IDProcLog, u.reportdate, u.sessionnum, u.firmid, u.extsettlecode, u.postype, u.bankaccid, u.guardepunitid,
              u.trdaccid, u.currencyid, u.currencyname, u.isin, u.securityid, u.secshortname, u.nettosum, u.datatype, u.debit, u.credit
         FROM tmp_mb_eqm13 u;
     Return SQL%ROWCOUNT;
   end Insert_EQM13;

   function Insert_EQM98 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета EQM98.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO mb_eqm98_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO mb_eqm98(id_mb_requisites, id_processing_log, reportdate, reporttype, mainfirmid, firmname, firmid, bankaccid, postype, realaccount,
                          depunitid, trdaccid, currencyid, currencyname, tradeno, operationno, settledate, settletime, clientcode, tradedate,
                          tradetype, settledate1, settledate2, recorddate, debitcredit, securityid, secshortname, isin, quantity, price,
                          d1, d2, tax, fee, type, sum)
       SELECT p_IDReg, p_IDProcLog, u.reportdate, u.reporttype, u.mainfirmid, u.firmname, u.firmid, u.bankaccid, u.postype, u.realaccount,
              u.depunitid, u.trdaccid, u.currencyid, u.currencyname, u.tradeno, u.operationno, u.settledate, u.settletime, u.clientcode, u.tradedate,
              u.tradetype, u.settledate1, u.settledate2, u.recorddate, u.debitcredit, u.securityid, u.secshortname, u.isin, u.quantity, u.price,
              u.d1, u.d2, u.tax, u.fee, u.type, u.sum
         FROM tmp_mb_eqm98 u;
     Return SQL%ROWCOUNT;
   end Insert_EQM98;

   function Insert_EQM99 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета EQM99.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO mb_eqm99_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO mb_eqm99(id_mb_requisites, id_processing_log, reportdate, reporttype, mainfirmid, firmname, firmid, bankaccid, bankaccunifiedpool,
                          levelinfo, parentbankaccid, postype, realaccount, depunitid, trdaccid, unifiedpooltrdacc, currencyid, currencyname,
                          securityid, isin, secshortname, openingbalance, closingbalance, debitsum, creditsum, nettosum, operationcode,
                          operationname, operationtime, docno, traninfo, sessionnum, clientcode, debit, credit, netto)
       SELECT p_IDReg, p_IDProcLog, u.reportdate, u.reporttype, u.mainfirmid, u.firmname, u.firmid, u.bankaccid, u.bankaccunifiedpool,
              u.levelinfo, u.parentbankaccid, u.postype, u.realaccount, u.depunitid, u.trdaccid, u.unifiedpooltrdacc, u.currencyid, u.currencyname,
              u.securityid, u.isin, u.secshortname, u.openingbalance, u.closingbalance, u.debitsum, u.creditsum, u.nettosum, u.operationcode,
              u.operationname, u.operationtime, u.docno, u.traninfo, u.sessionnum, u.clientcode, u.debit, u.credit, u.netto
         FROM tmp_mb_eqm99 u;
     Return SQL%ROWCOUNT;
   end Insert_EQM99;

   function Insert_EQM3T (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета EQM3T.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO mb_eqm3t_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO mb_eqm3t(id_mb_requisites, id_processing_log, tradedate, mainfirmid, firmname, firmid, currencyid, boardid, boardname, settledate,
                          securityid, secshortname, initialfacevalue, facevalue, seccurrencyid, pricetype, trdaccid, recno, tradeno,
                          tradetime, buysell, settlecode, decimals, price, quantity, value, amount, faceamount, clrcomm, orderno,
                          ordtypecode, accint, cpfirmid, cpfirmshortname, cptrdaccid, tradetype, userid, yield, period, extref,
                          clientcode, details, subdetails, matchref, brokerref)
       SELECT p_IDReg, p_IDProcLog, u.tradedate, u.mainfirmid, u.firmname, u.firmid, u.currencyid, u.boardid, u.boardname, u.settledate,
              u.securityid, u.secshortname, u.initialfacevalue, u.facevalue, u.seccurrencyid, u.pricetype, u.trdaccid, u.recno, u.tradeno,
              u.tradetime, u.buysell, u.settlecode, u.decimals, u.price, u.quantity, u.value, u.amount, u.faceamount, u.clrcomm, u.orderno,
              u.ordtypecode, u.accint, u.cpfirmid, u.cpfirmshortname, u.cptrdaccid, u.tradetype, u.userid, u.yield, u.period, u.extref,
              u.clientcode, u.details, u.subdetails, u.matchref, u.brokerref
         FROM tmp_mb_eqm3t u;
     Return SQL%ROWCOUNT;
   end Insert_EQM3T;

   function Insert_EQM2T (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета EQM2T.
     p_IDReg - ссылка на отчет,
   */
   begin
     INSERT INTO mb_eqm2t_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO mb_eqm2t(id_mb_requisites, id_processing_log, tradedate, mainfirmid, firmname, firmid, boardid, boardname, recno, orderno, status,
                          ordtypecode, buysell, securityid, secshortname, pricetype, currencyid, decimals, price, quantity, value,
                          trdaccid, entrytime, amendtime, settlecode, accint, cpfirmid, cpfirmshortname, userid, asp, clientcode,
                          details, subdetails, matchref, brokerref)
       SELECT p_IDReg, p_IDProcLog, u.tradedate, u.mainfirmid, u.firmname, u.firmid, u.boardid, u.boardname, u.recno, u.orderno, u.status,
              u.ordtypecode, u.buysell, u.securityid, u.secshortname, u.pricetype, u.currencyid, u.decimals, u.price, u.quantity, u.value,
              u.trdaccid, u.entrytime, u.amendtime, u.settlecode, u.accint, u.cpfirmid, u.cpfirmshortname, u.userid, u.asp, u.clientcode,
              u.details, u.subdetails, u.matchref, u.brokerref
         FROM tmp_mb_eqm2t u;
     Return SQL%ROWCOUNT;
   end Insert_EQM2T;

  function Insert_Table (p_FILENAME in varchar2, p_FileType in varchar2, p_IDProcLog in integer, p_Date in date default null) return INTEGER as
  /*Основная функция загрузки полученных отчетов МБ.
    p_FileName - имя принятого файла,
    p_FileType - тип файла.
    Возвращает информацию о результате загрузки и обработки.
  */
    l_IDReq   INTEGER;
    l_IdNForm INTEGER;
  begin
    IF p_FileType = 'SEM02' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'МБ\Формы\SEM02'));
    ELSIF p_FileType = 'SEM03' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'МБ\Формы\SEM03'));
    ELSIF p_FileType = 'SEM21' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'МБ\Формы\SEM21'));
    ELSIF p_FileType = 'SEM25' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'МБ\Формы\SEM25'));
    ELSIF p_FileType = 'SEM26' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'МБ\Формы\SEM26'));
    ELSIF p_FileType = 'EQM06' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'МБ\Формы\EQM06'));
    ELSIF p_FileType = 'EQM6C' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'МБ\Формы\EQM6C'));
    ELSIF p_FileType = 'EQM13' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'МБ\Формы\EQM13'));
    ELSIF p_FileType = 'EQM98' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'МБ\Формы\EQM98'));
    ELSIF p_FileType = 'EQM99' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'МБ\Формы\EQM99'));
    ELSIF p_FileType = 'EQM3T' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'МБ\Формы\EQM3T'));
    ELSIF p_FileType = 'EQM2T' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'МБ\Формы\EQM2T'));
    END IF;
    l_IDReq := RSP_MB.Insert_Requisites(p_FILENAME, p_IDProcLog, l_IdNForm, p_Date);
    IF p_FileType = 'SEM02' THEN
      Return RSP_MB.Insert_SEM02(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'SEM03' THEN
      Return RSP_MB.Insert_SEM03(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'SEM21' THEN
      Return RSP_MB.Insert_SEM21(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'SEM25' THEN
      Return RSP_MB.Insert_SEM25(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'SEM26' THEN
      Return RSP_MB.Insert_SEM26(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'EQM06' THEN
      Return RSP_MB.Insert_EQM06(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'EQM6C' THEN
      Return RSP_MB.Insert_EQM6C(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'EQM13' THEN
      Return RSP_MB.Insert_EQM13(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'EQM98' THEN
      Return RSP_MB.Insert_EQM98(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'EQM99' THEN
      Return RSP_MB.Insert_EQM99(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'EQM3T' THEN
      Return RSP_MB.Insert_EQM3T(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'EQM2T' THEN
      Return RSP_MB.Insert_EQM2T(l_IDReq, p_IDProcLog);
    END IF;
    Return null;
  end Insert_Table;

  function Get_ParamFile return T_FILEPARAMS as
    l_FileParams T_FILEPARAMS;
  begin
    SELECT to_date(nvl(x.TradeSessionDateSEM02, nvl(x.TradeDateSEM02,
                     nvl(x.TradeSessionDateSEM03, nvl(x.TradeDateSEM03,
                         nvl(x.TradeSessionDateSEM21, nvl(x.TradeDateSEM21,
                             nvl(x.ReportDateSEM25,
                                 nvl(x.ReportDateSEM26,
                                     nvl(x.ReportDateEQM06,
                                         nvl(x.ReportDateEQM6C,
                                             nvl(x.ReportDateEQM13,
                                                 nvl(x.ReportDateEQM98,
                                                     nvl(x.ReportDateEQM99,
                                                         nvl(x.TradeDateEQM3T,
                                                             nvl(x.TradeDateEQM2T,
                                                                 nvl(x.ReportDateCUX23,
                                                                     nvl(x.ReportDateCCX17,
                                                                         nvl(x.ReportDateCCX04,
                                                                             nvl(x.TradeDateCCX99,
                                                                                 nvl(x.ReportDateCCX10, x.ReportDateCUX22)))))))))))))))))))), 'YYYY-MM-DD'),
           to_date(x.DOC_DATE, 'YYYY-MM-DD'), to_date('0001-01-01'||' '||x.DOC_TIME, 'YYYY-MM-DD HH24:MI:SS'),
           x.doc_type_id, DBMS_CRYPTO.HASH(t.valxml.GetClobVal(), 2)
      INTO l_FileParams.TradeDate, l_FileParams.DOC_DATE, l_FileParams.DOC_TIME, l_FileParams.doc_type_id, l_FileParams.FileHash
      FROM tmp_xml t,
           XMLTABLE('//MICEX_DOC' PASSING valxml
                       COLUMNS doc_type_id     PATH 'DOC_REQUISITES/@DOC_TYPE_ID',
                               DOC_DATE        PATH 'DOC_REQUISITES/@DOC_DATE',
                               DOC_TIME        PATH 'DOC_REQUISITES/@DOC_TIME',
                               TradeSessionDateSEM02  PATH 'SEM02/@TradeSessionDate',
                               TradeDateSEM02  PATH 'SEM02/@TradeDate',
                               TradeSessionDateSEM03  PATH 'SEM03/@TradeSessionDate',
                               TradeDateSEM03  PATH 'SEM03/@TradeDate',
                               TradeSessionDateSEM21  PATH 'SEM21/@TradeSessionDate',
                               TradeDateSEM21  PATH 'SEM21/@TradeDate',
                               ReportDateSEM25 PATH 'SEM25/@ReportDate',
                               ReportDateSEM26 PATH 'SEM26/@ReportDate',
                               ReportDateEQM06 PATH 'EQM06/@ReportDate',
                               ReportDateEQM6C PATH 'EQM6C/@ReportDate',
                               ReportDateEQM13 PATH 'EQM13/@ReportDate',
                               ReportDateEQM98 PATH 'EQM98/@ReportDate',
                               ReportDateEQM99 PATH 'EQM99/@ReportDate',
                               TradeDateEQM3T  PATH 'EQM3T/@TradeDate',
                               TradeDateEQM2T  PATH 'EQM2T/@TradeDate',
                               ReportDateCUX23 PATH 'CUX23/@ReportDate',
                               ReportDateCCX17 PATH 'CCX17/@ReportDate',
                               ReportDateCCX10 PATH 'CCX10/@ReportDate',
                               ReportDateCCX04 PATH 'CCX04/@ReportDate',
                               TradeDateCCX99  PATH 'CCX99/@TRADE_DATE',
                               ReportDateCUX22 PATH 'CUX22/@ReportDate') x;
    Return l_FileParams;
  end Get_ParamFile;

  function Check_UniqueFile (p_IDProcAct IN INTEGER, p_FileHash IN VARCHAR2, p_FileDate IN DATE) return BOOLEAN as
    l_FileDate DATE;
    l_FileHash VARCHAR2(100);
  begin
    SELECT to_date(nvl(r.doc_date, '01.01.0001')||' '||to_char(r.doc_time, 'HH24:MI:SS'), 'DD.MM.YYYY HH24:MI:SS'), p.file_hash
      INTO l_FileDate, l_FileHash
      FROM mb_requisites r,
           processing_log p,
           processing_actual a
      WHERE p.id_processing_log = r.id_processing_log
        AND p.id_processing_log = a.id_processing_log
        AND a.id_processing_actual = p_IDProcAct;
    IF l_FileDate <> nvl(p_FileDate, l_FileDate) OR l_FileHash <> p_FileHash THEN
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
        AND pa.trade_date = p_FileParams.TradeDate
        AND pa.ord_num_str = p_FileParams.OrdNumStr
        AND pa.file_type = p_FileParams.doc_type_id
        AND pl.file_hash = p_FileParams.FileHash
        AND rownum = 1;
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

  function Load_MB (p_FILENAME in varchar2, p_Content in clob) return INTEGER as
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
    l_FileParams.OrdNum := to_number(regexp_replace(regexp_substr(upper(p_FILENAME), '(.*?)(\_|$)', 1, 3, NULL, 1) ,'[^[:digit:]]' ,''));
    l_FileParams.OrdNumStr := regexp_substr(upper(p_FILENAME), '(.*?)(\_|$)', 1, 3, NULL, 1) || '_' || regexp_substr(upper(p_FILENAME), '(.*?)(\_|$|\.)', 1, 4, NULL, 1);
    
    IF l_FileParams.doc_type_id IS NULL THEN
      l_FileParams.doc_type_id := regexp_substr(upper(p_FILENAME), '(.*?)(\_|$)', 1, 2, NULL, 1);
    END IF;
    
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
    l_RowsReated := RSP_MB.LOAD_File(p_FILENAME => p_FILENAME,
                                     p_FileType => l_FileParams.doc_type_id,
                                     P_MSG      => l_Msg);

    IF nvl(l_RowsReated, 0) = 0 THEN
      --логируем ошибку
      l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
      RSP_MB.LogData_Insert(l_IDSession, 2, l_RowsReated, l_Msg, l_Duration);
      RSP_MB.Processing_Log_Update(p_IDProcLog   => l_IDProcLog,
                                   p_RowsReaded  => NULL,
                                   p_RowsCreated => NULL,
                                   p_RRCreated   => NULL,
                                   p_ResNum      => NULL,
                                   p_ResText     => l_Msg);
      Return -1;
    END IF;

    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_MB.LogData_Insert(l_IDSession, 4, l_RowsReated, 'Успешно загружены данные из файла во временные таблицы', l_Duration);
    l_Systimestamp := systimestamp;

    --4. Переливаем данные в постоянные таблицы
    l_RowsCreated := RSP_MB.Insert_Table(p_FILENAME  => p_FILENAME,
                                         p_FileType  => l_FileParams.doc_type_id,
                                         p_IDProcLog => l_IDProcLog,
                                         p_Date      => l_FileParams.TradeDate);
    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_MB.LogData_Insert(l_IDSession, 5, l_RowsCreated, 'Успешно загружены данные в постоянные таблицы', l_Duration);
    l_Systimestamp := systimestamp;
/*
  --4. Создаем или обновляем записи репликации
  INSERT INTO logdata(id_logsession, ext_num, rows, info)
    VALUES(id_logsession, 4, l_Rows, 'Stage 4. Успешно созданы записи репликации');
*/

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
     WHERE trade_date  = l_FileParams.TradeDate
       AND file_type   = l_FileParams.doc_type_id
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
  end Load_MB;
end RSP_MB;
/
