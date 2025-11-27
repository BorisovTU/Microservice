CREATE OR REPLACE package body RSP_FMARKET as /*Тело пакета RSP_FMARKET*/
 function LoadRequisites (p_FileType IN VARCHAR2, p_TEXT out varchar2) RETURN INTEGER as
 /*Обработка отчета МБ.
 p_FILENAME - имя файла
 p_FileType - тип файла
 p_Text - информация о результате обработки.
 */
 begin
 INSERT INTO tmp_mb_requisites(doc_type_id, sender_id, sender_name, receiver_id)
 VALUES(p_FileType, const_SENDER_ID, const_SENDER_NAME, const_RECEIVER_ID);
 IF SQL%ROWCOUNT = 0 THEN
 p_Text := 'ошибка разбора формата МБ';
 Return 0;
 END IF;
 Return 1;
 end LoadRequisites;

 function Loadf04 (P_TEXT out varchar2) return INTEGER as
 /*Обработка отчета f04.
 p_Text - информация о результате обработки.
 */
 l_Str CLOB;
 l_Value RSP_COMMON.t_Value := RSP_COMMON.t_Value();
 l_Cnt INTEGER;
 l_Strv VARCHAR2(32000);
 l_tmp INTEGER := 1;
 l_pos INTEGER;
 begin
     SELECT substr(t.valclob, INSTR(t.valclob,chr(10))+1)||chr(10)
 INTO l_Str
 FROM tmp_xml t;

 l_pos:= INSTR(l_str, chr(10));

 WHILE l_pos > 0 LOOP
    l_strv := SUBSTR(l_str, l_tmp, l_pos - l_tmp);
    l_tmp:= l_pos + 1;
    l_Value := RSP_COMMON.ParseStr(p_Str => l_strv,
    p_Separator => ';'); 
    l_pos:= INSTR(l_str, chr(10), l_tmp);
    INSERT INTO TMP_FM_F04(ID_DEAL, ISIN, PRICE, VOL, KOD_SELL, KOD_BUY, DATE1,
                           TIME, PROFIT_USD, TYPE_BUY, TYPE_SELL,
                           VAR_MARG_B, VAR_MARG_S, USER_SELL, USER_BUY,
                           NO_BUY, NO_SELL, FEE_BUY, FEE_SELL,
                           DATE2, COMM_BUY, COMM_SELL, FEE_NS_B,
                           FEE_NS_S, PRICE_RUR, EXT_ID_B, EXT_ID_S,
                           DATE_CLR, REPO_ID, FEE_EX_B,
                           VAT_EX_B, FEE_CC_B, VAT_CC_B,
                           FEE_EX_S, VAT_EX_S, FEE_CC_S,
                           VAT_CC_S, ID_MULT, SIGNS_BUY, SIGNS_SELL, COUNTERPARTY, NCC_REQUEST_BUY, NCC_REQUEST_SELL,
                           VAR_MARG_B_SETTL_PRICE, VAR_MARG_B_SWAP_RATE, VAR_MARG_S_SETTL_PRICE, VAR_MARG_S_SWAP_RATE)
    VALUES(l_Value(1), l_Value(2), to_number(l_Value(3), '9999999999999999.99999'), l_Value(4), l_Value(5), l_Value(6), to_date(l_Value(7), 'YYYY/MM/DD'),
           to_date('01.01.0001'||' '||l_Value(8), 'DD.MM.YYYY HH24:MI:SS'), to_number(l_Value(9), '99999999999999999999.9999'), l_Value(10), l_Value(11),
           to_number(l_Value(12), '9999999999999999.99'), to_number(l_Value(13), '9999999999999999.99'), l_Value(14), l_Value(15),
           l_Value(16), l_Value(17), to_number(l_Value(18), '9999999999999999.99'), to_number(l_Value(19), '9999999999999999.99'),
           to_date(l_Value(20), 'DD.MM.YYYY'), l_Value(21), l_Value(22), to_number(l_Value(23), '9999999999999999.99'),
           to_number(l_Value(24), '9999999999999999.99'), to_number(l_Value(25), '9999999999999999.99999'), l_Value(26), l_Value(27),
           to_date(l_Value(28), 'DD.MM.YYYY'), l_Value(29), to_number(l_Value(30), '9999999999999999.99'),
           to_number(l_Value(31), '9999999999999999.99'), to_number(l_Value(32), '9999999999999999.99'), to_number(l_Value(33), '9999999999999999.99'),
           to_number(l_Value(34), '9999999999999999.99'), to_number(l_Value(35), '9999999999999999.99'), to_number(l_Value(36), '9999999999999999.99'),
           to_number(l_Value(37), '9999999999999999.99'), l_Value(38), l_Value(39), l_Value(40), l_Value(41), l_Value(42), l_Value(43),
           to_number(l_Value(44), '9999999999999999.99'),to_number(l_Value(45), '9999999999999999.99'), to_number(l_Value(46), '9999999999999999.99'), to_number(l_Value(47), '9999999999999999.99'));
    l_Value.DELETE;
 END LOOP;
 SELECT count(*)
 INTO l_Cnt
 FROM TMP_FM_F04;
 IF l_Cnt = 0 THEN
 p_Text := 'ошибка обработки элементов отчета срочного рынка МБ.';
 Return 0;
 ELSE
 Return l_Cnt;
 END IF;
 end Loadf04;

 function Loado04 (P_TEXT out varchar2) return INTEGER as
 /*Обработка отчета o04.
 p_Text - информация о результате обработки.
 */
 l_Str CLOB;
 l_Value RSP_COMMON.t_Value := RSP_COMMON.t_Value();
 l_Cnt INTEGER;
 l_Strv VARCHAR2(32000);
 l_tmp INTEGER := 1;
 l_pos INTEGER;
 begin
     SELECT substr(t.valclob, INSTR(t.valclob,chr(10))+1)||chr(10)
 INTO l_Str
 FROM tmp_xml t;

 l_pos:= INSTR(l_str, chr(10));

 WHILE l_pos > 0 LOOP
 l_strv := SUBSTR(l_str, l_tmp, l_pos - l_tmp);
 l_tmp:= l_pos + 1;
 l_Value := RSP_COMMON.ParseStr(p_Str => l_strv,
 p_Separator => ';');
 l_pos:= INSTR(l_str, chr(10), l_tmp);
 INSERT INTO TMP_FM_O04(ID_DEAL, ISIN, PRICE, VOL, KOD_SELL, KOD_BUY, DATE1,
                              TIME, PROFIT_USD, TYPE_BUY, TYPE_SELL, 
                              USER_BUY, USER_SELL, NO_BUY, NO_SELL, FEE_BUY, 
                              FEE_SELL, DATE2, COMM_BUY, COMM_SELL, 
                              FEE_NS_B, FEE_NS_S, PREM_BUY, 
                              PREM_SELL, PRICE_RUR, EXT_ID_B, EXT_ID_S, 
                              DATE_CLR, VAR_MARG_B, VAR_MARG_S, 
                              FEE_EX_B, VAT_EX_B, FEE_CC_B, 
                              VAT_CC_B, FEE_EX_S, VAT_EX_S, 
                              FEE_CC_S, VAT_CC_S, SIGNS_BUY, SIGNS_SELL, COUNTERPARTY, NCC_REQUEST_BUY, 
                              NCC_REQUEST_SELL)
 VALUES(l_Value(1), l_Value(2), to_number(l_Value(3), '9999999999999999.99999'), l_Value(4), l_Value(5), l_Value(6), to_date(l_Value(7), 'YYYY/MM/DD'),
 to_date('01.01.0001'||' '||l_Value(8), 'DD.MM.YYYY HH24:MI:SS'), to_number(l_Value(9), '99999999999999999999.9999'), l_Value(10), l_Value(11),
                l_Value(12), l_Value(13), l_Value(14), l_Value(15), to_number(l_Value(16), '9999999999999999.99999'), 
                to_number(l_Value(17), '9999999999999999.99999'), to_date(l_Value(18), 'DD.MM.YYYY'), l_Value(19), l_Value(20), 
 to_number(l_Value(21), '9999999999999999.99'), to_number(l_Value(22), '9999999999999999.99'), to_number(l_Value(23), '9999999999999999.99'),
                to_number(l_Value(24), '9999999999999999.99'), to_number(l_Value(25), '9999999999999999.99999'), l_Value(26), l_Value(27), 
                to_date(l_Value(28), 'DD.MM.YYYY'), to_number(l_Value(29), '9999999999999999.99999'), to_number(l_Value(30), '9999999999999999.99999'), 
 to_number(l_Value(31), '9999999999999999.99'), to_number(l_Value(32), '9999999999999999.99'), to_number(l_Value(33), '9999999999999999.99'),
 to_number(l_Value(34), '9999999999999999.99'), to_number(l_Value(35), '9999999999999999.99'), to_number(l_Value(36), '9999999999999999.99'),
                to_number(l_Value(37), '9999999999999999.99'), to_number(l_Value(38), '9999999999999999.99'), l_Value(39), l_Value(40), l_Value(41), l_Value(42), 
                l_Value(43));
 l_Value.DELETE;
 END LOOP;
 SELECT count(*)
 INTO l_Cnt
 FROM TMP_FM_o04;
 IF l_Cnt = 0 THEN
 p_Text := 'ошибка обработки элементов отчета срочного рынка МБ.';
 Return 0;
 ELSE
 Return l_Cnt;
 END IF;
 end Loado04;

 function Loadfpos (P_TEXT out varchar2) return INTEGER as
 /*Обработка отчета fpos.
 p_Text - информация о результате обработки.
 */
 l_Str CLOB;
 l_Value RSP_COMMON.t_Value := RSP_COMMON.t_Value();
 l_Cnt INTEGER;
 l_Strv VARCHAR2(32000);
 l_tmp INTEGER := 1;
 l_pos INTEGER;
 begin
 SELECT substr(t.valclob, INSTR(t.valclob,chr(10))+1)||chr(10)
 INTO l_Str
 FROM tmp_xml t;

 l_pos:= INSTR(l_str, chr(10));

 WHILE l_pos > 0 LOOP
 l_strv := SUBSTR(l_str, l_tmp, l_pos - l_tmp);
 l_tmp:= l_pos + 1;
 l_Value := RSP_COMMON.ParseStr(p_Str => l_strv,
 p_Separator => ';');
 l_pos:= INSTR(l_str, chr(10), l_tmp);
 INSERT INTO TMP_FM_FPOS(DATE_CLR, KOD, ACCOUNT, ISIN, POS_BEG, POS_END, VAR_MARG_P,
 VAR_MARG_D, SBOR,
 GO_NETTO, GO_BRUTTO,
 POS_EXEC, DU, SBOR_EXEC, SBOR_NOSYS,
 FEE_EXEC, FINE_EXEC,
 ACUUM_GO, FEE_TRANS,
 SBOR_EX, VAT_EX,
 SBOR_CC, VAT_CC, POS_FAILED,
 VAR_MARG_P_SETTL_PRICE, VAR_MARG_P_SWAP_RATE, VAR_MARG_D_SETTL_PRICE, VAR_MARG_D_SWAP_RATE )
 VALUES(to_date(l_Value(1), 'YYYY/MM/DD'), l_Value(2), l_Value(3), l_Value(4), l_Value(5), l_Value(6), to_number(l_Value(7), '99999999999999999999.99'),
 to_number(l_Value(8), '99999999999999999999.99'), to_number(l_Value(9), '99999999999999999999.99'),
 to_number(l_Value(10), '99999999999999999999.99'), to_number(l_Value(11), '99999999999999999999.99'),
 l_Value(12), l_Value(13), to_number(l_Value(14), '99999999999999999999.99'), to_number(l_Value(15), '9999999999999999.99'),
 to_number(l_Value(16), '9999999999999999.99'), to_number(l_Value(17), '99999999999999999999.99'),
 to_number(l_Value(18), '99999999999999999999.99'), to_number(l_Value(19), '99999999999999999999.99'),
 to_number(l_Value(20), '9999999999999999.99'), to_number(l_Value(21), '9999999999999999.99'),
 to_number(l_Value(22), '9999999999999999.99'), to_number(l_Value(23), '9999999999999999.99'), l_Value(24),
 to_number(l_Value(25), '9999999999999999.99'),to_number(l_Value(26), '9999999999999999.99'),to_number(l_Value(27), '9999999999999999.99'),to_number(l_Value(28), '9999999999999999.99') );
 l_Value.DELETE;
 END LOOP;
 SELECT count(*)
 INTO l_Cnt
 FROM TMP_FM_FPOS;
 IF l_Cnt = 0 THEN
 p_Text := 'ошибка обработки элементов отчета срочного рынка МБ.';
 Return 0;
 ELSE
 Return l_Cnt;
 END IF;
 end Loadfpos;

 function Loadopos (P_TEXT out varchar2) return INTEGER as
 /*Обработка отчета opos.
 p_Text - информация о результате обработки.
 */
 l_Str CLOB;
 l_Value RSP_COMMON.t_Value := RSP_COMMON.t_Value();
 l_Cnt INTEGER;
 l_Strv VARCHAR2(32000);
 l_tmp INTEGER := 1;
 l_pos INTEGER;
 begin
 SELECT substr(t.valclob, INSTR(t.valclob,chr(10))+1)||chr(10)
 INTO l_Str
 FROM tmp_xml t;

 l_pos:= INSTR(l_str, chr(10));

 WHILE l_pos > 0 LOOP
 l_strv := SUBSTR(l_str, l_tmp, l_pos - l_tmp);
 l_tmp:= l_pos + 1;
 l_Value := RSP_COMMON.ParseStr(p_Str => l_strv,
 p_Separator => ';');
 l_pos:= INSTR(l_str, chr(10), l_tmp);
 INSERT INTO TMP_FM_OPOS(DATE_CLR, KOD, ACCOUNT, ISIN, POS_BEG, POS_END, PREM,
 SBOR, GO,
 POS_EXEC, POS_ENDCIR, DU, SBOR_EXEC, SBOR_NOSYS,
 VAR_MARG_P, VAR_MARG_D, SBOR_EX,
 VAT_EX, SBOR_CC, VAT_CC)
 VALUES(to_date(l_Value(1), 'YYYY/MM/DD'), l_Value(2), l_Value(3), l_Value(4), l_Value(5), l_Value(6), to_number(l_Value(7), '99999999999999999999.99'),
 to_number(l_Value(8), '99999999999999999999.99'), to_number(l_Value(9), '99999999999999999999.99'),
 l_Value(10), l_Value(11), l_Value(12), to_number(l_Value(13), '99999999999999999999.99'), to_number(l_Value(14), '99999999999999999999.99'),
 to_number(l_Value(15), '9999999999999999.99'), to_number(l_Value(16), '9999999999999999.99'), to_number(l_Value(17), '99999999999999999999.99'),
 to_number(l_Value(18), '99999999999999999999.99'), to_number(l_Value(19), '99999999999999999999.99'), to_number(l_Value(20), '9999999999999999.99'));
 l_Value.DELETE;
 END LOOP;
 SELECT count(*)
 INTO l_Cnt
 FROM TMP_FM_OPOS;
 IF l_Cnt = 0 THEN
 p_Text := 'ошибка обработки элементов отчета срочного рынка МБ.';
 Return 0;
 ELSE
 Return l_Cnt;
 END IF;
 end Loadopos;

 function Loadfordlog (P_TEXT out varchar2) return INTEGER as
 /*Обработка отчета fordlog.
 p_Text - информация о результате обработки.
 */
 l_Str CLOB;
 l_Value RSP_COMMON.t_Value := RSP_COMMON.t_Value();
 l_Cnt INTEGER;
 l_Strv VARCHAR2(32000);
 l_tmp INTEGER := 1;
 l_pos INTEGER;
 begin
 SELECT substr(t.valclob, INSTR(t.valclob,chr(10))+1)||chr(10)
 INTO l_Str
 FROM tmp_xml t;

 l_pos:= INSTR(l_str, chr(10));

 WHILE l_pos > 0 LOOP
 l_strv := SUBSTR(l_str, l_tmp, l_pos - l_tmp);
 l_tmp:= l_pos + 1;
 l_Value := RSP_COMMON.ParseStr(p_Str => l_strv,
 p_Separator => ';');
 l_pos:= INSTR(l_str, chr(10), l_tmp);
 INSERT INTO TMP_FM_FORDLOG(N, ID_ORD, SESS_ID, ISIN, AMOUNT, AMOUNT_REST, ID_DEAL, XSTATUS, PRICE,
 MOMENT, DIR, ACTION, REMOVE_TYPE, DEAL_PRICE,
 CLIENT_CODE, LOGIN_FROM, COMENT, EXT_ID, BROKER_TO, BROKER_TO_RTS, BROKER_FROM_RTS, DATE_EXP,
 ID_ORD_FIRST, ASPREF, ASP)
 --, PRIVATE_ORDER_ID, PRIVATE_AMOUNT, PRIVATE_AMOUNT_REST, MM, ACTUALMM)
 VALUES(l_Value(1), l_Value(2), l_Value(3), l_Value(4), l_Value(5), l_Value(6), l_Value(7), l_Value(8), to_number(l_Value(9), '9999999999999999.99999'),
 to_timestamp(l_Value(10), 'YYYY-MM-DD HH24:MI:SS.FF'), l_Value(11), l_Value(12), l_Value(13), to_number(l_Value(14), '9999999999999999.99999'),
 l_Value(15), l_Value(16), l_Value(17), l_Value(18), l_Value(19), l_Value(20), l_Value(21), to_timestamp(l_Value(22), 'YYYY-MM-DD HH24:MI:SS.FF'),
 l_Value(23), l_Value(24), l_Value(25));
-- , l_Value(26), l_Value(27), l_Value(28)), l_Value(29), l_Value(30));
 l_Value.DELETE;
 END LOOP;
 SELECT count(*)
 INTO l_Cnt
 FROM TMP_FM_FORDLOG;
 IF l_Cnt = 0 THEN
 p_Text := 'ошибка обработки элементов отчета срочного рынка МБ.';
 Return 0;
 ELSE
 Return l_Cnt;
 END IF;
 end Loadfordlog;

 function Loadoordlog (P_TEXT out varchar2) return INTEGER as
 /*Обработка отчета oordlog.
 p_Text - информация о результате обработки.
 */
 l_Str CLOB;
 l_Value RSP_COMMON.t_Value := RSP_COMMON.t_Value();
 l_Cnt INTEGER;
 l_Strv VARCHAR2(32000);
 l_tmp INTEGER := 1;
 l_pos INTEGER;
 begin
 SELECT substr(t.valclob, INSTR(t.valclob,chr(10))+1)||chr(10)
 INTO l_Str
 FROM tmp_xml t;

 l_pos:= INSTR(l_str, chr(10));

 WHILE l_pos > 0 LOOP
 l_strv := SUBSTR(l_str, l_tmp, l_pos - l_tmp);
 l_tmp:= l_pos + 1;
 l_Value := RSP_COMMON.ParseStr(p_Str => l_strv,
 p_Separator => ';');
 l_pos:= INSTR(l_str, chr(10), l_tmp);
 INSERT INTO TMP_FM_OORDLOG(N, ID_ORD, SESS_ID, ISIN, AMOUNT, AMOUNT_REST, ID_DEAL, XSTATUS, PRICE,
 MOMENT, DIR, ACTION, REMOVE_TYPE, DEAL_PRICE,
 CLIENT_CODE, LOGIN_FROM, COMENT, EXT_ID, BROKER_TO, BROKER_TO_RTS, BROKER_FROM_RTS, DATE_EXP,
 ID_ORD_FIRST, ASPREF, ASP)
 --, PRIVATE_ORDER_ID, PRIVATE_AMOUNT, PRIVATE_AMOUNT_REST, MM, ACTUALMM)
 VALUES(l_Value(1), l_Value(2), l_Value(3), l_Value(4), l_Value(5), l_Value(6), l_Value(7), l_Value(8), to_number(l_Value(9), '9999999999999999.99999'),
 to_timestamp(l_Value(10), 'YYYY-MM-DD HH24:MI:SS.FF'), l_Value(11), l_Value(12), l_Value(13), to_number(l_Value(14), '9999999999999999.99999'),
 l_Value(15), l_Value(16), l_Value(17), l_Value(18), l_Value(19), l_Value(20), l_Value(21), to_timestamp(l_Value(22), 'YYYY-MM-DD HH24:MI:SS.FF'),
 l_Value(23), l_Value(24), l_Value(25));
-- , l_Value(26), l_Value(27), l_Value(28)), l_Value(29), l_Value(30));
 l_Value.DELETE;
 END LOOP;
 SELECT count(*)
 INTO l_Cnt
 FROM TMP_FM_OORDLOG;
 IF l_Cnt = 0 THEN
 p_Text := 'ошибка обработки элементов отчета срочного рынка МБ.';
 Return 0;
 ELSE
 Return l_Cnt;
 END IF;
 end Loadoordlog;

 function Loadf07 (P_TEXT out varchar2) return INTEGER as
 /*Обработка отчета f07.
 p_Text - информация о результате обработки.
 */
 l_Str CLOB;
 l_Value RSP_COMMON.t_Value := RSP_COMMON.t_Value();
 l_Cnt INTEGER;
 l_Strv VARCHAR2(32000);
 l_tmp INTEGER := 1;
 l_pos INTEGER;
 begin
 SELECT substr(t.valclob, INSTR(t.valclob,chr(10))+1)||chr(10)
 INTO l_Str
 FROM tmp_xml t;

 l_pos:= INSTR(l_str, chr(10));

 WHILE l_pos > 0 LOOP
 l_strv := SUBSTR(l_str, l_tmp, l_pos - l_tmp);
 l_tmp:= l_pos + 1;
 l_Value := RSP_COMMON.ParseStr(p_Str => l_strv,
 p_Separator => ';');
 l_pos:= INSTR(l_str, chr(10), l_tmp);
 INSERT INTO TMP_FM_F07(DATE1, CONTRACT, EXECUTION, VOLUME, VOL_RUBL,
 LOW, HIGH, OPEN,
 CLOSE, SETTL, TRADES, INTEREST,
 FEE, TICK_PRICE, TICK,
 AVRG, POSES_RUBL, LIMIT,
 RISK_WR, COFFOUT, BASE_FUT, IS_SPREAD, NAME,
 DATE2, EXECUTION2, DEPOSIT, IS_PERCENT,
 SETTL_RUR, LOT_VOLUME, TICK_PR_GO,
 LIMIT_L1, PR_SETTL, PR_SETTL_R,
 TYPE_EXEC, SECTION, SPOT, BASE, TYPE_SBOR, NS_VOLUME, NS_TRADES, NS_FEE,
 NS_VOLRUBL, L_TRADEDAY, MULTILEG, DEPOSIT_BUY,
 DEPOSIT_SELL)
 VALUES(to_date(l_Value(1), 'YYYY/MM/DD'), l_Value(2), to_date(l_Value(3), 'YYYY/MM/DD'), l_Value(4), to_number(l_Value(5), '99999999999999999.99'),
 to_number(l_Value(6), '9999999999999999.99999'), to_number(l_Value(7), '9999999999999999.99999'), to_number(l_Value(8), '9999999999999999.99999'),
 to_number(l_Value(9), '9999999999999999.99999'), to_number(l_Value(10), '9999999999999999.99999'), l_Value(11), l_Value(12),
 to_number(l_Value(13), '9999999999999999.99999'), to_number(l_Value(14), '9999999999999999.99999'), to_number(l_Value(15), '9999999999999999.99999'),
 to_number(l_Value(16), '9999999999999999.99999'), to_number(l_Value(17), '99999999999999999.99'), to_number(l_Value(18), '9999999999999999.99999'),
 to_number(l_Value(19), '9999999999999999.99999'), to_number(l_Value(20), '9999999.99999'), l_Value(21), l_Value(22), l_Value(23),
 to_date(l_Value(24), 'DD.MM.YYYY'), to_date(l_Value(25), 'DD.MM.YYYY'), to_number(l_Value(26), '9999999999999999.99999'), l_Value(27),
 to_number(l_Value(28), '9999999999999999.99999'), l_Value(29), to_number(l_Value(30), '9999999999999999.99999'),
 to_number(l_Value(31), '9999999999999999.99999'), to_number(l_Value(32), '9999999999999999.99999'), to_number(l_Value(33), '9999999999999999.99999'),
 l_Value(34), l_Value(35), l_Value(36), l_Value(37), l_Value(38), l_Value(39), l_Value(40), to_number(l_Value(41), '9999999999999999.99999'),
 to_number(l_Value(42), '9999999999999999.99999'), l_Value(43), l_Value(44), to_number(l_Value(45), '9999999999999999.99999'),
 to_number(l_Value(46), '9999999999999999.99999') );
 l_Value.DELETE;
 END LOOP;
 SELECT count(*)
 INTO l_Cnt
 FROM TMP_FM_F07;
 IF l_Cnt = 0 THEN
 p_Text := 'ошибка обработки элементов отчета срочного рынка МБ.';
 Return 0;
 ELSE
 Return l_Cnt;
 END IF;
 end Loadf07;

 function Loado07 (P_TEXT out varchar2) return INTEGER as
 /*Обработка отчета o07.
 p_Text - информация о результате обработки.
 */
 l_Str CLOB;
 l_Value RSP_COMMON.t_Value := RSP_COMMON.t_Value();
 l_Cnt INTEGER;
 l_Strv VARCHAR2(32000);
 l_tmp INTEGER := 1;
 l_pos INTEGER;
 begin
 SELECT substr(t.valclob, INSTR(t.valclob,chr(10))+1)||chr(10)
 INTO l_Str
 FROM tmp_xml t;

 l_pos:= INSTR(l_str, chr(10));

 WHILE l_pos > 0 LOOP
 l_strv := SUBSTR(l_str, l_tmp, l_pos - l_tmp);
 l_tmp:= l_pos + 1;
 l_Value := RSP_COMMON.ParseStr(p_Str => l_strv,
 p_Separator => ';');
 l_pos:= INSTR(l_str, chr(10), l_tmp);
 INSERT INTO TMP_FM_O07(DATE1, CONTRACT, EXECUTION, VOLUME, VOL_RUBL,
 LOW, HIGH, OPEN,
 CLOSE, AVRG, TRADES, INTEREST,
 FEE, TICK_PRICE, TICK,
 POSES_RUBL, DEPO_UNCOV, DEPO_COV,
 FUT_CONTR, STRIKE, PUT, EVROP, DATE2,
 EXECUTION2, NAME, CLOSE_TIME,
 VOLAT, THEORPRICE, TICK_PR_GO,
 PR_VOLAT, PR_THEORPR, FUT_TYPE,
 BASEGOBUY)
 VALUES(to_date(l_Value(1), 'YYYY/MM/DD'), l_Value(2), to_date(l_Value(3), 'YYYY/MM/DD'), l_Value(4), to_number(l_Value(5), '99999999999999999.99'),
 to_number(l_Value(6), '9999999999999999.99999'), to_number(l_Value(7), '9999999999999999.99999'), to_number(l_Value(8), '9999999999999999.99999'),
 to_number(l_Value(9), '9999999999999999.99999'), to_number(l_Value(10), '9999999999999999.99999'), l_Value(11), l_Value(12),
 to_number(l_Value(13), '9999999999999999.99999'), to_number(l_Value(14), '9999999999999999.99999'), to_number(l_Value(15), '9999999999999999.99999'),
 to_number(l_Value(16), '99999999999999999.99'), to_number(l_Value(17), '9999999999999999.99999'), to_number(l_Value(18), '9999999999999999.99999'),
 l_Value(19), to_number(l_Value(20), '9999999999999999.99999'), l_Value(21), l_Value(22), to_date(l_Value(23), 'DD.MM.YYYY'),
 to_date(l_Value(24), 'DD.MM.YYYY'), l_Value(25), to_date('01.01.0001'||' '||l_Value(26), 'DD.MM.YYYY HH24:MI:SS'),
 to_number(l_Value(27), '9999999999999999.99999'), to_number(l_Value(28), '9999999999999999.99999'), to_number(l_Value(29), '9999999999999999.99999'),
 to_number(l_Value(30), '9999999999999999.99999'), to_number(l_Value(31), '9999999999999999.99999'), l_Value(32),
 to_number(l_Value(33), '9999999999999999.99999'));
 l_Value.DELETE;
 END LOOP;
 SELECT count(*)
 INTO l_Cnt
 FROM TMP_FM_O07;
 IF l_Cnt = 0 THEN
 p_Text := 'ошибка обработки элементов отчета срочного рынка МБ.';
 Return 0;
 ELSE
 Return l_Cnt;
 END IF;
 end Loado07;

 function Loadpay (P_TEXT out varchar2) return INTEGER as
 /*Обработка отчета pay.
 p_Text - информация о результате обработки.
 */
 l_Str CLOB;
 l_Value RSP_COMMON.t_Value := RSP_COMMON.t_Value();
 l_Cnt INTEGER;
 l_Strv VARCHAR2(32000);
 l_tmp INTEGER := 1;
 l_pos INTEGER;
 begin
 SELECT substr(t.valclob, INSTR(t.valclob,chr(10))+1)||chr(10)
 INTO l_Str
 FROM tmp_xml t;

 l_pos:= INSTR(l_str, chr(10));

 WHILE l_pos > 0 LOOP
 l_strv := SUBSTR(l_str, l_tmp, l_pos - l_tmp);
 l_tmp:= l_pos + 1;
 l_Value := RSP_COMMON.ParseStr(p_Str => l_strv,
 p_Separator => ';');
 l_pos:= INSTR(l_str, chr(10), l_tmp);
 INSERT INTO TMP_FM_PAY(DATE_CLEARING, KOD, ACCOUNT, TYPE, ID_PAY, TYPE_PAY, PAY,
 NAME, COMMENTAR, DU, PAYER, INN, BIK, PURPOSE)
 VALUES(to_date(l_Value(1), 'YYYY/MM/DD'), l_Value(2), l_Value(3), l_Value(4), l_Value(5), l_Value(6), to_number(l_Value(7), '9999999999999999.99'),
 l_Value(8), l_Value(9), l_Value(10), l_Value(11), l_Value(12), l_Value(13), l_Value(14));
 l_Value.DELETE;
 END LOOP;
 SELECT count(*)
 INTO l_Cnt
 FROM TMP_FM_PAY;
 IF l_Cnt = 0 THEN
 p_Text := 'ошибка обработки элементов отчета срочного рынка МБ.';
 Return 0;
 ELSE
 Return l_Cnt;
 END IF;
 end Loadpay;

 function Loadmon (P_TEXT out varchar2) return INTEGER as
 /*Обработка отчета pay.
 p_Text - информация о результате обработки.
 */
 l_Str CLOB;
 l_Value RSP_COMMON.t_Value := RSP_COMMON.t_Value();
 l_Cnt INTEGER;
 l_Strv VARCHAR2(32000);
 l_tmp INTEGER := 1;
 l_pos INTEGER;
 begin
 SELECT substr(t.valclob, INSTR(t.valclob,chr(10))+1)||chr(10)
 INTO l_Str
 FROM tmp_xml t;

 l_pos:= INSTR(l_str, chr(10));

 WHILE l_pos > 0 LOOP
 l_strv := SUBSTR(l_str, l_tmp, l_pos - l_tmp);
 l_tmp:= l_pos + 1;
 l_Value := RSP_COMMON.ParseStr(p_Str => l_strv,
 p_Separator => ';');
 l_pos:= INSTR(l_str, chr(10), l_tmp);
 INSERT INTO TMP_FM_MON(DATE_CLEARING, KOD, ACCOUNT, TYPE, AMOUNT_BEG,
 VAR_MARG, PREM, PAY,
 FUT_SBOR, OPT_SBOR, NOV, GO,
 AMOUNT_END, FREE, DU,
 GOWIDE, FREEWIDE, MARGINCALL,
 SBOR_EX, VAT_EX, SBOR_CC,
 VAT_CC, RUB_BEG, RUB_PAY,
 RUB_END, COM_PL_BEG, COM_PL_PAY,
 COM_PL_PREM, COM_PL_END, EXT_REZ)
 VALUES(to_date(l_Value(1), 'YYYY/MM/DD'), l_Value(2), l_Value(3), l_Value(4), to_number(l_Value(5), '9999999999999999.99'),
 to_number(l_Value(6), '9999999999999999.99'), to_number(l_Value(7), '9999999999999999.99'), to_number(l_Value(8), '9999999999999999.99'),
 to_number(l_Value(9), '9999999999999999.99'), to_number(l_Value(10), '9999999999999999.99'), to_number(l_Value(11), '9999999999999999.99'), to_number(l_Value(12), '9999999999999999.99'),
 to_number(l_Value(13), '9999999999999999.99'), to_number(l_Value(14), '9999999999999999.99'), l_Value(15),
 to_number(l_Value(16), '9999999999999999.99'), to_number(l_Value(17), '9999999999999999.99'), l_Value(18),
 to_number(l_Value(19), '9999999999999999.99'), to_number(l_Value(20), '9999999999999999.99'), to_number(l_Value(21), '9999999999999999.99'),
 to_number(l_Value(22), '9999999999999999.99'), to_number(l_Value(23), '9999999999999999.99'), to_number(l_Value(24), '9999999999999999.99'),
 to_number(l_Value(25), '9999999999999999.99'), to_number(l_Value(26), '9999999999999999.99'), to_number(l_Value(27), '9999999999999999.99'),
 to_number(l_Value(28), '9999999999999999.99'), to_number(l_Value(29), '9999999999999999.99'), to_number(l_Value(30), '99999999999999999999.99'));
 l_Value.DELETE;
 END LOOP;
 SELECT count(*)
 INTO l_Cnt
 FROM TMP_FM_MON;
 IF l_Cnt = 0 THEN
 p_Text := 'ошибка обработки элементов отчета срочного рынка МБ.';
 Return 0;
 ELSE
 Return l_Cnt;
 END IF;
 end Loadmon;

 function Load_File (p_FILENAME in varchar2, p_FileType in varchar2, p_Msg out varchar2) return INTEGER as
 /*Основная функция загрузки полученных отчетов МБ.
 p_FileName - имя принятого файла.
 Возвращает информацию о результате загрузки и обработки.
 */
 l_Rows INTEGER;
 l_Text VARCHAR2(4000);
 begin
 p_Msg := 'Прием файла МБ '||p_FILENAME||': ';
 l_Rows := RSP_FMARKET.LOADREQUISITES(p_FileType, l_Text);
 IF p_FileType = 'f04' THEN
 l_Rows := RSP_FMARKET.Loadf04(l_Text);
 p_Msg := p_Msg||l_Text;
 END IF;
 IF p_FileType = 'o04' THEN
 l_Rows := RSP_FMARKET.Loado04(l_Text);
 p_Msg := p_Msg||l_Text;
 END IF;
 IF p_FileType = 'fpos' THEN
 l_Rows := RSP_FMARKET.Loadfpos(l_Text);
 p_Msg := p_Msg||l_Text;
 END IF;
 IF p_FileType = 'opos' THEN
 l_Rows := RSP_FMARKET.Loadopos(l_Text);
 p_Msg := p_Msg||l_Text;
 END IF;
 IF p_FileType = 'fordlog' THEN
 l_Rows := RSP_FMARKET.Loadfordlog(l_Text);
 p_Msg := p_Msg||l_Text;
 END IF;
 IF p_FileType = 'oordlog' THEN
 l_Rows := RSP_FMARKET.Loadoordlog(l_Text);
 p_Msg := p_Msg||l_Text;
 END IF;
 IF p_FileType = 'f07' THEN
 l_Rows := RSP_FMARKET.Loadf07(l_Text);
 p_Msg := p_Msg||l_Text;
 END IF;
 IF p_FileType = 'o07' THEN
 l_Rows := RSP_FMARKET.Loado07(l_Text);
 p_Msg := p_Msg||l_Text;
 END IF;
 IF p_FileType = 'pay' THEN
 l_Rows := RSP_FMARKET.Loadpay(l_Text);
 p_Msg := p_Msg||l_Text;
 END IF;
 IF p_FileType = 'mon' THEN
 l_Rows := RSP_FMARKET.Loadmon(l_Text);
 p_Msg := p_Msg||l_Text;
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

 function Insert_f04 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
 /*Обработка отчета f04.
 p_IDReg - ID записи отчета
 p_IDProcLog - ID записи процессинга
 */
 l_Cnt INTEGER;
 begin
 INSERT INTO fm_f04_tech(id_mb_requisites)
 VALUES(p_IDReg);
     INSERT INTO fm_f04(id_mb_requisites, id_processing_log, id_deal, isin, price, vol, kod_sell, kod_buy, date1, time, profit_usd, type_buy, type_sell, 
                        var_marg_b, var_marg_s, user_sell, user_buy, no_buy, no_sell, fee_buy, fee_sell, date2, comm_buy, comm_sell, fee_ns_b, 
                        fee_ns_s, price_rur, ext_id_b, ext_id_s, date_clr, repo_id, fee_ex_b, vat_ex_b, fee_cc_b, vat_cc_b, fee_ex_s, vat_ex_s, 
                        fee_cc_s, vat_cc_s, id_mult, signs_buy, signs_sell, counterparty, ncc_request_buy, ncc_request_sell, var_marg_b_settl_price, var_marg_b_swap_rate, var_marg_s_settl_price, var_marg_s_swap_rate)
       SELECT p_IDReg, p_IDProcLog, t.id_deal, t.isin, t.price, t.vol, t.kod_sell, t.kod_buy, t.date1, t.time, t.profit_usd, t.type_buy, t.type_sell, 
              t.var_marg_b, t.var_marg_s, t.user_sell, t.user_buy, t.no_buy, t.no_sell, t.fee_buy, t.fee_sell, t.date2, t.comm_buy, t.comm_sell, t.fee_ns_b, 
              t.fee_ns_s, t.price_rur, t.ext_id_b, t.ext_id_s, t.date_clr, t.repo_id, t.fee_ex_b, t.vat_ex_b, t.fee_cc_b, t.vat_cc_b, t.fee_ex_s, t.vat_ex_s, 
              t.fee_cc_s, t.vat_cc_s, t.id_mult, t.signs_buy, t.signs_sell, t.counterparty, t.ncc_request_buy, t.ncc_request_sell, var_marg_b_settl_price, var_marg_b_swap_rate, var_marg_s_settl_price, var_marg_s_swap_rate
 FROM tmp_fm_f04 t;
 l_Cnt := SQL%ROWCOUNT;
 Return l_Cnt;
 end Insert_f04;

 function Insert_o04 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
 /*Обработка отчета o04.
 p_IDReg - ID записи отчета
 p_IDProcLog - ID записи процессинга
 */
 l_Cnt INTEGER;
 begin
 INSERT INTO fm_o04_tech(id_mb_requisites)
 VALUES(p_IDReg);
     INSERT INTO fm_o04(id_mb_requisites, id_processing_log, id_deal, isin, price, vol, kod_sell, kod_buy, date1, time, profit_usd, type_buy, type_sell, 
                        user_buy, user_sell, no_buy, no_sell, fee_buy, fee_sell, date2, comm_buy, comm_sell, fee_ns_b, fee_ns_s, prem_buy, 
                        prem_sell, price_rur, ext_id_b, ext_id_s, date_clr, var_marg_b, var_marg_s, fee_ex_b, vat_ex_b, fee_cc_b, vat_cc_b, 
                        fee_ex_s, vat_ex_s, fee_cc_s, vat_cc_s, signs_buy, signs_sell, counterparty, ncc_request_buy, ncc_request_sell)
       SELECT p_IDReg, p_IDProcLog, t.id_deal, t.isin, t.price, t.vol, t.kod_sell, t.kod_buy, t.date1, t.time, t.profit_usd, t.type_buy, t.type_sell, 
              t.user_buy, t.user_sell, t.no_buy, t.no_sell, t.fee_buy, t.fee_sell, t.date2, t.comm_buy, t.comm_sell, t.fee_ns_b, t.fee_ns_s, t.prem_buy, 
              t.prem_sell, t.price_rur, t.ext_id_b, t.ext_id_s, t.date_clr, t.var_marg_b, t.var_marg_s, t.fee_ex_b, t.vat_ex_b, t.fee_cc_b, t.vat_cc_b, 
              t.fee_ex_s, t.vat_ex_s, t.fee_cc_s, t.vat_cc_s, t.signs_buy, t.signs_sell, t.counterparty, t.ncc_request_buy, t.ncc_request_sell
 FROM tmp_fm_o04 t;
 l_Cnt := SQL%ROWCOUNT;
 Return l_Cnt;
 end Insert_o04;

 function Insert_fpos (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
 /*Обработка отчета fpos.
 p_IDReg - ID записи отчета
 p_IDProcLog - ID записи процессинга
 */
 l_Cnt INTEGER;
 begin
 INSERT INTO fm_fpos_tech(id_mb_requisites)
 VALUES(p_IDReg);
 INSERT INTO fm_fpos(id_mb_requisites, id_processing_log, date_clr, kod, account, isin, pos_beg, pos_end, var_marg_p, var_marg_d, sbor, go_netto, go_brutto,
 pos_exec, du, sbor_exec, sbor_nosys, fee_exec, fine_exec, acuum_go, fee_trans, sbor_ex, vat_ex, sbor_cc, vat_cc, pos_failed, var_marg_p_settl_price, var_marg_p_swap_rate, var_marg_d_settl_price, var_marg_d_swap_rate)
 SELECT p_IDReg, p_IDProcLog, date_clr, kod, account, isin, pos_beg, pos_end, var_marg_p, var_marg_d, sbor, go_netto, go_brutto,
              pos_exec, du, sbor_exec, sbor_nosys, fee_exec, fine_exec, acuum_go, fee_trans, sbor_ex, vat_ex, sbor_cc, vat_cc, pos_failed, var_marg_p_settl_price, var_marg_p_swap_rate, var_marg_d_settl_price, var_marg_d_swap_rate
         FROM tmp_fm_fpos;
     l_Cnt := SQL%ROWCOUNT;
     Return l_Cnt;
   end Insert_fpos;

   function Insert_opos (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета opos.
     p_IDReg - ID записи отчета
     p_IDProcLog - ID записи процессинга
   */
     l_Cnt INTEGER;
   begin
     INSERT INTO fm_opos_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO fm_opos(id_mb_requisites, id_processing_log, date_clr, kod, account, isin, pos_beg, pos_end, prem, sbor, go, pos_exec, pos_endcir, du,
                         sbor_exec, sbor_nosys, var_marg_p, var_marg_d, sbor_ex, vat_ex, sbor_cc, vat_cc)
       SELECT p_IDReg, p_IDProcLog, date_clr, kod, account, isin, pos_beg, pos_end, prem, sbor, go, pos_exec, pos_endcir, du,
              sbor_exec, sbor_nosys, var_marg_p, var_marg_d, sbor_ex, vat_ex, sbor_cc, vat_cc
         FROM tmp_fm_opos;
     l_Cnt := SQL%ROWCOUNT;
     Return l_Cnt;
   end Insert_opos;

   function Insert_fordlog (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета fordlog.
     p_IDReg - ID записи отчета
     p_IDProcLog - ID записи процессинга
   */
     l_Cnt INTEGER;
   begin
     INSERT INTO fm_fordlog_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO fm_fordlog(id_mb_requisites, id_processing_log, n, id_ord, sess_id, isin, amount, amount_rest, id_deal, xstatus, price, moment, dir, action,
                            remove_type, deal_price, client_code, login_from, coment, ext_id, broker_to, broker_to_rts, broker_from_rts, date_exp,
                            id_ord_first, aspref, asp, private_amount, private_amount_rest, private_order_id, mm, actualmm)
       SELECT p_IDReg, p_IDProcLog, t.n, t.id_ord, t.sess_id, t.isin, t.amount, t.amount_rest, t.id_deal, t.xstatus, t.price, t.moment, t.dir, t.action,
              t.remove_type, t.deal_price, t.client_code, t.login_from, t.coment, t.ext_id, t.broker_to, t.broker_to_rts, t.broker_from_rts, t.date_exp,
              t.id_ord_first, t.aspref, t.asp, t.private_amount, t.private_amount_rest, t.private_order_id, t.mm, t.actualmm
         FROM tmp_fm_fordlog t;
     l_Cnt := SQL%ROWCOUNT;
     Return l_Cnt;
   end Insert_fordlog;

   function Insert_oordlog (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета oordlog.
     p_IDReg - ID записи отчета
     p_IDProcLog - ID записи процессинга
   */
     l_Cnt INTEGER;
   begin
     INSERT INTO fm_oordlog_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO fm_oordlog(id_mb_requisites, id_processing_log, n, id_ord, sess_id, isin, amount, amount_rest, id_deal, xstatus, price, moment, dir, action,
                            remove_type, deal_price, client_code, login_from, coment, ext_id, broker_to, broker_to_rts, broker_from_rts, date_exp,
                            id_ord_first, aspref, asp, private_amount, private_amount_rest, private_order_id, mm, actualmm)
       SELECT p_IDReg, p_IDProcLog, t.n, t.id_ord, t.sess_id, t.isin, t.amount, t.amount_rest, t.id_deal, t.xstatus, t.price, t.moment, t.dir, t.action,
              t.remove_type, t.deal_price, t.client_code, t.login_from, t.coment, t.ext_id, t.broker_to, t.broker_to_rts, t.broker_from_rts, t.date_exp,
              t.id_ord_first, t.aspref, t.asp, t.private_amount, t.private_amount_rest, t.private_order_id, t.mm, t.actualmm
         FROM tmp_fm_oordlog t;
     l_Cnt := SQL%ROWCOUNT;
     Return l_Cnt;
   end Insert_oordlog;

   function Insert_f07 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета f07.
     p_IDReg - ID записи отчета
     p_IDProcLog - ID записи процессинга
   */
     l_Cnt INTEGER;
   begin
     INSERT INTO fm_f07_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO fm_f07(id_mb_requisites, id_processing_log, date1, contract, execution, volume, vol_rubl, low, high, open, close, settl, trades, interest, fee,
                        tick_price, tick, avrg, poses_rubl, limit, risk_wr, coffout, base_fut, is_spread, name, date2, execution2, deposit,
                        is_percent, settl_rur, lot_volume, tick_pr_go, limit_l1, pr_settl, pr_settl_r, type_exec, section, spot, base, type_sbor,
                        ns_volume, ns_trades, ns_fee, ns_volrubl, l_tradeday, multileg, deposit_buy, deposit_sell)
       SELECT p_IDReg, p_IDProcLog, t.date1, t.contract, t.execution, t.volume, t.vol_rubl, t.low, t.high, t.open, t.close, t.settl, t.trades, t.interest, t.fee,
              t.tick_price, t.tick, t.avrg, t.poses_rubl, t.limit, t.risk_wr, t.coffout, t.base_fut, t.is_spread, t.name, t.date2, t.execution2, t.deposit,
              t.is_percent, t.settl_rur, t.lot_volume, t.tick_pr_go, t.limit_l1, t.pr_settl, t.pr_settl_r, t.type_exec, t.section, t.spot, t.base, t.type_sbor,
              t.ns_volume, t.ns_trades, t.ns_fee, t.ns_volrubl, t.l_tradeday, t.multileg, t.deposit_buy, t.deposit_sell
         FROM tmp_fm_f07 t;
     l_Cnt := SQL%ROWCOUNT;
     Return l_Cnt;
   end Insert_f07;

   function Insert_o07 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета o07.
     p_IDReg - ID записи отчета
     p_IDProcLog - ID записи процессинга
   */
     l_Cnt INTEGER;
   begin
     INSERT INTO fm_o07_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO fm_o07(id_mb_requisites, id_processing_log, date1, contract, execution, volume, vol_rubl, low, high, open, close, avrg, trades, interest, fee,
                        tick_price, tick, poses_rubl, depo_uncov, depo_cov, fut_contr, strike, put, evrop, date2, execution2, name, close_time,
                        volat, theorprice, tick_pr_go, pr_volat, pr_theorpr, fut_type, basegobuy)
       SELECT p_IDReg, p_IDProcLog, t.date1, t.contract, t.execution, t.volume, t.vol_rubl, t.low, t.high, t.open, t.close, t.avrg, t.trades, t.interest, t.fee,
              t.tick_price, t.tick, t.poses_rubl, t.depo_uncov, t.depo_cov, t.fut_contr, t.strike, t.put, t.evrop, t.date2, t.execution2, t.name, t.close_time,
              t.volat, t.theorprice, t.tick_pr_go, t.pr_volat, t.pr_theorpr, t.fut_type, t.basegobuy
         FROM tmp_fm_o07 t;
     l_Cnt := SQL%ROWCOUNT;
     Return l_Cnt;
   end Insert_o07;

   function Insert_pay (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета pay.
     p_IDReg - ID записи отчета
     p_IDProcLog - ID записи процессинга
   */
     l_Cnt INTEGER;
   begin
     INSERT INTO fm_pay_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO fm_pay(id_mb_requisites, id_processing_log, date_clearing, kod, account, type, id_pay, type_pay, pay, name, commentar, du,
                        payer, inn, bik, purpose)
       SELECT p_IDReg, p_IDProcLog, t.date_clearing, t.kod, t.account, t.type, t.id_pay, t.type_pay, t.pay, t.name, t.commentar, t.du,
              t.payer, t.inn, t.bik, t.purpose
         FROM tmp_fm_pay t;
     l_Cnt := SQL%ROWCOUNT;
     Return l_Cnt;
   end Insert_pay;

   function Insert_mon (p_IDReg in integer, p_IDProcLog in integer) return INTEGER as
   /*Обработка отчета mon.
     p_IDReg - ID записи отчета
     p_IDProcLog - ID записи процессинга
   */
     l_Cnt INTEGER;
   begin
     INSERT INTO fm_mon_tech(id_mb_requisites)
       VALUES(p_IDReg);
     INSERT INTO fm_mon(id_mb_requisites, id_processing_log, date_clearing, kod, account, type, amount_beg, var_marg, prem, pay, fut_sbor, opt_sbor, nov, go,
                        amount_end, free, du, gowide, freewide, margincall, sbor_ex, vat_ex, sbor_cc, vat_cc, rub_beg, rub_pay,
                        rub_end, com_pl_beg, com_pl_pay, com_pl_prem, com_pl_end, ext_rez)
       SELECT p_IDReg, p_IDProcLog, t.date_clearing, t.kod, t.account, t.type, t.amount_beg, t.var_marg, t.prem, t.pay, t.fut_sbor, t.opt_sbor, t.nov, t.go,
              t.amount_end, t.free, t.du, t.gowide, t.freewide, t.margincall, t.sbor_ex, t.vat_ex, t.sbor_cc, t.vat_cc, t.rub_beg, t.rub_pay,
              t.rub_end, t.com_pl_beg, t.com_pl_pay, t.com_pl_prem, t.com_pl_end, t.ext_rez
         FROM tmp_fm_mon t;
     l_Cnt := SQL%ROWCOUNT;
     Return l_Cnt;
   end Insert_mon;

   function Insert_Table (p_FILENAME in varchar2, p_FileType in varchar2, p_IDProcLog in integer, p_Date in date default null) return INTEGER as
  /*Основная функция загрузки полученных отчетов МБ.
    p_FileName - имя принятого файла,
    p_FileType - тип файла.
    Возвращает информацию о результате загрузки и обработки.
  */
    l_IDReq   INTEGER;
    l_IdNForm INTEGER;
  begin
    IF p_FileType = 'f04' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Срочный рынок\Формы\f04'));
    ELSIF p_FileType = 'o04' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Срочный рынок\Формы\o04'));
    ELSIF p_FileType = 'fpos' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Срочный рынок\Формы\fpos'));
    ELSIF p_FileType = 'opos' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Срочный рынок\Формы\opos'));
    ELSIF p_FileType = 'fordlog' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Срочный рынок\Формы\fordlog'));
    ELSIF p_FileType = 'oordlog' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Срочный рынок\Формы\oordlog'));
    ELSIF p_FileType = 'f07' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Срочный рынок\Формы\f07'));
    ELSIF p_FileType = 'o07' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Срочный рынок\Формы\o07'));
    ELSIF p_FileType = 'pay' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Срочный рынок\Формы\pay'));
    ELSIF p_FileType = 'mon' THEN
      l_IdNForm := RSP_COMMON.GET_ID_BY_CODE_DICT('NFORM', RSP_SETUP.Get_Value(P_PATH => 'Срочный рынок\Формы\mon'));
    END IF;
    l_IDReq := RSP_FMARKET.Insert_Requisites(p_FILENAME, p_IDProcLog, l_IdNForm, p_Date);
    IF p_FileType = 'f04' THEN
      Return RSP_FMARKET.Insert_f04(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'o04' THEN
      Return RSP_FMARKET.Insert_o04(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'fpos' THEN
      Return RSP_FMARKET.Insert_fpos(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'opos' THEN
      Return RSP_FMARKET.Insert_opos(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'fordlog' THEN
      Return RSP_FMARKET.Insert_fordlog(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'oordlog' THEN
      Return RSP_FMARKET.Insert_oordlog(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'f07' THEN
      Return RSP_FMARKET.Insert_f07(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'o07' THEN
      Return RSP_FMARKET.Insert_o07(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'pay' THEN
      Return RSP_FMARKET.Insert_pay(l_IDReq, p_IDProcLog);
    END IF;
    IF p_FileType = 'mon' THEN
      Return RSP_FMARKET.Insert_mon(l_IDReq, p_IDProcLog);
    END IF;
  end Insert_Table;

  function Get_ParamFile_csv (p_FILENAME IN VARCHAR2) return T_FILEPARAMS as
    l_FileParams T_FILEPARAMS;
  begin
    SELECT rtrim(rtrim(substr(p_FILENAME, 1, 4), '_'), '.'),
           DBMS_CRYPTO.HASH(t.valclob, 2)
      INTO l_FileParams.doc_type_id, l_FileParams.FileHash
      FROM tmp_xml t;
    IF l_FileParams.doc_type_id IN('ford', 'oord') THEN
      l_FileParams.doc_type_id := l_FileParams.doc_type_id||'log';
    ELSIF l_FileParams.doc_type_id LIKE 'pay%' THEN
      l_FileParams.doc_type_id := substr(l_FileParams.doc_type_id, 1, 3);
    ELSIF l_FileParams.doc_type_id LIKE 'mon%' THEN
      l_FileParams.doc_type_id := substr(l_FileParams.doc_type_id, 1, 3);
    END IF;
    Return l_FileParams;
  end Get_ParamFile_csv;

  function Get_ActualProcessing_Csv (p_FileParams IN T_FILEPARAMS) return INTEGER as
    l_IDProcAct INTEGER;
  begin
     SELECT pa.id_processing_actual
       INTO l_IDProcAct
       FROM processing_actual pa,
            processing_log pl
      WHERE pl.id_processing_log = pa.id_processing_log
        AND pa.file_type = p_FileParams.doc_type_id
        AND pl.file_hash = p_FileParams.FileHash
        AND rownum = 1;
    Return l_IDProcAct;
  exception
    WHEN NO_DATA_FOUND THEN
      Return NULL;
  end Get_ActualProcessing_Csv;

  function Processing_Log_Insert(p_FileParams IN T_FILEPARAMS, p_FILENAME in varchar2) return INTEGER as
    l_IDProcLog INTEGER;
  begin
    INSERT INTO processing_log(file_type, file_name, file_hash, beg_dt, ord_num, result_num, ord_num_str)
      VALUES(p_FileParams.doc_type_id, p_FileName, p_FileParams.FileHash, sysdate, p_FileParams.OrdNum, -1, p_FileParams.OrdNumStr)
    RETURNING id_processing_log INTO l_IDProcLog;
    Return l_IDProcLog;
  end Processing_Log_Insert;

  function Load_FMARKET (p_FILENAME in varchar2, p_Content in clob, p_Date in date default null) return INTEGER as
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
    INSERT INTO tmp_xml(VALCLOB)
      VALUES(rtrim(p_Content, chr(13)||chr(10)));

    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_MB.LogData_Insert(l_IDSession, 2, 1, 'Успешно загружено содержимое файла', l_Duration);

    --2. Проверяем надо ли обрабатывать файл (нет ли уже успешно загруженного того же типа с тем же хешем)
    l_FileParams := RSP_FMARKET.Get_ParamFile_csv(p_FILENAME);
    l_FileParams.OrdNum := 1;
    IF l_FileParams.doc_type_id IN('mon', 'pay') THEN
      l_FileParams.OrdNumStr := substr(p_FileName, 4, 4);
    ELSIF l_FileParams.doc_type_id IN('fpos', 'opos', 'f04', 'o04') THEN
      l_FileParams.OrdNumStr := substr(p_FileName, 5, 4);
    ELSIF l_FileParams.doc_type_id IN('fordlog', 'oordlog') THEN
      l_FileParams.OrdNumStr := substr(p_FileName, 8, 4);
    ELSE
      l_FileParams.OrdNumStr := '1';
    END IF;
    l_FileParams.TradeDate := p_Date;
    l_FileParams.DOC_DATE := p_Date;
    l_IDProcAct := RSP_FMARKET.Get_ActualProcessing_Csv(l_FileParams);

    IF l_IDProcAct IS NOT NULL AND RSP_MB.Check_UniqueFile(p_IDProcAct => l_IDProcAct,
                                                           p_FileHash  => l_FileParams.FileHash,
                                                           p_FileDate  => to_date(l_FileParams.DOC_DATE||' '||to_char(l_FileParams.DOC_TIME, 'HH24:MI:SS'),
                                                                                  'DD.MM.YYYY HH24:MI:SS')) THEN
      --инициализация ошибки
      RAISE e_Error;
      Return 1;
    END IF;

    l_IDProcLog := RSP_FMARKET.Processing_Log_Insert(l_FileParams, p_FILENAME);
    l_Duration := extract(second from (systimestamp - l_Systimestamp)) * 1000;
    RSP_MB.LogData_Insert(l_IDSession, 3, 1, 'Успешно создана запись процессинга', l_Duration);
    l_Systimestamp := systimestamp;

    --3. Загружаем данные из XML во временные таблицы
    l_RowsReated := RSP_FMARKET.LOAD_File(p_FILENAME => p_FILENAME,
                                          p_FileType => l_FileParams.doc_type_id,
                                          P_MSG      => l_Msg);

    IF l_RowsReated = 0 THEN
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
    l_RowsCreated := RSP_FMARKET.Insert_Table(p_FILENAME  => p_FILENAME,
                                              p_FileType  => l_FileParams.doc_type_id,
                                              p_IDProcLog => l_IDProcLog,
                                              p_Date      => l_FileParams.TradeDate);
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
  end Load_FMARKET;
end RSP_FMARKET;
/
