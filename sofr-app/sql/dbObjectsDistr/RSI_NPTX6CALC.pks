CREATE OR REPLACE PACKAGE RSI_NPTX6CALC
IS
   INFORATE9_15 CONSTANT NUMBER := 1; --справка по ставке 9 (15) %
   INFORATE_TOTAL CONSTANT NUMBER := 2; --справка по общей ставке
   INFORATE_35 CONSTANT NUMBER := 3; --справка по ставке 35%(только для резидентов)
   INFORATE9_NOTRES_DIV CONSTANT NUMBER := 4; --9% / ставке по дивидендам для нерезидентов.
   INFORATE_HIGHRATE_15 CONSTANT NUMBER := 5; --справка по повышенной ставке (15%)
   
   DL_NPTXOP_WRTKIND_WRTOFF CONSTANT NUMBER := 20; --Списание д/с
   
   DL_TXHOLD_OPTYPE_ENDYEAR CONSTANT NUMBER := 10; -- Окончание года
   DL_TXHOLD_OPTYPE_OUTMONEY CONSTANT NUMBER := 20; -- Вывод д/с
   DL_TXHOLD_OPTYPE_OUTAVOIR CONSTANT NUMBER := 30; -- Вывод ц/б
   DL_TXHOLD_OPTYPE_LUCRE CONSTANT NUMBER := 40; -- Материальная выгода
   DL_TXHOLD_OPTYPE_CLOSE CONSTANT NUMBER := 50; -- Закрытие договора
   ---------------------------------------------------------------------
   DL_TXHOLD_OPTYPE_PREHOLD CONSTANT NUMBER := 60; -- Доудержание налога
   DL_TXHOLD_OPTYPE_TAXREF CONSTANT NUMBER := 70; -- Возврат налога
   DL_TXHOLD_OPTYPE_DIVIDEND CONSTANT NUMBER := 80;  --Налог по дивидендам
   
   DL_TXBASECALC_OPTYPE_ENDYEAR CONSTANT NUMBER := 10; --Окончание года
   DL_TXBASECALC_OPTYPE_CLOSE_IIS CONSTANT NUMBER := 40; --Закрытие ИИС
   
   DL_VEKSELDRAWORDER CONSTANT NUMBER := 110; --Заявление на погашение векселей
   DL_VSINTERCHANGE CONSTANT NUMBER := 119; --Соглашение о зачете взаимных требований
   
   g_LastClient NUMBER;
   g_LastDate DATE;

   FUNCTION GetCloseDateIIS(p_Client NUMBER, p_BeginDate DATE, p_EndDate DATE) RETURN DATE;
   FUNCTION GetFirstDateIIS(p_Client NUMBER, p_BeginDate DATE, p_EndDate DATE) RETURN DATE;
   FUNCTION GetLastWorkDayYear(pDate DATE) RETURN DATE;
   FUNCTION GetDateOfIncomeOperation(p_MinDate DATE, p_MaxDate DATE, p_ClientID NUMBER) RETURN DATE;
   PROCEDURE CalcDataForPart2_CLient(p_ClientID IN NUMBER, p_ClientID_2 IN NUMBER, p_GUID IN VARCHAR2, p_BeginDate IN DATE, p_EndDate IN DATE, p_ByCB IN CHAR, p_ByDepo IN CHAR, p_ByVS IN CHAR, p_RepDate DATE);
   PROCEDURE CalcDataForPart2(p_BeginDate IN DATE, p_EndDate IN DATE, p_ByCB IN CHAR, p_ByDepo IN CHAR, p_ByVS IN CHAR, p_RepDate DATE);
END RSI_NPTX6CALC;
/