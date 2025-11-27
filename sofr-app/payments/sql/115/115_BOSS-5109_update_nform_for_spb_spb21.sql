declare
  l_IdTable INTEGER;
  l_IdNForm INTEGER;
  l_OrderField INTEGER;
  l_Top INTEGER;

  PROCEDURE InservNotVisibleField(p_Type IN NUMBER, p_Name IN VARCHAR2, p_Lkp IN NUMBER)
  IS
  BEGIN
    l_OrderField := l_OrderField + 1;
    INSERT INTO NFIELD(ID_NFORM, ID_NTABLE, ID_STYPEFIELD, NAME, 
                       ISGRID, ISVISIBLE, TEXT, ISTEXTUP, ORDERFIELD, 
                       TOP, LEFT, WIDTH, ISUPPER, 
                       ISEDIT, ISLKP, ISLKPONLY, ISREQ)
      VALUES(l_IdNForm, l_IdTable, p_Type, p_Name, 
             0, 0, NULL, 0, l_OrderField, 
             NULL, NULL, NULL, 0, 
             0, p_Lkp, p_Lkp, 0);
  END;

  PROCEDURE InservVisibleField(p_Type IN NUMBER, p_Name IN VARCHAR2, p_Text IN VARCHAR2)
  IS
  BEGIN
    l_OrderField := l_OrderField + 1;
    INSERT INTO NFIELD(ID_NFORM, ID_NTABLE, ID_STYPEFIELD, NAME, 
                       ISGRID, ISVISIBLE, TEXT, ISTEXTUP, ORDERFIELD, 
                       TOP, LEFT, WIDTH, ISUPPER, 
                       ISEDIT, ISLKP, ISLKPONLY, ISREQ)
      VALUES(l_IdNForm, l_IdTable, p_Type, p_Name, 
             1, 1, p_Text, 1, l_OrderField, 
             l_Top, 10, 400, 0, 
             0, 0, 0, 0);
    l_Top := l_Top + 40;
  END;

begin
  BEGIN
    SELECT ID_NTABLE INTO l_IdTable
      FROM NTABLE
     WHERE NAME = 'SPB_SPB21';
  EXCEPTION
    WHEN NO_DATA_FOUND THEN l_IdTable := 0;
  END;
 
  IF l_IdTable > 0 THEN
    SELECT ID_NFORM INTO l_IdNForm
      FROM NFORM
     WHERE ID_NTABLE = l_IdTable;

    DELETE FROM NFIELD
     WHERE ID_NFORM = l_IdNForm
       AND ID_NTABLE = l_IdTable;

  l_Top := 25;
  SELECT MAX(ORDERFIELD)
    INTO l_OrderField
    FROM NFIELD;

  InservNotVisibleField(2, 'ID_MB_REQUISITES', 1);
  InservNotVisibleField(2, 'ID_PROCESSING_LOG', 1);
  InservVisibleField(2, 'ID_SPB_SPB21', 'ИД');
  InservVisibleField(5, 'TRADEDATE', 'Дата торгов');
  InservVisibleField(2, 'VOLUME', 'Том отчета');
  InservVisibleField(2, 'VOLUMETOTAL', 'Всего томов отчета');
  InservVisibleField(1, 'REPORTNUMBER', 'Сквозной номер отчета, единый для всех томов отчета');
  InservVisibleField(1, 'REPORTLANG', 'Язык отчета (RU/EN)');
  InservVisibleField(1, 'REPORTCODE', 'Код отчета (SPB21)');
  InservVisibleField(1, 'BOARDID', 'Идентификатор группы инструментов');
  InservVisibleField(1, 'BOARDNAME', 'Наименование группы инструментов');
  InservVisibleField(1, 'SECURITYID', 'Идентификатор (код) инструмента');
  InservVisibleField(1, 'SECSHORTNAME', 'Краткое наименование инструмента');
  InservVisibleField(1, 'ISIN', 'Международный идентификатор инструмента (ISIN)');
  InservVisibleField(1, 'REGNUMBER', 'Государственный регистрационный номер выпуска');
  InservVisibleField(7, 'FACEVALUE', 'Номинал');
  InservVisibleField(1, 'SECCURRENCYID', 'Валюта номинала инструмента');
  InservVisibleField(2, 'SECURITYTYPE', 'Вид и тип ЦБ');
  InservVisibleField(1, 'ISSUERNAME', 'Наименование эмитента');
  InservVisibleField(1, 'ISSUERDETAILS', 'ИНН эмитента (управляющей компании)');
  InservVisibleField(1, 'QUOTELIST', 'Котировальный список');
  InservVisibleField(2, 'DECIM', 'Число значимых знаков после запятой в ценах (не нулевых)');
  InservVisibleField(1, 'CURRENCYID', 'Код валюты цены');
  InservVisibleField(1, 'CURRENCYNAME', 'Наименование валюты цены');
  InservVisibleField(7, 'ACCRUEDINTEREST', 'Для облигаций - НКД на дату отчета');
  InservVisibleField(1, 'TRADEPERIOD', 'Торговая сессия');
  InservVisibleField(1, 'SETTTYPE_MARKET', 'Код расчетов');
  InservVisibleField(1, 'TRADEMODE_MARKET', 'Наименование Режима торгов');
  InservVisibleField(7, 'PERIODTOTALAMOUNT', 'Общее количество ценных бумаг по заключенным Договорам, шт');
  InservVisibleField(7, 'PERIODTOTALVOLUME', 'Общий объём Договоров, валюта цены');
  InservVisibleField(2, 'PERIODTOTALCOUNT', 'Общее количество заключенных Договоров, шт');
  InservVisibleField(7, 'PERIODOPENPRICE', 'Первый Договор, цена, валюта цены');
  InservVisibleField(7, 'PERIODOPENVOLUME', 'Первый Договор, объем, валюта цены');
  InservVisibleField(7, 'PERIODLASTPRICE', 'Последний Договор, цена, валюта цены');
  InservVisibleField(7, 'PERIODLASTVOLUME', 'Последний Договор, объем, валюта цены');
  InservVisibleField(7, 'PERIODCURRENTPRICE', 'Последняя текущая цена, валюта цены');
  InservVisibleField(7, 'PERIODMAXDEALPRICE', 'Максимальная цена Договора за период, валюта инструмента');
  InservVisibleField(7, 'PERIODMINDEALPRICE', 'Минимальная цена Договора за период, валюта инструмента');
  InservVisibleField(7, 'PERIODWAPRICE', 'Средневзвешенная цена, валюта цены');
  InservVisibleField(1, 'SETTTYPE_ADDRESS', 'Код расчетов');
  InservVisibleField(1, 'TRADEMODE_ADDRESS', 'Наименование Режима торгов');
  InservVisibleField(7, 'ADDRESSPERIODTOTALAMOUNT', 'Общее количество ценных бумаг по заключенным Договорам, шт');
  InservVisibleField(7, 'ADDRESSPERIODTOTALVOLUME', 'Общий объём Договоров, валюта цены');
  InservVisibleField(2, 'ADDRESSPERIODTOTALCOUNT', 'Общее количество заключенных Договоров, шт');
  InservVisibleField(7, 'ADDRESSPERIODOPENPRICE', 'Первый Договор, цена, валюта цены');
  InservVisibleField(7, 'ADDRESSPERIODOPENVOLUME', 'Первый Договор, объем, валюта цены');
  InservVisibleField(7, 'ADDRESSPERIODLASTPRICE', 'Последний Договор, цена, валюта цены');
  InservVisibleField(7, 'ADDRESSPERIODLASTVOLUME', 'Последний Договор, объем, валюта цены');
  InservVisibleField(7, 'ADDRESSPERIODCURRENTPRICE', 'Последняя текущая цена, валюта цены');
  InservVisibleField(7, 'ADDRESSPERIODMAXDEALPRICE', 'Максимальная цена Договора за период, валюта инструмента');
  InservVisibleField(7, 'ADDRESSPERIODMINDEALPRICE', 'Минимальная цена Договора за период, валюта инструмента');
  InservVisibleField(7, 'ADDRESSPERIODWAPRICE', 'Средневзвешенная цена, валюта цены');
  InservVisibleField(7, 'TOTALAMOUNT', 'Общее количество ценных бумаг по заключенным Договорам за Торговый день, шт');
  InservVisibleField(7, 'TOTALVOLUME', 'Общий объём договоров за Торговый день в Режиме основных торгов, валюта цены');
  InservVisibleField(2, 'TOTALDEALCOUNT', 'Общее число Договоров за день в Режиме основных торгов');
  InservVisibleField(7, 'MAXDEALPRICE', 'Максимальная цена Договоров за день, валюта цены');
  InservVisibleField(7, 'MINDEALPRICE', 'Минимальная цена Договоров за день, валюта цены');
  InservVisibleField(7, 'CLOSEPRICE', 'Цена закрытия Торгового дня, валюта инструмента');
  InservVisibleField(7, 'PREVCLOSE', 'Цена закрытия предыдущего Торгового дня, валюта цены');
  InservVisibleField(7, 'TRENDCLOSE', 'Изменение цены закрытия Торгового дня, %');
  InservVisibleField(7, 'WAPRICE', 'Средневзвешенная цена, валюта инструмента');
  InservVisibleField(7, 'CURRENTPRICE', 'Текущая цена, валюта цены');
  InservVisibleField(7, 'ADMITTEDQUOTE', 'Признаваемая котировка, валюта инструмента');
  InservVisibleField(7, 'ADMITTEDQUOTEVOLUME', 'Объем Договоров для расчета признаваемой котировки, валюта цены');
  InservVisibleField(7, 'MARKETPRICE2', 'Рыночная цена (2), валюта цены');
  InservVisibleField(7, 'MP2VOLUME', 'Объем Договоров для расчета рыночной цены (2), валюта цены');
  InservVisibleField(7, 'MARKETPRICE3', 'Рыночная цена (3), валюта инструмента');
  InservVisibleField(7, 'MP3VOLUME', 'Объем Договоров для расчета рыночной цены (3), валюта цены');
  InservVisibleField(7, 'CLEARINGPRICE', 'Расчетная цена, предоставленная Клиринговой организацией, валюта цены');
  InservVisibleField(2, 'ZRID', 'Ид записи репликации');
  InservNotVisibleField(2, 'VERSIONREC', 0);
  InservNotVisibleField(10, 'TS', 0);
  END IF;
end;
/