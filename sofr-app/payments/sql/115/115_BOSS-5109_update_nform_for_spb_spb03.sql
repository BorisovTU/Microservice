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
     WHERE NAME = 'SPB_SPB03';
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
  InservVisibleField(2, 'ID_SPB_SPB03', 'ИД');
  InservVisibleField(5, 'REPORTDATE', 'Дата отчета');
  InservVisibleField(2, 'VOLUME', 'Том отчета');
  InservVisibleField(2, 'VOLUMETOTAL', 'Всего томов отчета');
  InservVisibleField(1, 'REPORTNUMBER', 'Сквозной номер отчета, единый для всех томов отчета');
  InservVisibleField(1, 'REPORTLANG', 'Язык отчета (RU/EN)');
  InservVisibleField(1, 'REPORTCODE', 'Код отчета (SPB03)');
  InservVisibleField(1, 'CLRACCCODE', 'Код ТКС');
  InservVisibleField(1, 'SUBCLRACCCODE', 'Код подраздела ТКС');         
  InservVisibleField(1, 'CURRENCYID', 'Идентификатор валюты цены');
  InservVisibleField(1, 'CURRENCYNAME', 'Наименование валюты цены');
  InservVisibleField(1, 'BOARDID', 'Идентификатор группы инструментов');
  InservVisibleField(1, 'BOARDNAME', 'Наименование группы инструментов');
  InservVisibleField(5, 'SETTLEDATE', 'Фактическая дата расчетов (дата исполнения)');
  InservVisibleField(1, 'SECURITYID', 'Идентификатор инструмента');
  InservVisibleField(1, 'SECSHORTNAME', 'Краткое наименование инструмента');
  InservVisibleField(1, 'ISIN', 'Международный идентификатор инструмента');
  InservVisibleField(1, 'REGNUMBER', 'Государственный регистрационный номер');
  InservVisibleField(7, 'FACEVALUE', 'Номинал ЦБ');
  InservVisibleField(1, 'SECCURRENCYID', 'Валюта номинала ЦБ');
  InservVisibleField(2, 'SECURITYTYPE', 'Категория ценной бумаги');
  InservVisibleField(1, 'PRICETYPE', 'PERC - цена указана в процентах от номинала, CASH - цена в валюте расчетов');
  InservVisibleField(2, 'RECNO', 'Номер по порядку в отчете');
  InservVisibleField(1, 'TRADENO', 'Номер договора в системе проведения торгов');
  InservVisibleField(1, 'TRADENOEXTRA', 'Дополнительный номер договора для связанной пары договоров');
  InservVisibleField(5, 'TRADEDATE', 'Дата регистрации договора');
  InservVisibleField(6, 'TRADETIME', 'Время регистрации договора в Системе проведения торгов');
  InservVisibleField(1, 'TRADEPERIOD', 'Торговая сессия');
  InservVisibleField(1, 'SPECIALPERIOD', 'Периоды проведения торгов');
  InservVisibleField(1, 'PRIMARYORDERID', 'Номер заявки, присвоенный биржей');
  InservVisibleField(1, 'ORDERID', 'Номер заявки');
  InservVisibleField(2, 'ORDERTYPE', 'Тип заявки');
  InservVisibleField(1, 'USERID', 'Имя участника торгов, подавшего заявку');
  InservVisibleField(1, 'COMMENTAR', 'Комментарий');
  InservVisibleField(1, 'ISMM', 'Признак Заявки, поданной во исполнение обязательств Маркетмейкера');
  InservVisibleField(1, 'BUYSELL', 'Направленность заявки (Покупка (B) / продажа (S))');
  InservVisibleField(1, 'SETTLECODE', 'Код расчётов торгового инструмента');
  InservVisibleField(1, 'TRADETYPE', 'Тип договора');
  InservVisibleField(2, 'TRADEINSTRUMENTTYPE', 'Тип торгового инструмента');
  InservVisibleField(2, 'TRADEMODEID', 'Режим торгов (номер режима)');
  InservVisibleField(1, 'TRADEMODENAME', 'Наименование режима торгов');
  InservVisibleField(2, 'DECIMALS', 'Число значимых знаков после запятой в ценах (не нулевых)');
  InservVisibleField(7, 'PRICE', 'Цена за единицу инструмента');
  InservVisibleField(7, 'QUANTITY', 'Объем договора, в лотах');
  InservVisibleField(7, 'VALUE', 'Объем договора, в валюте цены');
  InservVisibleField(7, 'AMOUNT', 'Обязательство по денежным средствам, в валюте цены');
  InservVisibleField(7, 'BALANCE', 'Обязательство по ценным бумагам');
  InservVisibleField(7, 'EXCHCOMM', 'Комиссионное вознаграждение за организацию торгов, в валюте цены');
  InservVisibleField(7, 'CLRCOMM', 'Комиссионное вознаграждение за клиринг договоров, в валюте цены');
  InservVisibleField(1, 'CLIENTCODE', 'Краткий код клиента');
  InservVisibleField(1, 'CLIENTDETAILS', 'ИНН или № паспорта Клиента или иные данные, включаемые в код клиента');
  InservVisibleField(1, 'CCPCODE', 'Код Центрального Контрагента');
  InservVisibleField(1, 'CPFIRMID', 'Идентификатор контрагента только для адресных сделок');
  InservVisibleField(1, 'CPFIRMSHORTNAME', 'Краткое наименование контрагента (только для адресных сделок)');
  InservVisibleField(1, 'OTCCODEINITIATOR', 'Идентификатор адресных сделок Участника торгов, подавшего заявку');
  InservVisibleField(1, 'OTCCODECONFIRMATOR', 'Идентификатор адресных сделок Участника торгов, которому адресована заявка');
  InservVisibleField(7, 'ACCINT', 'Накопленный купонный доход, в валюте цены');
  InservVisibleField(7, 'PRICE2', 'Цена второй части договора РЕПО');
  InservVisibleField(7, 'REPORATE', 'Ставка РЕПО');
  InservVisibleField(2, 'REPOPART', 'Часть договора РЕПО');
  InservVisibleField(2, 'REPOPERIOD', 'Срок РЕПО в календарных днях');
  InservVisibleField(2, 'TYPE', 'Тип договора');
  InservVisibleField(2, 'INTERNALTRADEMODEID', 'Внутренний идентификатор режима торгов в Торговой системе');
  InservVisibleField(2, 'ZRID', 'Ид записи репликации');
  InservNotVisibleField(2, 'VERSIONREC', 0);
  InservNotVisibleField(10, 'TS', 0);
  END IF;
end;
/