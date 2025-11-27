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
     WHERE NAME = 'SPB_MFB98';
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
  InservVisibleField(2, 'ID_SPB_MFB98', 'ИД');
  InservVisibleField(5, 'REPORTDATE', 'Дата отчета');
  InservVisibleField(2, 'VOLUME', 'Том отчета');
  InservVisibleField(2, 'VOLUMETOTAL', 'Всего томов отчета');
  InservVisibleField(1, 'REPORTNUMBER', 'Сквозной номер отчета, единый для всех томов отчета');
  InservVisibleField(1, 'REPORTLANG', 'Язык отчета (RU/EN)');
  InservVisibleField(1, 'REPORTCODE', 'Код отчета (MFB98)');
  InservVisibleField(1, 'FIRMID', 'Идентификатор Участника торгов');
  InservVisibleField(1, 'CLRACCCODE', 'Код ТКС');
  InservVisibleField(1, 'BANKACCCODE', 'Код денежного регистра');
  InservVisibleField(1, 'GUARDEPUNITID', 'Счет (субсчет) депо для учета ценных бумаг');
  InservVisibleField(1, 'TRADENO', 'Номер договора в системе торгов');
  InservVisibleField(1, 'TRADENOOTC', 'Номер договора ОТС');
  InservVisibleField(1, 'OPERATIONNO', 'Номер операции по передаче дохода/ценных бумаг');
  InservVisibleField(1, 'CLIENTCODE', 'Краткий код клиента или клиента Участника клиринга');
  InservVisibleField(5, 'TRADEDATE', 'Дата заключения договора');
  InservVisibleField(5, 'SETTLEDATE1', 'Дата исполнения 1-ой части');
  InservVisibleField(5, 'SETTLEDATE2', 'Дата исполнения 2-й части');
  InservVisibleField(1, 'DEBITCREDIT', 'Направленность обязательств по передаче Дохода/ценных бумаг');
  InservVisibleField(1, 'TRADESPECIALTYPE', 'Признаки сделки в рамках функционала QDD');
  InservVisibleField(1, 'SECURITYID', 'Идентификатор ценной бумаги по договору');
  InservVisibleField(1, 'SECSHORTNAME', 'Краткое наименование ценной бумаги по договору');
  InservVisibleField(1, 'ISIN', 'ISIN ценной бумаги');
  InservVisibleField(7, 'QUANTITY', 'Объем договора, в ценных бумагах');
  InservVisibleField(1, 'TYPE', 'Вид дохода/операции');
  InservVisibleField(5, 'RECORDDATE', 'Дата закрытия реестра по корпоративному событию');
  InservVisibleField(5, 'PAYMENTDATE', 'Дата проведения корпоративного события эмитентом');
  InservVisibleField(5, 'OBLIGATIONDATE', 'Дата передачи дохода/ценных бумаг/корректировка обязательств КЦ');
  InservVisibleField(1, 'FULLNETTO', 'Способ исполнения обязательств по передаче дохода');
  InservVisibleField(1, 'NEWSECURITYID', 'Идентификатор ценной бумаги, начисляемой как доп. обязательство');
  InservVisibleField(1, 'NEWSECSHORTNAME', 'Краткое наименование ценной бумаги, начисляемой как доп. обязательство');
  InservVisibleField(1, 'NEWISIN', 'ISIN ценной бумаги, начисляемой как доп. обязательство');
  InservVisibleField(1, 'CURRENCYID', 'Идентификатор валюты доходов');
  InservVisibleField(1, 'CURRENCYNAME', 'Наименование валюты доходов');  
  InservVisibleField(7, 'PRICE', 'Доход на одну ценную бумагу (купон или амортизация)');
  InservVisibleField(7, 'TAXRATE', 'Ставка налога на дивиденды (в долях единицы)');
  InservVisibleField(7, 'SUM', 'Сумма обязательства/требования по передаче дохода/ценных бумаг');
  InservVisibleField(2, 'ZRID', 'Ид записи репликации');
  InservNotVisibleField(2, 'VERSIONREC', 0);
  InservNotVisibleField(10, 'TS', 0);
  END IF;
end;
/