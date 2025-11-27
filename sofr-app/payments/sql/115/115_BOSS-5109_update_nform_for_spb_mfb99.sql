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
     WHERE NAME = 'SPB_MFB99';
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
  InservVisibleField(2, 'ID_SPB_MFB99', 'ИД');
  InservVisibleField(5, 'REPORTDATE', 'Дата отчета');
  InservVisibleField(2, 'VOLUME', 'Том отчета');
  InservVisibleField(2, 'VOLUMETOTAL', 'Всего томов отчета');
  InservVisibleField(1, 'REPORTNUMBER', 'Сквозной номер отчета, единый для всех томов отчета');
  InservVisibleField(1, 'REPORTLANG', 'Язык отчета (RU/EN)');
  InservVisibleField(1, 'REPORTCODE', 'Код отчета (MFB99)');
  InservVisibleField(1, 'FIRMID', 'Идентификатор Участника торгов');
  InservVisibleField(1, 'CLRACCCODE', 'Код ТКС');
  InservVisibleField(1, 'GUARANTEEFUND', 'Признак ТКС для учета взносов в ККО');
  InservVisibleField(1, 'POSTYPE', 'Тип информации о нетто обязательствах/требованиях');
  InservVisibleField(1, 'BANKACCCODE', 'Код денежного регистра');
  InservVisibleField(1, 'GUARDEPUNITID', 'Счет (субсчет) депо для учета ценных бумаг');
  InservVisibleField(1, 'CURRENCYID', 'Идентификатор валюты средств обеспечения');
  InservVisibleField(1, 'CURRENCYNAME', 'Наименование валюты средств обеспечения');  
  InservVisibleField(1, 'SECURITYID', 'Идентификатор инструмента');
  InservVisibleField(1, 'SECSHORTNAME', 'Краткое наименование инструмента');
  InservVisibleField(1, 'ISIN', 'Международный идентификатор ценной бумаги');
  InservVisibleField(7, 'OPENINGBALANCE', 'Входящий остаток');
  InservVisibleField(7, 'CLOSINGBALANCE', 'Исходящий остаток');
  InservVisibleField(7, 'OPENINGDEBTSSUM', 'Входящая задолженность по денежному регистру');
  InservVisibleField(7, 'DEBTSSUM', 'Задолженность по денежному регистру');
  InservVisibleField(7, 'DEBITSUM', 'Итого по дебету');
  InservVisibleField(7, 'CREDITSUM', 'Итого по кредиту');
  InservVisibleField(1, 'OPERATIONCODE', 'Код операции с обеспечением');
  InservVisibleField(6, 'OPERATIONTIME', 'Время проведения операции в учете КЦ');
  InservVisibleField(1, 'DOCNO', 'Номер документа, присвоенный КЦ');
  InservVisibleField(1, 'CUSTOMERNO', 'Номер документа, присвоенный участником (если есть)');
  InservVisibleField(1, 'PURPOSE', 'Пояснения к операции (если есть)');
  InservVisibleField(7, 'DEBIT', 'Дебет');
  InservVisibleField(7, 'CREDIT', 'Кредит');
  InservVisibleField(1, 'CLIENTCODE', 'Краткий код клиента, указанный в заявлении на операцию с активами');
  InservVisibleField(2, 'ZRID', 'Ид записи репликации');
  InservNotVisibleField(2, 'VERSIONREC', 0);
  InservNotVisibleField(10, 'TS', 0);
  END IF;
end;
/