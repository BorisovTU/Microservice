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
     WHERE NAME = 'SPB_MFB06C';
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
  InservVisibleField(2, 'ID_SPB_MFB06C', 'ИД');
  InservVisibleField(5, 'REPORTDATE', 'Дата отчета');
  InservVisibleField(2, 'VOLUME', 'Том отчета');
  InservVisibleField(2, 'VOLUMETOTAL', 'Всего томов отчета');
  InservVisibleField(1, 'REPORTNUMBER', 'Сквозной номер отчета, единый для всех томов отчета');
  InservVisibleField(1, 'REPORTLANG', 'Язык отчета (RU/EN)');
  InservVisibleField(1, 'REPORTCODE', 'Код отчета (MFB06)');
  InservVisibleField(1, 'FIRMID', 'Идентификатор Участника торгов');
  InservVisibleField(1, 'CLIENTCODE', 'Краткий код клиента или клиента клиента участника клиринга');
  InservVisibleField(1, 'CLIENTDETAILS', 'ИНН или № паспорта клиента или иной код');
  InservVisibleField(1, 'CURRENCYID', 'Идентификатор валюты цены');
  InservVisibleField(1, 'CURRENCYNAME', 'Наименование валюты цены');  
  InservVisibleField(2, 'INFTYPE', 'Тип информации');  
  InservVisibleField(1, 'CLEARINGTYPE', 'Тип клирингового пула');  
  InservVisibleField(10, 'CLEARINGTIME', 'Время формирования клирингового пула');
  InservVisibleField(5, 'SETTLEDATE', 'Фактическая дата расчетов');
  InservVisibleField(1, 'BOARDID', 'Код группы инструментов');
  InservVisibleField(1, 'BOARDNAME', 'Наименование группы инструментов');
  InservVisibleField(1, 'SECURITYID', 'Идентификатор поставляемого актива (инструмента)');
  InservVisibleField(1, 'BASESECURITYCODE', 'Код базового актива (не указывается, если совпадает с SecurityId)');
  InservVisibleField(1, 'ISIN', 'Международный идентификатор поставляемого актива (инструмента)');
  InservVisibleField(1, 'SECSHORTNAME', 'Краткое наименование поставляемого актива (инструмента)');
  InservVisibleField(7, 'FACEVALUE', 'Номинал ЦБ');
  InservVisibleField(1, 'SECCURRENCYID', 'Валюта номинала ЦБ');
  InservVisibleField(1, 'PRICECURRENCYID', 'Валюта цены (котирования)');
  InservVisibleField(2, 'SECURITYTYPE', 'Категория ЦБ');
  InservVisibleField(1, 'PRICETYPE', 'Тип цены (PERC - процент от номинала, CASH - в валюте)');
  InservVisibleField(2, 'RECNO', 'Номер по порядку');
  InservVisibleField(1, 'TRADENO', 'Номер договора');
  InservVisibleField(1, 'TRADENOEXTRA', 'Дополнительный номер договора');
  InservVisibleField(5, 'TRADEDATE', 'Дата заключения договора');
  InservVisibleField(6, 'TRADETIME', 'Время регистрации договора в Системе торгов');
  InservVisibleField(1, 'PRIMARYORDERID', 'Номер заявки/Номер оферты ОТС');
  InservVisibleField(1, 'ORDERID', 'Дополнительный идентификатор системы/Системы проведения торгов');
  InservVisibleField(1, 'COMMENTAR', 'Комментарий');
  InservVisibleField(2, 'TRADEPLACE', 'Код организатора торгов, зарегистри-ровавшего договор');
  InservVisibleField(1, 'BUYSELL', 'Направленность заявки/оферты ОТС (Покупка (B) / продажа (S))');
  InservVisibleField(1, 'SETTLECODE', 'Код расчётов по договору');
  InservVisibleField(2, 'TRADEPERIOD', 'Период заключения биржевых договоров');
  InservVisibleField(1, 'TRADETYPE', 'Тип договора');
  InservVisibleField(2, 'TRADEMODEID', 'Идентификатор режима торгов/ за-ключения внебиржевых договоров');
  InservVisibleField(2, 'TRADEINSTRUMENTTYPE', 'Тип торгового инструмента');
  InservVisibleField(2, 'DECIMALS', 'Число значимых знаков после запятой в ценах');
  InservVisibleField(7, 'PRICE', 'Цена за единицу инструмента');
  InservVisibleField(7, 'QUANTITY', 'Объем договора, в инструментах');
  InservVisibleField(7, 'VALUE', 'Объем договора, в валюте расчётов');
  InservVisibleField(7, 'AMOUNT', 'Обязательство по денежным средствам, в валюте расчетов');
  InservVisibleField(7, 'BALANCE', 'Обязательство по ценным бумагам');
  InservVisibleField(1, 'CORPEVENT', 'Проведение корпоративного события');
  InservVisibleField(7, 'EXCHCOMM', 'Комиссионное вознаграждение за организацию торгов в валюте расчетов');
  InservVisibleField(7, 'CLRCOMM', 'Комиссионное компания в валюте расчетов');
  InservVisibleField(7, 'LICCOMM', 'Лицензионное вознаграждение в валюте расчетов');
  InservVisibleField(1, 'CLRACCCODE', 'Код ТКС');
  InservVisibleField(1, 'SUBACCCLRCODE', 'Код Аналитического ТКС');
  InservVisibleField(1, 'CCPCODE', 'Идентификатор Центрального контрагента');
  InservVisibleField(1, 'CPFIRMID', 'Идентификатор контрагента');
  InservVisibleField(1, 'CPFIRMSHORTNAME', 'Краткое наименование контрагента');
  InservVisibleField(5, 'DUEDATE', 'Дата исполнения');
  InservVisibleField(7, 'ACCINT', 'Накопленный купонный доход в валюте расчетов');
  InservVisibleField(7, 'PRICE2', 'Цена второй части РЕПО');
  InservVisibleField(2, 'REPOPART', 'Часть РЕПО');
  InservVisibleField(2, 'REPOPERIOD', 'Срок РЕПО в календарных днях');
  InservVisibleField(7, 'REPORATE', 'Ставка РЕПО в %');
  InservVisibleField(2, 'TYPE', 'Тип договора');
  InservVisibleField(7, 'FINEDEBIT', 'Сумма штрафа удержанная, в валюте расчтетов');
  InservVisibleField(7, 'FINECREDIT', 'Сумма штрафа начисленная, в валюте расчетов');
  InservVisibleField(1, 'REPOSITORYNUMBER', 'Номер, присвоенный внебиржевому договору РЕПО ОТС Репозитарием');
  InservVisibleField(2, 'ZRID', 'Ид записи репликации');
  InservNotVisibleField(2, 'VERSIONREC', 0);
  InservNotVisibleField(10, 'TS', 0);
  END IF;
end;
/