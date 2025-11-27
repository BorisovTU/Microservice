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
     WHERE NAME = 'SPB_MFB13';
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
  InservVisibleField(2, 'ID_SPB_MFB13', 'ИД');
  InservVisibleField(5, 'REPORTDATE', 'Дата отчета');
  InservVisibleField(2, 'VOLUME', 'Том отчета');
  InservVisibleField(2, 'VOLUMETOTAL', 'Всего томов отчета');
  InservVisibleField(1, 'REPORTNUMBER', 'Сквозной номер отчета, единый для всех томов отчета');
  InservVisibleField(1, 'REPORTLANG', 'Язык отчета (RU/EN)');
  InservVisibleField(1, 'REPORTCODE', 'Код отчета (MFB13)');
  InservVisibleField(1, 'FIRMID', 'Идентификатор Участника торгов');
  InservVisibleField(1, 'CLEARINGTYPE', 'Тип клирингового пула');  
  InservVisibleField(10, 'CLEARINGTIME', 'Дата и время формирования клирингового пула. Время MSK');
  InservVisibleField(1, 'FINALOBLIGATIONS', 'Признак нетто-обязательств, до процедуры урегулирования нехватки активов');
  InservVisibleField(1, 'CLRACCCODE', 'Код ТКС');
  InservVisibleField(1, 'POSTYPE', 'Тип информации о нетто обязательствах/требованиях');
  InservVisibleField(1, 'BANKACCCODE', 'Код денежного регистра');
  InservVisibleField(1, 'GUARDEPUNITID', 'Счет (субсчет) депо для учета ценных бумаг');
  InservVisibleField(1, 'CURRENCYID', 'Идентификатор валюты обязательства');
  InservVisibleField(1, 'CURRENCYNAME', 'Наименование валюты обязательства');  
  InservVisibleField(1, 'SECURITYID', 'Идентификатор ценной бумаги');
  InservVisibleField(1, 'SECSHORTNAME', 'Краткое наименование ценной бумаги/эмитента ценной бумаги');
  InservVisibleField(7, 'DEBIT', 'Нетто-обязательство / обязательство');
  InservVisibleField(7, 'CREDIT', 'Нетто-требование / требование');
  InservVisibleField(7, 'SHORTAGE', 'Размер нехватки активов для поставки');
  InservVisibleField(2, 'ZRID', 'Ид записи репликации');
  InservNotVisibleField(2, 'VERSIONREC', 0);
  InservNotVisibleField(10, 'TS', 0);
  END IF;
end;
/