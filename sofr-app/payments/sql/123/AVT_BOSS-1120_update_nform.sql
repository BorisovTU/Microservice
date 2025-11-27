declare
  l_IdTable INTEGER;
  l_IdNForm INTEGER;
  l_OrderField INTEGER;
  l_Top INTEGER;

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
     WHERE NAME = 'FM_FPOS';
  EXCEPTION
    WHEN NO_DATA_FOUND THEN l_IdTable := 0;
  END;
 
  IF l_IdTable > 0 THEN
    SELECT ID_NFORM INTO l_IdNForm
      FROM NFORM
     WHERE ID_NTABLE = l_IdTable;

    SELECT MAX(ORDERFIELD)+1, MAX(TOP)+40
      INTO l_OrderField, l_Top 
      FROM NFIELD
     WHERE ID_NTABLE = l_IdTable;

    InservVisibleField(2, 'POS_ENFORCEMENT_EXEC', 'Количество принудительно исполненных позиций');
    InservVisibleField(7, 'PAY_ENFORCEMENT_EXEC', 'Сумма единовременного платежа');
  END IF;

  BEGIN
    SELECT ID_NTABLE INTO l_IdTable
      FROM NTABLE
     WHERE NAME = 'FM_F07';
  EXCEPTION
    WHEN NO_DATA_FOUND THEN l_IdTable := 0;
  END;
 
  IF l_IdTable > 0 THEN
    SELECT ID_NFORM INTO l_IdNForm
      FROM NFORM
     WHERE ID_NTABLE = l_IdTable;

    SELECT MAX(ORDERFIELD)+1, MAX(TOP)+40
      INTO l_OrderField, l_Top 
      FROM NFIELD
     WHERE ID_NTABLE = l_IdTable;

    InservVisibleField(7, 'EXP_ENFORCEMENT_PAY', 'Сумма единовременного платежа за один контракт');
  END IF;
end;
/