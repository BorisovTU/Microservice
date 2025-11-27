declare
  l_IdTable INTEGER;
  l_IdNForm INTEGER;
  l_OrderField INTEGER;
  l_Top INTEGER;

  PROCEDURE DeleteVisibleField(p_Name IN VARCHAR2)
  IS
  BEGIN
    UPDATE NFIELD
       SET TOP = TOP - 40
     WHERE ID_NTABLE = l_IdTable
       AND ID_NFORM = l_IdNForm
       AND TOP > (SELECT T.TOP
                    FROM NFIELD T
                   WHERE T.ID_NTABLE = l_IdTable
                     AND T.ID_NFORM = l_IdNForm
                     AND T.NAME = p_Name
                 );

    DELETE FROM NFIELD
     WHERE ID_NTABLE = l_IdTable
       AND ID_NFORM = l_IdNForm
       AND NAME = p_Name;     
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

    DeleteVisibleField('POS_ENFORCEMENT_EXEC');
    DeleteVisibleField('PAY_ENFORCEMENT_EXEC');
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

    DeleteVisibleField('EXP_ENFORCEMENT_PAY');
  END IF;
end;
/