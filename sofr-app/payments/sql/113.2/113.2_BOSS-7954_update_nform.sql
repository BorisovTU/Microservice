declare
  l_IdTable INTEGER;
  l_IdNForm INTEGER;
  l_OrderField INTEGER;
  l_Top INTEGER;

  PROCEDURE InservVisibleField(p_Type IN NUMBER, p_Name IN VARCHAR2, p_Text IN VARCHAR2)
  IS
  BEGIN
    INSERT INTO NFIELD(ID_NFORM, ID_NTABLE, ID_STYPEFIELD, NAME, 
                       ISGRID, ISVISIBLE, TEXT, ISTEXTUP, ORDERFIELD, 
                       TOP, LEFT, WIDTH, ISUPPER, 
                       ISEDIT, ISLKP, ISLKPONLY, ISREQ)
      VALUES(l_IdNForm, l_IdTable, p_Type, p_Name, 
             1, 1, p_Text, 1, l_OrderField, 
             l_Top, 10, 400, 0, 
             0, 0, 0, 0);

     l_OrderField := l_OrderField + 1;
  END;

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
  SELECT MAX(ORDERFIELD)+1
    INTO l_OrderField 
    FROM NFIELD;


  /*SEM02*/
  SELECT ID_NTABLE INTO l_IdTable
    FROM NTABLE
   WHERE NAME = 'MB_SEM02';

  SELECT ID_NFORM INTO l_IdNForm
    FROM NFORM
   WHERE ID_NTABLE = l_IdTable;

  SELECT MAX(TOP)+40
    INTO l_Top 
    FROM NFIELD
   WHERE ID_NTABLE = l_IdTable
     AND ID_NFORM = l_IdNForm;

  InservVisibleField(5, 'TRADESESSIONDATE', 'Дата торговой сессии');


  /*SEM03*/
  SELECT ID_NTABLE INTO l_IdTable
    FROM NTABLE
   WHERE NAME = 'MB_SEM03';

  SELECT ID_NFORM INTO l_IdNForm
    FROM NFORM
   WHERE ID_NTABLE = l_IdTable;

  DeleteVisibleField('ORDTYPE');
  DeleteVisibleField('ORDTYPECODE');

  SELECT MAX(TOP)+40
    INTO l_Top 
    FROM NFIELD
   WHERE ID_NTABLE = l_IdTable
     AND ID_NFORM = l_IdNForm;

  InservVisibleField(5, 'TRADESESSIONDATE', 'Дата торговой сессии');


  /*SEM21*/
  SELECT ID_NTABLE INTO l_IdTable
    FROM NTABLE
   WHERE NAME = 'MB_SEM21';

  SELECT ID_NFORM INTO l_IdNForm
    FROM NFORM
   WHERE ID_NTABLE = l_IdTable;

  SELECT MAX(TOP)+40
    INTO l_Top 
    FROM NFIELD
   WHERE ID_NTABLE = l_IdTable
     AND ID_NFORM = l_IdNForm;

  InservVisibleField(5, 'TRADESESSIONDATE', 'Дата торговой сессии');


  /*EQM06*/
  SELECT ID_NTABLE INTO l_IdTable
    FROM NTABLE
   WHERE NAME = 'MB_EQM06';

  SELECT ID_NFORM INTO l_IdNForm
    FROM NFORM
   WHERE ID_NTABLE = l_IdTable;

  SELECT MAX(ORDERFIELD)+1, MAX(TOP)+40
    INTO l_OrderField, l_Top 
    FROM NFIELD;

  InservVisibleField(5, 'TRADESESSIONDATE', 'Дата торговой сессии');


  /*EQM6C*/
  SELECT ID_NTABLE INTO l_IdTable
    FROM NTABLE
   WHERE NAME = 'MB_EQM6C';

  SELECT ID_NFORM INTO l_IdNForm
    FROM NFORM
   WHERE ID_NTABLE = l_IdTable;

  SELECT MAX(TOP)+40
    INTO l_Top 
    FROM NFIELD
   WHERE ID_NTABLE = l_IdTable
     AND ID_NFORM = l_IdNForm;

  InservVisibleField(5, 'TRADESESSIONDATE', 'Дата торговой сессии');
end;
/