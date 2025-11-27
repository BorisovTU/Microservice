CREATE OR REPLACE PACKAGE RSI_RsbOperDay IS

    SET_CHAR CONSTANT CHAR := 'X'   ;
  UNSET_CHAR CONSTANT CHAR := chr(0);

  c_MinPhase CONSTANT INTEGER := 1;
  c_MaxPhase CONSTANT INTEGER := 9999;

-- ?олучение даты последнего открытого опердн¤ дл¤ филиала
  FUNCTION GetDepartmentOperDate( p_Branch IN INTEGER )
  RETURN DATE RESULT_CACHE;

-- ?олучение следующей даты опердн¤
  FUNCTION GetNextOperDate(
                           p_Department IN INTEGER
                          ,p_OperDate   IN DATE
                          )
  RETURN DATE RESULT_CACHE ;

-- ?олучение параметров фазы лицевого счета
  PROCEDURE GetAccountPhase(
                            p_Phase       OUT INTEGER,
                            p_PhaseDate   OUT DATE,
                            p_Account  IN VARCHAR2,
                            p_Chapter  IN INTEGER,
                            p_FIID     IN INTEGER,
                            p_BankDate IN DATE
                           );

-- ?олучение фазы, котора¤ действует на день CurDate дл¤ счета
  FUNCTION GetPhase(
                    p_Account  VARCHAR2,
                    p_Chapter  INTEGER,
                    p_FIID     INTEGER,
                    p_CurDate  DATE
                   )
  RETURN INTEGER;

-- ?бработка лицевых счетов при открытии нового опердн¤
  PROCEDURE ProcessAccountsOnOpenDay(
                                     p_Department IN INTEGER,
                                     p_CurDate    IN DATE
                                    );

-- 'ункци¤ проверки возможности работы в операционном дне
  FUNCTION CheckOperDayPermission(p_CurDate IN DATE, p_Department IN NUMBER) RETURN BOOLEAN;

END;
/
