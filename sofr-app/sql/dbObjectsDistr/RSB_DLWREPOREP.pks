create or replace package rsb_dlwreporep
is

/**
 * Категории учёта */
  DL_CATID_MAINRESTP     CONSTANT NUMBER := 393; -- -ОД
  DL_CATID_MAINRESTR     CONSTANT NUMBER := 394; -- +ОД
  
  FUNCTION GetCatID (p_DealType IN NUMBER, p_DocKind IN NUMBER)
   RETURN NUMBER;
   
  PROCEDURE CreateData(DepartmentID IN NUMBER, BegDate IN DATE, EndDate IN DATE, Oper IN NUMBER, RepID OUT INTEGER, Err OUT INTEGER);

end rsb_dlwreporep;
/