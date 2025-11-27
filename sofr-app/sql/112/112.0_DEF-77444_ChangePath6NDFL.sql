DECLARE
  vResult NUMBER;
BEGIN
  vResult := RSI_RSB_REGVAL.ModRegStringValue('‘•\„ˆ…Š’ˆˆ\OUT\6-„”‹', '..\TxtFile');

  IF vResult <> 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'Unable to set regval, error code = ' || vResult); 
  END IF;
END;
/
