DECLARE
  vResult NUMBER;
BEGIN
  vResult := RSI_RSB_REGVAL.ModRegFlagValue('BANK_INI\WINDOWS REPORT\ˆ‘‹œ‡‚€’œ APACHE POI', NULL);

  IF vResult <> 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'Unable to set regval, error code = ' || vResult); 
  END IF;
END;
/
