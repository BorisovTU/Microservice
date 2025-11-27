DECLARE
  regParmID dregparm_dbt.t_keyid%type;
  stat NUMBER; 
  regPath VARCHAR(200)  := '‘•\\Š…‘Š… ‘‹“†ˆ‚€ˆ…\\‘ˆ‘Š_‚€‹’_‡€…’_‘ˆ‘_‡€—';
  regVal VARCHAR(2000)  := '';
BEGIN
  stat := RSI_RSB_REGVAL.GETREGPARM(regParmID, regPath);
    
  IF stat = 0 THEN
    SELECT trim(RSB_LOCALE.STRING_TO_UNICODE(RSB_LOCALE.RAW_TO_OEM(t_fmtBlobData_XXXX)))
      INTO regVal
      FROM dregval_dbt
     WHERE t_KeyID    = regParmID
       AND t_RegKind  = 0
       AND t_ObjectID = 0;

    IF regVal != CHR(0) THEN 
      regVal := 'GLD,SLV,'||regVal;
    ELSE
      regVal := 'GLD,SLV';
    END IF;
  
    stat := RSI_RSB_REGVAL.ModRegStringValue(regPath, regVal);
  END IF;
END;
/
