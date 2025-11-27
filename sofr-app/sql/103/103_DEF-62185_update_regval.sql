-- ΅­®Ά«¥­¨¥ ­ αβΰ®¥ª ΅ ­ª  Ά regval
DECLARE
BEGIN
  UPDATE dRegVal_dbt
    SET t_FmtBlobData_XXXX = to_blob(utl_raw.cast_to_raw('broker@rshb.ru; UL@rshb.ru; custody@rshb.ru'))
  WHERE t_KeyID = RSB_TOOLS.Find_Regkey('‘•\…‘… ‘‹“†‚€…\‚€‹”€–\€€‚‹’ ‘›’“ ');
  
  COMMIT;
END;
/