-- Добавление записей в таблицу FMT_NAMES для таблиц davoiriss_dbt и dfiwarnts_dbt
DECLARE
  fieldsCount NUMBER;
BEGIN
  SELECT count(1) INTO fieldsCount
    FROM fmt_fields
   WHERE t_fmtID = (SELECT t_ID FROM fmt_names WHERE t_name = 'davoiriss_dbt')
     AND t_Name = 't_couponrefuseright';
   IF fieldsCount = 0 THEN
     INSERT INTO fmt_fields
        (t_ID, t_fmtID, t_name, t_type, t_size, t_offset, t_outlen, t_decpoint, t_hidden, t_comment)
      VALUES 
        ((SELECT MAX(t_ID) FROM fmt_fields) + 1,
         (SELECT t_ID FROM fmt_names WHERE t_name = 'davoiriss_dbt'),
         't_couponrefuseright',
         12,  -- T_TYPE = FT_CHR
         1,   -- T_SIZE
         208, -- T_OFFSET
         0,   -- T_OUTLEN
         0,   -- T_DECPOINT
         CHR(0), -- T_HIDDEN
         'Право отказа от выплаты купона'); -- T_COMMENT
   END IF;
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/

DECLARE
  fieldsCount NUMBER;
BEGIN
  SELECT count(1) INTO fieldsCount
    FROM fmt_fields
   WHERE t_fmtID = (SELECT t_ID FROM fmt_names WHERE t_name = 'dfiwarnts_dbt')
     AND t_Name = 't_paymentrefuse';
   IF fieldsCount = 0 THEN
     INSERT INTO fmt_fields
        (t_ID, t_fmtID, t_name, t_type, t_size, t_offset, t_outlen, t_decpoint, t_hidden, t_comment)
      VALUES 
        ((SELECT MAX(t_ID) FROM fmt_fields) + 1,
         (SELECT t_ID FROM fmt_names WHERE t_name = 'dfiwarnts_dbt'),
         't_paymentrefuse',
         12,  -- T_TYPE = FT_CHR
         1,   -- T_SIZE
         313, -- T_OFFSET
         0,   -- T_OUTLEN
         0,   -- T_DECPOINT
         CHR(0), -- T_HIDDEN
         'Отказ от выплаты'); -- T_COMMENT
   END IF;
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/