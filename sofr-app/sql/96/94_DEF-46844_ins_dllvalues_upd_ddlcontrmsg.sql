DECLARE
BEGIN
   INSERT INTO dllvalues_dbt (t_list, t_element, t_code, t_name, t_flag, t_note, t_reserve)
   VALUES (1143, 506, '506', 'Уведомление о последствиях признания ФЛ КИ', 506, 'Уведомление о последствиях признания физического лица квалифицированным инвестором', chr(1));

EXCEPTION 
   WHEN OTHERS THEN NULL;
END;
/

DECLARE
BEGIN
  UPDATE ddlcontrmsg_dbt msg 
  SET msg.t_kind = 506 
  WHERE msg.t_kind = 501 
    AND EXISTS (SELECT 1 
                FROM dimgdata_dbt img 
                WHERE img.t_objectid = LPAD(msg.t_id, 34, '0') AND img.t_FileName LIKE '%_QI%');
END;
/

BEGIN
 EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/