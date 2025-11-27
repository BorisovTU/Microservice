DECLARE
    cnt     NUMBER;
    v_keyId NUMBER;
BEGIN
  SELECT COUNT (1)
    INTO cnt
    FROM DREGPARM_DBT
   WHERE T_NAME = 'Дата признания купонов по ц/б иностранных эмитентов';

  IF CNT = 0
  THEN
    SELECT T_KEYID INTO v_keyId FROM DREGPARM_DBT WHERE T_NAME = 'DEPO' AND t_parentid = 0;

    INSERT INTO DREGPARM_DBT (T_NAME, T_TYPE, T_DESCRIPTION, T_PARENTID, T_GLOBAL)
      VALUES('ДАТА ПРИЗН. КУПОНОВ ЦБ ИН.ЭМИТ', 2,'Дата признания купонов по ц/б иностранных эмитентов',v_keyId, 'X') 
      RETURNING t_keyid INTO v_keyId;
    
    INSERT INTO DREGVAL_DBT (T_KEYID, T_LINTVALUE, T_EXPDEP, T_OBJECTID, T_REGKIND, T_FMTBLOBDATA_XXXX)
      VALUES(v_keyId,0,0,0,0, '33312E30332E32303234');
  END IF;
END;
/