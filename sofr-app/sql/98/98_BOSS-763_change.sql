DECLARE
  -- Процедура добавления параметра RGDVRQ_TRADERCODE в таблицу dgtkoprm_dbt
  PROCEDURE addGTKOPRM
  IS
    v_Name dgtkoprm_dbt.t_name%TYPE := 'RGDVRQ_TRADERCODE';
    v_ObjKind number := 95;
    v_TypeID number := 6;
    v_Code number;
    v_NeedToAdd boolean := false; -- нужно добавлять ?
  BEGIN
    -- проверка, существует ли значение
    BEGIN
      SELECT t_code INTO v_Code FROM dgtkoprm_dbt r WHERE r.t_objectkind = v_ObjKind and r.t_name = v_Name;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN 
        v_NeedToAdd := true;
    END;
    -- если значения нет, создаем
    IF v_NeedToAdd = true THEN
       BEGIN
	  SELECT nvl(max(t_code), 0) + 1 INTO v_Code FROM dgtkoprm_dbt r WHERE r.t_objectkind = v_ObjKind;
          INSERT INTO dgtkoprm_dbt ( 
             t_koprmid, t_objectkind, t_code, t_name, t_typeid, t_refobjectkind
          ) VALUES (
             0, v_ObjKind, v_Code, v_Name, v_TypeID, 0
          );
          COMMIT;
       EXCEPTION WHEN others THEN 
          ROLLBACK;
       END;
    END IF;
  EXCEPTION
    WHEN others THEN 
      null;
  END;
  -- Процедура поля t_commentar varchar2(20) в таблицу d_foordlog_tmp
  PROCEDURE addCommentar
  IS
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE d_foordlog_tmp ADD t_commentar varchar2(20)';
  EXCEPTION WHEN others THEN 
    NULL;
  END;
BEGIN
  addGTKOPRM(); 	-- добавление параметра RGDVRQ_TRADERCODE в таблицу dgtkoprm_dbt
  addCommentar(); 	-- добавление поля t_commentar varchar2(20) в таблицу d_foordlog_tmp
END;
/
