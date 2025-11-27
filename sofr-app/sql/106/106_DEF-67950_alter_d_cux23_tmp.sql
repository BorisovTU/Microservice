DECLARE
  -- Процедура добавления нового параметра в таблицу dgtkoprm_dbt
  PROCEDURE addGTKOPRM( p_ObjKind IN dgtkoprm_dbt.t_objectkind%TYPE, p_Name IN dgtkoprm_dbt.t_name%TYPE )
  IS
    v_TypeID number := 3; -- тип = double
    v_Code number;
    v_NeedToAdd boolean := false; -- нужно добавлять ?
  BEGIN
    -- проверка, существует ли значение
    BEGIN
      SELECT t_code INTO v_Code FROM dgtkoprm_dbt r WHERE r.t_objectkind = p_ObjKind and r.t_name = p_Name;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN 
        v_NeedToAdd := true;
    END;
    -- если значения нет, создаем
    IF v_NeedToAdd = true THEN
       BEGIN
	  SELECT nvl(max(t_code), 0) + 1 INTO v_Code FROM dgtkoprm_dbt r WHERE r.t_objectkind = p_ObjKind;
          INSERT INTO dgtkoprm_dbt ( 
             t_koprmid, t_objectkind, t_code, t_name, t_typeid, t_refobjectkind
          ) VALUES (
             0, p_ObjKind, v_Code, p_Name, v_TypeID, 0
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
  -- Процедура добавления поля t_facevalue number(32,12) в таблицу d_cux23_tmp
  PROCEDURE addBrokerRef
  IS
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE d_cux23_tmp ADD t_facevalue number(32,12)';
  EXCEPTION WHEN others THEN 
    NULL;
  END;
BEGIN
  -- добавление параметров в таблицу dgtkoprm_dbt
  addGTKOPRM(53, 'RGDVNDL_FACEVALUE'); 	-- для внебиржевых сделок с ПИ
  addGTKOPRM(54, 'RGDVFXDL_FACEVALUE'); -- для конверсионных сделок с ПИ
  -- добавление поля t_facevalue number(32,12) в таблицу d_cux23_tmp
  addBrokerRef();
END;
/
