DECLARE
  -- Процедура добавления нового параметра в таблицу dgtkoprm_dbt
  PROCEDURE addGTKOPRM( p_ObjKind IN dgtkoprm_dbt.t_objectkind%TYPE, p_Name IN dgtkoprm_dbt.t_name%TYPE, p_TypeID IN dgtkoprm_dbt.t_typeid%TYPE )
  IS
    v_Code number;
    v_KoprmID number;
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
      SELECT nvl(max(t_code), 0) + 1 INTO v_Code FROM dgtkoprm_dbt r WHERE r.t_objectkind = p_ObjKind;
      SELECT nvl(max(t_koprmid), 0) + 1 INTO v_KoprmID FROM dgtkoprm_dbt r1 WHERE r1.t_koprmid < 15000; -- ограничиваем максимальным системным кодом

      INSERT INTO dgtkoprm_dbt ( 
         t_koprmid, t_objectkind, t_code, t_name, t_typeid, t_refobjectkind
      ) VALUES (
         v_KoprmID, p_ObjKind, v_Code, p_Name, p_TypeID, 0
      );
    END IF;
  END;
BEGIN
  addGTKOPRM(25, 'RGDLRQ_UNDERWR',  6); 	
  addGTKOPRM(25, 'RGDLRQ_TRDACCID', 6); 

  addGTKOPRM(27, 'RGDLTC_UNDERWR',  6); 
END;
/
