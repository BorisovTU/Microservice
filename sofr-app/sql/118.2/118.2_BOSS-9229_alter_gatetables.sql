-- Добавление поля в таблицы и обновление списка признаков ЗР
DECLARE
  -- Процедура добавления поля
  PROCEDURE addField (p_Table IN varchar2, p_Field IN varchar2, p_Type IN varchar2)
  IS
    v_count NUMBER;
  BEGIN
    SELECT count(1) INTO v_count
      FROM all_tab_columns 
     WHERE UPPER(table_name) = p_Table
       AND UPPER(column_name) = p_Field;
       
    IF v_count = 0 THEN
      EXECUTE IMMEDIATE 'ALTER TABLE '||p_Table||' ADD '||p_Field||' '||p_Type;
    END IF;
  EXCEPTION WHEN others THEN 
    DBMS_OUTPUT.PUT_LINE('Ошибка: '||SQLCODE||' - '||SQLERRM); 
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'BOSS-9229 Release 118.2 alter '||p_Table,
               p_msg_type => it_log.C_MSG_TYPE__ERROR -- при указании такого типа, функция залогирует стек ошибки, собранный в it_error
              );
    it_error.clear_error_stack;
  END;

  -- Процедура добавления нового параметра в таблицу dgtkoprm_dbt
  PROCEDURE addGTKOPRM( p_ObjKind IN dgtkoprm_dbt.t_objectkind%TYPE, p_Name IN dgtkoprm_dbt.t_name%TYPE )
  IS
    v_TypeID number := 6; -- тип = string
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
	  SELECT nvl(max(t_code), 0) + 1 INTO v_Code FROM dgtkoprm_dbt r WHERE r.t_objectkind = p_ObjKind  AND T_CODE < 1000;
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
  -- добавление полей в таблицы загрузки из Payments
  addField('D_FOORDLOG_TMP', 'T_ISHDAYTRADE', 'char(1)'); 
  addField('D_FOORDLOG_TMP', 'T_FILEDATE', 'Date'); 
  addField('D_FO04_TMP',     'T_ISHDAYTRADE', 'char(1)'); 
  addField('D_FO04_TMP',     'T_FILEDATE', 'Date'); 
  -- добавление параметров в таблицу dgtkoprm_dbt
  addGTKOPRM(95, 'RGDVRQ_ISHDAYTRADE'); -- для заявок
  addGTKOPRM(86, 'RGDVDL_ISHDAYTRADE'); -- для сделок

END;
/

--Компилирование пакета, который использует таблицы
DECLARE
BEGIN
  EXECUTE IMMEDIATE 'ALTER PACKAGE RSB_GTIM_CSV COMPILE BODY';
  
  EXCEPTION WHEN others THEN 
    DBMS_OUTPUT.PUT_LINE('Ошибка: '||SQLCODE||' - '||SQLERRM); 
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'BOSS-9229 Release 118.2 compile RSB_GTIM_CSV',
               p_msg_type => it_log.C_MSG_TYPE__ERROR -- при указании такого типа, функция залогирует стек ошибки, собранный в it_error
              );
    it_error.clear_error_stack;
END;
/