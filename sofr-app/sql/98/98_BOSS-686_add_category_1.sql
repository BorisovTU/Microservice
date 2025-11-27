DECLARE
  v_GroupID number := 116; 
  v_GroupName varchar2(32) := 'Margin Call';
  -- Процедура добавления группы
  PROCEDURE addObjGroup(p_ObjType number)
  IS
    v_Name dobjgroup_dbt.t_name%TYPE;
    v_NeedToAdd boolean := false; -- нужно добавлять ?
  BEGIN
    -- проверка, существует ли группа
    BEGIN
      SELECT t_name INTO v_Name FROM dobjgroup_dbt r WHERE r.t_objecttype = p_ObjType and r.t_groupid = v_GroupID;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN 
        v_NeedToAdd := true;
    END;
    -- если группы нет, создаем
    IF v_NeedToAdd = true THEN
       BEGIN
          INSERT INTO dobjgroup_dbt ( 
             t_objecttype, t_groupid, t_type, t_name, t_system, t_order, t_macroname, t_keepoldvalues
             , t_updateflag, t_successopflag, t_attrobjecttype, t_attrgroupid, t_ishidden, t_fullnameisbasic
             , t_syshidden, t_notusefielduse, t_ismeanparentnode, t_ismanualfirst, t_comment
          ) VALUES (
             p_ObjType, v_GroupID, 'X', v_GroupName, 'X', v_GroupID, chr(1), chr(0)
             , 1, chr(0), 0, 0, chr(0), chr(0) 
             , chr(0), chr(0), chr(0), chr(0), chr(1)
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
  -- Процедура добавления значения
  PROCEDURE addObjAttr(p_ObjType number, p_AttrID number, p_Num varchar2, p_Name varchar2)
  IS
    v_Date date := to_date('01-01-0001:00:00:00', 'DD.MM.YYYY HH24:MI:SS'); 
    v_Name dobjattr_dbt.t_name%TYPE;
    v_NeedToAdd boolean := false; -- нужно добавлять ?
  BEGIN
    -- проверка, существует ли значение
    BEGIN
      SELECT t_name INTO v_Name FROM dobjattr_dbt r WHERE r.t_objecttype = p_ObjType AND r.t_groupid = v_GroupID AND r.t_attrid = p_AttrID;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN 
        v_NeedToAdd := true;
    END;
    -- если значения нет, создаем
    IF v_NeedToAdd = true THEN
       BEGIN
          INSERT INTO dobjattr_dbt ( 
             t_objecttype, t_groupid, t_attrid, t_parentid, t_codelist, t_numinlist, t_nameobject
             , t_chattr, t_longattr, t_intattr, t_name, t_fullname, t_opendate, t_closedate
             , t_classificator, t_corractype, t_balance, t_isobject
             
          ) VALUES (
             p_ObjType, v_GroupID, p_AttrID, 0, chr(1), p_Num, p_Num
             , chr(0), 0, 0, p_Name, p_Name, v_Date, v_Date
             , 0, chr(1), chr(1), chr(0)
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
  -- Процедура добавления значений
  PROCEDURE addObjAttrs(aObjType number)
  IS
  BEGIN
    addObjAttr(aObjType, 1, '1', 'Да');
    addObjAttr(aObjType, 2, '0', 'Нет');
  EXCEPTION
    WHEN others THEN 
      null;
  END;
BEGIN
  -- 1. Добавление групп
  addObjGroup(140); -- срочный рынок, операция с ПИ
  addObjGroup(145); -- валютный рынок, внебиржевая операция с ПИ
  addObjGroup(148); -- валютный рынок, конверсионная операция
  -- 2. Добавление значений
  addObjAttrs(140); -- срочный рынок, операция с ПИ
  addObjAttrs(145); -- валютный рынок, внебиржевая операция с ПИ
  addObjAttrs(148); -- валютный рынок, конверсионная операция
END;
/
