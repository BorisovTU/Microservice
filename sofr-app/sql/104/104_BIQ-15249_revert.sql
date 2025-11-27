DECLARE
    cnt       NUMBER;
BEGIN
    SELECT COUNT (1)
      INTO cnt
      FROM dmenuitem_dbt 
     WHERE t_sznameitem like '%Загрузить операции вывода из ДУ (АРНУ)%';

    IF CNT > 0
    THEN
      delete from dmenuitem_dbt where t_sznameitem like '%Загрузить операции вывода из ДУ (АРНУ)%';
      delete from dmenuitem_dbt menu 
        where trim(t_sznameitem) like '%Репликация' and t_iidentprogram = 84 
         and (select count(1) from dmenuitem_dbt m where m.t_inumberfather = menu.t_inumberpoint 
         and m.t_iidentprogram = 84 and m.t_objectid = menu.t_objectid and m.t_istemplate = menu.t_istemplate ) = 0;
    END IF;
    COMMIT;
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/