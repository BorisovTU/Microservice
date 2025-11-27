/*Удаление неправильного пункта меню*/
DECLARE
BEGIN
  DELETE FROM DMENUITEM_DBT WHERE trim(replace(t_sznameitem, '~', '')) = 'Справочник особых условий' and t_inumberfather = 0; 
END;
/