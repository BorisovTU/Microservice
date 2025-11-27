/*Добавить тип прикрепляемого объекта*/
DECLARE
BEGIN

  INSERT INTO DIMGTYPE_DBT (T_IMAGETYPE, T_NAME, T_ISDEFAULT, T_OBJECTTYPE, T_SUBSYSTEMS)
  VALUES (102, 'Сведения о ФЛ и его ИИС, предоставл. пред. брокером', CHR(0), 207, CHR(1));

END;
/