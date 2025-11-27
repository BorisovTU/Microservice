-- Изменения по DEF-57381, Добавление документа категории
DECLARE
  logID VARCHAR2(9) := 'DEF-57381';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Добавление связки документа и категории
  PROCEDURE AddMcdoccat(p_DocName IN VARCHAR2, p_CategCode IN VARCHAR2)
  AS
    x_DocKind NUMBER;
    x_CatID NUMBER;
    x_CatNum NUMBER;
  BEGIN
    LogIt('Добавление связки документа '''||p_DocName||''' и категории '''||p_CategCode||'''');
    SELECT t_dockind INTO x_DocKind FROM doprkdoc_dbt r WHERE r.t_name = p_DocName;
    SELECT t_id, t_number INTO x_CatID, x_CatNum FROM dmccateg_dbt r WHERE r.t_code = p_CategCode;
    INSERT INTO dmcdoccat_dbt r (
      r.t_catid, r.t_catnum, r.t_dockind, r.t_notinuse, r.t_reserve
    ) VALUES (
      x_CatID, x_CatNum, x_DocKind, chr(0), chr(1)
    );

    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Добавлена связка документа '''||p_DocName||''' и категории '''||p_CategCode||'''');
  EXCEPTION
    WHEN OTHERS THEN
       LogIt('Ошибка добавления связки документа '''||p_DocName||''' и категории '''||p_CategCode||'''');
       EXECUTE IMMEDIATE 'ROLLBACK';
  END;
BEGIN
  -- AddMcdoccat
  AddMcdoccat(
    'Анкета векселя' 			-- документ
    , 'НДФЛ к перечислению 15%'         -- категория
  );
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/

