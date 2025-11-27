/*Обновление меню*/
DECLARE
BEGIN
   UPDATE DITEMSYST_DBT
      SET T_SZNAMEITEM = 'Операции сверки СНОБ'
    WHERE T_ICASEITEM = 20215
      AND T_CIDENTPROGRAM = 'S'
      AND trim(replace(T_SZNAMEITEM, '~', '')) = 'Операции технической сверки СНОБ';

END;
/