/*Обновление меню*/
DECLARE
BEGIN
   UPDATE DMENUITEM_DBT
      SET T_SZNAMEITEM = 'Операции сверки СНОБ',
          T_SZNAMEPROMPT = 'Операции сверки СНОБ'
    WHERE T_ICASEITEM = 20215
      AND T_IIDENTPROGRAM = 83
      AND trim(replace(T_SZNAMEITEM, '~', '')) = 'Операции технической сверки СНОБ';

END;
/