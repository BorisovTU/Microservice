/* обновление меню*/
DECLARE
BEGIN

   UPDATE dmenuitem_dbt
      SET t_sznameitem = 'Форма 6-НДФЛ 2022 г.'
    WHERE trim(replace(t_sznameitem, '~', '')) = '6-НДФЛ с 01.01.2021 с расшифровками';

END;
/
