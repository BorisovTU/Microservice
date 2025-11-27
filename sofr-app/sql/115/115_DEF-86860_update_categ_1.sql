/*Обновление категорий*/
DECLARE
BEGIN

UPDATE dmctempl_dbt tpl
   SET tpl.t_Kind_Account = 'А'
 WHERE tpl.t_CatID IN
          (SELECT cat.t_ID
             FROM dmccateg_dbt cat
            WHERE cat.t_Code IN
                     ('НДФЛ к возврату_18%',
                      'НДФЛ к возврату_20%',
                      'НДФЛ к возврату_22%'))
       AND tpl.t_Kind_Account = 'П';
END;
/