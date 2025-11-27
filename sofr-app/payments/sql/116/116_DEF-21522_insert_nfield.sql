DECLARE
  l_tableid number;
  l_formid number;
  l_cnt number;
BEGIN
  SELECT id_ntable
    INTO l_tableid
    FROM ntable
   WHERE upper(name) = 'DSS_SHEDULER_DBT';
   
   SELECT id_nform
     INTO l_formid
     FROM nform
    WHERE text = 'Плановая процедура';

  SELECT count(1)
    INTO l_cnt
    FROM nfield
   WHERE id_nform = l_formid
     AND id_ntable = l_tableid
     AND UPPER(name) = 'T_NEXTSTAMP';

  IF l_cnt = 0 THEN
    INSERT INTO nfield 
      (id_nform, id_ntable, id_stypefield, len, name, 
       redefinition, issend, issign, isgrid, isvisible, 
       text, istextup, orderfield, top, left, 
       isupper, isedit, islkp, islkponly)
    VALUES (l_formid, l_tableid, 9, 0, 'T_NEXTSTAMP',
            0, 0, 0, 1, 1, 
            'Следующий запуск', 0, 11983, 320, 180,
            0, 0, 0, 0);
  END IF;
END;
/