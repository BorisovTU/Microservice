DECLARE
    cnt       NUMBER;
    v_keyId   NUMBER;
BEGIN
    SELECT COUNT (1)
      INTO cnt
      FROM DREGPARM_DBT
     WHERE T_NAME =
           'ÑÄíÄ çÄó. ÇàÑàåéëíà áÄóàëãÖçàâ';

    IF CNT > 0
    THEN
        SELECT T_KEYID
          INTO v_keyId
          FROM DREGPARM_DBT
         WHERE T_NAME = 'SECUR' AND t_parentid = 0;

        SELECT T_KEYID
          INTO v_keyId
          FROM DREGPARM_DBT
         WHERE     T_NAME =
                   'ÑÄíÄ çÄó. ÇàÑàåéëíà áÄóàëãÖçàâ'
               AND t_parentid = v_keyId;

        DELETE FROM DREGVAL_DBT
              WHERE t_keyid = v_keyId;

        DELETE FROM DREGPARM_DBT
              WHERE t_keyid = v_keyId;

        COMMIT;
    END IF;
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/