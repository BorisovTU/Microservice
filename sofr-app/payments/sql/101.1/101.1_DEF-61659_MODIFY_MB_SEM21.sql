/*Расширение поля value для MB_SEM21*/
BEGIN
  EXECUTE IMMEDIATE 'alter table mb_sem21 modify (value number(26,8))';
END;
/

BEGIN
  EXECUTE IMMEDIATE 'alter table tmp_mb_sem21 modify (value number(26,8))';
END;
/

