--удаление всех схем имя которых начинается с BANKDISTR_RSHB

BEGIN
  FOR i IN (SELECT t.username FROM dba_users t WHERE t.username like 'BANKDISTR_RSHB%')
  LOOP
    BEGIN
      EXECUTE IMMEDIATE 'DROP USER ' || i.username || ' CASCADE';
      EXCEPTION 
        WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE('Ошибка: '||SQLCODE||' - '||SQLERRM);
    END;
  END LOOP;
END;
/