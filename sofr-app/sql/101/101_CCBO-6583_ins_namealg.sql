--Добавление нового регистра по собственным облигациям 
BEGIN
insert into DNAMEALG_DBT (T_ITYPEALG, T_INUMBERALG, T_SZNAMEALG, T_ILENNAME, T_IQUANTALG)
values (8243, 4, 'АР-07-17-2', 26, 0);                        
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/