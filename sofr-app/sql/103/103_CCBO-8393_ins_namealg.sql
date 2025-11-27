--Добавление нового регистра по собственным облигациям и вызова двух новых регистров одновременно 
BEGIN
insert into DNAMEALG_DBT (T_ITYPEALG, T_INUMBERALG, T_SZNAMEALG, T_ILENNAME, T_IQUANTALG)
values (8243, 3, 'АР-07-17-1', 26, 0);                        
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/
 
BEGIN
insert into DNAMEALG_DBT (T_ITYPEALG, T_INUMBERALG, T_SZNAMEALG, T_ILENNAME, T_IQUANTALG)
values (8243, 5, 'АР-07-17-1 и АР-07-17-2', 26, 0);                        
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/