BEGIN
  insert into dnamealg_dbt nm (nm.t_itypealg, nm.t_inumberalg, nm.t_sznamealg, nm.t_ilenname, nm.t_iquantalg )
    values (3167,6,'ожидание активации',24,5);
  update dnamealg_dbt nm set nm.t_iquantalg = 6 where nm.t_itypealg = 3167;
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
