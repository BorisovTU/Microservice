DECLARE
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE DLIMIT_BSDATECUR_DBT CASCADE CONSTRAINT';
EXCEPTION
   WHEN OTHERS
   THEN 
     NULL;
END;
/

DECLARE
BEGIN
   EXECUTE IMMEDIATE 'CREATE TABLE DLIMIT_BSDATECUR_DBT
                      (
                         T_BASEDATE         DATE,
                         T_SETTL_CURRID     NUMBER (10),
                         T_MARKETID         NUMBER (10),
                         T_SETTLEDATE       DATE,
                         T_DATEDIFF         NUMBER (10),
                         T_T1T2SHIFT        NUMBER (5)
                      )';
END;
/

DECLARE
BEGIN
   EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX DLIMIT_BSDATECUR_DBT_IDX0
                        ON DLIMIT_BSDATECUR_DBT (T_BASEDATE, T_MARKETID, T_SETTL_CURRID)';
END;
/