BEGIN
  INSERT INTO DTRNRULES_DBT (T_ID,
                            T_ACC_MASK,
                            T_SIDE,
                            T_ATTR_KIND,
                            T_OPER,
                            T_ATTR_VALUE,
                            T_ISACTIVE,
                            T_STARTDATE,
                            T_ENDDATE)
      VALUES (0,
              '70601*',
              0,
              -1,
              -1,
              CHR (1),
              CHR (88),
              TO_DATE ('01.01.2020', 'dd.mm.yyyy'),
              TO_DATE ('01.01.2099', 'dd.mm.yyyy'));

  INSERT INTO DTRNRULES_DBT (T_ID,
                            T_ACC_MASK,
                            T_SIDE,
                            T_ATTR_KIND,
                            T_OPER,
                            T_ATTR_VALUE,
                            T_ISACTIVE,
                            T_STARTDATE,
                            T_ENDDATE)
      VALUES (0,
              '70606*',
              1,
              -1,
              -1,
              CHR (1),
              CHR (88),
              TO_DATE ('01.01.2020', 'dd.mm.yyyy'),
              TO_DATE ('01.01.2099', 'dd.mm.yyyy'));

  INSERT INTO DTRNRULES_DBT (T_ID,
                            T_ACC_MASK,
                            T_SIDE,
                            T_ATTR_KIND,
                            T_OPER,
                            T_ATTR_VALUE,
                            T_ISACTIVE,
                            T_STARTDATE,
                            T_ENDDATE)
      VALUES (0,
              '70606*',
              0,
              1,
              0,
              '70601*',
              CHR (88),
              TO_DATE ('01.01.2020', 'dd.mm.yyyy'),
              TO_DATE ('01.01.2099', 'dd.mm.yyyy'));

  INSERT INTO DTRNRULES_DBT (T_ID,
                            T_ACC_MASK,
                            T_SIDE,
                            T_ATTR_KIND,
                            T_OPER,
                            T_ATTR_VALUE,
                            T_ISACTIVE,
                            T_STARTDATE,
                            T_ENDDATE)
      VALUES (0,
              '70601810500002760408',
              1,
              2,
              0,
              '0',
              CHR (88),
              TO_DATE ('01.01.2020', 'dd.mm.yyyy'),
              TO_DATE ('01.01.2099', 'dd.mm.yyyy'));
              
  INSERT INTO DTRNRULES_DBT (T_ID,
                            T_ACC_MASK,
                            T_SIDE,
                            T_ATTR_KIND,
                            T_OPER,
                            T_ATTR_VALUE,
                            T_ISACTIVE,
                            T_STARTDATE,
                            T_ENDDATE)
      VALUES (0,
              '93*',
              0,
              1,
              2,
              '93*, 99997*',
              CHR (88),
              TO_DATE ('01.01.2020', 'dd.mm.yyyy'),
              TO_DATE ('01.01.2099', 'dd.mm.yyyy'));

  INSERT INTO DTRNRULES_DBT (T_ID,
                            T_ACC_MASK,
                            T_SIDE,
                            T_ATTR_KIND,
                            T_OPER,
                            T_ATTR_VALUE,
                            T_ISACTIVE,
                            T_STARTDATE,
                            T_ENDDATE)
      VALUES (0,
              '93*',
              1,
              0,
              1,
              '99997*',
              CHR (88),
              TO_DATE ('01.01.2020', 'dd.mm.yyyy'),
              TO_DATE ('01.01.2099', 'dd.mm.yyyy'));

  INSERT INTO DTRNRULES_DBT (T_ID,
                            T_ACC_MASK,
                            T_SIDE,
                            T_ATTR_KIND,
                            T_OPER,
                            T_ATTR_VALUE,
                            T_ISACTIVE,
                            T_STARTDATE,
                            T_ENDDATE)
      VALUES (0,
              '96*',
              0,
              1,
              2,
              '96*, 99996*',
              CHR (88),
              TO_DATE ('01.01.2020', 'dd.mm.yyyy'),
              TO_DATE ('01.01.2099', 'dd.mm.yyyy'));

  INSERT INTO DTRNRULES_DBT (T_ID,
                            T_ACC_MASK,
                            T_SIDE,
                            T_ATTR_KIND,
                            T_OPER,
                            T_ATTR_VALUE,
                            T_ISACTIVE,
                            T_STARTDATE,
                            T_ENDDATE)
      VALUES (0,
              '96*',
              1,
              0,
              1,
              '99996*',
              CHR (88),
              TO_DATE ('01.01.2020', 'dd.mm.yyyy'),
              TO_DATE ('01.01.2099', 'dd.mm.yyyy'));
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