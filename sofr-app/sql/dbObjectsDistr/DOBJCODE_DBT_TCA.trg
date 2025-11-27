CREATE OR REPLACE TRIGGER DOBJCODE_DBT_TCA 
  AFTER INSERT OR UPDATE OF T_OBJECTTYPE, T_OBJECTID, T_CODEKIND, T_CODE, T_BANKDATE, T_BANKCLOSEDATE, T_UNIQUE
  ON DOBJCODE_DBT
DECLARE
  L_MAX INTEGER;
  V_T_OBJECTTYPE    DOBJCODE_DBT.T_OBJECTTYPE%TYPE;
  V_T_OBJECTID      DOBJCODE_DBT.T_OBJECTID%TYPE;
  V_T_CODEKIND      DOBJCODE_DBT.T_CODEKIND%TYPE;
  V_T_CODE          DOBJCODE_DBT.T_CODE%TYPE;
  V_T_BANKDATE      DOBJCODE_DBT.T_BANKDATE%TYPE;
  V_T_BANKCLOSEDATE DOBJCODE_DBT.T_BANKCLOSEDATE%TYPE;
  V_T_UNIQUE        DOBJCODE_DBT.T_UNIQUE%TYPE;
  V_T_AUTOKEY       DOBJCODE_DBT.T_AUTOKEY%TYPE;
  V_T_STATE         DOBJCODE_DBT.T_STATE%TYPE;

  v_stmt         VARCHAR2(1000);
  v_raise_exception NUMBER := 0;

  --created by objcode_trig_c.sql
BEGIN
  IF TRGPCKG_DOBJCODE_DBTC.V_NUMENT <> 0 THEN
    FOR V_I IN 1..TRGPCKG_DOBJCODE_DBTC.V_NUMENT LOOP
      V_T_OBJECTTYPE    :=TRGPCKG_DOBJCODE_DBTC.V_T_OBJECTTYPE (V_I);
      V_T_OBJECTID      :=TRGPCKG_DOBJCODE_DBTC.V_T_OBJECTID (V_I);
      V_T_CODEKIND      :=TRGPCKG_DOBJCODE_DBTC.V_T_CODEKIND (V_I);
      V_T_CODE          :=TRGPCKG_DOBJCODE_DBTC.V_T_CODE (V_I);
      V_T_BANKDATE      :=TRGPCKG_DOBJCODE_DBTC.V_T_BANKDATE (V_I);
      V_T_BANKCLOSEDATE :=TRGPCKG_DOBJCODE_DBTC.V_T_BANKCLOSEDATE (V_I);
      V_T_UNIQUE        :=TRGPCKG_DOBJCODE_DBTC.V_T_UNIQUE (V_I);
      V_T_AUTOKEY       :=TRGPCKG_DOBJCODE_DBTC.V_T_AUTOKEY(V_I);
      V_T_STATE         :=TRGPCKG_DOBJCODE_DBTC.V_T_STATE(V_I);

      IF V_T_BANKDATE < V_T_BANKCLOSEDATE THEN
        SELECT COUNT (*)
          INTO L_MAX
          FROM DOBJCODE_DBT
         WHERE T_OBJECTTYPE = V_T_OBJECTTYPE
           AND T_CODEKIND = V_T_CODEKIND
           AND (   (T_CODE = V_T_CODE and T_OBJECTTYPE != 207 and not (T_OBJECTTYPE = 3 AND T_CODEKIND=73))
                OR (    T_UNIQUE = V_T_UNIQUE
                    AND T_UNIQUE = 'X'
                    AND T_OBJECTID = V_T_OBJECTID
                   )
               )
           AND (    (   (    V_T_BANKDATE <= T_BANKDATE
                         AND T_BANKDATE < V_T_BANKCLOSEDATE
                        )
                     OR (    V_T_BANKDATE < DECODE(T_BANKCLOSEDATE, TO_DATE('01-01-0001', 'DD-MM-YYYY'), TO_DATE('31-12-9999', 'DD-MM-YYYY'), T_BANKCLOSEDATE)
                         AND DECODE(T_BANKCLOSEDATE, TO_DATE('01-01-0001', 'DD-MM-YYYY'), TO_DATE('31-12-9999', 'DD-MM-YYYY'), T_BANKCLOSEDATE) <= V_T_BANKCLOSEDATE
                        )
                     OR (    T_BANKDATE <= V_T_BANKDATE
                         AND V_T_BANKCLOSEDATE <= DECODE(T_BANKCLOSEDATE, TO_DATE('01-01-0001', 'DD-MM-YYYY'), TO_DATE('31-12-9999', 'DD-MM-YYYY'), T_BANKCLOSEDATE)
                        )
                    )
                AND T_BANKDATE < DECODE(T_BANKCLOSEDATE, TO_DATE('01-01-0001', 'DD-MM-YYYY'), TO_DATE('31-12-9999', 'DD-MM-YYYY'), T_BANKCLOSEDATE)
               );
      ELSE
        L_MAX := 0;
      END IF;
      IF ( L_MAX > 1 ) THEN
        TRGPCKG_DOBJCODE_DBTC.V_NUMENT := 0;

        v_stmt := 'BEGIN ' ||
                  '  INSERT INTO err$_dobjcode_dbt ' ||
                  '  (' ||
                  '   ora_err_mesg$, ' ||
                  '   t_autokey, ' ||
                  '   t_objecttype, ' ||
                  '   t_objectid, ' ||
                  '   t_codekind, ' ||
                  '   t_code, ' ||
                  '   t_bankdate, ' ||
                  '   t_state, ' ||
                  '   t_unique, ' ||
                  '   t_bankclosedate' ||
                  '  ) ' ||
                  '  VALUES ' ||
                  '  ( ' ||
                      '''ORA-04088: error during execution of trigger ''''DOBJCODE_DBT_TCA'''''', ' ||
                      '''' || TO_CHAR(V_T_AUTOKEY) || ''', ' ||
                      '''' || TO_CHAR(V_T_OBJECTTYPE) || ''', ' ||
                      '''' || TO_CHAR(V_T_OBJECTID) || ''', ' ||
                      '''' || TO_CHAR(V_T_CODEKIND) || ''', ' ||
                      '''' || REPLACE(TO_CHAR(V_T_CODE),'''','''''') || ''', ' ||
                      '''' || TO_CHAR(V_T_BANKDATE) || ''', ' ||
                      '''' || TO_CHAR(V_T_STATE) || ''', ' ||
                      '''' || REPLACE(TO_CHAR(V_T_UNIQUE),'''',chr(88)) || ''', ' ||
                      '''' || TO_CHAR(V_T_BANKCLOSEDATE) || '''' ||
                  '  );' ||
                  'END;' ;
        RSB_TOOLS.dynamic_autonomous(v_stmt);

        v_raise_exception := 1;
      END IF;

    END LOOP;

    IF v_raise_exception > 0 THEN
      RAISE DUP_VAL_ON_INDEX;

    END IF;

    TRGPCKG_DOBJCODE_DBTC.V_NUMENT := 0;
  END IF;

END DOBJCODE_DBT_TCA;
/