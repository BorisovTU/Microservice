BEGIN
  DELETE dobjatcor_dbt atcor
   WHERE atcor.t_ObjectType = 650
     AND atcor.t_Object IN (SELECT '000010'||com.t_Number
                              FROM dsfcomiss_dbt com
                             WHERE com.t_FeeType = 3
                               AND com.T_ServiceKind = 21);
END;
/
