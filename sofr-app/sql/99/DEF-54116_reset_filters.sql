-- Сброс фильтра

DECLARE
   v_err                NUMBER (10) := 0;
   v_KeyID              NUMBER (10) := 0;
   v_KeyType            NUMBER (10) := 0;
   v_RegKind            NUMBER (10) := 2;
   v_ObjectID           NUMBER (10) := 0;
   v_UserValueBlocked   VARCHAR2 (10);
BEGIN
   v_err :=
      RSB_Common.RSI_GetRegValueParam ('COMMON\FILTERS\S\16824__NPTXOFLT',
                                       v_KeyID,
                                       v_KeyType,
                                       v_RegKind,
                                       v_ObjectID,
                                       v_UserValueBlocked);

   IF ( (v_err = 0) AND (v_KeyID > 0))
   THEN
      DELETE FROM dregval_dbt
            WHERE t_KeyID = v_KeyID;
   END IF;
END;
/