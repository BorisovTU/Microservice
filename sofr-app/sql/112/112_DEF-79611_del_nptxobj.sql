/*Удалить лишние НДР*/
DECLARE
BEGIN

  FOR one_rec IN (SELECT DC.*
                    FROM DCDRECORDS_DBT CD, DCDNPTXOBDC_DBT DC
                   WHERE CD.T_CORPORATEACTIONTYPE = 'INTR'
                     AND CD.T_ISGETTAX = 'X'
                     AND DC.T_RECID = CD.T_ID
                 )
  LOOP

    DELETE FROM DCDNPTXOBDC_DBT WHERE T_ID = one_rec.T_ID;
    DELETE FROM DNPTXOBJBC_DBT WHERE T_OBJID = one_rec.T_OBJID;
    DELETE FROM DNPTXOBJ_DBT WHERE T_OBJID = one_rec.T_OBJID;

  END LOOP;
               
END;
/
