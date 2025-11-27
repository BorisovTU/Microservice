DECLARE

BEGIN

  UPDATE DNPTXOBJ_DBT OBJ
     SET OBJ.T_TAXPERIOD = NVL((SELECT EXTRACT(YEAR FROM OP.T_PREVDATE)
                                  FROM DNPTXOBDC_DBT DC, DNPTXOP_DBT OP
                                 WHERE DC.T_OBJID = OBJ.T_OBJID
                                   AND OP.T_ID = DC.T_DOCID
                                   AND OP.T_DOCKIND = 4608
                               ), EXTRACT(YEAR FROM OBJ.T_DATE))
   WHERE OBJ.T_LEVEL = 8
     AND OBJ.T_TAXPERIOD = 0
     AND OBJ.T_DATE >= TO_DATE('01.01.2021','DD.MM.YYYY');

END;
/
