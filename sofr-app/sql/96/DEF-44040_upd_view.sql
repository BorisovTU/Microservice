--обновление view календарей
BEGIN
   EXECUTE IMMEDIATE '
     CREATE OR REPLACE FORCE VIEW DCALPRMTREE_VIEW
     (
        T_ID,
        T_IDENTPROGRAM,
        T_NAME,
        T_LEVEL,
        T_PARENT,
        T_TEXTID
     )
     AS
        (SELECT q3.t_id,
                q3.T_IDENTPROGRAM,
                q3.T_NAME,
                q3.T_Level,
                q3.t_parent,
                TO_CHAR(q3.t_calid) as T_TEXTID
           FROM (  SELECT q2.*
                     FROM (SELECT q1.*,
                                  ROW_NUMBER ()
                                  OVER (PARTITION BY t_Parent
                                        ORDER BY t_Level, T_KNDCODE ASC)
                                     AS t_grouprownum
                             FROM (SELECT calPrm.T_ID,
                                          tp.t_NAME_TYPE AS T_IDENTPROGRAM,
                                          calKnd.T_NAME,
                                          0 AS t_level,
                                          NULL AS T_KNDCODE,
                                          calPrm.T_ID AS t_Parent,
                                          calKnd.t_id as t_calid
                                     FROM DDLCALPARAM_DBT calPrm,
                                          DCALKIND_DBT calKnd,
                                          DTYPEAC_DBT tp
                                    WHERE calKnd.T_ID = calPrm.T_CALKINDID
                                          AND tp.T_TYPE_ACCOUNT =
                                                 CHR (calPrm.T_IDENTPROGRAM)
                                          AND tp.T_INUMTYPE = 32
                                   UNION ALL
                                   SELECT -1,
                                          tp.t_NAME_TYPE AS T_IDENTPROGRAM,
                                             prmKnd.T_NAME
                                          || '': ''
                                          || calLnk.T_TEXTVALUE
                                             AS t_Name,
                                          1 AS t_Level,
                                          calLnk.T_KNDCODE,
                                          calPrm.T_ID AS t_Parent,
                                          calPrm.T_CALKINDID as t_calid
                                     FROM DDLCALPARAMLNK_DBT calLnk,
                                          DDLCALPARAM_DBT calPrm,
                                          DDLCALPARAMKND_DBT prmKnd,
                                          DTYPEAC_DBT tp
                                    WHERE calPrm.T_ID = calLnk.T_CALPARAMID
                                          AND prmKnd.T_CODE = calLnk.T_KNDCODE
                                          AND tp.T_TYPE_ACCOUNT =
                                                 CHR (calPrm.T_IDENTPROGRAM)
                                          AND tp.T_INUMTYPE = 32) q1) q2
                 ORDER BY q2.T_IDENTPROGRAM, q2.t_parent, q2.t_grouprownum) q3)';
END;
/