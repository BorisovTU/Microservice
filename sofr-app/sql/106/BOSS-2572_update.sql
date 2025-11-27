BEGIN
  INSERT INTO DSCSRVREPCNTR_DBT (T_SERVDOCID,
                                 T_DLCONTRID,
                                 T_GROUPNUMBER,
                                 T_ROWNUM,
                                 T_DLCONTRNUMBER,
                                 T_CLIENTNAME,
                                 T_EKK,
                                 T_CLIENTCODE)
     SELECT srvRep.T_ID,
            srvRep.T_CLIENTCONTRID,
            0,
            0,
            (SELECT t_number
               FROM dsfcontr_dbt sf
              WHERE t_id = SRVREP.T_CLIENTCONTRID)
               AS t_dlcontrnumber,
            prt.t_shortname,
            (SELECT dlobjcode.t_Code
               FROM ddlobjcode_dbt dlobjcode
              WHERE dlobjcode.t_ObjectType = 207 AND dlobjcode.t_CodeKind = 1
                    AND dlobjcode.t_BankCloseDate =
                           TO_DATE ('01 01 0001', 'DD MM YYYY')
                    AND dlobjcode.t_ObjectID = srvRep.T_CLIENTCONTRID)
               AS t_ekk,
            (SELECT t_code
               FROM dobjcode_dbt
              WHERE     t_objecttype = 3
                    AND t_codekind = 1
                    AND t_state = 0
                    AND t_objectid = srvRep.T_CLIENTID)
               AS t_clientcode
       FROM DSCSRVREP_DBT srvRep, dparty_dbt prt
      WHERE SRVREP.T_CLIENTCONTRID > 0 AND prt.t_partyid = srvRep.T_CLIENTID;
END;
/