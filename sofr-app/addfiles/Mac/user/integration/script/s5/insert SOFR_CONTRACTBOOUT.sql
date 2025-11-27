INSERT INTO SOFR_CONTRACTBOOUT (CONTRACTID,
                                CONTRACTNUMBER,
                                CONTRACTDATE,
                                CLIENTTYPE,
                                CLIENTID,
                                BRANCHID)
   SELECT contr.t_id,
          contr.t_number,
          contr.t_datebegin,
          party.t_legalform,
          contr.t_partyid,
          contr.t_department
     FROM dsfcontr_dbt contr
          LEFT JOIN dparty_dbt party ON contr.t_partyid = party.t_partyid
    WHERE contr.t_id IN (20, 21, 25)