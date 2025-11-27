/*Добавить записи*/
DECLARE
  v_RecNum NUMBER;
BEGIN

  FOR one_dlc IN (SELECT dlc.*,
                         NVL((SELECT MAX(ib.t_RecNum) FROM ddlcontrinfbrok_dbt ib WHERE ib.t_DlContrID = dlc.t_DlContrID), 0) As MaxRecNum
                    FROM ddlcontr_dbt dlc, dsfcontr_dbt sf
                   WHERE sf.t_ID = dlc.t_SfContrID
                     AND dlc.t_IIS = 'X'
                     AND dlc.t_IISTransfer = 'X'
                     AND sf.t_DateCLose = TO_DATE('01.01.0001','DD.MM.YYYY')  
                     AND NOT EXISTS(SELECT 1 FROM ddlcontrinfbrok_dbt ib WHERE ib.t_DlContrID = dlc.t_DlContrID and ib.t_Type IN (1,2))
                )
  LOOP                         
  
    v_RecNum :=  one_dlc.MaxRecNum;
    
    IF one_dlc.T_IISFirstDate > TO_DATE('01.01.0001','DD.MM.YYYY') THEN
       v_RecNum := v_RecNum + 1;
           
       INSERT INTO DDLCONTRINFBROK_DBT
       (T_INFID, T_DLCONTRID, T_RECNUM, T_BROKERNAME, T_CONTRNUMBER, T_CONTRDATE, T_NUMANDDATEREF, T_LISTCOUNT, T_OPER, T_CHANGEDATE, T_CHANGETIME, T_TYPE, T_VERSION)
       VALUES(0, one_dlc.t_DlContrID, v_RecNum, one_dlc.t_IISFirstBrokerName, one_dlc.t_IISFirstNumber, one_dlc.t_IISFirstDate, CHR(1), 0, 1, TO_DATE('01.01.0001','DD.MM.YYYY'), TO_DATE('01.01.0001','DD.MM.YYYY'), 1, 0);
    END IF;
     
    IF one_dlc.t_IISLastOpenDate != TO_DATE('01.01.0001','DD.MM.YYYY') AND one_dlc.t_IISFirstDate != TO_DATE('01.01.0001','DD.MM.YYYY') AND one_dlc.t_IISLastOpenDate != one_dlc.t_IISFirstDate THEN
      v_RecNum := v_RecNum + 1;
          
      INSERT INTO DDLCONTRINFBROK_DBT
      (T_INFID, T_DLCONTRID, T_RECNUM, T_BROKERNAME, T_CONTRNUMBER, T_CONTRDATE, T_NUMANDDATEREF, T_LISTCOUNT, T_OPER, T_CHANGEDATE, T_CHANGETIME, T_TYPE, T_VERSION)
      VALUES(0, one_dlc.t_DlContrID, v_RecNum, one_dlc.t_IISLastBrokerName, one_dlc.t_IISLastNumber, one_dlc.t_IISLastOpenDate, CHR(1), 0, 1, TO_DATE('01.01.0001','DD.MM.YYYY'), TO_DATE('01.01.0001','DD.MM.YYYY'), 2, 0);

    END IF; 
     
   END LOOP;
   
END;
/