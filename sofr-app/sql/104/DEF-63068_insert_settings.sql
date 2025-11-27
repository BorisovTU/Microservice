declare
begin
Insert into SOFR_IPS_SETTINGS
   (T_ID, T_SERVICENAME, T_PROCNAME, T_PROCNAMEANSWER, T_DIRECTION, T_TARGETSYSTEM, T_COMMENT)
 Values
   (1, 'eprshb_to_sofr_verify_snob_result', 'rshb_Sofr_ServiceReceive_Message', 'rshb_Sofr_ServiceStatus_Set', 0, 
    'SOFR', '‚ε®¤οι¨© ―®β®ª Ά ‘” ¨§ ‘•');
Insert into SOFR_IPS_SETTINGS
   (T_ID, T_SERVICENAME, T_PROCNAME, T_PROCNAMEANSWER, T_DIRECTION, T_TARGETSYSTEM, T_COMMENT)
 Values
   (2, 'sofr_to_eprshb_verify_snob', 'rshb_Sofr_ServiceSend_Message', 'rshb_Sofr_ServiceStatus_Set', 1, 
    'EPRSHB', 'αε®¤οι¨© ―®β®ª Ά ‘• ¨§ ‘”');
END;
/