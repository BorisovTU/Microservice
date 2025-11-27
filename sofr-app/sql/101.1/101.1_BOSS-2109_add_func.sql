BEGIN	
  INSERT INTO DFUNC_DBT
     (T_FUNCID, T_CODE, T_NAME, T_TYPE, T_FILENAME, 
      T_FUNCTIONNAME, T_INTERVAL, T_VERSION)
  VALUES
    (7778, 'ChngTariffAndSendMsg', 'Сменить тариф и отправить письмо', 1, 'ChngTariffAndSendMsg_funcobj.mac', 
     'ChngTariffAndSendMsg', 0, 0);
END;
/
