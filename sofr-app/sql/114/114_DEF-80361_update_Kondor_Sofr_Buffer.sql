BEGIN
  UPDATE Kondor_SOFR_Buffer_dbt 
     SET t_DateComplete = t_DateCreate, t_ErrorStatus = -1000, t_ErrorMessage = 'Сделки Repo не обрабатываются: '||t_DealCode
   WHERE t_ReqType = 'N'
     AND t_DateComplete = to_date('01.01.0001', 'DD.MM.YYYY')
     AND t_DealCode LIKE 'Repo:%';
END;
/