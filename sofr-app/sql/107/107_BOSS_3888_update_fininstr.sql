--Редактирование информации о фьючерсах на индексы
BEGIN
  --Редактируем индекс UKZT
  update dfininstr_dbt fin 
     set fin.T_SETTLEMENT_CODE = 1, --валютный индекс
         fin.T_NAME = 'Доллар США - казахстанский тенге', 
         fin.T_FACEVALUEFI = (SELECT T_FIID FROM DFININSTR_DBT WHERE T_CCY = 'USD'), 
         fin.T_PARENTFI = (SELECT T_FIID FROM DFININSTR_DBT WHERE T_CCY = 'KZT')  
   where fin.T_FI_CODE = 'UKZT';

   -- Для всех фьючерсных контрактов, у который базовый актив = валютный индекс, необходимо проставить валюту шага цены = котируемая валюта базового актива
   update dfideriv_dbt deriv 
      set deriv.t_tickfiid = (Select finb.t_parentfi 
                                from dfininstr_dbt finb 
                               where finb.T_FI_KIND  = 3 --индекс 
                                 AND finb.T_SETTLEMENT_CODE  = 1 --валютный
                                 AND finb.t_FIID = (Select fin.T_FACEVALUEFI 
                                                      from dfininstr_dbt fin 
                                                     where fin.t_FIID = deriv.T_FIID))
    where deriv.T_FIID IN (Select fin.t_FIID 
                             from dfininstr_dbt fin 
                            where fin.T_FI_KIND = 4 --ПИ
                              AND fin.T_AVOIRKIND = 1 --Фьючерс
                              AND fin.T_FACEVALUEFI IN (Select finb.t_FIID 
                                                          from dfininstr_dbt finb 
                                                         where finb.T_FI_KIND = 3 --индекс
                                                           AND finb.T_SETTLEMENT_CODE = 1/*валютный*/));

   -- Для всех фьючерсных контрактов выставить точность цены = 5
   update dfideriv_dbt deriv 
      set deriv.t_tickpoint = 5
    where deriv.T_FIID IN (Select fin.t_FIID 
                             from dfininstr_dbt fin 
                            where fin.T_FI_KIND = 4 --ПИ
                              AND fin.T_AVOIRKIND = 1); --Фьючерс

END;
/
