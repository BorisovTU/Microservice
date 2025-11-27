--DEF-73417 обновить базовый актив у депозитарных расписок на основе данных в буферной таблице РуДата
BEGIN
  update dfininstr_dbt fin
     set t_parentfi = 
         (select nvl((select t_fiid from davoiriss_dbt where t_isin=ISINCODEBASE_NRD or t_lsin=ISINCODEBASE_NRD), 1) 
           from sofr_info_fintoolreferencedata where status='В обращении' 
            and  isincode = (select t_isin from davoiriss_dbt avr where avr.t_fiid = fin.t_fiid) or regcode = (select t_lsin from davoiriss_dbt avr where avr.t_fiid = fin.t_fiid)  
         )
   where t_fiid in 
      (select fin.t_fiid from dfininstr_dbt fin, davoiriss_dbt avr 
        where fin.t_fiid = avr.t_fiid and RSB_FIInstr.FI_AvrKindsGetRoot(fin.t_FI_KIND, fin.t_AvoirKind) = 10 and fin.t_parentfi = 1 
          and exists 
         (select 1 from sofr_info_fintoolreferencedata where status='В обращении' and ( isincode = avr.t_isin or regcode = avr.t_lsin))
      );
END;
/