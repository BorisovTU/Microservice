 DECLARE
  v_el_max number;
 BEGIN
   
  select max(T_ELEMENT)
    into v_el_max
    from DLLVALUES_DBT v 
   where t_list = 5009;
 
 
 
     --Вставка 'invest@rshb.ru' 'broker@rshb.ru' 'custody@rshb.ru' скрытая копия
	 v_el_max:= v_el_max+1;
     Insert into DLLVALUES_DBT
       (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     Values
       (5009, v_el_max, to_char(v_el_max), 'ALL_EML_H', 1, 
        'Всем адресатам (скрытый)', chr(1));
  
    insert into usr_email_addr_dbt(t_group, t_email, t_place, t_comment)
    values(v_el_max, 'invest@rshb.ru','H',chr(1));
    
    insert into usr_email_addr_dbt(t_group, t_email, t_place, t_comment)
    values(v_el_max, 'broker@rshb.ru','H',chr(1));
    
    insert into usr_email_addr_dbt(t_group, t_email, t_place, t_comment)
    values(v_el_max, 'custody@rshb.ru','H',chr(1));
    
   --Вставка 'invest@rshb.ru' 'broker@rshb.ru'  скрытая копия      
    v_el_max:= v_el_max+1;  
      
     Insert into DLLVALUES_DBT
       (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     Values
       (5009, v_el_max, to_char(v_el_max), 'WITHOUT_CASTODY_H', 1, 
        'Без сustody (скрытый)', chr(1));
        
    insert into usr_email_addr_dbt(t_group, t_email, t_place, t_comment)
    values(v_el_max, 'invest@rshb.ru','H',chr(1));
    
    insert into usr_email_addr_dbt(t_group, t_email, t_place, t_comment)
    values(v_el_max, 'broker@rshb.ru','H',chr(1));   
        
   --Вставка 'custody@rshb.ru' скрытая копия
     v_el_max:= v_el_max+1;            

     Insert into DLLVALUES_DBT
       (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     Values
       (5009, v_el_max, to_char(v_el_max), 'ONLY_CASTODY_H', 1, 
        'Только  сustody (скрытый)', chr(1));          

     insert into usr_email_addr_dbt(t_group, t_email, t_place, t_comment)
     values(v_el_max, 'custody@rshb.ru','H',chr(1));
     
     
     --Вставка 'custody@rshb.ru' R - основные получатели
     v_el_max:= v_el_max+1;            

     Insert into DLLVALUES_DBT
       (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
     Values
       (5009, v_el_max, to_char(v_el_max), 'ONLY_CASTODY_R', 1, 
        'Только  сustody (основной)', chr(1));          

     insert into usr_email_addr_dbt(t_group, t_email, t_place, t_comment)
     values(v_el_max, 'custody@rshb.ru','R',chr(1));
    
 END;
 /