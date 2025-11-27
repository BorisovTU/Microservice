CREATE OR REPLACE package body IT_SEQ_ALM is

PROCEDURE Check_sequences AS
   i integer;
   percent_value constant numeric:= 95;
BEGIN
   --dbms_output.put_line('Begin ...');
  -- refresh data at the table Show_max_seq
  execute immediate ('TRUNCATE TABLE Show_max_seq');
  
  INSERT INTO Show_max_seq(sequence_Name, 
             Max_Value, 
             Min_Value, 
             Last_Number)
      select sequence_Name, 
             Max_Value, 
             Min_Value, 
             Last_Number
      from ALL_SEQUENCES
      where sequence_owner not in ('SYS','XDB', 'MDSYS','APEX_050000') 
        and Max_Value > 0
        and sequence_Name in ('U_F503_SEQ',
                              (select 'U_' || (select max(t_code) from dllvalues_dbt where t_list=5053) || '_SEQ' from dual) )
        and (Last_Number / (Max_Value - 1) * 100) > percent_value
        
               UNION ALL

      select sequence_Name, 
             Max_Value, 
             Min_Value, 
             Last_Number
      from ALL_SEQUENCES
      where sequence_owner not in ('SYS','XDB', 'MDSYS','APEX_050000')
        and Max_Value > 0
        and substr(sequence_Name, 1, 3) <> 'U_F'
        and (Last_Number / (Max_Value - 1) * 100) > percent_value
        
               UNION ALL

      -- there are sequences with negative numbers
      select sequence_Name, 
             Max_Value, 
             Min_Value, 
             Last_Number
      from ALL_SEQUENCES
      where sequence_owner not in ('SYS','XDB', 'MDSYS','APEX_050000')
        and Max_Value < 0
        and substr(sequence_Name, 1, 3) <> 'U_F'
        and (Last_Number / (Min_Value + 1) * 100) > percent_value;  

   i:= 1;
     
  for trg in 
    (select tr.trigger_name, 
            tr.trigger_body,
            tr.Owner,
            tr.Table_name,
            tr.Status 
     from all_triggers tr
     where tr.trigger_name <> 'DDVDEAL_DBT_TRBU' -- there is a problem with russian codepage in this trigger, CONVERT could not help
     )
  loop -- 
    for seq in 
      (SELECT upper(sequence_Name) AS seq_name
       FROM Show_max_seq)
       loop
         
        BEGIN 
         if instr(upper(convert(to_char(trg.trigger_body), 'CL8MSWIN1251', 'RU8PC866')), seq.seq_name) <> 0 then -- find trigger name
                dbms_output.put_line('Trigger name =  ' ||  trg.trigger_name);
           UPDATE Show_max_seq
           SET Trigger_name = trg.trigger_name,
               Owner = trg.Owner,
               Table_name = trg.Table_Name,
               Status = trg.status,
               Trigger_body = trg.Trigger_body
           WHERE upper(sequence_Name) = seq.seq_name;
         end if;  
        EXCEPTION WHEN OTHERS THEN
           dbms_output.put_line('-- error: ' ||  trg.trigger_name);  
        END;    

       end loop; -- sequences
      i:= i + 1;      
  end loop; -- triggers
    
EXCEPTION WHEN OTHERS THEN
    dbms_output.put_line('-- error: i = ' ||  to_char(i)); 
END;

END  IT_SEQ_ALM;
