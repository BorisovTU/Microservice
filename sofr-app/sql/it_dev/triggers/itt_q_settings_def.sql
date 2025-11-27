create or replace trigger itt_q_settings_def
  before insert or update on itt_q_settings
  for each row
    -- Фиксация времени изменения 
begin
  if inserting
  then
    :new.update_sysdate := sysdate;
    :new.create_sysdate := sysdate;
  else
    if updating('create_sysdate')
    then
      :new.create_sysdate := :old.create_sysdate;
    end if;
    :new.update_sysdate := sysdate;
  end if;
end ;
/
