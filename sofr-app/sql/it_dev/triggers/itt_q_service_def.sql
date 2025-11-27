create or replace trigger itt_q_service_def
  before insert or update on itt_q_service
  for each row
  -- Фиксация времени изменения 
begin
  if inserting
  then
    :new.service_id     := its_main.nextval;
    :new.create_sysdate := sysdate;
  else
    if updating('create_sysdate')
    then
      :new.create_sysdate := :old.create_sysdate;
    end if;
  end if;
  :new.update_time := systimestamp;
end ;
/

