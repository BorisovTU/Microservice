-- Тригер
create or replace trigger ITT_KAFKA_TOPIC_def
  before insert or update on ITT_KAFKA_TOPIC
  for each row
begin
  if inserting
  then
    :new.TOPIC_ID     := its_main.nextval;
    :new.create_sysdate := sysdate;
  else
    if updating('create_sysdate')
    then
      :new.create_sysdate := :old.create_sysdate;
    end if;
    if updating('TOPIC_ID')
    then
      :new.TOPIC_ID := :old.TOPIC_ID;
    end if;
  end if;
  :new.update_time := systimestamp;
end ;
/