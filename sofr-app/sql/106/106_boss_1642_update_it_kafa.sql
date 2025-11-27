--исправления замечаний от Зыкова М.
begin
 delete ITT_Q_SERVICE   where servicename = 'CreateUpdateClientInfo';
 commit;
end; 
/

begin
 update ITT_KAFKA_TOPIC set servicename = 'SINV.CreateUpdateClientInfo'  where servicename = 'CreateUpdateClientInfo';
 commit;
end;
/ 