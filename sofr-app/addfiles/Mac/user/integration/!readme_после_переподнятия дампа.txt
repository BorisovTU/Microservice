update dobjects_dbt set t_usernumber = 5005 where t_objecttype = 4
update dobjects_dbt set t_usernumber = 5006 where t_objecttype = 1
update dobjects_dbt set t_usernumber = 5007 where t_objecttype = 501


call DBMS_AQADM.START_QUEUE (Queue_name => 'aq_outgoing_ws');
call DBMS_AQADM.START_QUEUE (Queue_name => 'aq_incoming_ws');