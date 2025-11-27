create or replace view itv_q_out as
-- Исходящие сообщения для обработки внешними системами
-- union all itv_q_out02 b и т.д.
select q01.queuename,
       q01.qmsgid,
       q01.state,
       q01.correlation,
       q01.enqdt,
       q01.local_order_no,
       q01.msgid,
       q01.corrmsgid,
       q01.message_type,
       q01.delivery_type,
       q01.priority,
       q01.servicename,
       q01.servicegroup,
       q01.sender,
       q01.senderuser,
       q01.receiver,
       q01.txtmessbody
  from itv_q_out01 q01
 where q01.state = 0
union all
select  qXX.queuename  
       ,qXX.qmsgid
       ,qXX.state
       ,qXX.correlation
       ,qXX.enqdt
       ,qXX.local_order_no
       ,qXX.msgid
       ,qXX.corrmsgid
       ,qXX.message_type
       ,qXX.delivery_type
       ,qXX.priority
       ,qXX.servicename
       ,qXX.servicegroup
       ,qXX.sender
       ,qXX.senderuser
       ,qXX.receiver
       ,qXX.txtmessbody
  from itv_q_outxx qXX
  where qXX.state in ( 0,1) and qXX.delay <= systimestamp  
/
