create or replace view itv_q_out01 as
select q.q_name queuename -- Исходящие сообщения очереди 01
      ,q.msgid qmsgid
      ,q.state
      ,q.corrid correlation
      ,cast(from_tz(q.enq_time, '00:00') at time zone sessiontimezone as timestamp) enqdt
      ,q.local_order_no
      ,q.user_data.msgid msgid
      ,q.user_data.corrmsgid corrmsgid
      ,q.user_data.message_type  message_type
      ,q.user_data.delivery_type delivery_type
      ,q.user_data.priority      priority
      ,q.user_data.servicename   servicename
      ,q.user_data.servicegroup  servicegroup
      ,q.user_data.sender        sender
      ,q.user_data.senderuser    senderuser
      ,q.user_data.receiver      receiver
      ,dbms_lob.substr(q.user_data.messbody, 128, 1) txtmessbody
  from itt_queue_out_01 q
 where q.q_name = 'ITQ_OUT_01' with read only
/
