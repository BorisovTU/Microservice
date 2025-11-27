create or replace view itv_q_inxx as
select  'ITQ_IN_XX' queuename -- Входящие сообщения очереди XX
      ,q.qmsgid qmsgid
      ,q.state
      ,q.delay
      ,q.correlation correlation
      ,q.qenqdt enqdt
      ,q.log_id local_order_no
      ,q.msgid
      ,q.corrmsgid 
      ,q.message_type 
      ,q.delivery_type
      ,q.priority
      ,q.servicename
      ,q.servicegroup
      ,q.sender
      ,q.senderuser
      ,q.receiver
      ,Q.txtmessbody
  from itt_queue_in_XX q
/
