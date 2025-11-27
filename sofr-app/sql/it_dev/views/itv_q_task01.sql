create or replace force view itv_q_task01 as
-- Задания из очереди 01
select tq.queuename
      ,tq.qmsgid
      ,tq.state
      ,tq.correlation
      ,tq.enqdt
      ,tq.local_order_no
      ,tq.msgid
      ,tq.corrmsgid
      ,tq.message_type
      ,tq.delivery_type
      ,tq.priority
      ,tq.servicename
      ,tq.servicegroup
      ,tq.sender
      ,tq.senderuser
      ,tq.receiver
      ,tq.txtmessbody
  from ITV_Q_IN01 tq 
 where tq.state = 0
   and (tq.message_type = 'R' or (tq.message_type = 'A' and tq.delivery_type = 'A'))
   and it_q_message.check_correlation(p_correlation => tq.correlation
                                     ,p_message_type => tq.message_type
                                     ,p_delivery_type => tq.delivery_type
                                     ,p_priority => tq.priority
                                     ,p_msgid => tq.msgid
                                     ,p_corrmsgid => tq.corrmsgid
                                     ,p_qmcheck => 1) = 1
   and tq.qmsgid not in (select wm.qmsgid from itt_q_work_messages wm)
/