create or replace force view itv_q_taskxx as
-- Задания из табличной очереди XX
select t.queuename
      ,t.qmsgid
      ,t.state
      ,t.correlation
      ,t.enqdt
      ,t.local_order_no
      ,t.msgid
      ,t.corrmsgid
      ,t.message_type
      ,t.delivery_type
      ,t.priority
      ,t.servicename
      ,t.servicegroup
      ,t.sender
      ,t.senderuser
      ,t.receiver
      ,t.txtmessbody
  from ITV_Q_INXX t 
 where t.state in (0, 1)
   and t.delay <= systimestamp
   and (t.message_type = 'R' or (t.message_type = 'A' and t.delivery_type = 'A'))
   and it_q_message.check_correlation(p_correlation => t.correlation
                                     ,p_message_type => t.message_type
                                     ,p_delivery_type => t.delivery_type
                                     ,p_priority => t.priority
                                     ,p_msgid => t.msgid
                                     ,p_corrmsgid => t.corrmsgid
                                     ,p_qmcheck => 1) = 1
   with read only
/
