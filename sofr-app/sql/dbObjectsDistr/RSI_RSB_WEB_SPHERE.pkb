CREATE OR REPLACE PACKAGE BODY RSI_RSB_WEB_SPHERE IS

  FUNCTION InsRequestToWS(p_QueueID IN NUMBER, p_ReqIDTrEn IN VARCHAR2, p_ReqIDMes IN VARCHAR2, p_JMSCorrelationID IN VARCHAR2, p_XMLTrEn IN CLOB, p_Expiration IN NUMBER) RETURN INTEGER IS
    id                 pls_integer;
    agent              sys.aq$_agent := sys.aq$_agent(' ', null, 0);
    message            sys.aq$_jms_bytes_message;
    enqueue_options    dbms_aq.enqueue_options_t;
    message_properties dbms_aq.message_properties_t;
    msgid raw(16);
 ------------------------------------------------------------------------
 offset number := 1;
 amount number := 4500;
 len number := dbms_lob.getlength(p_XMLTrEn);
 lc_buffer varchar2(4500);
 i pls_integer := 1;
 ---------------------------------------------------------------------

    java_exp           exception;
    pragma EXCEPTION_INIT(java_exp, -24197);
  BEGIN

    -- Consturct a empty bytes message object
    message := sys.aq$_jms_bytes_message.construct;
/*
    -- Shows how to set the JMS header
    message.set_replyto(agent);
    message.set_type('tkaqpet1');
    message.set_userid('jmsuser');
    message.set_appid('plsql_enq');
    message.set_groupid('st');
    message.set_groupseq(1);
*/
    -- Shows how to set JMS user properties
    message.set_int_property('QueueID', p_QueueID);
    message.set_string_property('ReqIDTrEn', p_ReqIDTrEn);
    message.set_string_property('ReqIDMes', p_ReqIDMes);
    message.set_string_property('JMSCorrelationID', p_JMSCorrelationID);

    id := message.clear_body(-1);

 -- Write a String to the bytes message payload,
    -- the String is encoded in UTF8 in the message payload
 --message.write_utf(id, p_XMLTrEn);
 ---------------------------------------------------------------------------------------------
 while ( offset < len )
 loop
 dbms_lob.read(p_XMLTrEn, amount, offset, lc_buffer);
 --dbms_output.put_line('Line #'||i||':'||lc_buffer);
 dbms_output.put_line(lc_buffer);
 message.write_bytes(id, utl_raw.cast_to_raw(lc_buffer));
 offset := offset + amount;
 i := i + 1;
 end loop;
 ---------------------------------------------------------------------------------------------


    -- Flush the data from JAVA stored procedure (JServ) to PL/SQL side
    -- Without doing this, the PL/SQL message is still empty.
    message.flush(id);

    -- Use either clean_all or clean to clean up the message store when the user
    -- do not plan to do paylaod population on this message anymore
    sys.aq$_jms_bytes_message.clean_all();
    --message.clean(id);

    message_properties.expiration := p_Expiration;
    message_properties.correlation := p_JMSCorrelationID;
    enqueue_options.visibility := dbms_aq.IMMEDIATE;

    -- Enqueue this message into AQ queue using DBMS_AQ package
    dbms_aq.enqueue(queue_name => 'aq_outgoing_ws',
                    enqueue_options => enqueue_options,
                    message_properties => message_properties,
                    payload => message,
                    msgid => msgid);



    return 0;
  EXCEPTION
  WHEN java_exp THEN
    return 1;
  END;

  FUNCTION InsRequestToIncWS(p_QueueID IN NUMBER, p_ReqIDTrEn IN VARCHAR2, p_ReqIDMes IN VARCHAR2, p_JMSCorrelationID IN VARCHAR2, p_XMLTrEn IN CLOB ) RETURN INTEGER IS
    id                 pls_integer;
    agent              sys.aq$_agent := sys.aq$_agent(' ', null, 0);
    message            sys.aq$_jms_bytes_message;
    enqueue_options    dbms_aq.enqueue_options_t;
    message_properties dbms_aq.message_properties_t;
    msgid raw(16);
    objectId number;

    java_exp           exception;
    pragma EXCEPTION_INIT(java_exp, -24197);
  BEGIN

----------------------------------------------------------------------------------------------
  -- 1. вставляем запись в DQUEUE_EXCHANGE_DBT
  insert into DQUEUE_EXCHANGE_DBT (T_queueID ,T_JMSMessageID ,T_JMSCorrelationID,  T_XMLTrEn) values (p_QueueID, p_ReqIDMes, p_JMSCorrelationID, p_XMLTrEn)
  returning t_id into objectId;

  -- 2. вставляем запись в funcobj для вызова обрабочика вставленной записи
      INSERT INTO DFUNCOBJ_DBT (T_OBJECTTYPE, T_OBJECTID, T_FUNCID, T_STATE, T_PARAM)
        VALUES (0, objectId, 5700, 0, 0);




    return 0;
  EXCEPTION
  WHEN java_exp THEN
    return 1;
  END;

  FUNCTION InsRequestToIncSyncWS(p_QueueID IN NUMBER, p_ReqIDTrEn IN VARCHAR2, p_JMSMessageID IN VARCHAR2, p_JMSCorrelationID IN VARCHAR2, p_ParentReqIDTrEn IN VARCHAR2, p_XMLTrEn IN CLOB ) RETURN INTEGER IS
    id                 pls_integer;
    agent              sys.aq$_agent := sys.aq$_agent(' ', null, 0);
    message            sys.aq$_jms_bytes_message;
    enqueue_options    dbms_aq.enqueue_options_t;
    message_properties dbms_aq.message_properties_t;
    msgid raw(16);

    java_exp           exception;
    pragma EXCEPTION_INIT(java_exp, -24197);
  BEGIN

    -- Consturct a empty bytes message object
    message := sys.aq$_jms_bytes_message.construct;
/*
    -- Shows how to set the JMS header
    message.set_replyto(agent);
    message.set_type('tkaqpet1');
    message.set_userid('jmsuser');
    message.set_appid('plsql_enq');
    message.set_groupid('st');
    message.set_groupseq(1);
*/
    -- Shows how to set JMS user properties
    message.set_int_property('QueueID', p_QueueID);
    message.set_string_property('ReqIDTrEn', p_ReqIDTrEn);
    message.set_string_property('JMSMessageID', p_JMSMessageID);
    message.set_string_property('JMSCorrelationID', p_JMSCorrelationID);

    id := message.clear_body(-1);

    -- Write a String to the bytes message payload,
    -- the String is encoded in UTF8 in the message payload
    message.write_utf(id, p_XMLTrEn);

    -- Flush the data from JAVA stored procedure (JServ) to PL/SQL side
    -- Without doing this, the PL/SQL message is still empty.
    message.flush(id);

    -- Use either clean_all or clean to clean up the message store when the user
    -- do not plan to do paylaod population on this message anymore
    sys.aq$_jms_bytes_message.clean_all();
    --message.clean(id);

    message_properties.correlation    := p_ParentReqIDTrEn;

    enqueue_options.visibility := dbms_aq.IMMEDIATE;

    -- Enqueue this message into AQ queue using DBMS_AQ package
    dbms_aq.enqueue(queue_name => 'aq_incoming_sync_ws',
                    enqueue_options => enqueue_options,
                    message_properties => message_properties,
                    payload => message,
                    msgid => msgid);



    return 0;
  EXCEPTION
  WHEN java_exp THEN
    return 1;
  END;



  FUNCTION GetAnswerSyncInAQ(p_Correlation IN VARCHAR2, p_Wait IN NUMBER, p_ReqIDTrEnAns IN VARCHAR2, p_QueueIDIn OUT NUMBER, p_JMSMessageIDAns OUT VARCHAR2, p_JMSCorrelationIDAns OUT VARCHAR2, p_XrLogID OUT NUMBER) RETURN INTEGER IS
    dequeue_options     dbms_aq.dequeue_options_t;
    message_properties dbms_aq.message_properties_t;
    msgid raw(16);
    message            sys.aq$_jms_bytes_message;
    clob_data          clob;
    id                 pls_integer;
    agent              sys.aq$_agent := sys.aq$_agent(' ', null, 0);


    v_XMLTrEnAns           CLOB         ;
    v_RetVal               NUMBER;

    java_exp           exception;
    pragma EXCEPTION_INIT(java_exp, -25228);
  BEGIN

    v_RetVal := 0;
    dequeue_options.correlation    := p_Correlation;
    dequeue_options.wait           := p_Wait;
    dequeue_options.navigation     := DBMS_AQ.FIRST_MESSAGE;

    /*
    DBMS_AQ.DEQUEUE (
       queue_name          IN      VARCHAR2,
       dequeue_options     IN      dequeue_options_t,
       message_properties  OUT     message_properties_t,
       payload             OUT     "<ADT_1>"
       msgid               OUT     RAW);
    */

    dbms_aq.DEQUEUE(queue_name => 'aq_incoming_sync_ws',
           dequeue_options    => dequeue_options,
           message_properties => message_properties,
           payload            => message,
           msgid              => msgid);


    -- Retrieve the header
    agent := message.get_replyto;

    id := message.prepare(-1);

    message.read_utf(id, clob_data);

    v_XMLTrEnAns := clob_data;

    p_QueueIDIn           := message.get_int_property('QueueID');
    p_JMSMessageIDAns     := message.get_string_property('JMSMessageID');
    p_JMSCorrelationIDAns := message.get_string_property('JMSCorrelationID');

    message.clean(id);

    v_RetVal := WriteXrLog(3, 0 /*Исходящая*/, p_ReqIDTrEnAns, RSBSESSIONDATA.CNum, RSBSESSIONDATA.oper, RSBSESSIONDATA.curdate, v_XMLTrEnAns, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, p_XrLogID);

    return v_RetVal;
  EXCEPTION
  WHEN java_exp THEN
    it_error.put_error_in_stack;
    it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR, p_msg => 'Ошибка получения синхронного ответа: '||sqlerrm);
    return 1;
  END;

  FUNCTION TryGetSyncAnswer(p_Correlation IN VARCHAR2, p_ReqIDTrEnAns IN VARCHAR2, p_QueueIDIn OUT NUMBER, p_JMSMessageIDAns OUT VARCHAR2, p_JMSCorrelationIDAns OUT VARCHAR2, p_XrLogID OUT NUMBER) RETURN INTEGER IS
    v_XMLTrEnAns CLOB;
    v_RetVal               NUMBER;
  BEGIN
    v_RetVal := 0;
   
    BEGIN 
      select q.t_message, 1, q.t_JMSMessageID, q.t_JMSCorrelationID into v_XMLTrEnAns, p_QueueIDIn, p_JMSMessageIDAns, p_JMSCorrelationIDAns
        from dqueuelinks_dbt  l,
             dqueue_log_dbt  q,
             dxr_log_dbt xr
       where xr.t_reqid = p_Correlation
         and l.t_reqidtren = xr.t_reqid and l.t_queueid = 2
         and q.t_jmscorrelationid = l.t_jmsmessageid
         and q.t_qname = 'ESB_TO_SOFR' 
         and q.t_direction = 1
         and xr.t_sysdate = q.t_sysdate;
    EXCEPTION
      WHEN no_data_found THEN
        return 1;
    END;
   
    v_RetVal := WriteXrLog(3, 0 /*Исходящая*/, p_ReqIDTrEnAns, RSBSESSIONDATA.CNum, RSBSESSIONDATA.oper, RSBSESSIONDATA.curdate, v_XMLTrEnAns, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, p_XrLogID);
    return v_RetVal;
  EXCEPTION
    WHEN others THEN 
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR, p_msg => 'Ошибка обработки dqueue_log_dbt: '||sqlerrm);
      return 1;
  END;

END  RSI_RSB_WEB_SPHERE;
/