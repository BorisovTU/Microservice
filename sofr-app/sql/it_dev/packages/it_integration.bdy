create or replace package body it_integration is

  /***************************************************************************************************\
   Интеграционные решения ИНТЕХ
   **************************************************************************************************
    Изменения:
   ---------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание 
   ----------  ---------------  ---------------------------   ----------------------------------------
   27.01.2025  Зыков М.В.       BOSS-7573                     Доработки QMessage в части взаимодействия с адаптером для S3
   05.09.2024  Зыков М.В.       BOSS-5212                     BOSS-1574.9 Доработка СОФР для передачи в sofr_qmngr_mq_adapter параметра кластера платформенной Kafka
   15.05.2024  Топорков Д.В.    BIQ-16474                     CCBO-9309. Добавление процедуры Condor_GetLastSOFRSequenceDeal
   25.03.2024  Зыков М.В.       BOSS-2688                     BOSS-575 СОФР. Доработка формирования отчетной формы "Справка 5798-У" (справка для гос. служащего). Доработка Q-Manager 
   23.10.2023  Зыков М.В.       BOSS-1230                     BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
   17.10.2023  Зыков М.В.       BOSS-358                      BIQ-13699.2. СОФР. Этап 2 - добавление файла ограничений по срочному рынку в обработку IPS
   05.09.2022  Зыков М.В.       BIQ-11358                     PRJ-2146 BIQ-11358 Добавление параметра LimitCount
   11.05.2022  Мелихова О.С.    BIQ-11358                     Создание
  \**************************************************************************************************/
  --------------------------------- ИНТЕГРАЦИЯ QUIK  -------------------------------------
  function get_file_limit(p_string in varchar2 -- Если дата   в формате 'dd.mm.yyyy'  "QUIK_LIMITS"
                          -- '<XML p_date="22.09.2023" p_file_code="QUIK_LIMITS_FORTS"/>
                          ) return t_tab_limit
    pipelined is
    v_rec_limit t_rec_limit;
    v_date      date;
    v_file_code varchar2(100);
    v_error     clob;
    v_xml       xmltype;
  begin
    it_error.clear_error_stack;
    it_log.log('p_string = ' || p_string);
    begin
      v_date      := to_date(p_string, 'dd.mm.yyyy');
      v_file_code := it_file.C_FILE_CODE_QUIK;
    exception
      when others then
        begin
          v_xml := xmltype(p_string);
          select to_date(EXTRACTVALUE(v_xml, '/XML/@p_date'), 'dd.mm.yyyy')
                ,EXTRACTVALUE(v_xml, '/XML/@p_file_code')
            into v_date
                ,v_file_code
            from dual;
        exception
          when others then
            it_error.put_error_in_stack;
            it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
            v_error               := sqlerrm;
            v_rec_limit.id        := 0;
            v_rec_limit.body_clob := 'ERROR: Параметр ' || p_string || ' не соответствует формату ' || chr(13) || chr(10) || v_error;
            it_error.clear_error_stack;
        end;
    end;
    if v_rec_limit.id is null
       and v_file_code not in (it_file.C_FILE_CODE_QUIK, it_file.C_FILE_CODE_LIMIT_FORTS)
    then
      v_rec_limit.id        := 0;
      v_rec_limit.body_clob := 'ERROR: Недопустимое значение  p_file_code=' || v_file_code;
    end if;
    if v_rec_limit.id is null
    then
      it_log.log('date = ' || to_char(v_date, 'dd.mm.yyyy') || ' file_code= ' || v_file_code);
      it_lim_exp.get_last_file_lim_quik(p_date => v_date
                                       ,p_file_code => v_file_code
                                       ,p_file_clob => v_rec_limit.body_clob
                                       ,p_id_file => v_rec_limit.id
                                       ,p_note => v_rec_limit.xml_param);
    end if;
    if v_rec_limit.id is null
    then
      v_rec_limit.id        := 0;
      v_rec_limit.body_clob := 'ERROR: За дату ' || to_char(v_date, 'dd.mm.yyyy') || ' нет сохраненных файлов p_file_code=' || v_file_code;
    end if;
    pipe row(v_rec_limit);
    return;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      v_error               := 'ERROR: ' || to_clob(sqlerrm);
      v_rec_limit.id        := 0;
      v_rec_limit.body_clob := v_error;
      it_error.clear_error_stack;
      pipe row(v_rec_limit);
      return;
  end;

  --Сохранение файла лимита за указанную дату
  function ins_file_save(p_xml in clob) return t_tab_limit
    pipelined is
    v_rec_limit t_rec_limit;
    v_id_save   number;
    v_error     varchar2(4000);
  begin
    it_error.clear_error_stack;
    it_log.log('p_xml = ' || p_xml);
    v_id_save      := it_lim_exp.ins_file_save(p_xml => p_xml);
    v_rec_limit.id := v_id_save;
    if v_id_save != 0
    then
      v_rec_limit.body_clob := 'Файл ' || v_id_save || ' успешно сохранен';
    else
      v_rec_limit.body_clob := 'ERROR:  Ошибка сохранения файла';
    end if;
    pipe row(v_rec_limit);
    return;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      v_error               := 'ERROR: ' || to_clob(sqlerrm);
      v_rec_limit.id        := 0;
      v_rec_limit.body_clob := v_error;
      it_error.clear_error_stack;
      pipe row(v_rec_limit);
      return;
  end;

  --------------------------------- ИНТЕГРАЦИЯ КАФКА -------------------------------------
  --Получение списка сообщений для вычитки из QManager
  function qmanager_select_msg return t_tab_qmsg_kafka
    pipelined as
  begin
    for cur in (select q.QUEUENAME
                      ,q.QMSGID
                      ,q.CORRELATION
                  from itv_q_out q
                 order by q.ENQDT
                         ,q.LOCAL_ORDER_NO)
    loop
      pipe row(cur);
    end loop;
  end;

  -- Выгрузка сообщения из QManager УНИВЕРСАЛЬНАЯ 
  procedure qmanager_read_msg(p_wait_msg      in number --  Ожидание сообщения (сек)
                             ,p_QUEUENAME     in varchar2
                             ,p_QMSGID        in raw default null -- GUID Сообщениz в очереди 
                             ,o_kafka_cluster out varchar2 -- cluster
                             ,o_kafka_topic   out varchar2 -- Topc
                             ,o_msgID         out varchar2 -- GUID Сообщение
                             ,ocl_header      out clob -- Header 
                             ,ocl_message     out clob -- Body
                             ,o_ErrorCode     out number -- = 0 ОК
                             ,o_ErrorDesc     out varchar2) as
  begin
    it_kafka.qmanager_read_msg(p_wait_msg => p_wait_msg
                              ,p_QUEUENAME => p_QUEUENAME
                              ,p_qmsgid => p_qmsgid
                              ,o_msgID => o_msgID
                              ,o_kafka_cluster => o_kafka_cluster
                              ,o_kafka_topic => o_kafka_topic
                              ,ocl_header => ocl_header
                              ,ocl_message => ocl_message
                              ,o_ErrorCode => o_ErrorCode
                              ,o_ErrorDesc => o_ErrorDesc);
  end;

  -- Зарегистрировать ошибку сохранения сообщения  в транспортной системе
  procedure qmanager_read_msg_error(p_kafka_topic in varchar2
                                   ,p_msgID       in varchar2 -- GUID Сообщения
                                   ,p_ErrorCode   in number -- Код ошибки p_ErrorDesc 
                                   ,p_ErrorDesc   in varchar2) as
  begin
    it_kafka.qmanager_read_msg_error(p_kafka_topic => p_kafka_topic, p_msgID => p_msgID, p_ErrorCode => p_ErrorCode, p_ErrorDesc => p_ErrorDesc);
  end;

  -- Загрузка сообщения  в QManager c Header
  procedure qmanager_load_msg(p_kafka_topic varchar2
                             ,p_GUID        varchar2 -- GUID сообшения в КАФКА
                             ,p_ESBDT       timestamp -- Врема записи сообщения в КАФКА
                             ,pcl_header    clob -- Header 
                             ,pcl_message   clob -- Body
                             ,o_ErrorCode   out number -- != 0 - Ошибка o_ErrorDesc
                             ,o_ErrorDesc   out varchar2) as
  begin
    it_kafka.qmanager_load_msg(p_kafka_topic => p_kafka_topic
                              ,p_GUID => p_GUID
                              ,p_ESBDT => p_ESBDT
                              ,pcl_header => pcl_header
                              ,pcl_message => pcl_message
                              ,o_ErrorCode => o_ErrorCode
                              ,o_ErrorDesc => o_ErrorDesc);
  end;

  -- Выгрузка сообщения из QManager c Header
  procedure qmanager_read_msg(p_wait_msg    in number --  Ожидание сообщения (сек)
                             ,o_kafka_topic out varchar2 -- Topc
                             ,o_msgID       out varchar2 -- GUID Сообщения
                             ,ocl_header    out clob -- Header 
                             ,ocl_message   out clob -- Body
                             ,o_ErrorCode   out number -- != 0 o_ErrorDesc или нет сообщений
                             ,o_ErrorDesc   out varchar2) as
    v_kafka_cluster itt_kafka_topic.topic_cluster%type;
  begin
    it_kafka.qmanager_read_msg(p_wait_msg => p_wait_msg
                              ,o_msgID => o_msgID
                              ,o_kafka_cluster => v_kafka_cluster
                              ,o_kafka_topic => o_kafka_topic
                              ,ocl_header => ocl_header
                              ,ocl_message => ocl_message
                              ,o_ErrorCode => o_ErrorCode
                              ,o_ErrorDesc => o_ErrorDesc);
  end;

  -- Выгрузка сообщения из QManager c Header и с Cluster
  procedure qmanager_read_msg(p_wait_msg      in number --  Ожидание сообщения (сек)
                             ,o_kafka_cluster out varchar2 -- cluster
                             ,o_kafka_topic   out varchar2 -- Topc
                             ,o_msgID         out varchar2 -- GUID Сообщения
                             ,ocl_header      out clob -- Header 
                             ,ocl_message     out clob -- Body
                             ,o_ErrorCode     out number -- != 0 o_ErrorDesc или нет сообщений
                             ,o_ErrorDesc     out varchar2) as
  begin
    it_kafka.qmanager_read_msg(p_wait_msg => p_wait_msg
                              ,o_msgID => o_msgID
                              ,o_kafka_cluster => o_kafka_cluster
                              ,o_kafka_topic => o_kafka_topic
                              ,ocl_header => ocl_header
                              ,ocl_message => ocl_message
                              ,o_ErrorCode => o_ErrorCode
                              ,o_ErrorDesc => o_ErrorDesc);
  end;

  -- Выгрузка больщих сообщений из QManager через IPS S3
  procedure qmanager_read_msg_S3(p_wait_msg        in number --  Ожидание сообщения (сек)
                                ,p_QUEUENAME       varchar2
                                ,p_QMSGID          in raw default null -- GUID Сообщениz в очереди 
                                ,o_kafka_cluster   out varchar2 -- cluster
                                ,o_kafka_topic     out varchar2
                                ,o_S3xdatafilename out varchar2
                                ,o_S3_point        out varchar2 -- точка интеграции S3
                                ,o_msgID           out varchar2 -- GUID Сообщения
                                ,ocl_header        out clob -- Header 
                                ,ocl_message       out clob -- Body
                                ,o_ErrorCode       out number -- = 0 ОК
                                ,o_ErrorDesc       out varchar2) as
  begin
    it_kafka.qmanager_read_msg_S3(p_wait_msg => p_wait_msg
                                 ,p_QUEUENAME => p_QUEUENAME
                                 ,p_QMSGID => p_QMSGID
                                 ,o_kafka_cluster => o_kafka_cluster
                                 ,o_kafka_topic => o_kafka_topic
                                 ,o_S3xdatafilename => o_S3xdatafilename
                                 ,o_S3_point => o_S3_point
                                 ,o_msgID => o_msgID
                                 ,ocl_header => ocl_header
                                 ,ocl_message => ocl_message
                                 ,o_ErrorCode => o_ErrorCode
                                 ,o_ErrorDesc => o_ErrorDesc);
  end;

  -- Выгрузка больщих сообщений из QManager через IPS S3 
  procedure qmanager_read_msg_S3(p_wait_msg        in number --  Ожидание сообщения (сек)
                                ,o_kafka_cluster   out varchar2 -- cluster
                                ,o_kafka_topic     out varchar2
                                ,o_S3xdatafilename out varchar2
                                ,o_S3_point        out varchar2 -- точка интеграции S3
                                ,o_msgID           out varchar2 -- GUID Сообщения
                                ,ocl_header        out clob -- Header 
                                ,ocl_message       out clob -- Body
                                ,o_ErrorCode       out number -- = 0 ОК
                                ,o_ErrorDesc       out varchar2) as
  begin
    it_kafka.qmanager_read_msg_S3(p_wait_msg => p_wait_msg
                                 ,o_kafka_cluster => o_kafka_cluster
                                 ,o_kafka_topic => o_kafka_topic
                                 ,o_S3xdatafilename => o_S3xdatafilename
                                 ,o_S3_point => o_S3_point
                                 ,o_msgID => o_msgID
                                 ,ocl_header => ocl_header
                                 ,ocl_message => ocl_message
                                 ,o_ErrorCode => o_ErrorCode
                                 ,o_ErrorDesc => o_ErrorDesc);
  end;

  -- процедура дублирования сообщения в очередь для отправки header в KAFKA после выгрузки боди в S3 ( костыль так как адаптер S3 не хочет отправлять хедеры в KAFKA )
  procedure S3_load_header_msg_KAFKA(p_msgid itt_q_message_log.msgid%type -- GUID сообщения
                                    ) as
  begin
    it_kafka.S3_load_header_msg_KAFKA(p_msgid => p_msgid);
  end;

  /**
  Процедура получения номера следующего ID в таблице сделок из sequence, определяемого по i_SeqType:
   - "deal" для ddl_tick_dbt_seq
   - "ndeal" для ddvndeal_dbt_seq
  */
  PROCEDURE Condor_GetLastSOFRSequenceDeal(
                            p_ReqType      IN VARCHAR2,  -- Тип запроса (N - создание, U - изменение, D - удаление)
                            p_SecType      IN VARCHAR2,  -- Тип sequence
                            p_DealCode     IN VARCHAR2,  -- Код сделки. Поле входящей XML IFX->ProcessDeals_req->DealList->DealParm->IdentityDeal
                            p_RequestID    IN VARCHAR2,  -- ID запроса. Поле входящей XML IFX->ProcessDeals_req->?
                            o_SeqID        OUT NUMBER,   -- Значение последовательности для выбранного sequence
                            o_ErrorMessage OUT VARCHAR2) -- Текст возможной ошибки
  IS
  BEGIN
    o_ErrorMessage := '';
    
    IF p_ReqType = 'N' THEN
      IF LOWER(p_SecType) = 'tick' THEN
        SELECT ddl_tick_dbt_seq.NEXTVAL
          INTO o_SeqID
          FROM dual;
      ELSIF LOWER(p_SecType) = 'ndeal' THEN
        SELECT ddvndeal_dbt_seq.NEXTVAL
          INTO o_SeqID
          FROM dual;
      ELSE
        o_SeqID := null;
      END IF;
    ELSIF p_ReqType = 'U' OR p_ReqType = 'D' THEN
      o_SeqID := null;
    ELSE
      o_SeqID := -1;
      o_ErrorMessage := 'Неверный тип запроса ReqType = "' || p_ReqType || '" (ожидаются - "N", "U", "D")';
    END IF;
    
    IF NVL(o_SeqID, 1) > 0 THEN
      INSERT INTO Kondor_SOFR_Buffer_dbt
             (t_requestID,
              t_reqType,
              t_dealCode, 
              t_seqID,
              t_seqType, 
              t_ErrorStatus, 
              t_ErrorMessage, 
              t_DateCreate, 
              t_DateComplete)
      VALUES (p_RequestID,
              p_ReqType,
              p_DealCode,
              o_SeqID,
              CASE WHEN p_ReqType = 'N' THEN p_SecType ELSE null END,
              0,
              chr(1),
              SYSDATE,
              to_date('01.01.0001', 'DD.MM.YYYY'));
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      o_SeqID := -1;
      o_ErrorMessage := 'Ошибка '||SQLERRM;
  END Condor_GetLastSOFRSequenceDeal;

end;
/
