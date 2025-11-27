create or replace package nptx_money_utils as

  procedure send_error_mail (
      p_head varchar2,
      p_text varchar2
    );
                                                            
  function save_transfer_to_buf (
    p_src             dnptxop_req_dbt.src%type,
    p_ext_id          dnptxop_req_dbt.external_id%type,
    p_client_cft_id   dnptxop_req_dbt.client_cft_id%type,
    p_contract        dnptxop_req_dbt.contract%type,
    p_client_code     dnptxop_req_dbt.client_code%type,
    p_is_exchange     dnptxop_req_dbt.is_exchange%type,
    p_is_full_rest    dnptxop_req_dbt.is_full_rest%type,
    p_currency        dnptxop_req_dbt.currency%type,
    p_amount          dnptxop_req_dbt.amount%type,
    p_is_exchange_tgt dnptxop_req_dbt.is_exchange_target%type,
    p_req_date        dnptxop_req_dbt.req_date%type,
    p_req_time        dnptxop_req_dbt.req_time%type,
    p_file_name       dnptxop_req_dbt.file_name%type
  ) return number;

  function save_out_to_buf (
    p_src             dnptxop_req_dbt.src%type,
    p_ext_id          dnptxop_req_dbt.external_id%type,
    p_client_cft_id   dnptxop_req_dbt.client_cft_id%type,
    p_iis             dnptxop_req_dbt.iis%type,
    p_contract        dnptxop_req_dbt.contract%type,
    p_client_code     dnptxop_req_dbt.client_code%type,
    p_is_exchange     dnptxop_req_dbt.is_exchange%type,
    p_is_full_rest    dnptxop_req_dbt.is_full_rest%type,
    p_currency        dnptxop_req_dbt.currency%type,
    p_amount          dnptxop_req_dbt.amount%type,
    p_account         dnptxop_req_dbt.enroll_account%type,
    p_department      dnptxop_req_dbt.department%type,
    p_req_date        dnptxop_req_dbt.req_date%type,
    p_req_time        dnptxop_req_dbt.req_time%type,
    p_file_name       dnptxop_req_dbt.file_name%type
  ) return number;
 
  procedure set_buf_status (
    p_req_id    dnptxop_req_dbt.req_id%type,
    p_status_id dnptxop_req_dbt.status_id%type
  );
  
  procedure set_buf_error (
    p_req_id    dnptxop_req_dbt.req_id%type,
    p_error_id  dnptxop_req_dbt.error_id%type
  );
  
  procedure set_error_if_not_error (
    p_nptxop_id dnptxop_req_dbt.operation_id%type,
    p_error_id  dnptxop_req_dbt.error_id%type
  );

  procedure set_status_wait (
    p_nptxop_id dnptxop_req_dbt.operation_id%type
  );
  
  procedure set_status_done (
    p_nptxop_id dnptxop_req_dbt.operation_id%type
  );
  
  procedure set_status_reject(
    p_nptxop_id dnptxop_req_dbt.operation_id%type
  );

  procedure set_status_deleted (
    p_nptxop_id dnptxop_req_dbt.operation_id%type
  );
  
  function set_lock (
    p_req_id    dnptxop_req_dbt.req_id%type
  ) return number;
  
  procedure release_lock (
    p_req_id    dnptxop_req_dbt.req_id%type
  );
  
  procedure delete_from_auto_executing (
    p_req_id    dnptxop_req_dbt.req_id%type,
    p_op_id     dnptxop_dbt.t_id%type
  );

  function save_operation_by_buf (
    p_req_id dnptxop_req_dbt.req_id%type
  ) return number;

  function save_req_to_buf (
    p_src             dnptxop_req_dbt.src%type,
    p_ext_id          dnptxop_req_dbt.external_id%type,
    p_client_cft_id   dnptxop_req_dbt.client_cft_id%type,
    p_iis             dnptxop_req_dbt.iis%type,
    p_contract        dnptxop_req_dbt.contract%type,
    p_client_code     dnptxop_req_dbt.client_code%type,
    p_is_exchange     dnptxop_req_dbt.is_exchange%type,
    p_is_full_rest    dnptxop_req_dbt.is_full_rest%type,
    p_currency        dnptxop_req_dbt.currency%type,
    p_amount          dnptxop_req_dbt.amount%type,
    p_is_exchange_tgt dnptxop_req_dbt.is_exchange_target%type,
    p_account         dnptxop_req_dbt.enroll_account%type,
    p_department      dnptxop_req_dbt.department%type,
    p_req_date        dnptxop_req_dbt.req_date%type,
    p_req_time        dnptxop_req_dbt.req_time%type,
    p_file_name       dnptxop_req_dbt.file_name%type
  ) return number;

  function run_move_from_buf
    return number;

   procedure send_error_notification(p_proc     varchar2
                                   ,p_errcode   integer
                                   ,p_errtxt    varchar2
                                   ,p_nosupport integer);
   
   -- формирования данных для сообщения о статусе 
  procedure send_order_status(p_operid integer -- Идентификатор T_ID из DNPTXOP_DBT
                             ,p_status integer -- Статус поручения 2 - Исполнено 4 - Отклонено
                             );
  
  procedure send_order_status_errtxt(p_operid        integer -- Идентификатор T_ID из DNPTXOP_DBT
                                    ,p_status        integer -- Статус поручения 2 - Исполнено 4 - Отклонено
                                    ,p_automatic     integer  default 0 -- 1 - безинтерфейсный запуск 
                                    ,o_ErrorCode out integer
                                    ,o_ErrorDesc out varchar2);

   -- Проверка получения ответа на сообщение об изменении статуса
  procedure service_chk_send_order_status_resp(p_worklogid integer
                                              ,p_messmeta  xmltype );


 end nptx_money_utils;
/
