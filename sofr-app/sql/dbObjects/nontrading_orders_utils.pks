create or replace package nontrading_orders_utils as

  procedure send_error_mail (
      p_head varchar2,
      p_text varchar2
    );
                                                            
  function save_transfer_to_buf (
    p_src                nontrading_orders_buffer.src%type,
    p_ext_id             nontrading_orders_buffer.external_id%type,
    p_client_cft_id      nontrading_orders_buffer.client_cft_id%type,
    p_contract           nontrading_orders_buffer.contract%type,
    p_marketplace        nontrading_orders_buffer.marketplace_withdrawal%type,
    p_is_full_rest       nontrading_orders_buffer.is_full_rest%type,
    p_currency           nontrading_orders_buffer.currency%type,
    p_amount             nontrading_orders_buffer.amount%type,
    p_marketplace_enroll nontrading_orders_buffer.marketplace_enroll%type,
    p_req_date           nontrading_orders_buffer.req_date%type,
    p_req_time           nontrading_orders_buffer.req_time%type,
    p_file_name          nontrading_orders_buffer.file_name%type
  ) return number;

  function save_out_to_buf (
    p_src             nontrading_orders_buffer.src%type,
    p_ext_id          nontrading_orders_buffer.external_id%type,
    p_client_cft_id   nontrading_orders_buffer.client_cft_id%type,
    p_iis             nontrading_orders_buffer.iis%type,
    p_contract        nontrading_orders_buffer.contract%type,
    p_marketplace     nontrading_orders_buffer.marketplace_withdrawal%type,
    p_is_full_rest    nontrading_orders_buffer.is_full_rest%type,
    p_currency        nontrading_orders_buffer.currency%type,
    p_amount          nontrading_orders_buffer.amount%type,
    p_account         nontrading_orders_buffer.enroll_account%type,
    p_department      nontrading_orders_buffer.department%type,
    p_req_date        nontrading_orders_buffer.req_date%type,
    p_req_time        nontrading_orders_buffer.req_time%type,
    p_file_name       nontrading_orders_buffer.file_name%type
  ) return number;
 
  procedure set_buf_status (
    p_req_id    nontrading_orders_buffer.req_id%type,
    p_status_id nontrading_orders_buffer.status_id%type
  );
  
  procedure set_buf_error (
    p_req_id    nontrading_orders_buffer.req_id%type,
    p_error_id  nontrading_orders_buffer.error_id%type
  );
  
  procedure set_error_if_not_error (
    p_nptxop_id nontrading_orders_buffer.operation_id%type,
    p_error_id  nontrading_orders_buffer.error_id%type
  );

  procedure set_status_wait (
    p_nptxop_id nontrading_orders_buffer.operation_id%type
  );
  
  procedure set_status_done (
    p_nptxop_id nontrading_orders_buffer.operation_id%type
  );
  
  procedure set_status_reject(
    p_nptxop_id nontrading_orders_buffer.operation_id%type
  );

  procedure set_status_deleted (
    p_nptxop_id nontrading_orders_buffer.operation_id%type
  );
  
  function set_lock (
    p_req_id    nontrading_orders_buffer.req_id%type
  ) return number;
  
  procedure release_lock (
    p_req_id    nontrading_orders_buffer.req_id%type
  );
  
  procedure delete_from_auto_executing (
    p_req_id    nontrading_orders_buffer.req_id%type,
    p_op_id     dnptxop_dbt.t_id%type
  );

  function save_operation_by_buf (
    p_req_id nontrading_orders_buffer.req_id%type
  ) return number;

  function process_req (
    p_src                nontrading_orders_buffer.src%type,
    p_ext_id             nontrading_orders_buffer.external_id%type,
    p_client_cft_id      nontrading_orders_buffer.client_cft_id%type,
    p_iis                nontrading_orders_buffer.iis%type,
    p_contract           nontrading_orders_buffer.contract%type,
    p_marketplace        nontrading_orders_buffer.marketplace_withdrawal%type,
    p_is_full_rest       nontrading_orders_buffer.is_full_rest%type,
    p_currency           nontrading_orders_buffer.currency%type,
    p_amount             nontrading_orders_buffer.amount%type,
    p_marketplace_enroll nontrading_orders_buffer.marketplace_enroll%type,
    p_account            nontrading_orders_buffer.enroll_account%type,
    p_department         nontrading_orders_buffer.department%type,
    p_req_date           nontrading_orders_buffer.req_date%type,
    p_req_time           nontrading_orders_buffer.req_time%type,
    p_file_name          nontrading_orders_buffer.file_name%type
  ) return number;

  procedure create_all_operations_from_buf;

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

  procedure create_spground (
    p_nptxop_id  dnptxop_dbt.t_id%type,
    p_groundkind number,
    p_code       dnptxop_dbt.t_code%type,
    p_extcode    dnptxop_dbt.t_code%type,
    p_partyid    dnptxop_dbt.t_client%type,
    p_date       nontrading_orders_buffer.req_date%type,
    p_time       nontrading_orders_buffer.req_time%type,
    p_signdate   nontrading_orders_buffer.req_date%type,
    p_src        nontrading_orders_buffer.src%type
  );

end nontrading_orders_utils;
/
