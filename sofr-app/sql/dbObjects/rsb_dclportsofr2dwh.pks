create or replace package rsb_dclportsofr2dwh is

 /**************************************************************************************************\
    BIQ-15004. Разработка выгрузки результатов остатков ДС и ЦБ по клиентам в таблицу dclportsofr2dwh_dbt
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
    23.04.2023  Шестаков Д.В.     BIQ-15004                     
  \**************************************************************************************************/
  
  
  /**
   @brief Проиницилизировать глобальные переменные пакета
   @param[in] p_dt_begin  Дата начала
  */ 
 procedure initial_param(p_dt_begin  date default null);
 
  /**
   @brief Загрузить данные во временную таблицу, Первичная загрузка данных. ЦБ
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */   
 procedure load_bufferTable_1(p_out_err_msg out        varchar2
                             ,p_out_result  out        number);
 /**
   @brief Загрузить данные во временную таблицу, Первичная загрузка данных. ДС
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */   
 procedure load_bufferTable_2(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);
                              
                              
  /**
   @brief Стоимость зачисленных денежных средств за отчетную дату в рублевом эквиваленте.
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                                                             
 procedure load_bufferTable_3(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);
  
  /**
   @brief Стоимость списанных/выведенных денежных средств за отчетную дату в рублевом эквиваленте. 
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
 procedure load_bufferTable_4(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);
 
  /**
   @brief Стоимость зачисленных ценных бумаг за отчетную дату в рублевом эквиваленте. 
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                                                            
 procedure load_bufferTable_5(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);
 
  /**
   @brief Стоимость списанных/выведенных ценных бумаг за отчетную дату в рублевом эквиваленте.  
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */ 
 procedure load_bufferTable_6(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);
 
  /**
   @brief Сумма погашения номинала облигаций 
          Амортизация в руб. экв.
          Полученный купон в руб. экв.  
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                                                           
 procedure load_bufferTable_7(p_out_err_msg out        varchar2
                              ,p_out_result  out        number); 
 
  /**
   @brief Вставка данных в целевую таблицу
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
 procedure load_bufferTable_8(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);
 
   /**
   @brief Получить текст ошибки, код + описание
   @param[in] p_sql_code  код ошибки. SQLCODE
   @param[in] p_error_message описание ошибки. SQLERRM
   @return Полноценное описание ошибки
  */                                                                                                                                             
  function get_text_error(p_sql_code      number
                         ,p_error_message varchar2) return varchar2;
  
  /**
   @brief Отправка уведомлений в телеграм и на почту СОФР мониторинг через it_event
   @param[in] p_status  Статус сообщения логов. 0- упех, 1 - ошибка
   @param[in] p_message  Текст ошибки. Описание 
  */                                                                                                                                                         
 procedure log_register_event(p_status number
                             ,p_message varchar2 default null);
                             
                             

end rsb_dclportsofr2dwh;
/