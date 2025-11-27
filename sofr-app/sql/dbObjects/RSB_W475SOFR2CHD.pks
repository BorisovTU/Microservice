create or replace package RSB_W475SOFR2CHD is

 /**************************************************************************************************\
    BIQ-15436. Разработка выгрузки результатов БО в таблицу dkl11sofr2dwh_dbt
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
    23.10.2023  Шестаков Д.В.     BIQ-15436                      
  \**************************************************************************************************/
  
  
  /**
   @brief Проиницилизировать глобальные переменные пакета
   @param[in] p_dt_begin  Дата начала
   @param[in] p_dt_end Дата окончания
   @param[in] p_BROKERREP_DL_SETTLOPER Вид операции
   @param[in] p_FIKIND_CURRENCY Вид актива
   @param[in] p_DL_CALCOPER  Вид документа
   @param[in] p_ourbank Банк
  */
  procedure initial_param(p_dt_begin               date
                         ,p_dt_end                 date
                         ,p_BROKERREP_DL_SETTLOPER number
                         ,p_FIKIND_CURRENCY        number
                         ,p_DL_CALCOPER            number
                         ,p_ourbank                number); 
  
   /**
   @brief Загрузить данные во временную таблицу, Первичная загрузка данных
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                      
  procedure load_bufferTable_1(p_out_err_msg out        varchar2
                              ,p_out_result  out        number); 
  
   /**
   @brief Транформация данных, обогащение: Отбор данных, обновление t_contrnumber, Наименования бумаг, Суммы зачислений и списаний,Суммы списаний
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                            
  procedure load_bufferTable_2(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);  
 
   /**
   @brief  Поиск сумм списания/зачисления по расчетным операциям ВУ по аналогии с отчетом Брокера  
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                               
  procedure load_bufferTable_3(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);
  
   /**
   @brief Оборотов всего,  Обороты за период по сделкам РЕПО, Суммы СВОП,  Входящие остатки
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                              
  procedure load_bufferTable_4(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);  
  
  /**
   @brief Транформация данных, обогащение. Исходящие остатки 
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                                                      
  procedure load_bufferTable_5(p_out_err_msg out        varchar2
                              ,p_out_result  out        number); 
  
   /**
   @brief Транформация данных, обогащение.   Исходящие остатки 
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                              
  procedure load_bufferTable_6(p_out_err_msg out        varchar2
                              ,p_out_result  out        number); 
  
  /**
   @brief Транформация данных, обогащение.  Зачисления бумаг
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                               
  procedure load_bufferTable_7(p_out_err_msg out        varchar2
                              ,p_out_result  out        number); 
  
  /**
   @brief Транформация данных, обогащение.    Ценные бумаги
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_8(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);
                              
  /**
   @brief Транформация данных, обогащение. Вложения бумаг входящий  
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_9(p_out_err_msg out        varchar2
                              ,p_out_result  out        number); 
  
  /**
   @brief Транформация данных, обогащение. Вложения бумаг исходящий
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_10(p_out_err_msg out        varchar2
                              ,p_out_result  out        number); 
  
  /**
   @brief Транформация данных, обогащение. Для срочного рынка обороты за период - t_turnsum  
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_11(p_out_err_msg out        varchar2
                              ,p_out_result  out        number); 
  
  /**
   @brief Транформация данных, обогащение. t_turnsum 2 - Для валютного рынка обороты
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_12(p_out_err_msg out        varchar2
                              ,p_out_result  out        number); 
  
  /**
   @brief Транформация данных, обогащение. Сумма комиссий
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_13(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);   
  
  /**
   @brief Транформация данных, обогащение. Для срочного рынка - комиссия брокера  
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                                                       
  procedure load_bufferTable_14(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);
  
  /**
   @brief Транформация данных, обогащение. Комиссии Репо
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_15(p_out_err_msg out        varchar2
                              ,p_out_result  out        number); 
  
  /**
   @brief Транформация данных, обогащение. Комиссии СВОП
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_16(p_out_err_msg out        varchar2
                              ,p_out_result  out        number); 
  
  /**
   @brief Транформация данных, обогащение. Специальные Репо
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_17(p_out_err_msg out        varchar2
                              ,p_out_result  out        number); 
  
  /**
   @brief Транформация данных, обогащение
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_18(p_out_err_msg out        varchar2
                              ,p_out_result  out        number); 
                              
                                                               
  /**
   @brief Транформация данных, обогащение.ФИО Клиента {Field05} Результаты БО для ДРРК 
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_19(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);   
                              
  /**
   @brief Транформация данных, обогащение.Комиссия уплаченная банком по сделкам клиента не репо 
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_20(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);   
                              
  /**
   @brief Транформация данных, обогащение
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_21(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);   
                              
  /**
   @brief Транформация данных, обогащение. расход Банка по комиссионному вознаграждению биржи по сделкам РЕПО
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                             
  procedure load_bufferTable_22(p_out_err_msg out        varchar2
                              ,p_out_result  out        number);
                                  
  /**
   @brief Вставка данных в целевую таблицу
   @param[out] p_out_err_msg  Текст ошибки
   @param[out] p_out_result Результат выполнения процедуры. 0- успех, 1- ошибка
  */                                                       
  procedure load_bufferTable_23(p_out_err_msg out        varchar2
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
                                                                                                                
end RSB_W475SOFR2CHD;
/