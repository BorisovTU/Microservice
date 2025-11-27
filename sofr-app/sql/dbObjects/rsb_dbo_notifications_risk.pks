create or replace package rsb_dbo_notifications_risk is

 /**************************************************************************************************\
    "Ежегодное уведомление клиентов о рисках использования Банком в своих интересах ценных бумаг Клиента"
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание
    ----------  ---------------  ------------------------------   -------------------------------------
    20.01.2025  Шестаков Д.В.    BOSS-3774
  \**************************************************************************************************/



/*
1. Примечания нет, дата открытия больше 11 месяцев,можно отправлять
2. Примечания нет, дата открытия меньше 11 месяцев, не отправлять
3. Примечание есть, прошло 11 месяцев,можно отправлять
4. Примечание есть, но не прошло время для отправки, не отправлять
*/


  /**
   @brief Собрать данные во временную таблицу DSOFRSENDMSGEMAIL_TMP для отправки уведомлений
   @param[in] p_dlContrId  Id договора, 0 - грузим все договора
  */
 PROCEDURE collectDboNotifications(p_dlContrId  NUMBER DEFAULT 0);

  /**
   @brief Функция валидации Email адреса
   @param[in] p_email Email адрес
   @return Результат выполнения валидации. 1 - успех,  0 - ошибка
  */
 FUNCTION validateEmail(p_email VARCHAR2) RETURN NUMBER DETERMINISTIC;

 /**
   @brief Собрать данные во временную таблицу DSOFRSENDMSGEMAIL_TMP для печати отчета
   @param[in] p_dateBegin Дата начала отчетного периода
   @param[in] p_dateEnd Дата окончания отчетного периода
   @param[in] p_isAllClient Флаг формирования отчета по всем клиентам. Если [X] - все клиенты, иначе клиенты из таблицы dset_cln_u_tmp
  */
 PROCEDURE dboNotifications_Report(p_dateBegin   IN DATE,
                                   p_dateEnd   IN DATE,
                                   p_isAllClient IN CHAR);
 /**
   @brief Инсерт данных в таблицу DDLCONTRMSG_DBT
   @param[in] p_Dlcontrid  ID договора
   @param[in] p_recipientEmail  Получатель письма
   @param[in] p_senderEmail  Отправитель письма
   @param[in] p_textEmail  Текст, который содержится в письме
   @param[in] p_sendMesState  Статус отправки, успешно обработалось почтовым шлюзом?
   @param[out] p_out_msgId  Id созданного сообщения ДБО в тестовом формате, дополненное нулями слева до 34 символов
   @param[out] p_out_errMsg  Возвращает текст ошибки
   @param[out] p_out_errRes  Возвращает статус ошибки. 1 - есть ошибка при выполнении, 0 - ошибки нет
  */
  PROCEDURE insertDlContrMsg(p_Dlcontrid       Ddlcontr_Dbt.t_Dlcontrid%TYPE,
                             p_recipientEmail  VARCHAR2,
                             p_senderEmail     VARCHAR2,
                             p_textEmail       CLOB,
                             p_sendMesState    NUMBER,
                             p_out_msgId       OUT VARCHAR2,
                             p_out_errMsg      OUT VARCHAR2,
                             p_out_errRes      OUT NUMBER);

  /**
   @brief Инсерт данных в таблицу DNOTETEXT_DBT
   @param[in] p_dlcontrID  ID договора
   @param[in] p_recipientEmail  Получатель письма
   @param[in] p_senderEmail  Отправитель письма
   @param[out] p_out_errMsg  Возвращает текст ошибки
   @param[out] p_out_errRes  Возвращает статус ошибки. 1 - есть ошибка при выполнении, 0 - ошибки нет
  */
  PROCEDURE insertNoteText(p_dlcontrID       DDLCONTR_DBT.T_DLCONTRID%TYPE,
                           p_recipientEmail  VARCHAR2,
                           p_senderEmail     VARCHAR2,
                           p_out_errMsg      OUT VARCHAR2,
                           p_out_errRes      OUT NUMBER);
 /**
   @brief Update данных в таблице DDLCONTRMSG_DBT, закрытие пред. примечания, перед открытием нового
   @param[in] p_noteTextID  ID примечания
   @param[in] p_Dlcontrid  ID договора
   @param[in] p_recipientEmail  Получатель письма
   @param[in] p_senderEmail  Отправитель письма
   @param[out] p_out_errMsg  Возвращает текст ошибки
   @param[out] p_out_errRes  Возвращает статус ошибки. 1 - есть ошибка при выполнении, 0 - ошибки нет
  */
  PROCEDURE updateNoteText(p_noteTextID      DNOTETEXT_DBT.T_ID%TYPE,
                           p_dlcontrID       DDLCONTR_DBT.T_DLCONTRID%TYPE,
                           p_recipientEmail  VARCHAR2,
                           p_senderEmail     VARCHAR2,
                           p_out_errMsg      OUT VARCHAR2,
                           p_out_errRes      OUT NUMBER);

 /**
   @brief Процедура логгирования ошибок в ходе отправки уведомлений
   @param[in] p_head  Тема письма
   @param[in] p_recipientemail  Получатель письма
   @param[in] p_senderemail  Отправитель письма
   @param[in] p_errorText  Текст ошибки
   @param[in] p_dlcontrid  ID договора
  */
  PROCEDURE insertNotifyLog(p_head           VARCHAR2,
                            p_recipientemail VARCHAR2,
                            p_senderemail    VARCHAR2,
                            p_errorText      VARCHAR2,
                            p_dlcontrid      NUMBER);

end rsb_dbo_notifications_risk;
/