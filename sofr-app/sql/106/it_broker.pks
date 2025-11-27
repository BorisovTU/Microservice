CREATE OR REPLACE PACKAGE IT_BROKER AS

/******************************************************************************
 NAME: CLIENT_INFO
 PURPOSE: Формирование json сообщения для передачи в Свои Инвестии через kafka

 REVISIONS:
 Ver Date Author Description
 --------- ---------- --------------- ------------------------------------
 1.0 17.05.2024 Shishkin-ev 1. Created this package.
******************************************************************************/

/*
Сбор данных по клиенту 
@since RSHB 106
@qtest NO
@param p_Clientid  идентификатор клиента(dparty_dbt.t_partyid)
@param o_ErrorCode возвращаемый код ошибки
@param o_ErrorDesc возвращаемый текст ошибки
*/

PROCEDURE client_info(XML_CDC in varchar2 ,o_ErrorCode    out number   ,o_ErrorDesc    out varchar2);
                           
END IT_BROKER;
/