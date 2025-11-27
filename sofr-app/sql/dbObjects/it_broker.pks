CREATE OR REPLACE PACKAGE IT_BROKER AS

/******************************************************************************
 NAME: CLIENT_INFO
 PURPOSE: Формирование json сообщения для передачи в Свои Инвестии через kafka

 REVISIONS:
 Ver Date Author Description
 --------- ---------- --------------- ------------------------------------
 1.0 17.05.2024 Shishkin-ev 1. Created this package.
******************************************************************************/

/**
 * Функция получения кода на заданную дату (по ddlobjcode_dbt)
 * @qtest NO
 * @param p_ObjectType Тип объекта
 * @param p_CodeKind Вид кода
 * @param p_ObjectID Идентификатор объекта
 * @param p_Date Дата
 * @return Код на дату
 */
FUNCTION GetDlObjCodeOnDate( p_ObjectType IN NUMBER,
                                p_CodeKind   IN NUMBER,
                                p_ObjectID   IN NUMBER,
                                p_Date       IN DATE DEFAULT NULL
                              ) RETURN VARCHAR2;

/*
Сбор данных по клиенту 
@since RSHB 106
@qtest NO
@param p_Clientid  идентификатор клиента(dparty_dbt.t_partyid)
@param o_ErrorCode возвращаемый код ошибки
@param o_ErrorDesc возвращаемый текст ошибки
*/

PROCEDURE client_info(XML_CDC in varchar2 ,o_ErrorCode    out number   ,o_ErrorDesc    out varchar2);
 

/*
Неторговое поручение: Пополнение, списание, перевод
@since RSHB 113
@qtest NO
@param p_nptxopid ID операции
*/
PROCEDURE non_trade( p_id                in dnptxop_dbt.t_id%type,
                     p_dockind           in dnptxop_dbt.t_dockind%type,
                      p_client            in dnptxop_dbt.t_client%type, 
                      p_kind_operation    in dnptxop_dbt.t_kind_operation%type, 
                      p_subkind_operation in dnptxop_dbt.t_subkind_operation%type,
                      p_contract          in dnptxop_dbt.t_contract%type,
                      p_currency          in dnptxop_dbt.t_currency%type,
                      p_outsum            in dnptxop_dbt.t_outsum%type,
                      p_tax               in dnptxop_dbt.t_tax%type,
                      p_status            in dnptxop_dbt.t_status%type,
                      p_account           in dnptxop_dbt.t_account%type);

/*
Значение настройки "Неторговые операции" Вкл/Выкл
@since RSHB 113
@qtest NO
@return chr(0)/chr(88)
*/
FUNCTION IsRegval_NonTrade_On return char;

/*
Значение настройки "Неторговые операции, только ФЛ" Вкл/Выкл
@since RSHB 113
@qtest NO
@return chr(0)/chr(88)
*/
FUNCTION IsRegval_NonTrade_OnlyFL_On return char;

 /*
Значение настройки "ПУШ-УВЕДОМЛЕНИЯ ПО ВЫПЛАТАМ ЦБ" Вкл/Выкл
@since RSHB 117
@qtest NO
@return chr(0)/chr(88)
*/
 FUNCTION IsRegval_CorporateAction_On return char;

/*
Наименование операции для выгрузки по ее коду
@since RSHB 113
@qtest NO
@return Наименование в символьном виде
*/
FUNCTION GetOperationName(p_subkind_operation in dnptxop_dbt.t_subkind_operation%type) return varchar2;

END IT_BROKER;
/