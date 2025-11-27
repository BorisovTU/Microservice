CREATE OR REPLACE PACKAGE USR_PKG_IMPORT_SOFR
AS

 --добавим LOB запись для выгрузки в файл 0 - успешно, 1 - ошибка
 FUNCTION AddLOBToTMP( p_Mode IN INTEGER, p_IsDel IN INTEGER, p_FileName IN VARCHAR2 ) RETURN INTEGER;

 --добавим записи в буферную таблицу udl_lmtcashstock_exch_dbt 0 - успешно, 1 - ошибка
 FUNCTION AddInudl_lmtcashstock_exch RETURN INTEGER;

 --добавим записи в буферную таблицу udl_lmtsecuritest_exch_dbt 0 - успешно, 1 - ошибка
 FUNCTION AddInudl_lmtsecuritest_exch RETURN INTEGER;

 --добавим записи в буферную таблицу udl_lmtfuturmark_exch_dbt 0 - успешно, 1 - ошибка
 FUNCTION AddInudl_lmtfuturmark_exch RETURN INTEGER;

 --добавим записи в буферную таблицу udl_dl_lmtadjust_exch_dbt 0 - успешно, 1 - ошибка
 FUNCTION AddInudl_dl_lmtadjust_exch RETURN INTEGER;

 --добавим СПИ субъекта 0 - успешно, 1 - ошибка
 FUNCTION AddSfSiForParty( p_PartyId IN NUMBER, p_ServiceKind IN NUMBER, p_KindOper IN NUMBER, p_FiKind IN NUMBER, p_FiCode IN VARCHAR,
  p_Account IN VARCHAR, p_BankId IN NUMBER, p_BankDate IN DATE ) RETURN INTEGER;

 --получим счет из СПИ субъекта и PartyId банка из СПИ субъекта 0 - успешно, 1 - ошибка
 FUNCTION GetSfSiAccountAndBankPartyId( p_PartyId IN NUMBER, p_ServiceKind IN NUMBER, p_KindOper IN NUMBER, p_FiKind IN NUMBER, p_FiCode IN VARCHAR, p_Account IN VARCHAR, p_AccountResult OUT VARCHAR,
  p_BankId OUT NUMBER ) RETURN INTEGER;

 --получим Id субъект и счет ДО по номеру открытого ДО 0 - успешно, 1 - ошибка
 FUNCTION GetPropFromContrNum( p_LegalForm IN NUMBER, p_ContrNumber IN VARCHAR, p_ObjectType IN NUMBER, p_Account IN VARCHAR, p_ObjType IN NUMBER,
  p_FiKind IN NUMBER, p_FiCode IN VARCHAR, p_PartyId OUT NUMBER, p_ContrId OUT NUMBER, p_AccountContr OUT VARCHAR, p_ContrAccountId OUT NUMBER  ) RETURN INTEGER;


 --вставка CLOB-XML в лог 0 - успешно, 1 - ошибка
 FUNCTION AddRecXMLToLOG( p_Cnum IN NUMBER ) RETURN INTEGER;

 --обработать загруженный CLOB-XML регистрация клиентов на МБ 0 - успешно, 1 - ошибка
 FUNCTION ProcwssObjAttrib(p_ObjectType IN NUMBER, p_CodeKind IN NUMBER, p_GroupId IN NUMBER, p_SessionId IN NUMBER,
  p_FileName IN VARCHAR, p_Oper IN NUMBER) RETURN INTEGER;


END USR_PKG_IMPORT_SOFR;
