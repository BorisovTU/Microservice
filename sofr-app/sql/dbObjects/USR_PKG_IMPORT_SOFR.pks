CREATE OR REPLACE PACKAGE USR_PKG_IMPORT_SOFR
AS

 REG_OBJECT_SYNCH   CONSTANT VARCHAR2(2000) := 'РСХБ\ИНТЕГРАЦИЯ\СПРАВОЧНИКИ\OBJECTS_SYNH'; --ветка реестра содержит Id Пользовательского справочника объектов очеререди заданий
 REG_PAYM_OPROPER   CONSTANT VARCHAR2(2000) := 'РСХБ\ИНТЕГРАЦИЯ\СПРАВОЧНИКИ\ПЛАТЕЖИ_ОПЕРАЦИИ'; --ветка реестра содержит Id Пользовательского справочника DocKind pmpaym и oproper

 --заменяем не цифровые символы строки кодом ASCII
 FUNCTION ASCIISymNonDigitToCode( p_StrSource IN VARCHAR2, p_CntSymbCorrect IN INTEGER DEFAULT 0, p_isReverse INTEGER DEFAULT 0) RETURN VARCHAR2;

 --получить строку ошибки события
 FUNCTION GetStrEventErr( p_Mode IN INTEGER, p_Objecttype IN NUMBER, p_ObjectId IN NUMBER ) RETURN VARCHAR2;

 --получить строку расхождений 
 FUNCTION GetDifferenceStrErr( p_Mode IN INTEGER, p_AccD_BISQUIT IN VARCHAR2, p_AccC_BISQUIT IN VARCHAR2, p_AmCur_BISQUIT IN NUMBER,
   p_AmNatCur_BISQUIT IN NUMBER, p_Cur_BISQUIT IN VARCHAR2, p_Ground_BISQUIT IN VARCHAR2, p_DocNum_BISQUIT IN VARCHAR2,
   p_AcctrnId IN NUMBER, p_Id_BISQUIT IN VARCHAR2, p_ReqId_BISQUIT IN VARCHAR2 ) RETURN VARCHAR2;

 --проверим наличие кода Бисквит к платежу, 0 - не найден, 1 - найден, -1 - ошибка
 FUNCTION CheckBiscottoId( p_BiscottoId IN VARCHAR2 ) RETURN NUMBER;
 
 function FindSofrIdByBiscotto( p_BiscottoId IN VARCHAR2 ) RETURN NUMBER;

 --добавляем код Бисквит к платежу, 0 - успешно, 1 - ошибка
 FUNCTION AddBiscottoId( p_PmId IN NUMBER, p_DocKind IN NUMBER, p_BiscottoId IN VARCHAR2 ) RETURN NUMBER;

 --добавляем оферту для фин. инструмента, 0 - успешно, 1 - ошибка
 FUNCTION AddOffer( p_SOFR_FintoolId IN VARCHAR2, p_Fiid IN NUMBER ) RETURN NUMBER;

 --установить дату ограничения действия категории, добавить категорию, если имеются с более поздней датой действия 0 - успешно (ограничили или не нашли),
 -- 1 - успешно ( добавили категорию с ограничениями дат ), 2 - ошибка
 FUNCTION IsAttrIdDisconnectedSmart( p_ObjType IN NUMBER, p_GroupId IN NUMBER, p_PartyId IN NUMBER, p_AttrId IN NUMBER, p_ParentAttrId IN NUMBER,
   p_DateRating IN DATE ) RETURN NUMBER;

 FUNCTION IsAttrIdDisconnectedSmart2( p_ObjType IN NUMBER, p_GroupId IN NUMBER, p_PartyId IN VARCHAR2, p_AttrId IN NUMBER, p_ParentAttrId IN NUMBER,
   p_DateRating IN DATE ) RETURN NUMBER;


 --добавить код FIID 0 - успешно, 1 - ошибка
 FUNCTION AddCodeFIID( p_ObjType IN NUMBER, p_CodeKind IN NUMBER, p_ObjId IN NUMBER, p_Code IN VARCHAR2, p_Mode IN INTEGER ) RETURN NUMBER;

 --получим наименование субъекта-банк по коду 3 и 6
 FUNCTION GetFullNameFromTwoCode( p_Code_1 IN VARCHAR2, p_Code_2 IN VARCHAR2,
   p_CodeKind_1 IN NUMBER, p_CodeKind_2 IN NUMBER, p_Date IN DATE ) RETURN VARCHAR2;

 --получим Id субъекта-банк с закрытым кодом максимальной датой закрытия, -1 - не найден
 FUNCTION GetPtIdActORLastClosedByCode( p_Code IN VARCHAR2, p_CodeKind IN NUMBER, p_Date IN DATE ) RETURN NUMBER;

 --получим ветку реестра содержит Id Пользовательского справочника объектов очеререди заданий
 FUNCTION GetREG_OBJECT_SYNCH RETURN VARCHAR2;

 --получим ветку реестра содержит Id Пользовательского справочника DocKind pmpaym и oproper
 FUNCTION GetREG_PAYM_OPROPER RETURN VARCHAR2;

 --получим Id Пользовательского справочника объектов очеререди заданий
 FUNCTION GetOBJECT_SYNCH RETURN NUMBER;

 --получим Id Пользовательского справочника DocKind pmpaym и oproper
 FUNCTION GetPAYM_OPROPER RETURN NUMBER;

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
  p_FiKind IN NUMBER, p_FiCode IN VARCHAR, p_PartyId OUT NUMBER, p_ContrId OUT NUMBER, p_AccountContr OUT VARCHAR, p_ContrAccountId OUT NUMBER, p_ServKind OUT NUMBER ) RETURN INTEGER;


 --вставка CLOB-XML в лог 0 - успешно, 1 - ошибка
 FUNCTION AddRecXMLToLOG( p_Cnum IN NUMBER ) RETURN INTEGER;

 --обработать загруженный CLOB-XML регистрация клиентов на МБ 0 - успешно, 1 - ошибка
 FUNCTION ProcwssObjAttrib(p_ObjectType IN NUMBER, p_CodeKind IN NUMBER, p_GroupId IN NUMBER, p_SessionId IN NUMBER,
  p_FileName IN VARCHAR, p_Oper IN NUMBER) RETURN INTEGER;


    /* Функция вставки задания в очередь для пользовательских объектов */
    FUNCTION InsertSequenceJobUsrObj(p_object_id IN NUMBER,
                                     p_object_type IN NUMBER,
                                     p_process_type IN NUMBER,
                                     p_obj_type_code IN VARCHAR2) RETURN INTEGER;

    /* Функция обновления статуса записи в указанной таблице (utableprocessevent_dbt; utableprocessout_dbt, utableprocessin_dbt) */
    FUNCTION UpdateEvntProcessTable(p_recid IN NUMBER,
                                    p_status IN NUMBER,
                                    p_tbl_name IN VARCHAR2) RETURN INTEGER;

    /* Функция для начала обработки процесса с созданием задачи в dfuncobj_dbt */
    FUNCTION MakeStartProcessWTask(p_recid IN NUMBER,
                                   p_status IN NUMBER,
                                   p_type IN VARCHAR2,
                                   p_tbl_name IN VARCHAR2) RETURN INTEGER;

    /* Informatica. Вставка курса */
    FUNCTION ImportOneCourse_g(p_BaseFIID      IN dratedef_dbt.T_OTHERFI%TYPE,
                             p_OtherFIID     IN dratedef_dbt.T_FIID%TYPE,
                             p_RateKind      IN dratedef_dbt.T_TYPE%TYPE,
                             p_SinceDate     IN dratedef_dbt.T_SINCEDATE%TYPE,
                             --p_MarketCode IN varchar2,
                             --p_MarketCodeKind IN integer,
                             p_MarketPlace   IN dratedef_dbt.T_MARKET_PLACE%TYPE,
                             p_MarketSection IN dratedef_dbt.T_SECTION%TYPE,
                             p_Rate          IN dratedef_dbt.T_RATE%TYPE,
                             p_Scale         IN dratedef_dbt.T_SCALE%TYPE,
                             p_Point         IN dratedef_dbt.T_POINT%TYPE,
                             --p_BoardID IN varchar2,
                             p_IsRelative    IN dratedef_dbt.T_ISRELATIVE%TYPE default null,
                             p_IsDominant    IN dratedef_dbt.T_ISDOMINANT%TYPE default chr(0),
                             p_IsInverse     IN dratedef_dbt.T_ISINVERSE%TYPE default chr(0),
                             p_Oper          IN dratedef_dbt.T_OPER%TYPE default 0,
                             Err             OUT VARCHAR2)
    RETURN INTEGER;

    -- (kva) Функция генерации СМС-кода
    FUNCTION f_make_sms_code(p_type IN NUMBER,
                             p_client_id IN NUMBER,
                             p_num_of_try IN NUMBER,
                             p_sms_code OUT VARCHAR2) RETURN NUMBER;

    -- (kva) Функция генерации СМС-кода с обеспечением его исключительности по статусу
    -- (остальные коды заданного типа по заданному клиенту будут переведены в статус "Код не активен")
    FUNCTION f_make_only_one_code(p_type IN NUMBER,
                                  p_client_id IN NUMBER,
                                  p_num_of_try IN NUMBER,
                                  p_sms_code OUT VARCHAR2) RETURN NUMBER;

    -- (kva) Функция сохранения кодового слова в буферной таблице
    FUNCTION f_save_code_word_buf(p_cw_client IN NUMBER,
                                  p_code_word IN VARCHAR2,
                                  p_code_type IN NUMBER,
                                  p_code_stat IN NUMBER,
                                  p_ret_txt OUT VARCHAR2) RETURN NUMBER;

    -- (kva) Функция переноса кодового слова из буферной таблицы в таблицу текущих значений (с изменением статусов)
    FUNCTION f_save_accepted_code_word(p_cw_type IN NUMBER,
                                       p_client IN NUMBER,
                                       p_stat_cw_chng IN NUMBER,
                                       p_stat_cw_accepted IN NUMBER,
                                       p_ret_txt OUT VARCHAR2) RETURN NUMBER;

    -- (kva) Функция блокировки кодового слова
    FUNCTION f_block_code_word(p_cw_id IN NUMBER,
                               p_change_oper IN NUMBER) RETURN NUMBER;

    -- (kva) Функция добавления кодового слова
    FUNCTION f_add_code_word(p_cw_type IN NUMBER,
                             p_cw_client IN NUMBER,
                             p_code_word IN VARCHAR2,
                             p_man_oper IN NUMBER) RETURN NUMBER;

    /* Добавление категории к объекту.
     * 0 - категория успешно добавлена
     * 1 -значение категории уже заполнено. Ничего не редактируется
     * 2 - неизвестная ошибка
     */
    FUNCTION ConnectAttr (p_ObjType IN NUMBER,
                          p_GroupId IN NUMBER,
                          p_ObjId IN VARCHAR2,
                          p_AttrId IN NUMBER,
                          p_ValidFromDate IN DATE ) RETURN NUMBER;

    /**
     @brief    		Функция для обновления статуса процесса.
     @param[in]    	p_RecID    		ID записи
     @param[in]    	p_Status    		статус, который нужно установить
     @param[in]    	p_CheckConcurent	флаг проверки конкурентов (0 -- не проверять, 1 -- проверять)
     @param[in]    	p_TblName    		имя таблицы (utableprocessevent_dbt, utableprocessout_dbt, utableprocessin_dbt)
     @return                            	0 -- обновлено успешно, 1 -- не обновлено, -1 -- нет записи
    */
    FUNCTION UpdateProcess( 
      p_RecID IN NUMBER
      , p_Status IN NUMBER
      , p_CheckConcurent IN NUMBER DEFAULT 0
      , p_TblName IN VARCHAR2 DEFAULT 'utableprocessin_dbt'
    ) 
    RETURN number;

    /**
     @brief    		Функция для запуска процесса.
     @param[in]    	p_RecID    		ID записи
     @param[in]    	p_TblName    		имя таблицы (utableprocessevent_dbt, utableprocessout_dbt, utableprocessin_dbt)
     @param[in]    	p_SleepTime    		кол-во секунд, на которые процесс засыпает при наличии конкурентов
     @param[in]    	p_TimeOut    		тайм-аут, по истечении которого процесс останавливается
     @return                            	0 -- запуск произведен успешно, 1 -- ошибка запуска, -1 -- завершение по тайм-ауту
    */
    FUNCTION StartProcess( 
      p_RecID IN NUMBER
      , p_TblName IN VARCHAR2 DEFAULT 'utableprocessin_dbt'
      , p_SleepTime IN number DEFAULT 10
      , p_TimeOut IN NUMBER DEFAULT 600 
    ) 
    RETURN number;


END USR_PKG_IMPORT_SOFR;
/
