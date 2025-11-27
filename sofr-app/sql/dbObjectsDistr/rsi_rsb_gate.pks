CREATE OR REPLACE PACKAGE RSI_RSB_GATE IS
    m_notUseGateForObjCode  CHAR := CHR(0);

    ZERO_DATE CONSTANT DATE := TO_DATE('01010001', 'DDMMYYYY');

    CONST_APPID_RSBANK_SRC  CONSTANT NUMBER(5)  := 1;   -- Rs-Bank Source
    CONST_APPID_RSBANK_TGT  CONSTANT NUMBER(5)  := 11;  -- Rs-Bank Target

    CONST_ACTIONID_INS      CONSTANT NUMBER(5)  := 1;   -- Действие: добавить
    CONST_ACTIONID_UPD      CONSTANT NUMBER(5)  := 2;   -- Действие: обновить
    CONST_ACTIONID_DEL      CONSTANT NUMBER(5)  := 3;   -- Действие: удалить
    CONST_ACTIONID_SYNC     CONSTANT NUMBER(5)  := 4;   -- Действие: синхрониз.

    CONST_DIRECTION_IMP     CONSTANT NUMBER(5)  := 1; -- Импорт
    CONST_DIRECTION_EXP     CONSTANT NUMBER(5)  := 2; -- Экспорт
    CONST_KIND_DOWNLOAD     CONSTANT NUMBER(5)  := 1; -- Загрузка

    CONST_STATUS_RDYTOPROC  CONSTANT NUMBER(5)  := 7; -- Готов к обработке
    
    --Используемые статусы ЗР импорта
    CONST_STATUS_IMP_READYTOPROC   CONSTANT NUMBER(5) := 2; -- Готов к обработке
    CONST_STATUS_IMP_SUCCESSPROC   CONSTANT NUMBER(5) := 3; -- Обработан
    CONST_STATUS_IMP_REFUSEDTOPROC CONSTANT NUMBER(5) := 4; -- Отказ в обработке
    CONST_STATUS_IMP_POSTPONEPROC  CONSTANT NUMBER(5) := 5; -- Отложен
    CONST_STATUS_IMP_REJECTEDPROC  CONSTANT NUMBER(5) := 6; -- Отвергнут (корзина)
    
    TYPE ObjectChange_rec IS RECORD
    (
      T_OBJECTKIND      NUMBER(5),
      T_OBJECTCODE      VARCHAR2(100),
      T_OBJECTNAME      VARCHAR2(256),
      T_ACTIONID        NUMBER(5),
      T_ID_OPERATION    NUMBER(10),
      T_ID_STEP_NUMBER  NUMBER(5)
    );

    TYPE ObjectChange_cur IS REF CURSOR RETURN ObjectChange_rec;

    FUNCTION notUseGateForObjCode RETURN CHAR;

    PROCEDURE OptimizeGTRecords(p_seanceid IN NUMBER, p_applicationid_to IN NUMBER);

    PROCEDURE Al_RegistryObjectEx;

    PROCEDURE Al_RegistryObject(och_cur IN ObjectChange_cur, IsOperStartup
            IN CHAR DEFAULT 'X', StartId IN NUMBER DEFAULT RSBSESSIONDATA.oper);

    FUNCTION CheckClientIDByStatus( p_ClientID IN NUMBER,
                                    p_StatusID IN NUMBER,
                                    p_ObjectKind IN NUMBER DEFAULT 0,
                                    p_SysDate IN DATE DEFAULT ZERO_DATE,
                                    p_AppFromExclude IN STRING DEFAULT NULL
                                   )RETURN CHAR;
 
/**
 * Проверка наличия у клиента необработанных записей репликации (в статусе "Готов к обработке" или "Отказ в обработке")
 * @since RSHB 85
 * @qtest NO
 * @param p_ClientID Идентификатор клиента
 * @param p_ObjectKind Вид объекта (все, если 0)
 * @param p_SysDate Дата импорта
 * @param p_AppFromExclude Непроверяемые источники данных
 * @return 'X' при наличии необработанных ЗР, CHR(0) при отсутствии
 */                                  
    FUNCTION CheckClientIDByRawRecords( p_ClientID IN NUMBER,
                                        p_ObjectKind IN NUMBER DEFAULT 0,
                                        p_SysDate IN DATE DEFAULT ZERO_DATE,
                                        p_AppFromExclude IN STRING DEFAULT NULL
                                      )RETURN CHAR;

END RSI_RSB_GATE;
/