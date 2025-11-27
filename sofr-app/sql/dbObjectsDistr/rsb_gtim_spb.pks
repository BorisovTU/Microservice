CREATE OR REPLACE PACKAGE RSB_GTIM_SPB
IS

/**
 * Обновление типа ТКС
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_MarketID Идентификатор биржи
 * @return Код ошибки
 */
  FUNCTION UpdateClrAccCodeType_SPB03(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_MarketID IN NUMBER)
    RETURN NUMBER;
        
/**
 * Создание объектов записей репликаций по данным временной таблицы SPB03 (сделки)
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @param p_PrmStr Параметры передаваемые в строке (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_SPB03(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2)
    RETURN NUMBER;

/**
 * Создание объектов записей репликаций по данным временной таблицы MFB06C (клиринг)
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @param p_PrmStr Параметры передаваемые в строке (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_MFB06C(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2)
    RETURN NUMBER;

/**
 * Создание объектов записей репликаций по данным временной таблицы ORDERS (заявки)
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @param p_PrmStr Параметры передаваемые в строке (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_ORDERS(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2)
    RETURN NUMBER;
    
-- ОБРАБОТКА ИНФОРМАЦИИ О ДВИЖЕНИИ ДЕНЕЖНЫХ СРЕДСТВ MFB99

/**
 * Очистка таблицы MFB99
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
   FUNCTION DelMFB99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;  
    
/**
 * Заполнение временной таблицы сделок MFB99 на основании Payments
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Imp_date Дата импорта
 * @return Код ошибки
 */
  FUNCTION FillMFB99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Imp_date IN DATE)
    RETURN NUMBER;
    
/**
 * Обновление ID уже существующих объектов во временной таблице MFB99
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION UpdateObjMFB99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Обновление ID загруженных сделок в таблице Payments
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */
  FUNCTION UpdateMFB99_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Создание объектов записей репликаций по данным временной таблицы MFB99
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_MFB99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

  --ОБРАБОТКА ИНФОРМАЦИИ О НЕТТО-ТРЕБОВАНИЯХ И НЕТТО-ОБЯЗАТЕЛЬСТВАХ MFB13
  
/**
 * Очистка таблицы MFB13
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
   FUNCTION DelMFB13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER; 
    
/**
 * Заполнение временной таблицы сделок MFB13 на основании Payments
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Imp_date Дата импорта
 * @return Код ошибки
 */
  FUNCTION FillMFB13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Imp_date IN DATE)
    RETURN NUMBER;
    
/**
 * Обновление ID уже существующих объектов во временной таблице MFB13
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION UpdateObjMFB13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Обновление ID загруженных сделок в таблице Payments
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */
  FUNCTION UpdateMFB13_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Создание объектов записей репликаций по данным временной таблицы MFB13
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_MFB13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

END RSB_GTIM_SPB;
/