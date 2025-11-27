-- Author  : Dubovoy Andrey
-- Created : 22.03.2022
-- Purpose : Создания записей репликаций в шлюзе по подготовленным данным ММВБ

CREATE OR REPLACE PACKAGE RSB_GTIM_MMVB
IS

/**
 * Создание объектов записей репликаций по данным временной таблицы SEM02
 * @since RSHB 82
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @param p_PrmStr Прочие параметры передаваемые строкой (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_SEM02(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2)
    RETURN NUMBER;

/**
 * Создание объектов записей репликаций по данным временной таблицы SEM03
 * @since RSHB 82
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @param p_PrmStr Прочие параметры передаваемые строкой (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_SEM03(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2)
    RETURN NUMBER;

/**
 * Создание объектов записей репликаций по данным временной таблицы EQM06
 * @since RSHB 82
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @param p_PrmStr Прочие параметры передаваемые строкой (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_EQM06(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2)
    RETURN NUMBER;

/**
 * Очистка временной таблицы заявок на внебиржевые сделки EQM2T
 * @since RSHB 87
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION DelEQM2T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Очистка временной таблицы заключенных внебиржевых сделок EQM3T
 * @since RSHB 87
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION DelEQM3T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Заполнение временной таблицы заявок на внебиржевые сделки EQM2T на основании Payments
 * @since RSHB 87
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_ImpDate Дата импорта
 * @return Код ошибки
 */
  FUNCTION FillEQM2T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_ImpDate IN DATE)
    RETURN NUMBER;
    
/**
 * Обновление ID уже существующих объектов во временной таблице заявок на внебиржевые сделки EQM2T
 * @since RSHB 87
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION UpdateObjEQM2T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Обновление ID загруженных заявок на внебиржевые сделки в таблице Payments
 * @since RSHB 87
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */
  FUNCTION UpdateEQM2T_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Создание объектов записей репликаций по данным временной таблицы заявок на внебиржевые сделки EQM2T
 * @since RSHB 87
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @param p_MMVB_Code Код ММВБ
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_EQM2T(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_MMVB_Code IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Заполнение временной таблицы внебиржевых сделок EQM3T
 * @since RSHB 87
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_ImpDate Дата импорта
 * @return Код ошибки
 */
  FUNCTION FillEQM3T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_ImpDate IN DATE)
    RETURN NUMBER;

/**
 * Обновление ID уже существующих объектов во временной таблице внебиржевых сделок EQM3T
 * @since RSHB 87
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION UpdateObjEQM3T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Обновление Contr  EQM3T
 * @since RSHB 87
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION UpdateContrEQM3T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_ContrID IN NUMBER)
    RETURN NUMBER;
    
/**
 * обновление Doc_no EQM3T
 * @since RSHB 87
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */

  FUNCTION UpdateDocEQM3T(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
/**
 * Обновление ID загруженных внебиржевых сделок в таблице Payments
 * @since RSHB 87
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */
  FUNCTION UpdateEQM3T_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;

/**
 * Создание объектов записей репликаций по данным временной таблицы EQM3T
 * @since RSHB 87
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @param p_PrmStr Прочие параметры передаваемые строкой (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_EQM3T(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_PrmStr IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Очистка временной таблицы итоговых нетто-требований и нетто-обязательств EQM13
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION DelEQM13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Заполнение временной таблицы итоговых нетто-требований и нетто-обязательств EQM13 на основании Payments
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_ImpDate Дата импорта
 * @return Код ошибки
 */
  FUNCTION FillEQM13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_ImpDate IN DATE)
    RETURN NUMBER;
    
/**
 * Обновление параметра DocNo во временной таблице итоговых нетто-требований и нетто-обязательств EQM13
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */
  FUNCTION UpdateDocNoEQM13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Обновление ID уже существующих объектов во временной таблице итоговых нетто-требований и нетто-обязательств EQM13
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION UpdateObjEQM13(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Обновление ID загруженных итоговых нетто-требований и нетто-обязательств в таблице Payments
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */
  FUNCTION UpdateEQM13_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Создание объектов записей репликаций по данным временной таблицы итоговых нетто-требований и нетто-обязательств EQM13
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_EQM13(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Очистка временной таблицы данных о выдаче/погашении КСУ EQM99
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION DelEQM99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Заполнение временной таблицы данных о выдаче/погашении КСУ EQM99 на основании Payments
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_ImpDate Дата импорта
 * @return Код ошибки
 */
  FUNCTION FillEQM99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_ImpDate IN DATE)
    RETURN NUMBER;
    
/**
 * Обновление ID уже существующих объектов во временной таблице данных о выдаче/погашении КСУ EQM99
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION UpdateObjEQM99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Обновление ID загруженных данных о выдаче/погашении КСУ в таблице Payments
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */
  FUNCTION UpdateEQM99_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Создание объектов записей репликаций по данным временной таблицы данных о выдаче/погашении КСУ EQM99
 * @since RSHB 93
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_EQM99(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_MMVB_Code IN VARCHAR2)
    RETURN NUMBER;
    
END RSB_GTIM_MMVB;
/
