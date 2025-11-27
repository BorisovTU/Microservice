CREATE OR REPLACE PACKAGE RSB_GTIM_VR
IS
  NOTUSED_EXTCODE   CONSTANT NUMBER(5) := 1;
  NOTFOUND_EXTCODE  CONSTANT NUMBER(5) := 2;
  INCORRECT_EXTCODE CONSTANT NUMBER(5) := 3;
  NOTFOUND_PARTYID  CONSTANT NUMBER(5) := 4;
  NOTFOUND_FIID     CONSTANT NUMBER(5) := 5;
  NOTFOUND_CALCFIID CONSTANT NUMBER(5) := 6;

  --Виды операций на валютном рынке
  ВнебиржФорвард      CONSTANT NUMBER(5) := 2690; --Внебиржевой форвард (Внебиржевая сделка с ПИ)
  ВалютныйСВОП        CONSTANT NUMBER(5) := 2710; --Валютный СВОП (Внебиржевая сделка с ПИ)
  КороткийСВОП        CONSTANT NUMBER(5) := 2715; --Короткий СВОП (Внебиржевая сделка с ПИ)
  ПроцентныйСВОП      CONSTANT NUMBER(5) := 2720; --Процентный СВОП(сделка с ПИ)*/
  ПИКО_ПокупкаПродажа CONSTANT NUMBER(5) := 2730; --Конверсионная сделка ФИССиКО Покупка/продажа
  ПИКО_СВОП           CONSTANT NUMBER(5) := 2740; --Конверсионная сделка ФИССиКО СВОП
  ПокупкаПродажаT3    CONSTANT NUMBER(5) := 2745; --Сделка Т+3 Покупка/продажа
  СВОП_T3             CONSTANT NUMBER(5) := 2746; --Сделка Т+3 Валютный СВОП не ПФИ
  --!!!измененные значения для РСХБ!!!
  ВнебиржФорвард_РСХБ CONSTANT NUMBER(5) := 12690; --Внебиржевой форвард (Внебиржевая сделка с ПИ)
  ВалютныйСВОП_РСХБ   CONSTANT NUMBER(5) := 22710; --Валютный СВОП (Внебиржевая сделка с ПИ)
  ПроцентныйСВОП_РСХБ CONSTANT NUMBER(5) := 22720; --Процентный СВОП (сделка с ПИ)
  --!!!пользовательские типы операций!!!
  КороткийСВОП_Кл        CONSTANT NUMBER(5) := 32715; --Короткий СВОП (Внебиржевая сделка с ПИ)
  ПИКО_ПокупкаПродажа_Кл CONSTANT NUMBER(5) := 32730; --Конверсионная сделка ФИССиКО Покупка/продажа Клиентская

/**
 * Создание объектов записей репликаций по данным временной таблицы CUX23
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_IsRSHB Признак реализации для РСХБ
 * @param p_MMVB_Code Код ММВБ
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_CUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsRSHB IN NUMBER, p_MMVB_Code IN VARCHAR2)
    RETURN NUMBER;

 /**
 * Создание объектов записей репликаций по данным временной таблицы CUX22
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_CUX22(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

 /**
 * Заполнение временной таблицы сделок CUX23 на основании Payments
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */
  FUNCTION FillCUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;

/**
 * Заполнение временной таблицы заявок CUX22 на основании Payments
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */
  FUNCTION FillCUX22(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;

/**
 * Обновление параметра DocNo во временной таблице сделок CUX23
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */
  FUNCTION UpdateDocNoCUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;

/**
 * Проверка расчетных кодов во временной таблице сделок CUX23
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Own_deals Признак отбора собственных сделок (1 - отбираем, 0 - нет)
 * @param p_Client_deals Признак отбора клиентских сделок (1 - отбираем, 0 - нет)
 * @return Код ошибки
 */
  FUNCTION CheckExtSettleCodeCUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Own_deals IN NUMBER, p_Client_deals IN NUMBER)
    RETURN NUMBER;

/**
 * Проверка расчетных кодов во временной таблице заявок CUX22
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION CheckExtSettleCodeCUX22(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Обновление идентификаторов клиентов во временной таблице сделок CUX23
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_MarketID Идентификатор биржи
 * @return Код ошибки
 */
  FUNCTION UpdateClientID_CUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_MarketID IN NUMBER)
    RETURN NUMBER;

/**
 * Обновление идентификаторов клиентов во временной таблице заявок CUX22
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_MarketID Идентификатор биржи
 * @return Код ошибки
 */
  FUNCTION UpdateClientID_CUX22(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_MarketID IN NUMBER)
    RETURN NUMBER;

/**
 * Обновление ID уже существующих объектов во временной таблице сделок CUX23
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_IsRSHB Признак реализации для РСХБ
 * @return Код ошибки
 */
  FUNCTION UpdateObjCUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsRSHB IN NUMBER)
    RETURN NUMBER;

/**
 * Обновление ID уже существующих объектов во временной таблице заявок CUX22
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION UpdateObjCUX22(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Обновление ID загруженных сделок в таблице Payments
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_IsRSHB Признак реализации для РСХБ
 * @return Код ошибки
 */
  FUNCTION UpdateCUX23_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_IsRSHB IN NUMBER)
    RETURN NUMBER;

/**
 * Обновление ID загруженных заявок в таблице Payments
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */
  FUNCTION UpdateCUX22_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;

/**
 * Очистка временной таблицы сделок CUX23
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION DelCUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Очистка временной таблицы заявок CUX22
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION DelCUX22(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Удаление неподтвержденных прогнозных сделок
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @param p_IsRSHB Признак реализации для РСХБ
 * @param p_NumImport Количество записей
 * @return Код ошибки
 */
  FUNCTION MarkDealsForDelete(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE, p_IsRSHB IN NUMBER, p_NumImport OUT NUMBER)
    RETURN NUMBER;

/**
 * Обновление видов сделок во временной таблице сделок CUX23
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_IsRSHB Признак реализации для РСХБ
 * @return Код ошибки
 */
  FUNCTION UpdateKindCUX23(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsRSHB IN NUMBER)
    RETURN NUMBER;

/**
 * Проверка на то, что операционист является спциалистом БО или админом
 * @since RSHB 84
 * @qtest NO
 * @param p_Oper Идентификатор операциониста
 * @return 1 - операционист является спциалистом БО или админом, 0 - не является
 */
  FUNCTION IsAdminOrOperBO(p_Oper IN NUMBER)
    RETURN NUMBER;

/**
 * Заполнение временной таблицы комиссионных вознаграждений CCX10 на основании Payments
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @param p_IsMain Признак того, что вызываем основную загрузку из фондового рынка, т.е. отбираем комиссии по собственным РК, или из валютного рынка, т.е. отбираем комиссии по клиентским РК вида 99: 1 - ФР, 0 - ВР
 * @return Код ошибки
 */
  FUNCTION FillCCX10(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE, p_IsMain IN NUMBER)
    RETURN NUMBER;

/**
 * Очистка временной таблицы комиссионных вознаграждений CCX10
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION DelCCX10(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Обновление параметра DocNo во временной таблице комиссионных вознаграждений CCX10
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */
  FUNCTION UpdateDocNoCCX10(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;

/**
 * Обновление ID уже существующих объектов во временной таблице комиссионных вознаграждений CCX10
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION UpdateObjCCX10(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Создание объектов записей репликаций по данным временной таблицы комиссионных вознаграждений CCX10
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_CCX10(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Обновление ID загруженных комиссионных вознаграждений CCX10 в таблице Payments
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */
  FUNCTION UpdateCCX10_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
/**
 * Очистка временной таблицы CCX17
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION DelCCX17(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

 /**
 * Заполнение временной таблицы сделок CCX17 на основании Payments
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */
  FUNCTION FillCCX17(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;

/**
 * Обновление ID уже существующих объектов во временной таблице сделок CUX23
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION UpdateObjCCX17(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Создание объектов записей репликаций по данным временной таблицы CCX17
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_CCX17(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Обновление ID загруженных сделок в таблице Payments
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */
  FUNCTION UpdateCCX17_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;

/**
 * Заполнение временной таблицы итоговых нетто-требований и нетто-обязательств CCX4 на основании Payments
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */
  FUNCTION FillCCX4(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;

/**
 * Очистка временной таблицы итоговых нетто-требований и нетто-обязательств CCX4
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION DelCCX4(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Обновление ID уже существующих объектов во временной таблице итоговых нетто-требований и нетто-обязательств CCX4
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION UpdateObjCCX4(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Создание объектов записей репликаций по данным временной таблицы итоговых нетто-требований и нетто-обязательств CCX4
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_CCX4(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

/**
 * Обновление ID загруженных итоговых нетто-требований и нетто-обязательств CCX4 в таблице Payments
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */
  FUNCTION UpdateCCX4_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;

/**
 * Очистка временной таблицы CCX99
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_IsVR Признак обработки ВР
 * @return Код ошибки
 */
  FUNCTION DelCCX99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsVR IN NUMBER)
    RETURN NUMBER;

 /**
 * Заполнение временной таблицы сделок CCX99 на основании Payments
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */
  FUNCTION FillCCX99VR(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;

/**
 * Заполнение временной таблицы сделок CCX99 на основании Payments
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */
  FUNCTION FillCCX99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;

/**
 * Обновление ID уже существующих объектов во временной таблице сделок CCX99
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_IsVR Признак обработки ВР
 * @return Код ошибки
 */
  FUNCTION UpdateObjCCX99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsVR IN NUMBER)
    RETURN NUMBER;

/**
 * Создание объектов записей репликаций по данным временной таблицы CCX99
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_IsVR Признак обработки ВР
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_CCX99(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsVR IN NUMBER)
    RETURN NUMBER;

/**
 * Обновление ID загруженных сделок в таблице Payments
 * @since RSHB 84
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_IsVR Признак обработки ВР
 * @return Код ошибки
 */
  FUNCTION UpdateCCX99_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_IsVR IN NUMBER)
    RETURN NUMBER;

END RSB_GTIM_VR;
/