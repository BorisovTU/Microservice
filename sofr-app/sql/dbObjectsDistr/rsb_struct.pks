CREATE OR REPLACE PACKAGE rsb_struct AS

 TYPE r_fmtStruct_type IS RECORD
  ( t_Name   VARCHAR2(30),
    t_Type   INTEGER,
    t_Size   INTEGER,
    t_Offset INTEGER);

 TYPE t_fmtStruct_type IS TABLE OF r_fmtStruct_type
   INDEX BY BINARY_INTEGER;

/**
 * Переменные пакета для хранения структур из FMT
 */
 g_fmtStruct_tab t_fmtStruct_type;

 g_fmtStruct_rec r_fmtStruct_type;

 g_fmtStruct_name VARCHAR2(30);

/**
 * Процедура чтения структуры из FMT
 * @param p_structName  имя структуры
 */
 PROCEDURE readStruct (p_structName VARCHAR2);

/**
 * Функция получения размера структуры
 * @return NUMBER
 */
 FUNCTION getRecordSize (p_structName VARCHAR2) RETURN NUMBER;

/**
 * Функция получения наименования структуры
 * @return VARCHAR2
 */
 FUNCTION getStructName RETURN VARCHAR2;

/**
 * Функция получения целочисленного значения поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return NUMBER
 */
 FUNCTION getInt (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER;

/**
 * Функция получения целочисленного значения поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return NUMBER
 */
 FUNCTION getInt (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER;

/**
 * Функция получения целочисленного значения поля структуры
 * @param p_Value      структура содержащая значение
 * @return NUMBER
 */
 FUNCTION getInt (p_Value BLOB) RETURN NUMBER;

/**
 * Функция получения целочисленного значения поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return NUMBER
 */
 FUNCTION getLong (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER;

/**
 * Функция получения целочисленного значения поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return NUMBER
 */
 FUNCTION getLong (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER;

/**
 * Функция получения целочисленного значения поля структуры
 * @param p_Value      структура содержащая значение
 * @return NUMBER
 */
 FUNCTION getLong (p_Value BLOB) RETURN NUMBER;

/**
 * Функция получения вещественного значения поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return NUMBER
 */
 FUNCTION getDouble (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER;

/**
 * Функция получения вещественного значения поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return NUMBER
 */
 FUNCTION getDouble (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER;

/**
 * Функция получения вещественного значения поля структуры
 * @param p_Value      структура содержащая значение
 * @return NUMBER
 */
 FUNCTION getDouble (p_Value BLOB) RETURN NUMBER;

/**
 * Функция получения значения типа Деньги поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return NUMBER
 */
 FUNCTION getMoney (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER;

/**
 * Функция получения значения типа Деньги поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return NUMBER
 */
 FUNCTION getMoney (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER;

/**
 * Функция получения значения типа Деньги поля структуры
 * @param p_Value      структура содержащая значение
 * @return NUMBER
 */
 FUNCTION getMoney (p_Value BLOB) RETURN NUMBER;

/**
 * Функция получения строкового значения поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return VARCHAR2
 */
 FUNCTION getString (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN VARCHAR2;

/**
 * Функция получения строкового значения поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return VARCHAR2
 */
 FUNCTION getString (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN VARCHAR2;

/**
 * Функция получения строкового значения поля структуры
 * @param p_Value      структура содержащая значение
 * @return VARCHAR2
 */
 FUNCTION getString (p_Value BLOB) RETURN VARCHAR2;

/**
 * Функция получения символьного значения поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return CHAR
 */
 FUNCTION getChar (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN CHAR;

/**
 * Функция получения символьного значения поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return CHAR
 */
 FUNCTION getChar (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN CHAR;

/**
 * Функция получения символьного значения поля структуры
 * @param p_Value      структура содержащая значение
 * @return CHAR
 */
 FUNCTION getChar (p_Value BLOB) RETURN CHAR;

/**
 * Функция получения значения типа Дата поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return DATE
 */
 FUNCTION getDate (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN DATE;

/**
 * Функция получения значения типа Дата поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return DATE
 */
 FUNCTION getDate (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN DATE;

/**
 * Функция получения значения типа Дата поля структуры
 * @param p_Value      структура содержащая значение
 * @return DATE
 */
 FUNCTION getDate (p_Value BLOB) RETURN DATE;

/**
 * Функция получения значения типа Время поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return DATE
 */
 FUNCTION getTime (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN DATE;

/**
 * Функция получения значения типа Время поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return DATE
 */
 FUNCTION getTime (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN DATE;

/**
 * Функция получения значения типа Время поля структуры
 * @param p_Value      структура содержащая значение
 * @return DATE
 */
 FUNCTION getTime (p_Value BLOB) RETURN DATE;

/**
 * Функция получения значения типа Байт поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return NUMBER
 */
 FUNCTION getOneByte (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER;

/**
 * Функция получения значения типа Байт поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return NUMBER
 */
 FUNCTION getOneByte (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER;

/**
 * Функция получения значения типа RAW поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return RAW
 */
 FUNCTION getNByte (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN RAW;

/**
 * Функция получения значения типа RAW поля структуры
 * @param p_fieldName  имя поля
 * @param p_Value      структура содержащая значение
 * @param p_RecOffset  смещение
 * @return RAW
 */
 FUNCTION getNByte (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN RAW;

/**
 * Функция помещения целочисленного значения в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return BLOB
 */
 FUNCTION putInt (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB;

/**
 * Функция помещения целочисленного значения в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return RAW
 */
 FUNCTION putInt (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN RAW;

/**
 * Функция помещения целочисленного значения в поле структуры
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @return BLOB
 */
 FUNCTION putInt (p_destValue BLOB, p_srcValue NUMBER) RETURN BLOB;

/**
 * Функция помещения целочисленного значения в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return BLOB
 */
 FUNCTION putLong (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB;

/**
 * Функция помещения целочисленного значения в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return RAW
 */
 FUNCTION putLong (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN RAW;

/**
 * Функция помещения целочисленного значения в поле структуры
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @return BLOB
 */
 FUNCTION putLong (p_destValue BLOB, p_srcValue NUMBER) RETURN BLOB;

/**
 * Функция помещения вещественного значения в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return BLOB
 */
 FUNCTION putDouble (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue FLOAT, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB;

/**
 * Функция помещения вещественного значения в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return RAW
 */
 FUNCTION putDouble (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue FLOAT, p_RecOffset NUMBER DEFAULT 0) RETURN RAW;

/**
 * Функция помещения вещественного значения в поле структуры
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @return BLOB
 */
 FUNCTION putDouble (p_destValue BLOB, p_srcValue FLOAT) RETURN BLOB;

/**
 * Функция помещения значения типа Деньги в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return BLOB
 */
 FUNCTION putMoney (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB;

/**
 * Функция помещения значения типа Деньги в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return RAW
 */
 FUNCTION putMoney (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN RAW;

/**
 * Функция помещения значения типа Деньги в поле структуры
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @return BLOB
 */
 FUNCTION putMoney (p_destValue BLOB, p_srcValue NUMBER) RETURN BLOB;

/**
 * Функция помещения строкового значения в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return BLOB
 */
 FUNCTION putString (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue VARCHAR2, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB;

/**
 * Функция помещения строкового значения в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return RAW
 */
 FUNCTION putString (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue VARCHAR2, p_RecOffset NUMBER DEFAULT 0) RETURN RAW;

/**
 * Функция помещения строкового значения в поле структуры
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @return BLOB
 */
 FUNCTION putString (p_destValue BLOB, p_srcValue VARCHAR2) RETURN BLOB;

/**
 * Функция помещения символьного значения в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return BLOB
 */
 FUNCTION putChar (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue CHAR, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB;

/**
 * Функция помещения символьного значения в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return RAW
 */
 FUNCTION putChar (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue CHAR, p_RecOffset NUMBER DEFAULT 0) RETURN RAW;

/**
 * Функция помещения символьного значения в поле структуры
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @return BLOB
 */
 FUNCTION putChar (p_destValue BLOB, p_srcValue CHAR) RETURN BLOB;

/**
 * Функция помещения значения типа Дата в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return BLOB
 */
 FUNCTION putDate (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue DATE, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB;

/**
 * Функция помещения значения типа Дата в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return RAW
 */
 FUNCTION putDate (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue DATE, p_RecOffset NUMBER DEFAULT 0) RETURN RAW;

/**
 * Функция помещения значения типа Дата в поле структуры
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @return BLOB
 */
 FUNCTION putDate (p_destValue BLOB, p_srcValue DATE) RETURN BLOB;

/**
 * Функция помещения значения типа Время в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return BLOB
 */
 FUNCTION putTime (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue DATE, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB;

/**
 * Функция помещения значения типа Время в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return RAW
 */
 FUNCTION putTime (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue DATE, p_RecOffset NUMBER DEFAULT 0) RETURN RAW;

/**
 * Функция помещения значения типа Время в поле структуры
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @return BLOB
 */
 FUNCTION putTime (p_destValue BLOB, p_srcValue DATE) RETURN BLOB;

/**
 * Функция помещения значения типа Байт в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return
 */
 FUNCTION putOneByte (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB;

/**
 * Функция помещения значения типа Байт в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return RAW
 */
 FUNCTION putOneByte (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN RAW;

/**
 * Функция помещения значения типа RAW в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return BLOB
 */
 FUNCTION putNByte (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue RAW, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB;

/**
 * Функция помещения значения типа RAW в поле структуры
 * @param p_fieldName  имя поля
 * @param p_destValue  структура для помещения значения
 * @param p_srcValue   значение
 * @param p_RecOffset  смещение
 * @return RAW
 */
 FUNCTION putNByte (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue RAW, p_RecOffset NUMBER DEFAULT 0) RETURN RAW;

END rsb_struct;
/
