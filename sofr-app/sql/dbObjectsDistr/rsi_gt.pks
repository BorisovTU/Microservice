CREATE OR REPLACE PACKAGE RSI_GT
IS
  GTCODE_RECEIVE CONSTANT DGTAPP_DBT.T_CODE%TYPE := 'TGT-1'; --Получатель

  DIRECT_INSERT_YES CONSTANT NUMBER(5) := 1; --Добавление объектов в накопители непосредственно
  DIRECT_INSERT_NO  CONSTANT NUMBER(5) := 0; --Добавление объектов в накопители только после вызова DoRgInitRec

  ZeroDate CONSTANT DATE := TO_DATE('01.01.0001', 'DD.MM.YYYY');
  ZeroTime CONSTANT DATE := TO_DATE('01.01.0001 00:00:00', 'DD.MM.YYYY HH24:MI:SS');

/**
 * Инициализация глобального (в рамках текущей сессии) накопителя объектов ЗР для массовой вставки
 * @since RSHB 82
 * @qtest NO
 * @param p_DirectInsert Добавлять объекты напрямую в накопители (DIRECT_INSERT_YES, DIRECT_INSERT_NO)
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_ReceiveCode Код получателя
 * @param p_ErrMsg Описание ошибки
 * @return Код ошибки
 */
  FUNCTION Init(p_DirectInsert IN NUMBER, p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_ReceiveCode IN VARCHAR2, p_ErrMsg IN OUT NOCOPY VARCHAR2)
    RETURN NUMBER;

/**
 * Инициализация объектов для текущей записи репликации
 * @since RSHB 82
 * @qtest NO
 * @param p_ObjectKind Вид объекта
 * @param p_ObjectName Наименование объекта
 * @param p_CheckObjectCode Проверить код данного вида и источника, что он не вставлен ранее (1-да, 0-нет)
 * @param p_ObjectCode Код объекта
 * @param p_ClientID Идентификатор клиента
 * @param p_ErrMsg Описание ошибки
 * @return Код ошибки
 */
  FUNCTION InitRec(p_ObjectKind IN NUMBER, p_ObjectName IN VARCHAR2, p_CheckObjectCode IN NUMBER, p_ObjectCode IN VARCHAR2, p_ClientID IN NUMBER, p_ErrMsg IN OUT NOCOPY VARCHAR2)
    RETURN NUMBER;

/**
 * Добавление нового параметра к записи репликации
 * @since RSHB 82
 * @qtest NO
 * @param p_Name Наименование параметра
 * @param p_Val Значение параметра
 */
  PROCEDURE SetParmByName(p_Name IN VARCHAR2, p_Val IN VARCHAR2);

/**
 * Перенос добавленных объектов в накопители (используется только при DIRECT_INSERT_NO)
 * @since RSHB 82
 * @qtest NO
 */
  PROCEDURE DoInitRec;

/**
 * Получение описания крайней ошибки
 * @since RSHB 82
 * @qtest NO
 * @return p_Val Описание ошибки
 */
  FUNCTION GetLastError RETURN VARCHAR2;

/**
 * Сохранение накопленных объектов записей репликаций
 * @since RSHB 82
 * @qtest NO
 * @param p_ErrMsg Описание ошибки
 * @return Код ошибки
 */
  FUNCTION Save(p_ErrMsg IN OUT NOCOPY VARCHAR2) RETURN NUMBER;

/**
 * Сохранение накопленных объектов записей репликаций, при превышении параметра p_BatchSize
 * @since RSHB 82
 * @qtest NO
 * @param p_BatchSize Размер пачки для сохранения
 * @param p_ErrMsg Описание ошибки
 * @return Код ошибки
 */
  FUNCTION SaveBatch(p_BatchSize IN NUMBER, p_ErrMsg IN OUT NOCOPY VARCHAR2) RETURN NUMBER;

/**
 * Получение идентификатора текущего объекта
 * @since RSHB 82
 * @qtest NO
 * @return Идентификатор текущего объекта
 */
  FUNCTION GetObjectID RETURN NUMBER;

END RSI_GT;
/
