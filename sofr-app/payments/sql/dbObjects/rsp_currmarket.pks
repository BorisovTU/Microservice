CREATE OR REPLACE package RSP_CURRMARKET as
  /*Тип T_FILEPARAMS - уникальные параметры файла МБ
  */
  type T_FILEPARAMS is RECORD (TradeDate DATE, DOC_DATE DATE, DOC_TIME DATE, doc_type_id VARCHAR2(20), FileHash VARCHAR2(100), OrdNum INTEGER, OrdNumStr VARCHAR2(32));

  /*Функция LoadRequisites парсинга файла и заполнения временной таблицы отчета:
    P_TEXT - результат парсинга файла
    Возвращает 1 - при успешном выполнении, 0 - при ошибке выполнения.
  */
  function LoadRequisites RETURN INTEGER;

  /*Функция LoadCUX23 парсинга файла Выписки из реестра сделок и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadCUX23 return INTEGER;

  /*Функция LoadCCX17 парсинга файла Отчета об обязательствах по ПФИ и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadCCX17 return INTEGER;

  /*Функция LoadCCX10 парсинга файла о комиссионных вознаграждениях и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadCCX10 return INTEGER;

  /*Функция LoadCCX4 парсинга файла Отчета об итоговых нетто-требованиях/итоговых нетто-обязательствах и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadCCX4 return INTEGER;

  /*Функция LoadCCX99 парсинга файла Отчета о движении денежных средств и заполнения временной таблицы:
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadCCX99 return INTEGER;

  /*Функция LoadCUX22 парсинга файла выписки из реестра сделок и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadCUX22 return INTEGER;

  /*Функция Load_File парсинга файла МБ и заполнения временных таблиц:
    p_FILENAME - наименование файла
    p_FileType - тип файла
    p_Msg - результат парсинга файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function Load_File (p_FILENAME in varchar2, p_FileType in varchar2, p_Msg out varchar2) return INTEGER;

  /*Функция Insert_Requisites заполнения системной таблицы отчета:
    p_FILENAME - наименование файла
    p_IDProcLog - ID записи процессинга
    p_IDNFORM - ID формы отчета
    Возвращает ID записи отчета МБ.
  */
  function Insert_Requisites (p_FILENAME in varchar2, p_IDProcLog in integer, p_IDNFORM in integer, p_Date in date default null) RETURN INTEGER;

  /*Функция Insert_CUX23 заполнения системной таблицы Выписок из реестра сделок:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_CUX23 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_CCX17 заполнения системной таблицы Отчета об обязательствах по ПФИ:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_CCX17 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_CCX10 заполнения системной таблицы Отчета о комиссионных вознаграждениях по ПФИ:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_CCX10 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_CCX04 заполнения системной таблицы Отчета по всем валютам после окончания торгов USD:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_CCX04 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_CCX4P заполнения системной таблицы предварительного отчёта по одной или нескольким валютам:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_CCX4P (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_CCX99 заполнения системной таблицы Отчета о движении денежных средств:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_CCX99 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_CUX22 заполнения системной таблицы выписки из реестра сделок:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_CUX22 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_Table заполнения системных таблиц:
    p_FILENAME - наименование файла
    p_FileType - тип файла
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_Table (p_FILENAME in varchar2, p_FileType in varchar2, p_IDProcLog in integer) return INTEGER;

  /*Функция Load_CurrMarket - загрузка файлов МБ, вызывается из макроса mb_load.mac:
    p_FILENAME - имя файла МБ
    p_Content - содержимое файла МБ
    Возвращает результат обработки:
     0 - успешная загрузка
     1 - повторная загрузка файла
    -1 - ошибка загрузки файла
  */
  function Load_CurrMarket (p_FILENAME in varchar2, p_Content in clob) return INTEGER;
end RSP_CURRMARKET;
/