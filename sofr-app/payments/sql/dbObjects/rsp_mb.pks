CREATE OR REPLACE package RSP_MB as
  /*Тип T_FILEPARAMS - уникальные параметры файла МБ
  */
  type T_FILEPARAMS is RECORD (TradeDate DATE, DOC_DATE DATE, DOC_TIME DATE, doc_type_id VARCHAR2(20), FileHash VARCHAR2(100), OrdNum INTEGER, OrdNumStr VARCHAR2(32));

  const_SENDER_ID CONSTANT VARCHAR2(16) := 'MM0000100000';
  const_SENDER_NAME CONSTANT VARCHAR2(8) := 'МБ';
  const_RECEIVER_ID CONSTANT VARCHAR2(16) := 'MC0134700000';

  /*Функция LoadRequisites парсинга файла и заполнения временной таблицы отчета:
    p_FILENAME - имя файла отчета
    p_FileType - тип отчета
    P_TEXT     - результат парсинга файла
    Возвращает 1 - при успешном выполнении, 0 - при ошибке выполнения.
  */
  function LoadRequisites (p_FILENAME IN VARCHAR2, p_FileType IN VARCHAR2, p_TEXT out varchar2) RETURN INTEGER;

  /*Функция LoadSEM02 парсинга файла заявок и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadSEM02 (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadSEM03 парсинга файла сделок и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadSEM03 (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadSEM21 парсинга файла биржевой информации на фондовом рынке (файл котировок),  и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadSEM21 (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadSEM25 парсинга файла "Уведомления о необходимости внесения компенсационного взноса" и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadSEM25 (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadSEM26 парсинга файла "Уведомление об изменении расчетных параметров сделки РЕПО в связи с выплатой купонного дохода
    и/или погашением части основного долга (проведением амортизационной выплаты) по облигациям" и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadSEM26 (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadEQM06 парсинга файла клиринга и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadEQM06 (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadEQM6C парсинга файла Выписки из реестра сделок, принятых на клиринг (по сделкам клиентов) и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadEQM6C (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadEQM13 парсинга файла Отчета об итоговых нетто-обязательствах / нетто-требованиях и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadEQM13 (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadEQM98 парсинга файла Отчета об обязательствах по передаче/требованиях по получению дохода  и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadEQM98 (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadEQM99 парсинга файла Отчета об обеспечении  и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadEQM99 (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadEQM3T парсинга файла реестра внебиржевых сделок и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadEQM3T (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadEQM2T парсинга файла отчета о заявках на сделки и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadEQM2T (P_TEXT out varchar2) return INTEGER;

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

  /*Функция Insert_SEM02 заполнения системной таблицы заявок:
    p_IDReg - ID записи отчета МБ
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_SEM02 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_SEM03 заполнения системной таблицы сделок:
    p_IDReg - ID записи отчета МБ
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_SEM03 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_SEM21 заполнения системной таблицы Файла биржевой информации на фондовом рынке (файл котировок):
    p_IDReg - ID записи отчета МБ
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_SEM21 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_SEM25 заполнения системной таблицы "Уведомления о необходимости внесения компенсационного взноса":
    p_IDReg - ID записи отчета МБ
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_SEM25 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_SEM26 заполнения системной таблицы "Уведомление об изменении расчетных параметров сделки РЕПО в связи с выплатой купонного дохода
    и/или погашением части основного долга (проведением амортизационной выплаты) по облигациям":
    p_IDReg - ID записи отчета МБ
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_SEM26 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_EQM06 заполнения системной таблицы клиринга:
    p_IDReg - ID записи отчета МБ
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_EQM06 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_EQM6C заполнения системной таблицы Выписки из реестра сделок, принятых на клиринг (по сделкам клиентов):
    p_IDReg - ID записи отчета МБ
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_EQM6C (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_EQM13 заполнения системной таблицы Отчета об итоговых нетто-обязательствах / нетто-требованиях:
    p_IDReg - ID записи отчета МБ
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_EQM13 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_EQM98 заполнения системной таблицы Отчета об обязательствах по передаче/требованиях по получению дохода:
    p_IDReg - ID записи отчета МБ
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_EQM98 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_EQM99 заполнения системной таблицы Отчета об обеспечении:
    p_IDReg - ID записи отчета МБ
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_EQM99 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_EQM3T заполнения системной таблицы реестра внебиржевых сделок:
    p_IDReg - ID записи отчета МБ
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_EQM3T (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_EQM2T заполнения системной таблицы отчета о заявках на сделки:
    p_IDReg - ID записи отчета МБ
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_EQM2T (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_Table заполнения системных таблиц:
    p_FILENAME - наименование файла
    p_FileType - тип файла
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_Table (p_FILENAME in varchar2, p_FileType in varchar2, p_IDProcLog in integer, p_Date in date default null) return INTEGER;

  /*Функция Get_ParamFile получения параметров файла:
    Возвращает RECORD с параметрами файла.
  */
  function Get_ParamFile return T_FILEPARAMS;

  /*Функция Check_UniqueFile проверки файла МБ на уникальность:
    p_IDProcAct - ID записи актуального процессинга
    p_FileHash - хэш файла
    p_FileDate - дата файла МБ
    Возвращает результат проверки - True / False.
  */
  function Check_UniqueFile (p_IDProcAct IN INTEGER, p_FileHash IN VARCHAR2, p_FileDate IN DATE) return BOOLEAN;

  /*Функция Get_ActualProcessing поиска актуальной записи процессинга получения ID отчета МБ:
    p_FileParams - параметры файла МБ
    Возвращает ID записи актуального процессинга.
  */
  function Get_ActualProcessing (p_FileParams IN T_FILEPARAMS) return INTEGER;

  /*Функция GetRequisitesID получения ID отчета МБ:
    p_IDProcLog - ID записи Реестра загрузки файла
    Возвращает ID отчета МБ.
  */
  function GetRequisitesID(p_IDProcLog IN INTEGER) return INTEGER;

  /*Функция Processing_Log_Insert создания Реестра загрузки файлов:
    p_FileParams - уникальные параметры файла МБ
    p_FILENAME - наименование файла
    Возвращает ID созданной записи Реестра.
  */
  function Processing_Log_Insert(p_FileParams IN T_FILEPARAMS, p_FILENAME in varchar2) return INTEGER;

  /*Процедура Processing_Log_Update обновления Реестра загрузки файлов:
    p_IDProcLog - ИД записи Реестра загрузки файлов
    p_RowsReaded - количество созданных во временных таблицах записей файла МБ
    p_RowsCreated - количество созданных в системных таблицах записей файла МБ
    p_RRCreated - количество созданных записей рекликации
    p_ResNum - код результата выполнения процесса загрузки файла
    p_ResText - описание результата выполнения процесса загрузки файла
  */
  procedure Processing_Log_Update(p_IDProcLog IN INTEGER, p_RowsReaded IN INTEGER, p_RowsCreated IN INTEGER, p_RRCreated IN INTEGER, p_ResNum IN INTEGER, p_ResText IN VARCHAR2);

  /*Функция LogSession_Insert создания записи сессии заргузки:
    p_USERNICK - имя пользователя
    p_FileName - имя файла загрузки МБ
    p_ErrText - описание ошибки
    Возвращает ID созданной записи.
  */
  function LogSession_Insert(p_USERNICK IN VARCHAR2, p_FileName IN VARCHAR2) return integer;

  /*Процедура LogSession_Update сохранения ошибки работы сессии:
    p_IDSession - ИД сессии загрузки
    p_ErrCode - код ошибки
    p_ErrText - описание ошибки
  */
  procedure LogSession_Update(p_IDSession IN INTEGER, p_ErrCode IN VARCHAR2, p_ErrText IN VARCHAR2);

  /*Процедура LogData_Insert создания шага сессии загрузки LogData_Insert:
    p_IDSession - ИД сессии загрузки
    p_Ext_Num - порядковый номер шага
    p_Rows - количество созданных записей на шаге
    p_Info - описание шага
    p_Duration - время выполнения шага в мсек
  */
  procedure LogData_Insert(p_IDSession IN INTEGER, p_Ext_Num IN INTEGER, p_Rows IN INTEGER, p_Info IN VARCHAR2, p_Duration IN INTEGER);

  /*Функция Load_MB - загрузка файлов фондового рынка МБ, вызывается из макроса mb_load.mac:
    p_FILENAME - имя файла МБ
    p_Content - содержимое файла МБ
    Возвращает результат обработки:
     0 - успешная загрузка
     1 - повторная загрузка файла
    -1 - ошибка загрузки файла
  */
  function Load_MB (p_FILENAME in varchar2, p_Content in clob) return INTEGER;
end RSP_MB;
/
