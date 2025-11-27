CREATE OR REPLACE package RSP_SPB as /*Спецификация пакета RSP_SPB*/
  /*Тип T_FILEPARAMS - уникальные параметры файла МБ
  */
  type T_FILEPARAMS is RECORD (TradeDate DATE, DOC_DATE DATE, DOC_TIME DATE, doc_type_id VARCHAR2(20), FileHash VARCHAR2(100), OrdNum INTEGER, OrdNumStr VARCHAR2(32));

  const_SENDER_ID CONSTANT VARCHAR2(5) := 'SPBXM';
  const_SENDER_NAME CONSTANT VARCHAR2(32) := 'ПАО "СПБ Биржа"';
  const_RECEIVER_ID CONSTANT VARCHAR2(5) := 'RSXBM';
  
  /*Функция LoadRequisites парсинга файла и заполнения временной таблицы отчета:
    p_FILENAME - имя файла
    p_FileType - тип файла
    P_TEXT - результат парсинга файла
    Возвращает 1 - при успешном выполнении, 0 - при ошибке выполнения.
  */
  function LoadRequisites (p_FILENAME IN VARCHAR2, p_FileType IN VARCHAR2, p_TEXT out varchar2) RETURN INTEGER;

  /*Функция LoadSPB03 парсинга файла Информация о договорах, заключенных участником торгов за Торговый день, и заполнения временной таблицы:
    P_TEXT - результат парсинга файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadSPB03 (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadSPB21 парсинга файла отчета об итогах торгов за торговый день, и заполнения временной таблицы:
    P_TEXT - результат парсинга файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadSPB21 (P_TEXT out varchar2) return INTEGER;
  
  /*Функция LoadMFB06C парсинга файла клиентских сделок, включенных в клиринг, и заполнения временной таблицы:
    P_TEXT - результат парсинга файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadMFB06C (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadMFB13 парсинга файла отчета об итоговых нетто-обязательствах/нетто-требованиях, и заполнения временной таблицы:
    P_TEXT - результат парсинга файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadMFB13 (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadMFB98 парсинга файла отчета о глобальных операциях и об обязательствах/требованиях по передаче дохода, и заполнения временной таблицы:
    P_TEXT - результат парсинга файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadMFB98 (P_TEXT out varchar2) return INTEGER;

  /*Функция LoadMFB99 парсинга файла отчета об обеспечении, и заполнения временной таблицы:
    P_TEXT - результат парсинга файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadMFB99 (P_TEXT out varchar2) return INTEGER;
  
  /*Функция LoadORDERS парсинга файла Выписки из реестра заявок:
    P_TEXT - результат парсинга файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function LoadORDERS (P_TEXT out varchar2) return INTEGER;

  /*Функция Load_SPBFile парсинга файла СПБ и заполнения временных таблиц:
    p_FILENAME - наименование файла
    p_FileType - тип файла
    p_Msg - результат парсинга файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function Load_SPBFile (p_FILENAME in varchar2, p_FileType in varchar2, p_Msg out varchar2) return INTEGER;

  /*Функция Insert_Requisites заполнения системной таблицы отчета:
    p_FILENAME - наименование файла
    p_IDProcLog - ID записи процессинга
    p_IDNFORM - ID формы отчета
    Возвращает ID записи отчета.
  */
  function Insert_Requisites (p_FILENAME in varchar2, p_IDProcLog in integer, p_IDNFORM in integer) RETURN INTEGER;

  /*Функция Insert_SPB03 заполнения системной таблицы Информация о договорах:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_SPB03 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_SPB21 заполнения системной таблицы отчета об итогах торгов за торговый день:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_SPB21 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER ;
  
  /*Функция Insert_MFB06C заполнения системной таблицы сделок:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
   function Insert_MFB06C (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_MFB13 заполнения системной таблицы отчета об итоговых нетто-обязательствах/нетто-требованиях:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
   function Insert_MFB13 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_MFB98 заполнения системной таблицы отчета о глобальных операциях и об обязательствах/требованиях по передаче дохода:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
   function Insert_MFB98 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_MFB99 заполнения системной таблицы отчета об обеспечении:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
   function Insert_MFB99 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_ORDERS заполнения системной таблицы заявок:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
   function Insert_ORDERS (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_TableSPB заполнения системных таблиц:
    p_FILENAME - наименование файла
    p_FileType - тип файла
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_TableSPB (p_FILENAME in varchar2, p_FileType in varchar2, p_IDProcLog in integer) return INTEGER;

  /*Функция Get_ParamFile получения параметров файла:
    Возвращает RECORD с параметрами файла.
  */
  function Get_ParamFile return T_FILEPARAMS;

  /*Функция Get_ParamFile_Txt получения параметров файла:
    p_FILENAME - наименование файла
    Возвращает RECORD с параметрами файла.
  */
  function Get_ParamFile_Txt (p_FILENAME IN VARCHAR2) return T_FILEPARAMS;

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

  /*Функция Load_MB - загрузка файлов МБ, вызывается из макроса mb_load.mac:
    p_FILENAME - имя файла МБ
    p_Content - содержимое файла МБ
    Возвращает результат обработки:
     0 - успешная загрузка
     1 - повторная загрузка файла
    -1 - ошибка загрузки файла
  */
  function Load_SPB (p_FILENAME in varchar2, p_Content in clob) return INTEGER;
end RSP_SPB;
/