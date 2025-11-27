CREATE OR REPLACE package RSP_FMARKET as
  /*Тип T_FILEPARAMS - уникальные параметры файла МБ
  */
  type T_FILEPARAMS is RECORD (TradeDate DATE, DOC_DATE DATE, DOC_TIME DATE, doc_type_id VARCHAR2(20), FileHash VARCHAR2(100), OrdNum INTEGER, OrdNumStr VARCHAR2(32));

  const_SENDER_ID CONSTANT VARCHAR2(16) := 'MM0000100000';
  const_SENDER_NAME CONSTANT VARCHAR2(8) := 'МБ';
  const_RECEIVER_ID CONSTANT VARCHAR2(16) := 'MC0134700000';

  /*Функция LoadRequisites парсинга файла и заполнения временной таблицы отчета:
    p_FileType - тип отчета
    P_TEXT - результат парсинга файла
    Возвращает 1 - при успешном выполнении, 0 - при ошибке выполнения.
  */
  function LoadRequisites (p_FileType IN VARCHAR2, p_TEXT out varchar2) RETURN INTEGER;

  /*Функция Loadf04 парсинга файла операций с фьючерсами и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function Loadf04 (P_TEXT out varchar2) return INTEGER;

  /*Функция Loado04 парсинга файла Информации об опционных сделках брокерской фирмы (БФ) и ee клиентов и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function Loado04 (P_TEXT out varchar2) return INTEGER;

  /*Функция Loadfpos парсинга файла дневных изменений позиций по фьючерсам и операций исполнения фьючерсов и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function Loadfpos (P_TEXT out varchar2) return INTEGER;

  /*Функция Loadopos парсинга файла итогов дневных изменений позиций по опционам и операций исполнения опционов и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function Loadopos (P_TEXT out varchar2) return INTEGER;

  /*Функция Loadfordlog парсинга файла заявок-поручений по фьючерсам и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function Loadfordlog (P_TEXT out varchar2) return INTEGER;

  /*Функция Loadoordlog парсинга файла заявок-поручений по опционам и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function Loadoordlog (P_TEXT out varchar2) return INTEGER;

  /*Функция Loadf07 парсинга файла спецификаций стандартных фьючерсных контрактов и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function Loadf07 (P_TEXT out varchar2) return INTEGER;

  /*Функция Loado07 парсинга файла спецификаций стандартных опционных контрактов и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function Loado07 (P_TEXT out varchar2) return INTEGER;

  /*Функция Loadpay парсинга файла отчета об операциях по разделам денежного регистра и депо регистра обеспечения и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function Loadpay (P_TEXT out varchar2) return INTEGER;

  /*Функция Loadmon парсинга файла отчета о денежных средствах в рублях, иностранной валюте и ценных бумагах, 
    являющихся обеспечением об операциях по разделам денежного регистра и депо регистра обеспечения и заполнения временной таблицы:
    P_TEXT - наименование файла
    Возвращает количество вставленных записей во временную таблицу.
  */
  function Loadmon (P_TEXT out varchar2) return INTEGER;

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
    p_Date - дата файла
    Возвращает ID записи отчета МБ.
  */
  function Insert_Requisites (p_FILENAME in varchar2, p_IDProcLog in integer, p_IDNFORM in integer, p_Date in date default null) RETURN INTEGER;

  /*Функция Insert_f04 заполнения системной таблицы операций с фьючерсами:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_f04 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_o04 заполнения системной таблицы Информации об опционных сделках брокерской фирмы (БФ) и ee клиентов:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_o04 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_fpos заполнения системной таблицы дневных изменений позиций по фьючерсам и операций исполнения фьючерсов:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_fpos (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_opos заполнения системной таблицы итогов дневных изменений позиций по опционам и для импорта операций исполнения опционов:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_opos (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_fordlog заполнения системной таблицы заявок-поручений по фьючерсам:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_fordlog (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_oordlog заполнения системной таблицы заявок-поручений по опционам:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_oordlog (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_f07 заполнения системной таблицы спецификаций стандартных фьючерсных контрактов:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_f07 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_o07 заполнения системной таблицы спецификаций стандартных опционных контрактов:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_o07 (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_pay заполнения системной таблицы отчета об операциях по разделам денежного регистра и депо регистра обеспечения:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_pay (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_mon заполнения системной таблицы отчета о денежных средствах в рублях, иностранной валюте и ценных бумагах, 
    являющихся обеспечением об операциях по разделам денежного регистра и депо регистра обеспечения:
    p_IDReg - ID записи отчета
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_mon (p_IDReg in integer, p_IDProcLog in integer) return INTEGER;

  /*Функция Insert_Table заполнения системных таблиц:
    p_FILENAME - наименование файла
    p_FileType - тип файла
    p_IDProcLog - ID записи процессинга
    Возвращает количество сохраненных записей .
  */
  function Insert_Table (p_FILENAME in varchar2, p_FileType in varchar2, p_IDProcLog in integer, p_Date in date default null) return INTEGER;

  /*Функция Get_ParamFile_csv получения параметров файла:
    p_FILENAME - наименование файла
    Возвращает RECORD с параметрами файла.
  */
  function Get_ParamFile_csv (p_FILENAME IN VARCHAR2) return T_FILEPARAMS;

  /*Функция Get_ActualProcessing_Csv поиска актуальной записи процессинга получения ID отчета МБ:
    p_FileParams - параметры файла МБ
    Возвращает ID записи актуального процессинга.
  */
  function Get_ActualProcessing_Csv (p_FileParams IN T_FILEPARAMS) return INTEGER;

  /*Функция Processing_Log_Insert создания Реестра загрузки файлов:
    p_FileParams - уникальные параметры файла МБ
    p_FILENAME - наименование файла
    Возвращает ID созданной записи Реестра.
  */
  function Processing_Log_Insert(p_FileParams IN T_FILEPARAMS, p_FILENAME in varchar2) return INTEGER;

  /*Функция Load_FMARKET - загрузка файлов МБ, вызывается из макроса mb_load.mac:
    p_FILENAME - имя файла МБ
    p_Content - содержимое файла МБ
    Возвращает результат обработки:
     0 - успешная загрузка
     1 - повторная загрузка файла
    -1 - ошибка загрузки файла
  */
  function Load_FMARKET (p_FILENAME in varchar2, p_Content in clob, p_Date in date default null) return INTEGER;
end RSP_FMARKET;
/
