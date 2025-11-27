CREATE OR REPLACE PACKAGE BODY RSI_GT
IS
  ACTION_CREATE         CONSTANT NUMBER(5) := 1; --Действие: 'Создать'
  STATE_READY_TO_HANDLE CONSTANT NUMBER(5) := 2; --Статус ЗР: 'Готов к обработке'

  TYPE GTKOPRMMAP_T IS TABLE OF DGTKOPRM_DBT%ROWTYPE INDEX BY DGTKOPRM_DBT.T_NAME%TYPE;
  TYPE OKGTKOPRMMAP_T IS TABLE OF GTKOPRMMAP_T INDEX BY PLS_INTEGER;
  TYPE GTRECPRMARR_T IS TABLE OF DGTRECPRM_DBT%ROWTYPE;

  TYPE GTOBJECTARR_T IS TABLE OF DGTOBJECT_DBT%ROWTYPE;
  TYPE GTCODEARR_T IS TABLE OF DGTCODE_DBT%ROWTYPE;
  TYPE GTRECORDARR_T IS TABLE OF DGTRECORD_DBT%ROWTYPE;
  TYPE GTSNCRECARR_T IS TABLE OF DGTSNCREC_DBT%ROWTYPE;

  g_DirectInsert   NUMBER(5);                       --Способ добавления объектов в накопители (сразу/после вызова DoInitRec)
  g_SeanceID       DGTSEANCE_DBT.T_SEANCEID%TYPE;   --Идентификатор сеанса
  g_ObjectKind     DGTOBJECT_DBT.T_OBJECTKIND%TYPE; --Вид объекта
  g_ObjectID       DGTCODE_DBT.T_OBJECTID%TYPE;     --Идентификатор текущего объекта
  g_SourceAppID    DGTAPP_DBT.T_APPLICATIONID%TYPE; --Идентификатор источника
  g_ReceiveAppID   DGTAPP_DBT.T_APPLICATIONID%TYPE; --Идентификатор получателя

  g_GtKoPrmMap     GTKOPRMMAP_T;   --Типы параметров записи репликации
  g_OKGtKoPrmMap   OKGTKOPRMMAP_T; --Типы параметров записи репликации в зависимости от вида объекта
  g_GtRecPrmArr    GTRECPRMARR_T;  --Накопитель для параметров ЗР
  g_GtRecPrmArrTmp GTRECPRMARR_T;  --Накопитель для параметров ЗР при использовании DIRECT_INSERT_NO
  g_LastErr        VARCHAR2(1000) := CHR(1); --Описание ошибки

  g_GtObjectArr    GTOBJECTARR_T; --Накопитель объектов репликации
  g_GtCodeArr      GTCODEARR_T;   --Накопитель идентификаторов объектов
  g_GtRecordArr    GTRECORDARR_T; --Накопитель записей репликаций
  g_GtSncRecArr    GTSNCRECARR_T; --Накопитель записей сеанса репликаций

  g_GtObject       DGTOBJECT_DBT%ROWTYPE; --Текущий объект репликации
  g_GtCode         DGTCODE_DBT%ROWTYPE;   --Текущий идентификатор репликации
  g_GtRecord       DGTRECORD_DBT%ROWTYPE; --Текущая запись репликации
  g_GtSncRec       DGTSNCREC_DBT%ROWTYPE; --Текущая запись сеанса репликации
  g_GtRecPrm       DGTRECPRM_DBT%ROWTYPE; --Текущие параметры записи репликации


  --Описание рабочего сеанса
  FUNCTION Descr RETURN VARCHAR2
  IS
  BEGIN
    RETURN 'SeanceID=' || TO_CHAR(g_SeanceID) || ', ObjectKind=' || TO_CHAR(g_ObjectKind) || ', AppID=' || TO_CHAR(g_SourceAppID) || ', SessionID=' || TO_CHAR(USERENV('sessionid')) || ', SysDate=' || TO_CHAR(SYSDATE);
  END;

  --Инициализировать строку объекта репликации
  PROCEDURE InitRowGtObject
  IS
  BEGIN
    g_GtObject.t_ObjectID   := DGTOBJECT_DBT_SEQ.NEXTVAL;
    g_GtObject.t_ObjectKind := g_ObjectKind;
  --g_GtObject.t_Name       := CHR(1);
    g_GtObject.t_SysDate    := TRUNC(CURRENT_DATE);
    g_GtObject.t_SysTime    := CURRENT_DATE;
  END;

  --Инициализировать строку идентификаторов объекта
  PROCEDURE InitRowGtCode
  IS
  BEGIN
    g_GtCode.t_CodeID        := 0;
    g_GtCode.t_ObjectID      := g_GtObject.t_ObjectID;
    g_GtCode.t_ApplicationID := g_SourceAppID;
  --g_GtCode.t_OjectCode     := CHR(1);
    g_GtCode.t_ObjectKind    := g_ObjectKind;
  END;

  --Инициализировать строку записи репликации
  PROCEDURE InitRowGtRecord
  IS
  BEGIN
    g_GtRecord.t_RecordID           := DGTRECORD_DBT_SEQ.NEXTVAL;
    g_GtRecord.t_ObjectID           := g_GtObject.t_ObjectID;
    g_GtRecord.t_ApplicationID_From := g_SourceAppID;
    g_GtRecord.t_ApplicationID_To   := g_ReceiveAppID;
    g_GtRecord.t_ActionID           := ACTION_CREATE;
    g_GtRecord.t_StatusID           := STATE_READY_TO_HANDLE;
    g_GtRecord.t_SysDate            := TRUNC(CURRENT_DATE);
    g_GtRecord.t_SysTime            := CURRENT_DATE;
  --g_GtRecord.t_ClientID           := 0;
  END;

  --Инициализировать строку записи репликации сеанса
  PROCEDURE InitRowGtSncRec
  IS
  BEGIN
    g_GtSncRec.t_ID       := 0;
    g_GtSncRec.t_SeanceID := g_SeanceID;
    g_GtSncRec.t_ObjectID := g_GtObject.t_ObjectID;
    g_GtSncRec.t_RecordID := g_GtRecord.t_RecordID;
    g_GtSncRec.t_Text     := CHR(1);
    g_GtSncRec.t_Date     := TRUNC(SYSDATE);
    g_GtSncRec.t_Time     := SYSDATE;
    g_GtSncRec.t_Issue    := 0;
    g_GtSncRec.t_Comment  := CHR(1);
  END;

  --Инициализировать строку параметров ЗР
  PROCEDURE InitRowGtRecPrm
  IS
  BEGIN
    g_GtRecPrm.t_ID               := 0;
    g_GtRecPrm.t_RecordID         := g_GtRecord.t_RecordID;
    g_GtRecPrm.t_koprmID          := 0;
    g_GtRecPrm.t_RefApplicationID := 0;
    g_GtRecPrm.t_IntVal           := 0;
    g_GtRecPrm.t_MoneyVal         := 0;
    g_GtRecPrm.t_DoubleVal        := 0;
    g_GtRecPrm.t_StringVal        := CHR(1);
    g_GtRecPrm.t_DateVal          := ZeroDate;
    g_GtRecPrm.t_TimeVal          := ZeroTime;
  END;

  --Записать в строку параметров значение
  FUNCTION SetGtRecPrm(p_Name IN VARCHAR2, p_Val IN VARCHAR2)
    RETURN NUMBER
  IS
    v_stat    NUMBER(5) := 0;
    v_DateTmp DATE;
  BEGIN
    IF g_GtKoprmMap.EXISTS(p_Name) THEN
      g_GtRecPrm.t_KoPrmID := g_GtKoprmMap(p_Name).t_KoPrmID;

      IF g_GtKoprmMap(p_Name).t_TypeID = CNST.V_INTEGER THEN
        g_GtRecPrm.t_IntVal := NVL(TO_NUMBER(p_Val), 0);
      ELSIF g_GtKoprmMap(p_Name).t_TypeID IN (CNST.V_DOUBLE, CNST.V_DOUBLEL) THEN
        g_GtRecPrm.t_DoubleVal := NVL(TO_NUMBER(p_Val), 0);
      ELSIF g_GtKoprmMap(p_Name).t_TypeID IN (CNST.V_MONEY, CNST.V_MONEYL) THEN
        g_GtRecPrm.t_MoneyVal := NVL(TO_NUMBER(p_Val), 0);
      ELSIF g_GtKoprmMap(p_Name).t_TypeID = CNST.V_STRING THEN
        g_GtRecPrm.t_StringVal := NVL(p_Val, CHR(1));
      ELSIF g_GtKoprmMap(p_Name).t_TypeID = CNST.V_DATE THEN
        g_GtRecPrm.t_DateVal := NVL(TRUNC(TO_DATE(p_Val)), ZeroDate);
      ELSIF g_GtKoprmMap(p_Name).t_TypeID = CNST.V_TIME THEN
        v_DateTmp := NVL(TO_DATE(p_Val), ZeroTime);
        g_GtRecPrm.t_TimeVal := TO_DATE('01.01.0001 ' || TO_CHAR(v_DateTmp, 'HH24:MI:SS'), 'DD.MM.YYYY HH24:MI:SS');
      ELSE
        v_stat := 1;
        g_LastErr := 'Не определен тип параметра ' || p_Name || ' с TypeID:' || g_GtKoprmMap(p_Name).t_TypeID || ', ф-я SetGtRecPrm, ' || Descr;
      END IF;
    ELSE
      v_stat := 1;
      g_LastErr := 'Не найден параметр с именем:' || p_Name || ', ф-я SetGtRecPrm, ' || Descr;
    END IF;

    RETURN v_stat;
  END;

  --Получить параметры объекта вида
  PROCEDURE SetGtKoPrmMap(p_ObjectKind IN NUMBER)
  IS
  BEGIN
    FOR cData IN (SELECT KPRM.*
                    FROM DGTKOPRM_DBT KPRM
                   WHERE KPRM.T_OBJECTKIND = p_ObjectKind
                 )
    LOOP
      g_GtKoPrmMap(cData.T_NAME) := cData;
    END LOOP;
  END;

  --Добавление нового параметра к записи репликации
  PROCEDURE SetParmByName(p_Name IN VARCHAR2, p_Val IN VARCHAR2)
  IS
    v_stat   NUMBER(5)      := 0;
  BEGIN
    --инициализировать строку параметров
    InitRowGtRecPrm;

    --в зависимости от названия параметра, закрепить значение к соответствующему типу
    v_stat := SetGtRecPrm(p_Name, p_Val);

    --добавить строку параметров в коллекцию для вставки
    IF v_stat = 0 THEN
      IF g_DirectInsert = DIRECT_INSERT_YES THEN
        g_GtRecPrmArr.Extend();
        g_GtRecPrmArr(g_GtRecPrmArr.last) := g_GtRecPrm;
      ELSE
        g_GtRecPrmArrTmp.Extend();
        g_GtRecPrmArrTmp(g_GtRecPrmArrTmp.last) := g_GtRecPrm;
      END IF;
    END IF;

  EXCEPTION WHEN OTHERS THEN
    g_LastErr := 'Произошла непредвиденная ошибка при добавлении параметра:' || p_Name || ', ф-я SetGtRecPrm, ' || Descr;
  END;

  --Получить описание крайней ошибки
  FUNCTION GetLastError RETURN VARCHAR2
  IS
  BEGIN
    RETURN g_LastErr;
  END;

  FUNCTION GetAppIDByCode(p_Code IN VARCHAR2) RETURN NUMBER
  IS
    v_ApplicationID NUMBER(10);
  BEGIN
    SELECT GTAPP.T_APPLICATIONID INTO v_ApplicationID
      FROM DGTAPP_DBT GTAPP
     WHERE GTAPP.T_CODE = p_Code;

    RETURN v_ApplicationID;
  EXCEPTION WHEN NO_DATA_FOUND THEN RETURN 0;
  END;

  FUNCTION GetObjectIDFromGtCode(p_ObjectKind IN NUMBER, p_ApplicationID IN NUMBER, p_ObjectCode IN VARCHAR2) RETURN NUMBER
  IS
    v_ObjectID DGTCODE_DBT.T_CODEID%TYPE;
  BEGIN
    SELECT GTCODE.T_OBJECTID INTO v_ObjectID
      FROM DGTCODE_DBT GTCODE
     WHERE GTCODE.T_OBJECTKIND = p_ObjectKind
       AND GTCODE.T_APPLICATIONID = p_ApplicationID
       AND GTCODE.T_OBJECTCODE = p_ObjectCode;

    RETURN v_ObjectID;
  EXCEPTION WHEN NO_DATA_FOUND THEN RETURN 0;
  END;

  --Общая обязательная инициализация для работы вставки объектов
  FUNCTION Init(p_DirectInsert IN NUMBER, p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_ReceiveCode IN VARCHAR2, p_ErrMsg IN OUT NOCOPY VARCHAR2)
    RETURN NUMBER
  IS
    v_stat NUMBER(5) := 0;
  BEGIN
    p_ErrMsg  := CHR(1);
    g_LastErr := CHR(1);

  --SELECT VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER = 'NLS_DATE_FORMAT';

    --установка формата преобразования даты в строку в текущем сеансе
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT=''DD.MM.YYYY HH24:MI:SS''';

    g_DirectInsert := p_DirectInsert;
    --идентификатор сеанса
    g_SeanceID   := p_SeanceID;
    g_ObjectKind := 0;
    g_ObjectID   := 0;

    g_SourceAppID := GetAppIDByCode(p_SourceCode);
    IF g_SourceAppID = 0 THEN
      v_stat := 1;
      p_ErrMsg := 'Не удалось установить источник по коду: ' || p_SourceCode || ', ф-я RgInit,' || Descr;
    END IF;

    g_ReceiveAppID := GetAppIDByCode(p_ReceiveCode);
    IF g_ReceiveAppID = 0 THEN
      v_stat := 1;
      p_ErrMsg := 'Не удалось установить получателя по коду: ' || p_ReceiveCode || ', ф-я RgInit, ' || Descr;
    END IF;

    --инициализация накопителей
    g_GtObjectArr    := GTOBJECTARR_T();
    g_GtCodeArr      := GTCODEARR_T();
    g_GtRecordArr    := GTRECORDARR_T();
    g_GtSncRecArr    := GTSNCRECARR_T();
    g_GtRecPrmArr    := GTRECPRMARR_T();
    g_GtRecPrmArrTmp := GTRECPRMARR_T();

    RETURN v_stat;
  EXCEPTION WHEN OTHERS THEN
    BEGIN
      p_ErrMsg := 'Произошла непредвиденная ошибка при вызове RgInit, ' || Descr;
      RETURN 1;
    END;
  END;

  PROCEDURE AddObjectsToBatch
  IS
  BEGIN
    g_GtObjectArr.Extend();
    g_GtObjectArr(g_GtObjectArr.last) := g_GtObject;

    g_GtCodeArr.Extend();
    g_GtCodeArr(g_GtCodeArr.last) := g_GtCode;

    g_GtRecordArr.Extend();
    g_GtRecordArr(g_GtRecordArr.last) := g_GtRecord;

    g_GtSncRecArr.Extend();
    g_GtSncRecArr(g_GtSncRecArr.last) := g_GtSncRec;
  END;

  --Обязательная инициализация объектов для текущей записи репликации
  FUNCTION InitRec(p_ObjectKind IN NUMBER, p_ObjectName IN VARCHAR2, p_CheckObjectCode IN NUMBER, p_ObjectCode IN VARCHAR2, p_ClientID IN NUMBER, p_ErrMsg IN OUT NOCOPY VARCHAR2)
    RETURN NUMBER
  IS
    v_stat NUMBER(5) := 0;
  BEGIN
    g_LastErr := CHR(1);

    --проверить, что запись с таким кодом не вставлена ранее
    IF p_CheckObjectCode = 1 THEN
      g_ObjectID := GetObjectIDFromGtCode(p_ObjectKind, g_SourceAppID, p_ObjectCode);
      IF g_ObjectID > 0 THEN
        v_stat := 1;
        p_ErrMsg := 'Уже существует запись в DGTCODE_DBT с такими параметрами: ObjectCode=' || p_ObjectCode || ', ObjectKind=' || TO_CHAR(p_ObjectKind) || ', AppID=' || TO_CHAR(g_SourceAppID) || ', ф-я RgInitRec, ' || Descr;
      END IF;
    END IF;

    IF v_stat = 0 AND p_ObjectKind <> g_ObjectKind THEN
      --вид объекта
      g_ObjectKind := p_ObjectKind;

      --получим все возможные параметры для вида объекта
      g_GtKoPrmMap.DELETE;
      IF g_OKGtKoPrmMap.EXISTS(p_ObjectKind) THEN
        g_GtKoPrmMap := g_OKGtKoPrmMap(p_ObjectKind);
      ELSE
        SetGtKoPrmMap(p_ObjectKind);
        g_OKGtKoPrmMap(p_ObjectKind) := g_GtKoPrmMap;
      END IF;

      IF g_GtKoPrmMap.COUNT = 0 THEN
        v_stat := 1;
        p_ErrMsg := 'Не удалось получить параметры объекта вида: ' || TO_CHAR(p_ObjectKind) || ', ф-я RgInit, ' || Descr;
      END IF;
    END IF;

    IF v_stat = 0 THEN
      --объект репликации
      InitRowGtObject;
      g_GtObject.t_Name := NVL(p_ObjectName, CHR(1));
      g_ObjectID := g_GtObject.t_ObjectID;

      --идентификатор объекта
      InitRowGtCode;
      g_GtCode.t_ObjectCode := NVL(p_ObjectCode, CHR(1));

      --запись репликации
      InitRowGtRecord;
      g_GtRecord.t_ClientID := NVL(p_ClientID, 0);

      --запись сеанса репликации
      InitRowGtSncRec;

      IF g_DirectInsert = DIRECT_INSERT_YES THEN
        AddObjectsToBatch;
      ELSE
        g_GtRecPrmArrTmp.DELETE;
      END IF;
    END IF;

    RETURN v_stat;
  EXCEPTION WHEN OTHERS THEN
    BEGIN
      p_ErrMsg := 'Произошла непредвиденная ошибка при инициализации объектов ЗР с ObjectCode: ' || p_ObjectCode || ', RgInitRec, ' || Descr;
      RETURN 1;
    END;
  END;

  --Перенос добавленных объектов в накопители (используется только при DIRECT_INSERT_NO)
  PROCEDURE DoInitRec
  IS
  BEGIN
    AddObjectsToBatch;

    FOR i IN g_GtRecPrmArrTmp.FIRST .. g_GtRecPrmArrTmp.LAST
    LOOP
      g_GtRecPrmArr.Extend();
      g_GtRecPrmArr(g_GtRecPrmArr.last) := g_GtRecPrmArrTmp(i);
    END LOOP;
  END;

  --Сохранение объектов записей репликаций
  FUNCTION Save(p_ErrMsg IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_stat NUMBER(5) := 0;
  BEGIN
    p_ErrMsg := CHR(1);

    --вставка объектов репликаций
    IF g_GtObjectArr.COUNT > 0 THEN
      BEGIN
        FORALL i IN g_GtObjectArr.FIRST .. g_GtObjectArr.LAST
          INSERT INTO DGTOBJECT_DBT VALUES g_GtObjectArr(i);
      EXCEPTION WHEN OTHERS THEN
        v_stat := 1;
        p_ErrMsg := 'Произошла непредвиденная ошибка при массовой вставке записей в DGTOBJECT_DBT, ф-я RgSave,' || Descr;
      END;
      g_GtObjectArr.DELETE;
    END IF;

    --вставка идентификаторов объектов
    IF g_GtCodeArr.COUNT > 0 THEN
      IF v_stat = 0 THEN
        BEGIN
          FORALL i IN g_GtCodeArr.FIRST .. g_GtCodeArr.LAST SAVE EXCEPTIONS
            INSERT INTO DGTCODE_DBT VALUES g_GtCodeArr(i);
        EXCEPTION WHEN OTHERS THEN
          v_stat := 1;
          p_ErrMsg := 'Произошла непредвиденная ошибка при массовой вставке записей. Дублирование уникальных кодов записей репликации в DGTCODE_DBT: ';
          FOR i IN 1 .. LEAST(SQL%BULK_EXCEPTIONS.COUNT, 10) LOOP
            p_ErrMsg := p_ErrMsg || (CASE WHEN i = 1 THEN '' ELSE ', ' END) || '"' || g_GtCodeArr(SQL%BULK_EXCEPTIONS(i).ERROR_INDEX).T_OBJECTCODE || '"';
          END LOOP;
          p_ErrMsg := p_ErrMsg || '.' || Descr;
        END;
      END IF;
      g_GtCodeArr.DELETE;
    END IF;

    --вставка записей репликаций
    IF g_GtRecordArr.COUNT > 0 THEN
      IF v_stat = 0 THEN
        BEGIN
          FORALL i IN g_GtRecordArr.FIRST .. g_GtRecordArr.LAST
            INSERT INTO DGTRECORD_DBT VALUES g_GtRecordArr(i);
        EXCEPTION WHEN OTHERS THEN
          v_stat := 1;
          p_ErrMsg := 'Произошла непредвиденная ошибка при массовой вставке записей в DGTRECORD_DBT, ф-я RgSave, ' || Descr;
        END;
      END IF;
      g_GtRecordArr.DELETE;
    END IF;

    --вставка записей сеанса репликаций
    IF g_GtSncRecArr.COUNT > 0 THEN
      IF v_stat = 0 THEN
        BEGIN
          FORALL i IN g_GtSncRecArr.FIRST .. g_GtSncRecArr.LAST
            INSERT INTO DGTSNCREC_DBT VALUES g_GtSncRecArr(i);
        EXCEPTION WHEN OTHERS THEN
          v_stat := 1;
          p_ErrMsg := 'Произошла непредвиденная ошибка при массовой вставке записей в DGTSNCREC_DBT, ф-я RgSave, ' || Descr;
        END;
      END IF;
      g_GtSncRecArr.DELETE;
    END IF;

    --вставка параметров записей репликаций
    IF g_GtRecPrmArr.COUNT > 0 THEN
      IF v_stat = 0 THEN
        BEGIN
          FORALL i IN g_GtRecPrmArr.FIRST .. g_GtRecPrmArr.LAST
            INSERT INTO DGTRECPRM_DBT VALUES g_GtRecPrmArr(i);
        EXCEPTION WHEN OTHERS THEN
          v_stat := 1;
          p_ErrMsg := 'Произошла непредвиденная ошибка при массовой вставке записей в DGTRECPRM_DBT, ф-я RgSave, ' || Descr;
        END;
      END IF;
      g_GtRecPrmArr.DELETE;
    END IF;

    IF v_stat = 0 THEN
      COMMIT;
    ELSE
      ROLLBACK;
    END IF;

    RETURN v_stat;
  END;

  --Сохранение объектов записей репликаций по мере накопления
  FUNCTION SaveBatch(p_BatchSize IN NUMBER, p_ErrMsg IN OUT NOCOPY VARCHAR2) RETURN NUMBER
  IS
  BEGIN
    IF g_GtObjectArr.COUNT > p_BatchSize THEN
      RETURN Save(p_ErrMsg);
    ELSE
      RETURN 0;
    END IF;
  END;

  --Получение идентификатора объекта
  FUNCTION GetObjectID RETURN NUMBER
  IS
  BEGIN
    RETURN g_ObjectID;
  END;

END RSI_GT;
/
