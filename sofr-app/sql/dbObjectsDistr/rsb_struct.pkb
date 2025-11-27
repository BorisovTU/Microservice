CREATE OR REPLACE PACKAGE BODY rsb_struct AS

g_NLS_CHARACTERSET varchar2(128);

 PROCEDURE readStruct (p_structName VARCHAR2) AS
     v_i INTEGER;
   BEGIN
     SELECT 1 INTO v_i FROM fmt_names WHERE t_name = lower(p_structName);
     SELECT ff.t_Name, ff.t_Type, ff.t_Size, ff.t_Offset BULK COLLECT INTO g_fmtStruct_tab
       FROM fmt_fields ff, fmt_names fn
       WHERE fn.t_Name = lower(p_structName)
         AND fn.t_ID = ff.t_fmtID;
     g_fmtStruct_name := lower(p_structName);
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       RAISE_APPLICATION_ERROR(-20000, 'Structure ' || p_structName || ' not found in FMT_NAMES');
     WHEN OTHERS THEN
       RAISE_APPLICATION_ERROR(-20001, 'Error in rsb_struct.readStruct ' || SQLERRM);
   END readStruct;


 FUNCTION getRecordSize (p_structName VARCHAR2) RETURN NUMBER AS
     v_Size INTEGER;
   BEGIN
     SELECT sum(ff.t_Size) INTO v_Size FROM fmt_names fn, fmt_fields ff
       WHERE fn.t_Name = lower(p_structName)
         AND fn.t_ID = ff.t_fmtID;
     IF v_Size = 0 THEN
       RAISE_APPLICATION_ERROR(-20000, 'Structure ' || p_structName || ' not found in FMT_NAMES');
     END IF;
     RETURN v_Size;
   END;


 FUNCTION getStructName RETURN VARCHAR2 as
   BEGIN
     RETURN g_fmtStruct_name;
   END;

 PROCEDURE posInStruct (p_fieldName VARCHAR2) AS
   BEGIN
     FOR i IN 1..g_fmtStruct_tab.last LOOP
       g_fmtStruct_rec := g_fmtStruct_tab(i);
       EXIT WHEN lower(g_fmtStruct_tab(i).t_Name) = lower(p_fieldName);
       IF i = g_fmtStruct_tab.last THEN
         RAISE_APPLICATION_ERROR(-20000, 'Field ' || p_fieldName || ' not found in structure');
       END IF;
     END LOOP;
   EXCEPTION
     WHEN value_error THEN
       RAISE_APPLICATION_ERROR(-20001, 'Error in RSB_STRUCT.posInStruct: execute RSB_STRUCT.readStruct at first!');
   END posInStruct;

 FUNCTION bitNot (operator IN NUMBER) RETURN NUMBER IS
   BEGIN
     IF operator >= 0 THEN
       RETURN(4294967295 - operator);
     ELSE
       RETURN(-1 - operator);
     END IF;
   END bitNot;


 FUNCTION getInt (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,g_fmtStruct_rec.t_Size,g_fmtStruct_rec.t_Offset + p_RecOffset + 1), utl_raw.little_endian);
   END getInt;


 FUNCTION getInt (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,g_fmtStruct_rec.t_Size), utl_raw.little_endian);
   END getInt;

 FUNCTION getInt (p_Value BLOB) RETURN NUMBER AS
   BEGIN
     RETURN utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value, 0, 4), utl_raw.little_endian);
   END getInt;


 FUNCTION getLong (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,g_fmtStruct_rec.t_Size,g_fmtStruct_rec.t_Offset + p_RecOffset + 1), utl_raw.little_endian);
   END getLong;


 FUNCTION getLong (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,g_fmtStruct_rec.t_Size), utl_raw.little_endian);
   END getLong;

 FUNCTION getLong (p_Value BLOB) RETURN NUMBER AS
   BEGIN
     RETURN utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value,0,8), utl_raw.little_endian);
   END getLong;

 FUNCTION getDouble (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER AS
     v_raw    RAW(8);
     v_Expo   NUMBER;
     v_Manti  NUMBER;
     v_res    NUMBER;
     v_sign   BOOLEAN := FALSE;
     v_pwbase INT     := -1075;
  BEGIN
     posInStruct(p_fieldName);
     v_raw := utl_raw.reverse(dbms_lob.substr(p_Value, 8, g_fmtStruct_rec.t_Offset + p_RecOffset + 1));
     v_Expo := to_number(substr(v_raw,1,3),'FMXXXXXXXXXXXXXXXX');
     v_Manti := to_number('1'||substr(v_raw,4,13),'FMXXXXXXXXXXXXXXXX');

     IF v_expo >= power(2, 11) THEN
       v_sign := TRUE;
       v_Expo := v_Expo - power(2, 11);
     END IF;
     v_Expo := v_Expo + v_pwbase;
     v_res := power(2,v_Expo);
     v_res := v_Manti * v_res;
     IF v_sign THEN
       v_res := -v_res;
     END IF;
     RETURN v_res;
   END getDouble;


 FUNCTION getDouble (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER AS
     v_raw    RAW(8);
     v_Expo   NUMBER;
     v_Manti  NUMBER;
     v_res    NUMBER;
     v_sign   BOOLEAN := FALSE;
     v_pwbase INT     := -1075;
  BEGIN
     posInStruct(p_fieldName);
     v_raw := utl_raw.reverse(utl_raw.substr(p_Value, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, 8));
     v_Expo := to_number(substr(v_raw,1,3),'FMXXXXXXXXXXXXXXXX');
     v_Manti := to_number('1'||substr(v_raw,4,13),'FMXXXXXXXXXXXXXXXX');
     IF v_expo >= power(2, 11) THEN
       v_sign := TRUE;
       v_Expo := v_Expo - power(2, 11);
     END IF;
     v_Expo := v_Expo + v_pwbase;
     v_res := power(2,v_Expo);
     v_res := v_Manti * v_res;
     IF v_sign THEN
       v_res := -v_res;
     END IF;
     RETURN v_res;
   END getDouble;

 FUNCTION getDouble (p_Value BLOB) RETURN NUMBER AS
     v_raw    RAW(8);
     v_Expo   NUMBER;
     v_Manti  NUMBER;
     v_res    NUMBER;
     v_sign   BOOLEAN := FALSE;
     v_pwbase INT     := -1075;
  BEGIN
     v_raw := utl_raw.reverse(utl_raw.substr(p_Value, 0, 8));
     v_Expo := to_number(substr(v_raw,1,3),'FMXXXXXXXXXXXXXXXX');
     v_Manti := to_number('1'||substr(v_raw,4,13),'FMXXXXXXXXXXXXXXXX');

     IF v_expo >= power(2, 11) THEN
       v_sign := TRUE;
       v_Expo := v_Expo - power(2, 11);
     END IF;
     v_Expo := v_Expo + v_pwbase;
     v_res := power(2,v_Expo);
     v_res := v_Manti * v_res;
     IF v_sign THEN
       v_res := -v_res;
     END IF;
     RETURN v_res;
   END getDouble;

 FUNCTION getMoneyPriv (p_Expo NUMBER, p_LeaDi NUMBER, p_TraDi0 NUMBER, p_TraDi1  NUMBER) RETURN NUMBER AS
     v_Expo    NUMBER;
     v_LeaDi   NUMBER;
     v_TraDi0  NUMBER;
     v_TraDi1  NUMBER;
   BEGIN
     v_Expo   := p_Expo;
     v_LeaDi  := p_LeaDi;
     v_TraDi0 := p_TraDi0;
     v_TraDi1 := p_TraDi1;
     IF (v_LeaDi < 0) THEN
       v_LeaDi  := bitNot(v_LeaDi);
       v_TraDi0 := bitNot(v_TraDi0);
       v_TraDi1 := bitNot(v_TraDi1);

       IF (v_LeaDi < 0 ) THEN
         v_LeaDi := power(2,32) + v_LeaDi;
       END IF;
       IF (v_TraDi0 < 0 ) THEN
         v_TraDi0 := power(2,32) + v_TraDi0;
       END IF;
       IF (v_TraDi1 < 0 ) THEN
         v_TraDi1 := power(2,32) + v_TraDi1;
       END IF;

       v_TraDi1 := v_TraDi1 + 1;
       IF (v_TraDi1 = power(2,32)) THEN
         v_TraDi1 := 0;
         v_TraDi0 := v_TraDi0 + 1;
         IF (v_TraDi0 = power(2,32)) THEN
           v_TraDi0 := 0;
           v_LeaDi := v_LeaDi + 1;
         END IF;
       END IF;
       RETURN -(v_LeaDi*power(2,64) + v_TraDi0*power(2,32) + v_TraDi1)/power(10,v_Expo);
     END IF;
     IF (v_LeaDi < 0 ) THEN
       v_LeaDi := power(2,32) + v_LeaDi;
     END IF;
     IF (v_TraDi0 < 0 ) THEN
       v_TraDi0 := power(2,32) + v_TraDi0;
     END IF;
     IF (v_TraDi1 < 0 ) THEN
       v_TraDi1 := power(2,32) + v_TraDi1;
     END IF;
     RETURN (v_LeaDi*power(2,64) + v_TraDi0*power(2,32) + v_TraDi1)/power(10,v_Expo);
   END getMoneyPriv;


 FUNCTION getMoney (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER AS
     v_Expo    NUMBER;
     v_LeaDi   NUMBER;
     v_TraDi0  NUMBER;
     v_TraDi1  NUMBER;
   BEGIN
     posInStruct(p_fieldName);
     v_Expo   := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value, 2, g_fmtStruct_rec.t_Offset + p_RecOffset + 1 + 2), utl_raw.little_endian);
     v_LeaDi  := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value, 4, g_fmtStruct_rec.t_Offset + p_RecOffset + 1 + 4), utl_raw.little_endian);
     v_TraDi0 := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value, 4, g_fmtStruct_rec.t_Offset + p_RecOffset + 1 + 8), utl_raw.little_endian);
     v_TraDi1 := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value, 4, g_fmtStruct_rec.t_Offset + p_RecOffset + 1 + 12), utl_raw.little_endian);

     RETURN getMoneyPriv(v_Expo, v_LeaDi, v_TraDi0, v_TraDi1);
   END getMoney;


 FUNCTION getMoney (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER AS
     v_Expo    NUMBER;
     v_LeaDi   NUMBER;
     v_TraDi0  NUMBER;
     v_TraDi1  NUMBER;
   BEGIN
     posInStruct(p_fieldName);
     v_Expo   := utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value, g_fmtStruct_rec.t_Offset + p_RecOffset + 1 + 2, 2), utl_raw.little_endian);
     v_LeaDi  := utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value, g_fmtStruct_rec.t_Offset + p_RecOffset + 1 + 4, 4), utl_raw.little_endian);
     v_TraDi0 := utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value, g_fmtStruct_rec.t_Offset + p_RecOffset + 1 + 8, 4), utl_raw.little_endian);
     v_TraDi1 := utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value, g_fmtStruct_rec.t_Offset + p_RecOffset + 1 + 12, 4), utl_raw.little_endian);

     RETURN getMoneyPriv(v_Expo, v_LeaDi, v_TraDi0, v_TraDi1);
   END getMoney;

 FUNCTION getMoney (p_Value BLOB) RETURN NUMBER AS
     v_Expo    NUMBER;
     v_LeaDi   NUMBER;
     v_TraDi0  NUMBER;
     v_TraDi1  NUMBER;
   BEGIN
     v_Expo   := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value, 2, 1 + 2), utl_raw.little_endian);
     v_LeaDi  := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value, 4, 1 + 4), utl_raw.little_endian);
     v_TraDi0 := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value, 4, 1 + 8), utl_raw.little_endian);
     v_TraDi1 := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value, 4, 1 + 12), utl_raw.little_endian);

     RETURN getMoneyPriv(v_Expo, v_LeaDi, v_TraDi0, v_TraDi1);
   END getMoney;

 FUNCTION getString (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN VARCHAR2 AS
     v_retVal VARCHAR2(32767);
   BEGIN
     posInStruct(p_fieldName);

    if g_NLS_CHARACTERSET is null then 
      SELECT VALUE INTO g_NLS_CHARACTERSET FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET';
    end if;
   IF  g_NLS_CHARACTERSET = 'AL32UTF8' THEN
        v_retVal := RSB_LOCALE.STRING_TO_UNICODE(RSB_LOCALE.RAW_TO_OEM(dbms_lob.substr(p_Value,g_fmtStruct_rec.t_Size,g_fmtStruct_rec.t_Offset + p_RecOffset + 1)));
        RETURN NVL(SUBSTR(v_retVal,1,INSTR(v_retVal,CHR(0))-1),CHR(1));
   END IF;

    v_retVal := utl_raw.cast_to_varchar2(dbms_lob.substr(p_Value,g_fmtStruct_rec.t_Size,g_fmtStruct_rec.t_Offset + p_RecOffset + 1));
    RETURN NVL(SUBSTR(v_retVal,1,INSTR(v_retVal,CHR(0))-1),CHR(1));
   END getString;

 FUNCTION getString (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN VARCHAR2 AS
     v_retVal VARCHAR2(32767);
   BEGIN
     posInStruct(p_fieldName);

    if g_NLS_CHARACTERSET is null then 
      SELECT VALUE INTO g_NLS_CHARACTERSET FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET';
    end if;
   IF  g_NLS_CHARACTERSET = 'AL32UTF8' THEN
        v_retVal := RSB_LOCALE.STRING_TO_UNICODE(RSB_LOCALE.RAW_TO_OEM(utl_raw.substr(p_Value,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,g_fmtStruct_rec.t_Size)));
        RETURN NVL(SUBSTR(v_retVal,1,INSTR(v_retVal,CHR(0))-1),CHR(1));
   END IF;

    v_retVal := utl_raw.cast_to_varchar2(utl_raw.substr(p_Value,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,g_fmtStruct_rec.t_Size));
    RETURN NVL(SUBSTR(v_retVal,1,INSTR(v_retVal,CHR(0))-1),CHR(1));

   END getString;

 FUNCTION getString (p_Value BLOB) RETURN VARCHAR2 AS
     v_retVal VARCHAR2(32767);
   BEGIN
    if g_NLS_CHARACTERSET is null then 
      SELECT VALUE INTO g_NLS_CHARACTERSET FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET';
    end if;
   IF  g_NLS_CHARACTERSET = 'AL32UTF8' THEN
      v_retVal := RSB_LOCALE.STRING_TO_UNICODE(RSB_LOCALE.RAW_TO_OEM(p_Value));
   ELSE
       v_retVal := utl_raw.cast_to_varchar2(p_Value);
   END IF;

    RETURN v_retVal;

   END getString;

 FUNCTION getChar (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN CHAR AS
   BEGIN
    posInStruct(p_fieldName);
    if g_NLS_CHARACTERSET is null then 
      SELECT VALUE INTO g_NLS_CHARACTERSET FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET';
    end if;
    IF  g_NLS_CHARACTERSET = 'AL32UTF8' THEN
         RETURN RSB_LOCALE.STRING_TO_UNICODE(RSB_LOCALE.RAW_TO_OEM(dbms_lob.substr(p_Value,g_fmtStruct_rec.t_Size,g_fmtStruct_rec.t_Offset + p_RecOffset + 1)));
     END IF;

     RETURN utl_raw.cast_to_varchar2(dbms_lob.substr(p_Value,g_fmtStruct_rec.t_Size,g_fmtStruct_rec.t_Offset + p_RecOffset + 1));
   END getChar;

 FUNCTION getChar (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN CHAR AS
   BEGIN
     posInStruct(p_fieldName);
    if g_NLS_CHARACTERSET is null then 
      SELECT VALUE INTO g_NLS_CHARACTERSET FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET';
    end if;
   IF  g_NLS_CHARACTERSET = 'AL32UTF8' THEN
         RETURN RSB_LOCALE.STRING_TO_UNICODE(RSB_LOCALE.RAW_TO_OEM(utl_raw.substr(p_Value,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,g_fmtStruct_rec.t_Size)));
    END IF;

     RETURN utl_raw.cast_to_varchar2(utl_raw.substr(p_Value,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,g_fmtStruct_rec.t_Size));
   END getChar;

 FUNCTION getChar (p_Value BLOB) RETURN CHAR AS
   BEGIN
    if g_NLS_CHARACTERSET is null then 
      SELECT VALUE INTO g_NLS_CHARACTERSET FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET';
    end if;
   IF  g_NLS_CHARACTERSET = 'AL32UTF8' THEN
         RETURN RSB_LOCALE.STRING_TO_UNICODE(RSB_LOCALE.RAW_TO_OEM(p_Value));
     END IF;

     RETURN utl_raw.cast_to_varchar2(p_Value);
   END getChar;

 FUNCTION getDate (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN DATE AS
     v_day    INTEGER;
     v_month  INTEGER;
     v_year   INTEGER;
   BEGIN
     posInStruct(p_fieldName);
     v_day   := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,1,g_fmtStruct_rec.t_Offset + p_RecOffset + 1));
     v_month := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,1,g_fmtStruct_rec.t_Offset + p_RecOffset + 1 + 1));
     v_year  := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,2,g_fmtStruct_rec.t_Offset + p_RecOffset + 1 + 2),2);
     IF (v_day = 0 AND v_month = 0 AND v_year = 0) THEN
       v_day := 1;
       v_month := 1;
       v_year := 1;
     END IF;
     RETURN to_date(v_day || '.' || v_month || '.' || v_year,'DD.MM.YYYY');
   END getDate;


 FUNCTION getDate (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN DATE AS
     v_day    INTEGER;
     v_month  INTEGER;
     v_year   INTEGER;
   BEGIN
     posInStruct(p_fieldName);
     v_day   := utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,1));
     v_month := utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value,g_fmtStruct_rec.t_Offset + p_RecOffset + 1 + 1,1));
     v_year  := utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value,g_fmtStruct_rec.t_Offset + p_RecOffset + 1 + 2,2), utl_raw.little_endian);
     IF (v_day = 0 AND v_month = 0 AND v_year = 0) THEN
       v_day := 1;
       v_month := 1;
       v_year := 1;
     END IF;
     RETURN to_date(v_day || '.' || v_month || '.' || v_year,'DD.MM.YYYY');
   END getDate;

 FUNCTION getDate (p_Value BLOB) RETURN DATE AS
     v_day    INTEGER;
     v_month  INTEGER;
     v_year   INTEGER;
   BEGIN
     v_day   := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,1,1));
     v_month := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,1,1 + 1));
     v_year  := utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,2,1 + 2), utl_raw.little_endian);
     IF (v_day = 0 AND v_month = 0 AND v_year = 0) THEN
       v_day := 1;
       v_month := 1;
       v_year := 1;
     END IF;
     RETURN to_date(v_day || '.' || v_month || '.' || v_year,'DD.MM.YYYY');
   END getDate;

 FUNCTION getTime (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN DATE AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN to_date (
            '01-01-0001'|| ':'
            || utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,1,g_fmtStruct_rec.t_Offset + p_RecOffset + 2 + 2)) || ':'
            || utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,1,g_fmtStruct_rec.t_Offset + p_RecOffset + 2 + 1)) || ':'
            || utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,1,g_fmtStruct_rec.t_Offset + p_RecOffset + 2))
            ,'DD-MM-YYYY:HH24:MI:SS');
   END getTime;

 FUNCTION getTime (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN DATE AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN to_date (
            '01-01-0001'|| ':'
            || utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value,g_fmtStruct_rec.t_Offset + p_RecOffset + 2 + 2,1)) || ':'
            || utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value,g_fmtStruct_rec.t_Offset + p_RecOffset + 2 + 1,1)) || ':'
            || utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value,g_fmtStruct_rec.t_Offset + p_RecOffset + 2,1))
            ,'DD-MM-YYYY:HH24:MI:SS');
   END getTime;

 FUNCTION getTime (p_Value BLOB) RETURN DATE AS
   BEGIN
     RETURN to_date (
            '01-01-0001'|| ':'
            || utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,1, 1 + 3)) || ':'
            || utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,1, 1 + 2)) || ':'
            || utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,1, 1 + 1))
            ,'DD-MM-YYYY:HH24:MI:SS');
   END getTime;

 FUNCTION getOneByte (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN utl_raw.cast_to_binary_integer(dbms_lob.substr(p_Value,1,g_fmtStruct_rec.t_Offset + p_RecOffset + 1));
   END getOneByte;


 FUNCTION getOneByte (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN NUMBER AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN utl_raw.cast_to_binary_integer(utl_raw.substr(p_Value,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,1));
   END getOneByte;


 FUNCTION getNByte (p_fieldName VARCHAR2, p_Value BLOB, p_RecOffset NUMBER DEFAULT 0) RETURN RAW AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN dbms_lob.substr(p_Value, g_fmtStruct_rec.t_Size, g_fmtStruct_rec.t_Offset + p_RecOffset + 1);
   END getNByte;


 FUNCTION getNByte (p_fieldName VARCHAR2, p_Value RAW, p_RecOffset NUMBER DEFAULT 0) RETURN RAW AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN utl_raw.substr(p_Value, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
   END getNByte;


 FUNCTION putInt (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB AS
     v_retValue BLOB;
   BEGIN
     posInStruct(p_fieldName);
     v_retValue := p_destValue;
     dbms_lob.write(v_retValue,g_fmtStruct_rec.t_Size,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                    utl_raw.substr(utl_raw.cast_from_binary_integer(p_srcValue, utl_raw.little_endian),1,g_fmtStruct_rec.t_Size));
     RETURN v_retValue;
   END putInt;


 FUNCTION putInt (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN RAW AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN utl_raw.overlay(
             utl_raw.substr(utl_raw.cast_from_binary_integer(p_srcValue, utl_raw.little_endian),1,g_fmtStruct_rec.t_Size),
             p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
   END putInt;

 FUNCTION putInt (p_destValue BLOB, p_srcValue NUMBER) RETURN BLOB AS
     v_retValue BLOB;
   BEGIN
     v_retValue := p_destValue;
     select utl_raw.cast_from_binary_integer(p_srcValue, utl_raw.little_endian) into v_retValue from dual;

     RETURN v_retValue;
   END putInt;



 FUNCTION putLong (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB AS
     v_retValue BLOB;
   BEGIN
     posInStruct(p_fieldName);
     v_retValue := p_destValue;
     dbms_lob.write(v_retValue,g_fmtStruct_rec.t_Size,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                    utl_raw.substr(utl_raw.cast_from_binary_integer(p_srcValue, utl_raw.little_endian),1,g_fmtStruct_rec.t_Size));
     RETURN v_retValue;
   END putLong;


 FUNCTION putLong (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN RAW AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN utl_raw.overlay(
             utl_raw.substr(utl_raw.cast_from_binary_integer(p_srcValue, utl_raw.little_endian),1,g_fmtStruct_rec.t_Size),
             p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
   END putLong;

 FUNCTION putLong (p_destValue BLOB, p_srcValue NUMBER) RETURN BLOB AS
     v_retValue BLOB;
   BEGIN
     v_retValue := p_destValue;
    select utl_raw.cast_from_binary_integer(p_srcValue, utl_raw.little_endian) into v_retValue from dual;
    RETURN v_retValue;
   END putLong;


 FUNCTION putDouble (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue FLOAT, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB AS
     v_retValue BLOB;
     v_Sign     PLS_INTEGER := 0;
     v_abs      NUMBER;
     v_pwr      PLS_INTEGER := 1023;
   BEGIN
     posInStruct(p_fieldName);
     v_retValue := p_destValue;
     IF (p_srcValue < 0) THEN
       v_Sign := 2048;
     END IF;

     IF (p_srcValue = 0) THEN
       v_abs  := 0;
       v_Sign := 0;
       v_pwr  := 0;
     ELSE
       v_abs := abs(p_srcValue);
       IF (v_abs > 1) THEN
         WHILE (v_abs >= 2) LOOP
           v_abs := v_abs/2;
           v_pwr := v_pwr + 1;
           IF (v_pwr > 2047) THEN
             RAISE VALUE_ERROR;
           END IF;
         END LOOP;
       ELSE
         WHILE (v_abs < 1) LOOP
           v_abs := v_abs*2;
           v_pwr := v_pwr - 1;
           IF (v_pwr < 0) THEN
             RAISE VALUE_ERROR;
           END IF;
         END LOOP;
       END IF;
       v_abs := v_abs - 1;
       v_abs := ROUND(v_abs*power(2,52));
     END IF;
     dbms_lob.write(v_retValue,g_fmtStruct_rec.t_Size,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                    utl_raw.reverse(to_char((v_Sign + v_pwr)*power(2,52) + v_abs,'FM0XXXXXXXXXXXXXXX')));
     RETURN v_retValue;
   END putDouble;


 FUNCTION putDouble (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue FLOAT, p_RecOffset NUMBER DEFAULT 0) RETURN RAW AS
     v_Sign     PLS_INTEGER := 0;
     v_abs      NUMBER;
     v_pwr      PLS_INTEGER := 1023;
   BEGIN
     posInStruct(p_fieldName);
     IF (p_srcValue < 0) THEN
       v_Sign := 2048;
     END IF;

     IF (p_srcValue = 0) THEN
       v_abs  := 0;
       v_Sign := 0;
       v_pwr  := 0;
     ELSE
       v_abs := abs(p_srcValue);
       IF (v_abs > 1) THEN
         WHILE (v_abs >= 2) LOOP
           v_abs := v_abs/2;
           v_pwr := v_pwr + 1;
           IF (v_pwr > 2047) THEN
             RAISE VALUE_ERROR;
           END IF;
         END LOOP;
       ELSE
         WHILE (v_abs < 1) LOOP
           v_abs := v_abs*2;
           v_pwr := v_pwr - 1;
           IF (v_pwr < 0) THEN
             RAISE VALUE_ERROR;
           END IF;
         END LOOP;
       END IF;
       v_abs := v_abs - 1;
       v_abs := ROUND(v_abs*power(2,52));
     END IF;
     RETURN utl_raw.overlay(
             utl_raw.reverse(to_char((v_Sign + v_pwr)*power(2,52) + v_abs,'FM0XXXXXXXXXXXXXXX')),
             p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
   END putDouble;

 FUNCTION putDouble (p_destValue BLOB, p_srcValue FLOAT) RETURN BLOB AS
     v_retValue BLOB;
     v_Sign     PLS_INTEGER := 0;
     v_abs      NUMBER;
     v_pwr      PLS_INTEGER := 1023;
   BEGIN
     v_retValue := p_destValue;
     IF (p_srcValue < 0) THEN
       v_Sign := 2048;
     END IF;

     IF (p_srcValue = 0) THEN
       v_abs  := 0;
       v_Sign := 0;
       v_pwr  := 0;
     ELSE
     v_abs := abs(p_srcValue);
     IF (v_abs > 1) THEN
       WHILE (v_abs >= 2) LOOP
         v_abs := v_abs/2;
         v_pwr := v_pwr + 1;
         IF (v_pwr > 2047) THEN
           RAISE VALUE_ERROR;
         END IF;
       END LOOP;
     ELSE
       WHILE (v_abs < 1) LOOP
         v_abs := v_abs*2;
         v_pwr := v_pwr - 1;
         IF (v_pwr < 0) THEN
           RAISE VALUE_ERROR;
         END IF;
       END LOOP;
     END IF;
     v_abs := v_abs - 1;
     v_abs := ROUND(v_abs*power(2,52));
     END IF;
     dbms_lob.write(v_retValue,8,1,
                    utl_raw.reverse(to_char((v_Sign + v_pwr)*power(2,52) + v_abs,'FM0XXXXXXXXXXXXXXX')));
     RETURN v_retValue;
   END putDouble;

 PROCEDURE splitMoney(p_srcValue NUMBER, p_Expo OUT NUMBER, p_LeaDi OUT NUMBER, p_TraDi0 OUT NUMBER, p_TraDi1 OUT NUMBER) IS
     v_Money    NUMBER;
     v_Expo     NUMBER := 0;
     v_LeaDi    NUMBER;
     v_TraDi0   NUMBER;
     v_TraDi1   NUMBER;
   BEGIN
     v_Money := p_srcValue;
     WHILE (v_Money <> TRUNC(v_Money)) LOOP
       v_Expo  := v_Expo + 1;
       v_Money := p_srcValue * power(10, v_Expo);
     END LOOP;
     IF (v_Money < 0) THEN
       v_Money := power(2,96) + v_Money;
     END IF;
     v_TraDi1 := mod(v_Money,power(2,32));
     v_Money := (v_Money - v_TraDi1)/power(2,32);
     v_TraDi0 := mod(v_Money,power(2,32));
     v_LeaDi := (v_Money - v_TraDi0)/power(2,32);

     IF (v_LeaDi >= power(2,31)) THEN
       v_LeaDi := v_LeaDi - power(2,32);
     END IF;

     IF (v_TraDi0 >= power(2,31)) THEN
       v_TraDi0 := v_TraDi0 - power(2,32);
     END IF;

     IF (v_TraDi1 >= power(2,31)) THEN
       v_TraDi1 := v_TraDi1 - power(2,32);
     END IF;


   p_Expo := v_Expo;
   p_LeaDi := v_LeaDi;
   p_TraDi0 := v_TraDi0;
   p_TraDi1 := v_TraDi1;
  END splitMoney;

 FUNCTION putMoney (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB AS
     v_retValue BLOB;
     v_Expo     NUMBER := 0;
     v_LeaDi    NUMBER;
     v_TraDi0   NUMBER;
     v_TraDi1   NUMBER;
   BEGIN
     posInStruct(p_fieldName);
     v_retValue := p_destValue;

     splitMoney(p_srcValue, v_Expo, v_LeaDi, v_TraDi0, v_TraDi1);

     dbms_lob.write(v_retValue,g_fmtStruct_rec.t_Size,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
            '0000' || utl_raw.substr(utl_raw.cast_from_binary_integer(v_Expo, utl_raw.little_endian),1,2)
                   || utl_raw.cast_from_binary_integer(v_LeaDi, utl_raw.little_endian)
                   || utl_raw.cast_from_binary_integer(v_TraDi0, utl_raw.little_endian)
                   || utl_raw.cast_from_binary_integer(v_TraDi1, utl_raw.little_endian));
     RETURN v_retValue;
   END putMoney;


 FUNCTION putMoney (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN RAW AS
     v_Expo     NUMBER := 0;
     v_LeaDi    NUMBER;
     v_TraDi0   NUMBER;
     v_TraDi1   NUMBER;
   BEGIN
     posInStruct(p_fieldName);

     splitMoney(p_srcValue, v_Expo, v_LeaDi, v_TraDi0, v_TraDi1);

     RETURN utl_raw.overlay('0000' || utl_raw.substr(utl_raw.cast_from_binary_integer(v_Expo, utl_raw.little_endian),1,2)
                                   || utl_raw.cast_from_binary_integer(v_LeaDi, utl_raw.little_endian)
                                   || utl_raw.cast_from_binary_integer(v_TraDi0, utl_raw.little_endian)
                                   || utl_raw.cast_from_binary_integer(v_TraDi1, utl_raw.little_endian),
             p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
   END putMoney;

 FUNCTION putMoney (p_destValue BLOB, p_srcValue NUMBER) RETURN BLOB AS
     v_retValue BLOB;
     v_Expo     NUMBER := 0;
     v_LeaDi    NUMBER;
     v_TraDi0   NUMBER;
     v_TraDi1   NUMBER;
   BEGIN
     v_retValue := p_destValue;

     splitMoney(p_srcValue, v_Expo, v_LeaDi, v_TraDi0, v_TraDi1);

     dbms_lob.write(v_retValue,16,1,
            '0000' || utl_raw.substr(utl_raw.cast_from_binary_integer(v_Expo, utl_raw.little_endian),1,2)
                   || utl_raw.cast_from_binary_integer(v_LeaDi, utl_raw.little_endian)
                   || utl_raw.cast_from_binary_integer(v_TraDi0, utl_raw.little_endian)
                   || utl_raw.cast_from_binary_integer(v_TraDi1, utl_raw.little_endian));
     RETURN v_retValue;
   END putMoney;

 FUNCTION putString (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue VARCHAR2, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB AS
     v_retValue BLOB;
   BEGIN
     posInStruct(p_fieldName);
     v_retValue := p_destValue;

    if g_NLS_CHARACTERSET is null then 
      SELECT VALUE INTO g_NLS_CHARACTERSET FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET';
    end if;
    IF  g_NLS_CHARACTERSET = 'AL32UTF8' THEN
         IF (p_srcValue is NULL OR p_srcValue = CHR(1)) THEN
           dbms_lob.write(v_retValue, g_fmtStruct_rec.t_Size, g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                          utl_raw.overlay(RSB_LOCALE.OEM_TO_RAW(RSB_LOCALE.STRING_TO_OEM(CHR(0))), '00', 1, g_fmtStruct_rec.t_Size));
         ELSIF INSTR( p_srcValue, CHR( 0 ) ) = 0 THEN
           dbms_lob.write(v_retValue, g_fmtStruct_rec.t_Size, g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                          utl_raw.overlay(RSB_LOCALE.OEM_TO_RAW(RSB_LOCALE.STRING_TO_OEM(p_srcValue)), '00', 1, g_fmtStruct_rec.t_Size));
         ELSE
           dbms_lob.write(v_retValue, g_fmtStruct_rec.t_Size, g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                          utl_raw.overlay(RSB_LOCALE.OEM_TO_RAW(RSB_LOCALE.STRING_TO_OEM(SUBSTR(p_srcValue,1,INSTR(p_srcValue,CHR(0))-1))), '00', 1, g_fmtStruct_rec.t_Size));
         END IF;
    ELSE
         IF (p_srcValue is NULL OR p_srcValue = CHR(1)) THEN
           dbms_lob.write(v_retValue, g_fmtStruct_rec.t_Size, g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                          utl_raw.overlay(utl_raw.cast_to_raw(CHR(0)), '00', 1, g_fmtStruct_rec.t_Size));
         ELSIF INSTR( p_srcValue, CHR( 0 ) ) = 0 THEN
           dbms_lob.write(v_retValue, g_fmtStruct_rec.t_Size, g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                          utl_raw.overlay(utl_raw.cast_to_raw(p_srcValue), '00', 1, g_fmtStruct_rec.t_Size));
         ELSE
           dbms_lob.write(v_retValue, g_fmtStruct_rec.t_Size, g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                          utl_raw.overlay(utl_raw.cast_to_raw(SUBSTR(p_srcValue,1,INSTR(p_srcValue,CHR(0))-1)), '00', 1, g_fmtStruct_rec.t_Size));
         END IF;
   END IF;

     RETURN v_retValue;
   END putString;


 FUNCTION putString (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue VARCHAR2, p_RecOffset NUMBER DEFAULT 0) RETURN RAW AS
   BEGIN
     posInStruct(p_fieldName);
    if g_NLS_CHARACTERSET is null then 
      SELECT VALUE INTO g_NLS_CHARACTERSET FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET';
    end if;
    IF  g_NLS_CHARACTERSET = 'AL32UTF8' THEN
        IF (p_srcValue is NULL OR p_srcValue = CHR(1)) THEN
          RETURN utl_raw.overlay(RSB_LOCALE.OEM_TO_RAW(RSB_LOCALE.STRING_TO_OEM(CHR(0))),
                  p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
        ELSIF INSTR( p_srcValue, CHR( 0 ) ) = 0 THEN
          RETURN utl_raw.overlay(RSB_LOCALE.OEM_TO_RAW(RSB_LOCALE.STRING_TO_OEM(p_srcValue)),
                  p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
        ELSE
          RETURN utl_raw.overlay(RSB_LOCALE.OEM_TO_RAW(RSB_LOCALE.STRING_TO_OEM(SUBSTR(p_srcValue,1,INSTR(p_srcValue,CHR(0))-1))),
                  p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
      END IF;

    ELSE
        IF (p_srcValue is NULL OR p_srcValue = CHR(1)) THEN
          RETURN utl_raw.overlay(utl_raw.cast_to_raw(CHR(0)),
                  p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
        ELSIF INSTR( p_srcValue, CHR( 0 ) ) = 0 THEN
          RETURN utl_raw.overlay(utl_raw.cast_to_raw(p_srcValue),
                  p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
        ELSE
          RETURN utl_raw.overlay(utl_raw.cast_to_raw(SUBSTR(p_srcValue,1,INSTR(p_srcValue,CHR(0))-1)),
                  p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
        END IF;
    END IF;
   END putString;

 FUNCTION putString (p_destValue BLOB, p_srcValue VARCHAR2) RETURN BLOB AS
     v_retValue BLOB;
    r          RAW(32000);
   BEGIN
    if g_NLS_CHARACTERSET is null then 
      SELECT VALUE INTO g_NLS_CHARACTERSET FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET';
    end if;
    v_retValue := p_destValue;

   IF  g_NLS_CHARACTERSET = 'AL32UTF8' THEN
       r := RSB_LOCALE.OEM_TO_RAW(RSB_LOCALE.STRING_TO_OEM(p_srcValue));
       v_retValue := r;
   ELSE
       IF (p_srcValue is NULL OR p_srcValue = CHR(1)) THEN
         dbms_lob.write(v_retValue, LENGTH(p_srcValue), 1,
                        utl_raw.overlay(utl_raw.cast_to_raw(CHR(0)), '00', 1, LENGTH(p_srcValue)));
       ELSIF INSTR( p_srcValue, CHR( 0 ) ) = 0 THEN
         dbms_lob.write(v_retValue, LENGTH(p_srcValue), 1,
                        utl_raw.overlay(utl_raw.cast_to_raw(p_srcValue), '00', 1, LENGTH(p_srcValue)));
       ELSE
         dbms_lob.write(v_retValue, LENGTH(p_srcValue), 1,
                        utl_raw.overlay(utl_raw.cast_to_raw(SUBSTR(p_srcValue,1,INSTR(p_srcValue,CHR(0))-1)), '00', 1, LENGTH(p_srcValue)));
       END IF;
   END IF;

    RETURN v_retValue;
   END putString;

 FUNCTION putChar (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue CHAR, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB AS
     v_retValue BLOB;
   BEGIN
     posInStruct(p_fieldName);
     v_retValue := p_destValue;

    if g_NLS_CHARACTERSET is null then 
      SELECT VALUE INTO g_NLS_CHARACTERSET FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET';
    end if;
   IF  g_NLS_CHARACTERSET = 'AL32UTF8' THEN
         dbms_lob.write(v_retValue, g_fmtStruct_rec.t_Size, g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                    utl_raw.overlay(RSB_LOCALE.OEM_TO_RAW(RSB_LOCALE.STRING_TO_OEM(p_srcValue)), '00', 1, g_fmtStruct_rec.t_Size));
    ELSE
         dbms_lob.write(v_retValue, g_fmtStruct_rec.t_Size, g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                    utl_raw.overlay(utl_raw.cast_to_raw(p_srcValue), '00', 1, g_fmtStruct_rec.t_Size));
    END IF;


     RETURN v_retValue;
   END putChar;


 FUNCTION putChar (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue CHAR, p_RecOffset NUMBER DEFAULT 0) RETURN RAW AS
   BEGIN
     posInStruct(p_fieldName);

    if g_NLS_CHARACTERSET is null then 
      SELECT VALUE INTO g_NLS_CHARACTERSET FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET';
    end if;
   IF  g_NLS_CHARACTERSET = 'AL32UTF8' THEN
         RETURN utl_raw.overlay(RSB_LOCALE.OEM_TO_RAW(RSB_LOCALE.STRING_TO_OEM(p_srcValue)),
                 p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);

    END IF;

     RETURN utl_raw.overlay(utl_raw.cast_to_raw(p_srcValue),
             p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
   END putChar;

 FUNCTION putChar (p_destValue BLOB, p_srcValue CHAR) RETURN BLOB AS
     v_retValue BLOB;
     nls        NVARCHAR2(32);
   BEGIN
     v_retValue := p_destValue;

    if g_NLS_CHARACTERSET is null then 
      SELECT VALUE INTO g_NLS_CHARACTERSET FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET';
    end if;
   IF  g_NLS_CHARACTERSET = 'AL32UTF8' THEN
         v_retValue := RSB_LOCALE.OEM_TO_RAW(RSB_LOCALE.STRING_TO_OEM(p_srcValue));
    ELSE
         dbms_lob.write(v_retValue, 2, 1,
                        utl_raw.overlay(utl_raw.cast_to_raw(p_srcValue), '00', 1, 2));
    END IF;

     RETURN v_retValue;
   END putChar;



 FUNCTION putDate (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue DATE, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB AS
     v_retValue BLOB;
     v_Date     RAW(4);
   BEGIN
     posInStruct(p_fieldName);
     v_retValue := p_destValue;
     v_Date := utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'DD'), utl_raw.little_endian),1,1) ||
               utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'MM'), utl_raw.little_endian),1,1) ||
               utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'YYYY'), utl_raw.little_endian),1,2);
     IF (v_Date = '01010100') THEN
       v_Date := '00000000';
     END IF;
     dbms_lob.write(v_retValue,g_fmtStruct_rec.t_Size,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                    v_Date);
     RETURN v_retValue;
   END putDate;


 FUNCTION putDate (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue DATE, p_RecOffset NUMBER DEFAULT 0) RETURN RAW AS
     v_Date     RAW(4);
   BEGIN
     posInStruct(p_fieldName);
     v_Date := utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'DD'), utl_raw.little_endian),1,1) ||
               utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'MM'), utl_raw.little_endian),1,1) ||
               utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'YYYY'), utl_raw.little_endian),1,2);
     IF (v_Date = '01010100') THEN
       v_Date := '00000000';
     END IF;
     RETURN utl_raw.overlay(v_Date, p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
   END putDate;

 FUNCTION putDate (p_destValue BLOB, p_srcValue DATE) RETURN BLOB AS
     v_retValue BLOB;
     v_Date     RAW(4);
   BEGIN
     v_retValue := p_destValue;
     v_Date := utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'DD'), utl_raw.little_endian),1,1) ||
               utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'MM'), utl_raw.little_endian),1,1) ||
               utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'YYYY'), utl_raw.little_endian),1,2);
     IF (v_Date = '01010100') THEN
       v_Date := '00000000';
     END IF;
     dbms_lob.write(v_retValue,4,1,
                    v_Date);
     RETURN v_retValue;
   END putDate;

 FUNCTION putTime (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue DATE, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB AS
     v_retValue BLOB;
     v_Time     RAW(3);
   BEGIN
     posInStruct(p_fieldName);
     v_retValue := p_destValue;
     v_Time := utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'SS'), utl_raw.little_endian),1,1) ||
               utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'MI'), utl_raw.little_endian),1,1) ||
               utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'HH24'), utl_raw.little_endian),1,1);
     dbms_lob.write(v_retValue,g_fmtStruct_rec.t_Size,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                    v_Time);
     RETURN v_retValue;
   END putTime;


 FUNCTION putTime (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue DATE, p_RecOffset NUMBER DEFAULT 0) RETURN RAW AS
     v_Time     RAW(3);
   BEGIN
     posInStruct(p_fieldName);
     v_Time := utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'SS'), utl_raw.little_endian),1,1) ||
               utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'MI'), utl_raw.little_endian),1,1) ||
               utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'HH24'), utl_raw.little_endian),1,1);
     RETURN utl_raw.overlay(v_Time, p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
   END putTime;

 FUNCTION putTime (p_destValue BLOB, p_srcValue DATE) RETURN BLOB AS
     v_retValue BLOB;
     v_Time     RAW(4);
   BEGIN
     v_retValue := p_destValue;
     v_Time := '00' ||
               utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'SS'), utl_raw.little_endian),1,1) ||
               utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'MI'), utl_raw.little_endian),1,1) ||
               utl_raw.substr(utl_raw.cast_from_binary_integer(to_char(p_srcValue,'HH24'), utl_raw.little_endian),1,1);
     dbms_lob.write(v_retValue,4,1,
                    v_Time);
     RETURN v_retValue;
   END putTime;

 FUNCTION putOneByte (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB AS
     v_retValue BLOB;
   BEGIN
     posInStruct(p_fieldName);
     v_retValue := p_destValue;
     dbms_lob.write(v_retValue,g_fmtStruct_rec.t_Size,g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                    utl_raw.substr(utl_raw.cast_from_binary_integer(p_srcValue, utl_raw.little_endian),1,g_fmtStruct_rec.t_Size));
     RETURN v_retValue;
   END putOneByte;


 FUNCTION putOneByte (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue NUMBER, p_RecOffset NUMBER DEFAULT 0) RETURN RAW AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN utl_raw.overlay(
             utl_raw.substr(utl_raw.cast_from_binary_integer(p_srcValue, utl_raw.little_endian),1,g_fmtStruct_rec.t_Size),
             p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
   END putOneByte;


 FUNCTION putNByte (p_fieldName VARCHAR2, p_destValue BLOB, p_srcValue RAW, p_RecOffset NUMBER DEFAULT 0) RETURN BLOB AS
     v_retValue BLOB;
   BEGIN
     posInStruct(p_fieldName);
     v_retValue := p_destValue;
     dbms_lob.write(v_retValue, g_fmtStruct_rec.t_Size, g_fmtStruct_rec.t_Offset + p_RecOffset + 1,
                    utl_raw.overlay(p_srcValue, '00', 1, g_fmtStruct_rec.t_Size));
     RETURN v_retValue;
   END putNByte;


 FUNCTION putNByte (p_fieldName VARCHAR2, p_destValue RAW, p_srcValue RAW, p_RecOffset NUMBER DEFAULT 0) RETURN RAW AS
   BEGIN
     posInStruct(p_fieldName);
     RETURN utl_raw.overlay(p_srcValue, p_destValue, g_fmtStruct_rec.t_Offset + p_RecOffset + 1, g_fmtStruct_rec.t_Size);
   END putNByte;


END rsb_struct;
/
