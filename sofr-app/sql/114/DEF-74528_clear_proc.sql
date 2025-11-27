/* Formatted on 02.11.2024 9:32:28 (QP5 v5.149.1003.31008) */
DECLARE
   TYPE objRec IS RECORD (T_OBJECT_NAME VARCHAR (500));

   TYPE objRecArr IS TABLE OF objRec;

   v_objRecArr   objRecArr;

   v_Cursor      SYS_REFCURSOR;
BEGIN
   OPEN v_Cursor FOR
      'SELECT OBJECT_NAME
  FROM dba_objects
 WHERE     object_type = ''PROCEDURE''
       AND owner = SYS_CONTEXT (''USERENV'', ''CURRENT_SCHEMA'')
       AND object_name LIKE ''BGSQLEXECUTER_PROC_%''';

   LOOP
      FETCH v_Cursor
      BULK COLLECT INTO v_objRecArr
      LIMIT 1000;

      IF v_objRecArr.COUNT > 0
      THEN
         FOR indx IN v_objRecArr.FIRST .. v_objRecArr.LAST
         LOOP
            execute immediate 'DROP PROCEDURE ' || v_objRecArr (indx).T_OBJECT_NAME;
         END LOOP;
      END IF;

      EXIT WHEN v_Cursor%NOTFOUND;
   END LOOP;

   CLOSE v_Cursor;
END;
/