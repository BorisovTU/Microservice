DECLARE
   c_objecttype_cur number := 2; /* Валюта */
   c_objecttype_cash number := 131; /* Списание зачисление ДС */
   c_type_string number := 7; 
   c_type_money number := 25;  

   PROCEDURE CreateNoteKind(p_notekind in number, p_notename in varchar2, 
                            p_notetype in number, p_objecttype in number, p_history in varchar2) IS 
      nCount   NUMBER := 0;
    BEGIN
      SELECT COUNT (*) INTO nCount
        FROM dnotekind_dbt
       WHERE T_OBJECTTYPE = p_objecttype AND T_NOTEKIND = p_notekind;

      IF nCount > 0 THEN
         it_log.log ('Примечание вида '||p_notekind||' для объекта ' ||p_objecttype||' уже существует (создавалось ранее).');
      ELSE
         INSERT INTO dnotekind_dbt (T_OBJECTTYPE, T_NOTEKIND, T_NOTETYPE, T_NAME, T_KEEPOLDVALUES,
                                    T_NOTINUSE, T_ISPROTECTED, T_MAXLEN, T_NOTUSEFIELDUSE, T_MACRONAME,
                                    T_DECPL, T_ISPROGONLY)
         VALUES (p_objecttype, p_notekind, p_notetype, p_notename, p_history,
                 CHR (0), CHR (0), 0, CHR (0), chr(1), 0, CHR (0));
         DBMS_OUTPUT.put_line ( 'Примечание вида '||p_notekind||' для объекта ' ||p_objecttype||' создано.');
      END IF;
   EXCEPTION
      WHEN OTHERS THEN it_log.log ('Ошибка создания примечания '||sqlerrm);
   END;
BEGIN
   CreateNoteKind(101, 'Порог авторизации', c_type_money, c_objecttype_cur, chr(88));
   CreateNoteKind(104, 'Результат обработки неторгового поручения в QUIK', c_type_string, c_objecttype_cash, chr(0));
   commit;
  COMMIT;
EXCEPTION
   WHEN OTHERS THEN  it_log.log('Ошибка создания примечаний Порог авторизации и Результат обработки неторгового поручения в QUIK');
END;
