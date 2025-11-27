DECLARE
   c_objecttype number := 131; /* операция зачисления ДС*/
   c_type_string number := 7; 
   c_type_money number := 25;  

   PROCEDURE CreateNoteKind(p_notekind in number, p_notename in varchar2, 
                            p_notetype in number, p_objecttype in number, 
                            p_history in varchar2, p_program in varchar2) IS 
    BEGIN

      DELETE FROM dnotekind_dbt WHERE T_OBJECTTYPE = c_objecttype AND T_NOTEKIND = p_notekind 
                                  AND T_NOTETYPE = p_notetype;

      INSERT INTO dnotekind_dbt (T_OBJECTTYPE, T_NOTEKIND, T_NOTETYPE, T_NAME, T_KEEPOLDVALUES,
                                 T_NOTINUSE, T_ISPROTECTED, T_MAXLEN, T_NOTUSEFIELDUSE, T_MACRONAME,
                                 T_DECPL, T_ISPROGONLY)
      VALUES (c_objecttype, p_notekind, p_notetype, p_notename, p_history,
              CHR (0), CHR (0), 0, CHR (0), chr(1), 0, p_program);

   EXCEPTION
      WHEN OTHERS THEN 
        dbms_output.put_line('Ошибка обновления создания примечания');
        it_log.log('Ошибка обновления создания примечания');
   END;
BEGIN
   CreateNoteKind(104, 'Зачисление запрещено', c_type_string, 
     c_objecttype, chr(88), CHR (88));
   CreateNoteKind(105, 'Результат обработки неторгового поручения в QUIK', c_type_string, 
     c_objecttype, chr(0), CHR (0));

   commit;
   
END;