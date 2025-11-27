-- вырезание BOSS-2607 

--добавление удаленного примечания ЦБ 109 Точность при конвертации 
DECLARE
   c_objecttype number := 12; /* Ценная бумага */
   --c_type_string number := 7; 
   --c_type_money number := 25;  
   c_type_integer number := 0;  

   PROCEDURE CreateNoteKind(p_notekind in number, p_notename in varchar2, p_notetype in number) IS 
      nCount   NUMBER := 0;
    BEGIN
      SELECT COUNT (*) INTO nCount
        FROM dnotekind_dbt
       WHERE T_OBJECTTYPE = c_objecttype AND T_NOTEKIND = p_notekind;

      IF nCount > 0 THEN
         DBMS_OUTPUT.put_line ('Примечание вида '||p_notekind||' для объекта ' ||c_objecttype||' уже существует (создавалось ранее).');
      ELSE
         INSERT INTO dnotekind_dbt (T_OBJECTTYPE, T_NOTEKIND, T_NOTETYPE, T_NAME, T_KEEPOLDVALUES,
                                    T_NOTINUSE, T_ISPROTECTED, T_MAXLEN, T_NOTUSEFIELDUSE, T_MACRONAME,
                                    T_DECPL, T_ISPROGONLY)
         VALUES (c_objecttype, p_notekind, p_notetype, p_notename, CHR(88),
                 CHR (0), CHR (0), 0, CHR (0), chr(1), 0, CHR (0));
         DBMS_OUTPUT.put_line ( 'Примечание вида '||p_notekind||' для объекта ' ||c_objecttype||' создано.');
      END IF;
   EXCEPTION
      WHEN OTHERS THEN DBMS_OUTPUT.put_line ('Ошибка создания примечания '||sqlerrm);
   END;
BEGIN
   CreateNoteKind(109, 'Точность при конвертации', c_type_integer);

   commit;
EXCEPTION
WHEN OTHERS THEN NULL;     
END;
