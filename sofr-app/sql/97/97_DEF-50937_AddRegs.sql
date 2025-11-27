DECLARE
   v_KeyID NUMBER := 0;
   v_ParentKeyID NUMBER := 0;
   v_ValExist NUMBER := 0;
BEGIN
   v_ParentKeyID := RSB_COMMON.GETREGPARM('COMMON/ÑÍÎÁ');
   v_KeyID := RSB_COMMON.GETREGPARM('COMMON/ÑÍÎÁ/ÎÒÊÀÒ ÏÅÐÅÑ×ÅÒÀ ÍÎÁ');

   IF v_ParentKeyID > 0 AND v_KeyID <= 0
   THEN
      EXECUTE IMMEDIATE 'INSERT INTO DREGPARM_DBT ( ' ||
                        '                            t_KeyID, ' ||
                        '                            t_ParentID, ' ||
                        '                            t_Name, ' ||
                        '                            t_Type, ' ||
                        '                            t_Global, ' ||
                        '                            t_Description, ' ||
                        '                            t_Security, ' ||
                        '                            t_IsBranch, ' ||
                        '                            t_Template ' ||
                        '                         ) ' ||
                        '                  VALUES ( ' ||
                        '                            0, ' ||
                        '                            :1, ' ||
                        '                            ''ÎÒÊÀÒ ÏÅÐÅÑ×ÅÒÀ ÍÎÁ'', ' ||
                        '                            4, ' ||
                        '                            CHR(0), ' ||
                        '                            ''Âîçìîæíîñòü îòêàòà îïåðàöèé ðàñ÷åòà ÍÎÁ ñ óñòàíîâëåííûì ïðèçíàêîì "Ïåðåñ÷åò"'', ' ||
                        '                            CHR(0), ' ||
                        '                            CHR(0), ' ||
                        '                            CHR(0) ' ||
                        '                         ) ' ||
                        ' RETURNING t_KeyID INTO :2' USING v_ParentKeyID RETURNING INTO v_KeyID;
                        
      IF v_KeyID > 0
      THEN
         SELECT COUNT(1) INTO v_ValExist
         FROM DREGVAL_DBT
         WHERE t_KeyID = v_KeyID;
         
         
         IF v_ValExist = 0
         THEN
            EXECUTE IMMEDIATE 'INSERT INTO DREGVAL_DBT ( ' ||
                              '                           t_KeyID, ' ||
                              '                           t_RegKind, ' ||
                              '                           t_ObjectID, ' ||
                              '                           t_BlockUserValue, ' ||
                              '                           t_ExpDep, ' ||
                              '                           t_LIntValue, ' ||
                              '                           t_LDoubleValue ' ||
                              '                        ) ' ||
                              '                 VALUES ( ' ||
                              '                           :1, ' ||
                              '                           0, ' ||
                              '                           0, ' ||
                              '                           CHR(0), ' ||
                              '                           0, ' ||
                              '                           0, ' ||
                              '                           0 ' ||
                              '                        ) ' USING v_KeyID;
         END IF;
      END IF;
      
      COMMIT;
   END IF;
   
   DBMS_OUTPUT.PUT_LINE('v_KeyID = ' || v_KeyID);
END;
/