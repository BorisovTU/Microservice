DECLARE
   v_KeyID NUMBER := 0;
   v_ParentKeyID NUMBER := 0;
   v_ValExist NUMBER := 0;
   v_OldRegName VARCHAR2(100);
BEGIN
   v_ParentKeyID := RSB_COMMON.GETREGPARM('COMMON/СНОБ');
   v_KeyID := RSB_COMMON.GETREGPARM('COMMON/СНОБ/ОТКАТ ПЕРЕСЧЕТА НОБ');

   IF v_ParentKeyID > 0 AND v_KeyID <= 0
   THEN
      it_log.log('INSERT INTO DREGPARM_DBT T_KEYID = ' || v_KeyID || ', t_Name = ОТКАТ ПЕРЕСЧЕТА НОБ');

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
                        '                            ''ОТКАТ ПЕРЕСЧЕТА НОБ'', ' ||
                        '                            4, ' ||
                        '                            CHR(0), ' ||
                        '                            ''Возможность отката операций расчета НОБ с установленным признаком "Пересчет"'', ' ||
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
            it_log.log('Insert into dregval_dbt T_KEYID = ' || v_KeyID || ', t_LIntValue = 0');

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
   ELSE
      SELECT t_Name INTO v_OldRegName
      FROM DREGPARM_DBT
      WHERE t_KeyID = v_KeyID;

      it_log.log('Update dregparm_dbt t_KeyID = ' || v_KeyID || ', v_OldRegName = ' || v_OldRegName || ', New Reg Name = Возможность отката операций расчета НОБ с установленным признаком "Пересчет"');

      EXECUTE IMMEDIATE 'UPDATE DREGPARM_DBT ' ||
                        'SET t_Name = ''ОТКАТ ПЕРЕСЧЕТА НОБ'', ' ||
                        '    t_Description = ''Возможность отката операций расчета НОБ с установленным признаком "Пересчет"'' ' ||
                        'WHERE t_KeyID = :KeyID' USING v_KeyID;   
   END IF;
   
   COMMIT;
END;
/