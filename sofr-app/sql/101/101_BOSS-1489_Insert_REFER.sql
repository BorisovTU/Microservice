DECLARE
   v_SeqDescription VARCHAR2(4000);
   v_ObjName VARCHAR2(4000);
   v_RefDescription VARCHAR2(4000);
   v_RefObjName VARCHAR2(4000);
BEGIN
   BEGIN
      INSERT INTO DSEQUENT_DBT (
                                  t_SeqID,
                                  t_Description,
                                  t_ResetType,
                                  t_ResetDate,
                                  t_ResetTime,
                                  t_ResetMonths,
                                  t_IdentProgram,
                                  t_PassEnd,
                                  t_SysDateCheck,
                                  t_SysResetTime,
                                  t_CheckHistory,
                                  t_SeqStatus,
                                  t_StartValue,
                                  t_EndValue,
                                  t_Branch,
                                  t_IsLocal,
                                  t_Level,
                                  t_NoUpgrade,
                                  T_USEMETERINHIST,
                                  t_ResetCalcType,
                                  t_Reserve
                               )
                        VALUES (
                                  366,
                                  'Номер операции технической сверки СНОБ',
                                  0,
                                  to_date('01-01-0001:00:00:00', 'dd-mm-yyyy:hh24:mi:ss'),
                                  0,
                                  0,
                                  'Г',
                                  CHR(1),
                                  CHR(2),
                                  to_date('01-01-0001:00:00:00', 'dd-mm-yyyy:hh24:mi:ss'),
                                  CHR(2),
                                  0,
                                  0,
                                  0,
                                  1,
                                  CHR(2),
                                  1,
                                  CHR(2),
                                  CHR(2),
                                  0,
                                  NULL
                               );
                               
      v_SeqDescription := 'Номер операции технической сверки СНОБ';
   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN
         SELECT t_Description INTO v_SeqDescription
         FROM DSEQUENT_DBT
         WHERE t_SeqID = 366;
         
         IT_LOG.LOG('BOSS-1489. Уже существует запись в таблице DSEQUENT_DBT с t_SeqID = 366, t_Description = ''' || v_SeqDescription || '''');
   END;
   
   BEGIN
      INSERT INTO DOBJECTS_DBT (
                                  t_ObjectType,
                                  t_Name,
                                  t_Code,
                                  t_UserNumber,
                                  t_ParentObjectType,
                                  t_ServiceMacro,
                                  t_Module
                               )
                        VALUES (
                                  187,
                                  'Операция технической сверки СНОБ',
                                  'ОпСвСНОБ',
                                  0,
                                  0,
                                  NULL,
                                  CHR(2)
                               );
      
      v_ObjName := 'Операция технической сверки СНОБ';
   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN
         SELECT t_Name INTO v_ObjName
         FROM DOBJECTS_DBT
         WHERE t_ObjectType = 187;
         
      IT_LOG.LOG('BOSS-1489. Уже существует запись в таблице DOBJECTS_DBT с t_ObjectType = 187, t_Name = ''' || v_ObjName || '''');
   END;
   
   IF v_SeqDescription = 'Номер операции технической сверки СНОБ'
   THEN
      BEGIN
         INSERT INTO DREFER_DBT (
                                   t_RefID,
                                   t_SeqID,
                                   t_Description,
                                   t_CalcType,
                                   t_Template,
                                   t_MaxLen,
                                   t_MacroFile,
                                   t_MacroProc,
                                   t_State,
                                   t_BankDate,
                                   t_SysDate,
                                   t_SysTime,
                                   t_UserID,
                                   t_IdentProgram,
                                   t_CheckMissed,
                                   t_CheckRangeNumLimit,
                                   t_CheckHistory,
                                   t_CurRefID,
                                   t_Branch,
                                   t_IsLocal,
                                   t_NoUpgrade,
                                   t_Reserve
                                )
                         VALUES (
                                   370,
                                   366,
                                   'Номер операции технической сверки СНОБ',
                                   1,
                                   '\n',
                                   10,
                                   NULL,
                                   NULL,
                                   0,
                                   to_date('01-01-0001:00:00:00', 'dd-mm-yyyy:hh24:mi:ss'),
                                   to_date('01-01-0001:00:00:00', 'dd-mm-yyyy:hh24:mi:ss'),
                                   to_date('01-01-0001:00:00:00', 'dd-mm-yyyy:hh24:mi:ss'),
                                   0,
                                   'Г',
                                   CHR(2),
                                   CHR(2),
                                   CHR(2),
                                   0,
                                   1,
                                   CHR(2),
                                   CHR(2),
                                   NULL
                                );
                                
         v_RefDescription := 'Номер операции технической сверки СНОБ';
      EXCEPTION
         WHEN DUP_VAL_ON_INDEX
         THEN
            SELECT t_Description INTO v_RefDescription
            FROM DREFER_DBT
            WHERE t_RefID = 370;
            
            IT_LOG.LOG('BOSS-1489. Уже существует запись в таблице DREFER_DBT с t_RefID = 370, t_Description = ''' || v_RefDescription || '''');
      END;
      
      IF v_ObjName = 'Операция технической сверки СНОБ' and v_RefDescription = 'Номер операции технической сверки СНОБ'
      THEN
         BEGIN
            INSERT INTO DREFOBJ_DBT (
                                       t_ObjectType,
                                       t_RefType,
                                       t_RefID,
                                       t_IdentProgram,
                                       t_Name,
                                       t_State,
                                       t_BankDate,
                                       t_SysDate,
                                       t_SysTime,
                                       t_UserID,
                                       t_System,
                                       t_Reserve
                                    )
                             VALUES (
                                       187,
                                       1,
                                       370,
                                       'S',
                                       'Формирование номера операции технической сверки СНОБ',
                                       0,
                                       to_date('01-01-0001:00:00:00', 'dd-mm-yyyy:hh24:mi:ss'),
                                       to_date('01-01-0001:00:00:00', 'dd-mm-yyyy:hh24:mi:ss'),
                                       to_date('01-01-0001:00:00:00', 'dd-mm-yyyy:hh24:mi:ss'),
                                       0,
                                       'X',
                                       NULL
                                    );
         EXCEPTION
            WHEN DUP_VAL_ON_INDEX
            THEN
               SELECT t_Name INTO v_RefObjName
               FROM DREFOBJ_DBT
               WHERE t_ObjectType = 187
                 AND t_RefType = 1;
            
               IT_LOG.LOG('BOSS-1489. Уже существует запись в таблице DREFOBJ_DBT с t_ObjectType = 187 и t_RefID = 370, t_Name = ''' || v_RefObjName || '''');
         END;
      END IF;
   END IF;
END;
/