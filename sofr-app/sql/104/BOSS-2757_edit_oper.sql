DECLARE
   p_OPERBLOCKID_1   NUMBER (10);
   p_OPERBLOCKID_2   NUMBER (10);
   p_OPERBLOCKID_3   NUMBER (10);
   p_OPERBLOCKID_4   NUMBER (10);
   p_OPERBLOCKID_5   NUMBER (10);
BEGIN
   SELECT T_OPERBLOCKID
     INTO p_OPERBLOCKID_1
     FROM DOPROBLCK_DBT
    WHERE T_BLOCKID = 204701;

   SELECT T_OPERBLOCKID
     INTO p_OPERBLOCKID_2
     FROM DOPROBLCK_DBT
    WHERE T_BLOCKID = 204702;

   SELECT T_OPERBLOCKID
     INTO p_OPERBLOCKID_3
     FROM DOPROBLCK_DBT
    WHERE T_BLOCKID = 204703 and T_SORT = 3;

   INSERT INTO DOPRBLOCK_DBT (t_BlockID,
                              t_Name,
                              t_DocKind,
                              t_Parent,
                              t_Upgrade,
                              t_Version,
                              t_VersionWEB)
        VALUES (204704,
                'Получение ответа от СНОБ',
                4650,
                0,
                CHR (0),
                CHR (1),
                0);


   INSERT INTO DOPROBLCK_DBT (t_Kind_Operation,
                              t_BlockID,
                              t_Sort,
                              t_NotInUse,
                              t_NoInsert,
                              t_NoReplace,
                              T_NOCLOSEINSERT,
                              t_IsManual,
                              t_SymbolsForInsertion,
                              t_Symbol)
        VALUES (2047,
                204703,
                4,
                CHR (0),
                CHR (0),
                CHR (0),
                'X',
                CHR (0),
                CHR (1),
                CHR (0))
     RETURNING T_OPERBLOCKID
          INTO p_OPERBLOCKID_4;

   INSERT INTO DOPROBLCK_DBT (t_Kind_Operation,
                              t_BlockID,
                              t_Sort,
                              t_NotInUse,
                              t_NoInsert,
                              t_NoReplace,
                              T_NOCLOSEINSERT,
                              t_IsManual,
                              t_SymbolsForInsertion,
                              t_Symbol)
        VALUES (2047,
                204704,
                5,
                CHR (0),
                CHR (0),
                CHR (0),
                'X',
                'X',
                CHR (1),
                'П')
     RETURNING T_OPERBLOCKID
          INTO p_OPERBLOCKID_5;


   DELETE FROM DOPRCBLCK_DBT
         WHERE T_STATUSKINDID = 46501;

   INSERT INTO DOPRCBLCK_DBT (T_OPERBLOCKID,
                              T_STATUSKINDID,
                              T_NUMVALUE,
                              T_CONDITION)
        VALUES (p_OPERBLOCKID_1,
                46501,
                1,
                0);

   INSERT INTO DOPRCBLCK_DBT (T_OPERBLOCKID,
                              T_STATUSKINDID,
                              T_NUMVALUE,
                              T_CONDITION)
        VALUES (p_OPERBLOCKID_2,
                46501,
                2,
                0);

   INSERT INTO DOPRCBLCK_DBT (T_OPERBLOCKID,
                              T_STATUSKINDID,
                              T_NUMVALUE,
                              T_CONDITION)
        VALUES (p_OPERBLOCKID_3,
                46501,
                3,
                0);

   INSERT INTO DOPRCBLCK_DBT (T_OPERBLOCKID,
                              T_STATUSKINDID,
                              T_NUMVALUE,
                              T_CONDITION)
        VALUES (p_OPERBLOCKID_4,
                46501,
                3,
                0);

   INSERT INTO DOPRCBLCK_DBT (T_OPERBLOCKID,
                              T_STATUSKINDID,
                              T_NUMVALUE,
                              T_CONDITION)
        VALUES (p_OPERBLOCKID_5,
                46501,
                4,
                0);

   DELETE FROM DOPROSTEP_DBT
         WHERE T_BLOCKID = 204702 AND T_NUMBER_STEP = 20;

   INSERT INTO DOPROSTEP_DBT (t_BlockID,
                              t_Number_Step,
                              t_Kind_Action,
                              t_DayOffset,
                              t_Scale,
                              t_DayFlag,
                              t_CalendarID,
                              t_Symbol,
                              T_PREVIOUS_STEP,
                              T_MODIFICATION,
                              T_CARRY_MACRO,
                              T_PRINT_MACRO,
                              T_POST_MACRO,
                              T_NOTINUSE,
                              T_FIRSTSTEP,
                              T_NAME,
                              T_DATEKINDID,
                              T_REV,
                              T_AUTOEXECUTESTEP,
                              T_ONLYHANDCARRY,
                              T_ISALLOWFOROPER,
                              T_OPERORGROUP,
                              T_RESTRICTEARLYEXECUTION,
                              T_USERTYPES,
                              T_INITDATEKINDID,
                              T_ASKFORDATE,
                              T_BACKOUT,
                              T_ISBACKOUTGROUP,
                              T_MASSEXECUTEMODE,
                              T_ISCASE,
                              T_ISDISTAFFEXECUTE,
                              T_SKIPINITAFTERPLANDATE,
                              T_MASSPACKSIZE)
        VALUES (204704,
                40,
                1,
                0,
                0,
                CHR (0),
                0,
                'П',
                0,
                0,
                'nptxsnobver040.mac',
                CHR (1),
                CHR (1),
                CHR (0),
                NULL,
                'Получение ответа от СНОБ',
                465000000,
                CHR (0),
                'X',
                CHR (0),
                0,
                CHR (0),
                'X',
                CHR (1),
                0,
                CHR (0),
                0,
                CHR (0),
                0,
                CHR (0),
                CHR (0),
                'X',
                0);


   UPDATE DOPROSTEP_DBT
      SET T_AUTOEXECUTESTEP = CHR (0)
    WHERE T_BLOCKID = 204703 AND T_NUMBER_STEP = 25;

   UPDATE DOPROSTEP_DBT
      SET T_PREVIOUS_STEP = 10
    WHERE T_BLOCKID = 204702 AND T_NUMBER_STEP = 15;

   DELETE FROM DOPRSBLCK_DBT
         WHERE T_STATUSKINDID = 46501;

   INSERT INTO DOPRSBLCK_DBT (T_BLOCKID, T_STATUSKINDID, T_NUMVALUE)
        VALUES (204704, 46501, 4);

   INSERT INTO DOPRSBLCK_DBT (T_BLOCKID,
                              T_STATUSKINDID,
                              T_NUMVALUE,
                              T_DEFAULT)
        VALUES (204701,
                46501,
                2,
                CHR (0));

   INSERT INTO DOPRSBLCK_DBT (T_BLOCKID,
                              T_STATUSKINDID,
                              T_NUMVALUE,
                              T_DEFAULT)
        VALUES (204701,
                46501,
                4,
                'X');

   INSERT INTO DOPRSBLCK_DBT (T_BLOCKID,
                              T_STATUSKINDID,
                              T_NUMVALUE,
                              T_DEFAULT)
        VALUES (204702,
                46501,
                3,
                CHR (0));

   INSERT INTO DOPRSBLCK_DBT (T_BLOCKID,
                              T_STATUSKINDID,
                              T_NUMVALUE,
                              T_DEFAULT)
        VALUES (204703,
                46501,
                5,
                CHR (0));

   DELETE FROM DOPRSTVAL_DBT
         WHERE T_STATUSKINDID = 46501;

   INSERT INTO DOPRSTVAL_DBT (T_STATUSKINDID,
                              T_NUMVALUE,
                              T_NAME,
                              T_ELIMINATED)
        VALUES (46501,
                1,
                'Открыт',
                CHR (0));

   INSERT INTO DOPRSTVAL_DBT (T_STATUSKINDID,
                              T_NUMVALUE,
                              T_NAME,
                              T_ELIMINATED)
        VALUES (46501,
                2,
                'Отбор записей',
                CHR (0));

   INSERT INTO DOPRSTVAL_DBT (T_STATUSKINDID,
                              T_NUMVALUE,
                              T_NAME,
                              T_ELIMINATED)
        VALUES (46501,
                4,
                'Закрытие',
                CHR (0));

   INSERT INTO DOPRSTVAL_DBT (T_STATUSKINDID,
                              T_NUMVALUE,
                              T_NAME,
                              T_ELIMINATED)
        VALUES (46501,
                5,
                'Закрыт',
                CHR (0));

   INSERT INTO DOPRSTVAL_DBT (T_STATUSKINDID,
                              T_NUMVALUE,
                              T_NAME,
                              T_ELIMINATED)
        VALUES (46501,
                3,
                'Ожидение ответа от СНОБ',
                CHR (0));
END;
/