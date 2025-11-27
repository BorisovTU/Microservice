DECLARE
  v_BlockID NUMBER := 460325;
  v_DateKindID NUMBER := 5100003;
  v_Cnt NUMBER := 0;
  v_OperBlockID NUMBER := 0;
BEGIN
  SELECT COUNT(1) INTO v_Cnt FROM doprblock_dbt WHERE t_blockid = v_BlockID;
  IF v_Cnt = 0 THEN
    INSERT INTO doprblock_dbt (t_blockid, t_name, t_dockind, t_parent, t_upgrade, t_version, t_versionweb)
                       VALUES (v_BlockID, 'Вынос на просрочку', 51, 0, CHR(0), CHR(1), 0);
  END IF;

  SELECT COUNT(1) INTO v_Cnt FROM doprkdate_dbt WHERE t_datekindid = v_DateKindID;
  IF v_Cnt = 0 THEN
    INSERT INTO doprkdate_dbt (t_datekindid, t_dockind, t_numberdate, t_namedate, t_eliminated)
                       VALUES (v_DateKindID, 51, 1, 'Вынос на просрочку', CHR(0));
  END IF;

  SELECT COUNT(1) INTO v_Cnt FROM doprostep_dbt WHERE t_blockid = v_BlockID;
  IF v_Cnt = 0 THEN
    INSERT INTO doprostep_dbt (t_blockid, t_number_step, t_kind_action, t_dayoffset, t_scale,
                               t_dayflag, t_calendarid, t_symbol, t_previous_step, t_modification,
                               t_carry_macro, t_print_macro, t_post_macro, t_notinuse, t_firststep,
                               t_name, t_datekindid, t_rev, t_autoexecutestep, t_onlyhandcarry,
                               t_isallowforoper, t_operorgroup, t_restrictearlyexecution, t_usertypes, t_initdatekindid,
                               t_askfordate, t_backout, t_isbackoutgroup, t_massexecutemode, t_iscase,
                               t_isdistaffexecute, t_skipinitafterplandate, t_masspacksize) 
                       VALUES (v_BlockID, 27, 1, 0, 0,
                               CHR(0), 0, 'в', 0, 0,
                               'ExpirateDebt.mac', CHR(1), CHR(1), CHR(0), CHR(0),
                               'Вынос на просрочку', 5100003, CHR(0), CHR(88), CHR(0),
                               0, CHR(0), CHR(88), CHR(1), 0,
                               CHR(0), 0, CHR(0), 0, CHR(0),
                               CHR(0), CHR(0), 0);
  END IF;

  SELECT COUNT(1) INTO v_Cnt FROM doproblck_dbt WHERE t_blockid = v_BlockID;
  IF v_Cnt = 0 THEN
    INSERT INTO doproblck_dbt (t_operblockid, t_kind_operation, t_blockid, t_sort, t_notinuse, t_noinsert, t_noreplace, t_nocloseinsert, t_ismanual, t_symbolsforinsertion, t_symbol)
                       VALUES (0, 4603, v_BlockID, 27, CHR(0), CHR(0), CHR(0), 'X', 'X', 'ОИ', 'В') RETURNING t_operblockid INTO v_OperBlockID;
              
    INSERT INTO doprcblck_dbt (t_operblockid, t_statuskindid, t_numvalue, t_condition)
                       VALUES (v_OperBlockID, 512, 3, 0);
  END IF;
END;
/