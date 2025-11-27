--
-- Триггер на таблицу DBATCH_MCOPENACC_TMP для инициализации поля T_RECID
--
CREATE OR REPLACE TRIGGER DBATCH_MCOPENACC_TMP_RECID
  FOR INSERT ON DBATCH_MCOPENACC_TMP 
COMPOUND TRIGGER

  v_RecID INTEGER;

  --
  -- перед выполнением SQL-команды
  --
  BEFORE STATEMENT IS
  BEGIN

    SELECT NVL(MAX(t_RecID), 0) INTO v_RecID
    FROM dbatch_mcopenacc_tmp;

  END BEFORE STATEMENT;

  --
  -- перед выполнением вставки конкретной записи
  --
  BEFORE EACH ROW IS
  BEGIN

    --
    -- следующее значение поля t_RecID
    --
    v_RecID := v_RecID + 1;

    --
    -- устанавливаем значение поля t_RecID
    --
    :new.t_RecID := v_RecID;

  END BEFORE EACH ROW;

END DBATCH_MCOPENACC_TMP_RECID;
/
