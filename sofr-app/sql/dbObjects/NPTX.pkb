CREATE OR REPLACE PACKAGE BODY NPTX
IS
  -- Определяет по виду сделки, является ли она продажей (выбытием). Для сделки из двух частей - по первой части
  FUNCTION IsSale( p_Kind IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
  BEGIN
    RETURN
      RSI_NPTX.IsSale( p_Kind );
  END IsSale;

  -- Определяет по типу сделки, является ли она виртуальной.
  FUNCTION IsVirtual( p_Type IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
  BEGIN
    RETURN
      RSI_NPTX.IsVirtual( p_Type );
  END IsVirtual;

  -- Проверки пользователей для операций NPTXOP
  FUNCTION Check_Document( pMode IN NUMBER,
                           pDoc IN OUT NOCOPY DNPTXOP_DBT%ROWTYPE,
                           pOldDoc IN DNPTXOP_DBT%ROWTYPE ) RETURN NUMBER
  IS
    v_stat NUMBER(10) := 0;
  BEGIN
    IF pMode = 3 THEN --редактирование операции
      --здесь должны быть проверки при редактировании
      v_stat := 0;

    ELSIF pMode = 1 THEN --удаление
      --здесь должны быть проверки при удалении
      v_stat := 0;
    END IF;

    RETURN v_stat;
  END;

END NPTX;
/