CREATE OR REPLACE PACKAGE BODY RSI_RsbOperDay IS

-- Получение даты последнего открытого опердня для филиала
  FUNCTION GetDepartmentOperDate( p_Branch IN INTEGER ) return DATE RESULT_CACHE is

    v_BranchOperDate DATE;

  begin

    select  NVL(max(t.t_Curdate), TO_DATE('01.01.0001', 'dd.mm.yyyy') ) into v_BranchOperDate
    from dcurdate_dbt t
    where t.t_Branch = p_Branch
      and t.t_IsClosed <> 'X';

    return v_BranchOperDate;

  end;

-- GetNextOperDate
  FUNCTION GetNextOperDate(
                           p_Department IN INTEGER
                          ,p_OperDate   IN DATE
                          )
  RETURN DATE RESULT_CACHE IS

    v_NextOperDate DATE;

  BEGIN

    -- Определяем следующую дату опердня для узла ТС
    SELECT NVL(min(t.t_Curdate), p_OperDate) INTO v_NextOperDate
    FROM dcurdate_dbt t
    WHERE t.t_Branch   = p_Department
      AND t.t_IsClosed = UNSET_CHAR
      AND t.t_Curdate  > p_OperDate;

    RETURN v_NextOperDate;

  END;

-- Получение параметров фазы лицевого счета
  PROCEDURE GetAccountPhase(
                            p_Phase       OUT INTEGER,
                            p_PhaseDate   OUT DATE,
                            p_Account  IN VARCHAR2,
                            p_Chapter  IN INTEGER,
                            p_FIID     IN INTEGER,
                            p_BankDate IN DATE
                           )
  IS
  BEGIN

    p_Phase     := 1;
    p_PhaseDate := null;

    BEGIN

      SELECT t_Phase, t_PhaseDate INTO p_Phase, p_PhaseDate
      FROM dopdphase_dbt phase, daccount_dbt acc
      WHERE acc.t_Account       = p_Account
        AND acc.t_Chapter       = p_Chapter
        AND acc.t_Code_Currency = p_FIID
        AND phase.t_AccountID = acc.t_AccountID
        AND phase.t_PhaseDate = p_BankDate;

    EXCEPTION

      WHEN no_data_found THEN NULL;

    END;

    IF p_Phase = 0 THEN

      BEGIN

        SELECT t_Phase, t_Curdate INTO p_Phase, p_PhaseDate
        FROM dcurdate_dbt curdate, daccount_dbt acc
        WHERE acc.t_Account       = p_Account
          AND acc.t_Chapter       = p_Chapter
          AND acc.t_Code_Currency = p_FIID
          AND curdate.t_Branch  = acc.t_Department
          AND curdate.t_Curdate = p_BankDate;

      EXCEPTION

        WHEN no_data_found THEN NULL;

      END;

    END IF;

  END;

-- GetPhase
  FUNCTION GetPhase(
                    p_Account  VARCHAR2,
                    p_Chapter  INTEGER,
                    p_FIID     INTEGER,
                    p_CurDate  DATE
                   ) return INTEGER is

    v_Phase INTEGER;

    v_PhaseDate DATE;

  BEGIN

    GetAccountPhase( v_Phase, v_PhaseDate, p_Account, p_Chapter, p_FIID, p_CurDate );

    RETURN v_Phase;

  END;

  -- Обработка лицевых счетов при открытии нового опердня
  PROCEDURE ProcessAccountsOnOpenDay(
                                     p_Department IN INTEGER,
                                     p_CurDate    IN DATE
                                    )
  IS

    v_CountDay INTEGER;

  BEGIN

    RETURN;

  END;

  -- Функция проверки возможности работы в операционном дне
  FUNCTION CheckOperDayPermission(p_CurDate IN DATE, p_Department IN NUMBER) RETURN BOOLEAN
  IS
    v_RetVal    BOOLEAN;
    v_IsClosed  CHAR(1);
  BEGIN
     v_RetVal := true;

     SELECT t_IsClosed INTO v_IsClosed FROM dcurdate_dbt WHERE t_Curdate = p_CurDate AND t_Branch = p_Department;

     IF v_IsClosed = 'X' AND RsbSessionData.OperCloseDateRestrict = 1 THEN
       v_RetVal := false;
     END IF;

     IF (v_RetVal = true AND (p_CurDate < RsbSessionData.OperCurDateMin OR p_CurDate > RsbSessionData.OperCurDateMax)) THEN
       v_RetVal := false;
     END IF;

    RETURN v_RetVal;
  EXCEPTION
    WHEN no_data_found THEN
      RETURN false;
  END;
end;
/
