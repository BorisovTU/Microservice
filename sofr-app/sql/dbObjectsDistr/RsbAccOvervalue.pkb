CREATE OR REPLACE PACKAGE BODY RSI_RsbAccOvervalue IS

  --
  -- Определить, являются ли счета парными
  --
  FUNCTION definePairExRateAccounts( p_ExRateAccountPlus    IN  STRING
                                    ,p_ExRateAccountMinus   IN  STRING
                                    ,p_Chapter              IN  INTEGER
                                    ) RETURN CHAR deterministic 
  
  IS

    v_IsPairExRateAccounts CHAR;

    v_PairFlagAccPlus  INTEGER;
    v_PairFlagAccMinus INTEGER;

    v_PairAccountAccPlus  daccount_dbt.t_PairAccount%TYPE;
    v_PairAccountAccMinus daccount_dbt.t_PairAccount%TYPE;

  BEGIN

    v_IsPairExRateAccounts := UNSET_CHAR;

    BEGIN

      SELECT instr(t_Type_Account, TA_PAIR), t_PairAccount INTO v_PairFlagAccPlus, v_PairAccountAccPlus
      FROM daccount_dbt
      WHERE t_Chapter       = p_Chapter
        AND t_Account       = p_ExRateAccountPlus
        AND t_Code_Currency = NATCUR;

      SELECT instr(t_Type_Account, TA_PAIR), t_PairAccount INTO v_PairFlagAccMinus, v_PairAccountAccMinus
      FROM daccount_dbt
      WHERE t_Chapter       = p_Chapter
        AND t_Account       = p_ExRateAccountMinus
        AND t_Code_Currency = NATCUR;

      IF v_PairFlagAccPlus  <> 0 AND
         v_PairFlagAccMinus <> 0 AND
         v_PairAccountAccPlus  = p_ExRateAccountMinus AND
         v_PairAccountAccMinus = p_ExRateAccountPlus
      THEN

        v_IsPairExRateAccounts := SET_CHAR;

      END IF;

    EXCEPTION

      WHEN NO_DATA_FOUND THEN NULL;

    END;

    RETURN v_IsPairExRateAccounts;

  END;

  --
  -- Выбрать парный счет курсовой разницы
  --
  FUNCTION selectPairExRateAccounts( p_ExRateAccountPlus  IN OUT STRING
                                    ,p_ExRateAccountMinus IN OUT STRING
                                    ,p_Chapter            IN INTEGER
                                    ,p_RegDate            IN DATE
                                    ,p_PairMode           IN INTEGER
                                    ,p_ErrorMessage       OUT daccovervalue_tmp.t_ErrorMessage%TYPE
                                   )
  RETURN INTEGER
  IS

    v_stat INTEGER;

    ARHCARRY CONSTANT INTEGER := 23; -- признак следа архивной проводки

    v_AccountID daccount_dbt.t_AccountID%TYPE;

    v_CarryFlagAccPlus  INTEGER;
    v_CarryFlagAccMinus INTEGER;

    v_RestAccPlus  drestdate_dbt.t_Rest%TYPE;
    v_RestAccMinus drestdate_dbt.t_Rest%TYPE;

  BEGIN

    v_stat := 0;

    IF p_PairMode <> PAIR_MODE_NONE THEN

      --
      -- определяем количество проводок по счету положительных курсовых разниц
      --

      v_CarryFlagAccPlus := 0;

      BEGIN

        SELECT v_AccountID INTO v_AccountID
        FROM daccount_dbt
        WHERE t_Chapter       = p_Chapter
          AND t_Code_Currency = NATCUR
          AND t_Account       = p_ExRateAccountPlus;

        SELECT count(0) INTO v_CarryFlagAccPlus
        FROM dacctrn_dbt
        WHERE t_State         = 1
          AND t_Date_Carry    = p_RegDate
          AND t_Result_Carry <> ARHCARRY
          AND (t_AccountID_Payer    = v_AccountID
            OR t_AccountID_Receiver = v_AccountID);

      EXCEPTION

        WHEN NO_DATA_FOUND THEN NULL;

      END;

      --
      -- определяем количество проводок по счету отрицательных курсовых разниц
      --

      v_CarryFlagAccMinus := 0;

      BEGIN

        SELECT v_AccountID INTO v_AccountID
        FROM daccount_dbt
        WHERE t_Chapter       = p_Chapter
          AND t_Code_Currency = NATCUR
          AND t_Account       = p_ExRateAccountMinus;

        SELECT count(0) INTO v_CarryFlagAccMinus
        FROM dacctrn_dbt
        WHERE t_State         = 1
          AND t_Date_Carry    = p_RegDate
          AND t_Result_Carry <> ARHCARRY
          AND (t_AccountID_Payer    = v_AccountID
            OR t_AccountID_Receiver = v_AccountID);

      EXCEPTION

        WHEN NO_DATA_FOUND THEN NULL;

      END;

      -- если были проводки только по счету положительных курсовых разниц
      IF v_CarryFlagAccPlus <> 0 AND v_CarryFlagAccMinus = 0 THEN

        -- в проводке переоценки участвует счет положительных курсовых разниц
        p_ExRateAccountMinus := p_ExRateAccountPlus;

      ELSE

        -- если были проводки только по счету отрицательных курсовых разниц
        IF v_CarryFlagAccMinus <> 0 AND v_CarryFlagAccPlus = 0 THEN

          -- в проводке переоценки участвует счет отрицательных курсовых разниц
          p_ExRateAccountPlus := p_ExRateAccountMinus;


        ELSE

          --
          -- определяем остатки на счетах курсовых разниц
          --
          v_RestAccPlus  := RSI_Rsb_Account.RestALL( p_ExRateAccountPlus , p_Chapter, NATCUR, p_RegDate );
          v_RestAccMinus := RSI_Rsb_Account.RestALL( p_ExRateAccountMinus, p_Chapter, NATCUR, p_RegDate );

          -- если остаток есть только на счете положительных курсовых разниц
          IF v_RestAccPlus <> 0 AND v_RestAccMinus = 0 THEN

            -- в проводке переоценки участвует счет положительных курсовых разниц
            p_ExRateAccountMinus := p_ExRateAccountPlus;

          ELSE

            -- если остаток есть только на счете отрицательных курсовых разниц
            IF v_RestAccMinus <> 0 AND v_RestAccPlus = 0 THEN

              -- в проводке переоценки участвует счет отрицательных курсовых разниц
              p_ExRateAccountPlus := p_ExRateAccountMinus;

            ELSE

              -- если остатка нет ни на одном из счетов курсовых разниц
              IF v_RestAccMinus = 0 AND v_RestAccPlus = 0 THEN

                -- в проводке переоценки участвует счет положительных курсовых разниц
                p_ExRateAccountMinus := p_ExRateAccountPlus;

              ELSE

                -- остаток есть на обоих счетах курсовых разниц
                IF p_PairMode <> PAIR_MODE_CHECK_REST THEN

                  -- в проводке переоценки участвует счет положительных курсовых разниц
                  p_ExRateAccountMinus := p_ExRateAccountPlus;

                ELSE

                  v_stat := 1;

                  p_ErrorMessage := 'На дату урегулирования были проводки по обоим парным счетам курсовых разниц';

                END IF;

              END IF;

            END IF;

          END IF;

        END IF;

      END IF;

    END IF;

    RETURN v_stat;

  END;

  --
  -- Определить счета курсовых разниц
  --
  PROCEDURE GetExRateAccounts( p_Version IN INTEGER )
  IS

    v_t_Chapter Chapter_t;

    v_t_ExRateAccountPlus  ExRateAccountPlus_t;
    v_t_ExRateAccountMinus ExRateAccountMinus_t;

    v_IsPairExRateAccounts daccovervalue_tmp.t_IsPairExRateAccounts%TYPE;

  BEGIN

    --
    -- Устанавливаем счета курсовых разниц, если они еще не были определены
    --
    UPDATE daccovervalue_tmp accovervalue
    SET (accovervalue.t_ExRateAccountPlus, accovervalue.t_ExRateAccountMinus) = (SELECT accratediff.t_Account_Plus, accratediff.t_Account_Minus
                                                                                 FROM daccratediff_dbt accratediff
                                                                                 WHERE accratediff.t_Version       = p_Version
                                                                                   AND accratediff.t_Cover_Account = accovervalue.t_Account
                                                                                   AND accratediff.t_Chapter       = accovervalue.t_Chapter
                                                                                   AND accratediff.t_Code_Currency = accovervalue.t_Code_Currency)
    WHERE t_SkipAccount = 0
       AND (accovervalue.t_ExRateAccountPlus IS NULL OR accovervalue.t_ExRateAccountMinus IS NULL);


    --
    -- устанавливаем признаки парности счетов курсовых разниц
    --

    UPDATE daccovervalue_tmp
      SET t_IsPairExRateAccounts = definePairExRateAccounts(t_ExRateAccountPlus, t_ExRateAccountMinus, t_Chapter)
      WHERE t_SkipAccount = 0
        AND t_ExRateAccountPlus  IS NOT NULL
        AND t_ExRateAccountMinus IS NOT NULL;


     /*SELECT DISTINCT t_ExRateAccountPlus, t_ExRateAccountMinus, t_Chapter BULK COLLECT INTO v_t_ExRateAccountPlus, v_t_ExRateAccountMinus, v_t_Chapter
    FROM daccovervalue_tmp
    WHERE t_SkipAccount = 0
      AND t_ExRateAccountPlus  IS NOT NULL
      AND t_ExRateAccountMinus IS NOT NULL;


    IF v_t_ExRateAccountPlus.count <> 0 THEN

      FOR i IN v_t_ExRateAccountPlus.First..v_t_ExRateAccountPlus.Last LOOP

        v_IsPairExRateAccounts := definePairExRateAccounts( v_t_ExRateAccountPlus(i), v_t_ExRateAccountMinus(i), v_t_Chapter(i) );

        UPDATE daccovervalue_tmp
        SET t_IsPairExRateAccounts = v_IsPairExRateAccounts
        WHERE t_SkipAccount = 0
          AND t_Chapter            = v_t_Chapter(i)
          AND t_ExRateAccountPlus  = v_t_ExRateAccountPlus(i)
          AND t_ExRateAccountMinus = v_t_ExRateAccountMinus(i);

      END LOOP;

    END IF;*/

  END;


  --
  -- Первоначальная инициализация процедуры переоценки
  --
  PROCEDURE InitProcedure( p_Version IN INTEGER )
  IS

    TYPE Chapter_t  IS TABLE OF daccovervalue_tmp.t_Chapter%TYPE;
    TYPE Currency_t IS TABLE OF daccovervalue_tmp.t_Code_Currency%TYPE;
    TYPE Account_t  IS TABLE OF daccovervalue_tmp.t_Account%TYPE;

    v_Chapter  Chapter_t;
    v_Currency Currency_t;
    v_Account  Account_t;

  BEGIN

    UPDATE daccovervalue_tmp
    SET t_SkipAccount = 0;

    --
    -- Определяем счета курсовых разниц
    --
    GetExRateAccounts( p_Version );


  END;

  --
  -- Определение сумм переоценки по урегулируемым счетам покрытия
  --
  FUNCTION procCalcExRateSum( p_RegDate IN DATE, p_RestDate IN DATE, p_ZeroRest IN CHAR )
  RETURN INTEGER
  IS

    TYPE Chapter_t  IS TABLE OF daccovervalue_tmp.t_Chapter%TYPE;
    TYPE Currency_t IS TABLE OF daccovervalue_tmp.t_Code_Currency%TYPE;
    TYPE Account_t  IS TABLE OF daccovervalue_tmp.t_Account%TYPE;

    v_Chapter  Chapter_t;
    v_Currency Currency_t;
    v_Account  Account_t;

    v_CurrRest drestdate_dbt.t_Rest%TYPE;

    v_RestEquivalent daccovervalue_tmp.t_RestEquivalent%TYPE;

    v_stat INTEGER;

  BEGIN

    v_stat := 0;


   /* -- на  NTEST не работает 
    execute immediate 'truncate table daccovervalue2_tmp';

   insert \*+ parallel(8) noappend enable_parallel_dml *\ into daccovervalue2_tmp
          select \*+ parallel(8) *\ * from table(RSI_RsbAccOvervalue.procCalcExRateSumAcc(
                       cursor(select  * from daccovervalue_tmp d 
                                  where t_ExRateSum IS NULL AND t_SkipAccount = 0 )
                       ,p_RegDate 
                       ,p_RestDate 
                       ,p_ZeroRest))d ;

   select decode(count(1),0,0,1) into v_stat  from daccovervalue2_tmp where t_ErrorMessage is not null ;     

   MERGE \*+ use_hash(r,d) *\  into daccovervalue_tmp d
   using  (select  * 
            from daccovervalue2_tmp d ) r
     on  (  d.t_Chapter       = r.t_Chapter    
       and d.t_Code_Currency = r.t_Code_Currency
       and d.t_Account       = r.t_Account )
      when matched then update set
          d.t_Restequivalent = r.t_Restequivalent
        , d.t_Rest           = r.t_Rest
        , d.t_Skipaccount    = r.t_Skipaccount
        , d.t_Exratesum      = r.t_Exratesum
        , d.t_ErrorMessage   = r.t_ErrorMessage ;*/

  
    --
    -- Определяем остатки на счетах покрытия
    --

  UPDATE daccovervalue_tmp
    SET t_RestEquivalent = 0, t_Rest = round(RSI_Rsb_Account.RestALL(t_Account, t_Chapter, t_Code_Currency, p_RestDate, NATCUR), 2)
    WHERE t_ExRateSum IS NULL
      AND t_SkipAccount = 0;

    --
    -- Определяем эквиваленты остатков на валютных счетов
    --
   
    -- находим все валютные счета, покрытия которых урегулируются
    SELECT t_Chapter, t_Code_Currency, t_Account BULK COLLECT INTO v_Chapter, v_Currency, v_Account
    FROM daccovervalue_tmp
    WHERE t_ExRateSum IS NULL
      AND t_SkipAccount   = 0;

    IF v_Account.count <> 0 THEN

      FOR i IN v_Account.First..v_Account.Last LOOP

        -- определяем остаток на валютном счетах
        v_CurrRest := round(RSI_Rsb_Account.RestALL(v_Account(i), v_Chapter(i), v_Currency(i), p_RestDate), 2);

        -- если урегулируем только счета с нулевым остатком, и остаток на валютном счете не нулевой, то сбросим t_RestEquivalent
        IF p_ZeroRest <> UNSET_CHAR AND v_CurrRest <> 0 THEN

          UPDATE daccovervalue_tmp
          SET t_RestEquivalent = NULL
          WHERE t_Chapter       = v_Chapter(i)
            AND t_Code_Currency = v_Currency(i)
            AND t_Account       = v_Account(i);

        ELSE

          -- определяем эквивалент остатка на валютном счете в нац.валюте
          v_RestEquivalent := RSI_RSB_FIInstr.ConvSum( v_CurrRest, v_Currency(i), NATCUR, p_RegDate, 2 );

          IF v_RestEquivalent IS NOT NULL THEN

            UPDATE daccovervalue_tmp
            SET t_RestEquivalent = t_RestEquivalent + v_RestEquivalent
            WHERE t_Chapter       = v_Chapter(i)
              AND t_Code_Currency = v_Currency(i)
              AND t_Account       = v_Account(i)
              AND t_RestEquivalent IS NOT NULL;

          ELSE

            v_stat := 1;

            UPDATE daccovervalue_tmp
            SET t_SkipAccount = 1
               ,t_RestEquivalent = NULL
               ,t_ErrorMessage = 'Ошибка расчета эквивалента в национальной валюте остатка на счете ' || v_Account(i)
            WHERE t_Chapter       = v_Chapter(i)
              AND t_Code_Currency = v_Currency(i)
              AND t_Account       = v_Account(i);

          END IF;

        END IF;

      END LOOP;

    END IF;
    --
    -- Определяем курсовые разницы
    --
    UPDATE daccovervalue_tmp
    SET t_ExRateSum = t_Rest - t_RestEquivalent
    WHERE t_ExRateSum IS NULL
      AND t_SkipAccount = 0
      AND t_RestEquivalent IS NOT NULL;

   
    --
    -- Возвращаем признак того, что при выполнении расчета сумм были ошибки
    --
    RETURN v_stat;

  END;
  

 function procCalcExRateSumAcc(pc_daccovervalue tc_daccovervalue
                             ,p_RegDate        in date
                             ,p_RestDate       in date
                             ,p_ZeroRest       in char) return tt_daccovervalue
  parallel_enable(partition pc_daccovervalue by any)
  pipelined as
  vr_in_daccovervalue daccovervalue_tmp%rowtype;
  vr_daccovervalue    daccovervalue_tmp%rowtype;
  p_Account           daccovervalue_tmp.t_account%type;
  p_Chapter           daccovervalue_tmp.t_chapter%type;
  p_Code_Currency     daccovervalue_tmp.t_code_currency%type;
  v_CurrRest          number;
  v_RestEquivalent    number;
  v_stat              number := 0;
begin
  loop
    fetch pc_daccovervalue
      into vr_in_daccovervalue;
    exit when pc_daccovervalue%notfound;
    p_Chapter                         := vr_in_daccovervalue.t_chapter;
    p_Code_Currency                   := vr_in_daccovervalue.t_code_currency;
    p_Account                         := vr_in_daccovervalue.t_account;
    vr_daccovervalue.t_accountid      := vr_in_daccovervalue.t_accountid;
    vr_daccovervalue.t_chapter        := vr_in_daccovervalue.t_chapter;
    vr_daccovervalue.t_code_currency  := vr_in_daccovervalue.t_code_currency;
    vr_daccovervalue.t_account        := vr_in_daccovervalue.t_account;
    vr_daccovervalue.t_RestEquivalent := 0;
    vr_daccovervalue.t_Rest           := round(RSI_Rsb_Account.RestALL(p_Account, p_Chapter, p_Code_Currency, p_RestDate, NATCUR), 2);
    vr_daccovervalue.t_SkipAccount    := 0;
    vr_daccovervalue.t_ExRateSum      := null;
    vr_daccovervalue.t_ErrorMessage   := null;
    --
    -- Определяем эквиваленты остатков на валютных счетов
    --
    -- определяем остаток на валютном счетах
    v_CurrRest := round(RSI_Rsb_Account.RestALL(p_Account, p_Chapter, p_Code_Currency, p_RestDate), 2);
    -- если урегулируем только счета с нулевым остатком, и остаток на валютном счете не нулевой, то сбросим t_RestEquivalent
    if p_ZeroRest <> UNSET_CHAR
       and v_CurrRest <> 0
    then
      vr_daccovervalue.t_RestEquivalent := null;
    else
      -- определяем эквивалент остатка на валютном счете в нац.валюте
      v_RestEquivalent := RSI_RSB_FIInstr.ConvSum(v_CurrRest, p_Code_Currency, NATCUR, p_RegDate, 2);
      if v_RestEquivalent is not null
      then
        vr_daccovervalue.t_RestEquivalent := vr_daccovervalue.t_RestEquivalent + v_RestEquivalent;
        vr_daccovervalue.t_ExRateSum      := vr_daccovervalue.t_Rest - vr_daccovervalue.t_RestEquivalent;
      else
        v_stat                            := 1;
        vr_daccovervalue.t_SkipAccount    := 1;
        vr_daccovervalue.t_RestEquivalent := null;
        vr_daccovervalue.t_ErrorMessage   := 'Ошибка расчета эквивалента в национальной валюте остатка на счете ' || p_Account;
      end if;
    end if;
    pipe row(vr_daccovervalue);
  end loop;
end;


  --
  -- Определение сумм переоценки по урегулируемым счетам покрытия
  --
  FUNCTION CalcExRateSum( p_RegDate IN DATE, p_RestDate IN DATE, p_ZeroRest IN CHAR )
  RETURN INTEGER
  IS
  BEGIN

    --
    -- Первоначальная инициализация
    --
    UPDATE daccovervalue_tmp
    SET t_ExRateSum = NULL;

    --
    -- Определяем суммы переоценки
    --
    RETURN procCalcExRateSum( p_RegDate, p_RestDate, p_ZeroRest );

  END;

  --
  -- Переопределение сумм переоценки по урегулируемым счетам покрытия
  --
  FUNCTION ReCalcExRateSum( p_RegDate IN DATE, p_RestDate IN DATE, p_ZeroRest IN CHAR )
  RETURN INTEGER
  IS
  BEGIN

    --
    -- Сбрасываем суммы переоценки
    --
    UPDATE daccovervalue_tmp
    SET t_ExRateSum = NULL
    WHERE t_SkipAccount = 0
      AND t_ExRateSum  <> 0;

    --
    -- Определяем суммы переоценки
    --
    RETURN procCalcExRateSum( p_RegDate, p_RestDate, p_ZeroRest );

  END;

  --
  -- Определение счетов курсовых разниц, которые будут участвовать в проводках переоценки
  --
  FUNCTION DefineExRateAccount( p_RegDate IN DATE, p_PairMode IN INTEGER, p_Version IN INTEGER )
  RETURN INTEGER
  IS

    v_stat INTEGER;

    v_t_Chapter Chapter_t;

    v_t_ExRateAccountPlus  ExRateAccountPlus_t;
    v_t_ExRateAccountMinus ExRateAccountMinus_t;

    v_ExRateAccountPlus  daccovervalue_tmp.t_ExRateAccountPlus%TYPE;
    v_ExRateAccountMinus daccovervalue_tmp.t_ExRateAccountMinus%TYPE;

    v_SkipAccount INTEGER;

    v_ErrorMessage daccovervalue_tmp.t_ErrorMessage%TYPE;

  BEGIN

    v_stat := 0;

    --
    -- доопределяем счета курсовых разниц
    --
    GetExRateAccounts( p_Version );

    --
    -- первоначальная инициализация
    --
   UPDATE daccovervalue_tmp
    SET t_ExRateAccount = NULL
      , t_SkipAccount = case when t_SkipAccount = 0 AND (t_ExRateAccountPlus IS NULL OR t_ExRateAccountMinus IS NULL) 
                          then 1 else t_SkipAccount end ;

   /*UPDATE daccovervalue_tmp
    SET t_ExRateAccount = NULL;

    UPDATE daccovervalue_tmp
    SET t_SkipAccount = 1
    WHERE t_SkipAccount = 0 AND (t_ExRateAccountPlus IS NULL OR t_ExRateAccountMinus IS NULL);*/

    --
    -- Сначала обрабатываем записи с не парными счетами курсовых разниц
    --


    UPDATE daccovervalue_tmp
    SET t_ExRateAccount = case when t_ExRateSum > 0 then t_ExRateAccountPlus else t_ExRateAccountMinus end 
    WHERE t_SkipAccount          = 0
      AND t_IsPairExRateAccounts = UNSET_CHAR
      AND t_ExRateSum            != 0;

 
 

    /*-- устанавливаем счета положительных курсовых разниц
    UPDATE daccovervalue_tmp
    SET t_ExRateAccount = t_ExRateAccountPlus
    WHERE t_SkipAccount          = 0
      AND t_IsPairExRateAccounts = UNSET_CHAR
      AND t_ExRateSum            > 0;


    -- устанавливаем счета отрицательных курсовых разниц
    UPDATE daccovervalue_tmp
    SET t_ExRateAccount = t_ExRateAccountMinus
    WHERE t_SkipAccount          = 0
      AND t_IsPairExRateAccounts = UNSET_CHAR
      AND t_ExRateSum            < 0;*/

    --
    -- Теперь обрабатываем записи с не парными счетами курсовых разниц
    --

    SELECT DISTINCT t_ExRateAccountPlus, t_ExRateAccountMinus, t_Chapter BULK COLLECT INTO v_t_ExRateAccountPlus, v_t_ExRateAccountMinus, v_t_Chapter
    FROM daccovervalue_tmp accovervalue
    WHERE t_SkipAccount          = 0
      AND t_IsPairExRateAccounts = SET_CHAR
      AND t_ExRateSum           <> 0;

    IF v_t_ExRateAccountPlus.count <> 0 THEN

      FOR i IN v_t_ExRateAccountPlus.First..v_t_ExRateAccountPlus.Last LOOP

        v_ExRateAccountPlus  := v_t_ExRateAccountPlus(i);
        v_ExRateAccountMinus := v_t_ExRateAccountMinus(i);

        v_SkipAccount := selectPairExRateAccounts( v_ExRateAccountPlus, v_ExRateAccountMinus, v_t_Chapter(i), p_RegDate, p_PairMode, v_ErrorMessage );

        UPDATE daccovervalue_tmp
        SET t_ExRateAccount = case when t_ExRateSum > 0 then v_ExRateAccountPlus else v_ExRateAccountMinus end
            , t_SkipAccount = v_SkipAccount
            , t_ErrorMessage = v_ErrorMessage
        WHERE t_SkipAccount = 0
          AND t_Chapter            = v_t_Chapter(i)
          AND t_ExRateAccountPlus  = v_t_ExRateAccountPlus(i)
          AND t_ExRateAccountMinus = v_t_ExRateAccountMinus(i)
          AND t_ExRateSum != 0;
 
       /*--
        -- устанавливаем счет положительных курсовых разниц
        --
        UPDATE daccovervalue_tmp
        SET t_ExRateAccount = v_ExRateAccountPlus, t_SkipAccount = v_SkipAccount, t_ErrorMessage = v_ErrorMessage
        WHERE t_SkipAccount        = 0
          AND t_Chapter            = v_t_Chapter(i)
          AND t_ExRateAccountPlus  = v_t_ExRateAccountPlus(i)
          AND t_ExRateAccountMinus = v_t_ExRateAccountMinus(i)
          AND t_ExRateSum > 0;

        --
        -- устанавливаем счет отрицательных курсовых разниц
        --
        UPDATE daccovervalue_tmp
        SET t_ExRateAccount = v_ExRateAccountMinus, t_SkipAccount = v_SkipAccount, t_ErrorMessage = v_ErrorMessage
        WHERE t_SkipAccount = 0
          AND t_Chapter            = v_t_Chapter(i)
          AND t_ExRateAccountPlus  = v_t_ExRateAccountPlus(i)
          AND t_ExRateAccountMinus = v_t_ExRateAccountMinus(i)
          AND t_ExRateSum < 0;*/

        --
        -- сохраним признак возникновения ошибки определения счетов курсовых разниц
        --
        IF v_SkipAccount <> 0 THEN
          v_stat := 1;
        END IF;

      END LOOP;

    END IF;

    RETURN v_stat;

  END;

  --
  -- Определить, какой счет курсовой разницы использовать для переоценки
  --
  FUNCTION CorrectExRateAccount( p_ExRateAccountPlus  IN OUT STRING
                                ,p_ExRateAccountMinus IN OUT STRING
                                ,p_Chapter            IN INTEGER
                                ,p_RegDate            IN DATE
                                ,p_PairMode           IN INTEGER
                                ,p_ErrorMessage       OUT daccovervalue_tmp.t_ErrorMessage%TYPE
                               )
  RETURN INTEGER
  IS

    v_stat INTEGER;

  BEGIN

    v_stat := 0;

    IF definePairExRateAccounts(p_ExRateAccountPlus, p_ExRateAccountMinus, p_Chapter) = SET_CHAR THEN

      v_stat := selectPairExRateAccounts( p_ExRateAccountPlus, p_ExRateAccountMinus, p_Chapter, p_RegDate, p_PairMode, p_ErrorMessage );

    END IF;

    RETURN v_stat;

  END;

--
  -- Получить начальное значение номера проводки переоценки
  --
  FUNCTION GetStartNumberDoc( p_ParamID INTEGER )
  RETURN INTEGER
  IS

    v_Count INTEGER;

    v_NextNumberDoc drevalueaccprm_dbt.t_NextNumberDoc%TYPE;

    v_NumberDoc INTEGER;

    e_to_number EXCEPTION;
    PRAGMA EXCEPTION_INIT (e_to_number, -06502);

  BEGIN

    v_NumberDoc := -1;

    SELECT count(0) INTO v_Count
    FROM daccovervalue_tmp
    WHERE t_SkipAccount = 0
      AND t_ExRateSum <> 0
      AND (t_Numb_Document = chr(1) OR t_Numb_Document IS NULL);

    IF v_Count > 0 THEN 
    
      SELECT t_NextNumberDoc INTO v_NextNumberDoc
      FROM drevalueaccprm_dbt
      WHERE t_ParamID = p_ParamID
      FOR UPDATE;

      BEGIN
        v_NumberDoc := TO_NUMBER(v_NextNumberDoc);
      EXCEPTION
        WHEN e_to_number THEN v_NumberDoc := 1;
      END;

      UPDATE drevalueaccprm_dbt
      SET t_NextNumberDoc = v_NumberDoc + v_Count
      WHERE t_ParamID = p_ParamID;

    END IF;

    RETURN v_NumberDoc;

  END;

END;
/
