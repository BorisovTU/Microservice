CREATE OR REPLACE PACKAGE BODY Rsb_SCTX
IS
  ----------------------------------------------------------------------------------------------
  --                                  Вспомогательные функции(скрытые)                        --
  ----------------------------------------------------------------------------------------------
    FUNCTION iif( Cond IN BOOLEAN, n1 IN NUMBER, n2 IN NUMBER )
      RETURN NUMBER
    IS
    BEGIN
      IF( Cond ) THEN
         RETURN n1;
      ELSE
         RETURN n2;
      END IF;
    END;

    FUNCTION iif( Cond IN BOOLEAN, n1 IN DATE, n2 IN DATE )
      RETURN DATE
    IS
    BEGIN
      IF( Cond ) THEN
         RETURN n1;
      ELSE
         RETURN n2;
      END IF;
    END;

    FUNCTION iif( Cond IN BOOLEAN, n1 IN VARCHAR2, n2 IN VARCHAR2 )
      RETURN VARCHAR2
    IS
    BEGIN
      IF( Cond ) THEN
         RETURN n1;
      ELSE
         RETURN n2;
      END IF;
    END;
  function GetNoteTextStr( v_ObjectType IN NUMBER, v_ObjectID IN VARCHAR2, v_NoteKind IN NUMBER, v_EndDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') )
      return VARCHAR2
      is
      v_Text dnotetext_dbt.t_text%TYPE;
    begin
      begin
        select t_Text into v_Text
        from (select t_Text
                from dnotetext_dbt
               where t_DocumentID = v_ObjectID and
                     t_ObjectType = v_ObjectType and
                     t_NoteKind = v_NoteKind
               order by t_Date desc)
        where ROWNUM = 1;

        return SUBSTR(rsb_struct.getString(v_Text),1,3);

      exception
        when NO_DATA_FOUND then return NULL;
        when OTHERS then return NULL;
      end;
  end;

  function GetNoteText( v_ObjectType IN NUMBER, v_ObjectID IN VARCHAR2, v_NoteKind IN NUMBER, v_EndDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') )
    return NUMBER
    is
    v_Text dnotetext_dbt.t_text%TYPE;
  begin
    begin
      select t_Text into v_Text
      from (select t_Text
              from dnotetext_dbt
             where t_DocumentID = v_ObjectID and
                   t_ObjectType = v_ObjectType and
                   t_NoteKind = v_NoteKind
             order by t_Date desc)
      where ROWNUM = 1;

      return rsb_struct.getDouble(v_Text);

    exception
      when NO_DATA_FOUND then return NULL;
      when OTHERS then return NULL;
    end;
  end;

    -- вывод сообщений об ошибке в протокол
    PROCEDURE TXPutMsg( in_LotID IN NUMBER,
                        in_FIID IN NUMBER,
                        in_ErrorType IN NUMBER,
                        in_ErrorStr IN VARCHAR2,
                        in_IsTrigger IN BOOLEAN DEFAULT FALSE )
    IS
    BEGIN

      IF in_ErrorType = TXMES_DEBUG AND NOT gl_IsDebug THEN
        RETURN;
      ELSIF in_ErrorType = TXMES_OPTIM AND NOT gl_IsOptim THEN
        RETURN;
      ELSIF in_ErrorType = TXMES_ERROR THEN
        gl_WasError := TRUE;
      END IF;

      INSERT INTO DSCTXMES_DBT ErrLog
         (
            ErrLog.T_LotID,
            ErrLog.T_FIID,
            ErrLog.T_TYPE,
            ErrLog.T_MESSAGE,
            ErrLog.T_MESDATE
         )
         VALUES
         (
            in_LotID, in_FIID, in_ErrorType, in_ErrorStr, SYSDATE
         );

      IF (in_ErrorType = TXMES_DEBUG OR in_ErrorType = TXMES_OPTIM OR in_ErrorType = TXMES_TEST) AND NOT in_IsTrigger THEN
        COMMIT; -- в отладочном режиме сразу фиксируем
      END IF;
    END; --TXPutMsg

    -- вывод сообщения о текущем действии
    PROCEDURE TXPutCurrActMsg( in_Str IN VARCHAR2 )
    IS
    BEGIN

      DELETE
        FROM DSCTXMES_DBT
       WHERE T_ID = -1
         AND T_TYPE = TXMES_PROCESS;

      INSERT INTO DSCTXMES_DBT ErrLog
         (
            ErrLog.T_ID,
            ErrLog.T_LotID,
            ErrLog.T_FIID,
            ErrLog.T_TYPE,
            ErrLog.T_MESSAGE,
            ErrLog.T_MESDATE
         )
         VALUES
         (
            -1, 0, -1, TXMES_PROCESS, in_Str, SYSDATE
         );

      COMMIT;
    END; --TXPutMsg

    PROCEDURE RSI_BeginCalculate( NameCalc IN VARCHAR2 )
    IS
    BEGIN

      EXECUTE IMMEDIATE 'TRUNCATE TABLE DSCTXMES_DBT';
      INSERT INTO DSCTXMES_DBT ErrLog
         (
            ErrLog.T_ID,
            ErrLog.T_LotID,
            ErrLog.T_FIID,
            ErrLog.T_TYPE,
            ErrLog.T_MESSAGE,
            ErrLog.T_MESDATE
         )
         VALUES
         (
            -2, 0, -1, TXMES_PROCESS, NameCalc, SYSDATE
         );

      COMMIT;
    END;

    PROCEDURE EndCalculate
    IS
    BEGIN
      DELETE
        FROM DSCTXMES_DBT
       WHERE T_ID = -2
         AND T_TYPE = TXMES_PROCESS;
    END; --TXPutMsg


    ------------------------------------------------------------------------------------------------------------------
    -- функция получает из настроек системы насройки НУ и заносит их в глобализм
    ------------------------------------------------------------------------------------------------------------------
    procedure GetSettingsTax(pOnlyRate IN NUMBER)
     is
    begin
      RateTypes.MaxRate    := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА МАКСИМАЛЬНАЯ ЦЕНА'    , 0);
      RateTypes.MinRate    := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА МИНИМАЛЬНАЯ ЦЕНА'     , 0);
      RateTypes.MediumRate := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0);
      RateTypes.ReuterRate := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СРЕДНЯЯ ЦЕНА РЕЙТЕР'  , 0);
      RateTypes.TaxRate    := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА НАЛОГОВАЯ ЦЕНА'       , 0);
      RateTypes.CloseRate  := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА ЦЕНА ЗАКРЫТИЯ'        , 0);
      RateTypes.CloseRateBl := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА ЦЕНА ЗАКРЫТ. БЛУМБЕРГ', 0);
      RateTypes.TaxReserv   := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА НАЛОГОВЫЙ РЕЗЕРВ', 0);
      RateTypes.InvRate     := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА ПРИЗНАВАЕМ. КОТИРОВКА', 0);

      if pOnlyRate = 0 then
        ReestrValue.V0 := Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V0', 0);
        ReestrValue.V1 := Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V1', 0);
        ReestrValue.V2 := Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V2', 0);
        ReestrValue.V3 := Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V3', 0);
        ReestrValue.V4 := Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V4', 0);
        ReestrValue.V5 := Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V5', 0);
        ReestrValue.V6 := Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V6', 0);
        ReestrValue.V9 := Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V9', 0);
        ReestrValue.V10:= Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V10',0);
        ReestrValue.V11:= Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V11',0);
        ReestrValue.V12:= Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V12',0);
        ReestrValue.V13:= Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V13',0);
        ReestrValue.V14:= Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V14',0);
        ReestrValue.V15:= Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V15',0);
        ReestrValue.V20:= Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V20',0);
        ReestrValue.ModeTax := Rsb_Common.GetRegBoolValue('SECUR\РЕЖИМ ХРАНИЛИЩА ДАННЫХ ДЛЯ НУ');
      end if;
    end; --GetSettingsTax

    ------------------------------------------------------------------------------------------------------------------
    ---- функция получает курс типа для ценной бумаги на дату ratedate или за период
    ---- от ratedate-Ndays до ratedate с максимальной датой начала дествия курса
    ------------------------------------------------------------------------------------------------------------------
    function SPGetRate( FIID IN NUMBER, ToFIID IN NUMBER, RateType IN NUMBER, RateDate IN DATE, NDays IN NUMBER, RD OUT DRATEDEF_DBT%ROWTYPE, pMarketCountry IN VARCHAR2 DEFAULT CHR(1), pOnlyRate IN NUMBER DEFAULT 0, pIsMinMax IN NUMBER DEFAULT 0, pCanUseCross IN NUMBER DEFAULT 0, pMarket_Place IN NUMBER DEFAULT -1, pIsForeignMarket IN NUMBER DEFAULT 0 )
     return NUMBER
      is
     t_RD        DRATEDEF_DBT%ROWTYPE;
     v_RateDate  DATE;
     v_RateID    NUMBER;
     v_SinceDate DATE;
     v_Rate      NUMBER;
     v_IsMrkt    BOOLEAN;
     v_IsMinMax  NUMBER;
     v_ToFIID    NUMBER;
    begin
      t_RD := NULL;

      if( RateType in (RateTypes.MinRate,RateTypes.MaxRate, RateTypes.MediumRate) ) then
        v_IsMrkt := True;
      else
        v_IsMrkt := False;
      end if;

      v_IsMinMax := 0;

      if pIsMinMax > 0 then
        v_IsMinMax := pIsMinMax;
      end if;

      v_RateDate := RateDate;
      v_ToFIID := ToFIID;

      v_Rate := RSI_RSB_FIInstr.FI_GetRate( FIID, v_ToFIID, RateType, v_RateDate, NDays, v_IsMinMax, v_RateID, v_SinceDate, v_IsMrkt, pMarketCountry, pIsForeignMarket, pOnlyRate, pCanUseCross, pMarket_Place );

      begin
        select * into t_RD
          from dratedef_dbt
         where t_RateID = v_RateID;
      exception
        when OTHERS then return 1;
      end;

      t_RD.t_SinceDate := v_SinceDate;
      t_RD.t_Rate      := v_Rate;

      RD := t_RD;

      return 0;
    exception
      when OTHERS then return 1;
    end;

    function GetInAvrWrtStartDate(p_DealId IN NUMBER, p_Date IN DATE DEFAULT TO_DATE('31-12-9999','DD-MM-YYYY'))
     return Date DETERMINISTIC
      is
     t_DateVal   VARCHAR2 (35);
     v_SinceDate DATE;
     v_OFBU      CHAR(1);
    begin
      v_SinceDate := p_Date;
      
      SELECT TICK.T_OFBU
        INTO v_OFBU
        FROM DDL_TICK_DBT TICK
       WHERE TICK.T_DEALID = p_DealId;

      IF(v_OFBU = chr(88)) THEN
        t_DateVal := RSB_SECUR.GetObjAttrName (RSB_SECUR.OBJTYPE_SECDEAL,
                                         117, /*Дата вывода облигаций из ДУ*/
                                         RSB_SECUR.GetMainObjAttr (
                                             RSB_SECUR.OBJTYPE_SECDEAL,
                                             LPAD (p_DealId, 34, '0'),
                                             117, /*Дата вывода облигаций из ДУ*/
                                             TRUNC(SYSDATE)));
        begin 
          v_SinceDate := TO_DATE(t_DateVal, 'DDMMYYYY');
        exception when OTHERS then v_SinceDate:= TO_DATE('31-12-9999', 'DD-MM-YYYY');
        end;
      end if;
      return v_SinceDate;
    exception
      when OTHERS then return TO_DATE('31-12-9999', 'DD-MM-YYYY');
    end;

    ------------------------------------------------------------------------------------------------------------------
    ---- функция проверяет есть ли курс типа для ценной бумаги на дату ratedate или за период
    ---- от ratedate-Ndays до ratedate с максимальной датой начала дествия курса
    ------------------------------------------------------------------------------------------------------------------
    function IsSPGetRate( FIID IN NUMBER, ToFIID IN NUMBER, RateType IN NUMBER, RateDate IN DATE, NDays IN NUMBER, pMarketCountry IN VARCHAR2 DEFAULT CHR(1), pOnlyRate IN NUMBER DEFAULT 0, pIsMinMax IN NUMBER DEFAULT 0, pCanUseCross IN NUMBER DEFAULT 0, pMarket_Place IN NUMBER DEFAULT -1, pIsForeignMarket IN NUMBER DEFAULT 0 )
     return NUMBER
      is
      RD DRATEDEF_DBT%ROWTYPE;
    begin
      return SPGetRate(FIID, ToFIID, RateType, RateDate, NDays, RD, pMarketCountry, pOnlyRate, pIsMinMax, pCanUseCross, pMarket_Place, pIsForeignMarket);
    exception
      when OTHERS then return 1;
    end;

    ------------------------------------------------------------------------------------------------------------------
    ---- функция определяет количество рабочих дней между датами
    ------------------------------------------------------------------------------------------------------------------
    function GetCountWorkDays( pDate1 IN DATE, pDate2 IN DATE )
     return NUMBER
      is
     vCount NUMBER := 0;
     vDate1 DATE;
     vDate2 DATE;
    begin

      if( pDate1 <= pDate2) then
         vDate1 := pDate1;
         vDate2 := pDate2;
      else
         vDate1 := pDate2;
         vDate2 := pDate1;
      end if;

      while( vDate1 < vDate2) loop
        if( RSI_RsbCalendar.IsWorkDay( vDate1 ) != 0 ) then
           vCount := vCount + 1;
        end if;
        vDate1 := vDate1 + 1;
      end loop;

      return vCount;

    end;

    ------------------------------------------------------------------------------------------------------------------
    ---- функция получает курс типа для ценной бумаги ближайшую к дате ratedate
    ------------------------------------------------------------------------------------------------------------------
    function SPGetRate_Ex( FIID IN NUMBER, ToFIID IN NUMBER, RateType IN NUMBER, RateDate IN DATE, MaxRateDate IN DATE, NDays IN NUMBER, RD OUT DRATEDEF_DBT%ROWTYPE, pOnlyRate IN NUMBER DEFAULT 0, pIsMinMax IN NUMBER DEFAULT 0, pCanUseCross IN NUMBER )
     return NUMBER
      is
     t_RD        DRATEDEF_DBT%ROWTYPE;
     t_RD2       DRATEDEF_DBT%ROWTYPE;
     v_Min       NUMBER;
     v_Min2      NUMBER;
     pDate       DATE;
     pDateMin    DATE;
     pDateMax    DATE;
    begin

      pDateMin := RateDate;
      pDateMax := MaxRateDate;

      while( pDateMin <> pDateMax ) loop

         pDate := pDateMin + round( ( pDateMax - pDateMin ) / 2 );

         if( SPGetRate( FIID, ToFIID, RateType, pDate, round( ( pDateMax - pDateMin ) / 2 ), t_RD2, CHR(1), pOnlyRate, pIsMinMax, pCanUseCross ) = 0 and
            (t_RD2.t_sincedate >= pDateMin)
           ) then
          exit when (pDate = pDateMax);
            pDateMax := pDate;
         else
            pDateMin := pDate;
         end if;

      end loop;

      if( SPGetRate( FIID, ToFIID, RateType, RateDate, NDays, t_RD, CHR(1), pOnlyRate, pIsMinMax, pCanUseCross ) = 0 ) then
         v_Min  := ABS(RateDate - t_RD.t_SinceDate);
         v_Min2 := ABS(RateDate - t_RD2.t_SinceDate);
         if( v_Min2 <= v_Min ) then
            RD := t_RD2;
         else
            RD := t_RD;
         end if;
      else
         if( t_RD2.t_SinceDate is not NULL ) then
            RD := t_RD2;
         else
            return 1;
         end if;
      end if;

      return 0;
    exception
      when OTHERS then return 1;
    end;

    --определить срочность сделки
    function DealIsTerm(DealID IN NUMBER,
                        FIID   IN NUMBER,
                        ValueDate IN DATE --фактическая, а в её отсутствии плановая дата поставки
                      )
      return NUMBER
    is
      vIsTerm NUMBER;
      vTick   ddl_tick_dbt%ROWTYPE;
      v_Attr1   NUMBER;
      v_Attr2   NUMBER;
      v_Attr3   NUMBER;
      v_Attr4   NUMBER;
      v_Attr5   NUMBER;

    begin
      vIsTerm := 0;

      begin
        SELECT * INTO vTick FROM ddl_tick_dbt where t_DealID = DealID;
        exception
          when OTHERS then return 0;
      end;

      begin
          SELECT COUNT(1) INTO v_Attr1
            FROM dobjatcor_dbt objatcor
           WHERE objatcor.t_ObjectType = 12 AND
                 objatcor.t_GroupID    = 20 AND
                 objatcor.t_Object     = LPAD( FIID, 10, '0' ) AND
                 objatcor.t_AttrID     = 1;
        exception
          when OTHERS then v_Attr1 := 0;
      end;

      --делка может быть срочной только если v_Attr1 != 0
      if v_Attr1 = 0 then
        vIsTerm := 0;
        return vIsTerm;
      end if;


      if vTick.t_DealDate <= TO_DATE('31.12.2009', 'DD.MM.YYYY') then
        begin
            SELECT COUNT(1) INTO v_Attr2
              FROM dobjatcor_dbt objatcor
             WHERE objatcor.t_ObjectType = 101 AND
                   objatcor.t_GroupID    = 20 AND
                   objatcor.t_Object     = LPAD( vTick.t_DealID, 34, '0' ) AND
                   objatcor.t_AttrID     = 1; --да
          exception
            when OTHERS then v_Attr2 := 0;
        end;

        --сделка может быть срочной только если v_Attr2 = 0
        if v_Attr2 <> 0 then
          vIsTerm := 0;
          return vIsTerm;
        end if;
      end if;

      if vTick.t_DealDate > TO_DATE('31.12.2009', 'DD.MM.YYYY') then
        begin
            SELECT COUNT(1) INTO v_Attr4
              FROM dobjatcor_dbt objatcor
             WHERE objatcor.t_ObjectType = 101 AND
                   objatcor.t_GroupID    = 27 AND
                   objatcor.t_Object     = LPAD( vTick.t_DealID, 34, '0' ) AND
                   objatcor.t_AttrID     = 1; --да
          exception
            when OTHERS then v_Attr4 := 0;
        end;

        --сделка может быть срочной, только если v_Attr4 <> 0 and v_Attr5 <> 0
        if v_Attr4 = 0 then
          vIsTerm := 0;
          return vIsTerm;
        end if;

        begin
            SELECT COUNT(1) INTO v_Attr5
              FROM dobjatcor_dbt objatcor
             WHERE objatcor.t_ObjectType = 101 AND
                   objatcor.t_GroupID    = 28 AND
                   objatcor.t_Object     = LPAD( vTick.t_DealID, 34, '0' ) AND
                   objatcor.t_AttrID     = 1; --да
          exception
            when OTHERS then v_Attr5 := 0;
        end;

        --сделка может быть срочной, только если v_Attr4 <> 0 and v_Attr5 <> 0
        if v_Attr5 = 0 then
          vIsTerm := 0;
          return vIsTerm;
        end if;
      end if;

      begin
          select count(1) into v_Attr3
            from dparty_dbt where t_PartyID = vTick.t_PartyID and t_LegalForm = 1;
        exception
          when NO_DATA_FOUND then
              v_Attr3 := 0;
      end;


      if( ( ValueDate >= RSI_RsbCalendar.GetDateAfterWorkDay(vTick.t_DealDate,3)
            ) and
            ( v_Attr1 != 0
            ) and
            ( (vTick.t_DealDate <= TO_DATE('31.12.2009', 'DD.MM.YYYY') and v_Attr2 = 0) or
              (vTick.t_DealDate > TO_DATE('31.12.2009', 'DD.MM.YYYY') and v_Attr4 <> 0 and v_Attr5 <> 0)
            ) and
            ( v_Attr3 != 0
            )
          ) then
             vIsTerm := 1;
          else
             vIsTerm := 0;
      end if;

      return vIsTerm;

    end;

    -- Значение категории для сделки "Способ определения РЦ"
    FUNCTION RSI_MethodDetermEC( DealID IN NUMBER ) RETURN VARCHAR2
    IS
       CategoryValue dobjattr_dbt.t_NumInList % TYPE;
    BEGIN
       BEGIN
           SELECT Attr.t_NumInList INTO CategoryValue
             FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
            WHERE AtCor.t_ObjectType = Rsb_Secur.OBJTYPE_SECDEAL
              AND AtCor.t_GroupID    = 34 -- OBJGROUP_TICKRC
              AND AtCor.t_Object     = LPAD(DealID, 34, '0')
              AND Attr.t_AttrID      = AtCor.t_AttrID
              AND Attr.t_ObjectType  = AtCor.t_ObjectType
              AND Attr.t_GroupID     = AtCor.t_GroupID;
       EXCEPTION
          WHEN NO_DATA_FOUND THEN CategoryValue := chr(1);
          WHEN OTHERS THEN return 0;
       END;

       RETURN CategoryValue;
    END; -- RSI_DealAttrDvExe

    -- Значение категории для сделки "Исполнение поставочных срочных контрактов"
    FUNCTION RSI_DealFISS( DealID IN NUMBER ) RETURN NUMBER DETERMINISTIC
    IS
       CategoryValue dobjattr_dbt.t_NumInList % TYPE;
    BEGIN
       BEGIN
           SELECT Attr.t_NumInList INTO CategoryValue
             FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
            WHERE AtCor.t_ObjectType = Rsb_Secur.OBJTYPE_SECDEAL
              AND AtCor.t_GroupID    = 33 -- OBJGROUP_TICKFIIS
              AND AtCor.t_Object     = LPAD(DealID, 34, '0')
              AND Attr.t_AttrID      = AtCor.t_AttrID
              AND Attr.t_ObjectType  = AtCor.t_ObjectType
              AND Attr.t_GroupID     = AtCor.t_GroupID;
       EXCEPTION
          WHEN NO_DATA_FOUND THEN CategoryValue := chr(0);
          WHEN OTHERS THEN return 0;
       END;

       IF( CategoryValue <> chr(0) ) THEN
          return to_number(CategoryValue);
       ELSE
          return 0;
       END IF;

       RETURN 0;
    END; -- RSI_DealFISS

    -- установлен ли на объекте атрибут
    -- 0 - не установлен, не 0 - установлен
    FUNCTION CheckObjAttrPresenceByNum( objtype    IN NUMBER,
                                        groupID    IN NUMBER,
                                        numInList  IN VARCHAR2,
                                        objID      IN VARCHAR2,
                                        dat        IN DATE )
    RETURN NUMBER AS
        num NUMBER;
    BEGIN
        SELECT CASE
                   WHEN EXISTS(SELECT 1
                                 FROM dobjatcor_dbt atc
                                WHERE atc.t_ObjectType = objtype
                                  AND atc.t_GroupID    = groupID
                                  AND atc.t_Object     = objID
                                  AND atc.t_AttrID IN (SELECT att.t_AttrID
                                                         FROM dobjattr_dbt att
                                                        WHERE att.t_ObjectType = atc.t_ObjectType
                                                          AND att.t_GroupID    = atc.t_GroupID
                                                          AND att.t_numInList  = numInList)
                                  AND dat BETWEEN atc.t_ValidFromDate
                                              AND atc.t_ValidToDate)
                       THEN 1
                   ELSE 0
               END
          INTO num
          FROM DUAL;

        return num;
    END;

    --Определить, подходит ли сделка под режим корректировки
    function DealWithCorrect(DealID IN NUMBER,
                             FIID   IN NUMBER,
                             ValueDate IN DATE --фактическая, а в её отсутствии плановая дата поставки
                            )
      return NUMBER
    is
      vTick   ddl_tick_dbt%ROWTYPE;

    begin

        begin
          SELECT * INTO vTick FROM ddl_tick_dbt where t_DealID = DealID;
          exception
            when OTHERS then return 0;
        end;

        if( CheckObjAttrPresenceByNum(3,47,'1',lpad(vTick.t_PartyID, 10, '0'),ValueDate) = 0 )then
           return 0;
        end if;

        if( CheckObjAttrPresenceByNum(12,17,'0',lpad(FIID, 10, '0'),ValueDate) = 0 )then
           return 0;
        end if;

        return 1;

    end;--DealWithCorrect

    ------------------------------------------------------------------------------------------------------------------
    ---- функция получает параметры сделки необходимые для расчета
    ------------------------------------------------------------------------------------------------------------------
    function RSI_GetParmDeal( LotID IN NUMBER, IsNew IN NUMBER, Deal OUT R_Deal )
     return NUMBER
      is
       Leg       ddl_leg_dbt%ROWTYPE;
       TXLot     dsctxlot_dbt%ROWTYPE;
       Curs      NUMBER;
       v_IsReal  NUMBER;
       v_RQID    NUMBER;
       v_DelDate DATE;
    begin

      begin
        select * into TXLot from dsctxlot_dbt where t_ID = LotID;
      exception
         when NO_DATA_FOUND then
           MarketPrice.ErrorMsg := 'Не возможно найти лот с ID = '||LotID;
           return 1;
      end;

      if( TXLot.t_VirtualType in (2,3) ) then
         v_IsReal := 0;
      else
         v_IsReal := 1;
      end if;

      if( v_IsReal = 1 ) then
         -- получаем тикет сделки
         begin
           select * into Deal.Tick from ddl_tick_dbt where t_DealID = TXLot.t_DealID;
         exception
           when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении записи dl_tick.dbt для сделки DealID = '||TXLot.t_DealID;
                                   return 1;
         end;

         -- получаем группу операций
         begin
           select rsb_secur.get_OperationGroup(t_SysTypes) into Deal.OGrp
             from doprkoper_dbt
            where t_Kind_Operation = Deal.Tick.t_DealType;
         exception
           when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении группы операций для сделки DealID = '||TXLot.t_DealID;
                                   return 1;
         end;
         -- Получаем данные по сделке
         begin
           select * into Leg from ddl_leg_dbt where t_DealID = Deal.Tick.t_DealID and t_LegKind = 0 and t_LegID = 0;
         exception
           when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении записи dl_leg.dbt для сделки DealID = '||TXLot.t_DealID;
                                   return 1;
         end;
         -- получаем финансовый инструмент
         begin
           select * into Deal.FI from dfininstr_dbt where t_FIID = TXLot.t_FIID;
         exception
           when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении финансового инструмента FIID = '||TXLot.t_FIID;
                                   return 1;
         end;

         if( IsNew = 0 ) then
           -- получаем налоговую группу
           begin
             select t_TaxGroup into Deal.TaxGroup from davoiriss_dbt where t_FIID = TXLot.t_FIID;
           exception
             when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении ценной бумаги FIID = '||TXLot.t_FIID;
               return 1;
           end;
         end if;

         -- дата договора купли продажи по сделке Deal, а при отсутствии договора - дата заключения сделки или дата операции
         Deal.DZ    := Deal.Tick.t_DealDate;
         -- плановая дата поставки по сделк
         if( Leg.t_MaturityIsPrincipal='X') then
            Deal.DPD := Leg.t_Maturity;
         else
            Deal.DPD := Leg.t_Expiry;
         end if;
         -- получаем фактическую дату поставки по сделке
         Deal.DD := TO_DATE('31.12.9999','DD.MM.YYYY');
         v_RQID := TXLot.t_RQID;
         if TXLot.t_RQID > 0 then
         begin
             select t_FactDate into Deal.DD from ddlrq_dbt where t_ID = TXLot.t_RQID;
         exception
               when NO_DATA_FOUND then Deal.DD := TO_DATE('31.12.9999','DD.MM.YYYY');
         end;
         end if;

         if( IsNew = 0 ) then
            if( (Deal.FI.t_FaceValueFI = 0) and
                (Rsb_Secur.SecurKind(Deal.FI.t_AvoirKind) <> Rsb_Secur.AVOIRKIND_ORDINARY_BOND) ) then
               if( Leg.t_CFI != 0 ) then
                  --
                  Curs := GetNoteText( 993, LPAD(v_RQID, 10, '0'), 44);
                  Deal.Price := Leg.t_Price * iif(Curs is not NULL, Curs, 1);
               else
                  Deal.Price := Leg.t_Price;
               end if;
            elsif( (Deal.FI.t_FaceValueFI != 0) and
                   (Rsb_Secur.SecurKind(Deal.FI.t_AvoirKind) <> Rsb_Secur.AVOIRKIND_ORDINARY_BOND) ) then
               Deal.Price := RSI_Rsb_FIInstr.ConvSum( Leg.t_Price, Leg.t_CFI, Deal.FI.t_FaceValueFI, Deal.DZ );
            else
               Deal.Price := Leg.t_Price;
            end if;
         else
            Deal.Price := Leg.t_Price;
            if( Leg.t_RelativePrice = chr(88) ) then
               Deal.CFI := '%';
            else
               begin
                  SELECT fin.t_ISO_Number INTO Deal.CFI
                    FROM dfininstr_dbt fin
                   WHERE fin.T_FIID = Leg.T_CFI;
               exception
                  when OTHERS then Deal.CFI := chr(1);
               end;
            end if;
         end if;

        --фактическая, а в её отсутствии плановая дата поставки
        if Deal.DD = TO_DATE('31.12.9999','DD.MM.YYYY') then
          if( Leg.t_MaturityIsPrincipal = chr(88) ) then
            v_DelDate := Leg.t_Maturity;
          else
            v_DelDate := Leg.t_Expiry;
          end if;
        else
          v_DelDate := Deal.DD;
        end if;

        if( IsNew = 0 ) then
           if DealIsTerm(Deal.Tick.t_DealID, Deal.FI.t_FIID, v_DelDate) = 1 then
             Deal.IsTerm := True;
           else
             Deal.IsTerm := False;
           end if;
        else
           if( RSI_DealFISS(Deal.Tick.t_DealID) > 0 ) then
             Deal.IsTerm := True;
           else
             Deal.IsTerm := False;
           end if;
        end if;

      else
         --получаем данные о сделке
         begin
          select * into Deal.Tick
            from ddl_tick_dbt
          where t_DealID =  (select t_DealID  from dsctxlot_dbt  where t_ID = TXLot.t_RealID);
         exception
          when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении записи dl_tick.dbt для сделки DealID = '||TXLot.t_DealID;
                                   return 1;
         end;
         -- получаем данные ФИ
         begin
           select * into Deal.FI from dfininstr_dbt where t_FIID = TXLot.t_FIID;
         exception
           when NO_DATA_FOUND then MarketPrice.ErrorMsg := 'Ошибка при получении финансового инструмента FIID = '||TXLot.t_FIID;
                                   return 1;
         end;

         -- дата договора купли продажи по сделке Deal, а при отсутствии договора - дата заключения сделки или дата операции
         Deal.DZ     := TXLot.t_DealDate;
         Deal.DD     := TXLot.t_DealDate;
         Deal.IsTerm := False;
         Deal.Price  := TXLot.t_Price;
         if( TXLot.t_Type = 1 ) then
            Deal.OGrp := Rsb_Secur.get_OperationGroup('BX');
         elsif( TXLot.t_Type = 2 ) then
            Deal.OGrp := Rsb_Secur.get_OperationGroup('SX');
         end if;

      end if;

      return 0;

    end; --RSI_GetParmDeal

    function RSI_GetDealCountry(Deal IN R_Deal, pCountry OUT VARCHAR2)
    return VARCHAR2
    is
      vCountry dparty_dbt.t_NRCountry%TYPE;

    begin
      begin
        if Deal.Tick.t_MarketID <> -1 then
           select t_NRCountry into vCountry
             from dparty_dbt
            where t_PartyID = Deal.Tick.t_MarketID;
        else
           vCountry := Deal.Tick.t_Country;
        end if;

        if vCountry = CHR(1) then
           vCountry := 'RUS';
        end if;

        exception
           when NO_DATA_FOUND then vCountry := 'RUS';
      end;

      pCountry := vCountry;

      return vCountry;
    end; --RSI_GetDealCountry

    -- Проверяет, задана ли категория "Контролируемая сделка (НК)"
    FUNCTION IsControlCategByDeal( Deal IN R_Deal ) RETURN NUMBER DETERMINISTIC
    IS
       ControlDeal dobjattr_dbt.t_NumInList % TYPE;
    BEGIN
       BEGIN
          SELECT Attr.t_NumInList INTO ControlDeal
            FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
           WHERE     AtCor.t_ObjectType = Rsb_Secur.OBJTYPE_SECDEAL -- OBJTYPE_SECDEAL
                 AND AtCor.t_GroupID    = 36  -- Контролируемая сделка (НК)
                 AND AtCor.t_Object     = LPAD( Deal.Tick.t_DealID, 34, '0' )
                 AND Attr.t_AttrID      = AtCor.t_AttrID
                 AND Attr.t_ObjectType  = AtCor.t_ObjectType
                 AND Attr.t_GroupID     = AtCor.t_GroupID;

       EXCEPTION
          --Не задано - то же что "Нет"
          WHEN NO_DATA_FOUND THEN ControlDeal := '0';
          WHEN OTHERS THEN
             return 0;
       END;

       IF( ControlDeal = '1' ) THEN
          return 1;
       ELSIF( ControlDeal = '0' ) THEN
          return 0;
       END IF;

       RETURN 0;
    END; -- IsControlCategByDeal

    -- Проверяет, заполнена ли на дату сделки категория "Вид взаимозависимости" на клиенте контрагента или контрагенте по сделке
    FUNCTION IsFillDependObjCategByDate( Deal IN R_Deal ) RETURN NUMBER DETERMINISTIC
    IS
       PartyID NUMBER := 0;
       IsFillCategory NUMBER := 0;
    BEGIN
       -- сначала ищем связанный субъект по сделке - клиента контрагента и проверяем, задана ли на нем категория на дату заключения сделки
       BEGIN
          select T_ATTRID into PartyID
            from ( SELECT to_number(T.T_ATTRID) T_ATTRID
                     FROM dobjlink_dbt t
                    WHERE t.t_GroupID = 1 and t.t_AttrType=3 AND t.t_ObjectType=101 AND t.t_ObjectID=LPAD( Deal.Tick.t_DealID, 34, '0' )
                   order by T.T_LINKID desc
                 )
           where rownum = 1;
       EXCEPTION
          WHEN OTHERS THEN PartyID := 0;
       END;

       IF( PartyID > 0 ) THEN
          BEGIN
             SELECT COUNT(1) into IsFillCategory
               FROM dobjatcor_dbt objatcor
              WHERE objatcor.t_ObjectType = 3 AND  -- OBJTYPE_PARTY
                    objatcor.t_GroupID    = 58 AND -- Вид взаимозависимости
                    objatcor.t_Object     = LPAD( PartyID, 10, '0' ) AND
                    OBJATCOR.T_VALIDFROMDATE <= Deal.DZ AND
                    OBJATCOR.T_VALIDTODATE  >= Deal.DZ;
          EXCEPTION
             WHEN OTHERS THEN IsFillCategory := 0;
          END;
       END IF;

       -- если связанный ceбъект - клиент контрагента не задан или категория на нем не задана, то далее смотрим на контрагента по сделке
       IF( IsFillCategory <= 0 ) THEN
          -- определим контрагента
          if( (Rsb_Secur.IsExchange(Deal.oGrp) = 1) and (Deal.Tick.t_PartyID = -1) ) then
             PartyID := Deal.Tick.t_MarketID;
          else
             PartyID := Deal.Tick.t_PartyID;
          end if;

         -- определим, задана ли на контрагенте категория на дату заключения сделки
          BEGIN
             SELECT COUNT(1) into IsFillCategory
               FROM dobjatcor_dbt objatcor
              WHERE objatcor.t_ObjectType = 3 AND  -- OBJTYPE_PARTY
                    objatcor.t_GroupID    = 58 AND -- Вид взаимозависимости
                    objatcor.t_Object     = LPAD( PartyID, 10, '0' ) AND
                    OBJATCOR.T_VALIDFROMDATE <= Deal.DZ AND
                    OBJATCOR.T_VALIDTODATE  >= Deal.DZ;
          EXCEPTION
             WHEN OTHERS THEN IsFillCategory := 0;
          END;

       END IF;

       IF( IsFillCategory > 0 ) THEN
          IsFillCategory := 1;
       END IF;

       RETURN IsFillCategory;
    END; -- IsFillDependObjCategByDate

    -- Проверяет, является ли сделка контролируемой
    FUNCTION IsControlDeal( Deal IN R_Deal ) RETURN NUMBER DETERMINISTIC
    IS
    BEGIN
       IF ( (IsControlCategByDeal( Deal ) = 1) or (IsFillDependObjCategByDate( Deal ) = 1) ) THEN
         RETURN 1;
       END IF;

       RETURN 0;
    END; -- IsControlDeal

    procedure CalcMarketPrice( LotID IN NUMBER, LotBuyID IN NUMBER, IsFutures IN NUMBER, Is600Reg IN NUMBER )
     is
      Deal     R_Deal;
      DealBuy  R_Deal;
      TXLot    dsctxlot_dbt%ROWTYPE;
      DateBuy  DATE;
      RD       DRATEDEF_DBT%ROWTYPE;
      C0       DRATEDEF_DBT%ROWTYPE;
      C1       DRATEDEF_DBT%ROWTYPE;
      n_C0     NUMBER;
      n_C1     NUMBER;
      K        NUMBER;
      tmp      NUMBER;
      stat     NUMBER;
      v_Rate   NUMBER;
      PriceBuy NUMBER;
      Country  VARCHAR2(3);
      v_IsMinMax   NUMBER;
      v_IsShare    NUMBER;
      v_AvrRoot    NUMBER;
      v_SecureKind NUMBER;
      NoteValue    NUMBER(32,12);
      NoteValueFI   VARCHAR2(3);
      NoteValueFIID NUMBER;
      Nominal       NUMBER(32,12);
      v_Count       NUMBER;
      v_Termless    CHAR;
      v_DrawingDate DATE;
    begin

      MarketPrice := NULL;

      if RateTypes.MinRate = 0 or RateTypes.MinRate is null then -- т.е., если первый вход и ещё ничего не закачивали
        GetSettingsTax(1);
      end if;

      begin
       select * into TXLot from dsctxlot_dbt where t_ID = LotID;
       exception
         when NO_DATA_FOUND then
           MarketPrice.ErrorMsg := 'Не возможно найти лот с ID = '||LotID;
           return;
      end;

      if( RSI_GetParmDeal( LotID, 0, Deal ) != 0 ) then
         return;
      end if;

      MarketPrice.LotID := TXLot.t_ID;

      v_SecureKind := Rsb_Secur.SecurKind(Deal.FI.t_AvoirKind);

      v_IsShare := 0;
      v_AvrRoot := RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, Deal.FI.t_AvoirKind );
      if( v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_SHARE OR
          v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_INVESTMENT_SHARE OR
          v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_DEPOS_RECEIPT
        ) then
        v_IsShare := 1;
      end if;

      if( Rsb_Secur.IsRet_Issue(Deal.oGrp)=1 ) then
         MarketPrice.MarketPrice := 100;
         MarketPrice.QuoteValue  := 100;
         MarketPrice.Market      := 'погашение';
         MarketPrice.DateMarket  := Deal.Tick.t_DealDate;

         if( ( (Deal.Tick.t_DealDate <= TO_DATE('31.12.2009', 'DD.MM.YYYY') and
                ( (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MinRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-12),RD,CHR(1),0,0,v_IsShare) = 0) or
               (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MaxRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-12),RD,CHR(1),0,0,v_IsShare) = 0) or
               (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MediumRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-12),RD,CHR(1),0,0,v_IsShare) = 0)
                )
               ) or
               (Deal.Tick.t_DealDate > TO_DATE('31.12.2009', 'DD.MM.YYYY') and
                ( (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MinRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-3),RD,CHR(1),0,0,v_IsShare) = 0) or
                  (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MaxRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-3),RD,CHR(1),0,0,v_IsShare) = 0) or
                  (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MediumRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-3),RD,CHR(1),0,0,v_IsShare) = 0) or
                  (SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.CloseRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-3),RD,CHR(1),0,0,v_IsShare) = 0)
                )
               )
             )
             and RSI_rsb_fiinstr.FI_CirculateInMarket( Deal.FI.t_FIID, Deal.Tick.t_DealDate ) = 1
           ) then
            MarketPrice.ifMarket    := chr(88);
         else
            MarketPrice.ifMarket    := chr(0);
         end if;
         return;
      end if;

      if( Deal.Tick.t_DealDate > TO_DATE('31.12.2009', 'DD.MM.YYYY') and
          Deal.IsTerm = false and
          v_AvrRoot = RSI_rsb_fiinstr.AVOIRKIND_INVESTMENT_SHARE and
          Deal.FI.t_FaceValueFI = 0
         ) then
         K := 1.0;
      else
        if( Rsb_Secur.IsBuy(Deal.oGrp)=1 or Rsb_Secur.IsAvrWrtIn(Deal.oGrp)=1 ) then
          K := 1.2;
        else
          K := 0.8;
        end if;
      end if;

      if( instr(Deal.Tick.t_TypeDoc, 'P') = 1 ) then
         MarketPrice.MarketPrice := Deal.Price;
         MarketPrice.QuoteValue  := Deal.Price;
         MarketPrice.Market      := 'аукцион';
         MarketPrice.DateMarket  := Deal.DZ;
         return;
      end if;

      if( IsFutures <> 0 ) then
         Deal.IsTerm := False;
      end if;

      if( Deal.IsTerm ) then
         --4a
         MarketPrice.T1    := Deal.DZ-(Deal.DPD-Deal.DZ);
         if (Deal.DD <> TO_DATE('31.12.9999','DD.MM.YYYY')) then
            MarketPrice.T1 := Deal.DZ-(Deal.DD-Deal.DZ);
         end if;

         if( Rsb_Secur.IsBuy(Deal.oGrp)=1 or Rsb_Secur.IsAvrWrtIn(Deal.oGrp)=1 ) then
           v_IsMinMax := 2; --максимальная
         else
           v_IsMinMax := 1; --минимальная
         end if;

         --4b
         if( SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MediumRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-12),C0,CHR(1),(CASE WHEN v_SecureKind = Rsb_Secur.AVOIRKIND_ORDINARY_BOND THEN 1 ELSE 0 END), v_IsMinMax, v_IsShare ) = 0 ) then
            MarketPrice.QuoteValue  := C0.t_Rate;

            if( SPGetRate_Ex(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.MediumRate,MarketPrice.T1,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-12),C1,(CASE WHEN v_SecureKind = Rsb_Secur.AVOIRKIND_ORDINARY_BOND THEN 1 ELSE 0 END), v_IsMinMax, v_IsShare) = 0 ) then
               MarketPrice.MarketPrice2 := C1.t_Rate;
            else
               MarketPrice.MarketPrice2 := 0;
            end if;

            MarketPrice.MarketPrice := K * ( 2 * MarketPrice.QuoteValue - MarketPrice.MarketPrice2);

            begin
              select t_ShortName into MarketPrice.Market from dparty_dbt where t_PartyID = C0.t_Market_Place;
            exception
              when NO_DATA_FOUND then MarketPrice.Market := chr(0);
            end;

            begin
              select t_ShortName into MarketPrice.Market2 from dparty_dbt where t_PartyID = C1.t_Market_Place;
            exception
              when NO_DATA_FOUND then MarketPrice.Market2 := chr(0);
            end;

            MarketPrice.DateMarket := C0.t_SinceDate;
            MarketPrice.Date2      := C1.t_SinceDate;
            return;

         --4c
         elsif( SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.ReuterRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-12),C0,CHR(1),(CASE WHEN v_SecureKind = Rsb_Secur.AVOIRKIND_ORDINARY_BOND THEN 1 ELSE 0 END), 0, v_IsShare) = 0 ) then
            MarketPrice.QuoteValue  := C0.t_Rate;

            if( SPGetRate_Ex(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.ReuterRate,MarketPrice.T1,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-12),C1,(CASE WHEN v_SecureKind = Rsb_Secur.AVOIRKIND_ORDINARY_BOND THEN 1 ELSE 0 END), 0, v_IsShare) = 0 ) then
               MarketPrice.MarketPrice2 := C1.t_Rate;
            else
               MarketPrice.MarketPrice2 := 0;
            end if;

            MarketPrice.MarketPrice := K * ( 2 * MarketPrice.QuoteValue - MarketPrice.MarketPrice2);

            MarketPrice.Market     := 'Информационное агентство';
            MarketPrice.Market2    := 'Информационное агентство';
            MarketPrice.DateMarket := C0.t_SinceDate;
            MarketPrice.Date2      := C1.t_SinceDate;
            return;

         --4d
         elsif( SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.TaxRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-12),C0,CHR(1),(CASE WHEN v_SecureKind = Rsb_Secur.AVOIRKIND_ORDINARY_BOND THEN 1 ELSE 0 END), 0, v_IsShare) = 0 ) then
            MarketPrice.QuoteValue  := C0.t_Rate;

            if( SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.TaxRate,MarketPrice.T1,Deal.DZ-add_months(Deal.DZ,-12),C1,CHR(1),(CASE WHEN v_SecureKind = Rsb_Secur.AVOIRKIND_ORDINARY_BOND THEN 1 ELSE 0 END), 0, v_IsShare) = 0 ) then
               MarketPrice.MarketPrice2 := C1.t_Rate;
            else
               MarketPrice.MarketPrice2 := 0;
            end if;

            MarketPrice.MarketPrice := K * ( 2 * MarketPrice.QuoteValue - MarketPrice.MarketPrice2);

            MarketPrice.Market     := 'расчет';
            MarketPrice.Market2    := 'расчет';
            MarketPrice.DateMarket := Deal.DZ;
            MarketPrice.Date2      := MarketPrice.T1;
            return;

         --4e
         else
            MarketPrice.MarketPrice := Deal.Price;
            MarketPrice.Market      := 'факт';
            MarketPrice.Market2     := 'факт';
            MarketPrice.DateMarket  := Deal.DZ;
            MarketPrice.QuoteValue  := Deal.Price;
            MarketPrice.MarketPrice2:= Deal.Price;
            MarketPrice.Date2       := Deal.DZ;
            MarketPrice.ErrorMsg := 'Отсутствует курс вида "Налоговая цена" для ФИ '||Deal.FI.t_FI_Code||' за дату '||Deal.DZ;
            return;
         end if;
      --6
      else
         -- если на сделке задано примечание "Расчетная цена для сделки"
         NoteValue := GetNoteText(Rsb_Secur.OBJTYPE_SECDEAL, LPAD(Deal.Tick.t_DealID, 34, '0'), 23);
         if( NoteValue != 0 ) then
            if( v_AvrRoot in (RSI_RSB_FIInstr.AVOIRKIND_SHARE, RSI_RSB_FIInstr.AVOIRKIND_BOND) ) then -- для акций и облигаций возможно необходимо перевести сумму
               NoteValueFI := GetNoteTextStr(Rsb_Secur.OBJTYPE_SECDEAL, LPAD(Deal.Tick.t_DealID, 34, '0'), 28);
               if( NoteValueFI != chr(1) ) then -- если задано примечание "Единица измерения расчетной цены"
                  if( NoteValueFI <> '%' ) then
                     begin
                        SELECT fin.t_FIID INTO NoteValueFIID
                          FROM dfininstr_dbt fin
                         WHERE t_ISO_Number = NoteValueFI;
                     exception
                        when OTHERS then NoteValueFIID := Deal.FI.t_FaceValueFI;
                     end;

                     if( NoteValueFIID != Deal.FI.t_FaceValueFI ) then
                        NoteValue := RSI_RSB_FIInstr.ConvSum(NoteValue, NoteValueFIID, Deal.FI.t_FaceValueFI, Deal.DZ);
                     end if;

                     if( v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND ) then -- для облигаций в % от номинала
                        Nominal := RSI_RSB_FIInstr.FI_GetNominalOnDate(Deal.FI.t_FIID, Deal.DZ);
                        if( Nominal != 0 ) then
                           NoteValue := NoteValue / Nominal * 100.0;
                        else
                           NoteValue := 0;
                        end if;
                     end if;
                  end if;
               end if;
            end if;

            MarketPrice.QuoteValue  := NoteValue;
            MarketPrice.MarketPrice := MarketPrice.QuoteValue;
            MarketPrice.Market      := 'Расчет';
            MarketPrice.DateMarket  := Deal.DZ;
            MarketPrice.ifMarket    := chr(0);

            return;
         end if;

         -- Если дата заключения позже 1 января 2015 года и сделка заключена через биржу
         v_Count := 0;
         if( Deal.Tick.t_PartyID > 0 ) then
            begin
               SELECT COUNT(1) INTO v_Count
                 FROM DPARTYOWN_DBT
                WHERE T_PARTYID   = Deal.Tick.t_PartyID
                  AND T_PARTYKIND = 3; -- PTK_MARKETPLASE
            exception
               when OTHERS then v_Count := 0;
            end;
         end if;
         if( (Deal.DZ >= TO_DATE('01.01.2015','DD.MM.YYYY')) and ((Deal.Tick.t_MarketID > 0) OR (v_Count > 0)) ) then
            MarketPrice.QuoteValue  := Deal.Price;
            MarketPrice.MarketPrice := MarketPrice.QuoteValue;
            MarketPrice.Market      := 'Биржевая сделка';
            MarketPrice.DateMarket  := Deal.DZ;
            MarketPrice.ifMarket    := 'X';

            return;
         end if;

         -- Неконтролируемые сделки
         if( (Deal.DZ >= TO_DATE('01.01.2016','DD.MM.YYYY')) and ( IsControlDeal(Deal) != 1 ) ) then
            MarketPrice.QuoteValue  := Deal.Price;
            MarketPrice.MarketPrice := MarketPrice.QuoteValue;
            MarketPrice.Market      := 'Неконтролируемая сделка';
            MarketPrice.DateMarket  := Deal.DZ;

            if( RSI_rsb_fiinstr.FI_CirculateInMarket( Deal.FI.t_FIID, Deal.DZ ) = 1 ) then
               MarketPrice.ifMarket := 'X';
            else
               MarketPrice.ifMarket := chr(0);
            end if;

            return;
         end if;

         --6a
         if( Rsb_Secur.IsBuy(Deal.oGrp)=1 or Rsb_Secur.IsAvrWrtIn(Deal.oGrp)=1 ) then
            v_Rate := RateTypes.MaxRate;
            v_IsMinMax := 2; --максимальная
         else
            v_Rate := RateTypes.MinRate;
            v_IsMinMax := 1; --минимальная
         end if;

         if( Deal.Tick.t_DealDate <= TO_DATE('31.12.2009', 'DD.MM.YYYY') and SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,v_Rate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-12),C0,CHR(1),(CASE WHEN v_SecureKind = Rsb_Secur.AVOIRKIND_ORDINARY_BOND THEN 1 ELSE 0 END), v_IsMinMax, v_IsShare) = 0 ) then
           MarketPrice.QuoteValue  := C0.t_Rate;
           MarketPrice.MarketPrice := MarketPrice.QuoteValue;

           begin
             select t_ShortName into MarketPrice.Market from dparty_dbt where t_PartyID = C0.t_Market_Place;
           exception
             when NO_DATA_FOUND then MarketPrice.Market := chr(0);
           end;

           MarketPrice.DateMarket := C0.t_SinceDate;
           if( RSI_rsb_fiinstr.FI_CirculateInMarket( Deal.FI.t_FIID, Deal.DD ) = 1 ) then
              MarketPrice.ifMarket    := chr(88);
           else
              MarketPrice.ifMarket    := chr(0);
           end if;
           return;
         --6b
         elsif( Deal.Tick.t_DealDate > TO_DATE('31.12.2009', 'DD.MM.YYYY') and
                RSI_GetDealCountry(Deal, Country) <> CHR(1) and
                SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,v_Rate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-3)-1,C0,Country,(CASE WHEN v_SecureKind = Rsb_Secur.AVOIRKIND_ORDINARY_BOND THEN 1 ELSE 0 END), v_IsMinMax, v_IsShare) = 0
              ) then
           MarketPrice.QuoteValue  := C0.t_Rate;
           MarketPrice.MarketPrice := MarketPrice.QuoteValue;

           begin
             select t_ShortName into MarketPrice.Market from dparty_dbt where t_PartyID = C0.t_Market_Place;
           exception
             when NO_DATA_FOUND then MarketPrice.Market := chr(0);
           end;

           MarketPrice.DateMarket := C0.t_SinceDate;
           if( RSI_rsb_fiinstr.FI_CirculateInMarket( Deal.FI.t_FIID, Deal.DD ) = 1 ) then
              MarketPrice.ifMarket    := chr(88);
           else
              MarketPrice.ifMarket    := chr(0);
           end if;
           return;
        --6c
         elsif( SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.ReuterRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-12),C0,CHR(1),(CASE WHEN v_SecureKind = Rsb_Secur.AVOIRKIND_ORDINARY_BOND THEN 1 ELSE 0 END), v_IsMinMax, v_IsShare) = 0 ) then
           MarketPrice.QuoteValue  := C0.t_Rate;
           MarketPrice.MarketPrice := K * MarketPrice.QuoteValue;
           MarketPrice.Market     := 'Информационное агентство';
           MarketPrice.DateMarket := C0.t_SinceDate;
           MarketPrice.ifMarket   := chr(0);
           return;

        --6d
         elsif( SPGetRate(Deal.FI.t_FIID,Deal.FI.t_FaceValueFI,RateTypes.TaxRate,Deal.DZ,Deal.DZ-add_months(Deal.DZ,-12),C0,CHR(1),(CASE WHEN v_SecureKind = Rsb_Secur.AVOIRKIND_ORDINARY_BOND THEN 1 ELSE 0 END), v_IsMinMax, v_IsShare) = 0 ) then
           MarketPrice.QuoteValue  := C0.t_Rate;
           MarketPrice.MarketPrice := K * MarketPrice.QuoteValue;
           MarketPrice.Market     := 'расчет';
           MarketPrice.DateMarket := Deal.DZ;
           MarketPrice.ifMarket   := chr(0);
           return;

        --6g
         else
           MarketPrice.Market     := 'факт';
           MarketPrice.DateMarket := Deal.DZ;
           MarketPrice.ifMarket   := chr(0);
           MarketPrice.ErrorMsg := 'Отсутствует курс вида "Налоговая цена" для ФИ '||Deal.FI.t_FI_Code||' за дату '||Deal.DZ;

           if( v_SecureKind <> Rsb_Secur.AVOIRKIND_ORDINARY_BOND ) then
              MarketPrice.QuoteValue := Deal.Price;

           elsif( TXIsPercentAvoir(Deal.TaxGroup) = 1 ) then
              MarketPrice.QuoteValue := Deal.Price;

           elsif( TXIsDiscountAvoir(Deal.TaxGroup) = 1 ) then
              MarketPrice.Market     := 'расчет';  --Для дисконтных облигаций Market = "расчет"

              if( Rsb_Secur.IsBuy(Deal.oGrp)=1 or Rsb_Secur.IsAvrWrtIn(Deal.oGrp)=1 OR Is600Reg <> 0) then
                 MarketPrice.QuoteValue := Deal.Price;
              else
                 if( RSI_GetParmDeal( LotBuyID, 0, DealBuy ) != 0 ) then
                    return;
                 end if;

                 if( TXGetBondKind( Deal.TaxGroup ) = BOND_USUAL ) then
                    DateBuy  := DealBuy.DD;
                    PriceBuy := DealBuy.Price;
                 else
                    DateBuy  := Deal.FI.t_Issued;
                    PriceBuy := GetNoteText(12, LPAD(DealBuy.FI.t_FIID, 10, '0'), 10);
                 end if;

                 select t_Termless into v_Termless from davoiriss_dbt where t_FIID = Deal.FI.t_FIID;
                 v_DrawingDate := RSI_RSB_FIInstr.FI_GetNominalDrawingDate(Deal.FI.t_FIID, v_Termless);

                 if( (v_DrawingDate-DateBuy) <> 0 ) then
                    MarketPrice.QuoteValue := PriceBuy + (100 - PriceBuy)*((Deal.DD-DateBuy)/(v_DrawingDate-DateBuy));
                 else
                    MarketPrice.QuoteValue := 0;
                 end if;
              end if;
           end if;

           MarketPrice.MarketPrice := MarketPrice.QuoteValue;
         end if;
      end if;

    end; --CalcMarketPrice

    function RSI_GetValMarketByRate( FIID IN NUMBER, RD IN DRATEDEF_DBT%ROWTYPE ) return VARCHAR2
    is
       ValMarket   VARCHAR2(3);
       ValMarketID NUMBER;
    begin
       ValMarket := chr(1);
       if( RD.t_IsRelative = chr(88) ) then
          ValMarket := '%';
       else
          if( RD.t_OtherFI = FIID ) then
             ValMarketID := RD.t_FIID;
          else
             ValMarketID := RD.t_OtherFI;
          end if;

          begin
             SELECT fin.t_ISO_Number INTO ValMarket
               FROM dfininstr_dbt fin
              WHERE fin.T_FIID = ValMarketID;
          exception
             when OTHERS then ValMarket := chr(1);
          end;
       end if;

       return ValMarket;
    end;

    function RSI_GetValByRate( FIID IN NUMBER, RD IN DRATEDEF_DBT%ROWTYPE, OnDate IN DATE ) return NUMBER
    is
       ValRate NUMBER(32,12);
       Nominal NUMBER(32,12);
    begin
       ValRate := 0;

       if( RD.t_IsRelative = chr(88) ) then
          -- курс уже переведён из процентов, так что переводим назад
          Nominal := RSI_RSB_FIInstr.FI_GetNominalOnDate(FIID, OnDate);
          if( Nominal != 0 ) then
             ValRate := RD.t_Rate / Nominal * 100.0;
          end if;
       else
          ValRate := RD.t_Rate;
       end if;

       return ValRate;
    end;

    procedure CalcMarketPrice_NEW( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') )
     is
      TXLot          dsctxlot_dbt%ROWTYPE;
      Deal           R_Deal;
      v_IsShare      NUMBER;
      v_AvrRoot      NUMBER;
      MethodDetermEC VARCHAR2(35);
      DealFISS       NUMBER;
      Country        VARCHAR2(3);
      C0             DRATEDEF_DBT%ROWTYPE;
      C1             DRATEDEF_DBT%ROWTYPE;
      K_New          NUMBER;
      NoteValue      NUMBER(32,12);
      v_Rate         NUMBER;
      v_IsMinMax     NUMBER;
      v_Find         NUMBER := 0;
      ValMarketID    NUMBER;
      Nominal        NUMBER(32,12);
      v_Count        NUMBER;
      v_InvType      NUMBER := 0;
      v_IsInv        NUMBER;
    begin

      MarketPrice_NEW := NULL;

      if RateTypes.MinRate = 0 or RateTypes.MinRate is null then -- т.е., если первый вход и ещё ничего не закачивали
        GetSettingsTax(1);
      end if;

      begin
        select * into TXLot from dsctxlot_dbt where t_ID = LotID;
        exception
          when NO_DATA_FOUND then
            MarketPrice_NEW.ErrorMsg := 'Не возможно найти лот с ID = '||LotID;
            return;
      end;

      if( RSI_GetParmDeal(LotID, 1, Deal) != 0 ) then
        return;
      end if;

      MarketPrice_NEW.LotID  := TXLot.t_ID;
      MarketPrice_NEW.DealID := TXLot.t_DealID;

      v_IsShare := 0;
      v_AvrRoot := RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, Deal.FI.t_AvoirKind );
      if( v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_SHARE OR
          v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_INVESTMENT_SHARE OR
          v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_DEPOS_RECEIPT
        ) then
        v_IsShare := 1;
      end if;

      v_IsInv := 0;
      if( v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_INVESTMENT_SHARE ) then
        v_IsInv := 1;
      end if;

      if( v_IsInv = 1 ) then
         begin
            SELECT t_Type INTO v_InvType
            FROM davrinvst_dbt
            WHERE t_FIID = Deal.FI.t_FIID;
           exception
             when NO_DATA_FOUND then
               MarketPrice_NEW.ErrorMsg := 'Не найдено описание фонда инвестиционного пая с ID = '||Deal.FI.t_FIID;
               return;
         end;
      end if;

      -- 3.1. Признак обращаемости
              MarketPrice_NEW.IfMarket_New := chr(0);
      if( RSI_GetDealCountry(Deal, Country) <> CHR(1) ) then
         if( Country = 'RUS' ) then
            v_Rate := RateTypes.MediumRate;
         else
            v_Rate := RateTypes.CloseRate;
         end if;

         if( SPGetRate(Deal.FI.t_FIID, -1, v_Rate, Deal.DZ, Deal.DZ-add_months(Deal.DZ,-3), C0, Country, 0, 0, v_IsShare) = 0 ) then
            MarketPrice_NEW.IfMarket_New := chr(88);
         end if;
      end if;

      -- 3.2. Коэффициент для определения рыночной цены (K_New)
      if( (Rsb_Secur.IsBuy(Deal.oGrp) = 1) or (Rsb_Secur.IsAvrWrtIn(Deal.oGrp) = 1) ) then
         K_New := 1.2;
      else
         K_New := 0.8;
      end if;

      -- 3.3. Срочные сделки
      if( Deal.IsTerm = True ) then
         DealFISS := RSI_DealFISS(Deal.Tick.t_DealID);
         if( DealFISS = 1 ) then
            NoteValue := GetNoteText(Rsb_Secur.OBJTYPE_SECDEAL, LPAD(Deal.Tick.t_DealID, 34, '0'), 23);
            if( NoteValue != 0 ) then -- 1. Если значение классификатора "Вид ФИСС"= "форвард" и заполнено примечание "Рыночная цена для сделки"
               MarketPrice_NEW.QuoteValue_New := NoteValue;
               MarketPrice_NEW.Market_New     := 'Расчет';
               MarketPrice_NEW.DateMarket_New := Deal.DZ;
               MarketPrice_NEW.Val_Market_New := GetNoteTextStr(Rsb_Secur.OBJTYPE_SECDEAL, LPAD(Deal.Tick.t_DealID, 34, '0'), 28);
            else -- 2. Если значение классификатора "Вид ФИСС"= "форвард" и НЕ заполнено примечание "Рыночная цена для сделки"
               MarketPrice_NEW.QuoteValue_New := Deal.Price;
               MarketPrice_NEW.Market_New     := 'НА КОНТРОЛЬ';
               MarketPrice_NEW.DateMarket_New := Deal.DZ;
               MarketPrice_NEW.Val_Market_New := Deal.CFI;
               K_New := 1; -- переопределяем коэффициент
            end if;
         elsif( DealFISS in(2,3) ) then -- 3. Если значение классификатора "Вид ФИСС"= "фьючерс" или "опцион"
            MarketPrice_NEW.QuoteValue_New := Deal.Price;
            MarketPrice_NEW.Market_New     := 'Факт';
            MarketPrice_NEW.DateMarket_New := Deal.DZ;
            MarketPrice_NEW.Val_Market_New := Deal.CFI;
            K_New := 1;
         end if;
      else
        -- 3.4 Операции погашения
        if( Rsb_Secur.IsRet_Issue(Deal.oGrp) = 1 ) then
          MarketPrice_NEW.QuoteValue_New  := 100;
          MarketPrice_NEW.Market_New      := 'Погашение';
          MarketPrice_NEW.DateMarket_New  := Deal.DZ;
          MarketPrice_NEW.Val_Market_New  := chr(1);
          K_New := 1; -- переопределяем коэффициент
        else
          -- 3.5. Определение рыночной цены QuoteValue_New и источника курса Market_New
          NoteValue := GetNoteText(Rsb_Secur.OBJTYPE_SECDEAL, LPAD(Deal.Tick.t_DealID, 34, '0'), 23);
          if( NoteValue != 0 ) then -- 1. Если заполнено примечание "Рыночная цена для сделки"
             v_Find := 1;
             MarketPrice_NEW.QuoteValue_New := NoteValue;
             MarketPrice_NEW.Market_New     := 'Расчет';
             MarketPrice_NEW.DateMarket_New := Deal.DZ;
             MarketPrice_NEW.Val_Market_New := GetNoteTextStr(Rsb_Secur.OBJTYPE_SECDEAL, LPAD(Deal.Tick.t_DealID, 34, '0'), 28);
          elsif( Deal.DZ >= TO_DATE('01.01.2016','DD.MM.YYYY')) and ( IsControlDeal(Deal) != 1 ) then
             v_Find := 1;
             MarketPrice_NEW.QuoteValue_New := Deal.Price;
             MarketPrice_NEW.Market_New     := 'Неконтролируемая сделка';
             MarketPrice_NEW.DateMarket_New := Deal.DZ;
             MarketPrice_NEW.Val_Market_New := Deal.CFI;
             K_New := 1; -- переопределяем коэффициент
          end if;

          if( v_Find = 0 ) then
             v_Count := 0;

             if( Deal.Tick.t_PartyID > 0 ) then
                begin
                   SELECT COUNT(1) INTO v_Count
                     FROM DPARTYOWN_DBT
                    WHERE T_PARTYID   = Deal.Tick.t_PartyID
                      AND T_PARTYKIND = 3; -- PTK_MARKETPLASE
                exception
                   when OTHERS then v_Count := 0;
                end;
             end if;

             if( (Deal.DZ >= TO_DATE('01.01.2015','DD.MM.YYYY')) and ((Deal.Tick.t_MarketID > 0) OR (v_Count > 0)) ) then
                v_Find := 1;
                MarketPrice_NEW.QuoteValue_New := Deal.Price;
                MarketPrice_NEW.Market_New     := 'Биржевая сделка';
                MarketPrice_NEW.DateMarket_New := Deal.DZ;
                MarketPrice_NEW.Val_Market_New := Deal.CFI;
                K_New := 1; -- переопределяем коэффициент
             end if;

          end if;

          if( v_Find = 0 ) then
             --4.Если ц/б является обращающейся на дату заключения сделки
             if( MarketPrice_NEW.IfMarket_New = 'X' ) then
                --инвестиционные паи открытого инвестиционного фонда
                if( v_IsInv = 1 and v_InvType = 1 )then
                   if( SPGetRate(Deal.FI.t_FIID, -1, RateTypes.InvRate, Deal.DZ, Deal.DZ-add_months(Deal.DZ,-3), C0, CHR(1), 0, 0, v_IsShare) = 0 ) then
                      v_Find := 1;
                      MarketPrice_NEW.QuoteValue_New := RSI_GetValByRate(Deal.FI.t_FIID, C0, Deal.DZ);
                      begin
                        select t_ShortName into MarketPrice_NEW.Market_New from dparty_dbt where t_PartyID = C0.t_Market_Place;
                      exception
                        when NO_DATA_FOUND then MarketPrice_NEW.Market_New := chr(0);
                      end;
                      MarketPrice_NEW.DateMarket_New := C0.t_SinceDate;
                      MarketPrice_NEW.Val_Market_New := RSI_GetValMarketByRate(Deal.FI.t_FIID, C0);
                   end if;
                else--кроме инвестиционных паев открытого инвестиционного фонда
                   if( (Rsb_Secur.IsBuy(Deal.oGrp) = 1) or (Rsb_Secur.IsAvrWrtIn(Deal.oGrp) = 1) ) then
                      v_Rate     := RateTypes.MaxRate;
                      v_IsMinMax := 2; --максимальная
                   else
                      v_Rate     := RateTypes.MinRate;
                      v_IsMinMax := 1; --минимальная
                   end if;

                   -- a.Ищем курс минимальная/максимальная цена
                   if( (RSI_GetDealCountry(Deal, Country) <> CHR(1)) and
                       (SPGetRate(Deal.FI.t_FIID, -1, v_Rate, Deal.DZ, Deal.DZ-add_months(Deal.DZ,-3), C0, Country, 0, v_IsMinMax, v_IsShare) = 0)
                     ) then
                      v_Find := 1;
                      MarketPrice_NEW.QuoteValue_New := RSI_GetValByRate(Deal.FI.t_FIID, C0, Deal.DZ);
                      begin
                        select t_ShortName into MarketPrice_NEW.Market_New from dparty_dbt where t_PartyID = C0.t_Market_Place;
                      exception
                        when NO_DATA_FOUND then MarketPrice_NEW.Market_New := chr(0);
                      end;
                      MarketPrice_NEW.DateMarket_New := C0.t_SinceDate;
                      MarketPrice_NEW.Val_Market_New := RSI_GetValMarketByRate(Deal.FI.t_FIID, C0);
                      K_New := 1; -- переопределяем коэффициент
                   end if;
                end if;

                -- b.Если по предыдущему пункту ничего не найдено, ищется курс вида "Налоговая цена" на дату заключения сделки
                if( v_Find = 0 and SPGetRate(Deal.FI.t_FIID, -1, RateTypes.TaxRate, Deal.DZ, 0, C0, CHR(1), 0, 0, v_IsShare) = 0 ) then
                   v_Find := 1;
                   MarketPrice_NEW.QuoteValue_New := RSI_GetValByRate(Deal.FI.t_FIID, C0, Deal.DZ);
                   MarketPrice_NEW.Market_New     := 'Налоговая цена';
                   MarketPrice_NEW.DateMarket_New := C0.t_SinceDate;
                   MarketPrice_NEW.Val_Market_New := RSI_GetValMarketByRate(Deal.FI.t_FIID, C0);
                end if;

                -- c. Если по предыдущему пункту ничего не найдено
                if( v_Find = 0 ) then
                   v_Find := 1;
                   MarketPrice_NEW.QuoteValue_New := Deal.Price;
                   MarketPrice_NEW.Market_New     := 'НА КОНТРОЛЬ';
                   MarketPrice_NEW.DateMarket_New := Deal.DZ;
                   MarketPrice_NEW.Val_Market_New := Deal.CFI;
                   K_New := 1; -- переопределяем коэффициент
                end if;

             else
                --5.Если ц/б является НЕ обращающейся на дату заключения сделки

                if( v_IsInv = 0 ) then
                   if( SPGetRate(Deal.FI.t_FIID,-1,RateTypes.ReuterRate,Deal.DZ,0,C0,CHR(1),0,0,v_IsShare) = 0 ) then
                      v_Find := 1;
                      MarketPrice_NEW.QuoteValue_New := RSI_GetValByRate(Deal.FI.t_FIID, C0, Deal.DZ);
                      MarketPrice_NEW.Market_New     := 'Thomson Reuters';
                      MarketPrice_NEW.DateMarket_New := C0.t_SinceDate;
                      MarketPrice_NEW.Val_Market_New := RSI_GetValMarketByRate(Deal.FI.t_FIID, C0);
                   end if;

                   if( v_Find = 0 and SPGetRate(Deal.FI.t_FIID,-1,RateTypes.CloseRateBl,Deal.DZ,0,C0,CHR(1),0,0,v_IsShare) = 0 ) then
                      v_Find := 1;
                      MarketPrice_NEW.QuoteValue_New := RSI_GetValByRate(Deal.FI.t_FIID, C0, Deal.DZ);
                      MarketPrice_NEW.Market_New     := 'Bloomberg';
                      MarketPrice_NEW.DateMarket_New := C0.t_SinceDate;
                      MarketPrice_NEW.Val_Market_New := RSI_GetValMarketByRate(Deal.FI.t_FIID, C0);
                   end if;
                end if;

                if( v_Find = 0 and SPGetRate(Deal.FI.t_FIID, -1, RateTypes.TaxRate, Deal.DZ, 0, C0, CHR(1), 0, 0, v_IsShare) = 0 ) then
                   v_Find := 1;
                   MarketPrice_NEW.QuoteValue_New := RSI_GetValByRate(Deal.FI.t_FIID, C0, Deal.DZ);
                   MarketPrice_NEW.Market_New     := 'Налоговая цена';
                   MarketPrice_NEW.DateMarket_New := C0.t_SinceDate;
                   MarketPrice_NEW.Val_Market_New := RSI_GetValMarketByRate(Deal.FI.t_FIID, C0);
                end if;

                -- d. Если по предыдущему пункту ничего не найдено
                if( v_Find = 0 ) then
                   v_Find := 1;
                   MarketPrice_NEW.QuoteValue_New := Deal.Price;
                   MarketPrice_NEW.Market_New     := 'НА КОНТРОЛЬ';
                   MarketPrice_NEW.DateMarket_New := Deal.DZ;
                   MarketPrice_NEW.Val_Market_New := Deal.CFI;
                   K_New := 1; -- переопределяем коэффициент
                end if;

             end if;

          end if;

        end if;

      end if;

      if( v_IsInv = 1 ) then
         K_New := 1;
      end if;

      -- 3.6. Определение прочих параметров
      MarketPrice_NEW.MarketPrice_New := MarketPrice_NEW.QuoteValue_New * K_New;

      -- Рыночная котировка на отчетную дату, сложившаяся на ОРЦБ
      MarketPrice_NEW.QuoteValueRep := 0;
      MarketPrice_NEW.MarketRep     := chr(1);
      MarketPrice_NEW.DateMarketRep := TO_DATE('01.01.0001','DD.MM.YYYY');
      MarketPrice_NEW.Val_MarketRep := chr(1);
      if( RepDate != TO_DATE('01.01.0001','DD.MM.YYYY') ) then
         if( SPGetRate(Deal.FI.t_FIID, -1, RateTypes.MediumRate, RepDate, RepDate-add_months(RepDate,-3), C0, 'RUS', 0, 1/*минимальное*/, v_IsShare) = 0 ) then
            begin
              select t_ShortName into MarketPrice_NEW.MarketRep from dparty_dbt where t_PartyID = C0.t_Market_Place;
            exception
              when NO_DATA_FOUND then MarketPrice_NEW.MarketRep := chr(0);
            end;
            MarketPrice_NEW.DateMarketRep := C0.t_SinceDate;

            MarketPrice_NEW.QuoteValueRep := RSI_GetValByRate(Deal.FI.t_FIID, C0, RepDate);

            if( v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND ) then
               if( C0.t_OtherFI = Deal.FI.t_FIID ) then
                  ValMarketID := C0.t_FIID;
               else
                  ValMarketID := C0.t_OtherFI;
               end if;

               if( C0.t_IsRelative != chr(88) ) then
                  Nominal := RSI_RSB_FIInstr.FI_GetNominalOnDate(Deal.FI.t_FIID, C0.t_SinceDate);
                  if( Nominal != 0 ) then
                     MarketPrice_NEW.QuoteValueRep := RSI_RSB_FIInstr.ConvSum(C0.t_Rate, ValMarketID, Deal.FI.t_FaceValueFI, C0.t_SinceDate) / Nominal * 100;
                  else
                     MarketPrice_NEW.QuoteValueRep := 0;
                  end if;
               end if;

               MarketPrice_NEW.Val_MarketRep := '%';
            else
               MarketPrice_NEW.Val_MarketRep := RSI_GetValMarketByRate(Deal.FI.t_FIID, C0);
            end if;
         end if;
      end if;
    end; --CalcMarketPrice_NEW

    procedure CalcMarketPrice_REZ( FIID IN NUMBER, OnDate IN DATE )
     is
      v_fininstr     dfininstr_dbt%ROWTYPE;
      v_IsShare      NUMBER;
      v_AvrRoot      NUMBER;
      C0             DRATEDEF_DBT%ROWTYPE;
      ValMarketID    NUMBER;
      Nominal        NUMBER(32,12);
    begin

      MarketPrice_REZ := NULL;

      if RateTypes.MinRate = 0 or RateTypes.MinRate is null then -- т.е., если первый вход и ещё ничего не закачивали
        GetSettingsTax(1);
      end if;

      begin
        select * into v_fininstr from dfininstr_dbt where t_FIID = FIID;
        exception
          when NO_DATA_FOUND then
            MarketPrice_REZ.ErrorMsg := 'Невозможно найти ФИ с ID = '||FIID;
            return;
      end;

      MarketPrice_REZ.FIID   := FIID;
      MarketPrice_REZ.OnDate := OnDate;

      MarketPrice_REZ.IfMarketRez := chr(0);
      MarketPrice_REZ.MrktRez     := chr(0);
      MarketPrice_REZ.DMrktRez    := TO_DATE('01.01.0001','DD.MM.YYYY');
      MarketPrice_REZ.RezMrkt     := 0;

      -- Признак обращаемости
      if( RSI_rsb_fiinstr.FI_CirculateInMarket( MarketPrice_REZ.FIID, OnDate ) = 1 ) then

         v_IsShare := 0;
         v_AvrRoot := RSI_RSB_FIInstr.FI_AvrKindsGetRoot( 2, v_fininstr.t_AvoirKind );
         if( v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_SHARE OR
             v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_INVESTMENT_SHARE OR
             v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_DEPOS_RECEIPT
           ) then
           v_IsShare := 1;
         end if;

         -- Ищем курс в периоде трех месяцев до даты расчета резерва MarketPrice_REZ.OnDate ВКЛЮЧИТЕЛЬНО
         if( SPGetRate(MarketPrice_REZ.FIID, -1, RateTypes.TaxReserv, MarketPrice_REZ.OnDate, MarketPrice_REZ.OnDate-add_months(MarketPrice_REZ.OnDate,-3), C0, 'RUS', 0, 1/*минимальная*/, v_IsShare) = 0 ) then

            begin
              select t_ShortName into MarketPrice_REZ.MrktRez from dparty_dbt where t_PartyID = C0.t_Market_Place;
            exception
              when NO_DATA_FOUND then MarketPrice_REZ.MrktRez := chr(0);
            end;

            MarketPrice_REZ.IfMarketRez := chr(88);
            MarketPrice_REZ.DMrktRez    := C0.t_SinceDate;
            MarketPrice_REZ.RezMrkt     := RSI_GetValByRate(MarketPrice_REZ.FIID, C0, MarketPrice_REZ.OnDate);

            if( C0.t_OtherFI = MarketPrice_REZ.FIID ) then
               ValMarketID := C0.t_FIID;
            else
               ValMarketID := C0.t_OtherFI;
            end if;

            -- Для облигаций выводится в процентах от номинала на дату котировки.
            if( v_AvrRoot = RSI_RSB_FIInstr.AVOIRKIND_BOND ) then
               if( C0.t_IsRelative != chr(88) ) then

                  MarketPrice_REZ.RezMrkt := RSI_RSB_FIInstr.ConvSum(MarketPrice_REZ.RezMrkt, ValMarketID, v_fininstr.t_FaceValueFI, C0.t_SinceDate);
                  Nominal := RSI_RSB_FIInstr.FI_GetNominalOnDate(MarketPrice_REZ.FIID, C0.t_SinceDate);

                  if( Nominal != 0 ) then
                     MarketPrice_REZ.RezMrkt := MarketPrice_REZ.RezMrkt / Nominal * 100;
                  else
                     MarketPrice_REZ.RezMrkt := 0;
                  end if;

               end if;
            else
            -- Для акций и депозитарных расписок котировка выводится в валюте номинала (при необходимости конвертируется в валюту номинала на дату котировки).
               if( C0.t_IsRelative = chr(88) ) then
                  Nominal := RSI_RSB_FIInstr.FI_GetNominalOnDate(MarketPrice_REZ.FIID, C0.t_SinceDate);
                  MarketPrice_REZ.RezMrkt := MarketPrice_REZ.RezMrkt * Nominal / 100;
               else
                  MarketPrice_REZ.RezMrkt := RSI_RSB_FIInstr.ConvSum(MarketPrice_REZ.RezMrkt, ValMarketID, v_fininstr.t_FaceValueFI, C0.t_SinceDate);
               end if;
            end if;

         end if;
      end if;

    end; --CalcMarketPrice_REZ

    function GetMarketPrice( LotID IN NUMBER, LotBuyID IN NUMBER, IsFutures IN NUMBER, Is600Reg IN NUMBER )
     return NUMBER
      is
    begin
      if( MarketPrice.LotID is NULL or MarketPrice.LotID <> LotID ) then
         CalcMarketPrice( LotID, LotBuyID, IsFutures, Is600Reg );
      end if;
      return iif(MarketPrice.MarketPrice is not NULL, MarketPrice.MarketPrice, 0.0);
    end;

    function GetMarket( LotID IN NUMBER, LotBuyID IN NUMBER, IsFutures IN NUMBER )
     return VARCHAR2
      is
    begin
      if( MarketPrice.LotID is NULL or MarketPrice.LotID <> LotID ) then
         CalcMarketPrice( LotID, LotBuyID, IsFutures );
      end if;
      return MarketPrice.Market;
    end;

    function GetDateMarket( LotID IN NUMBER, LotBuyID IN NUMBER, IsFutures IN NUMBER )
     return DATE
      is
    begin
      if( MarketPrice.LotID is NULL or MarketPrice.LotID <> LotID ) then
         CalcMarketPrice( LotID, LotBuyID, IsFutures );
      end if;
      return MarketPrice.DateMarket;
    end;

    function GetDate2( LotID IN NUMBER, LotBuyID IN NUMBER, IsFutures IN NUMBER )
     return DATE
      is
    begin
      if( MarketPrice.LotID is NULL or MarketPrice.LotID <> LotID ) then
         CalcMarketPrice( LotID, LotBuyID, IsFutures );
      end if;
      return MarketPrice.Date2;
    end;

    function IfMarket( LotID IN NUMBER, LotBuyID IN NUMBER, IsFutures IN NUMBER )
     return VARCHAR2
      is
    begin
      if( MarketPrice.LotID is NULL or MarketPrice.LotID <> LotID) then
         CalcMarketPrice( LotID, LotBuyID, IsFutures );
      end if;
      return MarketPrice.IfMarket;
    end;

    function GetMarketPrice_NEW( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') )
     return NUMBER
      is
    begin
      if( MarketPrice_NEW.LotID is NULL or MarketPrice_NEW.LotID <> LotID ) then
         CalcMarketPrice_NEW( LotID, RepDate );
      end if;
      return iif(MarketPrice_NEW.MarketPrice_NEW is not NULL, MarketPrice_NEW.MarketPrice_NEW, 0.0);
    end;

    function GetQuoteValue_New( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') )
     return NUMBER
      is
    begin
      if( MarketPrice_NEW.LotID is NULL or MarketPrice_NEW.LotID <> LotID ) then
         CalcMarketPrice_NEW( LotID, RepDate );
      end if;
      return iif(MarketPrice_NEW.QuoteValue_New is not NULL, MarketPrice_NEW.QuoteValue_New, 0.0);
    end;

    function GetMarket_NEW( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') )
     return VARCHAR2
      is
    begin
      if( MarketPrice_NEW.LotID is NULL or MarketPrice_NEW.LotID <> LotID ) then
         CalcMarketPrice_NEW( LotID, RepDate );
      end if;
      return MarketPrice_NEW.Market_NEW;
    end;

    function GetDateMarket_NEW( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') )
     return DATE
      is
    begin
      if( MarketPrice_NEW.LotID is NULL or MarketPrice_NEW.LotID <> LotID ) then
         CalcMarketPrice_NEW( LotID, RepDate );
      end if;
      return MarketPrice_NEW.DateMarket_NEW;
    end;

    function GetVal_Market_NEW( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') )
     return VARCHAR2
      is
    begin
      if( MarketPrice_NEW.LotID is NULL or MarketPrice_NEW.LotID <> LotID ) then
         CalcMarketPrice_NEW( LotID, RepDate );
      end if;
      return MarketPrice_NEW.Val_Market_NEW;
    end;

    function IfMarket_NEW( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') )
     return VARCHAR2
      is
    begin
      if( MarketPrice_NEW.LotID is NULL or MarketPrice_NEW.LotID <> LotID ) then
         CalcMarketPrice_NEW( LotID, RepDate );
      end if;
      return MarketPrice_NEW.IfMarket_NEW;
    end;

    function GetQuoteValueRep( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') )
     return NUMBER
      is
    begin
      if( MarketPrice_NEW.LotID is NULL or MarketPrice_NEW.LotID <> LotID ) then
         CalcMarketPrice_NEW( LotID, RepDate );
      end if;
      return iif(MarketPrice_NEW.QuoteValueRep is not NULL, MarketPrice_NEW.QuoteValueRep, 0.0);
    end;

    function GetMarketRep( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') )
     return VARCHAR2
      is
    begin
      if( MarketPrice_NEW.LotID is NULL or MarketPrice_NEW.LotID <> LotID ) then
         CalcMarketPrice_NEW( LotID, RepDate );
      end if;
      return MarketPrice_NEW.MarketRep;
    end;

    function GetDateMarketRep( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') )
     return DATE
      is
    begin
      if( MarketPrice_NEW.LotID is NULL or MarketPrice_NEW.LotID <> LotID ) then
         CalcMarketPrice_NEW( LotID, RepDate );
      end if;
      return MarketPrice_NEW.DateMarketRep;
    end;

    function GetVal_MarketRep( LotID IN NUMBER, RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') )
     return VARCHAR2
      is
    begin
      if( MarketPrice_NEW.LotID is NULL or MarketPrice_NEW.LotID <> LotID ) then
         CalcMarketPrice_NEW( LotID, RepDate );
      end if;
      return MarketPrice_NEW.Val_MarketRep;
    end;

    function GetErrorMsg
     return VARCHAR2
      is
    begin
      return MarketPrice.ErrorMsg;
    end;

    function GetRezMrkt( FIID IN NUMBER, OnDate IN DATE )
     return NUMBER
      is
    begin
      if( MarketPrice_REZ.FIID is NULL or MarketPrice_REZ.FIID <> FIID or MarketPrice_REZ.OnDate <> OnDate ) then
         CalcMarketPrice_REZ( FIID, OnDate );
      end if;
      return iif(MarketPrice_REZ.RezMrkt is not NULL, MarketPrice_REZ.RezMrkt, 0.0);
    end;

    function IfMarketRez( FIID IN NUMBER, OnDate IN DATE )
     return VARCHAR2
      is
    begin
      if( MarketPrice_REZ.FIID is NULL or MarketPrice_REZ.FIID <> FIID or MarketPrice_REZ.OnDate <> OnDate ) then
         CalcMarketPrice_REZ( FIID, OnDate );
      end if;
      return MarketPrice_REZ.IfMarketRez;
    end;

    function GetMrktRez( FIID IN NUMBER, OnDate IN DATE )
     return VARCHAR2
      is
    begin
      if( MarketPrice_REZ.FIID is NULL or MarketPrice_REZ.FIID <> FIID or MarketPrice_REZ.OnDate <> OnDate ) then
         CalcMarketPrice_REZ( FIID, OnDate );
      end if;
      return MarketPrice_REZ.MrktRez;
    end;

    function GetDMrktRez( FIID IN NUMBER, OnDate IN DATE )
     return DATE
      is
    begin
      if( MarketPrice_REZ.FIID is NULL or MarketPrice_REZ.FIID <> FIID or MarketPrice_REZ.OnDate <> OnDate ) then
         CalcMarketPrice_REZ( FIID, OnDate );
      end if;
      return MarketPrice_REZ.DMrktRez;
    end;

    function GetErrorMsgRez
     return VARCHAR2
      is
    begin
      return MarketPrice_REZ.ErrorMsg;
    end;

  ----------------------------------------------------------------------------------------------
  --                                  Вспомогательные функции                                 --
  ----------------------------------------------------------------------------------------------

  --Проверить наличие категории
  FUNCTION CheckCateg( ObjType IN NUMBER, GroupID IN NUMBER, ObjID IN VARCHAR2, AttrID IN NUMBER )
    RETURN NUMBER DETERMINISTIC
  IS
    v_CT NUMBER;
  BEGIN

    SELECT COUNT(1)
      INTO v_CT
      FROM dobjatcor_dbt
     WHERE t_ObjectType = ObjType
       AND t_GroupID    = GroupID
       AND t_Object     = ObjID
       AND t_AttrID     = AttrID;

    IF v_CT > 0 THEN
      RETURN 1;
    END IF;

    RETURN 0;

    EXCEPTION
      WHEN NO_DATA_FOUND
        THEN RETURN 0;
  END;

  -- получить тип лота в зависимости от операции
  FUNCTION get_lotType( oGrp IN NUMBER, DealID IN NUMBER, DealPart IN NUMBER DEFAULT 1 )
    RETURN NUMBER DETERMINISTIC
  IS
  BEGIN
    IF (Rsb_Secur.IsBuy(oGrp)=1 OR
        Rsb_Secur.IsAvrWrtIn(oGrp)=1) AND
       Rsb_Secur.IsRepo(oGrp)<>1 AND
       Rsb_Secur.IsBackSale(oGrp)<>1 AND
       Rsb_Secur.IsLoan(oGrp)<>1 THEN
     RETURN RSB_SCTXC.TXLOTS_BUY;
    ELSIF (Rsb_Secur.IsSale(oGrp)=1 OR
           Rsb_Secur.IsAvrWrtOut(oGrp)=1 OR
           Rsb_Secur.IsRet_Issue(oGrp)=1) AND
          Rsb_Secur.IsRepo(oGrp)<>1 AND
          Rsb_Secur.IsBackSale(oGrp)<>1 AND
          Rsb_Secur.IsLoan(oGrp)<>1 THEN
      RETURN RSB_SCTXC.TXLOTS_SALE;
    ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
          Rsb_Secur.IsRepo(oGrp)=1 THEN
      if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
         RETURN RSB_SCTXC.TXLOTS_BACKREPO;
      else
         RETURN iif( DealPart = 1, RSB_SCTXC.TXLOTS_BUY, RSB_SCTXC.TXLOTS_SALE );
      end if;
    ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
          Rsb_Secur.IsLoan(oGrp)=1 THEN
      -- проверяем наличие категории "Является налоговым РЕПО" == "НЕТ"
      if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
         RETURN RSB_SCTXC.TXLOTS_LOANGET;
      else
         RETURN iif( DealPart = 1, RSB_SCTXC.TXLOTS_BUY, RSB_SCTXC.TXLOTS_SALE );
      end if;
    ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
          Rsb_Secur.IsRepo(oGrp)=1 THEN
      if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
         RETURN RSB_SCTXC.TXLOTS_REPO;
      else
         RETURN iif( DealPart = 1, RSB_SCTXC.TXLOTS_SALE, RSB_SCTXC.TXLOTS_BUY );
      end if;
    ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
          Rsb_Secur.IsLoan(oGrp)=1 THEN
      -- проверяем наличие категории "Является налоговым РЕПО" == "НЕТ"
      if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
         RETURN RSB_SCTXC.TXLOTS_LOANPUT;
      else
         RETURN iif( DealPart = 1, RSB_SCTXC.TXLOTS_SALE, RSB_SCTXC.TXLOTS_BUY );
      end if;
    ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
          Rsb_Secur.IsBackSale(oGrp)=1 AND
          Rsb_Secur.IsRepo(oGrp)<>1 AND
          Rsb_Secur.IsLoan(oGrp)<>1 THEN
      RETURN iif( DealPart = 1, RSB_SCTXC.TXLOTS_BUY, RSB_SCTXC.TXLOTS_SALE );
    ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
          Rsb_Secur.IsBackSale(oGrp)=1 AND
          Rsb_Secur.IsRepo(oGrp)<>1 AND
          Rsb_Secur.IsLoan(oGrp)<>1 THEN
      RETURN iif( DealPart = 1, RSB_SCTXC.TXLOTS_SALE, RSB_SCTXC.TXLOTS_BUY );
    ELSIF Rsb_Secur.check_GroupStr(oGrp,rsb_secur.IS_CONV_SHARE)=1 OR
          Rsb_Secur.check_GroupStr(oGrp,rsb_secur.IS_CONV_RECEIPT)=1 THEN
      RETURN iif( DealPart = 1, RSB_SCTXC.TXLOTS_SALE, RSB_SCTXC.TXLOTS_BUY );
    END IF;

    RETURN RSB_SCTXC.TXLOTS_UNDEF;
  END; --get_lotType

  -- получить название лота в зависимости от типа
  FUNCTION get_lotName( in_Type IN NUMBER )
    RETURN VARCHAR2 DETERMINISTIC
  IS
    v_Name VARCHAR2(30);
  BEGIN
    IF in_Type = RSB_SCTXC.TXLOTS_BUY THEN
      v_Name := 'Покупка';
    ELSIF in_Type = RSB_SCTXC.TXLOTS_SALE THEN
      v_Name := 'Продажа';
    ELSIF in_Type = RSB_SCTXC.TXLOTS_REPO THEN
      v_Name := 'Репо прямое';
    ELSIF in_Type = RSB_SCTXC.TXLOTS_BACKREPO THEN
      v_Name := 'Репо обратное';
    ELSIF in_Type = RSB_SCTXC.TXLOTS_LOANPUT THEN
      v_Name := 'Займ размещение';
    ELSIF in_Type = RSB_SCTXC.TXLOTS_LOANGET  THEN
      v_Name := 'Займ привлечение';
    ELSE
      v_Name := 'Не определено';
    END IF;

    RETURN v_Name;
  END;

  -- получить дату покупки лота в зависимости от операции
  FUNCTION get_lotBuyDate( oGrp IN NUMBER, DealID IN NUMBER, FactDate IN DATE, DealPart IN NUMBER DEFAULT 1 )
    RETURN DATE DETERMINISTIC
  IS
  BEGIN
    IF Rsb_Secur.IsBuy(oGrp)=1 AND
       Rsb_Secur.IsRepo(oGrp)<>1 AND
       Rsb_Secur.IsBackSale(oGrp)<>1 AND
       Rsb_Secur.IsLoan(oGrp)<>1 THEN
      RETURN FactDate;
    ELSIF( Rsb_Secur.IsAvrWrtIn(oGrp) = 1 ) THEN
      RETURN RSI_NPTO.GetDateFromAvrWrtIn(DealID, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY'));
    ELSIF (Rsb_Secur.IsSale(oGrp)=1 OR
           Rsb_Secur.IsAvrWrtOut(oGrp)=1 OR
           Rsb_Secur.IsRet_Issue(oGrp)=1) AND
          Rsb_Secur.IsRepo(oGrp)<>1 AND
          Rsb_Secur.IsBackSale(oGrp)<>1 AND
          Rsb_Secur.IsLoan(oGrp)<>1 THEN
      RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
    ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
          Rsb_Secur.IsRepo(oGrp)=1 THEN
      if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
         RETURN FactDate;
      else
         RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
      end if;
    ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
          Rsb_Secur.IsLoan(oGrp)=1 THEN
      if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
         RETURN FactDate;
      else
         RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
      end if;
    ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
          Rsb_Secur.IsRepo(oGrp)=1 THEN
      if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
         RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
      else
         RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
      end if;
    ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
          Rsb_Secur.IsLoan(oGrp)=1 THEN
      if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
         RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
      else
         RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
      end if;
    ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
          Rsb_Secur.IsBackSale(oGrp)=1 AND
          Rsb_Secur.IsRepo(oGrp)<>1 AND
          Rsb_Secur.IsLoan(oGrp)<>1 THEN
      RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
    ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
          Rsb_Secur.IsBackSale(oGrp)=1 AND
          Rsb_Secur.IsRepo(oGrp)<>1 AND
          Rsb_Secur.IsLoan(oGrp)<>1 THEN
      RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
    ELSIF Rsb_Secur.check_GroupStr(oGrp,rsb_secur.IS_CONV_SHARE)=1 OR
          Rsb_Secur.check_GroupStr(oGrp,rsb_secur.IS_CONV_RECEIPT)=1 THEN
      RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
    END IF;

    RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
  END; --get_lotBuyDate

  -- получить дату продажи лота в зависимости от операции
  FUNCTION get_lotSaleDate( oGrp IN NUMBER, DealID IN NUMBER, FactDate IN DATE, DealPart IN NUMBER DEFAULT 1 )
    RETURN DATE DETERMINISTIC
  IS
  BEGIN
    IF (Rsb_Secur.IsBuy(oGrp)=1 OR
        Rsb_Secur.IsAvrWrtIn(oGrp)=1) AND
       Rsb_Secur.IsRepo(oGrp)<>1 AND
       Rsb_Secur.IsBackSale(oGrp)<>1 AND
       Rsb_Secur.IsLoan(oGrp)<>1 THEN
      RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
    ELSIF( Rsb_Secur.IsAvrWrtOut(oGrp) = 1 ) THEN
      RETURN RSI_NPTO.GetDateFromAvrWrtIn(DealID, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY'));
    ELSIF (Rsb_Secur.IsSale(oGrp)=1 OR
           Rsb_Secur.IsRet_Issue(oGrp)=1) AND
          Rsb_Secur.IsRepo(oGrp)<>1 AND
          Rsb_Secur.IsBackSale(oGrp)<>1 AND
          Rsb_Secur.IsLoan(oGrp)<>1 THEN
      RETURN FactDate;
    ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
          Rsb_Secur.IsRepo(oGrp)=1 THEN
      if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
         RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
      else
         RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
      end if;
    ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
          Rsb_Secur.IsLoan(oGrp)=1 THEN
      if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
         RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
      else
         RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
      end if;
    ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
          Rsb_Secur.IsRepo(oGrp)=1 THEN
      if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
         RETURN FactDate;
      else
         RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
      end if;
    ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
          Rsb_Secur.IsLoan(oGrp)=1 THEN
      if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
         RETURN FactDate;
      else
         RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
      end if;
    ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
          Rsb_Secur.IsBackSale(oGrp)=1 AND
          Rsb_Secur.IsRepo(oGrp)<>1 AND
          Rsb_Secur.IsLoan(oGrp)<>1 THEN
      RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
    ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
          Rsb_Secur.IsBackSale(oGrp)=1 AND
          Rsb_Secur.IsRepo(oGrp)<>1 AND
          Rsb_Secur.IsLoan(oGrp)<>1 THEN
      RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
    ELSIF Rsb_Secur.check_GroupStr(oGrp,rsb_secur.IS_CONV_SHARE)=1 OR
          Rsb_Secur.check_GroupStr(oGrp,rsb_secur.IS_CONV_RECEIPT)=1 THEN
      RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
    END IF;

    RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
  END; --get_lotSaleDate

  ----- получить код лота
  FUNCTION get_lotCode( p_Code IN VARCHAR2, p_Num IN NUMBER ) RETURN VARCHAR2
  IS
     v_Code VARCHAR2(36);
  BEGIN

     v_Code := p_Code || '_КП' || LPAD( p_Num, 2, '0' );

     RETURN v_Code;
  END;--get_lotCode

  -- проверить наличие примечание определенного вида
  FUNCTION CheckExistsNote( in_NoteKind IN dnotekind_dbt.t_NoteKind%TYPE,
                            in_ObjType IN dnotetext_dbt.t_ObjectType%TYPE,
                            in_DocID IN NUMBER )
  RETURN NUMBER
  IS
    v_Count NUMBER;
  BEGIN

    SELECT count(1)
      INTO v_Count
      FROM dnotetext_dbt
     WHERE t_ObjectType = in_ObjType AND
           TRIM (TRANSLATE (t_DocumentID, '0123456789', '          ')) IS NULL AND -- отбрасываем нечисловые строки
           TO_NUMBER(t_DocumentID) = in_DocID AND
           t_NoteKind   = in_NoteKind;

    IF v_Count > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END; --CheckExistsNote

  ----Задает приоритет выбытий 2 частей  в зависимости от типа сделки.
    FUNCTION TXGetPart2Order( v_Type IN NUMBER )
      RETURN NUMBER DETERMINISTIC
    IS
    BEGIN

      IF(    ReestrValue.V1 = RSB_SCTXC.TXREG_V1_COMMON ) THEN
        IF( v_Type = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
          RETURN 1;
        ELSIF( v_Type = RSB_SCTXC.TXLOTS_LOANGET ) THEN
          RETURN 1;
        END IF;
      ELSIF( ReestrValue.V1 = RSB_SCTXC.TXREG_V1_ZR ) THEN
        IF( v_Type = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
          RETURN 2;
        ELSIF( v_Type = RSB_SCTXC.TXLOTS_LOANGET ) THEN
          RETURN 1;
        END IF;
      ELSIF( ReestrValue.V1 = RSB_SCTXC.TXREG_V1_RZ ) THEN
        IF( v_Type = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
          RETURN 1;
        ELSIF( v_Type = RSB_SCTXC.TXLOTS_LOANGET ) THEN
          RETURN 2;
        END IF;
      END IF;

    END; --TXGetPart2Order

  ----Задает приоритет выбытий 1 ч. Репо/Займа в зависимости от типа сделки.
    FUNCTION TXGetSaleOrder( v_Type IN NUMBER )
      RETURN NUMBER DETERMINISTIC
    IS
    BEGIN

      IF(    ReestrValue.V2 = RSB_SCTXC.TXREG_V2_COMMON ) THEN
        IF( v_Type = RSB_SCTXC.TXLOTS_REPO ) THEN
          RETURN 1;
        ELSIF( v_Type = RSB_SCTXC.TXLOTS_LOANPUT ) THEN
          RETURN 1;
        END IF;
      ELSIF( ReestrValue.V2 = RSB_SCTXC.TXREG_V2_RZ ) THEN
        IF( v_Type = RSB_SCTXC.TXLOTS_REPO ) THEN
          RETURN 1;
        ELSIF( v_Type = RSB_SCTXC.TXLOTS_LOANPUT ) THEN
          RETURN 2;
        END IF;
      ELSIF( ReestrValue.V2 = RSB_SCTXC.TXREG_V2_ZR ) THEN
        IF( v_Type = RSB_SCTXC.TXLOTS_REPO ) THEN
          RETURN 2;
        ELSIF( v_Type = RSB_SCTXC.TXLOTS_LOANPUT ) THEN
          RETURN 1;
        END IF;
      END IF;

    END; --TXGetSaleOrder

  ----Задает приоритет приходов в зависимости от типов сделок прихода и выбытия
    FUNCTION TXGetBuyOrder( v_buyType IN NUMBER, v_saleType IN NUMBER )
      RETURN NUMBER DETERMINISTIC
    IS
    BEGIN

      IF( v_saleType = RSB_SCTXC.TXLOTS_SALE ) THEN
        IF(    ReestrValue.V3 = RSB_SCTXC.TXREG_V3_COMMON ) THEN
          IF( v_buyType = RSB_SCTXC.TXLOTS_LOANGET ) THEN
            RETURN 1;
          ELSIF( v_buyType = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
            RETURN 1;
          ELSIF(v_buyType = RSB_SCTXC.TXLOTS_UNDEF) THEN
            RETURN -1;
          END IF;
        ELSIF( ReestrValue.V3 = RSB_SCTXC.TXREG_V3_ZR ) THEN
          IF( v_buyType = RSB_SCTXC.TXLOTS_LOANGET ) THEN
            RETURN 1;
          ELSIF( v_buyType = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
            RETURN 2;
          ELSIF(v_buyType = RSB_SCTXC.TXLOTS_UNDEF) THEN
            RETURN -1;
          END IF;
        ELSIF( ReestrValue.V3 = RSB_SCTXC.TXREG_V3_RZ ) THEN
          IF( v_buyType = RSB_SCTXC.TXLOTS_LOANGET ) THEN
            RETURN 2;
          ELSIF( v_buyType = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
            RETURN 1;
          ELSIF(v_buyType = RSB_SCTXC.TXLOTS_UNDEF) THEN
            RETURN -1;
          END IF;
        END IF;
      ELSE
        IF(    ReestrValue.V4 = RSB_SCTXC.TXREG_V4_COMMON ) THEN
          IF( v_buyType = RSB_SCTXC.TXLOTS_BUY ) THEN
            RETURN 1;
          ELSIF( v_buyType = RSB_SCTXC.TXLOTS_LOANGET ) THEN
            RETURN 1;
          ELSIF( v_buyType = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
            RETURN 1;
          ELSIF(v_buyType = RSB_SCTXC.TXLOTS_UNDEF) THEN
            RETURN -1;
          END IF;
        ELSIF( ReestrValue.V4 = RSB_SCTXC.TXREG_V4_ZR_B ) THEN
          IF( v_buyType = RSB_SCTXC.TXLOTS_BUY ) THEN
            RETURN 2;
          ELSIF( v_buyType = RSB_SCTXC.TXLOTS_LOANGET ) THEN
            RETURN 1;
          ELSIF( v_buyType = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
            RETURN 1;
          ELSIF(v_buyType = RSB_SCTXC.TXLOTS_UNDEF) THEN
            RETURN -1;
          END IF;
        ELSIF( ReestrValue.V4 = RSB_SCTXC.TXREG_V4_B_RZ ) THEN
          IF( v_buyType = RSB_SCTXC.TXLOTS_BUY ) THEN
            RETURN 1;
          ELSIF( v_buyType = RSB_SCTXC.TXLOTS_LOANGET ) THEN
            RETURN 2;
          ELSIF( v_buyType = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
            RETURN 2;
          ELSIF(v_buyType = RSB_SCTXC.TXLOTS_UNDEF) THEN
            RETURN -1;
          END IF;
        END IF;
      END IF;
      RETURN 0;
    END; --TXGetBuyOrder

  ----Задает приоритет связей при формировании подстановок для подбора приобретений при обработке связей при подстановке.
    FUNCTION TXGetSubstOrderBuy( v_Type IN NUMBER )
      RETURN NUMBER DETERMINISTIC
    IS
    BEGIN

      IF(    ReestrValue.V5 = RSB_SCTXC.TXREG_V5_COMMON ) THEN
        IF( v_Type = RSB_SCTXC.TXLOTS_BUY ) THEN
          RETURN 1;
        ELSIF( v_Type = RSB_SCTXC.TXLOTS_LOANGET ) THEN
          RETURN 1;
        ELSIF( v_Type = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
          RETURN 1;
        ELSIF(v_Type = RSB_SCTXC.TXLOTS_UNDEF) THEN
          RETURN -1;
        END IF;
      ELSIF(    ReestrValue.V5 = RSB_SCTXC.TXREG_V5_ZR_B ) THEN
        IF( v_Type = RSB_SCTXC.TXLOTS_BUY ) THEN
          RETURN 2;
        ELSIF( v_Type = RSB_SCTXC.TXLOTS_LOANGET ) THEN
          RETURN 1;
        ELSIF( v_Type = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
          RETURN 1;
        ELSIF(v_Type = RSB_SCTXC.TXLOTS_UNDEF) THEN
          RETURN -1;
        END IF;
      ELSIF(    ReestrValue.V5 = RSB_SCTXC.TXREG_V5_B_RZ ) THEN
        IF( v_Type = RSB_SCTXC.TXLOTS_BUY ) THEN
          RETURN 1;
        ELSIF( v_Type = RSB_SCTXC.TXLOTS_LOANGET ) THEN
          RETURN 2;
        ELSIF( v_Type = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
          RETURN 2;
        ELSIF(v_Type = RSB_SCTXC.TXLOTS_UNDEF) THEN
          RETURN -1;
        END IF;
      END IF;
    END; --TXGetSubstOrderBuy

  ----Задает приоритет связей при закрытии позиции для подбора сделок приобретений
    FUNCTION TXGetClPosOrderRepo( v_buyType IN NUMBER )
      RETURN NUMBER DETERMINISTIC
    IS
    BEGIN
      IF v_buyType = RSB_SCTXC.TXLOTS_BUY THEN
        RETURN 0;
      ELSIF( ReestrValue.V6 = RSB_SCTXC.TXREG_V6_COMMON ) THEN
        IF( v_buyType = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
          RETURN 1;
        ELSIF( v_buyType = RSB_SCTXC.TXLOTS_LOANGET ) THEN
          RETURN 1;
        ELSIF(v_buyType = RSB_SCTXC.TXLOTS_UNDEF) THEN
          RETURN -1;
        END IF;
      ELSIF( ReestrValue.V6 = RSB_SCTXC.TXREG_V6_RZ ) THEN
        IF( v_buyType = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
          RETURN 1;
        ELSIF( v_buyType = RSB_SCTXC.TXLOTS_LOANGET ) THEN
          RETURN 2;
        ELSIF(v_buyType = RSB_SCTXC.TXLOTS_UNDEF) THEN
          RETURN -1;
        END IF;
      ELSIF( ReestrValue.V6 = RSB_SCTXC.TXREG_V6_ZR ) THEN
        IF( v_buyType = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
          RETURN 2;
        ELSIF( v_buyType = RSB_SCTXC.TXLOTS_LOANGET ) THEN
          RETURN 1;
        ELSIF(v_buyType = RSB_SCTXC.TXLOTS_UNDEF) THEN
          RETURN -1;
        END IF;
      END IF;
    END; --TXGetClPosOrderRepo

  ----Вычисляет значение признака наличия свободного остатка в зависимости от параметров лота.
    FUNCTION TXGetIsFree( v_AMOUNT IN NUMBER, v_NETTING IN NUMBER, v_SALE IN NUMBER,
                          v_RETFLAG IN CHAR, v_INACC IN CHAR, v_BLOCKED IN CHAR )
      RETURN CHAR DETERMINISTIC
    IS
    BEGIN
      IF ((v_AMOUNT - v_NETTING - v_SALE > 0) AND
          (v_RETFLAG = CHR(0)) AND
          (v_INACC = CHR(88)) AND
          (v_BLOCKED = CHR(0) OR ReestrValue.V10 = RSB_SCTXC.TXREG_V10_YES)
         ) THEN
         RETURN CHR(88);
      ELSE
         RETURN CHR(0);
      END IF;
    END;--TXGetIsFree

  ---- Вычисляет перенастройку параметров лотов при изменении настроек.
  ---- Вызывается после изменения положения настроек или при апгрейде.
  ---- Аргументы равны "Да" (Да = 1, нет = 0), если требуется пересчитать значения соотв. полей, по умолчанию  - "Да"
    PROCEDURE TXRetuningLots( v_OrdForSale IN NUMBER, v_OrdForRepo IN NUMBER,
                              v_OrdForSubst IN NUMBER, v_OrdForClPosRepo IN NUMBER, v_IsFree IN NUMBER)
    IS
       OrdForSale      NUMBER;
       OrdForRepo      NUMBER;
       OrdForSubst     NUMBER;
       OrdForClPosRepo NUMBER;
       IsFree          CHAR;

       cursor c_Lots is
              SELECT T_ID, T_TYPE, T_AMOUNT, T_NETTING, T_SALE, T_RETFLAG, T_INACC, T_BLOCKED,
                     T_ORDFORSALE, T_ORDFORREPO, T_ORDFORSUBST, T_ORDFORCLPOSREPO, T_ISFREE
                FROM dsctxlot_dbt
               WHERE T_TYPE IN (RSB_SCTXC.TXLOTS_BUY, RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET);
    BEGIN
      GetSettingsTax;

      FOR Lot IN c_Lots LOOP
         OrdForSale      := Lot.T_ORDFORSALE;
         OrdForRepo      := Lot.T_ORDFORREPO;
         OrdForSubst     := Lot.T_ORDFORSUBST;
         OrdForClPosRepo := Lot.T_ORDFORCLPOSREPO;
         IsFree          := Lot.T_ISFREE;

         IF (v_OrdForSale = 1) THEN
           OrdForSale := TXGetBuyOrder(Lot.T_TYPE, RSB_SCTXC.TXLOTS_SALE);
         END IF;

         IF (v_OrdForRepo = 1) THEN
           OrdForRepo := TXGetBuyOrder(Lot.T_TYPE, RSB_SCTXC.TXLOTS_BACKREPO);
         END IF;

         IF (v_OrdForSubst = 1) THEN
           OrdForSubst := TXGetSubstOrderBuy(Lot.T_TYPE);
         END IF;

         IF (v_OrdForClPosRepo = 1) THEN
           OrdForClPosRepo := TXGetClPosOrderRepo(Lot.T_TYPE);
         END IF;

         IF (v_IsFree = 1) THEN
           IsFree := TXGetIsFree(Lot.T_AMOUNT, Lot.T_NETTING, Lot.T_SALE, Lot.T_RETFLAG, Lot.T_INACC, Lot.T_BLOCKED);
         END IF;

         UPDATE dsctxlot_dbt
            SET T_ORDFORSALE      = OrdForSale     ,
                T_ORDFORREPO      = OrdForRepo     ,
                T_ORDFORSUBST     = OrdForSubst    ,
                T_ORDFORCLPOSREPO = OrdForClPosRepo,
                T_ISFREE          = IsFree
          WHERE T_ID = Lot.T_ID;

      END LOOP;

    END; --TXRetuningLots

  ----Сгенерировать номер виртуального лота
    FUNCTION TXGenVirtNum( in_IsSale IN NUMBER,
                           in_date IN DATE,
                           in_Count IN NUMBER )
      RETURN VARCHAR2
    IS
    BEGIN

      RETURN 'V'||
             iif(in_IsSale = 1, 'S', 'B')||'C'||
             TO_CHAR(in_date, 'DDMMYY')||
             '/'||
             LTRIM(TO_CHAR(in_Count,'FM09999'));
    END;

  ----Получить кол-во виртуальных лотов по его номеру
    FUNCTION TXGetVirtCountByNum( in_Number IN VARCHAR2 )
      RETURN NUMBER DETERMINISTIC
    IS
    BEGIN
      RETURN TO_NUMBER(SUBSTR(in_Number, INSTR(in_Number, '/') + 1));
    END;

    --Выполняет пересортировку сделок за дату DealDate и время DealTime, начиная с лота BegLotID
    PROCEDURE RSI_TXDealSortOnDate (FIID IN NUMBER, DealDate IN DATE, DealTime IN DATE, BegLotID IN NUMBER)
    IS
      v_BegDealSortCode VARCHAR2(30) := CHR(1);
      v_DealSortCode    VARCHAR2(30) := CHR(1);
      v_DealSortPrev    NUMBER := -1;
      v_DS              NUMBER;

      CURSOR CDealSort(v_BegDealSortCode IN VARCHAR2, v_DealSortPrev IN NUMBER ) IS
                          SELECT t_ID, t_DealSortCode
                            FROM dsctxlot_dbt
                           WHERE t_FIID          = FIID
                             AND t_DealDate      = DealDate
                             AND t_DealTime      = DealTime
                             AND t_DealSortCode >= v_BegDealSortCode
                             AND (t_DealSort     > v_DealSortPrev OR t_DealSort = -1)
                           ORDER BY t_DealSortCode ASC, t_ID ASC;
    BEGIN
      IF (BegLotID <> 0) THEN
        BEGIN
          SELECT t_DealSortCode
            INTO v_BegDealSortCode
            FROM dsctxlot_dbt
           WHERE t_ID = BegLotID;
        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN v_BegDealSortCode := CHR(1);
        END;
      END IF;

      -- если не с конкретной сделки, найдём минимальный код для сортировки для всех непронумерованных лотов с этой бумагой/датой/временем
      IF (v_BegDealSortCode = CHR(1)) THEN
        BEGIN
          SELECT MIN(t_DealSortCode)
            INTO v_BegDealSortCode
            FROM dsctxlot_dbt
           WHERE t_FIID     = FIID
             AND t_DealDate = DealDate
             AND t_DealTime = DealTime
             AND t_DealSort = -1; --минимальную непронумерованную
        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN v_BegDealSortCode := CHR(1);
        END;
      END IF;


      BEGIN
        SELECT NVL(MAX(t_DealSort), -1)
          INTO v_DealSortPrev
          FROM dsctxlot_dbt
         WHERE t_FIID          = FIID
           AND t_DealDate      = DealDate
           AND t_DealTime      = DealTime
           AND t_DealSortCode <= v_BegDealSortCode
           AND t_DealSort      > -1;
      EXCEPTION
        WHEN NO_DATA_FOUND
          THEN v_DealSortPrev := -1;
      END;

      v_DS := iif( v_DealSortPrev = -1, 0, v_DealSortPrev);

      v_DealSortCode := v_BegDealSortCode;

      FOR DS IN CDealSort(v_BegDealSortCode, v_DealSortPrev) LOOP
         IF v_DealSortCode <> DS.t_DealSortCode THEN
            v_DS := v_DS + 1;
            v_DealSortCode := DS.t_DealSortCode;
         END IF;

         UPDATE dsctxlot_dbt
            SET T_DEALSORT = v_DS
          WHERE t_ID = DS.t_ID;

      END LOOP;

    END; -- RSI_TXDealSortOnDate

    PROCEDURE RSI_TXDealSortByRangeFIID(pFIID_beg IN NUMBER, pFIID_end IN NUMBER)
    IS
      CURSOR CDealGroup IS
                 SELECT t_FIID, t_DealDate, t_DealTime
                   FROM dsctxlot_dbt
                  WHERE t_DealSort = -1 AND t_FIID >= pFIID_beg and t_FIID < pFIID_end
                  GROUP BY t_FIID, t_DealDate, t_DealTime;
    BEGIN
      FOR DS IN CDealGroup LOOP
        RSI_TXDealSortOnDate (DS.t_FIID, DS.t_DealDate, DS.t_DealTime, 0);
      END LOOP;
    END; -- RSI_TXDealSortByRangeFIID

    --Выполняет пересортировку всех непронумерованных сделок
    PROCEDURE RSI_TXDealSortAll
    IS
      CURSOR CDealGroup IS
                 SELECT t_FIID, t_DealDate, t_DealTime
                   FROM dsctxlot_dbt
                  WHERE t_DealSort = -1
                  GROUP BY t_FIID, t_DealDate, t_DealTime;
      v_task_name VARCHAR2(30);
      v_sql_chunks CLOB;
      v_sql_process VARCHAR2(400);
      v_try NUMBER(5) := 0;
      v_status NUMBER;
    BEGIN

      TXPutMsg( 0,
                -1,
                TXMES_OPTIM,
                'RSI_TXDealSortAll' );
                  
                  
    IF(RunParallel > 1) THEN
      v_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;
      DBMS_PARALLEL_EXECUTE.create_task (task_name => v_task_name);

      v_sql_chunks := 'select min(t_FIID) as min_t_FIID, max(t_FIID) as max_t_FIID
         from (SELECT t_FIID, NTILE('||RunParallel||') over (order by t_FIID) as t_NTILE FROM dsctxlot_dbt WHERE t_DealSort = 0 group by t_fiid )
         group by t_NTILE' ;

      DBMS_PARALLEL_EXECUTE.create_chunks_by_sql(task_name => v_task_name,
                                                 sql_stmt  => v_sql_chunks,
                                                 by_rowid  => FALSE);

      v_sql_process := ' CALL Rsb_SCTX.RSI_TXDealSortByRangeFIID(:start_id, :end_id) ';

      DBMS_PARALLEL_EXECUTE.run_task(task_name => v_task_name,
                                          sql_stmt => v_sql_process,
                                          language_flag => DBMS_SQL.NATIVE,
                                          parallel_level => RunParallel);

      v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
      WHILE(v_try < 2 AND v_status != DBMS_PARALLEL_EXECUTE.FINISHED)
      LOOP
        v_try := v_try + 1;
        DBMS_PARALLEL_EXECUTE.resume_task(v_task_name);
        v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
      END LOOP;


      DBMS_PARALLEL_EXECUTE.drop_task(v_task_name);
      COMMIT;

    ELSE
      FOR DS IN CDealGroup LOOP
        RSI_TXDealSortOnDate (DS.t_FIID, DS.t_DealDate, DS.t_DealTime, 0);
      END LOOP;
    END IF;

    END; -- RSI_TXDealSortAll

    --Выполняет возврат суммарного количества из всех внутридневных Репо за дату D при недостатке ц/б на прочих лотах.
    PROCEDURE RSI_TXReturnAmountFromRepo (FIID IN NUMBER, D IN DATE, SaleID IN NUMBER)
    IS
      CURSOR c_lnk IS SELECT l.t_ID, l.t_Amount
                        FROM dsctxlnk_dbt l, dsctxlot_dbt s
                       WHERE l.t_SaleID = s.t_ID
                         AND s.t_BuyDate = D -- сегодня
                         AND s.t_BuyDate = s.t_SaleDate -- однодневки
                         AND s.t_FIID    = FIID
                         AND l.t_SaleID <> SaleID
                         AND l.t_RETFLAG <> CHR(88) --где флаг еще не установлен
                         AND ((s.t_Type = RSB_SCTXC.TXLOTS_REPO    AND l.t_Type = RSB_SCTXC.TXLNK_DELREPO) OR
                              (s.t_Type = RSB_SCTXC.TXLOTS_LOANPUT AND l.t_Type = RSB_SCTXC.TXLNK_LOANPUT)
                             );
    BEGIN
      TXPutMsg( 0,
                FIID,
                TXMES_DEBUG,
                'Вызов RSI_TXReturnAmountFromRepo FIID = '||FIID||', D = '||D||', SaleID = '||SaleID );

      FOR lnk IN c_lnk LOOP
         UPDATE dsctxlnk_dbt
            SET t_RetFlag = CHR(88)
          WHERE t_ID = lnk.t_ID;
      END LOOP;

      TXPutMsg( 0,
                FIID,
                TXMES_DEBUG,
                'Конец RSI_TXReturnAmountFromRepo FIID = '||FIID||', D = '||D||', SaleID = '||SaleID );
    END; --RSI_TXReturnAmountFromRepo

  ----------------------------------------------------------------------------------------------
  --                                    Функции списаний                                      --
  ----------------------------------------------------------------------------------------------

  ----Выполняет списание лота продажи SALELOT
    PROCEDURE RSI_TXLinkSale( v_SaleLot IN dsctxlot_dbt%ROWTYPE )
    IS
      v_S          NUMBER;
      v_Flag       NUMBER;
      v_FICODE     dfininstr_dbt.t_FI_Code%TYPE;
      v_PriorPort  NUMBER;
      v_DealID     NUMBER;
      v_BofficeKind NUMBER;
      v_OGroup     NUMBER;
      v_G1         NUMBER;
      v_G2         NUMBER;
      v_G3         NUMBER;
      v_G4         NUMBER;
      v_G5         NUMBER;
    BEGIN

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_DEBUG,
                'Вызов RSI_TXLinkSale SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate );
      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_OPTIM,
                'Вызов RSI_TXLinkSale SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate );

      v_S := v_SaleLot.t_Amount - v_SaleLot.t_Netting;

      IF ReestrValue.V14 = RSB_SCTXC.TXREG_V14_YES THEN
         IF( v_SaleLot.t_Origin = RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER )THEN
            RSI_TXLinkSaleToBuy(v_SaleLot, v_S, v_SaleLot.t_Portfolio);
         ELSE
            SELECT t_DealID, t_BofficeKind, RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(TK.T_DEALTYPE, TK.T_BOFFICEKIND))
            INTO v_DealID, v_BofficeKind, v_OGroup
            FROM ddl_tick_dbt TK
            WHERE t_DealID = case when v_SaleLot.t_DealID > 0 then v_SaleLot.t_DealID else nvl((select t_DealID from dsctxlot_dbt where t_ID = v_SaleLot.t_RealID),0) end;

            IF(RSB_SECUR.GetObjAttrNumber(v_BofficeKind, 49, RSB_SECUR.GETMAINOBJATTRNODATE(v_BofficeKind, LPAD(v_DealID, 34, '0'), 49)) <> chr(1))
            THEN
                v_PriorPort := to_number(RSB_SECUR.GetObjAttrNumber(v_BofficeKind, 49, RSB_SECUR.GETMAINOBJATTRNODATE(v_BofficeKind, LPAD(v_DealID, 34, '0'), 49)));
                --Если в сделке продажи в категории ?Приоритетный портфель ц/б? задан портфель ПВО, то значение кате-гории игнорируем
                if( v_PriorPort = RSB_PMWRTOFF.KINDPORT_BACK )then
                   v_PriorPort := -1;
                end if;
            ELSE
                v_PriorPort := -1;
            END IF;

            IF (v_PriorPort != -1)
            THEN
                RSI_TXLinkSaleToBuy(v_SaleLot, v_S, v_PriorPort);
            END IF;

            IF( v_S > 0 and v_PriorPort != v_SaleLot.t_Portfolio )THEN
               RSI_TXLinkSaleToBuy(v_SaleLot, v_S, v_SaleLot.t_Portfolio);
            END IF;

            IF( v_S > 0 )THEN
               RSB_PMWRTOFF.GetWriteOffGroups( v_BofficeKind,
                                               10,
                                               v_SaleLot.t_Portfolio,
                                               RSB_SECUR.IsTwoPart(v_OGroup),
                                               RSB_SECUR.IsSale(v_OGroup),
                                               RSI_RSB_FIInstr.FI_IsKSU(v_SaleLot.t_FIID),
                                               0,
                                               v_G1,
                                               v_G2,
                                               v_G3,
                                               v_G4,
                                               v_G5 );

               IF v_S > 0 and v_G1 > 0 and (v_G1 != v_PriorPort) and (v_G1 != v_SaleLot.t_Portfolio)
               THEN
                   RSI_TXLinkSaleToBuy(v_SaleLot, v_S, v_G1);
               END IF;

               IF v_S > 0 and v_G2 > 0 and (v_G2 != v_PriorPort) and (v_G2 != v_SaleLot.t_Portfolio)
               THEN
                   RSI_TXLinkSaleToBuy(v_SaleLot, v_S, v_G2);
               END IF;

               IF v_S > 0 and v_G3 > 0 and (v_G3 != v_PriorPort) and (v_G3 != v_SaleLot.t_Portfolio)
               THEN
                   RSI_TXLinkSaleToBuy(v_SaleLot, v_S, v_G3);
               END IF;

               IF v_S > 0 and v_G4 > 0 and (v_G4 != v_PriorPort) and (v_G4 != v_SaleLot.t_Portfolio)
               THEN
                   RSI_TXLinkSaleToBuy(v_SaleLot, v_S, v_G4);
               END IF;

               IF v_S > 0 and v_G5 > 0 and (v_G5 != v_PriorPort) and (v_G5 != v_SaleLot.t_Portfolio)
               THEN
                   RSI_TXLinkSaleToBuy(v_SaleLot, v_S, v_G5);
               END IF;
            END IF;
         END IF;
      ELSE
        RSI_TXLinkSaleToBuy(v_SaleLot, v_S);
      END IF;

      IF v_S > 0 THEN
         RSI_TXLinkSaleToReverseRepo(v_SaleLot, v_S);
      END IF;

      IF (v_S > 0) THEN
         SELECT t_FI_Code
           INTO v_FICODE
           FROM dfininstr_dbt
          WHERE t_FIID = v_SaleLot.t_FIID;

         TXPutMsg( v_SaleLot.t_ID,
                   v_SaleLot.t_FIID,
                   TXMES_WARNING,
                   'Недостаточно ц/б "'||v_FICODE||'" на дату '||TO_CHAR(v_SaleLot.t_SaleDate,'DD.MM.YYYY')||
                   ' для списания лота вида "'||get_lotName(v_SaleLot.t_Type)||'" с внешним кодом "'||v_SaleLot.t_DealCodeTS||'"' );
      END IF;

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_DEBUG,
                'Конец RSI_TXLinkSale SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate );
    END; --RSI_TXLinkSale

  ----Выполняет списание лота выбытия прямого Репо/размещения займа SALELOT
  ----из сделок покупок, обратного Репо и привлечения займа, закрытых не в день продажи
    PROCEDURE RSI_TXLinkDirectRepoToBuy( v_SaleLot IN dsctxlot_dbt%ROWTYPE, S IN OUT NUMBER, Portfolio IN NUMBER DEFAULT -1 )
    IS
      v_Buy_ID     NUMBER;
      v_BuyType    NUMBER;
      v_stat       NUMBER;
      v_FreeAmount NUMBER;
      v_Link       dsctxlnk_dbt%ROWTYPE;
      v_Type       NUMBER;
      v_LnkID      NUMBER;
      v_A          NUMBER;
    BEGIN
      v_stat := 0;

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_DEBUG,
                'Вызов RSI_TXLinkDirectRepoToBuy SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate||', S = '||S );

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_OPTIM,
                'Вызов RSI_TXLinkDirectRepoToBuy SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate||', S = '||S );

      WHILE S > 0 AND v_stat = 0 LOOP
        BEGIN
          IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
            -- если ФИФО
            if( ReestrValue.V15 = RSB_SCTXC.TXREG_V15_ASC )then  --Сортировка приобретений для прямого РЕПО
               SELECT t_ID, t_Type, FreeAmount
                 INTO v_Buy_ID, v_BuyType, v_FreeAmount
                 FROM (  SELECT q1.t_ID,
                                q1.t_Type,
                                (q1.t_Amount - q1.t_Netting - q1.t_Sale) FreeAmount
                           FROM (SELECT                   /*+ INDEX( Buy DSCTXLOT_DBT_IDXE)*/
                                       Buy.*
                                   FROM dsctxlot_dbt Buy
                                  WHERE     Buy.t_FIID = v_SaleLot.t_FIID
                                        AND Buy.t_BuyDate <= v_SaleLot.t_SaleDate
                                        AND Buy.t_IsFree = CHR (88)
                                        AND Buy.t_Portfolio =
                                               (CASE
                                                   WHEN Portfolio > -1
                                                   THEN
                                                      DECODE (Portfolio,
                                                              RSB_PMWRTOFF.KINDPORT_BACK, -1,
                                                              Portfolio)
                                                   ELSE
                                                      Buy.t_Portfolio
                                                END)
                                        AND EXISTS (SELECT 1
                                                      FROM DDL_TICK_DBT TICK
                                                     WHERE TICK.T_DEALID = Buy.T_DEALID
                                                       AND (TICK.T_OFBU <> 'X'
                                                        OR (    TICK.T_OFBU = 'X'
                                                       AND v_SaleLot.t_SaleDate >= RSB_SCTX.GETINAVRWRTSTARTDATE (TICK.T_DEALID))))
                                             ) q1
                       ORDER BY q1.t_OrdForRepo ASC,
                                q1.t_BegBuyDate ASC,
                                q1.t_DealDate ASC,
                                RSB_SCTX.GETINAVRWRTSTARTDATE (Q1.T_DEALID, Q1.T_DEALDATE) ASC,
                                q1.t_DealTime ASC,
                                q1.t_DealSort ASC)
                WHERE ROWNUM = 1;
            else
               SELECT t_ID, t_Type, FreeAmount
                 INTO v_Buy_ID, v_BuyType, v_FreeAmount
                 FROM (  SELECT q1.t_ID,
                                q1.t_Type,
                                (q1.t_Amount - q1.t_Netting - q1.t_Sale) FreeAmount
                           FROM (SELECT                   /*+ INDEX( Buy DSCTXLOT_DBT_IDXD)*/
                                       Buy.*
                                   FROM dsctxlot_dbt Buy
                                  WHERE     Buy.t_FIID = v_SaleLot.t_FIID
                                        AND Buy.t_BuyDate <= v_SaleLot.t_SaleDate
                                        AND Buy.t_IsFree = CHR (88)
                                        AND Buy.t_Portfolio =
                                               (CASE
                                                   WHEN Portfolio > -1
                                                   THEN
                                                      DECODE (Portfolio,
                                                              RSB_PMWRTOFF.KINDPORT_BACK, -1,
                                                              Portfolio)
                                                   ELSE
                                                      Buy.t_Portfolio
                                                END)
                                        AND EXISTS (SELECT 1
                                                      FROM DDL_TICK_DBT TICK
                                                     WHERE TICK.T_DEALID = Buy.T_DEALID
                                                       AND (TICK.T_OFBU <> 'X'
                                                        OR (    TICK.T_OFBU = 'X'
                                                       AND v_SaleLot.t_SaleDate >= RSB_SCTX.GETINAVRWRTSTARTDATE (TICK.T_DEALID))))
                                             ) q1
                       ORDER BY q1.t_OrdForRepo ASC,
                                q1.t_BegBuyDate DESC,
                                q1.t_DealDate DESC,
                                RSB_SCTX.GETINAVRWRTSTARTDATE (Q1.T_DEALID, Q1.T_DEALDATE) DESC,
                                q1.t_DealTime DESC,
                                q1.t_DealSort DESC)
                WHERE ROWNUM = 1;
            end if;
          END IF;

          v_Type := RSB_SCTXC.TXLNK_UNDEF;

          IF( v_SaleLot.t_Type = RSB_SCTXC.TXLOTS_REPO ) THEN
            v_Type := RSB_SCTXC.TXLNK_DELREPO;
          ELSIF( v_SaleLot.t_Type = RSB_SCTXC.TXLOTS_LOANPUT ) THEN
            v_Type := RSB_SCTXC.TXLNK_LOANPUT;
          END IF;

          v_A := iif( S < v_FreeAmount, S, v_FreeAmount );

          BEGIN
            SELECT t_ID
              INTO v_LnkID
              FROM dsctxlnk_dbt
             WHERE t_Type = v_Type
               AND t_BuyID = v_Buy_ID
               AND t_SaleID = v_SaleLot.t_ID
               AND t_SourceID = 0
               AND t_DestID = 0
               AND t_Lot1ID = 0
               AND t_Lot2ID = 0
               AND t_DATE = v_SaleLot.t_SaleDate;

            UPDATE dsctxlnk_dbt
               SET t_Amount = t_Amount + v_A
             WHERE t_ID = v_LnkID;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN
                 v_Link := NULL;

                 v_Link.t_SaleID     := v_SaleLot.t_ID;
                 v_Link.t_BuyID      := v_Buy_ID;
                 v_Link.t_Type       := v_Type;
                 v_Link.t_SourceID   := 0;
                 v_Link.t_DestID     := 0;
                 v_Link.t_Lot1ID     := 0;
                 v_Link.t_Lot2ID     := 0;
                 v_Link.t_Date       := v_SaleLot.t_SaleDate;
                 v_Link.t_Short      := 0;
                 v_Link.t_Ret        := 0;
                 v_Link.t_Ret2       := 0;
                 v_Link.t_RetSP      := 0;
                 v_Link.t_BegDate    := TO_DATE('01.01.0001', 'DD.MM.YYYY');
                 v_Link.t_EndDate    := TO_DATE('01.01.0001', 'DD.MM.YYYY');
                 v_Link.t_Amount     := v_A;
                 v_Link.t_RetFlag    := case when v_SaleLot.t_SaleDate = v_SaleLot.t_BuyDate then 'X' else CHR(0) end;
                 v_Link.t_FIID       := v_SaleLot.t_FIID;

                 INSERT INTO dsctxlnk_dbt VALUES v_Link;
              END;
          END;

          S := S - v_A;

        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
              BEGIN
                v_stat := 1;
              END;

        END;
      END LOOP;

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_DEBUG,
                'Конец RSI_TXLinkDirectRepoToBuy SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate||', S = '||S );

    END; --RSI_TXLinkDirectRepoToBuy

  ----Выполняет списание лота Прямого Репо или Займа Размещения однодневного SALELOT с лотов покупок
    PROCEDURE RSI_TXLinkOneDayRepoToBuy( v_SaleLot IN dsctxlot_dbt%ROWTYPE, S IN OUT NUMBER, Portfolio IN NUMBER DEFAULT -1 )
    IS
      v_A          NUMBER;
      v_Buy_ID     NUMBER;
      v_BuyType    NUMBER;
      v_stat       NUMBER;
      v_FreeAmount NUMBER;
      v_Link       dsctxlnk_dbt%ROWTYPE;
      v_Type       NUMBER;
    BEGIN
      v_stat := 0;

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_DEBUG,
                'Вызов RSI_TXLinkOneDayRepoToBuy SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate||', S = '||S );

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_OPTIM,
                'Вызов RSI_TXLinkOneDayRepoToBuy SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate||', S = '||S );

      WHILE S > 0 AND v_stat = 0 LOOP
        BEGIN
          IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
            -- если ФИФО
            SELECT t_ID, t_Type, t_FreeAmount - LnkAmount
              INTO v_Buy_ID, v_BuyType, v_FreeAmount
              FROM ( SELECT Buy.t_ID, Buy.t_Type, Buy.t_FreeAmount,
                            (SELECT NVL(SUM(lnk.t_Amount), 0) LnkAmount
                               FROM dsctxlnk_dbt lnk
                              WHERE lnk.t_SaleID = v_SaleLot.t_ID
                                AND lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_LOANPUT)
                                AND lnk.t_RetFlag = CHR(88)
                                AND lnk.t_BuyID = Buy.t_ID
                                AND lnk.t_Date  = Buy.t_SaleDate
                            ) LnkAmount
                       FROM V_SCTX_ONEDAYREPO Buy, dsctxlot_dbt Lot
                      WHERE Buy.t_FIID = v_SaleLot.t_FIID
                        AND Buy.t_BuyDate <= v_SaleLot.t_SaleDate
                        AND Buy.t_SaleDate = v_SaleLot.t_SaleDate
                        AND Lot.t_ID = Buy.t_ID
                        AND Lot.t_Portfolio = (CASE WHEN Portfolio > -1 THEN decode(Portfolio,RSB_PMWRTOFF.KINDPORT_BACK,-1,Portfolio) ELSE Lot.t_Portfolio END)
                        AND ((Buy.t_Blocked = CHR(0)) OR (ReestrValue.V10 = RSB_SCTXC.TXREG_V10_YES))
                        AND (Buy.t_FreeAmount -
                             (SELECT NVL(SUM(lnk.t_Amount), 0) LnkAmount
                                FROM dsctxlnk_dbt lnk
                               WHERE lnk.t_SaleID = v_SaleLot.t_ID
                                 AND lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_LOANPUT)
                                 AND lnk.t_RetFlag = CHR(88)
                                 AND lnk.t_BuyID = Buy.t_ID
                                 AND lnk.t_Date  = Buy.t_SaleDate
                             )
                            ) > 0
                   ORDER BY Buy.t_OrdRepo ASC,
                            Buy.t_BegBuyDate DESC,
                            Buy.t_DealDate DESC,
                            Buy.t_DealTime DESC,
                            Buy.t_DealSort DESC )
             WHERE ROWNUM = 1;
          END IF;

          v_A := iif( S < v_FreeAmount, S, v_FreeAmount );

          v_Type := RSB_SCTXC.TXLNK_UNDEF;

          IF( v_SaleLot.t_Type = RSB_SCTXC.TXLOTS_REPO ) THEN
            v_Type := RSB_SCTXC.TXLNK_DELREPO;
          ELSE
            v_Type := RSB_SCTXC.TXLNK_LOANPUT;
          END IF;

          v_Link := NULL;

          v_Link.t_SaleID     := v_SaleLot.t_ID;
          v_Link.t_BuyID      := v_Buy_ID;
          v_Link.t_Type       := v_Type;
          v_Link.t_SourceID   := 0;
          v_Link.t_DestID     := 0;
          v_Link.t_Lot1ID     := 0;
          v_Link.t_Lot2ID     := 0;
          v_Link.t_Date       := v_SaleLot.t_SaleDate;
          v_Link.t_Short      := 0;
          v_Link.t_Ret        := 0;
          v_Link.t_Ret2       := 0;
          v_Link.t_RetSP      := 0;
          v_Link.t_BegDate    := TO_DATE('01.01.0001', 'DD.MM.YYYY');
          v_Link.t_EndDate    := TO_DATE('01.01.0001', 'DD.MM.YYYY');
          v_Link.t_Amount     := v_A;
          v_Link.t_RetFlag    := CHR(88);
          v_Link.t_FIID       := v_SaleLot.t_FIID;

          INSERT INTO dsctxlnk_dbt VALUES v_Link;

          S := S - v_A;

        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
              BEGIN
                v_stat := 1;
              END;

        END;
      END LOOP;

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_DEBUG,
                'Конец RSI_TXLinkOneDayRepoToBuy SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate||', S = '||S );
    END; --RSI_TXLinkOneDayRepoToBuy

  ----Выполняет списание лота прямого репо/размещения займа
    PROCEDURE RSI_TXLinkDirectRepo( v_SaleLot_ IN dsctxlot_dbt%ROWTYPE )
    IS
      v_S          NUMBER;
      v_SaleID     NUMBER;
      v_Flag       NUMBER;
      v_FICODE     dfininstr_dbt.t_FI_Code%TYPE;
      v_ChildLot   dsctxlot_dbt%ROWTYPE;
      v_SaleLot    dsctxlot_dbt%ROWTYPE;
      v_PriorPort  NUMBER;
      v_BofficeKind NUMBER;
      v_OGroup     NUMBER;
      v_G1         NUMBER;
      v_G2         NUMBER;
      v_G3         NUMBER;
      v_G4         NUMBER;
      v_G5         NUMBER;
    BEGIN

      v_SaleLot := v_SaleLot_;
      IF v_SaleLot.t_IsComp = CHR(88) THEN
         RETURN;
      END IF;

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_DEBUG,
                'Вызов RSI_TXLinkDirectRepo SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate );
      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_OPTIM,
                'Вызов RSI_TXLinkDirectRepo SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate );

      IF v_SaleLot.t_ChildID > 0 THEN
         BEGIN
           SELECT * INTO v_ChildLot
             FROM dsctxlot_dbt
            WHERE t_ID = v_SaleLot.t_ChildID;
         exception
           when NO_DATA_FOUND then
               v_S := v_SaleLot.t_Amount;
         END;

         IF v_SaleLot.t_SaleDate = v_ChildLot.t_SaleDate THEN -- в дату лота, была ещё и компенсация
            v_S := v_ChildLot.t_Amount; -- берём уменьшенное количество

            v_SaleID := v_SaleLot.t_ChildID;
            v_SaleLot := null;

            SELECT * INTO v_SaleLot -- в списании должен участвовать компенсационный
              FROM dsctxlot_dbt
             WHERE t_ID = v_SaleID;
         ELSE
            v_S := v_SaleLot.t_Amount;
         END IF;
      ELSE
         v_S := v_SaleLot.t_Amount;
      END IF;

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_DEBUG,
                'RSI_TXLinkDirectRepo v_SaleLot.t_ID = '||v_SaleLot.t_ID );

      IF ReestrValue.V14 = RSB_SCTXC.TXREG_V14_YES THEN
          SELECT t_BofficeKind, RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(TK.T_DEALTYPE, TK.T_BOFFICEKIND))
          INTO v_BofficeKind, v_OGroup
          FROM ddl_tick_dbt TK
          WHERE t_DealID = v_SaleLot.t_DealID;

          IF(RSB_SECUR.GetObjAttrNumber(v_BofficeKind, 49, RSB_SECUR.GETMAINOBJATTRNODATE(v_BofficeKind, LPAD(v_SaleLot.t_DealID, 34, '0'), 49)) <> chr(1))
          THEN
              v_PriorPort := to_number(RSB_SECUR.GetObjAttrNumber(v_BofficeKind, 49, RSB_SECUR.GETMAINOBJATTRNODATE(v_BofficeKind, LPAD(v_SaleLot.t_DealID, 34, '0'), 49)));
          ELSE
              v_PriorPort := -1;
          END IF;
      ELSE
          v_PriorPort := -1;
      END IF;

      IF ReestrValue.V9 = RSB_SCTXC.TXREG_V9_NO THEN
         v_Flag := 1; --первый раз

         WHILE v_Flag = 1 OR v_Flag = 2 LOOP
            IF ReestrValue.V14 = RSB_SCTXC.TXREG_V14_YES THEN
                IF v_PriorPort != -1
                THEN
                    RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S, v_PriorPort);
                    IF (v_S > 0 AND v_SaleLot.T_BUYDATE = v_SaleLot.T_SALEDATE) THEN --однодневки
                       RSI_TXLinkOneDayRepoToBuy (v_SaleLot, v_S, v_PriorPort);
                    END IF;
                END IF;

                IF (v_S > 0 ) THEN
                    RSB_PMWRTOFF.GetWriteOffGroups( v_BofficeKind,
                                            10,
                                            v_SaleLot.t_Portfolio,
                                            RSB_SECUR.IsTwoPart(v_OGroup),
                                            RSB_SECUR.IsSale(v_OGroup),
                                            RSI_RSB_FIInstr.FI_IsKSU(v_SaleLot.t_FIID),
                                            0,
                                            v_G1,
                                            v_G2,
                                            v_G3,
                                            v_G4,
                                            v_G5 );

                    IF v_G1 > 0 and  v_PriorPort != v_G1 THEN
                        RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S, v_G1);
                        IF (v_S > 0 AND v_SaleLot.T_BUYDATE = v_SaleLot.T_SALEDATE) THEN --однодневки
                           RSI_TXLinkOneDayRepoToBuy (v_SaleLot, v_S, v_G1);
                        END IF;
                    END IF;

                    IF v_S > 0 and v_G2 > 0 and v_PriorPort != v_G2 THEN
                        RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S, v_G2);
                        IF (v_S > 0 AND v_SaleLot.T_BUYDATE = v_SaleLot.T_SALEDATE) THEN --однодневки
                           RSI_TXLinkOneDayRepoToBuy (v_SaleLot, v_S, v_G2);
                        END IF;
                    END IF;

                    IF v_S > 0 and v_G3 > 0 and v_PriorPort != v_G3 THEN
                        RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S, v_G3);
                        IF (v_S > 0 AND v_SaleLot.T_BUYDATE = v_SaleLot.T_SALEDATE) THEN --однодневки
                           RSI_TXLinkOneDayRepoToBuy (v_SaleLot, v_S, v_G3);
                        END IF;
                    END IF;

                    IF v_S > 0 and v_G4 > 0 and v_PriorPort != v_G4 THEN
                        RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S, v_G4);
                        IF (v_S > 0 AND v_SaleLot.T_BUYDATE = v_SaleLot.T_SALEDATE) THEN --однодневки
                           RSI_TXLinkOneDayRepoToBuy (v_SaleLot, v_S, v_G4);
                        END IF;
                    END IF;

                    IF v_S > 0 and v_G5 > 0 and v_PriorPort != v_G5 THEN
                        RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S, v_G5);
                        IF (v_S > 0 AND v_SaleLot.T_BUYDATE = v_SaleLot.T_SALEDATE) THEN --однодневки
                           RSI_TXLinkOneDayRepoToBuy (v_SaleLot, v_S, v_G5);
                        END IF;
                    END IF;
                END IF;
            ELSE--ReestrValue.V14 != RSB_SCTXC.TXREG_V14_YES
                RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S);
                IF (v_S > 0 AND v_SaleLot.T_BUYDATE = v_SaleLot.T_SALEDATE) THEN --однодневки
                   RSI_TXLinkOneDayRepoToBuy (v_SaleLot, v_S);
                END IF;
            END IF;

            IF (v_S > 0 AND v_Flag = 1) THEN
               RSI_TXReturnAmountFromRepo(v_SaleLot.T_FIID, v_SaleLot.T_SALEDATE, v_SaleLot.T_ID);
               v_Flag := 2;
            ELSE
               v_Flag := 0;
            END IF;
         END LOOP;
      ELSE
         -- не считать докомпенсационный лот внутридневным
         IF (v_SaleLot.T_BUYDATE = v_SaleLot.T_SALEDATE AND v_SaleLot.t_ChildID = 0) THEN --однодневки
            RETURN;
         ELSE
            IF ReestrValue.V14 = RSB_SCTXC.TXREG_V14_YES THEN
                IF v_PriorPort != -1
                THEN
                    RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S, v_PriorPort);
                END IF;
                IF (v_S > 0 ) THEN
                    RSB_PMWRTOFF.GetWriteOffGroups( v_BofficeKind,
                                            10,
                                            v_SaleLot.t_Portfolio,
                                            RSB_SECUR.IsTwoPart(v_OGroup),
                                            RSB_SECUR.IsSale(v_OGroup),
                                            RSI_RSB_FIInstr.FI_IsKSU(v_SaleLot.t_FIID),
                                            0,
                                            v_G1,
                                            v_G2,
                                            v_G3,
                                            v_G4,
                                            v_G5 );
                    IF v_G1 > 0 and  v_PriorPort != v_G1 THEN
                        RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S, v_G1);
                    END IF;
                    IF v_S > 0 and v_G2 > 0 and v_PriorPort != v_G2 THEN
                        RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S, v_G2);
                    END IF;
                    IF v_S > 0 and v_G3 > 0 and v_PriorPort != v_G3 THEN
                        RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S, v_G3);
                    END IF;
                    IF v_S > 0 and v_G4 > 0 and v_PriorPort != v_G4 THEN
                        RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S, v_G4);
                    END IF;
                    IF v_S > 0 and v_G5 > 0 and v_PriorPort != v_G5 THEN
                        RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S, v_G5);
                    END IF;
                END IF;
            ELSE--ReestrValue.V14 != RSB_SCTXC.TXREG_V14_YES
               RSI_TXLinkDirectRepoToBuy(v_SaleLot, v_S);
            END IF;
         END IF;
      END IF;

      IF (v_S > 0) THEN
         SELECT t_FI_Code
           INTO v_FICODE
           FROM dfininstr_dbt
          WHERE t_FIID = v_SaleLot.t_FIID;

         TXPutMsg( v_SaleLot.t_ID,
                   v_SaleLot.t_FIID,
                   TXMES_WARNING,
                   'Недостаточно ц/б "'||v_FICODE||'" на дату '||TO_CHAR(v_SaleLot.t_SaleDate,'DD.MM.YYYY')||
                   ' для списания лота вида "'||get_lotName(v_SaleLot.t_Type)||'" с внешним кодом "'||v_SaleLot.t_DealCodeTS||'"' );
      END IF;

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_DEBUG,
                'Конец RSI_TXLinkDirectRepo SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate );
    END; --RSI_TXLinkDirectRepo

  ----Выполняет списание лота продажи SALELOT из сделок покупки
    PROCEDURE RSI_TXLinkSaleToBuy( v_SaleLot IN dsctxlot_dbt%ROWTYPE, S IN OUT NUMBER, Portfolio IN NUMBER DEFAULT -1, Except_Portfolio IN NUMBER DEFAULT 0 )
    IS
      v_Buy_ID     NUMBER;
      v_stat       NUMBER;
      v_FreeAmount NUMBER;
      v_Link       dsctxlnk_dbt%ROWTYPE;
    BEGIN
      v_stat := 0;

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_DEBUG,
                'Вызов RSI_TXLinkSaleToBuy SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate||', S = '||S );

      WHILE S > 0 AND v_stat = 0 LOOP
        BEGIN
          IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
          -- если ФИФО
            SELECT t_ID, FreeAmount
              INTO v_Buy_ID, v_FreeAmount
              FROM (  SELECT q1.t_ID, (q1.t_Amount - q1.t_Netting - q1.t_Sale) FreeAmount
                        FROM (SELECT  /*+ INDEX( Buy DSCTXLOT_DBT_IDXA)*/
                                    Buy.*,
                                     CASE
                                        WHEN Rsb_SCTX.GetInAvrWrtStartDate (tick.t_DealID) =
                                                TO_DATE ('31-12-9999', 'DD-MM-YYYY')
                                        THEN
                                           Buy.t_DealDate
                                        ELSE
                                           Rsb_SCTX.GetInAvrWrtStartDate (tick.t_DealID)
                                     END
                                        AS t_wrtstart
                                FROM dsctxlot_dbt Buy, ddl_tick_dbt tick
                               WHERE     Buy.t_FIID = v_SaleLot.t_FIID
                                     AND Buy.t_BuyDate <= v_SaleLot.t_SaleDate
                                     AND Buy.t_Type = RSB_SCTXC.TXLOTS_BUY
                                     AND Buy.t_IsFree = CHR (88)
                                     AND Buy.t_Portfolio = (CASE WHEN Except_Portfolio = 0 AND Portfolio > -1 THEN Portfolio ELSE Buy.t_Portfolio END)
                                     AND Buy.t_Portfolio <> (CASE WHEN Except_Portfolio > 0 AND Portfolio > -1 THEN Portfolio ELSE -2/*просто такое значение, которое не бывает*/ END)
                                     AND tick.t_DealID = Buy.t_DealID
                                     AND (   (tick.t_Ofbu = 'X' AND v_SaleLot.t_SaleDate >= GetInAvrWrtStartDate (tick.t_DealID))
                                          OR tick.t_Ofbu <> 'X')) q1
                    ORDER BY q1.t_BegBuyDate ASC,
                             q1.t_DealDate ASC,
                             q1.t_wrtstart ASC,
                             q1.t_DealTime ASC,
                             q1.t_DealSort ASC)
             WHERE ROWNUM = 1;
          END IF;

          v_Link := NULL;

          v_Link.t_SaleID     := v_SaleLot.t_ID;
          v_Link.t_BuyID      := v_Buy_ID;
          v_Link.t_Type       := RSB_SCTXC.TXLNK_DELIVER;
          v_Link.t_SourceID   := 0;
          v_Link.t_DestID     := 0;
          v_Link.t_Lot1ID     := 0;
          v_Link.t_Lot2ID     := 0;
          v_Link.t_Date       := v_SaleLot.t_SaleDate;
          v_Link.t_Short      := 0;
          v_Link.t_Ret        := 0;
          v_Link.t_Ret2       := 0;
          v_Link.t_RetSP      := 0;
          v_Link.t_BegDate    := TO_DATE('01.01.0001', 'DD.MM.YYYY');
          v_Link.t_EndDate    := TO_DATE('01.01.0001', 'DD.MM.YYYY');
          v_Link.t_Amount     := iif( S < v_FreeAmount, S, v_FreeAmount );
          v_Link.t_RetFlag    := CHR(0);
          v_Link.t_FIID       := v_SaleLot.t_FIID;

          INSERT INTO dsctxlnk_dbt VALUES v_Link;

          S := S - v_Link.t_Amount;

        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
              BEGIN
                v_stat := 1;
              END;

        END;
      END LOOP;

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_DEBUG,
                'Конец RSI_TXLinkSaleToBuy SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate||', S = '||S );
    END; --RSI_TXLinkSaleToBuy


    --Выполняет обработку операции перемещения
    PROCEDURE RSI_TXProcessMoving(G IN dsctxgo_dbt%ROWTYPE)
    IS
     v_FICODE dfininstr_dbt.t_Fi_Code%type;
     v_FINAME dfininstr_dbt.t_Name%type;
     v_TaxGroup NUMBER;

     v_SourceP1 NUMBER;
     v_SourceP2 NUMBER;
     v_DestP    NUMBER;

     v_OperAmount1   NUMBER;
     v_OperAmount2   NUMBER;
     v_OperAmountBPP NUMBER;

     v_Amount1   NUMBER;
     v_Amount2   NUMBER;
     v_AmountBPP NUMBER;

     v_SaleAmount1   NUMBER;
     v_SaleAmount2   NUMBER;
     v_SaleAmountBPP NUMBER;

     v_SaleLot1 dsctxlot_dbt%ROWTYPE;
     v_SaleLot2 dsctxlot_dbt%ROWTYPE;

     v_BuyLot dsctxlot_dbt%ROWTYPE;
     v_NewBuyLot dsctxlot_dbt%ROWTYPE;

     v_BuyID     NUMBER;
     v_Buy_FreeAmount NUMBER;

     v_LotAmount1   NUMBER;
     v_LotAmount2   NUMBER;
     v_LotAmountBPP NUMBER;

     v_SL NUMBER;

     v_LnkID NUMBER;

     v_NewLnk dsctxlnk_dbt%ROWTYPE;
     v_NewLs dsctxls_dbt%ROWTYPE;

     M ddl_comm_dbt%ROWTYPE;

    BEGIN

      --1
      SELECT * INTO M
        FROM DDL_COMM_DBT
       WHERE T_DOCUMENTID = G.T_DOCUMENTID;

      TXPutMsg( 0,
                M.t_FIID,
                TXMES_DEBUG,
                'Обрабатываем операцию перемещения №'||M.t_COMMCODE );

      SELECT FIN.T_FI_CODE, FIN.T_NAME, AVR.T_TAXGROUP INTO v_FICODE, v_FINAME, v_TaxGroup
        FROM DFININSTR_DBT FIN, DAVOIRISS_DBT AVR
       WHERE FIN.T_FIID = M.T_FIID
         AND AVR.T_FIID = FIN.T_FIID;

      --2
      v_SourceP1 := RSB_PMWRTOFF.KINDPORT_UNDEF;
      v_SourceP2 := RSB_PMWRTOFF.KINDPORT_UNDEF;
      v_DestP    := RSB_PMWRTOFF.KINDPORT_UNDEF;

      --3
      IF M.T_OPERSUBKIND = RSB_SECUR.SUBKIND_SALE_TO_RETIRE THEN           -- Переклассификация ц/б "для продажи" в "удерживаемые до погашения"
        v_SourceP1 := RSB_PMWRTOFF.KINDPORT_SALE; --ППР
        v_DestP    := RSB_PMWRTOFF.KINDPORT_RETIRE; --ПУДП
      ELSIF M.T_OPERSUBKIND = RSB_SECUR.SUBKIND_RETIRE_TO_SALE THEN        -- Переклассификация ц/б, "удерживаемых до погашения", в "ц/б для продажи"
        v_SourceP1 := RSB_PMWRTOFF.KINDPORT_RETIRE; --ПУДП
        v_DestP    := RSB_PMWRTOFF.KINDPORT_SALE; --ППР
      ELSIF M.T_OPERSUBKIND = RSB_SECUR.SUBKIND_TO_CONTROL THEN            -- Перемещение в портфель контрольного участия
        v_SourceP1 := RSB_PMWRTOFF.KINDPORT_SALE; --ППР
        v_SourceP2 := RSB_PMWRTOFF.KINDPORT_TRADE; --ТП
        v_DestP    := RSB_PMWRTOFF.KINDPORT_CONTR; --ПКУ
      ELSIF M.T_OPERSUBKIND = RSB_SECUR.SUBKIND_FROM_CONTROL THEN          -- Перемещение из портфеля контрольного участия
        v_SourceP1 := RSB_PMWRTOFF.KINDPORT_CONTR; --ПКУ
        v_DestP    := RSB_PMWRTOFF.KINDPORT_SALE; --ППР
      ELSIF M.T_OPERSUBKIND = RSB_SECUR.SUBKIND_TRADE_TO_RETIRE_2129 THEN  -- Перевод ц/б торг.портфеля в "удерж.до погашения" (по 2129-У)
        v_SourceP1 := RSB_PMWRTOFF.KINDPORT_TRADE; --ТП
        v_DestP    := RSB_PMWRTOFF.KINDPORT_RETIRE; --ПУДП
      ELSIF M.T_OPERSUBKIND = RSB_SECUR.SUBKIND_TRADE_TO_SALE_2129 THEN    -- Перевод ц/б торг.портфеля в "ц/б для продажи" (по 2129-У)
        v_SourceP1 := RSB_PMWRTOFF.KINDPORT_TRADE; --ТП
        v_DestP    := RSB_PMWRTOFF.KINDPORT_SALE; --ППР
      ELSIF M.T_OPERSUBKIND = RSB_SECUR.SUBKIND_SALE_TO_RETIRE_2129 THEN   -- Перевод ц/б "для продажи" в "удерж.до погашения"(по 2129-У)
        v_SourceP1 := RSB_PMWRTOFF.KINDPORT_SALE; --ППР
        v_DestP    := RSB_PMWRTOFF.KINDPORT_RETIRE; --ПУДП
      ELSIF M.T_OPERSUBKIND = RSB_SECUR.SUBKIND_TRADE_TO_RETIRE_2014 THEN  -- Перевод ц/б торг.портфеля в "удерж.до погашения" (кризисный 2014)
        v_SourceP1 := RSB_PMWRTOFF.KINDPORT_TRADE; --ТП
        v_DestP    := RSB_PMWRTOFF.KINDPORT_RETIRE; --ПУДП
      ELSIF M.T_OPERSUBKIND = RSB_SECUR.SUBKIND_TRADE_TO_SALE_2014 THEN    -- Перевод ц/б торг.портфеля в "ц/б для продажи" (кризисный 2014)
        v_SourceP1 := RSB_PMWRTOFF.KINDPORT_TRADE; --ТП
        v_DestP    := RSB_PMWRTOFF.KINDPORT_SALE; --ППР
      ELSIF M.T_OPERSUBKIND = RSB_SECUR.SUBKIND_SALE_TO_RETIRE_2014 THEN   -- Перевод ц/б "для продажи" в "удерж.до погашения" (кризисный 2014)
        v_SourceP1 := RSB_PMWRTOFF.KINDPORT_SALE; --ППР
        v_DestP    := RSB_PMWRTOFF.KINDPORT_RETIRE; --ПУДП
      ELSE 
        v_SourceP1 := M.t_SrcPortfolio; --DEF-96457
        v_DestP    := M.t_DestPortofolio;
      END IF;

      TXPutMsg( 0,
                M.t_FIID,
                TXMES_DEBUG,
                'v_SourceP1 = '||TO_CHAR(v_SourceP1)||'; v_SourceP2 = '|| TO_CHAR(v_SourceP2)||'; v_DestP = '|| TO_CHAR(v_DestP));


      --4
      IF(M.T_OPERSUBKIND = 0) THEN
        v_OperAmount1 := M.t_hidden_sum;
      ELSE 
        BEGIN
          SELECT T_BASEAMOUNT INTO v_OperAmount1
            FROM DPMPAYM_DBT
           WHERE T_DOCKIND = M.T_DOCKIND
             AND T_DOCUMENTID = M.T_DOCUMENTID
             AND T_BASEAMOUNT > 0
             AND T_PURPOSE = 1 /*BAi*/
             AND (   (M.T_OPERSUBKIND <> RSB_SECUR.SUBKIND_TO_CONTROL)
                  OR (M.T_OPERSUBKIND = RSB_SECUR.SUBKIND_TO_CONTROL AND T_SUBPURPOSE = 1)
                 );
        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN v_OperAmount1 := 0;

        END;
      END IF;

      --5
      BEGIN
        SELECT T_BASEAMOUNT INTO v_OperAmount2
          FROM DPMPAYM_DBT
         WHERE T_DOCKIND = M.T_DOCKIND
           AND T_DOCUMENTID = M.T_DOCUMENTID
           AND T_BASEAMOUNT > 0
           AND T_PURPOSE = 1 /*BAi*/
           AND M.T_OPERSUBKIND = RSB_SECUR.SUBKIND_TO_CONTROL
           AND T_SUBPURPOSE = 2;

        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN v_OperAmount2 := 0;

      END;

      --6
      IF(M.T_OPERSUBKIND = 0) THEN
        v_OperAmountBPP := M.t_Currency_sum;
      ELSE 
        BEGIN
          SELECT T_SUM INTO v_OperAmountBPP
            FROM DDLSUM_DBT
           WHERE T_DOCKIND = M.T_DOCKIND
             AND T_DOCID = M.T_DOCUMENTID
             AND T_DATE = M.T_COMMDATE;

        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN v_OperAmountBPP := 0;

        END;
      END IF;

      --7
      v_Amount1 := 0;
      IF v_OperAmount1 > 0 THEN
        SELECT NVL(SUM(BUYLOT.T_AMOUNT - BUYLOT.T_NETTING - BUYLOT.T_SALE), 0) INTO v_Amount1
          FROM DSCTXLOT_DBT BUYLOT
         WHERE BUYLOT.T_FIID = M.T_FIID
           AND BUYLOT.T_BUYDATE <= M.T_COMMDATE
           AND BUYLOT.T_TYPE = RSB_SCTXC.TXLOTS_BUY
           AND BUYLOT.T_ISFREE = 'X'
           AND BUYLOT.T_PORTFOLIO = v_SourceP1;
      END IF;

      --8
      v_Amount2 := 0;
      IF v_OperAmount2 > 0 THEN
        SELECT NVL(SUM(BUYLOT.T_AMOUNT - BUYLOT.T_NETTING - BUYLOT.T_SALE), 0) INTO v_Amount2
          FROM DSCTXLOT_DBT BUYLOT
         WHERE BUYLOT.T_FIID = M.T_FIID
           AND BUYLOT.T_BUYDATE <= M.T_COMMDATE
           AND BUYLOT.T_TYPE = RSB_SCTXC.TXLOTS_BUY
           AND BUYLOT.T_ISFREE = 'X'
           AND BUYLOT.T_PORTFOLIO = v_SourceP2;
      END IF;

      --9
      v_AmountBPP := 0;
      IF v_OperAmountBPP > 0 THEN
        SELECT NVL(SUM(LNK.T_AMOUNT - TXGetSumSCTXLSOnDate(LNK.T_ID, M.T_COMMDATE)), 0) INTO v_AmountBPP
          FROM DSCTXLNK_DBT LNK, DSCTXLOT_DBT TXBUYLOT, DSCTXLOT_DBT CURRBUYLOT, DSCTXLOT_DBT CURRSALELOT
         WHERE LNK.T_TYPE IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO, RSB_SCTXC.TXLNK_LOANPUT, RSB_SCTXC.TXLNK_SUBSTLOAN)
           AND CURRBUYLOT.T_ID = LNK.T_BUYID
           AND TXBUYLOT.T_ID = CURRBUYLOT.T_BEGLOTID
           AND TXBUYLOT.T_FIID = M.T_FIID
           AND TXBUYLOT.T_TYPE = RSB_SCTXC.TXLOTS_BUY
           AND TXBUYLOT.T_PORTFOLIO = v_SourceP1
           AND CURRSALELOT.T_ID = LNK.T_SALEID
           AND LNK.T_DATE <= M.T_COMMDATE
           AND (LNK.T_BEGDATE = TO_DATE('01.01.0001','DD.MM.YYYY') OR LNK.T_BEGDATE <= M.T_COMMDATE)
           AND (LNK.T_ENDDATE = TO_DATE('01.01.0001','DD.MM.YYYY') OR LNK.T_ENDDATE <= M.T_COMMDATE)
           AND (CURRSALELOT.T_BUYDATE = TO_DATE('01.01.0001','DD.MM.YYYY') OR CURRSALELOT.T_BUYDATE > M.T_COMMDATE);
      END IF;

      TXPutMsg( 0,
                M.t_FIID,
                TXMES_DEBUG,
                'v_Amount1 = '||TO_CHAR(v_Amount1)||'; v_OperAmount1 = '|| TO_CHAR(v_OperAmount1));

      TXPutMsg( 0,
                M.t_FIID,
                TXMES_DEBUG,
                'v_Amount2 = '||TO_CHAR(v_Amount2)||'; v_OperAmount2 = '|| TO_CHAR(v_OperAmount2));

      TXPutMsg( 0,
                M.t_FIID,
                TXMES_DEBUG,
                'v_AmountBPP = '||TO_CHAR(v_AmountBPP)||'; v_OperAmountBPP = '|| TO_CHAR(v_OperAmountBPP));


      --10
      IF v_Amount1 < v_OperAmount1 OR v_Amount2 < v_OperAmount2 OR v_AmountBPP < v_OperAmountBPP THEN
        TXPutMsg( 0,
                  M.T_FIID,
                  TXMES_WARNING,
                  'Недостаточно ц/б "'||v_FICODE||'" для межпортфельного перемещения в рамках обработки операции перемещения №'||M.T_COMMCODE||' на дату '||TO_CHAR(M.T_COMMDATE,'DD.MM.YYYY'));
      END IF;

      --11
      v_SaleAmount1 := LEAST(v_OperAmount1, v_Amount1);

      --12
      v_SaleAmountBPP := LEAST(v_OperAmountBPP, v_AmountBPP);

      --13
      IF v_SaleAmount1 > 0 THEN
        v_SaleLot1 := NULL;

        v_SaleLot1.T_ID          := 0;
        v_SaleLot1.T_DEALID      := 0;
        v_SaleLot1.T_FIID        := M.T_FIID;
        v_SaleLot1.T_TAXGROUP    := v_TaxGroup;
        v_SaleLot1.T_TYPE        := RSB_SCTXC.TXLOTS_SALE;
        v_SaleLot1.T_VIRTUALTYPE := RSB_SCTXC.TXVDEAL_REAL;
        v_SaleLot1.T_BUYID       := 0;
        v_SaleLot1.T_REALID      := 0;
        v_SaleLot1.T_PRICE       := 0;
        v_SaleLot1.T_PRICEFIID   := -1;
        v_SaleLot1.T_PRICECUR    := CHR(1);
        v_SaleLot1.T_DEALDATE    := M.T_COMMDATE;
        v_SaleLot1.T_DEALTIME    := TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS');
        v_SaleLot1.T_DEALCODE    := M.T_COMMCODE;
        v_SaleLot1.T_DEALCODETS  := M.T_COMMCODE;
        v_SaleLot1.T_BUYDATE     := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_SaleLot1.T_SALEDATE    := M.T_COMMDATE;
        v_SaleLot1.T_RETRDATE    := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_SaleLot1.T_OLDTYPE     := 0;
        v_SaleLot1.T_AMOUNT      := v_SaleAmount1 + v_SaleAmountBPP;
        v_SaleLot1.T_NETTINGID   := 0;
        v_SaleLot1.T_NETTING     := 0;
        v_SaleLot1.T_SALE        := 0;
        v_SaleLot1.T_RETFLAG     := CHR(0);
        v_SaleLot1.T_ISFREE      := CHR(0);
        v_SaleLot1.T_BEGLOTID    := 0;
        v_SaleLot1.T_CHILDID     := 0;
        v_SaleLot1.T_ISCOMP      := CHR(0);
        v_SaleLot1.T_BEGBUYDATE  := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_SaleLot1.T_BEGSALEDATE := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_SaleLot1.T_COMPAMOUNT  := 0;
        v_SaleLot1.T_INACC       := CHR(0);
        v_SaleLot1.T_DEALSORTCODE:= CHR(1);
        v_SaleLot1.T_DEALSORT    := 0;
        v_SaleLot1.T_BLOCKED     := CHR(0);
        v_SaleLot1.T_RQID        := 0;
        v_SaleLot1.T_ORIGIN      := RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER;
        v_SaleLot1.T_GOID        := G.T_ID;
        v_SaleLot1.T_TOTALCOST   := 0;
        v_SaleLot1.T_PORTFOLIO   := v_SourceP1;

        INSERT INTO DSCTXLOT_DBT VALUES v_SaleLot1 RETURNING T_ID INTO v_SaleLot1.T_ID;

        UPDATE DSCTXLOT_DBT SET T_BEGLOTID = T_ID WHERE T_ID = v_SaleLot1.T_ID;

      END IF;

      --14
      v_SaleAmount2 := LEAST(v_OperAmount2, v_Amount2);

      --15
      IF v_SaleAmount2 > 0 THEN
        v_SaleLot2 := NULL;

        v_SaleLot2.T_ID          := 0;
        v_SaleLot2.T_DEALID      := 0;
        v_SaleLot2.T_FIID        := M.T_FIID;
        v_SaleLot2.T_TAXGROUP    := v_TaxGroup;
        v_SaleLot2.T_TYPE        := RSB_SCTXC.TXLOTS_SALE;
        v_SaleLot2.T_VIRTUALTYPE := RSB_SCTXC.TXVDEAL_REAL;
        v_SaleLot2.T_BUYID       := 0;
        v_SaleLot2.T_REALID      := 0;
        v_SaleLot2.T_PRICE       := 0;
        v_SaleLot2.T_PRICEFIID   := -1;
        v_SaleLot2.T_PRICECUR    := CHR(1);
        v_SaleLot2.T_DEALDATE    := M.T_COMMDATE;
        v_SaleLot2.T_DEALTIME    := TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS');
        v_SaleLot2.T_DEALCODE    := M.T_COMMCODE;
        v_SaleLot2.T_DEALCODETS  := M.T_COMMCODE;
        v_SaleLot2.T_BUYDATE     := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_SaleLot2.T_SALEDATE    := M.T_COMMDATE;
        v_SaleLot2.T_RETRDATE    := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_SaleLot2.T_OLDTYPE     := 0;
        v_SaleLot2.T_AMOUNT      := v_SaleAmount2;
        v_SaleLot2.T_NETTINGID   := 0;
        v_SaleLot2.T_NETTING     := 0;
        v_SaleLot2.T_SALE        := 0;
        v_SaleLot2.T_RETFLAG     := CHR(0);
        v_SaleLot2.T_ISFREE      := CHR(0);
        v_SaleLot2.T_BEGLOTID    := 0;
        v_SaleLot2.T_CHILDID     := 0;
        v_SaleLot2.T_ISCOMP      := CHR(0);
        v_SaleLot2.T_BEGBUYDATE  := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_SaleLot2.T_BEGSALEDATE := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_SaleLot2.T_COMPAMOUNT  := 0;
        v_SaleLot2.T_INACC       := CHR(0);
        v_SaleLot2.T_DEALSORTCODE:= CHR(1);
        v_SaleLot2.T_DEALSORT    := 0;
        v_SaleLot2.T_BLOCKED     := CHR(0);
        v_SaleLot2.T_RQID        := 0;
        v_SaleLot2.T_ORIGIN      := RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER;
        v_SaleLot2.T_GOID        := G.T_ID;
        v_SaleLot2.T_TOTALCOST   := 0;
        v_SaleLot2.T_PORTFOLIO   := v_SourceP2;

        INSERT INTO DSCTXLOT_DBT VALUES v_SaleLot2 RETURNING T_ID INTO v_SaleLot2.T_ID;

        UPDATE DSCTXLOT_DBT SET T_BEGLOTID = T_ID WHERE T_ID = v_SaleLot2.T_ID;

      END IF;

      --16
      WHILE v_SaleAmount1 > 0 LOOP
        IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
          -- если ФИФО
          BEGIN
            SELECT *
              INTO v_BuyLot
              FROM ( SELECT /*+ INDEX( Buy DSCTXLOT_DBT_IDXA)*/
                            Buy.*
                       FROM dsctxlot_dbt Buy
                      WHERE Buy.t_FIID = M.t_FIID
                        AND Buy.t_BuyDate <= M.t_CommDate
                        AND Buy.t_Type = RSB_SCTXC.TXLOTS_BUY
                        AND Buy.t_IsFree = 'X'
                        AND Buy.t_Portfolio = v_SourceP1
                   ORDER BY Buy.t_BegBuyDate ASC,
                            Buy.t_DealDate ASC,
                            Buy.t_DealTime ASC,
                            Buy.t_DealSort ASC )
             WHERE ROWNUM = 1;

             v_Buy_FreeAmount := (v_BuyLot.t_Amount - v_BuyLot.t_Netting - v_BuyLot.t_Sale);

             EXCEPTION
               WHEN NO_DATA_FOUND
                 THEN v_Buy_FreeAmount := 0;
          END;
        END IF;

        v_LotAmount1 := LEAST(v_SaleAmount1, v_Buy_FreeAmount);

        IF v_LotAmount1 > 0 THEN
          v_NewBuyLot := NULL;

          v_NewBuyLot := v_BuyLot;

          v_NewBuyLot.T_ID          := 0;
          v_NewBuyLot.T_AMOUNT      := v_LotAmount1;
          v_NewBuyLot.T_NETTING     := 0;
          v_NewBuyLot.T_SALE        := 0;
          v_NewBuyLot.T_ORIGIN      := RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER;
          v_NewBuyLot.T_PORTFOLIO   := v_DestP;
          v_NewBuyLot.T_GOID        := G.T_ID;

          INSERT INTO DSCTXLOT_DBT VALUES v_NewBuyLot RETURNING T_ID INTO v_NewBuyLot.T_ID;

          UPDATE DSCTXLOT_DBT SET T_BEGLOTID = T_ID WHERE T_ID = v_NewBuyLot.T_ID;

          RSI_TXDealSortOnDate(v_NewBuyLot.t_FIID, v_NewBuyLot.T_DEALDATE, v_NewBuyLot.T_DEALTIME, v_NewBuyLot.T_ID);

        END IF;

        v_SaleAmount1 := v_SaleAmount1 - v_LotAmount1;

      END LOOP;

      --17
      WHILE v_SaleAmount2 > 0 LOOP
        IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
          -- если ФИФО
          BEGIN
            SELECT *
              INTO v_BuyLot
              FROM ( SELECT /*+ INDEX( Buy DSCTXLOT_DBT_IDXA)*/
                            Buy.*
                       FROM dsctxlot_dbt Buy
                      WHERE Buy.t_FIID = M.t_FIID
                        AND Buy.t_BuyDate <= M.t_CommDate
                        AND Buy.t_Type = RSB_SCTXC.TXLOTS_BUY
                        AND Buy.t_IsFree = 'X'
                        AND Buy.t_Portfolio = v_SourceP2
                   ORDER BY Buy.t_BegBuyDate ASC,
                            Buy.t_DealDate ASC,
                            Buy.t_DealTime ASC,
                            Buy.t_DealSort ASC )
             WHERE ROWNUM = 1;

             v_Buy_FreeAmount := (v_BuyLot.t_Amount - v_BuyLot.t_Netting - v_BuyLot.t_Sale);

             EXCEPTION
               WHEN NO_DATA_FOUND
                 THEN v_Buy_FreeAmount := 0;
          END;
        END IF;

        v_LotAmount2 := LEAST(v_SaleAmount2, v_Buy_FreeAmount);

        BEGIN
          SELECT T_ID INTO v_BuyID
                FROM DSCTXLOT_DBT
               WHERE T_RQID = v_BuyLot.T_RQID
                 AND T_PORTFOLIO = v_DestP
                 AND T_ORIGIN = RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER;

          EXCEPTION
               WHEN NO_DATA_FOUND
                 THEN v_BuyID := 0;
        END;

        IF v_BuyID > 0 THEN

          UPDATE DSCTXLOT_DBT
             SET T_AMOUNT = T_AMOUNT + v_LotAmount2
           WHERE T_ID = v_BuyID;

        ELSIF v_LotAmount1 > 0 THEN
          v_NewBuyLot := NULL;

          v_NewBuyLot := v_BuyLot;

          v_NewBuyLot.T_ID          := 0;
          v_NewBuyLot.T_AMOUNT      := v_LotAmount2;
          v_NewBuyLot.T_NETTING     := 0;
          v_NewBuyLot.T_SALE        := 0;
          v_NewBuyLot.T_ORIGIN      := RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER;
          v_NewBuyLot.T_PORTFOLIO   := v_DestP;
          v_NewBuyLot.T_GOID        := G.T_ID;

          INSERT INTO DSCTXLOT_DBT VALUES v_NewBuyLot RETURNING T_ID INTO v_NewBuyLot.T_ID;

          UPDATE DSCTXLOT_DBT SET T_BEGLOTID = T_ID WHERE T_ID = v_NewBuyLot.T_ID;

          RSI_TXDealSortOnDate(v_NewBuyLot.t_FIID, v_NewBuyLot.T_DEALDATE, v_NewBuyLot.T_DEALTIME, v_NewBuyLot.T_ID);

        END IF;

        v_SaleAmount2 := v_SaleAmount2 - v_LotAmount2;

      END LOOP;

      --18
      IF v_SaleAmountBPP > 0 THEN
        FOR one_row IN (SELECT (LNK.T_AMOUNT - TXGetSumSCTXLSOnDate(LNK.T_ID, M.T_COMMDATE)) as LnkAmount,
                               LNK.T_TYPE as LnkType, LNK.T_SALEID as SaleID, LNK.T_ID as LnkID,
                               TXBUYLOT.T_RQID as RQID, TXBUYLOT.T_ID as TxBuyID
                          FROM DSCTXLNK_DBT LNK, DSCTXLOT_DBT TXBUYLOT, DSCTXLOT_DBT CURRBUYLOT, DSCTXLOT_DBT CURRSALELOT
                         WHERE LNK.T_TYPE IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO, RSB_SCTXC.TXLNK_LOANPUT, RSB_SCTXC.TXLNK_SUBSTLOAN)
                           AND CURRBUYLOT.T_ID = LNK.T_BUYID
                           AND TXBUYLOT.T_ID = CURRBUYLOT.T_BEGLOTID
                           AND TXBUYLOT.T_FIID = M.T_FIID
                           AND TXBUYLOT.T_TYPE = RSB_SCTXC.TXLOTS_BUY
                           AND TXBUYLOT.T_PORTFOLIO = v_SourceP1
                           AND CURRSALELOT.T_ID = LNK.T_SALEID
                           AND LNK.T_DATE <= M.T_COMMDATE
                           AND (LNK.T_BEGDATE = TO_DATE('01.01.0001','DD.MM.YYYY') OR LNK.T_BEGDATE <= M.T_COMMDATE)
                           AND (LNK.T_ENDDATE = TO_DATE('01.01.0001','DD.MM.YYYY') OR LNK.T_ENDDATE <= M.T_COMMDATE)
                           AND (CURRSALELOT.T_BUYDATE = TO_DATE('01.01.0001','DD.MM.YYYY') OR CURRSALELOT.T_BUYDATE > M.T_COMMDATE)
                         ORDER BY TXBUYLOT.T_BEGBUYDATE ASC, TXBUYLOT.T_DEALDATE ASC, TXBUYLOT.T_DEALTIME ASC, TXBUYLOT.T_DEALSORT ASC, LNK.T_ID ASC
                       )
        LOOP

          v_LotAmountBPP := LEAST(v_SaleAmountBPP, one_row.LnkAmount);

          TXPutMsg( 0,
                M.t_FIID,
                TXMES_DEBUG,
                'v_LotAmountBPP = '||TO_CHAR(v_LotAmountBPP));

          IF v_LotAmountBPP > 0 THEN
            BEGIN
              SELECT T_ID INTO v_BuyID
                FROM DSCTXLOT_DBT
               WHERE T_RQID = one_row.RQID
                 AND T_PORTFOLIO = v_DestP
                 AND T_ORIGIN = RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER
                 AND T_GOID = G.T_ID;

              EXCEPTION
                 WHEN NO_DATA_FOUND
                   THEN v_BuyID := 0;
            END;

            TXPutMsg( 0,
                        M.t_FIID,
                        TXMES_DEBUG,
                        'v_BuyID = '||TO_CHAR(v_BuyID));

            IF v_BuyID > 0 THEN

              UPDATE DSCTXLOT_DBT
                 SET T_AMOUNT = T_AMOUNT + v_LotAmountBPP
               WHERE T_ID = v_BuyID;

            ELSE

              v_NewBuyLot := NULL;

              SELECT * INTO v_BuyLot
                FROM DSCTXLOT_DBT
               WHERE T_ID = one_row.TxBuyID;


              v_NewBuyLot := v_BuyLot;

              v_NewBuyLot.T_ID          := 0;
              v_NewBuyLot.T_AMOUNT      := v_LotAmountBPP;
              v_NewBuyLot.T_NETTING     := 0;
              v_NewBuyLot.T_SALE        := 0;
              v_NewBuyLot.T_ORIGIN      := RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER;
              v_NewBuyLot.T_PORTFOLIO   := v_DestP;
              v_NewBuyLot.T_GOID        := G.T_ID;

              TXPutMsg( 0,
                        M.t_FIID,
                        TXMES_DEBUG,
                        'Вставляем лот по лоту v_BuyLot.t_ID = '||TO_CHAR(v_BuyLot.t_ID));

              INSERT INTO DSCTXLOT_DBT VALUES v_NewBuyLot RETURNING T_ID INTO v_NewBuyLot.T_ID;

              v_BuyID := v_NewBuyLot.T_ID;

              UPDATE DSCTXLOT_DBT SET T_BEGLOTID = T_ID WHERE T_ID = v_NewBuyLot.T_ID;

              RSI_TXDealSortOnDate(v_NewBuyLot.t_FIID, v_NewBuyLot.T_DEALDATE, v_NewBuyLot.T_DEALTIME, v_NewBuyLot.T_ID);

            END IF;

            --18.4. Перевешиваем связь на новый лот
            v_SL := RSB_SCTXC.TXLNK_SUBSTLOAN;
            IF one_row.LnkType = RSB_SCTXC.TXLNK_DELREPO OR one_row.LnkType = RSB_SCTXC.TXLNK_SUBSTREPO THEN
              v_SL := RSB_SCTXC.TXLNK_SUBSTREPO;
            END IF;

            BEGIN
              SELECT LCS.T_ID INTO v_LnkID
                FROM DSCTXLNK_DBT LCS
               WHERE LCS.T_TYPE = v_SL
                 AND LCS.T_BUYID = v_BuyID
                 AND LCS.T_SALEID = one_row.SaleID
                 AND LCS.T_SOURCEID = 0
                 AND LCS.T_DESTID = 0
                 AND LCS.T_LOT1ID = one_row.TxBuyID
                 AND LCS.T_LOT2ID = 0
                 AND LCS.T_DATE = M.T_COMMDATE;

              EXCEPTION
                 WHEN NO_DATA_FOUND
                   THEN v_LnkID := 0;
            END;

            IF v_LnkID > 0 THEN
              UPDATE DSCTXLNK_DBT
                 SET T_AMOUNT = T_AMOUNT + v_LotAmountBPP
               WHERE T_ID = v_LnkID;

            ELSE

              v_NewLnk := NULL;

              v_NewLnk.T_ID       := 0;
              v_NewLnk.T_SALEID   := one_row.SaleID;
              v_NewLnk.T_BUYID    := v_BuyID;
              v_NewLnk.T_TYPE     := v_SL;
              v_NewLnk.T_SOURCEID := 0;
              v_NewLnk.T_DESTID   := 0;
              v_NewLnk.T_LOT1ID   := one_row.TxBuyID;
              v_NewLnk.T_LOT2ID   := 0;
              v_NewLnk.T_DATE     := M.T_COMMDATE;
              v_NewLnk.T_SHORT    := 0;
              v_NewLnk.T_RET      := 0;
              v_NewLnk.T_RET2     := 0;
              v_NewLnk.T_RETSP    := 0;
              v_NewLnk.T_BEGDATE  := TO_DATE('01.01.0001','DD.MM.YYYY');
              v_NewLnk.T_ENDDATE  := TO_DATE('01.01.0001','DD.MM.YYYY');
              v_NewLnk.T_AMOUNT   := v_LotAmountBPP;
              v_NewLnk.T_RETFLAG  := CHR(0);
              v_NewLnk.T_FIID     := M.T_FIID;

              INSERT INTO DSCTXLNK_DBT VALUES v_NewLnk RETURNING T_ID INTO v_NewLnk.T_ID;
              v_LnkID := v_NewLnk.T_ID;

            END IF;

            v_NewLs := NULL;

            v_NewLs.T_CHILDID  := v_LnkID;
            v_NewLs.T_PARENTID := one_row.LnkID;
            v_NewLs.T_SHORT    := v_LotAmountBPP;

            INSERT INTO DSCTXLS_DBT VALUES v_NewLs;

            v_SaleAmountBPP := v_SaleAmountBPP - v_LotAmountBPP;
          END IF;
        END LOOP;
      END IF;

      --19
      FOR one_lot IN (SELECT *
                        FROM DSCTXLOT_DBT
                       WHERE T_TYPE = RSB_SCTXC.TXLOTS_BUY
                         AND T_FIID = M.T_FIID
                         AND T_BUYDATE = M.T_COMMDATE
                         AND T_ORIGIN = RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER
                         AND T_GOID = G.T_ID
                     )
      LOOP
        TXPutMsg( one_lot.t_ID,
                  one_lot.t_FIID,
                  TXMES_MESSAGE,
                  'Произведено зачисление '||TO_CHAR(one_lot.t_Amount)||' ц/б '||v_FICODE||' '||v_FINAME|| ' в рамках обработки операции перемещения №'||M.T_COMMCODE||' на дату '||TO_CHAR(M.T_COMMDATE,'DD.MM.YYYY'));

      END LOOP;


    END; --RSI_TXProcessMoving



  ----Выполняет списание лота продажи SALELOT из сделок обратного Репо и привлечения займа, закрытых не в день продажи
    PROCEDURE RSI_TXLinkSaleToReverseRepo( v_SaleLot IN dsctxlot_dbt%ROWTYPE, S IN OUT NUMBER )
    IS
      v_Buy_ID     NUMBER;
      v_BuyType    NUMBER;
      v_stat       NUMBER;
      v_FreeAmount NUMBER;
      v_Type       NUMBER;
      v_Link       dsctxlnk_dbt%ROWTYPE;
    BEGIN
      v_stat := 0;

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_DEBUG,
                'Вызов RSI_TXLinkSaleToReverseRepo SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate||', S = '||S );

      WHILE S > 0 AND v_stat = 0 LOOP
        BEGIN
          IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
          -- если ФИФО
            SELECT t_ID, t_Type, FreeAmount
              INTO v_Buy_ID, v_BuyType, v_FreeAmount
              FROM ( SELECT /*+ INDEX( Buy DSCTXLOT_DBT_IDXB)*/
                            Buy.t_ID, Buy.t_Type,
                            (Buy.t_Amount - Buy.t_Sale) FreeAmount
                       FROM dsctxlot_dbt Buy
                      WHERE Buy.t_FIID = v_SaleLot.t_FIID
                        AND Buy.t_BuyDate <= v_SaleLot.t_SaleDate
                        AND (Buy.t_Type = RSB_SCTXC.TXLOTS_BACKREPO OR Buy.t_Type = RSB_SCTXC.TXLOTS_LOANGET)
                        AND Buy.t_IsFree = CHR(88)
                   ORDER BY Buy.t_OrdForSale ASC,
                            Buy.t_BegBuyDate DESC,
                            Buy.t_DealDate DESC,
                            Buy.t_DealTime DESC,
                            Buy.t_DealSort DESC )
             WHERE ROWNUM = 1;
          END IF;

          v_Type := RSB_SCTXC.TXLNK_UNDEF;

          IF( v_BuyType = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
            v_Type := RSB_SCTXC.TXLNK_OPSREPO;
          ELSIF( v_BuyType = RSB_SCTXC.TXLOTS_LOANGET ) THEN
            v_Type := RSB_SCTXC.TXLNK_OPSLOAN;
          END IF;

          v_Link := NULL;

          v_Link.t_SaleID     := v_SaleLot.t_ID;
          v_Link.t_BuyID      := v_Buy_ID;
          v_Link.t_Type       := v_Type;
          v_Link.t_SourceID   := 0;
          v_Link.t_DestID     := 0;
          v_Link.t_Lot1ID     := 0;
          v_Link.t_Lot2ID     := 0;
          v_Link.t_Date       := v_SaleLot.t_SaleDate;
          v_Link.t_Short      := 0;
          v_Link.t_Ret        := 0;
          v_Link.t_Ret2       := 0;
          v_Link.t_RetSP      := 0;
          v_Link.t_BegDate    := TO_DATE('01.01.0001', 'DD.MM.YYYY');
          v_Link.t_EndDate    := TO_DATE('01.01.0001', 'DD.MM.YYYY');
          v_Link.t_Amount     := iif( S < v_FreeAmount, S, v_FreeAmount );
          v_Link.t_RetFlag    := CHR(0);
          v_Link.t_FIID       := v_SaleLot.t_FIID;

          INSERT INTO dsctxlnk_dbt VALUES v_Link;

          S := S - v_Link.t_Amount;

        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
              BEGIN
                v_stat := 1;
              END;

        END;
      END LOOP;

      TXPutMsg( v_SaleLot.t_ID,
                v_SaleLot.t_FIID,
                TXMES_DEBUG,
                'Конец RSI_TXLinkSaleToReverseRepo SaleLot.t_ID = '||v_SaleLot.t_ID||', SaleLot.t_SaleDate = '||v_SaleLot.t_SaleDate||', S = '||S );

    END; --RSI_TXLinkSaleToReverseRepo

  ----Выполняет связывание по 2 ч. лота Репо обратного/Займу размещения. PART2LOT
    PROCEDURE RSI_TXLinkPart2ToBuy( v_Part2Lot IN dsctxlot_dbt%ROWTYPE )
    IS
      v_S           NUMBER;
      v_SPS         NUMBER;
      v_OSPL        NUMBER;
      v_CSPL        NUMBER;
      v_FreeAmount  NUMBER;
      v_SaleLotID   NUMBER;
      v_BuyLot_Type NUMBER;
      v_BuyLot_ID   NUMBER;
      v_SaleLotType NUMBER;
      v_VB_ID       NUMBER;
      v_VS_ID       NUMBER;
      v_A           NUMBER;
      v_VirtualNum  NUMBER;
      v_Break       BOOLEAN;
      v_SL          NUMBER;
      v_Lnk_ID      NUMBER;
      v_LnkID       NUMBER;
      v_LCS_ID      NUMBER;

      v_Lnk_A       NUMBER;
      v_Lnk_S       NUMBER;
      v_SaleVrtType NUMBER;
      v_SaleLotDealCodeTS dsctxlot_dbt.t_DealCodeTS%TYPE;
      v_LotDealCode dsctxlot_dbt.t_DealCode%TYPE;
      v_RealID      NUMBER;
      v_SaleRID     NUMBER;
      v_SaleBEGLOTID     dsctxlot_dbt.T_BEGLOTID%TYPE;
      v_SaleDEALDATE     dsctxlot_dbt.T_DEALDATE%TYPE;
      v_SaleDEALTIME     dsctxlot_dbt.T_DEALTIME%TYPE;
      v_SaleBEGBUYDATE   dsctxlot_dbt.T_BEGBUYDATE%TYPE;
      v_SaleBEGSALEDATE  dsctxlot_dbt.T_BEGSALEDATE%TYPE;
      v_SalePRICE        dsctxlot_dbt.T_PRICE%TYPE;
      v_SalePRICEFIID    dsctxlot_dbt.T_PRICEFIID%TYPE;
      v_SalePRICECUR     dsctxlot_dbt.T_PRICECUR%TYPE;
      v_SaleDEALSORTCODE dsctxlot_dbt.T_DEALSORTCODE%TYPE;

      v_FICODE     dfininstr_dbt.t_FI_Code%TYPE;
      v_CSPStr     VARCHAR2(30);
      v_SDStr      VARCHAR2(30);

      TYPE OSPLNKCurTyp IS REF CURSOR;
      OSPLNK_cur OSPLNKCurTyp;

    BEGIN

      TXPutMsg( v_Part2Lot.t_ID,
                v_Part2Lot.t_FIID,
                TXMES_DEBUG,
                'Вызов RSI_TXLinkPart2ToBuy Part2Lot.t_ID = '||v_Part2Lot.t_ID||', Part2Lot.t_SaleDate = '||v_Part2Lot.t_SaleDate );

      TXPutMsg( v_Part2Lot.t_ID,
                v_Part2Lot.t_FIID,
                TXMES_OPTIM,
                'Вызов RSI_TXLinkPart2ToBuy Part2Lot.t_ID = '||v_Part2Lot.t_ID||', Part2Lot.t_SaleDate = '||v_Part2Lot.t_SaleDate );

      IF( v_Part2Lot.t_Type = RSB_SCTXC.TXLOTS_BACKREPO ) THEN
        v_OSPL := RSB_SCTXC.TXLNK_OPSREPO;
        v_CSPL := RSB_SCTXC.TXLNK_CLSREPO;
        v_CSPStr := 'Репо';
        v_SDStr  := 'обратное Репо';
      ELSE
        v_OSPL := RSB_SCTXC.TXLNK_OPSLOAN;
        v_CSPL := RSB_SCTXC.TXLNK_CLSLOAN;
        v_CSPStr := 'займу';
        v_SDStr  := 'привлечения займа';
      END IF;

      v_S := v_Part2Lot.t_Sale;

      IF v_S = 0 THEN
        TXPutMsg( v_Part2Lot.t_ID,
                  v_Part2Lot.t_FIID,
                  TXMES_DEBUG,
                  'Конец1 RSI_TXLinkPart2ToBuy Part2Lot.t_ID = '||v_Part2Lot.t_ID||', Part2Lot.t_SaleDate = '||v_Part2Lot.t_SaleDate );
        RETURN; -- нет списаний
      END IF;

      IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
        OPEN OSPLNK_cur FOR SELECT lnk.t_Amount, lnk.t_Short, lnk.t_ID LinkID,
                                   salelot.t_ID, salelot.t_VirtualType,
                                   salelot.t_RealID, salelot.t_Type, salelot.t_DealCodeTS,
                                   SALELOT.T_BEGLOTID    ,
                                   SALELOT.T_DEALDATE    ,
                                   SALELOT.T_DEALTIME    ,
                                   SALELOT.T_BEGBUYDATE  ,
                                   SALELOT.T_BEGSALEDATE ,
                                   SALELOT.T_PRICE       ,
                                   SALELOT.T_PRICEFIID   ,
                                   SALELOT.T_PRICECUR    ,
                                   SALELOT.T_DEALSORTCODE
                              FROM dsctxlnk_dbt lnk, dsctxlot_dbt salelot
                             WHERE lnk.t_BuyID = v_Part2Lot.t_ID
                               AND lnk.t_Type = v_OSPL
                               AND salelot.t_ID = lnk.t_SaleID
                               AND (lnk.t_Amount - lnk.t_Short) > 0
                          ORDER BY salelot.t_BegSaleDate ASC,
                                   salelot.t_DealDate ASC,
                                   salelot.t_DealTime ASC,
                                   salelot.t_DealSort ASC;
      END IF;

      LOOP
        FETCH OSPLNK_cur INTO v_Lnk_A, v_Lnk_S, v_Lnk_ID,
                              v_SaleLotID, v_SaleVrtType,
                              v_SaleRID, v_SaleLotType, v_SaleLotDealCodeTS,
                              v_SaleBEGLOTID    ,
                              v_SaleDEALDATE    ,
                              v_SaleDEALTIME    ,
                              v_SaleBEGBUYDATE  ,
                              v_SaleBEGSALEDATE ,
                              v_SalePRICE       ,
                              v_SalePRICEFIID   ,
                              v_SalePRICECUR    ,
                              v_SaleDEALSORTCODE;
        EXIT WHEN OSPLNK_cur%NOTFOUND OR
                  OSPLNK_cur%NOTFOUND IS NULL;

        BEGIN

          v_SPS := v_Lnk_A  - v_Lnk_S;
          v_Break := FALSE;

          WHILE v_SPS > 0 LOOP
            BEGIN
              IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
                -- если ФИФО
                SELECT *
                  INTO v_BuyLot_ID, v_BuyLot_Type, v_FreeAmount
                  FROM ( SELECT /*+ INDEX (Buy DSCTXLOT_DBT_IDXG)*/
                                Buy.t_ID,
                                Buy.t_Type,
                                (Buy.t_Amount - Buy.t_Netting - Buy.t_Sale) FreeAmount
                           FROM dsctxlot_dbt Buy
                          WHERE Buy.t_FIID = v_Part2Lot.t_FIID
                            AND Buy.t_BuyDate <= v_Part2Lot.t_SaleDate
                            AND (Buy.t_Type = RSB_SCTXC.TXLOTS_BACKREPO OR Buy.t_Type = RSB_SCTXC.TXLOTS_LOANGET)
                            AND Buy.t_IsFree = CHR(88)
                       ORDER BY Buy.t_OrdForClPosRepo ASC,
                                Buy.t_BegBuyDate DESC,
                                Buy.t_DealDate DESC,
                                Buy.t_DealTime DESC,
                                Buy.t_DealSort DESC )
                 WHERE ROWNUM = 1;
              END IF;

            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                BEGIN
                  v_Break := TRUE;
                END;
            END;

            EXIT WHEN v_Break;

            v_A := iif( v_SPS < v_FreeAmount, v_SPS, v_FreeAmount );

            SELECT NVL(MAX(TO_NUMBER(SUBSTR(t_DealCodeTS, INSTR(t_DealCodeTS, '/') + 1))), 0) + 1
              INTO v_VirtualNum
              FROM dsctxlot_dbt
             WHERE ( (t_SaleDate = v_Part2Lot.t_SaleDate AND t_Type = RSB_SCTXC.TXLOTS_SALE) OR
                     (t_BuyDate = v_Part2Lot.t_SaleDate AND t_Type = RSB_SCTXC.TXLOTS_BUY) )
               AND (t_VirtualType = RSB_SCTXC.TXVDEAL_MARKETCOST OR
                    t_VirtualType = RSB_SCTXC.TXVDEAL_ZEROCOST OR
                    t_VirtualType = RSB_SCTXC.TXVDEAL_CALC
                   );

            -- Вставить запись DSCTXLOT VB
            v_LotDealCode := TXGenVirtNum( 0, v_Part2Lot.t_SaleDate, v_VirtualNum );
            v_RealID      := iif(v_SaleVrtType = RSB_SCTXC.TXVDEAL_REAL, v_SaleLotID, v_SaleRID);

            INSERT INTO dsctxlot_dbt( t_DealID     ,
                                      t_FIID       ,
                                      t_TaxGroup   ,
                                      t_Type       ,
                                      t_BegLotID   ,
                                      t_VirtualType,
                                      t_DealCode   ,
                                      t_DealCodeTS ,
                                      t_BuyID      ,
                                      t_RealID     ,
                                      t_DealDate   ,
                                      t_DealTime   ,
                                      t_SaleDate   ,
                                      t_BegSaleDate,
                                      t_BuyDate    ,
                                      t_BegBuyDate ,
                                      t_RetrDate   ,
                                      t_Amount     ,
                                      t_NettingID  ,
                                      t_Price      ,
                                      t_PriceFIID  ,
                                      t_PriceCUR   ,
                                      t_OldType    ,
                                      t_Netting    ,
                                      t_Sale       ,
                                      t_RetFlag    ,
                                      t_IsFree     ,
                                      t_OrdForSale ,
                                      t_OrdForRepo ,
                                      t_DealSortCode
                                    )
                              VALUES(
                                      0,                                     --t_DealID
                                      v_Part2Lot.t_FIID,                     --t_FIID
                                      v_Part2Lot.t_TaxGroup,                 --t_TaxGroup
                                      RSB_SCTXC.TXLOTS_BUY,                  --t_Type
                                      v_SaleBEGLOTID,                        --t_BegLotID
                                      RSB_SCTXC.TXVDEAL_CALC,                --t_VirtualType
                                      v_LotDealCode,                         --t_DealCode
                                      v_LotDealCode,                         --t_DealCodeTS
                                      0,                                     --t_BuyID
                                      v_RealID,                              --t_RealID
                                      v_SaleDEALDATE,                        --t_DealDate
                                      v_SaleDEALTIME,                        --t_DealTime
                                      TO_DATE('01.01.0001', 'DD.MM.YYYY'),   --t_SaleDate
                                      TO_DATE('01.01.0001', 'DD.MM.YYYY'),   --t_BegSaleDate
                                      v_Part2Lot.t_SaleDate,                 --t_BuyDate
                                      v_SaleBEGBUYDATE,                      --t_BegBuyDate
                                      TO_DATE('01.01.0001', 'DD.MM.YYYY'),   --t_RetrDate
                                      v_A,                                   --t_Amount
                                      0,                                     --t_NettingID
                                      v_SalePRICE,                           --t_Price
                                      v_SalePRICEFIID,                       --t_PriceFIID
                                      v_SalePRICECUR,                        --t_PriceCUR
                                      RSB_SCTXC.TXLOTS_UNDEF,                --t_OldType
                                      0,                                     --t_Netting
                                      0,                                     --t_Sale
                                      CHR(0),                                --t_RetFlag
                                      CHR(0),                                --t_IsFree
                                      0,                                     --t_OrdForSale
                                      0,                                     --t_OrdForRepo
                                      v_SaleDEALSORTCODE                     --t_DealSortCode
                                    ) RETURNING t_ID INTO v_VB_ID;

            v_LotDealCode := TXGenVirtNum( 1, v_Part2Lot.t_SaleDate, v_VirtualNum + 1 );
            -- Вставить запись DSCTXLOT VS
            INSERT INTO dsctxlot_dbt( t_DealID     ,
                                      t_FIID       ,
                                      t_TaxGroup   ,
                                      t_Type       ,
                                      t_BegLotID   ,
                                      t_VirtualType,
                                      t_DealCode   ,
                                      t_DealCodeTS ,
                                      t_BuyID      ,
                                      t_RealID     ,
                                      t_DealDate   ,
                                      t_DealTime   ,
                                      t_SaleDate   ,
                                      t_BegSaleDate,
                                      t_BuyDate    ,
                                      t_BegBuyDate ,
                                      t_RetrDate   ,
                                      t_Amount     ,
                                      t_NettingID  ,
                                      t_Price      ,
                                      t_PriceFIID  ,
                                      t_PriceCUR   ,
                                      t_OldType    ,
                                      t_Netting    ,
                                      t_Sale       ,
                                      t_RetFlag    ,
                                      t_IsFree     ,
                                      t_OrdForSale ,
                                      t_OrdForRepo ,
                                      t_DealSortCode
                                    )
                              VALUES(
                                      0,                                     --t_DealID
                                      v_Part2Lot.t_FIID,                     --t_FIID
                                      v_Part2Lot.t_TaxGroup,                 --t_TaxGroup
                                      RSB_SCTXC.TXLOTS_SALE,                 --t_Type
                                      v_SaleBEGLOTID,                        --t_BegLotID
                                      RSB_SCTXC.TXVDEAL_CALC,                --t_VirtualType
                                      v_LotDealCode,                         --t_DealCode
                                      v_LotDealCode,                         --t_DealCodeTS
                                      v_VB_ID,                               --t_BuyID
                                      v_RealID,                              --t_RealID
                                      v_SaleDEALDATE,                        --t_DealDate
                                      v_SaleDEALTIME,                        --t_DealTime
                                      v_Part2Lot.t_SaleDate,                 --t_SaleDate
                                      v_SaleBEGSALEDATE,                     --t_BegSaleDate
                                      TO_DATE('01.01.0001', 'DD.MM.YYYY'),   --t_BuyDate
                                      TO_DATE('01.01.0001', 'DD.MM.YYYY'),   --t_BegBuyDate
                                      TO_DATE('01.01.0001', 'DD.MM.YYYY'),   --t_RetrDate
                                      v_A,                                   --t_Amount
                                      0,                                     --t_NettingID
                                      v_SalePRICE,                           --t_Price
                                      v_SalePRICEFIID,                       --t_PriceFIID
                                      v_SalePRICECUR,                        --t_PriceCUR
                                      RSB_SCTXC.TXLOTS_UNDEF,                --t_OldType
                                      0,                                     --t_Netting
                                      0,                                     --t_Sale
                                      CHR(0),                                --t_RetFlag
                                      CHR(0),                                --t_IsFree
                                      0,                                     --t_OrdForSale
                                      0,                                     --t_OrdForRepo
                                      v_SaleDEALSORTCODE                     --t_DealSortCode
                                    ) RETURNING t_ID INTO v_VS_ID;


            RSI_TXDealSortOnDate(v_Part2Lot.t_FIID, v_SaleDEALDATE, v_SaleDEALTIME, v_VB_ID);

            INSERT INTO dsctxlnk_dbt( t_SaleID,
                                      t_BuyID,
                                      t_Type,
                                      t_SourceID,
                                      t_DestID,
                                      t_Lot1ID,
                                      t_Lot2ID,
                                      t_Date,
                                      t_Short,
                                      t_Ret,
                                      t_Ret2,
                                      t_RetSP,
                                      t_BegDate,
                                      t_EndDate,
                                      t_Amount,
                                      t_RetFlag,
                                      t_FIID)
                              VALUES( v_SaleLotID,
                                      v_VB_ID,
                                      v_CSPL,
                                      v_Part2Lot.t_ID,
                                      0,
                                      0,
                                      0,
                                      v_Part2Lot.t_SaleDate,
                                      0,
                                      0,
                                      0,
                                      0,
                                      TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                      TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                      v_A,
                                      CHR(0),
                                      v_Part2Lot.t_FIID
                                    ) RETURNING t_ID INTO v_LCS_ID;

            INSERT INTO dsctxls_dbt( t_ChildID,
                                     t_ParentID,
                                     t_Short)
                             VALUES( v_LCS_ID,
                                     v_Lnk_ID,
                                     v_A );

            INSERT INTO dsctxlnk_dbt( t_SaleID,
                                      t_BuyID,
                                      t_Type,
                                      t_SourceID,
                                      t_DestID,
                                      t_Lot1ID,
                                      t_Lot2ID,
                                      t_Date,
                                      t_Short,
                                      t_Ret,
                                      t_Ret2,
                                      t_RetSP,
                                      t_BegDate,
                                      t_EndDate,
                                      t_Amount,
                                      t_RetFlag,
                                      t_FIID)
                              VALUES( v_VS_ID,
                                      v_BuyLot_ID,
                                      DECODE(v_BuyLot_Type, RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLNK_OPSREPO, RSB_SCTXC.TXLNK_OPSLOAN),
                                      0,
                                      0,
                                      0,
                                      0,
                                      v_Part2Lot.t_SaleDate,
                                      0,
                                      0,
                                      0,
                                      0,
                                      TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                      TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                      v_A,
                                      CHR(0),
                                      v_Part2Lot.t_FIID
                                     );
            v_S := v_S - v_A;
            v_SPS := v_SPS - v_A;

          END LOOP;



          WHILE v_SPS > 0 LOOP
            BEGIN
              IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
                -- если ФИФО
                SELECT *
                  INTO v_BuyLot_ID, v_BuyLot_Type, v_FreeAmount
                  FROM (  SELECT q1.t_ID,
                                 q1.t_Type,
                                 (q1.t_Amount - q1.t_Netting - q1.t_Sale) FreeAmount
                            FROM (SELECT                   /*+ INDEX (Buy DSCTXLOT_DBT_IDXA)*/
                                        Buy.*,
                                         CASE
                                            WHEN Rsb_SCTX.GetInAvrWrtStartDate (tick.t_DealID) =
                                                    TO_DATE ('31-12-9999', 'DD-MM-YYYY')
                                            THEN
                                               Buy.t_DealDate
                                            ELSE
                                               Rsb_SCTX.GetInAvrWrtStartDate (tick.t_DealID)
                                         END
                                            AS t_wrtstart
                                    FROM dsctxlot_dbt Buy, ddl_tick_dbt tick
                                   WHERE     Buy.t_FIID = v_Part2Lot.t_FIID
                                         AND Buy.t_BuyDate <= v_Part2Lot.t_SaleDate
                                         AND Buy.t_Type = RSB_SCTXC.TXLOTS_BUY
                                         AND Buy.t_IsFree = CHR (88)
                                         AND tick.t_DealID = Buy.t_DealID
                                         AND ( (tick.t_Ofbu = 'X'
                                                AND v_Part2Lot.t_SaleDate >=
                                                       GetInAvrWrtStartDate (tick.t_DealID))
                                              OR tick.t_Ofbu <> 'X')) q1
                        ORDER BY q1.t_BegBuyDate ASC,
                                 q1.t_DealDate ASC,
                                 q1.t_wrtstart ASC,
                                 q1.t_DealTime ASC,
                                 q1.t_DealSort ASC)
                 WHERE ROWNUM = 1;
              END IF;

            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                BEGIN
                  v_Break := TRUE;

                  SELECT t_FI_Code
                    INTO v_FICODE
                    FROM dfininstr_dbt
                   WHERE t_FIID = v_Part2Lot.t_FIID;

                  TXPutMsg( v_Part2Lot.t_ID,
                            v_Part2Lot.t_FIID,
                            TXMES_WARNING,
                            'Недостаточно ц/б "'||v_FICODE||'" на дату '||TO_CHAR(v_Part2Lot.t_SaleDate,'DD.MM.YYYY')||
                            ' для закрытия короткой позиции по '||v_CSPStr||' с внешним кодом "'||v_Part2Lot.t_DealCodeTS||
                            '", открытой продажей с внешним кодом "'||v_SaleLotDealCodeTS||'"' );
                END;
            END;

            EXIT WHEN v_Break;

            v_A := iif( v_SPS < v_FreeAmount, v_SPS, v_FreeAmount );

            INSERT INTO dsctxlnk_dbt( t_SaleID,
                                      t_BuyID,
                                      t_Type,
                                      t_SourceID,
                                      t_DestID,
                                      t_Lot1ID,
                                      t_Lot2ID,
                                      t_Date,
                                      t_Short,
                                      t_Ret,
                                      t_Ret2,
                                      t_RetSP,
                                      t_BegDate,
                                      t_EndDate,
                                      t_Amount,
                                      t_RetFlag,
                                      t_FIID  )
                             VALUES ( v_SaleLotID,
                                      v_BuyLot_ID,
                                      v_CSPL,
                                      v_Part2Lot.t_ID,
                                      0,
                                      0,
                                      0,
                                      v_Part2Lot.t_SaleDate,
                                      0,
                                      0,
                                      0,
                                      0,
                                      TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                      TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                      v_A,
                                      CHR(0),
                                      v_Part2Lot.t_FIID ) RETURNING t_ID INTO v_LCS_ID;

            INSERT INTO dsctxls_dbt( t_ChildID,
                                     t_ParentID,
                                     t_Short)
                             VALUES( v_LCS_ID,
                                     v_Lnk_ID,
                                     v_A );

            v_S := v_S - v_A;
            v_SPS := v_SPS - v_A;

          END LOOP;

        EXCEPTION
          WHEN OTHERS THEN NULL;
        END;

        EXIT WHEN v_S <= 0; --выход из цикла
      END LOOP;

      CLOSE OSPLNK_cur;

      IF( v_S = 0 ) THEN
        TXPutMsg( v_Part2Lot.t_ID,
                  v_Part2Lot.t_FIID,
                  TXMES_DEBUG,
                  'Конец2 RSI_TXLinkPart2ToBuy Part2Lot.t_ID = '||v_Part2Lot.t_ID||', Part2Lot.t_SaleDate = '||v_Part2Lot.t_SaleDate );
        RETURN; -- все связали, выходим
      END IF;

      -- формируем связи по подстановкам
      IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
        OPEN OSPLNK_cur FOR SELECT lnk.t_Amount - lnk.t_Short, salelot.t_ID, salelot.t_Type, salelot.t_DealCodeTS, lnk.t_ID
                              FROM dsctxlnk_dbt lnk, dsctxlot_dbt salelot
                             WHERE lnk.t_BuyID = v_Part2Lot.t_ID
                               AND lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_LOANPUT, RSB_SCTXC.TXLNK_SUBSTREPO, RSB_SCTXC.TXLNK_SUBSTLOAN)
                               AND salelot.t_ID = lnk.t_SaleID
                               AND (lnk.t_Amount - lnk.t_Short) > 0
                               AND (salelot.t_BuyDate > v_Part2Lot.t_SaleDate OR salelot.t_BuyDate = TO_DATE('01-01-0001','DD-MM-YYYY'))
                          ORDER BY salelot.t_BegSaleDate DESC,
                                   salelot.t_DealDate DESC,
                                   salelot.t_DealTime DESC,
                                   salelot.t_DealSort DESC;
      END IF;

      LOOP

        FETCH OSPLNK_cur INTO v_SPS, v_SaleLotID, v_SaleLotType, v_SaleLotDealCodeTS, v_Lnk_ID;
        EXIT WHEN OSPLNK_cur%NOTFOUND OR
                  OSPLNK_cur%NOTFOUND IS NULL;

        v_Break := FALSE;

        WHILE v_SPS > 0 LOOP
          BEGIN
            IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
              SELECT *
                INTO v_BuyLot_ID, v_FreeAmount
                FROM ( SELECT /*+ INDEX(Buy DSCTXLOT_DBT_IDXF)*/
                              Buy.t_ID,
                              (Buy.t_Amount - Buy.t_Netting - Buy.t_Sale) FreeAmount
                         FROM dsctxlot_dbt Buy
                        WHERE Buy.t_FIID = v_Part2Lot.t_FIID
                          AND Buy.t_BuyDate <= v_Part2Lot.t_SaleDate
                          AND Buy.t_IsFree = CHR(88)
                     ORDER BY Buy.t_OrdForSubst ASC,
                              Buy.t_BegBuyDate DESC,
                              Buy.t_DealDate DESC,
                              Buy.t_DealTime DESC,
                              Buy.t_DealSort DESC )
               WHERE ROWNUM = 1;
            END IF;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN
                v_Break := TRUE;

                SELECT t_FI_Code
                  INTO v_FICODE
                  FROM dfininstr_dbt
                 WHERE t_FIID = v_Part2Lot.t_FIID;

                TXPutMsg( v_Part2Lot.t_ID,
                          v_Part2Lot.t_FIID,
                          TXMES_WARNING,
                          'Недостаточно ц/б "'||v_FICODE||'" на дату '||TO_CHAR(v_Part2Lot.t_SaleDate,'DD.MM.YYYY')||
                          ' для переноса участия в сделке вида "'||get_lotName(v_SaleLotType)||'" с внешним кодом "'||v_SaleLotDealCodeTS||
                          '" при выбытии  лота "'||v_SDStr||'" с внешним кодом "'||v_Part2Lot.t_DealCodeTS||'"' );
              END;
          END;

          EXIT WHEN v_Break;

          v_A := iif( v_SPS < v_FreeAmount, v_SPS, v_FreeAmount );

          IF v_SaleLotType = RSB_SCTXC.TXLOTS_REPO THEN
            v_SL := RSB_SCTXC.TXLNK_SUBSTREPO;
          ELSE
            v_SL := RSB_SCTXC.TXLNK_SUBSTLOAN;
          END IF;

          BEGIN
            SELECT t_ID
              INTO v_LnkID
              FROM dsctxlnk_dbt
             WHERE t_Type = v_SL
               AND t_BuyID = v_BuyLot_ID
               AND t_SaleID = v_SaleLotID
               AND t_SourceID = 0
               AND t_DestID = 0
               AND t_Lot1ID = v_Part2Lot.t_ID
               AND t_Lot2ID = 0
               AND t_DATE = v_Part2Lot.t_SaleDate;

            UPDATE dsctxlnk_dbt
               SET t_Amount = t_Amount + v_A
             WHERE t_ID = v_LnkID;

            INSERT INTO dsctxls_dbt( t_ChildID,
                                     t_ParentID,
                                     t_Short)
                             VALUES( v_LnkID,
                                     v_Lnk_ID,
                                     v_A );

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN
                INSERT INTO dsctxlnk_dbt( t_SaleID,
                                          t_BuyID,
                                          t_Type,
                                          t_SourceID,
                                          t_DestID,
                                          t_Lot1ID,
                                          t_Lot2ID,
                                          t_Date,
                                          t_Short,
                                          t_Ret,
                                          t_Ret2,
                                          t_RetSP,
                                          t_BegDate,
                                          t_EndDate,
                                          t_Amount,
                                          t_RetFlag, t_FIID )
                                 VALUES ( v_SaleLotID,
                                          v_BuyLot_ID,
                                          v_SL,
                                          0,
                                          0,
                                          v_Part2Lot.t_ID,
                                          0,
                                          v_Part2Lot.t_SaleDate,
                                          0,
                                          0,
                                          0,
                                          0,
                                          TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                          TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                          v_A,
                                          CHR(0),
                                          v_Part2Lot.t_FIID
                                         ) RETURNING t_ID INTO v_LCS_ID;

                INSERT INTO dsctxls_dbt( t_ChildID,
                                         t_ParentID,
                                         t_Short)
                                 VALUES( v_LCS_ID,
                                         v_Lnk_ID,
                                         v_A );

              END;
          END;

          v_SPS := v_SPS - v_A;

        END LOOP;

      END LOOP;

      CLOSE OSPLNK_cur;

      TXPutMsg( v_Part2Lot.t_ID,
                v_Part2Lot.t_FIID,
                TXMES_DEBUG,
                'Конец3 RSI_TXLinkPart2ToBuy Part2Lot.t_ID = '||v_Part2Lot.t_ID||', Part2Lot.t_SaleDate = '||v_Part2Lot.t_SaleDate );
    END; --RSI_TXLinkPart2ToBuy

  ----Выполняет обработку лота компенсационного платежа прямого Репо/займа размещения
    PROCEDURE TXProcessCompPayOnDirectRepo( v_CLot IN dsctxlot_dbt%ROWTYPE )
    IS
      v_Lnk        NUMBER;
      v_SLnk       NUMBER;
      v_RLotID     NUMBER;
      v_Amount     NUMBER;
      v_A          NUMBER;
      v_LnkAmount  NUMBER;
      v_Buy_ID     NUMBER;
      v_BuyType    NUMBER;
      v_Link       dsctxlnk_dbt%ROWTYPE;
      v_LnkID      NUMBER;
      v_Lnk_ID     NUMBER;
      TYPE LNKCurTyp IS REF CURSOR;
      LNK_cur LNKCurTyp;

    BEGIN

      TXPutMsg( v_CLot.t_ID,
                v_CLot.t_FIID,
                TXMES_DEBUG,
                'Вызов TXProcessCompPayOnDirectRepo CLot.t_ID = '||v_CLot.t_ID||', CLot.t_SaleDate = '||v_CLot.t_SaleDate);
      TXPutMsg( v_CLot.t_ID,
                v_CLot.t_FIID,
                TXMES_OPTIM,
                'Вызов TXProcessCompPayOnDirectRepo CLot.t_ID = '||v_CLot.t_ID||', CLot.t_SaleDate = '||v_CLot.t_SaleDate);

      IF v_CLot.t_Type = RSB_SCTXC.TXLOTS_REPO THEN
         v_Lnk  := RSB_SCTXC.TXLNK_DELREPO;
         v_SLnk := RSB_SCTXC.TXLNK_SUBSTREPO;
      ELSE
         v_Lnk  := RSB_SCTXC.TXLNK_LOANPUT;
         v_SLnk := RSB_SCTXC.TXLNK_SUBSTLOAN;
      END IF;

      UPDATE dsctxlot_dbt
         SET t_BuyDate = v_CLot.t_SaleDate
       WHERE t_ChildID = v_CLot.t_ID;

      SELECT MIN(t_ID) INTO v_RLotID
        FROM dsctxlot_dbt
       WHERE t_ChildID = v_CLot.t_ID;

      UPDATE dsctxlnk_dbt
         SET t_RetFlag = CHR(88)
       WHERE t_SaleID = v_RLotID
         AND t_Type in (v_Lnk, v_SLnk);

      UPDATE dsctxlot_dbt
         SET t_InAcc = CHR(88)
       WHERE t_ID = v_CLot.t_ID;

      v_Amount := v_CLot.t_Amount;

      IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
        OPEN LNK_cur FOR SELECT Lnk.t_ID, Lnk.t_BuyID, B.t_Type, (Lnk.t_Amount-Lnk.t_Short) LnkAmount
                           FROM dsctxlot_dbt B, dsctxlnk_dbt Lnk
                      LEFT JOIN dsctxlot_dbt L1 on Lnk.t_Lot1ID = L1.t_ID
                          WHERE Lnk.t_Type in (v_Lnk, v_SLnk)
                            AND B.t_ID = Lnk.t_BuyID
                            AND Lnk.t_SaleID = v_RLotID
                            AND (B.t_SaleDate = TO_DATE('01-01-0001','DD-MM-YYYY') or
                                 B.t_SaleDate >= v_CLot.t_SaleDate
                                )
                            AND (Lnk.t_Amount-Lnk.t_Short) > 0
                       ORDER BY B.t_OrdForRepo ASC,
                                B.t_BegBuyDate DESC,
                                B.t_DealDate DESC,
                                B.t_DealTime DESC,
                                B.t_DealSort DESC,
                                (CASE WHEN Lnk.t_Lot1ID = 0 OR
                                           Lnk.t_Lot1ID IS NULL
                                      THEN 1
                                      ELSE 2
                                      END) ASC,
                                L1.t_BegBuyDate DESC,
                                L1.t_DealDate DESC,
                                L1.t_DealTime DESC,
                                L1.t_DealSort DESC;
      END IF;

      WHILE v_Amount > 0 LOOP

        FETCH LNK_cur INTO v_Lnk_ID, v_Buy_ID, v_BuyType, v_LnkAmount;
        EXIT WHEN LNK_cur%NOTFOUND OR
                  LNK_cur%NOTFOUND IS NULL;

        v_A := iif( v_Amount < v_LnkAmount, v_Amount, v_LnkAmount );

        BEGIN
          SELECT t_ID
            INTO v_LnkID
            FROM dsctxlnk_dbt
           WHERE t_Type = v_Lnk
             AND t_BuyID  = v_Buy_ID
             AND t_SaleID = v_CLot.t_ID
             AND t_SourceID = 0
             AND t_DestID = 0
             AND t_Lot1ID = 0
             AND t_Lot2ID = 0
             AND t_DATE = v_CLot.t_SaleDate;

          UPDATE dsctxlnk_dbt
             SET t_Amount = t_Amount + v_A
           WHERE t_ID = v_LnkID;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            BEGIN
              v_Link := NULL;
              v_Link.t_SaleID     := v_CLot.t_ID;
              v_Link.t_BuyID      := v_Buy_ID;
              v_Link.t_Type       := v_Lnk;
              v_Link.t_SourceID   := 0;
              v_Link.t_DestID     := 0;
              v_Link.t_Lot1ID     := 0;
              v_Link.t_Lot2ID     := 0;
              v_Link.t_Date       := v_CLot.t_SaleDate;
              v_Link.t_Short      := 0;
              v_Link.t_Ret        := 0;
              v_Link.t_Ret2       := 0;
              v_Link.t_RetSP      := 0;
              v_Link.t_BegDate    := TO_DATE('01.01.0001', 'DD.MM.YYYY');
              v_Link.t_EndDate    := TO_DATE('01.01.0001', 'DD.MM.YYYY');
              v_Link.t_Amount     := v_A;
              v_Link.t_RetFlag    := CHR(0);
              v_Link.t_FIID       := v_CLot.t_FIID;

              INSERT INTO dsctxlnk_dbt VALUES v_Link;

            END;
        END;

        v_Amount := v_Amount - v_A;

      END LOOP;

      CLOSE LNK_cur;

      TXPutMsg( v_CLot.t_ID,
                v_CLot.t_FIID,
                TXMES_DEBUG,
                'Конец TXProcessCompPayOnDirectRepo CLot.t_ID = '||v_CLot.t_ID||', CLot.t_SaleDate = '||v_CLot.t_SaleDate);

    END; --TXProcessCompPayOnDirectRepo

  ----Выполняет обработку лота компенсационного платежа обратного Репо/займа привлечения
    PROCEDURE TXProcessCompPayOnReverseRepo( v_CLot IN dsctxlot_dbt%ROWTYPE )
    IS
      v_RLotID      NUMBER;
      v_RLot        dsctxlot_dbt%ROWTYPE;
      v_OSPL        NUMBER;
      v_CSPL        NUMBER;
      v_S           NUMBER;
      v_SPS         NUMBER;
      v_A           NUMBER;
      v_VirtualNum  NUMBER;
      v_RealID      NUMBER;
      v_SaleLot     dsctxlot_dbt%ROWTYPE;
      v_VB_ID       NUMBER;
      v_VS_ID       NUMBER;
      v_SL          NUMBER;
      v_Lnk_A       NUMBER;
      v_Lnk_S       NUMBER;
      v_Lnk_ID      NUMBER;
      v_LnkID       NUMBER;
      v_SaleLotID   NUMBER;
      v_SaleVrtType NUMBER;
      v_SaleRID     NUMBER;
      v_stat        NUMBER;
      v_SaleLotType NUMBER;
      v_LCS_ID      NUMBER;
      v_LotDealCode dsctxlot_dbt.t_DealCode%TYPE;
      TYPE LNKCurTyp IS REF CURSOR;
      LNK_cur LNKCurTyp;

    BEGIN
      v_stat := 0;

      TXPutMsg( v_CLot.t_ID,
                v_CLot.t_FIID,
                TXMES_DEBUG,
                'Вызов TXProcessCompPayOnReverseRepo CLot.t_ID = '||v_CLot.t_ID||', CLot.t_BuyDate = '||v_CLot.t_BuyDate);
      TXPutMsg( v_CLot.t_ID,
                v_CLot.t_FIID,
                TXMES_OPTIM,
                'Вызов TXProcessCompPayOnReverseRepo CLot.t_ID = '||v_CLot.t_ID||', CLot.t_BuyDate = '||v_CLot.t_BuyDate);

      SELECT MIN(t_ID) INTO v_RLotID
        FROM dsctxlot_dbt
       WHERE t_ChildID = v_CLot.t_ID;

      UPDATE dsctxlot_dbt
         SET t_SaleDate = v_CLot.t_BuyDate,
             t_RetFlag  = CHR(88)
       WHERE t_ID = v_RLotID;

      SELECT * INTO v_RLot
        FROM dsctxlot_dbt
       WHERE t_ID = v_RLotID;

      UPDATE dsctxlot_dbt
         SET t_InAcc = CHR(88)
       WHERE t_ID = v_CLot.t_ID;

      IF v_RLot.t_Type = RSB_SCTXC.TXLOTS_BACKREPO THEN
         v_OSPL := RSB_SCTXC.TXLNK_OPSREPO;
         v_CSPL := RSB_SCTXC.TXLNK_CLSREPO;
      ELSE
         v_OSPL := RSB_SCTXC.TXLNK_OPSLOAN;
         v_CSPL := RSB_SCTXC.TXLNK_CLSLOAN;
      END IF;

      v_S := iif( v_RLot.t_Sale < v_CLot.t_Amount, v_RLot.t_Sale, v_CLot.t_Amount );

      IF( v_S = 0 ) THEN
        TXPutMsg( v_CLot.t_ID,
                  v_CLot.t_FIID,
                  TXMES_DEBUG,
                  'Конец1 TXProcessCompPayOnReverseRepo CLot.t_ID = '||v_CLot.t_ID||', CLot.t_BuyDate = '||v_CLot.t_BuyDate);
        RETURN; -- все связали, выходим
      END IF;

      --закрываем короткие позиции по продаже
      IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
        OPEN LNK_cur FOR SELECT lnk.t_Amount, lnk.t_Short, lnk.t_ID LinkID,
                                salelot.t_ID, salelot.t_VirtualType, salelot.t_RealID
                           FROM dsctxlnk_dbt lnk, dsctxlot_dbt salelot
                          WHERE lnk.t_BuyID  = v_RLot.t_ID
                            AND lnk.t_Type   = v_OSPL
                            AND lnk.t_SaleID = salelot.t_ID
                            AND (lnk.t_Amount - lnk.t_Short) > 0
                       ORDER BY salelot.t_BegSaleDate DESC,
                                salelot.t_DealDate DESC,
                                salelot.t_DealTime DESC,
                                salelot.t_DealSort DESC;
      END IF;

      WHILE v_S > 0 and v_stat = 0 LOOP

        FETCH LNK_cur INTO v_Lnk_A, v_Lnk_S, v_Lnk_ID, v_SaleLotID, v_SaleVrtType, v_SaleRID;
        EXIT WHEN LNK_cur%NOTFOUND OR
                  LNK_cur%NOTFOUND IS NULL;

        BEGIN

          SELECT * INTO v_SaleLot
            FROM dsctxlot_dbt
           WHERE t_ID = v_SaleLotID;

          v_SPS := v_Lnk_A  - v_Lnk_S;
          v_A   := iif( v_SPS < v_S, v_SPS, v_S );

          SELECT NVL(MAX(TO_NUMBER(SUBSTR(t_DealCodeTS, INSTR(t_DealCodeTS, '/') + 1))), 0) + 1
            INTO v_VirtualNum
            FROM dsctxlot_dbt
           WHERE ( (t_SaleDate = v_RLot.t_SaleDate AND t_Type = RSB_SCTXC.TXLOTS_SALE) OR
                   (t_BuyDate  = v_RLot.t_SaleDate AND t_Type = RSB_SCTXC.TXLOTS_BUY) )
             AND (t_VirtualType = RSB_SCTXC.TXVDEAL_MARKETCOST OR
                  t_VirtualType = RSB_SCTXC.TXVDEAL_ZEROCOST OR
                  t_VirtualType = RSB_SCTXC.TXVDEAL_CALC
                 );

          IF v_SaleVrtType = RSB_SCTXC.TXVDEAL_REAL THEN
             v_RealID := v_SaleLotID;
          ELSE
             v_RealID := v_SaleRID;
          END IF;
          v_LotDealCode := TXGenVirtNum( 0, v_RLot.t_SaleDate, v_VirtualNum );

          -- Вставить запись DSCTXLOT VB
          INSERT INTO dsctxlot_dbt( t_DealID     ,
                                    t_FIID       ,
                                    t_TaxGroup   ,
                                    t_Type       ,
                                    t_VirtualType,
                                    t_DealCode   ,
                                    t_DealCodeTS ,
                                    t_BuyID      ,
                                    t_RealID     ,
                                    t_DealDate   ,
                                    t_DealTime   ,
                                    t_SaleDate   ,
                                    t_BegSaleDate,
                                    t_BuyDate    ,
                                    t_BegBuyDate ,
                                    t_RetrDate   ,
                                    t_Amount     ,
                                    t_NettingID  ,
                                    t_Price      ,
                                    t_PriceFIID  ,
                                    t_PriceCUR   ,
                                    t_OldType    ,
                                    t_Netting    ,
                                    t_Sale       ,
                                    t_RetFlag    ,
                                    t_IsFree     ,
                                    t_OrdForSale ,
                                    t_OrdForRepo ,
                                    t_BegLotID   ,
                                    t_ChildID    ,
                                    t_IsComp     ,
                                    t_DealSortCode
                                  )
                            VALUES(
                                    0,                                     --t_DealID
                                    v_RLot.t_FIID,                         --t_FIID
                                    v_RLot.t_TaxGroup,                     --t_TaxGroup
                                    RSB_SCTXC.TXLOTS_BUY,                  --t_Type
                                    RSB_SCTXC.TXVDEAL_CALC,                --t_VirtualType
                                    v_LotDealCode,                         --t_DealCode
                                    v_LotDealCode,                         --t_DealCodeTS
                                    0,                                     --t_BuyID
                                    v_RealID,                              --t_RealID
                                    v_SaleLot.t_DealDate,                  --t_DealDate
                                    v_SaleLot.t_DealTime,                  --t_DealTime
                                    TO_DATE('01.01.0001', 'DD.MM.YYYY'),   --t_SaleDate
                                    TO_DATE('01.01.0001', 'DD.MM.YYYY'),   --t_BegSaleDate
                                    v_RLot.t_SaleDate,                     --t_BuyDate
                                    v_SaleLot.t_BegBuyDate,                --t_BegBuyDate
                                    TO_DATE('01.01.0001', 'DD.MM.YYYY'),   --t_RetrDate
                                    v_A,                                   --t_Amount
                                    0,                                     --t_NettingID
                                    v_SaleLot.t_Price,                     --t_Price
                                    v_SaleLot.t_PriceFIID,                 --t_PriceFIID
                                    v_SaleLot.t_PriceCUR,                  --t_PriceCUR
                                    RSB_SCTXC.TXLOTS_UNDEF,                --t_OldType
                                    0,                                     --t_Netting
                                    0,                                     --t_Sale
                                    CHR(0),                                --t_RetFlag
                                    CHR(0),                                --t_IsFree
                                    0,                                     --t_OrdForSale
                                    0,                                     --t_OrdForRepo
                                    v_SaleLot.t_BegLotID,                  --t_BegLotID
                                    0,                                     --t_ChildID
                                    CHR(0),                                --t_IsComp
                                    v_SaleLot.t_DealSortCode               --t_DealSortCode
                                  ) RETURNING t_ID INTO v_VB_ID;

          v_LotDealCode := TXGenVirtNum( 1, v_RLot.t_SaleDate, v_VirtualNum + 1 );

          -- Вставить запись DSCTXLOT VS
          INSERT INTO dsctxlot_dbt( t_DealID     ,
                                    t_FIID       ,
                                    t_TaxGroup   ,
                                    t_Type       ,
                                    t_VirtualType,
                                    t_DealCode   ,
                                    t_DealCodeTS ,
                                    t_BuyID      ,
                                    t_RealID     ,
                                    t_DealDate   ,
                                    t_DealTime   ,
                                    t_SaleDate   ,
                                    t_BegSaleDate,
                                    t_BuyDate    ,
                                    t_BegBuyDate ,
                                    t_RetrDate   ,
                                    t_Amount     ,
                                    t_NettingID  ,
                                    t_Price      ,
                                    t_PriceFIID  ,
                                    t_PriceCUR   ,
                                    t_OldType    ,
                                    t_Netting    ,
                                    t_Sale       ,
                                    t_RetFlag    ,
                                    t_IsFree     ,
                                    t_OrdForSale ,
                                    t_OrdForRepo ,
                                    t_BegLotID   ,
                                    t_ChildID    ,
                                    t_IsComp     ,
                                    t_DealSortCode
                                  )
                            VALUES(
                                    0,                                    --t_DealID
                                    v_RLot.t_FIID,                        --t_FIID
                                    v_RLot.t_TaxGroup,                    --t_TaxGroup
                                    RSB_SCTXC.TXLOTS_SALE,                --t_Type
                                    RSB_SCTXC.TXVDEAL_CALC,               --t_VirtualType
                                    v_LotDealCode,                        --t_DealCode
                                    v_LotDealCode,                        --t_DealCodeTS
                                    v_VB_ID,                              --t_BuyID
                                    v_RealID,                             --t_RealID
                                    v_SaleLot.t_DealDate,                 --t_DealDate
                                    v_SaleLot.t_DealTime,                 --t_DealTime
                                    v_RLot.t_SaleDate,                    --t_SaleDate
                                    v_SaleLot.t_BegSaleDate,              --t_BegSaleDate
                                    TO_DATE('01.01.0001', 'DD.MM.YYYY'),  --t_BuyDate
                                    TO_DATE('01.01.0001', 'DD.MM.YYYY'),  --t_BegBuyDate
                                    TO_DATE('01.01.0001', 'DD.MM.YYYY'),  --t_RetrDate
                                    v_A,                                  --t_Amount
                                    0,                                    --t_NettingID
                                    v_SaleLot.t_Price,                    --t_Price
                                    v_SaleLot.t_PriceFIID,                --t_PriceFIID
                                    v_SaleLot.t_PriceCUR,                 --t_PriceCUR
                                    RSB_SCTXC.TXLOTS_UNDEF,               --t_OldType
                                    0,                                    --t_Netting
                                    0,                                    --t_Sale
                                    CHR(0),                               --t_RetFlag
                                    CHR(0),                               --t_IsFree
                                    0,                                    --t_OrdForSale
                                    0,                                    --t_OrdForRepo
                                    v_SaleLot.t_BegLotID,                 --t_BegLotID
                                    0,                                    --t_ChildID
                                    CHR(0),                               --t_IsComp
                                    v_SaleLot.t_DealSortCode              --t_DealSortCode
                                  ) RETURNING t_ID INTO v_VS_ID;

          RSI_TXDealSortOnDate(v_RLot.t_FIID, v_SaleLot.t_DealDate, v_SaleLot.t_DealTime, v_VB_ID);

          INSERT INTO dsctxlnk_dbt( t_SaleID,
                                    t_BuyID,
                                    t_Type,
                                    t_SourceID,
                                    t_DestID,
                                    t_Lot1ID,
                                    t_Lot2ID,
                                    t_Date,
                                    t_Short,
                                    t_Ret,
                                    t_Ret2,
                                    t_RetSP,
                                    t_BegDate,
                                    t_EndDate,
                                    t_Amount,
                                    t_RetFlag, t_FIID)
                            VALUES( v_SaleLotID,
                                    v_VB_ID,
                                    v_CSPL,
                                    v_RLot.t_ID,
                                    0,
                                    0,
                                    0,
                                    v_RLot.t_SaleDate,
                                    0,
                                    0,
                                    0,
                                    0,
                                    TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                    TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                    v_A,
                                    CHR(0), v_RLot.t_FIID ) RETURNING t_ID INTO v_LCS_ID;

          INSERT INTO dsctxls_dbt( t_ChildID,
                                   t_ParentID,
                                   t_Short)
                           VALUES( v_LCS_ID,
                                   v_Lnk_ID,
                                   v_A );

          INSERT INTO dsctxlnk_dbt( t_SaleID,
                                    t_BuyID,
                                    t_Type,
                                    t_SourceID,
                                    t_DestID,
                                    t_Lot1ID,
                                    t_Lot2ID,
                                    t_Date,
                                    t_Short,
                                    t_Ret,
                                    t_Ret2,
                                    t_RetSP,
                                    t_BegDate,
                                    t_EndDate,
                                    t_Amount,
                                    t_RetFlag, t_FIID)
                            VALUES( v_VS_ID,
                                    v_CLot.t_ID,
                                    v_OSPL,
                                    0,
                                    0,
                                    0,
                                    0,
                                    v_RLot.t_SaleDate,
                                    0,
                                    0,
                                    0,
                                    0,
                                    TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                    TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                    v_A,
                                    CHR(0), v_RLot.t_FIID );

          v_S := v_S - v_A;
        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
              BEGIN
                v_stat := 1;
              END;
        END;
      END LOOP;
      CLOSE LNK_cur;

      IF( v_S = 0 ) THEN
        TXPutMsg( v_CLot.t_ID,
                  v_CLot.t_FIID,
                  TXMES_DEBUG,
                  'Конец2 TXProcessCompPayOnReverseRepo CLot.t_ID = '||v_CLot.t_ID||', CLot.t_BuyDate = '||v_CLot.t_BuyDate);
        RETURN; -- все связали, выходим
      END IF;

      --иначе формируем связи по подстановкам
      v_stat := 0;

      IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
        OPEN LNK_cur FOR SELECT lnk.t_Amount - lnk.t_Short, salelot.t_ID, salelot.t_Type, lnk.t_ID
                           FROM dsctxlnk_dbt lnk, dsctxlot_dbt salelot
                          WHERE lnk.t_BuyID  = v_RLot.t_ID
                            AND lnk.t_Type   IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_LOANPUT, RSB_SCTXC.TXLNK_SUBSTREPO, RSB_SCTXC.TXLNK_SUBSTLOAN)
                            AND lnk.t_SaleID = salelot.t_ID
                            AND (lnk.t_Amount - lnk.t_Short) > 0
                            AND (salelot.t_BuyDate > v_RLot.t_SaleDate OR salelot.t_BuyDate = TO_DATE('01-01-0001','DD-MM-YYYY'))
                       ORDER BY salelot.t_BegSaleDate ASC,
                                salelot.t_DealDate ASC,
                                salelot.t_DealTime ASC,
                                salelot.t_DealSort ASC;
      END IF;

      WHILE v_S > 0 and v_stat = 0 LOOP
        FETCH LNK_cur INTO v_SPS, v_SaleLotID, v_SaleLotType, v_Lnk_ID;
        EXIT WHEN LNK_cur%NOTFOUND OR
                  LNK_cur%NOTFOUND IS NULL;

        BEGIN
          v_A   := iif( v_SPS < v_S, v_SPS, v_S );

          IF v_SaleLotType = RSB_SCTXC.TXLOTS_REPO THEN
            v_SL := RSB_SCTXC.TXLNK_SUBSTREPO;
          ELSE
            v_SL := RSB_SCTXC.TXLNK_SUBSTLOAN;
          END IF;

          BEGIN
            SELECT t_ID
              INTO v_LnkID
              FROM dsctxlnk_dbt
             WHERE t_Type = v_SL
               AND t_BuyID = v_CLot.t_ID
               AND t_SaleID = v_SaleLotID
               AND t_SourceID = 0
               AND t_DestID = 0
               AND t_Lot1ID = v_RLot.t_ID
               AND t_Lot2ID = 0
               AND t_DATE = v_RLot.t_SaleDate;

            UPDATE dsctxlnk_dbt
               SET t_Amount = t_Amount + v_A
             WHERE t_ID = v_LnkID;

            INSERT INTO dsctxls_dbt( t_ChildID,
                                     t_ParentID,
                                     t_Short)
                             VALUES( v_LnkID,
                                     v_Lnk_ID,
                                     v_A );

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN
                INSERT INTO dsctxlnk_dbt( t_SaleID,
                                          t_BuyID,
                                          t_Type,
                                          t_SourceID,
                                          t_DestID,
                                          t_Lot1ID,
                                          t_Lot2ID,
                                          t_Date,
                                          t_Short,
                                          t_Ret,
                                          t_Ret2,
                                          t_RetSP,
                                          t_BegDate,
                                          t_EndDate,
                                          t_Amount,
                                          t_RetFlag, t_FIID )
                                 VALUES ( v_SaleLotID,
                                          v_CLot.t_ID,
                                          v_SL,
                                          0,
                                          0,
                                          v_RLot.t_ID,
                                          0,
                                          v_RLot.t_SaleDate,
                                          0,
                                          0,
                                          0,
                                          0,
                                          TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                          TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                          v_A,
                                          CHR(0), v_RLot.t_FIID ) RETURNING t_ID INTO v_LCS_ID;

                INSERT INTO dsctxls_dbt( t_ChildID,
                                         t_ParentID,
                                         t_Short)
                                 VALUES( v_LCS_ID,
                                         v_Lnk_ID,
                                         v_A );

              END;
          END;

          v_S := v_S - v_A;
        END;

      END LOOP;
      CLOSE LNK_cur;

      TXPutMsg( v_CLot.t_ID,
                v_CLot.t_FIID,
                TXMES_DEBUG,
                'Конец3 TXProcessCompPayOnReverseRepo CLot.t_ID = '||v_CLot.t_ID||', CLot.t_BuyDate = '||v_CLot.t_BuyDate);
    END; --TXProcessCompPayOnReverseRepo

  ----Выполняет закрытие незакрытых коротких позиций
    PROCEDURE TXCloseShortPos( in_CloseDate IN DATE, in_FIID IN NUMBER )
    IS
      v_Break       BOOLEAN;
      v_FIID        NUMBER;
      v_LinkID      NUMBER;
      v_SPS         NUMBER;
      v_SaleLotID   NUMBER;
      v_lotID       NUMBER;
      v_BuyLot_ID   NUMBER;
      v_FreeAmount  NUMBER;
      v_A           NUMBER;
      v_BuyLot_Type NUMBER;
      v_LCS_ID      NUMBER;

      TYPE OSPLNKCurTyp IS REF CURSOR;
      OSPLNK_cur OSPLNKCurTyp;

    BEGIN

      TXPutMsg( 0,
                in_FIID,
                TXMES_DEBUG,
                'Вызов TXCloseShortPos на дату CloseDate = '||in_CloseDate );
      TXPutMsg( 0,
                in_FIID,
                TXMES_OPTIM,
                'Вызов TXCloseShortPos на дату CloseDate = '||in_CloseDate );

      -- формируем связи по подстановкам
      IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
        OPEN OSPLNK_cur FOR SELECT (lnk.t_Amount - lnk.t_Short) SPS, salelot.t_FIID FIID, lnk.t_ID LinkID,
                                   salelot.t_ID, lot.t_ID, lot.t_Type
                              FROM dsctxlnk_dbt lnk, dsctxlot_dbt salelot, dsctxlot_dbt lot
                             WHERE lnk.t_Type IN (RSB_SCTXC.TXLNK_OPSREPO, RSB_SCTXC.TXLNK_OPSLOAN)
                               AND salelot.t_ID = lnk.t_SaleID
                               AND lot.t_ID = lnk.t_BuyID
                               AND (lnk.t_Amount - lnk.t_Short) > 0
                               AND salelot.t_SaleDate <= in_CloseDate
                               AND salelot.t_FIID = in_FIID
                          ORDER BY lot.t_OrdForClPosRepo ASC,
                                   salelot.t_BegSaleDate ASC,
                                   salelot.t_DealDate ASC,
                                   salelot.t_DealTime ASC,
                                   salelot.t_DealSort ASC,
                                   lot.t_BegBuyDate ASC,
                                   lot.t_DealDate ASC,
                                   lot.t_DealTime ASC,
                                   lot.t_DealSort ASC;
      END IF;

      LOOP

        FETCH OSPLNK_cur INTO v_SPS, v_FIID, v_LinkID, v_SaleLotID, v_lotID, v_BuyLot_Type;
        EXIT WHEN OSPLNK_cur%NOTFOUND OR
                  OSPLNK_cur%NOTFOUND IS NULL;

        v_Break := FALSE;

        WHILE v_SPS > 0 LOOP
          BEGIN
            IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
              -- если ФИФО
              SELECT *
                INTO v_BuyLot_ID, v_FreeAmount
                FROM (  SELECT q1.t_ID, (q1.t_Amount - q1.t_Netting - q1.t_Sale) FreeAmount
                          FROM (SELECT                   /*+ INDEX (Buy DSCTXLOT_DBT_IDXA)*/
                                      Buy.*,
                                       CASE
                                          WHEN Rsb_SCTX.GetInAvrWrtStartDate (tick.t_DealID) =
                                                  TO_DATE ('31-12-9999', 'DD-MM-YYYY')
                                          THEN
                                             Buy.t_DealDate
                                          ELSE
                                             Rsb_SCTX.GetInAvrWrtStartDate (tick.t_DealID)
                                       END
                                          AS t_wrtstart
                                  FROM dsctxlot_dbt Buy, ddl_tick_dbt tick
                                 WHERE     Buy.t_FIID = v_FIID
                                       AND Buy.t_BuyDate <= in_CloseDate
                                       AND Buy.t_IsFree = CHR (88)
                                       AND Buy.t_Type = RSB_SCTXC.TXLOTS_BUY      -- покупка
                                       AND tick.t_DealID = Buy.t_DealID
                                       AND ( (tick.t_Ofbu = 'X'
                                              AND in_CloseDate >=
                                                     GetInAvrWrtStartDate (tick.t_DealID))
                                            OR tick.t_Ofbu <> 'X')) q1
                      ORDER BY q1.t_BegBuyDate ASC,
                               q1.t_DealDate ASC,
                               q1.t_wrtstart ASC,
                               q1.t_DealTime ASC,
                               q1.t_DealSort ASC)
               WHERE ROWNUM = 1;
            END IF;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN
                v_Break := TRUE;
              END;
          END;

          EXIT WHEN v_Break;

          v_A := iif( v_SPS < v_FreeAmount, v_SPS, v_FreeAmount );

          INSERT INTO dsctxlnk_dbt( t_SaleID,
                                    t_BuyID,
                                    t_Type,
                                    t_SourceID,
                                    t_DestID,
                                    t_Lot1ID,
                                    t_Lot2ID,
                                    t_Date,
                                    t_Short,
                                    t_Ret,
                                    t_Ret2,
                                    t_RetSP,
                                    t_BegDate,
                                    t_EndDate,
                                    t_Amount,
                                    t_RetFlag, t_FIID )
                           VALUES ( v_SaleLotID,
                                    v_BuyLot_ID,
                                    DECODE(v_BuyLot_Type, RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLNK_CLSREPO, RSB_SCTXC.TXLNK_CLSLOAN),
                                    v_lotID,
                                    0,
                                    0,
                                    0,
                                    in_CloseDate,
                                    0,
                                    0,
                                    0,
                                    0,
                                    TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                    TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                    v_A,
                                    CHR(0), v_FIID ) RETURNING t_ID INTO v_LCS_ID;

          INSERT INTO dsctxls_dbt( t_ChildID,
                                   t_ParentID,
                                   t_Short)
                           VALUES( v_LCS_ID,
                                   v_LinkID,
                                   v_A );
          v_SPS := v_SPS - v_A;

        END LOOP;

      END LOOP;

      CLOSE OSPLNK_cur;

      TXPutMsg( 0,
                in_FIID,
                TXMES_DEBUG,
                'Конец TXCloseShortPos на дату CloseDate = '||in_CloseDate );
    END; --TXCloseShortPos

  ----Выполняет перетасовку покупок
    PROCEDURE TXShuffling( in_CalcDate IN DATE, in_FIID IN NUMBER )
    IS
      v_Break       BOOLEAN;
      v_FIID        NUMBER;
      v_LinkID      NUMBER;
      v_LnkID       NUMBER;
      v_SPS         NUMBER;
      v_SaleLotID   NUMBER;
      v_lotID       NUMBER;
      v_BuyLot_ID   NUMBER;
      v_FreeAmount  NUMBER;
      v_A           NUMBER;
      v_BuyLot_Type NUMBER;
      v_Count       NUMBER;
      v_lotBegBuyDate DATE;
      v_SaleLotType NUMBER;
      v_SL          NUMBER;
      v_LCS_ID      NUMBER;
      v_lotDealDate dsctxlot_dbt.t_DealDate%TYPE;
      v_lotDealTime dsctxlot_dbt.t_DealTime%TYPE;
      v_lotDealSort dsctxlot_dbt.t_DealSort%TYPE;
      v_DealID      NUMBER;

      TYPE OSPLNKCurTyp IS REF CURSOR;
      OSPLNK_cur OSPLNKCurTyp;

    BEGIN

      TXPutMsg( 0,
                in_FIID,
                TXMES_DEBUG,
                'Вызов TXShuffling на дату CalcDate = '||in_CalcDate );
      TXPutMsg( 0,
                in_FIID,
                TXMES_OPTIM,
                'Вызов TXShuffling на дату CalcDate = '||in_CalcDate );

      -- формируем связи по подстановкам
      IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
        OPEN OSPLNK_cur FOR SELECT (lnk.t_Amount - lnk.t_Short) SPS, salelot.t_FIID FIID, lnk.t_ID LinkID,
                                   salelot.t_ID, lot.t_ID, lot.t_BegBuyDate, salelot.t_Type,
                                   lot.t_DealDate, lot.t_DealTime, lot.t_DealSort, lot.t_DealID
                              FROM dsctxlnk_dbt lnk, dsctxlot_dbt salelot, dsctxlot_dbt lot
                             WHERE lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO  , RSB_SCTXC.TXLNK_LOANPUT,
                                                  RSB_SCTXC.TXLNK_SUBSTREPO, RSB_SCTXC.TXLNK_SUBSTLOAN)
                               AND salelot.t_ID = lnk.t_SaleID
                               AND lot.t_ID = lnk.t_BuyID
                               AND lot.t_Type = RSB_SCTXC.TXLOTS_BUY
                               AND lnk.t_RetFlag = CHR(0)
                               AND (lnk.t_Amount - lnk.t_Short) > 0
                               AND salelot.t_FIID = in_FIID
                               AND (lnk.t_Date < in_CalcDate or
                                    (lnk.t_Date = in_CalcDate and
                                     salelot.t_IsComp = CHR(88) and
                                     salelot.t_type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT) and
                                     salelot.t_SaleDate = in_CalcDate and
                                     salelot.t_SaleDate <> salelot.t_BuyDate
                                    )
                                   )
                          ORDER BY lot.t_BegBuyDate ASC,
                                   lot.t_DealDate ASC,
                                   lot.t_DealTime ASC,
                                   lot.t_DealSort ASC,
                                   salelot.t_BegSaleDate DESC,
                                   salelot.t_DealDate DESC,
                                   salelot.t_DealTime DESC,
                                   salelot.t_DealSort DESC;
      END IF;

      LOOP

        FETCH OSPLNK_cur INTO v_SPS, v_FIID, v_LinkID, v_SaleLotID, v_lotID, v_lotBegBuyDate, v_SaleLotType,
                              v_lotDealDate, v_lotDealTime, v_lotDealSort, v_DealID;
        EXIT WHEN OSPLNK_cur%NOTFOUND OR
                  OSPLNK_cur%NOTFOUND IS NULL;

        v_Break := FALSE;

        WHILE v_SPS > 0 LOOP
          BEGIN
            IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
              -- если ФИФО
              SELECT *
                INTO v_BuyLot_ID, v_BuyLot_Type, v_FreeAmount
                FROM ( SELECT /*+ INDEX (Buy DSCTXLOT_DBT_IDXF)*/
                              Buy.t_ID, Buy.t_Type,
                              (Buy.t_Amount - Buy.t_Netting - Buy.t_Sale) FreeAmount
                         FROM dsctxlot_dbt Buy
                        WHERE Buy.t_FIID = v_FIID
                          AND (Buy.t_SaleDate = TO_DATE('01.01.0001','DD.MM.YYYY') OR Buy.t_SaleDate > in_CalcDate)
                          AND Buy.t_IsFree = CHR(88)
                          AND Buy.t_ID <> v_lotID
                          AND Buy.t_DealID <> v_DealID
                          AND (Buy.t_Type <> RSB_SCTXC.TXLOTS_BUY OR
                               (Buy.t_BegBuyDate > v_lotBegBuyDate) OR
                               (Buy.t_BegBuyDate = v_lotBegBuyDate AND
                                (Buy.t_DealDate > v_lotDealDate OR
                                 (Buy.t_DealDate = v_lotDealDate AND
                                  (Buy.t_DealTime > v_lotDealTime OR
                                   (Buy.t_DealTime = v_lotDealTime AND
                                    (Buy.t_DealSort > v_lotDealSort
                                    )
                                   )
                                  )
                                 )
                                )
                               )
                              )
                          AND Buy.t_BuyDate <= in_CalcDate
                     ORDER BY Buy.t_OrdForSubst ASC,
                              Buy.t_BegBuyDate DESC,
                              Buy.t_DealDate DESC,
                              Buy.t_DealTime DESC,
                              Buy.t_DealSort DESC )
               WHERE ROWNUM = 1;
            END IF;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN
                v_Break := TRUE;
              END;
          END;

          EXIT WHEN v_Break;

          v_A := iif( v_SPS < v_FreeAmount, v_SPS, v_FreeAmount );

          IF v_SaleLotType = RSB_SCTXC.TXLOTS_REPO THEN
             v_SL := RSB_SCTXC.TXLNK_SUBSTREPO;
          ELSE
             v_SL := RSB_SCTXC.TXLNK_SUBSTLOAN;
          END IF;

          BEGIN
            SELECT t_ID
              INTO v_LnkID
              FROM dsctxlnk_dbt
             WHERE t_Type = v_SL
               AND t_BuyID = v_BuyLot_ID
               AND t_SaleID = v_SaleLotID
               AND t_SourceID = 0
               AND t_DestID = 0
               AND t_Lot1ID = v_lotID
               AND t_Lot2ID = 0
               AND t_DATE = in_CalcDate;

            UPDATE dsctxlnk_dbt
               SET t_Amount = t_Amount + v_A
             WHERE t_ID = v_LnkID;

            INSERT INTO dsctxls_dbt( t_ChildID,
                                     t_ParentID,
                                     t_Short)
                             VALUES( v_LnkID,
                                     v_LinkID,
                                     v_A );

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN
                INSERT INTO dsctxlnk_dbt( t_SaleID,
                                          t_BuyID,
                                          t_Type,
                                          t_SourceID,
                                          t_DestID,
                                          t_Lot1ID,
                                          t_Lot2ID,
                                          t_Date,
                                          t_Short,
                                          t_Ret,
                                          t_Ret2,
                                          t_RetSP,
                                          t_BegDate,
                                          t_EndDate,
                                          t_Amount,
                                          t_RetFlag, t_FIID )
                                 VALUES ( v_SaleLotID,
                                          v_BuyLot_ID,
                                          v_SL,
                                          0,
                                          0,
                                          v_lotID,
                                          0,
                                          in_CalcDate,
                                          0,
                                          0,
                                          0,
                                          0,
                                          TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                          TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                                          v_A,
                                          CHR(0), v_FIID ) RETURNING t_ID INTO v_LCS_ID;

                INSERT INTO dsctxls_dbt( t_ChildID,
                                         t_ParentID,
                                         t_Short)
                                 VALUES( v_LCS_ID,
                                         v_LinkID,
                                         v_A );

              END;
          END;

          v_SPS := v_SPS - v_A;

        END LOOP;

      END LOOP;

      CLOSE OSPLNK_cur;

      TXPutMsg( 0,
                in_FIID,
                TXMES_DEBUG,
                'Конец TXShuffling на дату CalcDate = '||in_CalcDate );
    END; --TXShuffling

    --Вставляет или корректирует запись остатка и списания
    PROCEDURE RSI_TXCorrectRest (p_Type    IN NUMBER, p_SaleID IN NUMBER, p_SourceID IN NUMBER,
                             p_BuyDate IN DATE,   p_SaleDate IN DATE, p_CorrectDate IN DATE,
                             p_FIID    IN NUMBER, p_Amount IN NUMBER, in_IsTrigger IN BOOLEAN DEFAULT FALSE)
    IS
      v_RestID NUMBER;
    BEGIN

      TXPutMsg( 0,
                -1,
                TXMES_DEBUG,
                'Вызов RSI_TXCorrectRest: p_Type = '||p_Type||
                ', p_SaleID = '||p_SaleID||', p_SourceID = '||p_SourceID||
                ', p_BuyDate = '||p_BuyDate||', p_SaleDate = '||p_SaleDate||
                ', p_CorrectDate = '||p_CorrectDate||
                ', p_FIID = '||p_FIID||', p_Amount = '||p_Amount,
                in_IsTrigger );

      IF p_Amount = 0 THEN
         TXPutMsg( 0,
                   -1,
                   TXMES_DEBUG,
                   'Конец1 RSI_TXCorrectRest: p_Type = '||p_Type||
                   ', p_SaleID = '||p_SaleID||', p_SourceID = '||p_SourceID||
                   ', p_BuyDate = '||p_BuyDate||', p_SaleDate = '||p_SaleDate||
                   ', p_CorrectDate = '||p_CorrectDate||
                   ', p_FIID = '||p_FIID||', p_Amount = '||p_Amount,
                   in_IsTrigger );
         RETURN;
      END IF;

      BEGIN
        SELECT t_ID
          INTO v_RestID
          FROM ( SELECT /*+ INDEX (Rest INDEX DSCTXREST_DBT_IDXA)*/
                        Rest.t_ID
                   FROM dsctxrest_dbt Rest
                  WHERE Rest.t_SourceID  = p_SourceID
                    AND Rest.T_BUYDATE   = p_BuyDate
                    AND Rest.t_Type = p_Type
                    AND (Rest.t_SaleID = 0 OR Rest.t_SaleID = p_SaleID)
                    AND (Rest.T_SALEDATE = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR Rest.T_SALEDATE = p_SaleDate)
               ORDER BY Rest.t_ID ASC )
         WHERE ROWNUM = 1;

        UPDATE dsctxrest_dbt
           SET t_Amount = t_Amount + p_Amount,
               t_ChangeDate = p_CorrectDate
         WHERE t_ID = v_RestID;

        TXPutMsg( 0,
                  -1,
                  TXMES_DEBUG,
                  'RSI_TXCorrectRest: после апдейта остатка t_ID = '||v_RestID,
                  in_IsTrigger );

      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          BEGIN
            INSERT INTO dsctxrest_dbt( t_Type       ,
                                       t_Amount     ,
                                       t_SaleID     ,
                                       t_SourceID   ,
                                       t_BuyDate    ,
                                       t_SaleDate   ,
                                       t_FIID       ,
                                       t_CreateDate ,
                                       t_ChangeDate
                                     )
                               VALUES(
                                       p_Type,
                                       p_Amount,
                                       p_SaleID,
                                       p_SourceID,
                                       p_BuyDate,
                                       p_SaleDate,
                                       p_FIID,
                                       p_CorrectDate,
                                       p_CorrectDate
                                     ) RETURNING t_ID INTO v_RestID;
            TXPutMsg( 0,
                      -1,
                      TXMES_DEBUG,
                      'RSI_TXCorrectRest: после вставки остатка t_ID = '||v_RestID,
                      in_IsTrigger );
          END;
      END;

      TXPutMsg( 0,
                -1,
                TXMES_DEBUG,
                'Конец2 RSI_TXCorrectRest: p_Type = '||p_Type||
                ', p_SaleID = '||p_SaleID||', p_SourceID = '||p_SourceID||
                ', p_BuyDate = '||p_BuyDate||', p_SaleDate = '||p_SaleDate||
                ', p_CorrectDate = '||p_CorrectDate||
                ', p_FIID = '||p_FIID||', p_Amount = '||p_Amount,
                in_IsTrigger );
    END; --RSI_TXCorrectRest

    --Выполняет обработку остатков и списаний при выбытии 2 ч ОР/ПЗ
    PROCEDURE RSI_TXUpdatePart2Rest (p_SourceID IN NUMBER, p_SaleDate IN DATE)
    IS
      CURSOR cBU IS
                    SELECT BU.t_Amount, BU.t_BuyDate, BU.t_FIID, BU.t_ID
                      FROM DSCTXREST_DBT BU
                     WHERE BU.T_SOURCEID = p_SourceID
                       AND BU.T_TYPE     = Rsb_SCTXC.TXREST_B_U
                       AND BU.T_AMOUNT   > 0;

      CURSOR cDRU IS
                    SELECT DRU.t_Amount, DRU.t_BuyDate, DRU.t_FIID, DRU.t_ID
                      FROM DSCTXREST_DBT DRU
                     WHERE DRU.T_SOURCEID = p_SourceID
                       AND DRU.T_TYPE     = Rsb_SCTXC.TXREST_DR_U
                       AND DRU.T_AMOUNT   > 0;
    BEGIN

      TXPutMsg( 0,
                -1,
                TXMES_DEBUG,
                'Вызов RSI_TXUpdatePart2Rest p_SourceID = '||p_SourceID||', p_SaleDate = '||p_SaleDate );
      TXPutMsg( 0,
                -1,
                TXMES_OPTIM,
                'Вызов RSI_TXUpdatePart2Rest p_SourceID = '||p_SourceID||', p_SaleDate = '||p_SaleDate );

      FOR BU IN cBU LOOP
         RSI_TXCorrectRest (Rsb_SCTXC.TXREST_B_S, 0, p_SourceID, BU.t_BuyDate,
                        p_SaleDate, p_SaleDate, BU.t_FIID, BU.t_Amount);

         UPDATE DSCTXREST_DBT RBU
            SET RBU.T_AMOUNT = 0,
                RBU.T_CHANGEDATE = p_SaleDate
          WHERE RBU.T_ID = BU.T_ID;

      END LOOP;

      FOR DRU IN cDRU LOOP
         RSI_TXCorrectRest (Rsb_SCTXC.TXREST_DR_S, 0, p_SourceID, DRU.t_BuyDate,
                        p_SaleDate, p_SaleDate, DRU.t_FIID, DRU.t_Amount);

         UPDATE DSCTXREST_DBT RDRU
            SET RDRU.T_AMOUNT = 0,
                RDRU.T_CHANGEDATE = p_SaleDate
          WHERE RDRU.T_ID = DRU.T_ID;
      END LOOP;

      TXPutMsg( 0,
                -1,
                TXMES_DEBUG,
                'Конец RSI_TXUpdatePart2Rest p_SourceID = '||p_SourceID||', p_SaleDate = '||p_SaleDate );
    END; --RSI_TXUpdatePart2Rest

    --Выполняет списание остатка при создании связи
    PROCEDURE RSI_TXLinkRest (p_NewTypeBU IN NUMBER, p_NewTypeDRU IN NUMBER, p_SourceID IN NUMBER,
                          p_LinkDate  IN DATE,   p_FIID IN NUMBER,       p_Amount IN NUMBER, in_IsTrigger IN BOOLEAN DEFAULT FALSE)
    IS
      v_Break       BOOLEAN;
      v_S           NUMBER;
      v_A           NUMBER;
      v_NT          NUMBER;
      v_RestAmount  NUMBER;
      v_RestType    NUMBER;
      v_RestBuyDate DATE;
      v_RestID      NUMBER;
    BEGIN

      TXPutMsg( 0,
                -1,
                TXMES_DEBUG,
                'Вызов RSI_TXLinkRest: p_NewTypeBU = '||p_NewTypeBU||
                ', p_NewTypeDRU = '||p_NewTypeDRU||', p_SourceID = '||p_SourceID||
                ', p_LinkDate = '||p_LinkDate||
                ', p_FIID = '||p_FIID||', p_Amount = '||p_Amount,
                in_IsTrigger );

      v_S := p_Amount;

      v_Break := FALSE;

      WHILE v_S > 0 LOOP
        BEGIN
          IF ReestrValue.V0 = RSB_SCTXC.TXREG_V0_FIFO THEN
            -- если ФИФО
            SELECT *
              INTO v_RestAmount, v_RestType, v_RestBuyDate, v_RestID
              FROM ( SELECT /*+ INDEX (Rest INDEX DSCTXREST_DBT_IDXA)*/
                            Rest.t_Amount, Rest.t_Type, Rest.t_BuyDate, Rest.t_ID
                       FROM dsctxrest_dbt Rest
                      WHERE Rest.T_TYPE IN (RSB_SCTXC.TXREST_B_U, RSB_SCTXC.TXREST_DR_U)
                        AND Rest.t_SourceID = p_SourceID
                        AND Rest.T_AMOUNT  > 0
                   ORDER BY Rest.t_BuyDate ASC,
                            Rest.t_Type ASC,
                            Rest.t_ID ASC )
             WHERE ROWNUM = 1;
          END IF;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            BEGIN
              v_Break := TRUE;
            END;
        END;

        EXIT WHEN v_Break;

        v_A  := iif( v_S < v_RestAmount, v_S, v_RestAmount );
        v_NT := iif( v_RestType = RSB_SCTXC.TXREST_B_U, p_NewTypeBU, p_NewTypeDRU );

        UPDATE dsctxrest_dbt
           SET t_Amount = t_Amount - v_A,
               t_ChangeDate = p_LinkDate
         WHERE t_ID = v_RestID;

        RSI_TXCorrectRest(v_NT, 0, p_SourceID, v_RestBuyDate, p_LinkDate, p_LinkDate, p_FIID, v_A, in_IsTrigger);
        v_S := v_S - v_A;

      END LOOP;

      TXPutMsg( 0,
                -1,
                TXMES_DEBUG,
                'Конец RSI_TXLinkRest: p_NewTypeBU = '||p_NewTypeBU||
                ', p_NewTypeDRU = '||p_NewTypeDRU||', p_SourceID = '||p_SourceID||
                ', p_LinkDate = '||p_LinkDate||
                ', p_FIID = '||p_FIID||', p_Amount = '||p_Amount,
                in_IsTrigger );
    END;--RSI_TXLinkRest

    --Выполняет обработку остатков и списаний при создании/обновлении связи
    PROCEDURE RSI_TXUpdateRestByLink (p_LinkType IN NUMBER, p_BuyID IN NUMBER, p_SaleID IN NUMBER, p_SourceID IN NUMBER,
                                  p_Lot1ID IN NUMBER, p_LinkDate IN DATE, p_FIID IN NUMBER, p_Amount IN NUMBER, in_IsTrigger IN BOOLEAN DEFAULT FALSE)
    IS
       v_Count        NUMBER;
       v_RestID       NUMBER;
       v_BuyDate      DATE;
       v_OpenRestDate DATE;
       v_FICODE       dfininstr_dbt.t_FI_Code%TYPE;
    BEGIN
       TXPutMsg( 0,
                 -1,
                 TXMES_DEBUG,
                 'Вызов RSI_TXUpdateRestByLink: p_LinkType = '||p_LinkType||', p_BuyID = '||p_BuyID||
                 ', p_SaleID = '||p_SaleID||', p_SourceID = '||p_SourceID||
                 ', p_Lot1ID = '||p_Lot1ID||', p_LinkDate = '||p_LinkDate||
                 ', p_FIID = '||p_FIID||', p_Amount = '||p_Amount,
                 in_IsTrigger );

       IF (p_LinkType = RSB_SCTXC.TXLNK_DELIVER) THEN
          RSI_TXLinkRest (RSB_SCTXC.TXREST_B_S, RSB_SCTXC.TXREST_DR_S, p_BuyID, p_LinkDate, p_FIID, p_Amount, in_IsTrigger);

       ELSIF (p_LinkType IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_LOANPUT)) THEN
          RSI_TXLinkRest (RSB_SCTXC.TXREST_B_DR, RSB_SCTXC.TXREST_DR_DR, p_BuyID, p_LinkDate, p_FIID, p_Amount, in_IsTrigger);

       ELSIF (p_LinkType IN (RSB_SCTXC.TXLNK_SUBSTREPO, RSB_SCTXC.TXLNK_SUBSTLOAN)) THEN

          RSI_TXCorrectRest (RSB_SCTXC.TXREST_DR_U, 0, p_Lot1ID, p_LinkDate,
                         TO_DATE('01.01.0001', 'DD.MM.YYYY'), p_LinkDate, p_FIID, p_Amount, in_IsTrigger);

          RSI_TXLinkRest (RSB_SCTXC.TXREST_B_DR, RSB_SCTXC.TXREST_DR_DR, p_BuyID, p_LinkDate, p_FIID, p_Amount, in_IsTrigger);

       ELSIF (p_LinkType IN (RSB_SCTXC.TXLNK_OPSREPO, RSB_SCTXC.TXLNK_OPSLOAN)) THEN

          RSI_TXCorrectRest (RSB_SCTXC.TXREST_Open, p_SaleID, p_BuyID, TO_DATE('01.01.0001', 'DD.MM.YYYY'),
                         p_LinkDate, p_LinkDate, p_FIID, p_Amount, in_IsTrigger);

          RSI_TXLinkRest (RSB_SCTXC.TXREST_B_DR, RSB_SCTXC.TXREST_DR_DR, p_BuyID, p_LinkDate, p_FIID, p_Amount, in_IsTrigger);

       ELSIF (p_LinkType IN (RSB_SCTXC.TXLNK_CLSREPO, RSB_SCTXC.TXLNK_CLSLOAN)) THEN

          --Найдём кол-во таких записей.
          BEGIN
            SELECT COUNT(1)
              INTO v_Count
              FROM dsctxrest_dbt
             WHERE T_TYPE = RSB_SCTXC.TXREST_Open
               AND t_SaleID   = p_SaleID
               AND t_SourceID = p_SourceID
               AND T_BUYDATE  = TO_DATE('01.01.0001', 'DD.MM.YYYY');
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN
                v_Count := 0;
              END;
          END;

          IF (v_Count <> 1) THEN
             SELECT t_FI_Code
               INTO v_FICODE
               FROM dfininstr_dbt
              WHERE t_FIID = p_FIID;

             TXPutMsg( 0,
                       -1,
                       TXMES_WARNING,
                       'Ошибка обработки остатка ц/б "'||v_FICODE||'" в дату '||TO_CHAR(p_LinkDate,'DD.MM.YYYY'));

             RETURN;
          ELSE
             BEGIN
               SELECT t_ID, t_SaleDate
                 INTO v_RestID, v_OpenRestDate
                 FROM dsctxrest_dbt
                WHERE T_TYPE = RSB_SCTXC.TXREST_Open
                  AND t_SaleID   = p_SaleID
                  AND t_SourceID = p_SourceID
                  AND T_BUYDATE  = TO_DATE('01.01.0001', 'DD.MM.YYYY');
             EXCEPTION
               WHEN NO_DATA_FOUND THEN NULL;
             END;

             UPDATE dsctxrest_dbt
                SET t_Amount = t_Amount - p_Amount,
                    t_Changedate = p_LinkDate
              WHERE t_ID = v_RestID;

             RSI_TXCorrectRest (RSB_SCTXC.TXREST_Closed, p_SaleID, p_SourceID, p_LinkDate,
                            v_OpenRestDate, p_LinkDate, p_FIID, p_Amount, in_IsTrigger);

             RSI_TXCorrectRest (RSB_SCTXC.TXREST_DR_U, 0, p_SourceID, p_LinkDate,
                            TO_DATE('01.01.0001', 'DD.MM.YYYY'), p_LinkDate, p_FIID, p_Amount, in_IsTrigger);

             RSI_TXLinkRest (RSB_SCTXC.TXREST_B_S, RSB_SCTXC.TXREST_DR_S, p_BuyID, p_LinkDate, p_FIID, p_Amount, in_IsTrigger);
          END IF;
       END IF;

       TXPutMsg( 0,
                 -1,
                 TXMES_DEBUG,
                 'Конец RSI_TXUpdateRestByLink: p_LinkType = '||p_LinkType||', p_BuyID = '||p_BuyID||
                 ', p_SaleID = '||p_SaleID||', p_SourceID = '||p_SourceID||
                 ', p_Lot1ID = '||p_Lot1ID||', p_LinkDate = '||p_LinkDate||
                 ', p_FIID = '||p_FIID||', p_Amount = '||p_Amount,
                 in_IsTrigger );
    END; -- RSI_TXUpdateRestByLink

  ---- Закрыть налоговый период
    PROCEDURE TXClosePeriod( v_CloseDate_in IN DATE )
    IS
      v_BuildDate      DATE;
      v_CloseDate_prev DATE;
      v_WasError       BOOLEAN;
    BEGIN

      RSI_BeginCalculate( 'Закрытие налогового периода' );

      GetSettingsTax();

      BEGIN
        SELECT TO_DATE(rsb_struct.getString(t_fmtblobdata_xxxx),'DD.MM.YYYY')
          INTO v_BuildDate
          FROM dregval_dbt
         WHERE t_KeyID = rsb_tools.find_regkey('SECUR\DATE_BUILD_TAXREG');
      EXCEPTION
        WHEN OTHERS THEN
          v_BuildDate := TO_DATE('01-01-0001','DD-MM-YYYY');
      END;

      BEGIN
        SELECT TO_DATE(rsb_struct.getString(t_fmtblobdata_xxxx),'DD.MM.YYYY')
          INTO v_CloseDate_prev
          FROM dregval_dbt
         WHERE t_KeyID = rsb_tools.find_regkey('SECUR\DATE_CLOSE_TAXREG');
      EXCEPTION
        WHEN OTHERS THEN
          v_CloseDate_prev := TO_DATE('01-01-0001','DD-MM-YYYY');
      END;

      TXPutMsg( 0,
                -1,
                TXMES_MESSAGE,
                'Закрытие налогового периода с '||TO_CHAR(v_BuildDate,'DD.MM.YYYY')||' по '||TO_CHAR(v_CloseDate_in,'DD.MM.YYYY'));

      IF v_CloseDate_in <= v_CloseDate_prev THEN
        TXPutMsg( 0,
                  -1,
                  TXMES_ERROR,
                  'Попытка закрыть закрытый период');
      ELSIF v_CloseDate_in > v_BuildDate THEN
        TXPutMsg( 0,
                  -1,
                  TXMES_ERROR,
                  'Попытка закрыть нерасчитанный период');
      ELSE
        v_WasError := FALSE;
        BEGIN
          UPDATE dregval_dbt
             SET t_fmtblobdata_xxxx = rsb_struct.putString( t_fmtblobdata_xxxx, TO_CHAR(v_CloseDate_in,'DD.MM.YYYY'))
           WHERE t_KeyID = rsb_tools.find_regkey('SECUR\DATE_CLOSE_TAXREG');

        EXCEPTION
          WHEN OTHERS THEN
            BEGIN
              v_WasError := TRUE;
              TXPutMsg( 0,
                        -1,
                        TXMES_ERROR,
                        'Ошибка при обновлении настройки "SECUR\DATE_CLOSE_TAXREG"');
            END;
        END;

        IF NOT v_WasError THEN
          TXPutMsg( 0,
                    -1,
                    TXMES_MESSAGE,
                    '** Налоговый период закрыт **');
        ELSE
          TXPutMsg( 0,
                    -1,
                    TXMES_MESSAGE,
                    '** Произошла ошибка при закрытии налогового периода **');
        END IF;
      END IF;

      EndCalculate;
      COMMIT;
    END; --TXClosePeriod


    --Вставка лотов
    PROCEDURE InsertLots( v_BegDate_in IN DATE, v_EndDate_in IN DATE, v_TaxGroup_in NUMBER, v_FIID_in NUMBER )
    IS
       v_Count NUMBER;
       v_LotID NUMBER;
       v_BuyDate     DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
       v_SaleDate    DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
       v_BegBuyDate  DATE;
       v_BegSaleDate DATE;
       v_Date2       DATE;
       v_All         NUMBER := 0;
       v_RestPaymAmount NUMBER;
       v_Break       BOOLEAN;
       clot dsctxlot_dbt%ROWTYPE;
       v_Tick ddl_tick_dbt%ROWTYPE;
       v_Leg  ddl_leg_dbt%ROWTYPE;
       v_Fin  dfininstr_dbt%ROWTYPE;
       v_Amount      NUMBER := 0;
       v_NewNumber   NUMBER := 0;
       v_AmountDR    NUMBER := 0;
       v_FiCodeDR    dfininstr_dbt.t_Fi_Code%TYPE;
       v_GO_CODE     dsctxgo_dbt.t_Code%TYPE;
       v_Numerator   NUMBER := 0;
       v_Denominator NUMBER := 0;
       v_rqamount         NUMBER;
       v_Kind             NUMBER;
       v_dealcode         ddl_tick_dbt.t_dealcode%TYPE;
       v_legprice1        ddl_leg_dbt.t_Price%TYPE;
       v_legprice2        ddl_leg_dbt.t_Price%TYPE;
       issale             NUMBER;
       isbuy              NUMBER;
       ogrp               NUMBER;
       v_CompRQ           ddlrq_dbt%ROWTYPE;

       CURSOR BasketPaym IS SELECT RQ.t_FIID, RQ.t_DocID
                              FROM ddlrq_dbt RQ, ddl_tick_dbt Tick, davoiriss_dbt avoir,
                                  (SELECT t_Kind_Operation, t_DocKind, rsb_secur.get_OperationGroup(t_SysTypes) oGrp
                                    FROM doprkoper_dbt) Opr
                             WHERE RQ.t_DocKind = RSB_SCTXC.DL_SECURITYDOC
                               AND RQ.t_State   = RSI_DLRQ.DLRQ_STATE_EXEC
                               AND RQ.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                               AND RQ.t_FactDate >= v_BegDate_in
                               AND RQ.t_FactDate <= v_EndDate_in
                               AND Opr.t_Kind_Operation = Tick.t_DealType
                               AND rsb_secur.IsBasket(Opr.oGrp) = 1
                               AND Tick.t_DealID  = RQ.t_DocID
                               AND Tick.t_ClientID = -1
                               AND avoir.t_FIID = RQ.t_FIID
                               AND avoir.t_TaxGroup NOT IN (888, 999)
                               AND (((v_FIID_in = -1) AND
                                     (v_TaxGroup_in = -1 OR v_TaxGroup_in = NVL(avoir.t_TaxGroup, 0)
                                     )
                                    ) OR (v_FIID_in = RQ.t_FIID)
                                   )
                             GROUP BY RQ.t_DocID, RQ.t_FIID
                             ORDER BY RQ.t_DocID ASC, RQ.t_FIID ASC;

       CURSOR BackPaym IS SELECT TXLot.t_DealID, RQ.t_FactDate, TXLot.t_Type, TXLot.t_ID, TXLot.t_DealCode, TXLot.t_BegLotID, TXLot.t_FIID
                            FROM ddlrq_dbt RQ, dsctxlot_dbt TXLot
                           WHERE RQ.t_DocKind = RSB_SCTXC.DL_SECURITYDOC
                             AND RQ.t_State   = RSI_DLRQ.DLRQ_STATE_EXEC
                             AND RQ.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                             AND RQ.t_Type    = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                             AND RQ.t_DealPart= 2
                             AND RQ.t_FactDate >= v_BegDate_in
                             AND RQ.t_FactDate <= v_EndDate_in
                             AND RQ.t_FIID = TXLot.t_FIID
                             AND (((v_FIID_in = -1) AND
                                   (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                                             from davoiriss_dbt
                                                                            where t_FIID = TXLot.t_FIID
                                                                          )
                                   )
                                  ) OR (v_FIID_in = TXLot.t_FIID)
                                 )
                             AND TXLot.t_IsComp = CHR(0)
                             AND TXLot.t_DealID = RQ.t_DocID
                             AND TXLot.t_Type IN ( RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANPUT, RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANGET )
                           ORDER BY TXLot.t_DealID ASC, RQ.t_FactDate ASC;

       CURSOR CompPaym IS SELECT sum(CASE WHEN rq.t_kind = rsi_dlrq.dlrq_kind_commit THEN rq.t_Amount ELSE -rq.t_Amount END) SumAmount, rq.t_fiid, rq.t_docid, rq.t_factdate
                            FROM ddlrq_dbt rq,
                                 ddl_tick_dbt tick, davoiriss_dbt av
                           WHERE rq.t_dockind = rsb_sctxc.dl_securitydoc
                             AND rq.t_state = rsi_dlrq.dlrq_state_exec
                             AND rq.t_subkind = rsi_dlrq.dlrq_subkind_avoiriss
                             AND rq.t_type = rsi_dlrq.dlrq_type_compdelivery
                             AND rq.t_factdate >= v_begdate_in
                             AND rq.t_factdate <= v_enddate_in
                             AND tick.t_dealid = rq.t_docid
                             AND tick.t_clientid = -1
                             AND av.t_FIID = rq.t_FIID
                             AND av.t_TaxGroup NOT IN (888, 999)
                             AND (   (    (v_fiid_in = -1)
                                      AND (   v_taxgroup_in = -1
                                           OR v_taxgroup_in = (SELECT NVL (t_taxgroup, 0)
                                                                 FROM davoiriss_dbt
                                                                WHERE t_fiid = rq.t_fiid)
                                          )
                                     )
                                  OR (v_fiid_in = rq.t_fiid)
                                 )
                             -- Отбираем все ТО, кроме тех что в дату 2ч сделки
                             AND not exists ( select RQ2.t_ID
                                                from DDLRQ_DBT RQ2
                                               where RQ2.T_DOCKIND  = RSB_SCTXC.DL_SECURITYDOC
                                                 and RQ2.t_DocID    = tick.t_dealid
                                                 and RQ2.t_state    = rsi_dlrq.dlrq_state_exec
                                                 and RQ2.t_subkind  = rsi_dlrq.dlrq_subkind_avoiriss
                                                 and RQ2.t_Type     = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                                                 and RQ2.t_DealPart = 2
                                                 and RQ2.t_FactDate = rq.t_factdate
                                            )
                        GROUP BY rq.t_docid, rq.t_fiid, rq.t_factdate
                        ORDER BY rq.t_docid ASC, rq.t_fiid ASC, rq.t_factdate ASC;

       CURSOR cDl_COMM IS
       SELECT *
         FROM DDL_COMM_DBT comm
        WHERE (comm.T_DOCKIND = 135 OR comm.t_DOCKIND = 139 OR
               (ReestrValue.V14 = RSB_SCTXC.TXREG_V14_YES AND comm.T_DocKind = 105 AND comm.t_OperSubKind <> RSB_SECUR.SUBKIND_UNRETIRE)) --(Глобальная операция с ц/б, Изменение номинала ц/б, Перемещение)
          AND comm.T_COMMDATE  >= v_BegDate_in
          AND comm.T_COMMDATE  <= v_EndDate_in
          AND comm.T_CommStatus = 2 --Закрыта
          AND (((v_FIID_in = -1) AND
                (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                          from davoiriss_dbt
                                                         where t_FIID = comm.t_FIID
                                                       )
                )
               ) OR (v_FIID_in = comm.t_FIID)
              );

       CURSOR cDl_COMM_GO IS
       SELECT comm.*, avr.t_TaxGroup
         FROM DDL_COMM_DBT comm, DAVOIRISS_DBT avr
        WHERE comm.T_DOCKIND IN (135) --(Глобальная операция с ц/б)
          AND comm.T_ENDDATE  >= v_BegDate_in
          AND comm.T_ENDDATE  <= v_EndDate_in
          AND comm.T_CommStatus = 2 --Закрыта
          AND avr.t_FIID = comm.t_FIID
          AND (((v_FIID_in = -1) AND
                (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                          from davoiriss_dbt
                                                         where t_FIID = comm.t_FIID
                                                       )
                )
               ) OR (v_FIID_in = comm.t_FIID)
              );


       CURSOR cSCDLFI(v_DocKind IN NUMBER, v_DocID IN NUMBER, v_All IN NUMBER) IS
       SELECT lfi.*
         FROM DSCDLFI_DBT lfi, DAVOIRISS_DBT avr
        WHERE lfi.T_DEALKIND = v_DocKind
          AND lfi.T_DEALID = v_DocID
          AND avr.t_FIID = lfi.T_NEWFIID
          AND 1 = (CASE WHEN (v_All = 1 OR (v_FIID_in <> -1 AND lfi.T_NEWFIID = v_FIID_in)) THEN 1
                        WHEN (v_All = 1 OR (v_TaxGroup_in <> -1 AND avr.T_TAXGROUP = v_TaxGroup_in)) THEN 1 ELSE 0 END );

    BEGIN

      FOR b IN BasketPaym LOOP

         begin
            select COUNT(1) INTO v_Count
              from DSCTXFI_DBT
             where T_DEALID = b.t_DocID
               and T_FIID   = b.t_FIID;
            exception
              when OTHERS then v_Count := 0;
         end;

         if( v_Count = 0 )then

            v_NewNumber := 0;

            -- ищем первый незанятый
            begin
               select rn into v_NewNumber
                 from (select sq.rn rn
                        from ( SELECT rownum AS rn FROM DUAL
                               CONNECT BY LEVEL <= (select NVL(MAX(F.T_FINUMBER), 0)
                                                      from DSCTXFI_DBT F
                                                     where F.T_FINUMBER > 0
                                                       and F.T_DEALID = b.T_DOCID
                                                   )
                             ) sq
                       WHERE sq.rn NOT IN (select F.T_FINUMBER
                                            from DSCTXFI_DBT F
                                           where F.T_FINUMBER > 0
                                             and F.T_DEALID = b.T_DOCID)
                       ORDER BY sq.rn
                      )
                where rownum = 1;
              exception
                 when OTHERS then select NVL(MAX(F.T_FINUMBER), 0) + 1 into v_NewNumber
                                            from DSCTXFI_DBT F
                                           where F.T_FINUMBER > 0
                                             and F.T_DEALID = b.T_DOCID;
            end;

            INSERT INTO DSCTXFI_DBT ( T_DEALID,
                                      T_FIID,
                                      T_FINUMBER )
                             VALUES ( b.t_DocID,
                                      b.t_FIID,
                                      v_NewNumber
                                    );
         end if;

      END LOOP;

      --Занести лоты покупок/продаж, совершенных за период.
      INSERT INTO dsctxlot_dbt (T_DEALID,
                                T_FIID,
                                T_TAXGROUP,
                                T_TYPE,
                                T_BEGLOTID,
                                T_CHILDID,
                                T_ISCOMP,
                                T_VIRTUALTYPE,
                                T_DEALCODE,
                                T_DEALCODETS,
                                T_BUYID,
                                T_REALID,
                                T_DEALDATE,
                                T_DEALTIME,
                                T_BUYDATE,
                                T_SALEDATE,
                                T_BEGBUYDATE,
                                T_BEGSALEDATE,
                                T_RETRDATE,
                                T_AMOUNT,
                                T_COMPAMOUNT,
                                T_NETTINGID,
                                T_PRICE,
                                T_PRICEFIID,
                                T_PRICECUR,
                                T_OLDTYPE,
                                T_NETTING,
                                T_SALE,
                                T_RETFLAG,
                                T_INACC,
                                T_ISFREE,
                                T_ORDFORSALE,
                                T_ORDFORREPO,
                                T_ORDFORSUBST,
                                T_ORDFORCLPOSREPO,
                                T_BLOCKED,
                                T_RQID,
                                T_PORTFOLIO )
                        SELECT  /*+  ORDERED  index(RQ DDLRQ_DBT_USR1)  */  Tick.t_DealID, --T_DEALID
                               RQ.t_FIID, --T_FIID
                               0, --T_TAXGROUP
                               get_lotType(Opr.oGrp, Tick.t_DealID, RQ.t_DealPart), --T_TYPE
                               0, --T_BEGLOTID - заполнится в триггере
                               0, --T_CHILDID
                               CHR(0), --T_ISCOMP
                               RSB_SCTXC.TXVDEAL_REAL, --T_VIRTUALTYPE
                               TXGetLotCode(RQ.t_DocID,RQ.t_FIID,Tick.t_DealCode,rsb_secur.IsBasket(Opr.oGrp),rsb_secur.IsBuy(Opr.oGrp),KINDLOT_NORMAL), --T_DEALCODE
                               TXGetLotCode(RQ.t_DocID,RQ.t_FIID,DECODE(Tick.t_DealCodeTS, CHR(1), Tick.t_DealCode, Tick.t_DealCodeTS),rsb_secur.IsBasket(Opr.oGrp),rsb_secur.IsBuy(Opr.oGrp),KINDLOT_NORMAL), --T_DEALCODETS
                               0, --T_BUYID
                               0, --T_REALID
                               Tick.t_DealDate, --T_DEALDATE
                               Tick.t_DealTime, --T_DEALTIME
                               get_lotBuyDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart), --T_BUYDATE
                               get_lotSaleDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart),--T_SALEDATE
                               TO_DATE('01.01.0001','DD.MM.YYYY'), --T_BEGBUYDATE - заполнится в триггере
                               TO_DATE('01.01.0001','DD.MM.YYYY'), --T_BEGSALEDATE - заполнится в триггере
                               TO_DATE('01.01.0001','DD.MM.YYYY'), --T_RETRDATE
                               RQ.t_Amount, --T_AMOUNT
                               0, --T_COMPAMOUNT
                               0, --T_NETTINGID
                               (CASE WHEN Leg.t_RelativePrice = CHR(0) OR
                                          Leg.t_RelativePrice IS NULL OR
                                          RQ.t_DocKind = RSB_SCTXC.DL_RETIREMENT THEN Leg.t_Price
                                     ELSE RSI_RSB_FIInstr.FI_GetNominalOnDate(RQ.t_FIID, RQ.t_FactDate)*Leg.t_Price/100 END), --T_PRICE
                               Leg.t_CFI,
                               Fin.t_CCY, --T_PRICECUR
                               0, --T_OLDTYPE
                               0, --T_NETTING
                               0, --T_SALE
                               CHR(0), -- T_RETFLAG
                               CHR(88), -- T_INACC
                               CHR(0), -- T_ISFREE
                               0, -- T_ORDFORSALE
                               0, -- T_ORDFORREPO
                               0, -- T_ORDFORSUBST
                               0, -- T_ORDFORCLPOSREPO
                               Tick.T_BLOCKED, --T_BLOCKED
                               RQ.t_ID, --T_RQID
                               (CASE WHEN Rsb_Secur.IsRepo(Opr.oGrp)=1 OR Rsb_Secur.IsLoan(Opr.oGrp)=1 OR RQ.T_DOCKIND = RSB_SECUR.OBJTYPE_RETIRE THEN -1
                                     WHEN RQ.T_DOCKIND = RSB_SCTXC.DL_CONVAVR AND RQ.T_DEALPART = 2 THEN Tick.T_PORTFOLIOID_2
                                     ELSE Tick.T_PORTFOLIOID END)-- T_PORTFOLIO
                          FROM ddlrq_dbt RQ, ddl_tick_dbt Tick, ddl_leg_dbt Leg, dfininstr_dbt Fin, davoiriss_dbt Av,
                               (SELECT t_Kind_Operation, t_DocKind, rsb_secur.get_OperationGroup(t_SysTypes) oGrp
                                  FROM doprkoper_dbt) Opr
                         WHERE Tick.t_DealID = RQ.t_DocID
                           AND Tick.t_BOfficeKind = RQ.t_DocKind
                           AND Opr.t_Kind_Operation = Tick.t_DealType
                           AND Leg.t_DealID = Tick.t_DealID
                           AND Leg.t_LegID = 0
                           AND Leg.t_LegKind = DECODE(RQ.t_DealPart, 1, 0, 2)
                           AND Fin.t_FIID = Leg.t_CFI
                           AND RQ.t_FIID = Av.t_FIID
                           AND Av.t_TaxGroup NOT IN (888, 999)
                           AND RQ.T_FACTDATE >= v_BegDate_in
                           AND ( (get_lotBuyDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart) >= v_BegDate_in
                              AND get_lotBuyDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart) <= v_EndDate_in )
                             OR  (get_lotSaleDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart) >= v_BegDate_in
                              AND get_lotSaleDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart) <= v_EndDate_in )
                               )
                           AND RQ.t_State   = RSI_DLRQ.DLRQ_STATE_EXEC
                           AND RQ.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                           AND RQ.t_Type    = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                           AND Tick.t_ClientID = -1
                           AND (v_FIID_in = -1 OR RQ.t_FIID = v_FIID_in)
                           AND (v_TaxGroup_in = -1 OR Av.t_TaxGroup = v_TaxGroup_in)
                           AND RSB_SECUR.GetMainObjAttrNoDate(RSB_SECUR.OBJTYPE_SECDEAL, LPAD (tick.t_DealId, 34, '0'), 118) <> 1
                           AND (RQ.t_DealPart = 1 OR
                                (RQ.t_DealPart = 2 AND
                                 ((Rsb_Secur.IsBackSale(oGrp)=1 OR RQ.t_DocKind = RSB_SCTXC.DL_CONVAVR
                                  ) or
                                  ((Rsb_Secur.IsRepo(Opr.oGrp)=1 OR Rsb_Secur.IsLoan(Opr.oGrp)=1
                                   ) and
                                   --категория "Является налоговым Репо" на сделке DDL_TICK.T_DEALID задана и равна False
                                   ( CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(Tick.t_DealID, 34, '0'), 2)=1
                                   )
                                  )
                                 )
                                )
                               )
                           AND (   RQ.t_DocKind IN (RSB_SCTXC.DL_SECURITYDOC, RSB_SCTXC.DL_RETIREMENT)
                                OR (RQ.t_DocKind = RSB_SCTXC.DL_CONVAVR AND Tick.t_DealDate < TO_DATE('01.01.2015','DD.MM.YYYY'))
                               )
                           AND Rsb_Secur.IsRet_Partly(Opr.oGrp) <> 1
                           AND Rsb_Secur.IsRet_Coupon(Opr.oGrp) <> 1;

      -- по списаниям/зачислениям ц/б
      INSERT INTO dsctxlot_dbt (T_DEALID,
                                T_FIID,
                                T_TAXGROUP,
                                T_TYPE,
                                T_BEGLOTID,
                                T_CHILDID,
                                T_ISCOMP,
                                T_VIRTUALTYPE,
                                T_DEALCODE,
                                T_DEALCODETS,
                                T_BUYID,
                                T_REALID,
                                T_DEALDATE,
                                T_DEALTIME,
                                T_BUYDATE,
                                T_SALEDATE,
                                T_BEGBUYDATE,
                                T_BEGSALEDATE,
                                T_RETRDATE,
                                T_AMOUNT,
                                T_COMPAMOUNT,
                                T_NETTINGID,
                                T_PRICE,
                                T_PRICEFIID,
                                T_PRICECUR,
                                T_OLDTYPE,
                                T_NETTING,
                                T_SALE,
                                T_RETFLAG,
                                T_INACC,
                                T_ISFREE,
                                T_ORDFORSALE,
                                T_ORDFORREPO,
                                T_ORDFORSUBST,
                                T_ORDFORCLPOSREPO,
                                T_BLOCKED,
                                T_RQID,
                                T_PORTFOLIO )
                        SELECT Tick.t_DealID, --T_DEALID
                               RQ.t_FIID, --T_FIID
                               0, --T_TAXGROUP
                               get_lotType(Opr.oGrp, Tick.t_DealID, RQ.t_DealPart), --T_TYPE
                               0, --T_BEGLOTID - заполнится в триггере
                               0, --T_CHILDID
                               CHR(0), --T_ISCOMP
                               RSB_SCTXC.TXVDEAL_REAL, --T_VIRTUALTYPE
                               TXGetLotCode(RQ.t_DocID, RQ.t_FIID, Tick.t_DealCode, 0, 0, KINDLOT_NORMAL), --T_DEALCODE
                               TXGetLotCode(RQ.t_DocID, RQ.t_FIID, DECODE(Tick.t_DealCodeTS, CHR(1), Tick.t_DealCode, Tick.t_DealCodeTS), 0, 0, KINDLOT_NORMAL), --T_DEALCODETS
                               0, --T_BUYID
                               0, --T_REALID
                               Tick.t_DealDate, --T_DEALDATE
                               (CASE WHEN Leg.t_SupplyTime = TO_DATE ('01.01.0001', 'DD.MM.YYYY') THEN Tick.t_DealTime ELSE Leg.t_SupplyTime END), --T_DEALTIME
                               get_lotBuyDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart), --T_BUYDATE
                               get_lotSaleDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart),--T_SALEDATE
                               TO_DATE('01.01.0001','DD.MM.YYYY'), --T_BEGBUYDATE - заполнится в триггере
                               TO_DATE('01.01.0001','DD.MM.YYYY'), --T_BEGSALEDATE - заполнится в триггере
                               TO_DATE('01.01.0001','DD.MM.YYYY'), --T_RETRDATE
                               RQ.t_Amount, --T_AMOUNT
                               0, --T_COMPAMOUNT
                               0, --T_NETTINGID
                               RSI_NPTO.GetPriceFromAvrWrtIn(Tick.t_DealID, Leg.t_Price), --T_PRICE
                               PriceFin.t_FIID,
                               PriceFin.t_CCY, --T_PRICECUR
                               0, --T_OLDTYPE
                               0, --T_NETTING
                               0, --T_SALE
                               CHR(0), -- T_RETFLAG
                               CHR(88), -- T_INACC
                               CHR(0), -- T_ISFREE
                               0, -- T_ORDFORSALE
                               0, -- T_ORDFORREPO
                               0, -- T_ORDFORSUBST
                               0, -- T_ORDFORCLPOSREPO
                               Tick.T_BLOCKED, --T_BLOCKED
                               RQ.t_ID, --T_RQID
                               Tick.T_PORTFOLIOID -- T_PORTFOLIO
                          FROM ddlrq_dbt RQ, ddl_tick_dbt Tick, ddl_leg_dbt Leg, dfininstr_dbt Fin, dfininstr_dbt PriceFin, davoiriss_dbt Av,
                               (SELECT t_Kind_Operation, t_DocKind, rsb_secur.get_OperationGroup(t_SysTypes) oGrp
                                  FROM doprkoper_dbt) Opr
                         WHERE Tick.t_DealID = RQ.t_DocID
                           AND Tick.t_BOfficeKind = RQ.t_DocKind
                           AND Opr.t_Kind_Operation = Tick.t_DealType
                           AND Leg.t_DealID = Tick.t_DealID
                           AND Leg.t_LegID = 0
                           AND Leg.t_LegKind = 0
                           AND Fin.t_FIID = Leg.t_CFI
                           AND PriceFin.t_FIID = RSI_NPTO.GetPriceFIIDFromAvrWrtIn(Tick.t_DealID, Leg.t_CFI)
                           AND RQ.t_FIID = Av.t_FIID
                           AND Av.t_TaxGroup NOT IN (888, 999)
                           AND ( (get_lotBuyDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart) >= v_BegDate_in
                              AND get_lotBuyDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart) <= v_EndDate_in )
                             OR  (get_lotSaleDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart) >= v_BegDate_in
                              AND get_lotSaleDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart) <= v_EndDate_in )
                               )
                           AND RQ.t_State   = RSI_DLRQ.DLRQ_STATE_EXEC
                           AND RQ.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                           AND RQ.t_Type    = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                           AND Tick.t_ClientID = -1
                           AND (v_FIID_in = -1 OR RQ.t_FIID = v_FIID_in)
                           AND (v_TaxGroup_in = -1 OR Av.t_TaxGroup = v_TaxGroup_in)
                           AND RQ.t_DealPart = 1
                           AND Tick.t_Flag3 = chr(88)
                           AND RQ.t_DocKind = RSB_SCTXC.DL_AVRWRT
                           AND (   ( Tick.t_Ofbu <> CHR (88) AND Rsb_SCTX.GetInAvrWrtStartDate (Tick.t_DealID) > v_EndDate_in )
                                OR ( Tick.t_Ofbu = CHR (88) AND Rsb_SCTX.GetInAvrWrtStartDate (Tick.t_DealID) <=  v_EndDate_in));

      FOR v_error_du 
        IN (SELECT CASE WHEN t_ofbu = CHR (88)
                   THEN 'По сделке вывода из ДУ ' || t_dealcode || ' не задана дата вывода, лот не создан'
                   ELSE 'По сделке вывода из ДУ ' || t_dealcode || ' не проставлен признак ОФБУ, лот не создан'
               END AS t_mes
          FROM ddl_tick_dbt Tick
         WHERE     t_BOfficeKind = 127
               AND t_dealtype = 2011
               AND (v_FIID_in = -1 OR t_pfi = v_FIID_in)
               AND (   (    Tick.t_Ofbu <> CHR (88)
                        AND Rsb_SCTX.GetInAvrWrtStartDate (Tick.t_DealID) <= v_EndDate_in )
                    OR (    Tick.t_Ofbu = CHR (88)
                        AND Rsb_SCTX.GetInAvrWrtStartDate (Tick.t_DealID) > v_EndDate_in)))
      LOOP
        TXPutMsg(0, v_FIID_in, TXMES_WARNING, v_error_du.t_mes);
      END LOOP;

      TXPutMsg(0, v_FIID_in, TXMES_DEBUG, 'Лоты по сделкам вставлены');
      TXPutMsg(0, v_FIID_in, TXMES_OPTIM, 'Лоты по сделкам вставлены');

      COMMIT;

      UPDATE dsctxlot_dbt
         SET t_BegLotID = t_ID
       WHERE t_BegLotID = 0
         AND t_IsComp   = CHR(0)
         AND t_VirtualType = RSB_SCTXC.TXVDEAL_REAL;

      TXPutMsg( 0,
                v_FIID_in,
                TXMES_DEBUG,
                'Заполнено t_beglotID' );
      TXPutMsg( 0,
                v_FIID_in,
                TXMES_OPTIM,
                'Заполнено t_beglotID' );
      COMMIT;

      RSI_TXDealSortAll;
      TXPutMsg( 0,
                v_FIID_in,
                TXMES_DEBUG,
                'Параметры сортировки лотов определены' );
      TXPutMsg( 0,
                v_FIID_in,
                TXMES_OPTIM,
                'Параметры сортировки лотов определены' );
      COMMIT;

      --Установить дату лотов по 2 части Репо и займа, совершенных за период
      IF v_BegDate_in <= v_EndDate_in THEN
        FOR c IN BackPaym LOOP

          IF( c.t_Type IN ( RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT ) ) THEN

            UPDATE dsctxlot_dbt TXLot
               SET TXLot.t_BuyDate = c.t_FactDate
             WHERE TXLot.t_BegLotID = c.t_BegLotID
               AND TXLot.t_BuyDate = TO_DATE('01.01.0001','DD.MM.YYYY');

            UPDATE dsctxlot_dbt TXLot
               SET TXLot.t_BegBuyDate = c.t_FactDate
             WHERE TXLot.t_BegLotID = c.t_BegLotID
               AND TXLot.t_BegBuyDate = TO_DATE('01.01.0001','DD.MM.YYYY');

          ELSE

            UPDATE dsctxlot_dbt TXLot
               SET TXLot.t_SaleDate = c.t_FactDate
             WHERE TXLot.t_BegLotID = c.t_BegLotID
               AND TXLot.t_SaleDate = TO_DATE('01.01.0001','DD.MM.YYYY');

            UPDATE dsctxlot_dbt TXLot
               SET TXLot.t_BegSaleDate = c.t_FactDate
             WHERE TXLot.t_BegLotID = c.t_BegLotID
               AND TXLot.t_BegSaleDate = TO_DATE('01.01.0001','DD.MM.YYYY');

          END IF;
        END LOOP;

      END IF;

      TXPutMsg( 0,
                v_FIID_in,
                TXMES_DEBUG,
                'Установлены даты лотов по 2 части Репо и займа, совершенных за период' );
      TXPutMsg( 0,
                v_FIID_in,
                TXMES_OPTIM,
                'Установлены даты лотов по 2 части Репо и займа, совершенных за период' );

      COMMIT;


      --Обработать ТО по компенсационной поставке по Репо и займам за период.
      --Перебираем суммарные комп. ТО за день, сгруппированные по сделке, бумаге, направлению.
      FOR cpaym IN CompPaym
      LOOP
         v_Kind := -1;
         IF cpaym.SumAmount > 0 THEN
            v_Kind := rsi_dlrq.dlrq_kind_commit;
         ELSIF cpaym.SumAmount < 0 THEN
            v_Kind := rsi_dlrq.dlrq_kind_request;
         END IF;

         IF v_Kind > -1 THEN
            -- Найти ТО с максимальным количеством бумаг направления v_Kind:
            BEGIN
              SELECT * INTO v_CompRQ
                FROM (SELECT rq.*
                        FROM ddlrq_dbt rq
                       WHERE rq.t_dockind = rsb_sctxc.dl_securitydoc
                         AND rq.t_docid = cpaym.t_docid
                         AND rq.t_fiid = cpaym.t_fiid
                         AND rq.t_state = rsi_dlrq.dlrq_state_exec
                         AND rq.t_subkind = rsi_dlrq.dlrq_subkind_avoiriss
                         AND rq.t_kind = v_Kind
                         AND rq.t_type = rsi_dlrq.dlrq_type_compdelivery
                         AND rq.t_factdate = cpaym.t_factdate
                       ORDER BY rq.t_Amount DESC, rq.t_ID ASC)
               WHERE ROWNUM = 1;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN EXIT;
               WHEN OTHERS THEN RETURN;
            END;

            BEGIN
               -- получить сделку по ТО.
               SELECT   tick.t_dealcode, leg1.t_price, NVL (leg2.t_price, 0),
                        rsb_secur.issale (opr.ogrp), rsb_secur.isbuy (opr.ogrp), opr.ogrp
                 INTO   v_dealcode, v_legprice1, v_legprice2, issale, isbuy, ogrp
                   FROM ddl_leg_dbt leg1,
                        ddl_leg_dbt leg2,
                        ddl_tick_dbt tick,
                        (SELECT t_kind_operation, t_dockind,
                                rsb_secur.get_operationgroup (t_systypes) ogrp
                           FROM doprkoper_dbt) opr
                  WHERE opr.t_kind_operation = tick.t_dealtype
                    AND tick.t_dealid = cpaym.t_docid
                    AND leg1.t_dealid = cpaym.t_docid
                    AND leg1.t_legkind = 0
                    AND leg2.t_dealid(+) = cpaym.t_docid
                    AND leg2.t_legkind(+) = 2;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  BEGIN
                     txputmsg
                        (0,
                         v_fiid_in,
                         txmes_warning,
                            'Ошибка обработки компенсационной поставки по сделке c DealID = '||cpaym.t_docid
                         || ' в дату '
                         || TO_CHAR (cpaym.t_factdate, 'DD.MM.YYYY')
                        );
                  END;
            END;

            if cpaym.SumAmount < 0 THEN
               v_rqamount := -cpaym.SumAmount;
            else
               v_rqamount := cpaym.SumAmount;
            end if;

            -- в прямом РЕПО - требования, в обратном - обязательства. Уменьшение обеспечения
            IF (   (v_Kind = 0 AND issale = 1)
                OR (v_Kind = 1 AND isbuy = 1)
               )
            THEN
               v_restpaymamount := v_rqamount;
                                      -- необработанное количество комп. взноса.
               --Для каждого комп. ТО, обработать все лоты соотв. РЕПО. Сортировка по убыванию.
               v_break := FALSE;

               WHILE v_restpaymamount > 0 AND v_break = FALSE
               LOOP
                  IF rsb_secur.isbuy (ogrp) = 1
                  THEN
                     BEGIN
                        SELECT *
                          INTO clot
                          FROM (SELECT   *
                                    FROM dsctxlot_dbt txlot
                                   WHERE txlot.t_fiid = cpaym.t_fiid
                                     AND txlot.t_dealid = cpaym.t_docid
                                     AND txlot.t_type IN
                                            (rsb_sctxc.txlots_backrepo,
                                             rsb_sctxc.txlots_loanget
                                            )
                                     AND txlot.t_childid = 0
                                     AND (   txlot.t_saledate >= cpaym.t_factdate
                                          OR txlot.t_saledate =
                                                TO_DATE ('01.01.0001',
                                                         'DD.MM.YYYY'
                                                        )
                                         )
                                ORDER BY txlot.t_begbuydate DESC,
                                         txlot.t_dealdate DESC,
                                         txlot.t_dealtime DESC,
                                         txlot.t_dealsort DESC)
                         WHERE ROWNUM = 1;
                     EXCEPTION
                        WHEN NO_DATA_FOUND
                        THEN
                           BEGIN
                              txputmsg
                                 (0,
                                  v_fiid_in,
                                  txmes_warning,
                                     'Ошибка обработки компенсационной поставки по сделке "'
                                  || v_dealcode
                                  || '" в дату '
                                  || TO_CHAR (cpaym.t_factdate, 'DD.MM.YYYY')
                                 );
                              v_break := TRUE;
                           END;
                     END;
                  ELSE
                     BEGIN
                        SELECT *
                          INTO clot
                          FROM (SELECT   *
                                    FROM dsctxlot_dbt txlot
                                   WHERE txlot.t_fiid = cpaym.t_fiid
                                     AND txlot.t_dealid = cpaym.t_docid
                                     AND txlot.t_type IN
                                            (rsb_sctxc.txlots_loanput,
                                             rsb_sctxc.txlots_repo
                                            )
                                     AND txlot.t_childid = 0
                                     AND (   txlot.t_buydate >= cpaym.t_factdate
                                          OR txlot.t_buydate =
                                                TO_DATE ('01.01.0001',
                                                         'DD.MM.YYYY'
                                                        )
                                         )
                                ORDER BY txlot.t_begsaledate DESC,
                                         txlot.t_dealdate DESC,
                                         txlot.t_dealtime DESC,
                                         txlot.t_dealsort DESC)
                         WHERE ROWNUM = 1;
                     EXCEPTION
                        WHEN NO_DATA_FOUND
                        THEN
                           BEGIN
                              txputmsg
                                 (0,
                                  v_fiid_in,
                                  txmes_warning,
                                     'Ошибка обработки компенсационной поставки по сделке "'
                                  || v_dealcode
                                  || '" в дату '
                                  || TO_CHAR (cpaym.t_factdate, 'DD.MM.YYYY')
                                 );
                              v_break := TRUE;
                           END;
                     END;
                  END IF;

                  EXIT WHEN v_break;

                  -- Если лот скомпенсирован полностью, то установить в нём дату 2ч. На связи (для ПР) и на 2ч (для ОР) признак возврата будет выставлен в createlots. Комп. лот не создавать.
                  IF v_restpaymamount >= clot.t_amount
                  THEN
                     IF clot.t_type IN
                           (rsb_sctxc.txlots_backrepo, rsb_sctxc.txlots_loanget)
                     THEN
                        UPDATE dsctxlot_dbt txlot
                           SET txlot.t_childid = -1,
                               txlot.t_saledate = cpaym.t_factdate,
                               txlot.t_begsaledate = cpaym.t_factdate
                         WHERE txlot.t_id = clot.t_id;
                     ELSE
                        UPDATE dsctxlot_dbt txlot
                           SET txlot.t_childid = -1,
                               txlot.t_buydate = cpaym.t_factdate,
                               txlot.t_begbuydate = cpaym.t_factdate  -- сомнительно
                         WHERE txlot.t_id = clot.t_id;
                     END IF;

                     v_restpaymamount := v_restpaymamount - clot.t_amount;
                  ELSE
                     -- Если лот скомпенсирован не полностью, то оставить старую обработку для него. Создать комп. лот и т.д.
                     v_amount := clot.t_amount;
                     v_buydate := clot.t_buydate;
                     v_saledate := clot.t_saledate;

                     BEGIN
                        SELECT *
                          INTO v_tick
                          FROM ddl_tick_dbt
                         WHERE t_dealid = clot.t_dealid;
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           BEGIN
                              txputmsg
                                 (0,
                                  v_fiid_in,
                                  txmes_warning,
                                     'Ошибка обработки компенсационной поставки по сделке "'
                                  || v_dealcode
                                  || '" в дату '
                                  || TO_CHAR (cpaym.t_factdate, 'DD.MM.YYYY')
                                  || '. Не найдена сделка с DealID = '
                                  || clot.t_dealid
                                 );
                              v_break := TRUE;
                           END;
                     END;

                     v_buydate :=
                        iif (clot.t_type IN
                                (rsb_sctxc.txlots_backrepo,
                                 rsb_sctxc.txlots_loanget
                                ),
                             cpaym.t_factdate,
                             v_buydate
                            );
                     v_saledate :=
                        iif (clot.t_type IN
                                (rsb_sctxc.txlots_backrepo,
                                 rsb_sctxc.txlots_loanget
                                ),
                             v_saledate,
                             cpaym.t_factdate
                            );
                     v_begbuydate :=
                        iif (clot.t_type IN
                                (rsb_sctxc.txlots_backrepo,
                                 rsb_sctxc.txlots_loanget
                                ),
                             clot.t_begbuydate,
                             v_buydate
                            );
                     v_begsaledate :=
                        iif (clot.t_type IN
                                (rsb_sctxc.txlots_backrepo,
                                 rsb_sctxc.txlots_loanget
                                ),
                             v_saledate,
                             clot.t_begsaledate
                            );
                     --можем скомпенсировать на этом лоте: нескомпенсированный остаток v_RestPaymAmount
                     v_amount := v_amount - v_restpaymamount;

                     INSERT INTO dsctxlot_dbt
                                 (t_dealid, t_fiid, t_taxgroup,
                                  t_type, t_beglotid, t_childid, t_iscomp,
                                  t_virtualtype,
                                  t_dealcode,
                                  t_dealcodets,
                                  t_buyid, t_realid,
                                  t_price,
                                  t_pricefiid, t_pricecur,
                                  t_dealdate, t_dealtime, t_buydate,
                                  t_saledate, t_begbuydate, t_begsaledate,
                                  t_retrdate,
                                  t_amount, t_compamount, t_nettingid,
                                  t_oldtype, t_netting, t_sale, t_retflag,
                                  t_inacc, t_isfree, t_ordforsale, t_ordforrepo,
                                  t_ordforsubst, t_ordforclposrepo,
                                  t_dealsortcode, t_rqid
                                 )
                          VALUES (clot.t_dealid, clot.t_fiid, clot.t_taxgroup,
                                  clot.t_type, clot.t_beglotid, 0, CHR (88),
                                  rsb_sctxc.txvdeal_real,
                                  txgetlotcode (clot.t_dealid,
                                                clot.t_fiid,
                                                v_tick.t_dealcode,
                                                rsb_secur.isbasket (ogrp),
                                                rsb_secur.isbuy (ogrp),
                                                kindlot_compdel_minus
                                               ),
                                  txgetlotcode (clot.t_dealid,
                                                clot.t_fiid,
                                                DECODE (v_tick.t_dealcodets,
                                                            CHR (1), v_tick.t_dealcode,
                                                            v_tick.t_dealcodets
                                                   ),
                                                rsb_secur.isbasket (ogrp),
                                                rsb_secur.isbuy (ogrp),
                                                kindlot_compdel_minus
                                            ),
                                  0, 0,
                                  (CASE
                                      WHEN v_legprice2 = 0
                                         THEN v_legprice1
                                      ELSE v_legprice2
                                   END
                                  ),
                                  clot.t_pricefiid, clot.t_pricecur,
                                  clot.t_dealdate, clot.t_dealtime, v_buydate,
                                  v_saledate, v_begbuydate, v_begsaledate,
                                  TO_DATE ('01.01.0001', 'DD.MM.YYYY'),
                                  v_amount, v_restpaymamount, 0,
                                  rsb_sctxc.txlots_undef, 0, 0, CHR (0),
                                  CHR (0), CHR (0), 0, 0,
                                  0, 0,
                                  clot.t_dealsortcode, v_CompRQ.t_id
                                 )
                       RETURNING t_id
                            INTO v_lotid;

                     UPDATE dsctxlot_dbt
                        SET t_childid = v_lotid
                      WHERE t_id = clot.t_id;

                     v_restpaymamount := 0;
                  END IF;
               END LOOP;
            ELSE -- увеличение обеспечения
               --Создать лоты комп. поставки по увеличению обеспечения в ПР и ОР.
               --Перебираем ТО комп. поставки за нужные даты, создаём лоты ПР и ОР.
               BEGIN
                  SELECT *
                    INTO v_tick
                    FROM ddl_tick_dbt
                   WHERE t_dealid = cpaym.t_docid;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     BEGIN
                        txputmsg
                           (0,
                            v_fiid_in,
                            txmes_warning,
                               'Ошибка обработки компенсационной поставки по сделке "'
                            || v_dealcode
                            || '" в дату '
                            || TO_CHAR (cpaym.t_factdate, 'DD.MM.YYYY')
                            || '. Не найдена сделка с DealID = '
                            || cpaym.t_docid
                           );
                        v_break := TRUE;
                     END;
               END;

               BEGIN
                  SELECT *
                    INTO v_leg
                    FROM ddl_leg_dbt
                   WHERE t_dealid = cpaym.t_docid AND t_legid = 0
                         AND t_legkind = 0;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     BEGIN
                        txputmsg
                           (0,
                            v_fiid_in,
                            txmes_warning,
                               'Ошибка обработки компенсационной поставки по сделке "'
                            || v_dealcode
                            || '" в дату '
                            || TO_CHAR (cpaym.t_factdate, 'DD.MM.YYYY')
                            || '. Не найден транш сделки с DealID = '
                            || cpaym.t_docid
                           );
                        v_break := TRUE;
                     END;
               END;

               BEGIN
                  SELECT *
                    INTO v_fin
                    FROM dfininstr_dbt
                   WHERE t_fiid = v_leg.t_cfi;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     BEGIN
                        txputmsg
                           (0,
                            v_fiid_in,
                            txmes_warning,
                               'Ошибка обработки компенсационной поставки по сделке "'
                            || v_dealcode
                            || '" в дату '
                            || TO_CHAR (cpaym.t_factdate, 'DD.MM.YYYY')
                            || '. Не найден финансовый инструмент с FIlID = '
                            || v_leg.t_cfi
                           );
                        v_break := TRUE;
                     END;
               END;

               INSERT INTO dsctxlot_dbt
                           (t_dealid, t_fiid, t_taxgroup,
                            t_type,
                            t_beglotid, t_childid, t_iscomp, t_virtualtype,
                            t_dealcode,
                            t_dealcodets,
                            t_buyid, t_realid, t_dealdate, t_dealtime,
                            t_buydate,
                            t_saledate,
                            t_begbuydate,
                            t_begsaledate,
                            t_retrdate,
                            t_amount, t_compamount, t_nettingid,
                            t_price,
                            t_pricefiid, t_pricecur, t_oldtype, t_netting,
                            t_sale, t_retflag, t_inacc, t_isfree, t_ordforsale,
                            t_ordforrepo, t_ordforsubst, t_ordforclposrepo,
                            t_blocked, t_rqid
                           )
                    VALUES (cpaym.t_docid,                             --T_DEALID
                                          cpaym.t_fiid,                  --T_FIID
                                                       0,            --T_TAXGROUP
                            (CASE
                                WHEN isbuy = 1
                                   THEN rsb_sctxc.txlots_backrepo
                                ELSE rsb_sctxc.txlots_repo
                             END
                            ),                                           --T_TYPE
                            0,               --T_BEGLOTID - заполнится в триггере
                              0,                                      --T_CHILDID
                                CHR (0),                               --T_ISCOMP
                                        rsb_sctxc.txvdeal_real,   --T_VIRTUALTYPE
                            txgetlotcode (cpaym.t_docid,
                                          cpaym.t_fiid,
                                          v_tick.t_dealcode,
                                          rsb_secur.isbasket (ogrp),
                                          rsb_secur.isbuy (ogrp),
                                          kindlot_compdel_plus
                                         ),                          --T_DEALCODE
                            txgetlotcode (cpaym.t_docid,
                                          cpaym.t_fiid,
                                          DECODE (v_tick.t_dealcodets,
                                                        CHR (1), v_tick.t_dealcode,
                                                        v_tick.t_dealcodets
                                               ),
                                          rsb_secur.isbasket (ogrp),
                                          rsb_secur.isbuy (ogrp),
                                          kindlot_compdel_plus
                                        ),                         --T_DEALCODETS
                            0,                                          --T_BUYID
                              0,                                       --T_REALID
                                v_tick.t_dealdate,                   --T_DEALDATE
                                                  v_tick.t_dealtime, --T_DEALTIME
                            (CASE
                                WHEN isbuy = 1
                                   THEN cpaym.t_factdate
                                ELSE TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                             END
                            ),                                        --T_BUYDATE
                            (CASE
                                WHEN isbuy = 1
                                   THEN TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                                ELSE cpaym.t_factdate
                             END
                            ),                                       --T_SALEDATE
                            TO_DATE ('01.01.0001', 'DD.MM.YYYY'),
                            --T_BEGBUYDATE - заполнится в триггере
                            TO_DATE ('01.01.0001', 'DD.MM.YYYY'),
                            --T_BEGSALEDATE - заполнится в триггере
                            TO_DATE ('01.01.0001', 'DD.MM.YYYY'),    --T_RETRDATE
                            v_rqamount,                          --T_AMOUNT
                                             0,                    --T_COMPAMOUNT
                                               0,                   --T_NETTINGID
                            (CASE
                                WHEN    v_leg.t_relativeprice = CHR (0)
                                     OR v_leg.t_relativeprice IS NULL
                                   THEN v_leg.t_price
                                ELSE   rsi_rsb_fiinstr.fi_getnominalondate
                                                                (cpaym.t_fiid,
                                                                 cpaym.t_factdate
                                                                )
                                     * v_leg.t_price
                                     / 100
                             END
                            ),                                          --T_PRICE
                            v_leg.t_cfi, v_fin.t_ccy,                --T_PRICECUR
                                                     0,               --T_OLDTYPE
                                                       0,             --T_NETTING
                            0,                                           --T_SALE
                              CHR (0),                               -- T_RETFLAG
                                      CHR (88),                        -- T_INACC
                                               CHR (0),               -- T_ISFREE
                                                       0,         -- T_ORDFORSALE
                            0,                                    -- T_ORDFORREPO
                              0,                                 -- T_ORDFORSUBST
                                0,                           -- T_ORDFORCLPOSREPO
                            v_tick.t_blocked,                         --T_BLOCKED
                                             v_CompRQ.t_id               --T_RQID
                           )
                 RETURNING t_id
                      INTO v_lotid;

               UPDATE dsctxlot_dbt
                  SET t_beglotid = t_id
                WHERE t_id = v_lotid;

               rsi_txdealsortall;

               --Установить дату по 2 части
               SELECT NVL (MIN (rq.t_factdate),
                           TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                          )
                 INTO v_date2
                 FROM ddlrq_dbt rq
                WHERE rq.t_docid = cpaym.t_docid
                  AND rq.t_dockind = rsb_sctxc.dl_securitydoc
                  AND rq.t_state = rsi_dlrq.dlrq_state_exec
                  AND rq.t_subkind = rsi_dlrq.dlrq_subkind_avoiriss
                  AND rq.t_type = rsi_dlrq.dlrq_type_delivery
                  AND rq.t_dealpart = 2
                  AND rq.t_factdate >= v_begdate_in
                  AND rq.t_factdate <= v_enddate_in;

               IF v_date2 <> TO_DATE ('01.01.0001', 'DD.MM.YYYY')
               THEN
                  IF (isbuy = 0)
                  THEN
                     UPDATE dsctxlot_dbt txlot
                        SET txlot.t_buydate = v_date2,
                            txlot.t_begbuydate = v_date2
                      WHERE txlot.t_id = v_lotid;
                  ELSE
                     UPDATE dsctxlot_dbt txlot
                        SET txlot.t_saledate = v_date2,
                            txlot.t_begsaledate = v_date2
                      WHERE txlot.t_id = v_lotid;
                  END IF;
               END IF;
            END IF;
         END IF;
      END LOOP;
      TXPutMsg( 0,
                v_FIID_in,
                TXMES_OPTIM,
                'Компенсация обработана' );


      --Глобальная операция с ц/б, Изменение номинала ц/б, Перемещение
      FOR DL_COMM IN cDl_COMM
      LOOP

        INSERT INTO DSCTXGO_DBT ( T_ID,
                                  T_DOCKIND,
                                  T_DOCUMENTID,
                                  T_KIND,
                                  T_CODE,
                                  T_SALEDATE,
                                  T_BUYDATE,
                                  T_FIID,
                                  T_OLDFACEVALUE,
                                  T_NEWFACEVALUE
                                )
                         VALUES ( 0,
                                  DL_COMM.T_DOCKIND,
                                  DL_COMM.T_DOCUMENTID,
                                  (CASE WHEN DL_COMM.T_DOCKIND = 135 THEN RSB_SCTXC.TXLOTORIGIN_GLOBCONVERT WHEN DL_COMM.T_DOCKIND = 105 THEN RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER ELSE RSB_SCTXC.TXLOTORIGIN_MODIFFACEVALUE END),
                                  DL_COMM.T_COMMCODE,
                                  DL_COMM.T_COMMDATE,
                                  (CASE WHEN DL_COMM.T_DOCKIND = 135 THEN DL_COMM.T_ENDDATE ELSE DL_COMM.T_COMMDATE END),
                                  DL_COMM.T_FIID,
                                  (CASE WHEN DL_COMM.T_DOCKIND = 135 OR DL_COMM.T_DOCKIND = 105 THEN 0 ELSE DL_COMM.T_HIDDEN_SUM END),
                                  (CASE WHEN DL_COMM.T_DOCKIND = 135 OR DL_COMM.T_DOCKIND = 105 THEN 0 ELSE DL_COMM.T_CURRENCY_SUM  END)
                                );

      END LOOP;


      FOR DL_COMM IN cDl_COMM_GO
      LOOP

        v_All := 1;

        IF v_FIID_in <> -1 THEN
           IF DL_COMM.T_FIID = v_FIID_in AND DL_COMM.T_BEGINDATE >= v_BegDate_in THEN
             v_All := 1;
           ELSE
             v_All := 0;
           END IF;
        ELSE
          IF v_TaxGroup_in <> -1 THEN
            IF DL_COMM.T_TAXGROUP = v_TaxGroup_in AND DL_COMM.T_BEGINDATE >= v_BegDate_in THEN
              v_All := 1;
            ELSE
              v_All := 0;
            END IF;
          END IF;
        END IF;

        FOR SCDLFI IN cSCDLFI(DL_COMM.T_DOCKIND, DL_COMM.T_DOCUMENTID, v_All)
        LOOP
          INSERT INTO DSCTXGOFI_DBT (T_ID,
                                     T_GOID,
                                     T_NUM,
                                     T_NEWFIID,
                                     T_NUMERATOR,
                                     T_DENOMINATOR
                                    )
                             VALUES (0,
                                     (SELECT T_ID FROM DSCTXGO_DBT WHERE T_DOCKIND = DL_COMM.T_DOCKIND AND T_DOCUMENTID = DL_COMM.T_DOCUMENTID),
                                     SCDLFI.T_NUM,
                                     SCDLFI.T_NEWFIID,
                                     SCDLFI.T_NUMERATOR,
                                     SCDLFI.T_DENOMINATOR
                                    );

        END LOOP;

      END LOOP;

      --10.
      UPDATE DSCTXGO_DBT G
         SET G.T_BUYDATE = NVL((SELECT RQ2.T_FACTDATE
                                  FROM DDL_TICK_DBT TK, DDLRQ_DBT RQ2, DAVOIRISS_DBT AVR
                                 WHERE TK.T_BOFFICEKIND = G.T_DOCKIND
                                   AND TK.T_DEALID = G.T_DOCUMENTID
                                   AND TK.T_DEALDATE >= TO_DATE('01.01.2015','DD.MM.YYYY')
                                   AND TK.T_CLIENTID = -1
                                   AND RQ2.T_DOCKIND = TK.T_BOFFICEKIND
                                   AND RQ2.T_DOCID = TK.T_DEALID
                                   AND RQ2.T_DEALPART = 2
                                   AND RQ2.T_TYPE = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                                   AND RQ2.T_FACTDATE >= v_BegDate_in
                                   AND RQ2.T_FACTDATE <= v_EndDate_in
                                   AND RQ2.T_STATE = RSI_DLRQ.DLRQ_STATE_EXEC
                                   AND AVR.T_FIID = TK.T_PFI
                                   AND (v_FIID_in = -1 OR AVR.T_FIID = v_FIID_in)
                                   AND (v_TaxGroup_in = -1 OR AVR.T_TAXGROUP = v_TaxGroup_in)
                                ), G.T_BUYDATE)
       WHERE G.T_DOCKIND = RSB_SCTXC.DL_CONVAVR
         AND G.T_BUYDATE = TO_DATE('01.01.0001','DD.MM.YYYY');

      --11.
      INSERT INTO DSCTXGO_DBT ( T_ID,
                                T_DOCKIND,
                                T_DOCUMENTID,
                                T_KIND,
                                T_CODE,
                                T_SALEDATE,
                                T_BUYDATE,
                                T_FIID,
                                T_OLDFACEVALUE,
                                T_NEWFACEVALUE
                              )
                         SELECT 0,
                                tk.t_BOfficeKind,
                                tk.t_DealID,
                                RSB_SCTXC.TXLOTORIGIN_GLOBCONVERT,
                                tk.t_DealCode,
                                rq1.t_FactDate,
                                rq2.t_FactDate,
                                tk.t_PFI,
                                0,
                                0
                           FROM DDL_TICK_DBT tk, DDLRQ_DBT rq1, DDLRQ_DBT rq2, DAVOIRISS_DBT avr
                          WHERE tk.t_BOfficeKind = RSB_SCTXC.DL_CONVAVR
                            AND tk.t_DealDate >= TO_DATE('01.01.2015','DD.MM.YYYY')
                            AND tk.t_ClientID = -1
                            AND avr.t_FIID = tk.t_PFI
                            AND (v_FIID_in = -1 OR avr.t_FIID = v_FIID_in)
                            AND (v_TaxGroup_in = -1 OR avr.t_TaxGroup = v_TaxGroup_in)
                            AND rq1.t_DocKind = tk.t_BOfficeKind
                            AND rq1.t_DocID = tk.t_DealID
                            AND rq1.t_DealPart = 1
                            AND rq1.t_Type = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                            AND rq1.t_FactDate >= v_BegDate_in
                            AND rq1.t_FactDate <= v_EndDate_in
                            AND rq1.t_State = RSI_DLRQ.DLRQ_STATE_EXEC
                            AND rq2.t_DocKind = tk.t_BOfficeKind
                            AND rq2.t_DocID = tk.t_DealID
                            AND rq2.t_DealPart = 2
                            AND rq2.t_Type = RSI_DLRQ.DLRQ_TYPE_DELIVERY;


      --12.
      FOR one_rec IN (SELECT TK.T_BOFFICEKIND, TK.T_DEALID, TK.T_DEALCODE,
                             rsb_secur.IsConvReceipt(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(TK.T_DEALTYPE, TK.T_BOFFICEKIND))) as IsConvReceipt,
                             AVR_IN.T_NUMBASEFI AS InNumBaseFI, FIN_IN.T_FI_CODE AS InFiCode, FIN_IN.T_FIID AS InFIID,
                             AVR_OUT.T_NUMBASEFI AS OutNumBaseFI, FIN_OUT.T_FI_CODE AS OutFiCode,
                             NVL((SELECT T_ID
                                    FROM DSCTXGO_DBT
                                   WHERE T_DOCKIND    = TK.T_BOFFICEKIND
                                     AND T_DOCUMENTID = TK.T_DEALID), 0) AS GOID
                        FROM DDL_TICK_DBT TK, DDLRQ_DBT RQ2, DFININSTR_DBT FIN_IN, DAVOIRISS_DBT AVR_IN, DFININSTR_DBT FIN_OUT, DAVOIRISS_DBT AVR_OUT
                       WHERE TK.T_BOFFICEKIND = RSB_SCTXC.DL_CONVAVR
                         AND TK.T_DEALDATE >= TO_DATE('01.01.2015','DD.MM.YYYY')
                         AND TK.T_DEALSTATUS = 20 --Закрыт
                         AND TK.T_CLIENTID = -1
                         AND RQ2.T_DOCKIND = TK.T_BOFFICEKIND
                         AND RQ2.T_DOCID = TK.T_DEALID
                         AND RQ2.T_DEALPART = 2
                         AND RQ2.T_TYPE = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                         AND RQ2.T_FACTDATE >= v_BegDate_in
                         AND RQ2.T_FACTDATE <= v_EndDate_in
                         AND AVR_OUT.T_FIID = TK.T_PFI
                         AND FIN_OUT.T_FIID = AVR_OUT.T_FIID
                         AND AVR_IN.T_FIID = RQ2.T_FIID
                         AND FIN_IN.T_FIID = AVR_IN.T_FIID
                         AND (v_FIID_in = -1 OR AVR_OUT.T_FIID = v_FIID_in)
                         AND (v_TaxGroup_in = -1 OR AVR_OUT.T_TAXGROUP = v_TaxGroup_in)
                     )
      LOOP

        IF one_rec.GOID = 0 THEN
          TXPutMsg( 0,
                    v_FIID_in,
                    TXMES_WARNING,
                    'В данных НУ не найдена операция конвертации с кодом "'||one_rec.t_DealCode||'", обработана не будет'
                  );
        ELSE

           IF one_rec.IsConvReceipt <> 0 THEN
             v_AmountDR := one_rec.OutNumBaseFI;
             v_FiCodeDR := one_rec.OutFiCode;
           ELSE
             v_AmountDR := one_rec.InNumBaseFI;
             v_FiCodeDR := one_rec.InFiCode;
           END IF;

           IF v_AmountDR = 0 THEN
             SELECT T_CODE INTO v_GO_CODE FROM DSCTXGO_DBT WHERE T_ID = one_rec.GOID;

             TXPutMsg( 0,
                       v_FIID_in,
                       TXMES_WARNING,
                       'Для депозитарной расписки с кодом "'||v_FiCodeDR||'" не задано количество ц/б. Операция конвертации с кодом "'||v_GO_CODE||'" обработана не будет'
                     );
           ELSE
             IF one_rec.IsConvReceipt <> 0 THEN
               v_Numerator   := v_AmountDR;
               v_Denominator := 1;
             ELSE
               v_Numerator   := 1;
               v_Denominator := v_AmountDR;
             END IF;

             INSERT INTO DSCTXGOFI_DBT (T_ID,
                                        T_GOID,
                                        T_NUM,
                                        T_NEWFIID,
                                        T_NUMERATOR,
                                        T_DENOMINATOR
                                       )
                                VALUES (0,
                                        one_rec.GOID,
                                        1,
                                        one_rec.InFIID,
                                        v_Numerator,
                                        v_Denominator
                                       );

           END IF;

        END IF;

      END LOOP;
      TXPutMsg( 0,
                v_FIID_in,
                TXMES_OPTIM,
                'ГО, конвертация обработаны' );

      RSI_TXDealSortAll;

      TXPutMsg( 0,
                v_FIID_in,
                TXMES_OPTIM,
                'Сортировка выполнена' );

      TXPutMsg( 0,
                v_FIID_in,
                TXMES_DEBUG,
                'Завершение InsertLots' );

      COMMIT;

    END; --InsertLots

    -- получить следующую ближайшую дату для связывания
    FUNCTION TXGetNextDate( v_Date_in DATE, v_TaxGroup_in NUMBER, v_FIID_in NUMBER )
    RETURN DATE
    AS
      v_Date     DATE;
      v_BuyDate  DATE;
      v_SaleDate DATE;
      v_GOBuyDate DATE;
      v_GOSaleDate DATE;
      v_CommDate DATE;
    BEGIN
      v_Date := NULL;

      -- берем min из v_BuyDate, v_SaleDate
      SELECT NVL(MIN(Lot.t_BuyDate), TO_DATE('31.12.9999','DD.MM.YYYY'))
        INTO v_BuyDate
        FROM dsctxlot_dbt Lot
       WHERE Lot.t_BuyDate > v_Date_in
         AND (((v_FIID_in = -1) AND
               (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                         from davoiriss_dbt
                                                        where t_FIID = Lot.t_FIID
                                                      )
               )
              ) OR (v_FIID_in = Lot.t_FIID)
             );

      SELECT NVL(MIN(Lot.t_SaleDate), TO_DATE('31.12.9999','DD.MM.YYYY'))
        INTO v_SaleDate
        FROM dsctxlot_dbt Lot
       WHERE Lot.t_SaleDate > v_Date_in
         AND (((v_FIID_in = -1) AND
               (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                         from davoiriss_dbt
                                                        where t_FIID = Lot.t_FIID
                                                      )
               )
              ) OR (v_FIID_in = Lot.t_FIID)
             );

      SELECT NVL(MIN(txgo.t_SaleDate), TO_DATE('31.12.9999','DD.MM.YYYY'))
        INTO v_GOSaleDate
        FROM dsctxgo_dbt txgo
       WHERE txgo.t_SaleDate > v_Date_in
         AND (((v_FIID_in = -1) AND
               (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                         from davoiriss_dbt
                                                        where t_FIID = txgo.t_FIID
                                                      )
                                   OR Exists(select 1
                                               from davoiriss_dbt av, dsctxgofi_dbt gofi
                                              where gofi.t_GOID = txgo.t_ID
                                                and av.t_FIID = gofi.t_NewFIID
                                                and av.t_TaxGroup = v_TaxGroup_in
                                            )
               )
              ) OR (v_FIID_in = txgo.t_FIID OR v_FIID_in IN (select gofi.t_NewFIID
                                                               from dsctxgofi_dbt gofi
                                                              where gofi.t_GOID = txgo.t_ID
                                                            )
                   )
             );

      SELECT NVL(MIN(txgo.t_BuyDate), TO_DATE('31.12.9999','DD.MM.YYYY'))
        INTO v_GOBuyDate
        FROM dsctxgo_dbt txgo
       WHERE txgo.t_BuyDate > v_Date_in
         AND (((v_FIID_in = -1) AND
               (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                         from davoiriss_dbt
                                                        where t_FIID = txgo.t_FIID
                                                      )
                                   OR Exists(select 1
                                               from davoiriss_dbt av, dsctxgofi_dbt gofi
                                              where gofi.t_GOID = txgo.t_ID
                                                and av.t_FIID = gofi.t_NewFIID
                                                and av.t_TaxGroup = v_TaxGroup_in
                                            )
               )
              ) OR (v_FIID_in = txgo.t_FIID OR v_FIID_in IN (select gofi.t_NewFIID
                                                               from dsctxgofi_dbt gofi
                                                              where gofi.t_GOID = txgo.t_ID
                                                            )
                   )
             );


      SELECT NVL(MIN(CM.T_COMMDATE), TO_DATE('31.12.9999','DD.MM.YYYY')) INTO v_CommDate
        FROM DDL_COMM_DBT CM
       WHERE CM.T_DOCKIND = 105 --Перемещение
         AND CM.T_COMMDATE > v_Date_in
         AND CM.T_COMMSTATUS = 2 --Закрыта
         AND CM.T_OPERSUBKIND <> RSB_SECUR.SUBKIND_UNRETIRE
         AND CM.T_FIID = (CASE WHEN v_FIID_in > 0 THEN v_FIID_in ELSE CM.T_FIID END)
         AND 1 = (CASE WHEN v_TaxGroup_in <= 0 OR (SELECT COUNT(1)
                                                     FROM DAVOIRISS_DBT AV
                                                    WHERE AV.T_FIID = CM.T_FIID
                                                      AND AV.T_TAXGROUP = v_TaxGroup_in) > 0 THEN 1 ELSE 0 END);


      v_Date := LEAST(v_BuyDate, v_SaleDate);
      v_Date := LEAST(v_Date, v_GOBuyDate);
      v_Date := LEAST(v_Date, v_GOSaleDate);
      v_Date := LEAST(v_Date, v_CommDate);

      IF v_Date = TO_DATE('31.12.9999','DD.MM.YYYY') THEN
        v_Date := NULL;
      END IF;

      RETURN v_Date;
    END;

  ----Проверка правильности связывания
    --Идея: Остатки по лотам и по ТОС не должны отличаться
    PROCEDURE TXTestLots( v_BegDate_in IN DATE, v_EndDate_in IN DATE, v_TaxGroup_in NUMBER, v_FIID_in NUMBER )
    IS
      v_Date     DATE;
      v_FIID     NUMBER;
      v_FI_Code  dfininstr_dbt.t_FI_Code%TYPE;
      v_FI_Name  dfininstr_dbt.t_Name%TYPE;
      v_DealCodeTS  dsctxlot_dbt.t_DealCodeTS%TYPE;
      v_DealCodeTS2 dsctxlot_dbt.t_DealCodeTS%TYPE;
      v_Amount1     dsctxlot_dbt.t_Amount%TYPE;
      v_Amount2     dsctxlot_dbt.t_Amount%TYPE;
      v_Type        VARCHAR2(100);
      v_lnkv        DV_SCTXLNK%ROWTYPE;

      TYPE TestLotsCurTyp IS REF CURSOR;
      c_TestLots TestLotsCurTyp;
    BEGIN

      v_FIID := NULL;
      v_FI_Code := NULL;
      v_FI_Name := NULL;

      v_Date := v_BegDate_in;

      ---Тест11 - универсальный
      WHILE v_Date < v_EndDate_in LOOP
        -- этот запрос не должен возвращать строк
        OPEN c_TestLots FOR SELECT s.t_fiid,
                                   s.t_fi_code,
                                   s.t_name
                              FROM dfininstr_dbt s, davoiriss_dbt av,
                                   (SELECT SUM (txrest.t_amount) t_Amount, txrest.t_fiid
                                      FROM dsctxrest_dbt txrest
                                     WHERE txrest.t_buydate <= v_Date
                                       AND txrest.t_buydate > TO_DATE ('01-01-0001', 'DD-MM-YYYY')
                                       AND (  txrest.t_saledate > v_Date
                                           OR txrest.t_saledate = TO_DATE ('01-01-0001', 'DD-MM-YYYY')
                                           )
                                       AND txrest.t_amount > 0
                                  GROUP BY txrest.t_fiid ) qrestost,
                                   (SELECT SUM (tbuy.t_amount) t_Amount, tbuy.t_fiid
                                      FROM dsctxlot_dbt tbuy
                                     WHERE tbuy.t_buydate > TO_DATE ('01-01-0001', 'DD-MM-YYYY')
                                       AND tbuy.t_buydate <= v_Date
                                  GROUP BY tbuy.t_fiid ) qbought,
                                   (SELECT SUM (tsell.t_amount) t_Amount, tsell.t_fiid
                                      FROM dsctxlot_dbt tsell
                                     WHERE tsell.t_saledate > TO_DATE ('01-01-0001', 'DD-MM-YYYY')
                                       AND tsell.t_saledate <= v_Date
                                 GROUP BY tsell.t_fiid ) qsold
                             WHERE s.t_FI_Kind = 2
                               AND av.t_FIID = s.t_FIID
                               AND (v_FIID_in = -1 OR s.t_FIID = v_FIID_in)
                               AND (v_TaxGroup_in = -1 OR av.t_TaxGroup = v_TaxGroup_in)
                               AND qrestost.t_FIID(+) = s.t_fiid
                               AND qbought.t_fiid(+) = s.t_fiid
                               AND qsold.t_fiid(+) = s.t_fiid
                               AND (qrestost.t_Amount + qsold.t_Amount - qbought.t_Amount) <> 0;

        LOOP
          FETCH c_TestLots INTO v_FIID, v_FI_Code, v_FI_Name;
          EXIT WHEN c_TestLots%NOTFOUND OR
                    c_TestLots%NOTFOUND IS NULL;

          IF v_FIID IS NOT NULL THEN
            TXPutMsg( 0,
                      v_FIID,
                      TXMES_TEST,
                      'Проверка 11: Не совпадают остатки по лотам и ТОС по выпуску FIID = '||v_FIID||
                      ' FI_Code = "'||v_FI_Code||'" FI_Name = "'||v_FI_Name||'" на дату '||TO_CHAR(v_Date, 'DD.MM.YYYY') );
          END IF;
        END LOOP;

        CLOSE c_TestLots;

        v_Date := TXGetNextDate( v_Date, v_TaxGroup_in, v_FIID_in );

        EXIT WHEN v_Date IS NULL;

      END LOOP;
      COMMIT;

      ---Тест 12  - - универсальный -строк быть не должно
      -- по лотам продажи должны сформироваться связи на весь объем продаж
      OPEN c_TestLots FOR
        SELECT lot.t_dealcodets, lot.t_saledate, lot.t_amount, sold.qsold, fi.t_fiid, fi.t_fi_code, fi.t_name
          FROM dsctxlot_dbt lot, dfininstr_dbt fi,
               (SELECT l.t_saleid, SUM (l.t_amount) qsold
                  FROM dsctxlnk_dbt l
                 WHERE l.t_type IN (RSB_SCTXC.TXLNK_DELIVER, RSB_SCTXC.TXLNK_NETTING, RSB_SCTXC.TXLNK_OPSREPO, RSB_SCTXC.TXLNK_OPSLOAN)
              GROUP BY l.t_saleid) sold
         WHERE lot.t_type = RSB_SCTXC.TXLOTS_SALE
           AND lot.t_amount <> NVL (sold.qsold, 0)
           AND lot.t_id = sold.t_saleid(+)
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = lot.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = lot.t_FIID)
               )
           AND fi.t_FIID = lot.t_FIID;

      LOOP
        FETCH c_TestLots INTO v_DealCodeTS, v_Date, v_Amount1, v_Amount2, v_FIID, v_FI_Code, v_FI_Name;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        TXPutMsg( 0,
                  v_FIID,
                  TXMES_TEST,
                  'Проверка 12: по лоту продажи не сформированы связи на весь объем продаж. '||
                  'Лот "'||v_DealCodeTS||'" за дату '||TO_CHAR(v_Date, 'DD.MM.YYYY')||'. В лоте '||v_Amount1||
                  ' в связях = '||v_Amount2||'. Ц/б FIID = '||v_FIID||' FI_Code = "'||v_FI_Code||'" FI_Name = "'||v_FI_Name||'"' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      ---Тест 13 - универсальный- аналогично тесту 12, но для прямых репо/размещений займа
      --- строк быть не должно
      -- по лотам прямых репо/размещений займа должны сформироваться связи на весь объем лота
      OPEN c_TestLots FOR
        SELECT lot.t_dealcodets, lot.t_saledate, lot.t_amount, sold.qsold, fi.t_fiid, fi.t_fi_code, fi.t_name
          FROM dsctxlot_dbt lot, dfininstr_dbt fi,
               (SELECT l.t_saleid, SUM (l.t_amount) qsold
                  FROM dsctxlnk_dbt l
                 WHERE l.t_type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_LOANPUT)
              GROUP BY l.t_saleid) sold
         WHERE lot.t_type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
           AND lot.t_amount <> NVL (sold.qsold, 0)
           AND sold.t_saleid(+) = lot.t_id
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = lot.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = lot.t_FIID)
               )
           AND fi.t_FIID = lot.t_FIID;

      LOOP
        FETCH c_TestLots INTO v_DealCodeTS, v_Date, v_Amount1, v_Amount2, v_FIID, v_FI_Code, v_FI_Name;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        TXPutMsg( 0,
                  v_FIID,
                  TXMES_TEST,
                  'Проверка 13: по лоту прямого РПО/размещения займа не сформированы связи на весь объем продаж. '||
                  'Лот "'||v_DealCodeTS||'" за дату '||TO_CHAR(v_Date, 'DD.MM.YYYY')||'. В лоте '||v_Amount1||
                  ' в связях = '||v_Amount2||'. Ц/б FIID = '||v_FIID||' FI_Code = "'||v_FI_Code||'" FI_Name = "'||v_FI_Name||'"' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      ---Тест 14 - универсальный
      --- сумма остатков, начинающихся с источника покупки, должна совпадать с количеством в лоте покупке
      --- строк быть не должно
      OPEN c_TestLots FOR
        SELECT lot.t_dealcodets, lot.t_buydate, lot.t_amount, ost.qost,  fi.t_fiid, fi.t_fi_code, fi.t_name
          FROM dsctxlot_dbt lot, dfininstr_dbt fi,
               (SELECT rest.t_sourceid, SUM (rest.t_amount) qost
                  FROM dsctxrest_dbt rest
                 WHERE rest.t_sourceid = rest.t_buyid
                   AND rest.t_amount > 0
                   AND rest.t_type NOT IN (RSB_SCTXC.TXREST_Open, RSB_SCTXC.TXREST_Closed) --- проверить типы записей об остатке, необходимо исключить ОКП/ЗКП
              GROUP BY rest.t_sourceid) ost
         WHERE lot.t_type IN (RSB_SCTXC.TXLOTS_BUY, RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET)
           AND NVL (ost.qost, 0) <> lot.t_amount
           AND ost.t_sourceid(+) = lot.t_id
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = lot.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = lot.t_FIID)
               )
           AND fi.t_FIID = lot.t_FIID;

      LOOP
        FETCH c_TestLots INTO v_DealCodeTS, v_Date, v_Amount1, v_Amount2, v_FIID, v_FI_Code, v_FI_Name;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        TXPutMsg( 0,
                  v_FIID,
                  TXMES_TEST,
                  'Проверка 14: сумма остатков, начинающихся с источника покупки, не совпадает с количеством в лоте покупке.'||
                  'Лот "'||v_DealCodeTS||'" за дату '||TO_CHAR(v_Date, 'DD.MM.YYYY')||'. В лоте '||v_Amount1||
                  ' в ТОС = '||v_Amount2||'. Ц/б FIID = '||v_FIID||' FI_Code = "'||v_FI_Code||'" FI_Name = "'||v_FI_Name||'"' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      ---Тест 14а - универсальный
      --- сумма остатков, начинающихся с источника покупки, должна совпадать с количеством в лоте покупке - то же, что и 14,
      ----но с другой формулировкой правил отбора
      --- строк быть не должно
      OPEN c_TestLots FOR
        SELECT lot.t_dealcodets, lot.t_buydate, lot.t_amount, ost.qost,  fi.t_fiid, fi.t_fi_code, fi.t_name
          FROM dsctxlot_dbt lot, dfininstr_dbt fi,
               (SELECT rest.t_sourceid, SUM (rest.t_amount) qost
                  FROM dsctxrest_dbt rest
                 WHERE rest.t_sourceid = rest.t_buyid
                   AND rest.t_amount > 0
                   AND rest.t_type NOT IN (RSB_SCTXC.TXREST_Open, RSB_SCTXC.TXREST_Closed) --- проверить типы записей об остатке, необходимо исключить ОКП/ЗКП
              GROUP BY rest.t_sourceid) ost
         WHERE lot.t_type IN (RSB_SCTXC.TXLOTS_BUY, RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET)
           AND NVL (ost.qost, 0) <> lot.t_amount
           AND lot.t_id = ost.t_sourceid(+)
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = lot.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = lot.t_FIID)
               )
           AND fi.t_FIID = lot.t_FIID;

      LOOP
        FETCH c_TestLots INTO v_DealCodeTS, v_Date, v_Amount1, v_Amount2, v_FIID, v_FI_Code, v_FI_Name;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        TXPutMsg( 0,
                  v_FIID,
                  TXMES_TEST,
                  'Проверка 14a: сумма остатков, начинающихся с источника покупки, не совпадает с количеством в лоте покупке.'||
                  'Лот "'||v_DealCodeTS||'" за дату '||TO_CHAR(v_Date, 'DD.MM.YYYY')||'. В лоте '||v_Amount1||
                  ' в ТОС = '||v_Amount2||'. Ц/б FIID = '||v_FIID||' FI_Code = "'||v_FI_Code||'" FI_Name = "'||v_FI_Name||'"' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      ---Тест 15 - универсальный-
      ---сумма остатков, где бумаги уходят впервые в прямое репо/размещенный займ, должна совпадать с количеством в лоте размещения
      --- строк быть не должно
      OPEN c_TestLots FOR
        SELECT lot.t_dealcodets, lot.t_saledate, lot.t_amount, ost.qost,  fi.t_fiid, fi.t_fi_code, fi.t_name
          FROM dsctxlot_dbt lot,  dfininstr_dbt fi,
               (SELECT rest.t_saleid, rest.t_saledate, SUM (rest.t_amount) qost
                  FROM dsctxrest_dbt rest
                 WHERE                           ---rest.t_saleid=rest.t_buyid   and
                       rest.t_type NOT IN (RSB_SCTXC.TXREST_Open, RSB_SCTXC.TXREST_Closed) --- проверить типы записей об остатке, необходимо исключить ОКП/ЗКП
                   AND rest.t_amount > 0
              GROUP BY rest.t_saleid, rest.t_saledate) ost
         WHERE lot.t_type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
           AND NVL (ost.qost, 0) <> lot.t_amount
           AND lot.t_id = ost.t_saleid(+)
           AND lot.t_saledate = ost.t_saledate(+)
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = lot.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = lot.t_FIID)
               )
           AND fi.t_FIID = lot.t_FIID;

      LOOP
        FETCH c_TestLots INTO v_DealCodeTS, v_Date, v_Amount1, v_Amount2, v_FIID, v_FI_Code, v_FI_Name;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        TXPutMsg( 0,
                  v_FIID,
                  TXMES_TEST,
                  'Проверка 15: сумма остатков, где бумаги уходят впервые в прямое репо/размещенный займ, должна совпадать с количеством в лоте размещения. '||
                  'Лот "'||v_DealCodeTS||'" за дату '||TO_CHAR(v_Date, 'DD.MM.YYYY')||'. В лоте '||v_Amount1||
                  ' в ТОС = '||v_Amount2||'. Ц/б FIID = '||v_FIID||' FI_Code = "'||v_FI_Code||'" FI_Name = "'||v_FI_Name||'"' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      --Тест 16 - универсальный- сумма остатков, где бумаги уходят в окончательную продажу (обычную),
      -- должна совпадать с количеством в лоте
      --- строк быть не должно
      OPEN c_TestLots FOR
        SELECT lot.t_dealcodets, n.t_sznamealg, lot.t_saledate, lot.t_amount, ost.qost,  fi.t_fiid, fi.t_fi_code, fi.t_name
          FROM dsctxlot_dbt lot, dfininstr_dbt fi, dnamealg_dbt n,
               (SELECT rest.t_destid, SUM (rest.t_amount) qost
                  FROM dsctxrest_dbt rest
                 WHERE                            --rest.t_destid=rest.t_saleid  and
                       rest.t_type IN (RSB_SCTXC.TXREST_DR_S, RSB_SCTXC.TXREST_B_S, RSB_SCTXC.TXREST_B_U)
                   AND rest.t_amount > 0
              GROUP BY rest.t_destid) ost
         WHERE lot.t_type = RSB_SCTXC.TXLOTS_SALE
           AND NVL (ost.qost, 0) <> lot.t_amount
           AND lot.t_id = ost.t_destid(+)
        --or (lot.t_type in (4, 6) and lot.t_saledate>TO_date('01-01-0001', 'DD-MM-YYYY'))---проверить типы лотов
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = lot.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = lot.t_FIID)
               )
           AND fi.t_FIID = lot.t_FIID
           AND n.t_itypealg = 7300 --ALG_SCTX_DEALKIND
           AND n.t_inumberalg = lot.t_type;

      LOOP
        FETCH c_TestLots INTO v_DealCodeTS, v_Type, v_Date, v_Amount1, v_Amount2, v_FIID, v_FI_Code, v_FI_Name;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        TXPutMsg( 0,
                  v_FIID,
                  TXMES_TEST,
                  'Проверка 16: сумма остатков, где бумаги уходят в окончательную продажу (обычную), не совпадает с количеством в лоте. '||
                  'Лот "'||v_DealCodeTS||'" за дату '||TO_CHAR(v_Date, 'DD.MM.YYYY')||' тип '||v_Type||'. В лоте '||v_Amount1||
                  ' в ТОС = '||v_Amount2||'. Ц/б FIID = '||v_FIID||' FI_Code = "'||v_FI_Code||'" FI_Name = "'||v_FI_Name||'"' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      --Тест 17 - универсальный- сумма остатков, где бумаги уходят в продажу по 2 части,
      -- должна совпадать с количеством в лоте
      --- строк быть не должно
      OPEN c_TestLots FOR
        SELECT lot.t_dealcodets, n.t_sznamealg, lot.t_buydate, lot.t_amount, ost.qost,  fi.t_fiid, fi.t_fi_code, fi.t_name
          FROM dsctxlot_dbt lot, dfininstr_dbt fi, dnamealg_dbt n,
               (SELECT   rest.t_destid, SUM (rest.t_amount) qost
                  FROM dsctxrest_dbt rest
                 WHERE                            --rest.t_destid=rest.t_saleid  and
                       rest.t_type IN (RSB_SCTXC.TXREST_DR_S, RSB_SCTXC.TXREST_B_S, RSB_SCTXC.TXREST_B_U)
                   AND rest.t_amount > 0
              GROUP BY rest.t_destid) ost
         WHERE lot.t_type IN (RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET)
           AND lot.t_saledate > TO_DATE ('01-01-0001', 'DD-MM-YYYY')
           AND NVL (ost.qost, 0) <> lot.t_amount
           AND lot.t_id = ost.t_destid(+)
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = lot.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = lot.t_FIID)
               )
           AND fi.t_FIID = lot.t_FIID
           AND n.t_itypealg = 7300 --ALG_SCTX_DEALKIND
           AND n.t_inumberalg = lot.t_type;

      LOOP
        FETCH c_TestLots INTO v_DealCodeTS, v_Type, v_Date, v_Amount1, v_Amount2, v_FIID, v_FI_Code, v_FI_Name;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        TXPutMsg( 0,
                  v_FIID,
                  TXMES_TEST,
                  'Проверка 17: сумма остатков, где бумаги уходят в продажу по 2 части, не совпадают с количеством в лоте. '||
                  'Лот "'||v_DealCodeTS||'" за дату '||TO_CHAR(v_Date, 'DD.MM.YYYY')||' тип '||v_Type||'. В лоте '||v_Amount1||
                  ' в ТОС = '||v_Amount2||'. Ц/б FIID = '||v_FIID||' FI_Code = "'||v_FI_Code||'" FI_Name = "'||v_FI_Name||'"' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      --Тест 18 - универсальный- остаток "открытие короткой позиции" должна соответсвовать
      ---текущему незакрытому остатку по позиции
      --- строк быть не должно
      OPEN c_TestLots FOR
        SELECT sale.t_dealcodets, src.t_dealcodets, ost.qost,  lnk.qlnk,  fi.t_fiid, fi.t_fi_code, fi.t_name
          FROM dsctxlot_dbt src, dsctxlot_dbt sale, dfininstr_dbt fi,
               (SELECT rest.t_sourceid, rest.t_saleid, SUM (rest.t_amount) qost
                  FROM dsctxrest_dbt rest
                 WHERE
                       rest.t_type IN (RSB_SCTXC.TXREST_Open, RSB_SCTXC.TXREST_Open)
                   AND rest.t_amount > 0
              GROUP BY rest.t_sourceid, rest.t_saleid) ost,
               (SELECT lo.t_buyid, lo.t_saleid,
                       lo.t_amount
                       - NVL ((SELECT SUM (lc.t_amount)
                                 FROM dsctxlnk_dbt lc
                                WHERE lc.t_type IN (RSB_SCTXC.TXLNK_CLSREPO, RSB_SCTXC.TXLNK_CLSLOAN)
                                  AND lc.t_sourceid = lo.t_buyid
                                  AND lc.t_saleid = lo.t_saleid),
                              0
                             ) qlnk
                  FROM dsctxlnk_dbt lo
                 WHERE lo.t_type IN (RSB_SCTXC.TXLNK_OPSREPO, RSB_SCTXC.TXLNK_OPSLOAN)) lnk
         WHERE lnk.t_buyid = src.t_id
           AND lnk.t_saleid = sale.t_id
           AND ost.t_sourceid = src.t_id
           AND ost.t_saleid = sale.t_id
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = sale.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = sale.t_FIID)
               )
           AND fi.t_FIID = sale.t_FIID
           AND ost.qost > 0;

      LOOP
        FETCH c_TestLots INTO v_DealCodeTS, v_DealCodeTS2, v_Amount1, v_Amount2, v_FIID, v_FI_Code, v_FI_Name;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        TXPutMsg( 0,
                  v_FIID,
                  TXMES_TEST,
                  'Проверка 18: остаток "открытие короткой позиции" должен соответсвовать текущему незакрытому остатку по позиции. '||
                  'Лот продажи "'||v_DealCodeTS||'", лот источника "'||v_DealCodeTS2||'". В связях '||v_Amount2||
                  ' в ТОС = '||v_Amount1||'. Ц/б FIID = '||v_FIID||' FI_Code = "'||v_FI_Code||'" FI_Name = "'||v_FI_Name||'"' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      ---Тест 20 - универсальный
      ---проверка корректности связей поставки и неттинг
      -- проверяется дата образования связи, количество, тип связываемых лотов, даты на лотах, заполнение позиций в связи
      OPEN c_TestLots FOR
        SELECT l.*
          FROM DV_SCTXLNK l, dsctxlot_dbt tbuy, dsctxlot_dbt tsell
         WHERE l.t_type IN (RSB_SCTXC.TXLNK_DELIVER, RSB_SCTXC.TXLNK_NETTING)
           AND (   l.t_lot1id <> 0
                OR l.t_lot2id <> 0
                OR l.t_sourceid <> 0
                OR l.t_destid <> 0
                OR l.t_date <> tsell.t_saledate
                OR tsell.t_amount < l.t_amount
                OR tbuy.t_amount < l.t_amount
                OR tbuy.t_buydate > tsell.t_saledate
                OR tsell.t_type <> RSB_SCTXC.TXLOTS_SALE
                OR tbuy.t_type <> RSB_SCTXC.TXLOTS_BUY
               )
           AND l.t_buyid = tbuy.t_id
           AND l.t_saleid = tsell.t_id
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = l.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = l.t_FIID)
               );

      LOOP
        FETCH c_TestLots INTO v_lnkv;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        SELECT n.t_sznamealg
          INTO v_type
          FROM dnamealg_dbt n
         WHERE n.t_itypealg = 7302 --ALG_SCTX_LINKTYPE
           AND n.t_inumberalg = v_lnkv.t_type;

        TXPutMsg( 0,
                  v_lnkv.t_FIID,
                  TXMES_TEST,
                  'Проверка 20: связь типа "'||v_type||'", дата '||TO_CHAR(v_lnkv.t_date, 'DD.MM.YYYY')||', покупка "'||v_lnkv.t_BuyDealCodeTS||
                  '", продажа "'||v_lnkv.t_SaleDealCodeTS||'", исх.пок. "'||v_lnkv.t_SourceDealCodeTS||
                  '", ок.прод. "'||v_lnkv.t_DestDealCodeTS||'", всп.1 "'||v_lnkv.t_Lot1DealCodeTS||
                  '", всп.2 "'||v_lnkv.t_Lot2DealCodeTS||'", выпуск "'||v_lnkv.t_FIName||'"'||
                  '" некорректна. Некорретна дата образования связи или количество, или тип связываемых лотов, '||
                  ' или даты на лотах, или не правильно заполненены позиции в связи' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      ---Тест 21 - универсальный
      ---проверка корректности связей прямое репо/размещение займа
      -- проверяется дата образования связи, количество, тип связываемых лотов, даты на лотах, заполнение позиций в связи
      OPEN c_TestLots FOR
        SELECT l.*
          FROM DV_SCTXLNK l, dsctxlot_dbt tbuy, dsctxlot_dbt tsell
         WHERE l.t_type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_LOANPUT)
           AND (   l.t_lot1id <> 0
                OR l.t_lot2id <> 0
                OR l.t_sourceid <> 0
                OR l.t_destid <> 0
                OR l.t_date <> tsell.t_saledate
                OR tsell.t_amount < l.t_amount
                OR tbuy.t_amount < l.t_amount
                OR tbuy.t_buydate > tsell.t_saledate
                OR (tsell.t_type <> RSB_SCTXC.TXLOTS_REPO AND l.t_type = RSB_SCTXC.TXLNK_DELREPO)
                OR (tsell.t_type <> RSB_SCTXC.TXLOTS_LOANPUT AND l.t_type = RSB_SCTXC.TXLNK_LOANPUT)
                OR tbuy.t_type NOT IN (RSB_SCTXC.TXLOTS_BUY, RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET)
               )
           AND l.t_buyid = tbuy.t_id
           AND l.t_saleid = tsell.t_id
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = l.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = l.t_FIID)
               );

      LOOP
        FETCH c_TestLots INTO v_lnkv;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        SELECT n.t_sznamealg
          INTO v_type
          FROM dnamealg_dbt n
         WHERE n.t_itypealg = 7302 --ALG_SCTX_LINKTYPE
           AND n.t_inumberalg = v_lnkv.t_type;

        TXPutMsg( 0,
                  v_lnkv.t_FIID,
                  TXMES_TEST,
                  'Проверка 21: связь типа "'||v_type||'", дата '||TO_CHAR(v_lnkv.t_date, 'DD.MM.YYYY')||', покупка "'||v_lnkv.t_BuyDealCodeTS||
                  '", продажа "'||v_lnkv.t_SaleDealCodeTS||'", исх.пок. "'||v_lnkv.t_SourceDealCodeTS||
                  '", ок.прод. "'||v_lnkv.t_DestDealCodeTS||'", всп.1 "'||v_lnkv.t_Lot1DealCodeTS||
                  '", всп.2 "'||v_lnkv.t_Lot2DealCodeTS||'", выпуск "'||v_lnkv.t_FIName||'"'||
                  '" некорректна. Некорретна дата образования связи или количество, или тип связываемых лотов, '||
                  ' или даты на лотах, или не правильно заполненены позиции в связи' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      ---Тест 22 - универсальный
      ---проверка корректности связей открытие короткой позиции
      -- проверяется дата образования связи, количество, тип связываемых лотов, даты на лотах, заполнение позиций в связи
      OPEN c_TestLots FOR
        SELECT l.*
          FROM DV_SCTXLNK l, dsctxlot_dbt tbuy, dsctxlot_dbt tsell
         WHERE l.t_type IN (RSB_SCTXC.TXLNK_OPSREPO, RSB_SCTXC.TXLNK_OPSLOAN)
           AND (   l.t_lot1id <> 0
                OR l.t_lot2id <> 0
                OR l.t_sourceid <> 0
                OR l.t_destid <> 0
                OR l.t_date <> tsell.t_saledate
                OR tsell.t_amount < l.t_amount
                OR tbuy.t_amount < l.t_amount
                OR tbuy.t_buydate > tsell.t_saledate
                OR (tbuy.t_type <> RSB_SCTXC.TXLOTS_BACKREPO AND l.t_type = RSB_SCTXC.TXLNK_OPSREPO)
                OR (tbuy.t_type <> RSB_SCTXC.TXLOTS_LOANGET AND l.t_type = RSB_SCTXC.TXLNK_OPSLOAN)
                OR tsell.t_type <> RSB_SCTXC.TXLOTS_SALE
               )
           AND l.t_buyid = tbuy.t_id
           AND l.t_saleid = tsell.t_id
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = l.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = l.t_FIID)
               );

      LOOP
        FETCH c_TestLots INTO v_lnkv;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        SELECT n.t_sznamealg
          INTO v_type
          FROM dnamealg_dbt n
         WHERE n.t_itypealg = 7302 --ALG_SCTX_LINKTYPE
           AND n.t_inumberalg = v_lnkv.t_type;

        TXPutMsg( 0,
                  v_lnkv.t_FIID,
                  TXMES_TEST,
                  'Проверка 22: связь типа "'||v_type||'", дата '||TO_CHAR(v_lnkv.t_date, 'DD.MM.YYYY')||', покупка "'||v_lnkv.t_BuyDealCodeTS||
                  '", продажа "'||v_lnkv.t_SaleDealCodeTS||'", исх.пок. "'||v_lnkv.t_SourceDealCodeTS||
                  '", ок.прод. "'||v_lnkv.t_DestDealCodeTS||'", всп.1 "'||v_lnkv.t_Lot1DealCodeTS||
                  '", всп.2 "'||v_lnkv.t_Lot2DealCodeTS||'", выпуск "'||v_lnkv.t_FIName||'"'||
                  '" некорректна. Некорретна дата образования связи или количество, или тип связываемых лотов, '||
                  ' или даты на лотах, или не правильно заполненены позиции в связи' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      ---Тест 23 - универсальный
      ---проверка корректности связей закрытие короткой позиции
      -- проверяется дата образования связи, количество, тип связываемых лотов, даты на лотах, заполнение позиций в связи
      OPEN c_TestLots FOR
        SELECT l.*
          FROM DV_SCTXLNK l, dsctxlot_dbt tbuy, dsctxlot_dbt tsell, dsctxlot_dbt tsrc
         WHERE l.t_type IN (RSB_SCTXC.TXLNK_CLSREPO, RSB_SCTXC.TXLNK_CLSLOAN)
           AND (   l.t_lot1id <> 0
                OR l.t_lot2id <> 0
                OR l.t_destid <> 0
                OR l.t_date > tsrc.t_saledate
                OR tsell.t_amount < l.t_amount
                OR tbuy.t_amount < l.t_amount
                OR tsrc.t_amount < l.t_amount
                OR (tsrc.t_type <> RSB_SCTXC.TXLOTS_BACKREPO AND l.t_type = RSB_SCTXC.TXLNK_CLSREPO)
                OR (tsrc.t_type <> RSB_SCTXC.TXLOTS_LOANGET AND l.t_type = RSB_SCTXC.TXLNK_CLSLOAN)
                OR tbuy.t_type <> RSB_SCTXC.TXLOTS_BUY
                OR tsell.t_type <> RSB_SCTXC.TXLOTS_SALE
               )
           AND l.t_buyid = tbuy.t_id
           AND l.t_saleid = tsell.t_id
           AND l.t_sourceid = tsrc.t_id
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = l.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = l.t_FIID)
               );

      LOOP
        FETCH c_TestLots INTO v_lnkv;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        SELECT n.t_sznamealg
          INTO v_type
          FROM dnamealg_dbt n
         WHERE n.t_itypealg = 7302 --ALG_SCTX_LINKTYPE
           AND n.t_inumberalg = v_lnkv.t_type;

        TXPutMsg( 0,
                  v_lnkv.t_FIID,
                  TXMES_TEST,
                  'Проверка 23: связь типа "'||v_type||'", дата '||TO_CHAR(v_lnkv.t_date, 'DD.MM.YYYY')||', покупка "'||v_lnkv.t_BuyDealCodeTS||
                  '", продажа "'||v_lnkv.t_SaleDealCodeTS||'", исх.пок. "'||v_lnkv.t_SourceDealCodeTS||
                  '", ок.прод. "'||v_lnkv.t_DestDealCodeTS||'", всп.1 "'||v_lnkv.t_Lot1DealCodeTS||
                  '", всп.2 "'||v_lnkv.t_Lot2DealCodeTS||'", выпуск "'||v_lnkv.t_FIName||'"'||
                  '" некорректна. Некорретна дата образования связи или количество, или тип связываемых лотов, '||
                  ' или даты на лотах, или не правильно заполненены позиции в связи' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      ---Тест 24 - универсальный
      ---проверка корректности связей подстановка репо/займ
      -- проверяется дата образования связи, количество, тип связываемых лотов, даты на лотах, заполнение позиций в связи
      OPEN c_TestLots FOR
        SELECT l.*
          FROM DV_SCTXLNK l, dsctxlot_dbt tbuy, dsctxlot_dbt tsell, dsctxlot_dbt tlot1
         WHERE l.t_type IN (RSB_SCTXC.TXLNK_SUBSTREPO, RSB_SCTXC.TXLNK_SUBSTLOAN)
           AND (   l.t_sourceid <> 0
                OR l.t_lot2id <> 0
                OR l.t_destid <> 0
                OR l.t_date <> tlot1.t_saledate
                OR tsell.t_amount < l.t_amount
                OR tbuy.t_amount < l.t_amount
                OR tlot1.t_amount < l.t_amount
                OR (tsell.t_type <> RSB_SCTXC.TXLOTS_REPO AND l.t_type = RSB_SCTXC.TXLNK_SUBSTREPO)
                OR (tsell.t_type <> RSB_SCTXC.TXLOTS_LOANPUT AND l.t_type = RSB_SCTXC.TXLNK_SUBSTLOAN)
                OR tbuy.t_type NOT IN (RSB_SCTXC.TXLOTS_BUY, RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET)
               )
           AND l.t_buyid = tbuy.t_id
           AND l.t_saleid = tsell.t_id
           AND l.t_lot1id = tlot1.t_id
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = l.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = l.t_FIID)
               );

      LOOP
        FETCH c_TestLots INTO v_lnkv;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        SELECT n.t_sznamealg
          INTO v_type
          FROM dnamealg_dbt n
         WHERE n.t_itypealg = 7302 --ALG_SCTX_LINKTYPE
           AND n.t_inumberalg = v_lnkv.t_type;

        TXPutMsg( 0,
                  v_lnkv.t_FIID,
                  TXMES_TEST,
                  'Проверка 24: связь типа "'||v_type||'", дата '||TO_CHAR(v_lnkv.t_date, 'DD.MM.YYYY')||', покупка "'||v_lnkv.t_BuyDealCodeTS||
                  '", продажа "'||v_lnkv.t_SaleDealCodeTS||'", исх.пок. "'||v_lnkv.t_SourceDealCodeTS||
                  '", ок.прод. "'||v_lnkv.t_DestDealCodeTS||'", всп.1 "'||v_lnkv.t_Lot1DealCodeTS||
                  '", всп.2 "'||v_lnkv.t_Lot2DealCodeTS||'", выпуск "'||v_lnkv.t_FIName||'"'||
                  '" некорректна. Некорретна дата образования связи или количество, или тип связываемых лотов, '||
                  ' или даты на лотах, или не правильно заполненены позиции в связи' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      --Тест 25 - универсальный
      ---проверка корректности связей возврат
      -- проверяется дата образования связи, количество, тип связываемых лотов, даты на лотах, заполнение позиций в связи
      OPEN c_TestLots FOR
        SELECT l.*
          FROM DV_SCTXLNK l, dsctxlot_dbt tbuy, dsctxlot_dbt tsell, dsctxlot_dbt tsrc
         WHERE l.t_type = RSB_SCTXC.TXLNK_DELRET
           AND (   l.t_destid <> 0
                OR (l.t_lot2id = 0 AND l.t_date <> tsell.t_saledate)
                OR tsell.t_amount < l.t_amount
                OR tbuy.t_amount < l.t_amount
                OR tsrc.t_amount < l.t_amount
                OR tsrc.t_type <> RSB_SCTXC.TXLOTS_BUY
                OR tbuy.t_type NOT IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
                OR tsell.t_type NOT IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT, RSB_SCTXC.TXLOTS_SALE)
               )
           AND l.t_buyid = tbuy.t_id
           AND l.t_saleid = tsell.t_id
           AND l.t_sourceid = tsrc.t_id
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = l.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = l.t_FIID)
               );

      LOOP
        FETCH c_TestLots INTO v_lnkv;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        SELECT n.t_sznamealg
          INTO v_type
          FROM dnamealg_dbt n
         WHERE n.t_itypealg = 7302 --ALG_SCTX_LINKTYPE
           AND n.t_inumberalg = v_lnkv.t_type;

        TXPutMsg( 0,
                  v_lnkv.t_FIID,
                  TXMES_TEST,
                  'Проверка 25: связь типа "'||v_type||'", дата '||TO_CHAR(v_lnkv.t_date, 'DD.MM.YYYY')||', покупка "'||v_lnkv.t_BuyDealCodeTS||
                  '", продажа "'||v_lnkv.t_SaleDealCodeTS||'", исх.пок. "'||v_lnkv.t_SourceDealCodeTS||
                  '", ок.прод. "'||v_lnkv.t_DestDealCodeTS||'", всп.1 "'||v_lnkv.t_Lot1DealCodeTS||
                  '", всп.2 "'||v_lnkv.t_Lot2DealCodeTS||'", выпуск "'||v_lnkv.t_FIName||'"'||
                  '" некорректна. Некорретна дата образования связи или количество, или тип связываемых лотов, '||
                  ' или даты на лотах, или не правильно заполненены позиции в связи' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      ---Тест 26 - универсальный
      ---проверка корректности связей  возврат
      -- проверяется дата образования связи, количество, тип связываемых лотов, даты на лотах, заполнение позиций в связи
      OPEN c_TestLots FOR
        SELECT l.*
          FROM DV_SCTXLNK l, dsctxlot_dbt tbuy, dsctxlot_dbt tsell, dsctxlot_dbt tsrc, dsctxlot_dbt tdest
         WHERE l.t_type = RSB_SCTXC.TXLNK_DELRET2
           AND (   l.t_date <> tdest.t_saledate
                OR tsell.t_amount < l.t_amount
                OR tbuy.t_amount < l.t_amount
                OR tsrc.t_amount < l.t_amount
                OR tdest.t_amount < l.t_amount
                OR tdest.t_type <> RSB_SCTXC.TXLOTS_SALE
                OR tsrc.t_type <> RSB_SCTXC.TXLOTS_BUY
                OR tbuy.t_type NOT IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
                OR tsell.t_type NOT IN (RSB_SCTXC.TXLOTS_SALE, RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
               )
           AND l.t_buyid = tbuy.t_id
           AND l.t_saleid = tsell.t_id
           AND l.t_sourceid = tsrc.t_id
           AND l.t_destid = tdest.t_id
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = l.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = l.t_FIID)
               );

      LOOP
        FETCH c_TestLots INTO v_lnkv;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        SELECT n.t_sznamealg
          INTO v_type
          FROM dnamealg_dbt n
         WHERE n.t_itypealg = 7302 --ALG_SCTX_LINKTYPE
           AND n.t_inumberalg = v_lnkv.t_type;

        TXPutMsg( 0,
                  v_lnkv.t_FIID,
                  TXMES_TEST,
                  'Проверка 26: связь типа "'||v_type||'", дата '||TO_CHAR(v_lnkv.t_date, 'DD.MM.YYYY')||', покупка "'||v_lnkv.t_BuyDealCodeTS||
                  '", продажа "'||v_lnkv.t_SaleDealCodeTS||'", исх.пок. "'||v_lnkv.t_SourceDealCodeTS||
                  '", ок.прод. "'||v_lnkv.t_DestDealCodeTS||'", всп.1 "'||v_lnkv.t_Lot1DealCodeTS||
                  '", всп.2 "'||v_lnkv.t_Lot2DealCodeTS||'", выпуск "'||v_lnkv.t_FIName||'"'||
                  '" некорректна. Некорретна дата образования связи или количество, или тип связываемых лотов, '||
                  ' или даты на лотах, или не правильно заполненены позиции в связи' );
      END LOOP;
      CLOSE c_TestLots;
      COMMIT;

      ---Тест 27 - универсальный
      ---проверка корректности связей  возврат
      -- проверяется дата образования связи, количество, тип связываемых лотов, даты на лотах, заполнение позиций в связи
      OPEN c_TestLots FOR
        SELECT l.*
          FROM DV_SCTXLNK l, dsctxlot_dbt tbuy, dsctxlot_dbt tsell, dsctxlot_dbt tsrc, dsctxlot_dbt tdest
         WHERE l.t_type = RSB_SCTXC.TXLNK_RETSPOS
           AND (   (l.t_lot2id = 0 AND l.t_date <> tsell.t_saledate)
                OR l.t_destid <> l.t_sourceid
                OR tsell.t_amount < l.t_amount
                OR tbuy.t_amount < l.t_amount
                OR tsrc.t_amount < l.t_amount
                OR tdest.t_amount < l.t_amount
                OR tdest.t_type NOT IN (RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET)
                OR tsrc.t_type NOT IN (RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET)
                OR tbuy.t_type NOT IN (RSB_SCTXC.TXLOTS_BUY, RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
                OR tsell.t_type NOT IN (RSB_SCTXC.TXLOTS_SALE, RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
               )
           AND l.t_buyid = tbuy.t_id
           AND l.t_saleid = tsell.t_id
           AND l.t_sourceid = tsrc.t_id
           AND l.t_destid = tdest.t_id
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = l.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = l.t_FIID)
               );

      LOOP
        FETCH c_TestLots INTO v_lnkv;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        SELECT n.t_sznamealg
          INTO v_type
          FROM dnamealg_dbt n
         WHERE n.t_itypealg = 7302 --ALG_SCTX_LINKTYPE
           AND n.t_inumberalg = v_lnkv.t_type;

        TXPutMsg( 0,
                  v_lnkv.t_FIID,
                  TXMES_TEST,
                  'Проверка 27: связь типа "'||v_type||'", дата '||TO_CHAR(v_lnkv.t_date, 'DD.MM.YYYY')||', покупка "'||v_lnkv.t_BuyDealCodeTS||
                  '", продажа "'||v_lnkv.t_SaleDealCodeTS||'", исх.пок. "'||v_lnkv.t_SourceDealCodeTS||
                  '", ок.прод. "'||v_lnkv.t_DestDealCodeTS||'", всп.1 "'||v_lnkv.t_Lot1DealCodeTS||
                  '", всп.2 "'||v_lnkv.t_Lot2DealCodeTS||'", выпуск "'||v_lnkv.t_FIName||'"'||
                  '" некорректна. Некорретна дата образования связи или количество, или тип связываемых лотов, '||
                  ' или даты на лотах, или не правильно заполненены позиции в связи' );
      END LOOP;
      CLOSE c_TestLots;

      COMMIT;

      ---Тест б/н - универсальный
      ---проверка на дублирование номеров виртуальных лотов
      -- строк быть не должно
      OPEN c_TestLots FOR
        SELECT lot.t_DealCode, lot.t_DealDate, lot.t_FIID
          FROM dsctxlot_dbt lot,
              ( SELECT l.t_dealdate,  l.t_dealcode, l.t_id
                  FROM dsctxlot_dbt l
                 WHERE l.t_dealid = 0
                   AND (v_FIID_in = -1 OR l.t_FIID = v_FIID_in)
                   AND (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                                 from davoiriss_dbt
                                                                where t_FIID = l.t_FIID
                                                              )
                       )
              ) l
         WHERE lot.t_dealdate = l.t_dealdate
           AND lot.t_dealcode = l.t_dealcode
           AND lot.t_id <> l.t_id
           AND lot.t_dealid = 0
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = lot.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = lot.t_FIID)
               );

      LOOP
        FETCH c_TestLots INTO v_DealCodeTS, v_Date, v_FIID;
        EXIT WHEN c_TestLots%NOTFOUND OR
                  c_TestLots%NOTFOUND IS NULL;

        TXPutMsg( 0,
                  v_FIID,
                  TXMES_TEST,
                  'Проверка: обнаружено дублирование номеров виртуальных лотов.'||
                  ' Лот "'||v_DealCodeTS||'" на дату '||TO_CHAR(v_Date, 'DD.MM.YYYY') );
      END LOOP;
      CLOSE c_TestLots;

      COMMIT;
    END; --TXTestLots

  ----Заполнение таблицы налоговых лотов
    PROCEDURE RSI_TXInsertLots( v_BegDate_in IN DATE, v_EndDate_in IN DATE, v_TaxGroup_in NUMBER, v_FIID_in NUMBER )
    IS
    BEGIN
      GetSettingsTax();

      InsertLots( v_BegDate_in, v_EndDate_in, v_TaxGroup_in, v_FIID_in );

      BEGIN
        UPDATE dregval_dbt
           SET t_fmtblobdata_xxxx = rsb_struct.putString( t_fmtblobdata_xxxx, TO_CHAR(v_EndDate_in,'DD.MM.YYYY'))
         WHERE t_KeyID = rsb_tools.find_regkey('SECUR\DATE_BUILD_TAXREG');

      EXCEPTION
        WHEN OTHERS THEN NULL;
      END;

      COMMIT;

    END;

    PROCEDURE TXRecreateLink(p_OldLnk DSCTXLNK_DBT%ROWTYPE, p_NewSaleID NUMBER, p_NewSaleDate DATE)
    IS
       v_NewLnkID NUMBER;
    BEGIN
       INSERT INTO DSCTXLNK_DBT (t_RetFlag,
                                 t_Date,
                                 t_BegDate,
                                 t_EndDate,
                                 t_SaleID,
                                 t_BuyID,
                                 t_Type,
                                 t_SourceID,
                                 t_DestID,
                                 t_Lot1ID,
                                 t_Lot2ID,
                                 t_FIID,
                                 t_Amount,
                                 t_Short,
                                 t_Ret,
                                 t_Ret2,
                                 t_RetSP
                                ) VALUES
                                (
                                 p_OldLnk.t_RetFlag,
                                 p_NewSaleDate,
                                 p_OldLnk.t_BegDate,
                                 p_OldLnk.t_EndDate,
                                 p_NewSaleID,
                                 p_OldLnk.t_BuyID,
                                 p_OldLnk.t_Type,
                                 p_OldLnk.t_SourceID,
                                 p_OldLnk.t_DestID,
                                 p_OldLnk.t_Lot1ID,
                                 p_OldLnk.t_Lot2ID,
                                 p_OldLnk.t_FIID,
                                 p_OldLnk.t_Amount,
                                 0,
                                 p_OldLnk.t_Ret,
                                 p_OldLnk.t_Ret2,
                                 p_OldLnk.t_RetSP
                                ) RETURNING t_ID INTO v_NewLnkID;

       INSERT INTO DSCTXLS_DBT (t_ChildID,
                                t_ParentID,
                                t_Short
                               ) VALUES
                               (v_NewLnkID,
                                p_OldLnk.t_ID,
                                p_OldLnk.t_Amount
                               );

       COMMIT;

    END;

    PROCEDURE CalcAV( v_Date IN DATE, v_FIID_beg IN NUMBER, v_FIID_end IN NUMBER, v_TaxGroup_in IN NUMBER)
    Is
      v_NewDealCode VARCHAR2(31);
      v_IsExistsNewLot BOOLEAN;
      v_NewLotFromArrID NUMBER;
      v_NewLotID NUMBER;
      v_ArrLotIDNum NUMBER := 0;
      TYPE t_ArrLotID IS TABLE OF NUMBER(10) NOT NULL INDEX BY BINARY_INTEGER;
      v_ArrLotID t_ArrLotID;
      v_FIID NUMBER;
      
      CURSOR c_CompRepo (v_Date IN DATE, v_FIID IN NUMBER) IS
                 SELECT *
                   FROM dsctxlot_dbt Lot
                  WHERE Lot.t_SaleDate = v_Date
                    AND Lot.t_Type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
                    AND Lot.t_FIID = v_FIID
                    AND Lot.t_IsComp = CHR(88);

      CURSOR c_CompBackRepo (v_Date IN DATE, v_FIID IN NUMBER) IS
                 SELECT *
                   FROM dsctxlot_dbt Lot
                  WHERE Lot.t_BuyDate = v_Date
                    AND Lot.t_Type IN (RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET)
                    AND Lot.t_FIID = v_FIID
                    AND Lot.t_IsComp = CHR(88);
                    
      TYPE SaleLotsCurTyp IS REF CURSOR;
      c_SaleLots0 SaleLotsCurTyp;
      c_SaleLots  SaleLotsCurTyp;
      c_SaleLots2 SaleLotsCurTyp;
      c_SaleLots3 SaleLotsCurTyp;
      
      v_SaleLots0 dsctxlot_dbt%ROWTYPE;
      v_SaleLots  dsctxlot_dbt%ROWTYPE;
      v_SaleLots2 dsctxlot_dbt%ROWTYPE;
      v_SaleLots3 dsctxlot_dbt%ROWTYPE;
    BEGIN
        GetSettingsTax();
        FOR v_FI IN (SELECT av.*
                           FROM davoiriss_dbt av
                          WHERE AV.T_FIID >= v_FIID_beg and AV.T_FIID <= v_FIID_end and
                                (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(av2.t_TaxGroup, 0)
                                                                            from davoiriss_dbt av2
                                                                           where av2.t_FIID = av.t_FIID
                                                                         )
                                )
                            AND (EXISTS ( SELECT Lot.t_ID
                                            FROM dsctxlot_dbt Lot
                                           WHERE (Lot.t_BuyDate = v_Date OR Lot.t_SaleDate = v_Date)
                                             AND Lot.t_FIID = av.t_FIID
                                        ) OR
                                 EXISTS ( SELECT txgo.t_ID
                                            FROM dsctxgo_dbt txgo
                                           WHERE (txgo.t_SaleDate = v_Date OR txgo.t_BuyDate = v_Date)
                                             AND (av.t_FIID = txgo.t_FIID OR av.t_FIID IN (select gofi.t_NewFIID
                                                                                             from dsctxgofi_dbt gofi
                                                                                            where gofi.t_GOID = txgo.t_ID
                                                                                          )
                                                 )
                                        ) OR
                                 EXISTS (SELECT 1
                                           FROM DDL_COMM_DBT CM
                                          WHERE CM.T_DOCKIND = 105 --Перемещение
                                            AND CM.T_COMMDATE = v_Date
                                            AND CM.T_COMMSTATUS = 2 --Закрыта
                                            AND CM.T_OPERSUBKIND <> RSB_SECUR.SUBKIND_UNRETIRE
                                            AND CM.T_FIID = av.t_FIID
                                        )
                                )
                        ) LOOP

           v_FIID := v_FI.t_FIID;

           TXPutMsg( 0,
                     v_FIID,
                     TXMES_OPTIM,
                     'Перед установкой t_RetFlag в лотах' );

           UPDATE dsctxlot_dbt lot
              SET lot.t_RetFlag = CHR(88)
            WHERE lot.t_Type IN (RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET)
              AND lot.t_SaleDate = v_Date
              AND lot.t_FIID = v_FIID;

           TXPutMsg( 0,
                     v_FIID,
                     TXMES_DEBUG,
                     'TXCreateLots: перед взведением признака возврата в связях в дату'||v_Date );

           TXPutMsg( 0,
                     v_FIID,
                     TXMES_OPTIM,
                     'TXCreateLots: перед взведением признака возврата в связях РЕПО в дату'||v_Date );

           FOR Lot IN (SELECT sctxlot.*
                       FROM DSCTXLOT_DBT sctxlot
                       WHERE sctxlot.t_Type = RSB_SCTXC.TXLOTS_REPO
                         AND sctxlot.t_BuyDate = v_Date
                         AND sctxlot.t_BuyDate <> sctxlot.t_SaleDate
                         AND sctxlot.t_FIID = v_FIID)
           LOOP
              v_NewDealCode := NVL(RTRIM(rsb_struct.getString(rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(Lot.t_DealID, 34, '0'), 37, v_Date)), CHR(0)), CHR(1));
              IF v_NewDealCode <> chr(1) THEN
              BEGIN
                 SELECT lot2.t_ID INTO v_NewLotID
                  FROM ddl_tick_dbt tick1, ddl_leg_dbt leg1, ddl_tick_dbt tick2, ddl_leg_dbt leg2, dsctxlot_dbt lot2
                 WHERE tick1.t_DealID = Lot.t_DealID
                   AND tick2.t_DealCode = v_NewDealCode
                   AND TICK2.T_BOFFICEKIND = 101
                   AND tick2.t_PartyID = tick1.t_PartyID
                   AND tick2.t_PFI = tick1.t_PFI
                   AND leg1.t_DealID = tick1.t_DealID
                   AND leg1.t_LegKind = 2
                   AND leg1.t_LegID = 0
                   AND leg2.t_DealID = tick2.t_DealID
                   AND leg2.t_LegKind <> 2
                   AND leg2.t_LegID = 0
                   AND (CASE WHEN leg2.t_MaturityIsPrincipal = 'X' THEN leg2.t_Maturity ELSE leg2.t_Expiry END) =
                       (CASE WHEN leg1.t_MaturityISPrincipal = 'X' THEN leg1.t_Maturity ELSE leg1.t_Expiry END)
                   AND leg2.t_Principal = leg1.t_Principal
                   AND LOT2.T_DEALID = tick2.t_DealID
                   AND LOT2.t_Amount = leg2.t_Principal
                   AND ROWNUM = 1;

              EXCEPTION
                 WHEN NO_DATA_FOUND
                 THEN
                    TXPutMsg( Lot.t_ID,
                              v_FIID,
                              TXMES_WARNING,
                              'Для сделки с кодом ' || Lot.t_DealCode || ' по примечанию "Код связанной сделки" не найдена связанная сделка ' || v_NewDealCode || ', соответствующая параметрам исходной сделки');

                    UPDATE dsctxlnk_dbt lnk
                      SET lnk.t_RetFlag = CHR(88)
                    WHERE lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO, RSB_SCTXC.TXLNK_LOANPUT, RSB_SCTXC.TXLNK_SUBSTLOAN)
                      AND Exists(SELECT lot.t_Type
                                  FROM dsctxlot_dbt lot
                                 WHERE lot.t_ID = lnk.t_SaleID
                                   AND lot.t_Type = RSB_SCTXC.TXLOTS_REPO
                                   AND lot.t_BuyDate = v_Date
                                   AND lot.t_BuyDate <> lot.t_SaleDate -- однодневки пропускаем
                                   AND lot.t_FIID = v_FIID
                                ) AND
                            lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO);
              END;

                 FOR Lnk IN (SELECT *
                              FROM dsctxlnk_dbt sctxlnk
                             WHERE sctxlnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO)
                               AND sctxlnk.t_SaleID = Lot.t_ID)
                 LOOP
                    v_IsExistsNewLot := FALSE;
                    v_NewLotFromArrID := v_ArrLotID.First();
                    WHILE(v_NewLotFromArrID IS NOT NULL)
                    LOOP
                       IF v_ArrLotID(v_NewLotFromArrID) = v_NewLotID
                       THEN
                          v_IsExistsNewLot := TRUE;
                       ELSE
                          v_IsExistsNewLot := FALSE;
                       END IF;

                       v_NewLotFromArrID := v_ArrLotID.Next(v_NewLotFromArrID);

                       EXIT WHEN v_IsExistsNewLot = TRUE;
                    END LOOP;

                    IF v_IsExistsNewLot = FALSE
                    THEN


                       UPDATE dsctxlnk_dbt sctxlnk
                        SET sctxlnk.t_RetFlag = CHR(88)
                       WHERE sctxlnk.t_ID = Lnk.t_ID;

                       TXRecreateLink(Lnk, v_NewLotID, v_Date);
                    END IF;
                 END LOOP;

                 v_IsExistsNewLot := FALSE;
                 v_NewLotFromArrID := v_ArrLotID.First();
                 WHILE(v_NewLotFromArrID IS NOT NULL)
                 LOOP
                    IF v_ArrLotID(v_NewLotFromArrID) = v_NewLotID
                    THEN
                       v_IsExistsNewLot := TRUE;
                    ELSE
                       v_IsExistsNewLot := FALSE;
                    END IF;

                    v_NewLotFromArrID := v_ArrLotID.Next(v_NewLotFromArrID);

                    EXIT WHEN v_IsExistsNewLot = TRUE;
                 END LOOP;
                 IF v_IsExistsNewLot = FALSE
                 THEN
                    v_ArrLotIDNum := v_ArrLotIDNum + 1;
                    v_ArrLotID(v_ArrLotIDNum) := v_NewLotID;
                 END IF;
              ELSE
                 UPDATE dsctxlnk_dbt lnk
                   SET lnk.t_RetFlag = CHR(88)
                 WHERE lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO, RSB_SCTXC.TXLNK_LOANPUT, RSB_SCTXC.TXLNK_SUBSTLOAN)
                   AND Exists(SELECT lot.t_Type
                               FROM dsctxlot_dbt lot
                              WHERE lot.t_ID = lnk.t_SaleID
                                AND lot.t_Type = RSB_SCTXC.TXLOTS_REPO
                                AND lot.t_BuyDate = v_Date
                                AND lot.t_BuyDate <> lot.t_SaleDate -- однодневки пропускаем
                                AND lot.t_FIID = v_FIID
                             ) AND
                         lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO);
              END IF;
           END LOOP;

           TXPutMsg( 0,
                     v_FIID,
                     TXMES_OPTIM,
                     'TXCreateLots: перед взведением признака возврата в связях займа в дату'||v_Date );

           UPDATE dsctxlnk_dbt lnk
              SET lnk.t_RetFlag = CHR(88)
            WHERE Exists(SELECT lot.t_Type
                           FROM dsctxlot_dbt lot
                          WHERE lot.t_ID = lnk.t_SaleID
                            AND lot.t_Type = RSB_SCTXC.TXLOTS_LOANPUT
                            AND lot.t_BuyDate = v_Date
                            AND lot.t_BuyDate <> lot.t_SaleDate -- однодневки пропускаем
                            AND lot.t_FIID = v_FIID
                        ) AND
                  lnk.t_Type IN (RSB_SCTXC.TXLNK_LOANPUT, RSB_SCTXC.TXLNK_SUBSTLOAN);

           IF ReestrValue.V14 = RSB_SCTXC.TXREG_V14_YES THEN
             TXPutMsg( 0,
                     v_FIID,
                     TXMES_DEBUG,
                     'Перед обработкой перемещений за дату '||v_Date );


             FOR G IN (SELECT *
                         FROM DSCTXGO_DBT
                        WHERE T_KIND = RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER
                          AND T_SALEDATE = v_Date
                          AND T_FIID = v_FIID
                      )
             LOOP
               RSI_TXProcessMoving(G);
             END LOOP;
           END IF;

           TXPutMsg( 0,
                     v_FIID,
                     TXMES_OPTIM,
                     'После обработки перемещений' );

           FOR CompRepo IN c_CompRepo(v_Date, v_FIID) LOOP
             TXProcessCompPayOnDirectRepo(CompRepo);
           END LOOP;

           IF ReestrValue.V11 = RSB_SCTXC.TXREG_V11_YES THEN
             TXShuffling(v_Date, v_FIID);
           END IF;

           IF ReestrValue.V9 = RSB_SCTXC.TXREG_V9_NO THEN
              IF ReestrValue.V20 = RSB_SCTXC.TXREG_V20_DESC THEN
                OPEN c_SaleLots0 FOR SELECT *
                                       FROM dsctxlot_dbt lot
                                      WHERE lot.t_SaleDate = v_Date
                                        AND lot.t_SaleDate = lot.t_BuyDate
                                        AND lot.t_ChildID = 0
                                        AND lot.t_Type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
                                        AND lot.t_FIID = v_FIID
                                   ORDER BY TXGetSaleOrder(lot.t_Type) ASC,
                                            lot.t_BegSaleDate DESC,
                                            lot.t_DealDate DESC,
                                            lot.t_DealTime DESC,
                                            lot.t_DealSort DESC;
              ELSE
                OPEN c_SaleLots0 FOR SELECT *
                                       FROM dsctxlot_dbt lot
                                      WHERE lot.t_SaleDate = v_Date
                                        AND lot.t_SaleDate = lot.t_BuyDate
                                        AND lot.t_ChildID = 0
                                        AND lot.t_Type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
                                        AND lot.t_FIID = v_FIID
                                   ORDER BY TXGetSaleOrder(lot.t_Type) ASC,
                                            lot.t_BegSaleDate ASC,
                                            lot.t_DealDate ASC,
                                            lot.t_DealTime ASC,
                                            lot.t_DealSort ASC;
              END IF;

              LOOP
                FETCH c_SaleLots0 INTO v_SaleLots0;
                EXIT WHEN c_SaleLots0%NOTFOUND OR
                          c_SaleLots0%NOTFOUND IS NULL;

                RSI_TXLinkDirectRepo(v_SaleLots0);
              END LOOP;

              CLOSE c_SaleLots0;
           END IF;

           OPEN c_SaleLots FOR SELECT *
                                 FROM dsctxlot_dbt lot
                                WHERE lot.t_SaleDate = v_Date
                                  AND lot.t_Type = RSB_SCTXC.TXLOTS_SALE
                                  AND lot.t_Origin IN (RSB_SCTXC.TXLOTORIGIN_DEAL, RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER)
                                  AND lot.t_FIID = v_FIID
                             ORDER BY lot.t_BegSaleDate ASC,
                                      lot.t_DealDate ASC,
                                      lot.t_DealTime ASC,
                                      lot.t_DealSort ASC;

           LOOP
             FETCH c_SaleLots INTO v_SaleLots;
             EXIT WHEN c_SaleLots%NOTFOUND OR
                       c_SaleLots%NOTFOUND IS NULL;

             RSI_TXLinkSale(v_SaleLots);
           END LOOP;

           CLOSE c_SaleLots;

           TXCloseShortPos(v_Date, v_FIID);

           FOR CompBackRepo IN c_CompBackRepo(v_Date, v_FIID) LOOP
             TXProcessCompPayOnReverseRepo(CompBackRepo);
           END LOOP;

           OPEN c_SaleLots2 FOR SELECT *
                                  FROM dsctxlot_dbt lot
                                 WHERE lot.t_SaleDate = v_Date
                                   AND lot.t_Type IN (RSB_SCTXC.TXLOTS_LOANGET, RSB_SCTXC.TXLOTS_BACKREPO)
                                   AND lot.t_FIID = v_FIID
                              ORDER BY TXGetPart2Order(lot.t_Type) ASC,
                                       lot.t_BegSaleDate ASC,
                                       lot.t_DealDate ASC,
                                       lot.t_DealTime ASC,
                                       lot.t_DealSort ASC;

           LOOP
             FETCH c_SaleLots2 INTO v_SaleLots2;
             EXIT WHEN c_SaleLots2%NOTFOUND OR
                       c_SaleLots2%NOTFOUND IS NULL;

             -- Выполнить принудительное ЗКП
             IF ((v_SaleLots2.t_Blocked = CHR(0)) OR (ReestrValue.V10 = RSB_SCTXC.TXREG_V10_YES)) THEN
                RSI_TXLinkPart2ToBuy(v_SaleLots2);
             END IF;

             RSI_TXUpdatePart2Rest (v_SaleLots2.t_ID, v_Date);

           END LOOP;

           CLOSE c_SaleLots2;

            IF ReestrValue.V20 = RSB_SCTXC.TXREG_V20_DESC THEN
              OPEN c_SaleLots3 FOR SELECT *
                                     FROM dsctxlot_dbt lot
                                    WHERE lot.t_SaleDate = v_Date
                                      AND ((lot.t_SaleDate != lot.t_BuyDate) or (lot.t_SaleDate = lot.t_BuyDate and lot.t_ChildID <> 0))
                                      AND lot.t_Type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
                                      AND lot.t_FIID = v_FIID
                                 ORDER BY TXGetSaleOrder(lot.t_Type) ASC,
                                          lot.t_BegSaleDate DESC,
                                          lot.t_DealDate DESC,
                                          lot.t_DealTime DESC,
                                          lot.t_DealSort DESC;
            ELSE
              OPEN c_SaleLots3 FOR SELECT *
                                     FROM dsctxlot_dbt lot
                                    WHERE lot.t_SaleDate = v_Date
                                      AND ((lot.t_SaleDate != lot.t_BuyDate) or (lot.t_SaleDate = lot.t_BuyDate and lot.t_ChildID <> 0))
                                      AND lot.t_Type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
                                      AND lot.t_FIID = v_FIID
                                 ORDER BY TXGetSaleOrder(lot.t_Type) ASC,
                                          lot.t_BegSaleDate ASC,
                                          lot.t_DealDate ASC,
                                          lot.t_DealTime ASC,
                                          lot.t_DealSort ASC;
            END IF;

           LOOP
             FETCH c_SaleLots3 INTO v_SaleLots3;
             EXIT WHEN c_SaleLots3%NOTFOUND OR
                       c_SaleLots3%NOTFOUND IS NULL;

             v_IsExistsNewLot := FALSE;
             v_NewLotFromArrID := v_ArrLotID.First();
             WHILE(v_NewLotFromArrID IS NOT NULL)
             LOOP
                IF v_ArrLotID(v_NewLotFromArrID) = v_NewLotID
                THEN
                   v_IsExistsNewLot := TRUE;
                ELSE
                   v_IsExistsNewLot := FALSE;
                END IF;

                v_NewLotFromArrID := v_ArrLotID.Next(v_NewLotFromArrID);

                EXIT WHEN v_IsExistsNewLot = TRUE;
             END LOOP;
             IF v_IsExistsNewLot = FALSE
             THEN
                RSI_TXLinkDirectRepo(v_SaleLots3);
             END IF;
           END LOOP;

           CLOSE c_SaleLots3;

           TXPutMsg( 0,
                     v_FIID,
                     TXMES_DEBUG,
                     'TXCreateLots: перед взведением признака возврата в связях однодневок в дату'||v_Date );

           TXPutMsg( 0,
                     v_FIID,
                     TXMES_OPTIM,
                     'TXCreateLots: перед взведением признака возврата в связях однодневок в дату'||v_Date );

           -- однодневки обновляем
           UPDATE dsctxlnk_dbt lnk
              SET lnk.t_RetFlag = CHR(88)
            WHERE lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_LOANPUT)
              AND lnk.t_RetFlag = CHR(0)
              AND ( ( Exists(SELECT lot.t_Type
                               FROM dsctxlot_dbt lot
                              WHERE lot.t_ID = lnk.t_SaleID
                                AND lot.t_Type = RSB_SCTXC.TXLOTS_REPO
                                AND lot.t_BuyDate = v_Date
                                AND lot.t_BuyDate = lot.t_SaleDate
                                AND lot.t_FIID = v_FIID
                            ) AND
                      lnk.t_Type = RSB_SCTXC.TXLNK_DELREPO
                    ) OR
                    ( Exists(SELECT lot.t_Type
                               FROM dsctxlot_dbt lot
                              WHERE lot.t_ID = lnk.t_SaleID
                                AND lot.t_Type = RSB_SCTXC.TXLOTS_LOANPUT
                                AND lot.t_BuyDate = v_Date
                                AND lot.t_BuyDate = lot.t_SaleDate
                                AND lot.t_FIID = v_FIID
                            ) AND
                      lnk.t_Type = RSB_SCTXC.TXLNK_LOANPUT
                    ) );
        END LOOP;
        
        COMMIT;
    END;

  ----Заполняет таблицы налогового учета
    PROCEDURE TXCreateLots( v_BegDate_in IN DATE, v_EndDate_in IN DATE, v_TaxGroup_in NUMBER, v_FIID_in NUMBER, v_IsDebug_in NUMBER DEFAULT 0, v_IsOptim_in NUMBER DEFAULT 0, v_IsRecalc_in NUMBER DEFAULT 0, v_Parallel_in NUMBER DEFAULT 0 )
    IS
      v_BegDate   DATE;
      v_EndDate   DATE;
      v_CloseDate DATE;
      v_Type      NUMBER;
      v_Date      DATE;
      v_Proc      NUMBER;
      v_SaleLots0 dsctxlot_dbt%ROWTYPE;
      v_SaleLots  dsctxlot_dbt%ROWTYPE;
      v_SaleLots2 dsctxlot_dbt%ROWTYPE;
      v_SaleLots3 dsctxlot_dbt%ROWTYPE;
      v_Count     NUMBER;
      v_FICODE    dfininstr_dbt.t_FI_Code%TYPE;
      v_FIID      NUMBER;

      TYPE SaleLotsCurTyp IS REF CURSOR;
      c_SaleLots0 SaleLotsCurTyp;
      c_SaleLots  SaleLotsCurTyp;
      c_SaleLots2 SaleLotsCurTyp;
      c_SaleLots3 SaleLotsCurTyp;
      v_Fin       dfininstr_dbt%ROWTYPE;
      v_RDate     DATE;
      v_GDate     DATE;
      v_OldDealID NUMBER;
      v_NewDealCode VARCHAR2(31);
      v_NewLotID NUMBER;
      v_NoteTextLen NUMBER;
      v_IsExistsNewLot BOOLEAN;
      v_NewLotFromArrID NUMBER;

      TYPE t_ArrLotID IS TABLE OF NUMBER(10) NOT NULL INDEX BY BINARY_INTEGER;
      v_ArrLotID t_ArrLotID;
      v_ArrLotIDNum NUMBER := 0;

      CURSOR CompLots(BegDate IN DATE) IS
                         SELECT lot.t_ID
                           FROM dsctxlot_dbt lot
                          WHERE lot.t_IsComp = CHR(88)
                            AND lot.t_InAcc  = CHR(88)
                            AND ( ( lot.T_TYPE IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT) AND
                                    lot.T_SALEDATE >= BegDate
                                  ) OR
                                  ( lot.T_TYPE IN (RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET) AND
                                    lot.T_BUYDATE >= BegDate
                                  )
                                )
                            AND (((v_FIID_in = -1) AND
                                  (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                                            from davoiriss_dbt
                                                                           where t_FIID = lot.t_FIID
                                                                         )
                                  )
                                 ) OR (v_FIID_in = lot.t_FIID)
                                );

      CURSOR LotsSalePart2(BegDate IN DATE) IS
             SELECT Lot.t_ID, Lot.t_SaleDate
               FROM dsctxlot_dbt Lot
              WHERE (Lot.t_Type IN ( RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET ))
                AND (Lot.t_SaleDate >= BegDate OR Lot.t_BegSaleDate >= BegDate)
                AND (((v_FIID_in = -1) AND
                      (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                                from davoiriss_dbt
                                                               where t_FIID = Lot.t_FIID
                                                             )
                      )
                     ) OR (v_FIID_in = Lot.t_FIID)
                    );

      CURSOR LotsBuyPart2(BegDate IN DATE) IS
             SELECT Lot.t_ID, Lot.t_BuyDate
               FROM dsctxlot_dbt Lot
              WHERE (Lot.t_Type IN ( RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT ))
                AND (Lot.t_BuyDate >= BegDate OR Lot.t_BegBuyDate >= BegDate)
                AND (((v_FIID_in = -1) AND
                      (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                                from davoiriss_dbt
                                                               where t_FIID = Lot.t_FIID
                                                             )
                      )
                     ) OR (v_FIID_in = Lot.t_FIID)
                    );

      CURSOR C_GlobalOper(BegDate IN DATE, EndDate IN DATE) IS
             SELECT comm.*
               FROM DDL_COMM_DBT comm, DAVOIRISS_DBT av
              WHERE comm.t_DocKind = 135
                AND comm.t_CommDate >= BegDate
                AND comm.t_CommDate <= EndDate
                AND comm.t_FIID      = av.t_FIID
                AND (v_FIID_in = -1 OR comm.t_FIID = v_FIID_in)
                AND (v_TaxGroup_in = -1 OR av.t_TaxGroup = v_TaxGroup_in);

      CURSOR c_CompRepo (v_Date IN DATE, v_FIID IN NUMBER) IS
                 SELECT *
                   FROM dsctxlot_dbt Lot
                  WHERE Lot.t_SaleDate = v_Date
                    AND Lot.t_Type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
                    AND Lot.t_FIID = v_FIID
                    AND Lot.t_IsComp = CHR(88);

      CURSOR c_CompBackRepo (v_Date IN DATE, v_FIID IN NUMBER) IS
                 SELECT *
                   FROM dsctxlot_dbt Lot
                  WHERE Lot.t_BuyDate = v_Date
                    AND Lot.t_Type IN (RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET)
                    AND Lot.t_FIID = v_FIID
                    AND Lot.t_IsComp = CHR(88);

      CURSOR cSCTXGOLotRepo IS
             SELECT G.T_SALEDATE, G.T_FIID, G.T_BUYDATE, G.T_DOCKIND
               FROM DSCTXGO_DBT G
              WHERE G.T_SALEDATE >= v_BegDate
                AND G.T_SALEDATE <= v_EndDate
                AND G.T_KIND <> RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER
                AND (((v_FIID_in = -1) AND
                      (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                                from davoiriss_dbt
                                                               where t_FIID = G.t_FIID
                                                             )
                      )
                     ) OR (v_FIID_in = G.t_FIID)
                    );


       CURSOR cLotsSaleDate(FIID IN NUMBER, SaleDate IN DATE, BuyDate IN DATE) IS
              SELECT R.T_SALEDATE, R.T_BUYDATE, R.T_TYPE, R.T_DEALCODETS FROM DSCTXLOT_DBT R
               WHERE R.T_FIID = FIID
                 AND 1 = (CASE WHEN (R.T_TYPE IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT) AND
                                     R.T_SALEDATE <= BuyDate AND
                                     (R.T_BUYDATE >= SaleDate OR R.T_BUYDATE = TO_DATE('01.01.0001','DD.MM.YYYY') OR R.T_BUYDATE IS NULL)) OR
                                    (R.T_TYPE IN (RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET) AND
                                     R.T_BUYDATE <= BuyDate AND
                                     (R.T_SALEDATE >= SaleDate OR R.T_SALEDATE = TO_DATE('01.01.0001','DD.MM.YYYY') OR R.T_SALEDATE IS NULL)) THEN 1
                               ELSE 0 END);

      TYPE WrtInLots IS REF CURSOR;
      c_WrtInLots WrtInLots;
      v_WrtInLots DSCTXLOT_DBT%ROWTYPE;

      v_task_name VARCHAR2(30);
      v_sql_chunks CLOB;
      v_sql_process VARCHAR2(400);
      v_try NUMBER(5) := 0;
      v_status NUMBER;
    BEGIN

      RSI_BeginCalculate( 'Формирование налоговых связей' );

      gl_WasError    := FALSE;

      IF v_IsDebug_in = 0 THEN
        gl_IsDebug     := false;
      ELSE
        gl_IsDebug     := true;
      END IF;

      IF v_IsOptim_in = 0 THEN
        gl_IsOptim     := false;
      ELSE
        gl_IsOptim     := true;
      END IF;

     IF v_Parallel_in > 0 THEN
       RunParallel := v_Parallel_in;
     ELSE
       RunParallel := 0;
     END IF;

      --Проверить даты
      BEGIN
        SELECT TO_DATE(rsb_struct.getString(t_fmtblobdata_xxxx),'DD.MM.YYYY') + 1
          INTO v_BegDate
          FROM dregval_dbt
         WHERE t_KeyID = rsb_tools.find_regkey('SECUR\DATE_BUILD_TAXREG');

      EXCEPTION
        WHEN OTHERS
          THEN v_BegDate := TO_DATE('01.01.0001','DD.MM.YYYY');
      END;

      BEGIN
        SELECT TO_DATE(rsb_struct.getString(t_fmtblobdata_xxxx),'DD.MM.YYYY')
          INTO v_CloseDate
          FROM dregval_dbt
         WHERE t_KeyID = rsb_tools.find_regkey('SECUR\DATE_CLOSE_TAXREG');

      EXCEPTION
        WHEN OTHERS
          THEN v_CloseDate := TO_DATE('01.01.0001','DD.MM.YYYY');
      END;

      v_BegDate := iif( v_BegDate < v_BegDate_in, v_BegDate, v_BegDate_in );
      v_EndDate := v_EndDate_in;

      TXPutCurrActMsg( 'Выполняется подготовка данных...' );

      TXPutMsg( 0,
                v_FIID_in,
                TXMES_MESSAGE,
                'Начало '||iif( v_IsRecalc_in != 0, 'пересчета', 'расчета' )||
                ' налоговых связей с даты '||TO_CHAR(v_BegDate, 'DD.MM.YYYY')||
                ' по дату '||TO_CHAR(v_EndDate, 'DD.MM.YYYY') );

      IF v_BegDate <= v_CloseDate AND
         v_BegDate != TO_DATE('01.01.0001','DD.MM.YYYY') AND
         v_CloseDate != TO_DATE('01.01.0001','DD.MM.YYYY') THEN

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_ERROR,
                  'Попытка '||iif( v_IsRecalc_in != 0, 'пересчета', 'расчета' )||
                  ' налоговых регистров по закрытому налоговому периоду' );

        RETURN;
      END IF;

      IF v_BegDate > v_EndDate THEN

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_ERROR,
                  'Неверный диапазон дат' );

        RETURN;
      END IF;

      BEGIN
        IF v_FIID_in = -1 and v_TaxGroup_in = -1 THEN
          UPDATE dregval_dbt
             SET t_fmtblobdata_xxxx = rsb_struct.putString(t_fmtblobdata_xxxx, TO_CHAR(v_BegDate,'DD.MM.YYYY'))
           WHERE t_KeyID = rsb_tools.find_regkey('SECUR\DATE_BUILD_TAXREG');
        END IF;

      EXCEPTION
        WHEN OTHERS THEN NULL;
      END;

      IF v_BegDate = TO_DATE('01.01.0001','DD.MM.YYYY') THEN
        v_BegDate := TO_DATE('01.01.0001','DD.MM.YYYY') + 1;
        COMMIT;
      END IF;

      GetSettingsTax();

      --Очистить содержимое таблиц за период, начиная с дат пересчета:

      TXPutMsg( 0,
                v_FIID_in,
                TXMES_DEBUG,
                'Чистим таблицы' );

      TXPutMsg( 0,
                v_FIID_in,
                TXMES_OPTIM,
                'Перед чисткой таблиц' );

      IF (v_BegDate - 1) = TO_DATE('01-01-0001','DD-MM-YYYY') AND v_FIID_in = -1 AND v_TaxGroup_in = -1 THEN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DSCTXLOT_DBT';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DSCTXLNK_DBT';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DSCTXREST_DBT';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DSCTXRBC_DBT';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DSCTXLS_DBT';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DSCTXGO_DBT';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DSCTXGOFI_DBT';
      ELSE

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_DEBUG,
                  'TXCreateLots: перед чисткой ГО' );
        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'TXCreateLots: перед откатом ГО' );

        --Чистим ГО
        IF v_FIID_in = -1 AND v_TaxGroup_in = -1 THEN
          DELETE
            FROM DSCTXGO_DBT
           WHERE T_SALEDATE >= v_BegDate
             AND T_KIND <> RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER;
        ELSE
          BEGIN
            SELECT MIN(G.T_BUYDATE) INTO v_GDate
              FROM DSCTXGO_DBT G
             WHERE G.T_SALEDATE >= v_BegDate
               AND G.T_KIND <> RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER
               AND (((v_FIID_in = -1) AND
                     (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                               from davoiriss_dbt
                                                              where t_FIID = G.t_FIID
                                                            )
                     )
                    ) OR (v_FIID_in = G.t_FIID)
                   );
            IF v_GDate IS NOT NULL THEN
              v_EndDate := v_GDate - 1;
            END IF;

            EXCEPTION
                 WHEN OTHERS THEN NULL;

          END;

          IF v_GDate IS NOT NULL THEN
            IF v_FIID_in > -1 THEN
              SELECT * INTO v_Fin FROM dfininstr_dbt WHERE t_FIID = v_FIID_in;

              TXPutMsg( 0,
                        v_FIID_in,
                        TXMES_MESSAGE,
                        'По бумаге '||v_Fin.T_FI_CODE||' '||v_Fin.T_NAME||
                        ' проводилась корпоративная операция с датой зачисления новых бумаг '||v_GDate||
                        '. Пересчет выполняется по дату '||v_EndDate );
            ELSE
              TXPutMsg( 0,
                        v_FIID_in,
                        TXMES_MESSAGE,
                        'Выполнялось зачисление по глобальной операции за дату  '||v_GDate||
                        '. Пересчет выполняется по дату '||v_EndDate );
            END IF;
          END IF;

          DELETE
            FROM DSCTXGO_DBT G
           WHERE G.T_SALEDATE >= v_BegDate
             AND G.T_KIND <> RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER
             AND ((((v_FIID_in = -1) AND
                    (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                              from davoiriss_dbt
                                                             where t_FIID = G.t_FIID
                                                           )
                    )
                   ) OR (v_FIID_in = G.t_FIID)
                  ) OR Exists(SELECT 1 FROM DSCTXLOT_DBT LOT WHERE LOT.T_GOID = G.T_ID AND LOT.T_BUYDATE > v_EndDate)
                 );

          DELETE
            FROM DSCTXGOFI_DBT F
           WHERE Exists (SELECT 1 FROM DSCTXLOT_DBT LOT WHERE LOT.T_GOID = F.T_GOID AND LOT.T_BUYDATE >= v_BegDate)
             AND ((((v_FIID_in = -1) AND
                    (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                              from davoiriss_dbt
                                                             where t_FIID = F.t_NEWFIID
                                                           )
                    )
                   ) OR (v_FIID_in = F.t_NEWFIID)
                  ) OR Exists(SELECT 1 FROM DSCTXLOT_DBT LOT WHERE LOT.T_GOID = F.T_GOID AND LOT.T_BUYDATE > v_EndDate)
                 );

          UPDATE DSCTXGO_DBT G
             SET G.T_BUYDATE = TO_DATE('01.01.0001','DD.MM.YYYY')
           WHERE G.T_DOCKIND = RSB_SCTXC.DL_CONVAVR
             AND G.T_SALEDATE < v_BegDate
             AND G.T_BUYDATE >= v_BegDate
             AND EXISTS(SELECT 1 FROM DAVOIRISS_DBT AVR
                         WHERE AVR.T_FIID = G.T_FIID
                           AND (v_FIID_in = -1 OR AVR.T_FIID = v_FIID_in)
                           AND (v_TaxGroup_in = -1 OR AVR.T_TAXGROUP = v_TaxGroup_in)
                       );

        END IF;

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_DEBUG,
                  'TXCreateLots: перед удалением остатков' );
        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'TXCreateLots: перед удалением остатков' );


        --Чистим остатки
        DELETE
          FROM dsctxrest_dbt rest
         WHERE rest.t_CreateDate >= v_BegDate
           AND ((((v_FIID_in = -1) AND
                  (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                            from davoiriss_dbt
                                                           where t_FIID = rest.t_FIID
                                                         )
                  )
                 ) OR (v_FIID_in = rest.t_FIID)
                ) OR rest.t_CreateDate > v_EndDate
               );

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_DEBUG,
                  'TXCreateLots: перед обновлением остатков' );

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'TXCreateLots: перед обновлением остатков' );

        UPDATE dsctxrest_dbt rest
           SET t_ChangeDate = v_BegDate - 1
         WHERE rest.t_ChangeDate >= v_BegDate
           AND ((((v_FIID_in = -1) AND
                  (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                            from davoiriss_dbt
                                                           where t_FIID = rest.t_FIID
                                                         )
                  )
                 ) OR (v_FIID_in = rest.t_FIID)
                ) OR rest.t_ChangeDate > v_EndDate
               );

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_DEBUG,
                  'TXCreateLots: перед взведением признака возврата в связях' );
        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'TXCreateLots: перед взведением признака возврата в связях' );

        -- сначала сбрасываем признак возврата в связах
        UPDATE dsctxlnk_dbt lnk
           SET lnk.t_RetFlag = CHR(0)
         WHERE lnk.t_RetFlag = CHR(88)
           AND ( ( (SELECT lot.t_Type
                      FROM dsctxlot_dbt lot
                     WHERE lot.t_ID = lnk.t_SaleID
                       AND lot.t_BuyDate >= v_BegDate
                       AND ((((v_FIID_in = -1) AND
                              (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                                        from davoiriss_dbt
                                                                       where t_FIID = lot.t_FIID
                                                                     )
                              )
                             ) OR (v_FIID_in = lot.t_FIID)
                            ) OR lot.t_BuyDate > v_EndDate
                           )
                   ) = RSB_SCTXC.TXLOTS_REPO AND
                   lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO) ) OR
                 ( (SELECT lot.t_Type
                      FROM dsctxlot_dbt lot
                     WHERE lot.t_ID = lnk.t_SaleID
                       AND lot.t_BuyDate >= v_BegDate
                       AND ((((v_FIID_in = -1) AND
                              (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                                        from davoiriss_dbt
                                                                       where t_FIID = lot.t_FIID
                                                                     )
                              )
                             ) OR (v_FIID_in = lot.t_FIID)
                            ) OR lot.t_BuyDate > v_EndDate
                           )
                   ) = RSB_SCTXC.TXLOTS_LOANPUT AND
                   lnk.t_Type IN (RSB_SCTXC.TXLNK_LOANPUT, RSB_SCTXC.TXLNK_SUBSTLOAN) ) );

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_DEBUG,
                  'TXCreateLots: перед чисткой связей' );
        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'TXCreateLots: перед откатом связей' );

        DELETE
          FROM dsctxlnk_dbt lnk
         WHERE t_Date >= v_BegDate
           AND ((((v_FIID_in = -1) AND
                  (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(av.t_TaxGroup, 0)
                                                            from davoiriss_dbt av
                                                           where lnk.t_FIID = av.t_FIID
                                                         )
                  )
                 ) OR (v_FIID_in = lnk.t_FIID)
                ) OR lnk.t_Date > v_EndDate
               );


        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_DEBUG,
                  'TXCreateLots: перед обновлением компенсационных лотов' );
        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'TXCreateLots: перед откатом лотов' );

        FOR LC IN CompLots(v_BegDate) LOOP
          UPDATE dsctxlot_dbt
             SET t_BuyDate = t_BegBuyDate,
                 t_ChildID = 0
           WHERE t_ChildID = LC.t_ID
             AND t_Type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
             AND t_SaleDate < v_BegDate;

          UPDATE dsctxlot_dbt
             SET t_SaleDate = t_BegSaleDate,
                 t_ChildID = 0
           WHERE t_ChildID = LC.t_ID
             AND t_Type IN (RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET)
             AND t_BuyDate < v_BegDate;
        END LOOP;
        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'TXCreateLots: после отката компенсации' );

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_DEBUG,
                  'TXCreateLots: перед чисткой лотов' );
        DELETE
          FROM dsctxlot_dbt lot
         WHERE ( (lot.t_Type IN ( RSB_SCTXC.TXLOTS_BUY, RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET )) AND
                 (lot.t_BuyDate >= v_BegDate)
               )
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = lot.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = lot.t_FIID)
               );

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'TXCreateLots: после удаления лотов покупки' );

        DELETE
          FROM dsctxlot_dbt lot
         WHERE ( (lot.t_Type IN ( RSB_SCTXC.TXLOTS_SALE, RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT )) AND
                 (lot.t_SaleDate >= v_BegDate)
               )
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = lot.t_FIID
                                                        )
                 )
                ) OR (v_FIID_in = lot.t_FIID)
               );

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'TXCreateLots: после удаления лотов продажи' );

        --чистим оставшееся от операций переводов
        FOR one_go IN (SELECT G.T_ID
                         FROM DSCTXGO_DBT G
                        WHERE G.T_SALEDATE >= v_BegDate
                          AND G.T_KIND = RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER
                          AND (((v_FIID_in = -1) AND
                                (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                                          from davoiriss_dbt
                                                                         where t_FIID = G.t_FIID
                                                                       )
                                )
                               ) OR (v_FIID_in = G.T_FIID)
                              )
                       )
        LOOP

          DELETE FROM DSCTXREST_DBT R
           WHERE R.T_SOURCEID IN (SELECT L.T_ID FROM DSCTXLOT_DBT L WHERE L.T_GOID = one_go.t_ID AND L.T_ORIGIN = RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER);

          DELETE FROM DSCTXLOT_DBT L WHERE L.T_GOID = one_go.t_ID AND L.T_ORIGIN = RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER;

          DELETE FROM DSCTXGO_DBT WHERE T_ID = one_go.t_ID;

        END LOOP;

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'TXCreateLots: перед обновлением лотов' );

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_DEBUG,
                  'TXCreateLots: перед обновлением лотов' );

        FOR SalePart2 IN LotsSalePart2(v_BegDate) LOOP
          IF SalePart2.t_SaleDate >= v_BegDate THEN
            UPDATE dsctxlot_dbt
               SET t_BegSaleDate = TO_DATE('01.01.0001','DD.MM.YYYY'),
                   t_SaleDate = TO_DATE('01.01.0001','DD.MM.YYYY'),
                   t_RetFlag = CHR(0)
             WHERE t_ID = SalePart2.t_ID;
          ELSE
            UPDATE dsctxlot_dbt
               SET t_BegSaleDate = TO_DATE('01.01.0001','DD.MM.YYYY')
             WHERE t_ID = SalePart2.t_ID;
          END IF;
        END LOOP;

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'TXCreateLots: перед даты 2ч лотов' );

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_DEBUG,
                  'TXCreateLots: перед даты 2ч лотов' );

        -- и потом чистим 2-е части в лотах
        FOR BuyPart2 IN LotsBuyPart2(v_BegDate) LOOP
          IF BuyPart2.t_BuyDate >= v_BegDate THEN
            UPDATE dsctxlot_dbt
               SET t_BegBuyDate = TO_DATE('01.01.0001','DD.MM.YYYY'),
                   t_BuyDate = TO_DATE('01.01.0001','DD.MM.YYYY')
             WHERE t_ID = BuyPart2.t_ID;
          ELSE
            UPDATE dsctxlot_dbt
               SET t_BegBuyDate = TO_DATE('01.01.0001','DD.MM.YYYY')
             WHERE t_ID = BuyPart2.t_ID;
          END IF;
        END LOOP;

        -- Удалить все записи DSCTXFI_DBT
        DELETE
          FROM DSCTXFI_DBT F
         WHERE (select count(1)
                  from DSCTXLOT_DBT Lot
                 where Lot.T_DEALID = F.T_DEALID
                   and Lot.T_FIID   = F.T_FIID
               ) = 0
           AND (((v_FIID_in = -1) AND
                 (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                           from davoiriss_dbt
                                                          where t_FIID = F.T_FIID
                                                        )
                 )
                ) OR (v_FIID_in = F.T_FIID)
               );

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'TXCreateLots: перед занулением t_childid' );

         UPDATE dsctxlot_dbt lot
            SET lot.t_childid = 0
          WHERE lot.t_childID not in (select t_ID from dsctxlot_dbt)
            AND lot.t_childID <> 0
            AND (   (    (v_fiid_in = -1)
                     AND (   v_taxgroup_in = -1
                          OR v_taxgroup_in =
                                            (SELECT NVL (t_taxgroup, 0)
                                               FROM davoiriss_dbt
                                              WHERE t_fiid = lot.t_fiid)
                         )
                    )
                 OR (v_fiid_in = lot.t_fiid)
                );
      END IF;

      COMMIT;

      --Если EndDate >= v_BegDate заполнить DSCTXLOT
      IF( v_BegDate <= v_EndDate ) THEN

        TXPutCurrActMsg( 'Выполняется вставка налоговых лотов...' );

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_DEBUG,
                  'Перед вставкой лотов' );
        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'Перед вставкой лотов' );


        InsertLots( v_BegDate, v_EndDate, v_TaxGroup_in, v_FIID_in );

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'После вставки лотов' );

        COMMIT;

        FOR G IN cSCTXGOLotRepo
        LOOP

          SELECT * INTO v_Fin
            FROM DFININSTR_DBT
           WHERE T_FIID = G.T_FIID;

          FOR R IN cLotsSaleDate(G.T_FIID, G.T_SALEDATE, G.T_BUYDATE)
          LOOP
            v_RDate := G.T_SALEDATE;
            IF R.T_TYPE = RSB_SCTXC.TXLOTS_REPO OR R.T_TYPE = RSB_SCTXC.TXLOTS_LOANPUT THEN
              IF R.T_SALEDATE > G.T_SALEDATE THEN
                v_RDate := R.T_SALEDATE;
              END IF;
            ELSE
              IF R.T_SALEDATE > R.T_BUYDATE THEN
                v_RDate := R.T_BUYDATE;
              END IF;
            END IF;

            TXPutMsg( 0,
                      v_FIID_in,
                      TXMES_WARNING,
                      'По бумаге '||v_Fin.T_FI_CODE||' '||v_Fin.T_NAME||' есть незакрытая сделка РЕПО '||R.T_DEALCODETS||
                      ' на дату '||v_RDate||', когда '||iif(G.T_DOCKIND = RSB_SCTXC.DL_CONVAVR,'проводилась операция конвертации','проводилось корпоративное действие') );

          END LOOP;
        END LOOP;

      END IF;

      IF v_EndDate >= v_BegDate AND ReestrValue.ModeTax = TRUE THEN
         FOR GlobalOper IN C_GlobalOper(v_BegDate, v_EndDate) LOOP
            SELECT Count(1) INTO v_Count
              FROM dsctxlot_dbt lot
             WHERE lot.t_FIID = GlobalOper.t_FIID
               AND ((lot.t_Type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT) AND
                     lot.t_SaleDate <= GlobalOper.t_CommDate AND
                     (lot.t_BuyDate >= GlobalOper.t_CommDate OR lot.t_BuyDate = TO_DATE('01.01.0001', 'DD.MM.YYYY'))
                    ) OR
                    (lot.t_Type IN (RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET) AND
                     lot.t_BuyDate   <= GlobalOper.t_CommDate AND
                     (lot.t_SaleDate >= GlobalOper.t_CommDate OR lot.t_SaleDate = TO_DATE('01.01.0001', 'DD.MM.YYYY'))
                    )
                   );

            IF (v_Count > 0) THEN
                SELECT t_FI_Code
                  INTO v_FICODE
                  FROM dfininstr_dbt
                 WHERE t_FIID = GlobalOper.t_FIID;

               TXPutMsg( 0,
                         GlobalOper.t_FIID,
                         TXMES_WARNING,
                         'На дату '||GlobalOper.t_CommDate||' глобальной операции с кодом "'||GlobalOper.t_CommCode||'" обнаружены открытые сделки РЕПО по ц/б с кодом "'||v_FICODE||'"');
            END IF;
         END LOOP;

         COMMIT;
      END IF;
      -- Выполняем списание
      v_Date := v_BegDate;
      WHILE v_Date <= v_EndDate LOOP

        IF v_EndDate <> v_BegDate THEN
           --v_Proc := (v_Date - v_BegDate) / (v_EndDate - v_BegDate) * 100;
           v_Proc :=  v_Date - v_BegDate + 1;
        ELSE
          v_Proc := 50;
        END IF;

         --TXPutCurrActMsg( 'Выполняется связывание: '||to_char(v_Proc,'999.99')||'%' );
         TXPutCurrActMsg( 'Выполняется связывание: '||to_char(v_Proc,'999999.99'));

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_DEBUG,
                  'выполняем списание v_Date = '||v_Date );
        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'выполняем списание v_Date = '||v_Date );

        IF ReestrValue.ModeTax = FALSE THEN
          FOR G IN (SELECT G.* FROM DSCTXGO_DBT G
                     WHERE G.T_SALEDATE = v_Date
                       AND G.T_KIND <> RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER
                       AND (((v_FIID_in = -1) AND
                             (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(t_TaxGroup, 0)
                                                                       from davoiriss_dbt
                                                                      where t_FIID = G.t_FIID
                                                                    )
                             )
                            ) OR (v_FIID_in = G.t_FIID)
                           )
                   ) LOOP

            RSI_TXProcessGO(G);
          END LOOP;

          FOR G IN (SELECT G.* FROM DSCTXGO_DBT G
                     WHERE G.T_BUYDATE = v_Date
                       AND G.T_KIND = RSB_SCTXC.TXLOTORIGIN_GLOBCONVERT --ГО
                   ) LOOP
            RSI_TXProcessGOFI(G, v_TaxGroup_in, v_FIID_in, v_BegDate);
          END LOOP;
        END IF;

        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'Перед циклом по бумагам' );

        IF(RunParallel > 1 AND v_FIID_in = -1) THEN
          v_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;
          DBMS_PARALLEL_EXECUTE.create_task (task_name => v_task_name);

          v_sql_chunks := 
          '  SELECT MIN (t_FIID) AS min_t_FIID, MAX (t_FIID) AS max_t_FIID
               FROM (  SELECT av.t_FIID, NTILE ('||RunParallel||') OVER (ORDER BY av.t_FIID) AS t_NTILE
                         FROM davoiriss_dbt av
                        WHERE     (   '||v_TaxGroup_in||' = -1
                                              OR '||v_TaxGroup_in||' = (SELECT NVL (av2.t_TaxGroup, 0)
                                              FROM davoiriss_dbt av2
                                             WHERE av2.t_FIID = av.t_FIID))
                              AND (   EXISTS
                                         (SELECT Lot.t_ID
                                            FROM dsctxlot_dbt Lot
                                           WHERE     (   Lot.t_BuyDate = TO_DATE('''||to_char (v_Date, 'DDMMYYYY')||''', ''DDMMYYYY'')
                                                      OR Lot.t_SaleDate = TO_DATE('''||to_char (v_Date, 'DDMMYYYY')||''', ''DDMMYYYY''))
                                                 AND Lot.t_FIID = av.t_FIID)
                                   OR EXISTS
                                         (SELECT txgo.t_ID
                                            FROM dsctxgo_dbt txgo
                                           WHERE     (   txgo.t_SaleDate = TO_DATE('''||to_char (v_Date, 'DDMMYYYY')||''', ''DDMMYYYY'')
                                                      OR txgo.t_BuyDate = TO_DATE('''||to_char (v_Date, 'DDMMYYYY')||''', ''DDMMYYYY''))
                                                 AND (   av.t_FIID = txgo.t_FIID
                                                      OR av.t_FIID IN (SELECT gofi.t_NewFIID
                                                                         FROM dsctxgofi_dbt gofi
                                                                        WHERE gofi.t_GOID = txgo.t_ID)))
                                   OR EXISTS
                                         (SELECT 1
                                            FROM DDL_COMM_DBT CM
                                           WHERE     CM.T_DOCKIND = 105
                                                 AND CM.T_COMMDATE = TO_DATE('''||to_char (v_Date, 'DDMMYYYY')||''', ''DDMMYYYY'')
                                                 AND CM.T_COMMSTATUS = 2 
                                                 AND CM.T_OPERSUBKIND <> 5
                                                 AND CM.T_FIID = av.t_FIID))
                     GROUP BY t_fiid)
             GROUP BY t_NTILE ' ;

          DBMS_PARALLEL_EXECUTE.create_chunks_by_sql(task_name => v_task_name,
                                                     sql_stmt  => v_sql_chunks,
                                                     by_rowid  => FALSE);

          v_sql_process := ' CALL Rsb_SCTX.CalcAV( TO_DATE('''||to_char (v_Date, 'DDMMYYYY')||''', ''DDMMYYYY''), :start_id, :end_id, '||v_TaxGroup_in||' ) ';

          DBMS_PARALLEL_EXECUTE.run_task(task_name => v_task_name,
                                         sql_stmt => v_sql_process,
                                         language_flag => DBMS_SQL.NATIVE,
                                         parallel_level => RunParallel);

          v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
          WHILE(v_try < 2 AND v_status != DBMS_PARALLEL_EXECUTE.FINISHED)
          LOOP
            v_try := v_try + 1;
            DBMS_PARALLEL_EXECUTE.resume_task(v_task_name);
            v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
          END LOOP;


          DBMS_PARALLEL_EXECUTE.drop_task(v_task_name);
          COMMIT;
        ELSE

            -- Цикл по бумагам. Работаем в этот день только с теми, по которым в этот день было движение.
            FOR v_FI IN (SELECT av.*
                           FROM davoiriss_dbt av
                          WHERE (((v_FIID_in = -1) AND
                                  (v_TaxGroup_in = -1 OR v_TaxGroup_in = (select NVL(av2.t_TaxGroup, 0)
                                                                            from davoiriss_dbt av2
                                                                           where av2.t_FIID = av.t_FIID
                                                                         )
                                  )
                                 ) OR (v_FIID_in = av.t_FIID)
                                )
                            AND (EXISTS ( SELECT Lot.t_ID
                                            FROM dsctxlot_dbt Lot
                                           WHERE (Lot.t_BuyDate = v_Date OR Lot.t_SaleDate = v_Date)
                                             AND Lot.t_FIID = av.t_FIID
                                        ) OR
                                 EXISTS ( SELECT txgo.t_ID
                                            FROM dsctxgo_dbt txgo
                                           WHERE (txgo.t_SaleDate = v_Date OR txgo.t_BuyDate = v_Date)
                                             AND (av.t_FIID = txgo.t_FIID OR av.t_FIID IN (select gofi.t_NewFIID
                                                                                             from dsctxgofi_dbt gofi
                                                                                            where gofi.t_GOID = txgo.t_ID
                                                                                          )
                                                 )
                                        ) OR
                                 EXISTS (SELECT 1
                                           FROM DDL_COMM_DBT CM
                                          WHERE CM.T_DOCKIND = 105 --Перемещение
                                            AND CM.T_COMMDATE = v_Date
                                            AND CM.T_COMMSTATUS = 2 --Закрыта
                                            AND CM.T_OPERSUBKIND <> RSB_SECUR.SUBKIND_UNRETIRE
                                            AND CM.T_FIID = av.t_FIID
                                        )
                                )
                        ) LOOP
               v_FIID := v_FI.t_FIID;

               TXPutMsg( 0,
                         v_FIID_in,
                         TXMES_OPTIM,
                         'Начало списания по бумаге v_FIID = '||v_FIID );

               TXPutMsg( 0,
                         v_FIID,
                         TXMES_OPTIM,
                         'Перед установкой t_RetFlag в лотах' );

               UPDATE dsctxlot_dbt lot
                  SET lot.t_RetFlag = CHR(88)
                WHERE lot.t_Type IN (RSB_SCTXC.TXLOTS_BACKREPO, RSB_SCTXC.TXLOTS_LOANGET)
                  AND lot.t_SaleDate = v_Date
                  AND lot.t_FIID = v_FIID;

               TXPutMsg( 0,
                         v_FIID,
                         TXMES_DEBUG,
                         'TXCreateLots: перед взведением признака возврата в связях в дату'||v_Date );

               TXPutMsg( 0,
                         v_FIID,
                         TXMES_OPTIM,
                         'TXCreateLots: перед взведением признака возврата в связях РЕПО в дату'||v_Date );

               FOR Lot IN (SELECT sctxlot.*
                           FROM DSCTXLOT_DBT sctxlot
                           WHERE sctxlot.t_Type = RSB_SCTXC.TXLOTS_REPO
                             AND sctxlot.t_BuyDate = v_Date
                             AND sctxlot.t_BuyDate <> sctxlot.t_SaleDate
                             AND sctxlot.t_FIID = v_FIID)
               LOOP
                  v_NewDealCode := NVL(RTRIM(rsb_struct.getString(rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(Lot.t_DealID, 34, '0'), 37, v_Date)), CHR(0)), CHR(1));
                  IF v_NewDealCode <> chr(1) THEN
                  BEGIN
                     SELECT lot2.t_ID INTO v_NewLotID
                      FROM ddl_tick_dbt tick1, ddl_leg_dbt leg1, ddl_tick_dbt tick2, ddl_leg_dbt leg2, dsctxlot_dbt lot2
                     WHERE tick1.t_DealID = Lot.t_DealID
                       AND tick2.t_DealCode = v_NewDealCode
                       AND TICK2.T_BOFFICEKIND = 101
                       AND tick2.t_PartyID = tick1.t_PartyID
                       AND tick2.t_PFI = tick1.t_PFI
                       AND leg1.t_DealID = tick1.t_DealID
                       AND leg1.t_LegKind = 2
                       AND leg1.t_LegID = 0
                       AND leg2.t_DealID = tick2.t_DealID
                       AND leg2.t_LegKind <> 2
                       AND leg2.t_LegID = 0
                       AND (CASE WHEN leg2.t_MaturityIsPrincipal = 'X' THEN leg2.t_Maturity ELSE leg2.t_Expiry END) =
                           (CASE WHEN leg1.t_MaturityISPrincipal = 'X' THEN leg1.t_Maturity ELSE leg1.t_Expiry END)
                       AND leg2.t_Principal = leg1.t_Principal
                       AND LOT2.T_DEALID = tick2.t_DealID
                       AND LOT2.t_Amount = leg2.t_Principal
                       AND ROWNUM = 1;

                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        TXPutMsg( Lot.t_ID,
                                  v_FIID,
                                  TXMES_WARNING,
                                  'Для сделки с кодом ' || Lot.t_DealCode || ' по примечанию "Код связанной сделки" не найдена связанная сделка ' || v_NewDealCode || ', соответствующая параметрам исходной сделки');

                        UPDATE dsctxlnk_dbt lnk
                          SET lnk.t_RetFlag = CHR(88)
                        WHERE lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO, RSB_SCTXC.TXLNK_LOANPUT, RSB_SCTXC.TXLNK_SUBSTLOAN)
                          AND Exists(SELECT lot.t_Type
                                      FROM dsctxlot_dbt lot
                                     WHERE lot.t_ID = lnk.t_SaleID
                                       AND lot.t_Type = RSB_SCTXC.TXLOTS_REPO
                                       AND lot.t_BuyDate = v_Date
                                       AND lot.t_BuyDate <> lot.t_SaleDate -- однодневки пропускаем
                                       AND lot.t_FIID = v_FIID
                                    ) AND
                                lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO);
                  END;

                     FOR Lnk IN (SELECT *
                                  FROM dsctxlnk_dbt sctxlnk
                                 WHERE sctxlnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO)
                                   AND sctxlnk.t_SaleID = Lot.t_ID)
                     LOOP
                        v_IsExistsNewLot := FALSE;
                        v_NewLotFromArrID := v_ArrLotID.First();
                        WHILE(v_NewLotFromArrID IS NOT NULL)
                        LOOP
                           IF v_ArrLotID(v_NewLotFromArrID) = v_NewLotID
                           THEN
                              v_IsExistsNewLot := TRUE;
                           ELSE
                              v_IsExistsNewLot := FALSE;
                           END IF;

                           v_NewLotFromArrID := v_ArrLotID.Next(v_NewLotFromArrID);

                           EXIT WHEN v_IsExistsNewLot = TRUE;
                        END LOOP;

                        IF v_IsExistsNewLot = FALSE
                        THEN


                           UPDATE dsctxlnk_dbt sctxlnk
                            SET sctxlnk.t_RetFlag = CHR(88)
                           WHERE sctxlnk.t_ID = Lnk.t_ID;

                           TXRecreateLink(Lnk, v_NewLotID, v_Date);
                        END IF;
                     END LOOP;

                     v_IsExistsNewLot := FALSE;
                     v_NewLotFromArrID := v_ArrLotID.First();
                     WHILE(v_NewLotFromArrID IS NOT NULL)
                     LOOP
                        IF v_ArrLotID(v_NewLotFromArrID) = v_NewLotID
                        THEN
                           v_IsExistsNewLot := TRUE;
                        ELSE
                           v_IsExistsNewLot := FALSE;
                        END IF;

                        v_NewLotFromArrID := v_ArrLotID.Next(v_NewLotFromArrID);

                        EXIT WHEN v_IsExistsNewLot = TRUE;
                     END LOOP;
                     IF v_IsExistsNewLot = FALSE
                     THEN
                        v_ArrLotIDNum := v_ArrLotIDNum + 1;
                        v_ArrLotID(v_ArrLotIDNum) := v_NewLotID;
                     END IF;
                  ELSE
                     UPDATE dsctxlnk_dbt lnk
                       SET lnk.t_RetFlag = CHR(88)
                     WHERE lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO, RSB_SCTXC.TXLNK_LOANPUT, RSB_SCTXC.TXLNK_SUBSTLOAN)
                       AND Exists(SELECT lot.t_Type
                                   FROM dsctxlot_dbt lot
                                  WHERE lot.t_ID = lnk.t_SaleID
                                    AND lot.t_Type = RSB_SCTXC.TXLOTS_REPO
                                    AND lot.t_BuyDate = v_Date
                                    AND lot.t_BuyDate <> lot.t_SaleDate -- однодневки пропускаем
                                    AND lot.t_FIID = v_FIID
                                 ) AND
                             lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_SUBSTREPO);
                  END IF;
               END LOOP;

               TXPutMsg( 0,
                         v_FIID,
                         TXMES_OPTIM,
                         'TXCreateLots: перед взведением признака возврата в связях займа в дату'||v_Date );

               UPDATE dsctxlnk_dbt lnk
                  SET lnk.t_RetFlag = CHR(88)
                WHERE Exists(SELECT lot.t_Type
                               FROM dsctxlot_dbt lot
                              WHERE lot.t_ID = lnk.t_SaleID
                                AND lot.t_Type = RSB_SCTXC.TXLOTS_LOANPUT
                                AND lot.t_BuyDate = v_Date
                                AND lot.t_BuyDate <> lot.t_SaleDate -- однодневки пропускаем
                                AND lot.t_FIID = v_FIID
                            ) AND
                      lnk.t_Type IN (RSB_SCTXC.TXLNK_LOANPUT, RSB_SCTXC.TXLNK_SUBSTLOAN);

               IF ReestrValue.V14 = RSB_SCTXC.TXREG_V14_YES THEN
                 TXPutMsg( 0,
                         v_FIID,
                         TXMES_DEBUG,
                         'Перед обработкой перемещений за дату '||v_Date );


                 FOR G IN (SELECT *
                             FROM DSCTXGO_DBT
                            WHERE T_KIND = RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER
                              AND T_SALEDATE = v_Date
                              AND T_FIID = v_FIID
                          )
                 LOOP
                   RSI_TXProcessMoving(G);
                 END LOOP;
               END IF;

               TXPutMsg( 0,
                         v_FIID,
                         TXMES_OPTIM,
                         'После обработки перемещений' );

               FOR CompRepo IN c_CompRepo(v_Date, v_FIID) LOOP
                 TXProcessCompPayOnDirectRepo(CompRepo);
               END LOOP;

               IF ReestrValue.V11 = RSB_SCTXC.TXREG_V11_YES THEN
                 TXShuffling(v_Date, v_FIID);
               END IF;

               IF ReestrValue.V9 = RSB_SCTXC.TXREG_V9_NO THEN
                  IF ReestrValue.V20 = RSB_SCTXC.TXREG_V20_DESC THEN
                    OPEN c_SaleLots0 FOR SELECT *
                                           FROM dsctxlot_dbt lot
                                          WHERE lot.t_SaleDate = v_Date
                                            AND lot.t_SaleDate = lot.t_BuyDate
                                            AND lot.t_ChildID = 0
                                            AND lot.t_Type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
                                            AND lot.t_FIID = v_FIID
                                       ORDER BY TXGetSaleOrder(lot.t_Type) ASC,
                                                lot.t_BegSaleDate DESC,
                                                lot.t_DealDate DESC,
                                                lot.t_DealTime DESC,
                                                lot.t_DealSort DESC;
                  ELSE
                    OPEN c_SaleLots0 FOR SELECT *
                                           FROM dsctxlot_dbt lot
                                          WHERE lot.t_SaleDate = v_Date
                                            AND lot.t_SaleDate = lot.t_BuyDate
                                            AND lot.t_ChildID = 0
                                            AND lot.t_Type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
                                            AND lot.t_FIID = v_FIID
                                       ORDER BY TXGetSaleOrder(lot.t_Type) ASC,
                                                lot.t_BegSaleDate ASC,
                                                lot.t_DealDate ASC,
                                                lot.t_DealTime ASC,
                                                lot.t_DealSort ASC;
                  END IF;

                  LOOP
                    FETCH c_SaleLots0 INTO v_SaleLots0;
                    EXIT WHEN c_SaleLots0%NOTFOUND OR
                              c_SaleLots0%NOTFOUND IS NULL;

                    RSI_TXLinkDirectRepo(v_SaleLots0);
                  END LOOP;

                  CLOSE c_SaleLots0;
               END IF;

               OPEN c_SaleLots FOR SELECT *
                                     FROM dsctxlot_dbt lot
                                    WHERE lot.t_SaleDate = v_Date
                                      AND lot.t_Type = RSB_SCTXC.TXLOTS_SALE
                                      AND lot.t_Origin IN (RSB_SCTXC.TXLOTORIGIN_DEAL, RSB_SCTXC.TXLOTORIGIN_PORTFTRANSFER)
                                      AND lot.t_FIID = v_FIID
                                 ORDER BY lot.t_BegSaleDate ASC,
                                          lot.t_DealDate ASC,
                                          lot.t_DealTime ASC,
                                          lot.t_DealSort ASC;

               LOOP
                 FETCH c_SaleLots INTO v_SaleLots;
                 EXIT WHEN c_SaleLots%NOTFOUND OR
                           c_SaleLots%NOTFOUND IS NULL;

                 RSI_TXLinkSale(v_SaleLots);
               END LOOP;

               CLOSE c_SaleLots;

               TXCloseShortPos(v_Date, v_FIID);

               FOR CompBackRepo IN c_CompBackRepo(v_Date, v_FIID) LOOP
                 TXProcessCompPayOnReverseRepo(CompBackRepo);
               END LOOP;

               OPEN c_SaleLots2 FOR SELECT *
                                      FROM dsctxlot_dbt lot
                                     WHERE lot.t_SaleDate = v_Date
                                       AND lot.t_Type IN (RSB_SCTXC.TXLOTS_LOANGET, RSB_SCTXC.TXLOTS_BACKREPO)
                                       AND lot.t_FIID = v_FIID
                                  ORDER BY TXGetPart2Order(lot.t_Type) ASC,
                                           lot.t_BegSaleDate ASC,
                                           lot.t_DealDate ASC,
                                           lot.t_DealTime ASC,
                                           lot.t_DealSort ASC;

               LOOP
                 FETCH c_SaleLots2 INTO v_SaleLots2;
                 EXIT WHEN c_SaleLots2%NOTFOUND OR
                           c_SaleLots2%NOTFOUND IS NULL;

                 -- Выполнить принудительное ЗКП
                 IF ((v_SaleLots2.t_Blocked = CHR(0)) OR (ReestrValue.V10 = RSB_SCTXC.TXREG_V10_YES)) THEN
                    RSI_TXLinkPart2ToBuy(v_SaleLots2);
                 END IF;

                 RSI_TXUpdatePart2Rest (v_SaleLots2.t_ID, v_Date);

               END LOOP;

               CLOSE c_SaleLots2;

                IF ReestrValue.V20 = RSB_SCTXC.TXREG_V20_DESC THEN
                  OPEN c_SaleLots3 FOR SELECT *
                                         FROM dsctxlot_dbt lot
                                        WHERE lot.t_SaleDate = v_Date
                                          AND ((lot.t_SaleDate != lot.t_BuyDate) or (lot.t_SaleDate = lot.t_BuyDate and lot.t_ChildID <> 0))
                                          AND lot.t_Type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
                                          AND lot.t_FIID = v_FIID
                                     ORDER BY TXGetSaleOrder(lot.t_Type) ASC,
                                              lot.t_BegSaleDate DESC,
                                              lot.t_DealDate DESC,
                                              lot.t_DealTime DESC,
                                              lot.t_DealSort DESC;
                ELSE
                  OPEN c_SaleLots3 FOR SELECT *
                                         FROM dsctxlot_dbt lot
                                        WHERE lot.t_SaleDate = v_Date
                                          AND ((lot.t_SaleDate != lot.t_BuyDate) or (lot.t_SaleDate = lot.t_BuyDate and lot.t_ChildID <> 0))
                                          AND lot.t_Type IN (RSB_SCTXC.TXLOTS_REPO, RSB_SCTXC.TXLOTS_LOANPUT)
                                          AND lot.t_FIID = v_FIID
                                     ORDER BY TXGetSaleOrder(lot.t_Type) ASC,
                                              lot.t_BegSaleDate ASC,
                                              lot.t_DealDate ASC,
                                              lot.t_DealTime ASC,
                                              lot.t_DealSort ASC;
                END IF;

               LOOP
                 FETCH c_SaleLots3 INTO v_SaleLots3;
                 EXIT WHEN c_SaleLots3%NOTFOUND OR
                           c_SaleLots3%NOTFOUND IS NULL;

                 v_IsExistsNewLot := FALSE;
                 v_NewLotFromArrID := v_ArrLotID.First();
                 WHILE(v_NewLotFromArrID IS NOT NULL)
                 LOOP
                    IF v_ArrLotID(v_NewLotFromArrID) = v_NewLotID
                    THEN
                       v_IsExistsNewLot := TRUE;
                    ELSE
                       v_IsExistsNewLot := FALSE;
                    END IF;

                    v_NewLotFromArrID := v_ArrLotID.Next(v_NewLotFromArrID);

                    EXIT WHEN v_IsExistsNewLot = TRUE;
                 END LOOP;
                 IF v_IsExistsNewLot = FALSE
                 THEN
                    RSI_TXLinkDirectRepo(v_SaleLots3);
                 END IF;
               END LOOP;

               CLOSE c_SaleLots3;

               TXPutMsg( 0,
                         v_FIID,
                         TXMES_DEBUG,
                         'TXCreateLots: перед взведением признака возврата в связях однодневок в дату'||v_Date );

               TXPutMsg( 0,
                         v_FIID,
                         TXMES_OPTIM,
                         'TXCreateLots: перед взведением признака возврата в связях однодневок в дату'||v_Date );

               -- однодневки обновляем
               UPDATE dsctxlnk_dbt lnk
                  SET lnk.t_RetFlag = CHR(88)
                WHERE lnk.t_Type IN (RSB_SCTXC.TXLNK_DELREPO, RSB_SCTXC.TXLNK_LOANPUT)
                  AND lnk.t_RetFlag = CHR(0)
                  AND ( ( Exists(SELECT lot.t_Type
                                   FROM dsctxlot_dbt lot
                                  WHERE lot.t_ID = lnk.t_SaleID
                                    AND lot.t_Type = RSB_SCTXC.TXLOTS_REPO
                                    AND lot.t_BuyDate = v_Date
                                    AND lot.t_BuyDate = lot.t_SaleDate
                                    AND lot.t_FIID = v_FIID
                                ) AND
                          lnk.t_Type = RSB_SCTXC.TXLNK_DELREPO
                        ) OR
                        ( Exists(SELECT lot.t_Type
                                   FROM dsctxlot_dbt lot
                                  WHERE lot.t_ID = lnk.t_SaleID
                                    AND lot.t_Type = RSB_SCTXC.TXLOTS_LOANPUT
                                    AND lot.t_BuyDate = v_Date
                                    AND lot.t_BuyDate = lot.t_SaleDate
                                    AND lot.t_FIID = v_FIID
                                ) AND
                          lnk.t_Type = RSB_SCTXC.TXLNK_LOANPUT
                        ) );
            END LOOP;
        END IF;
        
        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_OPTIM,
                  'завершено списание v_Date = '||v_Date );

        -- фиксируем измнения за день
        BEGIN
          IF v_FIID_in = -1 and v_TaxGroup_in = -1 THEN
            UPDATE dregval_dbt
               SET t_fmtblobdata_xxxx = rsb_struct.putString(t_fmtblobdata_xxxx, TO_CHAR(v_Date,'DD.MM.YYYY'))
             WHERE t_KeyID = rsb_tools.find_regkey('SECUR\DATE_BUILD_TAXREG');
          END IF;

        EXCEPTION
          WHEN OTHERS THEN NULL;
        END;

        COMMIT;

        v_Date := TXGetNextDate( v_Date, v_TaxGroup_in, v_FIID_in );

        EXIT WHEN v_Date IS NULL;

      END LOOP;

      COMMIT;

      --Запомнить начало следующего периода
      -- обновляем настройки только в случае успешного расчета
        BEGIN
          v_Date := iif(v_EndDate>v_BegDate,v_EndDate,v_BegDate);
          UPDATE dregval_dbt
             SET t_fmtblobdata_xxxx = rsb_struct.putString(t_fmtblobdata_xxxx, TO_CHAR(v_Date,'DD.MM.YYYY'))
           WHERE t_KeyID = rsb_tools.find_regkey('SECUR\DATE_BUILD_TAXREG');

        EXCEPTION
          WHEN OTHERS THEN NULL;
        END;

      IF NOT gl_WasError THEN
        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_MESSAGE,
                  '** '||iif( v_IsRecalc_in != 0, 'Пересчет', 'Расчет' )||' связей за период c '||
                  TO_CHAR(v_BegDate, 'DD.MM.YYYY')||' по '||TO_CHAR(v_EndDate, 'DD.MM.YYYY')||' выполнен успешно **' );
      ELSE
        TXPutMsg( 0,
                  v_FIID_in,
                  TXMES_MESSAGE,
                  '** '||iif( v_IsRecalc_in != 0, 'Пересчет', 'Расчет' )||' связей за период с '||
                  TO_CHAR(v_BegDate, 'DD.MM.YYYY')||' по '||TO_CHAR(v_EndDate, 'DD.MM.YYYY')||' завершен с ошибкой **' );
      END IF;

      TXPutCurrActMsg( 'Связываение лотов в НУ выполнено' );

      EndCalculate;
      COMMIT;

--      IF v_IsDebug_in = 1 THEN
--        TXPutCurrActMsg( 'Выполняется тестирование...' );
--
--        TXTestLots( TO_DATE('31-12-2005','DD-MM-YYYY'), v_EndDate_in, v_TaxGroup_in, v_FIID_in );
--
--        TXPutCurrActMsg( 'Тестирование выполнено' );
--      END IF;

    END; --TXCreateLots

    -- Признак процентной бумаги. Используется в регистрах. Принимает avoiriss.TaxGroup. 1 = да
    FUNCTION TXIsPercentAvoir( v_TaxGroup IN NUMBER )
    RETURN NUMBER
    IS
    BEGIN

      IF (v_TaxGroup = STATE_BOND_FED_PERC    or
          v_TaxGroup = STATE_BOND_SUBFED_PERC or
          v_TaxGroup = MOUN_BOND_15_PERC      or
          v_TaxGroup = MOUN_BOND_9_PERC       or
          v_TaxGroup = KORP_BOND_24_PERC      or
          v_TaxGroup = KORP_BOND_IP9          or
          v_TaxGroup = KORP_BOND_IP15         or
          v_TaxGroup = STATE_BOND_IN_LOAN     or
          v_TaxGroup = STATE_BOND_OUT_LOAN    or
          v_TaxGroup = OTHER_BOND_PERC        or
          v_TaxGroup = NATCUR_BOND_PERC_NOTNKD or
          v_TaxGroup = CORPORATE_BOND_PERC_NOT_ST or
          v_TaxGroup = NATCUR_BOND_PERC_NKD   or
          v_TaxGroup = OTHER_BOND_PERC_NOT_ST or
          v_TaxGroup = BOND_INDEXNOM          or
          v_TaxGroup = BOND_2017_2021_NONINDEXNOM or
          v_TaxGroup = STATE_MOUNT_BOND_PERC  or
          v_TaxGroup = NATCUR_BOND_PERC_109   or
          v_TaxGroup = CUR_BOND_PERC_130
         ) THEN
         RETURN 1;
      ELSE
         RETURN 0;
      END IF;
    END;

    -- Признак дисконтной бумаги. Используется в регистрах. Принимает avoiriss.TaxGroup. 1 = да
    FUNCTION TXIsDiscountAvoir( v_TaxGroup IN NUMBER )
    RETURN NUMBER
    IS
    BEGIN

      IF (v_TaxGroup = STATE_BOND_FED_DISC    or
          v_TaxGroup = STATE_BOND_SUBFED_DISC or
          v_TaxGroup = MOUN_BOND_15_DISC      or
          v_TaxGroup = MOUN_BOND_9_DISC       or
          v_TaxGroup = KORP_BOND_24_DISC      or
          v_TaxGroup = OTHER_BOND_DISC
         ) THEN
         RETURN 1;
      ELSE
         RETURN 0;
      END IF;
    END;

    -- Вернуть вид облигации - льготная(0)/обычная(1). Иначе -1.
    FUNCTION TXGetBondKind( v_TaxGroup IN NUMBER )
    RETURN NUMBER
    IS
    BEGIN

      IF (v_TaxGroup = STATE_BOND_FED_PERC    or
          v_TaxGroup = STATE_BOND_FED_DISC    or
          v_TaxGroup = STATE_BOND_SUBFED_PERC or
          v_TaxGroup = STATE_BOND_SUBFED_DISC or
          v_TaxGroup = MOUN_BOND_15_PERC      or
          v_TaxGroup = MOUN_BOND_15_DISC      or
          v_TaxGroup = MOUN_BOND_9_PERC       or
          v_TaxGroup = MOUN_BOND_9_DISC       or
          v_TaxGroup = KORP_BOND_IP9          or
          v_TaxGroup = KORP_BOND_IP15         or
          v_TaxGroup = STATE_BOND_IN_LOAN
         ) THEN
         RETURN BOND_FAVOUR;
      ELSIF
         (v_TaxGroup = KORP_BOND_24_PERC          or
          v_TaxGroup = KORP_BOND_24_DISC          or
          v_TaxGroup = STATE_BOND_OUT_LOAN        or
          v_TaxGroup = OTHER_BOND_PERC            or
          v_TaxGroup = OTHER_BOND_DISC            or
          v_TaxGroup = CORPORATE_BOND_PERC_NOT_ST or
          v_TaxGroup = OTHER_BOND_PERC_NOT_ST     or
          v_TaxGroup = NOT_EMISS_PERC_NATCUR      or
          v_TaxGroup = NOT_EMISS_PERC_NOT_NATCUR
         ) THEN
         RETURN BOND_USUAL;
      ELSE
         RETURN BOND_UNDEF;
      END IF;
    END;

      -- Сумма НДС по сделке. Используется в регистрах НУ.
 FUNCTION TXGetNDSComSum( v_DealID IN NUMBER,
                                 v_BOfficeKind IN NUMBER,
                                 v_CalcDate IN DATE,
                                 v_ToFIID IN NUMBER
                               )
    RETURN NUMBER
    IS
      v_Sum      NUMBER;

    BEGIN
       if ReestrValue.V13 is null then
          ReestrValue.V13:= Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V13',0); --Учитывать НДС по комиссиям в затратах : 0 - Да, 1 - Нет
       end if;

       IF ReestrValue.V13 = 0 THEN -- учитывать НДС по комиссиям в затратах
          v_Sum := 0;
       ELSE
          BEGIN
             Select NVL( Sum( RSI_RSB_FIInstr.ConvSum(dlcomis.t_NDS, comis.t_FIID_Comm, v_ToFIID, v_CalcDate)
                            ), 0
                       )
               into v_Sum
               From ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
              Where dlcomis.t_DocKind   = v_BOfficeKind
                and dlcomis.t_DocID     = v_DealID
                and dlcomis.t_FeeType   = comis.t_FeeType
                and dlcomis.t_ComNumber = comis.t_Number;
          exception
             when NO_DATA_FOUND then v_Sum := 0;
             when OTHERS then v_Sum := 0;
          end;
       END IF;

       RETURN v_Sum;
    END;

    -- Сумма комиссий по сделке. Используется в регистрах НУ.
    FUNCTION TXGetComissionsSum( v_DealID IN NUMBER,
                                 v_BOfficeKind IN NUMBER,
                                 v_CalcDate IN DATE,
                                 v_ToFIID IN NUMBER
                               )
    RETURN NUMBER
    IS
      v_Sum      NUMBER;

    BEGIN
       if ReestrValue.V13 is null then
          ReestrValue.V13:= Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V13',0); --Учитывать НДС по комиссиям в затратах : 0 - Да, 1 - Нет
       end if;

       IF ReestrValue.V13 = 0 THEN -- учитывать НДС по комиссиям в затратах
          BEGIN
             Select NVL( Sum( RSI_RSB_FIInstr.ConvSum(dlcomis.t_Sum, comis.t_FIID_Comm, v_ToFIID, v_CalcDate)
                            ), 0
                       )
               into v_Sum
               From ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
              Where dlcomis.t_DocKind   = v_BOfficeKind
                and dlcomis.t_DocID     = v_DealID
                and dlcomis.t_FeeType   = comis.t_FeeType
                and dlcomis.t_ComNumber = comis.t_Number;
          exception
             when NO_DATA_FOUND then v_Sum := 0;
             when OTHERS then v_Sum := 0;
          end;
       ELSE
          BEGIN
             Select NVL( Sum( RSI_RSB_FIInstr.ConvSum((dlcomis.t_Sum - dlcomis.t_NDS), comis.t_FIID_Comm, v_ToFIID, v_CalcDate)
                            ), 0
                       )
               into v_Sum
               From ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
              Where dlcomis.t_DocKind   = v_BOfficeKind
                and dlcomis.t_DocID     = v_DealID
                and dlcomis.t_FeeType   = comis.t_FeeType
                and dlcomis.t_ComNumber = comis.t_Number;
          exception
             when NO_DATA_FOUND then v_Sum := 0;
             when OTHERS then v_Sum := 0;
          end;
       END IF;

       RETURN v_Sum;
    END;

    -- Определение НДС ставки на дату
    FUNCTION TXGetNDSRateByDate(NDSRateID in NUMBER,
                                vDate in DATE,
                                NDSRate IN OUT NUMBER )
    RETURN CHAR
    IS
        chRes  char;
    BEGIN
        chRes := chr(88);
        NDSRate := 0;

        begin
            SELECT h.t_RateValue
            INTO NDSRate
               FROM DBilNDSRate_dbt r, DBilNDSRateHist_dbt h
               WHERE
                    r.t_NDSRateID = NDSRateID  AND
                    r.t_NDSRateID = h.t_NDSRateID AND
               h.t_ValidFromDate = (
                  SELECT MAX(h2.t_ValidFromDate)
                  FROM DBilNDSRateHist_dbt h2
                  WHERE
                  h.t_NDSRateID = h2.t_NDSRateID AND
                  h2.t_ValidFromDate <= vDate
               ) AND
              DECODE(r.t_ValidByDate,to_date('01.01.0001', 'dd.mm.yyyy'),
                     TO_DATE('31.12.9999', 'dd.mm.yyyy'), r.t_ValidByDate)>= vDate;
        exception
             when NO_DATA_FOUND then chRes := chr(0);
             when OTHERS then chRes := chr(0);
        end;

      RETURN chRes;

    END;


    -- Сумма комиссий по сделке за период. Используется в регистрах НУ.
    FUNCTION TXGetComissionsSumInPeriod( v_DealID IN NUMBER,
                                 v_BOfficeKind IN NUMBER,
                                 v_CalcDate IN DATE,
                                 v_ToFIID IN NUMBER,
                                 v_BegDate IN DATE,
                                 v_EndDate IN DATE,
                                 v_IfDD1 IN NUMBER DEFAULT 0,
                                 v_IfDD2 IN NUMBER DEFAULT 0
                               )
    RETURN NUMBER
    IS
      v_Sum1      NUMBER;
      v_Sum2      NUMBER;
      v_nds_rate  NUMBER;
    BEGIN
       IF(v_IfDD1 = 1) THEN
         BEGIN
            Select NVL( Sum( RSI_RSB_FIInstr.ConvSum(dlcomis.t_Sum, comis.t_FIID_Comm, v_ToFIID, v_CalcDate)
                           ), 0
                      )
              into v_Sum1
              From ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
             Where dlcomis.t_DocKind      = v_BOfficeKind
               and dlcomis.t_DocID        = v_DealID
               and dlcomis.t_FeeType      = comis.t_FeeType
               and dlcomis.t_ComNumber    = comis.t_Number
               and dlcomis.t_FactPayDate >= v_BegDate
               and dlcomis.t_FactPayDate  < v_EndDate
               and dlcomis.t_FactPayDate  < TO_DATE( '01.01.2013', 'DD.MM.YYYY' );
         exception
            when NO_DATA_FOUND then v_Sum1 := 0;
            when OTHERS then v_Sum1 := 0;
         end;

         BEGIN
            Select NVL( Sum( RSI_RSB_FIInstr.ConvSum((dlcomis.t_Sum-dlcomis.t_NDS), comis.t_FIID_Comm, v_ToFIID, v_CalcDate)
                           ), 0
                      )
              into v_Sum2
              From ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
             Where dlcomis.t_DocKind      = v_BOfficeKind
               and dlcomis.t_DocID        = v_DealID
               and dlcomis.t_FeeType      = comis.t_FeeType
               and dlcomis.t_ComNumber    = comis.t_Number
               and dlcomis.t_FactPayDate >= v_BegDate
               and dlcomis.t_FactPayDate  < v_EndDate
               and dlcomis.t_FactPayDate  >= TO_DATE( '01.01.2013', 'DD.MM.YYYY' );
         exception
            when NO_DATA_FOUND then v_Sum2 := 0;
            when OTHERS then v_Sum2 := 0;
         end;

       ELSIF(v_IfDD2 = 1) THEN
         BEGIN
            Select NVL( Sum( RSI_RSB_FIInstr.ConvSum(dlcomis.t_Sum, comis.t_FIID_Comm, v_ToFIID, v_CalcDate)
                           ), 0
                      )
              into v_Sum1
              From ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
             Where dlcomis.t_DocKind      = v_BOfficeKind
               and dlcomis.t_DocID        = v_DealID
               and dlcomis.t_FeeType      = comis.t_FeeType
               and dlcomis.t_ComNumber    = comis.t_Number
               and dlcomis.t_FactPayDate  = v_EndDate
               and dlcomis.t_FactPayDate  < TO_DATE( '01.01.2013', 'DD.MM.YYYY' );
         exception
            when NO_DATA_FOUND then v_Sum1 := 0;
            when OTHERS then v_Sum1 := 0;
         end;

         BEGIN
            Select NVL( Sum( RSI_RSB_FIInstr.ConvSum((dlcomis.t_Sum-dlcomis.t_NDS), comis.t_FIID_Comm, v_ToFIID, v_CalcDate)
                           ), 0
                      )
              into v_Sum2
              From ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
             Where dlcomis.t_DocKind      = v_BOfficeKind
               and dlcomis.t_DocID        = v_DealID
               and dlcomis.t_FeeType      = comis.t_FeeType
               and dlcomis.t_ComNumber    = comis.t_Number
               and dlcomis.t_FactPayDate  = v_EndDate
               and dlcomis.t_FactPayDate  >= TO_DATE( '01.01.2013', 'DD.MM.YYYY' );
         exception
            when NO_DATA_FOUND then v_Sum2 := 0;
            when OTHERS then v_Sum2 := 0;
         end;

       ELSE
         BEGIN
            Select NVL( Sum( RSI_RSB_FIInstr.ConvSum(dlcomis.t_Sum, comis.t_FIID_Comm, v_ToFIID, v_CalcDate)
                           ), 0
                      )
              into v_Sum1
              From ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
             Where dlcomis.t_DocKind      = v_BOfficeKind
               and dlcomis.t_DocID        = v_DealID
               and dlcomis.t_FeeType      = comis.t_FeeType
               and dlcomis.t_ComNumber    = comis.t_Number
               and dlcomis.t_FactPayDate >= v_BegDate
               and dlcomis.t_FactPayDate <= v_EndDate
               and dlcomis.t_FactPayDate < TO_DATE( '01.01.2013', 'DD.MM.YYYY' );
         exception
            when NO_DATA_FOUND then v_Sum1 := 0;
            when OTHERS then v_Sum1 := 0;
         end;

         BEGIN
            Select NVL( Sum( RSI_RSB_FIInstr.ConvSum((dlcomis.t_Sum-dlcomis.t_NDS), comis.t_FIID_Comm, v_ToFIID, v_CalcDate)
                           ), 0
                      )
              into v_Sum2
              From ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
             Where dlcomis.t_DocKind      = v_BOfficeKind
               and dlcomis.t_DocID        = v_DealID
               and dlcomis.t_FeeType      = comis.t_FeeType
               and dlcomis.t_ComNumber    = comis.t_Number
               and dlcomis.t_FactPayDate >= v_BegDate
               and dlcomis.t_FactPayDate <= v_EndDate
               and dlcomis.t_FactPayDate >= TO_DATE( '01.01.2013', 'DD.MM.YYYY' );
         exception
            when NO_DATA_FOUND then v_Sum2 := 0;
            when OTHERS then v_Sum2 := 0;
         end;
       end if;

       if (not TXGetNDSRateByDate(1, v_CalcDate, v_nds_rate) = 'X') then
           v_nds_rate := 0;
       end if;
       v_nds_rate := 1 + (v_nds_rate / 100);

       RETURN v_Sum1 / v_nds_rate + v_Sum2;

    END;

     -- Сумма НДС по сделке за период. Используется в регистрах НУ.
    FUNCTION TXGetNDSInPeriod(   v_DealID IN NUMBER,
                                 v_BOfficeKind IN NUMBER,
                                 v_CalcDate IN DATE,
                                 v_ToFIID IN NUMBER,
                                 v_BegDate IN DATE,
                                 v_EndDate IN DATE,
                                 v_IfDD1 IN NUMBER DEFAULT 0,
                                 v_IfDD2 IN NUMBER DEFAULT 0
                               )
    RETURN NUMBER
    IS
      v_Sum      NUMBER;
    BEGIN
       IF(v_IfDD1 = 1) THEN
         BEGIN
            Select NVL( Sum( RSI_RSB_FIInstr.ConvSum(dlcomis.t_NDS, comis.t_FIID_Comm, v_ToFIID, v_CalcDate)
                           ), 0
                      )
              into v_Sum
              From ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
             Where dlcomis.t_DocKind      = v_BOfficeKind
               and dlcomis.t_DocID        = v_DealID
               and dlcomis.t_FeeType      = comis.t_FeeType
               and dlcomis.t_ComNumber    = comis.t_Number
               and dlcomis.t_FactPayDate >= v_BegDate
               and dlcomis.t_FactPayDate  < v_EndDate;
         exception
            when NO_DATA_FOUND then v_Sum := 0;
            when OTHERS then v_Sum := 0;
         end;

       ELSIF(v_IfDD2 = 1) THEN
         BEGIN
            Select NVL( Sum( RSI_RSB_FIInstr.ConvSum(dlcomis.t_NDS, comis.t_FIID_Comm, v_ToFIID, v_CalcDate)
                           ), 0
                      )
              into v_Sum
              From ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
             Where dlcomis.t_DocKind      = v_BOfficeKind
               and dlcomis.t_DocID        = v_DealID
               and dlcomis.t_FeeType      = comis.t_FeeType
               and dlcomis.t_ComNumber    = comis.t_Number
               and dlcomis.t_FactPayDate  = v_EndDate;
         exception
            when NO_DATA_FOUND then v_Sum := 0;
            when OTHERS then v_Sum := 0;
         end;

       ELSE
         BEGIN
            Select NVL( Sum( RSI_RSB_FIInstr.ConvSum(dlcomis.t_NDS, comis.t_FIID_Comm, v_ToFIID, v_CalcDate)
                           ), 0
                      )
              into v_Sum
              From ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
             Where dlcomis.t_DocKind      = v_BOfficeKind
               and dlcomis.t_DocID        = v_DealID
               and dlcomis.t_FeeType      = comis.t_FeeType
               and dlcomis.t_ComNumber    = comis.t_Number
               and dlcomis.t_FactPayDate >= v_BegDate
               and dlcomis.t_FactPayDate <= v_EndDate;
         exception
            when NO_DATA_FOUND then v_Sum := 0;
            when OTHERS then v_Sum := 0;
         end;
       end if;
       RETURN v_Sum;
    END;


    -- Количество купонов по ц/б в периоде дат. Используется в регистрах НУ.
    FUNCTION TXGetCountCoupon(v_FIID IN NUMBER,
                              v_BegDate IN DATE,
                              v_EndDate IN DATE,
                              v_ExcludeDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY')
                             )
    RETURN NUMBER
    IS
      v_CountCoup NUMBER;
    BEGIN
      v_CountCoup := 0;
      BEGIN
        IF v_ExcludeDate = TO_DATE('01.01.0001','DD.MM.YYYY') THEN
          SELECT Count(1) INTO v_CountCoup
            FROM dfiwarnts_dbt
          WHERE t_FIID =  v_FIID
            AND t_IsPartial != 'X'
            AND t_DrawingDate BETWEEN v_BegDate AND v_EndDate;
        ELSE
          SELECT Count(1) INTO v_CountCoup
            FROM dfiwarnts_dbt
          WHERE t_FIID =  v_FIID
            AND t_IsPartial != 'X'
            AND t_DrawingDate BETWEEN v_BegDate AND v_EndDate
            AND t_DrawingDate <> v_ExcludeDate;
        END IF;

        exception
          when OTHERS then v_CountCoup := 0;
      END;
      RETURN v_CountCoup;
    END;


    -- Дата последней выплаты последнего купона в периоде дат. Используется в регистрах НУ
    FUNCTION TXGetMaxCouponDrawingDate(v_FIID IN NUMBER,
                                       v_BegDate IN DATE,
                                       v_EndDate IN DATE,
                                       v_ExcludeDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                       v_CoupRetData IN CHAR DEFAULT CHR(0)
                                      )
    RETURN DATE
    IS
      v_MaxDrawingDate DATE;
      v_MaxNumber dfiwarnts_dbt.t_Number%TYPE;
    BEGIN
      v_MaxDrawingDate := TO_DATE('01.01.0001','DD.MM.YYYY');
      BEGIN
        IF v_ExcludeDate = TO_DATE('01.01.0001','DD.MM.YYYY')  THEN
          SELECT warnt.t_DrawingDate, warnt.t_Number
            INTO v_MaxDrawingDate, v_MaxNumber
            FROM dfiwarnts_dbt warnt
           WHERE warnt.t_FIID = v_FIID
             AND warnt.t_IsPartial != 'X'
             AND warnt.t_DrawingDate = ( select max(t_DrawingDate)
                                           from dfiwarnts_dbt
                                          where t_FIID = warnt.t_FIID
                                            and t_IsPartial = warnt.t_IsPartial
                                            and t_DrawingDate BETWEEN v_BegDate AND v_EndDate
                                       );
        ELSE
          SELECT warnt.t_DrawingDate, warnt.t_Number
            INTO v_MaxDrawingDate, v_MaxNumber
            FROM dfiwarnts_dbt warnt
           WHERE warnt.t_FIID = v_FIID
             AND warnt.t_IsPartial != 'X'
             AND warnt.t_DrawingDate = ( select max(t_DrawingDate)
                                           from dfiwarnts_dbt
                                          where t_FIID = warnt.t_FIID
                                            and t_IsPartial = warnt.t_IsPartial
                                            and t_DrawingDate BETWEEN v_BegDate AND v_EndDate
                                            and t_DrawingDate <> v_ExcludeDate
                                       );
        END IF;

        IF( v_CoupRetData = CHR(88) AND Rsb_Common.GetRegIntValue('SECUR\НАЛОГОВЫЙ УЧЕТ\V24') = 1 ) THEN
          BEGIN
            SELECT NVL(rq.t_FactDate, rq.t_PlanDate)
              INTO v_MaxDrawingDate
              FROM ddl_tick_dbt tick, ddlrq_dbt rq
             WHERE tick.t_BofficeKind = RSB_SCTXC.DL_RETIREMENT
               AND tick.t_PFI = v_FIID
               AND tick.t_Number_Coupon = v_MaxNumber
               AND rsb_secur.IsRet_Partly(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tick.t_DealType, tick.t_BofficeKind))) = 0
               AND rq.t_DocKind = tick.t_BofficeKind
               AND rq.t_DocID = tick.t_DealID
               AND rq.t_DealPart = 1
               AND rq.t_Type = RSI_DLRQ.DLRQ_TYPE_PAYMENT;
          EXCEPTION
            WHEN OTHERS THEN NULL;
          END;
        END IF;

      EXCEPTION
        WHEN OTHERS THEN v_MaxDrawingDate := TO_DATE('01.01.0001','DD.MM.YYYY');
      END;

      RETURN v_MaxDrawingDate;
    END;

    --Получить количество ц/б, перевешенное на другие сделки-источники связями ППР на дату
    FUNCTION TXGetSumSCTXLSOnDate(LnkID IN NUMBER, OnDate IN DATE) RETURN NUMBER
    IS
      vShort NUMBER(32,12);
    BEGIN
      vShort := 0;
      BEGIN
        SELECT NVL(SUM (ls.t_short), 0) INTO vShort
          FROM dsctxls_dbt ls, dsctxlnk_dbt lnk
         WHERE ls.t_parentid = LnkID
           AND lnk.t_id = ls.t_childid
           AND lnk.t_Date <= OnDate;

        exception
          when OTHERS then vShort := 0;
      END;
      RETURN vShort;
    END; --TXGetSumSCTXLSOnDate

    --Выполняет списание лотов в ГО и ИН
    PROCEDURE RSI_TXProcessGO(p_GO IN DSCTXGO_DBT%ROWTYPE)
    IS
      v_SumAmount  NUMBER := 0;
      v_SaleLot    DSCTXLOT_DBT%ROWTYPE;
      v_Fin        DFININSTR_DBT%ROWTYPE;
      v_TaxGroup   NUMBER := 0;
    BEGIN

      SELECT NVL(SUM(Buy.T_AMOUNT - Buy.T_NETTING - Buy.T_SALE), 0) INTO v_SumAmount
        FROM DSCTXLOT_DBT Buy, DDL_TICK_DBT tick
       WHERE T_FIID  = p_GO.T_FIID
         AND Buy.T_BUYDATE <= p_GO.T_SALEDATE
         AND Buy.T_TYPE = RSB_SCTXC.TXLOTS_BUY
         AND Buy.T_ISFREE = CHR(88)
         AND tick.t_DealID = Buy.t_DealID
         AND (
                (tick.t_Ofbu = 'X'
                 AND p_GO.t_SaleDate >= GetInAvrWrtStartDate(tick.t_DealID))
                OR tick.t_Ofbu <> 'X'
             );

      IF v_SumAmount > 0 THEN

        BEGIN
          SELECT t_TaxGroup INTO v_TaxGroup
            FROM DAVOIRISS_DBT
           WHERE t_FIID = p_GO.T_FIID;

          exception
            when OTHERS then v_TaxGroup := 0;
        END;

        v_SaleLot := NULL;

        v_SaleLot.T_ID          := 0;
        v_SaleLot.T_DEALID      := 0;
        v_SaleLot.T_VIRTUALTYPE := RSB_SCTXC.TXVDEAL_REAL;
        v_SaleLot.T_BUYID       := 0;
        v_SaleLot.T_REALID      := 0;
        v_SaleLot.T_PRICE       := 0;
        v_SaleLot.T_PRICEFIID   := -1;
        v_SaleLot.T_PRICECUR    := CHR(1);
        v_SaleLot.T_FIID        := p_GO.T_FIID;
        v_SaleLot.T_TAXGROUP    := v_TaxGroup;
        v_SaleLot.T_TYPE        := RSB_SCTXC.TXLOTS_SALE;
        v_SaleLot.T_DEALCODE    := p_GO.T_CODE;
        v_SaleLot.T_DEALCODETS  := p_GO.T_CODE;
        v_SaleLot.T_DEALDATE    := p_GO.T_SALEDATE;
        v_SaleLot.T_DEALTIME    := TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS');
        v_SaleLot.T_SALEDATE    := p_GO.T_SALEDATE;
        v_SaleLot.T_BUYDATE     := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_SaleLot.T_RETRDATE    := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_SaleLot.T_OLDTYPE     := 0;
        v_SaleLot.T_AMOUNT      := v_SumAmount;
        v_SaleLot.T_NETTINGID   := 0;
        v_SaleLot.T_NETTING     := 0;
        v_SaleLot.T_SALE        := 0;
        v_SaleLot.T_RETFLAG     := CHR(0);
        v_SaleLot.T_ISFREE      := CHR(0);
        v_SaleLot.T_BEGLOTID    := 0;
        v_SaleLot.T_CHILDID     := 0;
        v_SaleLot.T_ISCOMP      := CHR(0);
        v_SaleLot.T_BEGBUYDATE  := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_SaleLot.T_BEGSALEDATE := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_SaleLot.T_COMPAMOUNT  := 0;
        v_SaleLot.T_INACC       := CHR(88);
        v_SaleLot.T_DEALSORTCODE:= CHR(1);
        v_SaleLot.T_DEALSORT    := 0;
        v_SaleLot.T_BLOCKED     := CHR(0);
        v_SaleLot.T_RQID        := 0;
        v_SaleLot.T_ORIGIN      := p_GO.T_KIND;
        v_SaleLot.T_GOID        := p_GO.T_ID;
        v_SaleLot.T_TOTALCOST   := 0;

        INSERT INTO DSCTXLOT_DBT VALUES v_SaleLot RETURNING t_ID INTO v_SaleLot.T_ID;

        UPDATE DSCTXLOT_DBT
           SET t_BEGLOTID = t_ID
         WHERE t_ID = v_SaleLot.T_ID;

        RSI_TXDealSortOnDate(v_SaleLot.T_FIID, v_SaleLot.T_DEALDATE, v_SaleLot.T_DEALTIME, v_SaleLot.T_ID);

        RSI_TXLinkSaleToBuy(v_SaleLot, v_SumAmount);

        IF v_SumAmount > 0 THEN
          TXPutMsg( 0,
                    p_GO.T_FIID,
                    TXMES_WARNING,
                    'Ошибка - неверное количество ц/б в списании в глобальной операции' );
        END IF;

        SELECT * INTO v_Fin
          FROM DFININSTR_DBT
         WHERE t_FIID = p_GO.T_FIID;

        TXPutMsg( 0,
                  p_GO.T_FIID,
                  TXMES_MESSAGE,
                  'Произведено списание ц/б '||v_Fin.T_FI_CODE||' '||v_Fin.T_NAME||
                  ' в рамках обработки '||iif(p_GO.T_DOCKIND = RSB_SCTXC.DL_CONVAVR,'конвертации','корпоративных действий')||' по операции № '||p_GO.T_CODE||' на дату '||TO_CHAR(p_GO.T_SALEDATE ,'DD.MM.YYYY'));

        IF p_GO.T_KIND = RSB_SCTXC.TXLOTORIGIN_MODIFFACEVALUE THEN
          RSI_TXProcessGON(p_GO, v_SaleLot);
        END iF;
      END IF;
    END; --RSI_TXProcessGO

    -- Выполняет зачисление лотов в ИН
    PROCEDURE RSI_TXProcessGON(p_GO IN DSCTXGO_DBT%ROWTYPE, p_SaleLot IN DSCTXLOT_DBT%ROWTYPE)
    IS
      v_Nnew NUMBER := 0;
      v_Nold NUMBER := 0;
      v_Fin  DFININSTR_DBT%ROWTYPE;
      v_OldAmount NUMBER := 0;
      v_NewAmount NUMBER := 0;
      v_NewLot    DSCTXLOT_DBT%ROWTYPE;
      v_TaxGroup   NUMBER := 0;
      v_CalcAmount NUMBER := 0;

      CURSOR cOldLots(v_SaleLotID IN NUMBER) IS
      SELECT oldlot.*
        FROM DSCTXLNK_DBT lnk, DSCTXLOT_DBT oldlot
       WHERE Lnk.T_SALEID = v_SaleLotID
         AND oldlot.T_ID = lnk.T_BUYID;

    BEGIN

      v_Nnew := 0;
      v_Nold := 0;

      SELECT * INTO v_Fin
        FROM DFININSTR_DBT
       WHERE t_FIID = p_GO.T_FIID;

      TXPutMsg( 0,
                p_GO.T_FIID,
                TXMES_MESSAGE,
                'Произведено зачисление ц/б '||v_Fin.T_FI_CODE||' '||v_Fin.T_NAME||
                ' в рамка обработки корпоративных действий по операции № '||p_GO.T_CODE||' на дату '||TO_CHAR(p_GO.T_SALEDATE,'DD.MM.YYYY') );

      FOR OLDLOT IN cOldLots(p_SaleLot.t_ID)
      LOOP
        IF OLDLOT.t_VirtualType <> RSB_SCTXC.TXVDEAL_REAL THEN
          TXPutMsg( 0,
                    p_GO.T_FIID,
                    TXMES_ERROR,
                    'Ошибка  - виртуальных сделок не должно быть в дату конвертации');
        END IF;

        v_OldAmount := OLDLOT.T_AMOUNT - OLDLOT.T_NETTING - OLDLOT.T_SALE;
        v_Nold      := v_Nold + v_OldAmount;
        v_NewAmount := OLDLOT.T_AMOUNT * p_GO.T_OLDFACEVALUE / p_GO.T_NEWFACEVALUE;
        v_Nnew      := v_Nnew + v_NewAmount;

        BEGIN
          SELECT t_TaxGroup INTO v_TaxGroup
            FROM DAVOIRISS_DBT
           WHERE t_FIID = p_GO.T_FIID;

          exception
            when OTHERS then v_TaxGroup := 0;
        END;

        v_NewLot := NULL;

        v_NewLot.T_ID          := 0;
        v_NewLot.T_DEALID      := OLDLOT.T_DEALID;
        v_NewLot.T_FIID        := p_GO.T_FIID;
        v_NewLot.T_TAXGROUP    := v_TaxGroup;
        v_NewLot.T_TYPE        := RSB_SCTXC.TXLOTS_BUY;
        v_NewLot.T_VIRTUALTYPE := RSB_SCTXC.TXVDEAL_REAL;
        v_NewLot.T_BUYID       := OLDLOT.T_ID;
        v_NewLot.T_REALID      := 0;
        v_NewLot.T_PRICE       := OLDLOT.T_PRICE*v_OldAmount/v_NewAmount;
        v_NewLot.T_PRICEFIID   := OLDLOT.T_PRICEFIID;
        v_NewLot.T_PRICECUR    := OLDLOT.T_PRICECUR;
        v_NewLot.T_DEALDATE    := OLDLOT.T_DEALDATE;
        v_NewLot.T_DEALTIME    := OLDLOT.T_DEALTIME;
        v_NewLot.T_DEALCODE    := OLDLOT.T_DEALCODE;
        v_NewLot.T_DEALCODETS  := OLDLOT.T_DEALCODETS;
        v_NewLot.T_BUYDATE     := p_GO.T_BUYDATE;
        v_NewLot.T_SALEDATE    := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_NewLot.T_RETRDATE    := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_NewLot.T_OLDTYPE     := 0;
        v_NewLot.T_AMOUNT      := v_NewAmount;
        v_NewLot.T_NETTINGID   := 0;
        v_NewLot.T_NETTING     := 0;
        v_NewLot.T_SALE        := 0;
        v_NewLot.T_RETFLAG     := CHR(0);
        v_NewLot.T_ISFREE      := CHR(0);
        v_NewLot.T_BEGLOTID    := 0;
        v_NewLot.T_CHILDID     := 0;
        v_NewLot.T_ISCOMP      := CHR(0);
        v_NewLot.T_BEGBUYDATE  := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_NewLot.T_BEGSALEDATE := TO_DATE('01.01.0001','DD.MM.YYYY');
        v_NewLot.T_COMPAMOUNT  := 0;
        v_NewLot.T_INACC       := CHR(88);
        v_NewLot.T_DEALSORTCODE:= CHR(1);
        v_NewLot.T_DEALSORT    := 0;
        v_NewLot.T_BLOCKED     := CHR(0);
        v_NewLot.T_RQID        := OLDLOT.T_RQID;
        v_NewLot.T_ORIGIN      := RSB_SCTXC.TXLOTORIGIN_MODIFFACEVALUE;
        v_NewLot.T_GOID        := p_GO.T_ID;
        v_NewLot.T_TOTALCOST   := 0;
        v_NewLot.T_PORTFOLIO   := OLDLOT.T_PORTFOLIO;

        INSERT INTO DSCTXLOT_DBT VALUES v_NewLot RETURNING t_ID INTO v_NewLot.T_ID;

        TXPutMsg( 0,
                  p_GO.T_FIID,
                  TXMES_MESSAGE,
                  'По сделке № '||OLDLOT.T_DEALCODETS||' '||v_NewAmount||' бумаг' || ' стоимостью ' || v_NewLot.T_TOTALCOST);

      END LOOP;

      UPDATE DSCTXLOT_DBT
           SET T_BEGLOTID = T_ID
         WHERE T_GOID = p_GO.T_ID
           AND T_BEGLOTID = 0 OR T_BEGLOTID IS NULL;


      RSI_TXDealSortAll;

      v_CalcAmount := round(v_Nold * p_GO.T_OLDFACEVALUE / p_GO.T_NEWFACEVALUE, v_Fin.T_SUMPRECISION);

      IF v_Nnew <> v_CalcAmount THEN
        TXPutMsg( 0,
                  p_GO.T_FIID,
                  TXMES_MESSAGE,
                  'Общее количество ц/б по лотам выпуска '||v_Fin.T_FI_CODE||' '||v_Fin.T_NAME||
                  ' в системе '||v_NewAmount||' штук, расчетное количество по операции '||v_CalcAmount||' штук, необходимо провести коррекцию количества на налоговых лотах');

      END IF;

    END; --RSI_TXProcessGON


    --Выполняет зачисление лотов в ГО
    PROCEDURE RSI_TXProcessGOFI(p_GO IN DSCTXGO_DBT%ROWTYPE, p_TAXGROUP IN NUMBER, p_FIID IN NUMBER, p_BegDate IN DATE)
    IS
      v_Count  NUMBER := 0;
      v_N      NUMBER := 0;
      v_KC     NUMBER := 0;
      v_Fin    DFININSTR_DBT%ROWTYPE;
      v_Avoir  DAVOIRISS_DBT%ROWTYPE;
      v_SaleLot  DSCTXLOT_DBT%ROWTYPE;
      v_Nnew NUMBER := 0;
      v_Nold NUMBER := 0;
      v_Cnew NUMBER := 0;
      v_OldAmount NUMBER := 0;
      v_NewAmount NUMBER := 0;
      v_CFI    DDL_LEG_DBT.T_CFI%TYPE;
      v_CFCur  DFININSTR_DBT.T_CCY%TYPE;
      v_Cold   DDL_LEG_DBT.T_TOTALCOST%TYPE;
      v_Price  NUMBER := 0;
      v_NewLot    DSCTXLOT_DBT%ROWTYPE;
      v_CalcAmount NUMBER := 0;
      v_NKD        NUMBER := 0;
      v_Character   VARCHAR2(40);

      CURSOR cGOFI(v_GOID IN NUMBER) IS
      SELECT *
        FROM DSCTXGOFI_DBT
       WHERE T_GOID = v_GOID;

      CURSOR cOldLots(v_SaleLotID IN NUMBER) IS
      SELECT oldlot.*, lnk.t_Amount as LnkAmount
        FROM DSCTXLNK_DBT lnk, DSCTXLOT_DBT oldlot
       WHERE Lnk.T_SALEID = v_SaleLotID
         AND oldlot.T_ID = lnk.T_BUYID;
    BEGIN

      BEGIN
        SELECT * INTO v_SaleLot
          FROM DSCTXLOT_DBT
         WHERE T_GOID = p_GO.T_ID;

        EXCEPTION
            WHEN OTHERS THEN RETURN;
      END;

      select TRIM(VALUE) INTO v_Character from nls_session_parameters where parameter = 'NLS_NUMERIC_CHARACTERS';

      IF p_FIID > -1 THEN
        SELECT COUNT(1) INTO v_Count
          FROM DSCTXGOFI_DBT GF
         WHERE GF.T_GOID = p_GO.T_ID
           AND GF.T_NEWFIID = p_FIID;

        IF (p_GO.T_FIID <> p_FIID OR p_GO.T_SALEDATE < p_BegDate) AND v_Count = 0 THEN
          RETURN;
        END IF;
      END IF;

      SELECT * INTO v_Fin
        FROM DFININSTR_DBT
       WHERE t_FIID = p_GO.T_FIID;

      SELECT * INTO v_Avoir
        FROM DAVOIRISS_DBT
       WHERE t_FIID = p_GO.T_FIID;

      IF p_TAXGROUP > -1 THEN
         SELECT COUNT(1) INTO v_Count
          FROM DSCTXGOFI_DBT GF, DAVOIRISS_DBT AV
         WHERE GF.T_GOID = p_GO.T_ID
           AND AV.T_FIID = GF.T_NEWFIID
           AND AV.T_TAXGROUP = p_TAXGROUP;

         IF (v_Avoir.t_TAXGROUP <> p_TAXGROUP OR p_GO.T_SALEDATE < p_BegDate) AND v_Count = 0 THEN
           RETURN;
         END IF;
      END IF;

      SELECT COUNT(1) INTO v_N
        FROM DSCTXGOFI_DBT GF
       WHERE GF.T_GOID = p_GO.T_ID;

      IF v_N > 1 THEN
        SELECT NVL(SUM(TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character))), 0) INTO v_KC
          FROM DSCTXGOFI_DBT F
         WHERE F.T_GOID = p_GO.T_ID;
      END IF;

      FOR F IN cGOFI(p_GO.T_ID)
      LOOP
        v_Nold := 0;
        v_Nnew := 0;

        SELECT * INTO v_Fin
          FROM DFININSTR_DBT
         WHERE t_FIID = F.T_NEWFIID;

        SELECT * INTO v_Avoir
          FROM DAVOIRISS_DBT
         WHERE t_FIID = p_GO.T_FIID;

        TXPutMsg( 0,
                  p_GO.T_FIID,
                  TXMES_MESSAGE,
                  'Произведено зачисление ц/б '||v_Fin.T_FI_CODE||' '||v_Fin.T_NAME||
                  ' в рамках обработки '||iif(p_GO.T_DOCKIND = RSB_SCTXC.DL_CONVAVR,'конвертации','корпоративных действий')||' по операции № '||p_GO.T_CODE||' на дату '||TO_CHAR(p_GO.T_BUYDATE,'DD.MM.YYYY') );

        FOR OLDLOT IN cOldLots(v_SaleLot.T_ID)
        LOOP
          IF OLDLOT.t_VirtualType <> RSB_SCTXC.TXVDEAL_REAL THEN
            TXPutMsg( 0,
                      p_GO.T_FIID,
                        TXMES_ERROR,
                    'Ошибка  - виртуальных сделок не должно быть в дату конвертации');
          END IF;

          v_OldAmount := OLDLOT.T_AMOUNT - OLDLOT.T_NETTING - OLDLOT.T_SALE + OLDLOT.LnkAmount;
          v_NewAmount := v_OldAmount * TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character));
          v_Nold      := v_Nold + v_OldAmount;
          v_Nnew      := v_Nnew + v_NewAmount;

          SELECT leg.T_CFI, fin.T_CCY, leg.T_TOTALCOST INTO v_CFI, v_CFCur, v_Cold
            FROM ddl_leg_dbt leg, dfininstr_dbt fin
           WHERE leg.T_LEGKIND = 0
             AND leg.T_DEALID = OLDLOT.T_DEALID
             AND leg.T_LEGID = 0
             AND fin.T_FIID = leg.T_CFI;

          IF v_N > 1 THEN
            v_Cnew := v_Cold * TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character ))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character ))/v_KC;
          ELSE
            v_Cnew := v_Cold;
          END IF;

          v_NKD := RSI_RSB_FIInstr.FI_CalcNKD(F.T_NEWFIID, p_GO.T_BUYDATE, v_NewAmount, 0);

          IF v_NKD <> 0 AND v_Fin.T_FACEVALUEFI <> v_CFI THEN
            v_NKD := RSI_RSB_FIInstr.ConvSum( v_NKD, v_Fin.T_FACEVALUEFI, v_CFI, p_GO.T_BUYDATE );
          END IF;

          v_Price := (v_Cnew - v_NKD)/v_NewAmount;

          v_NewLot := NULL;

          v_NewLot.T_ID          := 0;
          v_NewLot.T_DEALID      := OLDLOT.T_DEALID;
          v_NewLot.T_FIID        := F.T_NEWFIID;
          v_NewLot.T_TAXGROUP    := v_Avoir.T_TaxGroup;
          v_NewLot.T_TYPE        := RSB_SCTXC.TXLOTS_BUY;
          v_NewLot.T_VIRTUALTYPE := RSB_SCTXC.TXVDEAL_REAL;
          v_NewLot.T_BUYID       := OLDLOT.T_ID;
          v_NewLot.T_REALID      := 0;
          v_NewLot.T_PRICE       := v_Price;
          v_NewLot.T_PRICEFIID   := v_CFI;
          v_NewLot.T_PRICECUR    := v_CFCur;
          v_NewLot.T_DEALDATE    := OLDLOT.T_DEALDATE;
          v_NewLot.T_DEALTIME    := OLDLOT.T_DEALTIME;
          v_NewLot.T_DEALCODE    := OLDLOT.T_DEALCODE;
          v_NewLot.T_DEALCODETS  := OLDLOT.T_DEALCODETS;
          v_NewLot.T_BUYDATE     := p_GO.T_BUYDATE;
          v_NewLot.T_SALEDATE    := TO_DATE('01.01.0001','DD.MM.YYYY');
          v_NewLot.T_RETRDATE    := TO_DATE('01.01.0001','DD.MM.YYYY');
          v_NewLot.T_OLDTYPE     := 0;
          v_NewLot.T_AMOUNT      := v_NewAmount;
          v_NewLot.T_NETTINGID   := 0;
          v_NewLot.T_NETTING     := 0;
          v_NewLot.T_SALE        := 0;
          v_NewLot.T_RETFLAG     := CHR(0);
          v_NewLot.T_ISFREE      := CHR(0);
          v_NewLot.T_BEGLOTID    := 0;
          v_NewLot.T_CHILDID     := 0;
          v_NewLot.T_ISCOMP      := CHR(0);
          v_NewLot.T_BEGBUYDATE  := TO_DATE('01.01.0001','DD.MM.YYYY');
          v_NewLot.T_BEGSALEDATE := TO_DATE('01.01.0001','DD.MM.YYYY');
          v_NewLot.T_COMPAMOUNT  := 0;
          v_NewLot.T_INACC       := CHR(88);
          v_NewLot.T_DEALSORTCODE:= CHR(1);
          v_NewLot.T_DEALSORT    := 0;
          v_NewLot.T_BLOCKED     := CHR(0);
          v_NewLot.T_RQID        := OLDLOT.T_RQID;
          v_NewLot.T_ORIGIN      := RSB_SCTXC.TXLOTORIGIN_GLOBCONVERT;
          v_NewLot.T_GOID        := p_GO.T_ID;
          v_NewLot.T_TOTALCOST   := v_Cnew;
          v_NewLot.T_PORTFOLIO   := OLDLOT.T_PORTFOLIO;

          INSERT INTO DSCTXLOT_DBT VALUES v_NewLot;

          TXPutMsg( 0,
                    p_GO.T_FIID,
                    TXMES_MESSAGE,
                    'По сделке № '||OLDLOT.T_DEALCODETS||' '||v_NewAmount||' бумаг' || ' стоимостью ' || v_NewLot.T_TOTALCOST);

        END LOOP;

        v_CalcAmount := round(v_Nold * TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character ))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character )), v_Fin.T_SUMPRECISION);

        IF v_NewAmount <> v_CalcAmount THEN
          TXPutMsg( 0,
                    p_GO.T_FIID,
                    TXMES_MESSAGE,
                    'Общее количество ц/б по лотам выпуска '||v_Fin.T_FI_CODE||' '||v_Fin.T_NAME||
                    ' в системе '||v_NewAmount||' штук, расчетное количество по операции '||v_CalcAmount||' штук, необходимо провести коррекцию количества на налоговых лотах');
        END IF;
      END LOOP;

      UPDATE DSCTXLOT_DBT
         SET T_BEGLOTID = T_ID
       WHERE T_GOID = p_GO.T_ID
         AND T_BEGLOTID = 0 OR T_BEGLOTID IS NULL;

      RSI_TXDealSortAll;

    END;

    --Получить ценовые условия
    FUNCTION GetLeg(p_DealID IN NUMBER, p_Leg OUT DDL_LEG_DBT%ROWTYPE)
    RETURN NUMBER
    IS
    BEGIN
      select * into p_Leg from ddl_leg_dbt where t_DealID = p_DealID and t_LegKind = 0 and t_LegID = 0;

      RETURN 1;

      exception
        when OTHERS then return 0;
    END; --GetLeg

    --Цена в ВЦ
    FUNCTION TXGetLotPrice(p_LotID IN NUMBER)
    RETURN NUMBER
    IS
      v_Price  NUMBER := 0;
      v_Amount NUMBER := 0;
      v_Cost   NUMBER := 0;
      v_Leg    DDL_LEG_DBT%ROWTYPE;
      v_Lot    DSCTXLOT_DBT%ROWTYPE;
    BEGIN

      v_Price  := 0;

      SELECT * INTO v_Lot
        FROM DSCTXLOT_DBT
       WHERE T_ID = p_LotID;

      v_Amount := v_Lot.T_AMOUNT - v_Lot.T_NETTING - v_Lot.T_SALE;

      IF v_Lot.T_ORIGIN <> RSB_SCTXC.TXLOTORIGIN_GLOBCONVERT AND (v_Lot.T_VIRTUALTYPE = RSB_SCTXC.TXVDEAL_REAL OR v_Lot.T_VIRTUALTYPE = RSB_SCTXC.TXVDEAL_CALC) THEN
        IF v_Amount <> 0 THEN
          v_Price := TXGetLotCost(v_Lot.T_ID)/v_Amount;
        END IF;
      ELSIF v_Lot.T_ORIGIN = RSB_SCTXC.TXLOTORIGIN_GLOBCONVERT THEN
        IF v_Amount <> 0 THEN
          v_Price := TXGetLotCost(v_Lot.T_ID)/v_Amount;
        END IF;
      ELSE
        v_Price := v_Lot.T_PRICE;
      END IF;

      RETURN v_Price;

    END; --TXGetLotPrice

    --Стоимость без НКД в ВЦ
    FUNCTION TXGetLotCost(p_LotID IN NUMBER)
    RETURN NUMBER
    IS
      v_Amount NUMBER := 0;
      v_Cost   NUMBER := 0;
      v_Leg DDL_LEG_DBT%ROWTYPE;
      v_FaceValueFI NUMBER := 0;
      v_NKD    NUMBER := 0;
      v_Lot    DSCTXLOT_DBT%ROWTYPE;
    BEGIN

      v_Cost   := 0;

      SELECT * INTO v_Lot
        FROM DSCTXLOT_DBT
       WHERE T_ID = p_LotID;

      v_Amount := v_Lot.T_AMOUNT - v_Lot.T_NETTING - v_Lot.T_SALE;

      IF v_Lot.T_ORIGIN <> RSB_SCTXC.TXLOTORIGIN_GLOBCONVERT AND (v_Lot.T_VIRTUALTYPE = RSB_SCTXC.TXVDEAL_REAL OR v_Lot.T_VIRTUALTYPE = RSB_SCTXC.TXVDEAL_CALC) THEN
        IF GetLeg(v_Lot.T_DEALID, v_Leg) = 1 AND v_Leg.T_PRINCIPAL <> 0 THEN
          v_Cost := v_Leg.T_COST * v_Amount / v_Leg.T_PRINCIPAL;
        END IF;
      ELSIF v_Lot.T_ORIGIN = RSB_SCTXC.TXLOTORIGIN_GLOBCONVERT THEN
        IF GetLeg(v_Lot.T_DEALID, v_Leg) = 1 AND v_Leg.T_PRINCIPAL <> 0 THEN
         v_NKD := TXGetLotNKD(v_Lot.T_ID);

         SELECT t_FaceValueFI INTO v_FaceValueFI
           FROM DFININSTR_DBT
          WHERE t_FIID = v_Lot.T_FIID;

         IF v_NKD <> 0 THEN
           v_NKD := RSI_RSB_FIInstr.ConvSum( v_NKD, v_FaceValueFI, v_Lot.T_PRICEFIID, v_Lot.T_BUYDATE );
         END IF;

         v_Cost := TXGetLotTotalCost(v_Lot.T_ID) - v_NKD;
        END IF;
      ELSE
        v_Cost := v_Lot.T_PRICE * v_Amount;
      END IF;

      RETURN v_Cost;

    END; --TXGetLotCost

    --НКД в ВН
    FUNCTION TXGetLotNKD(p_LotID IN NUMBER)
    RETURN NUMBER
    IS
      v_Amount NUMBER := 0;
      v_Leg DDL_LEG_DBT%ROWTYPE;
      v_NKD    NUMBER := 0;
      v_Lot    DSCTXLOT_DBT%ROWTYPE;
    BEGIN

      v_NKD    := 0;

      SELECT * INTO v_Lot
        FROM DSCTXLOT_DBT
       WHERE T_ID = p_LotID;

      v_Amount := v_Lot.T_AMOUNT - v_Lot.T_NETTING - v_Lot.T_SALE;

      IF v_Lot.T_ORIGIN <> RSB_SCTXC.TXLOTORIGIN_GLOBCONVERT AND (v_Lot.T_VIRTUALTYPE = RSB_SCTXC.TXVDEAL_REAL OR v_Lot.T_VIRTUALTYPE = RSB_SCTXC.TXVDEAL_CALC) THEN
        IF GetLeg(v_Lot.T_DEALID, v_Leg) = 1 AND v_Leg.T_PRINCIPAL <> 0 THEN
          v_NKD := v_Leg.T_NKD * v_Amount / v_Leg.T_PRINCIPAL;
        END IF;
      ELSIF v_Lot.T_ORIGIN = RSB_SCTXC.TXLOTORIGIN_GLOBCONVERT THEN
        IF GetLeg(v_Lot.T_DEALID, v_Leg) = 1 AND v_Leg.T_PRINCIPAL <> 0 THEN
         v_NKD := RSI_RSB_FIInstr.FI_CalcNKD(v_Lot.T_FIID, v_Lot.T_BUYDATE, v_Amount, 0);
        END IF;
      ELSE
        v_NKD := RSI_RSB_FIInstr.FI_CalcNKD(v_Lot.T_FIID, v_Lot.T_DEALDATE, v_Amount, 0);
      END IF;

      RETURN v_NKD;

    END; --TXGetLotNKD

    --Стоимость с НКД  в ВЦ
    FUNCTION TXGetLotTotalCost(p_LotID NUMBER)
    RETURN NUMBER
    IS
      v_TotalCost NUMBER := 0;
      v_Amount NUMBER := 0;
      v_Leg DDL_LEG_DBT%ROWTYPE;
      v_NKD    NUMBER := 0;
      v_FaceValueFI NUMBER := 0;
      v_Lot    DSCTXLOT_DBT%ROWTYPE;
    BEGIN

      v_TotalCost := 0;

      SELECT * INTO v_Lot
        FROM DSCTXLOT_DBT
       WHERE T_ID = p_LotID;

      v_Amount := v_Lot.T_AMOUNT - v_Lot.T_NETTING - v_Lot.T_SALE;

      IF v_Lot.T_ORIGIN <> RSB_SCTXC.TXLOTORIGIN_GLOBCONVERT AND (v_Lot.T_VIRTUALTYPE = RSB_SCTXC.TXVDEAL_REAL OR v_Lot.T_VIRTUALTYPE = RSB_SCTXC.TXVDEAL_CALC) THEN
        IF GetLeg(v_Lot.T_DEALID, v_Leg) = 1 AND v_Leg.T_PRINCIPAL <> 0 THEN
          v_TotalCost := v_Leg.T_TOTALCOST * v_Amount / v_Leg.T_PRINCIPAL;
        END IF;
      ELSIF v_Lot.T_ORIGIN = RSB_SCTXC.TXLOTORIGIN_GLOBCONVERT THEN
        IF GetLeg(v_Lot.T_DEALID, v_Leg) = 1 AND v_Leg.T_PRINCIPAL <> 0 THEN
         v_TotalCost := v_Lot.T_TOTALCOST;
        END IF;
      ELSE
        v_NKD := TXGetLotNKD(v_Lot.T_ID);

        SELECT t_FaceValueFI INTO v_FaceValueFI
          FROM DFININSTR_DBT
         WHERE t_FIID = v_Lot.T_FIID;

        IF v_NKD <> 0 THEN
          v_NKD := RSI_RSB_FIInstr.ConvSum( v_NKD, v_FaceValueFI, v_Lot.T_PRICEFIID, v_Lot.T_DEALDATE );
        END IF;

        v_TotalCost := TXGetLotCost(v_Lot.T_ID) + v_NKD;
      END IF;

      RETURN v_TotalCost;

    END; --TXGetLotTotalCost

    procedure GetDealParmOnDate( p_DealID      IN NUMBER,
                                 p_BOfficeKind IN NUMBER,
                                 p_ChangeDate  IN DATE,
                                 p_Instance    IN NUMBER,
                                 p_Price       IN NUMBER,
                                 p_Cost        IN NUMBER,
                                 p_TotalCost   IN NUMBER,
                                 p_NKD         IN NUMBER,
                                 p_DealPart    IN NUMBER,
                                 p_CalcDate    IN DATE )
     is
      v_CalcDate DATE;
      v_spch     dsptkchng_dbt%ROWTYPE;
      v_DD1      DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
      v_DP1      DATE := TO_DATE('01.01.0001','DD.MM.YYYY');

    begin
      DealParm := NULL;

      DealParm.DealID   := p_DealID;
      DealParm.DealPart := p_DealPart;
      DealParm.CalcDate := p_CalcDate;

      -- т.к. p_CalcDate вычисляется сложно и звать этот расчёт для каждой записи долго,
      -- то будем если p_CalcDate не задано и p_DealPart = 1 делать данный расчёт здесь.
      -- На момент реализации этой ф-и, она используется только для регистров РЕПО (НУ)
      IF (p_DealPart = 1) THEN
         IF (p_ChangeDate <> TO_DATE('01.01.0001','DD.MM.YYYY') AND
             p_CalcDate = TO_DATE('01.01.0001','DD.MM.YYYY')
            ) THEN
            -- вычисляем. По условию регистров РЕПО, это MaxDate(DD1(), DP1()). Дублируем здесь этот расчёт.

           SELECT NVL(MIN( RQ.t_FactDate ), TO_DATE('01-01-0001','DD-MM-YYYY'))
             INTO v_DD1
             FROM ddlrq_dbt RQ
            WHERE RQ.t_DocKind = p_BOfficeKind
              AND RQ.t_DocID   = p_DealID
              AND RQ.t_State   = RSI_DLRQ.DLRQ_STATE_EXEC
              AND RQ.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
              AND RQ.t_Type    = RSI_DLRQ.DLRQ_TYPE_DELIVERY
              AND RQ.t_DealPart= 1;


           IF v_DD1 = TO_DATE('01.01.0001','DD.MM.YYYY') THEN
              SELECT NVL(MIN( RQ.t_FactDate ), TO_DATE('01-01-0001','DD-MM-YYYY'))
                INTO v_DD1
                FROM ddlrq_dbt RQ
               WHERE RQ.t_DocKind = p_BOfficeKind
                 AND RQ.t_DocID   = p_DealID
                 AND RQ.t_State  <> RSI_DLRQ.DLRQ_STATE_EXEC
                 AND RQ.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                 AND RQ.t_Type    = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                 AND RQ.t_DealPart= 1;
           END IF;


           SELECT NVL(MIN( RQ.t_FactDate ), TO_DATE('01-01-0001','DD-MM-YYYY'))
             INTO v_DP1
             FROM ddlrq_dbt RQ
            WHERE RQ.t_DocKind = p_BOfficeKind
              AND RQ.t_DocID   = p_DealID
              AND RQ.t_State   = RSI_DLRQ.DLRQ_STATE_EXEC
              AND RQ.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY
              AND RQ.t_Type    = RSI_DLRQ.DLRQ_TYPE_PAYMENT
              AND RQ.t_DealPart= 1;

           IF v_DP1 = TO_DATE('01.01.0001','DD.MM.YYYY') THEN
              SELECT NVL(MIN( RQ.t_FactDate ), TO_DATE('01-01-0001','DD-MM-YYYY'))
                INTO v_DP1
                FROM ddlrq_dbt RQ
               WHERE RQ.t_DocKind = p_BOfficeKind
                 AND RQ.t_DocID   = p_DealID
                 AND RQ.t_State  <> RSI_DLRQ.DLRQ_STATE_EXEC
                 AND RQ.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY
                 AND RQ.t_Type    = RSI_DLRQ.DLRQ_TYPE_PAYMENT
                 AND RQ.t_DealPart= 1;
           END IF;

           IF (v_DD1 > v_DP1) THEN
              v_CalcDate := v_DD1;
           ELSE
              v_CalcDate := v_DD1;
           END IF;
         ELSE
            v_CalcDate := p_CalcDate;
         END IF;
      ELSE
         v_CalcDate := p_CalcDate;
      END IF;

      IF ((p_ChangeDate = TO_DATE('01.01.0001','DD.MM.YYYY')) OR (v_CalcDate >= p_ChangeDate)) THEN
         --Всё из тикета
         DealParm.Price     := p_Price;
         DealParm.Cost      := p_Cost;
         DealParm.TotalCost := p_TotalCost;
         DealParm.NKD       := p_NKD;
      ELSE
         --Иначе лезем в историю
         BEGIN
            select sp.* INTO v_spch
              from dsptkchng_dbt sp
             where sp.t_oldinstance = ((select NVL(min(sp1.t_oldinstance),  p_Instance)
                                          from dsptkchng_dbt sp1
                                         where sp1.t_dealid = p_DealID
                                           and sp1.t_OldChangeDate > v_CalcDate
                                       ) - 1)
               and sp.t_dealid = p_DealID;

            IF (p_DealPart = 2) THEN
               DealParm.Price     := v_spch.t_OldPrice2;
               DealParm.Cost      := v_spch.t_OldCost2;
               DealParm.TotalCost := v_spch.t_OldTotalCost2;
               DealParm.NKD       := v_spch.t_OldNKD2;
            ELSE
               DealParm.Price     := v_spch.t_OldPrice1;
               DealParm.Cost      := v_spch.t_OldCost1;
               DealParm.TotalCost := v_spch.t_OldTotalCost1;
               DealParm.NKD       := v_spch.t_OldNKD1;
            END IF;

          exception
            when NO_DATA_FOUND then
                BEGIN
                   --Всё из тикета
                   DealParm.Price     := p_Price;
                   DealParm.Cost      := p_Cost;
                   DealParm.TotalCost := p_TotalCost;
                   DealParm.NKD       := p_NKD;
                END;

         END;

      END IF;
    END; --GetDealParmOnDate

    function GetDealTotalCostOnDate( p_DealID      IN NUMBER,
                                     p_BOfficeKind IN NUMBER,
                                     p_ChangeDate  IN DATE,
                                     p_Instance    IN NUMBER,
                                     p_Price       IN NUMBER,
                                     p_Cost        IN NUMBER,
                                     p_TotalCost   IN NUMBER,
                                     p_NKD         IN NUMBER,
                                     p_DealPart    IN NUMBER,
                                     p_CalcDate    IN DATE )
    return NUMBER
      is
    begin

      if( DealParm.DealID   is NULL or DealParm.DealID   <> p_DealID or
          DealParm.DealPart is NULL or DealParm.DealPart <> p_DealPart or
          DealParm.CalcDate is NULL or DealParm.CalcDate <> p_CalcDate ) then
         GetDealParmOnDate( p_DealID,
                            p_BOfficeKind,
                            p_ChangeDate,
                            p_Instance,
                            p_Price,
                            p_Cost,
                            p_TotalCost,
                            p_NKD,
                            p_DealPart,
                            p_CalcDate);
      end if;
      return iif(DealParm.TotalCost is not NULL, DealParm.TotalCost, 0.0);
    end;

    function TXGetLotCode( p_DealID   IN NUMBER,
                           p_FIID     IN NUMBER,
                           p_DealCode IN VARCHAR2,
                           p_IsBasket IN NUMBER,
                           p_IsBuy    IN NUMBER,
                           p_Mode     IN NUMBER
                         )
    return VARCHAR2
      is
         v_Code  DSCTXLOT_DBT.T_DEALCODE%TYPE;
         v_Num   NUMBER := 0;
         v_FiNum NUMBER := 1;
    begin

      v_Code := p_DealCode; -- без отсечённых концевых пробелов

      if( p_IsBasket <> 0 )then -- РЕПО на корзину
         begin
            select t_FiNumber INTO v_FiNum
              from DSCTXFI_DBT
             where T_DEALID = p_DealID
               and T_FIID   = p_FIID;

            exception when OTHERS then v_FiNum := 1;
         end;

         v_Code := v_Code||'_'||to_char(lpad(v_FiNum,3,0));
      end if;

      if( p_Mode = KINDLOT_NORMAL ) then --"обычные лоты"
         return v_Code;
      elsif( p_Mode = KINDLOT_COMPDEL_MINUS ) then --" комп. поставка, уменьшение "
         begin
            select COUNT(1) INTO v_Num
              from DSCTXLOT_DBT
             where T_DEALID = p_DealID
               and T_FIID   = p_FIID
               and NVL((select rq.t_Type
                          from ddlrq_dbt rq
                         where rq.t_ID = T_RQID
                           and rq.t_Kind = case when p_IsBuy = 1 then RSI_DLRQ.DLRQ_KIND_COMMIT else RSI_DLRQ.DLRQ_KIND_REQUEST end
                       ),RSI_DLRQ.DLRQ_TYPE_UNKNOWN) = RSI_DLRQ.DLRQ_TYPE_COMPDELIVERY;
            exception when OTHERS then v_Num := 0;
         end;

         v_Num := v_Num + 1;

         return v_Code||'_КП'||to_char(lpad(v_Num,3,0));

      elsif( p_Mode = KINDLOT_COMPDEL_PLUS ) then --" комп. поставка, увеличение "
         begin
            select COUNT(1) INTO v_Num
              from DSCTXLOT_DBT
             where T_DEALID = p_DealID
               and T_FIID   = p_FIID
               and NVL((select rq.t_Type
                          from ddlrq_dbt rq
                         where rq.t_ID = T_RQID
                           and rq.t_Kind = case when p_IsBuy = 1 then RSI_DLRQ.DLRQ_KIND_REQUEST else RSI_DLRQ.DLRQ_KIND_COMMIT end
                       ),RSI_DLRQ.DLRQ_TYPE_UNKNOWN) = RSI_DLRQ.DLRQ_TYPE_COMPDELIVERY;
            exception when OTHERS then v_Num := 0;
         end;

         v_Num := v_Num + 1;

         return v_Code||'_КПП'||to_char(lpad(v_Num,3,0));

      end if;

      return chr(0);

    end; --TXGetLotCode

END Rsb_SCTX;
/
