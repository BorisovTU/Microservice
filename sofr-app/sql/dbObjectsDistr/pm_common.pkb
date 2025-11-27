CREATE OR REPLACE PACKAGE BODY PM_COMMON
AS
--------------------------------------------------------------------------------
-- Основные общие функции и константы платежей
-- Тело пакета
--------------------------------------------------------------------------------


  SF_FEE_TYPE_PERIOD  CONSTANT INTEGER := 1; -- Периодическая
  SF_FEE_TYPE_SINGLE  CONSTANT INTEGER := 3; -- Единовременная
  SF_FEE_TYPE_ONCE    CONSTANT INTEGER := 6; -- Разовая
  SF_FEE_TYPE_INVOICE CONSTANT INTEGER := 9; -- Требованием на оплату

  TYPE PartyIDTable_t IS TABLE OF dparty_dbt.t_PartyID%type;
  TYPE AccountTable_t IS TABLE OF daccount_dbt.t_Account%type;

   -- Настройки
  InitOperTransitRegPath CONSTANT VARCHAR2(50) := 'CB/PAYMENTS/INITIATOROPER/INITIATOROPERTRANSIT';
  InitOperTransitRegUnknown CONSTANT VARCHAR2(50) := 'CB/PAYMENTS/INITIATOROPER/INITIATOROPERUNKNOW';
  ClientAccountRegPath CONSTANT VARCHAR2(50) :=  'PS/REQOPENACC/СЧЕТА КЛИЕНТОВ';
  BankAccountRegPath CONSTANT VARCHAR2(50) :=  'PS/REQOPENACC/СЧЕТА БАНКА';
  
  -- Перечисление для настройки АРМ ПОЗИЦИОНЕРА \ ПОЗИЦИОНИРОВАНИЕ \ ТИП КОРСХЕМЫ_ВНЕШ_ПЛАТЕЖ
  --Если в документе указан корсчет, то сторона определяется как внешняя, если для этого счета есть корсхема типа:
  REGVAL_TCFEP_ANY CONSTANT NUMBER(5) := 0;    -- любого
  REGVAL_TCFEP_LORO CONSTANT NUMBER(5) := 1;   -- ЛОРО
  REGVAL_TCFEP_NOSTRO CONSTANT NUMBER(5) := 2; -- НОСТРО, 
  REGVAL_TCFEP_NOTEXT CONSTANT NUMBER(5) := 3; -- не определяем

  TypeCorschemForExtPayRegPath CONSTANT VARCHAR2(100) :=  'АРМ ПОЗИЦИОНЕРА/ПОЗИЦИОНИРОВАНИЕ/ТИП КОРСХЕМЫ_ВНЕШ_ПЛАТЕЖ';


  M_OURBANK     NUMBER(10);
  M_SELFBIC    VARCHAR2(35); -- наш БИК
  M_SELFCORACC VARCHAR2(25); -- наш корсчет
  m_ClientAccountMask VARCHAR2(2000);
  m_BankAccountMask   VARCHAR2(2000);
  m_OurFilials PartyIDTable_t;
  m_OurHeadParty dparty_dbt.t_PartyID%type;
  m_Accounts AccountTable_t;
  m_DefaultMaxPriority dpmrmprop_dbt.t_Priority%type;
  m_CheckINNSumACCValue INTEGER;

  m_PersnAccountMask VARCHAR2(2000);
  m_PersnDepositMask VARCHAR2(2000);
  m_TypeCorschemForExtPayment number(5);

  PROCEDURE ResetAllGlobalsRegValues
  AS
  BEGIN

    PM_CLNNAMES.ResetGlobalsRegValues;
    PM_CSPREPRO.ResetGlobalsRegValues;
    PM_OPERATION.ResetGlobalsRegValues;
    PM_RESTFUN.ResetGlobalsRegValues;
    --PMEA_MEMORDER.ResetGlobalsRegValues;
    wld_pos.ResetGlobalsRegValues;
    RSI_PM_CHK117.ResetGlobalsRegValues;
    WLD_HEAD.ResetGlobalsRegValues;
    PM_ALLOWTRANSFER.ResetGlobalsRegValues;
    WLD_UFEBS.ResetGlobalsRegValues;
    RSI_PM_TAXPROP.ResetGlobalsRegValues;

    M_OURBANK := null;
    m_ClientAccountMask := null;
    m_BankAccountMask := null;
    m_DefaultMaxPriority := null;
    m_PersnAccountMask := null;
    m_CheckINNSumACCValue := null;
    m_PersnDepositMask := null;
    m_TypeCorschemForExtPayment := null;

    Init;

  END ResetAllGlobalsRegValues;

  
  FUNCTION getTypeCorschemForExtPayment RETURN number
  AS
  BEGIN
    if m_TypeCorschemForExtPayment is null then
      m_TypeCorschemForExtPayment := RSB_COMMON.GetRegIntValue (TypeCorschemForExtPayRegPath);
    end if;
    RETURN m_TypeCorschemForExtPayment;
  END getTypeCorschemForExtPayment;


  -- текущий опердень
  FUNCTION CURDATE RETURN DATE
  AS
  BEGIN
    RETURN RSBSESSIONDATA.CURDATE();
  END CURDATE;

  -- ID субъекта, связанного с филиалом операциониста
  FUNCTION OURBANK RETURN NUMBER
  AS
  BEGIN
    RETURN M_OURBANK;
  END OURBANK;

  -- наш БИК
  FUNCTION SELFBIC RETURN VARCHAR2
  AS
  BEGIN
    RETURN M_SELFBIC;
  END SELFBIC;

  -- наш корсчет
  FUNCTION SELFCORACC RETURN VARCHAR2
  AS
  BEGIN
    RETURN M_SELFCORACC;
  END SELFCORACC;

  -- Установка глобализмов
  PROCEDURE Init
  AS
    v_CodeOwnerID NUMBER(10);
    v_Stat NUMBER(10);
  BEGIN

    if M_OURBANK is null then

      M_OURBANK := RSBSESSIONDATA.OurBank();

      v_Stat := RSI_RSBPARTY.PT_GetPartyCodeEx( M_OURBANK, 3, M_SELFBIC, v_CodeOwnerID );
      if v_Stat <> 0 then
        M_SELFBIC := '';
      end if;


      begin

        select /*+ opt_param('_optimizer_connect_by_cost_based', 'true')*/
               t_CorAcc into M_SELFCORACC
        from ( select bd.t_CorAcc
                 from dparty_dbt pt
                inner join dbankdprt_dbt bd on bd.t_PartyID = pt.t_PartyID
                where bd.t_CorAcc <> CHR(1)
              connect by pt.t_PartyID = prior pt.t_Superior
                     and prior bd.t_CorAcc = CHR(1)
                start with pt.t_PartyID = M_OURBANK
                order by level ) pt2
        where rownum = 1;

      exception
        when NO_DATA_FOUND then
          M_SELFCORACC := '';
      end;

    end if;

  END Init;

  
  --Получение ноты для объекта p_ObjectType
  function GetNoteTextStr( p_ObjectID in number, p_ObjectType in number, p_NoteKind in number, p_Date in date ) return varchar2
  as
    v_Note varchar2(1500);
  begin
    
    SELECT NVL (
              (SELECT replace(UTL_RAW.CAST_TO_VARCHAR2(nt.t_Text), CHR(0), '')
                FROM dnotetext_dbt nt
              WHERE nt.t_ObjectType = p_objecttype
                    AND nt.t_DocumentID = LPAD ( p_ObjectID, CASE WHEN p_ObjectType IN (Rsb_Secur.OBJTYPE_PARTY, PM_COMMON.OBJTYPE_PAYMENT, PM_COMMON.OBJTYPE_AVOIRISS) THEN 10 ELSE 34 END, '0' )
                    -- Примечание вида
                    AND nt.t_NoteKind = p_NoteKind
                    -- Примечание действительно на текущий оперден
                    AND nt.t_ValidToDate >= RsbSessionData.CurDate
                    AND nt.t_Date <= p_Date),
              CHR(0))
    INTO v_Note
    FROM DUAL;
    
    return v_Note;
  END GetNoteTextStr;

  -- Получение "строковой" ноты платежа на дату
  function GetPaymNoteTextStr( p_PaymentID in number, p_Kind in number, p_Date in date ) return varchar2
  as
    v_Note varchar2(1500);
  begin
  
    v_Note := GetNoteTextStr( p_PaymentID, PM_COMMON.OBJTYPE_PAYMENT, p_Kind, p_Date);

    return v_Note;
  END GetPaymNoteTextStr;
      

  -- Изменение статуса платежа
  FUNCTION ChangePaymStatus( p_PaymentID IN NUMBER, p_PaymStatus IN NUMBER,
                             p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER,
                             p_Oper IN NUMBER ) RETURN NUMBER
  AS
    v_PmHistID   dpmhist_dbt.t_AutoKey%type;
    v_PrevStatus dpmpaym_dbt.t_PaymStatus%type;
    v_ValueDate  dpmpaym_dbt.t_ValueDate%type;
    v_IsPlanPaym dpmpaym_dbt.t_IsPlanPaym%type;
    v_IsFactPaym dpmpaym_dbt.t_IsFactPaym%type;
    v_stat    integer;
  BEGIN

    v_stat := 0;

    if p_PaymentID <> 0 then

      -- Изменение статуса одного платежа

      select t_PaymStatus, t_ValueDate, t_IsPlanPaym, t_IsFactPaym into v_PrevStatus, v_ValueDate, v_IsPlanPaym, v_IsFactPaym
      from dpmpaym_dbt
      where t_PaymentID = p_PaymentID;

      -- Собственно меняем статус платежа
      update dpmpaym_dbt
      set t_PaymStatus = p_PaymStatus
      where t_PaymentID = p_PaymentID;

      -- Вставляем запись истории
      insert into dpmhist_dbt
      ( t_AutoKey, t_PaymentID, t_StatusIDTo, t_StatusIDFrom, t_Oper, t_Date,
        t_SysDate, t_SysTime,
        t_OldValueDate, t_NewValueDATE, t_IsPlanPaym, t_IsFactPaym, t_Reserve, t_AccTrnID )
      values
      ( 0, p_PaymentID, p_PaymStatus, v_PrevStatus, p_Oper, RsbSessionData.curdate(),
        trunc(sysdate), to_date( '01010001' || to_char( sysdate, 'hh24miss' ), 'ddmmyyyyhh24miss' ),
        v_ValueDate, v_ValueDate, v_IsPlanPaym, v_IsFactPaym, CHR(1), 0 )
      returning t_AutoKey into v_PmHistID;

      -- Вставляем привязку к шагу операции
      if p_ID_Operation <> 0 then
        insert into doprdocs_dbt
        ( T_DOCKIND, T_DOCUMENTID, T_ID_OPERATION, T_ID_STEP, T_PART, T_STATUS, T_ORIGIN,
          T_SERVDOCKIND, T_SERVDOCID, T_AUTOKEY, T_LAUNCHOPER, T_FMTBLOBDATA_XXXX )
        values
        ( DLDOC_PAYMENTSTAT, lpad(v_PmHistID, 10, '0'), p_ID_Operation, p_ID_Step, 1, 0, 0,
          0, 0, 0, CHR(0), null );
      end if;

      -- Меняем статус свойств платежа
      if p_PaymStatus = PM_READY_TO_SEND then
        v_stat := ChangePmPropStatus( p_PaymentID, CHR(0), PM_PROP_READY );
      end if;

    else

      -- Массовое изменение статуса
      return MassChangePaymStatus( p_PaymStatus, p_Oper );

    end if;

    return v_stat;

  END ChangePaymStatus;

  -- Массовое изменение статуса платежей
  -- Может быть вызывно из ChangePaymStatus или отдельно
  FUNCTION MassChangePaymStatus( p_PaymStatus IN NUMBER, p_Oper IN NUMBER DEFAULT NULL ) RETURN INTEGER
  AS
    v_Oper    dpspohist_dbt.t_Oper%type;
    v_Date    dpspohist_dbt.t_Date%type;
    v_SysDate dpspohist_dbt.t_SysDate%type;
    v_SysTime dpspohist_dbt.t_SysTime%type;
    v_stat    integer;
  BEGIN

    v_stat := 0;
    v_Oper := p_Oper;
    if v_Oper is null then
      v_Oper := RsbSessionData.oper();
    end if;
    v_Date := RsbSessionData.curdate();
    select trunc(sysdate) into v_SysDate
    from dual;
    select to_date( '01010001' || to_char( sysdate, 'hh24miss' ), 'ddmmyyyyhh24miss' ) into v_SysTime
    from dual;

    -- Вставляем запись истории
    insert all
    into dpmhist_dbt
    ( t_AutoKey, t_PaymentID, t_StatusIDTo, t_StatusIDFrom, t_Oper, t_Date, t_SysDate, t_SysTime,
      t_OldValueDate, t_NewValueDATE, t_IsPlanPaym, t_IsFactPaym, t_Reserve, t_AccTrnID )
    values
    ( 0, v_PaymentID, p_PaymStatus, v_PrevStatus, p_UserID, p_OperDate, v_SysDate, v_SysTime,
      v_ValueDate, v_ValueDate, v_IsPlanPaym, v_IsFactPaym, PM_COMMON.RSB_EMPTY_STRING, 0 )
    select /*+leading(tmp pm) use_nl(pm)*/
           tmp.t_OrderID   as v_PaymentID,
           pm.t_PaymStatus as v_PrevStatus,
           pm.t_ValueDate  as v_ValueDate,
           pm.t_IsPlanPaym as v_IsPlanPaym,
           pm.t_IsFactPaym as v_IsFactPaym,
           decode(v_Oper, NULL, pm.t_Oper, v_Oper) p_UserID,
           decode(v_Date, to_date('01010001', 'DDMMYYYY'), pm.t_ValueDate, v_Date) p_OperDate
      from doprtemp_tmp tmp,
           dpmpaym_dbt  pm
     where pm.t_PaymentID = tmp.t_OrderID
       and tmp.t_ErrorStatus  = 0
       and tmp.t_SkipDocument = 0
       and pm.t_PaymStatus <> p_PaymStatus
       and pm.t_PaymStatus <> decode( p_PaymStatus, PM_FINISHED, PM_REJECTED, -1 );

    -- Вставляем привязку к шагу операции
    insert all
    into doprdocs_dbt
    ( T_DOCKIND, T_DOCUMENTID, T_ID_OPERATION, T_ID_STEP, T_PART, T_STATUS, T_ORIGIN,
      T_SERVDOCKIND, T_SERVDOCID, T_AUTOKEY, T_LAUNCHOPER, T_FMTBLOBDATA_XXXX )
    values
    ( DLDOC_PAYMENTSTAT, V_DOCUMENTID, V_ID_OPERATION, V_ID_STEP, 1, 0, 0,
      0, 0, 0, PM_COMMON.UNSET_CHAR, null )
    select lpad( ( select max( h.t_AutoKey )
                     from dpmhist_dbt h
                    where h.t_PaymentID = tmp.t_OrderID ), 10, '0' ) as V_DOCUMENTID,
           tmp.t_ID_Operation as V_ID_OPERATION,
           tmp.t_ID_Step      as V_ID_STEP
      from doprtemp_tmp tmp,
           dpmpaym_dbt pm
     where pm.t_PaymentID = tmp.t_OrderID
       and tmp.t_ErrorStatus = 0
       and tmp.t_SkipDocument = 0
       and tmp.t_ID_Operation > 0
       and pm.t_PaymStatus <> p_PaymStatus
       and pm.t_PaymStatus <> decode( p_PaymStatus, PM_FINISHED, PM_REJECTED, -1 );

    -- Собственно меняем статус платежа
    update dpmpaym_dbt pm
    set pm.t_PaymStatus = p_PaymStatus
    where pm.t_PaymentID in ( select tmp.t_OrderID
                                from doprtemp_tmp tmp
                                where tmp.t_ErrorStatus  = 0
                                  and tmp.t_SkipDocument = 0 )
      and pm.t_PaymStatus <> p_PaymStatus
      and pm.t_PaymStatus <> decode( p_PaymStatus, PM_FINISHED, PM_REJECTED, -1 );

    -- Меняем статус свойств платежа
    if p_PaymStatus = PM_READY_TO_SEND then
      v_stat := ChangePmPropStatus( 0, CHR(0), PM_PROP_READY );
    end if;

    if v_Stat = 0 then
      v_Stat := PaymStatusToXMLAll(1, p_PaymStatus);
    end if;

    return v_stat;

  END MassChangePaymStatus;


  -- Изменение статуса свойств платежа
  FUNCTION ChangePmPropStatus( p_PaymentID IN NUMBER, p_IsSender IN CHAR, p_PropStatus IN NUMBER, p_TpSchemID IN NUMBER )
    RETURN NUMBER
  AS
    RollbackData_c RSI_RsbOperation.BkoutData_cur;
  BEGIN

    if p_PaymentID <> 0 then

      -- Изменение статуса одной записи
      update dpmprop_dbt
      set t_PropStatus = p_PropStatus
      where t_PaymentID = p_PaymentID
        and ( t_TpSchemID = p_TpSchemID or p_TpSchemID is NULL )
        and ( t_IsSender = p_IsSender or p_IsSender is NULL );

    else

      begin
        -- сохраняем для возможности отката старый PropStatus
        insert into doprdocs_dbt
        ( T_DOCKIND, T_DOCUMENTID, T_ID_OPERATION, T_ID_STEP, T_PART, T_STATUS, T_ORIGIN,
          T_SERVDOCKIND, T_SERVDOCID, T_AUTOKEY, T_LAUNCHOPER, T_FMTBLOBDATA_XXXX )
        select distinct DLDOC_PAYMENTPROPSTAT, lpad( otmp.t_OrderID, 10, '0' )     ||
                                               lpad( prop.t_PropStatus, 5, '0' )   ||
                                               lpad( p_PropStatus, 5, '0' )        ||
                                               lpad( otmp.t_ID_Operation, 8, '0' ) ||
                                               lpad( otmp.t_ID_Step, 4, '0' ),
                        otmp.t_ID_Operation, otmp.t_ID_Step, 1, 0, 0, 0, 0, 0, CHR(0), null
        from doprtemp_tmp otmp,
             dpmprop_dbt  prop
        where otmp.t_ErrorStatus = 0
          and otmp.t_SkipDocument = 0
          and otmp.t_OrderID = prop.t_PaymentID
          and ( prop.t_TpSchemID = p_TpSchemID or p_TpSchemID is NULL )
          and ( prop.t_IsSender = p_IsSender or p_IsSender is NULL )
          and ( NOT EXISTS( SELECT 1
                              FROM dwlpm_dbt wlpm, dwlmeslnk_dbt lnk, dwlmes_dbt mes
                             WHERE wlpm.t_PaymentID = prop.t_PaymentID
                               AND lnk.t_ObjID = wlpm.t_WlPmID
                               AND lnk.t_ObjKind = PM_COMMON.OBJTYPE_PAYMENT
                               AND lnk.t_MesID = mes.t_MesID
                               AND mes.T_DIRECT = WLD_COMMON.WLD_MES_OUT
                               AND mes.t_State < WLD_COMMON.WLD_STATUS_MES_SEND )
                OR p_PropStatus <> PM_COMMON.PM_PROP_CLOSED );

        -- Массовое изменение статуса
        update dpmprop_dbt
        set t_PropStatus = p_PropStatus
        where t_PaymentID in ( select t_OrderID
                                 from doprtemp_tmp
                                where t_ErrorStatus = 0
                                  and t_SkipDocument = 0
                                  and ( NOT EXISTS( SELECT 1
                                                      FROM dwlpm_dbt wlpm, dwlmeslnk_dbt lnk, dwlmes_dbt mes
                                                     WHERE wlpm.t_PaymentID = t_OrderID
                                                       AND lnk.t_ObjID = wlpm.t_WlPmID
                                                       AND lnk.t_ObjKind = PM_COMMON.OBJTYPE_PAYMENT
                                                       AND lnk.t_MesID = mes.t_MesID
                                                       AND mes.T_DIRECT = WLD_COMMON.WLD_MES_OUT
                                                       AND mes.t_State < WLD_COMMON.WLD_STATUS_MES_SEND )
                                        OR p_PropStatus <> PM_COMMON.PM_PROP_CLOSED ) )
          and ( t_TpSchemID = p_TpSchemID or p_TpSchemID is NULL )
          and ( t_IsSender = p_IsSender or p_IsSender is NULL )
          and (t_PropStatus <> PM_COMMON.PM_PROP_CORREJECTED OR p_PropStatus <> PM_COMMON.PM_PROP_CLOSED);

        if p_PropStatus >= PM_COMMON.PM_PROP_DISCHARGED OR p_PropStatus = PM_COMMON.PM_PROP_REJECTED OR
           p_PropStatus <  PM_COMMON.PM_PROP_READY
        then

           if p_PropStatus <  PM_COMMON.PM_PROP_READY then
             -- Сохранение данных отката
             open RollbackData_c for select tmp.t_ID_Operation, tmp.t_ID_Step,
                                            'update dwlpm_dbt' ||
                                              ' set t_PropStatus =' || to_char(wlpm.t_PropStatus) ||
                                            ' where t_PaymentID =' || to_char(wlpm.t_PaymentID) ||
                                              ' and t_WlPmNum = 0'
                                       from doprtemp_tmp tmp,
                                            dwlpm_dbt wlpm
                                      where tmp.t_SkipDocument = 0
                                        and tmp.t_ErrorStatus  = 0
                                        and wlpm.t_PaymentID = tmp.t_OrderID
                                        and t_WlPmNum = 0
                                        and (t_PropStatus <> PM_COMMON.PM_PROP_CORREJECTED OR p_PropStatus <> PM_COMMON.PM_PROP_CLOSED);

             RSI_RsbOperation.SetBkoutDataForAll( RollbackData_c );

             close RollbackData_c;
           end if;

           update dwlpm_dbt
              set t_PropStatus = p_PropStatus
            where t_PaymentID in ( select t_OrderID
                                 from doprtemp_tmp
                                where t_ErrorStatus = 0
                                  and t_SkipDocument = 0
                                  and ( NOT EXISTS( SELECT 1
                                                      FROM dwlpm_dbt wlpm, dwlmeslnk_dbt lnk, dwlmes_dbt mes
                                                     WHERE wlpm.t_PaymentID = t_OrderID
                                                       AND lnk.t_ObjID = wlpm.t_WlPmID
                                                       AND lnk.t_ObjKind = PM_COMMON.OBJTYPE_PAYMENT
                                                       AND lnk.t_MesID = mes.t_MesID
                                                       AND mes.T_DIRECT = WLD_COMMON.WLD_MES_OUT
                                                       AND mes.t_State < WLD_COMMON.WLD_STATUS_MES_SEND )
                                       OR p_PropStatus <> PM_COMMON.PM_PROP_CLOSED) )
              and t_WlPmNum = 0
              and (t_PropStatus <> PM_COMMON.PM_PROP_CORREJECTED OR p_PropStatus <> PM_COMMON.PM_PROP_CLOSED);
        end if;
      exception when others then return 1;

      end;

    end if;

    return 0;

  END ChangePmPropStatus;

  -- Проверяет, является ли счёт банковским
  FUNCTION IsOwnerAccOwnBank( Acc IN VARCHAR2 ) RETURN NUMBER
  AS
  BEGIN

    if( Acc like '30109%' or
        Acc like '30111%' or
        Acc like '30112%' or
        Acc like '30113%' or
        Acc like '30122%' or
        Acc like '30123%' or
        Acc like '30214%' or
        Acc like '30230%' or
        Acc like '30231%' or
        Acc like '30401%' or
        Acc like '30403%' or
        Acc like '30405%' or
        Acc like '30606%' or
        Acc like '312%'   or
        Acc like '313%'   or
        Acc like '314%'   or
        Acc like '315%'   or
        Acc like '316%'   or
        Acc like '401%'   or
        Acc like '402%'   or
        Acc like '403%'   or
        Acc like '404%'   or
        Acc like '405%'   or
        Acc like '406%'   or
        Acc like '407%'   or
        Acc like '408%'   or
        Acc like '41%'    or
        Acc like '42%'    or
        Acc like '43%'
      )
    then
      return 0;
    else
      return 1;
    end if;

  END IsOwnerAccOwnBank;

  FUNCTION GetDepartmentByOper( p_Oper IN NUMBER )
    RETURN NUMBER
  AS
    v_Department NUMBER;
  BEGIN
    select t.T_CODEDEPART into v_Department
      from dperson_dbt t
     where t.T_OPER = p_Oper;
    return v_Department;
    exception when NO_DATA_FOUND then return null;
  END GetDepartmentByOper;

  FUNCTION DeletePayment( p_PaymentID IN NUMBER  ) RETURN INTEGER
  AS
    v_stat INTEGER;
    DocID  VARCHAR2(34);

    function CheckDelPmSend( p_ObjectID in number, p_ObjectType in number ) return integer
    as
      v_stat integer := 0;
    begin
      for rec in ( select pmsend.t_State
                   from dpmsend_dbt pmsend
                   where pmsend.t_ObjectID = p_ObjectID
                     and pmsend.t_ObjectType = p_ObjectType
                     and pmsend.t_State < PM_CONNECT.WLD_STATUS_PMSEND_RECANSWER )
      loop

        if rec.t_State = PM_CONNECT.WLD_STATUS_PMSEND_SEND then
          v_stat :=  PM_ERROR.PAYMERR_PMSEND_DELETE_SEND;
        end if;

        if rec.t_State = PM_CONNECT.WLD_STATUS_PMSEND_READY or rec.t_State = PM_CONNECT.WLD_STATUS_PMSEND_SENDERROR then
          v_stat :=  PM_ERROR.PAYMERR_PMSEND_DELETE_READY;
        end if;

      end loop;

      return v_stat;
    end CheckDelPmSend;

    function CheckDelPm( p_PaymentID IN NUMBER  ) RETURN INTEGER
    as
      v_stat INTEGER := 0;
    begin
      FOR rec IN  ( SELECT  dossier.T_STATE, dolnk.T_GENERAL
                      FROM  dwldolnk_dbt dolnk, dwldossie_dbt dossier
                     WHERE  dolnk.t_ObjID = p_PaymentID
                       AND  dolnk.t_ObjType = OBJTYPE_PAYMENT
                       AND  dossier.T_DOSSIERID = dolnk.T_DOSSIERID )
      LOOP

        IF rec.t_State = 40 or rec.t_State = 60 THEN
          v_stat :=  7275;
        END IF;

        IF rec.t_General = 'X' THEN
          v_stat :=  7276;
        END IF;

      END LOOP;

      if v_stat = 0 and IsBdTrSummaryPayment(p_PaymentID) then
        v_stat := PM_ERROR.PAYMERR_DELPM_BDTRSUM;
      end if;

      if v_stat = 0 then
        v_stat := CheckDelPmSend( p_PaymentID, OBJTYPE_PAYMENT );
      end if;

      return v_stat;
    end CheckDelPm;

  BEGIN

    v_stat := PaymStatusToXmlAll(1, 0, p_PaymentID, 'Удален');

    IF ( p_PaymentID <> 0 ) THEN

      v_stat := CheckDelPm(p_PaymentID);
      IF v_stat <> 0 THEN 
        return v_stat; 
      END IF;

      DELETE FROM DACCISPR_DBT
       WHERE (T_DOCKIND, T_DOCUMENTID) IN
        (SELECT T_DOCKIND, T_DOCUMENTID FROM DPMPAYM_DBT
          WHERE T_PAYMENTID = p_PaymentID);

      DELETE FROM DPMPAYM_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      DELETE FROM DPMADDPI_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      DELETE FROM DPMRMPROP_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      DELETE FROM DPMPROP_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      DELETE FROM DPMDEMAND_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      DELETE FROM DPMAKKR_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      DELETE FROM DPMCO_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      DELETE FROM DPMCURTR_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      DELETE FROM DPMKZ_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      DELETE FROM DPMTERROR_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      DELETE FROM DPMREPSES_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      DELETE FROM DSFINVLNK_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      DELETE FROM DPMPTFM_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      DELETE FROM DPMBFLNK_DBT
      WHERE T_PAYMENTID = p_PaymentID;

      /*DELETE FROM DOBJGUID_DBT
      WHERE T_OBJECTID = p_PaymentID
        AND T_OBJECTTYPE in (OBJTYPE_PAYMENT, OBJTYPE_PMCLAIM);*/

      DELETE FROM DOBJGUID_DBT
      WHERE T_OBJECTID = p_PaymentID
        AND T_OBJECTTYPE = OBJTYPE_PAYMENT;

      v_stat := WLD_COMMON.RSI_DeleteWlPm(p_PaymentID);
      IF v_stat <> 0 THEN 
        return v_stat; 
      END IF;

      DELETE FROM DPMROUTE_DBT
      WHERE T_OBJID = p_PaymentID AND T_OBJKIND = OBJTYPE_PAYMENT;

      DELETE FROM DPMSEND_DBT
      WHERE T_OBJECTID = p_PaymentID AND T_OBJECTTYPE = OBJTYPE_PAYMENT AND T_STATE <> PM_CONNECT.WLD_STATUS_PMSEND_SEND;

      DocID := LPAD( p_PaymentID, 34, '0' );
      v_stat := RSBSIGN.DropSgnMarksForDoc( DLDOC_PAYMENT, DocID);

      IF v_stat <> 0 THEN 
        return v_stat; 
      END IF;

    END IF;

    RETURN v_stat;
  END DeletePayment;

 function GetCodeOwner( p_CodeKind in integer, p_Code in varchar2, p_CodeOwnerID in out integer, p_Department out ddp_dep_dbt%rowtype, p_Date in date )
  return boolean
  as
  begin

    select pt.t_PartyID, nvl(dp.t_Code,0), nvl(dp.t_AccessMode,0), nvl(dp.t_NodeType,0)
      into p_CodeOwnerID, p_Department.t_Code, p_Department.t_AccessMode, p_Department.t_NodeType
      from dobjcode_dbt oc
     inner join dparty_dbt pt  on pt.t_PartyID = oc.t_ObjectID
      left join ddp_dep_dbt dp on dp.t_PartyID = oc.t_ObjectID
                              and dp.t_Status <> DEPARTMENT_STATUS_CLOSED
     where oc.t_CodeKind = p_CodeKind
       and oc.t_Code = p_Code
       and oc.t_ObjectType = OBJTYPE_PARTY
       and p_Date is not null and ( oc.t_BankDate <= p_Date and ( oc.t_BankCloseDate > p_Date or oc.t_BankCloseDate = PM_COMMON.RSB_EMPTY_DATE ) )
       and rownum = 1;

    return true;

  exception
    when NO_DATA_FOUND then return false;

  end GetCodeOwner;

  -- является ли счет корреспондентским
  function IsCorrAcc( p_FIID in integer, p_Chapter in integer, p_Account in varchar2 ) return boolean
  as
    v_IsNostro char;
    v_regVal number(10):= getTypeCorschemForExtPayment();
  begin

    select t_IsNostro into v_IsNostro 
      from dcorschem_dbt 
      where t_FIID = p_FIID 
        and t_Account = p_Account;

    if v_regVal is null or not v_regVal in (REGVAL_TCFEP_ANY, REGVAL_TCFEP_LORO, REGVAL_TCFEP_NOSTRO, REGVAL_TCFEP_NOTEXT) then
      v_regVal := REGVAL_TCFEP_ANY;
    end if;

    if v_IsNostro = PM_COMMON.SET_CHAR and v_regVal in (REGVAL_TCFEP_ANY, REGVAL_TCFEP_NOSTRO) then
      return true;
    elsif v_IsNostro = PM_COMMON.UNSET_CHAR and v_regVal in (REGVAL_TCFEP_ANY, REGVAL_TCFEP_LORO) then
      return true;
    end if;

    return false;

  exception
    when NO_DATA_FOUND then return false;

  end IsCorrAcc;

  -- получить структуру филиала по ID
  function GetPartyDep( p_PartyID in integer, p_Department out ddp_dep_dbt%rowtype ) return boolean
  as
  begin

    select * into p_Department
      from ddp_dep_dbt dp
     where dp.t_PartyID = p_PartyID
       and dp.t_Status <> DEPARTMENT_STATUS_CLOSED;

    return true;

  exception
    when NO_DATA_FOUND then return false;

  end GetPartyDep;
  
  -- найти код субъекта
  function FindPartCode( p_PartyID  in  integer, p_CodeKind in  integer ) return dpartcode_dbt.t_Code%type
  as
    v_Code dpartcode_dbt.t_Code%type;
  begin
    
    if p_PartyID = PM_COMMON.UNKNOWNPARTY then
      return null;
    end if;

    select t_Code into v_Code
      from dpartcode_dbt
     where t_PartyID = p_PartyID 
       and t_CodeKind = p_CodeKind;

    return v_Code;
  exception
    when NO_DATA_FOUND then return null;
  end FindPartCode;

  function CompareAccWithMask( p_Mask     in varchar2,
                               p_Account  in varchar2 ) return integer deterministic
  as
    m_Mask VARCHAR2(2000);
  begin
    if p_Mask != CHR(1) then

      m_Mask := '^(' || replace(
                        replace(
                        replace(
                        replace(
                        replace(
                        p_Mask,
                        ' ', '' ),
                        ',', '|' ),
                        ';', '|' ),
                        '*', '.*' ), '?', '.{1}')  || ')';
      if regexp_like( p_Account, m_Mask ) then
        return 1;
      end if;
    end if;

    return 0;
    
  end CompareAccWithMask;

  /*Перенесено на уровень пакета, т.к. будет использоваться в других функциях этого пакета*/
    function GetBankCorAcc( p_PartyID in number ) return varchar2
    as
      v_CorAcc varchar2(25);
    begin

      select  t_CorAcc into v_CorAcc
        from dbankdprt_dbt bd 
              where  bd.t_PartyID = p_PartyID
      and rownum = 1;

      return v_CorAcc;

    exception
      when NO_DATA_FOUND then return chr(1);

    end GetBankCorAcc;

  function PM_SetPI( p_Context        in     varchar2,
                     p_Group          in out integer,
                     p_IsPayer        in     char,
                     p_BankID         in out integer,
                     p_BankCodeKind   in out integer,
                     p_BankCode       in out varchar2,
                     p_BankName       in out varchar2,
                     p_CorrAcc        in out varchar2,
                     p_FIID           in     integer,
                     p_Chapter        in     integer,
                     p_Account        in     varchar2,
                     p_Client         in out integer,
                     p_ClientCodeKind in out integer,
                     p_ClientCode     in     varchar2,
                     p_ClientINN      in out varchar2,
                     p_ClientName     in out varchar2,
                     p_ValueDate      in date,
                     p_OurCorrAcc     in out varchar2,
                     p_InOurBalance   in out char,
                     p_DebetCredit    in     integer,
                     p_Purpose in integer
                   ) return boolean
  as
    v_stat            boolean;
    v_int             number(5);
    v_tmpCodeOwner    number(10);
    v_tmpAccessMode   number(5);
    v_Dep             ddp_dep_dbt%rowtype;
    v_Code            dpmprop_dbt.t_BankCode%type;

    v_tmpGroup        number(5);
    v_tmpBankID       number(10);
    v_tmpClient       number(10);
    v_tmpBankCode     dpmprop_dbt.t_BankCode%type;
    v_PartyID         number(10);
    v_FillBank        number(5);
    v_acc             daccount_dbt%rowtype;
    v_tmpClientINN    dpmrmprop_dbt.t_PayerINN%type;

  begin

    v_stat := true;

    v_tmpGroup    := p_Group;
    v_tmpBankID   := p_BankID;
    v_tmpClient   := p_Client;
    v_tmpBankCode := p_BankCode;

    if v_tmpBankID > 0 then -- 4.1.

      -- определяем код банка
      if p_BankCodeKind > 0 then -- Задан вид кода банка 4.1.2.
        if v_tmpBankCode = chr(1) then -- 4.1.2.1.
          v_tmpBankCode := FindPartCode( v_tmpBankID, p_BankCodeKind );
          if v_tmpBankCode is null then
            v_stat := false;
          end if;
        end if;
      end if;

      -- определяем группу платежа
      if v_tmpGroup = PAYMENTS_GROUP_UNDEF then -- 4.1.4
        if p_Account <> chr(1) and p_Purpose <> PM_PURP_BANKPAYORDER and IsCorrAcc(p_FIID, p_Chapter, p_Account) then -- 4.1.4.1
          v_tmpGroup := PAYMENTS_GROUP_EXTERNAL;
        else
          v_stat := GetPartyDep( v_tmpBankID, v_Dep );

          if v_Dep.t_Code > 0 and v_Dep.t_AccessMode <> DEPARTMENT_ACCESS_NOTCABS then
            if v_Dep.t_NodeType = DEPARTMENT_TYPE_VSP then
              return false;
            else
              v_tmpGroup := PAYMENTS_GROUP_BRANCH;
            end if;
          else
            v_tmpGroup := PAYMENTS_GROUP_EXTERNAL;
          end if;
        end if;
      end if;

      -- определяем клиента
      if p_Account <> chr(1) and v_tmpGroup = PAYMENTS_GROUP_BRANCH and v_tmpClient in (0, UNKNOWNPARTY) then -- 4.1.6
        begin
          select * into v_acc
            from daccount_dbt
          where t_code_currency = p_FIID
             and t_chapter = p_Chapter
             and t_account = p_Account;

        exception
          when NO_DATA_FOUND then
            v_tmpClient := 0;
            v_PartyID := 0;
            v_stat := false;

        end;

        if v_stat = true then
          v_tmpClient := PM_NAMES.PM_GetClientByAccount( v_acc, p_Context );

          select t_partyid  into v_PartyID
            from ddp_dep_dbt
           where t_code = v_acc.t_department;

          if v_tmpBankID <> v_PartyID then
            return false;
          end if;
        end if;

        v_stat := true;
      end if;

    elsif v_tmpBankID <= 0 then -- 4.2.

      if v_tmpBankCode <> chr(1) then -- 4.2.1

        if p_BankCodeKind <= 0 then return false; end if;

        -- 4.2.1.2
        -- Получить субъекта-носителя кода
        v_stat := GetCodeOwner(p_BankCodeKind, v_tmpBankCode, v_tmpCodeOwner, v_Dep, p_ValueDate);
        if v_stat = false then return false; end if;

        if p_Account = chr(1) then  -- 4.2.1.5
          -- 4.2.1.5.1
          v_tmpBankID := v_tmpCodeOwner;
        end if;

        -- 4.2.1.6
        if v_tmpGroup = PAYMENTS_GROUP_UNDEF then
          if p_Account <> chr(1) and p_Account is not null and IsCorrAcc(p_FIID, p_Chapter, p_Account) then
            v_tmpGroup := PAYMENTS_GROUP_EXTERNAL;
          else
            if v_Dep.t_Code > 0 and v_Dep.t_AccessMode <> DEPARTMENT_ACCESS_NOTCABS then
              if v_Dep.t_NodeType = DEPARTMENT_TYPE_VSP then
                return false;
              else
                v_tmpGroup := PAYMENTS_GROUP_BRANCH;
              end if;
            else
              v_tmpGroup := PAYMENTS_GROUP_EXTERNAL;
            end if;
          end if;
        end if;

        -- 4.2.1.7
        if p_Account <> chr(1) and p_Account is not null then  
          if v_Dep.t_Code > 0 then -- 4.2.1.7.1
          begin

            if v_Dep.t_NodeType = DEPARTMENT_TYPE_VSP then
              return false;
            end if;

            select dp.t_partyid, acc.t_client, nvl(dp.t_accessmode, DEPARTMENT_ACCESS_NOTCABS)
              into v_tmpBankID, v_tmpClient, v_tmpAccessMode
              from daccount_dbt acc, ddp_dep_dbt dp
             where dp.t_code = acc.t_department
               and acc.t_code_currency = p_FIID
               and acc.t_chapter = p_Chapter
               and acc.t_account = p_Account;

            select /*+FIRST_ROWS*/ dp3.t_Code into v_Code
              from ( select dp2.t_PartyID, oc.t_Code
                       from ( select dp.t_PartyID, level dp_level
                                from ddp_dep_dbt dp
                             connect by dp.t_Code = prior dp.t_ParentCode
                             start with dp.t_PartyID = v_tmpBankID ) dp2
                      inner join dobjcode_dbt oc on oc.t_ObjectType = OBJTYPE_PARTY
                                                and oc.t_CodeKind = p_BankCodeKind
                                                and oc.t_ObjectID = dp2.t_PartyID
                                                and oc.t_State = 0
                      order by dp2.dp_level ) dp3
             where rownum = 1;

            if v_Code <> v_tmpBankCode then
              raise NO_DATA_FOUND;
            elsif v_tmpGroup = PAYMENTS_GROUP_UNDEF then
              if v_tmpAccessMode <> DEPARTMENT_ACCESS_NOTCABS then
                v_tmpGroup := PAYMENTS_GROUP_BRANCH;
              else
                v_tmpBankID := v_tmpCodeOwner;
                if v_Dep.t_AccessMode <> DEPARTMENT_ACCESS_NOTCABS then
                  v_tmpGroup := PAYMENTS_GROUP_BRANCH;
                else
                  v_tmpGroup := PAYMENTS_GROUP_EXTERNAL;
                end if;
              end if;
            end if;

          exception
            when NO_DATA_FOUND then
            begin
              v_tmpBankID := v_tmpCodeOwner;
              if v_tmpGroup = PAYMENTS_GROUP_UNDEF then
                if v_Dep.t_AccessMode <> DEPARTMENT_ACCESS_NOTCABS then
                  v_tmpGroup := PAYMENTS_GROUP_BRANCH;
                else
                  v_tmpGroup := PAYMENTS_GROUP_EXTERNAL;
                end if;
              end if;
            end;
          end;
          else -- 4.2.1.7.2

            v_tmpBankID := v_tmpCodeOwner;
            if v_tmpGroup = PAYMENTS_GROUP_UNDEF then
              v_tmpGroup := PAYMENTS_GROUP_EXTERNAL;
            end if;

          end if;

        end if;

        else -- 4.2 .2        
          if v_tmpGroup <> PAYMENTS_GROUP_BRANCH then
            v_tmpBankID := PM_COMMON.UNKNOWNPARTY;
            v_tmpGroup  := PAYMENTS_GROUP_EXTERNAL;             
          end if;
      end if;

      if v_tmpClient <= 0 then
        v_stat := GetCodeOwner( p_ClientCodeKind, p_ClientCode, v_tmpClient, v_Dep, p_ValueDate );
      end if;

    end if;

    v_FillBank := RSB_COMMON.GetRegIntValue( p_Context || '\FILLBANK' );

    if v_FillBank > PM_NAMES.FN_NAMEBICDIR or v_FillBank < PM_NAMES.FN_NAME_ADDRCB_CA then
      v_FillBank := PM_NAMES.FN_NAME_ADDRCB_CA;
    end if;

    if v_tmpBankID > 0 and p_CorrAcc = chr(1) and ( v_FillBank = PM_NAMES.FN_NAME_ADDRCB_CA or
                                                    v_FillBank = PM_NAMES.FN_NAME_PLACE ) then
      p_CorrAcc := GetBankCorAcc( v_tmpBankID );
    end if;

    if v_tmpClient > 0 then
      
      if p_ClientINN = RSB_EMPTY_STRING then

      if p_DebetCredit = PRT_CREDIT and CompareAccWithMask(GetFillINNReceiverByContext(p_Context),p_Account) != 1 then
        v_int := RSI_RSBPARTY.PT_GetPartyCodeEx( v_tmpClient, PTCK_INN, p_ClientINN, v_tmpCodeOwner );
      end if;
      if p_DebetCredit = PRT_DEBIT and CompareAccWithMask(GetFillINNPayerByContext(p_Context),p_Account) != 1 then
        v_int := RSI_RSBPARTY.PT_GetPartyCodeEx( v_tmpClient, PTCK_INN, p_ClientINN, v_tmpCodeOwner );
      end if;

    end if;

    -- определение наименования клиента
      if p_ClientName = RSB_EMPTY_STRING then

      -- Получить субъекта-носителя кода
      v_stat := GetCodeOwner(p_BankCodeKind, v_tmpBankCode, v_tmpCodeOwner, v_Dep, p_ValueDate);

      v_int := PM_NAMES.PM_SetClientName( p_Context,
                                          v_tmpBankID,
                                          p_Chapter,
                                          p_FIID,
                                          p_Account,
                                          p_IsPayer,
                                          v_tmpCodeOwner,
                                          null,
                                          v_tmpClient,
                                          p_ClientName,
                                          v_tmpClientINN
                                        );
    end if;

    end if;

    -- Если в ИНН находится empty_INN, то это означает что изначально поле ИНН было пустое
    -- и настройка TSetPISetting::INN была установлена в false, то есть в этом случаи при пустом ИНН 
    -- во время установки ПИ заполнять его не надо 
    if p_ClientINN = 'empty_INN' then
       -- Тут просто сбрасываем это временное значение в chr(1)
       p_ClientINN := RSB_EMPTY_STRING;
    end if;

    -- определение наименования банка
    if p_BankName = RSB_EMPTY_STRING then
      v_int := PM_NAMES.PM_SetBankNameCABS( p_Context, v_tmpGroup, v_tmpBankID, p_BankCodeKind, v_tmpBankCode, null, p_BankName );
    end if;

    if p_OurCorrAcc = chr(1) and p_Account <> chr(1) and IsCorrAcc(p_FIID, p_Chapter, p_Account) and
       v_tmpGroup = PAYMENTS_GROUP_EXTERNAL then
      p_OurCorrAcc := p_Account;
      p_InOurBalance := SET_CHAR;
    end if;

    p_Group    := v_tmpGroup;
    p_BankID   := v_tmpBankID;
    p_Client   := v_tmpClient;
    p_BankCode := v_tmpBankCode;

    return true;

  exception
    when others then
    begin
      --p_BankID := UNKNOWNPARTY;
      --p_Group := PAYMENTS_GROUP_UNDEF;
      return false;
    end;

  end PM_SetPI;

  function PM_SetDepartments( p_DocKind        in     integer,
                              p_BankID         in     integer,
                              p_Account        in     varchar2,
                              p_FIID           in     integer,
                              p_Chapter        in     integer,
                              p_DebetCredit    in     integer,
                              p_Group          in     integer,
                              p_Corschem       in     integer,
                              p_CorFIID        in     integer,
                              p_StartDep       in out integer,
                              p_Dep            in out integer,
                              p_EndDep         in out integer,
                              p_FutureAccount  in out varchar2,
                              p_FIID_FutureAcc in out integer
                            ) return boolean
  as
    v_stat               boolean;
    v_Department         ddp_dep_dbt%rowtype;
    v_tmpFutureAccount   varchar2(35);
    v_tmpFIID_FutureAcc  number(10);
    v_tmpDep             number(5);
    v_count              integer;

    -- В этих платежах надо смотреть на то, что счёт - СМФР. Если так, то филиал = филиал корреспондента
    function needLookupInMFRAccounts( p_DocKind in integer, p_DebetCredit in integer ) return boolean
    as
    begin
      if p_DebetCredit = PRT_DEBIT then
        if  p_DocKind = DLDOC_BANKCLAIM then
          return true;
        end if;
      elsif p_DocKind <> DLDOC_BANKCLAIM     and
            p_DocKind <> CB_MULTYDOC         and
            p_DocKind <> DLDOC_MEMORIALORDER and
            p_DocKind <> CASH_BOF_ADDORDER   and
            p_DocKind <> CASH_PS_INCORDER    and
            p_DocKind <> CASH_PS_OUTORDER    and
            p_DocKind <> CASH_BOF_INCORDER   and
            p_DocKind <> CASH_BOF_OUTORDER   then
        return true;
      end if;
      return false;
    end needLookupInMFRAccounts;

    -- Поискать счёт среди СМФР. Найден - филиал = филиал корреспондента.
    procedure lookupInMFRAccounts( p_Account in varchar2, p_FIID in integer, p_Department in out ddp_dep_dbt.t_Code%type )
    as
      v_CorrDepartment ddp_dep_dbt.t_Code%type;
    begin
      SELECT /*+FIRST_ROWS(1)*/
             doc.t_corrdepartmentid
        INTO v_CorrDepartment
        FROM dmccateg_dbt cat,
             dmcaccdoc_dbt doc
       WHERE cat.t_number in (1010, 1011)
         AND doc.t_catid = cat.t_id
         AND doc.t_account = p_Account
         AND doc.t_currency = p_FIID
         AND doc.t_chapter = CHAPT1
         AND ROWNUM <= 1;
      p_Department := v_CorrDepartment;
    exception
      when NO_DATA_FOUND then
        null;
    end lookupInMFRAccounts;

  begin

    -- надо ли вообще искать филиалы?
    if p_FutureAccount <> chr(1) and p_FutureAccount is not null then
      if p_DocKind = DLDOC_BANKCLAIM then
        if p_DebetCredit = PRT_DEBIT then
          if p_EndDep > 0 then return true; end if;
        else
          if p_StartDep > 0 and p_Dep > 0 then
            return true;
          elsif p_StartDep > 0 and p_Dep = 0 then
            p_Dep := p_StartDep;
            return true;
          end if;
        end if;
      else
        if p_DebetCredit = PRT_DEBIT then
          if p_StartDep > 0 and p_Dep > 0 then
            return true;
          elsif p_StartDep > 0 and p_Dep = 0 then
            p_Dep := p_StartDep;
            return true;
          end if;
        else
          if p_EndDep > 0 then return true; end if;
        end if;
      end if;
    end if;

    -- инициализация
    v_tmpDep            := 0;
    v_tmpFutureAccount  := p_FutureAccount;
    v_tmpFIID_FutureAcc := p_FIID_FutureAcc;

    if p_Group = PAYMENTS_GROUP_INTERNAL then

      if needLookupInMFRAccounts( p_DocKind, p_DebetCredit ) then
        lookupInMFRAccounts( p_Account, p_FIID, v_tmpDep );
      end if;

      if v_tmpDep = 0 or v_tmpDep is null then
        select * into v_Department
          from ddp_dep_dbt
         where t_PartyID = p_BankID
           and t_Status <> DEPARTMENT_STATUS_CLOSED;

        if v_Department.t_NodeType = DEPARTMENT_TYPE_FILIAL then
          v_tmpDep := v_Department.t_Code;
        else
          v_tmpDep := v_Department.t_ParentCode;
        end if;
      end if;

      if v_tmpFutureAccount = chr(1) then
         v_tmpFutureAccount  := p_Account;
         v_tmpFIID_FutureAcc := p_FIID;
      end if;

    elsif p_Group = PAYMENTS_GROUP_EXTERNAL then

      if p_Corschem <> -1 then
        if p_FutureAccount <> chr(1) then
           v_tmpFutureAccount  := p_FutureAccount;
           v_tmpFIID_FutureAcc := p_FIID_FutureAcc;
        else
          -- определим FutureAccount из корсхемы
          select t_Account, t_FIID into v_tmpFutureAccount, v_tmpFIID_FutureAcc
            from dcorschem_dbt
           where t_Number = p_Corschem
             and t_FIID = p_CorFIID;
        end if;

        -- определяем филиал по счету
        select acc.t_Department into v_tmpDep
          from daccount_dbt acc, ddp_dep_dbt dp
         where acc.t_Department = dp.t_Code
           and acc.t_Code_Currency = v_tmpFIID_FutureAcc
           and acc.t_Chapter = CHAPT1
           and acc.t_Account = v_tmpFutureAccount;
      end if;

    end if;

    if p_FutureAccount = chr(1) or p_FutureAccount is null then
      p_FutureAccount  := v_tmpFutureAccount;
      p_FIID_FutureAcc := v_tmpFIID_FutureAcc;
    end if;
    -- Future счет существует в нашей базе?
    select count(1) into v_count
      from daccount_dbt acc
     where acc.t_Code_Currency = p_FIID_FutureAcc
       and acc.t_Chapter = p_Chapter
       and acc.t_Account = p_FutureAccount;
    if v_count = 0 or v_count is null then
      p_FutureAccount  := RSB_EMPTY_STRING;
    end if;

    if p_DocKind = DLDOC_BANKCLAIM then
      if p_DebetCredit = PRT_DEBIT then
        p_StartDep := 0;     -- чтобы StartDep не обновлялся
        p_Dep := 0;            -- чтобы Dep не обновлялся
        p_EndDep := v_tmpDep;
      else
        p_StartDep := v_tmpDep;
        p_Dep      := v_tmpDep;
        p_EndDep := 0;      -- чтобы EndDep не обновлялся
      end if;
    else
      if p_DebetCredit = PRT_DEBIT then
        p_StartDep := v_tmpDep;
        p_Dep      := v_tmpDep;
        p_EndDep := 0;   -- чтобы EndDep не обновлялся
      else
        p_StartDep := 0;   -- чтобы StartDep не обновлялся
        p_Dep := 0;    -- чтобы Dep не обновлялся
        p_EndDep := v_tmpDep;
      end if;
    end if;

    return true;

  exception
    when others then return false;

  end PM_SetDepartments;

  -- массовое доопредление полей платежей, указанных в dpmproc_dbt
  procedure SetPIForPmMass
  as
    v_stat          boolean default true;
    v_IsCurrPaym    char;
    v_Context       varchar(100);
  begin

    for rec in ( select t.*
                   from ( select decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_PayerBankID, pm.t_ReceiverBankID) as t_BankID,
                                 decode(prop.t_DebetCredit, PRT_DEBIT, rm.t_PayerCorrAccNostro, rm.t_ReceiverCorrAccNostro) as t_CorrAcc,
                                 decode(prop.t_DebetCredit, PRT_DEBIT, rm.t_PayerINN, rm.t_ReceiverINN) as t_ClientINN,
                                 decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_FIID, pm.t_PayFIID) as t_FIID,
                                 decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_PayerAccount, pm.t_ReceiverAccount) as t_Account,
                                 decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_Payer, pm.t_Receiver) as t_ClientID,
                                 decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_PayerCodeKind, pm.t_ReceiverCodeKind) as t_ClientCodeKind,
                                 decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_PayerCode, pm.t_ReceiverCode) as t_ClientCode,
                                 pm.t_StartDepartment, pm.t_Department, pm.t_EndDepartment, pm.t_Chapter,
                                 decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_FuturePayerAccount, pm.t_FutureReceiverAccount) as t_FutureAccount,
                                 decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_FIID_FuturePayAcc, pm.t_FIID_FutureRecAcc) as t_FIID_FutureAcc,
                                 pm.t_DocKind, prop.t_PaymentID, prop.t_DebetCredit, prop.t_CodeKind, prop.t_BankCode, prop.t_Group,
                                 prop.t_Corschem, prop.t_PayFIID, prop.t_OurCorrAcc, prop.t_InOurBalance, pm.t_BaseFIID,
                                 pm.t_FIID as t_PayerFIID,
                                 pm.t_PayFIID as t_ReceiverFIID,
                                 decode(prop.t_DebetCredit, PRT_DEBIT, rm.t_PayerBankName, rm.t_ReceiverBankName ) as t_BankName,
                                 decode(prop.t_DebetCredit, PRT_DEBIT, SET_CHAR, UNSET_CHAR ) as t_IsPayer,
                                decode(prop.t_DebetCredit, PRT_DEBIT, rm.t_PayerName, rm.t_ReceiverName ) as t_ClientName,
                                pm.t_ValueDate as t_ValueDate,
                                po.t_DocKind as t_SubDocKind,
                                pm.t_Purpose as t_Purpose
                            from dpmproc_tmp   tmp,
                                 dpmprop_dbt   prop,
                                 dpmpaym_dbt   pm,
                                 dpmrmprop_dbt rm,
                                 dpspayord_dbt po
                           where tmp.t_PaymentID = prop.t_PaymentID
                             and tmp.t_State = 0
                             and pm.t_PaymentID = prop.t_PaymentID
                             and rm.t_PaymentID = pm.t_PaymentID 
                             and po.t_OrderID(+) = pm.t_PaymentID ) t
                  where t.t_Group = PAYMENTS_GROUP_UNDEF or
                        t.t_BankID in (0, -1) or
                        t.t_ClientID in (0, -1) or
                        t.t_BankName = RSB_EMPTY_STRING or
                        t.t_ClientName = RSB_EMPTY_STRING or
                        t.t_ClientINN = 'empty_INN' or
                        t.t_StartDepartment = 0 or
                        t.t_Department = 0 or
                        t.t_EndDepartment = 0
               )
    loop

      if rec.t_DocKind = DLDOC_BANKCLAIM then
        if rec.t_BaseFIID = PM_COMMON.NATCUR and rec.t_PayerFIID = PM_COMMON.NATCUR and rec.t_ReceiverFIID = PM_COMMON.NATCUR then
          v_IsCurrPaym := PM_COMMON.UNSET_CHAR;
        else
          v_IsCurrPaym := PM_COMMON.SET_CHAR;
        end if;
      end if;

      v_Context := PM_NAMES.GetContextForPrimaryDoc(rec.t_DocKind, rec.t_SubDocKind, rec.t_SubDocKind);

      v_stat := PM_SetPI( v_Context,
                          rec.t_Group,
                          rec.t_IsPayer,
                          rec.t_BankID,
                          rec.t_CodeKind,
                          rec.t_BankCode,
                          rec.t_BankName,
                          rec.t_CorrAcc,
                          rec.t_FIID,
                          rec.t_Chapter,
                          rec.t_Account,
                          rec.t_ClientID,
                          rec.t_ClientCodeKind,
                          rec.t_ClientCode,
                          rec.t_ClientINN,
                          rec.t_ClientName,
                          rec.t_ValueDate,
                          rec.t_OurCorrAcc,
                          rec.t_InOurBalance,
                          rec.t_DebetCredit,
                          rec.t_Purpose );

      if v_stat then
        v_stat := PM_SetDepartments( rec.t_DocKind,
                                     rec.t_BankID,
                                     rec.t_Account,
                                     rec.t_FIID,
                                     rec.t_Chapter,
                                     rec.t_DebetCredit,
                                     rec.t_Group,
                                     rec.t_Corschem,
                                     rec.t_PayFIID,
                                     rec.t_StartDepartment,
                                     rec.t_Department,
                                     rec.t_EndDepartment,
                                     rec.t_FutureAccount,
                                     rec.t_FIID_FutureAcc );
      end if;

      update dpmpaym_dbt
         set t_PayerBankID           = decode(rec.t_DebetCredit, PRT_DEBIT, rec.t_BankID, t_PayerBankID),
             t_Payer                 = decode(rec.t_DebetCredit, PRT_DEBIT, rec.t_ClientID, t_Payer),
             t_PayerCodeKind         = decode(rec.t_DebetCredit, PRT_DEBIT, rec.t_ClientCodeKind, t_PayerCodeKind),
             t_ReceiverBankID        = decode(rec.t_DebetCredit, PRT_CREDIT, rec.t_BankID, t_ReceiverBankID),
             t_Receiver              = decode(rec.t_DebetCredit, PRT_CREDIT, rec.t_ClientID, t_Receiver),
             t_ReceiverCodeKind      = decode(rec.t_DebetCredit, PRT_CREDIT, rec.t_ClientCodeKind, t_ReceiverCodeKind),
             t_StartDepartment       = decode(rec.t_StartDepartment, 0, t_StartDepartment, rec.t_StartDepartment),
             t_Department            = decode(rec.t_Department, 0, t_Department, rec.t_Department),
             t_EndDepartment         = decode(rec.t_EndDepartment, 0, t_EndDepartment, rec.t_EndDepartment),
             t_FuturePayerAccount    = decode(rec.t_DebetCredit, PRT_DEBIT, rec.t_FutureAccount, t_FuturePayerAccount),
             t_FIID_FuturePayAcc     = decode(rec.t_DebetCredit, PRT_DEBIT, rec.t_FIID_FutureAcc, t_FIID_FuturePayAcc),
             t_FutureReceiverAccount = decode(rec.t_DebetCredit, PRT_CREDIT, rec.t_FutureAccount, t_FutureReceiverAccount),
             t_FIID_FutureRecAcc     = decode(rec.t_DebetCredit, PRT_CREDIT, rec.t_FIID_FutureAcc, t_FIID_FutureRecAcc)
       where t_PaymentID = rec.t_PaymentID;

      update dpmprop_dbt
         set t_Group    = rec.t_Group,
             t_Corschem = rec.t_Corschem,
             t_PayFIID  = rec.t_PayFIID,
             t_OurCorrAcc = rec.t_OurCorrAcc,
             t_InOurBalance = rec.t_InOurBalance,
             t_CodeKind = rec.t_CodeKind
       where t_PaymentID = rec.t_PaymentID
         and t_DebetCredit = rec.t_DebetCredit;

      update dpmrmprop_dbt
         set t_PayerCorrAccNostro    = decode(rec.t_DebetCredit, PRT_DEBIT,  rec.t_CorrAcc, t_PayerCorrAccNostro),
             t_ReceiverCorrAccNostro = decode(rec.t_DebetCredit, PRT_CREDIT, rec.t_CorrAcc, t_ReceiverCorrAccNostro),
             t_PayerINN              = decode(rec.t_DebetCredit, PRT_DEBIT,  rec.t_ClientINN, t_PayerINN ),
             t_ReceiverINN           = decode(rec.t_DebetCredit, PRT_CREDIT, rec.t_ClientINN, t_ReceiverINN ),
             t_PayerBankName         = decode(rec.t_DebetCredit, PRT_DEBIT,  rec.t_BankName, t_PayerBankName ),
             t_ReceiverBankName      = decode(rec.t_DebetCredit, PRT_CREDIT, rec.t_BankName, t_ReceiverBankName ),
             t_PayerName             = decode(rec.t_DebetCredit, PRT_DEBIT,  rec.t_ClientName, t_PayerName ),
             t_ReceiverName          = decode(rec.t_DebetCredit, PRT_CREDIT, rec.t_ClientName, t_ReceiverName )
       where t_PaymentID = rec.t_PaymentID;

    end loop;
    
    update doprcurst_dbt
       set t_NumValue = PM_OPERATION.OPRSTATUS_DIRECT_TRANZIT
     where t_StatusKindID = PM_OPERATION.OPRSTATUS_DIRECT
       and t_ID_Operation in ( select opr.t_ID_Operation
                                 from doproper_dbt opr, dpmproc_tmp tmp, dpmprop_dbt db, dpmprop_dbt cr
                                where db.t_PaymentID = tmp.t_PaymentID
                                  and db.t_DebetCredit = PRT_DEBIT
                                  and db.t_Group = PAYMENTS_GROUP_EXTERNAL
                                  and cr.t_PaymentID = tmp.t_PaymentID
                                  and cr.t_DebetCredit = PRT_CREDIT
                                  and cr.t_Group = PAYMENTS_GROUP_EXTERNAL
                                  and opr.t_DocKind = tmp.t_DocKind
                                  and opr.t_DocumentID = lpad(tmp.t_PaymentID, 34, '0')
                                  and tmp.t_State = 0
                             );
     
  end SetPIForPmMass;

  -- массовое определение филиалов для платежей, указанных в dpmproc_dbt
  procedure SetDepartmentsForPmMass
  as
    v_stat   boolean;
  begin

    for rec in ( select decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_PayerBankID, pm.t_ReceiverBankID) as t_BankID,
                        decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_FIID, pm.t_PayFIID) as t_FIID,
                        decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_PayerAccount, pm.t_ReceiverAccount) as t_Account,
                        pm.t_StartDepartment, pm.t_Department, pm.t_EndDepartment,
                        decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_FuturePayerAccount, pm.t_FutureReceiverAccount) as t_FutureAccount,
                        decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_FIID_FuturePayAcc, pm.t_FIID_FutureRecAcc) as t_FIID_FutureAcc,
                        pm.t_DocKind, prop.t_PaymentID, prop.t_DebetCredit, prop.t_Corschem, prop.t_PayFIID, prop.t_Group, pm.t_Chapter
                   from dpmproc_tmp tmp, dpmprop_dbt prop, dpmpaym_dbt pm
                  where tmp.t_PaymentID = prop.t_PaymentID
                    and tmp.t_State = 0
                    and pm.t_PaymentID = prop.t_PaymentID
                    and (pm.t_StartDepartment = 0 or pm.t_Department = 0 or pm.t_EndDepartment = 0 or
                         decode(prop.t_DebetCredit, PRT_DEBIT, pm.t_FuturePayerAccount, pm.t_FutureReceiverAccount) = RSB_EMPTY_STRING
                        )

               )
    loop

      v_stat := PM_SetDepartments( rec.t_DocKind,
                                   rec.t_BankID,
                                   rec.t_Account,
                                   rec.t_FIID,
                                   rec.t_Chapter,
                                   rec.t_DebetCredit,
                                   rec.t_Group,
                                   rec.t_Corschem,
                                   rec.t_PayFIID,
                                   rec.t_StartDepartment,
                                   rec.t_Department,
                                   rec.t_EndDepartment,
                                   rec.t_FutureAccount,
                                   rec.t_FIID_FutureAcc
                                 );
      update dpmpaym_dbt
         set t_StartDepartment       = decode(rec.t_StartDepartment, 0, t_StartDepartment, rec.t_StartDepartment),
             t_Department            = decode(rec.t_Department, 0, t_Department, rec.t_Department),
             t_EndDepartment         = decode(rec.t_EndDepartment, 0, t_EndDepartment, rec.t_EndDepartment),
             t_FuturePayerAccount    = decode(rec.t_DebetCredit, PRT_DEBIT, rec.t_FutureAccount, t_FuturePayerAccount),
             t_FIID_FuturePayAcc     = decode(rec.t_DebetCredit, PRT_DEBIT, rec.t_FIID_FutureAcc, t_FIID_FuturePayAcc),
             t_FutureReceiverAccount = decode(rec.t_DebetCredit, PRT_CREDIT, rec.t_FutureAccount, t_FutureReceiverAccount),
             t_FIID_FutureRecAcc     = decode(rec.t_DebetCredit, PRT_CREDIT, rec.t_FIID_FutureAcc, t_FIID_FutureRecAcc)
       where t_PaymentID = rec.t_PaymentID;

    end loop;

  end SetDepartmentsForPmMass;

  --
  -- массовая регистрация в шлюзе документов, указанных в dpmproc_tmp
  --
  procedure RegistryAllObjectInGate
  as
    och_cur RSI_RSB_GATE.ObjectChange_cur;
  begin

    open och_cur for select RG_PMPAYM, lpad(tmp.t_PaymentID, 10, '0'), 'ПЛАТЕЖ ' || tmp.t_PaymentID, RG_CREATEOBJEC, opr.t_ID_Operation, null
                       from dpmproc_tmp tmp,
                            doproper_dbt opr
                      where opr.t_DocKind = tmp.t_DocKind
                        and opr.t_DocumentID = lpad(tmp.t_PaymentID, 34, '0');

    RSI_RSB_GATE.Al_RegistryObject( och_cur );

    close och_cur;

  exception
    when INVALID_CURSOR then
      null;
  end RegistryAllObjectInGate;

  procedure GetOperIDAndRStepIDByPaymentID(p_PaymentID IN NUMBER, p_OperID OUT NUMBER, p_StepID OUT NUMBER)
  AS
  BEGIN

    p_OperID:=0;
    p_StepID:=0;

    SELECT op.t_id_operation, st.t_id_step INTO p_OperID, p_StepID
    FROM doproper_dbt op,
         dpmpaym_dbt  pm,
         doprstep_dbt st
    WHERE pm.t_PaymentID = p_PaymentID
      AND op.t_dockind = decode( pm.t_PrimDocKind, 0, pm.t_DocKind, pm.t_PrimDocKind )
      AND op.t_documentid = lpad( p_PaymentID, 34, '0' )
      AND st.t_ID_Operation = op.t_ID_Operation
  --    AND st.t_IsExecute = 'R'
      AND st.t_ID_Step = ( select min( t_ID_Step )
                           from doprstep_dbt
                           where t_ID_Operation = op.t_ID_Operation
                             and t_IsExecute = 'R' );

    exception
    when NO_DATA_FOUND
    then
      NULL;

  END GetOperIDAndRStepIDByPaymentID;

  --
  -- Изменение полей текущих сумм, счетов и валют
  -- Меняем поля dpmpaym_dbt, если соответствующее поля dpmcarryacc_tmp не равны null
  --
  FUNCTION UpdateFutureFields RETURN NUMBER
  AS
    v_stat         NUMBER;
    cur_BkoutData  RSI_RsbOperation.BkoutData_cur;
  BEGIN

    open cur_BkoutData for select doc.t_ID_Operation, doc.t_ID_Step,
                                'UPDATE dpmpaym_dbt '||
                                ' SET '||
                                substr( decode( pmpaym.t_FuturePayerAccount,    null, '', ', t_FuturePayerAccount = ''' || pmpaym.t_FuturePayerAccount ||'''' ) ||
                                        decode( pmpaym.t_FuturePayerAmount,     null, '', ', t_FuturePayerAmount = '|| pmpaym.t_FuturePayerAmount ) ||
                                        decode( pmpaym.t_FIID_FuturePayAcc,     null, '', ', t_FIID_FuturePayAcc = '|| to_char(pmpaym.t_FIID_FuturePayAcc) ) ||
                                        decode( pmpaym.t_FutureReceiverAccount, null, '', ', t_FutureReceiverAccount = '''|| pmpaym.t_FutureReceiverAccount ||'''')||
                                        decode( pmpaym.t_FutureReceiverAmount,  null, '', ', t_FutureReceiverAmount = ' || pmpaym.t_FutureReceiverAmount ) ||
                                        decode( pmpaym.t_FIID_FutureRecAcc,     null, '', ', t_FIID_FutureRecAcc = ' || to_char(pmpaym.t_FIID_FutureRecAcc) ), 2 ) ||
                                        ' where T_PaymentID = ' || to_char(pca.t_PaymentID)
                             from dpmcarryacc_tmp pca,
                                  dpmpaym_dbt pmpaym,
                                  V_PMMASSOPFOREXE doc
                            where pmpaym.t_PaymentID = pca.t_PaymentID
                              and doc.t_PaymentID = pca.t_PaymentID;

    RSI_RsbOperation.SetBkoutDataForAll( cur_BkoutData );
    close cur_BkoutData;

    UPDATE dpmpaym_dbt pm  SET ( t_FuturePayerAccount,
                                 t_FuturePayerAmount,
                                 t_FIID_FuturePayAcc,

                                 t_FutureReceiverAccount,
                                 t_FutureReceiverAmount,
                                 t_FIID_FutureRecAcc ) =

                               ( select nvl( pca.t_PayerAccount, pm.t_FuturePayerAccount ),
                                        nvl( pca.t_PayerAmount, pm.t_FuturePayerAmount ),
                                        nvl( pca.t_PayerFIID,  pm.t_FIID_FuturePayAcc ),

                                        nvl( pca.t_ReceiverAccount, pm.t_FutureReceiverAccount ),
                                        nvl( pca.t_ReceiverAmount, pm.t_FutureReceiverAmount ),
                                        nvl( pca.t_ReceiverFIID,  pm.t_FIID_FutureRecAcc )
                                   from dpmcarryacc_tmp pca
                                  where pca.t_PaymentID = pm.t_PaymentID
                               )
    WHERE pm.t_PaymentID IN ( SELECT pca.t_PaymentID
                                    FROM dpmcarryacc_tmp pca );

    return 0;
  END UpdateFutureFields;

  --
  -- субъект - один из наших филиалов?
  --
  function IsOurFilial( p_PartyID in number ) return integer
  as
    v_HeadParty dparty_dbt.t_PartyID%type;
  begin

    -- если в ТС, то просто
    if IsBankInTS( p_PartyID ) = 1 then
      return 1;
    end if;

    -- иначе надо искать в субъектах нашу "голову" и "голову" испытуемого

    -- наша голова
    if m_OurHeadParty is null then
      begin
        select t_PartyID into m_OurHeadParty
        from ( select t_PartyID, t_Superior
               from dparty_dbt
               start with t_PartyID = RSBSESSIONDATA.OurBank()
               connect by t_PartyID = prior t_Superior )
        where nvl(t_Superior, 0) in ( 0, -1 );
      exception
        when NO_DATA_FOUND then
          return 0;
      end;
    end if;

    -- голова испытуемого
    begin
      select t_PartyID into v_HeadParty
      from ( select t_PartyID, t_Superior
             from dparty_dbt
             start with t_PartyID = p_PartyID
             connect by t_PartyID = prior t_Superior )
      where nvl(t_Superior, 0) in ( 0, -1 );
    exception
      when NO_DATA_FOUND then
        return 0;
    end;

    if v_HeadParty = m_OurHeadParty then
      return 1;
    else
      return 0;
    end if;

  end IsOurFilial;

  --
  -- субъект входит в территориальную структуру
  --
  function IsBankInTS( p_PartyID in number ) return integer deterministic
  as
  begin

    if p_PartyID = RSBSESSIONDATA.OurBank() then
      return 1;
    end if;

    if m_OurFilials is null then
      select t_PartyID bulk collect into m_OurFilials
      from ddp_dep_dbt
      where t_NodeType = 1;
    end if;

    for i in m_OurFilials.first .. m_OurFilials.last
    loop
      if m_OurFilials(i) = p_PartyID then
        return 1;
      end if;
    end loop;

    return 0;

  end IsBankInTS;

  --
  -- счёт, указанный в платеже, клиентский?
  --
  function IsClientAccount( p_Group    in number,
                            p_ClientID in number,
                            p_Account  in varchar2 ) return integer deterministic
  as
  begin

    if p_Group <> PAYMENTS_GROUP_EXTERNAL and ( IsBankInTS( p_ClientID ) = 1 ) then
      return 0;
    else
      if m_ClientAccountMask is null then
        --m_ClientAccountMask := replace( RSB_COMMON.GetRegStrValue( 'PS/REQOPENACC/Счета клиентов' ), ';', '|' );
        m_ClientAccountMask := '^(' || replace(
                                       replace(
                                       replace(
                                       RSB_COMMON.GetRegStrValue( 'PS/REQOPENACC/Счета клиентов' ),
                                       ',', '|' ),
                                       ';', '|' ),
                                       '*', '.*' ) || ')';
      end if;
      --if RSI_RSB_MASK.CompareStringWithMask( m_ClientAccountMask, p_Account ) = 1 then
      if regexp_like( p_Account, m_ClientAccountMask ) then
        return 1;
      else
        return 0;
      end if;
    end if;

  end IsClientAccount;

  --
  -- счёт, указанный в платеже, банковский?
  --
  function IsBankAccount( p_Group    in number,
                            p_ClientID in number,
                            p_Account  in varchar2 ) return integer deterministic
  as
  begin

      if m_BankAccountMask is null then
        m_BankAccountMask := '^(' || replace(
                                       replace(
                                       replace(
                                       RSB_COMMON.GetRegStrValue( 'PS/REQOPENACC/Счета банка' ),
                                       ',', '|' ),
                                       ';', '|' ),
                                       '*', '.*' ) || ')';
      end if;

      if regexp_like( p_Account, m_BankAccountMask ) then
        return 1; --Банковский
      end if;

      if m_ClientAccountMask is null then
        m_ClientAccountMask := '^(' || replace(
                                       replace(
                                       replace(
                                       RSB_COMMON.GetRegStrValue( 'PS/REQOPENACC/Счета клиентов' ),
                                       ',', '|' ),
                                       ';', '|' ),
                                       '*', '.*' ) || ')';
      end if;

      if regexp_like( p_Account, m_ClientAccountMask ) then
        return 0; --Клиентский
      end if;

      --В настройках нет, смотрим клиента...
      if(p_ClientID > 0) then --Если задан клиент, то счёт есть в нашей базе
        begin
          if p_Group <> PAYMENTS_GROUP_EXTERNAL and ( IsBankInTS( p_ClientID ) = 1 ) then
            return 1;
          else
            return 0;
          end if;
        end;
      end if;

      return 1; -- Считаем по умолчанию считаем счёт банковским

  end IsBankAccount;


  procedure ParseMasInsPmLog(RecID    IN  dxml2_dfisclog_dbt.t_RecID%type,
                             NumDprt  IN  dxml2_dfisclog_dbt.t_NumDprt%type,
                             DocCount OUT NUMBER,
                             Stat     OUT NUMBER
                            )
  as
    v_BlobData BLOB;
    v_ClobData CLOB;
    v_ind NUMBER;
    v_offset_head  NUMBER;
    v_offset       NUMBER;
    v_recsize  NUMBER;
    v_mydataoffset NUMBER;
    v_source_offset INTEGER;
  begin

    SELECT  fisclog.T_FMTBLOBDATA_XXXX.GetClobVal(), fisclog.T_VARLENXML INTO v_ClobData, v_mydataoffset
    FROM dxml2_dfisclog_dbt fisclog
    WHERE fisclog.T_RECID = RecID AND fisclog.T_NUMDPRT = NumDprt;

    v_source_offset:= instr(DBMS_LOB.SUBSTR(v_ClobData, 100), '<value>') + 7;
    IF v_source_offset = 7 THEN
        Stat:= 0;
        RETURN;
    END IF;
    DBMS_LOB.CREATETEMPORARY(v_BlobData, true);
    DBMS_LOB.WRITEAPPEND(
            v_BlobData,
            (v_mydataoffset - 13 - v_source_offset) / 2,
            HexToRaw(DBMS_LOB.SUBSTR(v_ClobData, v_mydataoffset - 13 - v_source_offset, v_source_offset)));

    --Если блоб большой ( >65535 байт), то размер наших данных хранится в начале в виде int32,
    --если маленький, то в виде int16
    if dbms_lob.GetLength(v_BlobData) > 65535
    then
      v_mydataoffset := 5;
    else
      v_mydataoffset := 3;
    end if;

    DocCount := rsb_struct.getInt(dbms_lob.substr(v_BlobData, 4, v_mydataoffset));

    delete from dmasinspm_tmp;
    v_ind := 0;
    rsb_struct.readstruct('dmasinspm_tmp');
    v_offset_head := v_mydataoffset + 3; --Смещение, с которого начинаем считывание записей функциями rsb_struct.getXXXX. v_mydataoffset + 4-1: 4 - размер заголовка (int32), -1 - функции rsb_struct.getXXXX добавляют 1 к смещению
    v_recsize := rsb_struct.getRecordSize('dmasinspm_tmp');

    WHILE( v_ind < DocCount ) LOOP
       v_offset := v_offset_head + v_recsize*v_ind;
       INSERT INTO dmasinspm_tmp
       ( t_PaymentID,
         t_DocKind,
         t_Number,
         t_ValueDate,
         t_BaseFIID,
         t_BaseAmount,
         t_Chapter,
         t_PayerAccount,
         t_PayerBankCodeKind,
         t_PayerBankCode,
         t_ReceiverAccount,
         t_ReceiverBankCodeKind,
         t_ReceiverBankCode
       )
       VALUES
       ( rsb_struct.getLong  ( 't_PaymentID',            v_BlobData, v_offset),
         rsb_struct.getInt   ( 't_DocKind',              v_BlobData, v_offset),
         rsb_struct.getString( 't_Number',               v_BlobData, v_offset),
         rsb_struct.getDate  ( 't_ValueDate',            v_BlobData, v_offset),
         rsb_struct.getLong  ( 't_BaseFIID',             v_BlobData, v_offset),
         rsb_struct.getMoney ( 't_BaseAmount',           v_BlobData, v_offset),
         rsb_struct.getInt   ( 't_Chapter',              v_BlobData, v_offset),
         rsb_struct.getString( 't_PayerAccount',         v_BlobData, v_offset),
         rsb_struct.getInt   ( 't_PayerBankCodeKind',    v_BlobData, v_offset),
         rsb_struct.getString( 't_PayerBankCode',        v_BlobData, v_offset),
         rsb_struct.getString( 't_ReceiverAccount',      v_BlobData, v_offset),
         rsb_struct.getInt   ( 't_ReceiverBankCodeKind', v_BlobData, v_offset),
         rsb_struct.getString( 't_ReceiverBankCode',     v_BlobData, v_offset)
        );

        v_ind := v_ind + 1;
    END LOOP;
    commit;

    Stat := 0;

    DBMS_LOB.FREETEMPORARY(v_BlobData);

  end ParseMasInsPmLog;

  --
  -- Массово закрыть свойство картотеки невыясненных поступлений
  -- Обрабатывается множество платежей из V_PMMASSOP
  --
  PROCEDURE MassCloseUnknownProp
  AS
    RollbackData_c RSI_RsbOperation.BkoutData_cur;
  BEGIN

    -- Сохранение данных отката
    open RollbackData_c for select pm.t_ID_Operation, pm.t_ID_Step,
                                   'update drminprop_dbt ' ||
                                      'set t_Closed=CHR(0) ' ||
                                         ',t_OutDate=to_date(''01010001'',''ddmmyyyy'') ' ||
                                    'where t_PaymentID=' || to_char(pm.t_PaymentID)
                              from V_PMMASSOP    pm,
                                   drminprop_dbt prop
                             where pm.t_SkipDocument = 0
                               and pm.t_ErrorStatus  = 0
                               and prop.t_PaymentID  = pm.t_PaymentID
                               and prop.t_Closed     = UNSET_CHAR;


    RSI_RsbOperation.SetBkoutDataForAll( RollbackData_c );

    close RollbackData_c;

    update drminprop_dbt t
       set t_Closed = SET_CHAR,
           t_OutDate = RsbSessionData.curdate
     where t.t_PaymentID in ( select tmp.t_PaymentID
                                from V_PMMASSOP tmp
                               where tmp.t_SkipDocument = 0
                                 and tmp.t_ErrorStatus  = 0 )
       and t.t_Closed = UNSET_CHAR;

  END MassCloseUnknownProp;

  --
  -- Счет является корсчетом из корсхемы ЛОРО ?
  --
  function IsLoroAccount( p_Account in varchar2, p_FIID in number ) return integer
  as
    v_IsLoro integer default 0;
  begin

    select 1 into v_IsLoro
    from dual
    where exists ( select cors.t_Number
                   from dcorschem_dbt cors
                   where cors.t_Account = p_Account
                     and cors.t_FIID = p_FIID
                     and cors.t_IsNostro = chr(0) );
    return v_IsLoro;
  exception
    when NO_DATA_FOUND then
      return 0;
  end IsLoroAccount;

  --
  -- Является ли счет счетом МФР? А заодно и филиал-корреспондент определим...
  --
  function PM_AccountIsMFR( p_Account  in  varchar2,
                            p_Currency in  number,
                            p_Chapter  in  number,
                            v_Dep      out ddp_dep_dbt%rowtype
                          ) return char
  as
    v_IsMFR char default SET_CHAR;
  begin

    -- dmcaccdoc_dbt - очень большая таблица, записей по одному счёту в ней может быть очень много,
    -- поэтому запрос надо строить максимально эффективно
    begin
      select SET_CHAR into v_IsMFR
        from dual
       where exists ( select /*+ ordered use_nl(doc)*/ 1
                        from dmccateg_dbt cat, dmcaccdoc_dbt doc
                       where cat.t_number in (1010, 1011)
                         and doc.t_catid = cat.t_id
                         and doc.t_account = p_Account
                         and doc.t_currency = p_Currency
                         and doc.t_chapter = p_Chapter
                         and rownum <= 1 );
     exception
       when NO_DATA_FOUND then v_IsMFR := UNSET_CHAR;
     end;

     -- а вот теперь, если счёт - МФР (что довольно редкий случай), поищем филиал-корреспондент
     if v_IsMFR = SET_CHAR then
     begin
       select /*+FIRST_ROWS(1)*/
              dep.* into v_Dep
         from dmccateg_dbt cat,
              dmcaccdoc_dbt doc,
              ddp_dep_dbt dep
        where cat.t_number in (1010, 1011)
          and doc.t_catid = cat.t_id
          and doc.t_account = p_Account
          and doc.t_currency = p_Currency
          and doc.t_chapter = p_Chapter
          and dep.t_code = doc.t_corrdepartmentid
          and dep.t_status <> 3
          and rownum <= 1;
     exception
       when NO_DATA_FOUND then null;
     end;
     end if;

     return v_IsMFR;

  end PM_AccountIsMFR;

  /**
   * Найти ставку НДС для экземпляра комиссии, указанного в платеже
   * @param p_FeeType      Вид комиссии
   * @param p_FeeID        ID комиссии
   * @param p_MultiNDSRate Возвращаемый параметр - признак наличия разных ставок НДС в ТО
   * @return               Возвращает значение ставки НДС
   */
  FUNCTION GetNDSRateForComiss( p_FeeType in integer,
                                p_FeeID   in integer,
                                p_MultiNDSRate out integer ) RETURN NUMBER
  AS
    v_NDS number(32,12) default 0;
  BEGIN

    p_MultiNDSRate := 0;

    if    p_FeeType = SF_FEE_TYPE_PERIOD
       or p_FeeType = SF_FEE_TYPE_SINGLE
       or p_FeeType = SF_FEE_TYPE_ONCE then

      select t.t_NDSRateValue
        into v_NDS
        from dsfdef_dbt t
       where t.t_FeeType = p_FeeType
         and t.t_ID = p_FeeID;

    elsif p_FeeType = SF_FEE_TYPE_INVOICE then

      declare
        x_multiple_rows exception;
        pragma exception_init( x_multiple_rows, -1422 );
      begin
        select distinct t.t_NDSRateValue
          into v_NDS
          from dsfdef_dbt t
         where t.t_InvoiceID = p_FeeID;
      exception
        when x_multiple_rows then
          p_MultiNDSRate := 1;
          v_NDS := 0;
      end;

    end if;

    return v_NDS;

  EXCEPTION

    when NO_DATA_FOUND then
      v_NDS := 0;
      return v_NDS;

  END GetNDSRateForComiss;

 --Проверка существования этого признака для категории
  FUNCTION CheckObjAttrPresence( p_ObjType IN NUMBER,
                                 p_ObjID IN VARCHAR2,
                                 p_GroupID IN NUMBER,
                                 p_AttrID  IN NUMBER ) RETURN NUMBER
  AS
    v_ExistsValue NUMBER := 0;
  BEGIN
    SELECT COUNT(1) INTO v_ExistsValue
      FROM dobjatcor_dbt
     WHERE t_objecttype = p_ObjType
       AND t_object = p_ObjID
       AND t_groupid = p_GroupID
       AND t_attrid = p_AttrID;
    IF v_ExistsValue > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END CheckObjAttrPresence;


  -- Получение значения категории
  FUNCTION GetObjAttrValue( p_ObjType IN NUMBER,
                            p_ObjID IN VARCHAR2,
                            p_GroupID IN NUMBER,
                            p_Date  IN date ) RETURN NUMBER
  AS
    v_AttrID NUMBER := 0;
  BEGIN

    SELECT t_AttrID INTO v_AttrID
      FROM dobjatcor_dbt
     WHERE t_ObjectType = p_ObjType
       AND t_Object = p_ObjID
       AND t_Groupid = p_GroupID
       AND t_ValidFromDate <= p_Date
       AND t_ValidToDate > p_Date;

    return v_AttrID;

  EXCEPTION

    when NO_DATA_FOUND then
      v_AttrID := 0;
      return v_AttrID;

  END GetObjAttrValue;

  -- ОД "Баланс" - операционный день с признаком "баланс" в календаре филиала
  FUNCTION PM_GetOperDay_Balance( p_Department in number,
                                  p_FromDate in date default RsbSessionData.curdate
                                ) RETURN DATE
  AS
    v_Date date;
  BEGIN

    select nvl( min( d.t_CurDate ), RSB_EMPTY_DATE )
      into v_Date
      from dcurdate_dbt d
     where d.t_CurDate >= p_FromDate
       and d.t_Branch = p_Department
       and d.t_IsBalance = SET_CHAR;

    return v_Date;
  END PM_GetOperDay_Balance;

  -- ОД "Банковское обслуж." + "Баланс" - операционный день с признаком "баланс" и видом обслуживания "банковское"
  FUNCTION PM_GetOperDay_BankServBalance( p_Department in number ) RETURN DATE
  AS
    v_Date date;
    v_CalendarID integer;
  BEGIN
    v_CalendarID := RSI_RsbCalendar.GetCalendar(p_Department);

    select nvl( min( d.t_CurDate ), RSB_EMPTY_DATE )
      into v_Date
      from dcurdate_dbt d
     where d.t_CurDate >= RsbSessionData.curdate
       and d.t_Branch = p_Department
       and d.t_IsBalance = SET_CHAR
       and RSI_RsbCalendar.IsWorkDay(d.t_CurDate, v_CalendarID) = 1;

    return v_Date;
  END PM_GetOperDay_BankServBalance;

   --Получить множество проверяемых счетов
  FUNCTION GetAccounts (p_PaymentID IN INTEGER)
     RETURN boolean
  AS
  BEGIN
    SELECT t_Account 
      BULK COLLECT INTO m_Accounts
      FROM
       (
        SELECT DECODE(prop.t_DebetCredit, PRT_DEBIT, paym.t_PayerAccount, paym.t_ReceiverAccount) t_Account
          FROM dpmpaym_dbt paym, dpmprop_dbt prop
         WHERE     prop.t_PaymentID = paym.t_PaymentID
               AND paym.t_PaymentID = p_PaymentID
               AND prop.t_Group = PAYMENTS_GROUP_INTERNAL
        UNION ALL
        SELECT t_Account
          FROM dpmaddpi_dbt
         WHERE t_PaymentID = p_PaymentID
       );
    RETURN m_Accounts.COUNT > 0;
  END GetAccounts;

  --Определение категории платежа "Инициатор операции"
  FUNCTION GetOperInitiatorCtg (p_PaymentID IN INTEGER)
     RETURN INTEGER
  AS
     v_ClientPayment   NUMBER := 0;
     v_ExternPayment   NUMBER := 0;
     v_stat            NUMBER := 0;
  BEGIN
     SELECT COUNT (1)
       INTO v_ClientPayment
       FROM dpmpaym_dbt pm
      WHERE pm.t_PaymentID = p_PaymentID
            AND (pm.t_DocKind = PMDOC_CLIENTPAYMENT
                 OR pm.t_DocKind IN
                       (SELECT odoc.t_DocKind
                          FROM doprkdoc_dbt odoc
                         WHERE odoc.t_ParentDocKind = PMDOC_CLIENTPAYMENT));

     IF v_ClientPayment > 0
     THEN
        RETURN CTG_OPERINITIATOR_KLIENT;
     END IF;

     SELECT COUNT (1)
       INTO v_ExternPayment
       FROM dpmprop_dbt prop
      WHERE prop.t_PaymentID = p_PaymentID
            AND (prop.t_Group = PAYMENTS_GROUP_EXTERNAL
                 OR prop.t_PaymentID IN
                 (SELECT  memord.t_OrderID
                    FROM dmemorder_dbt memord, dbbcpord_dbt bbcpord
                   WHERE prop.t_PaymentID = memord.t_OrderID
                         AND memord.t_Origin = MEMORDER_FDOC_TRANZIT
                         OR prop.t_PaymentID = bbcpord.t_OrderID
                         AND bbcpord.t_Origin = CP_OR_TRANZIT));

     IF v_ExternPayment >= 2
     THEN
        RETURN RSB_COMMON.GetRegIntValue (InitOperTransitRegPath);
     END IF;

     IF GetAccounts (p_PaymentID) THEN
        FOR i IN m_Accounts.FIRST .. m_Accounts.LAST
        LOOP
           IF RSI_RSB_MASK.
               CompareStringWithMask (
                 RSB_Common.GetRegStrValue (ClientAccountRegPath),
                 m_Accounts (i)) = 1
           THEN
              RETURN CTG_OPERINITIATOR_KLIENT;
           END IF;
        END LOOP;

        FOR i IN m_Accounts.FIRST .. m_Accounts.LAST
        LOOP
           IF RSI_RSB_MASK.
               CompareStringWithMask (
                 RSB_Common.GetRegStrValue (BankAccountRegPath),
                 m_Accounts (i)) = 1
           THEN
              RETURN CTG_OPERINITIATOR_BANK;
           END IF;
        END LOOP;
     END IF;

     v_stat := RSB_COMMON.GetRegIntValue (InitOperTransitRegUnknown);
     RETURN v_stat;
  END GetOperInitiatorCtg;

  /**
   * Получить название узла ТС по ID
   * @param p_Department Номер узла ТС
   * @return VARCHAR2 Идентификатор узла ТС (ddp_dep_dbt.t_Name)
   */
  function GetNodeName( p_Department in number ) return varchar2 deterministic
  as
    v_Name ddp_dep_dbt.t_Name%type;
  begin
    select dp.t_Name
      into v_Name
      from ddp_dep_dbt dp
     where dp.t_Code = p_Department;

    return v_Name;

  exception
    when NO_DATA_FOUND then
      return '';
  end GetNodeName;

  /**
   * Массовая установка признака "Целевое финансирование" с привязкой к шагу
   * для документов из V_PMMASSOPFOREXE
   * @since 6.20.031
   */
  PROCEDURE MassSetIsPurpose
  as
    type SetPurpRec_t is record( t_PaymentID        number(10),
                                 t_ID_Operation     number(10),
                                 t_ID_Step          number( 5) );

    type SetPurpTbl_t is table of SetPurpRec_t;

    v_PurpPayms SetPurpTbl_t;

    v_RollbackData RSI_RsbOperation.BkoutData_tbl;
  begin
    -- отбор платежей, которым нужно проставить признак
      select pm.t_PaymentID, t.t_ID_Operation, t.t_ID_Step
      bulk collect into v_PurpPayms
        from V_PMMASSOPFOREXE t,
             dpmpaym_dbt      pm,
             daccount_dbt     ac,
             dpmprop_dbt      cr,
             dpmrmprop_dbt    rm
       where pm.t_PaymentID = t.t_OrderID
         and pm.t_IsPurpose <> PM_COMMON.SET_CHAR
         and pm.t_FIID = PM_COMMON.NATCUR
         and pm.t_PayFIID = PM_COMMON.NATCUR
         and ac.t_Account = pm.t_ReceiverAccount
         and ac.t_Code_Currency = pm.t_PayFIID
         and ac.t_Chapter = pm.t_Chapter
         and ac.t_Kind_Account = 'П'
         and cr.t_PaymentID = t.t_OrderID
         and cr.t_DebetCredit = PM_COMMON.PRT_CREDIT
         and cr.t_Group <> PM_COMMON.PAYMENTS_GROUP_EXTERNAL
         and regexp_like(pm.t_PayerAccount, '^(401)|(402)|(403)|(404)|(405)')
         and rm.t_PaymentID = t.t_OrderID
         and regexp_like(lower(rm.t_Ground), 'целев|финансировани|пособи|чернобыл');

    if v_PurpPayms.count > 0 then
      -- сохранение данных для отката шага
      v_RollbackData.extend( v_PurpPayms.count );

      for i in v_PurpPayms.first .. v_PurpPayms.last
      loop
        v_RollbackData( i ).t_ID_Operation := v_PurpPayms(i).t_ID_Operation;
        v_RollbackData( i ).t_ID_Step      := v_PurpPayms(i).t_ID_Step;
        v_RollbackData( i ).t_SQLQuery     :=
                                 'update dpmpaym_dbt ' ||
                                   ' set t_IsPurpose = chr(0) ' ||
                                 ' where t_PaymentID = ' || v_PurpPayms(i).t_PaymentID;

      end loop;

      RSI_RsbOperation.SetBkoutDataForAll( v_RollbackData );

      -- собственно простановка признака
      forall i in v_PurpPayms.first .. v_PurpPayms.last
        update dpmpaym_dbt
           set t_IsPurpose = PM_COMMON.SET_CHAR
         where t_PaymentID = v_PurpPayms(i).t_PaymentID;
    end if;

  end MassSetIsPurpose;

  /**
   * Максимальная допустимая очередность платежей
   * @return Возвращает максимальную очередность
   */
  FUNCTION PM_DefaultMaxPriority RETURN NUMBER
  AS
  BEGIN
    if m_DefaultMaxPriority is null then
      m_DefaultMaxPriority := RSB_COMMON.GetRegIntValue('CB\PAYMENTS\MAXPRIORITY');

      if m_DefaultMaxPriority is null then
        m_DefaultMaxPriority := 5;
      end if;
    end if;

    return m_DefaultMaxPriority;
  END;

/**
   * Получить вид операции по PaymentID и DocKind
   * @return Возвращает вид операции
   */
  function GetKindOperation( p_PaymentID in number, p_DocKind in number  ) return number
  as

    v_KindOperation   number := -1;

  begin

        if p_DocKind = PM_COMMON.CB_MULTYDOC then
          select t_Kind_Operation into v_KindOperation
            from dmultydoc_dbt
           where T_AUTOKEY = p_PaymentID;

        elsif p_DocKind in (PM_COMMON.DLDOC_BANKPAYMENT, PM_COMMON.DLDOC_BANKCLAIM)  then
          select t_Kind_Operation into v_KindOperation
            from dmemorder_dbt
           where T_ORDERID = p_PaymentID;

        elsif p_DocKind = PM_COMMON.BBANK_CPORDER then
          select t_Kind_Operation into v_KindOperation
            from dbbcpord_dbt
           where T_ORDERID = p_PaymentID;

        elsif p_DocKind = PM_COMMON.PS_CPORDER then
          select t_Kind_Operation into v_KindOperation
            from dpscpord_dbt
           where T_ORDERID = p_PaymentID;

        elsif p_DocKind = PM_COMMON.DLDOC_MEMORIALORDER then
          select t_Kind_Operation into v_KindOperation
            from dcb_doc_dbt
           where T_DOCUMENTID = p_PaymentID;

        elsif p_DocKind = PM_COMMON.PS_PAYORDER then
          select t_Kind_Operation into v_KindOperation
            from dpspayord_dbt
           where T_ORDERID = p_PaymentID;

        elsif p_DocKind = PM_COMMON.PS_INRQ then
          select t_KindOperation into v_KindOperation
            from dpsinrq_dbt
           where T_PAYMENTID = p_PaymentID;

        elsif p_DocKind in(PM_COMMON.CASH_BOF_ADDORDER, PM_COMMON.CASH_PS_INCORDER, PM_COMMON.CASH_PS_OUTORDER,
                             PM_COMMON.CASH_BOF_INCORDER, PM_COMMON.CASH_BOF_OUTORDER)  then

          select t_Kind_Operation into v_KindOperation
            from dpscshdoc_dbt
           where T_AUTOKEY = p_PaymentID;

        end if;

    return v_KindOperation;

  end GetKindOperation;

  function PaymStatusToXml( p_PaymentID in number, p_PaymStatus in number default 0, p_NewStatus in varchar default CHR(1) ) return CLOB
  is
    v_TmpBuff VARCHAR(4096);
    v_Acc VARCHAR(100);
    v_NewStatus VARCHAR(100);
    v_OldStatus NUMBER;
    v_PartyID NUMBER;
    v_Need NUMBER;
    v_RetVal CLOB := EMPTY_CLOB;
  begin
    dbms_lob.createtemporary(v_RetVal, TRUE);
    dbms_lob.open(v_RetVal, dbms_lob.lob_readwrite);
    v_TmpBuff:= '<DocStatus>';
    dbms_lob.writeappend(v_RetVal, LENGTH(v_TmpBuff), v_TmpBuff);

    select '<PaymentID>' || TO_CHAR(t_PaymentID) || '</PaymentID><DocKind>' || t_ShifrOper || '</DocKind><RSBankDocKind>' || t_DocKind || '</RSBankDocKind><Origin>' || GetPrimDocOrigin(t_PaymentID, t_DocKind) || '</Origin><Timesheet>' || TO_CHAR(SYSDATE, 'Dd.mm.yyyy hh:mi:ss')|| '</Timesheet>'
      into v_TmpBuff
      from dpmrmprop_dbt join dpmpaym_dbt using (t_PaymentID)
     where t_PaymentID = p_PaymentID;
    dbms_lob.writeappend(v_RetVal, LENGTH(v_TmpBuff), v_TmpBuff);

    v_NewStatus:= p_NewStatus;
    if v_NewStatus = CHR(1) or v_NewStatus is NULL or v_NewStatus = '' then
      select t_StatusIDFrom into v_OldStatus from dpmhist_dbt where t_AutoKey = (select MAX(t_AutoKey) from dpmhist_dbt where t_PaymentID = p_PaymentID);
      --сначала проверим старый статус, а то PM_READIED его затирать может
      v_NewStatus:= case v_OldStatus
                      when PM_I2PLACED then 'Списан с картотеки 2'
                      when PM_IWPPLACED then 'Списан с картотеки ОР'
                      else CHR(1) end;
      if v_NewStatus = CHR(1) then
        v_NewStatus:= case p_PaymStatus
                        when PM_READIED then 'Обрабатывается'
                        when PM_I2PLACED then 'На картотеке 2'
                        when PM_IWPPLACED then 'На картотеке ОР'
                        when PM_REJECTED then 'Отвергнут'
                        else '' end;
      end if;
    end if;
    v_TmpBuff:= '<StatusNew>' || CONVERT(v_NewStatus, 'CL8MSWIN1251') || '</StatusNew>';
    dbms_lob.writeappend(v_RetVal, LENGTH(v_TmpBuff), v_TmpBuff);

    select DECODE(prp.t_Group, PAYMENTS_GROUP_INTERNAL, pm.t_Payer, pm.t_Receiver),
           DECODE(prp.t_Group, PAYMENTS_GROUP_INTERNAL, pm.t_PayerAccount, pm.t_ReceiverAccount),
           DECODE(prp.t_Group, PAYMENTS_GROUP_INTERNAL, 1, DECODE(prpC.t_Group, PAYMENTS_GROUP_INTERNAL, 1, 0))
      into v_PartyID, v_Acc, v_Need
      from dpmpaym_dbt pm, dpmprop_dbt prp, dpmprop_dbt prpC
     where pm.t_PaymentID = p_PaymentID and pm.t_PaymentID = prp.t_PaymentID and prp.t_DebetCredit = PRT_DEBIT
                                        and pm.t_PaymentID = prpC.t_PaymentID and prpC.t_DebetCredit = PRT_CREDIT;
    if v_Need = 1 then
      v_TmpBuff:= '<To><PartyID>' || TO_CHAR(v_PartyID) || '</PartyID><AccountNumber>' || CONVERT(v_Acc, 'CL8MSWIN1251') || '</AccountNumber></To>';
    else
      v_TmpBuff:= '<To />';
    end if;
    dbms_lob.writeappend(v_RetVal, LENGTH(v_TmpBuff), v_TmpBuff);

    v_TmpBuff:= '</DocStatus>';
    dbms_lob.writeappend(v_RetVal, LENGTH(v_TmpBuff), v_TmpBuff);
    dbms_lob.close(v_RetVal);

    return v_RetVal;

  end PaymStatusToXml;


  function PaymStatusToXmlAll( p_Mode in number, p_PaymStatus in number default 0, p_PaymentID in number default 0, p_NewStatus in varchar default CHR(1) ) return number
  is
    v_TmpBuff VARCHAR(4096);
    v_Dep NUMBER;
    v_Old NUMBER;
    v_NeedInsert NUMBER := 0;
    v_DocInfo CLOB := EMPTY_CLOB;
    v_TmpCLOB CLOB;
    v_RegValue VARCHAR(2000);
    v_Value VARCHAR(100);
    v_Pos PLS_INTEGER;
    v_Parse PmSelectDocumentsTable_t:= PmSelectDocumentsTable_t();
  begin
    v_RegValue:= RSB_COMMON.GetRegStrValue('CB\ASYNC_CALL_SERVICE\EVENT\4\SELECTDOCUMENTS');
    if v_RegValue is NULL then
      v_RegValue:= '201:2, 202:2';
    end if;
    if v_RegValue = chr(1) then
      return 0;
    end if;

    while LENGTH(v_RegValue) > 0
    loop
      v_Pos:= INSTR(v_RegValue, ',');
      v_Parse.extend;
      if v_Pos > 0 then
        v_TmpBuff:= TRIM(SUBSTR(v_RegValue, 1, v_Pos - 1));
        v_RegValue:= TRIM(SUBSTR(v_RegValue, v_Pos + 1));
      else
        v_TmpBuff:= v_RegValue;
        v_RegValue:= '';
      end if;
      v_Pos:= INSTR(v_TmpBuff, ':');
      if v_Pos = 0 then
        return 1;
      end if;
      v_Parse(v_Parse.LAST):= PmSelectDocuments_t( TO_NUMBER(TRIM(SUBSTR(v_TmpBuff, 1, v_Pos - 1))), TO_NUMBER(TRIM(SUBSTR(v_TmpBuff, v_Pos + 1))));
    end loop;

    --сразу же проверим что бы был хотя бы один платеж который подходит по настройке
    begin
      if p_PaymentID <> 0 then
        select pm.t_Department into v_Dep
          from dpmpaym_dbt pm, TABLE(v_Parse) pp
         where pm.t_PaymentID = p_PaymentID
           and pp.t_DocKind = pm.t_DocKind
           and pp.t_Origin = GetPrimDocOrigin(pm.t_PaymentID, pm.t_DocKind);
      else
          if p_Mode = 0 then
            select pm.t_Department into v_Dep
              from dpmpaym_dbt pm, dpmproc_tmp tmp, TABLE(v_Parse) pp
             where pm.t_PaymentID = tmp.t_PaymentID
               and pp.t_DocKind = pm.t_DocKind
               and pp.t_Origin = GetPrimDocOrigin(pm.t_PaymentID, pm.t_DocKind)
               and rownum = 1;
          else
            select t_Department into v_Dep
              from dpmpaym_dbt pm, doprtemp_tmp tmp, TABLE(v_Parse) pp
             where pm.t_PaymentID = tmp.t_OrderID
               and pp.t_DocKind = pm.t_DocKind
               and pp.t_Origin = GetPrimDocOrigin(pm.t_PaymentID, pm.t_DocKind)
               and tmp.t_ErrorStatus = 0
               and tmp.t_SkipDocument = 0
               and rownum = 1;
          end if;
      end if;
    exception
      when NO_DATA_FOUND then begin
        -- нет данных для вставки
        return 0;
      end;
    end;

    dbms_lob.createtemporary(v_DocInfo, TRUE);
    dbms_lob.open(v_DocInfo, dbms_lob.lob_readwrite);
    v_TmpBuff:= '<?xml version="1.0" encoding="windows-1251"?><DocStatusInfo><From>';
    dbms_lob.writeappend(v_DocInfo, LENGTH(v_TmpBuff), v_TmpBuff);
    v_TmpCLOB:= RsbEventLog.GetEventSourceInfo(v_Dep, 0);
    dbms_lob.writeappend(v_DocInfo, dbms_lob.getLength(v_TmpCLOB), v_TmpCLOB);

    v_TmpBuff:= '</From><DocStatusList>';
    dbms_lob.writeappend(v_DocInfo, LENGTH(v_TmpBuff), v_TmpBuff);

    if p_PaymentID = 0 then
      if p_Mode = 0 then
        for rec in (select pm.t_PaymentID from dpmpaym_dbt pm, dpmproc_tmp tmp, TABLE(v_Parse) pp
                     where tmp.t_State = 0
                       and pm.t_PaymentID = tmp.t_PaymentID
                       and pp.t_DocKind = pm.t_DocKind
                       and pp.t_Origin = GetPrimDocOrigin(pm.t_PaymentID, pm.t_DocKind))
        loop
          v_TmpCLOB:= PaymStatusToXml(rec.t_PaymentID, 0, 'Создан');
          dbms_lob.writeappend(v_DocInfo, dbms_lob.getLength(v_TmpCLOB), v_TmpCLOB);
        end loop;
      else
        for rec in (select tmp.t_OrderID from dpmpaym_dbt pm, doprtemp_tmp tmp, TABLE(v_Parse) pp
                     where pm.t_PaymentID = tmp.t_OrderID
                       and pp.t_DocKind = pm.t_DocKind
                       and pp.t_Origin = GetPrimDocOrigin(pm.t_PaymentID, pm.t_DocKind)
                       and tmp.t_ErrorStatus = 0
                       and tmp.t_SkipDocument = 0 )
        loop
          select t_StatusIDFrom into v_Old from dpmhist_dbt where t_AutoKey = (select MAX(t_AutoKey) from dpmhist_dbt where t_PaymentID = rec.t_OrderID);
          if p_NewStatus <> CHR(1) or p_PaymStatus in (PM_READIED, PM_I2PLACED, PM_IWPPLACED, PM_REJECTED) or v_Old in (PM_I2PLACED, PM_IWPPLACED) then
            v_TmpCLOB:= PaymStatusToXml(rec.t_OrderID, p_PaymStatus, p_NewStatus);
            dbms_lob.writeappend(v_DocInfo, dbms_lob.getLength(v_TmpCLOB), v_TmpCLOB);
            v_NeedInsert:= 1;
          end if;
        end loop;
        if v_NeedInsert = 0 then
          dbms_lob.close(v_DocInfo);
          return 0;
        end if;
      end if;
    else
      v_TmpCLOB:= PaymStatusToXml(p_PaymentID, p_PaymStatus, p_NewStatus);
      dbms_lob.writeappend(v_DocInfo, dbms_lob.getLength(v_TmpCLOB), v_TmpCLOB);
    end if;

    v_TmpBuff:= '</DocStatusList></DocStatusInfo>';
    dbms_lob.writeappend(v_DocInfo, LENGTH(v_TmpBuff), v_TmpBuff);
    dbms_lob.close(v_DocInfo);

    return RsbEventLog.InsertEventLog(ASYNC_CALL_EVENT_DOC_STATUS, OBJTYPE_PMPAYMSTATUS, v_DocInfo);
  end PaymStatusToXmlAll;

  function DefineObjectKind( p_PrimDocKind in NUMBER ) return NUMBER
  as
  begin
    case p_PrimDocKind
    when PS_PAYORDER then return OBJTYPE_PSPAYORD;
    when DLDOC_BANKPAYMENT then return OBJTYPE_BANKPAYMENT;
    when DLDOC_BANKCLAIM then return OBJTYPE_BANKCLAIM;
    when BBANK_CPORDER then return OBJTYPE_BBANKCPORDER;
    when PS_CPORDER then return OBJTYPE_PSCPORDER;
    --when CASH_BOF_ADDORDER then return OBJTYPE_BBCASHORDER;
    --when CASH_BOF_INCORDER then return OBJTYPE_BBCASHORDER;
    --when CASH_BOF_OUTORDER then return OBJTYPE_BBCASHORDER;
    when CASH_PS_INCORDER then return OBJTYPE_PSCASHORDERIN;
    when CASH_PS_OUTORDER then return OBJTYPE_PSCASHORDEROUT;
    when DLDOC_MEMORIALORDER then return OBJTYPE_MEMORIALORDER;
    when CB_MULTYDOC then return OBJTYPE_MULTYDOC;
    when PMDOC_CASHDOCUMENT then return OBJTYPE_CASHDOCUMENT;
    else return 0;
    end case;
  end DefineObjectKind;

  function GetPrimDocOrigin( p_PaymentID in NUMBER, p_DocKind in NUMBER default 0) return NUMBER
  as
    v_Origin NUMBER:= 0;
    v_DocKind NUMBER:= p_DocKind;
  begin
    if v_DocKind = 0 then
      select t_DocKind, t_PrimDocOrigin
        into v_DocKind, v_Origin
        from dpmpaym_dbt
       where t_PaymentID = p_PaymentID;
      if not v_DocKind in (CB_MULTYDOC, DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM,
          BBANK_CPORDER, PS_PAYORDER, PS_CPORDER, PM_COMMON.PS_INRQ,
          CASH_BOF_ADDORDER, CASH_PS_INCORDER, CASH_PS_OUTORDER,
          CASH_BOF_INCORDER, CASH_BOF_OUTORDER, DLDOC_INOUTORDER,
          DLDOC_MEMORIALORDER) then
        return v_Origin;
      end if;
    end if;
    case
    when v_DocKind = CB_MULTYDOC then
      select t_Origin into v_Origin from dmultydoc_dbt where t_AutoKey = p_PaymentID;
    when v_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM) then
      select t_Origin into v_Origin from dmemorder_dbt where t_OrderID = p_PaymentID;
    when v_DocKind = BBANK_CPORDER then
      select t_Origin into v_Origin from dbbcpord_dbt where t_OrderID = p_PaymentID;
    when v_DocKind = PS_PAYORDER then
      select t_Origin into v_Origin from dpspayord_dbt where t_OrderID = p_PaymentID;
    when v_DocKind = PS_CPORDER then
      select t_Origin into v_Origin from dpscpord_dbt where t_OrderID = p_PaymentID;
    when v_DocKind = DLDOC_MEMORIALORDER then
      select t_Origin into v_Origin from dcb_doc_dbt where t_DocumentID = p_PaymentID;
    when v_DocKind = PM_COMMON.PS_INRQ then
      select t_Origin into v_Origin from dpsinrq_dbt where t_PaymentID = p_PaymentID;
    when v_DocKind in (CASH_BOF_ADDORDER, CASH_PS_INCORDER, CASH_PS_OUTORDER,
        CASH_BOF_INCORDER, CASH_BOF_OUTORDER, DLDOC_INOUTORDER) then
      select t_Origin into v_Origin from dpscshdoc_dbt where t_AutoKey = p_PaymentID;
    else
      select t_PrimDocOrigin into v_Origin from dpmpaym_dbt where t_PaymentID = p_PaymentID;
    end case;
    return v_Origin;
  exception
    when OTHERS then return -1;
  end GetPrimDocOrigin;

  function DeterminePrimDocOrigin( p_DocKind  in NUMBER, p_Origin in NUMBER default 0) return NUMBER
  as
    v_PrimDocOrigin NUMBER:= PD_OR_MANUAL;
  begin
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, CASH_BOF_ADDORDER, CASH_PS_INCORDER, 
                        CASH_PS_OUTORDER, CASH_BOF_INCORDER, CASH_BOF_OUTORDER, DLDOC_INOUTORDER,
                        PS_BUYCURORDER) and
          p_Origin = 0 --<РучнойВвод>
        ) or
        ( p_DocKind in (DLDOC_MEMORIALORDER, PS_PAYORDER, CB_MULTYDOC, BBANK_CPORDER,
                               PS_CPORDER, PM_COMMON.PS_INRQ) and
          p_Origin = 1 --<РучнойВвод>
        ) 
    then
        v_PrimDocOrigin := PD_OR_MANUAL; -- РучнойВвод
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM) and
          p_Origin = 2 --<СозданАвтоматич>
        ) or
        ( p_DocKind = DLDOC_MEMORIALORDER and
          p_Origin = 10 --<Автоматически>
        ) or
        ( p_DocKind = PS_PAYORDER and
          p_Origin = 5 --<СозданАвтоматич>
        ) or
        ( p_DocKind in (CASH_BOF_ADDORDER, CASH_PS_INCORDER, CASH_PS_OUTORDER, CASH_BOF_INCORDER, CASH_BOF_OUTORDER, 
                        DLDOC_INOUTORDER) and
          p_Origin = 2 --<СозданАвто>
        ) or
        ( p_DocKind = CB_MULTYDOC and
          p_Origin = 5 --<СозданАвто>
        ) or
        ( p_DocKind in (BBANK_CPORDER, PS_CPORDER,PS_BUYCURORDER) and
          p_Origin = 7 --<СозданАвтомат>
        ) or        
        ( p_DocKind = PM_COMMON.PS_INRQ and
          p_Origin = 2 --<СозданАвто>
        ) or        
        ( p_DocKind = WL_INDOC )  
    then
        v_PrimDocOrigin := PD_OR_AUTO; -- Авто
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, BBANK_CPORDER, PS_CPORDER) and
          p_Origin = 3 --<КомиссияЗаОбсл>
        ) or        
        ( p_DocKind = PS_PAYORDER and
          p_Origin = 7 --<КомиссияЗаОбсл>
        ) 
    then
        v_PrimDocOrigin := PD_OR_SF; -- Комиссия за обслуживание
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM) and
          p_Origin = 7 --<Кредитование>
        ) or
        ( p_DocKind in (DLDOC_MEMORIALORDER, CB_MULTYDOC) and
          p_Origin = 2 --<Кредитование>
        ) or
        ( p_DocKind = PS_PAYORDER and
          p_Origin = 8 --<Кредитование>
        ) or
        ( p_DocKind in (CASH_BOF_ADDORDER, CASH_PS_INCORDER, CASH_PS_OUTORDER, CASH_BOF_INCORDER, CASH_BOF_OUTORDER, 
                        DLDOC_INOUTORDER) and
          p_Origin = 3 --<Кредитование>
        ) or
        ( p_DocKind in (BBANK_CPORDER, PS_CPORDER) and
          p_Origin = 9 --<Кредитование>
        ) 
    then
        v_PrimDocOrigin := PD_OR_LOANS; -- Подсистема "Кредитование"
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM) and
          p_Origin = 8 --<ОбслужФизЛиц>
        ) or
        ( p_DocKind in (DLDOC_MEMORIALORDER, CB_MULTYDOC) and
          p_Origin = 3 --<ОбслужФизЛиц>
        ) or
        ( p_DocKind = PS_PAYORDER and
          p_Origin = 9 --<ОбслужФизЛиц>
        ) or
        ( p_DocKind in (CASH_BOF_ADDORDER, CASH_PS_INCORDER, CASH_PS_OUTORDER, CASH_BOF_INCORDER, CASH_BOF_OUTORDER, 
                        DLDOC_INOUTORDER) and
          p_Origin = 4 --<ОбслужФизЛиц>
        ) or
        ( p_DocKind in (BBANK_CPORDER, PS_CPORDER) and
          p_Origin = 10 --<ОбслужФизЛиц>
        ) 
    then
        v_PrimDocOrigin := PD_OR_RETAIL; -- Подсистема "Обслуживание физических лиц"
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, DLDOC_MEMORIALORDER,
                              PS_PAYORDER, CB_MULTYDOC, BBANK_CPORDER, PS_CPORDER) and
          p_Origin = 18 --<Проценты>
        ) 
    then
        v_PrimDocOrigin := PD_OR_PERCENT; -- Подсистема "Проценты"
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, CASH_BOF_ADDORDER, CASH_PS_INCORDER, CASH_PS_OUTORDER, 
                        CASH_BOF_INCORDER, CASH_BOF_OUTORDER, DLDOC_INOUTORDER) and
          p_Origin =1 --<Зарплата>
        ) or
        ( p_DocKind = DLDOC_MEMORIALORDER and
          p_Origin = 4 --<Incounting>
        ) or
        ( p_DocKind in (BBANK_CPORDER, PS_CPORDER) and
          p_Origin = 6 --<Зарплата>
        ) 
    then
        v_PrimDocOrigin := PD_OR_INCOUNTING;  -- Подсистема "Incounting" (Подсистема "Зарплата")
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, CASH_BOF_ADDORDER, CASH_PS_INCORDER, CASH_PS_OUTORDER, 
                        CASH_BOF_INCORDER, CASH_BOF_OUTORDER, DLDOC_INOUTORDER) and
          p_Origin = 5 --<Депозиты>
        ) or
        ( p_DocKind = DLDOC_MEMORIALORDER and
          p_Origin = 11 --<Депозиты>
        ) or
        ( p_DocKind = PS_PAYORDER and
          p_Origin = 10 --<Депозиты>
        ) or
        ( p_DocKind = CB_MULTYDOC and
          p_Origin = 4 --<Депозиты>
        ) or
        ( p_DocKind in (BBANK_CPORDER, PS_CPORDER) and
          p_Origin = 20 --<Депозиты>
        ) 
    then
        v_PrimDocOrigin := PD_OR_DEPLEGPERS;  -- Подсистема "Депозиты юридических лиц" (Депозиты юридических лиц)
    end if;
    if  ( p_DocKind in (BBANK_CPORDER, PS_CPORDER) and
          p_Origin = 5 --<КредитыДепозиты>
        ) 
    then
        v_PrimDocOrigin := PD_OR_CREDDEP;  -- Подсиcтема "Кредиты-Депозиты"
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM) and
          p_Origin = 4 --<УчетДоговоров>
        ) 
    then
        v_PrimDocOrigin := PD_OR_REGCONTR; -- Подсистема "Учет договоров"
    end if;
    if  ( p_DocKind = PS_PAYORDER and
          p_Origin = 3 --<CognitiveForms>
        ) 
    then
        v_PrimDocOrigin :=  PD_OR_CF; -- CognitiveForms
    end if;
    if  ( p_DocKind = PS_PAYORDER and
          p_Origin = 6 --<FineReaderBank>
        ) 
    then
        v_PrimDocOrigin :=  PD_OR_FRB; -- FineReaderBank
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, DLDOC_MEMORIALORDER) and
          p_Origin = 19 --<КлиентБанк>
        ) or
       ( p_DocKind in (PS_PAYORDER, BBANK_CPORDER, PS_CPORDER) and
          p_Origin = 2 --<КлиентБанк>
        ) or        
        ( p_DocKind = PS_BUYCURORDER and
          p_Origin = 1 --<КлиентБанк>
        ) 
    then
        v_PrimDocOrigin := PD_OR_CLB; -- Клиент-Банк (Подсистема <Клиент-Банк>)
    end if;
    if  ( p_DocKind in (PS_PAYORDER, BBANK_CPORDER, PS_CPORDER) and
          p_Origin = 2 --<КлиентБанк>
        ) 
    then
        v_PrimDocOrigin := PD_OR_CLSB; -- Клиент-Сбербанк
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, DLDOC_MEMORIALORDER,
                        CASH_BOF_ADDORDER, CASH_PS_INCORDER, CASH_PS_OUTORDER, 
                         CASH_BOF_INCORDER, CASH_BOF_OUTORDER, DLDOC_INOUTORDER, 
                          BBANK_CPORDER, PS_CPORDER ) and
          p_Origin = 30 --<Кредитование5.50>
        ) 
    then
        v_PrimDocOrigin := PD_OR_LOANS_5_50; -- Подсистема "Кредитование 5.50"
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, DLDOC_MEMORIALORDER,
                        CASH_BOF_ADDORDER, CASH_PS_INCORDER, CASH_PS_OUTORDER, 
                         CASH_BOF_INCORDER, CASH_BOF_OUTORDER, DLDOC_INOUTORDER, 
                          BBANK_CPORDER, PS_CPORDER ) and
          p_Origin = 31 --<ОбслужФизЛиц5.50>
        ) 
    then
        v_PrimDocOrigin := PD_OR_RETAIL_5_50; -- Подсистема "Обслуживание физических лиц 5.50"
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, DLDOC_MEMORIALORDER,
                        CASH_BOF_ADDORDER, CASH_PS_INCORDER, CASH_PS_OUTORDER, 
                         CASH_BOF_INCORDER, CASH_BOF_OUTORDER, DLDOC_INOUTORDER, 
                          BBANK_CPORDER, PS_CPORDER ) and
          p_Origin = 32 --<МВОДБ5.50>
        ) 
    then
        v_PrimDocOrigin := PD_OR_MVODB_5_50; -- Подсистема "МВОДБ 5.50"
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM ) and
          p_Origin = 12 --<ОплатаТреб>
        ) or 
        ( p_DocKind in ( BBANK_CPORDER, PS_CPORDER ) and
          p_Origin = 15 --<ОплатаТреб>
        ) 
    then
        v_PrimDocOrigin := PD_OR_PAYCLAIM; -- Оплата требования
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM ) and
          p_Origin = 13 --<ОбрНевПлат>
        ) or 
        ( p_DocKind in ( BBANK_CPORDER, PS_CPORDER ) and
          p_Origin = 16 --<ОбрНевПлат>
        ) 
    then
        v_PrimDocOrigin := PD_OR_PROCUNKNOWNPM; -- Обработка невыясненного платежа
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM ) and
          p_Origin = 6 --<ВозврНевыясПлат>
        ) or 
        ( p_DocKind in ( BBANK_CPORDER, PS_CPORDER ) and
          p_Origin = 8 --<Возврат>
        ) 
    then
        v_PrimDocOrigin := PD_OR_RETURNUNKNOWNPM; -- Возврат невыясненного платежа
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM ) and
          p_Origin = 10 --<ПеренапрНевПлат>
        ) or 
        ( p_DocKind in ( BBANK_CPORDER, PS_CPORDER ) and
          p_Origin = 12 --<ПеренапрНевПлат>
        ) 
    then
        v_PrimDocOrigin := PD_OR_REDIRECTUNKNOWNPM; -- Перенаправление невыясненного платежа
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM ) and
          p_Origin = 11 --<ТранзитПлат>
        ) or 
        ( p_DocKind in ( BBANK_CPORDER, PS_CPORDER ) and
          p_Origin = 13 --<ТранзитПлат>
        ) 
    then
        v_PrimDocOrigin := PD_OR_TRANSITPAY; -- Транзитный платеж
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, DLDOC_MEMORIALORDER,
                              CB_MULTYDOC) and
          p_Origin = 9 --<ПлатНовРекв>
        ) or 
        ( p_DocKind in ( BBANK_CPORDER, PS_CPORDER ) and
          p_Origin = 11 --<ПлатНовРекв>
        ) 
    then
        v_PrimDocOrigin := PD_OR_NEWREQUISITPAY; -- Платеж по новым реквизитам
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, PS_PAYORDER,
                              BBANK_CPORDER, PS_CPORDER, PM_COMMON.PS_INRQ ) and
          p_Origin = 21 --<ПоручФНС>
        ) 
    then
        v_PrimDocOrigin := PD_OR_FNS; -- Поручение на списание ФНС
    end if;
    if  ( p_DocKind in (PS_PAYORDER, PM_COMMON.PS_INRQ ) and
          p_Origin = 19 --<ВыстБПол>
        ) 
    then
        v_PrimDocOrigin := PD_OR_PAYEEBANK; -- Выставлен банком получателя
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, BBANK_CPORDER, 
                              PS_CPORDER ) and
          p_Origin = 40 --<ПереводОстатка>
        ) 
    then
        v_PrimDocOrigin := PD_OR_CLOSTRANS; -- Перевод остатка с закрываемого счета
    end if;
    if  ( p_DocKind in (PS_PAYORDER, CASH_BOF_ADDORDER, CASH_PS_INCORDER, CASH_PS_OUTORDER, 
                        CASH_BOF_INCORDER, CASH_BOF_OUTORDER, DLDOC_INOUTORDER, BBANK_CPORDER, 
                        PS_CPORDER ) and
          p_Origin = 17 --<ВозврСНакСчета>
        ) 
    then
        v_PrimDocOrigin :=  PD_OR_RETSAVACCREM; -- Возврат с накопительного счета с остатком
    end if;
    if  ( p_DocKind in (DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, BBANK_CPORDER, 
                              PS_CPORDER ) and
          p_Origin = 22 --<ПереофМФПлатежа>
        ) 
    then
        v_PrimDocOrigin := PD_OR_MFR; -- Переоформление межфилиального платежа
    end if;
    if  ( p_DocKind = PS_BUYCURORDER and
          p_Origin = 3 --<ПродВалПоИПВС>
        ) 
    then
        v_PrimDocOrigin := PD_OR_SELCURCOLORD; -- Продажа валюты по инкассовому поручению
    end if;
    if  ( p_DocKind in (CB_MULTYDOC, DLDOC_BANKPAYMENT, DLDOC_BANKCLAIM, BBANK_CPORDER, DLDOC_MEMORIALORDER,
                        PS_BUYCURORDER, PS_PAYORDER, PS_CPORDER, PS_INRQ, PMDOC_CASHDOCUMENT ) and
          p_Origin = 31
        ) 
    then
        v_PrimDocOrigin := PD_OR_HCSCHARGE; -- Оплата по начислению ГИС ЖКХ
    end if;

    if  ( p_DocKind in ( PS_PAYORDER, DLDOC_BANKPAYMENT, DLDOC_BANKORDER, CASH_BOF_OUTORDER ) and
          p_Origin = 53
        )
    then
        v_PrimDocOrigin := PD_OR_PARTAT; -- Частичное исполнение платежа
    end if;
    
    return v_PrimDocOrigin;
  end DeterminePrimDocOrigin;

  function CheckUIN
  ( p_ReceiverAccount in VARCHAR2,
    p_UIN in VARCHAR2,
    p_ErrMsg out VARCHAR2
  ) return INTEGER
  as

    type ArrCoeff_t is VARRAY(16) of INTEGER;
    v_Coeff ArrCoeff_t := ArrCoeff_t(3,7,1,3,7,3,7,1,3,7,3,7,1,3,7,3);

    v_Sum1 INTEGER := 0;
    v_Sum2 INTEGER := 0;
    v_Key  INTEGER := 0;

    function GetUinAccMask return VARCHAR2
    as
      v_RegValue VARCHAR(2000);
    begin
      v_RegValue:= RSB_COMMON.GetRegStrValue('CB\PAYMENTS\CHECK_CONTROLKEY_UIN');
      if v_RegValue is NULL or v_RegValue = RSB_EMPTY_STRING then
        v_RegValue:= '40822*';
      end if;

      return v_RegValue;
    end GetUinAccMask;
  begin
    p_ErrMsg := RSB_EMPTY_STRING;

    -- Если ReceiverAccount не заполнен или не подходит под маску счетов,
    -- заданную в настройке CB\PAYMENTS\CHECK_CONTROLKEY_UIN, вернуть 0
    if p_ReceiverAccount = RSB_EMPTY_STRING or p_ReceiverAccount is null then
      return 0;
    end if;
    if RSI_RSB_MASK.CompareStringWithMask(GetUinAccMask(), p_ReceiverAccount) != 1 then
      return 0;
    end if;

    -- Реквизит "Код" для платежа с указанным счетом получателя должен быть заполнен
    if p_UIN = RSB_EMPTY_STRING or p_UIN is null then
      p_ErrMsg := 'Реквизит "Код" для платежа с указанным счетом получателя должен быть заполнен';
      return PM_ERROR.PAYMERR_EMPTY_UIN;
    end if;

    -- Вычисляем сумму
    for i in 1 .. 5 loop
      v_Sum1 := v_Sum1 + to_number( substr(p_ReceiverAccount, i, 1) ) * v_Coeff(i);
    end loop;
    for i in 2 .. 16 loop
      v_Sum1 := v_Sum1 + to_number( substr(p_UIN, i, 1) ) * v_Coeff(i);
    end loop;

    v_Sum2 := v_Sum1 + to_number( substr(p_UIN, 1, 1) ) * v_Coeff(1);
    if mod(v_Sum2, 10) = 0 then
      return 0;
    else
      v_Key := mod(v_Sum1 * 3, 10);
      p_ErrMsg := 'Контрольный ключ реквизита "Код" имеет значение ' || substr(p_UIN, 1, 1) || '. Должно быть ' || v_Key;
      return PM_ERROR.PAYMERR_UIN_WRONG_KEY;
    end if;

    return 0;
  end CheckUIN;

  function IsBdTrSummaryPayment(p_PaymentID IN dpmpaym_dbt.T_PAYMENTID%type)
    return BOOLEAN
  as
    v_ExistsTransfer integer default 0;
  begin
    select NVL
           ( ( select 1 
                 from dual
                where exists 
                    ( select 1
                        from dbdtransf_dbt
                       where t_CoverPaymentID = p_PaymentID )
             ),
             0
           ) into v_ExistsTransfer
      from dual;

    if v_ExistsTransfer = 1 then
      RETURN TRUE;
    end if;

    return false;
  end IsBdTrSummaryPayment;
  
  function GetFillINNPayerByDocKind( p_DocKind IN INTEGER, p_SubDocKind IN INTEGER ) return VARCHAR2
  as
    v_Context    varchar(100);
    v_RegValue   varchar2(2000);
  begin
    v_Context := PM_NAMES.GetContextForPrimaryDoc(p_DocKind, p_SubDocKind);
    v_RegValue := RSB_COMMON.GetRegStrValue( v_Context || '\FILLINNPAYER');
    return v_RegValue;
  end GetFillINNPayerByDocKind;
  
  function GetFillINNPayerByContext( p_Context in varchar2 ) return VARCHAR2
  as
    v_RegValue   varchar2(2000);
  begin
    v_RegValue := RSB_COMMON.GetRegStrValue( p_Context || '\FILLINNPAYER');
    return v_RegValue;
  end GetFillINNPayerByContext;

  function GetFillINNReceiverByDocKind( p_DocKind IN INTEGER, p_SubDocKind IN INTEGER ) return VARCHAR2
  as
    v_Context    varchar(100);
    v_RegValue   varchar2(2000);
  begin
    v_Context := PM_NAMES.GetContextForPrimaryDoc(p_DocKind, p_SubDocKind);
    v_RegValue := RSB_COMMON.GetRegStrValue( v_Context || '\FILLINNReceiver');
    return v_RegValue;
  end GetFillINNReceiverByDocKind;
  
  function GetFillINNReceiverByContext( p_Context in varchar2 ) return VARCHAR2
  as
    v_RegValue   varchar2(2000);
  begin
    v_RegValue := RSB_COMMON.GetRegStrValue( p_Context || '\FILLINNReceiver');
    return v_RegValue;
  end GetFillINNReceiverByContext;
  
  function CheckWaitExec( p_ValueDate in DATE, p_DocKind in NUMBER, p_SubDockind in NUMBER ) return INTEGER
  is
  begin
    if p_DocKind in (PS_PAYORDER, PS_CPORDER, PS_INRQ) and 
        p_SubDockind <> 1 and 
        p_ValueDate > RsbSessionData.CurDate and 
        RSB_COMMON.GetRegBoolValue('PS\FUTURE_DATE_PAYMENT\USEINDWAITEXEC') = true then
      return 1;
    else
      return 0;
    end if; 
  end CheckWaitExec;

  function InPersnMaskCheck( p_Account in varchar2 ) return boolean
  as
  begin
    if m_PersnAccountMask is null then
      m_PersnAccountMask:= RSB_COMMON.GetRegStrValue( 'PS\REQOPENACC\СЧЕТА ФИЗИЧЕСКИХ ЛИЦ' );
    end if;

    if m_PersnAccountMask is NULL or m_PersnAccountMask = PM_COMMON.RSB_EMPTY_STRING then
      -- Если настройка не задана, проверки не выполняются
      return FALSE;
    end if;

    if RSI_RSB_MASK.CompareStringWithMask( m_PersnAccountMask, p_Account ) = 1 then
      return TRUE;
    end if;

    return FALSE;
  end InPersnMaskCheck;

  function IsConsolidatedAccount( p_Account in varchar2, p_Type_Account in varchar2, p_LegalForm in integer ) return integer
  as
  begin
    if( ( p_Type_Account like '%И%' ) or ( p_LegalForm = PM_COMMON.PTLEGF_INST and InPersnMaskCheck( p_Account ) = true ) ) then
      return PM_COMMON.ACC_CONSOLIDATED;
    else
      return PM_COMMON.ACC_NOT_CONSOLIDATED;
    end if;
  end IsConsolidatedAccount;
  
  function ConsolidatedAccCheckRules( p_Account in varchar2, p_Type_Account in varchar2, p_LegalForm in integer ) return integer 
  as
  begin  
   if m_CheckINNSumACCValue is null then
     m_CheckINNSumACCValue := RSB_COMMON.GetRegIntValue( 'CB\PAYMENTS\DEPARTMENTALINFO\CHECKINNSUMACC' );
   end if;
   if( ( p_Type_Account like '%И%' and m_CheckINNSumACCValue = CHISA_NOTCHECK ) 
    or ( p_Type_Account like '%И%' and m_CheckINNSumACCValue = CHISA_BOTH_CHECK and InPersnMaskCheck( p_Account ) = false ) ) then
     return PM_COMMON.CAR_NOT_CHECK;
   elsif( ( p_LegalForm = PM_COMMON.PTLEGF_PERSN ) 
       or ( p_LegalForm = PM_COMMON.PTLEGF_INST and ( m_CheckINNSumACCValue = CHISA_PERSN_ACC_MASK_CHECK or m_CheckINNSumACCValue = CHISA_BOTH_CHECK ) and InPersnMaskCheck( p_Account ) = true ) ) then
     return PM_COMMON.CAR_PERSN_ACC_CHECK;
   else
     return PM_COMMON.CAR_INST_ACC_CHECK;
   end if;
  end  ConsolidatedAccCheckRules;

  function GetLegalForm(p_PartyID IN dparty_dbt.t_PartyID%type) 
    return dparty_dbt.t_LegalForm%type
  is
    v_LegalForm dparty_dbt.t_LegalForm%type := PTLEGF_ALL;
  begin
    if p_PartyID > 0 then
      select t_LegalForm into v_LegalForm
        from dparty_dbt
       where t_PartyID = p_PartyID;
    end if;

    return v_LegalForm;
  end GetLegalForm;

  function GetIsEmployer(p_PartyID IN dparty_dbt.t_PartyID%type) 
    return dpersn_dbt.t_IsEmployer%type
  is
    v_IsEmployer dpersn_dbt.t_IsEmployer%type := UNSET_CHAR;
  begin
    if p_PartyID > 0 then

      select NVL( ( select prs.t_IsEmployer
                      from dpersn_dbt prs
                     where prs.t_PersonID = p_PartyID
                  ), 
                  PM_COMMON.UNSET_CHAR ) into v_IsEmployer
        from dual;

    end if;

    return v_IsEmployer;
  end GetIsEmployer;

  FUNCTION IsPersonalAcc(p_Account IN VARCHAR2) RETURN INTEGER
  AS

    function IsAccInRegMask(p_Account IN VARCHAR2, p_RegPath IN VARCHAR2, g_Mask IN OUT VARCHAR2, p_DefaultRegVal IN VARCHAR2)
      return boolean
    as
    begin
      if g_Mask is null then
        g_Mask := RSB_COMMON.GetRegStrValue(p_RegPath);

        if g_Mask is null then
          g_Mask := p_DefaultRegVal;
        end if;
      end if;

      if g_Mask <> PM_COMMON.RSB_EMPTY_STRING and 
        CompareAccWithMask( g_Mask, p_Account ) = 1 
      then
        return true;
      end if;

      return false;
    end IsAccInRegMask;

  BEGIN
    if IsAccInRegMask(p_Account, 'PS/REQOPENACC/OPERATION/365-П/СЧЕТ_ФИЗЛИЦА', m_PersnAccountMask, '408*') then
      return 1;
    end if;

    if IsAccInRegMask(p_Account, 'PS/REQOPENACC/OPERATION/365-П/ВКЛАД_ФИЗЛИЦА', m_PersnDepositMask, '423*, 426*') then
      return 1;
    end if;

    return 0;
  END IsPersonalAcc;

  FUNCTION RSI_GetAccOwnerKind
  ( AccountList IN VARCHAR2,
    ClientID IN dparty_dbt.t_PartyID%TYPE DEFAULT 0,
    ClientType IN dparty_dbt.t_LegalForm%TYPE DEFAULT PM_COMMON.PTLEGF_ALL,
    ClientINN IN dobjcode_dbt.t_Code%TYPE DEFAULT PM_COMMON.RSB_EMPTY_STRING
  ) RETURN INTEGER
  AS
    v_IsCategory number := 0;
    v_IsPersonalAccInList boolean := false;
    v_IsNonPersonalAccInList boolean := false;
    v_LenINN integer := LENGTH(ClientINN);
  BEGIN
    IF ClientID > 0 THEN
      if ClientType = PM_COMMON.PTLEGF_INST or 
         PM_COMMON.GetLegalForm(ClientID) = PM_COMMON.PTLEGF_INST
      then
        RETURN ACCOUNTS_OWNER_KIND_PS;
      end if;

      if PM_COMMON.GetIsEmployer(ClientID) = PM_COMMON.UNSET_CHAR
      then
        -- проверяем значение категории "Тип субъекта"
        select NVL( ( select 1
                        from dobjatcor_dbt att
                       where att.t_ObjectType = PM_COMMON.OBJTYPE_PARTY
                         and att.t_GroupID = PM_COMMON.PARTY_ATTR_TYPE
                         and att.t_AttrID in ( PM_COMMON.PARTY_AT_NOTARY,
                                               PM_COMMON.PARTY_AT_LAWYER,
                                               PM_COMMON.PARTY_AT_LOMBARD,
                                               PM_COMMON.PARTY_AT_KFX,
                                               PM_COMMON.PARTY_AT_TRUSTMANAGER,
                                               PM_COMMON.PARTY_AT_MANAGPARTNER,
                                               PM_COMMON.PARTY_AT_ARBITRMANAGER )
                         and att.t_Object = to_char(ClientID, 'FM0999999999')
                         AND ROWNUM = 1 
                    ), 0 ) into v_IsCategory
        from dual;

        if v_IsCategory = 0 then
          RETURN ACCOUNTS_OWNER_KIND_PERSN;
        end if;
      end if;
    END IF;

    IF AccountList IS NOT NULL AND AccountList <> PM_COMMON.RSB_EMPTY_STRING THEN

      FOR rec IN ( SELECT trim(regexp_substr(t_AccountList, '[^,]+', 1, level)) AS t_Account
                     FROM ( SELECT AccountList AS t_AccountList 
                              FROM dual ) t
                   CONNECT BY instr(t_AccountList, ',', 1, level - 1) > 0 )
      LOOP
        if IsPersonalAcc(rec.t_Account) = 1 then
          v_IsPersonalAccInList := true;
        else
          v_IsNonPersonalAccInList := true;
        end if;

        if v_IsPersonalAccInList and v_IsNonPersonalAccInList then
          exit; -- дальше проверять нет смысла, выходим из цикла
        end if;
      END LOOP;

      IF v_IsPersonalAccInList AND NOT v_IsNonPersonalAccInList THEN
        RETURN ACCOUNTS_OWNER_KIND_PERSN;
      END IF;

      IF v_IsNonPersonalAccInList AND NOT v_IsPersonalAccInList THEN
        RETURN ACCOUNTS_OWNER_KIND_PS;
      END IF;

      IF ClientID > 0 AND v_IsPersonalAccInList AND v_IsNonPersonalAccInList THEN
        RETURN ACCOUNTS_OWNER_KIND_ALL;
      END IF;
    END IF;

    IF v_LenINN = 10 THEN
      RETURN ACCOUNTS_OWNER_KIND_PS;
    END IF;

    IF v_LenINN = 12 THEN
      RETURN ACCOUNTS_OWNER_KIND_PERSN;
    END IF;

    RETURN ACCOUNTS_OWNER_KIND_UNDEF;
  END RSI_GetAccOwnerKind;

  FUNCTION GetDprtPartyID(p_DprtID in ddp_dep_dbt.t_Code%type) 
    RETURN dparty_dbt.t_PartyID%type
  AS
    v_PartyID dparty_dbt.t_PartyID%type;
  BEGIN
    select t_PartyID into v_PartyID
      from ddp_dep_dbt
     where t_Code = p_DprtID;

    return v_PartyID;
  END GetDprtPartyID;

  -- Является ли банк УБР
  FUNCTION IsBankUBR( p_BankID IN NUMBER ) RETURN BOOLEAN
  AS
    v_RegPBR VARCHAR2(2000) := RSB_COMMON.GetRegStrValue('МЕЖБАНКОВСКИЕ РАСЧЕТЫ\УФЭБС\ПБР_Тип_Участника');
    v_Element NUMBER(5) := 0;
  BEGIN
    if v_RegPBR is null then
      v_RegPBR := '00, 10,12, 15, 40'; -- значение по умолчанию
    end if;

    SELECT Count(1) into v_Element
      FROM dbankdprt_dbt bnkdprt
     WHERE bnkdprt.t_PartyID = p_BankID
       AND ( bnkdprt.t_PZN in 
               ( SELECT trim( regexp_substr(str, '[^,]+', 1, level) ) str
                   FROM ( SELECT v_RegPBR str FROM dual ) t   
                 CONNECT BY instr(str, ',', 1, level - 1) > 0 
               )
           );

    IF v_Element = 0 THEN 
      return false;
    ELSE
      return true;
    END IF;
  END IsBankUBR;

  FUNCTION IsBankUBR_Num( p_BankID IN NUMBER ) RETURN NUMBER
  AS
    v_Result BOOLEAN := false;  
  BEGIN
    v_Result := IsBankUBR( p_BankID );
    IF v_Result = true THEN 
      return 1;
    ELSE
      return 0;
    END IF;
  END IsBankUBR_Num;


FUNCTION GetOperParticipantsFromRoute (p_PaymentID IN NUMBER)
   RETURN OperParticipants_t
AS
   PayerBank              dpmroute_dbt%ROWTYPE;
   PrevInstructingAgent   dpmroute_dbt%ROWTYPE;
   SenderBank             dpmroute_dbt%ROWTYPE;
   ExecutorBank           dpmroute_dbt%ROWTYPE;
   IntermediaryBank       dpmroute_dbt%ROWTYPE;
   ReceiverBank           dpmroute_dbt%ROWTYPE;

   Participants           OperParticipants_t;
   
   TYPE Route_t IS TABLE OF dpmroute_dbt%ROWTYPE;   
   routes Route_t;
   
   OutPayment NUMBER := 0;
   InPayment NUMBER := 0;
   
   cur_i NUMBER := -1;
   PartyZero dpmroute_dbt%ROWTYPE;
   
   FUNCTION AddFillPMROUTE (pmroute_in IN dpmroute_dbt%ROWTYPE)
   RETURN dpmroute_dbt%ROWTYPE
AS
   route   dpmroute_dbt%ROWTYPE := pmroute_in;
BEGIN
   IF route.t_CodeName IS NULL OR route.t_CodeName = CHR (1)
   THEN
      IF route.t_CodeKind = 0 OR route.t_CodeKind IS NULL
      THEN
         route.t_CodeKind := PM_COMMON.PTCK_CLIENT;
      END IF;

      BEGIN
         SELECT objkcode.t_Shortname
           INTO route.t_CodeName
           FROM dobjkcode_dbt objkcode
          WHERE     objkcode.t_ObjectType = PM_COMMON.OBJTYPE_PARTY
                AND Objkcode.t_Codekind = route.t_codekind;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            route.t_CodeName := CHR (1);
      END;
   END IF;

   IF     route.t_PartyID <> -1
      AND route.t_PartyID IS NOT NULL
      AND route.t_CodeKind <> 0
      AND route.t_CodeKind IS NOT NULL
      AND (route.t_CodeValue = CHR (1) OR route.t_CodeValue IS NULL)
   THEN
      BEGIN
         SELECT partcode.t_Code
           INTO route.t_CodeValue
           FROM dpartcode_dbt partcode
          WHERE     partcode.t_PartyID = route.t_PartyID
                AND partcode.t_Codekind = route.t_codekind;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            route.t_CodeValue := CHR (1);
      END;
   END IF;

   IF     route.t_PartyID <> -1
      AND route.t_PartyID IS NOT NULL
      AND (route.t_Name = CHR (1) OR route.t_Name IS NULL)
   THEN
      BEGIN
         SELECT party.t_Name
           INTO route.t_Name
           FROM dparty_dbt party
          WHERE party.t_PartyID = route.t_PartyID;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            route.t_Name := CHR (1);
      END;
   END IF;

   RETURN route;
END AddFillPMROUTE;

FUNCTION GetRouteFromPayment (p_PaymentID IN NUMBER)
   RETURN Route_t
AS
   route          Route_t;
   v_pmpaym       dpmpaym_dbt%ROWTYPE;
   v_pmprop_in    dpmprop_dbt%ROWTYPE;
   v_pmprop_out   dpmprop_dbt%ROWTYPE;
   v_pmrmprop     dpmrmprop_dbt%ROWTYPE;

   v_Sort         NUMBER (5);

   CURSOR routes_db_in
   IS
        SELECT *
          FROM dpmroute_dbt pmroute
         WHERE     pmroute.t_sort < 0
               AND pmroute.t_ObjID = p_PaymentID
               AND pmroute.t_ObjKind = PM_COMMON.OBJTYPE_PAYMENT
      ORDER BY pmroute.t_Sort DESC;
      
         CURSOR routes_db_out
   IS
        SELECT *
          FROM dpmroute_dbt pmroute
         WHERE     pmroute.t_sort > 0
               AND pmroute.t_ObjID = p_PaymentID
               AND pmroute.t_ObjKind = PM_COMMON.OBJTYPE_PAYMENT
      ORDER BY pmroute.t_Sort ASC;
BEGIN
   BEGIN
      SELECT pmpaym.*
        INTO v_pmpaym
        FROM dpmpaym_dbt pmpaym
       WHERE pmpaym.t_PaymentID = p_PaymentID;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         route := NULL;
   END;

   BEGIN
      SELECT pmprop.*
        INTO v_pmprop_in
        FROM dpmprop_dbt pmprop
       WHERE     pmprop.t_PaymentID = p_PaymentID
             AND pmprop.t_isSender = PM_COMMON.SET_CHAR;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         route := NULL;
   END;

   BEGIN
      SELECT pmprop.*
        INTO v_pmprop_out
        FROM dpmprop_dbt pmprop
       WHERE     pmprop.t_PaymentID = p_PaymentID
             AND pmprop.t_isSender = PM_COMMON.UNSET_CHAR;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         route := NULL;
   END;

   BEGIN
      SELECT pmrmprop.*
        INTO v_pmrmprop
        FROM dpmrmprop_dbt pmrmprop
       WHERE pmrmprop.t_PaymentID = p_PaymentID;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         route := NULL;
   END;

   IF     v_pmpaym.t_paymentID IS NOT NULL
      AND v_pmprop_in.t_paymentID IS NOT NULL
      AND v_pmprop_out.t_paymentID IS NOT NULL
      AND v_pmrmprop.t_paymentID IS NOT NULL
   THEN
      route := Route_t ();

      -- Узел нашего банка
      route.EXTEND ();
      route (route.LAST).t_ObjID := v_pmpaym.t_PaymentID;
      route (route.LAST).t_ObjKind := PM_COMMON.OBJTYPE_PAYMENT;
      route (route.LAST).t_Sort := 0;
      route (route.LAST).t_PartyID := PM_COMMON.OURBANK;

      IF v_pmprop_in.t_Group = PAYMENTS_GROUP_EXTERNAL
      THEN
         BEGIN
            SELECT dp_dep.t_PartyID, corschem.t_Account
              INTO route (route.LAST).t_PartyID,
                   route (route.LAST).t_OutAccount
              FROM ddp_dep_dbt dp_dep
                   INNER JOIN dcorschem_dbt corschem
                      ON dp_dep.t_code = corschem.t_department
             WHERE     corschem.t_FIID = v_pmprop_in.t_PayFIID
                   AND corschem.t_Number = v_pmprop_in.t_corschem;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               NULL;
         END;
      END IF;

      IF v_pmprop_out.t_Group = PAYMENTS_GROUP_EXTERNAL
      THEN
         BEGIN
            SELECT dp_dep.t_PartyID, corschem.t_Account
              INTO route (route.LAST).t_PartyID,
                   route (route.LAST).t_OutAccount
              FROM ddp_dep_dbt dp_dep
                   INNER JOIN dcorschem_dbt corschem
                      ON dp_dep.t_code = corschem.t_department
             WHERE     corschem.t_FIID = v_pmprop_out.t_PayFIID
                   AND corschem.t_Number = v_pmprop_out.t_corschem;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               NULL;
         END;
      END IF;

      route (route.LAST) := AddFillPMROUTE (route (route.LAST));

      -- Входящая ветка

      IF v_pmprop_in.t_Group = PAYMENTS_GROUP_EXTERNAL
      THEN
         v_Sort := -1;

         route.EXTEND ();
         route (route.LAST).t_ObjID := v_pmpaym.t_PaymentID;
         route (route.LAST).t_ObjKind := PM_COMMON.OBJTYPE_PAYMENT;
         route (route.LAST).t_Sort := v_Sort;

         IF v_pmprop_out.t_OurCorrID <> -1
         THEN
            route (route.LAST).t_PartyID := v_pmprop_out.t_OurCorrID;
            route (route.LAST).t_OutAccount := v_pmprop_out.t_OurCorrAcc;
            route (route.LAST).t_Codekind := v_pmprop_out.t_OurCorrCodeKind;
            route (route.LAST).t_CodeValue := v_pmprop_out.t_OurCorrCode;

            IF v_pmprop_in.t_DebetCredit = PRT_CREDIT
            THEN
               route (route.LAST).t_Name :=
                  v_pmrmprop.t_OurReceiverCorrName;
            ELSE
               route (route.LAST).t_Name := v_pmrmprop.t_OurPayerCorrName;
            END IF;
         ELSE
            BEGIN
               SELECT corschem.t_CorrID
                 INTO route (route.LAST).t_PartyID
                 FROM ddp_dep_dbt dp_dep
                      INNER JOIN dcorschem_dbt corschem
                         ON dp_dep.t_code = corschem.t_department
                WHERE     corschem.t_FIID = v_pmprop_in.t_PayFIID
                      AND corschem.t_Number = v_pmprop_in.t_corschem;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  NULL;
            END;
         END IF;

         v_Sort := v_Sort - 1;

         route (route.LAST) := AddFillPMROUTE (route (route.LAST));

         FOR p IN routes_db_in
         LOOP
            route.EXTEND ();
            route (route.LAST) := p;
            route (route.LAST).t_Sort := v_Sort;
            v_Sort := v_Sort - 1;
         END LOOP;

         IF v_pmprop_in.t_CorrID <> -1
         THEN
            route.EXTEND ();
            route (route.LAST).t_PartyID := v_pmprop_in.t_CorrID;
            route (route.LAST).t_OutAccount := v_pmprop_in.t_CorrAcc;
            route (route.LAST).t_Codekind := v_pmprop_in.t_CorrCodeKind;
            route (route.LAST).t_CodeValue := v_pmprop_in.t_CorrCode;

            IF v_pmprop_in.t_DebetCredit = PRT_CREDIT
            THEN
               route (route.LAST).t_Name := v_pmrmprop.t_ReceiverCorrBankName;
            ELSE
               route (route.LAST).t_Name := v_pmrmprop.t_PayerCorrBankName;
            END IF;

            route (route.LAST).t_InstructionAbonent := SET_CHAR;
            route (route.LAST).t_Sort := v_Sort;
            v_Sort := v_Sort - 1;
            
            route (route.LAST) := AddFillPMROUTE (route (route.LAST));            
         END IF;
         
         route.EXTEND ();
         route (route.LAST).t_ObjID := v_pmpaym.t_PaymentID;
         route (route.LAST).t_ObjKind := PM_COMMON.OBJTYPE_PAYMENT;
         route (route.LAST).t_Sort := v_Sort;
         
         IF v_pmprop_in.t_DebetCredit = PRT_CREDIT
            THEN
               route (route.LAST).t_PartyID := v_pmpaym.t_PayerBankID;
               route (route.LAST).t_OutAccount := v_pmrmprop.t_PayerCorrAccNostro;
               route (route.LAST).t_Name := v_pmrmprop.t_PayerBankName;
            ELSE
               route (route.LAST).t_PartyID := v_pmpaym.t_ReceiverBankID;
               route (route.LAST).t_OutAccount := v_pmrmprop.t_ReceiverCorrAccNostro;
               route (route.LAST).t_Name := v_pmrmprop.t_ReceiverBankName;
            END IF;
            
            route (route.LAST).t_Codekind := v_pmprop_in.t_CodeKind;
            route (route.LAST).t_CodeValue := v_pmprop_in.t_BankCode;
            route (route.LAST).t_CodeName := v_pmprop_in.t_CodeName;
         
      END IF;
   
         -- Исходящая ветка

      IF v_pmprop_out.t_Group = PAYMENTS_GROUP_EXTERNAL
      THEN
         v_Sort := 1;

         route.EXTEND ();
         route (route.LAST).t_ObjID := v_pmpaym.t_PaymentID;
         route (route.LAST).t_ObjKind := PM_COMMON.OBJTYPE_PAYMENT;
         route (route.LAST).t_Sort := v_Sort;

         IF v_pmprop_out.t_OurCorrID <> -1
         THEN
            route (route.LAST).t_PartyID := v_pmprop_out.t_OurCorrID;
            route (route.LAST).t_OutAccount := v_pmprop_out.t_OurCorrAcc;
            route (route.LAST).t_Codekind := v_pmprop_out.t_OurCorrCodeKind;
            route (route.LAST).t_CodeValue := v_pmprop_out.t_OurCorrCode;

            IF v_pmprop_out.t_DebetCredit = PRT_CREDIT
            THEN
               route (route.LAST).t_Name :=
                  v_pmrmprop.t_OurReceiverCorrName;
            ELSE
               route (route.LAST).t_Name := v_pmrmprop.t_OurPayerCorrName;
            END IF;
         ELSE
            BEGIN
               SELECT corschem.t_CorrID
                 INTO route (route.LAST).t_PartyID
                 FROM ddp_dep_dbt dp_dep
                      INNER JOIN dcorschem_dbt corschem
                         ON dp_dep.t_code = corschem.t_department
                WHERE     corschem.t_FIID = v_pmprop_out.t_PayFIID
                      AND corschem.t_Number = v_pmprop_out.t_corschem;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  NULL;
            END;
         END IF;

         v_Sort := v_Sort + 1;

         route (route.LAST) := AddFillPMROUTE (route (route.LAST));

         FOR p IN routes_db_out
         LOOP
            route.EXTEND ();
            route (route.LAST) := p;
            route (route.LAST).t_Sort := v_Sort;
            v_Sort := v_Sort + 1;
         END LOOP;

         IF v_pmprop_out.t_CorrID <> -1
         THEN
            route.EXTEND ();
            route (route.LAST).t_PartyID := v_pmprop_out.t_CorrID;
            route (route.LAST).t_OutAccount := v_pmprop_out.t_CorrAcc;
            route (route.LAST).t_Codekind := v_pmprop_out.t_CorrCodeKind;
            route (route.LAST).t_CodeValue := v_pmprop_out.t_CorrCode;

            IF v_pmprop_out.t_DebetCredit = PRT_CREDIT
            THEN
               route (route.LAST).t_Name := v_pmrmprop.t_ReceiverCorrBankName;
            ELSE
               route (route.LAST).t_Name := v_pmrmprop.t_PayerCorrBankName;
            END IF;

            route (route.LAST).t_InstructionAbonent := SET_CHAR;
            route (route.LAST).t_Sort := v_Sort;
            v_Sort := v_Sort + 1;
            
            route (route.LAST) := AddFillPMROUTE (route (route.LAST));
         END IF;
         
         route.EXTEND ();
         route (route.LAST).t_ObjID := v_pmpaym.t_PaymentID;
         route (route.LAST).t_ObjKind := PM_COMMON.OBJTYPE_PAYMENT;
         route (route.LAST).t_Sort := v_Sort;
         
         IF v_pmprop_out.t_DebetCredit = PRT_CREDIT
            THEN
               route (route.LAST).t_PartyID := v_pmpaym.t_ReceiverBankID;
               route (route.LAST).t_OutAccount := v_pmrmprop.t_ReceiverCorrAccNostro;
               route (route.LAST).t_Name := v_pmrmprop.t_ReceiverBankName;
            ELSE
               route (route.LAST).t_PartyID := v_pmpaym.t_PayerBankID;
               route (route.LAST).t_OutAccount := v_pmrmprop.t_PayerCorrAccNostro;
               route (route.LAST).t_Name := v_pmrmprop.t_PayerBankName;
            END IF;
            
            route (route.LAST).t_Codekind := v_pmprop_out.t_CodeKind;
            route (route.LAST).t_CodeValue := v_pmprop_out.t_BankCode;
            route (route.LAST).t_CodeName := v_pmprop_out.t_CodeName;
         
      END IF;
   END IF;

   RETURN route;
END GetRouteFromPayment;
   
BEGIN

  routes := GetRouteFromPayment(p_PaymentID);

   SELECT pmpaym.t_PayerBankID,
          pmprop_in.t_Codekind,
          pmprop_in.t_BankCode,
          pmrmprop.t_payerbankname,
          pmprop_in.t_codename,
          pmrmprop.t_PayerCorrAccNostro,
          pmpaym.t_ReceiverBankID,
          pmprop_out.t_Codekind,
          pmprop_out.t_BankCode,
          pmrmprop.t_receiverbankname,
          pmprop_out.t_codename,
          pmrmprop.t_ReceiverCorrAccNostro
     INTO PayerBank.t_partyID,
          PayerBank.t_Codekind,
          PayerBank.t_CodeValue,
          PayerBank.t_Name,
          PayerBank.t_CodeName,
          PayerBank.t_Outaccount,
          ReceiverBank.t_partyID,
          ReceiverBank.t_Codekind,
          ReceiverBank.t_CodeValue,
          ReceiverBank.t_Name,
          ReceiverBank.t_CodeName,
          ReceiverBank.t_Inaccount
     FROM dpmpaym_dbt pmpaym
          INNER JOIN dpmprop_dbt pmprop_in
             ON     pmprop_in.t_PaymentID = pmpaym.t_PaymentID
                AND pmprop_in.t_isSender = PM_COMMON.SET_CHAR
          INNER JOIN dpmprop_dbt pmprop_out
             ON     pmprop_out.t_PaymentID = pmpaym.t_PaymentID
                AND pmprop_out.t_isSender = PM_COMMON.UNSET_CHAR
          INNER JOIN dpmrmprop_dbt pmrmprop
             ON pmrmprop.t_PaymentID = pmpaym.t_PaymentID
    WHERE pmpaym.t_PaymentID = p_PaymentID;
    
    BEGIN
      SELECT CASE WHEN pmpaym.t_DocKind = PM_COMMON.DLDOC_BANKPAYORDER THEN 1 ELSE 0 END,
             CASE WHEN pmpaym.t_PrimDocKind = PM_COMMON.WL_INDOC AND pmpaym.t_Purpose = PM_COMMON.PM_PURP_BANKPAYORDER THEN 1 ELSE 0 END
      INTO OutPayment, InPayment
             FROM dpmpaym_dbt pmpaym WHERE pmpaym.t_PaymentID = p_PaymentID;
    EXCEPTION
      WHEN NO_DATA_FOUND
        THEN
          NULL;
    END;
    
    IF OutPayment = 1 THEN     
       FOR i
          IN  routes.FIRST .. routes.LAST
       LOOP
          IF routes(i).t_Sort = 0
          THEN
             PartyZero.t_PartyID := routes(i).t_PartyID;
             cur_i := i; 
             EXIT;
          END IF;
       END LOOP;     

       IF PartyZero.t_PartyID <> 0 THEN
        SenderBank.t_PartyID := PartyZero.t_PartyID;
        
         BEGIN
          SELECT corschem.t_CorAccount INTO SenderBank.t_OutAccount
            FROM dcorschem_dbt corschem
         INNER JOIN dpmprop_dbt pmprop ON pmprop.t_Corschem = corschem.t_Number and pmprop.t_PayFIID = corschem.t_FIID
            WHERE pmprop.t_PaymentID = p_PaymentID AND pmprop.T_IsSender = PM_COMMON.UNSET_CHAR;
         EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
              NULL;
         END;
         
         BEGIN
          SELECT t.t_ShortName || ' ' || t.t_Adress INTO SenderBank.t_Name
            FROM (select party.t_ShortName t_ShortName, adress.t_adress t_Adress from dparty_dbt party
          INNER JOIN dadress_dbt adress ON party.t_PartyID = adress.t_PartyID
            WHERE party.t_PartyID = SenderBank.t_PartyID order by decode(adress.t_type, 2, -1, adress.t_type)) t where rownum = 1;
         EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
              NULL;
         END;
         
         SenderBank.t_CodeKind := PM_COMMON.PTCK_BIC;
         SenderBank.t_CodeValue := RSI_RSBPARTY.PT_GetPartyCode( SenderBank.t_PartyID, SenderBank.t_CodeKind );
         
         IF cur_i > routes.FIRST THEN          
          PrevInstructingAgent := routes( cur_i - 1 );
         END IF;
         
         IF cur_i + 1 < routes.LAST AND PM_COMMON.IsBankUBR ( routes( cur_i + 1 ).t_partyID ) THEN
          ExecutorBank := routes( cur_i + 2 );
         end if;
         
         IF ExecutorBank.t_PartyID IS NOT NULL AND cur_i + 2 < routes.LAST THEN
          IntermediaryBank := routes( cur_i + 3 );
         end if;
         
         IF ExecutorBank.t_PartyID = ReceiverBank.t_PartyID THEN
          ReceiverBank.t_PartyID := NULL;
         END if;
        
       END IF;

       
    ELSIF InPayment = 1 THEN
       FOR i
          IN  routes.FIRST .. routes.LAST
       LOOP
          IF routes(i).t_Sort = 0
          THEN
             PartyZero.t_PartyID := routes(i).t_PartyID;
             cur_i := i; 
             EXIT;
          END IF;
       END LOOP;
       
        IF PartyZero.t_PartyID <> 0 THEN
          ExecutorBank.t_PartyID := PartyZero.t_PartyID;
        
         BEGIN
          SELECT corschem.t_CorAccount INTO ExecutorBank.t_OutAccount
            FROM dcorschem_dbt corschem
         INNER JOIN dpmprop_dbt pmprop ON pmprop.t_Corschem = corschem.t_Number and pmprop.t_PayFIID = corschem.t_FIID
            WHERE pmprop.t_PaymentID = p_PaymentID AND pmprop.T_IsSender = PM_COMMON.SET_CHAR;
         EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
              NULL;
         END;
         
         BEGIN
          SELECT t.t_ShortName || ' ' || t.t_Adress INTO ExecutorBank.t_Name
            FROM (select party.t_ShortName t_ShortName, adress.t_adress t_Adress from dparty_dbt party
          INNER JOIN dadress_dbt adress ON party.t_PartyID = adress.t_PartyID
            WHERE party.t_PartyID = ExecutorBank.t_PartyID order by decode(adress.t_type, 2, -1, adress.t_type)) t where rownum = 1;  
         EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
              NULL;
         END;
         
         ExecutorBank.t_CodeKind := PM_COMMON.PTCK_BIC;
         ExecutorBank.t_CodeValue := RSI_RSBPARTY.PT_GetPartyCode( ExecutorBank.t_PartyID, ExecutorBank.t_CodeKind );
         
         IF cur_i > routes.FIRST THEN          
          IntermediaryBank := routes( cur_i - 1 );
         END IF;
         
         IF cur_i + 1 < routes.LAST AND PM_COMMON.IsBankUBR ( routes( cur_i + 1 ).t_partyID ) THEN
          SenderBank := routes( cur_i + 2 );
         end if;
         
         IF SenderBank.t_PartyID IS NOT NULL AND cur_i + 2 < routes.LAST THEN
          PrevInstructingAgent := routes( cur_i + 3 );
         end if;
         
         IF SenderBank.t_PartyID = PayerBank.t_PartyID THEN
          PayerBank.t_PartyID := NULL;
         END if;
        END IF;
      END IF;
      
    Participants := OperParticipants_t();

    IF PayerBank.t_partyID IS NOT NULL THEN
       Participants.EXTEND();
       Participants(Participants.LAST) := OperParticipant_t
        ( PayerBank.T_PARTYID,
          PayerBank.T_CODEKIND,
          PayerBank.T_CODEVALUE,
          PayerBank.T_NAME,
          PayerBank.T_CODENAME,
          Nvl(PayerBank.T_INACCOUNT, RSB_EMPTY_STRING),
          Nvl(PayerBank.T_OUTACCOUNT, RSB_EMPTY_STRING),
          PM_COMMON.PART_PAYERBANK);
    END IF;

    IF PrevInstructingAgent.t_partyID IS NOT NULL THEN
      Participants.EXTEND();
      Participants(Participants.LAST) := OperParticipant_t
        ( PrevInstructingAgent.T_PARTYID,
          PrevInstructingAgent.T_CODEKIND,
          PrevInstructingAgent.T_CODEVALUE,
          PrevInstructingAgent.T_NAME,
          PrevInstructingAgent.T_CODENAME,
          Nvl(PrevInstructingAgent.T_INACCOUNT, RSB_EMPTY_STRING),
          Nvl(PrevInstructingAgent.T_OUTACCOUNT, RSB_EMPTY_STRING),
          PM_COMMON.PART_PREVINSTRUCTINGAGENT);
    END IF;

    IF SenderBank.t_partyID IS NOT NULL THEN
      Participants.EXTEND();
      Participants(Participants.LAST) := OperParticipant_t
        ( SenderBank.T_PARTYID,
          SenderBank.T_CODEKIND,
          SenderBank.T_CODEVALUE,
          SenderBank.T_NAME,
          SenderBank.T_CODENAME,
          Nvl(SenderBank.T_INACCOUNT, RSB_EMPTY_STRING),
          Nvl(SenderBank.T_OUTACCOUNT, RSB_EMPTY_STRING),
          PM_COMMON.PART_SENDERBANK);
    END IF;

    IF ExecutorBank.t_partyID IS NOT NULL THEN
       Participants.EXTEND();
       Participants(Participants.LAST) := OperParticipant_t
        ( ExecutorBank.T_PARTYID,
          ExecutorBank.T_CODEKIND,
          ExecutorBank.T_CODEVALUE,
          ExecutorBank.T_NAME,
          ExecutorBank.T_CODENAME,
          Nvl(ExecutorBank.T_INACCOUNT, RSB_EMPTY_STRING),
          Nvl(ExecutorBank.T_OUTACCOUNT, RSB_EMPTY_STRING),
          PM_COMMON.PART_EXECUTORBANK);
    END IF;

    IF IntermediaryBank.t_partyID IS NOT NULL THEN
      Participants.EXTEND();
      Participants(Participants.LAST) := OperParticipant_t
        ( IntermediaryBank.T_PARTYID,
          IntermediaryBank.T_CODEKIND,
          IntermediaryBank.T_CODEVALUE,
          IntermediaryBank.T_NAME,
          IntermediaryBank.T_CODENAME,
          Nvl(IntermediaryBank.T_INACCOUNT, RSB_EMPTY_STRING),
          Nvl(IntermediaryBank.T_OUTACCOUNT, RSB_EMPTY_STRING),
          PM_COMMON.PART_INTERMEDIARYBANK);
    END IF;

    IF ReceiverBank.t_partyID IS NOT NULL THEN
       Participants.EXTEND();
       Participants(Participants.LAST) := OperParticipant_t
         ( ReceiverBank.T_PARTYID,
           ReceiverBank.T_CODEKIND,
           ReceiverBank.T_CODEVALUE,
           ReceiverBank.T_NAME,
           ReceiverBank.T_CODENAME,
           Nvl(ReceiverBank.T_INACCOUNT, RSB_EMPTY_STRING),
           Nvl(ReceiverBank.T_OUTACCOUNT, RSB_EMPTY_STRING),
           PM_COMMON.PART_RECEIVERBANK);
    END IF;
   

    RETURN Participants;
  END GetOperParticipantsFromRoute;

 function GetCodeOwnerName( p_CodeKind in integer, p_Code in varchar2, p_Date in date )
  return dparty_dbt.t_Name%type
  as
    v_tmpCodeOwner number(10);
    v_PartyName    dparty_dbt.t_Name%type;
    v_Dep          ddp_dep_dbt%rowtype;
  begin
    if GetCodeOwner(p_CodeKind, p_Code, v_tmpCodeOwner, v_Dep, p_Date) then
      v_PartyName := pm_scrhlp.GetPartyName( v_tmpCodeOwner );
    else
      v_PartyName := PM_COMMON.RSB_EMPTY_STRING;
    end if;
    return v_PartyName;
  end GetCodeOwnerName;

  function GetCodeOwnerCorAcc( p_CodeKind in integer, p_Code in varchar2, p_Date in date ) return varchar2
  as
    v_Dep          ddp_dep_dbt%rowtype;
    v_tmpCodeOwner number(10);
    v_CorAcc varchar2(25);
  begin

    if GetCodeOwner(p_CodeKind, p_Code, v_tmpCodeOwner, v_Dep, p_Date) then
      v_CorAcc := GetBankCorAcc( v_tmpCodeOwner );
    else
      v_CorAcc := PM_COMMON.RSB_EMPTY_STRING;
    end if;    
    return v_CorAcc;
  end GetCodeOwnerCorAcc;

  function GetPartyCodeOnDateWOutOwner( p_PartyID in number, p_CodeKind in number, p_Date in date )
  return varchar2
  as
    v_Code  dobjcode_dbt.t_Code%type;
    v_Owner number;
  begin
    v_Code := RSI_RSBPARTY.GetPartyCodeOnDate( p_PartyID, p_CodeKind, p_Date, v_Owner );
    return v_Code;
  end GetPartyCodeOnDateWOutOwner;


END PM_COMMON;
/
