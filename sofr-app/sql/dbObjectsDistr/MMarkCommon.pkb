create or replace package body MMarkCommon
is
    function GetQCategControl(
        ControlDate in date
    )
        return pls_integer
    is
        type Control_t is table of ddlresset_dbt%ROWTYPE;
        Control Control_t;
        idx pls_integer := 0;
        rv pls_integer := MMarkConst.MM_QCR_OTHER;
    begin
        select * bulk collect into Control from ddlresset_dbt
        where (t_module = 'J') and (t_accountbase = 1)
        and(t_setdate <= ControlDate) order by t_setdate desc;

        idx := Control.First;
        if (Control.Count = 0) then
            rv := MMarkConst.MM_QCR_OTHER; 
        elsif ((Control(idx).t_QualityCategory = 1) and (Control(idx).t_ReservePercent = 1)) then
            rv := MMarkConst.MM_QCR_DD;
        elsif ((Control(idx).t_QualityCategory = 1) and (Control(idx).t_ReservePercent = 2)) then
            rv := MMarkConst.MM_QCR_DC;
        elsif ((Control(idx).t_QualityCategory = 1) and (Control(idx).t_ReservePercent = 3)) then
            rv := MMarkConst.MM_QCR_DMIN;
        elsif ((Control(idx).t_QualityCategory = 1) and (Control(idx).t_ReservePercent = 4)) then
            rv := MMarkConst.MM_QCR_DMAX;
        elsif ((Control(idx).t_QualityCategory = 2) and (Control(idx).t_ReservePercent = 1)) then
            rv := MMarkConst.MM_QCR_CD;
        elsif ((Control(idx).t_QualityCategory = 2) and (Control(idx).t_ReservePercent = 2)) then
            rv := MMarkConst.MM_QCR_CC;
        elsif ((Control(idx).t_QualityCategory = 2) and (Control(idx).t_ReservePercent = 3)) then
            rv := MMarkConst.MM_QCR_CMIN;
        elsif ((Control(idx).t_QualityCategory = 2) and (Control(idx).t_ReservePercent = 4)) then
            rv := MMarkConst.MM_QCR_CMAX;
        elsif ((Control(idx).t_QualityCategory = 3) and (Control(idx).t_ReservePercent = 1)) then
            rv := MMarkConst.MM_QCR_RD;
        elsif ((Control(idx).t_QualityCategory = 3) and (Control(idx).t_ReservePercent = 2)) then
            rv := MMarkConst.MM_QCR_RC;
        elsif ((Control(idx).t_QualityCategory = 3) and (Control(idx).t_ReservePercent = 3)) then
            rv := MMarkConst.MM_QCR_RMIN;
        elsif ((Control(idx).t_QualityCategory = 3) and (Control(idx).t_ReservePercent = 4)) then
            rv := MMarkConst.MM_QCR_RMAX;
        else
            rv := MMarkConst.MM_QCR_OTHER;
        end if;

        return rv;

        exception
          when others then
              return MMarkConst.MM_QCR_OTHER;
    end GetQCategControl;

    function CheckQualityCategory(
        QualityCategory in out ddlreslnk_dbt.t_qualitycategory%TYPE,
        ReservePercent  in out ddlreslnk_dbt.t_reservepercent%TYPE,
        ChangeMode      in     integer default 0,
        CDate           in     date default to_date ('0001-01-01', 'yyyy-mm-dd')
    )
        return number
    is
        Control pls_integer;
        rv number := MMarkConst.MMERR_NONE;
    begin
        case 
          when QualityCategory = 1 then
              if (ReservePercent <> 0) then
                  rv := MMarkConst.MMERR_RESERVE_BAD_PERCENT; 
              end if;
          when QualityCategory = 2 then
              if ((ReservePercent < 1)or(ReservePercent > 20)) then
                  rv := MMarkConst.MMERR_RESERVE_BAD_PERCENT; 
              end if;
          when QualityCategory = 3 then
              if ((ReservePercent < 21)or(ReservePercent > 50)) then
                  rv := MMarkConst.MMERR_RESERVE_BAD_PERCENT; 
              end if;
          when QualityCategory = 4 then
              if ((ReservePercent < 51)or(ReservePercent > 100)) then
                  rv := MMarkConst.MMERR_RESERVE_BAD_PERCENT; 
              end if;
          when QualityCategory = 5 then
              if (ReservePercent <> 100) then
                  rv := MMarkConst.MMERR_RESERVE_BAD_PERCENT; 
              end if;
          else 
              rv := MMarkConst.MMERR_RESERVE_BAD_PERCENT;
        end case;

        if ((ChangeMode = 1)and(rv <> MMarkConst.MMERR_NONE)) then -- режим вызывается, когда надо привести в соответствие КК и % резерва 
            Control := GetQCategControl(CDate);
            case
              when (Control = MMarkConst.MM_QCR_DD)   or
                   (Control = MMarkConst.MM_QCR_DMIN) or
                   (Control = MMarkConst.MM_QCR_CD)   or
                   (Control = MMarkConst.MM_QCR_CC)   or
                   (Control = MMarkConst.MM_QCR_CMIN) then
                   case
                    when QualityCategory = 1 then
                        ReservePercent := 0;
                    when QualityCategory = 2 then
                        ReservePercent := 1;
                    when QualityCategory = 3 then
                        ReservePercent := 21;
                    when QualityCategory = 4 then
                        ReservePercent := 51;
                    when QualityCategory = 5 then
                        ReservePercent := 100;
                    else
                        ReservePercent := 100;
                  end case;
              when (Control = MMarkConst.MM_QCR_DMAX) or
                   (Control = MMarkConst.MM_QCR_CMAX) then
                  case
                    when QualityCategory = 1 then
                        ReservePercent := 0;
                    when QualityCategory = 2 then
                        ReservePercent := 20;
                    when QualityCategory = 3 then
                        ReservePercent := 50;
                    when QualityCategory = 4 then
                       ReservePercent := 100;
                    when QualityCategory = 5 then
                        ReservePercent := 100;
                    else
                        ReservePercent := 100;
                  end case;
              when (Control = MMarkConst.MM_QCR_DC)   or
                   (Control = MMarkConst.MM_QCR_RD)   or
                   (Control = MMarkConst.MM_QCR_RC)   or
                   (Control = MMarkConst.MM_QCR_RMIN) or
                   (Control = MMarkConst.MM_QCR_RMAX) then
                  if (ReservePercent = 0) then
                      QualityCategory := 1;
                  elsif ((ReservePercent > 0)and(ReservePercent <= 20)) then
                      QualityCategory := 2;
                  elsif ((ReservePercent > 20)and(ReservePercent <= 50)) then
                      QualityCategory := 3;
                  elsif ((ReservePercent > 50)and(ReservePercent < 100)) then
                      QualityCategory := 4;
                  elsif (ReservePercent = 100) then
                      QualityCategory := 5;
                  end if;
              else
                   QualityCategory := 1;
                   ReservePercent := 0;
            end case;
            
            rv := MMarkConst.MMERR_NONE;
        elsif ((ChangeMode = 2)and(rv <> MMarkConst.MMERR_NONE)) then -- режим вызывается, когда надо привести КК в соответствие с % резерва
            Control := GetQCategControl(CDate);
            if (Control = MMarkConst.MM_QCR_RD)or(Control = MMarkConst.MM_QCR_RC)or
               (Control = MMarkConst.MM_QCR_RMIN)or(Control = MMarkConst.MM_QCR_RMAX) then
                if (ReservePercent = 0) then
                    QualityCategory := 1;
                elsif ((ReservePercent > 0)and(ReservePercent <= 20)) then
                    QualityCategory := 2;
                elsif ((ReservePercent > 20)and(ReservePercent <= 50)) then
                    QualityCategory := 3;
                elsif ((ReservePercent > 50)and(ReservePercent < 100)) then
                    QualityCategory := 4;
                elsif (ReservePercent = 100) then
                    QualityCategory := 5;
                end if;

                rv := MMarkConst.MMERR_NONE;
            end if;
        elsif ((ChangeMode = 3)and(rv <> MMarkConst.MMERR_NONE)) then -- режим вызывается, когда надо привести % резерва в соответствие с КК 
            Control := GetQCategControl(CDate);
            if (Control = MMarkConst.MM_QCR_DD)or(Control = MMarkConst.MM_QCR_DC)or(Control = MMarkConst.MM_QCR_DMIN)or
               (Control = MMarkConst.MM_QCR_CD)or(Control = MMarkConst.MM_QCR_CC)or(Control = MMarkConst.MM_QCR_CMIN)or
               (Control = MMarkConst.MM_QCR_RD)or(Control = MMarkConst.MM_QCR_RC)or(Control = MMarkConst.MM_QCR_RMIN)or
               (Control = MMarkConst.MM_QCR_OTHER) then
                case
                  when QualityCategory = 1 then
                      ReservePercent := 0;
                  when QualityCategory = 2 then
                      ReservePercent := 1;
                  when QualityCategory = 3 then
                      ReservePercent := 21;
                  when QualityCategory = 4 then
                      ReservePercent := 51;
                  when QualityCategory = 5 then
                      ReservePercent := 100;
                  else
                      ReservePercent := 100;
                end case;

                rv := MMarkConst.MMERR_NONE;
            elsif (Control = MMarkConst.MM_QCR_DMAX)or(Control = MMarkConst.MM_QCR_CMAX)or(Control = MMarkConst.MM_QCR_RMAX) then
                case
                  when QualityCategory = 1 then
                      ReservePercent := 0;
                  when QualityCategory = 2 then
                      ReservePercent := 20;
                  when QualityCategory = 3 then
                      ReservePercent := 50;
                  when QualityCategory = 4 then
                      ReservePercent := 100;
                  when QualityCategory = 5 then
                      ReservePercent := 100;
                  else
                      ReservePercent := 100;
                end case;
                
                rv := MMarkConst.MMERR_NONE;
            end if;
        end if;

        return rv;

        exception
          when others then
              QualityCategory := 1;
              ReservePercent := 0;
              return 1;
    end CheckQualityCategory;
    
    procedure GetRiskParm(
        DealID           in  number,
        PartyID          in  number,
        CDate            in  date,
        QualityCategory  out ddlreslnk_dbt.t_qualitycategory%TYPE,
        ReservePercent   out ddlreslnk_dbt.t_reservepercent%TYPE
    )
    is
        QC      ddlreslnk_dbt.t_qualitycategory%TYPE;
        RP      ddlreslnk_dbt.t_reservepercent%TYPE;
        Control pls_integer;
        chk     number := 0;
    begin
        Control := GetQCategControl(CDate);
        case
          when (Control = MMarkConst.MM_QCR_DD)or(Control = MMarkConst.MM_QCR_DC)or
               (Control = MMarkConst.MM_QCR_DMIN)or(Control = MMarkConst.MM_QCR_DMAX) then
              begin
                  select * into QC from
                  (
                    select rlnk.t_qualitycategory from ddlreslnk_dbt rlnk where
                    rlnk.t_type = 3 and rlnk.t_parentid = DealID
                    and rlnk.t_childid = -1 and rlnk.t_lnkdate <= CDate
                    order by rlnk.t_lnkdate desc, rlnk.t_id desc
                  ) where rownum = 1;

                  QualityCategory := QC;

                  exception
                    when NO_DATA_FOUND then
                        QualityCategory := 1;
              end;
          when (Control = MMarkConst.MM_QCR_CD)or(Control = MMarkConst.MM_QCR_CC)or
               (Control = MMarkConst.MM_QCR_CMIN)or(Control = MMarkConst.MM_QCR_CMAX) then
              begin
                  select t_attrid into QC from
                  (
                      select atc.t_attrid from dobjatcor_dbt atc where
                      atc.t_objecttype = 3 and atc.t_groupid = 13
                      and atc.t_object = to_char(PartyID, 'FM0999999999')
                      and atc.t_validfromdate <= CDate
                      and atc.t_validtodate >= CDate
                      and atc.t_general = 'X'
                      order by rownum desc
                  ) where rownum = 1;
                  
                  QualityCategory := QC;

                  exception
                    when NO_DATA_FOUND then
                        QualityCategory := 1;
              end;
          when (Control = MMarkConst.MM_QCR_RD)or(Control = MMarkConst.MM_QCR_RC)or
               (Control = MMarkConst.MM_QCR_RMIN)or(Control = MMarkConst.MM_QCR_RMAX) then
              QualityCategory := 1;
          else
              QualityCategory := 1;
        end case;

        case
          when (Control = MMarkConst.MM_QCR_DD)or(Control = MMarkConst.MM_QCR_CD)or(Control = MMarkConst.MM_QCR_RD) then
              begin
                  select * into RP from
                  (
                    select rlnk.t_reservepercent from ddlreslnk_dbt rlnk where
                    rlnk.t_type = 3 and rlnk.t_parentid = DealID
                    and rlnk.t_childid = -1 and rlnk.t_lnkdate <= CDate
                    order by rlnk.t_lnkdate desc, rlnk.t_id desc
                  ) where rownum = 1;

                  ReservePercent := RP;

                  exception
                    when NO_DATA_FOUND then
                        ReservePercent := 0;
              end;
              when (Control = MMarkConst.MM_QCR_DC)or(Control = MMarkConst.MM_QCR_RC)or(Control = MMarkConst.MM_QCR_CC) then
              begin
                  select MMark_UTL.castRawToDouble(nt.t_text) into RP from dnotetext_dbt nt where
                  nt.t_objecttype = 3 and nt.t_notekind = 3
                  and nt.t_documentid = to_char(PartyID, 'FM0999999999')
                  and nt.t_validtodate >= CDate
                  and nt.t_date <= CDate;

                  ReservePercent := RP;

                  exception
                    when NO_DATA_FOUND then
                        ReservePercent := 0;
              end;
          when (Control = MMarkConst.MM_QCR_DMIN)or(Control = MMarkConst.MM_QCR_CMIN)or(Control = MMarkConst.MM_QCR_RMIN) then
              ReservePercent  := 0;
          when (Control = MMarkConst.MM_QCR_DMAX)or(Control = MMarkConst.MM_QCR_CMAX)or(Control = MMarkConst.MM_QCR_RMAX) then
              ReservePercent  := 0;
          else
              ReservePercent  := 0;
        end case;

        chk := CheckQualityCategory(QualityCategory, ReservePercent, 1, CDate);

        exception
          when others then
              QualityCategory := 1;
              ReservePercent  := 0;
    end GetRiskParm;

    function GetQualityCategory(
        DealID           in  number,
        PartyID          in  number,
        CDate            in  date
    )
        return ddlreslnk_dbt.t_qualitycategory%TYPE
    is
        QualityCategory ddlreslnk_dbt.t_qualitycategory%TYPE := 1;
        ReservePercent ddlreslnk_dbt.t_reservepercent%TYPE := 0;
    begin
        GetRiskParm(DealID, PartyID, CDate, QualityCategory, ReservePercent);
        return QualityCategory;
    end GetQualityCategory;
    
    function GetReservePercent(
        DealID           in  number,
        PartyID          in  number,
        CDate            in  date
    )
        return ddlreslnk_dbt.t_reservepercent%TYPE
    is
        QualityCategory ddlreslnk_dbt.t_qualitycategory%TYPE := 1;
        ReservePercent ddlreslnk_dbt.t_reservepercent%TYPE := 0;
    begin
        GetRiskParm(DealID, PartyID, CDate, QualityCategory, ReservePercent);
        return ReservePercent;
    end GetReservePercent;

    procedure GetCurrentRiskParm(
        DealID           in  number,
        CDate            in  date,
        QualityCategory  out ddlreslnk_dbt.t_qualitycategory%TYPE,
        ReservePercent   out ddlreslnk_dbt.t_reservepercent%TYPE
    )
    is
        QC      ddlreslnk_dbt.t_qualitycategory%TYPE;
        RP      ddlreslnk_dbt.t_reservepercent%TYPE;
    begin
        begin
            select * into QC from
            (
              select rlnk.t_qualitycategory from ddlreslnk_dbt rlnk where
              rlnk.t_type = 3 and rlnk.t_parentid = DealID
              and rlnk.t_childid = -1 and rlnk.t_lnkdate <= CDate
              order by rlnk.t_lnkdate desc, rlnk.t_id desc
            ) where rownum = 1;

            QualityCategory := QC;

            exception
                when NO_DATA_FOUND then
                    QualityCategory := 1;
        end;
              
        begin
            select * into RP from
            (
              select rlnk.t_reservepercent from ddlreslnk_dbt rlnk where
              rlnk.t_type = 3 and rlnk.t_parentid = DealID
              and rlnk.t_childid = -1 and rlnk.t_lnkdate <= CDate
              order by rlnk.t_lnkdate desc, rlnk.t_id desc
            ) where rownum = 1;

            ReservePercent := RP;

            exception
                when NO_DATA_FOUND then
                    ReservePercent := 0;
        end;

        exception
          when others then
              QualityCategory := 1;
              ReservePercent  := 0;
    end GetCurrentRiskParm;
    
    function GetCurrentQualityCategory(
        DealID           in  number,
        CDate            in  date
    )
        return ddlreslnk_dbt.t_qualitycategory%TYPE
    is
        QualityCategory ddlreslnk_dbt.t_qualitycategory%TYPE := 1;
        ReservePercent ddlreslnk_dbt.t_reservepercent%TYPE := 0;
    begin
        GetCurrentRiskParm(DealID, CDate, QualityCategory, ReservePercent);
        return QualityCategory;
    end GetCurrentQualityCategory;

    function GetCurrentReservePercent(
        DealID           in  number,
        CDate            in  date
    )
        return ddlreslnk_dbt.t_reservepercent%TYPE
    is
        QualityCategory ddlreslnk_dbt.t_qualitycategory%TYPE := 1;
        ReservePercent ddlreslnk_dbt.t_reservepercent%TYPE := 0;
    begin
        GetCurrentRiskParm(DealID, CDate, QualityCategory, ReservePercent);
        return ReservePercent;
    end GetCurrentReservePercent;
    
    function GetAccountNumber(
        DealID           in  number,
        AccCat           in  VARCHAR2,
        CDate            in  date
    )
        return dmcaccdoc_dbt.t_account%TYPE
    is
        Acc dmcaccdoc_dbt.t_account%TYPE := '';
    begin
        select t_Account into Acc from 
        (
            select mcacc.* from dmcaccdoc_dbt mcacc, dmccateg_dbt categ 
            where 
            mcacc.t_DocKind = 102
            and mcacc.t_CatID = categ.t_ID
            and mcacc.t_DocID = DealID
            and categ.t_Code = AccCat
            and  
            CDate between mcacc.t_ActivateDate and decode(mcacc.t_DisablingDate, TO_DATE('01.01.0001', 'dd.mm.yyyy'), TO_DATE('31.12.9999', 'dd.mm.yyyy'), mcacc.t_DisablingDate) 
            order by mcacc.t_ID
        )
        where rownum = 1;
        
        return Acc;
        
        exception
          when others then return NULL;
    end GetAccountNumber;

    function GetAccountNumber(
        DealID           in  number,
        AccCat           in  VARCHAR2,
        BeginDate        in  date,
        EndDate          in  date,
        FiRole           in  number
    )
        return dmcaccdoc_dbt.t_account%TYPE
    is
        Acc dmcaccdoc_dbt.t_account%TYPE := '';
    begin
        select t_Account into Acc from
        (
            select mcacc.* from dmcaccdoc_dbt mcacc, dmccateg_dbt categ
            where
            mcacc.t_DocKind = MMarkConst.DL_IBCDOC
            and mcacc.t_CatID = categ.t_ID
            and mcacc.t_DocID = DealID
            and categ.t_Code = AccCat
            and (FiRole = -1 or mcacc.t_FiRole = FiRole)
            and
            (
                BeginDate between mcacc.t_ActivateDate and decode(mcacc.t_DisablingDate, TO_DATE('01.01.0001', 'dd.mm.yyyy'), TO_DATE('31.12.9999', 'dd.mm.yyyy'), mcacc.t_DisablingDate)
                OR
                EndDate between mcacc.t_ActivateDate and decode(mcacc.t_DisablingDate, TO_DATE('01.01.0001', 'dd.mm.yyyy'), TO_DATE('31.12.9999', 'dd.mm.yyyy'), mcacc.t_DisablingDate)
                OR
                mcacc.t_ActivateDate between BeginDate and EndDate
                OR
                mcacc.t_DisablingDate between BeginDate and EndDate
            )
            order by mcacc.t_ID desc
        )
        where rownum = 1;

        return Acc;

        exception
          when others then return NULL;
    end GetAccountNumber;

    function GetTickStatus(
        DealID           in  number,
        Status           out VARCHAR2
    )
        return number
    is
        StepDate      DATE;
        PaymCountExec INTEGER := 0;
        PaymCount     INTEGER := 0;
        SendMes       INTEGER := 0;
        ReceivedMes   INTEGER := 0;
        stat          INTEGER := 0;
    begin
        if (DealID = 0)
        then
            Status := 'Ввод'; -- выводится при вводе сделки в систему
        else
        
            Status := 'Введена'; -- выводится при вводе сделки в систему
            stat := GetDateStepBySymbol(DealID, 'с', StepDate);
            if (stat = 0) then
                Status := 'Сделка пролонгирована';
                return 0;
            end if;
            
            stat := GetDateStepBySymbol(DealID, 'З', StepDate);
            if (stat = 0) then
                Status := 'Открыты счета';
            end if;
            
            select nvl(count(1), 0) into PaymCountExec from ddllnkmes_dbt where t_id = DealID and t_state in ('Отправлено', 'Доставлено адресату', 'Принято');
            if (PaymCountExec > 0) then
                Status := 'Направлено подтверждение';
            end if;
            
            select nvl(count(1), 0) into PaymCountExec from ddllnkmes_dbt where t_id = DealID and t_state = 'Обработано';
            if (PaymCountExec > 0) then
                Status := 'Сквитовано подтверждение';
            end if;
            

            select count(1) into PaymCount     from dpmpaym_dbt where t_dockind=102 and t_purpose=9 and t_documentid=DealID;
            select count(1) into PaymCountExec from dpmpaym_dbt where t_dockind=102 and t_purpose=9 and t_documentid=DealID and t_paymstatus=32000;
            if (PaymCountExec = PaymCount) then
                 Status := 'Исполнен платеж по основному долгу';
            end if;
            
            stat := GetDateStepBySymbol(DealID, 'н', StepDate);
            if (stat = 0) then
                Status := 'Начислены %%';
            end if;
            
            select count(1) into PaymCount     from dpmpaym_dbt where t_dockind=102 and t_purpose=10 and t_documentid=DealID;
            select count(1) into PaymCountExec from dpmpaym_dbt where t_dockind=102 and t_purpose=10 and t_documentid=DealID and t_paymstatus=32000;
            if (PaymCountExec = PaymCount) then
                 Status := 'Исполнен платеж по основному долгу';
            end if;
            
            select count(1) into PaymCount     from dpmpaym_dbt where t_dockind=102 and t_purpose=11 and t_documentid=DealID;
            select count(1) into PaymCountExec from dpmpaym_dbt where t_dockind=102 and t_purpose=11 and t_documentid=DealID and t_paymstatus=32000;
            if (PaymCountExec = PaymCount) then
                 Status := 'Исполнен платеж по погашению %%';
            end if;
            
            select count(1) into PaymCount     from dpmpaym_dbt where t_dockind=102 and t_documentid=DealID;
            select count(1) into PaymCountExec from dpmpaym_dbt where t_dockind=102 and t_documentid=DealID and t_paymstatus=32000;
            if (PaymCountExec = PaymCount) then
                 Status := 'Расчеты завершены';
            end if;
            
        end if;

        return 0;

        exception
            when others then return -1;
    end GetTickStatus;

  -- получание даты выполнения последнего шага по символу
  FUNCTION GetDateStepBySymbol(ObjN     IN NUMBER,
                               Symb     IN CHAR,
                               StepDate OUT DATE)
    RETURN NUMBER
  IS
    sdate DATE;
  BEGIN
    SELECT MAX(t_fact_date) INTO sdate FROM doprstep_dbt
      INNER JOIN doproper_dbt ON doproper_dbt.t_id_operation = doprstep_dbt.t_id_operation
      WHERE t_documentid = lpad(ObjN,34,'0') AND doprstep_dbt.t_dockind = 102 AND t_isexecute = 'X' AND t_symbol = Symb;
      
    IF (sdate IS NULL) THEN
      RETURN -1;
    END IF;
    
    StepDate := sdate;

    RETURN 0;
    EXCEPTION WHEN NO_DATA_FOUND THEN RETURN -1;
  END GetDateStepBySymbol;

  FUNCTION getRest(chapter IN NUMBER, fiId IN NUMBER, account IN VARCHAR2, reportDate IN DATE)
    RETURN NUMBER
    AS
        v_rest NUMBER;
    BEGIN
        SELECT rd.t_rest
          INTO v_rest
          FROM drestdate_dbt rd,
               daccount_dbt  acc
         WHERE acc.t_chapter       = chapter
           AND acc.t_code_currency = fiId
           AND acc.t_account       = account
           AND rd.t_accountId      = acc.t_accountId
           AND rd.t_restCurrency   = fiId
           AND rd.t_restDate IN (SELECT MAX (t_restDate)
                                   FROM drestdate_dbt r
                                  WHERE r.t_accountId     = acc.t_accountId
                                    AND r.t_restCurrency  = fiId
                                    AND r.t_restDate <= reportDate);

        RETURN v_rest;

        EXCEPTION
            WHEN NO_DATA_FOUND
                THEN RETURN 0;
  END;

  function GetNoteTextMoney( v_ObjectType IN NUMBER, v_ObjectID IN NUMBER, v_NoteKind IN NUMBER, v_Date IN DATE)
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
                   t_NoteKind = v_NoteKind and
                   v_Date between t_Date and t_ValidToDate
             order by t_Date desc)
      where ROWNUM = 1;

    exception
      when NO_DATA_FOUND then return NULL;
      when OTHERS then return NULL;
    end;
    return rsb_struct.getMoney(v_Text);
  end;

  function GetNoteTextString( v_ObjectType IN NUMBER, v_ObjectID IN NUMBER, v_NoteKind IN NUMBER, v_Date IN DATE)
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
                   t_NoteKind = v_NoteKind and
                   v_Date between t_Date and t_ValidToDate
             order by t_Date desc)
      where ROWNUM = 1;

    exception
      when NO_DATA_FOUND then return NULL;
      when OTHERS then return NULL;
    end;
    return rsb_struct.getString(v_Text);
  end;
  
  FUNCTION GetAccRest(objid    IN NUMBER,
                      objn     IN NUMBER,
                      opdate   IN DATE,
                      cat      IN VARCHAR2,
                      chapter  IN NUMBER,
                      fiId     IN NUMBER,
                      withMark IN BOOLEAN)
                          
    RETURN NUMBER
  IS 
    res    NUMBER   := 0;
    accnum VARCHAR2(25) := '';
  BEGIN
    accnum := GetAccountNumber(objn, cat, opdate);
    IF (accnum IS NULL) THEN
      RETURN 0;
    END IF;
    res := GetRest(chapter, fiId, accnum, opdate);
    IF (not withMark) THEN
      res := ABS(res);
    END IF;
    
    RETURN res;
    
  END GetAccRest;

  FUNCTION GetPaymSum(operationid IN NUMBER,
                      stepid      IN NUMBER,
                      opdate      In DATE,
                      pm_purpose  IN INTEGER)
    RETURN NUMBER
  IS
    PmSum NUMBER := 0;
  BEGIN
    SELECT DISTINCT t_futurepayeramount INTO PmSum FROM doprdocs_dbt oprdocs
      INNER JOIN dpmdocs_dbt pmdocs ON oprdocs.t_acctrnid = pmdocs.t_acctrnid
      INNER JOIN dpmpaym_dbt pmpaym ON pmdocs.t_paymentid = pmpaym.t_paymentid
      WHERE oprdocs.t_id_operation = operationid 
        AND oprdocs.t_id_step = stepid 
        AND pmpaym.t_paymstatus = 32000
        AND pmpaym.t_purpose = pm_purpose
        AND pmpaym.t_valuedate <= opdate;
        
    RETURN PmSum;
    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN NULL;
      WHEN OTHERS THEN RETURN NULL;                
  END GetPaymSum;
  
  FUNCTION GetLastStepBeforeStep(operationid IN NUMBER,
                                 stepid      IN NUMBER,
                                 stepsymb    IN CHAR)
    RETURN NUMBER
  IS
    max_id_step NUMBER := 0;
  BEGIN
    SELECT MAX(t_id_step) INTO max_id_step FROM doprstep_dbt 
      WHERE t_id_operation = operationid 
        AND t_id_step <= stepid 
        AND t_symbol = stepsymb;
        
    RETURN max_id_step;
    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN RETURN NULL;
      WHEN OTHERS THEN RETURN NULL;                
  END GetLastStepBeforeStep;

  FUNCTION GetPastPaymSum(objid    IN NUMBER,
                          objn     IN NUMBER,
                          calcdate IN DATE)
    RETURN NUMBER
  IS
    pmsum NUMBER := 0;
  BEGIN
    SELECT SUM(CASE WHEN (pm.t_PartPaymRestAmountMain < pm.t_Amount AND pm.t_PartPaymRestAmountMain > 0) THEN pm.t_PartPaymRestAmountMain ELSE pm.t_Amount END)
    INTO pmsum
    FROM dpmpaym_dbt pm
    WHERE t_dockind = objid AND t_documentid = objn AND pm.t_PaymStatus = 32000 AND t_valuedate = calcdate AND t_purpose NOT IN (9, 83, 11) /*Предоставление ОД, затраты, проценты*/;
    
    IF (pmsum IS NULL) THEN
      RETURN 0;
    END IF; 
    
    return pmsum;
    
  EXCEPTION
      when NO_DATA_FOUND then return 0;
      when OTHERS then return -1;
  
  END GetPastPaymSum;

FUNCTION GetCClaimsString(dealid IN NUMBER)
    RETURN VARCHAR2
IS
    CClaimsStr VARCHAR2 (1000) := LoansConst.STR_EMPTY; 
BEGIN
    select listagg(t_dealcode, ', ') within group (order by t_dealcode) 
      into CClaimsStr
      from ddl_tick_dbt tick
      inner join dmmrepreq_dbt repreq on tick.t_DealID = repreq.t_ClaimID
      where repreq.t_DealID = dealid;
    
    RETURN CClaimsStr;
END GetCClaimsString;

FUNCTION IsBissextileYear (p_Date IN DATE) RETURN NUMBER
    IS
    BEGIN

        IF (EXTRACT( DAY FROM(LAST_DAY(ADD_MONTHS(p_Date,(2 - EXTRACT( MONTH FROM(p_Date))) )))) = 29)
        THEN
            RETURN 1;
        ELSE
            RETURN 0;
        END IF;
END;

FUNCTION CalcPercentEx2(
     objid      IN NUMBER,
     objn       IN NUMBER,
     DateFrom   IN DATE,
     DateTo     IN DATE,
     Rest       IN NUMBER,
     Rate       IN NUMBER,
     Calendar   IN INTEGER,
     CntEndDate IN DATE
   )
   RETURN NUMBER
   IS
    Res          NUMBER  := 0;
    DaysInPeriod NUMBER  := 0;
    DaysInYear   NUMBER  := 0;
    GraphDate1   DATE := DateFrom;
    GraphDate2   DATE := DateTo;
    PrevDate     DATE;
    Year1        NUMBER(5);
    Year2        NUMBER(5);
    iEndYearDate DATE;

BEGIN
    Year1 := EXTRACT( YEAR FROM(GraphDate1));
    Year2 := EXTRACT( YEAR FROM(GraphDate2));
    IF (
        (Year1 != Year2) AND
        (GraphDate1 < GraphDate2)
        )
    THEN -- Если период если период больше года
    LOOP
        iEndYearDate := LAST_DAY(ADD_MONTHS(GraphDate1,(12 - EXTRACT( MONTH FROM(GraphDate1))) ));
        IF (iEndYearDate > GraphDate2)
        THEN
            iEndYearDate := GraphDate2;
        END IF;

        DaysInYear := RSI_RSB_PERCENT.GetDaysInYearByCalendar(Calendar, GraphDate1);
        DaysInPeriod := RSI_RSB_PERCENT.GetDaysInPeriodByCalendar(Calendar, GraphDate1, iEndYearDate, CntEndDate);

        Res := Res + (Rest * DaysInPeriod * (Rate/100)/DaysInYear);

        GraphDate1 := iEndYearDate + 1;

        EXIT WHEN iEndYearDate = GraphDate2;
    END LOOP;
    ELSE -- Период период не более одного года
        DaysInYear := RSI_RSB_PERCENT.GetDaysInYearByCalendar(Calendar, GraphDate1);
        DaysInPeriod := RSI_RSB_PERCENT.GetDaysInPeriodByCalendar(Calendar, GraphDate1, GraphDate2, CntEndDate);

        Res := Res + Rest * DaysInPeriod * (Rate/100)/DaysInYear;
    END IF;
    RETURN Res;
END CalcPercentEx2;

FUNCTION CalcPercent(
     objid    IN NUMBER,
     objn     IN NUMBER,
     DateFrom IN DATE,
     DateTo   IN DATE
   )
   RETURN NUMBER
   IS
    Res          NUMBER  := 0;
    CurRest      NUMBER  := 0;
    ind          INTEGER := 0;
    GraphDate1   DATE := DateFrom;
    GraphDate2   DATE := DateTo;
    PrevDate     DATE;
    GenagrID     NUMBER  := 0;
    InclFirstDay CHAR(1) := CNST.UNSET_CHAR;
    LEG          DDL_LEG_DBT%ROWTYPE;
    TICK         DDL_TICK_DBT%ROWTYPE;
    
    Rate         FLOAT := 0;
    RateDateTo   DATE := DateTo;
    RateDateFrom DATE;

    CURSOR c_planrest IS
        SELECT t_ValueDate As t_ValueDate, DECODE(t_futurepayeramount, 0, t_amount, t_futurepayeramount) AS t_Amount FROM dpmpaym_dbt graph
        WHERE t_dockind = objid AND t_documentid = objn AND t_purpose = 10 AND t_ValueDate >= DateFrom
        ORDER BY t_ValueDate ASC;
        
    CURSOR c_rateval IS
        SELECT prcrateval.t_RateDate, prcrateval.t_Rate,
            (SELECT MIN(t_RateDate-1) FROM dprcrateval_dbt prcrateval2 WHERE prccntr.t_RateID =  prcrateval2.t_RateID AND prcrateval2.t_RateDate > prcrateval.t_RateDate) AS t_NextRateDate
         FROM dprcrateval_dbt prcrateval
        INNER JOIN dprccontract_dbt prccntr ON prccntr.t_RateID = prcrateval.t_RateID
        INNER JOIN ddl_tick_dbt tick ON tick.t_DealCode = t_ObjectID
              WHERE t_ContractType = 7 AND t_ObjectType = 103 AND t_DealID = objn AND prcrateval.t_RateDate <= DateTo
        ORDER BY prcrateval.t_RateDate DESC;
BEGIN
    SELECT * INTO LEG FROM DDL_LEG_DBT WHERE t_DealID = objn AND T_LEGKIND = 0 AND T_LEGID = 1;
    SELECT NVL(t_GenAgrID, 0) INTO genagrID FROM DDL_TICK_DBT WHERE t_DealID = objn;
    IF (genagrID > 0) THEN
      SELECT NVL(t_IncludeDay, CNST.UNSET_CHAR) INTO InclFirstDay FROM DDL_GENAGR_DBT WHERE t_GenagrID = genagrID;
    END IF;
    
    FOR rec IN c_planrest
        LOOP
            IF (ind > 0) THEN
                GraphDate1 := PrevDate;
            END IF;

            GraphDate2 := LEAST(DateTo, rec.t_ValueDate);
 
            -- не начислять проценты в первый день периода
            IF (NOT (InclFirstDay = CNST.SET_CHAR AND ind = 0)) THEN
               GraphDate1 := GraphDate1 + 1;
            END IF;
            
            if (GraphDate2 >= GraphDate1) then
              SELECT SUM(DECODE(t_futurepayeramount, 0, t_amount, t_futurepayeramount)) INTO CurRest FROM dpmpaym_dbt graph
                   WHERE t_dockind = objid AND t_documentid = objn AND t_purpose = 10 AND t_ValueDate > GraphDate1 -1 ;
                   
              FOR raterec IN c_rateval
              LOOP
                   Rate := raterec.t_Rate;
                   RateDateTo := raterec.t_NextRateDate;
                   IF (RateDateTo IS NULL OR RateDateTo > GraphDate2) THEN 
                      RateDateTo := GraphDate2;
                   END IF;
                   RateDateFrom := raterec.t_RateDate;
                   IF (RateDateFrom < GraphDate1) THEN
                       RateDateFrom := GraphDate1;
                   END IF;
                   IF (RateDateFrom <= RateDateTo) THEN
                      res := res + CalcPercentEx2(objid, objn, RateDateFrom, RateDateTo, CurRest, Rate, LEG.t_Basis, LEG.t_Maturity) ;
                   END IF;
               END LOOP;    
            end if;

            PrevDate := rec.t_ValueDate;
            ind := ind + 1;
        END LOOP;
        
        IF (ind = 0 AND LEG.t_PlanMaturity != to_date ('0001-01-01', 'yyyy-mm-dd')) THEN -- на сделке нет платежей по погашению ОД, для сделок до востребования
          -- не начислять проценты в первый день периода
           IF (NOT (InclFirstDay = CNST.SET_CHAR AND ind = 0)) THEN
               GraphDate1 := GraphDate1 + 1;
           END IF;
            
           FOR raterec IN c_rateval
           LOOP
               Rate := raterec.t_Rate;
               RateDateTo := raterec.t_NextRateDate;
               IF (RateDateTo IS NULL OR RateDateTo > GraphDate2) THEN 
                  RateDateTo := GraphDate2;
               END IF;
               RateDateFrom := raterec.t_RateDate;
               IF (RateDateFrom < GraphDate1) THEN
                    RateDateFrom :=  GraphDate1;
               END IF;
               IF (RateDateFrom <= RateDateTo) THEN
                  res := res + CalcPercentEx2(objid, objn, RateDateFrom, RateDateTo, LEG.t_Principal, Rate, LEG.t_Basis, LEG.t_Maturity) ;
               END IF;
            END LOOP;    
        END IF;
    RETURN ROUND(Res, 2);
END CalcPercent;

end MMarkCommon;
/
