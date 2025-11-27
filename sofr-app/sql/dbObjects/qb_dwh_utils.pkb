create or replace package body qb_dwh_utils is

 ------------------------------------------------------
 --Функция обработки на пустоту для начальных дат
 ------------------------------------------------------
 function NvlBegDate (in_Date in Date) return date is
 out_Result date := in_Date;
 begin
 if out_result is null or out_result = to_date('01.01.0001','dd.mm.yyyy') then
 out_Result := DT_BEGIN;
 end if;
 Return out_Result;
 end;

 ------------------------------------------------------
 --Функция обработки на пустоту для конечных дат
 ------------------------------------------------------
 function NvlEndDate (in_Date in Date) return date is
 out_Result date := in_Date;
 begin
 if out_result is null or out_result = to_date('01.01.0001','dd.mm.yyyy') then
 out_Result := DT_END;
 end if;
 Return out_Result;
 end;

 ------------------------------------------------------
 -- Функция преобразования даты в строку перед записью в DWH
 ------------------------------------------------------
 function DateToChar (in_Date in Date) return varchar2 PARALLEL_ENABLE  is
 begin
 return to_char(in_Date, 'dd-mm-yyyy');
 end;

 ------------------------------------------------------
 -- Функция преобразования даты в строку со временем перед записью в DWH
 ------------------------------------------------------
 function DateTimeToChar (in_Date in Date) return varchar2 PARALLEL_ENABLE is
 begin
 return to_char(in_Date, 'dd-mm-yyyy hh24:mi:ss');
 end;

 ------------------------------------------------------
 -- Функция преобразования числа в строку перед записью в DWH
 ------------------------------------------------------
 function NumberToChar (in_Number in number, in_Lingth in number default 4, in_RoundType in number default 1) return varchar2 is
 vTMP number;
 vSTR varchar2(50);
 v_Lingth number;
 out_Result varchar2(100);
 begin
 if in_Lingth < 0 then
   if instr(to_char(in_Number),'.') > 0 then
     v_Lingth := length(substr(to_char(in_Number),instr(to_char(in_Number),'.')+1));
   else
     v_Lingth :=0;
   end if;
   --Raise_application_error(-20000, instr(to_char(in_Number),'.'));
 else
   v_Lingth := in_Lingth;
 end if;
   if in_RoundType = 0 then
      vTMP := trunc(in_Number, v_Lingth);
   else
      vTMP := round(in_Number, v_Lingth);
   end if;

 if in_Lingth > 0 then
 vSTR := '.' || LPAD('0', in_Lingth, '0');
 elsif in_Lingth < 0 and v_Lingth > 0 then
 vSTR :=  RPAD('.', length(substr(to_char(in_Number),instr(to_char(in_Number),'.'))), '0');
 --Raise_application_error(-20000, v_Lingth);
 end if;
 out_Result :=trim( to_char(vTMP,'9999999999999999999999999999999999999990'|| vSTR));
 return trim(out_Result);
 end;


 ------------------------------------------------------
 --Функция Возвращает дату старта ЦХД из реестра</b></font>
 ------------------------------------------------------
 function GetDWHMigrationDate return date is
 out_result date;
 begin
 out_result := nvl(to_date( RSB_Common.GetRegStrValue('MMARK\ДАТА СТАРТА ЦХД'),'dd.mm.yyyy'),qb_dwh_utils.DT_BEGIN);
 return out_result;
 end;
 ------------------------------------------------------
 -- Функция Возвращает подразделение для сделки
  ------------------------------------------------------
  function GetDepartmentByDealID(in_Kind_DocID number, in_DocID number) return number is
    out_Result number;
  begin
    if in_Kind_DocID in (102,208) then

      select t.t_department
        into out_Result
        from ddl_tick_dbt t
       where t.t_bofficekind = in_Kind_DocID
             and t.t_dealid = in_DocID;

    elsif in_Kind_DocID in (4611) then
      select genagr.t_department
        into out_Result
        from ddl_genagr_dbt genagr
       where genagr.t_dockind = in_Kind_DocID
             and genagr.t_genagrid = in_DocID;

    elsif in_Kind_DocID in (126) then
      select dl_order.t_department
        into out_Result
        from ddl_order_dbt dl_order
       where dl_order.t_dockind = in_Kind_DocID
             and dl_order.t_contractid = in_DocID;
    end if;
    return out_Result;
  exception
    when others then
        return null;
  end;

   -- Получить запись примечания
   function Get_Note(in_Obj_Tp  in number,
                     in_Obj_Id  in varchar2,
                     in_Note_Id in Dnotetext_Dbt.t_Notekind%TYPE,
                     in_Date    in date default null) return Dnotetext_Dbt%ROWTYPE IS
      v_Ret Dnotetext_Dbt%ROWTYPE;
   begin
      SELECT *
        INTO v_Ret
        FROM (SELECT *
                FROM Dnotetext_Dbt t
               WHERE t.t_Objecttype = in_Obj_Tp
                 AND t.t_Documentid = in_Obj_Id
                 AND t.t_Notekind = in_Note_Id
                 AND t.t_Date <= Nvl(in_Date, Rsbsessiondata.Curdate)
                 AND t.t_Validtodate >= Nvl(in_Date, Rsbsessiondata.Curdate)
               ORDER BY t.t_Date DESC)
       WHERE Rownum = 1;
      RETURN v_Ret;
   exception
      when others then
         return null;
   end Get_Note;

   -- получить символьное значение примечания
   FUNCTION Get_Note_Chr(in_Obj_Tp  in number,
                         in_Obj_Id  in varchar2,
                         in_Note_Id in Dnotetext_Dbt.t_Notekind%TYPE,
                         in_Date    in date default null) RETURN VARCHAR2 IS
      r_Not Dnotetext_Dbt%ROWTYPE;
   BEGIN
      r_Not := Get_Note(in_Obj_Tp, in_Obj_Id, in_Note_Id, in_Date);
      IF r_Not.t_Id IS NULL THEN
         RETURN NULL;
      END IF;
      RETURN TRIM(Chr(0) FROM Rsb_Struct.Getstring(r_Not.t_Text));
   EXCEPTION
      WHEN OTHERS THEN
         RETURN NULL;
   END Get_Note_Chr;

   -- получить цифровое значение примечания
   FUNCTION Get_Note_Num(in_Obj_Tp  in number,
                         in_Obj_Id  in varchar2,
                         in_Note_Id in Dnotetext_Dbt.t_Notekind%TYPE,
                         in_Date    in date default null) RETURN NUMBER IS
      r_Not Dnotetext_Dbt%ROWTYPE;
      r_Ntk Dnotekind_Dbt%ROWTYPE;
   BEGIN
      SELECT *
        INTO r_Ntk
        FROM Dnotekind_Dbt t
       WHERE t.t_Objecttype = in_Obj_Tp
         AND t.t_Notekind = in_Note_Id;
      r_Not := Get_Note(in_Obj_Tp, in_Obj_Id, in_Note_Id, in_Date);
      IF r_Not.t_Id IS NULL THEN
         RETURN NULL;
      END IF;
      CASE r_Ntk.t_Notetype
         WHEN 0 THEN
            RETURN TRIM(Chr(0) FROM Rsb_Struct.Getint(r_Not.t_Text));
         WHEN 1 THEN
            RETURN TRIM(Chr(0) FROM Rsb_Struct.Getlong(r_Not.t_Text));
         WHEN 2 THEN
            RETURN TRIM(Chr(0) FROM Rsb_Struct.Getdouble(r_Not.t_Text));
         WHEN 3 THEN
            RETURN TRIM(Chr(0) FROM Rsb_Struct.Getdouble(r_Not.t_Text));
         WHEN 4 THEN
            RETURN TRIM(Chr(0) FROM Rsb_Struct.Getdouble(r_Not.t_Text));
         WHEN 25 THEN
            RETURN TRIM(Chr(0) FROM Rsb_Struct.Getmoney(r_Not.t_Text));
         ELSE
            RETURN 0;
      END CASE;
   EXCEPTION
      WHEN OTHERS THEN
         RETURN NULL;
   END Get_Note_Num;

   -- получить значение даты-время примечания
   FUNCTION Get_Note_Dat(in_Obj_Tp  in number,
                         in_Obj_Id  in varchar2,
                         in_Note_Id in Dnotetext_Dbt.t_Notekind%TYPE,
                         in_Date    in date default null) RETURN DATE IS
      r_Not Dnotetext_Dbt%ROWTYPE;
      r_Ntk Dnotekind_Dbt%ROWTYPE;
   BEGIN
      SELECT *
        INTO r_Ntk
        FROM Dnotekind_Dbt t
       WHERE t.t_Objecttype = in_Obj_Tp
         AND t.t_Notekind = in_Note_Id;
      r_Not := Get_Note(in_Obj_Tp, in_Obj_Id, in_Note_Id, in_Date);
      IF r_Not.t_Id IS NULL THEN
         RETURN NULL;
      END IF;
      CASE r_Ntk.t_Notetype
         WHEN 9 THEN
            RETURN TRIM(Chr(0) FROM Rsb_Struct.Getdate(r_Not.t_Text));
         WHEN 10 THEN
            RETURN TRIM(Chr(0) FROM Rsb_Struct.Gettime(r_Not.t_Text));
         ELSE
            RETURN NULL;
      END CASE;
   EXCEPTION
      WHEN OTHERS THEN
         RETURN NULL;
   END Get_Note_Dat;

  ------------------------------------------------------
  --Функция получения кода субъекта DWH на основании ID субъекта в RS-Bank.
  ------------------------------------------------------
  function GetCODE_SUBJECT (in_ID in number) return varchar2 is
    out_result varchar2(250);
  begin
    select t.t_code || case when 0 < (select count(1)
                                        from Dobjcode_Dbt o
                                       where o.t_objecttype = 3
                                             and o.t_codekind in (3,6)
                                             and o.t_state = 0
                                             and o.t_objectid = t.t_partyid and rownum < 2 )
                                 or 0 < (select count(1)
                                                   from dpartyown_dbt o
                                                  where o.t_partykind = 2
                                                        and o.t_partyid = t.t_partyid  and rownum < 2 )
                                      then '#BANKS'
                            when p.t_legalform = 1 then
                              '#CUST_CORP'
                            when p.t_legalform = 2 then
                              case
                                when pers.t_isemployer = chr(88) then
                                  '#CUST_CORP'
                                else
                                  '#PERSON'
                              end
                        end
      into out_result
      from dpartcode_dbt t
     inner join dparty_dbt p on p.t_partyid = t.t_partyid
      left join dpersn_dbt pers
        on (p.t_partyid = pers.t_personid)
     where t.t_partyid = in_ID
           and t.t_codekind = 101
           and t.t_state = 0;
    return out_result;
  exception
      when no_data_found then
        --out_result := 'НЕ ЗАПОЛНЕН КОД 101';
        out_result := '-1';
        return out_result;
      when others then
        Raise_application_error(SQLCODE, SQLERRM);
  end;


  ------------------------------------------------------
  -- Функция получения кода подразделения DWH на основании ID в RS-Bank.
  ------------------------------------------------------
  function GetCODE_DEPARTMENT (in_ID in number) return varchar2 is
    out_result    varchar2(30);
  begin
    /*
    TODO: owner="k_guslyakov" category="Test" priority="1 - High" created="16.10.2018"
    text="Необходимо привести в соответствие наименования филиал"
    */
    select d.t_name
      into out_result
      from ddp_dep_dbt d
     where d.t_code = in_ID;
    return out_result;
  end;

  ------------------------------------------------------
  -- Возвращает ISO код
  ------------------------------------------------------
  function GetFINSTR_CODE (in_ID in number) return varchar2 is
    out_result varchar2(30);
  begin
    select decode(t_iso_number, 643, 810, t_iso_number)
      into out_result
      from dfininstr_dbt
     where t_fiid = in_ID;
    return out_result;
  end;

  ------------------------------------------------------
  -- Функция получения код валюты(Возвращает USD/EUR и т.д.) DWH на основании DFININSTR.T_FIID в RS-Bank.
  ------------------------------------------------------
  function GetCURR_CODE_TXT (in_ID in number) return varchar2 is
    out_result varchar2(30);
  begin
    select t_ccy
      into out_result
      from dfininstr_dbt
     where t_fiid = in_ID;
    return out_result;
  end;
  ------------------------------------------------------
  -- Функция возвращает сгенерированный ID компаненты для EXT_FILE. Используется в GetComponentCode
  ------------------------------------------------------
  function GetEXT_FILE_ID (in_EventID in number, in_DT in varchar2, in_Rec_Status varchar2) return varchar2 is
    out_result    varchar2(100);
    tmpRec_Status varchar2(100);
  begin
    tmpRec_Status := case in_Rec_Status when REC_ADD    then 'B0'
                                        when REC_CLOSED then 'B1'
                                        when REC_DELETE then 'A4'
                     end;
    out_result := in_EventID || '#' || in_DT || '#' || tmpRec_Status;
    return out_result;
  end;

  function ModifyCodeSubject(codesub in varchar2) return varchar2 is
    modcode varchar2(50);
  begin
    if regexp_like (codesub, '#00#.#') then  -- код БИС
      modcode := replace(replace(replace(regexp_replace(regexp_replace(codesub, '#00#.#', '#'), '^\d{4}#IBSOXXX#', '0000#BISQUIT#' ), 'BANKS', 'banks'), 'PERSON', 'person'), 'CUST_CORP', 'cust_corp');
    else
      modcode := regexp_replace(codesub, '^\d{4}#IBSOXXX#', '9999#IBSOXXX#');
    end if;
    return modcode;
  end;

  ------------------------------------------------------
  -- Функция генератор кодов, идентифицирующих компоненты предметной области, затронутые учетным событием Требования 2.4
  ------------------------------------------------------
  function GetComponentCode (in_Component in varchar2, in_System in varchar2, in_department in number,
                             in_ComponentID in varchar2, in_dockind in number default 0) return varchar2 is
    out_result varchar2(500);
    Dep        varchar2(500);
    vTMP       varchar2(500);
    vPaymentID number;
  begin
    /*
    (Рогалев, 01.11)
    Подход такой: коды СОФРовых сущностей выгружаем без кода филиала и кода системы,
    коды Бисквитных сущностей должны быть сделаны так, как они хранятся в ЦХД.
    По большей части формат Филиал#Бисквит#Код_в_Бисквит.
    К сущностям СОФР относим: коды сделок, ролей счетов, ролей клиентов
    К сущностям Биса относим все остальные коды
    Отдельное требование по кодам атрибутов и индикаторов - это вроде как софровые сущности, но сам код (без филиала и системы) должен совпадать с кодом Биса.
    */
    if in_Component = 'FCT_DEAL' then

      --Dep := GetCODE_DEPARTMENT(in_department);
      out_result := /*Dep ||'#'|| in_System ||'#'||*/ in_ComponentID ;
    elsif in_Component = 'EXT_FILE' then
      Dep := GetCODE_DEPARTMENT(in_department);
      out_result := Dep ||'#'|| in_System ||'#'|| in_ComponentID ;
    elsif in_Component = 'DET_ACCOUNT' then
      Dep := GetCODE_DEPARTMENT(in_department);
      out_result := Dep ||'#'|| in_System ||'#'|| in_ComponentID ;
    elsif in_Component = 'FCT_HALFCARRY' then
      Dep := GetCODE_DEPARTMENT(in_department);
      -- вырежим #1 из хвоста кода проводки
      if in_System = System_IBSO then
        out_result := Dep ||'#'|| in_System ||'#'|| regexp_replace(in_ComponentID, '#\d*$');
      else
        out_result := Dep ||'#'|| in_System ||'#'|| in_ComponentID ;
      end if;
    elsif in_Component = 'DET_SUBJECT_ROLEDEAL' then
      --Dep := GetCODE_DEPARTMENT(in_department);
      out_result := /*Dep ||'#'|| in_System ||'#'||*/ in_ComponentID ;
    elsif in_Component = 'DET_RISK' then
      out_result := /*in_System ||'#'||*/ in_ComponentID ;
    elsif in_Component = 'FCT_CARRY' then
      Dep := GetCODE_DEPARTMENT(in_department);
      -- комментарий Юрий
      begin
      select nvl(decode(t.t_userfield4, chr(1), null, t.t_userfield4),
                 decode(p.t_userfield4, chr(1), null, p.t_userfield4)
                ), p.t_paymentid
        into vTMP, vPaymentID
        from dacctrn_dbt t
             --left outer join doprdocs_dbt d on d.t_acctrnid = t.t_acctrnid
             --left outer join doproper_dbt o on o.t_id_operation = d.t_id_operation
             left outer join dpmdocs_dbt pmdoc on pmdoc.t_acctrnid = t.t_acctrnid
             left outer join dpmpaym_dbt p  on ((in_dockind=0 or p.t_dockind = in_dockind)
                                                or (in_dockind = 102 and p.t_dockind = 322)
                                                or (in_dockind = 4626 and p.t_dockind = 4627)
                                               )
                                               and p.t_paymentid = pmdoc.t_paymentid
       where t.t_acctrnid = in_ComponentID;
      exception
        when others then
          vTMP := null;
      end;
      -- проверим может есть в связанном платеже код проводки бисквит
      if vTMP is null then
        begin
          select decode(p.t_userfield4, chr(1), null,p.t_userfield4)
            into vTMP
            from dpmlink_dbt l
           inner join dpmpaym_dbt p on p.t_paymentid = l.t_initialpayment
            where l.t_purposepayment = vPaymentID;
        exception
          when others then
            vTMP := null;
        end;
      end if;
      -- вырежим #1 из хвоста кода проводки
      if in_System = System_IBSO then
        vTMP := regexp_replace(vTMP, '#\d*$');
      end if;
      out_result := Dep ||'#'|| in_System ||'#'|| vTMP;
    elsif in_Component = 'FCT_CARRY_SEPARATE' then
      Dep := GetCODE_DEPARTMENT(in_department);
      -- вырежим #1 из хвоста кода проводки
      if in_System = System_IBSO then
        out_result := Dep ||'#'|| in_System ||'#'|| regexp_replace(in_ComponentID, '#\d*$');
      else
        out_result := Dep ||'#'|| in_System ||'#'|| in_ComponentID ;
      end if;
    elsif in_Component = 'DET_SUBJECT' then
      Dep := GetCODE_DEPARTMENT(in_department);
      vTMP := GetCODE_SUBJECT(in_ComponentID);
      if vTMP = '-1' then
            out_result := vTMP;
      else
          out_result := Dep ||'#'|| in_System ||'#'|| vTMP;
      end if;
      out_result := ModifyCodeSubject(out_result);
    elsif in_Component = 'DET_KINDPROCRATE' then
      --Dep := GetCODE_DEPARTMENT(in_department);
      out_result := /*Dep ||'#'|| in_System ||'#'|| */in_ComponentID ;
    elsif in_Component = 'FCT_PROVISIONDEAL' then
      --Dep := GetCODE_DEPARTMENT(in_department);
      out_result := /*Dep ||'#'|| in_System ||'#'||*/ in_ComponentID ;
    elsif in_Component = 'DET_PROVISIONDEAL_TYPE' then
      --Dep := GetCODE_DEPARTMENT(in_department);
      out_result := /*Dep ||'#'|| in_System ||'#'||*/ in_ComponentID ;
    elsif in_Component = 'FCT_PROVISION_OBJECT' then
      out_result := in_ComponentID ;

    end if;
    return out_result;
  end;

  function Get_BKI_GUID (in_DocID in number, in_Kind_DocID number) return varchar2 is
    out_result varchar2(50);
    pragma autonomous_transaction;

    begin
      if (in_DocID <= 0) or
         (in_Kind_DocID <= 0)
         then
           return out_result;
      end if;

      begin

        select u.T_GUID
           into out_result
         from RSHB_BKI_GUID u
        where u.t_objectid   = in_DocID
         and u.t_objecttype = in_Kind_DocID;

      exception
        when others then
          out_result := null;
      end;

      if (out_result is null) then

       select u.t_guid
        into out_result
        from RSHB_BKI_GUID u
       where u.t_objectid   is NULL
         and u.t_objecttype is NULL
         and u.t_guid is not NULL
         and rownum = 1;

         BEGIN
                update RSHB_BKI_GUID set t_objectid = in_DocID, t_objecttype = in_Kind_DocID
                where t_guid = out_result;
commit;                
           COMMIT;
                  RETURN out_result;
         EXCEPTION
             WHEN OTHERS THEN
             ROLLBACK;
                 RETURN out_result;
         END;

      end if;

    return out_result;
  end;

  ------------------------------------------------------
  -- Запись данных ASS_FCT_DEAL (Связь между сделками)
  ------------------------------------------------------
  procedure ins_ASS_FCT_DEAL (in_Parent_Code        varchar2,
                              in_Child_Code         varchar2,
                              in_Type_Deal_Rel_Code varchar2,
                              in_Rec_Status         varchar2,
                              in_DT                 varchar2,
                              in_SysMoment          varchar2,
                              in_Ext_File           varchar2
                              ) is
  begin
    insert into Ldr_Infa.ASS_FCT_DEAL (Parent_Code, Child_Code, Type_Deal_Rel_Code,Rec_Status, DT, SysMoment, Ext_File)
                               values (in_Parent_Code, in_Child_Code, in_Type_Deal_Rel_Code, in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                               
  end;

  ------------------------------------------------------
  -- Запись данных FCT_DEAL (Общие условия по сделке)
  ------------------------------------------------------
  procedure ins_FCT_DEAL (in_Code            varchar2,
                          in_Subject_Code    varchar2,
                          in_Department_Code varchar2,
                          in_DealType        varchar2,
                          in_DocNum          varchar2,
                          in_Is_Interior     varchar2,
                          in_BeginDate       varchar2,
                          in_EndDate         varchar2,
                          in_Note            varchar2,
                          in_Rec_Status      varchar2,
                          in_DT              varchar2,
                          in_SysMoment       varchar2,
                          in_Ext_File        varchar2)  is
  begin
    insert into Ldr_Infa.FCT_DEAL (Code, Subject_Code, Department_Code, DealType, DocNum, Is_Interior,
                                   BeginDate, EndDate, Note, Rec_Status, DT, SysMoment, Ext_File)
                           values (in_Code, in_Subject_Code, in_Department_Code, in_DealType, in_DocNum, in_Is_Interior,
                                   in_BeginDate, in_EndDate, in_Note, in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                   
  end;

  ------------------------------------------------------
  --Запись данных в FCT_MBKCREDEAL (Специфические условия по иежбанковскому кредиту)
  ------------------------------------------------------
  procedure ins_FCT_MBKCREDEAL (in_Deal_Code   varchar2,
                                in_Finstr_Code varchar2,
                                in_DealSum     varchar2,
                                in_DealTypeMBK varchar2,
                                in_TypeSynd    varchar2,
                                in_Rec_Status  varchar2,
                                in_SysMoment   varchar2,
                                in_Ext_File    varchar2) is
  begin
    insert into Ldr_Infa.FCT_MBKCREDEAL (Deal_Code, Finstr_Code, DealSum, DealTypeMBK, TypeSynd, Rec_Status, SysMoment, Ext_File)
                                 values (in_Deal_Code, in_Finstr_Code, in_DealSum, in_DealTypeMBK, in_TypeSynd, in_Rec_Status, in_SysMoment, in_Ext_File);
commit;                                 
  end;

  ------------------------------------------------------
  --Запись данных в FCT_MBKDEPDEAL (Специфические условия по иежбанковскому депозиту)
  ------------------------------------------------------
  procedure ins_FCT_MBKDEPDEAL (in_Deal_Code   varchar2,
                                in_Finstr_Code varchar2,
                                in_DealSum     varchar2,
                                in_DealTypeMBK varchar2,
                                in_Rec_Status  varchar2,
                                in_SysMoment   varchar2,
                                in_Ext_File    varchar2) is
  begin
    insert into Ldr_Infa.FCT_MBKDEPDEAL (Deal_Code, Finstr_Code, DealSum, DealTypeMBK, Rec_Status, SysMoment, Ext_File)
                                 values (in_Deal_Code, in_Finstr_Code, in_DealSum, in_DealTypeMBK, in_Rec_Status, in_SysMoment, in_Ext_File);
commit;                                 
  end;

  ------------------------------------------------------
  --Запись данных в FCT_PROLONGATION (Пролонгация сделок)
  ------------------------------------------------------
  procedure ins_FCT_PROLONGATION (in_Parent_Code varchar2,
                                  in_Child_Code  varchar2,
                                  in_LongSum     varchar2,
                                  in_longsum_Nat varchar2,
                                  in_Code_Parent_Orig varchar2,
                                  in_Code_Child_Orig  varchar2,
                                  in_Rec_Status  varchar2,
                                  in_DT          varchar2,
                                  in_SysMoment   varchar2,
                                  in_Ext_File    varchar2
                                  ) is
  begin
    insert into Ldr_Infa.FCT_PROLONGATION (Parent_Code, Child_Code, LongSum, longsum_Nat, Code_Parent_Orig, Code_Child_Orig,
                                           Rec_Status, DT, SysMoment, Ext_File)
                                   values (in_Parent_Code, in_Child_Code, in_LongSum, in_longsum_Nat, in_Code_Parent_Orig, in_Code_Child_Orig,
                                           in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                           
  end;


  ------------------------------------------------------
  --Запись данных в ASS_ACCOUNTDEAL (Связь сделки со счетом)
  ------------------------------------------------------
  procedure ins_ASS_ACCOUNTDEAL (in_Account_Code in varchar2,
                                 in_Deal_Code    in varchar2,
                                 in_RoleAccount_Deal_Code in varchar2,
                                 in_Rec_Status  in varchar2,
                                 in_DT          in varchar2,
                                 in_SysMoment   in varchar2,
                                 in_Ext_File    in varchar2,
                                 in_DT_END      in varchar2 default null-- KS 23.02.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END 
                                  ) is
   begin
     insert into Ldr_Infa.ASS_ACCOUNTDEAL (Account_Code, Deal_Code, RoleAccount_Deal_Code,
                                           Rec_Status, DT, SysMoment, Ext_File, Dt_End)
                                    select in_Account_Code, in_Deal_Code, in_RoleAccount_Deal_Code,
                                           in_Rec_Status, in_DT, in_SysMoment, in_Ext_File, in_DT_END
                                     from dual
                                    where not exists(
                                                    select 1
                                                      from ldr_infa.ass_accountdeal aa
                                                     where aa.account_code = in_Account_Code
                                                           and aa.deal_code = in_Deal_Code
                                                           and aa.roleaccount_deal_code = in_RoleAccount_Deal_Code
                                                           and aa.Rec_Status = in_Rec_Status
                                                           and aa.SysMoment = in_SysMoment
                                                           and aa.dt = in_DT
                                                           and (aa.dt_end = in_DT_END or (aa.dt_end is null and in_DT_END is null)));
commit;                                                           
   end;


  /** <font color=teal><b>Функция возвращает роль лицевого счета в сделке для категории учета для DWH</b></font>
  *   @param in_CategoryID ID категории учета dmccateg_dbt.t_id
  *   @return Возвращает сгенерированный код DWH для категории учета
  */
  function GetRoleAccount_Deal_Code (in_CategoryID in number) return varchar2 is
    out_result varchar2(100);
  begin
    /*
    TODO: owner="k_guslyakov" category="Finish" priority="1 - High" created="18.10.2018"
    text="RoleAccountDeal ждем решение от банка как определять соответствие пока использую просто код категории.
          19.10.2018 Предварительное решение примечание на счете"
    */
    select UPPER(c.t_code)
      into out_result
      from dmccateg_dbt c
     where c.t_id = in_CategoryID;
    return out_result;
  end;

  /** <font color=teal><b>Процедура добавления связей ASS_ACCOUNTDEAL (Связь сделки со счетом) на основании привязанных к документу Категорий Учета </b></font>
  *   @param in_Kind_DocID  Входящий параметр. Вид документа к которому привязаны категории учета
  *   @param in_DocID       Входящий параметр. ID документа к которому привязаны категории учета
  *   @param in_Date        Дата последнего события с привязкой категории учета
  *   @param in_dwhDeal     Код сдежки используемый при генерации Кода сделки для DWH( IDсделки # Тип сделки)
  *   @param in_Rec_Status  Тип учетного события REC_ADD/REC_CLOSED/REC_DELETE
  *   @param in_DT          Дата учетного события
  *   @param in_SysMoment   Момент (дата и время с точностью до секунды), когда началось формирование порции.
  *   @param in_Ext_File    Идентификатор порции данных, в рамках которой была выгружена рассматриваемая строка
  */
  procedure add_ASS_ACCOUNTDEAL (in_Kind_DocID in number,
                                          in_DocID      in number,
                                          in_Date       in date,
                                          in_dwhDeal    in varchar2,
                                          in_Rec_Status in varchar2,
                                          in_DT         in varchar2,
                                          in_SysMoment  in varchar2,
                                          in_Ext_File   in varchar2
                                         ) is
    begin
      -- Пройдемся по всем изменениям в привязках за день
      for i in (select c.t_id,
                       c.t_number,
                       c.t_code,
                       case
                          when (acc.t_userfield4 is null) or
                              (acc.t_userfield4 = chr(0)) or
                              (acc.t_userfield4 = chr(1)) or
                              (acc.t_userfield4 like '0x%') then
                            acc.t_account
                          else
                            acc.t_userfield4
                       end t_account,
                       --> AS 2021-10-13 IM4249806 SD6226778 связь счет-сделка из СОФР в ЦХД выгружается с некорректной датой
                       -- d.t_activatedate,
                       greatest (d.t_activatedate, acc.t_open_date) t_activatedate,
                       --<
                       d.t_disablingdate,
                       d.t_actiondate,
                       d.t_Departmentid Department
                  from dmcaccdoc_dbt d
                       inner join dmccateg_dbt c on c.t_id = d.t_catid
                       inner join daccount_dbt acc on acc.t_account = d.t_account
                 where d.t_dockind = in_Kind_DocID
                       and c.t_id != 500
                       and d.t_activatedate != d.t_disablingdate -- не выгружаю связи действующие 1 день, считаем ошибкой
                       and d.t_account is not null
                       and d.t_docid = in_DocID
               ) loop
          -- Выгрузка новых связей
          ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_IBSO, i.Department, i.t_account),
                               in_dwhDeal,
                               GetRoleAccount_Deal_Code(i.t_id),
                               qb_dwh_utils.REC_ADD,
                               qb_dwh_utils.DateToChar(i.t_activatedate),
                               in_SysMoment, in_Ext_File,
                               -- KS 23.02.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                               case i.t_disablingdate
                                 when to_date('01.01.0001','dd.mm.yyyy') then qb_dwh_utils.DateToChar(DT_END)
                                 else qb_dwh_utils.DateToChar(i.t_disablingdate-1)
                               end
                              );
        /*
        if i.t_disablingdate != to_date('01.01.0001','dd.mm.yyyy')  then
          -- Закрытие действующих связий
          ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_IBSO, i.Department, i.t_account),
                               in_dwhDeal,
                               GetRoleAccount_Deal_Code(i.t_id),
                               --in_Rec_Status,
                               qb_dwh_utils.REC_CLOSED,
                               qb_dwh_utils.DateToChar(i.t_disablingdate),
                               in_SysMoment, in_Ext_File
                              );
        end if;
        */

      end loop;

      -- Для драгметаллов привяжем счета которые не привязаны к сделке но участвовали в проводках
      for i in (with deal as
                     (select /*+ materialize*/ DVN.T_DATE
                            ,DVN.T_ID
                            ,T.T_ACCOUNT_PAYER
                            ,T.T_ACCOUNT_RECEIVER
                  from ddvndeal_dbt dvn
                       inner join ddvnfi_dbt nFI on nFI.t_Type = 0 and nFI.t_DealID = dvn.t_ID
                       inner join doproper_dbt o on o.t_documentid = lpad(dvn.t_id, 34, '0')
                                                    and o.t_dockind = dvn.t_dockind
                       inner join doprdocs_dbt d on o.t_id_operation = d.t_id_operation
                       inner join dacctrn_dbt t on t.t_acctrnid = d.t_acctrnid
                  where dvn.t_dockind = in_Kind_DocID
                       and dvn.t_id = in_DocID  ),
                  deal_acc as
                     (select  t.T_DATE
                            ,t.T_ID
                            ,T.T_ACCOUNT_PAYER as account
                        from deal t
                        union 
                        select t.T_DATE
                            ,t.T_ID
                            ,T.T_ACCOUNT_RECEIVER
                        from deal t
                      )
                    select /*+ ordered use_nl(dc)*/ distinct c.t_id,
                       c.t_number,
                       c.t_code,
                       --acc.t_userfield4 t_account, --dc.t_account,
                       case
                          when (acc.t_userfield4 is null) or
                              (acc.t_userfield4 = chr(0)) or
                              (acc.t_userfield4 = chr(1)) or
                              (acc.t_userfield4 like '0x%') then
                            acc.t_account
                          else
                            acc.t_userfield4
                       end t_account,
                       greatest (dc.t_activatedate,d.t_date) t_activatedate,
                       case when (dc.t_disablingdate = to_date('01.01.0001','dd.mm.yyyy')
                                  and trunc (sysdate) >= (select max(nFI0.t_Paydate)
                                                            from ddvnfi_dbt nFI0
                                                           where nFI0.t_DealID = d.t_ID))
                                 or
                                 (dc.t_disablingdate != to_date('01.01.0001','dd.mm.yyyy')
                                  and dc.t_disablingdate >= (select max(nFI0.t_Paydate)
                                                               from ddvnfi_dbt nFI0
                                                              where nFI0.t_DealID = d.t_ID)
                                 )
                            then (select max(nFI0.t_Paydate)
                                    from ddvnfi_dbt nFI0
                                   where nFI0.t_DealID = d.t_ID)
                            when
                                 (dc.t_disablingdate != to_date('01.01.0001','dd.mm.yyyy')
                                  and dc.t_disablingdate <= (select max(nFI0.t_Paydate)
                                                               from ddvnfi_dbt nFI0
                                                              where nFI0.t_DealID = d.t_ID)
                                 )
                            then dc.t_disablingdate
                            else to_date('01.01.0001','dd.mm.yyyy')
                       end t_disablingdate,
                       greatest (dc.t_actiondate,d.t_date) t_actiondate,
                       dc.t_Departmentid Department
                    from  deal_acc d
                       
                /*  from ddvndeal_dbt dvn
                       inner join ddvnfi_dbt nFI on nFI.t_Type = 0 and nFI.t_DealID = dvn.t_ID
                       inner join doproper_dbt o on o.t_documentid = lpad(dvn.t_id, 34, '0')
                                                    and o.t_dockind = dvn.t_dockind
                       inner join doprdocs_dbt d on o.t_id_operation = d.t_id_operation
                       inner join dacctrn_dbt t on t.t_acctrnid = d.t_acctrnid*/
                 
                      inner join dmcaccdoc_dbt dc on dc.t_account = d.account --in (t.t_account_payer, t.t_account_receiver)
                                                      and dc.t_dockind = 0
                                                      --and dc.t_contractor = dvn.t_contractor

                       inner join dmccateg_dbt c on c.t_id = dc.t_catid
                       inner join daccount_dbt acc on acc.t_account = dc.t_account
                 /*where dvn.t_dockind = in_Kind_DocID
                       and dvn.t_id = in_DocID*/) loop
          -- Выгрузка новых связей
          qb_dwh_utils.ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_IBSO, i.Department, i.t_account),
                               in_dwhDeal,
                               qb_dwh_utils.GetRoleAccount_Deal_Code(i.t_id),
                               qb_dwh_utils.REC_ADD,
                               qb_dwh_utils.DateToChar(i.t_activatedate),
                               in_SysMoment, in_Ext_File,
                               -- KS 23.02.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                               case i.t_disablingdate
                                 when to_date('01.01.0001','dd.mm.yyyy') then qb_dwh_utils.DateToChar(DT_END)
                                 else qb_dwh_utils.DateToChar(i.t_disablingdate-1)
                               end
                              );
        /*
        if i.t_disablingdate != to_date('01.01.0001','dd.mm.yyyy')  then
          -- Закрытие действующих связий
          qb_dwh_utils.ins_ASS_ACCOUNTDEAL (qb_dwh_utils.GetComponentCode ('DET_ACCOUNT',qb_dwh_utils.System_IBSO, i.Department, i.t_account),
                               in_dwhDeal,
                               qb_dwh_utils.GetRoleAccount_Deal_Code(i.t_id),
                               qb_dwh_utils.REC_CLOSED,
                               qb_dwh_utils.DateToChar(i.t_disablingdate),
                               in_SysMoment, in_Ext_File
                              );
        end if;
        */

      end loop;
    end;


  ------------------------------------------------------
  --Запись данных в FCT_DEALRISK (Риски по сделке)
  ------------------------------------------------------
  procedure ins_FCT_DEALRISK (in_Deal_Code              in varchar2,
                              in_RiskCat_Code_TypeRisk  in varchar2,
                              in_RiskCat_Code           in varchar2,
                              in_Reserve_Rate           in varchar2,
                              in_GROUND                 in varchar2,
                              in_Rec_Status             in varchar2,
                              in_DT                     in varchar2,
                              in_SysMoment              in varchar2,
                              in_Ext_File               in varchar2
                             ) is
   begin
     insert into Ldr_Infa.FCT_DEALRISK (Deal_Code, RiskCat_Code_TypeRisk, RiskCat_Code, Reserve_Rate, GROUND,
                                        Rec_Status, DT, SysMoment, Ext_File)
                                values (in_Deal_Code, in_RiskCat_Code_TypeRisk, in_RiskCat_Code, in_Reserve_Rate, in_GROUND,
                                        in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                        
   end;


  ------------------------------------------------------
  --Запись данных в Ldr_Infa.FCT_PROCRATE_DEAL (Ставка по сделке)
  ------------------------------------------------------
  procedure ins_FCT_PROCRATE_DEAL (in_Deal_Code            in varchar2,
                                   in_KindProcRate_Code    in varchar2,
                                   in_SubKindProcRate_Code in varchar2,
                                   in_ProcBase_Code        in varchar2,
                                   in_ProcRate             in varchar2,
                                   in_ProcSum              in varchar2,
                                   in_DT_Next_OverValue    in varchar2,
                                   in_DT_Contract          in varchar2,
                                   in_Rec_Status           in varchar2,
                                   in_DT                   in varchar2,
                                   in_SysMoment            in varchar2,
                                   in_Ext_File             in varchar2
                                  ) is
   begin
     insert into Ldr_Infa.FCT_PROCRATE_DEAL (Deal_Code, KindProcRate_Code, SubKindProcRate_Code, ProcBase_Code, ProcRate, ProcSum,
                                             DT_Next_OverValue, DT_Contract, Rec_Status, DT, SysMoment, Ext_File)
                                     values (in_Deal_Code, in_KindProcRate_Code, in_SubKindProcRate_Code, in_ProcBase_Code, in_ProcRate, in_ProcSum,
                                             /*in_DT_Next_OverValue*/null, in_DT_Contract, in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                             
   end;

  ------------------------------------------------------
  --Запись данных в FCT_SUBJECT_ROLEDEAL (Ставка по сделке)
  ------------------------------------------------------
  procedure ins_FCT_SUBJECT_ROLEDEAL (in_Deal_Code    in varchar2,
                                      in_Subject_Code in varchar2,
                                      in_Role_Code    in varchar2,
                                      in_Is_Agreement in varchar2,
                                      in_DT_Agreement in varchar2,
                                      in_BeginDate    in varchar2,
                                      in_EndDate      in varchar2,
                                      in_Rec_Status   in varchar2,
                                      in_DT           in varchar2,
                                      in_SysMoment    in varchar2,
                                      in_Ext_File     in varchar2
                                      ) is
   begin
     insert into Ldr_Infa.FCT_SUBJECT_ROLEDEAL (Deal_Code, Subject_Code, Role_Code, Is_Agreement, DT_Agreement, BeginDate, EndDate,
                                                Rec_Status, DT, SysMoment, Ext_File)
                                        values (in_Deal_Code, in_Subject_Code, in_Role_Code, in_Is_Agreement, in_DT_Agreement, in_BeginDate, in_EndDate,
                                                in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                                
   end;


  ------------------------------------------------------
  --Запись данных в ASS_DEAL_CAT_VAL (Связь сделки со значением ограниченного доп.атрибута)
  ------------------------------------------------------
  procedure ins_ASS_DEAL_CAT_VAL (in_Deal_Code    in varchar2,
                                  in_Deal_Cat_Val_Code_Deal_Cat in varchar2,
                                  in_Deal_Cat_Val_CODE    in varchar2,
                                  in_Rec_Status   in varchar2,
                                  in_DT           in varchar2,
                                  in_SysMoment    in varchar2,
                                  in_Ext_File     in varchar2
                                 ) is
   begin
     insert into Ldr_Infa.ass_deal_cat_val(deal_code, Deal_Cat_Val_Code_Deal_Cat, Deal_Cat_Val_CODE,
                                           Rec_Status, DT, SysMoment, Ext_File)
                                    values(in_Deal_Code,  in_Deal_Cat_Val_Code_Deal_Cat, in_Deal_Cat_Val_CODE,
                                           in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                           
   end;

  ------------------------------------------------------
  -- Добавляем данные в ASS_DEAL_CAT_VAL (Связь сделки со значением ограниченного доп.атрибута)
  -- на основании категорий учета
  ------------------------------------------------------
  procedure add_ASS_DEAL_CAT_VAL (in_Kind_DocID in number,
                                  in_DocID      in number,
                                  in_Date       in date,
                                  in_dwhDeal    in varchar2,
                                  in_Rec_Status in varchar2,
                                  in_DT         in varchar2,
                                  in_SysMoment  in varchar2,
                                  in_Ext_File   in varchar2
                                 ) is
     --vDepartment number;
     vKind_DocID number;
  begin
    if in_Kind_DocID = 102 then
      vKind_DocID := 103;
    else
      vKind_DocID := in_Kind_DocID;
    end if;
    --vDepartment := qb_dwh_utils.GetDepartmentByDealID(in_Kind_DocID, in_DocID);
    for rec_CatObj in (Select to_number(o.t_object) DEAL_CODE,
                              g.t_objecttype || '|C|' || g.t_groupid  DEAL_CAT_VAL_CODE_DEAL_CAT,
                              a.t_objecttype || '|C|' || a.t_groupid || /*'|' || a.t_attrid || */'#' || UPPER(a.t_name)  DEAL_CAT_VAL_CODE,
                              decode(in_Rec_Status, qb_dwh_utils.REC_ADD, o.t_validfromdate, o.t_validtodate) DT
                         from dobjatcor_dbt o
                              inner join dobjgroup_dbt g on g.t_groupid = o.t_groupid and g.t_objecttype = o.t_objecttype
                              inner join dobjattr_dbt a  on a.t_attrid = o.t_attrid and a.t_groupid = o.t_groupid and a.t_objecttype = o.t_objecttype
                        where o.t_objecttype = vKind_DocID
                              and o.t_object = lpad(in_DocID,34,'0')
                              and (
                               (in_Rec_Status = qb_dwh_utils.REC_ADD and in_Date between o.t_validfromdate and o.t_validtodate)
                               or
                               (in_Rec_Status = qb_dwh_utils.REC_CLOSED and in_Date = o.t_validtodate)
                              )
                       ) loop
       ins_ASS_DEAL_CAT_VAL (in_dwhDeal,
                             rec_CatObj.Deal_Cat_Val_Code_Deal_Cat,
                             rec_CatObj.Deal_Cat_Val_Code,
                             in_Rec_Status,
                             qb_dwh_utils.DateToChar(rec_CatObj.Dt),
                             in_SysMoment,
                             in_Ext_File
                            );
    end loop;
    if in_Kind_DocID = 102 then
      for i in(  select 'EXTERNAL_DEALTYPE' DEAL_CAT_VAL_CODE_DEAL_CAT,
                        'EXTERNAL_DEALTYPE#' ||
                        case when t.t_bofficekind = 102 and instr(op_kind.t_systypes, 'S') > 1 and t.t_typedoc = 'D' then 1 --decode(t.t_typedoc, 'L',2, 'D', 1)
                             when t.t_bofficekind = 102 and instr(op_kind.t_systypes, 'S') > 1 and t.t_typedoc = 'L' then 2
                             when t.t_bofficekind = 102 and instr(op_kind.t_systypes, 'B') > 1 and t.t_typedoc = 'D' then 3
                             when t.t_bofficekind = 102 and instr(op_kind.t_systypes, 'B') > 1 and t.t_typedoc = 'L' then 4
                        end DEAL_CAT_VAL_CODE,
                        l.t_start DT
                   from ddl_tick_dbt t
                        inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
                        inner join doprkoper_dbt  op_kind on op_kind.t_kind_operation = t.t_dealtype
          where t.t_bofficekind = in_Kind_DocID
                and t.t_dealid = in_DocID) loop
       ins_ASS_DEAL_CAT_VAL (in_dwhDeal,
                             i.Deal_Cat_Val_Code_Deal_Cat,
                             i.Deal_Cat_Val_Code,
                             in_Rec_Status,
                             qb_dwh_utils.DateToChar(i.Dt),
                             in_SysMoment,
                             in_Ext_File
                            );
       end loop;
    end if;

    if in_Kind_DocID = 4626 then
      for i in(  select '0000#BISQUIT#ТИПДОГП' DEAL_CAT_VAL_CODE_DEAL_CAT,
                        '0000#BISQUIT#ТИПДОГП#МЕЖБАНК' DEAL_CAT_VAL_CODE,
                        csa.t_begdate DT
                   from ddvcsa_dbt csa
                  where csa.t_csaid = in_DocID) loop
        qb_dwh_utils.ins_ASS_DEAL_CAT_VAL (in_dwhDeal,
                                           i.Deal_Cat_Val_Code_Deal_Cat,
                                           i.Deal_Cat_Val_Code,
                                           in_Rec_Status,
                                           qb_dwh_utils.DateToChar(i.Dt),
                                           in_SysMoment,
                                           in_Ext_File
                                          );
       end loop;
     end if;
  end;
  ------------------------------------------------------
  --Запись данных в FCT_DEAL_INDICATOR (Значение свободного доп.атрибута сделки)
  ------------------------------------------------------
  procedure ins_FCT_DEAL_INDICATOR (in_Deal_Code              in varchar2,
                                    in_Deal_ATTR_Code         in varchar2,
                                    in_Currency_Curr_Code_TXT in varchar2,
                                    in_Measurement_Unit_Code  in varchar2, -- Одно значение -1 Не определено
                                    in_Number_Value           in varchar2,
                                    in_Date_Value             in varchar2,
                                    in_String_Value           in varchar2,
                                    in_Rec_Status   in varchar2,
                                    in_DT           in varchar2,
                                    in_SysMoment    in varchar2,
                                    in_Ext_File     in varchar2
                                 ) is
   begin
     insert into Ldr_Infa.fct_deal_indicator(Deal_Code , Deal_ATTR_Code, Currency_Curr_Code_TXT, Measurement_Unit_Code,
                                             Number_Value, Date_Value, String_Value,
                                             Rec_Status, DT, SysMoment, Ext_File)
                                     values (in_Deal_Code , in_Deal_ATTR_Code, in_Currency_Curr_Code_TXT, in_Measurement_Unit_Code,
                                             in_Number_Value, in_Date_Value, in_String_Value,
                                             in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                             
   end;

  ------------------------------------------------------
  --Добавление данных в FCT_DEAL_INDICATOR (Значение свободного доп.атрибута сделки)
  -- На основании Примечаний по объекту
  ------------------------------------------------------
  procedure add_FCT_DEAL_INDICATOR (in_Kind_DocID in number,
                                    in_DocID      in number,
                                    in_Date       in date,
                                    in_dwhDeal    in varchar2,
                                    in_Rec_Status in varchar2,
                                    in_DT         in varchar2,
                                    in_SysMoment  in varchar2,
                                    in_Ext_File   in varchar2,
                                    in_CSA_TYPE   in number default 0
                                   ) is
     --vDepartment number;
     vKind_DocID number;
  begin
    if in_Kind_DocID = 102 then
      vKind_DocID := 103;
    else
      vKind_DocID := in_Kind_DocID;
    end if;
    for rec_Note in (select to_number(t.t_documentid) DEAL_CODE,
                             k.t_objecttype || '|T|' || k.t_notekind DEAL_ATTR_CODE,
                             '-1' CURRENCY_CURR_CODE_TXT,
                             '-1' MEASUREMENT_UNIT_CODE,
                             Case when k.t_notetype  in (0,1,2,3,4,25) then qb_dwh_utils.Get_Note_Num(t.t_objecttype,t.t_documentid, t.t_notekind, t.t_date) end NUMBER_VALUE,
                             Case when k.t_notetype in (9) then qb_dwh_utils.Get_Note_Dat(t.t_objecttype,t.t_documentid, t.t_notekind, t.t_date) end DATE_VALUE,
                             Case when k.t_notetype in (7) then substr(trim(qb_dwh_utils.Get_Note_Chr(t.t_objecttype,t.t_documentid, t.t_notekind, t.t_date)),1,255) end  STRING_VALUE,
                             decode(in_Rec_Status, qb_dwh_utils.REC_ADD, t.t_date, t.t_validtodate) DT
                        from dnotetext_dbt t
                             inner join dnotekind_dbt k on k.t_notekind=t.t_notekind and k.t_objecttype = t.t_objecttype
                       where t.t_objecttype = vKind_DocID
                             and t.t_documentid = lpad(in_DocID, 34, '0')
                             and (
                               (in_Rec_Status = qb_dwh_utils.REC_ADD and in_Date between t.t_date and t.t_validtodate)
                               or
                               (in_Rec_Status = qb_dwh_utils.REC_CLOSED and in_Date = t.t_validtodate)
                               )

                     ) loop
       ins_FCT_DEAL_INDICATOR (in_dwhDeal,
                               rec_Note.Deal_ATTR_Code,
                               rec_Note.Currency_Curr_Code_TXT,
                               rec_Note.Measurement_Unit_Code, -- Одно значение -1 Не определено
                               qb_dwh_utils.NumberToChar(rec_Note.Number_Value,4),
                               DateToChar(rec_Note.Date_Value),
                               rec_Note.String_Value,
                               in_Rec_Status,
                               DateToChar(rec_Note.DT),
                               in_SysMoment,
                               in_Ext_File
                              );
     end loop;

    if in_Kind_DocID = 102 then
       for i in (select 'CLOSE-DATE' Deal_ATTR_Code,
                        '-1' CURRENCY_CURR_CODE_TXT,
                        '-1' MEASUREMENT_UNIT_CODE,
                        null NUMBER_VALUE,
                        nvl(qb_dwh_utils.Get_Note_Dat(103,t.t_dealid,103),l.t_maturity) DATE_VALUE,
                        null  STRING_VALUE,
                        nvl(qb_dwh_utils.Get_Note_Dat(103,t.t_dealid,103),l.t_start) DT
                   from ddl_tick_dbt t
                        inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
                        inner join doprkoper_dbt  op_kind on op_kind.t_kind_operation = t.t_dealtype
          where nvl(qb_dwh_utils.Get_Note_Dat(103,t.t_dealid,103),l.t_maturity) <= in_Date
                and t.t_dealstatus = 20
                and t.t_bofficekind = in_Kind_DocID
                and t.t_dealid = in_DocID
            union all
            select 'CRED-OFFSET' Deal_ATTR_Code,
                        '-1' CURRENCY_CURR_CODE_TXT,
                        '-1' MEASUREMENT_UNIT_CODE,
                        null NUMBER_VALUE,
                        null  DATE_VALUE,
                        case when (select count(1) from dmmpaymode_dbt mm where mm.t_reserve = '1' and mm.t_dealid = t.t_dealid) > 0 then '<-' else '->'end  STRING_VALUE,
                        l.t_start DT
                   from ddl_tick_dbt t
                        inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
                        inner join doprkoper_dbt  op_kind on op_kind.t_kind_operation = t.t_dealtype
          where t.t_bofficekind = in_Kind_DocID
                and t.t_dealid = in_DocID
                union all
                select 'DELAY-OFFSET-INT' Deal_ATTR_Code,
                        '-1' CURRENCY_CURR_CODE_TXT,
                        '-1' MEASUREMENT_UNIT_CODE,
                        null NUMBER_VALUE,
                        null DATE_VALUE,
                        case when (select count(1) from dmmpaymode_dbt mm where mm.t_reserve = '1' and mm.t_dealid = t.t_dealid) > 0 then '<-' else '->'end  STRING_VALUE,
                        l.t_start DT
                   from ddl_tick_dbt t
                        inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
                        inner join doprkoper_dbt  op_kind on op_kind.t_kind_operation = t.t_dealtype
          where t.t_bofficekind = in_Kind_DocID
                and t.t_dealid = in_DocID
                union all
          select
                'FLOATING-RATE-FORMULA' Deal_ATTR_Code,
                '-1' CURRENCY_CURR_CODE_TXT,
                '-1' MEASUREMENT_UNIT_CODE,
                null NUMBER_VALUE,
                null DATE_VALUE,
                fi.t_fi_code || '+' || qb_dwh_utils.NumberToChar(l.t_correct,-1) || '/' ||
                decode(gen.t_includeday,chr(0), 'от даты предоставления средств','от даты сделки') ||
                case when l.t_countday > 0 then ' +' || qb_dwh_utils.NumberToChar(l.t_countday,0) || ' дн'
                                  when l.t_countday < 0 then qb_dwh_utils.NumberToChar(l.t_countday,0) || ' дн'
                                  else null end STRING_VALUE,
                l.t_start DT
            from ddl_tick_dbt t
            inner join ddl_leg_dbt l on l.t_tablepercent = chr(88) and l.t_dealid = t.t_dealid and l.t_legID = 1
                 inner join dfininstr_dbt fi on fi.t_fiid = l.t_caption
                 left outer join ddl_genagr_dbt gen on gen.t_genagrid = t.t_genagrid
           where t.t_bofficekind = in_Kind_DocID
             and t.t_dealid = in_DocID) loop
       ins_FCT_DEAL_INDICATOR (in_dwhDeal,
                               i.Deal_ATTR_Code,
                               i.Currency_Curr_Code_TXT,
                               i.Measurement_Unit_Code, -- Одно значение -1 Не определено
                               i.Number_Value,
                               DateToChar(i.Date_Value),
                               i.String_Value,
                               in_Rec_Status,
                               DateToChar(i.DT),
                               in_SysMoment,
                               in_Ext_File
                              );
     end loop;
   end if;

     if in_Kind_DocID = 4626 then
       for i in (select '0000#BISQUIT#ДАТПЕРЕХ' Deal_ATTR_Code,
                        '-1' CURRENCY_CURR_CODE_TXT,
                        '-1' MEASUREMENT_UNIT_CODE,
                        null NUMBER_VALUE,
                        to_date('01.01.3000','dd.mm.yyyy') DATE_VALUE,
                        null  STRING_VALUE,
                        csa.t_begdate DT
                   from ddvcsa_dbt csa
                        inner join dparty_dbt p on p.t_notresident != chr(88) and p.t_partyid = csa.t_partyid
                  where csa.t_csaid = in_DocID
                 union all
                 select '0000#BISQUIT#INT-OFFSET' Deal_ATTR_Code,
                        '-1' CURRENCY_CURR_CODE_TXT,
                        '-1' MEASUREMENT_UNIT_CODE,
                        null NUMBER_VALUE,
                        null DATE_VALUE,
                        '->' STRING_VALUE,
                        csa.t_begdate DT
                   from ddvcsa_dbt csa
                        inner join dparty_dbt p on p.t_partyid = csa.t_partyid
                  where csa.t_csaid = in_DocID
                 union all
                 select --nvl(v3511.t_code, 'Общие условия') n3511,
                        'FLOATING-RATE-FORMULA' Deal_ATTR_Code,
                        '-1' CURRENCY_CURR_CODE_TXT,
                        '-1' MEASUREMENT_UNIT_CODE,
                        null NUMBER_VALUE,
                        null DATE_VALUE,
                        fi.t_fi_code || '+' || qb_dwh_utils.NumberToChar(ind.t_spread,-1) || '/' ||
                                     decode(gen.t_includeday,chr(88), 'от даты входящего остатка', 'от даты исходящего остатка') ||
                                     case when ind.t_days > 0 then ' +' || qb_dwh_utils.NumberToChar(ind.t_days,0) || ' дн'
                                          when ind.t_days < 0 then qb_dwh_utils.NumberToChar(ind.t_days,0) || ' дн'
                                          else null end STRING_VALUE,
                        ind.t_begdate DT
                   from ddvcsa_dbt csa
                        inner join ddl_genagr_dbt gen on gen.t_genagrid = csa.t_genagrid
                        inner join ddvcsaind_dbt ind on ind.t_csaid =csa.t_csaid and (ind.t_kind = 0 or ind.t_kind =in_CSA_TYPE)
                        inner join dfininstr_dbt fi on fi.t_fiid = ind.t_fiid
                        left outer join dllvalues_dbt v3511 on v3511.t_list = 3511 and v3511.t_element = ind.t_kind
                  where in_CSA_TYPE != 0
                        and csa.t_csaid = in_DocID) loop
         ins_FCT_DEAL_INDICATOR (in_dwhDeal,
                                 i.Deal_ATTR_Code,
                                 i.Currency_Curr_Code_TXT,
                                 i.Measurement_Unit_Code, -- Одно значение -1 Не определено
                                 i.Number_Value,
                                 qb_dwh_utils.DateToChar(i.Date_Value),
                                 i.String_Value,
                                 in_Rec_Status,
                                 qb_dwh_utils.DateToChar(i.Dt),
                                 in_SysMoment,
                                 in_Ext_File
                                );
         end loop;
     end if;
     /*2020-09-01 AS добавляем GUID для БКИ в соответствии с BIQ-6948*/
     if (in_DocID > 0) then
        for i in (select 'UID-BKI' Deal_ATTR_Code,
                        '-1' CURRENCY_CURR_CODE_TXT,
                        '-1' MEASUREMENT_UNIT_CODE,
                        null NUMBER_VALUE,
                        null DATE_VALUE,
                        qb_dwh_utils.Get_BKI_GUID(in_DocID, in_Kind_DocID) STRING_VALUE,
                        l.t_start DT
                   from ddl_tick_dbt t
                        inner join ddl_leg_dbt l on l.t_dealid = t.t_dealid and l.t_legID = 1
                        inner join doprkoper_dbt op_kind on op_kind.t_kind_operation = t.t_dealtype
          where t.t_bofficekind = in_Kind_DocID
            and t.t_dealid = in_DocID) loop

              ins_FCT_DEAL_INDICATOR (in_dwhDeal,
                                 i.Deal_ATTR_Code,
                                 i.Currency_Curr_Code_TXT,
                                 i.Measurement_Unit_Code, -- Одно значение -1 Не определено
                                 i.Number_Value,
                                 qb_dwh_utils.DateToChar(i.Date_Value),
                                 i.String_Value,
                                 in_Rec_Status,
                                 qb_dwh_utils.DateToChar(i.Dt),
                                 in_SysMoment,
                                 in_Ext_File
                                );
              end loop;
      end if;
   end;

---------------------------------------
-- 4 очередь
--------------------------------------

  ------------------------------------------------------
  --Запись данных в ASS_CARRYDEAL (Связь проводки со сделкой)
  ------------------------------------------------------
  procedure ins_ASS_CARRYDEAL (in_Carry_Code   in varchar2,
                               in_Deal_Code    in varchar2,
                               in_Rec_Status   in varchar2,
                               in_DT           in varchar2,
                               in_SysMoment    in varchar2,
                               in_Ext_File     in varchar2
                              ) is
   begin

     if instr(in_Carry_Code,'АННУЛ') =0 then
     insert into Ldr_Infa.ass_carrydeal(Carry_Code, Deal_Code,
                                             Rec_Status, DT, SysMoment, Ext_File)
                                     values (in_Carry_Code, in_Deal_Code,
                                             in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                             
     end if;

   end;

  ------------------------------------------------------
  --Запись данных в ASS_CARRYDEAL (Связь проводки со сделкой)
  ------------------------------------------------------
  procedure ins_ASS_HALFCARRYDEAL (in_HalfCarry_Code   in varchar2,
                                   in_Deal_Code    in varchar2,
                                   in_Rec_Status   in varchar2,
                                   in_DT           in varchar2,
                                   in_SysMoment    in varchar2,
                                   in_Ext_File     in varchar2
                                  ) is
   begin

     if instr(in_HalfCarry_Code,'АННУЛ') =0 then
     insert into Ldr_Infa.ass_halfcarrydeal(HalfCarry_Code, Deal_Code,
                                             Rec_Status, DT, SysMoment, Ext_File)
                                     values (in_HalfCarry_Code, in_Deal_Code,
                                             in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                             
     end if;

   end;
  ------------------------------------------------------
  --Запись данных в FCT_DEAL_CARRY (Проводка сделочной модели)
  ------------------------------------------------------
  procedure ins_FCT_DEAL_CARRY (in_Account_DBT_Code in varchar2,
                                in_Account_CRD_Code in varchar2,
                                in_Code             in varchar2,
                                in_docnum           in varchar2,
                                in_ground           in varchar2,
                                in_value            in varchar2,
                                in_value_nat        in varchar2,
                                in_info                in varchar2,
                                in_Rec_Status       in varchar2,
                                in_DT               in varchar2,
                                in_SysMoment        in varchar2,
                                in_Ext_File         in varchar2
                               ) is
   begin
     insert into Ldr_Infa.fct_deal_carry(Account_DBT_Code, Account_CRD_Code, Code, docnum, ground, value, value_nat, info,
                                         Rec_Status, DT, SysMoment, Ext_File)
                                 select in_Account_DBT_Code, in_Account_CRD_Code, in_Code, in_docnum, substr(in_ground,1,255), in_value, in_value_nat, in_info,
                                        in_Rec_Status, in_DT, in_SysMoment, in_Ext_File
                                   from dual
                                  where not exists (select 1
                               from ldr_infa.fct_deal_carry lc
                              where lc.code = in_docnum);
commit;                              
   end;

  ------------------------------------------------------
  --Получение кода связи проводки и сделки на основании dpmpaym_dbt.t_purpose
  ------------------------------------------------------
  function GetCode_Carry_Links(in_Purpose in number) return varchar2 is
    out_result varchar2(100);
  begin
    /*
    TODO: owner="k_guslyakov" category="Finish" priority="2 - Medium" created="20.10.2018"
    text="Необходимо уточнить таблицу соответствия dpmpaym_dbt.t_purpose и кода связи проводки с делкой"
    */
    out_result := to_char(in_Purpose);
    return out_result;
  end;
  ------------------------------------------------------
  --Запись данных в FCT_DM_CARRY_ASS (Связь сделки и проводки сделочной модели)
  ------------------------------------------------------
  procedure ins_FCT_DM_CARRY_ASS (in_Code            in varchar2,
                                  in_Deal_Carry_Code in varchar2,
                                  in_Deal_Code       in varchar2,
                                  in_Rec_Status      in varchar2,
                                  in_DT              in varchar2,
                                  in_SysMoment       in varchar2,
                                  in_Ext_File        in varchar2
                                 ) is
   begin
     insert into Ldr_Infa.fct_dm_carry_ass(Code, Deal_Carry_Code, Deal_Code,
                                  Rec_Status, DT, SysMoment, Ext_File)
                          values (in_Code, in_Deal_Carry_Code, in_Deal_Code,
                                  in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                  
   end;


  ------------------------------------------------------
  --Запись данных в FCT_DEAL_RST (Остатки и обороты в разрезе сделок и счетов)
  -- Отсутствует поле РОль
  ------------------------------------------------------
  procedure ins_FCT_DEAL_RST (in_Deal_Code     in varchar2,
                              in_Account_Code in varchar2,
                              in_Val_RST_ACC_IN  in varchar2,
                              in_Val_RST_CUR_IN  in varchar2,
                              in_Val_RST_NAT_IN  in varchar2,
                              in_Val_RST_RUR_IN  in varchar2,
                              in_Val_RST_AMT_IN  in varchar2,
                              in_Val_DBT_ACC     in varchar2,
                              in_Val_DBT_CUR     in varchar2,
                              in_Val_DBT_NAT     in varchar2,
                              in_Val_DBT_RUR     in varchar2,
                              in_Val_DBT_AMT     in varchar2,
                              in_Val_CRD_ACC     in varchar2,
                              in_Val_CRD_CUR     in varchar2,
                              in_Val_CRD_NAT     in varchar2,
                              in_Val_CRD_RUR     in varchar2,
                              in_Val_CRD_AMT     in varchar2,
                              in_Val_RST_ACC_OUT  in varchar2,
                              in_Val_RST_CUR_OUT in varchar2,
                              in_Val_RST_NAT_OUT in varchar2,
                              in_Val_RST_RUR_OUT in varchar2,
                              in_Val_RST_AMT_OUT in varchar2,
                              in_Rec_Status      in varchar2,
                              in_DT              in varchar2,
                              in_SysMoment       in varchar2,
                              in_Ext_File        in varchar2
                             ) is
   begin
     insert into Ldr_Infa.fct_deal_rst(Deal_Code, Account_Code,
                                       Val_RST_ACC_IN, Val_RST_CUR_IN, Val_RST_NAT_IN, Val_RST_RUR_IN, Val_RST_AMT_IN,
                                       Val_DBT_ACC, Val_DBT_CUR, Val_DBT_NAT, Val_DBT_RUR, Val_DBT_AMT,
                                       Val_CRD_ACC, Val_CRD_CUR, Val_CRD_NAT, Val_CRD_RUR, Val_CRD_AMT,
                                       Val_RST_ACC_OUT, Val_RST_CUR_OUT, Val_RST_NAT_OUT, Val_RST_RUR_OUT, Val_RST_AMT_OUT,
                                       Rec_Status, DT, SysMoment, Ext_File)
                                     values (in_Deal_Code, in_Account_Code,
                                             in_Val_RST_ACC_IN, in_Val_RST_CUR_IN, in_Val_RST_NAT_IN, in_Val_RST_RUR_IN, in_Val_RST_AMT_IN,
                                             in_Val_DBT_ACC, in_Val_DBT_CUR, in_Val_DBT_NAT, in_Val_DBT_RUR, in_Val_DBT_AMT,
                                             in_Val_CRD_ACC, in_Val_CRD_CUR, in_Val_CRD_NAT, in_Val_CRD_RUR, in_Val_CRD_AMT,
                                             in_Val_RST_ACC_OUT, in_Val_RST_CUR_OUT, in_Val_RST_NAT_OUT, in_Val_RST_RUR_OUT, in_Val_RST_AMT_OUT,
                                             in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                             
   end;

-- 5 очередь
  ------------------------------------------------------
  --Запись данных в FCT_ATTR_SCHEDULE (Набор параметров планового графика)
  ------------------------------------------------------
  procedure ins_FCT_ATTR_SCHEDULE (in_Deal_Code               in varchar2,
                                   in_TypeRePay_Code          in varchar2,
                                   in_External_TypeRePay_Code in varchar2,
                                   in_TypeSCH                 in varchar2,
                                   in_Periodicity             in varchar2,
                                   in_Count_Period            in varchar2,
                                   in_Month_Pay               in varchar2,
                                   in_Day_Pay                 in varchar2,
                                   in_Is_WorkDay              in varchar2,
                                   in_Grace_Periodicity       in varchar2,
                                   in_Grace_Count_Period      in varchar2,
                                   in_Sum_RePay               in varchar2,
                                   in_DT_Open_Per             in varchar2,
                                   in_DT_Close_Per            in varchar2,
                                   in_Rec_Status              in varchar2,
                                   in_DT                      in varchar2,
                                   in_SysMoment               in varchar2,
                                   in_Ext_File                in varchar2
                                  ) is
   begin
     insert into Ldr_Infa.fct_attr_schedule(Deal_Code, TypeRePay_Code, External_TypeRePay_Code, TypeSCH,
                                            Periodicity, Count_Period, Month_Pay, Day_Pay, Is_WorkDay,
                                            Grace_Periodicity, Grace_Count_Period, Sum_RePay, DT_Open_Per, DT_Close_Per,
                                            Rec_Status, DT, SysMoment, Ext_File)
                                    values (in_Deal_Code, in_TypeRePay_Code, in_External_TypeRePay_Code, in_TypeSCH,
                                            in_Periodicity, in_Count_Period, in_Month_Pay, in_Day_Pay, in_Is_WorkDay,
                                            in_Grace_Periodicity, in_Grace_Count_Period, in_Sum_RePay, in_DT_Open_Per, in_DT_Close_Per,
                                            in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                            
   end;

  ------------------------------------------------------
  --Запись данных в FCT_REPAYSCHEDULE_DM (Строка планового графика ? таблица с неактуальными данными)
  --DT_Open,
  ------------------------------------------------------
  procedure ins_FCT_REPAYSCHEDULE_DM (in_Deal_Code       in varchar2,
                                      in_Code            in varchar2,
                                      in_Dt_Open         in varchar2,
                                      in_TypeSchedule    in varchar2,
                                      in_TypeRepay_Code  in varchar2,
                                      in_MovingDirection in varchar2,
                                      in_FinStr_Code     in varchar2,
                                      in_EventSum        in varchar2,
                                      in_FinStrAmount    in varchar2,
                                      in_DealSum         in varchar2,
                                      in_Rec_Status      in varchar2,
                                      --in_DT              in varchar2,
                                      in_SysMoment       in varchar2,
                                      in_Ext_File        in varchar2
                                      ) is
   begin
     insert into Ldr_Infa.fct_repayschedule_dm(Deal_Code, Code, TypeSchedule, TypeRepay_Code,
                                               MovingDirection, FinStr_Code, EventSum, FinStrAmount, DealSum,
                                               Rec_Status, DT, SysMoment, Ext_File)
                                       values (in_Deal_Code, in_Code, in_TypeSchedule, in_TypeRepay_Code,
                                               in_MovingDirection, in_FinStr_Code, in_EventSum, in_FinStrAmount, in_DealSum,
                                               in_Rec_Status, in_Dt_Open,--in_DT,
                                               in_SysMoment, in_Ext_File);
commit;                                               
   end;

  ------------------------------------------------------
  --Запись данных в FCT_REPAYSCHEDULE_H (Строка планового графика ? таблица с актуальными данными)
  --in_Prev_Dt_Open
  ------------------------------------------------------
  procedure ins_FCT_REPAYSCHEDULE_H (in_Deal_Code        in varchar2,
                                     in_TypeRepay_Code    in varchar2,
                                     in_FinStr_Code      in varchar2,
                                     in_Code             in varchar2,
                                     in_Pay_Date          in varchar2,
                                     in_TypeSchedule     in varchar2,
                                     in_MovingDirection  in varchar2,
                                     in_EventSum        in varchar2,
                                     in_FinStrAmount     in varchar2,
                                     in_DealSum          in varchar2,
                                     in_Prev_DT_Open     in varchar2,
                                     in_Rec_Status       in varchar2,
                                     in_DT               in varchar2,
                                     in_SysMoment        in varchar2,
                                     in_Ext_File         in varchar2
                                     ) is
   begin
     insert into Ldr_Infa.fct_repayschedule_h(Deal_Code, TypeRepay_Code, FinStr_Code, Code, Pay_Date, TypeSchedule,
                                             MovingDirection, EventSum, FinStrAmount, DealSum, Prev_DT_Open,
                                             Rec_Status, DT, SysMoment, Ext_File)
                                     values (in_Deal_Code, in_TypeRepay_Code, in_FinStr_Code, in_Code, in_Pay_Date, in_TypeSchedule,
                                             in_MovingDirection, in_EventSum, in_FinStrAmount, in_DealSum, in_Prev_DT_Open,
                                             in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                             
   end;

  -----------------------------------------------------
  --FCT_PROVISION_OBJECT (Объект залога)
  -----------------------------------------------------
  procedure ins_FCT_PROVISION_OBJECT (in_CODE          in varchar2,
                                      in_TypeProvision_Object_Code in varchar2,
                                      in_finstr_code   in varchar2,
                                      in_Amount        in varchar2,
                                      in_Balance_value in varchar2,
                                      in_Market_value  in varchar2,
                                      in_Note          in varchar2,
                                      in_Rec_Status    in varchar2,
                                      in_DT            in varchar2,
                                      in_SysMoment     in varchar2,
                                      in_Ext_File      in varchar2
                                     ) is
   begin
     insert into Ldr_Infa.FCT_PROVISION_OBJECT(CODE,
                                               TypeProvision_Object_Code,
                                               finstr_code,
                                               Amount,
                                               Balance_value,
                                               Market_value,
                                               Note,
                                               Rec_Status,
                                               DT,
                                               SysMoment,
                                               Ext_File)
                                        values(in_CODE,
                                               in_TypeProvision_Object_Code,
                                               in_finstr_code,
                                               in_Amount,
                                               in_Balance_value,
                                               in_Market_value,
                                               in_Note,
                                               in_Rec_Status,
                                               in_DT,
                                               in_SysMoment,
                                               in_Ext_File);
commit;                                               
   end;
  -----------------------------------------------------
  --FCT_PROVISION_OBJ_ATTR (Значение атрибута по объекту обеспечения)
  -----------------------------------------------------

  procedure ins_FCT_PROVISION_OBJ_ATTR (in_Object_Code in varchar2,
                                        in_Object_TypeAttr_Code in varchar2,
                                        in_FinSTR_Code in varchar2,
                                        in_Value       in varchar2,
                                        in_Rec_Status  in varchar2,
                                        in_DT          in varchar2,
                                        in_SysMoment   in varchar2,
                                        in_Ext_File    in varchar2
                                       ) is
   begin
     insert into Ldr_Infa.FCT_PROVISION_OBJ_ATTR (Object_Code,
                                                  Object_TypeAttr_Code,
                                                  FinSTR_Code,
                                                  Value,
                                                  Rec_Status,
                                                  DT,
                                                  SysMoment,
                                                  Ext_File)
                                           values(in_Object_Code,
                                                  in_Object_TypeAttr_Code,
                                                  in_FinSTR_Code,
                                                  in_Value,
                                                  in_Rec_Status,
                                                  in_DT,
                                                  in_SysMoment,
                                                  in_Ext_File);
commit;                                                  
   end;
  -----------------------------------------------------
  --FCT_PROVISIONDEAL (Специфические условия по сделке обеспечения)
  -----------------------------------------------------
  procedure ins_FCT_PROVISIONDEAL (in_Deal_Code   in varchar2,
                                   in_FinSTR_Code in varchar2,
                                   in_ProvisionDeal_Type_Code in varchar2,
                                   in_Quality     in varchar2,
                                   in_Summa       in varchar2,
                                   in_Rec_Status  in varchar2,
                                   in_SysMoment   in varchar2,
                                   in_Ext_File    in varchar2
                                  ) is
   begin
     insert into Ldr_Infa.FCT_PROVISIONDEAL(Deal_Code,
                                            FinSTR_Code,
                                            ProvisionDeal_Type_Code,
                                            Quality,
                                            Summa,
                                            Rec_Status,
                                            SysMoment,
                                            Ext_File)
                                     values(in_Deal_Code,
                                            in_FinSTR_Code,
                                            in_ProvisionDeal_Type_Code,
                                            in_Quality,
                                            in_Summa,
                                            in_Rec_Status,
                                            in_SysMoment,
                                            in_Ext_File);
commit;                                            
   end;

  -----------------------------------------------------
   --FCT_PROVISIONDEAL_CRED_OBJ (Связь объекта обеспечения со сделкой и с кредитной сделкой)
  -----------------------------------------------------

  procedure ins_FCT_PROVISIONDEAL_CRED_OBJ (in_ProvisionDeal_Code   in varchar2,
                                            in_CreditDeal_Code in varchar2,
                                            in_Provision_Sum in varchar2,
                                            in_Amount     in varchar2,
                                            in_Rec_Status  in varchar2,
                                            in_DT          in varchar2,
                                            in_SysMoment   in varchar2,
                                            in_Ext_File    in varchar2
                                           ) is
   begin
     insert into Ldr_Infa.FCT_PROVISIONDEAL_CRED_OBJ(ProvisionDeal_Code,
                                                     CreditDeal_Code,
                                                     Provision_Sum,
                                                     Amount,
                                                     Rec_Status,
                                                     DT,
                                                     SysMoment,
                                                     Ext_File)
                                              values(in_ProvisionDeal_Code,
                                                     in_CreditDeal_Code,
                                                     in_Provision_Sum,
                                                     in_Amount,
                                                     in_Rec_Status,
                                                     in_DT,
                                                     in_SysMoment,
                                                     in_Ext_File);
commit;                                                     
   end;
  -----------------------------------------------------
  --Запись данных в Ldr_Infa.ASS_DEAL_MIGRATION ()
  -----------------------------------------------------
  procedure ins_ASS_DEAL_MIGRATION ( in_Deal_Cur_Code    in varchar2,
                                     in_Deal_Prev_Code   in varchar2,
                                     in_Department_Cur_Code  in varchar2,
                                     in_Department_Prev_Code in varchar2,
                                     in_Migration_Type   in varchar2,
                                     in_Rec_Status       in varchar2,
                                     in_DT               in varchar2,
                                     in_SysMoment        in varchar2,
                                     in_Ext_File         in varchar2)is
  begin
    null;
    insert into Ldr_Infa.ass_deal_migration (Deal_Cur_Code,
                                             Deal_Prev_Code,
                                             Department_Cur_Code_department,
                                             Department_Prev_Code_departmen,
                                             Migration_Type,
                                             Rec_Status,
                                             DT,
                                             SysMoment,
                                             Ext_File)
                                     values (in_Deal_Cur_Code,
                                             in_Deal_Prev_Code,
                                             in_Department_Cur_Code,
                                             in_Department_Prev_Code,
                                             in_Migration_Type,
                                             in_Rec_Status,
                                             in_DT,
                                             in_SysMoment,
                                             in_Ext_File);
commit;                                             
  end;
  procedure clearAll(in_Type number default 0) is
  begin
    --Очистка
    execute immediate 'truncate table Ldr_Infa.ass_fct_deal';     -- Связь между сделками
    execute immediate 'truncate table Ldr_Infa.fct_deal';         -- Общие условия по сделке
    execute immediate 'truncate table Ldr_Infa.fct_mbkcredeal';   -- Специфические условия по межбанковскому кредиту   -- Реализовано
    execute immediate 'truncate table Ldr_Infa.fct_mbkdepdeal';   -- Специфические условия по межбанковскому депозиту  -- Реализовано
    execute immediate 'truncate table Ldr_Infa.fct_prolongation';

    execute immediate 'truncate table Ldr_Infa.ASS_ACCOUNTDEAL';      --Связь счета со сделкой
    execute immediate 'truncate table Ldr_Infa.FCT_DEALRISK';         --Риск по сделке
    execute immediate 'truncate table Ldr_Infa.FCT_PROCRATE_DEAL';    --Ставка по сделке
    execute immediate 'truncate table Ldr_Infa.FCT_SUBJECT_ROLEDEAL'; --Роль субъекта в сделке

    --Третья очередь
    execute immediate 'truncate table Ldr_Infa.ASS_DEAL_CAT_VAL';     --Связь сделки со значением ограниченного доп.атрибута
    execute immediate 'truncate table Ldr_Infa.FCT_DEAL_INDICATOR';   --Значение свободного доп.атрибута сделки
    --Четвертая очередь
    execute immediate 'truncate table Ldr_Infa.ASS_CARRYDEAL';        -- Связь проводки со сделкой
    execute immediate 'truncate table Ldr_Infa.FCT_DEAL_CARRY';       -- Проводка сделочной модели
    execute immediate 'truncate table Ldr_Infa.FCT_DM_CARRY_ASS';     -- Связь сделки и проводки сделочной модели
    execute immediate 'truncate table Ldr_Infa.FCT_DEAL_RST';         -- Остатки и обороты в разрезе сделок и счетов
    --Пятая очередь
    execute immediate 'truncate table Ldr_Infa.FCT_ATTR_SCHEDULE';    -- Набор параметров планового графика
    execute immediate 'truncate table Ldr_Infa.FCT_REPAYSCHEDULE_DM'; -- Строка планового графика ? таблица с неактуальными данными
    execute immediate 'truncate table Ldr_Infa.FCT_REPAYSCHEDULE_H';

    execute immediate 'truncate table Ldr_Infa.FCT_PROVISIONDEAL';
    execute immediate 'truncate table Ldr_Infa.FCT_PROVISIONDEAL_CRED_OBJ';
    execute immediate 'truncate table Ldr_Infa.FCT_PROVISION_OBJECT';

    execute immediate 'truncate table Ldr_Infa.DET_SYSTEM';

    execute immediate 'truncate table Ldr_Infa.FCT_CREDITLINEDEAL';
    execute immediate 'truncate table Ldr_Infa.DET_DEAL_CAT';
    execute immediate 'truncate table Ldr_Infa.DET_DEAL_CAT_VAL';
    execute immediate 'truncate table Ldr_Infa.DET_DEAL_TYPEATTR';
    execute immediate 'truncate table Ldr_Infa.DET_DEPARTMENT';
    execute immediate 'truncate table Ldr_Infa.DET_MEASUREMENT_UNIT';
    execute immediate 'truncate table Ldr_Infa.DET_RISK';
    execute immediate 'truncate table Ldr_Infa.DET_PROCBASE';
    execute immediate 'truncate table Ldr_Infa.DET_PROVISIONDEAL_TYPE';
    execute immediate 'truncate table Ldr_Infa.DET_SUBJECT_ROLEDEAL';     --Справочник ролей субъекта в сделке - Не требуется
    execute immediate 'truncate table Ldr_Infa.Det_Roleaccount_Deal';
    execute immediate 'truncate table Ldr_Infa.ASS_DEAL_MIGRATION';

    execute immediate 'truncate table ldr_infa.ass_halfcarrydeal';
    execute immediate 'truncate table ldr_infa.fct_lc';
    execute immediate 'truncate table ldr_infa.FCT_HEDG_CHG'; --хеджирование
    
    --События и ошибки по ним
    /*if in_type = 0 then
    delete from DQB_BP_EVENT_ERROR_DBT;commit;  -- Ошибки произошедьшии при событии
    delete from DQB_BP_EVENT_ATTR_DBT;commit;   -- Аттрибуты
    delete from DQB_BP_EVENT_DBT;commit;      -- События
    end if;*/
  end;

  -----------------------------------------------------
  --Запись данных в DET_DEAL_CAT (Ограниченный доп.атрибут сделки)
  -----------------------------------------------------
  procedure ins_DET_DEAL_CAT ( in_Code_Deal_Cat    in varchar2,
                               in_Name_Deal_Cat in varchar2,
                               in_is_MultiValued in varchar2,
                               in_Rec_Status       in varchar2,
                               in_DT               in varchar2,
                               in_SysMoment        in varchar2,
                               in_Ext_File         in varchar2) is
  begin
    null;
    insert into Ldr_Infa.DET_DEAL_CAT(Code_Deal_Cat, Name_Deal_Cat, is_MultiValued,
                                      Rec_Status, DT, SysMoment, Ext_File)
                               values(in_Code_Deal_Cat, in_Name_Deal_Cat, in_is_MultiValued,
                                      in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                      
  end;

  -----------------------------------------------------
  -- Запись данных в DET_DEAL_CAT_VAL  (Значение ограниченного доп.атрибута сделки)
  -----------------------------------------------------
  procedure ins_DET_DEAL_CAT_VAL ( in_Deal_Cat_Code     in varchar2,
                                   in_Code_Deal_Cat_Val in varchar2,
                                   in_Name_Deal_Cat_Val in varchar2,
                                   in_Rec_Status        in varchar2,
                                   in_DT                in varchar2,
                                   in_SysMoment         in varchar2,
                                   in_Ext_File          in varchar2) is
   begin
       insert into Ldr_Infa.DET_DEAL_CAT_VAL( Deal_Cat_Code, Code_Deal_Cat_Val, Name_Deal_Cat_Val,
                                              Rec_Status, DT, SysMoment, Ext_File)
                                      values( in_Deal_Cat_Code, in_Code_Deal_Cat_Val, in_Name_Deal_Cat_Val,
                                              in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                              
   end;

  -----------------------------------------------------
  --Добавление ограниченного аттрибута и значений
  -----------------------------------------------------
  procedure add_DET_DEAL_CAT  (in_Object_Type in number,
                               in_Rec_Status  in varchar2,
                               in_DT          in varchar2,
                               in_SysMoment   in varchar2,
                               in_Ext_File    in varchar2) is
  begin
    for rec_Cat in (Select g.t_objecttype objecttype,
                           g.t_groupid groupid,
                           g.t_objecttype || '|C|' || g.t_groupid Code_Deal_Cat,
                           upper(g.t_name) Name_Deal_Cat,
                           decode(g.t_type, chr(88), 0, 1) is_MultiValued,
                           g.*
                      from dobjgroup_dbt g
                     where g.t_objecttype = in_Object_Type) loop
      ins_DET_DEAL_CAT(rec_Cat.Code_Deal_Cat, rec_Cat.Name_Deal_Cat, rec_Cat.Is_Multivalued,
                       in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
        for rec_Attr in (select a.t_objecttype || '|C|' || a.t_groupid Deal_Code_Cat,
                                a.t_objecttype || '|C|' || a.t_groupid ||/* '|' || a.t_attrid ||*/ '#' || upper(a.t_name) Code_Deal_Cat_Val,
                                substr(a.t_fullname,1,250) Name_Deal_Cat_Val,
                                a.*
                           from dobjattr_dbt a
                          where decode(a.t_opendate, to_date('01.01.0001','dd.mm.yyyy'), to_date('01.01.1980', 'dd.mm.yyyy'), a.t_opendate) <= sysdate
                                and decode(a.t_closedate, to_date('01.01.0001','dd.mm.yyyy'), to_date('01.01.3100', 'dd.mm.yyyy'), a.t_closedate) >= sysdate
                                and a.t_objecttype = rec_Cat.Objecttype
                                and a.t_groupid = rec_Cat.Groupid) loop
            ins_DET_DEAL_CAT_VAL(rec_Attr.Deal_Code_Cat,rec_Attr.Code_Deal_Cat_Val,rec_Attr.Name_Deal_Cat_Val,
                                 in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
        end loop;
    end loop;
    if in_Object_Type = 103 then

      ins_DET_DEAL_CAT('EXTERNAL_DEALTYPE', 'Классы сделок, допустиые в системе', 0,
                       in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);

        for rec_Attr in (select 'EXTERNAL_DEALTYPE' Deal_Code_Cat,
                                'EXTERNAL_DEALTYPE#1' Code_Deal_Cat_Val,
                                'Размещенный межбанковский депозит' Name_Deal_Cat_Val
                           from dual
                         union all
                         select 'EXTERNAL_DEALTYPE' Deal_Code_Cat,
                                'EXTERNAL_DEALTYPE#2' Code_Deal_Cat_Val,
                                'Размещенный межбанковский кредит' Name_Deal_Cat_Val
                           from dual
                         union all
                         select 'EXTERNAL_DEALTYPE' Deal_Code_Cat,
                                'EXTERNAL_DEALTYPE#3' Code_Deal_Cat_Val,
                                'Привлеченный межбанковский депозит' Name_Deal_Cat_Val
                           from dual
                         union all
                         select 'EXTERNAL_DEALTYPE' Deal_Code_Cat,
                                'EXTERNAL_DEALTYPE#4' Code_Deal_Cat_Val,
                                'Привлеченный межбанковский кредит' Name_Deal_Cat_Val
                           from dual) loop
            ins_DET_DEAL_CAT_VAL(rec_Attr.Deal_Code_Cat,rec_Attr.Code_Deal_Cat_Val,rec_Attr.Name_Deal_Cat_Val,
                                 in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
        end loop;
    end if;

    if in_Object_Type = 4626 then

      ins_DET_DEAL_CAT('0000#BISQUIT#ТИПДОГП', 'ТИПДОГП', 0,
                       in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);

        for rec_Attr in (select '0000#BISQUIT#ТИПДОГП' Deal_Code_Cat,
                                '0000#BISQUIT#ТИПДОГП#МЕЖБАНК' Code_Deal_Cat_Val,
                                'МЕЖБАНК' Name_Deal_Cat_Val
                           from dual) loop
            ins_DET_DEAL_CAT_VAL(rec_Attr.Deal_Code_Cat,rec_Attr.Code_Deal_Cat_Val,rec_Attr.Name_Deal_Cat_Val,
                                 in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
        end loop;
    end if;
  end;

  -----------------------------------------------------
  --Запись данных в DET_DEAL_TYPEATTR  (Свободный доп.атрибут по сделке)
  -----------------------------------------------------
  procedure ins_DET_DEAL_TYPEATTR ( in_Code           in varchar2,
                                    in_Name           in varchar2,
                                    in_Is_Money_Value in varchar2,
                                    in_Data_Type      in varchar2,
                                    in_Rec_Status     in varchar2,
                                    in_DT             in varchar2,
                                    in_SysMoment      in varchar2,
                                    in_Ext_File       in varchar2) is
   begin
       insert into Ldr_Infa.DET_DEAL_TYPEATTR( Code, Name, Is_Money_Value, Data_Type,
                                               Rec_Status, DT, SysMoment, Ext_File)
                                       values( in_Code, in_Name, in_Is_Money_Value, in_Data_Type,
                                               in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                               
   end;

  -----------------------------------------------------
  --Заполнить справочник DET_DEAL_TYPEATTR  (Свободный доп.атрибут по сделке)
  -----------------------------------------------------
  procedure add_DET_DEAL_TYPEATTR ( in_Object_Type in number,
                                    in_Rec_Status     in varchar2,
                                    in_DT             in varchar2,
                                    in_SysMoment      in varchar2,
                                    in_Ext_File       in varchar2) is
   begin
       for rec_Note in (select n.t_objecttype || '|T|' || n.t_notekind Code,
                               n.t_name Name,
                               case when n.t_notetype = 25 then 1 else 0 end Is_Money_Value,
                               case when n.t_notetype in (0,1,2,3,4,25) then 1 -- 1 ? число
                                    when n.t_notetype in (9) then 2            -- 2 ? дата;
                                    when n.t_notetype in (7) then 3            -- 3 ? строка.
                               end Data_Type/*,
                               n.**/
                         from dnotekind_dbt n
                        where n.t_objecttype = in_Object_Type
                        union all
                        select 'CLOSE-DATE', 'Дата фактического закрытия договора', 0, 2 from dual
                        union all
                        /* 2020-09-01 AS добавлен свободный реквизит UID-BKI в рамках BIQ-6948 */
                        select 'UID-BKI', 'уникальный идентификатор договора (сделки) для БКИ', 0, 3 from dual
                        union all
                        select 'CRED-OFFSET', 'Режим смещения дат', 0, 3 from dual
                        union all
                        select 'DELAY-OFFSET-INT', 'Режим смещения оконч.периода(проценты)', 0, 3 from dual
                        union all
                        select '0000#BISQUIT#ДАТПЕРЕХ', 'ДАТПЕРЕХ', 0, 3 from dual
                        union all
                        select '0000#BISQUIT#INT-OFFSET', 'INT-OFFSET', 0, 3 from dual
                        union all
                        select 'FLOATING-RATE-FORMULA', 'Формула плавающей ставки', 0, 3 from dual) loop
         ins_DET_DEAL_TYPEATTR(rec_Note.Code,rec_Note.Name,rec_Note.Is_Money_Value,rec_Note.Data_Type,
                               in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
       end loop;
   end;
  -----------------------------------------------------
  --Запись данных в DET_DEPARTMENT  (Подразделение Банка)
  -----------------------------------------------------
  procedure ins_DET_DEPARTMENT ( in_Code_Department        in varchar2,
                                 in_Name_Department        in varchar2,
                                 in_Rank                   in varchar2,
                                 in_is_HaveBalance         in varchar2,
                                 in_Currency_curr_code_txt in varchar2,
                                 in_Rec_Status             in varchar2,
                                 in_DT                     in varchar2,
                                 in_SysMoment              in varchar2,
                                 in_Ext_File               in varchar2) is
   begin
       insert into Ldr_Infa.DET_DEPARTMENT( Code_Department, Name_Department, Rank, is_HaveBalance, Currency_curr_code_txt,
                                            Rec_Status, DT, SysMoment, Ext_File)
                                    values( in_Code_Department, in_Name_Department, in_Rank, in_is_HaveBalance, in_Currency_curr_code_txt,
                                            in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                            
   end;

  -----------------------------------------------------
  --Добавим DET_DEPARTMENT  (Подразделение Банка)
  -----------------------------------------------------
  procedure add_DET_DEPARTMENT ( in_Rec_Status             in varchar2,
                                 in_DT                     in varchar2,
                                 in_SysMoment              in varchar2,
                                 in_Ext_File               in varchar2) is
   begin
     for rec_Dep in (select d.t_name Code_Department,
                            p.t_shortname Name_Department,
                            d.t_parentcode Rank,
                            decode(d.t_nodetype, 1, 1,0) is_HaveBalance,
                            '643' Currency_curr_code_txt
                       from ddp_dep_dbt d
                            inner join dparty_dbt p on p.t_partyid = d.t_partyid) loop
        ins_DET_DEPARTMENT(rec_Dep.Code_Department, rec_Dep.Name_Department, rec_Dep.Rank,rec_Dep.Is_Havebalance, rec_Dep.Currency_Curr_Code_Txt,
                           in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                           
     end loop;
   end;
  -----------------------------------------------------
  --Запись данных в DET_MEASUREMENT_UNIT  (Единица измерения)
  -----------------------------------------------------
  procedure ins_DET_MEASUREMENT_UNIT ( in_Code        in varchar2,
                                       in_Name        in varchar2,
                                       in_Rec_Status  in varchar2,
                                       in_DT          in varchar2,
                                       in_SysMoment   in varchar2,
                                       in_Ext_File    in varchar2) is
   begin
       insert into Ldr_Infa.DET_MEASUREMENT_UNIT( Code, Name,
                                                  Rec_Status, DT, SysMoment, Ext_File)
                                          values( in_Code, in_Name,
                                                  in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                                  
   end;

  -----------------------------------------------------
  --Добавим данные DET_MEASUREMENT_UNIT  (Единица измерения)
  -----------------------------------------------------
  procedure add_DET_MEASUREMENT_UNIT ( in_Rec_Status  in varchar2,
                                       in_DT          in varchar2,
                                       in_SysMoment   in varchar2,
                                       in_Ext_File    in varchar2) is
   begin
     for rec_dmu in (select m.t_measurecode Code,
                            m.t_name Name
                       from dmeasure_dbt m) loop
        ins_DET_MEASUREMENT_UNIT(rec_dmu.code, rec_dmu.name,
                                 in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                 
     end loop;
   end;

  -----------------------------------------------------
  --Запись данных в DET_RISK  (Группа риска)
  -----------------------------------------------------
  procedure ins_DET_RISK ( in_TypeRisk_Code in varchar2,
                           in_Code_RiskCat  in varchar2,
                           in_Name_RiskCat  in varchar2,
                           in_Min_Proc      in varchar2,
                           in_Max_Proc      in varchar2,
                           in_Rec_Status    in varchar2,
                           in_DT            in varchar2,
                           in_SysMoment     in varchar2,
                           in_Ext_File      in varchar2) is
   begin
       insert into Ldr_Infa.DET_RISK( TypeRisk_Code, Code_RiskCat, Name_RiskCat, Min_Proc, Max_Proc,
                                      Rec_Status, DT, SysMoment, Ext_File )
                              values( in_TypeRisk_Code, in_Code_RiskCat, in_Name_RiskCat, in_Min_Proc, in_Max_Proc,
                                      in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                      
   end;

  -----------------------------------------------------
  --Добавим в DET_RISK  (Группа риска)
  -----------------------------------------------------
  procedure add_DET_RISK ( in_Rec_Status    in varchar2,
                           in_DT            in varchar2,
                           in_SysMoment     in varchar2,
                           in_Ext_File      in varchar2) is
   begin
       --select * from dobjgroup_dbt g where g.t_objecttype = 669 and g.t_groupid = 19
       for rec in (select '254i' TYPERISK_CODE, '1' CODE_RISKCAT, 'Первая группа риска' NAME_RISKCAT, '0' MIN_PROC, '0' MAX_PROC from dual
                   union all
                   select '254i' TYPERISK_CODE, '2' CODE_RISKCAT, 'Вторая группа риска' NAME_RISKCAT, '1' MIN_PROC, '20' MAX_PROC from dual
                   union all
                   select '254i' TYPERISK_CODE, '3' CODE_RISKCAT, 'Третья группа риска' NAME_RISKCAT, '21' MIN_PROC, '50' MAX_PROC from dual
                   union all
                   select '254i' TYPERISK_CODE, '4' CODE_RISKCAT, 'Четвертая группа риска' NAME_RISKCAT, '51' MIN_PROC, '99' MAX_PROC from dual
                   union all
                   select '254i' TYPERISK_CODE, '5' CODE_RISKCAT, 'Пятая группа риска' NAME_RISKCAT, '100' MIN_PROC, '100' MAX_PROC from dual
                  ) loop
         ins_DET_RISK(rec.typerisk_code,rec.code_riskcat,rec.name_riskcat,rec.min_proc,rec.max_proc,
                      in_Rec_Status, '01-01-1980', in_SysMoment, in_Ext_File );                     
       end loop;
   end;

  -----------------------------------------------------
  --Запись данных в DET_PROCBASE  (База расчета процентов)
  -----------------------------------------------------
  procedure ins_DET_PROCBASE ( in_Code         in varchar2,
                               in_Name         in varchar2,
                               in_Days_Year    in varchar2,
                               in_Days_Month   in varchar2,
                               in_Sign_31      in varchar2,
                               in_First_Day    in varchar2,
                               in_Last_Day     in varchar2,
                               in_Null_MainSum in varchar2,
                               in_Rec_Status   in varchar2,
                               in_DT           in varchar2,
                               in_SysMoment    in varchar2,
                               in_Ext_File     in varchar2) is
   begin
       insert into Ldr_Infa.DET_PROCBASE (Code, Name, Days_Year, Days_Month, Sign_31, First_Day, Last_Day, Null_MainSum,
                                          Rec_Status, DT, SysMoment, Ext_File)
                                   values(in_Code, in_Name, in_Days_Year, in_Days_Month, in_Sign_31, in_First_Day, in_Last_Day, in_Null_MainSum,
                                          in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                          
   end;


  -----------------------------------------------------
  --Запись данных в DET_PROCBASE  (База расчета процентов)
  -----------------------------------------------------
  procedure add_DET_PROCBASE ( in_Rec_Status   in varchar2,
                               in_DT           in varchar2,
                               in_SysMoment    in varchar2,
                               in_Ext_File     in varchar2) is
   begin
       for rec_ProcBase in (
                            select '0' CODE, '30'  DAYS_MONTH, '360' DAYS_YEAR, '01-01-1980' DT, '1' FIRST_DAY, '0' LAST_DAY, 'Дата платежа = дате начисления %% 360/30' NAME, '1' NULL_MAINSUM, '0' SIGN_31 from dual  --360/30
                            union all
                            select '2' CODE,  '31'  DAYS_MONTH, '360' DAYS_YEAR, '01-01-1980' DT, '1' FIRST_DAY, '0' LAST_DAY, 'Дата платежа = дате нач.%%(кал./360)' NAME, '0' NULL_MAINSUM, '1' SIGN_31 from dual --360/Act
                            union all
                            select '1' CODE, '31'  DAYS_MONTH, '366' DAYS_YEAR, '01-01-1980' DT, '1' FIRST_DAY, '0' LAST_DAY, 'Дата платежа = дате нач. %%(кал.дней)' NAME, '0' NULL_MAINSUM, '1' SIGN_31 from dual --Act/Act
                            union all
                            select '40' CODE, '31'  DAYS_MONTH, '365' DAYS_YEAR, '01-01-1980' DT, '1' FIRST_DAY, '0' LAST_DAY, 'Расчет %% с базой 356 дней' NAME, '0' NULL_MAINSUM, '1' SIGN_31 from dual --365/Act
                           ) loop
         ins_DET_PROCBASE ( rec_ProcBase.Code,
                            rec_ProcBase.Name,
                            rec_ProcBase.Days_Year,
                            rec_ProcBase.Days_Month,
                            rec_ProcBase.Sign_31,
                            rec_ProcBase.First_Day,
                            rec_ProcBase.Last_Day,
                            rec_ProcBase.Null_MainSum,
                            in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
       end loop;
   end;
  -----------------------------------------------------
  --Запись данных в DET_PROVISIONDEAL_TYPE  (Тип сделки обеспечения)
  -----------------------------------------------------
  procedure ins_DET_PROVISIONDEAL_TYPE ( in_Code       in varchar2,
                                         in_Name       in varchar2,
                                         in_RSDH_Type  in varchar2,
                                         in_Rec_Status in varchar2,
                                         in_DT         in varchar2,
                                         in_SysMoment  in varchar2,
                                         in_Ext_File   in varchar2) is
   begin
       insert into Ldr_Infa.DET_PROVISIONDEAL_TYPE( Code, Name, RSDH_Type,
                                                    Rec_Status, DT, SysMoment, Ext_File)
                                            values( in_Code, in_Name, in_RSDH_Type,
                                                    in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                                    
   end;

  -----------------------------------------------------
  --Запись данных в DET_PROVISIONDEAL_TYPE  (Тип сделки обеспечения)
  -----------------------------------------------------
  procedure add_DET_PROVISIONDEAL_TYPE ( in_Rec_Status in varchar2,
                                         in_DT         in varchar2,
                                         in_SysMoment  in varchar2,
                                         in_Ext_File   in varchar2) is
   begin
       for rec in (select '2' RSDH_TYPE, '100' Code, 'Договор залога ценных бумаг' NAME, '01-01-1980' DT from dual) loop
         ins_DET_PROVISIONDEAL_TYPE(rec.code, rec.name,rec.rsdh_type,
                                    in_Rec_Status, rec.Dt, in_SysMoment, in_Ext_File);
       end loop;
   end;
  -----------------------------------------------------
  --Запись данных в FCT_CREDITLINEDEAL  (Специфические условия по кредитной линии)
  -----------------------------------------------------
  procedure ins_FCT_CREDITLINEDEAL ( in_Deal_Code     in varchar2,
                                     in_FinStr_Code   in varchar2,
                                     in_Type_By_Limit in varchar2,
                                     in_Type_Contract in varchar2,
                                     in_PaymentLimit  in varchar2,
                                     in_DebtLimit     in varchar2,
                                     in_Rec_Status    in varchar2,
                                     in_DT            in varchar2,
                                     in_SysMoment     in varchar2,
                                     in_Ext_File      in varchar2) is
   begin
       insert into Ldr_Infa.FCT_CREDITLINEDEAL ( Deal_Code, FinStr_Code, Type_By_Limit, Type_Contract, PaymentLimit, DebtLimit,
                                                 Rec_Status, DT, SysMoment, Ext_File)
                                         values( in_Deal_Code, in_FinStr_Code, in_Type_By_Limit, in_Type_Contract, in_PaymentLimit, in_DebtLimit,
                                                 in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                                 
   end;

  -----------------------------------------------------
  --Запись данных в DET_SUBJECT_ROLEDEAL  ()
  -----------------------------------------------------
  procedure ins_DET_SUBJECT_ROLEDEAL ( in_Code           in varchar2,
                                       in_Name           in varchar2,
                                       in_Rsdh_Role_Code in varchar2,
                                       in_Rec_Status     in varchar2,
                                       in_DT             in varchar2,
                                       in_SysMoment      in varchar2,
                                       in_Ext_File       in varchar2) is
   begin
       insert into Ldr_Infa.DET_SUBJECT_ROLEDEAL ( Code, Name, Rsdh_Role_Code,
                                                 Rec_Status, Dt,Sysmoment,Ext_File)
                                         values( in_Code, in_Name, in_Rsdh_Role_Code,
                                                 in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                                 
   end;

  -----------------------------------------------------
  --Запишем DET_SUBJECT_ROLEDEAL
  -----------------------------------------------------
  procedure add_DET_SUBJECT_ROLEDEAL ( in_Rec_Status     in varchar2,
                                       in_DT             in varchar2,
                                       in_SysMoment      in varchar2,
                                       in_Ext_File       in varchar2) is
   begin
     for rec in (select 'КОНТРАГЕНТ' Code, 'КОНТРАГЕНТ' Name, '' Rsdh_Role_Code from dual
                 union all
                 select 'БРОКЕР' Code, 'БРОКЕР' Name, '' Rsdh_Role_Code from dual
                 union all
                 select 'КЛИЕНТ' Code, 'КЛИЕНТ' Name, '' Rsdh_Role_Code from dual
                 union all
                 select 'ТРАЙДЕР' Code, 'ТРАЙДЕР' Name, '' Rsdh_Role_Code from dual
                 union all
                 select 'ДЕПОЗИТАРИЙ' Code, 'ДЕПОЗИТАРИЙ' Name, '' Rsdh_Role_Code from dual
                 union all
                 select 'ТОРГОВАЯ ПЛОЩАДКА' Code, 'ТОРГОВАЯ ПЛОЩАДКА' Name, '' Rsdh_Role_Code from  dual
                 union all
                 select 'ЗАЛОГОДАТЕЛЬ' Code, 'ЗАЛОГОДАТЕЛЬ' Name, '' Rsdh_Role_Code from dual
                 --union all
                 --select 'КОНТРАГЕНТ ПО ДОГОВОРУ CSA' Code, 'КОТНРАГЕНТ ПО ДОГОВОРУ CSA' Name, '' Rsdh_Role_Code from dual
                ) loop
       ins_DET_SUBJECT_ROLEDEAL( rec.Code, rec.Name, rec.Rsdh_Role_Code,
                                 in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
      end loop;
   end;

  -----------------------------------------------------
  --Запись данных в DET_SYSTEM  ()
  -----------------------------------------------------
  procedure add_DET_SYSTEM ( in_Rec_Status     in varchar2,
                             in_DT             in varchar2,
                             in_SysMoment      in varchar2,
                             in_Ext_File       in varchar2 ) is
   begin
       insert into Ldr_Infa.DET_SYSTEM ( System_Code, System_Name, Rank,
                                                 Rec_Status, Dt,Sysmoment,Ext_File)
                                         values( System_RS, 'СОФР', '600',
                                                 in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;                                                 
   end;

  -----------------------------------------------------
  --Запись данных в DET_ROLEACCOUNT_DEAL  ()
  -----------------------------------------------------
  procedure ins_DET_ROLEACCOUNT_DEAL ( in_Code           in varchar2,
                                       in_Name           in varchar2,
                                       in_oRole_Code in varchar2,
                                       in_Rec_Status     in varchar2,
                                       in_DT             in varchar2,
                                       in_SysMoment      in varchar2,
                                       in_Ext_File       in varchar2) is
   begin
       insert into Ldr_Infa.DET_ROLEACCOUNT_DEAL( Code, Name, oRole_Code,
                                                 Rec_Status, Dt,Sysmoment,Ext_File)
                                         values( in_Code, in_Name, in_oRole_Code,
                                                 in_Rec_Status, DateToChar(DT_BEGIN)/*in_DT*/, in_SysMoment, in_Ext_File);
commit;                                                 
   end;
  -----------------------------------------------------
  --Запись данных в DET_ROLEACCOUNT_DEAL  ()
  -----------------------------------------------------
  procedure add_DET_ROLEACCOUNT_DEAL ( in_Rec_Status     in varchar2,
                             in_DT             in varchar2,
                             in_SysMoment      in varchar2,
                             in_Ext_File       in varchar2 ) is
   begin
   --
     for rec in (select distinct c.t_id,
                          c.t_number,
                          upper(c.t_code) code,
                          c.t_name name,
                          '0' OROLE_CODE
                    from dmcdoccat_dbt dc
                         inner join dmccateg_dbt c on c.t_id != 500 and c.t_id = dc.t_catid
                  where dc.t_dockind = 102) loop
         ins_DET_ROLEACCOUNT_DEAL(rec.code, rec.name, rec.orole_code, in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
      end loop;
      -- Категории учета по драг металам
     for rec in ( with t as (select distinct dvn.t_dockind
                               from ddvndeal_dbt dvn
                         inner join ddvnfi_dbt     nFI on nFI.t_Type = 0 and nFI.t_DealID = dvn.t_ID)
                   select distinct c.t_id,
                          c.t_number,
                          upper(c.t_code) code,
                          c.t_name name,
                          '0' OROLE_CODE
                    from t
                         inner join dmcdoccat_dbt dc on dc.t_dockind = t.t_dockind
                         inner join dmccateg_dbt c on c.t_id = dc.t_catid
                   where not exists (select 1 -- исключим дубли по 102 коду
                                      from dmcdoccat_dbt dc0
                                           inner join dmccateg_dbt c0 on c0.t_id = dc0.t_catid
                                    where dc0.t_dockind = 102
                                          and dc0.t_catid = dc.t_catid)
                            ) loop
         ins_DET_ROLEACCOUNT_DEAL(rec.code, rec.name, rec.orole_code, in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
      end loop;

     for rec in (select distinct c.t_id,
                          c.t_number,
                          upper(c.t_code) code,
                          c.t_name name,
                          '0' OROLE_CODE
                    from dmcdoccat_dbt dc
                         inner join dmccateg_dbt c on c.t_id = dc.t_catid
                  where dc.t_dockind = 4626) loop
         ins_DET_ROLEACCOUNT_DEAL(rec.code, rec.name, rec.orole_code, in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
      end loop;
   end;

  -----------------------------------------------------
  --Запись данных в FCT_LC  (Аккредитивы)
  -----------------------------------------------------
  procedure ins_FCT_LC ( in_Deal_Code            in varchar2,
                         in_MovingDirection in varchar2,
                         in_TypeLC          in varchar2,
                         --in_SubTypeLC       in varchar2,
                         --in_TypeExecute     in varchar2,
                         in_AmountLC        in varchar2,
                         in_NumberLC        in varchar2,
                         in_Beneficiary_Code_Subject in varchar2,
                         in_Principal_Code_Subject   in varchar2,
                         in_Bank_Beneficiary_Code_Subject in varchar2,
                         in_Bank_Principal_Code_Subject in varchar2,
                         in_FinStr_Code    in varchar2,
                         in_Rec_Status     in varchar2,
                         in_DT             in varchar2,
                         in_SysMoment      in varchar2,
                         in_Ext_File       in varchar2) is
   begin

       insert into Ldr_Infa.FCT_LC( Deal_Code, MovingDirection, TypeLC, --SubTypeLC, TypeExecute,
       AmountLC,
                                    NumberLC, Beneficiary_Code_Subject, Principal_Code_Subject,
                                    Bank_Beneficiary_Code_Subject, Bank_Principal_Code_Subject, FinStr_Code,
                                    Rec_Status, DT, SysMoment, Ext_File)
                            values( in_Deal_Code, in_MovingDirection, in_TypeLC,--in_SubTypeLC, in_TypeExecute,
                             in_AmountLC,
                                    in_NumberLC, in_Beneficiary_Code_Subject, in_Principal_Code_Subject,
                                    in_Bank_Beneficiary_Code_Subject, in_Bank_Principal_Code_Subject, in_FinStr_Code,
                                    in_Rec_Status, in_DT, in_SysMoment, in_Ext_File);
commit;

   end;
   
  -----------------------------------------------------
  --Запись данных в FCT_HEDG_CHG  (Хеджирование)
  -----------------------------------------------------
  procedure ins_FCT_HEDG_CHG (in_dt                       in varchar2,
                              in_code                     in varchar2, 
                              in_deal_code                in varchar2, 
                              in_finstr_code              in varchar2, 
                              in_asudr_deal_code          in varchar2, 
                              in_portfolio_code           in varchar2, 
                              in_sub_portf_code           in varchar2, 
                              in_currency_curr_code_txt   in varchar2, 
                              in_cost_on_date             in varchar2, 
                              in_prev_cost                in varchar2, 
                              in_chg_amount               in varchar2, 
                              in_deal_kind_code           in varchar2, 
                              in_hedge_rel_code           in varchar2, 
                              in_hedg_begin_dt            in varchar2, 
                              in_hedg_end_dt              in varchar2, 
                              in_hedg_tool_code           in varchar2,
                              in_tool_code_sofr           in varchar2, 
                              in_inc_acc_code             in varchar2, 
                              in_dec_acc_code             in varchar2,
                              in_inc_acc_num              in varchar2, 
                              in_dec_acc_num              in varchar2,  
                              in_rec_status               in varchar2, 
                              in_sysmoment                in varchar2, 
                              in_ext_file                 in varchar2
                              ) is
   begin
            insert into ldr_infa.FCT_HEDG_CHG(dt, code, deal_code, finstr_code, asudr_deal_code, portfolio_code,sub_portf_code, currency_curr_code_txt, 
                                        cost_on_date, prev_cost, chg_amount, deal_kind_code, hedge_rel_code, hedg_begin_dt, hedg_end_dt, 
                                        hedg_tool_code, tool_code_sofr, inc_acc_code, dec_acc_code, inc_acc_num, dec_acc_num, rec_status, sysmoment, ext_file)
            values(in_dt, in_code, in_deal_code, in_finstr_code, in_asudr_deal_code, in_portfolio_code, in_sub_portf_code, in_currency_curr_code_txt, 
                   in_cost_on_date, in_prev_cost, in_chg_amount, in_deal_kind_code, in_hedge_rel_code, in_hedg_begin_dt, in_hedg_end_dt, 
                   in_hedg_tool_code, in_tool_code_sofr, in_inc_acc_code, in_dec_acc_code, in_inc_acc_num, in_dec_acc_num, in_rec_status, in_sysmoment, in_ext_file);
commit;


   end;

  -----------------------------------------------------
  --Получить последний операционный дня
  -----------------------------------------------------
  function GetLastClosedOD (in_Department in number) return date is
    vDate Date;
  begin
        select max(c.t_curdate)
          into vDate
          from dcurdate_dbt c where c.t_branch = in_Department and c.t_isclosed = chr(88);
    return vDate;
  end;
  -----------------------------------------------------
  --Запись данных в в лог выгрузки с указанием филиала и последнего операционного дня
  -----------------------------------------------------
  procedure add_export_log ( in_id          in number,
                             in_id_pre      in number,
                             in_filcode     in varchar2,
                             in_datelastod  in date,
                             in_beg_date    in date,
                             in_end_date    in date ) is
  v_table varchar2(4000);

  begin
    v_table := 'ASS_ACCOUNTDEAL;ASS_CARRYDEAL;ASS_DEAL_CAT_VAL;ASS_DEAL_MIGRATION;ASS_FCT_DEAL;ASS_HALFCARRYDEAL;DET_DEAL_CAT;DET_DEAL_CAT_VAL;DET_DEAL_TYPEATTR;DET_DEPARTMENT;DET_MEASUREMENT_UNIT;DET_PROCBASE;DET_PROVISIONDEAL_TYPE;DET_RISK;DET_ROLEACCOUNT_DEAL;DET_SUBJECT_ROLEDEAL;DET_SYSTEM;FCT_ATTR_SCHEDULE;FCT_CREDITLINEDEAL;FCT_DEAL;FCT_DEAL_CARRY;FCT_DEAL_INDICATOR;FCT_DEALRISK;FCT_DEAL_RST;FCT_DM_CARRY_ASS;FCT_LC;FCT_MBKCREDEAL;FCT_MBKDEPDEAL;FCT_PROCRATE_DEAL;FCT_PROLONGATION;FCT_PROVISIONDEAL;FCT_PROVISIONDEAL_CRED_OBJ;FCT_PROVISION_OBJECT;FCT_REPAYSCHEDULE_DM;FCT_REPAYSCHEDULE_H;FCT_SUBJECT_ROLEDEAL;';
    insert into ldr_infa.fct_department_od (id,
                                            id_pre,
                                            filcode,
                                            startlog,
                                            endlog,
                                            datelastod,
                                            corr,
                                            system_code,
                                            dt_begin,
                                            dt_end,
                                            table_load)
                                     values (in_id,
                                             in_id_pre,
                                             in_filcode,
                                             in_beg_date,
                                             in_end_date,
                                             in_datelastod,
                                             null,
                                             System_RS,
                                             trunc(in_beg_date),
                                             trunc(in_end_date),
                                             v_table
                                             );
commit;                                             
  end;

  function GetAccountUF4(acc in daccount_dbt.t_account%type)
    return varchar2 DETERMINISTIC
  is
    uf4 varchar2(250);
  begin
    select case
             when (t_userfield4 is null) or
                  (t_userfield4 = chr(0)) or
                  (t_userfield4 = chr(1)) or
                  (t_userfield4 like '0x%') then
               t_account
             else
               t_userfield4
           end
      into uf4
      from daccount_dbt
     where t_account = acc
       and t_code_currency = (select fi.t_fiid
                                from dfininstr_dbt fi
                                where fi.t_fi_code = substr(acc, 6, 3));
    return uf4;
  exception
    when no_data_found then
      return null;
  end;

  -----------------------------------------------------
  --Запись данных в в лог выгрузки с указанием филиала и последнего операционного дня (для выгрузки прочих ПФИ)
  -----------------------------------------------------
  procedure add_export_log_pfi ( in_id          in number,
                                 in_id_pre      in number,
                                 in_filcode     in varchar2,
                                 in_datelastod  in date,
                                 in_beg_date    in date,
                                 in_end_date    in date) is
  v_table varchar2(4000);

  begin
    v_table := 'ASS_ACCOUNTDEAL;ASS_CONTRACT_DEAL;ASS_DEAL_CAT_VAL;ASS_FCT_DEAL;DET_CONTRACT;DET_CURRENCY_PAIR;DET_DEAL_CAT;DET_DEAL_CAT_VAL;DET_DEAL_TYPEATTR;DET_EXCHANGE;DET_FINSTR;DET_INDEX;DET_PROCBASE;DET_RATE;DET_RATE_PAIR;DET_ROLEACCOUNT_DEAL;DET_TYPEATTR;DET_TYPE_RATE;FCT_BANKNOTE;FCT_DEAL;FCT_DEAL_INDICATOR;FCT_DEAL_RST;FCT_FINSTR_RATE;FCT_FUTURES;FCT_FXSWAP;FCT_IRS;FCT_OPTION;FCT_SPOTFORWARD;FCT_SUBJ_INDICATOR;';
    insert into ldr_infa_pfi.fct_department_od (id,
                                                id_pre,
                                                filcode,
                                                startlog,
                                                endlog,
                                                datelastod,
                                                corr,
                                                system_code,
                                                dt_begin,
                                                dt_end,
                                                table_load)
                                     values (in_id,
                                             decode(in_id_pre,
                                                    -1,(select nvl(max(id),0) from ldr_infa_pfi.fct_department_od),
                                                    in_id_pre),
                                             in_filcode,
                                             in_beg_date,
                                             in_end_date,
                                             in_datelastod,
                                             null,
                                             System_RS,
                                             trunc(in_beg_date),
                                             trunc(in_end_date),
                                             v_table
                                             );
commit;                                             
  end;


end qb_dwh_utils;
/
