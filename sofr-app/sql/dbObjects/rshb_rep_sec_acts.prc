create or replace procedure rshb_rep_sec_acts(Department     number
                                              ,IsPeriod       number
                                              ,ReportDateBeg  date
                                              ,ReportDateEnd  date
                                              ,WithNoChanging char
                                              ,KDUAcc_flag    number
                                              ,FullReport     number) is
  /*
  $Name:        sec_acts.mac
  $Module:      Ценные бумаги
  $Description: ОТЧЕТ: "АКТЫ СВЕРКИ Ц/Б"(БО)
  */
  IsFirstDate        boolean := true;
  IsIncludeData      boolean;
  v_buf              varchar2(32000);
  v_Account          varchar2(50);
  v_Client_name      varchar2(2000);
  v_Subnumber        varchar2(2000);
  v_Issuer_name      varchar2(2000);
  v_AvrKindRoot      varchar2(2000);
  v_AvrKind          varchar2(2000);
  v_Definition       varchar2(2000);
  v_RegNumber        varchar2(2000);
  v_isin             varchar2(2000);
  v_VU               varchar2(2000);
  v_Dep              varchar2(2000);
  v_BO               varchar2(2000);
  v_Delta_VU_Dep     varchar2(2000);
  v_Delta_CB_Dep     varchar2(2000);
  v_Delta_VU_CB      varchar2(2000);
  v_CauseDiff        varchar2(2000);
  v_RestVU           number; /*Остаток по данным внутреннего учета*/
  v_RestCB           number; /*Остаток по данным учетной системы бэк-офиса*/
  v_RestDep          number;
  v_DeltaRest_VU_Dep number;
  v_DeltaRest_CB_Dep number;
  v_DeltaRest_VU_CB  number;

 v_row_scv constant varchar2(32000) := 
'"%Account%";"%Client_name%";"%Subnumber%";"%Issuer_name%";'||
'"%AvrKindRoot%";"%AvrKind%";"%Definition%";"%RegNumber%";%isin%;'||
'%VU%;%Dep%;%BO%;%Delta_VU_Dep%;%Delta_CB_Dep%;%Delta_VU_CB%';

 v_row_scv_end constant varchar2(20) := chr(13)||chr(10);

  /*     PRIVATE MACRO БылоДвижениеПоСчетуВУ( Currency:integer, Account:string )
  END;*/
  function MovementDK(p_fiid    number
                     ,p_account varchar2
                     ,p_Chapter number default 22) return boolean as
  begin
    if p_account is not null
    then
      if rsi_rsb_account.debetac(p_account, p_Chapter, p_fiid, ReportDateBeg, ReportDateEnd) != 0
      then
        return true;
      end if;
      if rsi_rsb_account.kreditac(p_account, p_Chapter, p_fiid, ReportDateBeg, ReportDateEnd) != 0
      then
        return true;
      end if;
    end if;
    return false;
  end;

  /*     PRIVATE MACRO БылоДвижениеДЕПО(FIID:integer, ClientID:integer, Department:integer)
       END;*/
  function MovementDep(p_FIID     number
                      ,p_ClientID number) return boolean as
    v_sql           varchar2(2000);
    cur_acc         sys_refcursor;
    v_ret_val       boolean := false;
    v_code_currency daccount_dbt.t_code_currency%type;
    v_account       daccount_dbt.t_account%type;
    v_chapter       daccount_dbt.t_chapter%type;
  begin
    if KDUAcc_flag != 0
    then
      v_sql := 'SELECT acc.t_code_currency,acc.t_account,acc.t_chapter
      FROM daccount_dbt acc,
           dscqracc_dbt qracc
    WHERE     acc.t_Chapter = 5
          AND acc.t_Client = :p_ClientID
      AND acc.t_Code_Currency = :p_FIID
          AND ACC.T_KIND_ACCOUNT = ''А''
          AND acc.t_Department = :Department
          and acc.t_accountid = qracc.t_accountid
          AND (   acc.t_Close_Date = TO_DATE (''01.01.0001'', ''DD.MM.YYYY'')
              OR acc.t_Close_Date >= :ReportDateBeg )
          AND acc.t_Open_Date <   :ReportDateEnd                 --GetSQLDate(IIF(IsPeriod,ReportDateEnd,ReportDateBeg))
          AND acc.t_client = :p_ClientID ';
    else
      v_sql := 'SELECT acc.t_code_currency,acc.t_account,acc.t_chapter
             FROM daccount_dbt acc,
                 ddepoacnt_dbt dppt,
                 ddepoacnt_dbt dpacc,
                 ddepoac_dbt dpac
             WHERE acc.t_Chapter = 5
             AND acc.t_Client = :p_ClientID
             AND acc.t_Code_Currency = :p_FIID
             AND dppt.t_AutoKey = acc.t_DepoAcc
             AND dpacc.t_AutoKey = acc.t_DepoRoot
             AND dpac.t_ID = dpacc.t_Type
             AND dppt.t_Department = :Department
             AND dpacc.t_Department = dppt.t_Department
             AND (acc.t_Close_Date = TO_DATE(''01.01.0001'',''DD.MM.YYYY'') or acc.t_Close_Date >= :ReportDateBeg )
             AND acc.t_Open_Date < :ReportDateEnd -- GetSQLDate(IIF(IsPeriod,ReportDateEnd,ReportDateBeg))
             AND dpac.t_Type = 1
             AND dppt.t_Owner = :p_ClientID';
    end if;
    open cur_acc for v_sql
      using p_ClientID, p_FIID, Department, ReportDateBeg, ReportDateEnd;
    loop
      fetch cur_acc
        into v_code_currency
            ,v_account
            ,v_chapter;
      exit when cur_acc%notfound;
      if MovementDK(v_code_currency, v_account, v_chapter)
      then
        v_ret_val := true;
        exit;
      end if;
    end loop;
    return v_ret_val;
  end;
  function str_xml( p_string varchar2) return varchar2 as
  begin
    return dbms_xmlgen.convert(translate(p_string,'A'||chr(0)||chr(1),'A')); -- REGEXP_REPLACE(p_string, '[^[:print:]]', ''));--
  end;
begin
  it_rsl_string.clear;

  for cur_row in ( SELECT nvl(ds.T_RESTDEP, 0.000000) value,
                       ds.*,
                       CASE WHEN round(ds.T_RESTCB,5) = round(ds.T_REST,5) AND round(ds.T_RESTCB,5) = round(ds.T_RESTDEP,5) THEN 'Расхождений нет.'
                         ELSE null
                       END cause_dif1,
                       CASE
                         WHEN (SELECT COUNT(1) FROM dpersn_dbt dth
                                WHERE dth.t_personid = ds.t_clientid
                                  AND dth.t_death <= ReportDateBeg
                                  AND dth.t_death > TO_DATE('01010001', 'ddmmyyyy')) > 0
                                  and (ds.T_RESTDEP = 0 OR ds.T_RESTDEP IS NULL)
                                  and (ds.t_restcb > 0 or ds.t_rest > 0) THEN
                          'У клиента обнаружена установленная дата смерти. Расхождение может быть вызвано отсутствием данных по депозитарному учету из-за  перевода ЦБ на невыгружаемый раздел блокированных ЦБ в связи со смертью депонента.'
                         WHEN ds.T_RESTCB = ds.T_REST AND ds.T_RESTCB = ds.T_RESTDEP THEN null
                         WHEN (ds.T_RESTCB - TO_NUMBER(ds.T_RESTDEP)) != 0 AND ds.t_subnumber IN ('78/51-635109_ф','78/51-635109-1_ф','07/43-643671_ф','07/43-643671-1_ф','62/19-1313564_ф',
                                                                                                  '62/19-1313564-1_ф','08/12-263730_ф','08/12-263730-1_ф','27/05-1001489_ф','27/05-1001489-1_ф',
                                                                                                  '62/30-38248_ф','27-40157_ф','07/30-208132-1_ф','62/09-1023325-1_ф','62/37-2050931-1_ф',
                                                                                                  '01/03-22590_ф','01/03-22590-1_ф') THEN 'Два договора на один счет депо.'
                       END cause_dif2,
                       case when SUBSTR(ds.note_5, 0, 1) = 'Y' then 1 else 0 end is_spec_depo
                  FROM (SELECT spacts.t_clientid,
                               (select  nvl(p.t_shortname, p.t_name) from dparty_dbt p where t_partyid = spacts.t_clientid ) as t_client_name,
                               spacts.t_fiid,
                               nvl(spacts.t_restcb,0.000000) as t_restcb,
                               nvl(spacts.t_restdep,0.000000) as t_restdep,
                               nvl(spacts.t_rest,0.000000) as t_rest,
                               nvl(spacts.t_account, chr(1)) as t_account,
                               sfsubcontr.t_number t_subnumber,
                               av.t_isin,
                               av.t_lsin,
                               fin.t_definition,
                               REPLACE(UTL_RAW.cast_to_varchar2(note5.t_text), CHR(0)) note_5,
                               (select  nvl(p.t_shortname, p.t_name) from dparty_dbt p where t_partyid = fin.t_Issuer ) as t_issuer_name,
                               avrkindroot.T_Name AvrKindNameRoot,
                               avrkind.T_Name AvrKindName,
                               spacts.t_sectioncode
                          FROM dsp_acts_tmp spacts
                          LEFT JOIN dsfcontr_dbt sfsubcontr ON spacts.t_contrid = sfsubcontr.t_id
                          LEFT JOIN dnotetext_dbt note5 ON note5.t_documentid = LPAD(spacts.t_contrid, 10, 0)
                                                       AND note5.t_notekind = 5
                                                       and note5.t_objecttype = 659
                          LEFT JOIN davoiriss_dbt av ON spacts.t_fiid = av.t_fiid
                          LEFT JOIN dfininstr_dbt fin ON fin.t_fiid = spacts.t_fiid
                          LEFT JOIN davrkinds_dbt avrkindroot ON fin.t_Fi_Kind = AVRKINDROOT.T_FI_KIND
                                                             AND RSB_FIINSTR.FI_AVRKINDSGETROOT(fin.t_Fi_Kind, fin.t_AvoirKind) = avrkindroot.t_AvoirKind
                          LEFT JOIN davrkinds_dbt avrkind ON fin.t_AvoirKind = avrkind.t_AvoirKind
                                                         AND fin.t_Fi_Kind = avrkind.t_Fi_Kind) ds
                   order by t_client_name,t_clientid,t_fiid)
  loop
    v_Account := str_xml(cur_row.t_account);
    v_RestVU  := cur_row.t_rest; /*Остаток по данным внутреннего учета*/
    v_RestCB  := cur_row.t_restcb; /*Остаток по данным учетной системы бэк-офиса*/
    v_RestDep := cur_row.value; /*Остаток по данным депозитарного учета*/

    if IsFirstDate and IsPeriod = 0
    then
      /*на утро первого дня периода(если выводим на каждую дату)*/
      IsFirstDate   := false;
      IsIncludeData := (v_RestVU != 0 or v_RestCB != 0 or v_RestDep != 0);
    else
      if WithNoChanging != 'X'
         and v_RestVU = v_RestDep --(RestVU == RestCB) and (RestCB == RestDep) )
         and v_RestDep = v_RestCB
         and not MovementDK(cur_row.t_fiid, v_Account) --(not БылоДвижениеПоСчетуВУ(Avr.FIID,Account) and
         and not MovementDep(cur_row.t_fiid, cur_row.t_clientid) --(not БылоДвижениеДЕПО(Avr.FIID,Avr.ClientID,Department) and
      then
        IsIncludeData := false;
      else
        IsIncludeData :=( v_RestVU != 0 or v_RestDep != 0 or v_RestCB != 0); --( (RestVU != 0.0 ) OR (RestDep != 0.0) OR (RestCB != 0.0) )
      end if;
    end if;

    if IsIncludeData
    then
      v_DeltaRest_VU_Dep := v_RestVU - v_RestDep;
      v_DeltaRest_CB_Dep := v_RestCB - v_RestDep;
      v_DeltaRest_VU_CB  := v_RestVU - v_RestCB;

      if FullReport != 0
         or v_DeltaRest_VU_Dep != 0
         or v_DeltaRest_CB_Dep != 0
         or v_DeltaRest_VU_CB != 0
      then
        v_Client_name := str_xml(cur_row.t_client_name);
        v_Subnumber   := str_xml(cur_row.t_subnumber);
        v_Issuer_name := str_xml(cur_row.t_issuer_name);
        v_AvrKindRoot := str_xml(cur_row.AvrKindNameRoot);
        v_AvrKind     := str_xml(cur_row.AvrKindName);
        v_Definition  := str_xml(cur_row.t_definition);
        v_RegNumber   := str_xml(cur_row.t_lsin);
        v_isin        := str_xml(cur_row.t_isin);

        v_CauseDiff := str_xml(cur_row.cause_dif1);
        if cur_row.is_spec_depo = 1
        then
          v_CauseDiff := trim(v_CauseDiff || ' СпецДЕПО');
        end if;
        v_CauseDiff   := trim(v_CauseDiff || ' ' || str_xml(cur_row.cause_dif2));

        v_VU           :=to_char(v_RestVU);
        v_Dep          :=to_char(v_RestDep);
        v_BO           :=to_char(v_RestCB);
        v_Delta_VU_Dep :=to_char(v_DeltaRest_VU_Dep);
        v_Delta_CB_Dep :=to_char(v_DeltaRest_CB_Dep);
        v_Delta_VU_CB  :=to_char(v_DeltaRest_VU_CB);

        v_buf := replace(v_row_scv, '%Account%', v_Account);
        v_buf := replace(v_buf, '%Client_name%', v_Client_name);
        v_buf := replace(v_buf, '%Subnumber%', v_Subnumber);
        v_buf := replace(v_buf, '%Issuer_name%', v_Issuer_name);
        v_buf := replace(v_buf, '%AvrKindRoot%', v_AvrKindRoot);
        v_buf := replace(v_buf, '%AvrKind%', v_AvrKind);
        v_buf := replace(v_buf, '%Definition%', v_Definition);
        v_buf := replace(v_buf, '%RegNumber%', v_RegNumber);
        v_buf := replace(v_buf, '%isin%', v_isin);
        v_buf := replace(v_buf, '%VU%', v_VU);
        v_buf := replace(v_buf, '%Dep%', v_Dep);
        v_buf := replace(v_buf, '%BO%', v_BO);
        v_buf := replace(v_buf, '%Delta_VU_Dep%', v_Delta_VU_Dep);
        v_buf := replace(v_buf, '%Delta_CB_Dep%', v_Delta_CB_Dep);
        v_buf := replace(v_buf, '%Delta_VU_CB%', v_Delta_VU_CB);

        v_buf := v_buf || ';"' || v_CauseDiff || '"';
        v_buf := v_buf || ';"' || cur_row.t_sectioncode || '"';
        v_buf := v_buf || v_row_scv_end; 

       it_rsl_string.append_varchar(v_buf);
      end if;
    end if;
  end loop;

exception
  when others then
    it_error.put_error_in_stack;
    it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
    it_error.clear_error_stack;
    raise;
end;
/
