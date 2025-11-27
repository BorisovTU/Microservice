CREATE OR REPLACE package body RSO_DEALING
is

  -- ВАЖНО!!!! Для корректной работы, по слиянию массивов сумм по периодам (например, остатки по 2-м счетам)
  -- необходимо передавать периоды со старшей даты начала периода, т.е. сортировка по убыванию дат начала периодов
  PROCEDURE RSI_InsertBaseSum(ContractID  IN       NUMBER,                      -- ID процентного договора
                          sDate       IN       DATE,                        -- дата начала периода
                          eDate       IN       DATE,                        -- дата окончания периода
                          SumRest     IN       dprcbaserest_tmp.t_Rest%TYPE -- сумма периода
                         )
  is
      v_idx   NUMBER;
      v_idxP  NUMBER;

      type BaseRest_t is table of dprcbaserest_tmp%ROWTYPE;
      BaseRest     BaseRest_t;
      BaseRestPrev BaseRest_t;
  begin
      -- проверяем существование приода с такой же датой начала
      select * bulk collect into BaseRest from dprcbaserest_tmp baserest where
      baserest.t_ContractID = ContractID and baserest.t_Date = sDate;

      -- проверяем существование предыдущих периодов
      select * bulk collect into BaseRestPrev from dprcbaserest_tmp baserest where
      baserest.t_ContractID = ContractID and baserest.t_Date < sDate
      order by baserest.t_Date desc;

      v_idx  := BaseRest.First;
      v_idxP := BaseRestPrev.First;

      if (BaseRest.Count > 0) then -- попали в начало существующего периода, то обновим сумму в начале периода
          update dprcbaserest_tmp baserest set t_rest = t_rest + abs(SumRest) where
          baserest.t_ContractID = ContractID and baserest.t_Date = sDate;
      elsif ((BaseRest.Count = 0)and(BaseRestPrev.Count = 0))then -- попали в несуществующий период (перед первым), вставим переданную сумму
          insert into dprcbaserest_tmp values (ContractID, sDate, abs(SumRest));
      elsif ((BaseRest.Count = 0)and(BaseRestPrev.Count > 0)) then -- попали в период, вставим сумму текущего периода + переданную сумму
          insert into dprcbaserest_tmp values (ContractID, sDate, (abs(SumRest) + BaseRestPrev(v_idxP).t_Rest));
      end if;

      -- обновим суммы периода sDate..eDate на переданную сумму
      update dprcbaserest_tmp baserest set t_rest = t_rest + abs(SumRest) where
      baserest.t_ContractID = ContractID and baserest.t_Date > sDate and baserest.t_Date <= eDate;
  end;

  -- Процедура формирования базы для расчета процентов
  PROCEDURE RSI_RestList(ContractID      IN       NUMBER,    -- ID процентного договора
                     BeginDate       IN       DATE,      -- дата начала периода
                     EndDate         IN       DATE       -- дата окончания периода
                    )
  is
      v_idx                   NUMBER := 0;
      v_idxrest               NUMBER;

      v_DealID                ddl_tick_dbt.t_DealID%TYPE;
      v_isPriv                NUMBER := 1;
      v_ObjectType            dprccontract_dbt.t_ObjectType%TYPE;
      v_Principal             ddl_leg_dbt.t_Principal%TYPE;
      v_FIID                  dprccontract_dbt.t_fiid%TYPE;
      v_RestType              dprccalc_dbt.t_resttype%TYPE;
      v_GenAgrID              NUMBER := 0;

      v_PeriodBeginDate       DATE;
      v_PeriodEndDate         DATE;

      v_ResPeriodBegin        DATE;
      v_ResPeriodEnd          DATE;

      v_flagExit              BOOLEAN := false;
      v_noRest                NUMBER  := 0;
      v_idxnoRest             NUMBER := 0;
      v_InclFirstDay          CHAR(1) := CNST.UNSET_CHAR;

      type McAccDoc_t is table of dmcaccdoc_dbt%ROWTYPE;
      type Paym_t is table of dpmpaym_dbt%ROWTYPE;

      TYPE RestDate_rec IS RECORD
      (
          T_DATE_VALUE drestdate_dbt.t_restdate%TYPE,
          T_REST       drestdate_dbt.t_rest%TYPE
      );
      type RestDate_t is table of RestDate_rec;

      McAccDoc McAccDoc_t;
      RestDate RestDate_t;
      Paym Paym_t;
  begin
      -- т.к. в текущей реализации невозможно получить параметры сделки в БД в момент ее ввода,
      -- то для получения суммы ОД перед расчтетом заполняется таблица dprcbaserest_tmp, где
      -- t_date - дата предоставления средств, t_rest - сумма сделки.
      if (ContractID >= 0) then -- для существующих сделок
          begin
              -- получим начальные параметры для работы функции - это: ИД сделки, Тип объекта(%%, просроченный ОД, просроченные %%),
              -- валюта сделки, сумма сделки, дата предоставления средств, тип остатка, системный тип сделки (Привлчение или размещение)
              select tick.t_DealID, prc.t_ContractType, leg.t_PFI, leg.t_Principal, leg.t_Start, (leg.t_Start + leg.t_Duration),
              decode(bitand(tick.t_DealGroup, 1), 1, 0, 1), calc.t_RestType, tick.t_GenAgrID
              into v_DealID, v_ObjectType, v_FIID, v_Principal, v_ResPeriodBegin, v_ResPeriodEnd, v_isPriv, v_RestType, v_GenAgrID
              from ddl_tick_dbt tick, dprccontract_dbt prc, ddl_leg_dbt leg, dprccalc_dbt calc
              where
              (leg.t_DealID = tick.t_DealID)
              and (leg.t_LegKind = 0)and(leg.t_LegID = 1)
              and (prc.t_ObjectID = tick.t_DealCode)
              and (calc.t_calcid = prc.t_calcid)
              and (prc.t_ContractID = ContractID);

              EXCEPTION
                WHEN NO_DATA_FOUND then -- не нашли сделку - не чего и считать
                    return;
          end;
          
          if (v_GenagrID > 0) then
            select nvl(t_IncludeDay, CNST.UNSET_CHAR) into v_InclFirstDay from ddl_genagr_dbt where t_genagrid = v_GenAgrID;
          end if;

          -- при редактировании сделки получим историю остатков (надо же знать сумму сделки, дату начала и окончания сделки)
          select t_date, t_rest bulk collect into RestDate from dprcbaserest_tmp where t_contractid = 0 order by t_date;
          if (RestDate.Count = 1) then -- нет остатков - ну и ладно, параметры ранее получили.
              v_Principal      := RestDate(RestDate.First).t_Rest;
              v_ResPeriodBegin := RestDate(RestDate.First).t_Date_Value;
--          elsif (RestDate.Count = 2) then
--              RSI_InsertBaseSum(ContractID, RestDate(RestDate.First + 1).t_Date_Value, RestDate(RestDate.First + 1).t_Date_Value, RestDate(RestDate.First + 1).t_Rest);
          end if;
          RestDate.Delete;
      else -- для сделок на этапе ввода
          -- сразу получим историю остатков (при вводе сделки это одна запись)
          select t_date, t_rest bulk collect into RestDate from dprcbaserest_tmp where t_contractid = 0;
          if (RestDate.Count = 0) then -- нет остатков - нет расчету!!!.
              return;
          end if;

          -- получим начальные параметры для работы функции - это: ИД сделки, Тип объекта(%%, просроченный ОД, просроченные %%),
          -- сумма сделки, тип остатка, дата предоставления средств
          v_DealID         := 0;
          v_Principal      := RestDate(RestDate.First).t_Rest;
          v_ResPeriodBegin := RestDate(RestDate.First).t_Date_Value;
          v_ResPeriodEnd   := EndDate;
          v_ObjectType     := MMarkConst.PC_IBC_CON; -- при вводе расчитываем только проценты
          -- заполним произволным значением. т.к. параметры при вводе не известны
          v_FIID           := 0;
          v_isPriv         := 0;

          select calc.t_RestType into v_RestType
          from dprccontract_isrv_tmp prc, dprccalc_isrv_tmp calc
          where
          (calc.t_calcid = prc.t_calcid)
          and (prc.t_ContractID = ContractID);
          
          begin  
             select nvl(t_InclFirstDay, CNST.UNSET_CHAR) into v_InclFirstDay from dmmprcinfo_tmp;
             
             EXCEPTION
                WHEN NO_DATA_FOUND then
                    NULL;
          end;

          RestDate.Delete;
      end if;

      -- чистим так, чтобы не убить историю остатков при вводе новой сделки
      DELETE FROM dprcbaserest_tmp where t_contractid <> 0;

      if (v_RestType = 1) then -- Тип остатка - входящий
          v_PeriodBeginDate := BeginDate - 1;
          v_PeriodEndDate   := EndDate - 1;
      else
          v_PeriodBeginDate := BeginDate;
          v_PeriodEndDate   := EndDate;
      end if;

      if (v_ObjectType = MMarkConst.PC_IBC_CON) then -- расчет процентов
          if (v_DealID > 0) then
              -- получим счет по ОД, актуализированный по сделке
              select accdoc.* bulk collect into McAccDoc
              from dmcaccdoc_dbt accdoc, dmccateg_dbt categ
              where
               (accdoc.t_dockind = 102)and
               (accdoc.t_docid = v_DealID)and
               (categ.t_number = accdoc.t_catnum )and
                (categ.t_code = MMARKCONST.tdr_mainrest or
                 categ.t_code = MMARKCONST.tdr_nomcost or
                 categ.t_code = MMARKCONST.tdr_mainrest_tf)
              order by categ.t_code;
          end if;

          if (McAccDoc is NULL or McAccDoc.Count = 0) then -- счет еще не привязан к сделке, то берем начальную сумму сделки
              if (v_RestType = 1) then -- Тип остатка - входящий
                  if (v_InclFirstDay = CNST.UNSET_CHAR) then
                     v_ResPeriodBegin := v_ResPeriodBegin + 1;
                  end if;
              else
                  if (EndDate = v_ResPeriodEnd) then
                      RSI_InsertBaseSum(ContractID, v_ResPeriodEnd, v_ResPeriodEnd, 0);
                  end if;
              end if;

              RSI_InsertBaseSum(ContractID, v_ResPeriodBegin, v_ResPeriodBegin, v_Principal);
          else
              -- иначе попробуем получить осатки по счету ОД
              -- и если остатков нет, то опять же вставляем начальную сумму сделки
              -- если есть, то вставляем массив остатков в порядке убывания
              for v_idx in McAccDoc.First .. McAccDoc.Last
              loop
                  select rd.t_restdate, rd.t_rest bulk collect into RestDate from drestdate_dbt rd, daccount_dbt acc
                  where acc.t_account = McAccDoc(v_idx).t_Account and acc.t_code_currency = v_FIID and acc.t_chapter = McAccDoc(v_idx).t_Chapter and
                        rd.t_AccountID = acc.t_AccountID and rd.t_restdate <= v_PeriodEndDate and rd.t_restcurrency = v_FIID
                        and not (rd.t_Rest = rd.t_PlanRest and rd.t_PlanRest = rd.t_Debet and rd.t_Debet = rd.t_Credit and rd.t_Credit = 0) --288269
                  order by t_restdate desc;

                  if (RestDate.Count = 0) then -- остатков по счету нет
                      v_noRest := v_noRest + 1;
                  else
                      v_flagExit := false;
                      for v_idxrest in RestDate.First .. RestDate.Last
                      loop
                          v_ResPeriodBegin := RestDate(v_idxrest).t_Date_Value;
                          if (v_ResPeriodBegin > v_PeriodBeginDate) then
                              if (v_RestType = 1) then -- Тип остатка - входящий
                                 v_ResPeriodBegin := v_ResPeriodBegin + 1;
                              end if;
                          elsif (v_ResPeriodBegin = v_PeriodBeginDate) then
                              if (v_RestType = 1) then -- Тип остатка - входящий
                                 v_ResPeriodBegin := v_ResPeriodBegin + 1;
                              end if;
                            v_flagExit := true;
                          elsif (v_ResPeriodBegin < v_PeriodBeginDate) then
                              v_ResPeriodBegin := v_PeriodBeginDate;
                              v_flagExit := true;
                          end if;
                          
                          --включать перый день в расчет
                          if ((v_idxrest = RestDate.Last) and (v_InclFirstDay = CNST.SET_CHAR) and (v_RestType = 1)) then
                              v_ResPeriodBegin := v_ResPeriodBegin - 1;
                          end if;

                          if (v_idxrest > RestDate.First) then
                              v_ResPeriodEnd := RestDate(v_idxrest - 1).t_Date_Value - 1;
                              if (v_RestType = 1) then -- Тип остатка - входящий
                                  v_ResPeriodEnd   := v_ResPeriodEnd + 1;
                              end if;
                          else
                              v_ResPeriodEnd := TO_DATE('31.12.9999', 'dd.mm.yyyy');
                          end if;

                          RSI_InsertBaseSum(ContractID, v_ResPeriodBegin, v_ResPeriodEnd, RestDate(v_idxrest).t_Rest);
                          exit when v_flagExit;
                      end loop;
                  end if;

                  RestDate.Delete; -- почистим коллекцию
                  v_idxnoRest := v_idxnoRest + 1;
              end loop;

              if (v_noRest = v_idxnoRest) then
                  if (v_RestType = 1) then -- Тип остатка - входящий
                      if (v_InclFirstDay = CNST.UNSET_CHAR) then
                         v_ResPeriodBegin := v_ResPeriodBegin + 1;
                      end if;
                  end if;
                  RSI_InsertBaseSum(ContractID, v_ResPeriodBegin, v_ResPeriodBegin, v_Principal);
              end if;
          end if;
      elsif (v_ObjectType = MMarkConst.PC_IBC_CON_EXP) then -- расчет штрафа по просроченному ОД
          -- для штрафа по просроченному ОД берем счет в зависимости от типа сделки
          if (v_DealID > 0) then
              if (v_isPriv = 1) then -- сделка привлечения
                  select accdoc.* bulk collect into McAccDoc
                  from dmcaccdoc_dbt accdoc, dmccateg_dbt categ
                  where
                    (accdoc.t_dockind = 102)and
                    (accdoc.t_docid = v_DealID)and
                    (categ.t_number = accdoc.t_catnum )and
                    (categ.t_code = MMARKCONST.tdr_exprestP)
                  order by categ.t_code;
              else -- сделка размещения
                  select accdoc.* bulk collect into McAccDoc
                  from dmcaccdoc_dbt accdoc, dmccateg_dbt categ
                  where
                    (accdoc.t_dockind = 102)and
                    (accdoc.t_docid = v_DealID)and
                    (categ.t_number = accdoc.t_catnum )and
                    (categ.t_code = MMARKCONST.tdr_exprestR)
                  order by categ.t_code;
              end if;
          end if;

          if (McAccDoc.Count > 0) then -- штраф можно посчитать только в том случае, если счета есть, и на них есть остатки
              v_idx := McAccDoc.First;

              select rd.t_restdate, rd.t_rest bulk collect into RestDate from drestdate_dbt rd, daccount_dbt acc
              where acc.t_account = McAccDoc(v_idx).t_Account and acc.t_code_currency = v_FIID and acc.t_chapter = McAccDoc(v_idx).t_Chapter and
                    rd.t_AccountID = acc.t_AccountID and rd.t_restdate <= v_PeriodEndDate and rd.t_restcurrency = v_FIID
                    and not (rd.t_Rest = rd.t_PlanRest and rd.t_PlanRest = rd.t_Debet and rd.t_Debet = rd.t_Credit and rd.t_Credit = 0) --288269
                  order by t_restdate desc;

                  if (RestDate.Count > 0) then
                      v_flagExit := false;
                      for v_idxrest in RestDate.First .. RestDate.Last
                      loop
                          v_ResPeriodBegin := RestDate(v_idxrest).t_Date_Value;
                          if (v_ResPeriodBegin > v_PeriodBeginDate) then
                              if (v_RestType = 1) then -- Тип остатка - входящий
                                  v_ResPeriodBegin := v_ResPeriodBegin + 1;
                              end if;
                          elsif (v_ResPeriodBegin = v_PeriodBeginDate) then
                              if (v_RestType = 1) then -- Тип остатка - входящий
                                  v_ResPeriodBegin := v_ResPeriodBegin + 1;
                              end if;
                            v_flagExit := true;
                          elsif (v_ResPeriodBegin < v_PeriodBeginDate) then
                              v_ResPeriodBegin := v_PeriodBeginDate;
                              v_flagExit := true;
                          end if;

                          if (v_idxrest > RestDate.First) then
                              v_ResPeriodEnd := RestDate(v_idxrest - 1).t_Date_Value - 1;
                              if (v_RestType = 1) then -- Тип остатка - входящий
                                  v_ResPeriodEnd   := v_ResPeriodEnd + 1;
                              end if;
                          else
                              v_ResPeriodEnd := TO_DATE('31.12.9999', 'dd.mm.yyyy');
                          end if;

                          RSI_InsertBaseSum(ContractID, v_ResPeriodBegin, v_ResPeriodEnd, RestDate(v_idxrest).t_Rest);
                          exit when v_flagExit;
                      end loop;
                  end if;
          else
              select pm.* bulk collect into Paym from dpmpaym_dbt pm
              where pm.t_DocKind = 102 and pm.t_DocumentID = v_DealID
              and pm.t_Purpose = 18 and pm.t_PaymStatus = 1000;

              if (Paym.Count > 0) then
                  v_ResPeriodBegin := Paym(Paym.First).t_ValueDate;
                  if (v_RestType = 1) then -- Тип остатка - входящий
                      v_ResPeriodBegin := v_ResPeriodBegin + 1;
                  end if;
                  v_ResPeriodEnd := TO_DATE('31.12.9999', 'dd.mm.yyyy');

                  RSI_InsertBaseSum(ContractID, v_ResPeriodBegin, v_ResPeriodEnd, Paym(Paym.First).t_FuturePayerAmount);
              end if;
          end if;
      elsif(v_ObjectType = MMarkConst.PC_IBC_CON_EXPPC) then -- расчет штрафа по просроченным процентам
          -- для штрафа по просроченным %% берем счет в зависимости от типа сделки, к тому же для
          -- сделок размещния необходимо учесть балансовые и внебалансовые счета
          if (v_DealID > 0) then
              if (v_isPriv = 1) then
                  select accdoc.* bulk collect into McAccDoc
                  from dmcaccdoc_dbt accdoc, dmccateg_dbt categ
                  where
                    (accdoc.t_dockind = 102)and
                    (accdoc.t_docid = v_DealID)and
                    (categ.t_number = accdoc.t_catnum )and
                    (categ.t_code = MMARKCONST.tdr_exppercP)
                  order by categ.t_code;
              else
                  select accdoc.* bulk collect into McAccDoc
                  from dmcaccdoc_dbt accdoc, dmccateg_dbt categ
                  where
                    (accdoc.t_dockind = 102)and
                    (accdoc.t_docid = v_DealID)and
                    (categ.t_number = accdoc.t_catnum )and
                    ((categ.t_code = MMARKCONST.tdr_exppercR)or(categ.t_code = MMARKCONST.tdr_expperc_vb))
                  order by categ.t_code;
              end if;
          end if;

          if (McAccDoc.Count > 0) then  -- штраф можно посчитать тлько в том случае, если счета есть, и на них есть остатки
              for v_idx in McAccDoc.First .. McAccDoc.Last
              loop
                  select rd.t_restdate, rd.t_rest bulk collect into RestDate from drestdate_dbt rd, daccount_dbt acc
                  where acc.t_account = McAccDoc(v_idx).t_Account and acc.t_code_currency = v_FIID and acc.t_chapter = McAccDoc(v_idx).t_Chapter and
                        rd.t_AccountID = acc.t_AccountID and rd.t_restdate <= v_PeriodEndDate and rd.t_restcurrency = v_FIID
                        and not (rd.t_Rest = rd.t_PlanRest and rd.t_PlanRest = rd.t_Debet and rd.t_Debet = rd.t_Credit and rd.t_Credit = 0) --288269
                  order by t_restdate desc;

                  if (RestDate.Count > 0) then
                      v_flagExit := false;
                      for v_idxrest in RestDate.First .. RestDate.Last
                      loop
                          v_ResPeriodBegin := RestDate(v_idxrest).t_Date_Value;
                          if (v_ResPeriodBegin > v_PeriodBeginDate) then
                              if (v_RestType = 1) then -- Тип остатка - входящий
                                  v_ResPeriodBegin := v_ResPeriodBegin + 1;
                              end if;
                          elsif (v_ResPeriodBegin = v_PeriodBeginDate) then
                              if (v_RestType = 1) then -- Тип остатка - входящий
                                  v_ResPeriodBegin := v_ResPeriodBegin + 1;
                              end if;
                            v_flagExit := true;
                          elsif (v_ResPeriodBegin < v_PeriodBeginDate) then
                              v_ResPeriodBegin := v_PeriodBeginDate;
                              v_flagExit := true;
                          end if;

                          if (v_idxrest > RestDate.First) then
                              v_ResPeriodEnd := RestDate(v_idxrest - 1).t_Date_Value - 1;
                              if (v_RestType = 1) then -- Тип остатка - входящий
                                  v_ResPeriodEnd   := v_ResPeriodEnd + 1;
                              end if;
                          else
                              v_ResPeriodEnd := TO_DATE('31.12.9999', 'dd.mm.yyyy');
                          end if;

                          RSI_InsertBaseSum(ContractID, v_ResPeriodBegin, v_ResPeriodEnd, RestDate(v_idxrest).t_Rest);
                          exit when v_flagExit;
                      end loop;
                  end if;
              end loop;
          else
              select pm.* bulk collect into Paym from dpmpaym_dbt pm
              where pm.t_DocKind = 102 and pm.t_DocumentID = v_DealID
              and pm.t_Purpose = 19 and pm.t_PaymStatus = 1000;

              if (Paym.Count > 0) then
                  v_ResPeriodBegin := Paym(Paym.First).t_ValueDate;
                  if (v_RestType = 1) then -- Тип остатка - входящий
                      v_ResPeriodBegin := v_ResPeriodBegin + 1;
                  end if;
                  v_ResPeriodEnd := TO_DATE('31.12.9999', 'dd.mm.yyyy');

                  RSI_InsertBaseSum(ContractID, v_ResPeriodBegin, v_ResPeriodEnd, Paym(Paym.First).t_FuturePayerAmount);
              end if;
          end if;
      end if;

--    EXCEPTION
--        WHEN OTHERS THEN NULL;
  end; -- RestList

end RSO_DEALING;
/
