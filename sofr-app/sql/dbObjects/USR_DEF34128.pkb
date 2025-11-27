create or replace package body usr_def34128 as

  -- код клиента по номеру контракта
  function contract_partyid(rec d_tmp_compens_def34128_dbt%rowtype)
    return number is
    p_partyid integer := -1;
  begin
    select t_partyid
      into p_partyid
      from dsfcontr_dbt dsf
     where dsf.t_number = rec.t_contract
       and dsf.t_dateclose = to_date('01010001', 'ddmmyyyy') fetch
     first 1 rows only;
    return p_partyid;
  exception
    when no_data_found then
      return - 1;
  end;

  -- название тарифного плана по дате и номеру контракта
  function plan_name(p_id            in number,
                     p_contract_date in d_tmp_compens_def34128_dbt.t_contract_date%type)
    return varchar2 is
    plan_name varchar2(50) := '';
  begin
    select t_name
      into plan_name
      from dsfplan_dbt
     where t_sfplanid = rsb_brkrep.getsfplanid(p_id, p_contract_date);
    return plan_name;
  exception
    when no_data_found then
      return plan_name;
  end;

  --в дефекте приложена таблица соответствия тарифов 
  function correspond_tarif_trader(n integer) return integer is 
  begin
    if (n in (1003,1039)) then
      return 1039;
    elsif (n in (1001,1035)) then
      return 1035;
    elsif (n in (1002,1037)) then
      return 1037;
    elsif (n in (1064)) then
      return 1064;
    elsif (n in (1028)) then
      return 1028;
    elsif (n in (1025)) then
      return 1025;
    elsif (n in (1029)) then
      return 1029;
    elsif (n in (1026)) then
      return 1026;
    elsif (n in (1027)) then
      return 1027;
    end if;
    return 0;
  end;

  -- % комиссии Трейдер по сетке 
  function comission_trader_grid_value(p_comnumber integer , p_cost number default 0)
    return number is
  begin
    -- жестко привяжем к кодам комиссий 
    if (p_comnumber in (1035, 1037, 1039)) then
               case 
                 when p_cost>100000000.00 then return 0.00037;
                 when p_cost>30000000.00 then return 0.0004;
                 when p_cost>10000000.00 then return 0.00045;
                 when p_cost>=0.00 then return 0.00055;
               end case;
    elsif (p_comnumber in (1028, 1027, 1029, 1025, 1026)) then
               case 
                 when p_cost>100000000.00 then return 0.00055;
                 when p_cost>30000000.00 then return 0.00075;
                 when p_cost>1000000.00 then return 0.00105;
                 when p_cost>=0.00 then return 0.00155;
               end case;
    elsif (p_comnumber in (1064)) then
               case 
                 when p_cost>100000000.00 then return 0.00007;
                 when p_cost>=0.00 then return 0.00015;
               end case;
    end if;
    --dbms_output.put_line('Тариф не определен ' || p_comnumber);
    return 0;
  end;
  
  /*function cost_by_ddleg(p_docid     in ddlcomis_dbt.t_docid%type) return ddl_leg_dbt%rowtype is
  v_row ddl_leg_dbt%rowtype;
  begin 
    select * into v_row from ddl_leg_dbt d where d.t_dealid = p_docid;
    return v_row;
  end;

  function cost_by_ddvn(p_docid     in ddlcomis_dbt.t_docid%type) return ddvnfi_dbt%rowtype is
  v_row ddvnfi_dbt%rowtype;
  begin 
    select fi.* into v_row  from ddvndeal_dbt dvn, ddvnfi_dbt fi
      where FI.T_DEALID = DVN.T_ID
        and DVN.T_ID = p_docid; 
    return v_row;
  end;*/

  -- комиссия по сделке БОЦБ
  procedure comission_by_ddleg(p_comnumber in ddlcomis_dbt.t_comnumber%type,
                             p_docid     in ddlcomis_dbt.t_docid%type,
                             p_date      in date,
                             p_comis     OUT number,
                             p_comis_nc  OUT number,
                             p_cost_nc   OUT number,
                             p_cost   OUT number
                         ) is
    v_rec_leg ddl_leg_dbt%rowtype;
    v_comission number(32,12);
  begin
    select * into v_rec_leg from ddl_leg_dbt where t_dealid = p_docid;
    if (v_rec_leg.t_cfi = 0) then
      begin
        v_comission:= comission_trader_grid_value(correspond_tarif_trader(p_comnumber), v_rec_leg.t_cost);
        p_comis    := v_rec_leg.t_cost * v_comission;
        p_comis_nc := p_comis;
        p_cost_nc  := v_rec_leg.t_cost;
        p_cost  := v_rec_leg.t_cost;
      end;
    else
      begin
        p_cost  := v_rec_leg.t_cost;
        p_cost_nc  := RSB_FIInstr.ConvSum(v_rec_leg.t_cost,
                                          v_rec_leg.t_cfi,
                                          0,
                                          p_Date);
        v_comission:= comission_trader_grid_value(correspond_tarif_trader(p_comnumber), p_cost_nc);
        p_comis    := v_rec_leg.t_cost * v_comission;
        p_comis_nc := p_cost_nc * v_comission;
      end;
    end if;
  end;

  -- комиссия по сделке Фиссико
  procedure comission_by_ddvndeal(p_comnumber in ddlcomis_dbt.t_comnumber%type,
                             p_docid     in ddlcomis_dbt.t_docid%type,
                             p_date      in date,
                             p_comis     OUT number,
                             p_comis_nc  OUT number,
                             p_cost_nc   OUT number,
                             p_cost   OUT number
                         ) is
    v_rec_leg ddvnfi_dbt%rowtype;
    v_comission number(32,12);
  begin
    select FI.* into v_rec_leg  from ddvndeal_dbt dvn, ddvnfi_dbt fi
      where FI.T_DEALID = DVN.T_ID
        and DVN.T_ID = p_docid; 
    if (v_rec_leg.t_pricefiid = 0) then
      begin
        v_comission:= comission_trader_grid_value(correspond_tarif_trader(p_comnumber), v_rec_leg.t_cost);
        p_comis    := v_rec_leg.t_cost * v_comission;
        p_comis_nc := p_comis;
        p_cost_nc  := v_rec_leg.t_cost; 
      end;
    else
      begin
        p_cost_nc := RSB_FIInstr.ConvSum(v_rec_leg.t_cost,
                                          v_rec_leg.t_pricefiid ,
                                          0,
                                          p_Date);
        v_comission:= comission_trader_grid_value(correspond_tarif_trader(p_comnumber), v_rec_leg.t_cost);
        p_comis    := v_rec_leg.t_cost * v_comission;
        p_comis_nc := RSB_FIInstr.ConvSum(v_rec_leg.t_cost,
                                          v_rec_leg.t_pricefiid ,
                                          0,
                                          p_Date) * v_comission;
      end;
    end if;
  end;

  -- сумма компенаций по одной площадке за день
  function mp_compensation_by_date(p_sfsubcontractid           in number /*субдоговор по площадке*/,
                                   p_date         in date,
                                   p_comis        in out number,
                                   p_comis_nc     in out number,
                                   p_old_comis    in out number,
                                   p_old_comis_nc in out number,
                                   p_count_legs_by_contract_mp_day in out integer)
    return number is
    v_comis    number(32, 12) := 0;
    v_comis_nc number(32, 12) := 0;
    v_old_comis_nc number(32, 12) := 0;
    v_cost_nc number(32,12) :=0;
    v_sum_comis_nc number(32,12) :=0;
    v_cost number(32,12) :=0;
    v_sum_comis number(32,12) :=0;
    i integer := 0;
    --v_ddleg    ddl_leg_dbt%rowtype;
    --v_sum_cost     number(32, 12) := 0;
    --v_sum_cost_nc  number(32, 12) := 0;
    --v_ddvn     ddvnfi_dbt%rowtype;
    cost_nc_array_by_comnumber cost_nc_array_by_comnumber_type;  -- сумма сделок в разрезе по комиссиям в нацвалюте
    cost_array_by_comnumber cost_nc_array_by_comnumber_type;  -- сумма сделок в разрезе по комиссиям
    v_comiss_grid_value number(32,12) :=0; --запомнить последнее значение комиссии за день
  begin
    --dbms_output.put_line(p_date);
    p_old_comis_nc := 0;
    p_old_comis    := 0;
    p_comis_nc     := 0;
    p_comis        := 0;
    p_count_legs_by_contract_mp_day :=0;
    -- цикл по комиссиям (сделкам), взятым по площадке за дату
    for rec in (SELECT com.*, ds.t_fiid_comm
                  FROM ddlcomis_dbt com, dsfcomiss_dbt ds
                 WHERE com.t_contract = p_sfsubcontractid
                   and ds.t_feetype = 1  --ds.t_feetype
                   and com.t_comnumber = ds.t_number
                   AND com.t_date = p_date
                   AND com.t_comnumber in (1041,
                                           1064, 
                                           1035,
                                           1037,
                                           1039,
                                           1028,
                                           1027,
                                           1029,
                                           1025,
                                           1026,
                                           1001,
                                           1002,
                                           1003)) loop
      /*if (rec.t_docid=9473033) then  --отладка
        null;
      end if;*/
      v_comis    := 0;
      v_comis_nc := 0;
      v_cost_nc  := 0;
      v_sum_comis_nc :=0;
      v_cost  := 0;
      v_sum_comis :=0;
      begin
        if (rec.t_dockind = 101) then
          comission_by_ddleg(rec.t_comnumber,
                         rec.t_docid,
                         p_date,
                         v_comis,
                         v_comis_nc, v_cost_nc, v_cost);
        elsif (rec.t_dockind = 4813) then
          comission_by_ddvndeal(rec.t_comnumber,
                         rec.t_docid,
                         p_date,
                         v_comis,
                         v_comis_nc, v_cost_nc, v_cost);
        else
          dbms_output.put_line('Отсутствует в обработке! rec.t_dockind='||rec.t_dockind);
        end if;
        begin
          cost_nc_array_by_comnumber(rec.t_comnumber) := cost_nc_array_by_comnumber(rec.t_comnumber) + v_cost_nc;
        exception 
          when others then
            cost_nc_array_by_comnumber(rec.t_comnumber) := v_cost_nc;
        end;
      exception
        when others then
          insert into dl_tmp_leg_comiss_dbt
            (t_sfsubcontractid,
             t_dealid,
             t_dealstart,
             t_old_comis,
             t_old_comis_nc,
             t_comis,
             t_comis_nc,
             t_compens,
             t_error)
          values
            (rec.t_contract,
             rec.t_docid,
             p_date,
             null,
             null,
             null,
             null,
             null,
             'Ошибка при подсчете по сделке ');
          continue;
      end;  -- это еще не выход из цикла
      p_count_legs_by_contract_mp_day := p_count_legs_by_contract_mp_day + 1;
      p_old_comis    := p_old_comis + rec.t_sum;
      v_old_comis_nc := RSB_FIInstr.ConvSum(rec.t_sum,
                                                rec.t_fiid_comm,
                                                0,
                                                p_Date);
      p_old_comis_nc := p_old_comis_nc + v_old_comis_nc;
      /*if (rec.t_sum/v_old_comis_nc<>v_comis/v_comis_nc) then --отладка
        null;
      end if;*/
      insert into dl_tmp_leg_comiss_dbt
        (t_sfsubcontractid,
         t_dealid,
         t_dealstart,
         t_old_comis,
         t_old_comis_nc,
         t_comis,
         t_comis_nc,
         t_compens,
         t_dockind)
      values
        (rec.t_contract,
         rec.t_docid,
         p_date,
         rec.t_sum,
         v_old_comis_nc,
         v_comis,
         v_comis_nc,
         v_old_comis_nc - v_comis_nc,
         rec.t_dockind);
      p_comis           := p_comis + v_comis;
      p_comis_nc        := p_comis_nc + v_comis_nc;
    end loop;
    --dbms_output.put_line('count='||cost_nc_array_by_comnumber.count);
    i := cost_nc_array_by_comnumber.first; -- первый элемент массива комиссий
    while i is not null loop
      --dbms_output.put_line('cost(i)='||cost_nc_array_by_comnumber(i)||' '||i);
      v_sum_comis_nc := v_sum_comis_nc + cost_nc_array_by_comnumber(i) * comission_trader_grid_value(correspond_tarif_trader(i), cost_nc_array_by_comnumber(i));
      v_comiss_grid_value := comission_trader_grid_value(correspond_tarif_trader(i), cost_nc_array_by_comnumber(i));
      i:=cost_nc_array_by_comnumber.next(i);
    end loop;
    update dl_tmp_leg_comiss_dbt k1 set k1.t_error = to_char(v_comiss_grid_value)  where k1.t_sfsubcontractid = p_sfsubcontractid and k1.t_dealstart = p_date;
    return p_old_comis_nc - v_sum_comis_nc; --p_comis_nc; -- компенсация только в национальной валюте
  end mp_compensation_by_date;

  -- Возвращает сумму компенсации по площадке
  function mp_compensation(compens_rec    in d_tmp_compens_def34128_dbt%rowtype,
                           p_sfsubcontractid           in number /*субдоговор по площадке*/,
                           p_comis        in out number,
                           p_comis_nc     in out number,
                           p_old_comis    in out number,
                           p_old_comis_nc in out number,
                           p_count_legs_by_contract_mp in out integer) return number is
    tarif_name          varchar2(50) := '';
    v_date              date := compens_rec.t_contract_date;
    result_compensation number := 0;
    v_comis             number := 0;
    v_comis_nc          number := 0;
    v_old_comis         number := 0;
    v_old_comis_nc      number := 0;
    v_count_legs_by_contract_mp_day integer :=0;
  begin
    p_comis        := 0;
    p_comis_nc     := 0;
    p_old_comis    := 0;
    p_old_comis_nc := 0;
    p_count_legs_by_contract_mp := 0;
    while v_date < sysdate loop
      tarif_name := lower(plan_name(p_sfsubcontractid, v_date));
      if (tarif_name = 'трейдер' or tarif_name = '') then
        begin
          --dbms_output.put_line('обнаружен тариф Трейдер - компенсация по данной площадке не нужна - выходим из цикла по дням, так как предполагаем, что тариф Трейдер последний');
          exit;
        end;
      end if;
      if (tarif_name = '') then
        begin
          --dbms_output.put_line('тариф не определен - идем в следующий день');
          v_date := v_date + 1;
          continue;
        end;
      end if;
      -- вычисление компенсации за день; предполагаем, что это тариф Базовый или Инвестор
      result_compensation := result_compensation +
                             mp_compensation_by_date(p_sfsubcontractid,
                                                     v_date,
                                                     v_comis,
                                                     v_comis_nc,
                                                     v_old_comis,
                                                     v_old_comis_nc,
                                                     v_count_legs_by_contract_mp_day);
      p_comis             := p_comis + v_comis;
      p_comis_nc          := p_comis_nc + v_comis_nc;
      p_old_comis         := p_old_comis + v_old_comis;
      p_old_comis_nc      := p_old_comis_nc + v_old_comis_nc;
      p_count_legs_by_contract_mp := p_count_legs_by_contract_mp + v_count_legs_by_contract_mp_day;
      v_date              := v_date + 1;
    end loop;
    return result_compensation;
  end mp_compensation;

  function contract_compensation(compens_rec    IN d_tmp_compens_def34128_dbt%rowtype,
                                 p_taxbase      OUT dcompcomiss_tmp.t_taxbase%type,
                                 p_sfcontr_id   OUT dsfcontr_dbt.t_id%type,
                                 p_comis        IN OUT dcompcomiss_tmp.t_comis%type,
                                 p_comis_nc     IN OUT dcompcomiss_tmp.t_comis_natcur%type,
                                 p_old_comis    IN OUT dcompcomiss_tmp.t_comis_natcur%type,
                                 p_old_comis_nc IN OUT dcompcomiss_tmp.t_old_comis_natcur %type,
                                 p_count_legs_by_contract IN OUT integer)
    return number is
    compensation   number(32, 12) := 0;
    dlcontrid      ddlcontr_dbt.t_dlcontrid%type;
    v_comis        dcompcomiss_tmp.t_comis%type := 0;
    v_comis_nc     dcompcomiss_tmp.t_comis_natcur%type := 0;
    v_old_comis    dcompcomiss_tmp.t_comis_natcur%type := 0;
    v_old_comis_nc dcompcomiss_tmp.t_old_comis_natcur%type := 0;
    v_count_legs_by_contract_mp integer :=0;
  begin
    p_comis        := 0;
    p_comis_nc     := 0;
    p_old_comis    := 0;
    p_old_comis_nc := 0;
    p_count_legs_by_contract :=0;
    --цикл по площадкам по договору (вернее по субдоговорам по площадкам)
    for rec in (select sfc_s.t_id /*,
                                                                                                                                                                                          sfc_s.t_number*/,
                       sfc_s.t_partyid,
                       dlc.t_dlcontrid,
                       sfc.t_id contr_id /*,
                                                                                                                                                       sfc_s.t_datebegin,
                                                                                                                                                       sfc_s.t_dateclose,
                                                                                                                                                       sfc_s.t_servkind,
                                                                                                                                                       sfc_s.t_servkindsub,
                                                                                                                                                       mp.t_marketid,
                                                                                                                                                       sfc_s.t_number t_number_s*/
                  from ddlcontr_dbt   dlc,
                       dsfcontr_dbt   sfc,
                       ddlcontrmp_dbt mp,
                       dsfcontr_dbt   sfc_s
                 where dlc.t_sfcontrid = sfc.t_id
                   and sfc.t_number = compens_rec.t_contract
                   and mp.t_dlcontrid = dlc.t_dlcontrid
                   and sfc_s.t_id = mp.t_sfcontrid
                   and ((mp.t_marketid = 2 and sfc_s.t_servkind in (1, 21)) -- ммвб фондовый и валютный
                       or (mp.t_marketid = 151337 and sfc_s.t_servkind = 1)) -- спб фондовый
                ) loop
      v_comis        := 0;
      v_comis_nc     := 0;
      v_old_comis    := 0;
      v_old_comis_nc := 0;
      v_count_legs_by_contract_mp :=0;
      compensation   := compensation +
                        mp_compensation(compens_rec,
                                        rec.t_id /*субдоговор по площадке*/,
                                        v_comis,
                                        v_comis_nc,
                                        v_old_comis,
                                        v_old_comis_nc,
                                        v_count_legs_by_contract_mp); -- сумма компенсаций по всем площадкам
      dlcontrid      := rec.t_dlcontrid;
      p_sfcontr_id   := rec.contr_id;
      p_comis        := p_comis + v_comis;
      p_comis_nc     := p_comis_nc + v_comis_nc;
      p_old_comis    := p_old_comis + v_old_comis;
      p_old_comis_nc := p_old_comis_nc + v_old_comis_nc;
      p_count_legs_by_contract := p_count_legs_by_contract + v_count_legs_by_contract_mp ;
    end loop;
    p_taxbase := RSB_SECUR.GetDlContrTaxBase(dlcontrid,
                                             nvl(rsbsessiondata.m_curdate,
                                                 sysdate)); -- почему-то 0 выходит
    return compensation;
  end;

  procedure run is
    v_partyid      number(10);
    compensation   number(32, 12) := -1;
    v_taxbase      number(32, 12) := 0;
    v_contractid   dsfcontr_dbt.t_id%type;
    v_comis        number(32, 12) := 0;
    v_comis_nc     number(32, 12) := 0;
    v_old_comis    number(32, 12) := 0;
    v_old_comis_nc number(32, 12) := 0;
    v_count_legs_by_contract integer :=0;
    v_error varchar(200) := '';
    v_i integer :=0;
  begin
    execute immediate 'truncate table dcompcomiss_tmp';
    execute immediate 'truncate table dl_tmp_leg_comiss_dbt';
    for rec in (select * from d_tmp_compens_def34128_dbt /*where t_contract = '00/02-19292715'*/ /*fetch first 10 rows only*/) loop
      v_i := v_i + 1;
      --dbms_output.put_line('i='||v_i);
      --dbms_output.put_line('rec.t_contract='||rec.t_contract);
      v_comis        := 0;
      v_comis_nc     := 0;
      v_old_comis    := 0;
      v_old_comis_nc := 0;
      v_partyid      := contract_partyid(rec); -- если нет контракта, то вернется -1
      v_error := '';
      v_contractid := 0;
      compensation   := 0;
      v_taxbase := 0;
      if (v_partyid = -1) then
        -- контракта не существует
        --dbms_output.put_line('договора не существует ' || rec.t_contract);
        v_error := 'договора не существует или закрыт ' || rec.t_contract;
        insert into dcompcomiss_tmp
        (t_partyid,
         t_sfcontractid,
         t_compsum,
         t_taxbase,
         t_comis,
         t_comis_natcur,
         t_old_comis,
         t_old_comis_natcur,
         t_error,
         t_sfcontractnumber)
        values
        (v_partyid, /*rec.t_contract*/
         v_contractid,
         compensation,
         v_taxbase,
         v_comis,
         v_comis_nc,
         v_old_comis,
         v_old_comis_nc,
         v_error,
         rec.t_contract);
        commit;
        continue;
      end if;
      compensation := contract_compensation(rec,
                                            v_taxbase,
                                            v_contractid,
                                            v_comis,
                                            v_comis_nc,
                                            v_old_comis,
                                            v_old_comis_nc, 
                                            v_count_legs_by_contract );
      if (v_count_legs_by_contract=0) then 
        v_error := 'Не было сделок с комиссиями по трем площадкам по данному контракту';
      end if;
      insert into dcompcomiss_tmp
        (t_partyid,
         t_sfcontractid,
         t_compsum,
         t_taxbase,
         t_comis,
         t_comis_natcur,
         t_old_comis,
         t_old_comis_natcur,
         t_error,
         t_sfcontractnumber)
      values
        (v_partyid, /*rec.t_contract*/
         v_contractid,
         compensation,
         v_taxbase,
         v_comis,
         v_comis_nc,
         v_old_comis,
         v_old_comis_nc,
         v_error,
         rec.t_contract);
      commit;
    end loop;
    commit;
  exception
    when others then
      dbms_output.put_line('неизвестная ошибка ' || sqlcode || ' ' ||
                           sqlerrm || dbms_utility.format_error_backtrace);
      rollback;
  end;

end usr_def34128;
/
