CREATE OR REPLACE PACKAGE BODY Rsb_ChangeTariff
IS
  /**
   @brief Корректировка тарифной сетки существующих комиссий
  */                      
  PROCEDURE UpdateSfTarif
  IS
  BEGIN
    update dsftarif_dbt tar
       set tar.t_tarifsum = 1000,
           tar.t_minvalue = 1500000,
           tar.t_maxvalue = 9.99999999999E15 
     where tar.t_tarsclid in (select scl.t_id
                                from dsfcomiss_dbt com, dsftarscl_dbt scl
                               where com.t_code in ('ПАОМскБиржПервРСХБ_RUR', 'ПАОМскБиржПервРСХБ_USD', 'ПАОМскБиржПервРСХБ_EUR', 'ПАОМскБиржПервРСХБ_CHF', 'ПАОМскБиржПервРСХБ_CNY', 
                                                    'ПАОМскБиржПервСТОР_RUR', 'ПАОМскБиржПервСТОР_USD', 'ПАОМскБиржПервСТОР_EUR', 'ПАОМскБиржПервСТОР_CHF', 'ПАОМскБиржПервСТОР_CNY')
                                 and scl.t_feetype = com.t_feetype
                                 and scl.t_commnumber = com.t_number 
                             );
  END UpdateSfTarif; 

  /**
   @brief Корректировка алгоритма расчета существующих комиссий
  */                      
  PROCEDURE UpdateSfCalCal
  IS
  BEGIN
    update dsfcalcal_dbt cal
       set cal.t_calcmethod = 1,
           cal.t_summin = 0,
           cal.t_summax = 0 
     where (cal.t_feetype, cal.t_commnumber) in (select com.t_feetype, com.t_number
                                                   from dsfcomiss_dbt com
                                                  where com.t_code in ('ПАОМскБиржПервРСХБ_RUR', 'ПАОМскБиржПервРСХБ_USD', 'ПАОМскБиржПервРСХБ_EUR', 'ПАОМскБиржПервРСХБ_CHF', 'ПАОМскБиржПервРСХБ_CNY', 
                                                                       'ПАОМскБиржПервСТОР_RUR', 'ПАОМскБиржПервСТОР_USD', 'ПАОМскБиржПервСТОР_EUR', 'ПАОМскБиржПервСТОР_CHF', 'ПАОМскБиржПервСТОР_CNY')
                                                );
   
  END UpdateSfCalCal; 

  /**
   @brief Заведение новых комиссий
  */                      
  PROCEDURE AddComiss
  IS
    rec_count number := 0;
    parent_number number := 0;
    feetype number := 1; /*Периодическая*/
   
    PROCEDURE CreateComiss(p_com_name in varchar2, p_com_comment in varchar2, p_fiid in number, p_tarifsum in number)
    IS
      com_tarsclID number(10) := 0;
      parent_tarsclID number(10) := 0;
      com_number number := 0;
    BEGIN
      select max(T_NUMBER)+1 into com_number from dsfcomiss_dbt;
      INSERT
        INTO DSFCOMISS_DBT (T_FEETYPE,T_NUMBER,T_CODE,T_NAME,T_CALCPERIODTYPE,T_CALCPERIODNUM,T_DATE,T_PAYNDS,T_FIID_COMM,T_GETSUMMIN,
                T_SUMMIN,T_SUMMAX,T_RATETYPE,T_RECEIVERID,T_INCFEETYPE,T_INCCOMMNUMBER,T_FORMALG,T_SERVICEKIND,T_SERVICESUBKIND,T_CALCCOMISSSUMALG,
                T_SETACCSEARCHALG,T_FIID_PAYSUM,T_DATEBEGIN,T_DATEEND,T_INSTANTPAYMENT,T_PRODUCTID,T_NDSCATEG,T_ISFREEPERIOD,
                T_ISBANKEXPENSES,T_ISCOMPENSATIONCOM,T_COMMENT,T_COMISSID,T_PARENTCOMISSID)
      VALUES (feetype,com_number,p_com_name,p_com_comment,
                1,1,TO_DATE('01/01/0001 00:00:00','MM/DD/YYYY HH24:MI:SS'),1,p_fiid,CHR(0),0,0,0,1,0,0,1,1,8,1,
                1,p_fiid,TO_DATE('01/01/0001 00:00:00','MM/DD/YYYY HH24:MI:SS'),TO_DATE('01/01/0001 00:00:00','MM/DD/YYYY HH24:MI:SS'),CHR(0),0,0,CHR(0),
                CHR(0),CHR(0),p_com_comment,0,0);
   
      INSERT INTO DSFCALCAL_DBT (T_FEETYPE,T_COMMNUMBER,T_KIND,T_NUMBER,T_FILTERTYPE,T_FILTERMACRO,T_SCALETYPE,T_SCALEMACRO,T_SCALEMACRORET,
                                 T_CALCMETHOD,T_FIID_TARSCL,T_SUMMIN,T_SUMMAX,T_DESCRIPTION,T_BINDTOOBJECTS,T_ISALLOCATE,T_CONCOMID,T_ISBATCHMODE,T_FMTBLOBDATA_XXXX)
           SELECT T_FEETYPE,com_number,T_KIND,T_NUMBER,T_FILTERTYPE,T_FILTERMACRO,T_SCALETYPE,T_SCALEMACRO,T_SCALEMACRORET,
                  T_CALCMETHOD,T_FIID_TARSCL,T_SUMMIN,T_SUMMAX,T_DESCRIPTION,T_BINDTOOBJECTS,T_ISALLOCATE,T_CONCOMID,T_ISBATCHMODE,T_FMTBLOBDATA_XXXX
                  FROM DSFCALCAL_DBT WHERE T_FEETYPE = feetype AND T_COMMNUMBER = parent_number AND T_CONCOMID = 0;                    
   
      INSERT INTO DSFTARSCL_DBT (T_FEETYPE,T_COMMNUMBER,T_ALGKIND,T_ALGNUMBER,T_BEGINDATE,T_ISBLOCKED,
                                 T_ID,T_ENDDATE,T_CONCOMID)
           VALUES (feetype,com_number,8,1,TO_DATE('01/01/0001 00:00:00','MM/DD/YYYY HH24:MI:SS'),CHR(0),
                   0,TO_DATE('01/01/0001 00:00:00','MM/DD/YYYY HH24:MI:SS'),0);
   
      SELECT t_id INTO com_tarsclID
        FROM DSFTARSCL_DBT
       WHERE t_feetype = feetype AND t_commnumber = com_number AND t_concomID = 0;
   
      SELECT t_id INTO parent_tarsclID
        FROM DSFTARSCL_DBT
       WHERE t_feetype = feetype AND t_commnumber = parent_number AND t_concomID = 0;
   
      INSERT INTO DSFTARIF_DBT (T_ID,T_TARSCLID,T_SIGN,T_BASETYPE,T_BASESUM,T_TARIFTYPE,
                                T_TARIFSUM,T_MINVALUE,T_MAXVALUE,T_SORT)
           SELECT 0,com_tarsclID,T_SIGN,T_BASETYPE,T_BASESUM,T_TARIFTYPE,
                                p_tarifsum,T_MINVALUE,T_MAXVALUE,T_SORT
             FROM DSFTARIF_DBT WHERE T_TARSCLID = parent_tarsclID;
           
      INSERT INTO DOBJATCOR_DBT (T_OBJECTTYPE,T_GROUPID,T_ATTRID,T_OBJECT,T_GENERAL,T_VALIDFROMDATE,
                                 T_OPER,T_VALIDTODATE,T_SYSDATE,T_SYSTIME,T_ISAUTO)
           SELECT T_OBJECTTYPE,T_GROUPID,T_ATTRID,LPAD(feetype, 5, '0')||LPAD(com_number, 5, '0'),T_GENERAL,T_VALIDFROMDATE,
                                 T_OPER,T_VALIDTODATE,T_SYSDATE,T_SYSTIME,T_ISAUTO
             FROM DOBJATCOR_DBT WHERE T_OBJECTTYPE = 650 AND T_OBJECT = LPAD(feetype, 5, '0')||LPAD(parent_number, 5, '0');
    END;  
  BEGIN
    select T_NUMBER into parent_number from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПервСТОР_EUR');
    if parent_number > 0 then 
      /*Создание ПАОМскБиржПервСТОРНОТЫПС_RUR*/
      select count(1) into rec_count from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПервСТОРНОТЫПС_RUR');
      if rec_count = 0 then
        CreateComiss('ПАОМскБиржПервСТОРНОТЫПС_RUR', 'Комиссия за размещение бумаг не Нот, но включая процентные структурные Ноты', 0, 1000);
      end if;
   
      /*Создание ПАОМскБиржПервСТОРНОТЫПС_USD*/
      select count(1) into rec_count from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПервСТОРНОТЫПС_USD');
      if rec_count = 0 then
        CreateComiss('ПАОМскБиржПервСТОРНОТЫПС_USD', 'Комиссия за размещение бумаг не Нот, но включая процентные структурные Ноты', 7, 1000);
      end if;
   
      /*Создание ПАОМскБиржПервСТОРНОТЫПС_EUR*/
      select count(1) into rec_count from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПервСТОРНОТЫПС_EUR');
      if rec_count = 0 then
        CreateComiss('ПАОМскБиржПервСТОРНОТЫПС_EUR', 'Комиссия за размещение бумаг не Нот, но включая процентные структурные Ноты', 8, 1000);
      end if;
   
      /*Создание ПАОМскБиржПервСТОРНОТЫПС_CNY*/
      select count(1) into rec_count from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПервСТОРНОТЫПС_CNY');
      if rec_count = 0 then
        CreateComiss('ПАОМскБиржПервСТОРНОТЫПС_CNY', 'Комиссия за размещение бумаг не Нот, но включая процентные структурные Ноты', 11, 1000);
      end if;
   
      /*Создание ПАОМскБиржПервСТОРНОТЫНЕПС_RUR*/
      select count(1) into rec_count from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПервСТОРНОТЫНЕПС_RUR');
      if rec_count = 0 then
        CreateComiss('ПАОМскБиржПервСТОРНОТЫНЕПС_RUR', 'Комиссия за размещение всех Нот, за исключением процентных структурных Нот', 0, 15000);
      end if;
   
      /*Создание ПАОМскБиржПервСТОРНОТЫНЕПС_USD*/
      select count(1) into rec_count from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПервСТОРНОТЫНЕПС_USD');
      if rec_count = 0 then
        CreateComiss('ПАОМскБиржПервСТОРНОТЫНЕПС_USD', 'Комиссия за размещение всех Нот, за исключением процентных структурных Нот', 7, 15000);
      end if;
   
      /*Создание ПАОМскБиржПервСТОРНОТЫНЕПС_EUR*/
      select count(1) into rec_count from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПервСТОРНОТЫНЕПС_EUR');
      if rec_count = 0 then
        CreateComiss('ПАОМскБиржПервСТОРНОТЫНЕПС_EUR', 'Комиссия за размещение всех Нот, за исключением процентных структурных Нот', 8, 15000);
      end if;
   
      /*Создание ПАОМскБиржПервСТОРНОТЫНЕПС_CNY*/
      select count(1) into rec_count from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПервСТОРНОТЫНЕПС_CNY');
      if rec_count = 0 then
        CreateComiss('ПАОМскБиржПервСТОРНОТЫНЕПС_CNY', 'Комиссия за размещение всех Нот, за исключением процентных структурных Нот', 11, 15000);
      end if;   
    end if;
  END AddComiss; 

  /**
   @brief Добавление новых комиссий под тарифные планы
   @param[in]  p_ChangeDate    дата вступления изменений в силу
   @param[in]  p_TPName        наименование тарифного плана  
  */                      
  PROCEDURE LinkComissToSf(p_ChangeDate IN DATE, p_TPName IN VARCHAR2)
  IS
    l_tp_commiss_link t_tp_commiss_link_list := t_tp_commiss_link_list(t_tp_commiss_link(tp_name => p_TPName, commiss_name => 'ПАОМскБиржПервСТОРНОТЫПС_RUR'),
                                                                       t_tp_commiss_link(tp_name => p_TPName, commiss_name => 'ПАОМскБиржПервСТОРНОТЫПС_USD'),
                                                                       t_tp_commiss_link(tp_name => p_TPName, commiss_name => 'ПАОМскБиржПервСТОРНОТЫПС_EUR'),
                                                                       t_tp_commiss_link(tp_name => p_TPName, commiss_name => 'ПАОМскБиржПервСТОРНОТЫПС_CNY'),
                                                                       t_tp_commiss_link(tp_name => p_TPName, commiss_name => 'ПАОМскБиржПервСТОРНОТЫНЕПС_RUR'),
                                                                       t_tp_commiss_link(tp_name => p_TPName, commiss_name => 'ПАОМскБиржПервСТОРНОТЫНЕПС_USD'),
                                                                       t_tp_commiss_link(tp_name => p_TPName, commiss_name => 'ПАОМскБиржПервСТОРНОТЫНЕПС_EUR'),
                                                                       t_tp_commiss_link(tp_name => p_TPName, commiss_name => 'ПАОМскБиржПервСТОРНОТЫНЕПС_CNY')
                                                                      );   
  
    function get_commiss (p_commis_id dsfcomiss_dbt.t_comissid%type)
      return dsfcomiss_dbt%rowtype is
      l_commiss_row dsfcomiss_dbt%rowtype;
    begin
      select *
        into l_commiss_row
        from dsfcomiss_dbt c
       where c.t_comissid = p_commis_id;
      
      return l_commiss_row;
    end get_commiss;
    
    function save_link_comiss_w_tp (
      p_tp_id           dsfconcom_dbt.t_objectid%type,
      p_feetype         dsfconcom_dbt.t_feetype%type,
      p_commnumber      dsfconcom_dbt.t_commnumber%type,
      p_calcperiodtype  dsfconcom_dbt.t_calcperiodtype%type,
      p_calcperiodnum   dsfconcom_dbt.t_calcperiodnum%type
    ) return dsfconcom_dbt.t_id%type is
      l_id dsfconcom_dbt.t_id%type;
    begin
      insert into dsfconcom_dbt(t_objectid,
                                t_feetype,
                                t_commnumber,
                                t_status,
                                t_calcperiodtype,
                                t_calcperiodnum,
                                t_date,
                                t_getsummin,
                                t_summin,
                                t_summax,
                                t_datebegin,
                                t_dateend,
                                t_objecttype,
                                t_id,
                                t_sfplanid,
                                t_isfreeperiod,
                                t_isindividual)
      values (p_tp_id,
              p_feetype,
              p_commnumber,
              case when p_calcperiodtype = 0 then 1 else 0 end,
              p_calcperiodtype,
              p_calcperiodnum,
              to_date('01.01.0001', 'dd.mm.yyyy'),
              chr(0),
              0,
              0,
              p_ChangeDate,
              to_date('01.01.0001', 'dd.mm.yyyy'),
              57,
              0,
              0,
              chr(0),
              chr(88))
      returning t_id into l_id;
    
      return l_id;
    end save_link_comiss_w_tp;
    
    function save_tarscl (
      p_feetype         dsftarscl_dbt.t_feetype%type,
      p_commnumber      dsftarscl_dbt.t_commnumber%type,
      p_link_id         dsftarscl_dbt.t_concomid%type
    ) return dsftarscl_dbt.t_id%type is
      l_id dsftarscl_dbt.t_id%type;
    begin
      insert into dsftarscl_dbt (t_feetype,
                                 t_commnumber,
                                 t_algkind,
                                 t_algnumber,
                                 t_begindate,
                                 t_isblocked,
                                 t_id,
                                 t_enddate,
                                 t_concomid)
      values (p_feetype,
              p_commnumber,
              8,
              1,
              to_date('01.01.0001', 'dd.mm.yyyy'),
              chr(0),
              0,
              to_date('01.01.0001', 'dd.mm.yyyy'),
              p_link_id)
      returning t_id into l_id;
  
      --Копируем на все тарифы основную тарифную сетку
      INSERT INTO DSFTARIF_DBT (T_ID, T_TARSCLID, T_SIGN, T_BASETYPE, T_BASESUM, T_TARIFTYPE, T_TARIFSUM, T_MINVALUE, T_MAXVALUE, T_SORT)
      SELECT 0,
             l_id,
             tarif.T_SIGN,
             tarif.T_BASETYPE,
             tarif.T_BASESUM,
             tarif.T_TARIFTYPE,
             tarif.T_TARIFSUM,
             tarif.T_MINVALUE,
             tarif.T_MAXVALUE,
             tarif.T_SORT
       FROM DSFTARIF_DBT tarif, dsftarscl_dbt tarscl
      WHERE tarif.T_TARSCLID = tarscl.t_id
        AND tarscl.t_feetype = p_feetype
        AND tarscl.t_commnumber = p_commnumber
        AND tarscl.t_concomid = 0;
      
      return l_id;
    end save_tarscl;
    
    procedure link_commiss_to_contracts (
      p_tp_id           dsfconcom_dbt.t_objectid%type,
      p_feetype         dsfconcom_dbt.t_feetype%type,
      p_commnumber      dsfconcom_dbt.t_commnumber%type
    ) is
    begin
      insert into dsfconcom_dbt (t_objectid,
                                 t_objecttype,
                                 t_feetype,
                                 t_commnumber,
                                 t_sfplanid,
                                 t_status,
                                 t_datebegin,
                                 t_calcperiodtype,
                                 t_calcperiodnum,
                                 t_date,
                                 t_isfreeperiod,
                                 t_getsummin,
                                 t_summin,
                                 t_summax,
                                 t_dateend,
                                 t_isindividual,
                                 t_isbankexpenses,
                                 t_iscompensationcom)
      select p.t_sfcontrid,
             659,
             tp_link.t_feetype,
             tp_link.t_commnumber,
             p.t_sfplanid,
             tp_link.t_status,
             greatest(p.t_begin, tp_link.t_datebegin),
             tp_link.t_calcperiodtype,
             tp_link.t_calcperiodnum,
             tp_link.t_date,
             tp_link.t_isfreeperiod,
             tp_link.t_getsummin,
             tp_link.t_summin,
             tp_link.t_summax,
             tp_link.t_dateend,
             chr(0),
             tp_link.t_isbankexpenses,
             tp_link.t_iscompensationcom
        from dsfconcom_dbt tp_link
        join dsfcontrplan_dbt p on p.t_sfplanid = tp_link.t_objectid
       where tp_link.t_objectid = p_tp_id
         and tp_link.t_objecttype = 57
         and tp_link.t_feetype = p_feetype
         and tp_link.t_commnumber = p_commnumber
         and p.t_end = to_date('01.01.0001', 'dd.mm.yyyy');
    end link_commiss_to_contracts;
    
    procedure link_tarscl_w_concom (
      p_tarscl_id     dsfcomtarscl_dbt.t_tarsclid%type,
      p_feetype       dsfconcom_dbt.t_feetype%type,
      p_commnumber    dsfconcom_dbt.t_commnumber%type,
      p_tp_id         dsfconcom_dbt.t_sfplanid%type
    ) is
    begin
      insert into dsfcomtarscl_dbt (t_concomid,
                                    t_tarsclid,
                                    t_level)
        select concom.t_id,
               p_tarscl_id,
               3
          from dsfconcom_dbt concom
         where concom.t_objecttype = 659
           and concom.t_feetype = p_feetype
           and concom.t_commnumber = p_commnumber
           and concom.t_sfplanid = p_tp_id;
    end link_tarscl_w_concom;
    
    procedure link_commiss_to_tp (
      p_commiss_id dsfcomiss_dbt.t_comissid%type,
      p_tp_id      dsfplan_dbt.t_sfplanid%type
    ) is
      l_link_id     dsfconcom_dbt.t_id%type;
      l_commiss_row dsfcomiss_dbt%rowtype;
      l_tarscl_id   dsftarscl_dbt.t_id%type;
    begin
      l_commiss_row := get_commiss(p_commis_id => p_commiss_id);
  
      l_link_id := save_link_comiss_w_tp(p_tp_id          => p_tp_id,
                                         p_feetype        => l_commiss_row.t_feetype,
                                         p_commnumber     => l_commiss_row.t_number,
                                         p_calcperiodtype => l_commiss_row.t_calcperiodtype,
                                         p_calcperiodnum  => l_commiss_row.t_calcperiodnum);
  
      l_tarscl_id := save_tarscl(p_feetype    => l_commiss_row.t_feetype,
                                 p_commnumber => l_commiss_row.t_number,
                                 p_link_id    => l_link_id);
  
      link_commiss_to_contracts(p_tp_id      => p_tp_id,
                                p_feetype    => l_commiss_row.t_feetype,
                                p_commnumber => l_commiss_row.t_number);
  
      link_tarscl_w_concom(p_tarscl_id  => l_tarscl_id,
                           p_feetype    => l_commiss_row.t_feetype,
                           p_commnumber => l_commiss_row.t_number,
                           p_tp_id      => p_tp_id);
    end link_commiss_to_tp;
  BEGIN
    for rec in (select p.t_sfplanid,
                       cp.t_comissid
                  from table(l_tp_commiss_link) t
                  left join dsfplan_dbt p on p.t_name = t.tp_name
                  left join dsfcomiss_dbt cp on cp.t_code = t.commiss_name
               )
    loop
      link_commiss_to_tp(p_commiss_id => rec.t_comissid, p_tp_id => rec.t_sfplanid);
    end loop;

  END LinkComissToSf; 


  /**
   @brief Исключение старых комиссий из под тарифных планов
   @param[in]  p_ChangeDate    дата вступления изменений в силу
   @param[in]  p_TPName        наименование тарифного плана   
  */                      
  PROCEDURE ExclusionComissFromTP(p_ChangeDate IN DATE, p_TPName IN VARCHAR2)
  IS
  BEGIN
    update dsfconcom_dbt concom
       set concom.t_dateend = p_ChangeDate
     where concom.t_objecttype = 57
       and (concom.t_feetype, concom.t_commnumber) in (select com.t_feetype, com.t_number
                                                         from dsfcomiss_dbt com
                                                        where com.t_code in ('ПАОМскБиржПервСТОР_RUR', 'ПАОМскБиржПервСТОР_USD', 'ПАОМскБиржПервСТОР_EUR', 'ПАОМскБиржПервСТОР_CHF', 'ПАОМскБиржПервСТОР_CNY')
                                                      )
       and concom.t_objectid in (select plan.t_sfplanid 
                                   from dsfplan_dbt plan
                                  where plan.t_name = p_TPName
                                );
   
    update dsfconcom_dbt concom
       set concom.t_dateend = p_ChangeDate
     where concom.t_objecttype = 659
       and (concom.t_feetype, concom.t_commnumber) in (select com.t_feetype, com.t_number
                                                         from dsfcomiss_dbt com
                                                        where com.t_code in ('ПАОМскБиржПервСТОР_RUR', 'ПАОМскБиржПервСТОР_USD', 'ПАОМскБиржПервСТОР_EUR', 'ПАОМскБиржПервСТОР_CHF', 'ПАОМскБиржПервСТОР_CNY')
                                                      )
       and concom.t_sfplanid in (select plan.t_sfplanid 
                                   from dsfplan_dbt plan
                                  where plan.t_name = p_TPName
                                )
       and concom.t_dateend = to_date('01010001','ddmmyyyy');
  END ExclusionComissFromTP; 

END Rsb_ChangeTariff;