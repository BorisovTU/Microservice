create or replace package body RSB_W475SOFR2CHD is

 g_dt_begin               date;
 g_dt_end                 date;
 g_BROKERREP_DL_SETTLOPER number;
 g_FIKIND_CURRENCY        number;
 g_DL_CALCOPER            number;
 g_ourbank                number;
 
 procedure initial_param(p_dt_begin               date
                        ,p_dt_end                 date
                        ,p_BROKERREP_DL_SETTLOPER number
                        ,p_FIKIND_CURRENCY        number
                        ,p_DL_CALCOPER            number
                        ,p_ourbank                number) 
 is
   
 begin
   
 execute immediate 'truncate table dkl11sofr2dwh_dbt';
 execute immediate 'truncate table W475_TMP';
 
   select trunc(p_dt_end,'mm')
         ,trunc(last_day(p_dt_end))
    into g_dt_begin, g_dt_end
    from dual;

 g_BROKERREP_DL_SETTLOPER := p_BROKERREP_DL_SETTLOPER;
 g_FIKIND_CURRENCY        := p_FIKIND_CURRENCY;
 g_DL_CALCOPER            := p_DL_CALCOPER;
 g_ourbank                := p_ourbank;
  
 end;                         
 
 
 procedure load_bufferTable_1(p_out_err_msg out        varchar2
                             ,p_out_result  out        number) 
 is

 begin  

  --raise_application_error(-20101, 'Expecting at least 1000 tables');
  --1 Первичная загрузка данных
  INSERT INTO W475_TMP (t_dlcontrid
                        ,t_sfcontrid
                        ,t_contrnumber
                        ,t_contrdatebegin
                        ,t_planname
                        ,t_clientname
                        ,t_clientcode
                        ,t_depname
                        ,t_marketname
                        ,t_sfcontridsm
                        ,t_sfcontridfm
                        ,t_sfcontridom
                        ,t_sfcontridcm
                        ,t_depcode
                        ,T_FINNAME
                        , T_DS_PLUS
                        , T_DS_MINUS
                        , T_TURNSUM
                        , T_TURNSUMREPO
                        , T_TURNSUMSVOP
                        , T_DS_IN
                        , T_DS_OUT
                        , T_P_PLUS
                        , T_P_MINUS
                        , T_P_IN
                        , T_P_OUT
                        , T_COMSUM
                        , T_COMSUMREPO
                        , T_SPECIAL_REPO
                        , T_COMSUMSVOP
                        , T_VAL_SVOP
                        ,t_partyid
                        ,born_date
                        , t_ComSumBank
                        , t_ComSumBankRepo) 
      (select t_dlcontrid
            , t_sfcontrid
            , t_contrnumber
            , t_contrdatebegin
            , t_planname
            , t_clientname
            , t_clientcode
            , t_depname
            , t_marketname
            , t_sfcontridsm
            , t_sfcontridfm
            , t_sfcontridom
            , t_sfcontridcm
            , t_depcode
            ,  chr(1),0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
            ,t_partyid
            ,pt.t_born AS born_date
            , 0, 0    
            from 
            (select  /*USE_NL(SFCONTRCM,SFCONTROM,SFCONTRFM,SFCONTRSM,DP_DEP)*/  t_dt1
                                                       , t_dt2
                                                       , t_dlcontrid
                                                                 , t_department
                                                                 , t_partyid
                                                                 , nvl(t_sfcontridsm, nvl(t_sfcontridfm, nvl(t_sfcontridom, t_sfcontridcm))) t_sfcontrid --ИД договора
                                                                 , t_contrnumber --№ договора об оказании услуг брокерского обслуживания на рынке ценных бумаг
                                                                 , t_contrdatebegin --Дата открытия договора
                                                                 , t_planname --Тарифный план 
                                                                 , t_clientname --ФИО Клиента 
                                                                 , t_clientcode --Уникальный номер клиента
                                                                 , t_marketname
                                                                 , (select ptdep.t_name   
                                                                          from dparty_dbt ptdep --субьект экономики
                                                                              ,ddp_dep_dbt dpdep   --УЗЕЛ ТС  
                                                                          where dpdep.t_name = t_depcode -- код филиала.пользовательский номер узла  
                                                                           and dpdep.t_partyid = ptdep.t_partyid --идентификатор связанного субъекта 
                                                                           ) t_depname --наименования филиала.
                                                                  ,t_depcode
                                                                  ,nvl(t_sfcontridsm, 0) t_sfcontridsm --замена пустоты на ноль
                                                                  ,nvl(t_sfcontridfm, 0) t_sfcontridfm
                                                                  ,nvl(t_sfcontridom, 0)t_sfcontridom
                                                                  ,nvl(t_sfcontridcm, 0) t_sfcontridcm        
                                                                 from (
                                                                 select t_dt1, t_dt2 --Даты которые задаем в отчете
                                                                   ,dlcontr.t_dlcontrid t_dlcontrid --идентификатор  ДБО
                                                                  ,sfcontr.t_id t_sfcontrid --идентификатор договора 
                                                                  , sfcontr.t_department t_department --филиал 
                                                                  , sfcontr.t_partyid t_partyid --плательщик 
                                                                  , sfcontr.t_number t_contrnumber --номер договора 
                                                                  , sfcontr.t_datebegin t_contrdatebegin --дата начала 
                                                                  , nvl(sfplan.t_name,chr(1)) t_planname --наименование  ТП
                                                                  , nvl(party.t_name,chr(1)) t_clientname  --полное наименование субьекта экономики
                                                                  , nvl((select t_code 
                                                                          from ddlobjcode_dbt --код объекта
                                                                         where t_objecttype = 207  --идентификатор типа объекта 
                                                                           and t_codekind = 1 /*\*and t_state = 0*\ */
                                                                           and t_objectid = dlcontrmp.t_dlcontrid --связка с площадкой ДБО
                                                                           ),chr(1)) t_clientcode --код объекта 
                                                                  , case 
                                                                         when SubStr(sfcontr.t_number,3,1) = '-' or SubStr(sfcontr.t_number,3,1) = '/' 
                                                                               then   SubStr( sfcontr.t_number,1,2)                        
                                                                         else '00'                       
                                                                   end||'00' t_depcode,  --Код филиала                 
                                                                   case 
                                                                     when sfcontrsm.t_id is not null and dlcontrmp.t_marketid = 151337
                                                                       then 'СПБ'
                                                                     when sfcontrsm.t_id is not null and dlcontrmp.t_marketid = 2    
                                                                       then 'Фондовый'  
                                                                     when sfcontrfm.t_id is not null 
                                                                       then 'Срочный'                       
                                                                     when sfcontrom.t_id is not null 
                                                                       then 'Внебиржевой'                         
                                                                     when sfcontrcm.t_id is not null 
                                                                        then 'Валютный'                         
                                                                     else chr(1) 
                                                                   end t_marketname --Какой рынок 
                                                                 , sfcontrsm.t_id t_sfcontridsm /*Фондовый дилинг Биржевой рынок*/
                                                                 , sfcontrfm.t_id t_sfcontridfm /* Срочные контракты Биржевой рынок*/
                                                                 , sfcontrom.t_id t_sfcontridom /*Фондовый дилинг Внебиржевой рынок**/
                                                                 , sfcontrcm.t_id t_sfcontridcm /* Валютный дилинг*/
                                                                           
                                                                 from dsfcontr_dbt sfcontr --   Договор обслуживания 
                                                                 join (select g_dt_begin t_dt1, g_dt_end t_dt2 from dual) on 1 = 1 
                                                                      and sfcontr.t_partyid != 1   -- плательщик не равен 1         
                                                                 join dset_sfc_u_tmp_ set_sfc_u on set_sfc_u.t_contrid = sfcontr.t_id and set_sfc_u.t_setflag = chr(88)    --временная табличка          
                                                                 join ddlcontr_dbt dlcontr on dlcontr.t_sfcontrid = sfcontr.t_id  --Договор брокерского обслуживания          
                                                                 join ddlcontrmp_dbt dlcontrmp on dlcontrmp.t_dlcontrid = dlcontr.t_dlcontrid  --Площадка ДБО          
                                                                 left join dparty_dbt party on party.t_partyid = sfcontr.t_partyid   --Субьект экономики        
                                                                 left join dsfcontrplan_dbt sfcontrplan on sfcontrplan.t_sfcontrid = dlcontrmp.t_sfcontrid --История ТП договора
                                                                              and  sfcontrplan.t_begin <= t_dt2 --дата начала 
                                                                              and (sfcontrplan.t_end = to_date('01.01.0001','dd.mm.yyyy') or  sfcontrplan.t_end >= t_dt2)  --дата окончания             
                                                                left join dsfplan_dbt sfplan on sfplan.t_sfplanid = sfcontrplan.t_sfplanid  --Тарифный план        
                                                                left join dobjcode_dbt objcode on objcode.t_objecttype = 3 --КОд объекта
                                                                               and   objcode.t_codekind = 8 --идентификатор вида кода объекта 
                                                                               and   objcode.t_objectid = party.t_partyid --идентификатор типа объекта 
                                                                               and   objcode.t_bankdate <= t_dt2 --дата операционного дня 
                                                                               and   (objcode.t_bankclosedate = to_date('01.01.0001','dd.mm.yyyy') or   objcode.t_bankclosedate >= t_dt2)  -- дата закрытия кода           
                                                                  left join ddp_dep_dbt dp_dep on dp_dep.t_code = sfcontr.t_department  --Узел терр. структуры            
                                                                  left join dsfcontr_dbt sfcontrsm on sfcontrsm.t_id = dlcontrmp.t_sfcontrid --   Договор обслуживания 
                                                                        and sfcontrsm.t_servkind = 1/*Фондовый дилинг */
                                                                        and  sfcontrsm.t_servkindsub = 8/*Биржевой рынок */
                                                                 left join dsfcontr_dbt sfcontrfm on sfcontrfm.t_id = dlcontrmp.t_sfcontrid 
                                                                    and sfcontrfm.t_servkind = 15/*Срочные контракты*/  
                                                                    and sfcontrfm.t_servkindsub = 8/*Биржевой рынок*/ 
                                                                 left join dsfcontr_dbt sfcontrom on sfcontrom.t_id = dlcontrmp.t_sfcontrid 
                                                                    and sfcontrom.t_servkind = 1/*Фондовый дилинг*/ 
                                                                    and sfcontrom.t_servkindsub = 9/*Внебиржевой рынок*/
                                                                 left join dsfcontr_dbt sfcontrcm on sfcontrcm.t_id = dlcontrmp.t_sfcontrid 
                                                                    and sfcontrcm.t_servkind = 21/*Валютный дилинг*/
                                                                )       
                                                                where (1 = 0      or t_sfcontridsm is not null             
                                                                                  or t_sfcontridfm is not null             
                                                                                  or t_sfcontridom is not null             
                                                                                  or t_sfcontridcm is not null) 
                                                                   
                                  
                                  
                                  ) left join dpersn_dbt pt ON pt.t_personid = t_partyid  --Физическое лицо
                                   );
                                   
  p_out_result:= 0; 
  p_out_err_msg:= '';
                                      
 exception
  when others  
    then 

       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);  
  end;
  
  
  procedure load_bufferTable_2(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
 is

 begin  
  --raise_application_error(-20101, 'Expecting at least 1000 tables');
    --2 Отбор данных
  UPDATE W475_TMP W475     
  SET t_Sfcontrid =   (select max(sfcontr.t_id) 
                          From dsfcontr_dbt sfcontr, dsfcontr_dbt sfcontr2, ddlcontr_dbt dlcontr                           
                         where sfcontr.t_ServKind in (15  /*15 Срочные контракты (ФИССИКО), 21 Валютный дилинг*/)              
                           and (sfcontr.t_DateClose = to_date('01.01.0001', 'dd.mm.yyyy') or sfcontr.t_DateClose >= g_dt_end )                                                  
                           and sfcontr2.t_ServKind = 0                                                    
                           and (sfcontr2.t_DateClose = to_date('01.01.0001', 'dd.mm.yyyy') or sfcontr2.t_DateClose >= g_dt_end )                                                 
                           and sfcontr2.t_id = dlcontr.t_sfcontrid                                   
                           and dlcontr.t_dlcontrid = W475.T_dlcontrid                                 
                           and sfcontr.t_id in (select contr.T_SFCONTRID from  DDLCONTRMP_DBT contr where contr.t_dlcontrid =  dlcontr.t_dlcontrid)
                                                                 )     
  where W475.t_Sfcontrid is null;

  --3  Обновление данных после уточнения используемы при печати (см.ниже )
  UPDATE W475_TMP t      
  SET t_contrnumber =         (select d.t_number from dsfcontr_dbt d where d.t_id =  t.t_Sfcontrid);
  
  
 --конкатенация  площадки в случае если обрезан договор.
  MERGE INTO W475_TMP w475                                                                                                
   using  (
   SELECT  t.t_contrnumber || 
           case when substr(t.t_contrnumber,-1) <> '_' then '_' end || 
           case 
               when d.t_servkind = 1 and d.t_servkindsub = 8 and dlcontm.t_marketid <> 151337 then 'ф' 
               when d.t_servkind = 1 and d.t_servkindsub = 9 then 'в'
               when d.t_servkind = 15 and d.t_servkindsub = 8 then 'с'
               when d.t_servkind = 21 and d.t_servkindsub = 8 then 'v'
               else 's'
           end as t_contrnumber_new,
           t.t_sfcontrid      
    FROM W475_TMP t inner join DSFCONTR_DBT D on t.t_sfcontrid = d.t_id
                     left join ddlcontrmp_dbt dlcontm on d.t_id = dlcontm.t_sfcontrid
    WHERE lower(t.t_contrnumber)  not like '%!_ф' ESCAPE '!' 
      and lower(t.t_contrnumber) not like '%!_в' ESCAPE '!'  
      and lower(t.t_contrnumber) not like '%!_с' ESCAPE '!' 
      and lower(t.t_contrnumber) not like '%!_c' ESCAPE '!' 
      and lower(t.t_contrnumber) not like '%!_v' ESCAPE '!' 
      and lower(t.t_contrnumber) not like '%!_s' ESCAPE '!' 
      and dlcontm.t_marketid > 0) r       
        ON (r.t_sfcontrid = w475.t_sfcontrid )                                       
        WHEN MATCHED THEN                                                               
          UPDATE SET W475.t_Contrnumber = r.t_contrnumber_new;

  --4  Наименования бумаг 
  UPDATE W475_TMP     
  SET t_finname = (SELECT rsb_struct.getstring (t_text) 
                    FROM dnotetext_dbt                          
                      WHERE     t_notekind = 150                                             
                          AND t_objecttype = 207                                          
                           AND t_documentid = LPAD (t_dlcontrid, 34, 0));


                    
   --5 Суммы зачислений и списаний;                      
  MERGE INTO W475_TMP w475                                                                                                
   using  (SELECT nptxop.t_contract 
                 ,NVL(SUM(decode(nptxop.t_subkind_operation,10,RSI_RSB_FIInstr.ConvSum(nptxop.t_outsum,nptxop.t_currency,0,nptxop.T_OPERDATE,1),0)),0) rest_plus
                 ,NVL(SUM(decode(nptxop.t_subkind_operation,20,RSI_RSB_FIInstr.ConvSum(nptxop.t_outsum,nptxop.t_currency,0,nptxop.T_OPERDATE,1),0)),0) rest_minus               
            FROM dnptxop_dbt nptxop, ddlrq_dbt rq                                                                                
              WHERE nptxop.t_dockind = 4607                                                                                         
                AND nptxop.t_subkind_operation in( 10,20)/*зачисление и списание*/     
                AND nptxop.t_status = 2                                                         
                AND rq.t_dockind = nptxop.t_dockind                                    
                AND rq.t_docid = nptxop.t_id                                                    
                AND rq.t_state != 7                                                             
                AND nptxop.t_operdate BETWEEN g_dt_begin AND g_dt_end
                group by nptxop.t_contract
                ) r       
        ON (r.t_contract = w475.t_sfcontrid )                                       
        WHEN MATCHED THEN                                                               
          UPDATE SET W475.t_ds_plus = R.REST_plus, W475.t_ds_minus = R.REST_minus;
          
 
 
 p_out_result:= 0; 
 p_out_err_msg:= ''; 
  
         
 exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);   
  end;
  
  
  -- Function and procedure implementations
  procedure load_bufferTable_3(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
  is
 
 FUNCTION GetClientInAccSum(p_acc_number varchar2
                           ,p_code_cur   number
                           ,p_client_ID  number
                           ,p_is_IN      number)

 RETURN NUMBER
   IS
  v_sum number:= 0;
  v_sql clob;
  BEGIN
     --Поиск сумм списания/зачисления по расчетным операциям ВУ по аналогии с отчетом Брокера  
   v_sql := q'[SELECT NVL (SUM(RSI_RSB_FIInstr.ConvSum(dl_acc.t_sum,dl_acc.t_fiid,0,dl_acc.t_valuedate,1)),0) AS InOutSum               
               FROM ddl_acc_dbt dl_acc,                                   
                    doproper_dbt oproper,                                 
                    doprdocs_dbt oprdocs,                                 
                    dacctrn_dbt acctrn                                    
              WHERE dl_acc.t_fikind = :FIKIND_CURRENCY                      
                AND dl_acc.t_fiid = :code_cur                        
                AND dl_acc.t_operkind = :BROKERREP_DL_SETTLOPER                 
                AND dl_acc.t_opertype = 1                             
                AND dl_acc.t_client = :client_ID                  
                AND dl_acc.t_dockind = :DL_CALCOPER                     
                AND dl_acc.t_valuedate >= :m_BeginDate                  
                AND dl_acc.t_valuedate <= :m_DateEnd                  
                AND dl_acc.t_dockind = oproper.t_dockind              
                AND LPAD (DL_ACC.T_ID, 10, 0) = oproper.t_documentid  
                AND oprdocs.t_id_operation = oproper.t_id_operation   
                AND OPRDOCS.T_ACCTRNID = acctrn.t_acctrnid]';
                  
   
    if(p_is_IN = 1) then
        v_sql := v_sql || ' AND ACCTRN.T_ACCOUNT_RECEIVER = :accNumber ';
    else
        v_sql := v_sql || ' AND ACCTRN.T_ACCOUNT_PAYER = :accNumber ';
    end if;         
   -- dbms_output.put_line(v_sql);
    
    execute immediate v_sql
    into v_sum
    using  g_FIKIND_CURRENCY --вид актива
         , p_code_cur
         , g_BROKERREP_DL_SETTLOPER --вид операции
         , p_client_ID  
         , g_DL_CALCOPER --вид документа
         , g_dt_begin
         , g_dt_end
         , p_acc_number;

  RETURN v_sum;
 exception when others 
   then
     return v_sum;
 END; 
 
begin
               
  --7 Получаем id договора и апдейтим суммы с учетом операций РОВУ 
  declare
   v_ds_plus  number:= 0;
   v_ds_minus number:= 0;
   v_servkind     dsfcontr_dbt.t_servkind%TYPE;
   v_servkind_sub dsfcontr_dbt.t_servkindsub%TYPE;
  begin                 
    for r in ( select  t_dlcontrid 
                     , t_sfcontrid 
                     , t_ds_plus
                     , t_ds_minus
     from  W475_TMP t 
     where t.t_sfcontrid in (select t_clientcontr 
                               from ddl_acc_dbt 
                              where t_fikind = 1 
                                and t_opertype = 1 
                                and t_date between g_dt_begin and g_dt_end ) 
       and  t.t_sfcontrid is not null)
       
       loop
         --получить вид и подвид обслуживания
         select distinct 
                sf.t_servkind
               ,sf.t_servkindsub 
          into v_servkind
              ,v_servkind_sub
         from ddlcontrmp_dbt mp
             ,dsfcontr_dbt sf
          where mp.t_sfcontrid = sf.t_id 
            and mp.t_dlcontrid = r.t_dlcontrid
            and nvl(MP.T_SFCONTRID,0) = r.t_sfcontrid;
            
        v_ds_plus := nvl(r.t_ds_plus,0);
        v_ds_minus:= nvl(r.t_ds_minus,0);
        
        for rr in (
               select ac.t_account
                     ,ac.t_code_currency
                     ,sf.t_partyid 
               from DDLCONTRMP_DBT MP
                  , DSFCONTR_DBT SF
                  , DACCOUNT_DBT AC
                  , DSETTACC_DBT SA
                  , DSFSSI_DBT SI 
              where MP.T_DLCONTRID = r.t_dlcontrid 
                AND SF.T_ID = MP.T_SFCONTRID     
                AND nvl (SF.T_SERVKIND,0) = v_servkind 
                AND SF.T_SERVKINDSUB = v_servkind_sub
                AND SI.T_OBJECTTYPE = 659
                AND SI.T_OBJECTID = LPAD(SF.T_ID, 10, '0')
                AND SA.T_SETTACCID = SI.T_SETACCID
                AND AC.T_ACCOUNT = SA.T_ACCOUNT
                AND AC.T_CHAPTER = SA.T_CHAPTER
                AND AC.T_CODE_CURRENCY = SA.T_FIID
                AND AC.T_BALANCE IN (30601, 30606)
                )
              loop
                v_ds_plus := v_ds_plus + GetClientInAccSum(rr.t_account
                                                          ,rr.t_code_currency
                                                          ,rr.t_partyid
                                                          ,1);
                v_ds_minus:= v_ds_minus + GetClientInAccSum(rr.t_account
                                                            ,rr.t_code_currency
                                                            ,rr.t_partyid
                                                            ,0);
              
              end loop;
                
              UPDATE W475_TMP   
                 SET t_ds_plus = v_ds_plus
                   , t_ds_minus = v_ds_minus
              WHERE  t_sfcontrid = r.t_sfcontrid;
      end loop;
       
  end;  
                                          
   p_out_result:= 0; 
   p_out_err_msg:= '';
   
 
             
exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);                                                                                                                                         
end;

 
  
  
 procedure load_bufferTable_4(p_out_err_msg out        varchar2
                             ,p_out_result  out        number) 
 is

 begin 
   
  --8  t_turnsum/ Оборотов всего
    
   MERGE INTO W475_TMP w475         
   using (SELECT /*+ cardinality(w 100)*/ 
                      dl_tick.t_clientcontrid
                    , NVL (SUM (decode( dlrq.t_type, 3, decode (rsb_secur.IsSale(rsb_secur.get_operationgroup(rsb_secur.get_opersystypes(dl_tick.t_dealtype, dl_tick.t_bofficekind))), 1, 1, -1), 1)*RSI_RSB_FIInstr.ConvSum(dlrq.t_factamount, dlrq.t_fiid, 0, dlrq.t_factdate, 1)),0) rest             
          FROM  W475_TMP w 
                      join ddl_tick_dbt dl_tick on w.t_sfcontrid = dl_tick.t_clientcontrid                            
                      JOIN ddlrq_dbt dlrq  ON  dlrq.t_dockind = dl_tick.t_bofficekind                                                                 
                       AND dlrq.t_docid = dl_tick.t_dealid                                                            
                       AND dlrq.t_subkind = 0                                                                                      
                       AND dlrq.t_type IN (2,3)                                                       
                       AND dlrq.t_kind IN (0, 1)                                                                          
          WHERE dl_tick.t_bofficekind in(101,117)                             
            AND dlrq.t_factdate BETWEEN g_dt_begin AND g_dt_end
            group by dl_tick.t_clientcontrid) r  
            ON (r.t_clientcontrid = w475.t_sfcontrid )                                           
    WHEN MATCHED THEN 
      UPDATE SET W475.T_turnsum = R.REST;           
                
      
   /*9  t_turnsumrepo; Обороты за период по сделкам РЕПО*/
   
   MERGE INTO W475_TMP w475  using 
   (SELECT d.t_SfContrID
          ,NVL (SUM (decode( dlrq.t_type, 3, decode (Opr.IsSale, 1, 1, -1), 1)*RSI_RSB_FIInstr.ConvSum(dlrq.t_factamount, dlrq.t_fiid, 0, dlrq.t_factdate, 1)),0) rest            
          FROM  W475_TMP d,            
           ( select t_Kind_Operation
                   ,rsb_secur.IsRepo (rsb_secur.get_operationgroup (rsb_secur.get_opersystypes (t_Kind_Operation,t_DocKind))) as IsRepo
                   ,rsb_secur.IsSale (rsb_secur.get_operationgroup (rsb_secur.get_opersystypes (t_Kind_Operation,t_DocKind))) as IsSale                        
               from doprkoper_dbt                       
              where t_DocKind = 101) Opr,                
                ddl_tick_dbt dl_tick   
                JOIN ddlrq_dbt dlrq ON    dlrq.t_dockind = dl_tick.t_bofficekind                                                                  
                AND dlrq.t_docid = dl_tick.t_dealid                                     
                AND dlrq.t_subkind = 0                                                                                      
                AND dlrq.t_type IN (2,3)                                
                AND dlrq.t_kind IN (0, 1)                                                                                   
                AND dlrq.t_factdate BETWEEN g_dt_begin AND g_dt_end
            WHERE d.t_SfContrID is not NULL AND d.t_SfContrID > 0                                                             
              AND dl_tick.t_bofficekind = 101                                               
              AND dl_tick.t_ClientID = d.t_PartyID                           
              AND dl_tick.t_ClientContrID = d.t_SfContrID                           
              AND Opr.t_Kind_Operation = dl_tick.t_DealType                         
              AND Opr.IsRepo != 0                   
             group by d.t_SfContrID) r  ON (r.t_SfContrID = w475.t_sfcontrid )                                                    
             WHEN MATCHED THEN                                                 
               UPDATE SET W475.T_turnsumrepo = R.REST; 
               
   --10  t_sum_svop        
      MERGE INTO W475_TMP W475 
        using 
       (select /*+use_hash(dvd,dvnfi) */ 
              dvn.t_clientcontr t_id
             , nvl(sum(dvnfi.t_cost),0) t_sum_svop                                  
        from ddvndeal_dbt dvn
           , ddvnfi_dbt dvnfi                    
        where  dvn.t_kind = 32715 
         and dvn.t_type in (3,5,6,7) 
         and dvn.t_id = dvnfi.t_dealid 
         and dvnfi.t_type = 0               
         and dvn.t_client !=1 and dvn.t_state in(1,2)                                                                       
         and dvnfi.t_execdate between g_dt_begin AND g_dt_end                                                                          
         group by dvn.t_clientcontr
         ) R                                                                              
          ON (R.t_id = W475.T_sfcontrid)                                                               
            WHEN MATCHED 
              THEN                                                                                                    
                UPDATE SET W475.t_turnsumsvop = R.t_sum_svop ;
                
   -- 11  t_ds_in_out;  Входящие остатки; 
   
   MERGE INTO W475_TMP W475      using 
    (
        SELECT  t_id, 
                case  
                    when (t_servkind <> 1/*Фондовый дилинг */  or  t_servkindsub <> 8/*Биржевой рынок */                                               
                      or (select count(*) from ddlcontrmp_dbt dlcontm where dlcontm.t_sfcontrid = q.t_id and dlcontm.t_marketid = 151337) > 0)           
                       and   RSB_SECUR.GetGeneralMainObjAttr(659,LPAD (t_id, 10, '0'),102, to_date('31122999','ddmmyyyy'))  = 1                        
                     then 0                                                                                                                               
                    else NVL (ABS (SUM (RSB_FIInstr.ConvSum (rest_in,t_code_currency,0,g_dt_begin - 1 ,1))),0)                                                      
              end rest_in,                                                                                                                                
              case                                                                                                                                              
                    when (t_servkind <> 1/*Фондовый дилинг */  or  t_servkindsub <> 8/*Биржевой рынок */                                               
                      or (select count(*) from ddlcontrmp_dbt dlcontm where dlcontm.t_sfcontrid = q.t_id and dlcontm.t_marketid = 151337) > 0)           
                       and   RSB_SECUR.GetGeneralMainObjAttr(659,LPAD (t_id, 10, '0'),102, to_date('31122999','ddmmyyyy'))  = 1                        
                      then 0                                                                                                                              
                    else NVL (ABS (SUM (RSB_FIInstr.ConvSum ( rest_out,t_code_currency,0,g_dt_end,1))),0)                                                     
              end rest_out                                                                                                                                 
        from (select /*+ leading(w sf ss s a) cardinality(w 100) */  sf.t_id ,a.t_account, a.t_chapter, a.t_code_currency, sf.t_servkind, sf.t_servkindsub 
             ,RSB_ACCOUNT.RESTALL(a.t_account, a.t_chapter, a.t_code_currency,g_dt_begin- 1) rest_in 
             ,RSB_ACCOUNT.RESTALL(a.t_account, a.t_chapter, a.t_code_currency, g_dt_end ) rest_out 
            FROM dsettacc_dbt s,                 
              dsfssi_dbt ss,                     
              dsfcontr_dbt sf,                   
              daccount_dbt a,                    
               W475_TMP w                        
            WHERE s.t_settaccid = ss.t_setaccid  
              AND ss.t_objecttype = 659          
              AND ss.t_objectid = sf.t_id        
              AND a.t_account = s.t_account      
              AND a.t_chapter = s.t_chapter      
              AND a.t_chapter = 1                
              AND sf.t_id = w.t_sfcontrid) q 
        where rest_in != 0 or  rest_out != 0                       
        group by t_id,t_servkind,t_servkindsub
             
             ) R   
          ON (R.t_id = W475.T_sfcontrid)                                          
          WHEN MATCHED THEN        
            UPDATE SET W475.T_DS_IN  = R.REST_IN
                      ,W475.T_DS_OUT = R.REST_OUT;
                      
  p_out_result:= 0; 
  p_out_err_msg:= '';                        
  
                      
 exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);   
  end;
  
  
  
  procedure load_bufferTable_5(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
  is
  begin   
    
  --12.1  t_ds_out -  Исходящие остатки 
    
    MERGE INTO W475_TMP W475   
   using (SELECT  /* cardinality(w 100)*/ 
                sf.t_id,
                case 
                  when (sf.t_servkind <> 1/*Фондовый дилинг */  or  sf.t_servkindsub <> 8/*Биржевой рынок */
                       or (select count(*) 
                              from ddlcontrmp_dbt dlcontm
                             where dlcontm.t_sfcontrid = sf.t_id
                             and dlcontm.t_marketid = 151337) > 0) 
                    and   RSB_SECUR.GetGeneralMainObjAttr(659,LPAD (sf.t_id, 10, '0'),102, to_date('31122999','ddmmyyyy')) = 1
                     then 0
                  else NVL (ABS (SUM (RSB_FIInstr.ConvSum (RSB_ACCOUNT.RESTALL (a.t_account,a.t_chapter,a.t_code_currency,g_dt_end),a.t_code_currency,0,g_dt_end,1))),0) 
                end rest
            FROM dsettacc_dbt s,                   
                 dsfssi_dbt ss,                    
                 dsfcontr_dbt sf,                  
                 daccount_dbt a,                    
                 W475_TMP w                         
           WHERE     s.t_settaccid = ss.t_setaccid  
                 AND ss.t_objecttype = 659          
                 AND ss.t_objectid = sf.t_id        
                 AND a.t_account = s.t_account      
                 AND a.t_chapter = s.t_chapter      
                 AND a.t_chapter = 1                
                 AND sf.t_id = w.t_sfcontrid         
                 group by sf.t_id,t_servkind,t_servkindsub) R                
  ON (R.t_id = W475.T_sfcontrid)                     
  WHEN MATCHED THEN                                  
  UPDATE SET W475.T_DS_OUT = R.REST;   
     
                                                        
 p_out_result:= 0; 
 p_out_err_msg:= ''; 
          
 exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);   
 end;
 
 procedure load_bufferTable_6(p_out_err_msg out        varchar2
                             ,p_out_result  out        number) 
  is

  begin 
    
  --12.2  t_ds_out  T_DS_IN -  Исходящие остатки

   MERGE INTO W475_TMP W475  using 
   (select sfcontr.t_id t_id
          ,case when  RSB_SECUR.GetGeneralMainObjAttr(659,LPAD (sfcontr.t_id, 10, '0'),102, to_date('31122999','ddmmyyyy')) = 1
                  then 0
                else 
                  nvl(sum(rsb_account.restall(acc.t_account, acc.t_chapter, acc.t_code_currency, g_dt_begin - 1, 0)),0) 
            end REST_IN
          ,case when  RSB_SECUR.GetGeneralMainObjAttr(659,LPAD (sfcontr.t_id, 10, '0'),102, to_date('31122999','ddmmyyyy')) = 1
                  then 0
                else 
                  nvl(sum(rsb_account.restall(acc.t_account, acc.t_chapter, acc.t_code_currency, g_dt_end , 0)),0) 
            end REST_OUT                                    
    from dsfssi_dbt ssi  
      join dsettacc_dbt sa on sa.t_SettAccID = ssi.t_SetAccID                            
      join daccount_dbt acc on acc.t_Account = sa.t_Account 
                                     and  acc.t_Chapter = sa.t_Chapter 
                                     and  acc.t_Code_Currency = sa.t_FIID  
      join dsfcontr_dbt sfcontr on sfcontr.t_ServKind in (15/*15 Срочные контракты (ФИССИКО)*/) 
            and   (sfcontr.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr.t_DateClose >= g_dt_begin)                             
      join dsfcontr_dbt sfcontr2 on sfcontr2.t_ServKind = 0 
             and  (sfcontr2.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr2.t_DateClose >= g_dt_begin)                          
       join ddlcontr_dbt dlcontr on sfcontr2.t_id = dlcontr.t_sfcontrid 
            AND dlcontr.t_iis <> 'X'   
      where ssi.t_ObjectID = to_char(sfcontr.t_id,'FM0000000000')                                                                                      
       and SSI.T_OBJECTTYPE = 659                   
       AND sfcontr.t_id in (select t_sfcontrid from W475_TMP)                                                            
       and  sfcontr.t_partyid = sfcontr2.t_partyid                                                             
   group by sfcontr.t_id) R  ON (R.t_id = W475.T_sfcontrid)            
    WHEN MATCHED THEN                                                                                                
      UPDATE SET W475.T_DS_IN  = R.REST_IN
               , W475.T_DS_OUT = R.REST_OUT; 
               
               
 p_out_result:= 0; 
 p_out_err_msg:= ''; 
          
 exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);   
 end;
 
 
 procedure load_bufferTable_7(p_out_err_msg out        varchar2
                             ,p_out_result  out        number) 
  is

  begin 
    
 --14  t_p_plus, t_p_minus, - Зачисления бумаг
    MERGE INTO W475_TMP w475                                                                                                                 
     using      
     (SELECT dl_tick.t_clientcontrid
           , NVL (SUM (ROUND (decode(dlrq.t_kind,0,rsb_secur.SC_ConvSumTypeRep (dlrq.t_factamount,dlrq.t_fiid,0,0,12,dlrq.t_factdate),0), 2)),0) rest_plus
           , NVL (SUM (ROUND (decode(dlrq.t_kind,1,rsb_secur.SC_ConvSumTypeRep (dlrq.t_factamount,dlrq.t_fiid,0,0,12,dlrq.t_factdate),0), 2)),0) rest_minus              
         FROM ddl_tick_dbt dl_tick                                                                                  
         JOIN ddlrq_dbt dlrq  ON  dlrq.t_dockind = dl_tick.t_bofficekind   
             AND dlrq.t_docid = dl_tick.t_dealid                                                                                      
             AND dlrq.t_dealpart = 1                                                                                                       
             AND dlrq.t_subkind = 1                                               
             AND dlrq.t_type = 8                                                                                                           
             AND dlrq.t_kind in (0,1)                                                                                             
          WHERE dl_tick.t_bofficekind = 127                                                              
            AND dl_tick.t_clientcontrid in (select t_sfcontrid from W475_TMP)                                                             
            AND dlrq.t_factdate BETWEEN g_dt_begin  AND g_dt_end
            group by dl_tick.t_clientcontrid ) r  
          ON (r.t_clientcontrid = w475.t_sfcontrid and t_sfcontridfm = 0)             
        when matched then             
          UPDATE SET t_p_plus = r.rest_plus
                    ,t_p_minus = r.rest_minus;
               
               
 p_out_result:= 0; 
 p_out_err_msg:= ''; 
          
 exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);   
 end;
 
  procedure load_bufferTable_8(p_out_err_msg out        varchar2
                             ,p_out_result  out        number) 
  is

  begin 
    
    /*15  t_p_minus */         
    MERGE INTO W475_TMP w475                                                                                                                 
     using      
     (SELECT dl_tick.t_clientcontrid
           , NVL (SUM (ROUND (decode(dlrq.t_kind,0,rsb_secur.SC_ConvSumTypeRep (dlrq.t_factamount,dlrq.t_fiid,0,0,12,dlrq.t_factdate),0), 2)),0) rest_plus
           , NVL (SUM (ROUND (decode(dlrq.t_kind,1,rsb_secur.SC_ConvSumTypeRep (dlrq.t_factamount,dlrq.t_fiid,0,0,12,dlrq.t_factdate),0), 2)),0) rest_minus              
         FROM ddl_tick_dbt dl_tick                                                                                  
         JOIN ddlrq_dbt dlrq  ON  dlrq.t_dockind = dl_tick.t_bofficekind   
             AND dlrq.t_docid = dl_tick.t_dealid                                                                                      
             AND dlrq.t_dealpart = 1                                                                                                       
             AND dlrq.t_subkind = 1                                               
             AND dlrq.t_type = 8                                                                                                           
             AND dlrq.t_kind in (0,1)                                                                                             
          WHERE dl_tick.t_bofficekind = 127                                                              
            AND dl_tick.t_clientcontrid in (select t_sfcontrid from W475_TMP)                                                             
            AND dlrq.t_factdate BETWEEN g_dt_begin AND g_dt_end
            group by dl_tick.t_clientcontrid ) r  
          ON (r.t_clientcontrid = w475.t_sfcontrid and t_sfcontridfm = 0)             
        when matched then             
          UPDATE SET t_p_plus = r.rest_plus
                    ,t_p_minus = r.rest_minus;             
               
 p_out_result:= 0; 
 p_out_err_msg:= ''; 
          
 exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);   
 end;
 
 procedure load_bufferTable_9(p_out_err_msg out        varchar2
                             ,p_out_result  out        number) 
  is

  begin 
    
   --16 Вложения бумаг входящий     
    MERGE INTO W475_TMP w475                                                                             
      USING 
      ( SELECT T_CONTRACT
              ,NVL(T_SUM - CASE WHEN T_SUM != 0 THEN T_NKD ELSE 0 END,0) AS T_SUM          
        FROM (SELECT /*+ leading(w PMWRTCL FIN) cardinality(w 100) */ 
                      PMWRTCL.T_CONTRACT
                     ,sum(nvl(round(t_amount* (select GetRate(PMWRTCL.t_fiid, 0,g_dt_begin - 1) from dual), 2), 0)) AS T_SUM
                     ,sum(nvl(round(t_amount* (select getNKdAmount(PMWRTCL.t_fiid,g_dt_begin - 1,0) from dual),2),0)) AS T_NKD           
             FROM DPMWRTCL_DBT PMWRTCL
                 ,DFININSTR_DBT FIN 
                 ,W475_TMP  w            
             WHERE (g_dt_begin - 1) BETWEEN PMWRTCL.T_BEGDATE 
               AND PMWRTCL.T_ENDDATE             
               AND PMWRTCL.T_FIID = FIN.T_FIID               
               AND t_amount != 0                  
               and PMWRTCL.T_CONTRACT = w.t_sfcontrid             
               and PMWRTCL.T_PARTY = w.t_partyid           
             GROUP BY PMWRTCL.T_CONTRACT                 )) PMWRTCLSUM                           
             ON (T_SFCONTRID = PMWRTCLSUM.T_CONTRACT)      
       WHEN MATCHED THEN                             
         UPDATE SET T_P_IN = PMWRTCLSUM.T_SUM;             
               
 p_out_result:= 0; 
 p_out_err_msg:= ''; 
          
 exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);   
 end;
  
 
 procedure load_bufferTable_10(p_out_err_msg out        varchar2
                             ,p_out_result  out        number) 
  is

  begin 
 
  --17 Вложения бумаг исходящий
       
      UPDATE W475_TMP w475                                                                        
       SET w475.T_P_OUT = 
              (SELECT  NVL(T_SUM - CASE WHEN T_SUM != 0 THEN T_NKD ELSE 0 END,0) AS T_SUM                    
                FROM (SELECT sum(nvl(round(t_amount*(select GetRate(PMWRTCL.t_fiid, 0, g_dt_end) from dual), 2), 0)) AS T_SUM
                            ,sum(nvl(round(t_amount* (select getNKdAmount(PMWRTCL.t_fiid,g_dt_end,0) from dual),2),0)) AS T_NKD                                 
                      FROM DPMWRTCL_DBT PMWRTCL, DFININSTR_DBT FIN                                
                     WHERE PMWRTCL.T_PARTY = w475.t_PartyID         
                       AND PMWRTCL.T_CONTRACT = w475.t_SfContrID                                  
                       AND PMWRTCL.T_BEGDATE <= g_dt_end                                        
                       AND PMWRTCL.T_ENDDATE >= g_dt_end      
                       AND PMWRTCL.T_AMOUNT != 0                                     
                       AND FIN.T_FIID = PMWRTCL.T_FIID)) ;  
                                    
  p_out_result:= 0; 
  p_out_err_msg:= ''; 
             
  exception
  when others  
    then 
      ROLLBACK; 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);  
  end;
  
  procedure load_bufferTable_11(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
  is

  begin 
    
   --18 Для срочного рынка обороты за период - t_turnsum                    
    MERGE INTO W475_TMP W475    
      using 
       (select /*+ cardinality(w 100)*/ 
              sfcontr.t_id t_id
             ,nvl(sum(t_buy),0) t_buy
             ,nvl(sum(t_sale),0) t_sale                                                
        from ddvfiturn_dbt dvfiturn                                                                                                      
        join dsfcontr_dbt sfcontr on sfcontr.t_ServKind in (15/*15 Срочные контракты (ФИССИКО)*/, 21/*Валютный дилинг*/) 
          and  (sfcontr.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr.t_DateClose >= g_dt_begin)
          and dvfiturn.t_clientcontr = sfcontr.t_id                                                                
        join dsfcontr_dbt sfcontr2 on sfcontr2.t_ServKind = 0 
          and (sfcontr2.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr.t_DateClose >= g_dt_begin)            
        join ddlcontr_dbt dlcontr on sfcontr2.t_id = dlcontr.t_sfcontrid 
          AND dlcontr.t_iis <> 'X'                                        
        join W475_TMP W on  sfcontr.t_id =  W.T_sfcontrid                                                               
       where sfcontr.t_partyid = sfcontr2.t_partyid 
        and  t_date between g_dt_begin and g_dt_end                        
       group by sfcontr.t_id) R                                                                                                 
       ON (R.t_id = W475.T_sfcontrid)                                                                                                   
       WHEN MATCHED THEN                                                          
         UPDATE SET W475.t_turnsum = R.t_buy + R.t_sale; 
  
   p_out_result:= 0; 
   p_out_err_msg:= ''; 
  
  exception
  when others  
    then 
      ROLLBACK; 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);  
  end;
  
   procedure load_bufferTable_12(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
  is

  begin 
  
   --19  t_turnsum 2 - Для валютного рынка обороты за период
     
        MERGE INTO W475_TMP w475                                                    
            USING 
            ( SELECT dvn.t_clientcontr     t_clientcontrid
                     ,NVL (SUM (RSI_RSB_FIInstr.ConvSum (dvnfi.t_cost,       
                                                         dvnfi.t_pricefiid,  
                                                         0,                  
                                                         dvn.t_date,         
                                                         0)),0) REST                                           
             FROM ddvndeal_dbt dvn, ddvnfi_dbt dvnfi                     
              WHERE  dvn.t_date BETWEEN g_dt_begin AND g_dt_end                              
               AND dvn.t_dvkind IN (3, 6, 7)                          
               AND dvn.t_state IN (1, 2)                              
               AND dvnfi.t_dealid = dvn.t_id                          
          GROUP BY dvn.t_clientcontr) r                                   
      ON (r.t_clientcontrid = w475.t_sfcontrid)                           
      WHEN MATCHED                                                                
        THEN                                                                        
          UPDATE SET W475.t_turnsum = R.REST;    
   p_out_result:= 0; 
  p_out_err_msg:= ''; 
  
  exception
  when others  
    then 
      ROLLBACK; 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);  
  end;
  
  
   procedure load_bufferTable_13(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
  is

  begin 
  
   /*
     20  t_comsum - Сумма комиссий
    Комиссионное вознаграждение банка по сделкам  {Field21} Результаты БО для ДРРК  
    Сумма комиссий биржевых сделок + комиссии сделок по свопам - комиссии по сделкам Репо
    */
    
    MERGE INTO W475_TMP w475  USING 
    ( select /*+ cardinality(d 100)*/ 
            DLC.T_CONTRACT t_id 
           ,NVL (SUM (RSI_RSB_FIInstr.ConvSum (dlc.t_sum,cm.t_fiid_comm,0,dlc.t_factpaydate,1)),0) t_comsum                
     FROM W475_TMP d 
      join ddlcomis_dbt dlc on  DLC.T_CONTRACT = d.t_sfcontrid                                                           
      JOIN dsfcomiss_dbt cm ON cm.t_FeeType = dlc.t_Feetype 
       AND cm.t_Number = dlc.t_ComNumber AND cm.t_ReceiverID = 1             
     WHERE  dlc.t_DocKind in (101, 4813)                                                         
      AND dlc.t_factpaydate between g_dt_begin AND g_dt_end
      group by DLC.T_CONTRACT ) r          
        ON (r.t_id = w475.t_sfcontrid)                           
        WHEN MATCHED                                                               
           THEN 
             UPDATE SET w475.t_comsum = R.t_comsum;
   p_out_result:= 0; 
  p_out_err_msg:= ''; 
  
  exception
  when others  
    then 
      ROLLBACK; 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);  
  end;
  
  
   procedure load_bufferTable_14(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
  is

  begin 
  
    --21 Для срочного рынка - комиссия брокера           
    MERGE INTO W475_TMP W475                                                                                                            
    using 
     (select sfcontr.t_id t_id, nvl(sum(t_sum),0) t_sumbr               
         from ddvdeal_dbt dvdeal                                                                                                            
          join dsfcontr_dbt sfcontr on sfcontr.t_ServKind in (15/*15 Срочные контракты (ФИССИКО)*/, 21/*Валютный дилинг*/) 
            and (sfcontr.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr.t_DateClose >= g_dt_begin) 
            and  dvdeal.t_clientcontr = sfcontr.t_id                                                                  
          join doprkoper_dbt oprkoper on oprkoper.t_kind_operation = dvdeal.t_kind 
            and  regexp_like(oprkoper.t_systypes, '[U|O]') 
            and  regexp_like(oprkoper.t_systypes, '[B|S]')                                                           
          left join doproper_dbt oproper on oproper.t_kind_operation = dvdeal.t_kind 
             and oproper.t_documentid = lpad(dvdeal.t_id, 34, '0')   
          join dsfcontr_dbt sfcontr2 on sfcontr2.t_ServKind = 0 
            and (sfcontr2.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr2.t_DateClose >= g_dt_begin)             
          join ddlcontr_dbt dlcontr on sfcontr2.t_id = dlcontr.t_sfcontrid AND dlcontr.t_iis <> 'X'    
          join ddvdlcom_dbt dvdlcom on dvdlcom.t_dealid = dvdeal.t_id                                                                        
          join dsfcomiss_dbt sfcomiss on sfcomiss.t_comissid = dvdlcom.t_comissid                                                            
          join ddp_dep_dbt dp_dep on dp_dep.t_partyid = sfcomiss.t_receiverid                                 
       where dvdeal.t_state > 0 
         and dvdeal.t_date <= g_dt_end
         and  (oproper.t_end_date is null or oproper.t_end_date >= g_dt_begin) 
         and  sfcontr.t_partyid = sfcontr2.t_partyid                                                                                        
         group by sfcontr.t_id) R                     
           ON (R.t_id = W475.T_sfcontrid)         
             WHEN MATCHED 
               THEN                                                                                                                  
                UPDATE SET W475.t_comsum = R.t_sumbr;  
                
  p_out_result:= 0; 
  p_out_err_msg:= ''; 
  
  exception
  when others  
    then 
      ROLLBACK; 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);  
  end;
  
  
  procedure load_bufferTable_15(p_out_err_msg out        varchar2
                               ,p_out_result  out        number) 
  is

  begin 
                
     /* 22  t_comsumrepo Комиссии Репо*/
                
    MERGE INTO W475_TMP W475    using 
    ( SELECT /*+ cardinality(d 100)*/ 
       DLC.T_CONTRACT t_id 
      ,NVL (SUM (RSI_RSB_FIInstr.ConvSum (dlc.t_sum, cm.t_fiid_comm,0,dlc.t_factpaydate,1)),0)  t_comsumrepo            
      FROM W475_TMP d
         , ddl_tick_dbt dl_tick
         , ddlcomis_dbt dlc
         JOIN dsfcomiss_dbt cm ON cm.t_FeeType = dlc.t_Feetype 
           AND cm.t_Number  = dlc.t_ComNumber AND cm.t_ReceiverID = g_ourbank           
      WHERE dl_tick.t_BOfficeKind = 101                                                                                        
        AND dl_tick.t_ClientID > 0                    
        AND dlc.t_DocKind = dl_tick.t_bofficekind                    
        AND dlc.t_docid = dl_tick.t_dealid                    
        AND DLC.T_CONTRACT = d.t_sfcontrid                                                                                                   
        AND dlc.t_factpaydate  BETWEEN g_dt_begin AND g_dt_end                                                    
        AND rsb_secur.IsRepo (rsb_secur.get_operationgroup (rsb_secur.get_opersystypes (dl_tick.t_dealtype,dl_tick.t_bofficekind))) != 0           
      group by DLC.T_CONTRACT) r   
     ON (R.t_id = W475.T_sfcontrid)                                                                                                   
       WHEN MATCHED 
         THEN                                                
          UPDATE SET W475.t_comsumrepo = R.t_comsumrepo;
          
  p_out_result:= 0; 
  p_out_err_msg:= ''; 
  
  exception
  when others  
    then 
      ROLLBACK; 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);  
  end;
  
  procedure load_bufferTable_16(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
  is

  begin 
     
  -- 23 Комиссии СВОП
       MERGE INTO W475_TMP W475                                                                                                         
     using (select dvn.t_clientcontr t_id
                 , sum(dlc.t_sum) t_svop_sum                                                                 
            from ddlcomis_dbt dlc
              , dsfcomiss_dbt sfc
              , ddvndeal_dbt dvn                                                            
           where dlc.t_docid = dvn.t_id 
             and dlc.t_dockind = dvn.t_dockind 
             and dlc.t_feetype = sfc.t_feetype                          
             and dlc.t_comnumber = sfc.t_number 
             and dvn.t_date between g_dt_begin AND g_dt_end
             and dvn.t_dvkind in (3, 6, 7)                       
             and dvn.t_state in (1, 2) 
             and upper(sfc.t_code) like 'МСКБ_ВБ_СП%'                                                   
            group by dvn.t_clientcontr)R                                                                            
     ON (R.t_id = W475.T_sfcontrid)                                                                                                  
          WHEN MATCHED THEN                                                                                                               
               UPDATE SET W475.T_COMSUMSVOP = R.t_svop_sum;
    
    
   
              
   
  
  p_out_result:= 0; 
  p_out_err_msg:= '';
  
      
  exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);   
  end;
  
  
  procedure load_bufferTable_17(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
  is

  begin 
  
   --24  t_special_repo -  Специальные Репо;
         
    MERGE INTO W475_TMP  W475      
     USING 
     ( 
     SELECT /* cardinality(d 100)*/ d.t_SfContrID,  
           NVL (SUM (RSI_RSB_FIInstr.ConvSum (dlrq.t_factamount,  
                                              dlrq.t_fiid,  
                                              0,  
                                              dlrq.t_factdate,  
                                              1  
                                             )),0) sumperc  
      FROM W475_TMP d, 
           (select t_Kind_Operation,  
                   rsb_secur.IsRepo(rsb_secur.get_operationgroup (rsb_secur.get_opersystypes(t_Kind_Operation, t_DocKind))) as IsRepo
              from doprkoper_dbt 
             where t_DocKind = 101 
           ) Opr,  
           ddl_tick_dbt dl_tick  
           JOIN  
              ddlrq_dbt dlrq  
           ON     dlrq.t_dockind = dl_tick.t_bofficekind  
              AND dlrq.t_docid = dl_tick.t_dealid  
              AND dlrq.t_subkind = 0  
              AND dlrq.t_type = 3  
              AND dlrq.t_kind = 1  
              AND dlrq.t_factdate BETWEEN g_dt_begin AND g_dt_end  
     WHERE d.t_SfContrID is not NULL AND d.t_SfContrID > 0  
           AND dl_tick.t_bofficekind = 101  
           AND dl_tick.t_ClientID = d.t_PartyID 
           AND dl_tick.t_ClientContrID = d.t_SfContrID 
           AND Opr.t_Kind_Operation = dl_tick.t_DealType 
           AND Opr.IsRepo != 0  
           AND EXISTS  
                  (SELECT 1  
                     FROM dobjatcor_dbt atcor  
                    WHERE     atcor.t_objecttype = 101  
                          AND atcor.t_groupid = 103  
                          AND atcor.t_object = LPAD (dl_tick.t_dealid, 34, 0)  
                          AND atcor.t_attrid =  
                                 (SELECT objattr.t_attrid  
                                    FROM DOBJATTR_DBT objattr  
                                   WHERE atcor.t_objecttype =  
                                            objattr.t_objecttype  
                                         AND atcor.t_groupid =  
                                                objattr.t_groupid  
                                         AND LOWER (objattr.t_name) =  
                                                'да'))  
  GROUP BY d.t_SfContrID
     ) r         
     ON (r.t_SfContrID = W475.t_sfcontridsm             
        AND 1 = CASE                       
                  WHEN W475.T_SFCONTRIDSM = 0 
                    THEN 0                       
                  WHEN r.t_SfContrID = W475.T_SFCONTRIDSM 
                    THEN 1                       
                  ELSE 0            
                 END) 
        WHEN MATCHED 
          THEN    
            UPDATE SET W475.t_special_repo = r.sumperc; 
            
   
  p_out_result:= 0; 
  p_out_err_msg:= '';
  
      
  exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);   
  end;
  
  
  procedure load_bufferTable_18(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
  is

  begin 
  
    
    --25 t_sum_svop  P_W475_CurrencyMarket  
    MERGE INTO W475_TMP W475  
       using (SELECT /* cardinality(w 100)*/ 
                   a.t_id t_id1
                   ,(sum(b.cost)-sum(a.cost)) T_sum_val 
              FROM W475_TMP w 
               join (select dvn.t_clientcontr t_id,dvnfi.t_cost cost, dvn.t_id deal  
                         from ddvnfi_dbt dvnfi, ddvndeal_dbt dvn                                                                                   
                    where dvn.t_kind = 32715 
                     and dvn.t_type = 6 
                     and dvn.t_id = dvnfi.t_dealid 
                     and dvnfi.t_type= 0 
                     and dvnfi.t_execdate between g_dt_begin AND g_dt_end
                     and dvn.t_date >= g_dt_begin  and dvn.t_state = 2   
                    union     
                    select dvn.t_clientcontr t_id
                          ,dvnfi.t_cost cost
                          ,dvn.t_id deal   
                      from ddvnfi_dbt dvnfi
                        , ddvndeal_dbt dvn   
                      where dvn.t_kind = 32715 
                        and dvn.t_type = 5 
                        and dvn.t_id = dvnfi.t_dealid 
                        and dvnfi.t_type= 2 
                        and dvnfi.t_execdate between g_dt_begin  AND g_dt_end
                        and dvn.t_date >= g_dt_begin
                        and dvn.t_state = 2                
                   ) a  on w.T_sfcontrid = a.t_id  
              INNER JOIN (select dvn.t_clientcontr t_id,dvnfi.t_cost cost, dvn.t_id deal   
                            from ddvnfi_dbt dvnfi, ddvndeal_dbt dvn  
                            where dvn.t_kind = 32715 and dvn.t_type = 6 and dvn.t_id = dvnfi.t_dealid and dvnfi.t_type= 2 and dvn.t_state = 2   
                          union   
                          select dvn.t_clientcontr t_id,dvnfi.t_cost cost, dvn.t_id deal   
                            from ddvnfi_dbt dvnfi, ddvndeal_dbt dvn   
                            where dvn.t_kind = 32715 and dvn.t_type = 5 and dvn.t_id = dvnfi.t_dealid and dvnfi.t_type= 0 and dvn.t_state = 2       
                          ) b  
              ON a.deal = b.deal  
              group by a.t_id   )R                      
              ON (R.t_id1 = W475.T_sfcontrid)   
                   WHEN MATCHED THEN                 
                         UPDATE SET W475.T_VAL_SVOP = R.t_sum_val;  
                         
    
  p_out_result:= 0; 
  p_out_err_msg:= '';
  
      
  exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);   
  end;
  
  
  
  procedure load_bufferTable_19(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
  is

  begin 
  
     /* 
        26  t_clientname
        t_clientname VARCHAR2(320) ФИО Клиента {Field05} Результаты БО для ДРРК  
        Поле t_name таблицы dparty_dbt  
        */
        
        UPDATE W475_TMP w      
         SET T_CLIENTNAME = (SELECT T_NAME                                                       
                              FROM DSFCONTR_DBT SF                                             
                            WHERE SF.t_id = w.t_Sfcontrid)             
         WHERE EXISTS  (SELECT 1                                                            
                          FROM DSFCONTR_DBT SF
                              ,DPARTYOWN_DBT PO                           
                        WHERE SF.t_id = w.t_Sfcontrid                                   
                         AND po.t_PartyID = sf.t_partyid 
                         AND po.t_partykind = 65);  
  p_out_result:= 0; 
  p_out_err_msg:= '';
  
      
  exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);   
  end;
  
  
  
  procedure load_bufferTable_20(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
  is

  begin 
  
   /* 27 t_comsumBank Комиссия уплаченная банком по сделкам клиента не репо 
           в т.ч. расход Банка по комиссионному вознаграждению биржи по сделкам (кроме сделок РЕПО) 
     */ 
        UPDATE W475_TMP d                                                                                                                      
      SET d.t_comsumBank = 
          ((SELECT NVL (SUM (RSI_RSB_FIInstr.ConvSum (dlc.t_sum,cm.t_fiid_comm,0,dlc.t_factpaydate,1)),0)                        
              FROM ddl_tick_dbt tick
                  ,ddlcomis_dbt dlc
                  ,dsfcomiss_dbt cm              
           WHERE tick.t_BOfficeKind = 101                
            AND tick.t_ClientID = d.t_PartyID                
            AND tick.T_clientcontrid = d.t_sfcontrid                                                                                 
            AND dlc.t_DocKind = tick.t_bofficekind 
            AND dlc.t_docid = tick.t_dealid 
            AND dlc.T_ISBANKEXPENSES = 'X'                                                                             
            AND cm.t_FeeType = dlc.t_Feetype 
            AND cm.t_Number = dlc.t_ComNumber 
            AND cm.t_ReceiverID != g_ourbank                                          
            AND rsb_secur.IsRepo (rsb_secur.get_operationgroup (rsb_secur.get_opersystypes (tick.t_dealtype,tick.t_bofficekind))) = 0                 
            AND dlc.t_factpaydate between g_dt_begin AND g_dt_end ) + (SELECT NVL (SUM (RSI_RSB_FIInstr.ConvSum (dlc.t_sum,cm.t_fiid_comm,0,dlc.t_factpaydate,1)),0)                                        
          
          FROM ddlcomis_dbt dlc
             , ddvndeal_dbt tick
             , dsfcomiss_dbt cm                                                                                                            
          WHERE dlc.t_DocKind = 4813 
           AND dlc.T_ISBANKEXPENSES = 'X'                                                                           
           AND cm.t_FeeType = dlc.t_Feetype 
           AND cm.t_Number = dlc.t_ComNumber 
           AND cm.t_ReceiverID != g_ourbank                                       
           AND tick.T_client = d.t_partyid                                                                                                    
           AND tick.T_clientcontr = d.t_sfcontrid                                                                                              
           and tick.t_dockind =   dlc.t_DocKind 
           AND tick.t_id = dlc.t_docid                            
           AND dlc.t_factpaydate between g_dt_begin AND g_dt_end )) ;  
    
  p_out_result:= 0; 
  p_out_err_msg:= '';
  
      
  exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);   
  end;
   
  procedure load_bufferTable_21(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
  is

  begin 
  
   --28  t_comsumBank-2
      MERGE INTO W475_TMP W475                                                                                                            
      using 
      (select sfcontr.t_id t_id, nvl(sum(t_sum),0) t_sumbr              
        from ddvdeal_dbt dvdeal                                                                                                        
        join dsfcontr_dbt sfcontr on sfcontr.t_ServKind in (15/*15 Срочные контракты (ФИССИКО)*/, 21/*Валютный дилинг*/) 
           and  (sfcontr.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr.t_DateClose >= g_dt_begin) 
           and dvdeal.t_clientcontr = sfcontr.t_id                                                                   
        join doprkoper_dbt oprkoper on oprkoper.t_kind_operation = dvdeal.t_kind 
           and  regexp_like(oprkoper.t_systypes, '[U|O]')
           and  regexp_like(oprkoper.t_systypes, '[B|S]')                                                           
        left join doproper_dbt oproper on oproper.t_kind_operation = dvdeal.t_kind 
           and oproper.t_documentid = lpad(dvdeal.t_id, 34,'0')                                                
        join dsfcontr_dbt sfcontr2 on sfcontr2.t_ServKind = 0 
          and (sfcontr2.t_DateClose = to_date('01.01.0001','dd.mm.yyyy') or sfcontr2.t_DateClose >= g_dt_begin)             
        join ddlcontr_dbt dlcontr on sfcontr2.t_id = dlcontr.t_sfcontrid 
          AND dlcontr.t_iis <>  'X'                                          
        join ddvdlcom_dbt dvdlcom on dvdlcom.t_dealid = dvdeal.t_id 
          AND dvdlcom.T_ISBANKEXPENSES = 'X'                                     
        join dsfcomiss_dbt sfcomiss on sfcomiss.t_comissid = dvdlcom.t_comissid 
          and sfcomiss.t_ReceiverID != g_ourbank                             
        join ddp_dep_dbt dp_dep on dp_dep.t_partyid = sfcomiss.t_receiverid                          
      where dvdeal.t_state > 0 
        and dvdeal.t_date <= g_dt_end
        and  (oproper.t_end_date is null or oproper.t_end_date >= g_dt_begin) 
        and sfcontr.t_partyid = sfcontr2.t_partyid                                                                                        
       group by sfcontr.t_id) R              
       ON (R.t_id = W475.T_sfcontrid)                                                                                      
         WHEN MATCHED 
           THEN  
           UPDATE SET W475.t_comsumBank = W475.t_comsumBank + R.t_sumbr; 
            
  p_out_result:= 0; 
  p_out_err_msg:= '';
      
  exception
  when others  
    then 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);   
  end;
  
  procedure load_bufferTable_22(p_out_err_msg out        varchar2
                              ,p_out_result  out        number) 
  
  is 

  begin
      
     /* 29
        t_comsumbankrepo  NUMBER  в т.ч. расход Банка по комиссионному вознаграждению биржи по сделкам РЕПО   {Field22_1} 
      */
      
     UPDATE W475_TMP d      
     SET d.t_comsumBankRepo =  (SELECT 
                                   NVL (SUM (RSI_RSB_FIInstr.ConvSum (dlc.t_sum,cm.t_fiid_comm,0,dlc.t_factpaydate,1)),0)                 
                                FROM ddl_tick_dbt tick
                                    ,ddlcomis_dbt dlc
                                    ,dsfcomiss_dbt cm                
                                WHERE tick.t_BOfficeKind = 101                
                                  AND tick.t_ClientID = d.t_PartyID                
                                  AND tick.T_clientcontrid = d.t_sfcontrid                
                                  AND rsb_secur.IsRepo (rsb_secur.get_operationgroup (rsb_secur.get_opersystypes (tick.t_dealtype,tick.t_bofficekind))) != 0   
                                  AND dlc.t_DocKind = tick.t_bofficekind 
                                  AND dlc.t_docid = tick.t_dealid                 
                                  AND dlc.T_ISBANKEXPENSES = 'X'                                                                     
                                  AND cm.t_FeeType = dlc.t_Feetype 
                                  AND cm.t_Number = dlc.t_ComNumber 
                                  AND cm.t_ReceiverID != 1                    
                                  AND dlc.t_factpaydate between g_dt_begin AND g_dt_end );
                                                            
  p_out_result:= 0; 
  p_out_err_msg:= '';
   
   
  exception
  when others  
    then 
      p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);  
  end;
  
  procedure load_bufferTable_23(p_out_err_msg out        varchar2
                               ,p_out_result  out        number) 
  is

  begin                               
     insert /*+ append*/ into dkl11sofr2dwh_dbt (t_recid,
                                 t_contrnumber,
                                 t_contrdatebegin,
                                 t_planname,
                                 t_clientname,
                                 t_borndate,
                                 t_clientcode,
                                 t_ds_out,
                                 t_p_out,
                                 t_clienttype,
                                 t_depname,
                                 t_depcode,
                                 t_finname,
                                 t_marketname,
                                 t_ds_plus,
                                 t_p_plus,
                                 t_ds_minus,
                                 t_p_minus,
                                 t_netto,
                                 t_assetsbegin,
                                 t_assetsend,
                                 t_restperc,
                                 t_turnsum,
                                 t_turnsumrepo,
                                 t_comsum,
                                 t_comsumbank,
                                 t_profit,
                                 t_comsumrepo,
                                 t_comsumbankrepo,
                                 t_profitrepo,
                                 t_specreposwap,
                                 t_totalprofit,
                                 t_enddatereport,
                                 t_uploadtime,
                                 t_partyid,
                                 t_contrsofrid)
                                 
        select row_number() over (order by 1)
              ,t_contrnumber
              ,t_contrdatebegin
              ,t_planname
              ,t_clientname
              ,born_date
              ,t_clientcode
              ,t_ds_out
              ,t_p_out
              ,(SELECT case 
                          when po.t_partykind = 65 then 'УК'
                          when p.t_legalform = 1 or pp.t_isemployer = chr(88)  then 'ЮЛ'
                          when p.t_legalform = 2 then 'ФЛ'
                        end
                  FROM DPARTY_DBT p left join dpersn_dbt pp on p.t_partyid = pp.t_personid  
                                    left join dpartyown_dbt po on po.t_partyid = p.t_partyid and po.t_partykind = 65
                 where  p.t_partyid = w.t_partyid) --type client
              ,t_depname
              ,t_depcode
              ,t_finname
              ,t_marketname
              ,t_ds_plus
              ,t_p_plus
              ,t_ds_minus
              ,t_p_minus
              ,nvl(t_ds_plus + t_p_plus - t_ds_minus - t_p_minus,0)
              ,nvl(t_ds_in + t_p_in,0)
              ,nvl(t_ds_out + t_p_out,0)
              ,null
              ,nvl(t_turnsum - t_turnsumrepo - t_turnsumsvop,0)
              ,nvl(t_turnsumrepo,0)
              ,nvl(t_comsum + t_comsumsvop - t_comsumrepo,0)  
              ,nvl(t_comsumBank,0)
              ,nvl(t_comsum + t_comsumsvop - t_comsumrepo - t_comsumBank,0)
              ,nvl(t_comsumrepo,0)
              ,nvl(t_comsumBankRepo,0)
              ,nvl(t_comsumrepo - t_comsumBankRepo,0)
              ,nvl(t_special_repo + t_val_svop,0)
              ,nvl(t_comsum - t_comsumBank + t_special_repo + t_val_svop + t_comsumsvop - t_comsumBankRepo,0)
              ,LAST_DAY( g_dt_end )
              ,systimestamp 
              ,t_partyid
              ,t_sfcontrid           
       FROM  W475_TMP w;
       
      COMMIT; 
       
     update dkl11sofr2dwh_dbt
     set t_restperc = case 
                       when t_assetsend + t_ds_plus + t_p_plus > 0 
                         then 
                               ((t_assetsend - t_assetsbegin - t_ds_plus -t_p_plus + t_ds_minus + t_p_minus)/
                               (t_assetsend + t_ds_plus + t_p_plus)) * 100
                         else 0     
                      end;
              
       p_out_result:= 0; 
       p_out_err_msg:= null;
       
       COMMIT;    
  
 RSB_W475SOFR2CHD.log_register_event(p_status => p_out_result
                                    ,p_message => p_out_err_msg);
      
      
  exception
  when others  
    then 
      ROLLBACK; 
       p_out_result:= 1; 
       p_out_err_msg:= get_text_error(SQLCODE,SQLERRM);    
       log_register_event(p_status => p_out_result
                         ,p_message => p_out_err_msg);
                            
  end;
  
  function get_text_error(p_sql_code      number
                         ,p_error_message varchar2) return varchar2
                         
  is 
   v_text_message varchar2(4000);
  begin
    
    v_text_message:= 'Error code '||p_sql_code||'. '|| substr(p_error_message,0, 300);   
    return v_text_message;
    
  end;                        
                         
                         
  procedure log_register_event(p_status number
                              ,p_message varchar2 default null) --0 - Успешно, 1 - Ошибка
    
  is
     v_ERRORTEXT varchar2(1000);
     v_ID itt_q_message_log.msgid%type;
  begin
  
  if(p_status = 0) then
  
    IT_EVENT.RegisterEvent(NULL
                         ,'SOFR'
                         , 'w475sofr2chd'
                         ,'Процедура w475sofr2chd.mac: успешное заполнение буферной таблицы dkl11sofr2dwh_dbt результатами БО для ДРРК'
                         ,'<XML LevelInfo = "1"/>'
                         ,v_ERRORTEXT
                         ,v_ID);
                           
  elsif(p_status = 1) then
  
    IT_EVENT.RegisterEvent(NULL
                         ,'SOFR'
                         , 'w475sofr2chd'
                         , 'Процедура w475sofr2chd.mac: ошибка заполнения буферной таблицы dkl11sofr2dwh_dbt результатами БО для ДРРК. ' || chr(10) 
                            || p_message
                         ,'<XML LevelInfo = "8"/>'
                         ,v_ERRORTEXT
                         ,v_ID);  
   end if;
   
   
  end;
  
end RSB_W475SOFR2CHD;
/