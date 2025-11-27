----------------------------------------------------------------------------
-- Date:       27 April 2006                                              -- 
-- Object:     RSCOMSecurDeal "Операция с ценными бумагами"               --
-- Programmer: Ivanov Alexandr                                            --
-- Note:                                                                  --
----------------------------------------------------------------------------
/*
CREATE OR REPLACE VIEW v_sp_securdealhistory
AS
(select 
      Ticket.T_DEALID                  T_DEALID,
      Ticket.T_INSTANCE                T_INSTANCE,
      Ticket.T_CHANGEDATE              T_CHANGEDATE,
      Ticket.T_CHANGEKIND              T_CHANGEKIND,
      Ticket.T_FIXSUM                  T_FIXSUM,
      Ticket.T_PARTYID                 T_PARTYID,
      Ticket.T_PREOUTLAY               T_PREOUTLAY,
      Ticket.T_DEALTIME                T_DEALTIME,
      DECODE( Part.T_LEGKIND, 0, 1, 2) T_LEGKIND,
      Part.T_PRINCIPAL                 T_PRINCIPAL,
      Part.T_BASIS                     T_BASIS,
      Part.T_CFI                       T_CFI,
      Part.T_PRICE                     T_PRICE,
      Part.T_COST                      T_COST,
      Part.T_TOTALCOST                 T_TOTALCOST,
      Part.T_NKD                       T_NKD,
      Part.T_FORMULA                   T_FORMULA,
      (select T_PAYFIID 
         from DPMPAYM_DBT 
        where T_DOCUMENTID = Ticket.T_DEALID and 
              T_DOCKIND = Ticket.T_BOFFICEKIND and 
              T_PURPOSE = DECODE( Part.T_LEGKIND, 0, 2, 4 ) ) T_PAYFIID,
      NVL( (select T_AMOUNT 
              from DPMPAYM_DBT 
             where T_DOCUMENTID = Ticket.T_DEALID and 
                   T_DOCKIND = Ticket.T_BOFFICEKIND and 
                   T_PURPOSE = DECODE( Part.T_LEGKIND, 0, 42, 43 ) ), 0) T_ADVANCE,
      Part.T_START               T_START,
      Part.T_MATURITY            T_MATURITY,
      Part.T_EXPIRY              T_EXPIRY,
      Part.T_MATURITYISPRINCIPAL T_MATURITYISPRINCIPAL,
      Part.T_REGISTRAR           T_REGISTRAR,
      Part.T_PAYREGTAX           T_PAYREGTAX
   from ddl_tick_dbt Ticket, ddl_leg_dbt Part
  where Part.T_DEALID = Ticket.T_DEALID and 
        Part.T_LEGKIND != 1 and
        Ticket.T_BOFFICEKIND in (101, 105, 117, 127, 138)
)
UNION
(select 
      OldDeal.T_DEALID,
      OldDeal.T_OLDINSTANCE,
      OldDeal.T_OLDCHANGEDATE,
      OldDeal.T_OLDCHANGEKIND,
      OldDeal.T_OLDFIXSUM,
      OldDeal.T_OLDPARTYID,
      OldDeal.T_OLDPREOUTLAY,
      OldDeal.T_OLDDEALTIME,
      1,              
      OldDeal.T_OLDPRINCIPAL,
      OldDeal.T_OLDBASIS,
      OldDeal.T_OLDCFI1,
      OldDeal.T_OLDPRICE1,
      OldDeal.T_OLDCOST1,
      OldDeal.T_OLDTOTALCOST1,
      OldDeal.T_OLDNKD1,
      OldDeal.T_OLDFORMULA1,
      OldDeal.T_OLDPAYFIID1,
      OldDeal.T_OLDADVANCE1,
      OldDeal.T_OLDSTART1,
      OldDeal.T_OLDMATURITY1,
      OldDeal.T_OLDEXPIRY1,
      OldDeal.T_OLDMATURITYISPRINCIPAL1,
      OldDeal.T_OLDREGISTRAR1,
      OldDeal.T_OLDPAYREGTAX1
   from dsptkchng_dbt OldDeal
  where (OldDeal.T_OLDINSTANCE = 0) or 
        ((OldDeal.T_OLDINSTANCE != 0) and (OldDeal.T_OLDCHANGEDATE > TO_DATE('01.01.0001', 'DD.MM.YYYY')))
)
UNION
(select 
      OldDeal.T_DEALID,
      OldDeal.T_OLDINSTANCE,
      OldDeal.T_OLDCHANGEDATE,
      OldDeal.T_OLDCHANGEKIND,
      OldDeal.T_OLDFIXSUM,
      OldDeal.T_OLDPARTYID,
      OldDeal.T_OLDPREOUTLAY,
      OldDeal.T_OLDDEALTIME,
      2,              
      OldDeal.T_OLDPRINCIPAL,
      OldDeal.T_OLDBASIS,
      OldDeal.T_OLDCFI2,
      OldDeal.T_OLDPRICE2,
      OldDeal.T_OLDCOST2,
      OldDeal.T_OLDTOTALCOST2,
      OldDeal.T_OLDNKD2,
      OldDeal.T_OLDFORMULA2,
      OldDeal.T_OLDPAYFIID2,
      OldDeal.T_OLDADVANCE2,
      OldDeal.T_OLDSTART2,
      OldDeal.T_OLDMATURITY2,
      OldDeal.T_OLDEXPIRY2,
      OldDeal.T_OLDMATURITYISPRINCIPAL2,
      OldDeal.T_OLDREGISTRAR2,
      OldDeal.T_OLDPAYREGTAX2
   from dsptkchng_dbt OldDeal
  where (OldDeal.T_OLDINSTANCE = 0) or 
        ((OldDeal.T_OLDINSTANCE != 0) and (OldDeal.T_OLDCHANGEDATE > TO_DATE('01.01.0001', 'DD.MM.YYYY')))
);

CREATE OR REPLACE VIEW v_sp_securdealdates
AS
(SELECT
        Tick.T_DEALID  T_DealID,
        (case when Tick.T_CLIENTID = -1 then Rsb_Secur.get_OperStepDate(Tick.T_DEALID, Tick.T_BOFFICEKIND, Tick.T_DEALTYPE, NULL, 120, 1) 
                                        else NULL end)                                                      T_BALANCEDATE1,
        (case when Tick.T_CLIENTID = -1 then Rsb_Secur.get_OperStepDate(Tick.T_DEALID, Tick.T_BOFFICEKIND, Tick.T_DEALTYPE, NULL, 120, 2) 
                                        else NULL end)                                                      T_BALANCEDATE2,
        Rsb_Secur.get_OperStepDate(Tick.T_DEALID, Tick.T_BOFFICEKIND, Tick.T_DEALTYPE, ':', 0, 0)           T_REJECTDEALDATE,
        Rsb_Secur.get_OperStepDate(Tick.T_DEALID, Tick.T_BOFFICEKIND, Tick.T_DEALTYPE, ';', 0, 0)           T_REJECTPARTDATE,
        Rsb_Secur.get_OperStepDate(Tick.T_DEALID, Tick.T_BOFFICEKIND, Tick.T_DEALTYPE, '>', 0, 0)           T_DELAYDATE1,
        Rsb_Secur.get_OperStepDate(Tick.T_DEALID, Tick.T_BOFFICEKIND, Tick.T_DEALTYPE, '8', 0, 0)           T_DELAYAVANCEDATE1,
        Rsb_Secur.get_OperStepDate(Tick.T_DEALID, Tick.T_BOFFICEKIND, Tick.T_DEALTYPE, '&', 0, 0)           T_DELAYPAYDATE1,
        Rsb_Secur.get_OperStepDate(Tick.T_DEALID, Tick.T_BOFFICEKIND, Tick.T_DEALTYPE, '?', 0, 0)           T_DELAYDATE2,
        Rsb_Secur.get_OperStepDate(Tick.T_DEALID, Tick.T_BOFFICEKIND, Tick.T_DEALTYPE, '9', 0, 0)           T_DELAYAVANCEDATE2,
        Rsb_Secur.get_OperStepDate(Tick.T_DEALID, Tick.T_BOFFICEKIND, Tick.T_DEALTYPE, '*', 0, 0)           T_DELAYPAYDATE2
--        TO_DATE(rsb_struct.getString(Rsb_Account.GetRsVoxPrmVal('BeginDate')), 'DD MM YYYY HH24:MI:SS') T_BEGINDATE,
--        TO_DATE(rsb_struct.getString(Rsb_Account.GetRsVoxPrmVal('EndDate')), 'DD MM YYYY HH24:MI:SS')   T_ENDDATE
   FROM DDL_TICK_DBT Tick
  WHERE Tick.T_BOFFICEKIND in (101, 105, 117, 127, 138)
);

CREATE OR REPLACE VIEW v_sp_securdealconst
AS
(select
      Ticket.T_DEALID                                                               T_DEALID,
      Ticket.T_BOFFICEKIND                                                          T_DOCKIND,
      Ticket.T_DEALTYPE                                                             T_OPERKIND,
      (Rsb_Secur.get_OperationType( Ticket.T_BOFFICEKIND, Ticket.T_DEALTYPE, (case when (Dates.T_ENDDATE >= Dates.T_REJECTPARTDATE) or (Dates.T_ENDDATE is NULL) then Dates.T_REJECTPARTDATE end)) ) T_DEALKIND,
      Ticket.T_DEALCODE                                                             T_DEALCODE,
      Ticket.T_DEALDATE                                                             T_DEALDATE,
      (case when (Dates.T_ENDDATE >= Ticket.T_CLOSEDATE) or 
                 (Dates.T_ENDDATE is NULL)then Ticket.T_CLOSEDATE end)              T_CLOSEDATE,
      Ticket.T_DEPARTMENT                                                           T_DEPARTMENT,
      Ticket.T_FLAG1                                                                T_FLAG1,
      Ticket.T_MARKETID                                                             T_MARKETID,
      (case when (Part1.T_INCOMERATE > 0) and 
                 ((Dates.T_ENDDATE < Dates.T_REJECTPARTDATE) and
                 (Dates.T_ENDDATE is not NULL)) then Part1.T_INCOMERATE end)        T_INCOMERATE,
      (case when Ticket.T_CLIENTID > 0 then Ticket.T_CLIENTID else Rsb_Kernel.GetSelfID() end) T_CLIENTID,
      Part1.T_PFI                                                                   T_FIID,
      (101)                                                                         T_OBJECTTYPE, ---а это случайно не тоже самое что и T_DocKind 
      (TO_CHAR( Ticket.T_DEALID, 'FM0999999999999999999999999999999999' ))          T_OBJECTID,
      (case when Part2.T_ID is not NULL then 176 else 101 end)                      T_DOCTYPE,
      (case when Part2.T_ID is not NULL then Part1.T_ID else Ticket.T_DEALID end)   T_DOCID1,
      (case when (Dates.T_ENDDATE >= Dates.T_REJECTDEALDATE) or
                 (Dates.T_ENDDATE is NULL) then Dates.T_REJECTDEALDATE end)         T_REJECTDEALDATE,
      (case when (Dates.T_ENDDATE >= Dates.T_BALANCEDATE1) or
                 (Dates.T_ENDDATE is NULL) then Dates.T_BALANCEDATE1 end)           T_BALANCEDATE1,
      (case when (Dates.T_ENDDATE >= Dates.T_DELAYDATE1) or 
                 (Dates.T_ENDDATE is NULL)then Dates.T_DELAYDATE1 end)              T_DELAYDATE1,
      (case when (Dates.T_ENDDATE >= Dates.T_DELAYAVANCEDATE1) or
                 (Dates.T_ENDDATE is NULL) then Dates.T_DELAYAVANCEDATE1 end)       T_DELAYAVANCEDATE1,
      (case when (Dates.T_ENDDATE >= Dates.T_DELAYPAYDATE1) or 
                 (Dates.T_ENDDATE is NULL) then Dates.T_DELAYPAYDATE1 end)          T_DELAYPAYDATE1,
      (case when (Dates.T_ENDDATE < Dates.T_REJECTPARTDATE) and 
                 (Dates.T_ENDDATE is not NULL) then Part2.T_ID end)                 T_DOCID2,
      (case when (Dates.T_ENDDATE >= Dates.T_REJECTPARTDATE) or
                 (Dates.T_ENDDATE is NULL) then Dates.T_REJECTPARTDATE end)         T_REJECTPARTDATE,
      (case when ((Dates.T_ENDDATE >= Dates.T_BALANCEDATE2) or 
                 (Dates.T_ENDDATE is NULL)) then Dates.T_BALANCEDATE2 end)          T_BALANCEDATE2,
      (case when (Dates.T_ENDDATE >= Dates.T_DELAYDATE2) or 
                 (Dates.T_ENDDATE is NULL) then Dates.T_DELAYDATE2 end)             T_DELAYDATE2,
      (case when (Dates.T_ENDDATE >= Dates.T_DELAYAVANCEDATE2) or 
                 (Dates.T_ENDDATE is NULL) then Dates.T_DELAYAVANCEDATE2 end)       T_DELAYAVANCEDATE2,
      (case when (Dates.T_ENDDATE >= Dates.T_DELAYPAYDATE2) or 
                 (Dates.T_ENDDATE is NULL) then Dates.T_DELAYPAYDATE2 end)          T_DELAYPAYDATE2
   from ddl_tick_dbt Ticket, ddl_leg_dbt Part1, ddl_leg_dbt Part2, v_sp_securdealdates Dates
  where Part1.T_DEALID = Ticket.T_DEALID and Part1.T_LEGKIND = 0 and
        Part2.T_DEALID(+) = Ticket.T_DEALID and Part2.T_LEGKIND(+) = 2 and
        Dates.T_DEALID = Ticket.T_DEALID and 
        Ticket.T_BOFFICEKIND in (101, 105, 117, 127, 138)
);

CREATE OR REPLACE VIEW v_sp_securdeal
AS
(select 
        Ticket.T_DEALID,
        Ticket.T_DOCKIND,
        Ticket.T_OPERKIND,
        Ticket.T_DEALKIND,
        Ticket.T_DEALCODE,
        Ticket.T_DEALDATE,
        History1.T_DEALTIME,  
        Ticket.T_CLOSEDATE,
        Ticket.T_DEPARTMENT,
        Ticket.T_FLAG1,   
        Ticket.T_MARKETID,
        Ticket.T_CLIENTID,
        History1.T_PARTYID,   
        Ticket.T_FIID,
        History1.T_FIXSUM,    
        History1.T_PREOUTLAY, 
        Ticket.T_INCOMERATE,
        History1.T_PRINCIPAL, 
        History1.T_BASIS,     
        Ticket.T_OBJECTTYPE,
        Ticket.T_OBJECTID,
        Ticket.T_DOCTYPE,
        Ticket.T_DOCID1,
        History1.T_CFI                       T_CFI1,      
        History1.T_PRICE                     T_PRICE1,    
        History1.T_COST                      T_COST1,     
        History1.T_TOTALCOST                 T_TOTALCOST1,
        History1.T_NKD                       T_NKD1,      
        History1.T_FORMULA                   T_FORMULA1,  
        History1.T_PAYFIID                   T_PAYFIID1,
        History1.T_ADVANCE                   T_ADVANCE1,
        (case when (History1.T_ADVANCE > 0 ) then History1.T_START end)            T_PLANAVANCEDATE1,
        (case when Ticket.T_DOCKIND != 117 then 
                  (case when History1.T_MATURITYISPRINCIPAL='X' then History1.T_MATURITY
                                                                else History1.T_EXPIRY end ) end) T_PLANDATE1,
        (case when History1.T_MATURITYISPRINCIPAL!='X' then History1.T_MATURITY
                                                      else History1.T_EXPIRY end ) T_PLANPAYDATE1,
        History1.T_REGISTRAR                 T_REGISTRAR1,          
        History1.T_PAYREGTAX                 T_PAYREGTAX1,
        Ticket.T_REJECTDEALDATE,
        Ticket.T_BALANCEDATE1,
        (case when Ticket.T_BALANCEDATE1 is not NULL then Rsb_Secur.get_StepCarrySum(Ticket.T_DEALID, Ticket.T_DOCKIND, Ticket.T_OPERKIND, History1.T_PAYFIID, NULL, 120, 1) else NULL end) T_BALANCECOST1,
        Ticket.T_DELAYDATE1,
        Ticket.T_DELAYAVANCEDATE1,
        Ticket.T_DELAYPAYDATE1,
        Ticket.T_DOCID2,
        History2.T_CFI                       T_CFI2,      
        History2.T_PRICE                     T_PRICE2,    
        History2.T_COST                      T_COST2,     
        History2.T_TOTALCOST                 T_TOTALCOST2,
        History2.T_NKD                       T_NKD2,      
        History2.T_FORMULA                   T_FORMULA2,  
        History2.T_PAYFIID                   T_PAYFIID2,
        History2.T_ADVANCE                   T_ADVANCE2,
        (case when (History2.T_ADVANCE > 0 ) then History2.T_START end)            T_PLANAVANCEDATE2,
        (case when Ticket.T_DOCKIND != 117 then 
                  (case when History2.T_MATURITYISPRINCIPAL='X' then History2.T_MATURITY
                                                                else History2.T_EXPIRY end ) end) T_PLANDATE2,
        (case when History2.T_MATURITYISPRINCIPAL!='X' then History2.T_MATURITY
                                                      else History2.T_EXPIRY end ) T_PLANPAYDATE2,
        History2.T_REGISTRAR                 T_REGISTRAR2,          
        History2.T_PAYREGTAX                 T_PAYREGTAX2,
        Ticket.T_REJECTPARTDATE,          
        Ticket.T_BALANCEDATE2,
        (case when Ticket.T_BALANCEDATE2 is not NULL then Rsb_Secur.get_StepCarrySum(Ticket.T_DEALID, Ticket.T_DOCKIND, Ticket.T_OPERKIND, History2.T_PAYFIID, NULL, 120, 2) else NULL end) T_BALANCECOST2,
        Ticket.T_DELAYDATE2,
        Ticket.T_DELAYAVANCEDATE2,
        Ticket.T_DELAYPAYDATE2,
        ---------------------------------------------------------------
        -- Счета по категориям 
        --   FIROLE_BA            = 3,  //Базовый актив сделки
        --   FIROLE_CA            = 4,  //Контрактив сделки
        (case when (Rsb_Secur.get_DealPartBuyType(Ticket.T_OPERKIND,Ticket.T_DOCKIND, 0)=1) then 3 else 4 end ) AS T_DemandAccountRole1,
        (case when (ABS(History1.T_MATURITY-History1.T_EXPIRY) < 3) then 161 else 163 end) AS T_DEMANDGACCOUNTCATEGORY1,
        (case when (Rsb_Secur.get_DealPartBuyType(Ticket.T_OPERKIND,Ticket.T_DOCKIND, 0)=1) then 4 else 3 end ) AS T_LiabilityAccountRole1,
        (case when (ABS(History1.T_MATURITY-History1.T_EXPIRY) < 3) then 162 else 164 end) AS T_LIABILITYGACCOUNTCATEGORY1,
       -------------------------------
        (case when (Rsb_Secur.get_DealPartBuyType(Ticket.T_OPERKIND,Ticket.T_DOCKIND, 0)=1) then 198 else 200 end ) AS T_DELAYSUPPLYACCOUNTCATEGORY1,
        (case when (Rsb_Secur.get_DealPartBuyType(Ticket.T_OPERKIND,Ticket.T_DOCKIND, 0)=1) then 198 else 200 end ) AS T_AVANCEDELAYACCOUNTCATEGORY1,
        (case when (Rsb_Secur.get_DealPartBuyType(Ticket.T_OPERKIND,Ticket.T_DOCKIND, 0)=1) then 198 else 200 end ) AS T_PAYDELAYACCOUNTCATEGORY1,
       -- Счета по категориям 
        (case when ((Rsb_Secur.IsRepo( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1) OR 
                  (Rsb_Secur.IsBackSale( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1)) then (case when (Rsb_Secur.get_DealPartBuyType(Ticket.T_OPERKIND,Ticket.T_DOCKIND, 1)=1) then 3 else 4 end ) else NULL end) AS  T_DEMANDACCOUNTROLE2,
        (case when ((Rsb_Secur.IsRepo( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1) OR 
                  (Rsb_Secur.IsBackSale( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1)) then (case when (ABS(History2.T_MATURITY-History2.T_EXPIRY) < 3) then 161 else 163 end) else NULL end) AS T_DEMANDGACCOUNTCATEGORY2,
        (case when ((Rsb_Secur.IsRepo( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1) OR 
                  (Rsb_Secur.IsBackSale( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1)) then (case when (Rsb_Secur.get_DealPartBuyType(Ticket.T_OPERKIND,Ticket.T_DOCKIND, 1)=1) then 4 else 3 end ) else NULL end) AS  T_LIABILITYACCOUNTROLE2,
        (case when ((Rsb_Secur.IsRepo( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1) OR 
                  (Rsb_Secur.IsBackSale( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1)) then (case when (ABS(History2.T_MATURITY-History2.T_EXPIRY) < 3) then 162 else 164 end) else NULL end) AS T_LIABILITYGACCOUNTCATEGORY2,
      -----------------------------
        (case when ((Rsb_Secur.IsRepo( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1) OR 
                  (Rsb_Secur.IsBackSale( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1)) then (case when (Rsb_Secur.get_DealPartBuyType(Ticket.T_OPERKIND,Ticket.T_DOCKIND, 0)=1) then 198 else 200 end ) else NULL end) AS  T_DELAYSUPPLYACCOUNTCATEGORY2,
        (case when ((Rsb_Secur.IsRepo( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1) OR 
                  (Rsb_Secur.IsBackSale( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1)) then (case when (Rsb_Secur.get_DealPartBuyType(Ticket.T_OPERKIND,Ticket.T_DOCKIND, 0)=1) then 198 else 200 end ) else NULL end) AS  T_AVANCEDELAYACCOUNTCATEGORY2,
        (case when ((Rsb_Secur.IsRepo( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1) OR 
                  (Rsb_Secur.IsBackSale( Rsb_Secur.get_OperationGroup(Rsb_Secur.get_OperSysTypes(Ticket.T_OPERKIND,Ticket.T_DOCKIND) ) ) = 1)) then (case when (Rsb_Secur.get_DealPartBuyType(Ticket.T_OPERKIND,Ticket.T_DOCKIND, 0)=1) then 198 else 200 end ) else NULL end) AS  T_PAYDELAYACCOUNTCATEGORY2,
      -----------------------------
        (case when Ticket.T_CLIENTID != -1 and Ticket.T_DELAYDATE1 is not NULL  then History1.T_TOTALCOST 
                                                                                else Rsb_Secur.get_StepCarrySum(Ticket.T_DEALID, Ticket.T_DOCKIND, Ticket.T_OPERKIND, 0, '>', 0, 0) end) AS T_DelaySupplyCost1,
        (case when Ticket.T_CLIENTID != -1 and Ticket.T_DELAYAVANCEDATE1 is not NULL then History1.T_TOTALCOST 
                                                                                     else Rsb_Secur.get_StepCarrySum(Ticket.T_DEALID, Ticket.T_DOCKIND, Ticket.T_OPERKIND, 0, '8', 0, 0) end) AS T_AvanceDelayCost1,
        (case when Ticket.T_CLIENTID != -1 and Ticket.T_DELAYPAYDATE1 is not NULL then History1.T_TOTALCOST 
                                                                                  else Rsb_Secur.get_StepCarrySum(Ticket.T_DEALID, Ticket.T_DOCKIND, Ticket.T_OPERKIND, 0, '&', 0, 0) end) AS T_PayDelayCost1,
        (case when Ticket.T_CLIENTID != -1 and Ticket.T_DELAYDATE2 is not NULL then History2.T_TOTALCOST 
                                                                               else Rsb_Secur.get_StepCarrySum(Ticket.T_DEALID, Ticket.T_DOCKIND, Ticket.T_OPERKIND, 0, '?', 0, 0) end) AS T_DelaySupplyCost2,
        (case when Ticket.T_CLIENTID != -1 and Ticket.T_DELAYAVANCEDATE2 is not NULL then History2.T_TOTALCOST 
                                                                                     else Rsb_Secur.get_StepCarrySum(Ticket.T_DEALID, Ticket.T_DOCKIND, Ticket.T_OPERKIND, 0, '9', 0, 0) end) AS T_AvanceDelayCost2,
        (case when Ticket.T_CLIENTID != -1 and Ticket.T_DELAYPAYDATE2 is not NULL then History2.T_TOTALCOST 
                                                                                  else Rsb_Secur.get_StepCarrySum(Ticket.T_DEALID, Ticket.T_DOCKIND, Ticket.T_OPERKIND, 0, '*', 0, 0) end) AS T_PayDelayCost2
   from v_sp_securdealconst Ticket, v_sp_securdealdates Dates, v_sp_securdealhistory History1
        left join v_sp_securdealhistory History2 on History2.T_DEALID = History1.T_DEALID and
                                                    History2.T_LEGKIND = 2 and
                                                    History2.T_INSTANCE = History1.T_INSTANCE and
                                                    (select T_REJECTPARTDATE from v_sp_securdealdates where T_DEALID = History1.T_DEALID ) is NULL
  where Dates.T_DEALID = Ticket.T_DEALID and
        History1.T_DEALID = Ticket.T_DEALID and
        History1.T_INSTANCE = (select MAX(T_INSTANCE) 
                                from v_sp_securdealhistory 
                               where ((T_CHANGEDATE < Dates.T_ENDDATE) or (Dates.T_ENDDATE is NULL) )and (T_DEALID = Ticket.T_DEALID)) and
        History1.T_LEGKIND = 1
);
*/
DECLARE
  E_TABLE_NOT_EXISTS EXCEPTION;
  e_no_exist_seq     EXCEPTION;
  PRAGMA EXCEPTION_INIT( E_TABLE_NOT_EXISTS, -942);
  PRAGMA EXCEPTION_INIT( e_no_exist_seq,     -2289 );
BEGIN
   BEGIN
     EXECUTE IMMEDIATE 'DROP TABLE DV_SECURDEAL_TMP CASCADE CONSTRAINTS';
   EXCEPTION 
     WHEN E_TABLE_NOT_EXISTS THEN NULL;
   END;

   BEGIN
      EXECUTE IMMEDIATE 'DROP SEQUENCE DV_SECURDEAL_TMP_SEQ';
   EXCEPTION
      WHEN e_no_exist_seq   THEN NULL;
   END;

   COMMIT;
END;
/

CREATE TABLE DV_SECURDEAL_TMP
(
  T_ID                  NUMBER(10),
  T_DEALID              NUMBER(10),
  T_DOCKIND             NUMBER(5),
  T_OPERKIND            NUMBER(5),
  T_DEALKIND            VARCHAR2(10),
  T_DEALCODE            VARCHAR2(30),
  T_DEALDATE            DATE,
  T_DEALTIME            DATE,
  T_REJECTPARTDATE      DATE,
  T_CLOSEDATE           DATE,
  T_DEPARTMENT          NUMBER(10),
  T_FLAG1               CHAR(1),
  T_MARKETID            NUMBER(10),
  T_CLIENTID            NUMBER(10),
  T_PARTYID             NUMBER(10),
  T_FIID                NUMBER(10),
  T_FIXSUM              NUMBER(5),
  T_PREOUTLAY           NUMBER(32,12),
  T_INCOMERATE          NUMBER(32,12),
  T_PRINCIPAL           NUMBER(32,12),
  T_BASIS               NUMBER(5),
  T_OBJECTTYPE          NUMBER(5),
  T_OBJECTID            VARCHAR2(35),
  T_DOCTYPE             NUMBER(5),
  T_DOCID1              NUMBER(10),
-------------------------------------
  T_CFI1                NUMBER(10),
  T_PRICE1              NUMBER(32,12),
  T_ADVANCE1            NUMBER(32,12),
  T_COST1               NUMBER(32,12),
  T_TOTALCOST1          NUMBER(32,12),
  T_NKD1                NUMBER(32,12),
  T_PAYFIID1            NUMBER(10),
  T_FORMULA1            NUMBER(5),
  T_PLANAVANCEDATE1     DATE,
  T_PLANDATE1           DATE,
  T_PLANPAYDATE1        DATE,
  T_REGISTRAR1          NUMBER(10),
  T_PAYREGTAX1          CHAR(1),
  T_BALANCEDATE1        DATE,
  T_BALANCECOST1        NUMBER(32,12),
  T_DELAYDATE1          DATE,
  T_REMDELAYDATE1       DATE,
  T_DELAYSUPPLYCOST1    NUMBER(32,12),
  T_DELAYAVANCEDATE1    DATE,
  T_REMDELAYAVANCEDATE1 DATE,
  T_DELAYAVANCECOST1    NUMBER(32,12),
  T_DELAYPAYDATE1       DATE,
  T_REMDELAYPAYDATE1    DATE,
  T_DELAYPAYCOST1       NUMBER(32,12),
  T_STATE1              NUMBER(10),
------------------------------------
  T_DOCID2              NUMBER(10),
  T_CFI2                NUMBER(10),
  T_PRICE2              NUMBER(32,12),
  T_ADVANCE2            NUMBER(32,12),
  T_COST2               NUMBER(32,12),
  T_TOTALCOST2          NUMBER(32,12),
  T_NKD2                NUMBER(32,12),
  T_PAYFIID2            NUMBER(10),
  T_FORMULA2            NUMBER(5),
  T_PLANAVANCEDATE2     DATE,
  T_PLANDATE2           DATE,
  T_PLANPAYDATE2        DATE,
  T_REGISTRAR2          NUMBER(10),
  T_PAYREGTAX2          CHAR(1),
  T_BALANCEDATE2        DATE,
  T_BALANCECOST2        NUMBER(32,12),
  T_DELAYDATE2          DATE,
  T_REMDELAYDATE2       DATE,
  T_DELAYSUPPLYCOST2    NUMBER(32,12),
  T_DELAYAVANCEDATE2    DATE,
  T_REMDELAYAVANCEDATE2 DATE,
  T_DELAYAVANCECOST2    NUMBER(32,12),
  T_DELAYPAYDATE2       DATE,
  T_REMDELAYPAYDATE2    DATE,
  T_DELAYPAYCOST2       NUMBER(32,12),
  T_STATE2              NUMBER(10),
------------------------------------
  T_GUARANTEE               NUMBER(32,12),
  T_RISKGROUPCATEGORYNUM    NUMBER(5),
  T_QUALITYGROUPCATEGORYNUM NUMBER(5),
  T_RISKPERCENT             NUMBER(32,12),
  T_DEMANDRISKPERCENT       NUMBER(32,12),
------------------------------------
-- категории
------------------------------------
  T_DEMANDGACCOUNTCATEGORY1     NUMBER(5),
  T_LIABILITYGACCOUNTCATEGORY1  NUMBER(5),
  T_DELAYSUPPLYACCOUNTCATEGORY1 NUMBER(5),
  T_DELAYAVANCEACCOUNTCATEGORY1 NUMBER(5),
  T_DELAYPAYACCOUNTCATEGORY1    NUMBER(5),
  T_DEMANDGACCOUNTCATEGORY2     NUMBER(5),
  T_LIABILITYGACCOUNTCATEGORY2  NUMBER(5),
  T_DELAYSUPPLYACCOUNTCATEGORY2 NUMBER(5),
  T_DELAYAVANCEACCOUNTCATEGORY2 NUMBER(5),
  T_DELAYPAYACCOUNTCATEGORY2    NUMBER(5)
)
/
--ON COMMIT PRESERVE ROWS;


--
-- DDVTAXREG_TMP_IDX0  (Index) 
--
--  Dependencies: 
--   DDVTAXREG_TMP (Table)
--
CREATE UNIQUE INDEX DV_SECURDEAL_TMP_IDX0 ON DV_SECURDEAL_TMP (T_ID)
/


CREATE SEQUENCE DV_SECURDEAL_TMP_SEQ NOCACHE
/

--
-- DDVTAXREG_TMP_T0_AINC  (Trigger) 
--
--  Dependencies: 
--   DDVTAXREG_TMP (Table)
--
CREATE OR REPLACE TRIGGER DV_SECURDEAL_TMP_T0_AINC
 BEFORE INSERT OR UPDATE OF T_ID ON DV_SECURDEAL_TMP FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.t_ID = 0 OR :new.t_ID IS NULL) THEN
 SELECT DV_SECURDEAL_TMP_seq.nextval INTO :new.t_ID FROM dual;
 ELSE
 select last_number into v_id from user_sequences where sequence_name = upper('DV_SECURDEAL_TMP_SEQ');
 IF :new.t_ID >= v_id THEN
 RAISE DUP_VAL_ON_INDEX;
 END IF;
 END IF;
END;
/
