CREATE OR REPLACE PACKAGE BODY user_NPTXFunc
AS

 /**

# changelog
 |date       |autor          |tasks                                           |note
 |-----------|---------------|------------------------------------------------|-------------------------------------------------------------
 |24.09.2025 |Велигжанин А.В.|DEF-102805                                      |Доработка INS_NPTXOP(). Инициализация nptxop.T_PAYPURPOSE

  */

FUNCTION GenerateOpCodebySyq(pEKK in varchar2, pYear in varchar2, pDBO in varchar2) RETURN varchar2 
IS
 pCode varchar2(30) := '';
BEGIN
  if pEKK is not null then
    pCode := pCode || pEKK|| '/';
  end if;
  
  pCode := pCode || u_NPTX_SEQ.Nextval;
  pCode := pCode ||'/' || pYear;
  
  if pDBO is not null then
     pCode := pCode || '/' || pDBO;
    end if;
    
  return pCode;  

END GenerateOpCodebySyq;


FUNCTION SetNPTXSequence(pCount in number) RETURN NUMBER 
IS
  dropseq_sql varchar2(200);
  createseq_sql varchar2(200);
BEGIN
   dropseq_sql := 'drop SEQUENCE u_NPTX_SEQ';
   createseq_sql := 'CREATE SEQUENCE u_NPTX_SEQ START WITH '||pCount||
                              ' MINVALUE 1 NOCYCLE NOCACHE NOORDER'; 
   execute immediate dropseq_sql;
   execute immediate createseq_sql; 
   return 0;                                                      
   exception when others then
   return 1;
END SetNPTXSequence;

FUNCTION GetLastClientOrderCount (pDate in date) RETURN NUMBER 
IS
  LastOrderCount_str varchar2(30);
  LastOrderCount number;
  LastOrderNum varchar2(30);
  pos_1 number;
  pos_2 number;
  
  function IS_NUMBER(str in varchar2) return varchar2 
    IS
      dummy number;
    begin
        dummy := TO_NUMBER(str);
        return ('TRUE');
    Exception WHEN OTHERS then
        return ('FALSE');
    end;
  
BEGIN
  LastOrderCount := 0;

  LastOrderNum := getLastClientOrderNum(pDate);

  if length(LastOrderNum) = 0 then
    return 0;
    end if;

  select instr(LastOrderNum, '/',1,1 ) into pos_1 from dual;
  select instr(LastOrderNum, '/',1,2 ) into pos_2 from dual;

  if pos_1 > 0 then
    select substr(LastOrderNum,pos_1 + 1, abs((pos_1 + 1) - pos_2) ) into LastOrderCount_str from dual;
    if IS_NUMBER(LastOrderCount_str)= 'TRUE' then
       LastOrderCount :=  TO_NUMBER(LastOrderCount_str);     
      end if;
    end if;
 
  return LastOrderCount;
  exception when no_data_found then
    return LastOrderCount;
END GetLastClientOrderCount;


   function getLastClientOrderNum(pDate in date) return varchar2
     as
     cClentOrder ClientOrder_typ;
     begin 
       select   t.t_id,
         t.t_code,
         t.t_operdate,
         t.t_spgroundid,
         t.t_kind,
         t.t_xld,
         t.t_altxld,
         t.t_registrdatetime,         
         t.t_signeddate 
            into cClentOrder
 from 
(select  nptx.t_id,
         nptx.t_code,
         nptx.t_operdate,
         spground.t_spgroundid,
         spground.t_kind,
         spground.t_xld,
         spground.t_registrdate,
         spground.t_registrtime,
         spground.t_altxld,
         spground.t_signeddate,
         spground.t_signedtime,
         spground.t_registrdate + (spground.t_registrtime - to_date('01010001','ddmmyyyy')) as t_registrdatetime,
         ROW_NUMBER() OVER (ORDER BY (spground.t_registrdate + (spground.t_registrtime - to_date('01010001','ddmmyyyy'))) desc) AS ROW_NUMBER
      
  from dnptxop_dbt nptx, dspgrdoc_dbt spgrdoc, dspground_dbt spground
 where t_dockind = 4607
   and t_kind_operation = 2037
   and t_subkind_operation = 20
   and spgrdoc.t_sourcedockind = nptx.t_dockind
   and spgrdoc.t_sourcedocid = nptx.t_id
   and spground.t_spgroundid = spgrdoc.t_spgroundid
   and spground.t_kind = 251
   and spground.t_registrdate < pDate) t
  where 1=1 
   and ROW_NUMBER = 1;
  
   return cClentOrder.t_xld;
   exception when no_data_found then
     return 'null';
     end;
   function CreateNPTXSeq return number
      as     
            seq_startval number := 1;                                                                                                               
            createseq_sql varchar2(500);                                                                                   
            begin                                                                                                          
            createseq_sql := 'CREATE SEQUENCE u_NPTX_SEQ START WITH '||seq_startval||' MAXVALUE 46656'||          
                              ' MINVALUE 1 NOCYCLE NOCACHE NOORDER';                                                       
            execute immediate createseq_sql;                                                                               
            return 0;
   END;
   
    function GetNextValNPTXSeq return number                                                             
    as                                                                                                                
    getnextval_sql varchar2(500);                                                                                     
    seqnextval number;                                                                                                
    err number;                                                                                                       
    sequence_is_missing EXCEPTION;                                                                                    
    PRAGMA EXCEPTION_INIT (sequence_is_missing, -2289);                                                               
    begin                                                                                                             
       getnextval_sql := 'select u_NPTX_SEQ.NEXTVAL from DUAL';                                               
       execute immediate getnextval_sql into seqnextval;                                                              
       return seqnextval;                                                                                             
                                                                                                                      
       exception when sequence_is_missing then                                                                        
         err := CreateNPTXSeq();                                                                   
         if err = 0 then                                                                                              
           seqnextval := GetNextValNPTXSeq();                                                                          
           return seqnextval;                                                                                         
         end if;                                                                                                      
         return 0;                                                                                                    
    end;                                                                                                              
   
/******************************************************************************************/     
 /*Вставка операции*/
   PROCEDURE AddTransfer(p_KindOP IN DNPTXOP_DBT.T_SUBKIND_OPERATION%TYPE,
                                        p_OperCode IN DNPTXOP_DBT.T_CODE%TYPE,
                                        p_OperDate IN DNPTXOP_DBT.T_OPERDATE%TYPE,
                                        p_Time IN DNPTXOP_DBT.T_TIME%TYPE,
                                        p_PartyID IN DNPTXOP_DBT.T_CLIENT%TYPE,
                                        p_ContrID IN DNPTXOP_DBT.T_CONTRACT%TYPE,
                                        p_Amount IN DNPTXOP_DBT.T_OUTSUM%TYPE,
                                        p_Account IN DNPTXOP_DBT.T_ACCOUNT%TYPE,
                                        p_FIID IN DNPTXOP_DBT.T_CURRENCY%TYPE,
                                        p_Filial IN DNPTXOP_DBT.T_DEPARTMENT%TYPE,

                                        p_FlagTax IN DNPTXOP_DBT.T_FLAGTAX%TYPE,
                                        p_TransIIS IN DNPTXOP_DBT.T_IIS%TYPE,
                                        p_IISAmount IN DNPTXOP_DBT.T_CURRENTYEAR_SUM%TYPE,
                                        p_CalcNDFL IN DNPTXOP_DBT.T_CALCNDFL%TYPE,
                                        p_Partial IN DNPTXOP_DBT.T_PARTIAL%TYPE,
                                        p_CloseContr IN DNPTXOP_DBT.T_CLOSECONTR%TYPE,

                                        p_TaxAccount IN DNPTXOP_DBT.T_ACCOUNTTAX%TYPE,
                                        p_TaxFIID IN  DNPTXOP_DBT.T_FIID%TYPE,
                                        --реквизиты для СПИ
                                        p_SPIAccount IN DDLRQACC_DBT.T_ACCOUNT%TYPE,
                                        p_BankID IN DDLRQACC_DBT.T_BANKID%TYPE,
                                        p_BankCode IN DDLRQACC_DBT.T_BANKCODE%TYPE,
                                        p_BankCodeKind IN DDLRQACC_DBT.T_BANKCODEKIND%TYPE,

                                        p_CorrAccount IN DDLRQACC_DBT.T_CORRACC%TYPE,
                                        p_CorrBankID IN DDLRQACC_DBT.T_BANKCORRID%TYPE,
                                        p_CorrBankCode IN DDLRQACC_DBT.T_BANKCORRCODE%TYPE,
                                        p_CorrBankCodeKind IN DDLRQACC_DBT.T_BANKCORRCODEKIND%TYPE,
                                        --параметры для документа основания
                                        p_SPALTXLD in DSPGROUND_DBT.T_ALTXLD%TYPE,
                                        p_SPREGISTRDATE in DSPGROUND_DBT.T_REGISTRDATE%TYPE,
                                        p_SPREGISTRTIME in DSPGROUND_DBT.T_REGISTRTIME%TYPE,
                                        p_RetOperID OUT DNPTXOP_DBT.T_ID%TYPE,
                                        p_Err OUT VARCHAR2)
AS
  UnknownValue CONSTANT   NUMBER := -1;
  ZeroValue CONSTANT   NUMBER := 0;
  UnknownDate CONSTANT    DATE := TO_DATE ('01.01.0001', 'DD.MM.YYYY');
  UnknownTime CONSTANT    DATE := TO_DATE ('01.01.0001 00:00:00', 'DD.MM.YYYY HH24:MI:SS');

  v_nptxop DNPTXOP_DBT%ROWTYPE;
  v_dlrqacc DDLRQACC_DBT%ROWTYPE;
  v_dlrq DDLRQ_DBT%ROWTYPE;
  v_spground DSPGROUND_DBT%ROWTYPE;
  v_spgrdoc  DSPGRDOC_DBT%ROWTYPE;


  PROCEDURE INS_NPTXOP (p_nptxop IN OUT DNPTXOP_DBT%ROWTYPE )
  AS
  BEGIN
    p_nptxop.T_ID                := NVL(p_nptxop.T_ID, ZeroValue);
    p_nptxop.T_DOCKIND           := NVL(p_nptxop.T_DOCKIND, 4607);
    p_nptxop.T_KIND_OPERATION    := NVL(p_nptxop.T_KIND_OPERATION, 2037);
    p_nptxop.T_SUBKIND_OPERATION := NVL(p_nptxop.T_SUBKIND_OPERATION, 0);
    p_nptxop.T_CODE              := NVL(p_nptxop.T_CODE, '0');
    p_nptxop.T_OPERDATE          := NVL(p_nptxop.T_OPERDATE, UnknownDate);
    p_nptxop.T_CLIENT            := NVL(p_nptxop.T_CLIENT, UnknownValue);
    p_nptxop.T_CONTRACT          := NVL(p_nptxop.T_CONTRACT, UnknownValue);
    p_nptxop.T_PREVDATE          := NVL(p_nptxop.T_PREVDATE, UnknownDate);
    p_nptxop.T_PLACEKIND         := NVL(p_nptxop.T_PLACEKIND, 2); /*Банк*/
    p_nptxop.T_PLACE             := NVL(p_nptxop.T_PLACE, RsbSessionData.OperDprt); /*РНКБ*/
    p_nptxop.T_TAXBASE           := NVL(p_nptxop.T_TAXBASE, ZeroValue);
    p_nptxop.T_OUTSUM            := NVL(p_nptxop.T_OUTSUM, ZeroValue);
    p_nptxop.T_OUTCOST           := NVL(p_nptxop.T_OUTCOST, ZeroValue);
    p_nptxop.T_TOUT              := NVL(p_nptxop.T_TOUT, ZeroValue);
    p_nptxop.T_TOTALTAXSUM       := NVL(p_nptxop.T_TOTALTAXSUM, ZeroValue);
    p_nptxop.T_PREVTAXSUM        := NVL(p_nptxop.T_PREVTAXSUM, ZeroValue);
    p_nptxop.T_TAXSUM            := NVL(p_nptxop.T_TAXSUM, ZeroValue);
    p_nptxop.T_TAX               := NVL(p_nptxop.T_TAX, ZeroValue);
    p_nptxop.T_METHOD            := NVL(p_nptxop.T_METHOD, ZeroValue);
    p_nptxop.T_ACCOUNT           := NVL(p_nptxop.T_ACCOUNT, CHR(0));
    p_nptxop.T_CURRENCY          := NVL(p_nptxop.T_CURRENCY, ZeroValue);
    p_nptxop.T_STATUS            := NVL(p_nptxop.T_STATUS, ZeroValue);
    p_nptxop.T_OPER              := NVL(p_nptxop.T_OPER, RsbSessionData.Oper);
    p_nptxop.T_DEPARTMENT        := NVL(p_nptxop.T_DEPARTMENT, 1);
    p_nptxop.T_IIS               := NVL(p_nptxop.T_IIS, CHR(0));
    p_nptxop.T_TAXTOPAY          := NVL(p_nptxop.T_TAXTOPAY, ZeroValue);
    p_nptxop.T_CALCNDFL          := NVL(p_nptxop.T_CALCNDFL, CHR(0));
    p_nptxop.T_RECALC            := NVL(p_nptxop.T_RECALC, CHR(0));
    p_nptxop.T_BEGRECALCDATE     := NVL(p_nptxop.T_BEGRECALCDATE, UnknownDate);
    p_nptxop.T_ENDRECALCDATE     := NVL(p_nptxop.T_ENDRECALCDATE, UnknownDate);
    p_nptxop.T_TIME              := NVL(p_nptxop.T_TIME, UnknownTime);
    p_nptxop.T_CURRENTYEAR_SUM   := NVL(p_nptxop.T_CURRENTYEAR_SUM, ZeroValue);
    p_nptxop.T_CURRENCYSUM       := NVL(p_nptxop.T_CURRENCYSUM, ZeroValue);
    p_nptxop.T_FLAGTAX           := NVL(p_nptxop.T_FLAGTAX, CHR(0));
    p_nptxop.T_PARTIAL           := NVL(p_nptxop.T_PARTIAL, CHR(0));
    p_nptxop.T_ACCOUNTTAX        := NVL(p_nptxop.T_ACCOUNTTAX, CHR(0));
    p_nptxop.T_TAXSUM2           := NVL(p_nptxop.T_TAXSUM2, ZeroValue);
    p_nptxop.T_FIID              := NVL(p_nptxop.T_FIID, ZeroValue);
    p_nptxop.T_CLOSECONTR        := NVL(p_nptxop.T_CLOSECONTR, CHR(0));
    p_nptxop.T_LIMITSTATUS       := NVL(p_nptxop.T_LIMITSTATUS, ZeroValue);

    --добавлено в 71 патче
    p_nptxop.T_PLACEKIND2        := NVL(p_nptxop.T_PLACEKIND2, ZeroValue);
    p_nptxop.T_PLACE2            := NVL(p_nptxop.T_PLACE2, ZeroValue);
    p_nptxop.T_MARKETPLACE       := NVL(p_nptxop.T_MARKETPLACE, UnknownValue);
    p_nptxop.T_MARKETPLACE2      := NVL(p_nptxop.T_MARKETPLACE2, UnknownValue);
    p_nptxop.T_MARKETSECTOR      := NVL(p_nptxop.T_MARKETSECTOR, ZeroValue);
    p_nptxop.T_MARKETSECTOR2     := NVL(p_nptxop.T_MARKETSECTOR2, ZeroValue);
    p_nptxop.T_TAXDP             := NVL(p_nptxop.T_TAXDP, ZeroValue);
  --  p_nptxop.T_CURRENTYEAR_CUR   := NVL(p_nptxop.T_CURRENTYEAR_CUR, UnknownValue);

    p_nptxop.T_PAYPURPOSE        := NVL(p_nptxop.T_PAYPURPOSE, 1);  -- DEF-102805

  END;

  PROCEDURE INS_DLREQACC (p_dlrqacc IN OUT DDLRQACC_DBT%ROWTYPE )
  AS
  BEGIN
    p_dlrqacc.T_ID               := NVL(p_dlrqacc.T_ID, ZeroValue);
    p_dlrqacc.T_DOCKIND          := NVL(p_dlrqacc.T_DOCKIND, 4607);
    p_dlrqacc.T_DOCID            := NVL(p_dlrqacc.T_DOCID, ZeroValue);
    p_dlrqacc.T_SUBKIND          := NVL(p_dlrqacc.T_SUBKIND, ZeroValue);
    p_dlrqacc.T_TYPE             := NVL(p_dlrqacc.T_TYPE, 2);
    p_dlrqacc.T_PARTY            := NVL(p_dlrqacc.T_PARTY, UnknownValue);
    p_dlrqacc.T_FIID             := NVL(p_dlrqacc.T_FIID, ZeroValue);
    p_dlrqacc.T_BANKID           := NVL(p_dlrqacc.T_BANKID, RsbSessionData.OperDprt);
    p_dlrqacc.T_CHAPTER          := NVL(p_dlrqacc.T_CHAPTER, ZeroValue);
    p_dlrqacc.T_ACCOUNT          := NVL(p_dlrqacc.T_ACCOUNT, CHR(0));
    p_dlrqacc.T_BANKCODEKIND     := NVL(p_dlrqacc.T_BANKCODEKIND, ZeroValue);
    p_dlrqacc.T_BANKCODE         := NVL(p_dlrqacc.T_BANKCODE, CHR(1));
    p_dlrqacc.T_BANKNAME         := NVL(p_dlrqacc.T_BANKNAME, CHR(1));
    p_dlrqacc.T_BANKCORRID       := NVL(p_dlrqacc.T_BANKCORRID, ZeroValue);
    p_dlrqacc.T_BANKCORRCODEKIND := NVL(p_dlrqacc.T_BANKCORRCODEKIND, ZeroValue);
    p_dlrqacc.T_BANKCORRCODE     := NVL(p_dlrqacc.T_BANKCORRCODE, CHR(1));
    p_dlrqacc.T_BANKCORRNAME     := NVL(p_dlrqacc.T_BANKCORRNAME, CHR(1));
    p_dlrqacc.T_CORRACC          := NVL(p_dlrqacc.T_CORRACC, CHR(1));
    p_dlrqacc.T_VERSION          := NVL(p_dlrqacc.T_VERSION, ZeroValue);
  END;

  PROCEDURE INS_DDLRQ (p_dlrq IN OUT DDLRQ_DBT%ROWTYPE )
  AS
  BEGIN
    p_dlrq.T_ID             := NVL(p_dlrq.T_ID, ZeroValue);
    p_dlrq.T_DOCKIND        := NVL(p_dlrq.T_DOCKIND, 4607);
    p_dlrq.T_DOCID          := NVL(p_dlrq.T_DOCID, ZeroValue);
    p_dlrq.T_DEALPART       := NVL(p_dlrq.T_DEALPART, 1);
    p_dlrq.T_KIND           := NVL(p_dlrq.T_KIND, UnknownValue); /*в зависимости от типа списания/зачисления 0-требование, 1-обязательство*/
    p_dlrq.T_SUBKIND        := NVL(p_dlrq.T_SUBKIND, ZeroValue);
    p_dlrq.T_TYPE           := NVL(p_dlrq.T_TYPE, 2);
    p_dlrq.T_NUM            := NVL(p_dlrq.T_NUM, ZeroValue);
    p_dlrq.T_AMOUNT         := NVL(p_dlrq.T_AMOUNT, ZeroValue);
    p_dlrq.T_FIID           := NVL(p_dlrq.T_FIID, ZeroValue);
    p_dlrq.T_PARTY          := NVL(p_dlrq.T_PARTY, UnknownValue);
    p_dlrq.T_RQACCID        := NVL(p_dlrq.T_RQACCID, UnknownValue);
    p_dlrq.T_PLACEID        := NVL(p_dlrq.T_PLACEID, 1);
    p_dlrq.T_STATE          := NVL(p_dlrq.T_STATE, ZeroValue); /*0-отложенная*/
    p_dlrq.T_PLANDATE       := NVL(p_dlrq.T_PLANDATE, UnknownDate); -- заполняется
    p_dlrq.T_FACTDATE       := NVL(p_dlrq.T_FACTDATE, UnknownDate);
    p_dlrq.T_USENETTING     := NVL(p_dlrq.T_USENETTING, CHR(0));
    p_dlrq.T_NETTING        := NVL(p_dlrq.T_NETTING, CHR(0));
    p_dlrq.T_CLIRING        := NVL(p_dlrq.T_CLIRING, CHR(0));
    p_dlrq.T_INSTANCE       := NVL(p_dlrq.T_INSTANCE, ZeroValue);
    p_dlrq.T_CHANGEDATE     := NVL(p_dlrq.T_CHANGEDATE, UnknownDate); -- заполняется
    p_dlrq.T_ACTION         := NVL(p_dlrq.T_ACTION, ZeroValue); /*0-отложенная*/
    p_dlrq.T_ID_OPERATION   := NVL(p_dlrq.T_ID_OPERATION, ZeroValue); /*пока нет операции, вставка в отложенные*/
    p_dlrq.T_ID_STEP        := NVL(p_dlrq.T_ID_STEP, ZeroValue); /*пока нет шагов, вставка в отложенные*/
    p_dlrq.T_SOURCE         := NVL(p_dlrq.T_SOURCE, ZeroValue);
    p_dlrq.T_SOURCEOBJKIND  := NVL(p_dlrq.T_SOURCEOBJKIND, UnknownValue);
    p_dlrq.T_SOURCEOBJID    := NVL(p_dlrq.T_SOURCEOBJID, ZeroValue);
    p_dlrq.T_VERSION        := NVL(p_dlrq.T_VERSION, ZeroValue);
    p_dlrq.T_ISKVIT         := NVL(p_dlrq.T_ISKVIT, CHR(0));
    p_dlrq.T_FACTAMOUNT     := NVL(p_dlrq.T_FACTAMOUNT, ZeroValue);
    p_dlrq.T_FACTRECEIVERID := NVL(p_dlrq.T_FACTRECEIVERID, UnknownValue);
    p_dlrq.T_ISCONFIRMED    := NVL(p_dlrq.T_ISCONFIRMED, CHR(0));
    p_dlrq.T_TAXRATEBUY     := NVL(p_dlrq.T_TAXRATEBUY, ZeroValue);
    p_dlrq.T_TAXSUMBUY      := NVL(p_dlrq.T_TAXSUMBUY, ZeroValue);
    p_dlrq.T_TAXRATESELL    := NVL(p_dlrq.T_TAXRATESELL, ZeroValue);
    p_dlrq.T_TAXSUMSELL     := NVL(p_dlrq.T_TAXSUMSELL, ZeroValue);
  END;

  PROCEDURE INS_SPGROUND (p_SPGROUND IN OUT DSPGROUND_DBT%ROWTYPE )
  AS
  BEGIN

   p_spground.T_SPGROUNDID     :=NVL(p_spground.T_SPGROUNDID, ZeroValue);
   p_spground.T_DOCLOG         :=NVL(p_spground.T_DOCLOG, 513);
   p_spground.T_KIND           :=NVL(p_spground.T_KIND, 251);
   p_spground.T_DIRECTION      :=NVL(p_spground.T_DIRECTION, 1);
   p_spground.T_XLD            :=NVL(p_spground.T_XLD, '0'); -- заполняется
   p_spground.T_REGISTRDATE    :=NVL(p_spground.T_REGISTRDATE, UnknownDate);  --заполняется
   p_spground.T_REGISTRTIME    :=NVL(p_spground.T_REGISTRTIME, UnknownTime);  --заполняется
   p_spground.T_PARTY          :=NVL(p_spground.T_PARTY, UnknownValue);       -- заполняется
   p_spground.T_ALTXLD         :=NVL(p_spground.T_ALTXLD, '0'); --заполняется
   p_spground.T_SIGNEDDATE     :=NVL(p_spground.T_SIGNEDDATE, UnknownDate);   -- заполняется
   p_spground.T_SIGNEDTIME     :=NVL(p_spground.T_SIGNEDTIME, UnknownTime);
   p_spground.T_PROXY          :=NVL(p_spground.T_PROXY, UnknownValue);
   p_spground.T_DIVISION       :=NVL(p_spground.T_DIVISION, ZeroValue);
   p_spground.T_REFERENCES     :=NVL(p_spground.T_REFERENCES, 1);
   p_spground.T_RECEPTIONIST   :=NVL(p_spground.T_RECEPTIONIST, 1);
   p_spground.T_COPIES         :=NVL(p_spground.T_COPIES, ZeroValue);
   p_spground.T_SENT           :=NVL(p_spground.T_SENT, '');
   p_spground.T_DELIVERYKIND   :=NVL(p_spground.T_DELIVERYKIND, ZeroValue);
   p_spground.T_BACKOFFICE     :=NVL(p_spground.T_BACKOFFICE, 'S');
   p_spground.T_COMMENT        :=NVL(p_spground.T_COMMENT, '');
   p_spground.T_SOURCEDOCID    :=NVL(p_spground.T_SOURCEDOCID, ZeroValue);
   p_spground.T_SOURCEDOCKIND  :=NVL(p_spground.T_SOURCEDOCKIND, ZeroValue);
   p_spground.T_DOCTEMPLATE    :=NVL(p_spground.T_DOCTEMPLATE, ZeroValue);
   p_spground.T_TERMINATEDATE  :=NVL(p_spground.T_TERMINATEDATE, UnknownDate);
   p_spground.T_PARTYNAME      :=NVL(p_spground.T_PARTYNAME, CHR(1)); --заполняется
   p_spground.T_PARTYCODE      :=NVL(p_spground.T_PARTYCODE, CHR(1)); -- заполняется
   p_spground.T_BEGINNINGDATE  :=NVL(p_spground.T_BEGINNINGDATE, UnknownTime);
   p_spground.T_SENTDATE       :=NVL(p_spground.T_SENTDATE, UnknownTime);
   p_spground.T_SENTTIME       :=NVL(p_spground.T_SENTTIME, UnknownTime);
   p_spground.T_DEPARTMENT     :=NVL(p_spground.T_DEPARTMENT, 1);
   p_spground.T_BRANCH         :=NVL(p_spground.T_BRANCH, 1);
   p_spground.T_PARENT         :=NVL(p_spground.T_PARENT, ZeroValue);
   p_spground.T_USERLOG        :=NVL(p_spground.T_USERLOG, ZeroValue);
   p_spground.T_VERSION        :=NVL(p_spground.T_VERSION, 2);
   p_spground.T_ISMAKEAUTO     :=NVL(p_spground.T_ISMAKEAUTO, '');
   p_spground.T_TECHAUTODOC    :=NVL(p_spground.T_TECHAUTODOC, ZeroValue);
   p_spground.T_DEPONENT       :=NVL(p_spground.T_DEPONENT, UnknownValue);
   p_spground.T_HAVESUBJLIST   :=NVL(p_spground.T_HAVESUBJLIST, '');
   p_spground.T_SUBJECTID      :=NVL(p_spground.T_SUBJECTID, UnknownValue);
   p_spground.T_REGISTERID     :=NVL(p_spground.T_REGISTERID, ZeroValue);
   p_spground.T_DEPOACNTID     :=NVL(p_spground.T_DEPOACNTID, ZeroValue);
   p_spground.T_MSGNUMBER      :=NVL(p_spground.T_MSGNUMBER, '');
   p_spground.T_MSGDATE        :=NVL(p_spground.T_MSGDATE, UnknownTime);
   p_spground.T_MSGTIME        :=NVL(p_spground.T_MSGTIME, UnknownTime);
   p_spground.T_METHODAPPLIC   :=NVL(p_spground.T_METHODAPPLIC, 1);


   null;
  END;

  PROCEDURE INS_SPGRDOC (p_SPGRDOC IN OUT DSPGRDOC_DBT%ROWTYPE )
  AS
  BEGIN

    p_spgrdoc.T_SOURCEDOCKIND :=NVL(p_spgrdoc.T_SOURCEDOCKIND, 4607);
    p_spgrdoc.T_SOURCEDOCID   :=NVL(p_spgrdoc.T_SOURCEDOCID, ZeroValue);   -- заполняется
    p_spgrdoc.T_SPGROUNDID    :=NVL(p_spgrdoc.T_SPGROUNDID, ZeroValue );  -- заполняется
    p_spgrdoc.T_ORDER         :=NVL(p_spgrdoc.T_ORDER, 1);
    p_spgrdoc.T_DEBITCREDIT   :=NVL(p_spgrdoc.T_DEBITCREDIT, 0);
    p_spgrdoc.T_STATUS        :=NVL(p_spgrdoc.T_STATUS, '');
    p_spgrdoc.T_VERSION       :=NVL(p_spgrdoc.T_VERSION, 0);

    null;
  END;

  FUNCTION GetPtName(p_partyid IN DPARTY_DBT.T_NAME%TYPE) RETURN VARCHAR2
  AS
    RetName VARCHAR2(320 CHAR) := CHR(1);
  BEGIN
    SELECT t_name INTO RetName FROM dparty_dbt WHERE t_partyid = p_partyid;
    RETURN RetName;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RETURN RetName;
  END;

  FUNCTION GetPtCode(p_partyid IN DPARTY_DBT.T_PARTYID%TYPE) RETURN VARCHAR2
  AS
    PtCode VARCHAR2(50 CHAR) := CHR(1);
  BEGIN
    select T_CODE INTO PtCode from dobjcode_dbt where t_objecttype = 3
           and t_CODEKIND = 1 AND T_STATE = 0 AND t_objectid = p_partyid AND ROWNUM = 1;
    return PtCode;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RETURN PtCode;
  END;


BEGIN
 
  --Создадим операцию
  v_nptxop.T_SUBKIND_OPERATION := p_KindOP;
  v_nptxop.T_CODE              := p_OperCode;
  v_nptxop.T_OPERDATE          := p_OperDate;
  v_nptxop.T_CLIENT            := p_PartyID;
  v_nptxop.T_CONTRACT          := p_ContrID;
  v_nptxop.T_OUTSUM            := p_Amount;
  v_nptxop.T_ACCOUNT           := p_Account;
  v_nptxop.T_CURRENCY          := p_FIID;
  v_nptxop.T_DEPARTMENT        := p_Filial;
  v_nptxop.T_TIME              := p_Time;
  v_nptxop.T_TAXSUM2           := p_Amount;
  v_nptxop.T_FIID              := p_FIID;

  IF p_KindOP = 20 THEN
    IF p_CalcNDFL = CHR(88) THEN
      v_nptxop.T_CALCNDFL := p_CalcNDFL;
    ELSE
      v_nptxop.T_CALCNDFL := NULL;
    END IF;

    IF p_FlagTax = CHR(88) THEN

      v_nptxop.T_FLAGTAX := p_FlagTax;

      IF p_TaxAccount IS NULL THEN
        v_nptxop.T_ACCOUNTTAX := v_nptxop.T_ACCOUNT;
      ELSE
        v_nptxop.T_ACCOUNTTAX := p_TaxAccount;
      END IF;

      IF p_TaxFIID IS NOT NULL THEN
        v_nptxop.T_FIID := p_TaxFIID;
      END IF;

    ELSE
      v_nptxop.T_FLAGTAX := NULL;
    END IF;

    IF p_CloseContr = CHR(88) THEN
      v_nptxop.T_CLOSECONTR := p_CloseContr;
    ELSE
      v_nptxop.T_CLOSECONTR := NULL;
    END IF;

  END IF;

  IF p_TransIIS = CHR(88) THEN
    v_nptxop.T_IIS := p_TransIIS;
    v_nptxop.T_CURRENTYEAR_SUM := p_IISAmount;
  ELSE
    v_nptxop.T_IIS := NULL;
    v_nptxop.T_CURRENTYEAR_SUM := NULL;
  END IF;

  IF p_Partial = CHR(88) THEN
    v_nptxop.T_PARTIAL := p_Partial;
  END IF;

  INS_NPTXOP(v_nptxop);
  INSERT INTO DNPTXOP_DBT values v_nptxop RETURNING t_id INTO v_nptxop.T_ID;

  --Вставим платежные реквизиты ТО
  v_dlrqacc.T_DOCID   := v_nptxop.T_ID;
  v_dlrqacc.T_PARTY   := p_PartyID;
  v_dlrqacc.T_FIID    := p_FIID;
  v_dlrqacc.T_ACCOUNT := p_SPIAccount;
  v_dlrqacc.T_BANKID := p_BankID;
  v_dlrqacc.T_BANKNAME := GetPtName(v_dlrqacc.T_BANKID);
  v_dlrqacc.T_BANKCODE := p_BankCode;
  v_dlrqacc.T_BANKCODEKIND := p_BankCodeKind;

  v_dlrqacc.T_CORRACC := p_CorrAccount;
  v_dlrqacc.T_BANKCORRID := p_CorrBankID;
  v_dlrqacc.T_BANKCORRNAME := GetPtName(v_dlrqacc.T_BANKCORRID);
  v_dlrqacc.T_BANKCORRCODE := p_CorrBankCode;
  v_dlrqacc.T_BANKCORRCODEKIND := p_CorrBankCodeKind;

  INS_DLREQACC(v_dlrqacc);
  INSERT INTO DDLRQACC_DBT values v_dlrqacc RETURNING t_id INTO v_dlrqacc.T_ID;

  --Вставим реквизиты ТО по операциям с ц.б
  v_dlrq.T_DOCID := v_nptxop.T_ID;
  IF p_KindOP = 10
  THEN
    v_dlrq.T_KIND := 1;
  ELSE
    v_dlrq.T_KIND := 0;
  END IF;
  v_dlrq.T_AMOUNT     := p_Amount;
  v_dlrq.T_FIID       := p_FIID;
  v_dlrq.T_PARTY      := p_PartyID;
  v_dlrq.T_RQACCID    := v_dlrqacc.T_ID;
  v_dlrq.T_PLANDATE   := p_OperDate;
  v_dlrq.T_CHANGEDATE := p_OperDate;

  INS_DDLRQ(v_dlrq);
  INSERT INTO DDLRQ_DBT values v_dlrq RETURNING t_id INTO v_dlrq.T_ID;

   v_spground.T_XLD            := p_OperCode; -- заполняется
   v_spground.T_REGISTRDATE    := p_SPREGISTRDATE; --заполняется
   v_spground.T_REGISTRTIME    := p_SPREGISTRTIME; --заполняется
   v_spground.T_PARTY          := p_PartyID; -- заполняется
   v_spground.T_ALTXLD         := p_SPALTXLD; --заполняется
   v_spground.T_SIGNEDDATE     := p_SPREGISTRDATE; -- заполняется
   v_spground.T_PARTYNAME      := GetPtName(p_PartyID); --заполняется
   v_spground.T_PARTYCODE      := GetPtCode(p_PartyID); -- заполняется

  INS_SPGROUND(v_spground);
  INSERT INTO DSPGROUND_DBT values v_SPGROUND RETURNING T_SPGROUNDID INTO v_spground.T_SPGROUNDID;

  v_spgrdoc.T_SOURCEDOCID   := v_nptxop.T_ID;  -- заполняется
  v_spgrdoc.T_SPGROUNDID    := v_spground.T_SPGROUNDID;  -- заполняется

  INS_SPGRDOC(v_spgrdoc);
  INSERT INTO DSPGRDOC_DBT values v_spgrdoc;


  p_RetOperID := v_nptxop.T_ID;
EXCEPTION WHEN OTHERS THEN
  p_Err := SUBSTR(SQLERRM, 1, 1000);
END;

--перенумерация
FUNCTION UpdateOrderXLD( pDate in Date, pCount in Number) RETURN NUMBER 
IS
 cur_count number;
 new_xld varchar2(30);
 err number;
 cur_year varchar2(4); 
 function GetClientEKKForXLD(pContract in number) return varchar2
   is
     vEKK varchar2(10); 
   begin
       select dlcode.t_code into vEKK
         from ddlcontrmp_dbt mp, ddlobjcode_dbt dlcode
        where 1 = 1
          and dlcode.t_objecttype = 207
          and dlcode.t_codekind = 1
          and dlcode.t_objectid = mp.t_dlcontrid
          and mp.t_sfcontrid = pContract
--          and t_bankclosedate = to_date('01010001', 'ddmmyyyy')
          and rownum = 1;
      return vEKK||'/';
      exception when no_data_found then
        return '';         
   end;
   function CheckOrderIsDBO (pXLD in varchar2) return varchar2
     is
     begin
     if instr(pXLD,'/ДБО') > 0 then
       return '/ДБО';
       end if; 
     return '';
     end;
procedure ClearTMPTable
  is 
  begin
    delete from uNPTXRename_tmp;
    commit;
  end;  

procedure FillTMPTable
  is
  begin
        insert into uNPTXRename_tmp 
        ( select t.t_id, t.t_contract,
               t.t_code,
               t.t_operdate,
               t.t_spgroundid,
               t.t_kind,
               t.t_xld,
               t.t_altxld,
               t.t_registrdatetime,         
               t.t_signeddate, chr(1), chr(1) 
       from 
      (select  nptx.t_id,
               nptx.t_code,
               nptx.t_contract,
               nptx.t_operdate,
               spground.t_spgroundid,
               spground.t_kind,
               spground.t_xld,
               spground.t_registrdate,
               spground.t_registrtime,
               spground.t_altxld,
               spground.t_signeddate,
               spground.t_signedtime,
               spground.t_registrdate + (spground.t_registrtime - to_date('01010001','ddmmyyyy')) as t_registrdatetime,
               ROW_NUMBER() OVER (ORDER BY (spground.t_registrdate + (spground.t_registrtime - to_date('01010001','ddmmyyyy'))) asc) AS ROW_NUMBER
        from dnptxop_dbt nptx, dspgrdoc_dbt spgrdoc, dspground_dbt spground
       where t_dockind = 4607
         and t_kind_operation = 2037
         and t_subkind_operation = 20
         and spgrdoc.t_sourcedockind = nptx.t_dockind
         and spgrdoc.t_sourcedocid = nptx.t_id
         and spground.t_spgroundid = spgrdoc.t_spgroundid
         and spground.t_kind = 251
         and spground.t_registrdate >= pDate) t
        where 1=1 );
       commit;
    end;
      
procedure SaveNewXLD(pREC in uNPTXRename_tmp%ROWTYPE, pNewXLD in varchar2)   
  is
  begin
    update uNPTXRename_tmp tmp set tmp.T_NEW_XLD = pNewXLD where tmp.t_id =pREC.t_Id and tmp.t_spgroundid = pREC.t_Spgroundid;
    commit;
  end;

function PreCheckRenum return varchar2
  is
  nRec number;
  begin
    for rec in (select * from uNPTXRename_tmp) loop
      update dnptxop_dbt nptx set nptx.t_code = substr('!'||nptx.t_code,1,25) where nptx.t_id = rec.t_id; 
      end loop;
    for rec in (select * from uNPTXRename_tmp) loop
      select count(1) into nRec from dnptxop_dbt nptx where nptx.t_code = rec.t_new_xld;
      if nRec > 0 then
        update uNPTXRename_tmp tmp set tmp.t_renum_err = 'Операция с номером '|| rec.t_new_xld ||' уже существует.' where tmp.t_id = REC.t_Id and tmp.t_spgroundid = REC.t_Spgroundid;
        end if;
      update dnptxop_dbt nptx set nptx.t_code = rec.t_code where nptx.t_id = rec.t_id;  
      end loop;  
      commit;
      return 0;
  end;  


procedure ExecRenum
  is
  cur_rec_id number;
  cur_rec_spgroundid number;

  begin
    for rec in (select * from uNPTXRename_tmp) loop
      cur_rec_id := rec.t_id;
      cur_rec_spgroundid := rec.t_spgroundid;
      update dnptxop_dbt nptx set nptx.t_code = substr(rec.t_new_xld,1,25) where nptx.t_id = rec.t_id;
      update dspground_dbt spground set spground.t_xld = rec.t_new_xld where spground.t_spgroundid = rec.t_spgroundid;
      update uNPTXRename_tmp tmp set tmp.t_renum_err = 'OK' where tmp.t_id = rec.t_id and tmp.t_spgroundid = rec.t_spgroundid;  
    end loop;  
  commit;
  exception when others then
    update uNPTXRename_tmp tmp set tmp.t_renum_err = 'OK' where tmp.t_id = cur_rec_id and tmp.t_spgroundid = cur_rec_spgroundid;  
    commit;
end;    

BEGIN
  cur_count := pCount;
  select extract (YEAR from sysdate) into cur_year from dual;
  ClearTMPTable;
  FillTMPTable;
  cur_count := pCount;
  for rec in (select * from uNPTXRename_tmp order by T_REGISTRDATETIME asc) loop
    new_xld := '';
    new_xld := new_xld||GetClientEKKForXLD(rec.t_contract)||cur_count||'/'||cur_year||CheckOrderIsDBO(rec.t_XLD) ;
    new_xld := substr(new_xld,1,25); -- ограничение по длине поля
    SaveNewXLD(rec, new_xld);
    cur_count := cur_count + 1; 
  end loop;
  err := PreCheckRenum;   
  if err = 0 then
    ExecRenum;
    err := SetNPTXSequence(cur_count);
    end if;
  return 1;
END UpdateOrderXLD;


END user_NPTXFunc;
/
