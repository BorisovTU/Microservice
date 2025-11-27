CREATE OR REPLACE PACKAGE user_NPTXFunc
AS
   type ClientOrder_typ  is record

    ( t_id number(10),
      t_code varchar2(25),
      t_operdate date,
      t_spgroundid number(10),
      t_kind number(5),
      t_xld varchar2(30),
      t_altxld varchar2(30),
      t_registrdatetime date,         
      t_signeddate date
    );
    
 FUNCTION GenerateOpCodebySyq(pEKK in varchar2, pYear in varchar2, pDBO in varchar2) RETURN varchar2;
 FUNCTION SetNPTXSequence(pCount in number) RETURN NUMBER ;
 function GetLastClientOrderCount (pDate in date) RETURN NUMBER; 
 function getLastClientOrderNum(pDate in date) return varchar2;
 function CreateNPTXSeq return number;
 function GetNextValNPTXSeq return number;  
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
                                        p_Err OUT VARCHAR2);
--перенумерация                                        
FUNCTION UpdateOrderXLD( pDate in Date, pCount in Number) RETURN NUMBER ;                                        

END user_NPTXFunc;
/
