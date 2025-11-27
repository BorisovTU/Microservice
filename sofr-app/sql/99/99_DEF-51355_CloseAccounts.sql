DECLARE
  zeroDate DATE := to_date('01.01.0001', 'DD.MM.YYYY');
  logID VARCHAR2(9) := 'DEF-51355';
  
  CURSOR c_closedContracts IS -- закрытые ДБО
    SELECT dl.t_dlContrID, 
           sf.t_partyID,
           pt.t_legalForm
      FROM ddlcontr_dbt dl, dsfcontr_dbt sf, dparty_dbt pt
     WHERE dl.t_sfContrID = sf.t_ID
       AND sf.t_DateClose > zeroDate
       AND sf.t_partyID = pt.t_partyID;
     
  CURSOR c_subContracts (p_dlContractID IN NUMBER) IS -- субдоговора ДБО
    SELECT sf.t_ID,
           sf.t_DateClose
      FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf
     WHERE mp.t_DlContrID = p_dlContractID
       AND mp.t_SfContrID = sf.t_ID;
  
  CURSOR c_contractAccounts (p_sfContractID IN NUMBER, p_categoryID IN NUMBER) IS -- открытые счета субдоговора
    SELECT acc.t_accountID, 
           acc.t_account, 
           (SELECT NVL(rest.t_rest, 0) 
              FROM dRestDate_dbt rest
             WHERE rest.t_accountID = acc.t_accountID
               AND rest.t_restCurrency = acc.t_code_currency
               AND rest.t_restDate = (SELECT MAX(t_restDate) FROM dRestDate_dbt WHERE t_accountID = acc.t_accountID AND t_restDate <= sysdate)) as t_rest,
           (SELECT NVL(rest.t_planRest, 0) 
              FROM dRestDate_dbt rest
             WHERE rest.t_accountID = acc.t_accountID
               AND rest.t_restCurrency = acc.t_code_currency
               AND rest.t_restDate = (SELECT MAX(t_restDate) FROM dRestDate_dbt WHERE t_accountID = acc.t_accountID AND t_restDate <= sysdate)) as t_planRest
      FROM dmcaccdoc_dbt mc, daccount_dbt acc
     WHERE mc.t_clientContrID = p_sfContractID
       AND mc.t_catID = p_categoryID
       AND mc.t_isCommon = chr(88)
       AND acc.t_account = mc.t_account
       AND acc.t_open_close != 'З';
  
  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
    
  -- Закрытие открытых счетов на закрытых договорах ДБО
  PROCEDURE CloseAccountsOnClosedContractsByCategory(p_category VARCHAR2, p_checkNotCompany BOOLEAN DEFAULT FALSE)
  AS
    categoryID NUMBER;
    totalAccounts NUMBER := 0;
    closedAccounts NUMBER := 0;
  BEGIN
    LogIt('Операция закрытия счетов категории '''||p_category||''' для закрытых ДБО');
    BEGIN
      SELECT t_ID INTO categoryID
        FROM dMcCateg_dbt
       WHERE t_Code  = p_category;
    EXCEPTION
      WHEN OTHERS THEN categoryID := -1;
    END;
     
    IF (categoryID = -1) THEN
      LogIt('Категория с именем '''||p_category||''' не найдена');
    ELSE
      FOR closedContract IN c_closedContracts LOOP
        FOR subContract IN c_subContracts(closedContract.t_dlContrID) LOOP
          IF subContract.t_DateClose = zeroDate THEN
            LogIt('Открытый субдоговор sfContr (id='||subContract.t_ID||') для закрытого ДБО dlContr (id='||closedContract.t_dlContrID||')');
          END IF;
          
          FOR oneAccount IN c_contractAccounts(subContract.t_ID, categoryID) LOOP
            IF ((p_checkNotCompany) AND (closedContract.t_LegalForm = 1 /*ЮЛ*/)) THEN
              LogIt('Для закрытого ДБО dlContr (id='||closedContract.t_dlContrID||') зарегистрированного на ЮЛ (partyID = '||closedContract.t_partyID||') найден счет категории '''||p_category||''' '||oneAccount.t_account||'. Счет не закрыт.');
            ELSIF ((oneAccount.t_rest != 0) OR (oneAccount.t_planRest != 0)) THEN
              LogIt('Ненулевые остатки для счета '''||oneAccount.t_account||'''. Счет не закрыт');
            ELSE 
              UPDATE dAccount_dbt acc
                 SET acc.t_Open_Close = 'З', 
                     acc.t_Close_Date = sysdate - 1
               WHERE acc.t_AccountID = oneAccount.t_accountID;
               
              closedAccounts :=  closedAccounts + 1;
            END IF;
            
            totalAccounts := totalAccounts + 1;
          END LOOP;
        END LOOP;
      END LOOP;
      
      LogIt('Для закрытых ДБО отобрано '||totalAccounts||' счетов категории '''||p_category||'''. Закрыто счетов '||closedAccounts);
    END IF;
    COMMIT;
  END;
BEGIN
  -- CloseAccountsOnClosedContractsByCategory('');
  CloseAccountsOnClosedContractsByCategory('ДС клиента, ц/б', true);
  CloseAccountsOnClosedContractsByCategory('+РасчетыКомисс1', true);
  CloseAccountsOnClosedContractsByCategory('ДС, Расч. с клиентом, ВУ');
  CloseAccountsOnClosedContractsByCategory('ДС Клиента, ВУ');
  CloseAccountsOnClosedContractsByCategory('ЦБ Клиента, ВУ');
  CloseAccountsOnClosedContractsByCategory('ЦБ, Расч. с клиентом, ВУ');
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/