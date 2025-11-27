BEGIN
  UPDATE DOBJATTR_DBT
     SET T_FULLNAME = CASE WHEN T_NUMINLIST = 'TQIR' THEN 'T+ Bonds inv. risk'
                           WHEN T_NUMINLIST = 'TQPI' THEN 'T+ stocks inv. risk'
                           WHEN T_NUMINLIST = 'FQBR' THEN 'T+: FRGN stocks, DRs'
                           WHEN T_NUMINLIST = 'TQOE' THEN 'T+ Bonds EUR'
                           WHEN T_NUMINLIST = 'PSIR' THEN 'Neg. deals: inv. risk bonds'
                           WHEN T_NUMINLIST = 'FFCB' THEN 'REPO: with CB fl.rate (auction)'
                           WHEN T_NUMINLIST = 'TQRD' THEN '’+ D bonds'
                           WHEN T_NUMINLIST = 'OCBR' THEN 'OTC Bonds with CCP'
                           WHEN T_NUMINLIST = 'PTOE' THEN 'Neg. deals with CCP: Bonds EUR'
                           WHEN T_NUMINLIST = 'TQOY' THEN 'T+: Bonds CNY'
                           WHEN T_NUMINLIST = 'EQMP' THEN 'REPO with CCP 1m.'
                           WHEN T_NUMINLIST = 'PTOY' THEN 'Neg. deals with CCP: Bonds CNY'
                           WHEN T_NUMINLIST = 'PACT' THEN 'Auction: negotiated orders'
                           WHEN T_NUMINLIST = 'TQIY' THEN 'T+ Bonds inv. risk (CNY)'
                           WHEN T_NUMINLIST = 'OCTR' THEN 'OTC Bonds T+'
                           WHEN T_NUMINLIST = 'OCBU' THEN 'OTC Bonds with CCP (USD)'
                           WHEN T_NUMINLIST = 'OCTE' THEN 'OTC Bonds T+ (EUR)'
                           WHEN T_NUMINLIST = 'OCTU' THEN 'OTC Bonds T+ (USD)'
                           WHEN T_NUMINLIST = 'OCBE' THEN 'OTC Bonds with CCP (EUR)'
                       END
   WHERE T_OBJECTTYPE = 101
     AND T_GROUPID = 106
     AND T_NUMINLIST IN ('TQIR','TQPI','FQBR','TQOE','PSIR','FFCB','TQRD','OCBR','PTOE','TQOY','EQMP','PTOY','PACT','TQIY','OCTR','OCBU','OCTE','OCTU','OCBE');
                      
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/


BEGIN
  EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/