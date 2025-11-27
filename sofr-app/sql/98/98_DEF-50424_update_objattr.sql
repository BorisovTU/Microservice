BEGIN
  UPDATE DOBJATTR_DBT
     SET T_NAME = CASE WHEN T_NUMINLIST = 'TQIR' THEN 'Т+ Облигации ПИР'
                       WHEN T_NUMINLIST = 'TQPI' THEN 'Т+ Акции ПИР'
                       WHEN T_NUMINLIST = 'FQBR' THEN 'Т+ Ин.Акции и ДР'
                       WHEN T_NUMINLIST = 'TQOE' THEN 'Т+ Облигации (расч.в EUR)'
                       WHEN T_NUMINLIST = 'PSIR' THEN 'РПС Облигации ПИР'
                       WHEN T_NUMINLIST = 'FFCB' THEN 'РЕПО с ЦБ РФ: Аукц.плав.ставки'
                       WHEN T_NUMINLIST = 'TQRD' THEN 'Т+ Облигации Д'
                       WHEN T_NUMINLIST = 'OCBR' THEN 'ОТС: Облигации с ЦК'
                       WHEN T_NUMINLIST = 'PTOE' THEN 'РПС с ЦК: Облигации (расч.EUR)'
                       WHEN T_NUMINLIST = 'TQOY' THEN 'Т+ Облигации (расч.в CNY)'
                       WHEN T_NUMINLIST = 'EQMP' THEN 'РЕПО с ЦК 1 мес.'
                       WHEN T_NUMINLIST = 'PTOY' THEN 'РПС с ЦК: Облигации (расч.CNY)'
                       WHEN T_NUMINLIST = 'PACT' THEN 'Аукцион: адресные заявки'
                       WHEN T_NUMINLIST = 'TQIY' THEN 'Т+ Облигации ПИР (расч.в CNY)'
                       WHEN T_NUMINLIST = 'OCTR' THEN 'OTC: Облигации T+'
                       WHEN T_NUMINLIST = 'OCBU' THEN 'ОТС: Облигации с ЦК (USD)'
                       WHEN T_NUMINLIST = 'OCTE' THEN 'OTC: Облигации T+ (EUR)'
                       WHEN T_NUMINLIST = 'OCTU' THEN 'OTC: Облигации T+ (USD)'
                       WHEN T_NUMINLIST = 'OCBE' THEN 'ОТС: Облигации с ЦК (EUR)'
                       ELSE ' ' END
   WHERE T_OBJECTTYPE = 101
     AND T_GROUPID = 106
     AND T_NAME IN (CHR(1), ' ');
                      
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