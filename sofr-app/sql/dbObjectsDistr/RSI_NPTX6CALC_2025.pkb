CREATE OR REPLACE PACKAGE BODY RSI_NPTX6CALC_2025
IS


  --Формирование расшифровок для 6-НДФЛ 2025
  PROCEDURE CalcNptx6Decoding(p_BegDate IN DATE, p_EndDate IN DATE, p_ByCB IN CHAR, p_ByDepo IN CHAR, p_ByVS IN CHAR, p_BySOFR IN CHAR, p_ByDIASOFT IN CHAR)
  AS
    v_Limit NUMBER := 10000;

    v_TaxPeriod NUMBER;

    TYPE nptx6dec_t IS TABLE OF dnptx6decoding_tmp%ROWTYPE;
    nptx6dec     dnptx6decoding_tmp%rowtype;
    nptx6dec_ins nptx6dec_t := nptx6dec_t();

    v_k NUMBER;
    v_NewTotalNOB  NUMBER; 
    v_PrevTotalNOB NUMBER;
    v_RecNOB       NUMBER;
    v_RecTotalPlus        NUMBER;
    v_RecTotalPlus13      NUMBER;
    v_RecTotalMinusReal13 NUMBER;

    v_IsResident NUMBER;

    v_RestTotalMinusOver NUMBER;
    v_TotalMinusNormal   NUMBER;

    v_RestTotalMinusReal NUMBER;
    v_NewTotalMinusReal NUMBER;
    v_WrtTotalMinusReal NUMBER;
    
    v_RestMinusObjSum NUMBER;
    v_MaxAddMinusSum NUMBER;
    v_AddMinusSum NUMBER;
    v_WrtMinusSum NUMBER;
    v_RestWrtMinusSum NUMBER;

    v_RestOverSum NUMBER;
    v_MaxSumTo NUMBER;
    v_AddNormSum NUMBER;


  BEGIN

    v_TaxPeriod := EXTRACT(YEAR FROM p_EndDate);

    FOR one_cl IN (select t_PartyID from dnptx6client_tmp)
    LOOP
      INSERT INTO DNPTX6COLLECT_TMP
      (T_ISSNOB,
       T_SNOB_CLIENT,
       T_SNOB_TYPE,
       T_SNOB_DESCR,
       T_SNOB_INC_DATE ,
       T_SNOB_PERIOD,
       T_SNOB_BASE,
       T_SNOB_NOB, 
       T_SNOB_RATE, 
       T_SNOB_CALC_TAX, 
       T_SNOB_KBK, 
       T_SNOB_HOLD_TAX,
       T_SNOB_TORATE,
       T_ISNDR,
       T_NDR_CLIENT,
       T_NDR_DATE, 
       T_NDR_TAXBASEKIND,
       T_NDR_SUM_PLUS_G,
       T_NDR_SUM_MINUS_G,
       T_NDR_KBK
      )
      with objkind as (select k.t_Element, k.t_Code, 
                              (case when k.t_TaxBaseKind = 9 then 6 
                                    when k.t_TaxBaseKind = 3 then 2
                                    when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1,RSI_NPTXC.TXOBJ_PLUSG_1011_2,RSI_NPTXC.TXOBJ_PLUSG_1011_3,RSI_NPTXC.TXOBJ_MINUSG_618) then 2
                                    when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_2_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_3_IIS,RSI_NPTXC.TXOBJ_MINUSG_619) then 5
                                    else k.t_TaxBaseKind end) as T_TAXBASEKIND 
                        from dnptxkind_dbt k where k.t_Code like 'PlusG%' or k.t_Code like 'MinusG%'),
           tb as (SELECT 1 AS ISSNOB,
                         Q.T_CLIENTID AS SNOB_CLIENT,
                         Q.T_TYPE AS SNOB_TYPE,
                         Q.T_DESCRIPTION AS SNOB_DESCR ,
                         Q.T_INCDATE AS SNOB_INC_DATE ,
                         Q.T_TAXPERIOD AS SNOB_PERIOD ,
                         Q.T_TAXBASEKIND AS SNOB_BASE,
                         SUM(Q.T_TAXBASECURRPAY) AS SNOB_NOB, -- ноб. по тек. выплате
                         Q.T_RATEHOLDPITAX AS SNOB_RATE, -- ставка НФДЛ   удержанный
                         SUM(Q.T_CALCPITAX) AS SNOB_CALC_TAX, -- НДФЛ исчисленный
                         Q.T_BCCHOLDPITAX AS SNOB_KBK, --КБК удержанный
                         SUM(Q.T_HOLDPITAX) AS SNOB_HOLD_TAX, -- НДФЛ удержанный
                         Q.SNOB_TORATE
                    FROM (SELECT STB.T_CLIENTID,
                                 STB.T_TYPE,
                                 STB.T_DESCRIPTION,
                                 STB.T_INCDATE,
                                 STB.T_TAXPERIOD,
                                 STB.T_TAXBASEKIND,
                                 STB.T_TAXBASECURRPAY, -- ноб. по тек. выплате
                                 STB.T_RATEHOLDPITAX, -- ставка НФДЛ   удержанный
                                 STB.T_CALCPITAX, -- НДФЛ исчисленный
                                 STB.T_BCCHOLDPITAX, --КБК удержанный
                                 STB.T_HOLDPITAX, -- НДФЛ удержанный
                                 (CASE WHEN     STB.T_RATEHOLDPITAX = 13 
                                            AND STB.T_TAXBASECURRPAY = 0 
                                            AND STB.T_APPLSTAXBASEINCLUDE > 2400000 
                                            AND STB.T_TAXBASEKIND IN ( RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK, RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_IIS) 
                                              THEN 15 
                                       ELSE STB.T_RATEHOLDPITAX END) as SNOB_TORATE
                            FROM DNPTXTOTALBASE_DBT STB
                           WHERE STB.T_CLIENTID = one_cl.t_PartyID 
                             AND STB.T_STORSTATE = 1
                             AND STB.T_TYPE IN (1,2,3)
                             AND ((STB.T_INCDATE BETWEEN p_BegDate AND p_EndDate AND STB.T_TAXPERIOD = v_TaxPeriod) OR (p_EndDate = TO_DATE('31.12.'||TO_CHAR(v_TaxPeriod), 'DD.MM.YYYY') AND STB.T_TAXPERIOD = v_TaxPeriod))
                             AND 1 = (CASE WHEN p_BySOFR = CNST.SET_CHAR AND STB.T_DESCRIPTION NOT LIKE '%Диасофт%' THEN 1
                                           WHEN p_ByDIASOFT = CNST.SET_CHAR AND STB.T_DESCRIPTION LIKE '%Диасофт%' THEN 1
                                           ELSE 0 END
                                     )
                             AND 1 = (CASE WHEN p_ByCB = CNST.SET_CHAR AND STB.T_TAXBASEKIND IN (0, RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK, RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_IIS) THEN 1
                                           WHEN p_ByVS = CNST.SET_CHAR AND STB.T_TAXBASEKIND IN (RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_ONB) THEN 1
                                           ELSE 0 END 
                                     )
                         ) Q
                   GROUP BY Q.T_CLIENTID, Q.T_TYPE, Q.T_DESCRIPTION, Q.T_INCDATE, Q.T_TAXPERIOD, Q.T_TAXBASEKIND, Q.T_RATEHOLDPITAX, Q.T_BCCHOLDPITAX, Q.SNOB_TORATE
                  ),
           ndr as (SELECT 1 AS ISNDR,
                          obj.T_CLIENT AS NDR_CLIENT,
                          obj.T_DATE AS NDR_DATE, 
                          knd.T_TAXBASEKIND as NDR_TAXBASEKIND,
                          obj.T_ANALITIC2 AS NDR_KBK,
                          (SELECT NVL(SUM(obj_plus.T_SUM0), 0)  
                             FROM DNPTXOBJ_DBT obj_plus
                            WHERE (obj_plus.T_TECHNICAL = CHR(0) OR obj_plus.T_TECHNICAL IS NULL)
                              AND (obj_plus.T_FROMOUTSYST = CHR(0) OR obj_plus.T_FROMOUTSYST IS NULL)
                              AND obj_plus.T_CLIENT = obj.t_Client
                              AND obj_plus.T_DATE = obj.t_Date
                              AND obj_plus.T_KIND IN (select k.t_Element 
                                                        from objkind k 
                                                       where k.t_Code like 'PlusG%'
                                                         and k.t_TaxBaseKind = knd.t_TaxBaseKind
                                                     )
                              and 1 = (case when obj_plus.t_Kind = RSI_NPTXC.TXOBJ_PLUSG_1011 and obj_plus.t_Sum0 = NVL((select /*+ index(obj1 dnptxobj_dbt_idxa)*/ SUM(obj1.t_Sum0) 
                                                                                                                           from dnptxobj_dbt obj1 
                                                                                                                          where obj1.t_Client = obj_plus.t_Client 
                                                                                                                            and obj1.t_Date = obj_plus.t_Date  
                                                                                                                            and obj1.t_Kind IN (RSI_NPTXC.TXOBJ_PLUSG_1011_1,RSI_NPTXC.TXOBJ_PLUSG_1011_2,RSI_NPTXC.TXOBJ_PLUSG_1011_3)
                                                                                                                            and obj1.t_AnaliticKind6 = obj_plus.t_AnaliticKind6    
                                                                                                                            and obj1.t_Analitic6 = obj_plus.t_Analitic6 
                                                                                                                            and obj1.t_OutSystCode = obj_plus.t_OutSystCode 
                                                                                                                            and obj1.t_OutObjID = obj_plus.t_OutObjID 
                                                                                                                            and (obj1.t_Technical = CHR(0) or obj1.t_Technical is null) 
                                                                                                                            and (obj1.t_FromOutSyst = CHR(0) or obj1.t_FromOutSyst is null) 
                                                                                                                        ),0) then 0 
                                            else 1 end) 
                              and 1 = (case when obj_plus.t_Kind = RSI_NPTXC.TXOBJ_PLUSG_1011_IIS and obj_plus.t_Sum0 = NVL((select /*+ index(obj1 dnptxobj_dbt_idxa)*/ SUM(obj1.t_Sum0) 
                                                                                                                               from dnptxobj_dbt obj1 
                                                                                                                              where obj1.t_Client = obj_plus.t_Client 
                                                                                                                                and obj1.t_Date = obj_plus.t_Date 
                                                                                                                                and obj1.t_Kind IN (RSI_NPTXC.TXOBJ_PLUSG_1011_1_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_2_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_3_IIS)
                                                                                                                                and obj1.t_AnaliticKind6 = obj_plus.t_AnaliticKind6 
                                                                                                                                and obj1.t_Analitic6 = obj_plus.t_Analitic6 
                                                                                                                                and obj1.t_OutSystCode = obj_plus.t_OutSystCode 
                                                                                                                                and obj1.t_OutObjID = obj_plus.t_OutObjID 
                                                                                                                                and (obj1.t_Technical = CHR(0) or obj1.t_Technical is null) 
                                                                                                                                and (obj1.t_FromOutSyst = CHR(0) or obj1.t_FromOutSyst is null) 
                                                                                                                            ),0) then 0 
                                            else 1 end)
                              AND obj_plus.T_ANALITICKIND2 = obj.T_ANALITICKIND2
                              AND obj_plus.T_ANALITIC2 = obj.T_ANALITIC2               
                          ) as NDR_SUM_PLUS_G,
                          (SELECT NVL(SUM(obj_minus.T_SUM0), 0)
                             FROM DNPTXOBJ_DBT obj_minus
                            WHERE (obj_minus.T_TECHNICAL = CHR(0) OR obj_minus.T_TECHNICAL IS NULL)
                              AND (obj_minus.T_FROMOUTSYST = CHR(0) OR obj_minus.T_FROMOUTSYST IS NULL)
                              AND obj_minus.T_CLIENT = obj.t_Client
                              AND obj_minus.T_DATE = obj.t_Date
                              AND obj_minus.T_KIND IN (select k.t_Element 
                                                         from objkind k 
                                                        where k.t_Code like 'MinusG%'
                                                          and k.t_TaxBaseKind = knd.t_TaxBaseKind
                                                      )
                              AND obj_minus.T_ANALITICKIND2 = obj.T_ANALITICKIND2
                              AND obj_minus.T_ANALITIC2 = obj.T_ANALITIC2
                          ) as NDR_SUM_MINUS_G
                    FROM DNPTXOBJ_DBT obj
                         JOIN objkind knd ON obj.T_KIND = knd.T_ELEMENT
                   WHERE (obj.T_TECHNICAL = CHR(0) OR obj.T_TECHNICAL IS NULL)
                     AND (obj.T_FROMOUTSYST = CHR(0) OR obj.T_FROMOUTSYST IS NULL)
                     AND obj.T_CLIENT = one_cl.t_PartyID
                     AND obj.T_DATE BETWEEN p_BegDate AND p_EndDate
                     AND obj.T_ANALITICKIND2 IN (0, RSI_NPTXC.TXOBJ_KIND2040)
                     and 1 = (CASE WHEN p_ByCB = CNST.SET_CHAR AND (knd.t_TaxBaseKind = 0 OR knd.t_TaxBaseKind IN (RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK, RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_IIS)) THEN 1
                                   WHEN p_ByVS = CNST.SET_CHAR AND knd.t_TaxBaseKind IN (RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_ONB) THEN 1
                                   ELSE 0 END 
                             )
                     and 1 = (case when obj.t_Kind = RSI_NPTXC.TXOBJ_PLUSG_1011 and obj.t_Sum0 = NVL((select /*+ index(obj1 dnptxobj_dbt_idxa)*/ SUM(obj1.t_Sum0) 
                                                                                                                           from dnptxobj_dbt obj1 
                                                                                                                          where obj1.t_Client = obj.t_Client 
                                                                                                                            and obj1.t_Date = obj.t_Date  
                                                                                                                            and obj1.t_Kind IN (RSI_NPTXC.TXOBJ_PLUSG_1011_1,RSI_NPTXC.TXOBJ_PLUSG_1011_2,RSI_NPTXC.TXOBJ_PLUSG_1011_3)
                                                                                                                            and obj1.t_AnaliticKind6 = obj.t_AnaliticKind6    
                                                                                                                            and obj1.t_Analitic6 = obj.t_Analitic6 
                                                                                                                            and obj1.t_OutSystCode = obj.t_OutSystCode 
                                                                                                                            and obj1.t_OutObjID = obj.t_OutObjID 
                                                                                                                            and (obj1.t_Technical = CHR(0) or obj1.t_Technical is null) 
                                                                                                                            and (obj1.t_FromOutSyst = CHR(0) or obj1.t_FromOutSyst is null) 
                                                                                                                        ),0) then 0 
                                   else 1 end) 
                     and 1 = (case when obj.t_Kind = RSI_NPTXC.TXOBJ_PLUSG_1011_IIS and obj.t_Sum0 = NVL((select /*+ index(obj1 dnptxobj_dbt_idxa)*/ SUM(obj1.t_Sum0) 
                                                                                                                     from dnptxobj_dbt obj1 
                                                                                                                    where obj1.t_Client = obj.t_Client 
                                                                                                                      and obj1.t_Date = obj.t_Date 
                                                                                                                      and obj1.t_Kind IN (RSI_NPTXC.TXOBJ_PLUSG_1011_1_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_2_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_3_IIS)
                                                                                                                      and obj1.t_AnaliticKind6 = obj.t_AnaliticKind6 
                                                                                                                      and obj1.t_Analitic6 = obj.t_Analitic6 
                                                                                                                      and obj1.t_OutSystCode = obj.t_OutSystCode 
                                                                                                                      and obj1.t_OutObjID = obj.t_OutObjID 
                                                                                                                      and (obj1.t_Technical = CHR(0) or obj1.t_Technical is null) 
                                                                                                                      and (obj1.t_FromOutSyst = CHR(0) or obj1.t_FromOutSyst is null) 
                                                                                                                  ),0) then 0 
                                  else 1 end)
                 GROUP BY obj.T_CLIENT, obj.T_DATE, knd.T_TAXBASEKIND, obj.T_ANALITICKIND2, obj.T_ANALITIC2
           )
      SELECT NVL(tb.ISSNOB, 0),
             tb.SNOB_CLIENT,
             tb.SNOB_TYPE,
             tb.SNOB_DESCR,
             tb.SNOB_INC_DATE,
             tb.SNOB_PERIOD,
             tb.SNOB_BASE,
             NVL(tb.SNOB_NOB, 0), 
             tb.SNOB_RATE, 
             tb.SNOB_CALC_TAX, 
             tb.SNOB_KBK, 
             tb.SNOB_HOLD_TAX,
             tb.SNOB_TORATE,
             NVL(ndr.ISNDR, 0),
             ndr.NDR_CLIENT,
             ndr.NDR_DATE, 
             ndr.NDR_TAXBASEKIND,
             NVL(ndr.NDR_SUM_PLUS_G, 0),
             NVL(ndr.NDR_SUM_MINUS_G, 0),
             (CASE WHEN ndr.NDR_KBK <= 0 THEN CHR(1) ELSE NVL((SELECT LLV.T_NAME FROM DLLVALUES_DBT LLV WHERE LLV.T_LIST = 3522 AND LLV.T_ELEMENT = ndr.NDR_KBK), CHR(1)) END)
        FROM tb FULL JOIN ndr ON ndr.NDR_DATE = tb.SNOB_INC_DATE AND ndr.NDR_TAXBASEKIND = tb.SNOB_BASE AND ndr.NDR_KBK <= 0;
    END LOOP;

    --Если в дату DatePaid было несколько событий с РАЗНЫМ описанием с TypeOper = 1, то необходимо определить какие НДР относятся к каждому событию (через связь события, операции расчета НОБ и НДР) 
    --и распределить сумма доходов и вычетов по каждому событию
    FOR one_rec IN (WITH objkind as (select k.t_Element, k.t_Code, 
                                            (case when k.t_TaxBaseKind = 9 then 6 
                                                  when k.t_TaxBaseKind = 3 then 2
                                                  when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1,RSI_NPTXC.TXOBJ_PLUSG_1011_2,RSI_NPTXC.TXOBJ_PLUSG_1011_3,RSI_NPTXC.TXOBJ_MINUSG_618) then 2
                                                  when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_2_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_3_IIS,RSI_NPTXC.TXOBJ_MINUSG_619) then 5
                                                  else k.t_TaxBaseKind end) as T_TAXBASEKIND 
                                      from dnptxkind_dbt k where k.t_Code like 'PlusG%' or k.t_Code like 'MinusG%')
                    SELECT NVL((SELECT SUM(OBJ.T_SUM0)
                                  FROM DNPTXTOTALBASE_DBT TB, DOPROPER_DBT OPR, DNPTXOBDC_DBT DC, DNPTXOBJ_DBT OBJ
                                 WHERE TB.T_CLIENTID    = TMP.T_SNOB_CLIENT
                                   AND TB.T_TAXBASEKIND = TMP.T_SNOB_BASE
                                   AND TB.T_TYPE        = TMP.T_SNOB_TYPE
                                   AND TB.T_INCDATE     = TMP.T_SNOB_INC_DATE
                                   AND OPR.T_ID_OPERATION = TB.T_ID_OPERATION
                                   AND DC.T_DOCID = TO_NUMBER(OPR.T_DOCUMENTID)
                                   AND OBJ.T_OBJID = DC.T_OBJID
                                   AND OBJ.T_DATE = TMP.T_SNOB_INC_DATE
                                   AND OBJ.T_KIND IN (select k.t_Element 
                                                        from objkind k 
                                                       where k.t_Code like 'PlusG%'
                                                         and 1 = (CASE WHEN p_ByCB = CNST.SET_CHAR AND (k.t_TaxBaseKind = 0 OR k.t_TaxBaseKind IN (RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK, RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_IIS)) THEN 1
                                                                       WHEN p_ByVS = CNST.SET_CHAR AND k.t_TaxBaseKind IN (RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_ONB) THEN 1
                                                                       ELSE 0 END 
                                                                 )
                                                     ) 
                                   and 1 = (case when obj.t_Kind = RSI_NPTXC.TXOBJ_PLUSG_1011 and obj.t_Sum0 = NVL((select /*+ index(obj1 dnptxobj_dbt_idxa)*/ SUM(obj1.t_Sum0) 
                                                                                                                           from dnptxobj_dbt obj1 
                                                                                                                          where obj1.t_Client = obj.t_Client 
                                                                                                                            and obj1.t_Date = obj.t_Date  
                                                                                                                            and obj1.t_Kind IN (RSI_NPTXC.TXOBJ_PLUSG_1011_1,RSI_NPTXC.TXOBJ_PLUSG_1011_2,RSI_NPTXC.TXOBJ_PLUSG_1011_3)
                                                                                                                            and obj1.t_AnaliticKind6 = obj.t_AnaliticKind6    
                                                                                                                            and obj1.t_Analitic6 = obj.t_Analitic6 
                                                                                                                            and obj1.t_OutSystCode = obj.t_OutSystCode 
                                                                                                                            and obj1.t_OutObjID = obj.t_OutObjID 
                                                                                                                            and (obj1.t_Technical = CHR(0) or obj1.t_Technical is null) 
                                                                                                                            and (obj1.t_FromOutSyst = CHR(0) or obj1.t_FromOutSyst is null) 
                                                                                                                        ),0) then 0 
                                            else 1 end) 
                                   and 1 = (case when obj.t_Kind = RSI_NPTXC.TXOBJ_PLUSG_1011_IIS and obj.t_Sum0 = NVL((select /*+ index(obj1 dnptxobj_dbt_idxa)*/ SUM(obj1.t_Sum0) 
                                                                                                                                    from dnptxobj_dbt obj1 
                                                                                                                                   where obj1.t_Client = obj.t_Client 
                                                                                                                                     and obj1.t_Date = obj.t_Date 
                                                                                                                                     and obj1.t_Kind IN (RSI_NPTXC.TXOBJ_PLUSG_1011_1_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_2_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_3_IIS)
                                                                                                                                     and obj1.t_AnaliticKind6 = obj.t_AnaliticKind6 
                                                                                                                                     and obj1.t_Analitic6 = obj.t_Analitic6 
                                                                                                                                     and obj1.t_OutSystCode = obj.t_OutSystCode 
                                                                                                                                     and obj1.t_OutObjID = obj.t_OutObjID 
                                                                                                                                     and (obj1.t_Technical = CHR(0) or obj1.t_Technical is null) 
                                                                                                                                     and (obj1.t_FromOutSyst = CHR(0) or obj1.t_FromOutSyst is null) 
                                                                                                                                 ),0) then 0 
                                                 else 1 end)
                               ), 0) as SUM_PLUS_G,
                           NVL((SELECT SUM(OBJ.T_SUM0)
                                 FROM DNPTXTOTALBASE_DBT TB, DOPROPER_DBT OPR, DNPTXOBDC_DBT DC, DNPTXOBJ_DBT OBJ
                                WHERE TB.T_CLIENTID    = TMP.T_SNOB_CLIENT
                                  AND TB.T_TAXBASEKIND = TMP.T_SNOB_BASE
                                  AND TB.T_TYPE        = TMP.T_SNOB_TYPE
                                  AND TB.T_INCDATE     = TMP.T_SNOB_INC_DATE
                                  AND OPR.T_ID_OPERATION = TB.T_ID_OPERATION
                                  AND DC.T_DOCID = TO_NUMBER(OPR.T_DOCUMENTID)
                                  AND OBJ.T_OBJID = DC.T_OBJID
                                  AND OBJ.T_DATE = TMP.T_SNOB_INC_DATE
                                  AND OBJ.T_KIND IN (select k.t_Element 
                                                       from objkind k 
                                                      where k.t_Code like 'MinusG%'
                                                        and 1 = (CASE WHEN p_ByCB = CNST.SET_CHAR AND (k.t_TaxBaseKind = 0 OR k.t_TaxBaseKind IN (RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK, RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_IIS)) THEN 1
                                                                      WHEN p_ByVS = CNST.SET_CHAR AND k.t_TaxBaseKind IN (RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_ONB) THEN 1
                                                                      ELSE 0 END 
                                                                )
                                                    ) 
                              ), 0) as SUM_MINUS_G,
                           TMP.*
                      FROM DNPTX6COLLECT_TMP TMP
                     WHERE TMP.T_ISSNOB = 1
                       AND TMP.T_SNOB_TYPE = 1
                       AND EXISTS(SELECT 1
                                    FROM DNPTX6COLLECT_TMP TMP1
                                   WHERE TMP1.T_SNOB_CLIENT   = TMP.T_SNOB_CLIENT
                                     AND TMP1.T_SNOB_BASE     = TMP.T_SNOB_BASE
                                     AND TMP1.T_SNOB_INC_DATE = TMP.T_SNOB_INC_DATE
                                     AND TMP1.T_SNOB_RATE     = TMP.T_SNOB_RATE
                                     AND TMP1.T_SNOB_TYPE     = TMP.T_SNOB_TYPE
                                     AND TMP1.T_SNOB_DESCR   <> TMP.T_SNOB_DESCR
                                 )
                   )
    LOOP

      UPDATE DNPTX6COLLECT_TMP
         SET T_NDR_SUM_PLUS_G = one_rec.SUM_PLUS_G,
             T_NDR_SUM_MINUS_G = one_rec.SUM_MINUS_G
       WHERE T_ISSNOB        = one_rec.T_ISSNOB      
         AND T_SNOB_TYPE     = one_rec.T_SNOB_TYPE
         AND T_SNOB_CLIENT   = one_rec.T_SNOB_CLIENT  
         AND T_SNOB_BASE     = one_rec.T_SNOB_BASE    
         AND T_SNOB_INC_DATE = one_rec.T_SNOB_INC_DATE
         AND T_SNOB_RATE     = one_rec.T_SNOB_RATE    
         AND T_SNOB_TYPE     = one_rec.T_SNOB_TYPE    
         AND T_SNOB_DESCR    = one_rec.T_SNOB_DESCR;   

    END LOOP;                   

    
    FOR one_rec IN (SELECT Q.*
                      FROM (SELECT TMP.T_ISSNOB,         
                                   TMP.T_SNOB_CLIENT,    
                                   TMP.T_SNOB_TYPE,      
                                   TMP.T_SNOB_DESCR,     
                                   TMP.T_SNOB_INC_DATE,  
                                   TMP.T_SNOB_PERIOD,    
                                   TMP.T_SNOB_BASE,      
                                   TMP.T_SNOB_NOB,       
                                   TMP.T_SNOB_RATE,      
                                   TMP.T_SNOB_CALC_TAX,  
                                   TMP.T_SNOB_KBK,       
                                   TMP.T_SNOB_HOLD_TAX,  
                                   TMP.T_SNOB_TORATE,    
                                   TMP.T_ISNDR,          
                                   TMP.T_NDR_CLIENT,     
                                   TMP.T_NDR_DATE,       
                                   TMP.T_NDR_TAXBASEKIND,
                                   TMP.T_NDR_SUM_PLUS_G,
                                   TMP.T_NDR_SUM_MINUS_G,
                                   TMP.T_NDR_KBK,
                                   NVL((SELECT SUM(TMP1.T_SNOB_NOB)
                                          FROM DNPTX6COLLECT_TMP TMP1
                                         WHERE TMP1.T_ISSNOB = 1
                                           AND TMP1.T_SNOB_CLIENT = TMP.T_SNOB_CLIENT
                                           AND TMP1.T_SNOB_BASE IN (RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK, RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_IIS) 
                                           AND TMP1.T_SNOB_INC_DATE BETWEEN p_BegDate AND p_EndDate
                                           AND TMP1.T_SNOB_RATE = 13
                                       ), 0) AS SNOB_13,
                                   NVL((SELECT SUM(TMP1.T_SNOB_NOB)
                                          FROM DNPTX6COLLECT_TMP TMP1
                                         WHERE TMP1.T_ISSNOB = 1
                                           AND TMP1.T_SNOB_CLIENT = TMP.T_SNOB_CLIENT
                                           AND TMP1.T_SNOB_BASE IN (RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK, RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_IIS) 
                                           AND TMP1.T_SNOB_INC_DATE BETWEEN p_BegDate AND p_EndDate
                                           AND TMP1.T_SNOB_RATE = 15
                                       ), 0) AS SNOB_15
                              FROM DNPTX6COLLECT_TMP TMP
                             UNION ALL
                             SELECT TMP.T_ISSNOB,         
                                    TMP.T_SNOB_CLIENT,    
                                    TMP.T_SNOB_TYPE,      
                                    TMP.T_SNOB_DESCR,     
                                    TMP.T_SNOB_INC_DATE,  
                                    TMP.T_SNOB_PERIOD,    
                                    TMP.T_SNOB_BASE,      
                                    TMP.T_SNOB_NOB,       
                                    TMP.T_SNOB_TORATE AS T_SNOB_RATE,      
                                    TMP.T_SNOB_CALC_TAX,  
                                    '18210102180011000110' AS T_SNOB_KBK,       
                                    0 AS T_SNOB_HOLD_TAX,  
                                    TMP.T_SNOB_TORATE,    
                                    TMP.T_ISNDR,          
                                    TMP.T_NDR_CLIENT,     
                                    TMP.T_NDR_DATE,       
                                    TMP.T_NDR_TAXBASEKIND,
                                    TMP.T_NDR_SUM_PLUS_G,
                                    TMP.T_NDR_SUM_MINUS_G,
                                    TMP.T_NDR_KBK,
                                    NVL((SELECT SUM(TMP1.T_SNOB_NOB)
                                           FROM DNPTX6COLLECT_TMP TMP1
                                          WHERE TMP1.T_ISSNOB = 1
                                            AND TMP1.T_SNOB_CLIENT = TMP.T_SNOB_CLIENT
                                            AND TMP1.T_SNOB_BASE IN (RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK, RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_IIS) 
                                            AND TMP1.T_SNOB_INC_DATE BETWEEN p_BegDate AND p_EndDate
                                            AND TMP1.T_SNOB_RATE = 13
                                        ), 0) AS SNOB_13,
                                    NVL((SELECT SUM(TMP1.T_SNOB_NOB)
                                           FROM DNPTX6COLLECT_TMP TMP1
                                          WHERE TMP1.T_ISSNOB = 1
                                            AND TMP1.T_SNOB_CLIENT = TMP.T_SNOB_CLIENT
                                            AND TMP1.T_SNOB_BASE IN (RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK, RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_IIS) 
                                            AND TMP1.T_SNOB_INC_DATE BETWEEN p_BegDate AND p_EndDate
                                            AND TMP1.T_SNOB_RATE = 15
                                        ), 0) AS SNOB_15
                               FROM DNPTX6COLLECT_TMP TMP
                              WHERE TMP.T_SNOB_TORATE > T_SNOB_RATE
                                AND NOT EXISTS(SELECT 1
                                                 FROM DNPTXTOTALBASE_DBT STB1
                                                WHERE STB1.T_CLIENTID = TMP.T_SNOB_CLIENT
                                                  AND STB1.T_INCDATE = TMP.T_SNOB_INC_DATE
                                                  AND STB1.T_STORSTATE = 1
                                                  AND STB1.T_RATEHOLDPITAX = TMP.T_SNOB_TORATE
                                                  AND STB1.T_TAXBASEKIND = TMP.T_SNOB_BASE
                                              )
                          ) Q
                     ORDER BY (CASE WHEN Q.T_ISSNOB = 1 THEN Q.T_SNOB_CLIENT ELSE Q.T_NDR_CLIENT END) ASC,
                              (CASE WHEN Q.T_ISSNOB = 1 THEN Q.T_SNOB_INC_DATE ELSE Q.T_NDR_DATE END) ASC,
                              (CASE WHEN Q.T_ISSNOB = 1 THEN Q.T_SNOB_RATE ELSE 100 END) ASC
                   )
    LOOP
      nptx6dec.t_ID               := 0;
      nptx6dec.t_TypeOper         := CHR(1);
      nptx6dec.t_ClientID         := -1;
      nptx6dec.t_TaxBaseType      := 0;
      nptx6dec.t_TaxPeriod        := 0;
      nptx6dec.t_DatePaid         := TO_DATE('01.01.0001','DD.MM.YYYY');
      nptx6dec.t_TotalPlus        := 0;
      nptx6dec.t_TotalMinusNormal := 0;
      nptx6dec.t_TotalMinusReal   := 0;
      nptx6dec.t_TotalMinusOver   := 0;
      nptx6dec.t_TaxBaseSum       := 0;
      nptx6dec.t_Rate             := 0;
      nptx6dec.t_TaxCalculated    := 0;
      nptx6dec.t_KBK              := CHR(1);
      nptx6dec.t_TaxPaid          := 0;
      nptx6dec.t_TypeSNOB         := 0;
      nptx6dec.t_RecType          := RECTYPE_NORMAL;

      IF one_rec.T_NDR_KBK <> CHR(1) THEN
        nptx6dec.t_RecType := RECTYPE_NDR_CHANGECODES;
      END IF;

      --TypeOper
      IF one_rec.T_ISSNOB = 1 THEN --Если строка формируется по данным массива СНОБ (в т.ч. если для данной записи есть данные из массива НДР), то заполняется значением поля Описание события T_DESCRIPTION из отобранной записи
        nptx6dec.t_TypeOper := one_rec.T_SNOB_DESCR;
      ELSIF one_rec.T_ISNDR = 1 THEN --Если строка формируется только по данным массива НДР, то всегда выводится текст "Вывод д/с. НДР"
        IF nptx6dec.t_RecType = RECTYPE_NDR_CHANGECODES THEN
          nptx6dec.t_TypeOper := 'Вывод д/с. НДР. Замена кодов.';
        ELSE
          nptx6dec.t_TypeOper := 'Вывод д/с. НДР';
        END IF;
      END IF;

      --ClientID
      IF one_rec.T_ISSNOB = 1 THEN
        nptx6dec.t_ClientID := one_rec.T_SNOB_CLIENT;
      ELSE
        nptx6dec.t_ClientID := one_rec.T_NDR_CLIENT;
      END IF;

      v_IsResident := RSI_NPTX.STB_IsResidentStatus(nptx6dec.t_ClientID, p_EndDate);

      --TaxBaseType
      IF one_rec.T_ISSNOB = 1 THEN --Если строка формируется по данным массива СНОБ (в т.ч. если для данной записи есть данные из массива НДР), то заполняется значением поля Тип НОБ T_TAXBASEKIND из отобранной записи.
        nptx6dec.t_TaxBaseType := one_rec.T_SNOB_BASE;
      ELSE --Если строка формируется только по данным массива НДР, то тип НБ, который указан на НДР в справочнике Виды объектов НДР (поле T_TAXBASEKIND)
        nptx6dec.t_TaxBaseType := one_rec.T_NDR_TAXBASEKIND;
      END IF;

      --TaxPeriod
      IF one_rec.T_ISSNOB = 1 THEN --Если строка формируется по данным массива СНОБ (в т.ч. если для данной записи есть данные из массива НДР), то заполняется значением поля Налоговый период T_TAXPERIOD из отобранной записи 
        nptx6dec.t_TaxPeriod := one_rec.T_SNOB_PERIOD;
      ELSE --Если строка формируется только по данным массива НДР, то выводится Налоговый период T_TAXPERIOD, если не заполнен, то ГГГГ из даты НДР
        --В отобранны НДР нет налогового периода, т.к. он только для 8-го уровня заполняется. Поэтому просто берем год из даты
        nptx6dec.t_TaxPeriod := EXTRACT(YEAR FROM one_rec.T_NDR_DATE);
      END IF;

      --DatePaid
      IF one_rec.T_ISSNOB = 1 THEN --Если строка формируется по данным массива СНОБ (в т.ч. если для данной записи есть данные из массива НДР), то заполняется значением поля Дата получения дохода T_INCREGIONDATE из отобранной записи (= дата НДР T_DATE)
        nptx6dec.t_DatePaid := one_rec.T_SNOB_INC_DATE;
      ELSE --Если строка формируется только по данным массива НДР, то выводится дата НДР T_DATE
        nptx6dec.t_DatePaid := one_rec.T_NDR_DATE;
      END IF;

      BEGIN
        SELECT LK.T_TYPESNOB
          INTO nptx6dec.t_TypeSNOB
          FROM DNPTXLINKSKINDSNOB_DBT LK
         WHERE LK.T_TAXBASETYPE = nptx6dec.t_TaxBaseType
           AND LK.T_BEGDATETYPESNOB <= nptx6dec.t_DatePaid
           AND LK.T_ENDDATETYPESNOB >= nptx6dec.t_DatePaid
           AND ROWNUM = 1;

        EXCEPTION
           WHEN OTHERS THEN nptx6dec.t_TypeSNOB := 0;
      END;

      --TotalPlus
      IF (one_rec.T_ISSNOB = 1 AND one_rec.T_ISNDR <> 1) OR (one_rec.T_SNOB_TYPE <> 1) THEN --Если строка формируется только по данным массива СНОБ (нет НДР в эту дату или с таким видов НБ), либо TypeOper события <> 1, то выводится 0
        nptx6dec.t_TotalPlus := 0;
        nptx6dec.t_TotalMinusReal := 0;
      ELSIF one_rec.T_ISSNOB = 1 THEN --Если строка формируется по данным массива СНОБ, и при этом есть данные из массива НДР за эту дату и с таким видом НБ,  то T_TOTALPLUS =  сумма НДР  5-го уровня вида PlusG%
        IF nptx6dec.t_TaxBaseType = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_ONB THEN
          --Вычислим ранее учтенные суммы
          SELECT NVL(SUM(T_TOTALPLUS), 0)
            INTO v_RecTotalPlus
            FROM DNPTX6DECODING_TMP
           WHERE T_CLIENTID = nptx6dec.t_ClientID
             AND T_DATEPAID = nptx6dec.t_DatePaid
             AND T_RATE <= one_rec.T_SNOB_RATE
             AND T_TYPESNOB = nptx6dec.t_TypeSNOB
             AND T_RECTYPE <> RECTYPE_NDR_CHANGECODES;

          nptx6dec.t_TotalPlus := LEAST(one_rec.T_NDR_SUM_PLUS_G-v_RecTotalPlus, one_rec.T_SNOB_NOB);

        ELSE 
          IF one_rec.T_SNOB_TORATE > one_rec.T_SNOB_RATE THEN
            nptx6dec.t_TotalPlus := 0;
            nptx6dec.t_TotalMinusReal := 0;
          ELSE
          
            IF one_rec.SNOB_15 = 0 THEN
              IF one_rec.T_SNOB_RATE = 15 THEN
                 --Вычислим ранее учтенные суммы
                SELECT NVL(SUM(T_TOTALPLUS), 0), NVL(SUM(T_TOTALMINUSREAL), 0)
                  INTO v_RecTotalPlus13, v_RecTotalMinusReal13
                  FROM DNPTX6DECODING_TMP
                 WHERE T_CLIENTID = nptx6dec.t_ClientID
                   AND T_DATEPAID = nptx6dec.t_DatePaid
                   AND T_RATE = 13
                   AND T_TYPESNOB = nptx6dec.t_TypeSNOB
                   AND T_RECTYPE <> RECTYPE_NDR_CHANGECODES;

                nptx6dec.t_TotalPlus := one_rec.T_NDR_SUM_PLUS_G - v_RecTotalPlus13;
                nptx6dec.t_TotalMinusReal := one_rec.T_NDR_SUM_MINUS_G - v_RecTotalMinusReal13;
              ELSE
                nptx6dec.t_TotalPlus := one_rec.T_NDR_SUM_PLUS_G;
                nptx6dec.t_TotalMinusReal := one_rec.T_NDR_SUM_MINUS_G;
              END IF;
            ELSE
              IF one_rec.T_SNOB_RATE = 13 THEN
                
                --Вычислим ранее учтенную сумму НОБ
                SELECT NVL(SUM(T_TOTALPLUS-T_TOTALMINUSREAL), 0)
                  INTO v_PrevTotalNOB
                  FROM DNPTX6DECODING_TMP
                 WHERE T_CLIENTID = nptx6dec.t_ClientID
                   AND T_TYPESNOB = nptx6dec.t_TypeSNOB
                   AND T_RATE = one_rec.T_SNOB_RATE
                   AND T_RECTYPE <> RECTYPE_NDR_CHANGECODES;

                v_NewTotalNOB := v_PrevTotalNOB + (one_rec.T_NDR_SUM_PLUS_G - one_rec.T_NDR_SUM_MINUS_G);
                IF v_NewTotalNOB >= one_rec.SNOB_13 THEN
                  v_RecNOB := one_rec.SNOB_13 - v_PrevTotalNOB;
                  
                  nptx6dec.t_TotalPlus := LEAST(one_rec.T_NDR_SUM_PLUS_G, one_rec.T_NDR_SUM_MINUS_G + v_RecNOB);
                ELSE
                  nptx6dec.t_TotalPlus := one_rec.T_NDR_SUM_PLUS_G;
                END IF;

                
                nptx6dec.t_TotalMinusReal := one_rec.T_NDR_SUM_MINUS_G;
              ELSIF one_rec.T_SNOB_RATE = 15 THEN

                --Вычислим ранее учтенные суммы
                SELECT NVL(SUM(T_TOTALPLUS), 0), NVL(SUM(T_TOTALMINUSREAL), 0)
                  INTO v_RecTotalPlus13, v_RecTotalMinusReal13
                  FROM DNPTX6DECODING_TMP
                 WHERE T_CLIENTID = nptx6dec.t_ClientID
                   AND T_DATEPAID = nptx6dec.t_DatePaid
                   AND T_RATE = 13
                   AND T_TYPESNOB = nptx6dec.t_TypeSNOB
                   AND T_RECTYPE <> RECTYPE_NDR_CHANGECODES;

                nptx6dec.t_TotalPlus := one_rec.T_NDR_SUM_PLUS_G - v_RecTotalPlus13;
                nptx6dec.t_TotalMinusReal := one_rec.T_NDR_SUM_MINUS_G - v_RecTotalMinusReal13;
              ELSE
                nptx6dec.t_TotalPlus := one_rec.T_NDR_SUM_PLUS_G;
                nptx6dec.t_TotalMinusReal := one_rec.T_NDR_SUM_MINUS_G;
              END IF;
            END IF;
          END IF;
        END IF;
      ELSE --Если строка формируется только по данным массива НДР, то T_TOTALPLUS = сумма НДР  5-го уровня вида PlusG с одинаковой датой T_DATEPAID и одинаковым видом НБ
        nptx6dec.t_TotalPlus := one_rec.T_NDR_SUM_PLUS_G;
        nptx6dec.t_TotalMinusReal := one_rec.T_NDR_SUM_MINUS_G;
      END IF;

      --TotalMinusReal
      IF nptx6dec.t_RecType = RECTYPE_NDR_CHANGECODES THEN
        nptx6dec.t_TotalMinusNormal := nptx6dec.t_TotalMinusReal;
      ELSE
        IF nptx6dec.t_TotalMinusReal > nptx6dec.t_TotalPlus THEN
          nptx6dec.t_TotalMinusNormal := nptx6dec.t_TotalPlus;
        ELSE
          nptx6dec.t_TotalMinusNormal := nptx6dec.t_TotalMinusReal;
        END IF;
      END IF;

      --TaxBaseSum
      IF one_rec.T_ISSNOB = 1 THEN
        nptx6dec.t_TaxBaseSum := one_rec.T_SNOB_NOB;
      END IF;

      --Rate
      IF one_rec.T_ISSNOB = 1 THEN
        nptx6dec.t_Rate := one_rec.T_SNOB_RATE;
      ELSE
        IF nptx6dec.t_RecType = RECTYPE_NDR_CHANGECODES THEN
          nptx6dec.t_Rate := 13;
          IF v_IsResident = 0 THEN
            nptx6dec.t_Rate := 30;
          END IF;
        ELSE
          SELECT NVL(MAX(T_RATE), 0)
            INTO nptx6dec.t_Rate
            FROM DNPTX6DECODING_TMP
           WHERE T_CLIENTID = nptx6dec.t_ClientID
             AND T_TAXBASETYPE IN (RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK, RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_IIS)
             AND T_TYPESNOB = nptx6dec.t_TypeSNOB
             AND T_TAXBASESUM > 0;

          IF nptx6dec.t_Rate = 0 THEN
            nptx6dec.t_Rate := 13;
            IF v_IsResident = 0 THEN
              nptx6dec.t_Rate := 30;
            END IF;

          END IF;
        END IF;
      END IF;

      --TaxCalculated
      IF one_rec.T_ISSNOB = 1 THEN
        nptx6dec.t_TaxCalculated := one_rec.T_SNOB_CALC_TAX;
      END IF;

      --KBK
      IF one_rec.T_ISSNOB = 1 THEN
        nptx6dec.t_KBK := one_rec.T_SNOB_KBK;
      ELSE
        IF nptx6dec.t_RecType = RECTYPE_NDR_CHANGECODES THEN
          nptx6dec.t_KBK := one_rec.T_NDR_KBK;
        ELSE
          BEGIN
            SELECT llv.t_Name
              INTO nptx6dec.t_KBK
              FROM DNPTXSNOBLIMIT_DBT nptxlim, DLLVALUES_DBT llv
             WHERE nptxlim.t_TypeSNOB = 3 /*СНОБ2*/
               AND nptxlim.t_TaxRate = nptx6dec.t_Rate
               AND nptxlim.t_IsResident = (CASE WHEN v_IsResident = 1 THEN 'X' ELSE CHR(0) END)
               AND llv.t_List = 3522
               AND llv.t_Code = TO_CHAR(nptxlim.t_CodeKBK);

            EXCEPTION
             WHEN OTHERS THEN nptx6dec.t_KBK := CHR(1);
          END;
        END IF;
      END IF;

      --TaxPaid
      IF one_rec.T_ISSNOB = 1 AND one_rec.T_SNOB_PERIOD = EXTRACT(YEAR FROM p_BegDate) THEN
        nptx6dec.t_TaxPaid := one_rec.T_SNOB_HOLD_TAX;
      END IF;


      INSERT INTO DNPTX6DECODING_TMP VALUES nptx6dec;

    END LOOP;

    --Перераспределить вычеты, которые не соответствуют коду дохода в дате
    --Отбираем все объекты НДР MinusG, для которых нет в той же дате соттветствия PlusG по таблице соответствий
    FOR one_minus_obj IN (with objkind as (select k.t_Element, k.t_Code, 
                                                  (case when k.t_TaxBaseKind = 9 then 6 
                                                        when k.t_TaxBaseKind = 3 then 2
                                                        when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1,RSI_NPTXC.TXOBJ_PLUSG_1011_2,RSI_NPTXC.TXOBJ_PLUSG_1011_3,RSI_NPTXC.TXOBJ_MINUSG_618) then 2
                                                        when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_2_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_3_IIS,RSI_NPTXC.TXOBJ_MINUSG_619) then 5
                                                        else k.t_TaxBaseKind end) as T_TAXBASEKIND 
                                            from dnptxkind_dbt k where k.t_Code like 'PlusG%' or k.t_Code like 'MinusG%')
                          SELECT DISTINCT MINUS_OBJ.T_OBJID, MINUS_OBJ.T_SUM0, MINUS_OBJ.T_DATE, MINUS_OBJ.T_CLIENT, MINUS_OBJ.T_KIND, minus_knd.T_TAXBASEKIND
                            FROM DNPTX6DECODING_TMP TMP, DNPTXOBJ_DBT MINUS_OBJ, objkind minus_knd
                           WHERE TMP.T_RECTYPE <> RECTYPE_NDR_CHANGECODES
                             AND MINUS_OBJ.T_CLIENT = TMP.T_CLIENTID
                             AND MINUS_OBJ.T_DATE = TMP.T_DATEPAID
                             AND MINUS_OBJ.T_KIND = minus_knd.T_ELEMENT
                             AND minus_knd.T_TAXBASEKIND = TMP.T_TAXBASETYPE
                             AND minus_knd.T_CODE LIKE 'MinusG%'
                             AND NOT EXISTS(SELECT 1
                                              FROM DNPTXOBJ_DBT PLUS_OBJ, objkind plus_knd, dnptxmatchinout_dbt mio
                                             WHERE PLUS_OBJ.T_CLIENT = TMP.T_CLIENTID
                                               AND PLUS_OBJ.T_DATE = TMP.T_DATEPAID
                                               AND PLUS_OBJ.T_KIND = plus_knd.T_ELEMENT
                                               AND plus_knd.T_TAXBASEKIND = TMP.T_TAXBASETYPE
                                               AND mio.T_KIND_INCOME = PLUS_OBJ.T_KIND
                                               AND mio.T_KIND_OUTCOME = MINUS_OBJ.T_KIND
                                               AND mio.T_DATEBEGIN <= PLUS_OBJ.T_DATE
                                               AND mio.T_DATEEND >= PLUS_OBJ.T_DATE
                                           )
                         )
    LOOP

      v_RestMinusObjSum := one_minus_obj.T_SUM0;

      --ищем ближайшую дату (Т-n) по принципу снизу-вверх, где имеется соответствующий код дохода, после чего  
      --уменьшаем сумму вычетов (нормированная) в дате Т
      --увеличиваем сумму вычетов (нормированная) в дате Т-n
      FOR one_plus IN (with objkind as (select k.t_Element, k.t_Code, 
                                              (case when k.t_TaxBaseKind = 9 then 6 
                                                    when k.t_TaxBaseKind = 3 then 2
                                                    when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1,RSI_NPTXC.TXOBJ_PLUSG_1011_2,RSI_NPTXC.TXOBJ_PLUSG_1011_3,RSI_NPTXC.TXOBJ_MINUSG_618) then 2
                                                    when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_2_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_3_IIS,RSI_NPTXC.TXOBJ_MINUSG_619) then 5
                                                    else k.t_TaxBaseKind end) as T_TAXBASEKIND 
                                        from dnptxkind_dbt k where k.t_Code like 'PlusG%' or k.t_Code like 'MinusG%')
                      SELECT SUM(q_plus_obj.T_SUM0) as PlusObjSum, q_plus_obj.T_CLIENT, q_plus_obj.T_DATE, q_plus_obj.T_KIND, q_plus_obj.T_TAXBASEKIND,
                             NVL((SELECT SUM(MINUS_OBJ.T_SUM0)
                                    FROM DNPTXOBJ_DBT MINUS_OBJ, objkind minus_knd
                                   WHERE MINUS_OBJ.T_CLIENT = q_plus_obj.T_CLIENT
                                     AND MINUS_OBJ.T_DATE = q_plus_obj.T_DATE
                                     AND MINUS_OBJ.T_KIND = minus_knd.T_ELEMENT
                                     AND minus_knd.T_TAXBASEKIND = q_plus_obj.T_TAXBASEKIND
                                     AND minus_knd.T_CODE LIKE 'MinusG%'
                                     AND EXISTS(SELECT 1
                                                  FROM dnptxmatchinout_dbt mio1
                                                 WHERE mio1.T_KIND_INCOME = q_plus_obj.T_KIND
                                                   AND mio1.T_KIND_OUTCOME = MINUS_OBJ.T_KIND
                                                   AND mio1.T_DATEBEGIN <= q_plus_obj.T_DATE
                                                   AND mio1.T_DATEEND >= q_plus_obj.T_DATE
                                               )
                                 ), 0) AS AnotherCodesMinusSum
                        FROM (SELECT DISTINCT PLUS_OBJ.*, plus_knd.T_TAXBASEKIND
                                FROM DNPTX6DECODING_TMP TMP, DNPTXOBJ_DBT PLUS_OBJ, objkind plus_knd, dnptxmatchinout_dbt mio
                               WHERE TMP.T_CLIENTID = one_minus_obj.T_CLIENT
                                 AND TMP.T_DATEPAID <> one_minus_obj.T_DATE
                                 AND TMP.T_TAXBASETYPE = one_minus_obj.T_TAXBASEKIND
                                 AND TMP.T_RECTYPE <> RECTYPE_NDR_CHANGECODES
                                 AND PLUS_OBJ.T_CLIENT = TMP.T_CLIENTID
                                 AND PLUS_OBJ.T_DATE = TMP.T_DATEPAID
                                 AND PLUS_OBJ.T_KIND = plus_knd.T_ELEMENT
                                 AND plus_knd.T_TAXBASEKIND = TMP.T_TAXBASETYPE
                                 AND plus_knd.T_CODE LIKE 'PlusG%'
                                 AND mio.T_KIND_INCOME = PLUS_OBJ.T_KIND
                                 AND mio.T_KIND_OUTCOME = one_minus_obj.T_KIND
                                 AND mio.T_DATEBEGIN <= PLUS_OBJ.T_DATE
                                 AND mio.T_DATEEND >= PLUS_OBJ.T_DATE
                             ) q_plus_obj
                       GROUP BY q_plus_obj.T_CLIENT, q_plus_obj.T_DATE, q_plus_obj.T_KIND, q_plus_obj.T_TAXBASEKIND
                       ORDER BY q_plus_obj.T_CLIENT, (CASE WHEN q_plus_obj.T_DATE < one_minus_obj.T_DATE THEN 1 ELSE 2 END) ASC, q_plus_obj.T_DATE DESC  
                     )
      LOOP
        IF one_plus.PlusObjSum > one_plus.AnotherCodesMinusSum THEN
          --Если сумма НДР по подходящему коду дохода больше чем сумма других подходящих вычетов по этому коду дохода, то можем максимально зачесть разницу
          v_MaxAddMinusSum := LEAST((one_plus.PlusObjSum - one_plus.AnotherCodesMinusSum), v_RestMinusObjSum);

          --Так как в одну дату может быть несколько записей по разным ставкам, но начинаем учитывать с меньшей ставки
          FOR one_upd IN (SELECT TMP.*
                            FROM DNPTX6DECODING_TMP TMP
                           WHERE TMP.T_CLIENTID = one_plus.T_CLIENT
                             AND TMP.T_DATEPAID = one_plus.T_DATE
                             AND TMP.T_TAXBASETYPE = one_plus.T_TAXBASEKIND
                             AND TMP.T_RECTYPE <> RECTYPE_NDR_CHANGECODES
                           ORDER BY TMP.T_RATE ASC
                         )
          LOOP

            --Добавить в нормированный расход можем только до TotalPlus
            v_AddMinusSum := LEAST((one_upd.T_TOTALPLUS - one_upd.T_TOTALMINUSNORMAL), v_MaxAddMinusSum);

            --Увеличиваем нормированную сумму
            UPDATE DNPTX6DECODING_TMP
               SET T_TOTALMINUSNORMAL = T_TOTALMINUSNORMAL + v_AddMinusSum
             WHERE T_ID = one_upd.T_ID;

            --Уменьшить сумму нормированных вычетов в дате one_minus_obj.T_DATE
            v_RestWrtMinusSum := v_AddMinusSum;
            FOR one_minus_upd IN (SELECT TMP.*
                                    FROM DNPTX6DECODING_TMP TMP
                                   WHERE TMP.T_CLIENTID = one_minus_obj.T_CLIENT
                                     AND TMP.T_DATEPAID = one_minus_obj.T_DATE
                                     AND TMP.T_TAXBASETYPE = one_minus_obj.T_TAXBASEKIND
                                     AND TMP.T_RECTYPE <> RECTYPE_NDR_CHANGECODES
                                   ORDER BY TMP.T_RATE DESC
                                 )
            LOOP
              v_WrtMinusSum := LEAST(one_minus_upd.T_TOTALMINUSNORMAL, v_RestWrtMinusSum);

              UPDATE DNPTX6DECODING_TMP
                 SET T_TOTALMINUSNORMAL = T_TOTALMINUSNORMAL - v_WrtMinusSum
               WHERE T_ID = one_minus_upd.T_ID;

              v_RestWrtMinusSum := v_RestWrtMinusSum - v_WrtMinusSum;

              IF v_RestWrtMinusSum = 0 THEN
                EXIT;
              END IF;
            END LOOP;


            v_MaxAddMinusSum := v_MaxAddMinusSum - v_AddMinusSum;
            v_RestMinusObjSum := v_RestMinusObjSum - v_AddMinusSum;

            IF v_MaxAddMinusSum = 0 THEN
              EXIT;
            END IF;

          END LOOP;

          IF v_RestMinusObjSum = 0 THEN
            EXIT;
          END IF;

        END IF;

      END LOOP;

    END LOOP;

    --Перераспределяем отрицательные вычеты
 /*   FOR one_rec IN (SELECT *
                      FROM DNPTX6DECODING_TMP
                     WHERE T_TOTALMINUSREAL < 0
                     ORDER BY T_TOTALMINUSREAL DESC, T_ID ASC
                   )
    LOOP

      v_RestTotalMinusReal := ABS(one_rec.T_TOTALMINUSREAL);

      FOR one_upd IN (SELECT TMP.*
                        FROM DNPTX6DECODING_TMP TMP
                       WHERE TMP.T_CLIENTID = one_rec.T_CLIENTID
                         AND TMP.T_TOTALMINUSREAL > 0
                         AND TMP.T_TYPESNOB = one_rec.T_TYPESNOB
                       ORDER BY (CASE WHEN TMP.T_ID < one_rec.T_ID THEN 1 ELSE 2 END) ASC, TMP.T_ID DESC
                     )
      LOOP

        v_NewTotalMinusReal := GREATEST(one_upd.t_TotalMinusReal - v_RestTotalMinusReal, 0);
        v_WrtTotalMinusReal := one_upd.t_TotalMinusReal - v_NewTotalMinusReal;

        UPDATE DNPTX6DECODING_TMP
           SET T_TOTALMINUSREAL = v_NewTotalMinusReal,
               T_TOTALMINUSNORMAL = (CASE WHEN v_NewTotalMinusReal > one_upd.t_TotalPlus THEN one_upd.t_TotalPlus ELSE v_NewTotalMinusReal END)
         WHERE T_ID = one_upd.T_ID;


        v_RestTotalMinusReal := v_RestTotalMinusReal - v_WrtTotalMinusReal;

        IF v_RestTotalMinusReal = 0 THEN
          EXIT;
        END IF;
      END LOOP;

      UPDATE DNPTX6DECODING_TMP
         SET T_TOTALMINUSREAL = v_RestTotalMinusReal,
             T_TOTALMINUSNORMAL = (CASE WHEN v_RestTotalMinusReal > one_rec.t_TotalPlus THEN one_rec.t_TotalPlus ELSE v_RestTotalMinusReal END)
       WHERE T_ID = one_rec.T_ID;

    END LOOP; */

    UPDATE DNPTX6DECODING_TMP
       SET T_TOTALMINUSOVER = T_TOTALMINUSNORMAL - T_TOTALMINUSREAL
     WHERE T_RECTYPE <> RECTYPE_NDR_CHANGECODES;

    --После обработки всех записей рассчитываем нормированные расходы, если по записям клиента есть превышение суммы расходов
    FOR one_rec IN (SELECT *
                      FROM DNPTX6DECODING_TMP
                     WHERE T_TOTALMINUSOVER < 0
                       AND T_RECTYPE <> RECTYPE_NDR_CHANGECODES
                     ORDER BY T_ID ASC
                   )
    LOOP
      
      v_RestTotalMinusOver := ABS(one_rec.T_TOTALMINUSOVER);

      --Отбираем все доходы, по которым есть превышения вычетов по таблице соответствия
      FOR one_plus_kind IN (with objkind as (select k.t_Element, k.t_Code, 
                                              (case when k.t_TaxBaseKind = 9 then 6 
                                                    when k.t_TaxBaseKind = 3 then 2
                                                    when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1,RSI_NPTXC.TXOBJ_PLUSG_1011_2,RSI_NPTXC.TXOBJ_PLUSG_1011_3,RSI_NPTXC.TXOBJ_MINUSG_618) then 2
                                                    when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_2_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_3_IIS,RSI_NPTXC.TXOBJ_MINUSG_619) then 5
                                                    else k.t_TaxBaseKind end) as T_TAXBASEKIND 
                                        from dnptxkind_dbt k where k.t_Code like 'PlusG%' or k.t_Code like 'MinusG%')
                        SELECT q.*
                          FROM (SELECT NVL(SUM(PLUS_OBJ.T_SUM0), 0) AS SumPlusObj,
                                       NVL((SELECT SUM(MINUS_OBJ.T_SUM0)
                                              FROM DNPTXOBJ_DBT MINUS_OBJ, objkind minus_knd
                                             WHERE MINUS_OBJ.T_CLIENT = one_rec.T_CLIENTID
                                               AND MINUS_OBJ.T_DATE = one_rec.T_DATEPAID
                                               AND MINUS_OBJ.T_KIND = minus_knd.T_ELEMENT
                                               AND minus_knd.T_TAXBASEKIND = one_rec.T_TAXBASETYPE
                                               AND minus_knd.T_CODE LIKE 'MinusG%'
                                               AND EXISTS(SELECT 1
                                                            FROM dnptxmatchinout_dbt mio1
                                                           WHERE mio1.T_KIND_INCOME = PLUS_OBJ.T_KIND
                                                             AND mio1.T_KIND_OUTCOME = MINUS_OBJ.T_KIND
                                                             AND mio1.T_DATEBEGIN <= one_rec.T_DATEPAID
                                                             AND mio1.T_DATEEND >= one_rec.T_DATEPAID
                                                         )
                                           ), 0) SumMinusObj,
                                       PLUS_OBJ.T_KIND
                                  FROM DNPTXOBJ_DBT PLUS_OBJ, objkind plus_knd
                                 WHERE PLUS_OBJ.T_CLIENT = one_rec.T_CLIENTID
                                   AND PLUS_OBJ.T_DATE = one_rec.T_DATEPAID
                                   AND PLUS_OBJ.T_KIND = plus_knd.T_ELEMENT
                                   AND plus_knd.T_TAXBASEKIND = one_rec.T_TAXBASETYPE
                                   AND plus_knd.T_CODE LIKE 'PlusG%'
                                 GROUP BY PLUS_OBJ.T_KIND
                               ) q
                         WHERE q.SumMinusObj > q.SumPlusObj
                       )
      LOOP

        v_RestOverSum := ABS(one_plus_kind.SumMinusObj - one_plus_kind.SumPlusObj);

        --Для каждого подходящего дохода отбираем все расходы из таблицы соответствия в порядке обратного приоритета
        FOR one_minus_kind IN (with objkind as (select k.t_Element, k.t_Code, 
                                              (case when k.t_TaxBaseKind = 9 then 6 
                                                    when k.t_TaxBaseKind = 3 then 2
                                                    when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1,RSI_NPTXC.TXOBJ_PLUSG_1011_2,RSI_NPTXC.TXOBJ_PLUSG_1011_3,RSI_NPTXC.TXOBJ_MINUSG_618) then 2
                                                    when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_2_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_3_IIS,RSI_NPTXC.TXOBJ_MINUSG_619) then 5
                                                    else k.t_TaxBaseKind end) as T_TAXBASEKIND 
                                        from dnptxkind_dbt k where k.t_Code like 'PlusG%' or k.t_Code like 'MinusG%')
                               SELECT q.*
                                 FROM (SELECT MINUS_OBJ.T_KIND, SUM(MINUS_OBJ.T_SUM0) as SumMinusObj
                                         FROM DNPTXOBJ_DBT MINUS_OBJ, objkind minus_knd
                                        WHERE MINUS_OBJ.T_CLIENT = one_rec.T_CLIENTID
                                          AND MINUS_OBJ.T_DATE = one_rec.T_DATEPAID
                                          AND MINUS_OBJ.T_KIND = minus_knd.T_ELEMENT
                                          AND minus_knd.T_TAXBASEKIND = one_rec.T_TAXBASETYPE
                                          AND minus_knd.T_CODE LIKE 'MinusG%'
                                          AND EXISTS(SELECT 1
                                                       FROM dnptxmatchinout_dbt mio1
                                                      WHERE mio1.T_KIND_INCOME = one_plus_kind.T_KIND
                                                        AND mio1.T_KIND_OUTCOME = MINUS_OBJ.T_KIND
                                                        AND mio1.T_DATEBEGIN <= one_rec.T_DATEPAID
                                                        AND mio1.T_DATEEND >= one_rec.T_DATEPAID
                                                    )
                                        GROUP BY MINUS_OBJ.T_KIND
                                       ) q, dnptxmatchinout_dbt mio
                                WHERE mio.T_KIND_INCOME = one_plus_kind.T_KIND
                                  AND mio.T_KIND_OUTCOME = q.T_KIND
                                  AND mio.T_DATEBEGIN <= one_rec.T_DATEPAID
                                  AND mio.T_DATEEND >= one_rec.T_DATEPAID
                               ORDER BY mio.T_PRIORITY DESC
                              )
        LOOP

          --Для каждого вида вычета вычисляем максимальную сумму распеределения именно по нему
          v_MaxSumTo := LEAST(one_minus_kind.SumMinusObj, v_RestOverSum);

          --Находим все записи в расшифровках, где нет превышения и есть подходящий под наш вычет код дохода с недобором вычета
          FOR one_upd IN (with objkind as (select k.t_Element, k.t_Code, 
                                              (case when k.t_TaxBaseKind = 9 then 6 
                                                    when k.t_TaxBaseKind = 3 then 2
                                                    when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1,RSI_NPTXC.TXOBJ_PLUSG_1011_2,RSI_NPTXC.TXOBJ_PLUSG_1011_3,RSI_NPTXC.TXOBJ_MINUSG_618) then 2
                                                    when k.t_TaxBaseKind = 0 and k.t_Element in (RSI_NPTXC.TXOBJ_PLUSG_1011_1_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_2_IIS,RSI_NPTXC.TXOBJ_PLUSG_1011_3_IIS,RSI_NPTXC.TXOBJ_MINUSG_619) then 5
                                                    else k.t_TaxBaseKind end) as T_TAXBASEKIND 
                                        from dnptxkind_dbt k where k.t_Code like 'PlusG%' or k.t_Code like 'MinusG%')
                          SELECT TMP.*,
                                 NVL((SELECT TMP1.T_ID
                                        FROM DNPTX6DECODING_TMP TMP1
                                       WHERE TMP1.T_CLIENTID = TMP.T_CLIENTID 
                                         AND TMP1.T_DATEPAID = TMP.T_DATEPAID
                                         AND TMP1.T_TYPESNOB = TMP.T_TYPESNOB
                                         AND TMP1.T_TYPEOPER = TMP.T_TYPEOPER
                                         AND TMP1.T_RATE <> TMP.T_RATE
                                         AND TMP1.T_TOTALMINUSREAL = 0
                                         AND TMP1.T_TOTALPLUS > TMP1.T_TAXBASESUM
                                         AND TMP1.T_RATE = 15
                                         AND TMP1.T_RECTYPE <> RECTYPE_NDR_CHANGECODES
                                     ), 0) as ID_15,
                                 NVL((SELECT SUM(PLUS_OBJ.T_SUM0)
                                        FROM DNPTXOBJ_DBT PLUS_OBJ, objkind plus_knd, dnptxmatchinout_dbt mio
                                       WHERE PLUS_OBJ.T_CLIENT = TMP.T_CLIENTID
                                         AND PLUS_OBJ.T_DATE = TMP.T_DATEPAID
                                         AND PLUS_OBJ.T_KIND = plus_knd.T_ELEMENT
                                         AND plus_knd.T_TAXBASEKIND = TMP.T_TAXBASETYPE
                                         AND plus_knd.T_CODE LIKE 'PlusG%'
                                         AND mio.T_KIND_INCOME = PLUS_OBJ.T_KIND
                                         AND mio.T_KIND_OUTCOME = one_minus_kind.T_KIND
                                         AND mio.T_DATEBEGIN <= TMP.T_DATEPAID
                                         AND mio.T_DATEEND >= TMP.T_DATEPAID
                                     ), 0) AS PlusObjSum,
                                 NVL((SELECT SUM(MINUS_OBJ.T_SUM0)
                                        FROM DNPTXOBJ_DBT MINUS_OBJ
                                       WHERE MINUS_OBJ.T_CLIENT = TMP.T_CLIENTID
                                         AND MINUS_OBJ.T_DATE = TMP.T_DATEPAID
                                         AND MINUS_OBJ.T_KIND = one_minus_kind.T_KIND
                                     ), 0) AS MinusObjSum
                            FROM DNPTX6DECODING_TMP TMP
                           WHERE TMP.T_CLIENTID = one_rec.T_CLIENTID
                             AND TMP.T_TOTALMINUSOVER = 0
                             AND TMP.T_TYPESNOB = one_rec.T_TYPESNOB
                             AND TMP.T_RECTYPE <> RECTYPE_NDR_CHANGECODES
                             AND EXISTS(SELECT 1
                                          FROM DNPTXOBJ_DBT PLUS_OBJ, objkind plus_knd, dnptxmatchinout_dbt mio
                                         WHERE PLUS_OBJ.T_CLIENT = TMP.T_CLIENTID
                                           AND PLUS_OBJ.T_DATE = TMP.T_DATEPAID
                                           AND PLUS_OBJ.T_KIND = plus_knd.T_ELEMENT
                                           AND plus_knd.T_TAXBASEKIND = TMP.T_TAXBASETYPE
                                           AND plus_knd.T_CODE LIKE 'PlusG%'
                                           AND mio.T_KIND_INCOME = PLUS_OBJ.T_KIND
                                           AND mio.T_KIND_OUTCOME = one_minus_kind.T_KIND
                                           AND mio.T_DATEBEGIN <= TMP.T_DATEPAID
                                           AND mio.T_DATEEND >= TMP.T_DATEPAID
                                       )
                           ORDER BY (CASE WHEN TMP.T_RATE = one_rec.t_Rate THEN 1 ELSE 2 END) ASC, (CASE WHEN TMP.T_ID > one_rec.T_ID THEN 1 ELSE 2 END) ASC, TMP.T_ID ASC
                         )
          LOOP

            --Сумма, которую можно добавить в дату по существующим объектам
            v_AddNormSum := one_upd.PlusObjSum - one_upd.MinusObjSum;

            IF v_AddNormSum > 0 THEN
              --Определяем минимум между возможной суммой по объектам и имеющейся разницей между доходами и нормированными расходами в записи (нельзя, чтобы нормированные расходы были больше TotalPlus)
              v_AddNormSum := LEAST(v_AddNormSum, (one_upd.t_TotalPlus - one_upd.t_TotalMinusNormal));

              --Определяем минимум между получившейся суммой и суммой, которую нам надо зачесть по виду расхода
              v_AddNormSum := LEAST(v_AddNormSum, v_MaxSumTo);

              v_TotalMinusNormal := one_upd.t_TotalMinusNormal + v_AddNormSum;

              UPDATE DNPTX6DECODING_TMP
                 SET T_TOTALMINUSNORMAL = v_TotalMinusNormal
               WHERE T_ID = one_upd.T_ID;


              IF one_upd.t_Rate = 13 AND one_upd.ID_15 > 0 AND v_TotalMinusNormal > one_upd.t_TotalMinusReal THEN

                --Если есть в эту же дату записьпо 15%, то после нормировки в записи по 13% увеличим доходы, а в записи по 15% уменьшим

                UPDATE DNPTX6DECODING_TMP
                   SET T_TOTALPLUS = T_TOTALPLUS + (v_TotalMinusNormal - one_upd.t_TotalMinusReal)
                 WHERE T_ID = one_upd.T_ID;

                UPDATE DNPTX6DECODING_TMP
                   SET T_TOTALPLUS = T_TOTALPLUS - (v_TotalMinusNormal - one_upd.t_TotalMinusReal)
                 WHERE T_ID = one_upd.ID_15;

              END IF;

              v_MaxSumTo    := v_MaxSumTo - v_AddNormSum;
              v_RestOverSum := v_RestOverSum - v_AddNormSum;
              v_RestTotalMinusOver := v_RestTotalMinusOver - v_AddNormSum;

              IF v_MaxSumTo = 0 THEN
                EXIT;
              END IF;

            END IF;

          END LOOP;

          IF v_RestOverSum = 0 THEN
            EXIT;
          END IF;

        END LOOP;

        IF v_RestTotalMinusOver = 0 THEN
          EXIT;
        END IF;

      END LOOP;

    END LOOP;

    UPDATE DNPTX6DECODING_TMP
       SET T_TOTALMINUSOVER = T_TOTALMINUSNORMAL - T_TOTALMINUSREAL;

  END;

END RSI_NPTX6CALC_2025;
/