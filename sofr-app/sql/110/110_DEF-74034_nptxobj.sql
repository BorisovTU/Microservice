/*ÑÆ°†¢´•≠®• ® Æ°≠Æ¢´•≠®• çÑê ØÆ DEF-74034*/
DECLARE

  PROCEDURE AddNptxObj(p_Client IN NUMBER, p_Kind IN NUMBER, p_Date IN DATE, p_Sum IN NUMBER)
  AS
    v_Level NUMBER := 0;
  BEGIN

    SELECT t_Level INTO v_Level
      FROM dnptxkind_dbt
     WHERE t_Element = p_Kind;


    INSERT INTO DNPTXOBJ_DBT (T_OBJID, 
                              T_DATE, 
                              T_CLIENT, 
                              T_DIRECTION, 
                              T_LEVEL, 
                              T_USER, 
                              T_KIND, 
                              T_SUM, 
                              T_CUR, 
                              T_SUM0, 
                              T_ANALITICKIND1, 
                              T_ANALITIC1, 
                              T_ANALITICKIND2, 
                              T_ANALITIC2, 
                              T_ANALITICKIND3, 
                              T_ANALITIC3, 
                              T_ANALITICKIND4, 
                              T_ANALITIC4, 
                              T_ANALITICKIND5, 
                              T_ANALITIC5, 
                              T_ANALITICKIND6, 
                              T_ANALITIC6, 
                              T_COMMENT, 
                              T_FROMOUTSYST, 
                              T_OUTSYSTCODE, 
                              T_OUTOBJID, 
                              T_SOURCEOBJID, 
                              T_TECHNICAL, 
                              T_TAXPERIOD
                             )
                      VALUES (0,            --T_OBJID,        
                              p_Date,       --T_DATE,         
                              p_Client,     --T_CLIENT,       
                              (CASE WHEN v_Level = 8 THEN 2 ELSE 1 END), --T_DIRECTION,    
                              v_Level,      --T_LEVEL,        
                              CHR(0),       --T_USER,         
                              p_Kind,       --T_KIND,         
                              round(p_Sum, 2),        --T_SUM,          
                              0,            --T_CUR,          
                              round(p_Sum, 2),        --T_SUM0,         
                              0,            --T_ANALITICKIND1,
                              -1,           --T_ANALITIC1,    
                              0,            --T_ANALITICKIND2,
                              -1,           --T_ANALITIC2,    
                              0,            --T_ANALITICKIND3,
                              -1,           --T_ANALITIC3,    
                              0,            --T_ANALITICKIND4,
                              -1,           --T_ANALITIC4,    
                              0,            --T_ANALITICKIND5,
                              -1,           --T_ANALITIC5,    
                              0,            --T_ANALITICKIND6,
                              -1,           --T_ANALITIC6,    
                              CHR(1),       --T_COMMENT,      
                              CHR(0),       --T_FROMOUTSYST,  
                              (CASE WHEN v_Level = 8 THEN CHR(1) ELSE 'äéåèÖçë' END),    --T_OUTSYSTCODE,  
                              CHR(1),       --T_OUTOBJID,     
                              0,            --T_SOURCEOBJID,  
                              CHR(0),       --T_TECHNICAL,    
                              0             --T_TAXPERIOD     
                             );

  END;

  PROCEDURE UpdateNptxObj(p_Client IN NUMBER, p_Kind IN NUMBER, p_Date IN DATE, p_Sum IN NUMBER)
  AS
  BEGIN
    UPDATE dnptxobj_dbt obj
       SET obj.t_FromOutSyst = CHR(0),
           obj.t_OutSystCOde = 'äéåèÖçë'
     WHERE obj.t_Client = p_Client
       AND obj.t_Date = p_Date
       AND obj.t_Kind = p_Kind
       AND obj.t_Sum0 = p_Sum
       AND obj.t_Level < 8;
  END;

BEGIN
  FOR one_rec IN (SELECT r.*,
                         (SELECT COUNT (1)
                            FROM dnptxobj_dbt obj
                           WHERE     obj.t_Client = r.t_PartyID
                                 AND obj.t_Date = r.t_Date
                                 AND obj.t_Kind IN (1160)
                                 AND round(obj.t_Sum0, 2) = round(r.t_PlusG_4800, 2)
                         ) cntBaseG9,
                         (SELECT COUNT (1)
                            FROM dnptxobj_dbt obj
                           WHERE     obj.t_Client = r.t_PartyID
                                 AND obj.t_Date = r.t_Date
                                 AND obj.t_Kind IN (1142)
                                 AND round(obj.t_Sum0, 2) = round(r.t_PlusG_4800, 2)
                         ) cntBaseBill,
                         (SELECT COUNT (1)
                            FROM dnptxobj_dbt obj
                           WHERE     obj.t_Client = r.t_PartyID
                                 AND obj.t_Date = r.t_Date
                                 AND obj.t_Kind IN (1170)
                                 AND round(obj.t_Sum0, 2) = round(r.t_PlusG_4800, 2)
                         ) cnt4800,
                         (SELECT COUNT (1)
                            FROM dnptxobj_dbt obj
                           WHERE     obj.t_Client = r.t_PartyID
                                 AND obj.t_Date = r.t_Date
                                 AND obj.t_Kind IN (1143)
                                 AND round(obj.t_Sum0, 2) = round(r.t_PaidComp, 2) + round(r.t_PaidComp_15, 2)
                         ) cntPaid
                    FROM dnptxdefferedobj_dbt r
                   WHERE EXTRACT (YEAR FROM r.t_Date) IN (2023, 2024)
                  )
  LOOP

    IF one_rec.cnt4800 = 0 THEN
      AddNptxObj(one_rec.t_PartyID, 1170 /*TXOBJ_PLUSG_4800*/, one_rec.t_Date, one_rec.t_PlusG_4800);
    ELSE
      UpdateNptxObj(one_rec.t_PartyID, 1170 /*TXOBJ_PLUSG_4800*/, one_rec.t_Date, one_rec.t_PlusG_4800);
    END IF;

    IF one_rec.cntBaseG9 = 0 THEN
      AddNptxObj(one_rec.t_PartyID, 1160 /*TXOBJ_BASEG9*/, one_rec.t_Date, one_rec.t_PlusG_4800);
    ELSE
      UpdateNptxObj(one_rec.t_PartyID, 1160 /*TXOBJ_BASEG9*/, one_rec.t_Date, one_rec.t_PlusG_4800);
    END IF;

    IF one_rec.cntBaseBill = 0 THEN
      AddNptxObj(one_rec.t_PartyID, 1142 /*TXOBJ_BASEBILL*/, one_rec.t_Date, one_rec.t_PlusG_4800);
    ELSE
      UpdateNptxObj(one_rec.t_PartyID, 1142 /*TXOBJ_BASEBILL*/, one_rec.t_Date, one_rec.t_PlusG_4800);
    END IF;

    IF one_rec.cntPaid = 0 THEN
      AddNptxObj(one_rec.t_PartyID, 1143 /*TXOBJ_PAIDBILL*/, one_rec.t_Date, one_rec.t_PaidComp + one_rec.t_PaidComp_15);
    ELSE
      UpdateNptxObj(one_rec.t_PartyID, 1143 /*TXOBJ_PAIDBILL*/, one_rec.t_Date, one_rec.t_PaidComp + one_rec.t_PaidComp_15);
    END IF;

  END LOOP;
END;
/
