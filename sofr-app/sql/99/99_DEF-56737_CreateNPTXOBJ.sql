/* Создание НДР по комиссиям */

DECLARE 
BEGIN

  FOR one_com IN (SELECT SFC.T_NUMBER, 
                         SFC.T_PARTYID, 
                         DLC.T_IIS, 
                         SFD.T_STATUS, 
                         SFD.T_SUM, 
                         SFD.T_DATEFEE,
                         SFD.T_FIID_SUM,
                         SFC.T_ID AS CONTRACTID
                    FROM DSFDEF_DBT     SFD, 
                         DSFCONTR_DBT   SFC, 
                         DDLCONTRMP_DBT MP, 
                         DDLCONTR_DBT   DLC 
                   WHERE SFD.T_STATUS = 40 
                     AND SFD.T_FEETYPE = 1 
                     AND SFD.T_COMMNUMBER = 1063 
                     AND SFD.T_SFCONTRID = SFC.T_ID 
                     AND MP.T_SFCONTRID = SFC.T_ID 
                     AND DLC.T_DLCONTRID = MP.T_DLCONTRID 
                     AND NOT EXISTS (SELECT 1 
                            FROM DNPTXOBJ_DBT OBJ 
                           WHERE OBJ.T_DATE = SFD.T_DATEFEE 
                             AND OBJ.T_CLIENT = SFC.T_PARTYID 
                             AND OBJ.T_LEVEL = 4 
                             AND OBJ.T_KIND = (CASE 
                                   WHEN DLC.T_IIS = CHR(88) THEN 
                                   /*RSI_NPTXC.TXOBJ_GENERAL_IIS*/ 
                                    525 
                                   ELSE 
                                   /*RSI_NPTXC.TXOBJ_GENERAL*/ 
                                    520 
                                 END) 
                             AND OBJ.T_SUM = SFD.T_SUM) 
                     AND EXISTS (SELECT * 
                            FROM DNPTXOP_DBT OP 
                           WHERE OP.T_DOCKIND = 4605 
                             AND OP.T_STATUS = 2 
                             AND OP.T_OPERDATE >= SFD.T_DATEFEE 
                             AND OP.T_CLIENT = SFC.T_PARTYID)
                )
  LOOP

    INSERT INTO DNPTXOBJ_DBT (
                               T_DATE         ,     -- T_DATE
                               T_CLIENT       ,     -- T_CLIENT
                               T_DIRECTION    ,     -- T_DIRECTION
                               T_LEVEL        ,     -- T_LEVEL
                               T_USER         ,     -- T_USER
                               T_KIND         ,     -- T_KIND
                               T_SUM          ,     -- T_SUM
                               T_CUR          ,     -- T_CUR
                               T_SUM0         ,     -- T_SUM0
                               T_ANALITICKIND1,     -- T_ANALITICKIND1
                               T_ANALITIC1    ,     -- T_ANALITIC1
                               T_ANALITICKIND2,     -- T_ANALITICKIND2
                               T_ANALITIC2    ,     -- T_ANALITIC2
                               T_ANALITICKIND3,     -- T_ANALITICKIND3
                               T_ANALITIC3    ,     -- T_ANALITIC3
                               T_ANALITICKIND4,     -- T_ANALITICKIND4
                               T_ANALITIC4    ,     -- T_ANALITIC4
                               T_ANALITICKIND5,     -- T_ANALITICKIND5
                               T_ANALITIC5    ,     -- T_ANALITIC5
                               T_ANALITICKIND6,     -- T_ANALITICKIND6
                               T_ANALITIC6    ,     -- T_ANALITIC6
                               T_COMMENT      ,     -- T_COMMENT
                               T_FROMOUTSYST  ,
                               T_OUTSYSTCODE  ,
                               T_OUTOBJID     ,
                               T_SOURCEOBJID,
                               T_TECHNICAL
                              )
                       VALUES (
                               one_com.t_DateFee,                      -- T_DATE
                               one_com.t_PartyID,                      -- T_CLIENT
                               2,                                      -- T_DIRECTION
                               4,                                      -- T_LEVEL
                               CHR(0),                                 -- T_USER
                               (CASE WHEN one_com.t_IIS = CHR(88) THEN 525 ELSE 520 END),     -- T_KIND
                               one_com.t_Sum,                          -- T_SUM
                               one_com.t_FIID_Sum,                     -- T_CUR
                               RSI_RSB_FIInstr.ConvSum( one_com.t_Sum, one_com.t_FIID_SUm, 0, one_com.t_DateFee, 1 ),  -- T_SUM0
                               0,                                      -- T_ANALITICKIND1
                               -1,                                     -- T_ANALITIC1
                               0,                                      -- T_ANALITICKIND2
                               -1,                                     -- T_ANALITIC2
                               0,                                      -- T_ANALITICKIND3
                               -1,                                     -- T_ANALITIC3
                               0,                                      -- T_ANALITICKIND4
                               -1,                                     -- T_ANALITIC4
                               0,                                      -- T_ANALITICKIND5
                               -1,                                     -- T_ANALITIC5
                               6020,                                   -- T_ANALITICKIND6
                               one_com.ContractID,                     -- T_ANALITIC6
                               'Auto56737 '||(CASE WHEN one_com.t_IIS = CHR(88) THEN 'Общие расходы по ИИС' ELSE 'Общие расходы' END),                               -- T_COMMENT
                               CHR(0),
                               CHR(1),
                               CHR(1),
                               0,
                               CHR(0)
                              );
  END LOOP;

  COMMIT;

END;
/