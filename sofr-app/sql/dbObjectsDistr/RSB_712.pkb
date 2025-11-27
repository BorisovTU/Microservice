CREATE OR REPLACE PACKAGE BODY RSB_712
-- Форма 712
IS
  PROCEDURE DS712MOVE (p_SessionID   IN VARCHAR2,
                       p_DateEnd     IN VARCHAR2)
  IS
    SQL_ST    VARCHAR2 (32767);
    SQL_STW   VARCHAR2 (32767);
  BEGIN
    SQL_ST :=
      'UPDATE D712MOVE_DBT
         SET D712MOVE_DBT.sectype =
             NVL (rsb_secur.GetObjAttrName (12,
                            1,
                            NVL (rsb_secur.GetMainObjAttr (
                                 12,
                                 LPAD (D712MOVE_DBT.t_FIID, 10, ''0''),
                                 1, 
                                 '|| p_DateEnd ||' ),
                               0)),
              ''''),
           D712MOVE_DBT.secrate =
             Rsb_Secur.SC_GetCostByCourse712 (
               1,
               D712MOVE_DBT.t_FIID, 
               '||p_DateEnd||',
               D712MOVE_DBT.t_FI_Kind,
               D712MOVE_DBT.t_AvoirKind,
               (CASE
                WHEN D712MOVE_DBT.t_SubKind = 8
                THEN
                  NVL ((SELECT mp.t_MarketId
                      FROM ddlcontrmp_dbt mp
                       WHERE mp.t_SfContrId = D712MOVE_DBT.t_ID),
                     -1)
                ELSE
                  -1
              END)),
           D712MOVE_DBT.qnty =
             (SELECT /*+ full(DPMWRTCL_DBT)*/
                 NVL (SUM (T_AMOUNT), 0)
              FROM DPMWRTCL_DBT
             WHERE   T_FIID = D712MOVE_DBT.t_fiid
                 AND 1 =
                   (CASE
                    WHEN D712MOVE_DBT.t_id = -1 THEN 1
                    WHEN D712MOVE_DBT.t_id = T_CONTRACT THEN 1
                    ELSE 0
                  END)
                 AND T_BEGDATE <= '|| p_DateEnd ||' AND T_ENDDATE >= '|| p_DateEnd ||' )';

    SQL_STW := 'D712MOVE_DBT.SessionID = ' || p_SessionID || ' ';

    RSB_DLUTILS.PARALLELUPDATEEXECUTE ('D712MOVE_DBT', SQL_ST, SQL_STW, 8);
  END DS712MOVE;

  PROCEDURE DS712IIS (p_SessionID   IN VARCHAR2,
                      p_DateBeg     IN VARCHAR2,
                      p_DateEnd     IN VARCHAR2)
  IS
    SQL_ST    VARCHAR2 (32767);
    SQL_STW   VARCHAR2 (32767);
  BEGIN
    SQL_ST :=
      'UPDATE D712IIS_DBT
         SET D712IIS_DBT.sum_in =
             (SELECT 
                 NVL (SUM (nptxop.t_taxsum2), 0)   SUM
              FROM dnptxop_dbt nptxop
             WHERE   nptxop.t_account = D712IIS_DBT.T_ACCOUNT
                 AND nptxop.t_SubKind_Operation = 10
                 AND nptxop.t_status = 2
                 AND nptxop.t_operdate BETWEEN ' || p_DateBeg || ' AND ' || p_DateEnd || '),
           D712IIS_DBT.sum_out =
             (SELECT 
                 NVL (SUM (nptxop.t_taxsum2), 0)   SUM
              FROM dnptxop_dbt nptxop
             WHERE   nptxop.t_account = D712IIS_DBT.T_ACCOUNT
                 AND nptxop.t_SubKind_Operation = 20
                 AND nptxop.t_status = 2
                 AND nptxop.t_operdate BETWEEN ' || p_DateBeg || ' AND ' || p_DateEnd || '),
           D712IIS_DBT.IS_ACTIVE =
             CASE
               WHEN  EXISTS
                     (SELECT 1
                      FROM ddl_tick_dbt tick
                     WHERE   tick.t_BOfficeKind = 101
                         AND (   tick.t_ClientID =
                             D712IIS_DBT.T_pID
                          OR (  tick.t_PartyID =
                              D712IIS_DBT.T_pID
                            AND tick.t_IsPartyClient = ''X''))
                         AND (   tick.t_ClientContrID =
                             D712IIS_DBT.T_SfID
                          OR (  tick.t_PartyContrID =
                              D712IIS_DBT.T_SfID
                            AND tick.t_IsPartyClient = ''X'')
                          OR EXISTS
                               (SELECT 1
                                FROM dsfcontr_dbt  sf,
                                   ddlcontrmp_dbt  mp
                               WHERE   sf.t_ServKind IN
                                       (1, 15, 21)
                                   AND mp.t_SfContrID =
                                     sf.t_ID
                                   AND mp.t_DlContrID =
                                     D712IIS_DBT.T_DlID
                                   AND (   tick.t_ClientContrID =
                                       mp.t_SfContrID
                                    OR (  tick.t_PartyContrID =
                                        mp.t_SfContrID
                                      AND tick.t_IsPartyClient =
                                        ''X''))))
                         AND tick.t_DealDate BETWEEN ' || p_DateBeg || ' AND ' || p_DateEnd || '
                         AND tick.t_DealStatus > 0)
                OR EXISTS
                     (SELECT 1
                      FROM ddvndeal_dbt dvn
                     WHERE   dvn.t_Client = D712IIS_DBT.T_pID
                         AND (   dvn.t_Clientcontr =
                             D712IIS_DBT.T_SfID
                          OR EXISTS
                               (SELECT 1
                                FROM dsfcontr_dbt  sf,
                                   ddlcontrmp_dbt  mp
                               WHERE   sf.t_ServKind IN
                                       (1, 15, 21)
                                   AND mp.t_SfContrID =
                                     sf.t_ID
                                   AND mp.t_DlContrID =
                                     D712IIS_DBT.T_DlID
                                   AND dvn.t_ClientContr =
                                     mp.t_SfContrID))
                         AND dvn.t_Date BETWEEN ' || p_DateBeg || ' AND ' || p_DateEnd || '
                         AND dvn.t_State > 0)
                OR EXISTS
                     (SELECT 1
                      FROM ddvdeal_dbt dv
                     WHERE   dv.t_Client = D712IIS_DBT.T_pID
                         AND (   dv.t_ClientContr =
                             D712IIS_DBT.T_SfID
                          OR EXISTS
                               (SELECT 1
                                FROM dsfcontr_dbt  sf,
                                   ddlcontrmp_dbt  mp
                               WHERE   sf.t_ServKind IN
                                       (1, 15, 21)
                                   AND mp.t_SfContrID =
                                     sf.t_ID
                                   AND mp.t_DlContrID =
                                     D712IIS_DBT.T_DlID
                                   AND dv.t_ClientContr =
                                     mp.t_SfContrID))
                         AND dv.t_Date BETWEEN ' || p_DateBeg || ' AND ' || p_DateEnd || '
                         AND dv.t_State > 0)
               THEN
                 ''Да''
               ELSE
                 ''Нет''
             END';

    SQL_STW := 'D712IIS_DBT.SessionID = ' ||p_SessionID || ' ';

    RSB_DLUTILS.PARALLELUPDATEEXECUTE ('D712IIS_DBT', SQL_ST, SQL_STW, 8);
  END DS712IIS;

  PROCEDURE DS712IIS_RSHB (p_SessionID   IN VARCHAR2,
                           p_DateBeg     IN DATE,
                          p_DateEnd     IN DATE) as
  v_YES constant varchar2(2)  := 'Да' ;
  v_NO constant varchar2(3)  := 'Нет' ;

  BEGIN

  merge into D712IIS_DBT d
    using (select /*+ leading(D712) use_hash(NPTXOP) index(NPTXOP DNPTXOP_DBT_IDX4 )*/ nptxop.t_account
             ,sum(decode(nptxop.t_SubKind_Operation, 10, nptxop.t_taxsum2, 0)) sum_in
             ,sum(decode(nptxop.t_SubKind_Operation, 20, nptxop.t_taxsum2, 0)) sum_out
         from D712IIS_DBT d712
         join dnptxop_dbt nptxop
           on (nptxop.t_account = D712.T_ACCOUNT)
        where d712.SessionID = p_SessionID
          and nptxop.t_SubKind_Operation in (10, 20)
          and nptxop.t_status = 2
          and nptxop.t_operdate between p_DateBeg and p_DateEnd
        group by nptxop.t_account) s
     on (d.SessionID = p_SessionID and d.T_ACCOUNT = s.t_account )
   WHEN MATCHED THEN
        UPDATE SET d.sum_in = s.sum_in
             ,d.sum_out = s.sum_out  ; 

   UPDATE D712IIS_DBT
         SET    D712IIS_DBT.IS_ACTIVE =
             CASE
               WHEN   EXISTS
                     (SELECT 1
                      FROM ddvndeal_dbt dvn
                     WHERE   dvn.t_Client = D712IIS_DBT.T_pID
                         AND (   dvn.t_Clientcontr =
                             D712IIS_DBT.T_SfID
                          OR EXISTS
                               (SELECT 1
                                FROM dsfcontr_dbt  sf,
                                   ddlcontrmp_dbt  mp
                               WHERE   sf.t_ServKind IN
                                       (1, 15, 21)
                                   AND mp.t_SfContrID =
                                     sf.t_ID
                                   AND mp.t_DlContrID =
                                     D712IIS_DBT.T_DlID
                                   AND dvn.t_ClientContr =
                                     mp.t_SfContrID))
                         AND dvn.t_Date BETWEEN p_DateBeg  AND p_DateEnd 
                         AND dvn.t_State > 0)
               THEN
                 v_YES
               ELSE
                 v_NO
               END
           where D712IIS_DBT.SessionID = p_SessionID ;

     UPDATE D712IIS_DBT
         SET    D712IIS_DBT.IS_ACTIVE =
             CASE
               WHEN  EXISTS
                     (SELECT 1
                      FROM ddvdeal_dbt dv
                     WHERE   dv.t_Client = D712IIS_DBT.T_pID
                         AND (   dv.t_ClientContr =
                             D712IIS_DBT.T_SfID
                          OR EXISTS
                               (SELECT 1
                                FROM dsfcontr_dbt  sf,
                                   ddlcontrmp_dbt  mp
                               WHERE   sf.t_ServKind IN
                                       (1, 15, 21)
                                   AND mp.t_SfContrID =
                                     sf.t_ID
                                   AND mp.t_DlContrID =
                                     D712IIS_DBT.T_DlID
                                   AND dv.t_ClientContr =
                                     mp.t_SfContrID))
                         AND dv.t_Date BETWEEN  p_DateBeg AND  p_DateEnd 
                         AND dv.t_State > 0)
               THEN
                 v_YES
               ELSE
                 v_NO
               END
           where D712IIS_DBT.SessionID = p_SessionID and IS_ACTIVE =  v_NO ;


      UPDATE D712IIS_DBT
         SET    D712IIS_DBT.IS_ACTIVE =
             CASE
               WHEN EXISTS
                     (SELECT /*+ index(tick DDL_TICK_DBT_IDX_U1 )*/  1 
                      FROM ddl_tick_dbt tick
                     WHERE   tick.t_BOfficeKind = 101
                         AND   tick.t_ClientID = D712IIS_DBT.T_pID
                         AND (   tick.t_ClientContrID = D712IIS_DBT.T_SfID
                          OR (  tick.t_PartyContrID =   D712IIS_DBT.T_SfID
                            AND tick.t_IsPartyClient = 'X')
                          OR EXISTS
                               (SELECT 1
                                FROM dsfcontr_dbt  sf,
                                   ddlcontrmp_dbt  mp
                               WHERE   sf.t_ServKind IN  (1, 15, 21)
                                   AND mp.t_SfContrID =  sf.t_ID
                                   AND mp.t_DlContrID =  D712IIS_DBT.T_DlID
                                   AND (   tick.t_ClientContrID =  mp.t_SfContrID
                                    OR (  tick.t_PartyContrID =  mp.t_SfContrID
                                      AND tick.t_IsPartyClient = 'X'))))
                         AND tick.t_DealDate BETWEEN  p_DateBeg  AND p_DateEnd 
                         AND tick.t_DealStatus > 0)
               THEN
                 v_YES
               ELSE
                 v_NO
               END
           where D712IIS_DBT.SessionID = p_SessionID and IS_ACTIVE =  v_NO ;

      UPDATE D712IIS_DBT
         SET    D712IIS_DBT.IS_ACTIVE =
             CASE
               WHEN EXISTS
                     (SELECT /*+ index(tick DDL_TICK_DBT_IDX_U2 )*/ 1
                      FROM ddl_tick_dbt tick
                     WHERE   tick.t_BOfficeKind = 101
                         AND   tick.t_PartyID =  D712IIS_DBT.T_pID AND tick.t_IsPartyClient = 'X'
                         AND (   tick.t_ClientContrID = D712IIS_DBT.T_SfID
                          OR (  tick.t_PartyContrID =   D712IIS_DBT.T_SfID
                            AND tick.t_IsPartyClient = 'X')
                          OR EXISTS
                               (SELECT 1
                                FROM dsfcontr_dbt  sf,
                                   ddlcontrmp_dbt  mp
                               WHERE   sf.t_ServKind IN  (1, 15, 21)
                                   AND mp.t_SfContrID =  sf.t_ID
                                   AND mp.t_DlContrID =  D712IIS_DBT.T_DlID
                                   AND (   tick.t_ClientContrID =  mp.t_SfContrID
                                    OR (  tick.t_PartyContrID =  mp.t_SfContrID
                                      AND tick.t_IsPartyClient = 'X'))))
                         AND tick.t_DealDate BETWEEN  p_DateBeg  AND p_DateEnd 
                         AND tick.t_DealStatus > 0)
               THEN
                 v_YES
               ELSE
                 v_NO
               END
           where D712IIS_DBT.SessionID = p_SessionID and IS_ACTIVE =  v_NO ;


  END DS712IIS_RSHB;

  PROCEDURE DS712SumByFIID (p_SessionID IN VARCHAR2, p_DateEnd IN VARCHAR2)
  IS
    SQL_ST    VARCHAR2 (32767);
    SQL_STW   VARCHAR2 (32767);
  BEGIN
    SQL_ST :=
      'UPDATE D712SUMBYFIID_DBT
         SET D712SUMBYFIID_DBT.t_Cost =
             RSB_SECUR.SC_GetCostByCourse712 (D712SUMBYFIID_DBT.t_Amount,
                            D712SUMBYFIID_DBT.t_FIID,
                            ' || p_DateEnd || ',
                            D712SUMBYFIID_DBT.t_FI_Kind,
                            D712SUMBYFIID_DBT.t_AvoirKind,
                            D712SUMBYFIID_DBT.t_MarketId),
           D712SUMBYFIID_DBT.t_HasNominal =
             RSB_SECUR.SC_HasNominalOnRateDate712 (D712SUMBYFIID_DBT.t_FIID, 
             ' || p_DateEnd || '),
           D712SUMBYFIID_DBT.t_NominalDate =
             RSB_SECUR.SC_GetNominalDate712 (D712SUMBYFIID_DBT.t_FIID, 
             ' || p_DateEnd || ')';

    SQL_STW := 'D712SUMBYFIID_DBT.SessionID = ' ||p_SessionID || '  ';

    RSB_DLUTILS.PARALLELUPDATEEXECUTE ('D712SUMBYFIID_DBT', SQL_ST, SQL_STW, 8);
  END DS712SumByFIID;

  PROCEDURE DelD712MOVE (p_SessionID IN VARCHAR2)
  IS
  BEGIN
    EXECUTE IMMEDIATE   'DELETE FROM D712MOVE_DBT WHERE SessionID = ' ||p_SessionID|| ' ';
  END DelD712MOVE;

  PROCEDURE DelD712SumByFIID (p_SessionID IN VARCHAR2)
  IS
  BEGIN
    EXECUTE IMMEDIATE   'DELETE FROM D712SUMBYFIID_DBT WHERE SessionID = ' ||p_SessionID|| ' ';
  END DelD712SumByFIID;

  PROCEDURE DelD712IIS (p_SessionID IN VARCHAR2)
  IS
  BEGIN
    EXECUTE IMMEDIATE   'DELETE FROM D712IIS_DBT WHERE SessionID = ' ||p_SessionID|| ' ';
  END DelD712IIS;
END;
/
