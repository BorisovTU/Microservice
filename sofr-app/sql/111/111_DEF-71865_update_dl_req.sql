BEGIN
  UPDATE ddl_req_dbt req
     SET req.t_MarketID = -1
   WHERE req.t_IsExchange <> 'X';
END;
/

BEGIN
  UPDATE ddl_req_dbt req
     SET req.t_MarketID = CASE WHEN req.t_SourceKind = 101 THEN
                                    NVL(( SELECT tk.t_MarketID   
                                        FROM ddl_tick_dbt tk, dspground_dbt ground, dspgrdoc_dbt dealdoc, dspgrdoc_dbt reqdoc
                                       WHERE dealdoc.t_sourcedocid = tk.t_DealID
                                         AND dealdoc.t_sourcedockind = tk.t_BOfficeKind
                                         AND ground.t_spgroundid = dealdoc.t_spgroundid
                                         AND ground.t_spgroundid = reqdoc.t_spgroundid
                                         AND dealdoc.t_sourcedocid != reqdoc.t_sourcedocid
                                         AND dealdoc.t_sourcedockind != reqdoc.t_sourcedockind
                                         AND reqdoc.t_sourcedocid = req.t_id
                                         AND reqdoc.t_sourcedockind = req.t_kind
                                         AND req.t_client = tk.t_ClientID 
                                         AND rownum = 1), -1)
                               WHEN req.t_SourceKind IN (199, 4813) THEN 2/*МосБиржа*/
                               WHEN req.t_SourceKind = 192 THEN 2/*МосБиржа*/
                               ELSE -1 END
   WHERE req.t_IsExchange = 'X';
END;
/