CREATE OR REPLACE PACKAGE BODY RSHB_VTB_LOADLOTNDFL
AS
    FUNCTION mapAll
        RETURN NUMBER
    AS
        CURSOR v_maps IS
            SELECT *
              FROM UVTB_DATA
             WHERE T_DEALID IS NULL AND T_CLIENTID IS NOT NULL;

        v_stat     NUMBER(5);
        v_cntErr   NUMBER(10);
    BEGIN
        v_stat := 0;
        v_cntErr := 0;
 
        FOR v_mapOne IN v_maps
        LOOP
            v_stat := RSHB_VTB_LOADLOTNDFL.mapOne (v_mapOne.t_ClientID,
                                                   v_mapOne.OperNo,
                                                   v_mapOne.t_ID);
            IF (v_stat > 0) THEN
                v_cntErr := v_cntErr + 1;
            END IF;
        END LOOP;

        RETURN v_cntErr;
    END mapAll;

    FUNCTION madMapping (p_Qty IN NUMBER, p_OperNo IN VARCHAR2)
        RETURN NUMBER
    AS
        CURSOR v_mapps IS
            SELECT vtbid, rsid, t_dealcode
              FROM (SELECT t_id vtbid, t_dealid rsid, t_dealcode
                      FROM (  SELECT ROWNUM rn, t_id
                                FROM uvtb_data vtb_data
                               WHERE     vtb_data.operno = p_OperNo
                                     AND vtb_data.qty = p_Qty
                                     AND vtb_data.t_dealid IS NULL
                            ORDER BY t_id) vtbdeals
                           LEFT JOIN
                           (  SELECT ROWNUM rn, tick.t_dealid, tick.t_dealcode
                                FROM ddl_tick_dbt tick
                                     INNER JOIN ddl_leg_dbt leg
                                         ON     leg.T_DEALID = tick.t_dealid
                                            AND leg.t_legkind = 0
                                            AND leg.t_legid = 0
                               WHERE     tick.T_BOFFICEKIND = 127
                                     AND tick.T_DEALTYPE = 2011
                                     AND tick.t_dealcode LIKE
                                             p_OperNo || '_NDFL%'
                                     AND leg.t_principal = p_Qty
                                     AND EXISTS
                                             (SELECT 1
                                                FROM dobjatcor_dbt cor
                                               WHERE     cor.t_objecttype = 101
                                                     AND cor.t_groupid = 210
                                                     AND t_object =
                                                         LPAD (tick.t_dealid, 34, '0'))
                                     AND NOT EXISTS
                                             (SELECT 1
                                                FROM uvtb_data uvtb
                                               WHERE uvtb.t_dealid =
                                                     tick.t_dealid)
                            ORDER BY tick.t_dealid) rsdeals
                               ON rsdeals.rn = vtbdeals.rn
                    UNION
                    SELECT t_id vtbid, t_dealid rsid, t_dealcode
                      FROM (  SELECT ROWNUM rn, t_id
                                FROM uvtb_data vtb_data
                               WHERE     vtb_data.operno = p_OperNo
                                     AND vtb_data.qty = p_Qty
                                     AND vtb_data.t_dealid IS NULL
                            ORDER BY t_id) vtbdeals
                           RIGHT JOIN
                           (  SELECT ROWNUM rn, tick.t_dealid, tick.t_dealcode
                                FROM ddl_tick_dbt tick
                                     INNER JOIN ddl_leg_dbt leg
                                         ON     leg.T_DEALID = tick.t_dealid
                                            AND leg.t_legkind = 0
                                            AND leg.t_legid = 0
                               WHERE     tick.T_BOFFICEKIND = 127
                                     AND tick.T_DEALTYPE = 2011
                                     AND tick.t_dealcode LIKE
                                             p_OperNo || '_NDFL%'
                                     AND leg.t_principal = p_Qty
                                     AND EXISTS
                                             (SELECT 1
                                                FROM dobjatcor_dbt cor
                                               WHERE     cor.t_objecttype = 101
                                                     AND cor.t_groupid = 210
                                                     AND t_object =
                                                         LPAD (tick.t_dealid, 34, '0'))
                                     AND NOT EXISTS
                                             (SELECT 1
                                                FROM uvtb_data uvtb
                                               WHERE uvtb.t_dealid = tick.t_dealid)
                            ORDER BY tick.t_dealid) rsdeals
                               ON rsdeals.rn = vtbdeals.rn);
    BEGIN
        FOR v_mapp IN v_mapps
        LOOP
            CASE
                WHEN v_mapp.vtbid IS NULL
                THEN
                    BEGIN
                        UPDATE ddl_tick_dbt
                           SET t_dealcode = t_dealcode || '_DEL'
                         WHERE     t_dealid = v_mapp.rsid
                               AND t_dealcode NOT LIKE '%_DEL%';
                    EXCEPTION
                        WHEN OTHERS
                        THEN RETURN 1;
                    END;
                WHEN v_mapp.rsid IS NULL
                THEN
                    BEGIN
                        UPDATE uvtb_data
                           SET t_state = 'R', t_comment = CHR (0)
                         WHERE t_id = v_mapp.vtbid;
                    EXCEPTION
                        WHEN OTHERS
                        THEN RETURN 1;
                    END;
                ELSE
                    BEGIN
                        UPDATE uvtb_data
                           SET (t_dealid, t_dealcode) =
                                   (SELECT t_dealid, t_dealcode
                                      FROM ddl_tick_dbt
                                     WHERE t_dealid = v_mapp.rsid),
                               t_state = 'OKM',
                               t_comment = CHR (0)
                         WHERE t_id = v_mapp.vtbid;
                    EXCEPTION
                        WHEN OTHERS
                        THEN RETURN 1;
                    END;
            END CASE;
        END LOOP;

        RETURN 0;
    END madMapping;

    FUNCTION mapOne (p_ClientID   IN NUMBER,
                     p_OperNo     IN VARCHAR2,
                     p_ID         IN NUMBER)
        RETURN NUMBER
    AS
        v_Cnt        NUMBER (5);
        v_DealID     NUMBER (10);
        v_stat       NUMBER (5);
        v_Qty        NUMBER (32, 12);
        v_DealCode   VARCHAR2 (30);
    BEGIN
        v_stat := 0;

        SELECT COUNT (*)            cnt,
               MAX (t_dealid)       dealid,
               MAX (t_dealcode)     dealcode
          INTO v_Cnt, v_DealID, v_DealCode
          FROM ddl_tick_dbt tick
         WHERE     tick.T_BOFFICEKIND = 127
               AND tick.T_DEALTYPE = 2011
               AND tick.t_clientid = p_ClientID
               AND tick.t_dealcode LIKE p_OperNo || '_NDFL%'
               AND EXISTS
                       (SELECT 1
                          FROM dobjatcor_dbt cor
                         WHERE     cor.t_objecttype = 101
                               AND cor.t_groupid = 210
                               AND t_object = LPAD (tick.t_dealid, 34, '0'))
               AND (   NOT EXISTS
                           (SELECT 1
                              FROM uvtb_data uvtb
                             WHERE uvtb.t_dealid = tick.t_dealid)
                    OR EXISTS
                           (SELECT 1
                              FROM uvtb_data vtb
                             WHERE     vtb.t_id = p_ID
                                   AND vtb.t_dealid = tick.t_dealid));

        CASE WHEN v_Cnt = 1
             THEN
                BEGIN
                    UPDATE uvtb_data
                       SET (t_dealid, t_dealcode) =
                               (SELECT t_dealid, t_dealcode
                                  FROM ddl_tick_dbt
                                 WHERE t_dealid = v_DealID),
                           t_state = 'OKM'
                     WHERE t_id = p_ID;

                    v_stat := 0;
                EXCEPTION
                    WHEN OTHERS
                    THEN RETURN 1;
                END;
            WHEN v_Cnt = 0
            THEN
                BEGIN
                    UPDATE uvtb_data
                       SET t_state = 'R'
                     WHERE t_id = p_ID;

                    v_stat := 0;
                EXCEPTION
                    WHEN OTHERS
                    THEN RETURN 1;
                END;
            WHEN v_Cnt > 1
            THEN
                BEGIN
                    SELECT Qty
                      INTO v_Qty
                      FROM uvtb_data
                     WHERE t_id = p_ID;

                    v_stat := RSHB_VTB_LOADLOTNDFL.madMapping (v_Qty, p_OperNo);
                EXCEPTION
                    WHEN OTHERS
                    THEN RETURN 1;
                END;
        END CASE;

        RETURN v_stat;
        EXCEPTION
            WHEN OTHERS
            THEN RETURN 1;
       
    END mapOne;
END RSHB_VTB_LOADLOTNDFL;
/
