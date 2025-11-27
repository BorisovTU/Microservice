BEGIN
    it_log.log('START data_patch DEF-90143', 'MSG');

    FOR req_rec IN (
        SELECT *
          FROM DDL_REQ_DBT A
          JOIN DSPGRDOC_DBT B
            ON A.T_ID = B.T_SOURCEDOCID
           AND B.T_SOURCEDOCKIND = 350
         WHERE T_MARKETKIND = 4
           AND NOT EXISTS (
              SELECT 1
                FROM DSPGRDOC_DBT C
               WHERE C.T_SPGROUNDID = B.T_SPGROUNDID
                 AND C.T_SOURCEDOCKIND = 192
           )
    ) LOOP
        DECLARE
            v_deal_id DDVDEAL_DBT.T_ID%TYPE;
        BEGIN
            SELECT d.T_ID
              INTO v_deal_id
              FROM ddvdeal_dbt d
             WHERE d.t_extcode = req_rec.t_codets
               AND ROWNUM = 1;

            INSERT INTO DSPGRDOC_DBT (
                t_sourcedockind,
                t_sourcedocid,
                t_spgroundid
            ) VALUES (
                192,
                v_deal_id,
                req_rec.t_spgroundid
            );

            it_log.log('Insert deal with ID: ' || TO_CHAR(v_deal_id), 'MSG');

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('Deal with ID ' || TO_CHAR(req_rec.t_codets) || ' not found.');
        END;
    END LOOP;

    it_log.log('FINISH data_patch DEF-90143', 'MSG');

END;
/