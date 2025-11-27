SET DEFINE OFF;
INSERT INTO dqueuews_dbt
            (t_id, t_directionid, t_queuename, t_type, t_isactive
            )
     VALUES (NULL, 1, 'ESB_TO_SOFR', 1, 'X'
            );
INSERT INTO dqueuews_dbt
            (t_id, t_directionid, t_queuename, t_type, t_isactive
            )
     VALUES (NULL, 2, 'SOFR_TO_ESB', 2, 'X'
            );
COMMIT ;