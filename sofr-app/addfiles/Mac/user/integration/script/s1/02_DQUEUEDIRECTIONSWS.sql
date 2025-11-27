SET DEFINE OFF;
INSERT INTO dqueuedirectionsws_dbt
            (t_id, t_name, t_host, t_port, t_queuemanagername, t_channelname, t_login, t_password, t_isactive
            )
     VALUES (NULL, 'QW_in', '10.4.116.110', 1420, 'IIB.ADP.MI1', 'SOFR.SVRCONN', 'svcgo-sofr', 'sofr#1', 'X'
            );
INSERT INTO dqueuedirectionsws_dbt
            (t_id, t_name, t_host, t_port, t_queuemanagername, t_channelname, t_login, t_password, t_isactive
            )
     VALUES (NULL, 'QW_out', '10.4.116.110', 1420, 'IIB.ADP.MI1', 'SOFR.SVRCONN', 'svcgo-sofr', 'sofr#1', 'X'
            );
COMMIT ;