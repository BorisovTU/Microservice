begin
    UPDATE dregval_dbt
       SET T_LINTVALUE = 250
     WHERE T_KEYID =
              (SELECT T_KEYID
                 FROM (    SELECT p.t_keyid,
                                  SYS_CONNECT_BY_PATH (p.t_name, '/') PATH
                             FROM dregparm_dbt p
                       CONNECT BY PRIOR p.t_keyid = p.t_parentid
                       START WITH p.t_parentid = 0)
                WHERE PATH =
                         '/SECUR/éÅöÖäíõ ÉãéÅ. éè. çÄ òÄÉÖ')
           AND t_objectid = 0;
end;
/