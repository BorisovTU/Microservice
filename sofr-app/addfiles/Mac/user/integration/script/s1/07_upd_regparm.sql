declare
  Req_Id dregparm_dbt.t_keyid%type;
  Prn_Id dregparm_dbt.t_parentid%type;
begin
  begin
      SELECT T_KEYID INTO Req_Id
        from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
               from dregparm_dbt p
            connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
       where Path='/SECUR';
  exception
    when no_data_found then Req_Id:=0;
  end;
  if Req_Id=0 then
      Prn_Id := 0;
          INSERT INTO dregparm_dbt (t_keyId, t_ParentId, t_Name, t_Type, t_Global, t_Description, t_Security, t_IsBranch)
          VALUES (0, Prn_Id, 'SECUR', 0, CHR(0), 'Настройки подсистемы "Ценные бумаги"', CHR(0), CHR(0))
          RETURNING t_KeyId INTO Req_Id;
          INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
          VALUES (Req_Id,0,0,CHR(0),0,0,0,to_blob(utl_raw.cast_to_raw(chr(0))));
  end if;
end;
/

declare
  Req_Id dregparm_dbt.t_keyid%type;
  Prn_Id dregparm_dbt.t_parentid%type;
begin
  begin
      SELECT T_KEYID INTO Req_Id
        from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
               from dregparm_dbt p
            connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
       where Path='/SECUR/INTEGR_SERV_PRM';
  exception
    when no_data_found then Req_Id:=0;
  end;
  if (Req_Id>0) then
      null;
  else
      begin
          SELECT T_KEYID INTO Prn_Id
            from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
                   from dregparm_dbt p, dregval_dbt v
                  where v.t_keyid=p.t_keyid and v.t_objectid=0
                connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
           where Path='/SECUR';
      exception
        when no_data_found then Prn_Id:=0;
      end;
      if (Prn_Id!=0) then 
          INSERT INTO dregparm_dbt (t_keyId, t_ParentId, t_Name, t_Type, t_Global, t_Description, t_Security, t_IsBranch)
          VALUES (0, Prn_Id, 'INTEGR_SERV_PRM', 0, CHR(88), 'Интеграционные сервисы', CHR(0), CHR(0))
          RETURNING t_KeyId INTO Req_Id;
          INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
          VALUES (Req_Id,0,0,CHR(0),0,0,0,to_blob(utl_raw.cast_to_raw(chr(0))));
      end if;
  end if;
end;
/

declare
  Req_Id dregparm_dbt.t_keyid%type;
  Prn_Id dregparm_dbt.t_parentid%type;
begin
  begin
      SELECT T_KEYID INTO Req_Id
        from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
               from dregparm_dbt p
            connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
       where Path='/SECUR/INTEGR_SERV_PRM/EXT_SERVICES';
  exception
    when no_data_found then Req_Id:=0;
  end;
  if (Req_Id>0) then
      null;
   else
      begin
          SELECT T_KEYID INTO Prn_Id
            from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
                   from dregparm_dbt p, dregval_dbt v
                  where v.t_keyid=p.t_keyid and v.t_objectid=0
                connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
           where Path='/SECUR/INTEGR_SERV_PRM';
      exception
        when no_data_found then Prn_Id:=0;
      end;
      if (Prn_Id!=0) then 
          INSERT INTO dregparm_dbt (t_keyId, t_ParentId, t_Name, t_Type, t_Global, t_Description, t_Security, t_IsBranch)
          VALUES (0, Prn_Id, 'EXT_SERVICES', 0, CHR(88), 'Исходящие запросы', CHR(0), CHR(0))
          RETURNING t_KeyId INTO Req_Id;
          INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
          VALUES (Req_Id,0,0,CHR(0),0,0,0,to_blob(utl_raw.cast_to_raw(chr(0))));
      end if;
  end if;
end;
/
declare
  Req_Id dregparm_dbt.t_keyid%type;
  Prn_Id dregparm_dbt.t_parentid%type;
begin
  begin
      SELECT T_KEYID INTO Req_Id
        from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
               from dregparm_dbt p
            connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
       where Path='/SECUR/INTEGR_SERV_PRM/EXT_SERVICES/UPDATESTATUSDEAL';
  exception
    when no_data_found then Req_Id:=0;
  end;
  if (Req_Id>0) then
      null;
  else
      begin
          SELECT T_KEYID INTO Prn_Id
            from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
                   from dregparm_dbt p, dregval_dbt v
                  where v.t_keyid=p.t_keyid and v.t_objectid=0
                connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
           where Path='/SECUR/INTEGR_SERV_PRM/EXT_SERVICES';
      exception
        when no_data_found then Prn_Id:=0;
      end;
      if (Prn_Id!=0) then 
          INSERT INTO dregparm_dbt (t_keyId, t_ParentId, t_Name, t_Type, t_Global, t_Description, t_Security, t_IsBranch)
          VALUES (0, Prn_Id, 'UPDATESTATUSDEAL', 0, CHR(88), 'Обновление статуса сделки', CHR(0), CHR(0))
          RETURNING t_KeyId INTO Req_Id;
          INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
          VALUES (Req_Id,0,0,CHR(0),0,0,0,to_blob(utl_raw.cast_to_raw(chr(0))));
      end if;
  end if;
end;
/

declare
  Req_Id dregparm_dbt.t_keyid%type;
  Prn_Id dregparm_dbt.t_parentid%type;
begin
  begin
      SELECT T_KEYID INTO Req_Id
        from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
               from dregparm_dbt p
            connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
       where Path='/SECUR/INTEGR_SERV_PRM/EXT_SERVICES/UPDATESTATUSDEAL/USERNAME';
  exception
    when no_data_found then Req_Id:=0;
  end;
  if (Req_Id>0) then
      UPDATE dregval_dbt 
         SET T_LINTVALUE = 0,
             T_LDOUBLEVALUE = 0,
             T_FMTBLOBDATA_XXXX = to_blob(utl_raw.cast_to_raw('Web'||chr(0)))
       WHERE T_KEYID = Req_Id and t_objectid=0;
  else
      begin
          SELECT T_KEYID INTO Prn_Id
            from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
                   from dregparm_dbt p, dregval_dbt v
                  where v.t_keyid=p.t_keyid and v.t_objectid=0
                connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
           where Path='/SECUR/INTEGR_SERV_PRM/EXT_SERVICES/UPDATESTATUSDEAL';
      exception
        when no_data_found then Prn_Id:=0;
      end;
      if (Prn_Id!=0) then 
          INSERT INTO dregparm_dbt (t_keyId, t_ParentId, t_Name, t_Type, t_Global, t_Description, t_Security, t_IsBranch)
          VALUES (0, Prn_Id, 'USERNAME', 2, CHR(88), '', CHR(0), CHR(0))
          RETURNING t_KeyId INTO Req_Id;
          INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
          VALUES (Req_Id,0,0,CHR(0),0,0,0,to_blob(utl_raw.cast_to_raw('Web'||chr(0))));
      end if;
  end if;
end;
/

declare
  Req_Id dregparm_dbt.t_keyid%type;
  Prn_Id dregparm_dbt.t_parentid%type;
begin
  begin
      SELECT T_KEYID INTO Req_Id
        from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
               from dregparm_dbt p
            connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
       where Path='/SECUR/INTEGR_SERV_PRM/EXT_SERVICES/UPDATESTATUSDEAL/USERPASSWORD';
  exception
    when no_data_found then Req_Id:=0;
  end;
  if (Req_Id>0) then
      UPDATE dregval_dbt 
         SET T_LINTVALUE = 0,
             T_LDOUBLEVALUE = 0,
             T_FMTBLOBDATA_XXXX = to_blob(utl_raw.cast_to_raw('Web12'||chr(0)))
       WHERE T_KEYID = Req_Id and t_objectid=0;
  else
      begin
          SELECT T_KEYID INTO Prn_Id
            from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
                   from dregparm_dbt p, dregval_dbt v
                  where v.t_keyid=p.t_keyid and v.t_objectid=0
                connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
           where Path='/SECUR/INTEGR_SERV_PRM/EXT_SERVICES/UPDATESTATUSDEAL';
      exception
        when no_data_found then Prn_Id:=0;
      end;
      if (Prn_Id!=0) then 
          INSERT INTO dregparm_dbt (t_keyId, t_ParentId, t_Name, t_Type, t_Global, t_Description, t_Security, t_IsBranch)
          VALUES (0, Prn_Id, 'USERPASSWORD', 2, CHR(88), '', CHR(0), CHR(0))
          RETURNING t_KeyId INTO Req_Id;
          INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
          VALUES (Req_Id,0,0,CHR(0),0,0,0,to_blob(utl_raw.cast_to_raw('Web12'||chr(0))));
      end if;
  end if;
end;
/

declare
  Req_Id dregparm_dbt.t_keyid%type;
  Prn_Id dregparm_dbt.t_parentid%type;
begin
  begin
      SELECT T_KEYID INTO Req_Id
        from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
               from dregparm_dbt p
            connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
       where Path='/SECUR/INTEGR_SERV_PRM/EXT_SERVICES/UPDATESTATUSDEAL/RULENAME';
  exception
    when no_data_found then Req_Id:=0;
  end;
  if (Req_Id>0) then
      UPDATE dregval_dbt 
         SET T_LINTVALUE = 0,
             T_LDOUBLEVALUE = 0,
             T_FMTBLOBDATA_XXXX = to_blob(utl_raw.cast_to_raw('RSHB_Sofr2Kondor_Status2Queue'||chr(0)))
       WHERE T_KEYID = Req_Id and t_objectid=0;
  else
      begin
          SELECT T_KEYID INTO Prn_Id
            from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
                   from dregparm_dbt p, dregval_dbt v
                  where v.t_keyid=p.t_keyid and v.t_objectid=0
                connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
           where Path='/SECUR/INTEGR_SERV_PRM/EXT_SERVICES/UPDATESTATUSDEAL';
      exception
        when no_data_found then Prn_Id:=0;
      end;
      if (Prn_Id!=0) then 
          INSERT INTO dregparm_dbt (t_keyId, t_ParentId, t_Name, t_Type, t_Global, t_Description, t_Security, t_IsBranch)
          VALUES (0, Prn_Id, 'RULENAME', 2, CHR(88), '', CHR(0), CHR(0))
          RETURNING t_KeyId INTO Req_Id;
          INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
          VALUES (Req_Id,0,0,CHR(0),0,0,0,to_blob(utl_raw.cast_to_raw('RSHB_Sofr2Kondor_Status2Queue'||chr(0))));
      end if;
  end if;
end;

commit;
/

