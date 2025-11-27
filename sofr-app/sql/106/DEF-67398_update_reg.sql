declare
    procedure PutRegVal(ppath_parent in varchar, pname in varchar2, pdesc in varchar2, pval in varchar2, ptype in number default 2) is        
      Req_Id dregparm_dbt.t_keyid%type;
      Prn_Id dregparm_dbt.t_parentid%type;
      v_val  dregval_dbt.T_LINTVALUE%type;
      v_valD dregval_dbt.T_LDOUBLEVALUE%type;
    begin
      begin
          SELECT T_KEYID INTO Req_Id
            from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
                   from dregparm_dbt p
                connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
           where Path='/'||ppath_parent||'/'||pname;
      exception
        when no_data_found then Req_Id:=0;
      end;
      
      if ptype = 4 then -- флаг
         if nvl(pval,'') = 'X' then
            v_val := 88;
         else
            v_val := 0;
         end if;
      elsif (ptype = 0) then
         v_val := to_number(pval,'9999.99');
      elsif (ptype = 1) then
         v_valD := to_number(pval,'9999.99');
      end if;
                  
      if Req_Id>0 then
         if pval is not null then
            if ptype = 2 then 
               UPDATE dregval_dbt 
                  SET T_LINTVALUE = 0,
                      T_LDOUBLEVALUE = 0,
                      T_FMTBLOBDATA_XXXX = to_blob(utl_raw.cast_to_raw(pval||chr(0)))
                WHERE T_KEYID = Req_Id and t_objectid=0;
            elsif ptype = 1 then
               UPDATE dregval_dbt 
                  SET T_LINTVALUE = 0,
                      T_LDOUBLEVALUE = v_valD,
                      T_FMTBLOBDATA_XXXX = null
                WHERE T_KEYID = Req_Id and t_objectid=0;
            else 
               UPDATE dregval_dbt 
                  SET T_LINTVALUE = v_val,
                      T_LDOUBLEVALUE = 0,
                      T_FMTBLOBDATA_XXXX = null
                WHERE T_KEYID = Req_Id and t_objectid=0;
            end if;
         end if;
      else      
          begin
              SELECT T_KEYID INTO Prn_Id
                from(select p.t_keyid,SYS_CONNECT_BY_PATH(p.t_name, '/') Path
                       from dregparm_dbt p, dregval_dbt v
                      where v.t_keyid=p.t_keyid and v.t_objectid=0
                    connect by prior p.t_keyid=p.t_parentid start with p.t_parentid=0)
               where Path='/'||ppath_parent;
          exception
            when no_data_found then Prn_Id:=0;
          end;
          if (Prn_Id!=0) then 
              INSERT INTO dregparm_dbt (t_keyId, t_ParentId, t_Name, t_Type, t_Global, t_Description, t_Security, t_IsBranch)
              VALUES (0, Prn_Id, pname, ptype, CHR(88), pdesc, CHR(0), CHR(0))
              RETURNING t_KeyId INTO Req_Id;
              if ptype = 2 then  -- строка
                 INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
                 VALUES (Req_Id,0,0,CHR(0),0,0,0,to_blob(utl_raw.cast_to_raw(pval||chr(0))));
              elsif ptype = 1 then -- double
                 INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
                 VALUES (Req_Id,0,0,CHR(0),0,0,v_valD,null);
              elsif ptype in (0,4) then -- целое или флаг 
                 INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
                 VALUES (Req_Id,0,0,CHR(0),0,v_val,0,null);
              end if;
          end if;
      end if;

    end;

begin
    PutRegVal('SECUR', 'ПРИОРИТЕЗАЦИЯ ЗАПРОСОВ КОНВЕЙЕР', 'Использовать алгоритм приоритезации запросов конвейера, при котором наиболее тяжелые запросы выполняются в первую очередь', chr(88), 4);
end;
