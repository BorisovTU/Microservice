  declare
    procedure PutRegVal(ppath_parent in varchar, pname in varchar2, pdesc in varchar2, pval in varchar2, ptype in number default 2) is
      Req_Id dregparm_dbt.t_keyid%type;
      Prn_Id dregparm_dbt.t_parentid%type;
      v_val dregval_dbt.T_LINTVALUE%type;
      v_cnt number;
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
      
      if ptype = 4 then -- δ« £
         if nvl(pval,'') = 'X' then
            v_val := 88;
         else
            v_val := 0;
         end if;
      elsif  ptype = 0 then
         v_val := to_number(pval);
      end if;
      
      if Req_Id>0 then
         if pval <> chr(0) then
            -- ―ΰ®Ά¥ΰ¨¬ ¥ι¥ ¨ dregval_dbt
            select count(*) into v_cnt 
            from dregval_dbt where T_KEYID = Req_Id;
            if v_cnt = 0 then
                 if ptype = 2 then  -- αβΰ®ª 
                    INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
                    VALUES (Req_Id,0,0,CHR(0),0,0,0,to_blob(utl_raw.cast_to_raw(pval||chr(0))));
                elsif ptype in (0,4) then -- ζ¥«®¥ ¨«¨ δ« £ 
                     INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
                     VALUES (Req_Id,0,0,CHR(0),0,v_val,0,null);
                end if;
            else 
                if ptype = 2 then 
                   UPDATE dregval_dbt 
                      SET T_LINTVALUE = 0,
                          T_LDOUBLEVALUE = 0,
                          T_FMTBLOBDATA_XXXX = to_blob(utl_raw.cast_to_raw(pval||chr(0)))
                    WHERE T_KEYID = Req_Id and t_objectid=0;
                else 
                   UPDATE dregval_dbt 
                      SET T_LINTVALUE = v_val,
                          T_LDOUBLEVALUE = 0,
                          T_FMTBLOBDATA_XXXX = null
                    WHERE T_KEYID = Req_Id and t_objectid=0;
                end if;
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
            when no_data_found then raise;
            
          end;
          if (Prn_Id!=0) then 
              INSERT INTO dregparm_dbt (t_keyId, t_ParentId, t_Name, t_Type, t_Global, t_Description, t_Security, t_IsBranch)
              VALUES (0, Prn_Id, pname, ptype, CHR(88), pdesc, CHR(0), CHR(0))
              RETURNING t_KeyId INTO Req_Id;
              if ptype = 2 then  -- αβΰ®ª 
                 INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
                 VALUES (Req_Id,0,0,CHR(0),0,0,0,to_blob(utl_raw.cast_to_raw(pval||chr(0))));
              elsif ptype in (0,4) then -- ζ¥«®¥ ¨«¨ δ« £ 
                 INSERT INTO dregval_dbt (T_KEYID, T_REGKIND, T_OBJECTID, T_BLOCKUSERVALUE, T_EXPDEP, T_LINTVALUE, T_LDOUBLEVALUE, T_FMTBLOBDATA_XXXX)
                 VALUES (Req_Id,0,0,CHR(0),0,v_val,0,null);
              end if;
          end if;
      end if;

    end;

begin
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…', '€‚’-‘‹……', 'ΰ¨§­ ª  Άβ®¬ β¨η¥αª®£® ¨α―®«­¥­¨ο ®―¥ΰ ζ¨©','', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……', '„ ”‹', '','', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……', '…”', '','', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……', '„ ‹', '','', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……/„ ”‹', '‚›‚„› †€', '','', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……/„ ”‹', '‚›‚„› ‚…†€', '','X', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……/„ ”‹', '……‚„› †€-‚…†€', '','', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……/„ ”‹', '……‚„› ‚…†€-†€', '','X', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……/…”', '‚›‚„› †€', '','', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……/…”', '‚›‚„› ‚…†€', '','', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……/…”', '……‚„› †€-‚…†€', '','', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……/…”', '……‚„› ‚…†€-†€', '','', 4);    
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……/„ ‹', '‚›‚„› †€', '','', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……/„ ‹', '‚›‚„› ‚…†€', '','', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……/„ ‹', '……‚„› ‘ ‚…†', '','', 4);
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……/„ ‹', '……‚„› € ‚…†“', '','', 4);   
    PutRegVal('‘•/’…ƒ€–/…’ƒ‚›… “—…/€‚’-‘‹……/„ ‹', '……‚„› †…‚›… ‹™€„', '','', 4);
    commit;
end;
