begin
    update DDL_TICK_DBT 
      set  T_OFBU = chr(0)
    where T_BOFFICEKIND = 101
      and T_DEALTYPE = 32742;
exception
    when NO_DATA_FOUND
    then NULL;
end;
/