begin
  for cur in (select distinct w.dealid
                   from PKO_WriteOff w
                   join ddl_tick_dbt t on t.t_dealid = w.dealid 
                  where w.opertype = 2
                    and w.pkostatus != 7
                    and nvl(w.iscanceled, chr(0)) != 'X'
                    and nvl(w.iscompleted, chr(0)) != 'X'
                    and t.t_dealtype = 32743
                    and t.t_dealstatus != 20
                  order by w.dealid)
   loop
     update ddlgrdeal_dbt gr
         set gr.t_plandate = date'9999-12-31'
       where gr.t_docid = cur.dealid
         and gr.t_dockind = 127;

     it_diasoft.Set_GRSOBUDate(p_deailid => cur.dealid, p_GRSOBUDate => date'9999-12-31');
   end loop;
end ;
/
