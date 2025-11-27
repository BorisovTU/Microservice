IMPORT mmarkinter, OprInter,funk,dl_plib;
   private var que,rs9,ii,{curdate},da,dealid,rs5,acc4;
   da=槍�({curdate});
   que=" select a.t_account, c.t_partyid  from  dmcaccdoc_dbt a,  dmccateg_dbt b, ddl_tick_dbt c where a.t_dockind=102  and a.t_docid=c.t_dealid  and a.t_catid=b.t_id "; 
   que=que+" and (t_code='+且神是ｧ･爐, 私2' or t_code='-且神是ｧ･爐, 私2' or t_code='-且神是ｧ･爐, %0' or t_code='+且神是ｧ･爐, %0' )and c.t_dealtype=12335 ";
//   que=que+" and (t_code='+且神是ｧ･爐, 私2' or t_code='-且神是ｧ･爐, 私2')and c.t_dealtype=12335 ";

   rs9 = LnGetRecordSet(que);
   if(rs9 != null)
     while(rs9.moveNext())
       acc4=(rs9.value(0));
       ii=rs9.value(1);
         que="update daccount_dbt set t_client="+ii+"  where t_account='"+acc4+"'";
         rs5 = LnGetRecordSet(que);
         println(ii);

     end;
   end;

end;




