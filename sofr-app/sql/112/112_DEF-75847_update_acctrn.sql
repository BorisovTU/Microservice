declare
  --n number(10):=0; -- закоментировать
  r number(10):=0; 
begin
  for pmdoc in (select * from dpmdocs_dbt where t_acctrnid=0 and T_PAYMENTID > 8789065)
  loop
    --n:=n+1; -- закоментировать
    --if n>1000 then exit; end if; -- закоментировать
    for pmpaym in (select * from dpmpaym_dbt where t_paymentid = pmdoc.t_paymentid and length(t_userfield4)>0 and t_userfield4<>chr(1) and t_userfield4<>chr(0))
    loop
      for pmlink in (select * from dpmlink_dbt where t_purposepayment = pmdoc.t_paymentid) 
      loop
        for pmpaym2 in (select * from dpmpaym_dbt where t_paymentid = pmlink.t_initialpayment) 
        loop
          for tick in (select * from ddl_tick_dbt t where t.t_bofficekind = pmpaym2.t_dockind and t.t_dealid = pmpaym2.t_documentid)
          loop
            for oproper in (select * from doproper_dbt o where o.t_dockind = tick.t_bofficekind and o.t_documentid = lpad(tick.t_dealid,34,'0'))
            loop
              for oprdoc in (select * from doprdocs_dbt o1 where o1.t_id_operation = oproper.t_id_operation and o1.t_dockind = 1)
              loop
                for acctr in (select * from dacctrn_dbt t1 where t1.t_acctrnid = oprdoc.t_acctrnid)
                loop
                  if acctr.t_number_pack = 170 and length(acctr.t_userfield4)<2 and (acctr.t_userfield4=chr(1) or acctr.t_userfield4<>chr(0) or acctr.t_userfield4 is null) then 
                    r:=r+1;
                    update dacctrn_dbt t2 set t2.t_userfield4 = pmpaym.t_userfield4 where t2.t_acctrnid = acctr.t_acctrnid;
                    it_log.log_handle(p_object   => 'update dacctrn_dbt t_userfield4',
                                      p_msg      => 'дата:=' || to_char(acctr.t_date_carry) || ', номер='||to_char(acctr.t_numb_document 
                                                     || ', сумма=' || acctr.t_sum_natcur || ', идентификатор=' || acctr.t_acctrnid
                                                     || ', userfield4=' || pmpaym.t_userfield4) ,
                                      p_msg_type => it_log.C_MSG_TYPE__MSG);
                  end if;
                end loop;
              end loop;
            end loop;
          end loop;  
        end loop;
      end loop;
    end loop;
  end loop;
  dbms_output.put_line('обновлено проводок '||r);
  --rollback; -- закоментировать
end;
