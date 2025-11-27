declare
  procedure create_acc_paried_check (
    p_accnum       varchar2
  ) is
  begin
    insert into dacc_paried_check_dbt(t_accountnumber, t_catid, t_is_deleted)
    values (p_accnum,
            0,
            chr(0));
  end create_acc_paried_check;
  
  
begin
  
  create_acc_paried_check('47403/47404');
  create_acc_paried_check('47421/47424');
  create_acc_paried_check('52601/52602');
  create_acc_paried_check('70613/70614');
  create_acc_paried_check('32027/32028');
  create_acc_paried_check('10603/10605');
  create_acc_paried_check('10634/10635');
  create_acc_paried_check('11401/11402');
  create_acc_paried_check('47446/47451');
  create_acc_paried_check('47465/47466');
  create_acc_paried_check('50140/50141');
  create_acc_paried_check('50428/50429');
  create_acc_paried_check('50430/50431');
  create_acc_paried_check('50508/50509');
  create_acc_paried_check('50720/50721');
  create_acc_paried_check('50738/50739');
  create_acc_paried_check('50770/50771');
  create_acc_paried_check('50909/50910');
  create_acc_paried_check('51232/51233');
  create_acc_paried_check('51234/51235');
  create_acc_paried_check('51238/51239');
  create_acc_paried_check('51339/51340');
  create_acc_paried_check('51341/51342');
  create_acc_paried_check('51526/51527');
  create_acc_paried_check('51528/51529');
  create_acc_paried_check('60107/60108');
  create_acc_paried_check('60120/60121');
  create_acc_paried_check('60213/60214');
  create_acc_paried_check('70602/70607');
  create_acc_paried_check('70702/70707');
  
  
  commit;
end;
/