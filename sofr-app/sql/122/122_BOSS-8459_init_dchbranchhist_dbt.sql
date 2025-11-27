begin
  insert into dchbranchhist_dbt (t_bcID
                                ,t_bcSeries
                                ,t_bcNumber
                                ,t_sysDate
                                ,t_operDate
                                ,t_oper
                                ,t_department
                                ,t_branch)
                         select  bnr.t_bcID
                                ,bnr.t_bcSeries
                                ,bnr.t_bcNumber
                                ,sysdate
                                ,bnr.t_issuedate
                                ,1
                                ,bnr.t_department
                                ,bnr.t_branch
                           from dvsbanner_dbt bnr;
end;
/