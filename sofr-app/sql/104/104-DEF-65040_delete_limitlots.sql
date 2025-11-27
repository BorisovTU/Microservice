begin
  delete from d_limitlots_tmp t where t.t_marketid is null;
  commit;
end;
/
