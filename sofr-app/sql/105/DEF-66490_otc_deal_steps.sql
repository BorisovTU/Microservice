begin
  update doprostep_dbt o
     set o.t_autoexecutestep = chr(88)
  where o.t_blockid = 20000525
    and o.t_number_step = 13;

  commit;
end;
/
