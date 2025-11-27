begin
  update doprostep_dbt o
     set o.t_autoexecutestep = 'X',
         o.t_carry_macro = 'nptxwrt070.mac'
   where o.t_blockid = 203707;
  commit;
end;
/
