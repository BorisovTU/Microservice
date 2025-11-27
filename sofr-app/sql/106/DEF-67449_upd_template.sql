begin
  update dmcaccdoc_dbt m
     set m.t_templnum = 2
   where m.t_account in (select /*+ cardinality(t 10)*/ t.column_value
                           from table(t_string_list('47403344699003001347',
                                                    '47403344099001001347',
                                                    '47403398399003001347',
                                                    '47403398799001001347',
                                                    '47403784699003001347',
                                                    '47403784099001001347',
                                                    '47403933699003001347',
                                                    '47403933099001001347',
                                                    '47403949599003001347',
                                                    '47403949999001001347')) t)
     and m.t_templnum != 2;

  update dmcaccdoc_dbt m
     set m.t_templnum = 4
   where m.t_account in (select /*+ cardinality(t 10)*/ t.column_value
                           from table(t_string_list('47404344999003001347',
                                                    '47404344399001001347',
                                                    '47404398699003001347',
                                                    '47404398099001001347',
                                                    '47404784999003001347',
                                                    '47404784399001001347',
                                                    '47404933999003001347',
                                                    '47404933399001001347',
                                                    '47404949899003001347',
                                                    '47404949299001001347')) t)
     and m.t_templnum != 4;
  commit;
end;
/
