--добавление вида ЦБ 51(Клиринговый сертификат участия) в таблицу констант 
DECLARE
  cnt integer;
BEGIN
  select count(*) into cnt from qb_dwh_const4exp_val t where id = 7 and value = 51;
  if (cnt>0) then
    it_log.log('Вид ЦБ 51(Клиринговый сертификат участия) в таблицу констант добавлен ранее. Сообщение из 99_DEF-52160_insert_qb_dwh_const4exp_val.sql'); 
    return;
  end if;
  insert into qb_dwh_const4exp_val values (7, 51);
  EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION 
  WHEN OTHERS THEN
        it_error.put_error_in_stack;
        it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR); 
        it_error.clear_error_stack;
     NULL;
END;

