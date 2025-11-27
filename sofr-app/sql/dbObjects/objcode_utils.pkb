create or replace package body objcode_utils as 

  procedure insert_code (
    p_object_type dobjcode_dbt.t_objecttype%type,
    p_code_kind   dobjcode_dbt.t_codekind%type,
    p_object_id   dobjcode_dbt.t_objectid%type,
    p_code        dobjcode_dbt.t_code%type,
    p_date        dobjcode_dbt.t_bankdate%type
  ) is
  begin
    insert into dobjcode_dbt (t_objecttype,
                              t_codekind,
                              t_objectid,
                              t_code,
                              t_state,
                              t_bankdate,
                              t_sysdate,
                              t_systime,
                              t_userid,
                              t_bankclosedate,
                              t_normcode)
    values (p_object_type,
            p_code_kind,
            p_object_id,
            p_code,
            0,
            trunc(p_date),
            sysdate,
            sysdate,
            nvl(Rsbsessiondata.oper, 0),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            chr(1)
           );
  end insert_code;
  
  procedure update_code_by_autokey (
    p_autokey dobjcode_dbt.t_autokey%type,
    p_code    dobjcode_dbt.t_code%type
  ) is
  begin
    update dobjcode_dbt c
       set c.t_code = p_code
     where c.t_autokey = p_autokey;
  end update_code_by_autokey;
  
  procedure close_code_by_autokey (
    p_autokey dobjcode_dbt.t_autokey%type,
    p_date    dobjcode_dbt.t_bankclosedate%type
  ) is
  begin
    update dobjcode_dbt c
       set c.t_state = 1,
           c.t_bankclosedate = p_date
     where c.t_autokey = p_autokey;
  end close_code_by_autokey;
  
  /*
    сохранение кода.
    Обработанные случаи:
    - аналогичного кода на объекте ещё не было
      просто инсерт в таблицу
    - аналогичный код на объекте есть и он был добавлен в дату p_date
      просто апдейт этого кода
    - аналогичный код на объекте есть и он был добавлен раньше, чем p_date
      необходимо закрыть старый код и инсертить новый
    - на объекте существует аналогичный код с датой начала позже, чем p_date.
      в данной процедуре эта ситуация не обрабатывается. Если нужен будет подобный фнукционал,
      то можно сделать новую процедуру. что-то типа save_code_with_history или с более очевидным названием. Ну или доработать эту
  */
  procedure save_code (
    p_object_type dobjcode_dbt.t_objecttype%type,
    p_code_kind   dobjcode_dbt.t_codekind%type,
    p_object_id   dobjcode_dbt.t_objectid%type,
    p_code        dobjcode_dbt.t_code%type,
    p_date        dobjcode_dbt.t_bankdate%type
  ) is
    l_code_row dobjcode_dbt%rowtype;
    l_date     dobjcode_dbt.t_bankdate%type := trunc(p_date);
  begin
    l_code_row := objcode_read.get_code_row(p_object_type => p_object_type,
                                            p_code_kind   => p_code_kind,
                                            p_object_id   => p_object_id);

    if l_code_row.t_autokey is null
    then
      insert_code(p_object_type => p_object_type,
                  p_code_kind   => p_code_kind,
                  p_object_id   => p_object_id,
                  p_code        => p_code,
                  p_date        => l_date);
    elsif l_code_row.t_bankdate = l_date
    then
      update_code_by_autokey(p_autokey => l_code_row.t_autokey,
                             p_code    => p_code);
    elsif l_code_row.t_bankdate < l_date
    then
      close_code_by_autokey(p_autokey => l_code_row.t_autokey,
                            p_date    => l_date);
      insert_code(p_object_type => p_object_type,
                  p_code_kind   => p_code_kind,
                  p_object_id   => p_object_id,
                  p_code        => p_code,
                  p_date        => l_date);
    elsif l_code_row.t_bankdate > l_date
    then
      null;
    end if;
  end save_code;

end objcode_utils;
/