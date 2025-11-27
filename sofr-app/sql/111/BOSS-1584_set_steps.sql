begin
  update doprostep_dbt s
     set s.t_post_macro = chr(1)
   where s.t_blockid = 203702
     and s.t_post_macro in ('nptxwrt105_203702.mac', 'nptxwrt107_203702.mac');
end;
/

declare

  function get_prev_step (
    p_block_id doprostep_dbt.t_blockid%type
  ) return doprostep_dbt.t_number_step%type is
    l_number doprostep_dbt.t_number_step%type;
  begin
    select nvl(max(s.t_number_step), 0)
      into l_number
      from doprostep_dbt s
     where s.t_blockid = p_block_id;

    return l_number;
  end get_prev_step;
  
  procedure new_step_as_last (
    p_block_id   doprostep_dbt.t_blockid%type,
    p_number     doprostep_dbt.t_number_step%type,
    p_symbol     doprostep_dbt.t_symbol%type,
    p_macro      doprostep_dbt.t_carry_macro%type,
    p_post_macro doprostep_dbt.t_post_macro%type,
    p_name       doprostep_dbt.t_name%type,
    p_auto       doprostep_dbt.t_autoexecutestep%type
  ) is
    l_prev_step doprostep_dbt.t_previous_step%type;
  begin

    delete doprostep_dbt s where s.t_blockid = p_block_id and s.t_number_step = p_number;
    
    l_prev_step := get_prev_step(p_block_id => p_block_id);
    insert into doprostep_dbt (t_blockid,
                               t_number_step,
                               t_kind_action,
                               t_dayoffset,
                               t_scale,
                               t_dayflag,
                               t_calendarid,
                               t_symbol,
                               t_previous_step,
                               t_modification,
                               t_carry_macro,
                               t_print_macro,
                               t_post_macro,
                               t_notinuse,
                               t_firststep,
                               t_name,
                               t_datekindid,
                               t_rev,
                               t_autoexecutestep,
                               t_onlyhandcarry,
                               t_isallowforoper,
                               t_operorgroup,
                               t_restrictearlyexecution,
                               t_usertypes,
                               t_initdatekindid,
                               t_askfordate,
                               t_backout,
                               t_isbackoutgroup,
                               t_massexecutemode,
                               t_masspacksize)
    values (p_block_id,
            p_number,
            1,
            0,
            0,
            chr(0),
            0,
            nvl(p_symbol, chr(0)),
            l_prev_step,
            0,
            p_macro,
            chr(1),
            nvl(p_post_macro, chr(1)),
            chr(0),
            chr(0),
            p_name,
            460700000,
            chr(0),
            p_auto,
            chr(0),
            0,
            chr(0),
            chr(0),
            chr(1),
            0,
            chr(0),
            0,
            chr(0),
            0,
            0);
  end new_step_as_last;
  
  function is_step_exists (
    p_block_id   doprostep_dbt.t_blockid%type,
    p_number     doprostep_dbt.t_number_step%type
  ) return boolean is
    l_cnt integer;
  begin
    select count(1)
      into l_cnt
      from doprostep_dbt s
     where s.t_blockid = p_block_id
       and s.t_number_step = p_number;

    return l_cnt > 0;
  end is_step_exists;

  procedure new_step_as_first (
    p_block_id   doprostep_dbt.t_blockid%type,
    p_number     doprostep_dbt.t_number_step%type,
    p_symbol     doprostep_dbt.t_symbol%type,
    p_macro      doprostep_dbt.t_carry_macro%type,
    p_post_macro doprostep_dbt.t_post_macro%type,
    p_name       doprostep_dbt.t_name%type,
    p_auto       doprostep_dbt.t_autoexecutestep%type
  ) is
  begin
    if is_step_exists(p_block_id => p_block_id,
                      p_number   => p_number) then
      return;
    end if;

    update doprostep_dbt s
       set s.t_previous_step = p_number
     where s.t_blockid = p_block_id
       and s.t_previous_step = 0;

    insert into doprostep_dbt (t_blockid,
                               t_number_step,
                               t_kind_action,
                               t_dayoffset,
                               t_scale,
                               t_dayflag,
                               t_calendarid,
                               t_symbol,
                               t_previous_step,
                               t_modification,
                               t_carry_macro,
                               t_print_macro,
                               t_post_macro,
                               t_notinuse,
                               t_firststep,
                               t_name,
                               t_datekindid,
                               t_rev,
                               t_autoexecutestep,
                               t_onlyhandcarry,
                               t_isallowforoper,
                               t_operorgroup,
                               t_restrictearlyexecution,
                               t_usertypes,
                               t_initdatekindid,
                               t_askfordate,
                               t_backout,
                               t_isbackoutgroup,
                               t_massexecutemode,
                               t_masspacksize)
    values (p_block_id,
            p_number,
            1,
            0,
            0,
            chr(0),
            0,
            nvl(p_symbol, chr(0)),
            0,
            0,
            p_macro,
            chr(1),
            nvl(p_post_macro, chr(1)),
            chr(0),
            chr(0),
            p_name,
            460700000,
            chr(0),
            p_auto,
            chr(0),
            0,
            chr(0),
            chr(0),
            chr(1),
            0,
            chr(0),
            0,
            chr(0),
            0,
            0);
  end new_step_as_first;

begin

  new_step_as_last(p_block_id   => 203700,
                   p_number     => 20,
                   p_symbol     => null,
                   p_macro      => 'nptxwrt020.mac',
                   p_post_macro => null,
                   p_name       => 'Отправка неторгового поручения по списанию в QUIK',
                   p_auto       => chr(88));

  new_step_as_last(p_block_id   => 203700,
                   p_number     => 21,
                   p_symbol     => null,
                   p_macro      => 'nptxwrt021.mac',
                   p_post_macro => null,
                   p_name       => 'Получение результата обработки неторгового поручения в QUIK',
                   p_auto       => chr(88));

  new_step_as_first(p_block_id   => 203702,
                   p_number     => 81,
                   p_symbol     => null,
                   p_macro      => 'nptxwrt081.mac',
                   p_post_macro => null,
                   p_name       => 'Получение результата обработки неторгового поручения в QUIK',
                   p_auto       => chr(88));

  new_step_as_first(p_block_id   => 203702,
                   p_number     => 80,
                   p_symbol     => null,
                   p_macro      => 'nptxwrt080.mac',
                   p_post_macro => null,
                   p_name       => 'Отправка неторгового поручения по НДФЛ в QUIK',
                   p_auto       => chr(88));
end;
/