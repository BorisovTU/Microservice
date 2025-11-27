declare
    g_days_string varchar2(4000) := '000000000000000000000000000000010100000000010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100000000010100000000000000010100010100010100010100000000000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100000000010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100000000010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100010100000000000000010100010100010100010100';

    procedure create_calendar (
        p_id          dcalkind_dbt.t_id%type,
        p_name        dcalkind_dbt.t_name%type,
        p_days_string varchar2
    ) is
    begin
        delete dcalendar_dbt where t_id = p_id;
        delete dcalkind_dbt where t_id = p_id;

        insert into dcalkind_dbt (t_id,
                                  t_name,
                                  t_balance,
                                  T_RETAILINSATURDAY,
                                  T_RETAILINSUNDAY,
                                  T_RETAILINHOLYDAY,
                                  T_PARENTID)
        values (p_id,
                p_name,
                2,
                chr(0),
                chr(0),
                chr(0),
                0);

        insert into dcalendar_dbt (t_id, t_year, t_daysinyear, t_calendays)
        values(p_id, 2025, 365, p_days_string);
    end create_calendar;

    procedure link_param (
        p_id    ddlcalparamlnk_dbt.t_calparamid%type,
        p_code  ddlcalparamlnk_dbt.t_kndcode%type,
        p_value ddlcalparamlnk_dbt.t_value%type,
        p_text  ddlcalparamlnk_dbt.t_textvalue%type
    ) is
    begin
        insert into ddlcalparamlnk_dbt (t_calparamid,
                                        t_kndcode,
                                        t_value,
                                        t_textvalue)
        values (p_id,
                p_code,
                p_value,
                p_text);
    end link_param;

    procedure save_param (
        p_id ddlcalparam_dbt.t_id%type,
        p_prog ddlcalparam_dbt.t_identprogram%type,
        p_calendar_id ddlcalparam_dbt.t_calkindid%type
    ) is
    begin
        insert into ddlcalparam_dbt(t_id, t_identprogram, t_calkindid)
        values (p_id, p_prog, p_calendar_id);
    end save_param;

    procedure delete_parameters (
        p_calendar_id number
    ) is
      l_param_id number(10);
    begin
        select p.T_ID
          into l_param_id
          from ddlcalparam_dbt p
         where p.T_CALKINDID = p_calendar_id;

        delete ddlcalparamlnk_dbt where t_calparamid = l_param_id;
        delete ddlcalparam_dbt where T_ID = l_param_id;
    exception
        when no_data_found then
            null;
    end delete_parameters;

    procedure set_calendar_parameters(
        p_calendar_id number,
        p_object_name varchar2
    ) is
        l_param_id ddlcalparamlnk_dbt.T_CALPARAMID%TYPE;
    begin
        delete_parameters(p_calendar_id => p_calendar_id);

        l_param_id := DDLCALPARAM_DBT_SEQ.nextval;

        save_param(p_id => l_param_id, p_prog => 83, p_calendar_id => p_calendar_id);

        link_param(p_id => l_param_id, p_code => 'DayType',     p_value => '2', p_text => 'Расчетные');
        link_param(p_id => l_param_id, p_code => 'Market',      p_value => '2', p_text => 'Публичное акционерное общество (ПАО) "Московская Биржа ММВБ-РТС"');
        link_param(p_id => l_param_id, p_code => 'MarketPlace', p_value => '1', p_text => 'Фондовый');
        link_param(p_id => l_param_id, p_code => 'ObjectType',  p_value => '1', p_text => 'Биржевая сделка');

        link_param(p_id => l_param_id, p_code => 'Object', p_value => p_object_name, p_text => p_object_name);
    end set_calendar_parameters;

begin
    create_calendar(p_id => 212, p_name => 'Календарь ММВБ фондовый расчетный рубли ПРЕПО', p_days_string => g_days_string);
    create_calendar(p_id => 213, p_name => 'Календарь ММВБ фондовый расчетный рубли ПРЕПО с КСУ', p_days_string => g_days_string);
    create_calendar(p_id => 214, p_name => 'Календарь ММВБ фондовый расчетный рубли ОРЕПО', p_days_string => g_days_string);
    create_calendar(p_id => 215, p_name => 'Календарь ММВБ фондовый расчетный рубли ОРЕПО с КСУ ', p_days_string => g_days_string);

    set_calendar_parameters(p_calendar_id => 212, p_object_name => 'Прямое РЕПО на бирже без признания ц/б');
    set_calendar_parameters(p_calendar_id => 213, p_object_name => 'Прямое биржевое РЕПО с КСУ');
    set_calendar_parameters(p_calendar_id => 214, p_object_name => 'Обратное РЕПО на бирже без признания ц/б');
    set_calendar_parameters(p_calendar_id => 215, p_object_name => 'Обратное биржевое РЕПО с КСУ');
end;
/