CREATE OR REPLACE package body qb_bp_utils is

  ------------------------------------------------------
  -- Функция логирование данных типа varchar
  ------------------------------------------------------
  procedure Check_Val(in_idevent in number,
                      in_tname   in varchar2,
                      in_cname   in varchar2,
                      in_new     in varchar2,
                      in_old     in varchar2,
                      in_rowid   in varchar2,
                      in_pkid    in varchar2) is
  begin
    if ( (in_new <> in_old) or (in_new is null and in_old is not NULL) or
       (in_new is not null and in_old is NULL)) then
      insert into DQB_BP_EVENT_AUDIT_DBT
        (T_IDEVENT, T_TNAME, T_CNAME, T_RID, T_PKID, T_OLD, T_NEW)
      values
        (in_idevent, in_tname, in_cname, in_rowid, in_pkid, in_old, in_new);
COMMIT;        
    end if;
  end;

  ------------------------------------------------------
  -- Функция логирование данных типа date
  ------------------------------------------------------
  procedure Check_Val(in_idevent in number,
                      in_tname   in varchar2,
                      in_cname   in varchar2,
                      in_new     in date,
                      in_old     in date,
                      in_rowid   in varchar2,
                      in_pkid    in varchar2) is
  begin
    
    if ( (in_new <> in_old) or (in_new is null and in_old is not NULL) or
       (in_new is not null and in_old is NULL)) then
      insert into DQB_BP_EVENT_AUDIT_DBT
        (T_IDEVENT, T_TNAME, T_CNAME, T_RID, T_PKID, T_OLD, T_NEW)
      values
        (in_idevent, in_tname, in_cname, in_rowid, in_pkid, to_char(in_old,'dd.mm.yyyy hh24:mi:ss'), to_char(in_new,'dd.mm.yyyy hh24:mi:ss'));
COMMIT;        
    end if;
    
  end;
  
  ------------------------------------------------------
  -- Функция логирование данных типа number
  ------------------------------------------------------
  procedure Check_Val(in_idevent in number,
                      in_tname   in varchar2,
                      in_cname   in varchar2,
                      in_new     in number,
                      in_old     in number,
                      in_rowid   in varchar2,
                      in_pkid    in varchar2) is
  begin
    
    if ( (in_new <> in_old) or (in_new is null and in_old is not NULL) or
       (in_new is not null and in_old is NULL)) then
      /*
      TODO: owner="k_guslyakov" category="Fix" priority="2 - Medium" created="16.10.2018"
      text="Необходимо принять решение нужно ли явно преобразовать в varchar с разделителем точка (.)"
      */
      insert into DQB_BP_EVENT_AUDIT_DBT
        (T_IDEVENT, T_TNAME, T_CNAME, T_RID, T_PKID, T_OLD, T_NEW)
      values
        (in_idevent, in_tname, in_cname, in_rowid, in_pkid, in_old, in_new);
COMMIT;        
    end if;
    
  end;
  
  ------------------------------------------------------
  -- очистка справочников/данных, сброс счетчиков и тд
  ------------------------------------------------------
  procedure clear_all is
    stmp varchar2(1000);
  begin
    /*
    TODO: owner="k_guslyakov" category="Review" priority="3 - Low" created="16.10.2018"
    text="Дополнить всеми объектами и сиквенсами, проверить что бы последовательность не протеворечила FK."
    */
    Raise_application_error(-20000, 'Подумай перед запуском и закомментируй...Потом верни обратно...');
    delete from DQB_BP_DICT_EVENT_DBT t; COMMIT;
    delete from DQB_BP_DICT_STEP_DBT t; COMMIT;
    delete from DQB_BP_DICT_PROCESS_DBT t; COMMIT;
    delete from DQB_BP_DICT_ATTR_DBT; COMMIT;
    delete from DQB_BP_MAP_ATTR_DBT; COMMIT;
    delete from DQB_BP_EVENT_ATTR_DBT; COMMIT;
    delete from DQB_BP_EVENT_ERROR_DBT; COMMIT;
    delete from DQB_BP_EVENT_AUDIT_DBT; COMMIT;
    delete from DQB_BP_EVENT_DBT; COMMIT;
    begin
      stmp := 'alter sequence DQB_SEQ_PROCESS increment by 1';
      execute immediate stmp;
    end;
    /*Коммитим руками дабы подумать еше раз, а надо ли...*/
  end clear_all;
  
  ------------------------------------------------------
  -- Инициализация выполнения шага на основании события
  ------------------------------------------------------
  procedure StartEvent(in_idEventKind  in number,
                       in_Value        in varchar2,
                       out_idEvent     out number) is
   nId number(10);
  begin
    
    -- Cherednichenko 2022-06-03 предварительно очищаем лог ошибок по событию, чтобы соблюсти констрейт
    delete from DQB_BP_EVENT_ERROR_DBT where t_idevent = (select t_id from dqb_bp_event_dbt bp where bp.t_value = nvl(in_Value, 0)); COMMIT;
    delete from dqb_bp_event_dbt bp where bp.t_value = nvl(in_Value, 0); COMMIT;
    select DQB_SEQ_PROCESS.NEXTVAL into nId from dual;
    
    insert into dqb_bp_event_dbt (t_id, t_idevent, t_value)
                          values (nId, in_idEventKind, nvl(in_Value, 0)) -- Если не передан первичный ключ, запишем ID события как ключ 
    returning T_ID into out_idEvent; 
COMMIT;    
  end StartEvent;
  
  ------------------------------------------------------
  -- Инициализация выполнения шага на основании процесса и шага
  ------------------------------------------------------
  procedure StartEventByProcess(in_idProcess in number,
                                in_idStep    in number,
                                in_Value     in varchar2,
                                out_idEvent  out number) is
    nId number(10);
  begin
    -- Найдем событие
    begin
      select e.t_id
        into nId
        from dQB_BP_Dict_Event_dbt e
             inner join dQB_BP_Dict_Process_dbt p on p.t_id = e.t_idprocess
             inner join dQB_BP_Dict_Step_dbt s    on s.t_id = e.t_idstep
       where e.t_idprocess  = in_idProcess
             and e.t_idstep = in_idStep;
    exception
      when too_many_rows then
        Raise_application_error(-20000, 'qb_bp_utils: 01 Невозможно идентифицировать, найдено более одного значения процессов!');
      when no_data_found then    
        Raise_application_error(-20000, 'qb_bp_utils: 02 Не найдено ни одного значения процессов!');
    end;
    -- Запишем событие
    StartEvent(nId, in_Value, out_idEvent);
    
  end StartEventByProcess;

  ------------------------------------------------------
  --  записать ошибки выполнения при их наличии
  ------------------------------------------------------
  procedure SetError (in_idEvent     in number,
                      in_ErrCode     in varchar2,
                      in_ErrMsg      in varchar2,
                      in_Is_Critical in number,
                      in_Attr_Id     in varchar2,
                      in_Attr_Vаlue  in varchar2)is
  begin
    insert into dqb_bp_event_error_dbt (t_idevent, t_errcode, t_errmsg, t_is_critical, t_attr_id, t_attr_value)
                                values (in_idEvent, in_ErrCode, in_ErrMsg, in_Is_Critical, in_Attr_Id, in_Attr_Vаlue);
COMMIT;                                
  end SetError;
           
  ------------------------------------------------------    
  -- Завершить событие
  ------------------------------------------------------
  procedure EndEvent(in_idEvent in number, in_HasError in number) is
    cntTmp  number := 0;
  begin
    --Если явно передали наличие ошибок то установим значение
    if in_HasError is not null then
      update dqb_bp_event_dbt e set e.t_hasError = in_HasError where e.t_id = in_idEvent; COMMIT;
      return;
    end if;
    
    -- Проверим наличие критических ошибок
    select count(1) 
      into cntTmp
      from dqb_bp_event_error_dbt e
     where e.t_is_critical = 1
           and e.t_idevent = in_idEvent;
    if cntTmp > 0 then
      update dqb_bp_event_dbt e set e.t_hasError = 1 where e.t_id = in_idEvent; COMMIT;
      return;
    end if;
    
    -- Проверим наличие предупреждений
    select count(1) 
      into cntTmp
      from dqb_bp_event_error_dbt e
     where e.t_is_critical = 0
           and e.t_idevent = in_idEvent;
    if cntTmp > 0 then
      update dqb_bp_event_dbt e set e.t_hasError = 1 where e.t_id = in_idEvent; COMMIT;
      return;
    end if;
  end EndEvent;
                   
  ------------------------------------------------------
  -- Запись аттрибута
  ------------------------------------------------------
  procedure SetAttrValueEx (in_idEvent       in number, 
                            in_AttrId        in number,
                            in_Sort          in number,
                            in_Value         in varchar2
                           ) is
  begin
    insert into dqb_bp_event_attr_dbt (t_idEvent, t_attr_id, t_sort, t_value)
                               values (in_idEvent, in_AttrId, in_Sort, in_Value);
COMMIT;                               
  end;
            
  ------------------------------------------------------   
  --Переопределенная функция записи аттрибута, тип данных Varchar2
  ------------------------------------------------------
  procedure SetAttrValue (in_idEvent       in number, 
                          in_EventAttrId   in number,
                          in_Value         in varchar2,
                          in_Sort          in number default 0) is
  begin
    SetAttrValueEx(in_idEvent, in_EventAttrId, in_Sort, in_Value);
  end;  
  
  ------------------------------------------------------
  --Переопределенная функция записи аттрибута, тип данных Date
  ------------------------------------------------------
  procedure SetAttrValue (in_idEvent       in number, 
                          in_EventAttrId   in number,
                          in_Value         in date,
                          in_Sort          in number default 0,
                          in_WithTime      in number default 0) is
    vStr varchar2(100);
  begin
    if in_WithTime > 1 then
      vStr := to_char(in_Value,'dd.mm.yyyy hh24:mi:ss');
    else
      vStr := to_char(in_Value,'dd.mm.yyyy');
    end if;
    SetAttrValueEx(in_idEvent, in_EventAttrId, in_Sort, vStr);
    
  end;
  
  ------------------------------------------------------
  --Переопределенная функция записи аттрибута, тип данных Number, разделитель явно указан точка(.)
  ------------------------------------------------------
  procedure SetAttrValue (in_idEvent       in number, 
                          in_EventAttrId   in number,
                          in_Value         in number,
                          in_Sort          in number default 0,
                          in_Point         in number default 0) is
    vSTR       varchar2(50);
    vResult varchar2(100);
  begin
    if in_Point > 0 then
      vSTR := '.' || LPAD('0', in_Point, '0');
    end if;
    vResult := trim(to_char(in_Value,'9999999999999999999999999999999999999999999999999990'|| vSTR)); 
    SetAttrValueEx(in_idEvent, in_EventAttrId, in_Sort, vResult);
  end; 
                
  ------------------------------------------------------                                 
  --Возвращает аттрибут по событию
  ------------------------------------------------------
  function GetAttrValue (in_idEvent       in number, 
                         in_EventAttrId   in number,
                         in_Sort          in number default 0
                        ) return varchar2 is
    out_result varchar2(4000);
  begin
    select e.t_value 
      into out_result
      from dqb_bp_event_attr_dbt e 
     where e.t_idevent = in_idEvent
           and e.t_attr_id = in_EventAttrId
           and e.t_sort = in_Sort;
    return out_result;
  end;   
    /*
    TODO: owner="k_guslyakov" category="Finish" priority="3 - Low" created="16.10.2018"
    text="Нужно реализовать:
          1. Процедуры работы с контекстом для записи и чтения порядковых номеров аттрибутов
          2. В процедурах SetAttrValue проверить по справочнику типов наличие пользовательского контекста и получить порядковый номер из него"
    */
end qb_bp_utils;
/
