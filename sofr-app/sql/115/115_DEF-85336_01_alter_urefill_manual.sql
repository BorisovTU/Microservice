--Изменение urefill_manual_dbt
begin
  execute immediate 'alter table urefill_manual_dbt add t_type number(10) default 0';
  execute immediate 'comment on column urefill_manual_dbt.t_type is ''Тип подрепления: 0 - ручное; 1 - автоматическое''';
end;
/
