declare
  n      number;
  t_name varchar2(100) := 'DACCOVERVALUE2_TMP';
begin
  select count(*) into n from user_tables t where t.table_name = upper(t_name);
  if n = 0
  then
    execute immediate 'create global temporary table daccovervalue2_tmp
(
  t_accountid            NUMBER(10) not null,
  t_chapter              NUMBER(5) not null,
  t_code_currency        NUMBER(10) not null,
  t_account              VARCHAR2(25) not null,
  t_exrateaccountplus    VARCHAR2(25),
  t_exrateaccountminus   VARCHAR2(25),
  t_ispairexrateaccounts CHAR(1),
  t_rest                 NUMBER(32,12),
  t_restequivalent       NUMBER(32,12),
  t_exratesum            NUMBER(32,12),
  t_exrateaccount        VARCHAR2(25),
  t_skipaccount          NUMBER(5),
  t_errormessage         VARCHAR2(2000),
  t_numb_document        VARCHAR2(15)
)
on commit preserve rows';
    execute immediate 'comment on table DACCOVERVALUE2_TMP
  is ''Для промежуточных расчетов в RSI_RsbAccOvervalue.pkb ''';
  end if;
end;

