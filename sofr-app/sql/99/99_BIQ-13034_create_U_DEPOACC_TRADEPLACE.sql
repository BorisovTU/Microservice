declare
  v_tabledefinition varchar2(32000);
begin
  begin
    execute immediate 'drop table U_DEPOACC_TRADEPLACE';
  exception
    when others then
      null;
  end;
  execute immediate 'create table U_DEPOACC_TRADEPLACE
(
  t_depoacc_firstletter  CHAR(1),
  t_depoacc_middlenumber VARCHAR2(10),
  t_market               VARCHAR2(4),
  t_agreement_suffix     CHAR(2)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  )';

  -- Add comments to the table 
  execute immediate '  comment on table U_DEPOACC_TRADEPLACE is ''BIQ-13034 настроечной таблица для функции GetSubAgreementByDepoAccount''';
  -- Add comments to the columns 
  execute immediate '  comment on column U_DEPOACC_TRADEPLACE.t_depoacc_firstletter is ''первый символ в номере счета депо''';
  execute immediate '  comment on column U_DEPOACC_TRADEPLACE.t_depoacc_middlenumber is ''фрагмент счета депо''';
  execute immediate '  comment on column U_DEPOACC_TRADEPLACE.t_market is ''обозначение площадки: "MOEX", "SPB" или "OTC"''';
  execute immediate '  comment on column U_DEPOACC_TRADEPLACE.t_agreement_suffix is ''суффикс''';

  execute immediate 'create unique index U_DEPOACC_TRADEPLACE_IDX1 on U_DEPOACC_TRADEPLACE (T_DEPOACC_FIRSTLETTER, T_DEPOACC_MIDDLENUMBER, T_MARKET)
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  )';
end;
