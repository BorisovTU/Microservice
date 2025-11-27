-- Create table
BEGIN
  EXECUTE IMMEDIATE 'create table FCT_HEDG_CHG
		    (
		      DT VARCHAR2(10),
		      CODE VARCHAR2(100),
		      DEAL_CODE VARCHAR2(100),
		      FINSTR_CODE VARCHAR2(60),
		      ASUDR_DEAL_CODE VARCHAR2(100),
		      PORTFOLIO_CODE VARCHAR2(100),
		      SUB_PORTF_CODE VARCHAR2(100),
		      CURRENCY_CURR_CODE_TXT VARCHAR2(3),
		      COST_ON_DATE VARCHAR2(50),
		      PREV_COST VARCHAR2(50),
		      CHG_AMOUNT VARCHAR2(50),
		      DEAL_KIND_CODE VARCHAR2(100),
		      HEDGE_REL_CODE VARCHAR2(100),
		      HEDG_BEGIN_DT VARCHAR2(10),
		      HEDG_END_DT VARCHAR2(10),
		      HEDG_TOOL_CODE VARCHAR2(100),
		      TOOL_CODE_SOFR VARCHAR2(100),
		      INC_ACC_CODE VARCHAR2(100),
		      INC_ACC_NUM VARCHAR2(30 BYTE),
		      DEC_ACC_CODE VARCHAR2(100),
		      DEC_ACC_NUM VARCHAR2(30 BYTE),
		      SYSMOMENT VARCHAR2(20),
		      EXT_FILE VARCHAR2(300),
		      REC_STATUS VARCHAR2(1)
		    )'  ;
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

-- Add comments to the table 
comment on table FCT_HEDG_CHG
  is 'Хеджирование. Изменения справедливой стоимости'/
-- Add comments to the columns 
comment on column FCT_HEDG_CHG.DT
  is 'Техническое поле. Дата учётного события в СИ'/
comment on column FCT_HEDG_CHG.CODE
  is 'Уникальный идентификатор записи в справочнике РСХБ. Хеджирование. Изменение СС'/
comment on column FCT_HEDG_CHG.DEAL_CODE
  is 'Уникальный идентификатор сделки ОХ в системе источнике.'/
comment on column FCT_HEDG_CHG.FINSTR_CODE
  is 'Код финансового инструмента.'/
comment on column FCT_HEDG_CHG.ASUDR_DEAL_CODE
  is 'Уникальный код сделки ОХ в АСУДР'/
comment on column FCT_HEDG_CHG.PORTFOLIO_CODE
  is 'Код портфеля в системе АСУДР'/
comment on column FCT_HEDG_CHG.SUB_PORTF_CODE
  is 'Код субпортфеля в системе АСУДР'/
comment on column FCT_HEDG_CHG.CURRENCY_CURR_CODE_TXT
  is 'Буквенный код валюты.'/
comment on column FCT_HEDG_CHG.COST_ON_DATE
  is 'Сумма СС на дату в рублях'/
comment on column FCT_HEDG_CHG.PREV_COST
  is 'Предыдущее значение СС  в рублях'/
comment on column FCT_HEDG_CHG.CHG_AMOUNT
  is 'Сумма изменения СС в рублях'/
comment on column FCT_HEDG_CHG.DEAL_KIND_CODE
  is 'Вид сделки ОХ'/
comment on column FCT_HEDG_CHG.HEDGE_REL_CODE
  is 'Код отношений хеджирования'/
comment on column FCT_HEDG_CHG.HEDG_BEGIN_DT
  is 'Дата начала отношений хеджирования'/
comment on column FCT_HEDG_CHG.HEDG_END_DT
  is 'Дата окончания отношений хеджированияv'/
comment on column FCT_HEDG_CHG.HEDG_TOOL_CODE
  is 'Код сделки ИХ в АСУДР.'/
comment on column FCT_HEDG_CHG.TOOL_CODE_SOFR
  is 'Код сделки ИХ в СОФР.'/
comment on column FCT_HEDG_CHG.INC_ACC_NUM
  is 'Номер счета для уменьшения СС'/
comment on column FCT_HEDG_CHG.INC_ACC_CODE
  is 'Код счета для уменьшения СС'/
comment on column FCT_HEDG_CHG.DEC_ACC_NUM
  is 'Номер счета для увеличения СС'/
comment on column FCT_HEDG_CHG.DEC_ACC_CODE
  is 'Код счета для увеличения СС'/
comment on column FCT_HEDG_CHG.SYSMOMENT
  is 'Техническое поле. Дата и время загрузки'/
comment on column FCT_HEDG_CHG.EXT_FILE
  is 'Техническое поле. Наименование файла выгрузки'/
comment on column FCT_HEDG_CHG.REC_STATUS
  is 'Техническое поле'/

-- Grant/Revoke object privileges 
grant select, insert, update, delete on FCT_HEDG_CHG to PUBLIC/
