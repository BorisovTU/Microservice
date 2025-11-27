-- Изменения по DEF-52640, изменение ddlclboavoir_dbt c логированием
DECLARE
  logID VARCHAR2(9) := 'DEF-52640';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Добавление поля в таблицу
  PROCEDURE AlterThisTable( p_Table IN varchar2, p_Field IN varchar2, p_Type IN varchar2, p_Default IN varchar2 )
  AS
  BEGIN
    LogIt('Изменение в '||p_Table||': добавление поля '||p_Field);
    EXECUTE IMMEDIATE 'ALTER TABLE '||p_Table||' ADD ('||p_Field||' '||p_Type||' '||p_Default||')';
    LogIt('Добавлено поле '||p_Field||' в таблицу '||p_Table);
  EXCEPTION
    WHEN OTHERS THEN
       LogIt('Ошибка добавления поля '||p_Field||' в таблицу '||p_Table);
  END;
BEGIN
  -- Добавление поля
  AlterThisTable('DDLCLBOAVOIR_DBT', 'T_CLIENTCONTRID', 'NUMBER(10,0)', 'DEFAULT 0');
  AlterThisTable('DDLCLBOAVOIR_DBT', 'T_PARTYID', 'NUMBER(10,0)', 'DEFAULT 0');
  AlterThisTable('DDLCLBOAVOIR_DBT', 'T_MARKETID', 'NUMBER(10,0)', 'DEFAULT 0');
  AlterThisTable('DDLCLBOAVOIR_DBT', 'T_PRICE', 'NUMBER(32,12)', 'DEFAULT 0');
  AlterThisTable('DDLCLBOAVOIR_DBT', 'T_COURSE', 'NUMBER(32,12)', 'DEFAULT 0');      -- рыночный курс ц/б
  AlterThisTable('DDLCLBOAVOIR_DBT', 'T_NKD', 'NUMBER(32,12)', 'DEFAULT 0');         -- курс НКД для одной ц/б
  AlterThisTable('DDLCLBOAVOIR_DBT', 'T_COURSEFI', 'NUMBER(10,0)', 'DEFAULT 0');     -- валюта цены
  AlterThisTable('DDLCLBOAVOIR_DBT', 'T_COURSECB', 'NUMBER(32,12)', 'DEFAULT 0');    -- курс ЦБ РФ для валюты номинала
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
