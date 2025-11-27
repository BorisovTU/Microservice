declare
  vcnt      number;
begin
  select count(*) into vcnt from user_tables t where t.table_name = 'SOFR_IPS_MESSAGE';
  if vcnt = 0 then
    execute immediate 'create table SOFR_IPS_MESSAGE 
                         (  T_ID           Number(10)     not null, 
                            T_ServiceID    Number(5)      not null, 
                            T_Message      Blob           not null, 
                            T_FileName     Varchar2(255)  not null, 
                            T_Direction    Number(1), 
                            T_Status       Number(1)      not null, 
                            T_UserField1   Varchar2(255), 
                            T_UserField2   Varchar2(255), 
                            T_Flag1        Char(1), 
                            T_Flag2        Char(1))';
    execute immediate 'comment on column SOFR_IPS_MESSAGE.T_ID         is ''Автоматически генерируемый id записи''';
    execute immediate 'comment on column SOFR_IPS_MESSAGE.T_ServiceID  is ''Идентификатор сервиса''';
    execute immediate 'comment on column SOFR_IPS_MESSAGE.T_Message    is ''Сообщение''';
    execute immediate 'comment on column SOFR_IPS_MESSAGE.T_FileName   is ''Имя файла пакета''';
    execute immediate 'comment on column SOFR_IPS_MESSAGE.T_Direction  is ''Направление сообщения''';
    execute immediate 'comment on column SOFR_IPS_MESSAGE.T_Status     is ''Статус обработки сообщения''';
    execute immediate 'comment on column SOFR_IPS_MESSAGE.T_UserField1 is ''Пользовательское поле 1''';
    execute immediate 'comment on column SOFR_IPS_MESSAGE.T_UserField2 is ''Пользовательское поле 2''';
    execute immediate 'comment on column SOFR_IPS_MESSAGE.T_Flag1      is ''Пользовательское поле 1''';
    execute immediate 'comment on column SOFR_IPS_MESSAGE.T_Flag2      is ''Пользовательское поле 1''';
  end if;

  select count(*) into vcnt from user_sequences  where sequence_name = upper('SOFR_IPS_MESSAGE_SEQ');
  if vcnt = 0 then
    execute immediate  'CREATE SEQUENCE SOFR_IPS_MESSAGE_SEQ
                             START WITH 1
                             MAXVALUE 9999999999999999999999999999
                             MINVALUE 1';
  end if;

  execute immediate 'CREATE OR REPLACE TRIGGER "SOFR_IPS_MESSAGE_T0_AINC" 
                         BEFORE INSERT OR UPDATE OF T_ID ON SOFR_IPS_MESSAGE FOR EACH ROW
                        DECLARE
                         v_id INTEGER;
                        BEGIN
                         IF (:new.T_ID = 0 OR :new.T_ID IS NULL) THEN
                           SELECT SOFR_IPS_MESSAGE_SEQ.nextval INTO :new.T_ID FROM dual;
                         ELSE
                           select last_number into v_id from user_sequences where sequence_name = upper (''SOFR_IPS_MESSAGE_SEQ'');
                           IF :new.T_ID >= v_id THEN
                              RAISE DUP_VAL_ON_INDEX;
                           END IF;
                         END IF;
                        END;';

end;

