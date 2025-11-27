declare
    vcnt number;
    vsql varchar2(2000);
begin

   select count(*) into vcnt from user_sequences  where sequence_name = upper('DSCQINV_NOTIFY_SEQ');
   if vcnt = 0 then
      execute immediate  'CREATE SEQUENCE DSCQINV_NOTIFY_SEQ
                             START WITH 1
                             MAXVALUE 9999999999999999999999999999
                             MINVALUE 1';
   end if;
   
   select count(*) into vcnt from user_tables where table_name = upper('DSCQINV_NOTIFY_DBT');
   if vcnt = 0 then
      execute immediate  'create table DSCQINV_NOTIFY_DBT (
                              T_ID            NUMBER(10)            ,
                              T_PARTYID       NUMBER(10)            DEFAULT 0,
                              T_SFCONTRID     NUMBER(10)            DEFAULT 0,
                              T_DLCONTRID     NUMBER(10)            DEFAULT 0,
                              T_LASTREGDATE   DATE                  DEFAULT TO_DATE(''01.01.0001'',''DD.MM.YYYY''),
                              T_CONTROLDATE   DATE                  DEFAULT TO_DATE(''01.01.0001'',''DD.MM.YYYY''),
                              T_MESSAGETYPEID NUMBER(10)            DEFAULT 0,
                              T_SENDDATE      DATE                  DEFAULT TO_DATE(''01.01.0001'',''DD.MM.YYYY''),
                              T_FACTSENDDATE  DATE                  DEFAULT TO_DATE(''01.01.0001'',''DD.MM.YYYY''),
                              T_SENDSTATE     NUMBER(5)             DEFAULT 0,
                              T_ERROR         VARCHAR2(2000 CHAR)   DEFAULT CHR(1),
                              T_PATH          VARCHAR2(2000 CHAR)   DEFAULT CHR(1),
                              T_MESSAGEID     NUMBER(10)            DEFAULT 0,
                              T_CREATEDATETIME DATE                 DEFAULT TO_DATE(''01.01.0001'',''DD.MM.YYYY''),
                              T_UPDATEDATETIME DATE                 DEFAULT sysdate

                            )';
      execute immediate 'comment on table DSCQINV_NOTIFY_DBT is ''Уведомления по реестру квал.инвесторов''';
      execute immediate 'COMMENT ON COLUMN DSCQINV_NOTIFY_DBT.T_LASTREGDATE IS ''Дата последней регистрации в качестве КИ''';
      execute immediate 'COMMENT ON COLUMN DSCQINV_NOTIFY_DBT.T_CONTROLDATE IS ''Дата истечения статуса''';
      execute immediate 'COMMENT ON COLUMN DSCQINV_NOTIFY_DBT.T_MESSAGETYPEID IS ''ID типа уведомления''';
      execute immediate 'COMMENT ON COLUMN DSCQINV_NOTIFY_DBT.T_SENDDATE IS ''Дата отправки уведомления''';
      execute immediate 'COMMENT ON COLUMN DSCQINV_NOTIFY_DBT.T_FACTSENDDATE IS ''Фактическое время отправки''';
      execute immediate 'COMMENT ON COLUMN DSCQINV_NOTIFY_DBT.T_SENDSTATE IS ''Статус отправки: 0-ожидает, 1-отправлено, 100-ошибка''';
      execute immediate 'COMMENT ON COLUMN DSCQINV_NOTIFY_DBT.T_ERROR IS ''Текст ошибки''';
      execute immediate 'COMMENT ON COLUMN DSCQINV_NOTIFY_DBT.T_PATH IS ''Путь к файлу''';
      execute immediate 'COMMENT ON COLUMN DSCQINV_NOTIFY_DBT.T_MESSAGEID IS ''ID созданного сообщения''';
      execute immediate 'COMMENT ON COLUMN DSCQINV_NOTIFY_DBT.T_CREATEDATETIME IS ''Дата и время создания записи''';
      execute immediate 'COMMENT ON COLUMN DSCQINV_NOTIFY_DBT.T_UPDATEDATETIME IS ''Дата и время последнего обновления записи''';

      execute immediate 'CREATE INDEX DSCQINV_NOTIFY_DBT_IDX0 ON DSCQINV_NOTIFY_DBT (T_PARTYID,T_SFCONTRID,T_LASTREGDATE,T_CONTROLDATE)';
   end if;
   
   execute immediate 'CREATE OR REPLACE TRIGGER "DSCQINV_NOTIFY_T0_AINC" 
                         BEFORE INSERT OR UPDATE OF T_ID ON DSCQINV_NOTIFY_DBT FOR EACH ROW
                        DECLARE
                         v_id INTEGER;
                        BEGIN
                         IF (:new.T_ID = 0 OR :new.T_ID IS NULL) THEN
                           SELECT DSCQINV_NOTIFY_SEQ.nextval INTO :new.T_ID FROM dual;
                         ELSE
                           select last_number into v_id from user_sequences where sequence_name = upper (''DSCQINV_NOTIFY_SEQ'');
                           IF :new.T_ID >= v_id THEN
                              RAISE DUP_VAL_ON_INDEX;
                           END IF;
                         END IF;
                        END;';
   
end;    