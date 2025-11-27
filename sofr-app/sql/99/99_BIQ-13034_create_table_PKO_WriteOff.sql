-- создание картотеки 
declare
  v_tabledefinition varchar2(32000);
begin
  begin
    execute immediate 'drop table PKO_WriteOff';
  exception
    when others then
      null;
  end;
  execute immediate 'create table PKO_WriteOff(ID number(10),
                            DealId Number(10),
                            DealCode varchar2(10),
                            OperationId Number(10),
                            ClientId Number(10),
                            ClientCode varchar2(10),
                            Market varchar2(10),
                            SecurityId Number(10),
                            Qnty Number(32, 12),
                            StartWriteOffDate Date,
                            ExpirationDate Date,
                            IsLimitCorrected Char(1),
                            LimitCorrectionTimeStamp timestamp,
                            IsCanceled Char(1),
                            CancelationTimestamp timestamp,
                            IsCompleted Char(1),
                            CompletionTimestamp timestamp,
                            xml_from_diasoft clob,
                            guid varchar2(128),
                            PkoStatus integer,
                            CustodyOrderId varchar2(10),
                            foundCustodyOrderId char(1),
                            foundCOId number(10),
                            idContract number(10),
                            OperationTime varchar2(30),
                            OperationTimeOra timestamp,
                            OperType integer,
                            errList varchar2(200)
                            )';

  begin
    begin
      execute immediate 'drop sequence PKO_WriteOff_SEQ';
    exception
      when others then
        null;
    end;
    -- Create sequence 
    execute immediate 'create sequence PKO_WriteOff_SEQ minvalue 1 maxvalue 9999999999999999999999999999 start
      with 1 increment by 1 nocache';
  end;

  begin
    execute immediate 'CREATE OR REPLACE TRIGGER "PKO_WriteOff_T0_AINC" BEFORE INSERT OR UPDATE OF id ON PKO_WriteOff FOR EACH ROW DECLARE v_id INTEGER;
    BEGIN
      IF (:new.id = 0 OR :new.id IS NULL) THEN
        SELECT PKO_WriteOff_seq.nextval INTO :new.id FROM dual;
      ELSE
        select last_number
          into v_id
          from user_sequences
         where sequence_name = upper(''PKO_WriteOff_SEQ'');
        IF :new.id >= v_id THEN
          RAISE DUP_VAL_ON_INDEX;
        END IF;
      END IF;
    END;';
  end;

end;
