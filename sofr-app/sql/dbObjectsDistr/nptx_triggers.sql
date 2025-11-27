-- триггеры для построения связей НУ

--Пакет для хранения списков изменяемых объектов и семафоров для триггеров лотов
CREATE OR REPLACE PACKAGE RSI_TRG_DNPTXLOT_DBT IS

   v_UpdateBegLotID BOOLEAN    := False;

   v_ID_Operation   NUMBER(10) := 0;
   v_ID_Step        NUMBER(5)  := 0;

   v_IsRestoreBC    BOOLEAN    := False;

   v_IsCreateLots   BOOLEAN    := False;

END RSI_TRG_DNPTXLOT_DBT;
/

CREATE OR REPLACE TRIGGER DNPTXLOT_DBT_TOTAL 
  FOR INSERT OR UPDATE OR DELETE ON DNPTXLOT_DBT 
COMPOUND TRIGGER

  TYPE tp_nptxbc is table of dnptxbc_dbt%rowtype index by pls_integer;

  arr_nptxbc tp_nptxbc;

  ind pls_integer := 0;

/*  BEFORE STATEMENT IS
  BEGIN

  END BEFORE STATEMENT; */

  BEFORE EACH ROW IS
  BEGIN

    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN
      RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'Триггер DNPTXLOT_DBT_TOTAL' );

      IF INSERTING THEN

        IF (RSI_NPTX.IsSale(:new.t_Kind) = 0) then -- покупки
           :NEW.T_ISFREE     := RSI_NPTX.GetIsFree(:NEW.T_AMOUNT, :NEW.T_SALE, :NEW.T_RETFLAG, :NEW.T_INACC,
                                                   :NEW.T_BLOCKED, :NEW.T_BUYDATE, :NEW.T_SALEDATE);
           :NEW.T_ORDFORSALE := RSI_NPTX.GetBuyOrderForSale(:NEW.T_KIND);
           :NEW.T_ORDFORREPO := RSI_NPTX.GetBuyOrderForRepo(:NEW.T_KIND);
           :NEW.T_VIRGIN     := :NEW.T_AMOUNT;
        ELSE
           :NEW.T_ISFREE     := CHR(0);
           :NEW.T_ORDFORSALE := 0;
           :NEW.T_ORDFORREPO := 0;
        END IF;

        IF :NEW.t_DealCodeTS = CHR(1) then
           :NEW.t_DealCodeTS := :new.t_DealCode;
        END IF;

        IF :NEW.t_SortCode = CHR(1) then
          :NEW.t_SortCode := LPAD(RTRIM(:NEW.t_DealCodeTS), 30, ' ' );
        END IF;

        IF :NEW.t_BegBuyDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') THEN
          :NEW.t_BegBuyDate     := :NEW.t_BuyDate;
        END IF;

        IF :NEW.t_BegSaleDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') THEN
          :NEW.t_BegSaleDate    := :NEW.t_SaleDate;
        END IF;

        IF ((:NEW.t_Type = RSI_NPTXC.NPTXDEAL_REAL) AND (:NEW.t_BegLotID = 0)) THEN
          RSI_TRG_DNPTXLOT_DBT.v_UpdateBegLotID := True;
        END IF;

      END IF;


      IF UPDATING AND :NEW.T_KIND IN (1, 4, 6) AND (:OLD.T_SALE <> :NEW.T_SALE OR
                                                    :OLD.T_RETFLAG <> :NEW.T_RETFLAG OR
                                                    :OLD.T_INACC <> :NEW.T_INACC OR
                                                    :OLD.T_BLOCKED <> :NEW.T_BLOCKED OR
                                                    :OLD.T_BUYDATE <> :NEW.T_BUYDATE OR
                                                    :OLD.T_SALEDATE <> :NEW.T_SALEDATE)
      THEN

       :NEW.T_ISFREE := RSI_NPTX.GetIsFree(:NEW.T_AMOUNT, :NEW.T_SALE, :NEW.T_RETFLAG, :NEW.T_INACC,
                                           :NEW.T_BLOCKED, :NEW.T_BUYDATE, :NEW.T_SALEDATE);
      END IF;
    END IF;

  END BEFORE EACH ROW;

  AFTER EACH ROW IS
    v_BCID     NUMBER(10) := 0;
    v_BegDate  DATE := TO_DATE('01.01.0001', 'DD.MM.YYYY');

  BEGIN

    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN

      IF RSI_TRG_DNPTXLOT_DBT.v_ID_Operation > 0 THEN

        ind := ind + 1;

        arr_nptxbc(ind).t_BCID         := 0;
        arr_nptxbc(ind).t_ObjKind      := RSI_NPTXC.NPTXBC_OBJKIND_LOT; --Лот
        IF DELETING THEN
          arr_nptxbc(ind).t_ObjID      := :OLD.T_ID;
        ELSE
          arr_nptxbc(ind).t_ObjID      := :NEW.T_ID;
        END IF;
        arr_nptxbc(ind).t_ID_Operation := RSI_TRG_DNPTXLOT_DBT.v_ID_Operation;
        arr_nptxbc(ind).t_ID_Step      := RSI_TRG_DNPTXLOT_DBT.v_ID_Step;

        IF INSERTING THEN
          arr_nptxbc(ind).t_Action       := RSI_NPTXC.NPTXBC_ACTION_CREATE; --Создание
          arr_nptxbc(ind).t_BackObjID    := 0;
        ELSIF UPDATING OR DELETING THEN
          IF UPDATING THEN
            arr_nptxbc(ind).t_Action     := RSI_NPTXC.NPTXBC_ACTION_UPDATE; --Обновление
          ELSE
            arr_nptxbc(ind).t_Action     := RSI_NPTXC.NPTXBC_ACTION_DELETE; --Удаление
          END IF;

          INSERT INTO DNPTXLOTBC_DBT(T_LOTBCID
                           , T_ID
                           , T_DOCKIND
                           , T_DOCID
                           , T_DEALDATE
                           , T_DEALTIME
                           , T_DEALCODE
                           , T_DEALCODETS
                           , T_CLIENT
                           , T_CONTRACT
                           , T_FIID
                           , T_KIND
                           , T_TYPE
                           , T_BUYID
                           , T_REALID
                           , T_AMOUNT
                           , T_SALE
                           , T_VIRGIN
                           , T_COMPAMOUNT
                           , T_PRICE
                           , T_PRICEFIID
                           , T_TOTALCOST
                           , T_NKD
                           , T_BUYDATE
                           , T_SALEDATE
                           , T_SORTCODE
                           , T_RETFLAG
                           , T_ISFREE
                           , T_BEGLOTID
                           , T_CHILDID
                           , T_BEGBUYDATE
                           , T_BEGSALEDATE
                           , T_INACC
                           , T_BLOCKED
                           , T_ORIGIN
                           , T_GOID
                           , T_CLGOID
                           , T_OLDDATE
                           , T_ORDFORSALE
                           , T_ORDFORREPO
                           , T_RQID
                           , T_NOTCOUNTEDONIIS)
                    VALUES(0
                           , :OLD.T_ID
                           , :OLD.T_DOCKIND
                           , :OLD.T_DOCID
                           , :OLD.T_DEALDATE
                           , :OLD.T_DEALTIME
                           , :OLD.T_DEALCODE
                           , :OLD.T_DEALCODETS
                           , :OLD.T_CLIENT
                           , :OLD.T_CONTRACT
                           , :OLD.T_FIID
                           , :OLD.T_KIND
                           , :OLD.T_TYPE
                           , :OLD.T_BUYID
                           , :OLD.T_REALID
                           , :OLD.T_AMOUNT
                           , :OLD.T_SALE
                           , :OLD.T_VIRGIN
                           , :OLD.T_COMPAMOUNT
                           , :OLD.T_PRICE
                           , :OLD.T_PRICEFIID
                           , :OLD.T_TOTALCOST
                           , :OLD.T_NKD
                           , :OLD.T_BUYDATE
                           , :OLD.T_SALEDATE
                           , :OLD.T_SORTCODE
                           , :OLD.T_RETFLAG
                           , :OLD.T_ISFREE
                           , :OLD.T_BEGLOTID
                           , :OLD.T_CHILDID
                           , :OLD.T_BEGBUYDATE
                           , :OLD.T_BEGSALEDATE
                           , :OLD.T_INACC
                           , :OLD.T_BLOCKED
                           , :OLD.T_ORIGIN
                           , :OLD.T_GOID
                           , :OLD.T_CLGOID
                           , :OLD.T_OLDDATE
                           , :OLD.T_ORDFORSALE
                           , :OLD.T_ORDFORREPO
                           , :OLD.T_RQID
                           , :OLD.T_NOTCOUNTEDONIIS) RETURNING T_LOTBCID INTO v_BCID;

          IF v_BCID > 0 THEN
            arr_nptxbc(ind).t_BackObjID  := v_BCID;
          ELSE
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Ошибка при сохранении лота в истории' );
            RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20642,'');
          END IF;
        END IF;

      END IF;

      IF (INSERTING OR (UPDATING AND :OLD.T_INACC <> :NEW.T_INACC)) AND :NEW.T_KIND IN (1, 4, 6) AND :NEW.T_INACC = CHR(88) AND :NEW.T_RETFLAG <> CHR(88) THEN

        INSERT INTO dnptxts_dbt( T_CLIENT   ,
                                 T_CONTRACT ,
                                 T_FIID     ,
                                 T_BUYID    ,
                                 T_SALEID   ,
                                 T_TYPE     ,
                                 T_BEGDATE  ,
                                 T_ENDDATE  ,
                                 T_AMOUNT
                               )
                         VALUES(
                                 :NEW.t_Client,                       --T_CLIENT
                                 :NEW.t_Contract,                     --T_CONTRACT
                                 :NEW.t_FIID,                         --T_FIID
                                 :NEW.t_ID,                           --T_BUYID
                                 0,                                   --T_SALEID
                                 RSI_NPTXC.NPTXTS_REST,               --T_TYPE
                                 :NEW.t_BuyDate,                      --T_BEGDATE
                                 TO_DATE('01.01.0001', 'DD.MM.YYYY'), --T_ENDDATE
                                 :NEW.t_Amount                        --T_AMOUNT
                               );

        IF( :NEW.t_KIND = 1 ) THEN
           v_BegDate := :NEW.t_BuyDate;
           --Если в зачислении указана дата начала срока непрерывного владения,
           -- то в качестве даты начала в записи <покупки> используем именно ее, а не дату первоначальной покупки
           if( :NEW.t_DocKind = RSI_NPTXC.DL_AVRWRT )then
              begin
                 select decode(tick.t_TaxOwnBegDate,TO_DATE('01.01.0001', 'DD.MM.YYYY'),:NEW.t_BuyDate,tick.t_TaxOwnBegDate) into v_BegDate
                   from ddl_tick_dbt tick
                  where tick.t_DealID = :NEW.t_DocID;
              exception
              when NO_DATA_FOUND then v_BegDate := :NEW.t_BuyDate;
              end;
           end if;

           INSERT INTO dnptxts_dbt( T_CLIENT   ,
                                    T_CONTRACT ,
                                    T_FIID     ,
                                    T_BUYID    ,
                                    T_SALEID   ,
                                    T_TYPE     ,
                                    T_BEGDATE  ,
                                    T_ENDDATE  ,
                                    T_AMOUNT
                                  )
                            VALUES(
                                    :NEW.t_Client,                       --T_CLIENT
                                    :NEW.t_Contract,                     --T_CONTRACT
                                    :NEW.t_FIID,                         --T_FIID
                                    :NEW.t_ID,                           --T_BUYID
                                    0,                                   --T_SALEID
                                    RSI_NPTXC.NPTXTS_BUY,                --T_TYPE
                                    v_BegDate,                           --T_BEGDATE
                                    TO_DATE('01.01.0001', 'DD.MM.YYYY'), --T_ENDDATE
                                    :NEW.t_Amount                        --T_AMOUNT
                                  );
        END IF;

      END IF;


      IF UPDATING AND :OLD.T_RETFLAG <> :NEW.T_RETFLAG AND :NEW.T_KIND IN (3, 5) AND :NEW.T_RETFLAG = CHR(88) THEN
        RSI_NPTX.UpdateTSByDirectRepo (:NEW.T_ID, :NEW.T_BUYDATE);
      END IF;
    END IF;

  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN
    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN

      IF INSERTING THEN
        IF RSI_TRG_DNPTXLOT_DBT.v_UpdateBegLotID = True THEN
           UPDATE dnptxlot_dbt
              SET t_BegLotID = t_ID
            WHERE t_BegLotID = 0
              AND t_Type = RSI_NPTXC.NPTXDEAL_REAL;

           RSI_TRG_DNPTXLOT_DBT.v_UpdateBegLotID := False;
        END IF;
      END IF;


      FORALL counter in 1..arr_nptxbc.count()
       INSERT INTO dnptxbc_dbt
       VALUES arr_nptxbc(counter);
    END IF;

  END AFTER STATEMENT;

END DNPTXLOT_DBT_TOTAL;
/

CREATE OR REPLACE TRIGGER DNPTXLNK_DBT_TOTAL
  FOR INSERT OR UPDATE OR DELETE ON DNPTXLNK_DBT 
COMPOUND TRIGGER

  TYPE tp_nptxbc    IS TABLE OF dnptxbc_dbt%rowtype INDEX BY pls_integer;
  TYPE tp_DelLinkID IS TABLE OF dnptxlnk_dbt.t_ID%TYPE;

  arr_nptxbc  tp_nptxbc;
  arr_DelLinkID tp_DelLinkID := tp_DelLinkID();

  ind pls_integer := 0;
  indlnk pls_integer := 0;

/*  BEFORE STATEMENT IS
  BEGIN

  END BEFORE STATEMENT; */

  BEFORE EACH ROW IS
    v_Virgin NUMBER := 0;
    v_ShortCorrection NUMBER := 0;
    v_DA     NUMBER;
    v_SDA    NUMBER;
    v_DV     NUMBER;

  BEGIN

    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN
      IF INSERTING THEN

        RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'Перед вставкой связи t_id = '||:new.t_ID||' T_TYPE='||:new.T_TYPE||' T_AMOUNT='||:new.T_AMOUNT||' T_BUYID='||:new.T_BUYID||' T_SALEID='||:new.T_SALEID );
        BEGIN
           SELECT t_Virgin INTO v_Virgin
             FROM DNPTXLOT_DBT
            WHERE T_ID = :new.t_BuyID;
        EXCEPTION
        WHEN NO_DATA_FOUND then v_Virgin := 0;
        END;

        IF v_Virgin > :new.t_Amount THEN
           v_Virgin := :new.t_Amount;
        END IF;

        :new.t_Virgin := v_Virgin;

        update dnptxlot_dbt lot
           set lot.t_Virgin = lot.t_Virgin - :new.t_Virgin
         where lot.t_ID = :new.t_BuyID;

        if( (:new.t_Type in ( RSI_NPTXC.NPTXLNK_DELIVER, RSI_NPTXC.NPTXLNK_OPPOS, RSI_NPTXC.NPTXLNK_CLPOS )
            ) OR
            (:new.t_RetFlag <> CHR(88) AND :new.t_Type in ( RSI_NPTXC.NPTXLNK_REPO, RSI_NPTXC.NPTXLNK_SUBSTREPO )
            )
          ) then

           update dnptxlot_dbt lot
              set lot.t_Sale = lot.t_Sale + :new.t_Amount
            where lot.t_ID = :new.t_BuyID;
        end if;


        if( (:new.t_Type in ( RSI_NPTXC.NPTXLNK_DELIVER, RSI_NPTXC.NPTXLNK_OPPOS )
            ) OR
            (:new.t_RetFlag <> CHR(88) AND :new.t_Type = RSI_NPTXC.NPTXLNK_REPO
            )
          ) then

           update dnptxlot_dbt lot
              set lot.t_Sale = lot.t_Sale + :new.t_Amount
            where lot.t_ID = :new.t_SaleID;
        end if;

        if( (:new.t_Type = RSI_NPTXC.NPTXLNK_CLPOS
            ) OR
            (:new.t_RetFlag <> CHR(88) AND :new.t_Type = RSI_NPTXC.NPTXLNK_SUBSTREPO
            )
          ) then

           update dnptxlot_dbt lot
              set lot.t_Sale = lot.t_Sale - :new.t_Amount
            where lot.t_ID = :new.t_SourceID;
        end if;

        RSI_NPTX.UpdateTSByLink (:new.T_TYPE, :new.T_BUYID, :new.T_SALEID, :new.T_SOURCEID, :new.T_DATE, :new.T_AMOUNT);

      END IF;

      IF UPDATING THEN
        IF :OLD.T_AMOUNT <> :NEW.T_AMOUNT THEN

          v_DA  := :new.t_Amount - :new.t_Short - :old.t_Amount + :old.t_Short;
          v_SDA := :new.t_Amount - :old.t_Amount;

          if (v_SDA > 0) then

             BEGIN
                SELECT t_Virgin INTO v_DV
                  FROM DNPTXLOT_DBT
                 WHERE T_ID = :new.t_BuyID;
             EXCEPTION
             WHEN NO_DATA_FOUND then v_DV := 0;
             END;

             IF v_DV > v_SDA THEN
                v_DV := v_SDA;
             END IF;

             :new.t_Virgin := :old.t_Virgin + v_DV;

             update dnptxlot_dbt
                set t_Virgin = t_Virgin - v_DV
              where t_ID = :new.t_BuyID;
          else
             RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Частичный откат связи в текущей версии не предусмотрен' );
             RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20626,'');
          end if;

          if( (:new.t_Type in ( RSI_NPTXC.NPTXLNK_DELIVER, RSI_NPTXC.NPTXLNK_OPPOS, RSI_NPTXC.NPTXLNK_CLPOS )
              ) OR
              (:new.t_RetFlag <> CHR(88) AND :new.t_Type in ( RSI_NPTXC.NPTXLNK_REPO, RSI_NPTXC.NPTXLNK_SUBSTREPO )
              )
            ) then

            update dnptxlot_dbt
               set t_Sale = t_Sale + v_DA
             where t_ID = :new.t_BuyID;
          end if;

          if( (:new.t_Type in ( RSI_NPTXC.NPTXLNK_DELIVER, RSI_NPTXC.NPTXLNK_OPPOS )
              ) OR
              (:new.t_RetFlag <> CHR(88) AND :new.t_Type = RSI_NPTXC.NPTXLNK_REPO
              )
            ) then

            update dnptxlot_dbt
               set t_Sale = t_Sale + v_SDA
             where t_ID = :new.t_SaleID;
          end if;


          if( (:new.t_Type = RSI_NPTXC.NPTXLNK_CLPOS
              ) OR
              (:new.t_RetFlag <> CHR(88) AND :new.t_Type = RSI_NPTXC.NPTXLNK_SUBSTREPO
              )
            ) then
            update dnptxlot_dbt
               set t_Sale = t_Sale - v_DA
             where t_ID = :new.t_SourceID;
          end if;

          if( v_DA >= 0 ) then
             RSI_NPTX.UpdateTSByLink (:new.T_TYPE, :new.T_BUYID, :new.T_SALEID, :new.T_SOURCEID, :new.T_DATE, v_DA);
          end if;

        END IF;

      END IF;

      IF DELETING THEN

        update dnptxlot_dbt
           set t_Virgin = t_Virgin + :old.t_Virgin
         where t_ID = :old.t_BuyID;

        BEGIN
           SELECT NVL(sum(t_Short), 0) INTO v_ShortCorrection
             FROM DNPTXLS_DBT
            WHERE T_PARENTID = :old.t_ID;
        EXCEPTION
        WHEN NO_DATA_FOUND then v_ShortCorrection := 0;
        END;

        if( (:old.t_Type in ( RSI_NPTXC.NPTXLNK_DELIVER, RSI_NPTXC.NPTXLNK_OPPOS, RSI_NPTXC.NPTXLNK_CLPOS )
            ) OR
            (:old.t_RetFlag <> CHR(88) AND :old.t_Type in ( RSI_NPTXC.NPTXLNK_REPO, RSI_NPTXC.NPTXLNK_SUBSTREPO )
            )
          ) then

           update dnptxlot_dbt
              set t_Sale = t_Sale - :old.t_Amount + :old.t_Short - v_ShortCorrection
            where t_ID = :old.t_BuyID;
        end if;

        if( (:old.t_Type in ( RSI_NPTXC.NPTXLNK_DELIVER, RSI_NPTXC.NPTXLNK_OPPOS )
            ) OR
            (:old.t_RetFlag <> CHR(88) AND :old.t_Type = RSI_NPTXC.NPTXLNK_REPO
            )
          ) then

           update dnptxlot_dbt
              set t_Sale = t_Sale - :old.t_Amount
            where t_ID = :old.t_SaleID;
        end if;


        if( (:old.t_Type = RSI_NPTXC.NPTXLNK_CLPOS
            ) OR
            (:old.t_RetFlag <> CHR(88) AND :old.t_Type = RSI_NPTXC.NPTXLNK_SUBSTREPO
            )
          ) then

           update dnptxlot_dbt
              set t_Sale = t_Sale + :old.t_Amount - :old.t_Short + v_ShortCorrection
            where t_ID = :old.t_SourceID;
        end if;

        arr_DelLinkID.extend;
        arr_DelLinkID(arr_DelLinkID.LAST) := :old.t_ID;

      END IF;
    END IF;

  END BEFORE EACH ROW;

  AFTER EACH ROW IS
    v_BCID     NUMBER(10) := 0;
    v_EndDate  DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
  BEGIN
    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN

      IF RSI_TRG_DNPTXLOT_DBT.v_ID_Operation > 0 THEN

        ind := ind + 1;

        arr_nptxbc(ind).t_BCID         := 0;
        arr_nptxbc(ind).t_ObjKind      := RSI_NPTXC.NPTXBC_OBJKIND_LNK; --Связь
        IF DELETING THEN
          arr_nptxbc(ind).t_ObjID      := :OLD.T_ID;
        ELSE
          arr_nptxbc(ind).t_ObjID      := :NEW.T_ID;
        END IF;
        arr_nptxbc(ind).t_ID_Operation := RSI_TRG_DNPTXLOT_DBT.v_ID_Operation;
        arr_nptxbc(ind).t_ID_Step      := RSI_TRG_DNPTXLOT_DBT.v_ID_Step;

        IF INSERTING THEN
          arr_nptxbc(ind).t_Action       := RSI_NPTXC.NPTXBC_ACTION_CREATE; --Создание
          arr_nptxbc(ind).t_BackObjID    := 0;
        ELSIF UPDATING OR DELETING THEN
          IF UPDATING THEN
            arr_nptxbc(ind).t_Action     := RSI_NPTXC.NPTXBC_ACTION_UPDATE; --Обновление
          ELSE
            arr_nptxbc(ind).t_Action     := RSI_NPTXC.NPTXBC_ACTION_DELETE; --Удаление
          END IF;

          INSERT INTO DNPTXLNKBC_DBT(T_LNKBCID
                                   , T_ID
                                   , T_CLIENT
                                   , T_CONTRACT
                                   , T_FIID
                                   , T_BUYID
                                   , T_SALEID
                                   , T_SOURCEID
                                   , T_TYPE
                                   , T_DATE
                                   , T_AMOUNT
                                   , T_SHORT
                                   , T_VIRGIN
                                   , T_RETFLAG
                                   , T_PRIVAMOUNT)
                    VALUES(0
                           , :OLD.T_ID
                           , :OLD.T_CLIENT
                           , :OLD.T_CONTRACT
                           , :OLD.T_FIID
                           , :OLD.T_BUYID
                           , :OLD.T_SALEID
                           , :OLD.T_SOURCEID
                           , :OLD.T_TYPE
                           , :OLD.T_DATE
                           , :OLD.T_AMOUNT
                           , :OLD.T_SHORT
                           , :OLD.T_VIRGIN
                           , :OLD.T_RETFLAG
                           , :OLD.T_PRIVAMOUNT) RETURNING T_LNKBCID INTO v_BCID;

          IF v_BCID > 0 THEN
            arr_nptxbc(ind).t_BackObjID  := v_BCID;
          ELSE
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Ошибка при сохранении связи в истории' );
            RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20642,'');
          END IF;
        END IF;

      END IF;

      IF UPDATING AND :OLD.T_RETFLAG <> :NEW.T_RETFLAG AND :NEW.t_Type IN (2, 3) THEN

        RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'Триггер DNPTXLNK_DBT_TOTAL: После апдейта T_RETFLAG в связи t_id = '||:new.t_ID );

        if( :new.t_RetFlag = CHR(88) and :old.t_RetFlag = CHR(0) ) then

          update dnptxlot_dbt
             set t_Sale = t_Sale - :new.t_Amount + :new.t_Short
           where t_ID = :new.t_BuyID;

          update dnptxlot_dbt
             set t_Sale = t_Sale - :new.t_Amount
           where t_ID = :new.t_SaleID;

          if :new.t_Type = RSI_NPTXC.NPTXLNK_SUBSTREPO then
             update dnptxlot_dbt
                set t_Sale = t_Sale + :new.t_Amount - :new.t_Short
              where t_ID = :new.t_SourceID;
          end if;

          if :new.t_Type = RSI_NPTXC.NPTXLNK_SUBSTREPO or :new.t_Type = RSI_NPTXC.NPTXLNK_REPO then

             begin
               select t_BuyDate into v_EndDate
                 from dnptxlot_dbt
                where t_id = :new.t_SaleID;
             exception
               when NO_DATA_FOUND then NULL;
             end;

             RSI_NPTX.UpdateTSBuyByLink(:NEW.t_TYPE, :NEW.T_BUYID, :NEW.T_SALEID, :NEW.T_DATE, v_EndDate, :NEW.T_AMOUNT);

          end if;

        elsif( :new.t_RetFlag = CHR(0) and :old.t_RetFlag = CHR(88) ) then

          update dnptxlot_dbt
             set t_Sale = t_Sale + :new.t_Amount - :new.t_Short
           where t_ID = :new.t_BuyID;

          update dnptxlot_dbt
             set t_Sale = t_Sale + :new.t_Amount
           where t_ID = :new.t_SaleID;

          if :new.t_Type = RSI_NPTXC.NPTXLNK_SUBSTREPO then
             update dnptxlot_dbt
                set t_Sale = t_Sale - :new.t_Amount + :new.t_Short
              where t_ID = :new.t_SourceID;
          end if;

        end if;

      END IF;
    END IF;

  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN
    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN
      IF DELETING THEN

        IF arr_DelLinkID.count > 0 THEN

          FORALL i in arr_DelLinkID.first .. arr_DelLinkID.last
            DELETE FROM dnptxls_dbt
             WHERE t_ChildID = arr_DelLinkID(i);

        END IF;

      END IF;

      FORALL counter IN 1..arr_nptxbc.count()
       INSERT INTO dnptxbc_dbt
       VALUES arr_nptxbc(counter);
    END IF;

  END AFTER STATEMENT;

END DNPTXLNK_DBT_TOTAL;
/

CREATE OR REPLACE TRIGGER DNPTXLS_DBT_TOTAL
  FOR INSERT OR UPDATE OR DELETE ON DNPTXLS_DBT 
COMPOUND TRIGGER

  TYPE tp_nptxbc is table of dnptxbc_dbt%rowtype index by pls_integer;

  arr_nptxbc tp_nptxbc;

  ind pls_integer := 0;

/*  BEFORE STATEMENT IS
  BEGIN

  END BEFORE STATEMENT; */

/*  BEFORE EACH ROW IS
  BEGIN

  END BEFORE EACH ROW; */

  AFTER EACH ROW IS
    v_BCID     NUMBER(10) := 0;
    v_EndDate  DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
  BEGIN
    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN

      IF RSI_TRG_DNPTXLOT_DBT.v_ID_Operation > 0 THEN

        ind := ind + 1;

        arr_nptxbc(ind).t_BCID         := 0;
        arr_nptxbc(ind).t_ObjKind      := RSI_NPTXC.NPTXBC_OBJKIND_LS; --Перевешивание связи
        IF DELETING THEN
          arr_nptxbc(ind).t_ObjID      := :OLD.T_ID;
        ELSE
          arr_nptxbc(ind).t_ObjID      := :NEW.T_ID;
        END IF;
        arr_nptxbc(ind).t_ID_Operation := RSI_TRG_DNPTXLOT_DBT.v_ID_Operation;
        arr_nptxbc(ind).t_ID_Step      := RSI_TRG_DNPTXLOT_DBT.v_ID_Step;

        IF INSERTING THEN
          arr_nptxbc(ind).t_Action       := RSI_NPTXC.NPTXBC_ACTION_CREATE; --Создание
          arr_nptxbc(ind).t_BackObjID    := 0;
        ELSIF UPDATING OR DELETING THEN
          IF UPDATING THEN
            arr_nptxbc(ind).t_Action     := RSI_NPTXC.NPTXBC_ACTION_UPDATE; --Обновление
          ELSE
            arr_nptxbc(ind).t_Action     := RSI_NPTXC.NPTXBC_ACTION_DELETE; --Удаление
          END IF;

          INSERT INTO DNPTXLSBC_DBT(T_LSBCID
                                  , T_CHILDID
                                  , T_PARENTID
                                  , T_SHORT
                                  , T_ID)
                    VALUES(0
                           , :OLD.T_CHILDID
                           , :OLD.T_PARENTID
                           , :OLD.T_SHORT
                           , :OLD.T_ID) RETURNING T_LSBCID INTO v_BCID;

          IF v_BCID > 0 THEN
            arr_nptxbc(ind).t_BackObjID  := v_BCID;
          ELSE
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Ошибка при сохранении информации о перевешивании связи в истории' );
            RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20642,'');
          END IF;
        END IF;

      END IF;

      if INSERTING then

         update dnptxlnk_dbt lnk
            set lnk.t_Short = lnk.t_Short + :new.t_Short
          where lnk.t_ID = :new.t_ParentID;

      elsif DELETING then

         update dnptxlnk_dbt lnk
            set lnk.t_Short = lnk.t_Short - :old.t_Short
          where lnk.t_ID = :old.t_ParentID;
      end if;
    END IF;

  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN

    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN
      FORALL counter IN 1..arr_nptxbc.count()
       INSERT INTO dnptxbc_dbt
       VALUES arr_nptxbc(counter);
    END IF;

  END AFTER STATEMENT;

END DNPTXLS_DBT_TOTAL;
/

CREATE OR REPLACE TRIGGER DNPTXGO_DBT_TOTAL
  FOR INSERT OR UPDATE OR DELETE ON DNPTXGO_DBT 
COMPOUND TRIGGER

  TYPE tp_nptxbc is table of dnptxbc_dbt%rowtype index by pls_integer;

  arr_nptxbc tp_nptxbc;

  ind pls_integer := 0;

/*  BEFORE STATEMENT IS
  BEGIN

  END BEFORE STATEMENT; */

/*  BEFORE EACH ROW IS
  BEGIN

  END BEFORE EACH ROW; */

  AFTER EACH ROW IS
    v_BCID     NUMBER(10) := 0;
    v_EndDate  DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
  BEGIN
    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN

      IF RSI_TRG_DNPTXLOT_DBT.v_ID_Operation > 0 THEN

        ind := ind + 1;

        arr_nptxbc(ind).t_BCID         := 0;
        arr_nptxbc(ind).t_ObjKind      := RSI_NPTXC.NPTXBC_OBJKIND_GO; --Глобальная операция
        IF DELETING THEN
          arr_nptxbc(ind).t_ObjID      := :OLD.T_ID;
        ELSE
          arr_nptxbc(ind).t_ObjID      := :NEW.T_ID;
        END IF;
        arr_nptxbc(ind).t_ID_Operation := RSI_TRG_DNPTXLOT_DBT.v_ID_Operation;
        arr_nptxbc(ind).t_ID_Step      := RSI_TRG_DNPTXLOT_DBT.v_ID_Step;

        IF INSERTING THEN
          arr_nptxbc(ind).t_Action       := RSI_NPTXC.NPTXBC_ACTION_CREATE; --Создание
          arr_nptxbc(ind).t_BackObjID    := 0;
        ELSIF UPDATING OR DELETING THEN
          IF UPDATING THEN
            arr_nptxbc(ind).t_Action     := RSI_NPTXC.NPTXBC_ACTION_UPDATE; --Обновление
          ELSE
            arr_nptxbc(ind).t_Action     := RSI_NPTXC.NPTXBC_ACTION_DELETE; --Удаление
          END IF;

          INSERT INTO DNPTXGOBC_DBT(T_GOBCID
                                  , T_ID
                                  , T_DOCKIND
                                  , T_DOCUMENTID
                                  , T_KIND
                                  , T_CODE
                                  , T_SALEDATE
                                  , T_BUYDATE
                                  , T_FIID
                                  , T_OLDFACEVALUE
                                  , T_NEWFACEVALUE)
                    VALUES(0
                           , :OLD.T_ID
                           , :OLD.T_DOCKIND
                           , :OLD.T_DOCUMENTID
                           , :OLD.T_KIND
                           , :OLD.T_CODE
                           , :OLD.T_SALEDATE
                           , :OLD.T_BUYDATE
                           , :OLD.T_FIID
                           , :OLD.T_OLDFACEVALUE
                           , :OLD.T_NEWFACEVALUE) RETURNING T_GOBCID INTO v_BCID;

          IF v_BCID > 0 THEN
            arr_nptxbc(ind).t_BackObjID  := v_BCID;
          ELSE
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Ошибка при сохранении информации о глобальной операции в истории' );
            RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20642,'');
          END IF;
        END IF;

      END IF;

      IF DELETING THEN
        update dnptxlot_dbt
           set t_ClGOID = 0,
               t_RetFlag = CHR(0),
               t_BuyDate = t_OldDate
         where t_ClGOID = :old.t_ID
           and t_Kind in (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT);

        update dnptxlot_dbt
           set t_ClGOID = 0,
               t_SaleDate = t_OldDate,
               t_RetFlag = CHR(0)
         where t_ClGOID = :old.t_ID
           and t_Kind in (RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANGET);

        delete
          from dnptxgofi_dbt gofi
         where gofi.t_GOID = :old.t_ID;

      END IF;
    END IF;

  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN
    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN

      FORALL counter IN 1..arr_nptxbc.count()
       INSERT INTO dnptxbc_dbt
       VALUES arr_nptxbc(counter);
    END IF;

  END AFTER STATEMENT;

END DNPTXGO_DBT_TOTAL;
/

CREATE OR REPLACE TRIGGER DNPTXGOFI_DBT_TOTAL
  FOR INSERT OR UPDATE OR DELETE ON DNPTXGOFI_DBT 
COMPOUND TRIGGER

  TYPE tp_nptxbc is table of dnptxbc_dbt%rowtype index by pls_integer;

  arr_nptxbc tp_nptxbc;

  ind pls_integer := 0;

/*  BEFORE STATEMENT IS
  BEGIN

  END BEFORE STATEMENT; */

/*  BEFORE EACH ROW IS
  BEGIN

  END BEFORE EACH ROW; */

  AFTER EACH ROW IS
    v_BCID     NUMBER(10) := 0;
    v_EndDate  DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
  BEGIN

    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN
      IF RSI_TRG_DNPTXLOT_DBT.v_ID_Operation > 0 THEN

        ind := ind + 1;

        arr_nptxbc(ind).t_BCID         := 0;
        arr_nptxbc(ind).t_ObjKind      := RSI_NPTXC.NPTXBC_OBJKIND_GOFI; --Выпуск по ГО
        IF DELETING THEN
          arr_nptxbc(ind).t_ObjID      := :OLD.T_ID;
        ELSE
          arr_nptxbc(ind).t_ObjID      := :NEW.T_ID;
        END IF;
        arr_nptxbc(ind).t_ID_Operation := RSI_TRG_DNPTXLOT_DBT.v_ID_Operation;
        arr_nptxbc(ind).t_ID_Step      := RSI_TRG_DNPTXLOT_DBT.v_ID_Step;

        IF INSERTING THEN
          arr_nptxbc(ind).t_Action       := RSI_NPTXC.NPTXBC_ACTION_CREATE; --Создание
          arr_nptxbc(ind).t_BackObjID    := 0;
        ELSIF UPDATING OR DELETING THEN
          IF UPDATING THEN
            arr_nptxbc(ind).t_Action     := RSI_NPTXC.NPTXBC_ACTION_UPDATE; --Обновление
          ELSE
            arr_nptxbc(ind).t_Action     := RSI_NPTXC.NPTXBC_ACTION_DELETE; --Удаление
          END IF;

          INSERT INTO DNPTXGOFIBC_DBT(T_GOFIBCID
                                    , T_ID
                                    , T_GOID
                                    , T_NUM
                                    , T_NEWFIID
                                    , T_NUMERATOR
                                    , T_DENOMINATOR)
                    VALUES(0
                           , :OLD.T_ID
                           , :OLD.T_GOID
                           , :OLD.T_NUM
                           , :OLD.T_NEWFIID
                           , :OLD.T_NUMERATOR
                           , :OLD.T_DENOMINATOR) RETURNING T_GOFIBCID INTO v_BCID;

          IF v_BCID > 0 THEN
            arr_nptxbc(ind).t_BackObjID  := v_BCID;
          ELSE
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Ошибка при сохранении информации о выпуске по ГО в истории' );
            RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20642,'');
          END IF;
        END IF;

      END IF;
    END IF;

  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN
    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN
      FORALL counter IN 1..arr_nptxbc.count()
       INSERT INTO dnptxbc_dbt
       VALUES arr_nptxbc(counter);
    END IF;
  END AFTER STATEMENT;

END DNPTXGOFI_DBT_TOTAL;
/

CREATE OR REPLACE TRIGGER DNPTXTS_DBT_TOTAL
  FOR INSERT OR UPDATE OR DELETE ON DNPTXTS_DBT 
COMPOUND TRIGGER

  TYPE tp_nptxbc is table of dnptxbc_dbt%rowtype index by pls_integer;

  arr_nptxbc tp_nptxbc;

  ind pls_integer := 0;

/*  BEFORE STATEMENT IS
  BEGIN

  END BEFORE STATEMENT; */

/*  BEFORE EACH ROW IS
  BEGIN

  END BEFORE EACH ROW; */

  AFTER EACH ROW IS
    v_BCID     NUMBER(10) := 0;
    v_EndDate  DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
  BEGIN
    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN
      IF RSI_TRG_DNPTXLOT_DBT.v_ID_Operation > 0 THEN

        ind := ind + 1;

        arr_nptxbc(ind).t_BCID         := 0;
        arr_nptxbc(ind).t_ObjKind      := RSI_NPTXC.NPTXBC_OBJKIND_TS; --Состояние лота
        IF DELETING THEN
          arr_nptxbc(ind).t_ObjID      := :OLD.T_ID;
        ELSE
          arr_nptxbc(ind).t_ObjID      := :NEW.T_ID;
        END IF;
        arr_nptxbc(ind).t_ID_Operation := RSI_TRG_DNPTXLOT_DBT.v_ID_Operation;
        arr_nptxbc(ind).t_ID_Step      := RSI_TRG_DNPTXLOT_DBT.v_ID_Step;

        IF INSERTING THEN
          arr_nptxbc(ind).t_Action       := RSI_NPTXC.NPTXBC_ACTION_CREATE; --Создание
          arr_nptxbc(ind).t_BackObjID    := 0;
        ELSIF UPDATING OR DELETING THEN
          IF UPDATING THEN
            arr_nptxbc(ind).t_Action     := RSI_NPTXC.NPTXBC_ACTION_UPDATE; --Обновление
          ELSE
            arr_nptxbc(ind).t_Action     := RSI_NPTXC.NPTXBC_ACTION_DELETE; --Удаление
          END IF;

          INSERT INTO DNPTXTSBC_DBT(T_TSBCID
                                  , T_ID
                                  , T_CLIENT
                                  , T_CONTRACT
                                  , T_FIID
                                  , T_BUYID
                                  , T_SALEID
                                  , T_TYPE
                                  , T_BEGDATE
                                  , T_ENDDATE
                                  , T_AMOUNT)
                    VALUES(0
                           , :OLD.T_ID
                           , :OLD.T_CLIENT
                           , :OLD.T_CONTRACT
                           , :OLD.T_FIID
                           , :OLD.T_BUYID
                           , :OLD.T_SALEID
                           , :OLD.T_TYPE
                           , :OLD.T_BEGDATE
                           , :OLD.T_ENDDATE
                           , :OLD.T_AMOUNT) RETURNING T_TSBCID INTO v_BCID;

          IF v_BCID > 0 THEN
            arr_nptxbc(ind).t_BackObjID  := v_BCID;
          ELSE
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Ошибка при сохранении информации о состоянии лота в истории' );
            RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20642,'');
          END IF;
        END IF;

      END IF;

      if INSERTING then
        RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'Триггер DNPTXTS_DBT_DBG: Вставлена запись в ТС t_id = '||:new.t_ID||' T_TYPE='||:new.T_TYPE||' T_BUYID='||:new.T_BUYID||' T_AMOUNT='||:new.T_AMOUNT||' T_ENDDATE='||:new.T_ENDDATE );

      elsif DELETING then
        RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'Триггер DNPTXTS_DBT_DBG: Удалена запись в ТС t_id = '||:old.t_ID||' T_TYPE='||:old.T_TYPE||' T_BUYID='||:old.T_BUYID||' T_AMOUNT='||:old.T_AMOUNT||' T_ENDDATE='||:old.T_ENDDATE );

      else
        RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'Триггер DNPTXTS_DBT_DBG: Обновление записи в ТС t_id = '||:old.t_ID||' OLD.AMOUNT='||:old.T_AMOUNT||' NEW.AMOUNT='||:new.T_AMOUNT||' OLD.ENDDATE='||:old.T_ENDDATE||' NEW.ENDDATE='||:new.T_ENDDATE );
      end if;
    END IF;

  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN
    IF RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC = FALSE THEN
      FORALL counter IN 1..arr_nptxbc.count()
       INSERT INTO dnptxbc_dbt
       VALUES arr_nptxbc(counter);
    END IF;

  END AFTER STATEMENT;

END DNPTXTS_DBT_TOTAL;
/
