CREATE OR REPLACE TRIGGER DDVDEAL_DBT_TRBU 
  BEFORE DELETE OR INSERT OR UPDATE OF t_STATE, T_FIID, T_DEPARTMENT, T_BROKER, T_CLIENT, T_BROKERCONTR, T_CLIENTCONTR, T_ISTRUST, T_OFBU, T_POSACC, t_GenAgrID, t_Date_CLR ON DDVDEAL_DBT FOR EACH ROW
DECLARE
   v_exist_fipos        INTEGER;
   v_state_dvoper       INTEGER;
   v_oper_totalcalc     ddvoper_dbt.t_TOTALCALC%TYPE;
   v_oper_state         ddvoper_dbt.t_STATE%TYPE;
   v_state_fiturn       INTEGER;
   v_AvoirKind          INTEGER;
   v_STATE              ddvfipos_dbt.t_STATE%TYPE;
   v_LONGPOSITION       ddvfiturn_dbt.t_LONGPOSITION%TYPE;
   v_LONGPOSITIONCOST   ddvfiturn_dbt.t_LONGPOSITIONCOST%TYPE;
   v_SHORTPOSITION      ddvfiturn_dbt.t_SHORTPOSITION%TYPE;
   v_SHORTPOSITIONCOST  ddvfiturn_dbt.t_SHORTPOSITIONCOST%TYPE;
   v_FIID               dfininstr_dbt.t_FIID%TYPE;
   v_tick               dfideriv_dbt.t_tick%TYPE;
   v_PriceMode          dfideriv_dbt.t_PRICEMODE%TYPE;
   v_TickCost           NUMBER;
   v_Margin             NUMBER;
   ExistRec             NUMBER;
   v_NewID              NUMBER(10) := 0;

   eNUMS_ROW_MORE EXCEPTION;
   PRAGMA EXCEPTION_INIT(eNUMS_ROW_MORE, -1422);

   FiTurn  ddvfiturn_dbt%ROWTYPE;
   v_exist_fiturn   INTEGER;

   CURSOR CDlCom IS SELECT *
                      FROM ddvdlcom_dbt
                     WHERE t_DealID = :new.t_ID;

   CURSOR CDlComOld IS SELECT *
                         FROM ddvdlcom_dbt
                        WHERE t_DealID = :old.t_ID;

   CURSOR CDlTurn IS SELECT *
                       FROM ddvdlturn_dbt
                      WHERE t_DealID = :new.t_ID;

   CURSOR CDlTurnOld IS SELECT *
                          FROM ddvdlturn_dbt
                         WHERE t_DealID = :old.t_ID;
BEGIN

   TRGPCKG_DDVDEAL_DBT_TRBIUD.v_InTrgr := True;

   IF( INSERTING ) THEN
      SELECT ddvdeal_dbt_seq.nextval INTO v_NewID FROM dual;
   ELSE
      v_NewID := :new.t_ID;
   END IF;

   IF( INSERTING OR
       ( UPDATING AND ( :new.T_FIID        != :old.T_FIID OR
                        :new.T_DEPARTMENT  != :old.T_DEPARTMENT OR
                        :new.T_BROKER      != :old.T_BROKER OR
                        :new.T_CLIENT      != :old.T_CLIENT OR
                        :new.T_BROKERCONTR != :old.T_BROKERCONTR OR
                        :new.T_CLIENTCONTR != :old.T_CLIENTCONTR OR
                        :new.T_ISTRUST     != :old.T_ISTRUST OR
                        :new.T_OFBU        != :old.T_OFBU OR
                        :new.t_GenAgrID    != :old.t_GenAgrID
                      )
       )
     ) THEN
      BEGIN
         rsb_derivatives.RSI_DV_CheckAndOpenPosition( :NEW.T_FIID, :NEW.T_DEPARTMENT, :NEW.T_BROKER, :NEW.T_CLIENT,
                                                      :NEW.T_BROKERCONTR, :NEW.T_CLIENTCONTR, :NEW.T_TYPE,
                                                      :NEW.T_ISTRUST, :NEW.T_OFBU, :NEW.t_GenAgrID );
      EXCEPTION
        --если вдруг идёт работа во многопотоке, то два потока могут обновременно попытаться открыть позицию
        --а значит у одного из них может случится дублирование вставки
        --на этот случай просто запускаем функцию ещё раз, чтобы отработали все нужные проверки по открытой позиции
        WHEN DUP_VAL_ON_INDEX THEN 
           rsb_derivatives.RSI_DV_CheckAndOpenPosition( :NEW.T_FIID, :NEW.T_DEPARTMENT, :NEW.T_BROKER, :NEW.T_CLIENT,
                                                        :NEW.T_BROKERCONTR, :NEW.T_CLIENTCONTR, :NEW.T_TYPE,
                                                        :NEW.T_ISTRUST, :NEW.T_OFBU, :NEW.t_GenAgrID );
      END;

   ELSIF( UPDATING AND :new.T_DATE_CLR != :old.T_DATE_CLR ) THEN -- менялась только T_DATE_CLR
      BEGIN
         SELECT count(1) INTO ExistRec
           FROM ddvfipos_dbt
          WHERE t_FIID        = :new.t_FIID
            AND t_DEPARTMENT  = :new.t_DEPARTMENT
            AND t_Broker      = :new.t_Broker
            AND t_ClientContr = :new.t_ClientContr
            AND t_GenAgrID    = :new.t_GenAgrID
            AND t_state       = 2;

         IF( ExistRec > 0 ) THEN
            RAISE_APPLICATION_ERROR(-20506,''); -- Позиция по производному инструменту закрыта
         END IF;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
      END;

   END IF;

   IF( INSERTING OR
       ( UPDATING AND ( :new.T_FIID        != :old.T_FIID OR
                        :new.T_DEPARTMENT  != :old.T_DEPARTMENT OR
                        :new.T_BROKER      != :old.T_BROKER OR
                        :new.T_CLIENT      != :old.T_CLIENT OR
                        :new.T_BROKERCONTR != :old.T_BROKERCONTR OR
                        :new.T_CLIENTCONTR != :old.T_CLIENTCONTR OR
                        :new.T_ISTRUST     != :old.T_ISTRUST OR
                        :new.T_OFBU        != :old.T_OFBU OR
                        :new.T_DATE_CLR    != :old.T_DATE_CLR OR
                        :new.t_GenAgrID    != :old.t_GenAgrID
                      )
       )
     ) THEN

      BEGIN
         SELECT count(1)
           INTO ExistRec
           FROM DDVFITURN_DBT
          WHERE T_FIID         = :new.T_FIID
            AND T_DEPARTMENT   = :new.T_DEPARTMENT
            AND T_BROKER       = :new.T_BROKER
            AND T_CLIENTCONTR  = :new.T_CLIENTCONTR
            AND T_DATE         = :new.T_DATE_CLR
            AND t_GenAgrID     = :new.t_GenAgrID
            AND t_State        = 2;

         IF( ExistRec > 0 ) THEN
            RAISE_APPLICATION_ERROR(-20509,''); -- Итоги дня по позиции закрыты
         END IF;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN ExistRec := 0;
      END;

      BEGIN
         SELECT t_STATE, t_TOTALCALC INTO v_oper_state, v_oper_totalcalc
           FROM (SELECT dvoper.t_STATE, dvoper.t_TOTALCALC
                   FROM ddvoper_dbt dvoper, doprkoper_dbt oprkoper
                  WHERE dvoper.t_DOCKIND     = RSB_Derivatives.DV_GetDocKind
                    AND dvoper.t_DATE       >= :new.T_DATE_CLR
                    AND dvoper.t_DEPARTMENT  = :NEW.t_DEPARTMENT
                    AND (((dvoper.t_Flag1 = rsb_derivatives.ALG_SP_MONEY_SOURCE_OWN) AND (:NEW.t_Client <= 0)) OR
                         ((dvoper.t_Flag1 = rsb_derivatives.ALG_SP_MONEY_SOURCE_CLIENT) AND (:NEW.t_Client >= 1)) OR
                         ((dvoper.t_Flag1 = rsb_derivatives.ALG_SP_MONEY_SOURCE_TRUST) AND (:NEW.t_IsTrust = chr(88))) OR
                         (dvoper.t_Flag1 = 0))
                    AND dvoper.t_GenAgrID = :NEW.t_GenAgrID
                    AND dvoper.t_Party = DECODE(:NEW.t_BROKER, -1, (SELECT fin.t_ISSUER
                                                                      FROM dfininstr_dbt fin
                                                                     WHERE fin.t_FIID = :NEW.t_FIID), :NEW.t_BROKER )
                    AND ((:NEW.t_BROKER = -1) OR (:NEW.t_BROKER != -1 AND dvoper.t_PARTYCONTR = :NEW.t_BROKERCONTR))
                    AND ( dvoper.t_STATE = 2 OR dvoper.t_TOTALCALC = 'X' )
                    AND oprkoper.t_Kind_Operation = dvoper.t_OperKind
                    AND instr(oprkoper.t_SysTypes, 'C') = 0
                    AND instr(oprkoper.t_SysTypes, 'S') = 0
                 ORDER BY dvoper.t_DATE)
          WHERE ROWNUM = 1;

         IF( v_oper_state = 2 ) THEN
            RAISE_APPLICATION_ERROR(-20508,''); -- Операция расчетов по производным инструментам закрыта
         ELSIF( v_oper_totalcalc = 'X' ) THEN
            RAISE_APPLICATION_ERROR(-20528,''); -- По операции расчетов выполнен расчет итогов
         END IF;

      EXCEPTION
         WHEN NO_DATA_FOUND THEN NULL;
      END;

   END IF;

   -- Перевод в открытые
   IF( (DELETING = false) AND (:new.t_STATE = 1 /*DVDEAL_OPEN*/ AND (INSERTING OR :old.t_STATE = 0 /*DVDEAL_PREP*/)) ) THEN

      /*ZMV BIQ-8258 блокировка исполнения сделок срочного рынка для договоров, 
      у которых категория "Сведения о наличии или отсутствии ИИС у другого ПУ" = "Да" И категория "Предоставлены подтвержд. документы о расторжении ИИС у другого ПУ" на дату операции равно "Нет" или не заполнено*/ 
      rsb_derivatives.DV_CheckContrIISwithAnotherSP(:new.t_CLIENTCONTR,  :new.t_DATE);

      BEGIN
         v_exist_fipos := 1;
         SELECT t_STATE INTO v_STATE
           FROM ddvfipos_dbt
          WHERE t_IsTrust     = :new.t_IsTrust
            AND t_FIID        = :new.t_FIID
            AND t_DEPARTMENT  = :new.t_DEPARTMENT
            AND t_BROKER      = :new.t_BROKER
            AND t_CLIENTCONTR = :new.t_CLIENTCONTR
            AND t_GenAgrID    = :new.t_GenAgrID;

         EXCEPTION WHEN NO_DATA_FOUND THEN v_exist_fipos := 0;
      END;

      IF( v_exist_fipos = 0 ) THEN
         RAISE_APPLICATION_ERROR(-20505,''); -- Не открыта позиция по производному инструменту
      ELSIF( v_exist_fipos = 1 AND v_STATE = 2 ) THEN
         RAISE_APPLICATION_ERROR(-20506,''); -- Позиция по производному инструменту закрыта
      END IF;

      IF( rsb_derivatives.DV_Setting_ExecDealBefore = 0 ) THEN

         v_state_dvoper := -1;

         BEGIN
            SELECT t_STATE INTO v_state_dvoper
              FROM ddvoper_dbt dvoper, doprkoper_dbt oprkoper
             WHERE dvoper.t_DOCKIND    = rsb_derivatives.DV_GetDocKind
               AND dvoper.t_DATE       = :new.T_DATE_CLR
               AND dvoper.t_DEPARTMENT = :new.t_DEPARTMENT
               AND (((dvoper.t_Flag1 = rsb_derivatives.ALG_SP_MONEY_SOURCE_OWN) AND (:NEW.t_Client <= 0)) OR
                    ((dvoper.t_Flag1 = rsb_derivatives.ALG_SP_MONEY_SOURCE_CLIENT) AND (:NEW.t_Client >= 1)) OR
                    ((dvoper.t_Flag1 = rsb_derivatives.ALG_SP_MONEY_SOURCE_TRUST) AND (:NEW.t_IsTrust = chr(88))) OR
                    (dvoper.t_Flag1 = 0))
               AND dvoper.t_GenAgrID = :new.t_GenAgrID
               AND dvoper.t_Party = DECODE(:new.t_BROKER, -1, (SELECT fin.t_ISSUER
                                                                 FROM dfininstr_dbt fin
                                                                WHERE fin.t_FIID = :new.t_FIID), :new.t_BROKER)
               AND ( (:new.t_BROKER = -1) OR (:new.t_BROKER != -1 AND dvoper.t_PARTYCONTR = :NEW.T_BROKERCONTR) )
               AND oprkoper.t_Kind_Operation = dvoper.t_OperKind
               AND instr(oprkoper.t_SysTypes, 'C') = 0
               AND instr(oprkoper.t_SysTypes, 'S') = 0;
         EXCEPTION
            WHEN NO_DATA_FOUND THEN
               v_state_dvoper := -1;
            WHEN eNUMS_ROW_MORE THEN
              RAISE_APPLICATION_ERROR(-20524,''); -- За день введены несколько операций расчетов с одинаковыми параметрами
         END;

         IF( v_state_dvoper = -1 OR v_state_dvoper = 0 ) THEN
           RAISE_APPLICATION_ERROR(-20507,''); -- Не открыта операция расчетов по производным инструментам
         END IF;

      END IF;

      rsb_derivatives.RSI_DV_AttachPositionTurn(:new.t_FIID, :new.t_DEPARTMENT, :new.t_BROKER, :new.t_CLIENTCONTR, :new.T_DATE_CLR, :new.t_GenAgrID);

      IF( rsb_derivatives.DV_Setting_AccExContracts = 1 ) THEN -- По сделке
         rsb_derivatives.RSI_DV_AttachDealTurn(v_NewID, :new.T_DATE_CLR, CHR(88), :new.t_DealCost);
      END IF;

      -- Выбрать запись DDVFITURN
      BEGIN
        v_exist_fiturn := 1;

        SELECT * INTO FiTurn
          FROM ddvfiturn_dbt turn
         WHERE turn.t_IsTrust     = :new.t_IsTrust
           AND turn.t_FIID        = :new.t_FIID
           AND turn.t_DEPARTMENT  = :new.t_DEPARTMENT
           AND turn.t_BROKER      = :new.t_BROKER
           AND turn.t_CLIENTCONTR = :new.t_CLIENTCONTR
           AND turn.t_GenAgrID    = :new.t_GenAgrID
           AND turn.t_DATE        = :new.T_DATE_CLR;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_exist_fiturn := 0;
      END;

      IF( v_exist_fiturn = 0 ) THEN
         RAISE_APPLICATION_ERROR(-20526,''); -- Не найдены итоги дня по позиции
      END IF;

      BEGIN
         v_AvoirKind := 0;
         SELECT t_AVOIRKIND INTO v_AvoirKind
           FROM dfininstr_dbt
          WHERE t_FIID = :new.t_FIID;
         EXCEPTION
           WHEN NO_DATA_FOUND THEN
             v_AvoirKind := 0;
      END;

      BEGIN
         v_tick := 1;
         SELECT t_TICK INTO v_tick
           FROM dfideriv_dbt fider
          WHERE fider.t_FIID = :new.t_FIID;
         EXCEPTION
           WHEN NO_DATA_FOUND THEN
             v_tick := 1;
      END;

      v_TickCost := rsb_derivatives.DV_TickCost(:new.t_FIID, :new.T_DATE_CLR);

      :new.t_TURN := CHR(0); -- общий случай

      IF( :new.t_TYPE = 'E' OR :new.t_TYPE = 'R' ) THEN
         IF( rsb_derivatives.DV_Setting_AccExContracts = 0 ) THEN -- По позиции
            IF( ((v_AvoirKind = rsb_derivatives.DV_DERIVATIVE_FUTURES) and (rsb_derivatives.DV_Setting_TurnPos = 1)) OR
                ((v_AvoirKind = rsb_derivatives.DV_DERIVATIVE_OPTION) and (rsb_derivatives.DV_Setting_TurnPos = 2)) OR
                (rsb_derivatives.DV_Setting_TurnPos = 3) ) THEN

               :new.t_TURN := CHR(88); -- Переустанавливаем

               IF( FiTurn.t_LONGPOSITION = FiTurn.t_SHORTPOSITION ) THEN
                  RAISE_APPLICATION_ERROR(-20525,''); -- Исполняемая нетто позиция равна нулю
               END IF;

               IF( FiTurn.t_LONGPOSITION > FiTurn.t_SHORTPOSITION ) THEN
                  :new.t_POSITION := rsb_derivatives.DV_POSITION_LONG;
               ELSE
                  :new.t_POSITION := rsb_derivatives.DV_POSITION_SHORT;
               END IF;

               :new.t_AMOUNT := ABS(FiTurn.t_SHORTPOSITION - FiTurn.t_LONGPOSITION);
            END IF;
         ELSE
            UPDATE DDVDLTURN_DBT
               SET t_EXECCOST = :new.t_POSITIONCOST,
                   t_EXECUTION = :new.t_AMOUNT
             WHERE t_DealID = v_NewID
               AND t_Date   = :new.T_DATE_CLR;
         END IF;
      END IF;

      -- Полное исполнение без свертки, исполняем на всю стоимость
      IF( (:new.t_TURN = CHR(0)) AND (((:new.t_POSITION = rsb_derivatives.DV_POSITION_LONG) and (:new.t_AMOUNT = FiTurn.t_LONGPOSITION)) OR
                                      ((:new.t_POSITION = rsb_derivatives.DV_POSITION_SHORT) and (:new.t_AMOUNT = FiTurn.t_SHORTPOSITION)))
        ) THEN  -- Полное исполнение, исполняем на всю стоимость
         IF( :new.t_POSITION = rsb_derivatives.DV_POSITION_LONG ) THEN
            -- недокументированный в ТЗ функционал
            :new.t_POSITIONCOST := FiTurn.t_LONGPOSITIONCOST;
         ELSE
            -- недокументированный в ТЗ функционал
            :new.t_POSITIONCOST := FiTurn.t_SHORTPOSITIONCOST;
         END IF;
      ELSIF( (:new.t_TURN = CHR(88)) AND (:new.t_AMOUNT = ABS(FiTurn.t_SHORTPOSITION - FiTurn.t_LONGPOSITION)) ) THEN -- Полное исполнение со сверткой, исполняем на всю стоимость
         :new.t_POSITIONCOST := ABS(FiTurn.t_SHORTPOSITION - FiTurn.t_LONGPOSITION);
      ELSIF( (:new.t_TURN = CHR(88)) AND (:new.t_AMOUNT != ABS(FiTurn.t_SHORTPOSITION - FiTurn.t_LONGPOSITION)) ) THEN -- Частичное исполнение со сверткой, не поддерживаем
         RAISE_APPLICATION_ERROR(-20590,''); -- Частичная свертка нетто позиции не поддерживается
      ELSE -- частичное исполнение без сверки
         BEGIN
            v_PriceMode := 0;
            SELECT t_PRICEMODE INTO v_PriceMode
              FROM dfideriv_dbt fider
             WHERE fider.t_FIID = :new.t_FIID;
            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                v_PriceMode := 0;
         END;

         IF((:new.t_TYPE != 'E') AND (:new.t_TYPE != 'R') ) THEN
            :new.t_POSITIONCOST := ROUND(:new.t_COST * :new.t_AMOUNT, 2);
         END IF;

         -- Проверим, есть ли столько стоимости
         IF( ((:new.t_TURN = CHR(0)) AND (((:new.t_POSITION = rsb_derivatives.DV_POSITION_LONG) AND (:new.t_POSITIONCOST >= FiTurn.t_LONGPOSITIONCOST)) OR
                                          ((:new.t_POSITION = rsb_derivatives.DV_POSITION_SHORT) AND (:new.t_POSITIONCOST >= FiTurn.t_SHORTPOSITIONCOST)))) OR
             ((:new.t_TURN = CHR(88)) AND (:new.t_POSITIONCOST >= abs(FiTurn.t_SHORTPOSITIONCOST - FiTurn.t_LONGPOSITIONCOST)))
           ) THEN
            RAISE_APPLICATION_ERROR(-20591,''); -- Овердрафт по стоимости позиции
         END IF;
      END IF;

      IF( (rsb_derivatives.DV_Setting_AccExContracts = 1) AND (:new.t_TYPE != 'E') AND (:new.t_TYPE != 'R') ) THEN
         :new.t_DEALCOST := :new.t_POSITIONCOST;
         UPDATE DDVDLTURN_DBT
            SET t_DEALCOST = :new.t_POSITIONCOST
          WHERE t_DealID = v_NewID
            AND t_Date   = :new.T_DATE_CLR;
      END IF;

      IF( (v_AvoirKind = rsb_derivatives.DV_DERIVATIVE_OPTION) AND (:new.t_TYPE != 'E' AND :new.t_TYPE != 'R') ) THEN
         IF( v_TickCost = 0. ) THEN
            RAISE_APPLICATION_ERROR(-20537,''); -- Не задан курс вида 'Стоимость минимального шага цены'
         END IF;
         :new.t_POSITIONBONUS :=  ROUND(:new.t_BONUS * :new.t_AMOUNT * v_TickCost / v_tick, 2);
      END IF;
   END IF;

   -- Учет по позиции
   IF( (((rsb_derivatives.DV_Setting_AccExContracts = 0) OR (:new.t_TYPE = 'E') OR (:new.t_TYPE = 'R')) AND (:new.t_STATE = 2) AND ((:old.t_STATE = 1) OR INSERTING)) OR
       ((rsb_derivatives.DV_Setting_AccExContracts = 1) AND (:new.t_POSACC = CHR(88)) AND ((:old.t_POSACC = CHR(0)) OR INSERTING)) ) THEN

     IF( (rsb_derivatives.DV_Setting_AccExContracts = 0) OR (:new.t_TYPE = 'E') OR (:new.t_TYPE = 'R') ) THEN -- По позиции
        :new.t_POSACC := CHR(88);
     END IF;

     v_state_fiturn := -999;

     BEGIN
        SELECT turn.t_STATE INTO v_state_fiturn
          FROM ddvfiturn_dbt turn
         WHERE turn.t_IsTrust     = :new.t_IsTrust
           AND turn.t_FIID        = :new.t_FIID
           AND turn.t_DEPARTMENT  = :new.t_DEPARTMENT
           AND turn.t_BROKER      = :new.t_BROKER
           AND turn.t_CLIENTCONTR = :new.t_CLIENTCONTR
           AND turn.t_GenAgrID    = :new.t_GenAgrID
           AND turn.t_DATE        = :new.T_DATE_CLR;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN
           v_state_fiturn := -999;
     END;

     IF( v_state_fiturn = -999 ) THEN
       RAISE_APPLICATION_ERROR(-20512,''); -- Не открыт день по позиции
     END IF;
     IF( v_state_fiturn = 2 ) THEN
       RAISE_APPLICATION_ERROR(-20509,''); -- День по позиции закрыт
     END IF;

     IF( :new.t_TYPE = 'B' OR :new.t_TYPE = 'D') THEN
        UPDATE ddvfiturn_dbt turn
           SET turn.t_BUY              = turn.t_BUY              + :new.t_AMOUNT,
               turn.t_LONGPOSITION     = turn.t_LONGPOSITION     + :new.t_AMOUNT,
               turn.t_LONGPOSITIONCOST = turn.t_LONGPOSITIONCOST + :new.t_POSITIONCOST,
               turn.t_PaidBonus        = turn.t_PaidBonus        + :new.t_POSITIONBONUS
         WHERE turn.t_IsTrust     = :new.t_IsTrust
           AND turn.t_FIID        = :new.t_FIID
           AND turn.t_DEPARTMENT  = :new.t_DEPARTMENT
           AND turn.t_BROKER      = :new.t_BROKER
           AND turn.t_CLIENTCONTR = :new.t_CLIENTCONTR
           AND turn.t_GenAgrID    = :new.t_GenAgrID
           AND turn.t_DATE        = :new.T_DATE_CLR;

     ELSIF( :new.t_TYPE = 'S' OR :new.t_TYPE = 'G' ) THEN

        UPDATE ddvfiturn_dbt turn
           SET turn.t_SALE              = turn.t_SALE              + :new.t_AMOUNT,
               turn.t_SHORTPOSITION     = turn.t_SHORTPOSITION     + :new.t_AMOUNT,
               turn.t_SHORTPOSITIONCOST = turn.t_SHORTPOSITIONCOST + :new.t_POSITIONCOST,
               turn.t_RECEIVEDBONUS     = turn.t_RECEIVEDBONUS     + :new.t_POSITIONBONUS
         WHERE turn.t_IsTrust     = :new.t_IsTrust
           AND turn.t_FIID        = :new.t_FIID
           AND turn.t_DEPARTMENT  = :new.t_DEPARTMENT
           AND turn.t_BROKER      = :new.t_BROKER
           AND turn.t_CLIENTCONTR = :new.t_CLIENTCONTR
           AND turn.t_GenAgrID    = :new.t_GenAgrID
           AND turn.t_DATE        = :new.T_DATE_CLR;

     ELSIF( :new.t_TYPE = 'E' OR :new.t_TYPE = 'R' ) THEN

        v_LONGPOSITION       := 0;
        v_LONGPOSITIONCOST   := 0;
        v_SHORTPOSITION      := 0;
        v_SHORTPOSITIONCOST  := 0;

        SELECT t_LONGPOSITION, t_LONGPOSITIONCOST, t_SHORTPOSITION, t_SHORTPOSITIONCOST INTO v_LONGPOSITION, v_LONGPOSITIONCOST, v_SHORTPOSITION, v_SHORTPOSITIONCOST
          FROM ddvfiturn_dbt turn
         WHERE turn.t_IsTrust     = :new.t_IsTrust
           AND turn.t_FIID        = :new.t_FIID
           AND turn.t_DEPARTMENT  = :new.t_DEPARTMENT
           AND turn.t_BROKER      = :new.t_BROKER
           AND turn.t_CLIENTCONTR = :new.t_CLIENTCONTR
           AND turn.t_GenAgrID    = :new.t_GenAgrID
           AND turn.t_DATE        = :new.T_DATE_CLR;

        IF( :new.t_POSITION = rsb_derivatives.DV_POSITION_LONG ) THEN

           IF( :new.T_TURN != 'X') THEN

              IF( :new.t_AMOUNT > v_LONGPOSITION ) THEN
                 RAISE_APPLICATION_ERROR(-20510,''); -- Овердрафт по длинной позиции
              END IF;

              UPDATE ddvfiturn_dbt turn
                 SET turn.t_LONGEXECUTION    = turn.t_LONGEXECUTION    + :new.t_AMOUNT,
                     turn.t_LONGPOSITION     = turn.t_LONGPOSITION     - :new.t_AMOUNT,
                     turn.t_LONGPOSITIONCOST = turn.t_LONGPOSITIONCOST - :new.t_POSITIONCOST
               WHERE turn.t_IsTrust     = :new.t_IsTrust
                 AND turn.t_FIID        = :new.t_FIID
                 AND turn.t_DEPARTMENT  = :new.t_DEPARTMENT
                 AND turn.t_BROKER      = :new.t_BROKER
                 AND turn.t_CLIENTCONTR = :new.t_CLIENTCONTR
                 AND turn.t_GenAgrID    = :new.t_GenAgrID
                 AND turn.t_DATE        = :new.T_DATE_CLR;
           ELSE
              IF( :new.t_AMOUNT + v_SHORTPOSITION > v_LONGPOSITION ) THEN
                 RAISE_APPLICATION_ERROR(-20510,''); -- Овердрафт по длинной позиции
              END IF;

              :new.t_TURNAMOUNT := v_SHORTPOSITION;
              :new.t_TURNCOST   := v_SHORTPOSITIONCOST;

              UPDATE ddvfiturn_dbt turn
                 SET turn.t_LONGEXECUTION     = turn.t_LONGEXECUTION  +  :new.t_AMOUNT,
                     turn.t_SHORTPOSITION     = 0,
                     turn.t_SHORTPOSITIONCOST = 0,
                     turn.t_LONGPOSITION      = turn.t_LONGPOSITION - (:new.t_AMOUNT + :new.t_TURNAMOUNT),
                     turn.t_LONGPOSITIONCOST  = turn.t_LONGPOSITIONCOST - (:new.t_POSITIONCOST + :new.t_TURNCOST)
               WHERE turn.t_IsTrust     = :new.t_IsTrust
                 AND turn.t_FIID        = :new.t_FIID
                 AND turn.t_DEPARTMENT  = :new.t_DEPARTMENT
                 AND turn.t_BROKER      = :new.t_BROKER
                 AND turn.t_CLIENTCONTR = :new.t_CLIENTCONTR
                 AND turn.t_GenAgrID    = :new.t_GenAgrID
                 AND turn.t_DATE        = :new.T_DATE_CLR;
           END IF;

        ELSIF( :new.t_POSITION = rsb_derivatives.DV_POSITION_SHORT ) THEN

           IF( :new.T_TURN != 'X') THEN
              IF( :new.t_AMOUNT > v_SHORTPOSITION ) THEN
                 RAISE_APPLICATION_ERROR(-20511,''); -- Овердрафт по короткой позиции
              END IF;

              UPDATE ddvfiturn_dbt turn
                 SET turn.t_SHORTEXECUTION    = turn.t_SHORTEXECUTION    + :new.t_AMOUNT,
                     turn.t_SHORTPOSITION     = turn.t_SHORTPOSITION     - :new.t_AMOUNT,
                     turn.t_SHORTPOSITIONCOST = turn.t_SHORTPOSITIONCOST - :new.t_POSITIONCOST
               WHERE turn.t_IsTrust     = :new.t_IsTrust
                 AND turn.t_FIID        = :new.t_FIID
                 AND turn.t_DEPARTMENT  = :new.t_DEPARTMENT
                 AND turn.t_BROKER      = :new.t_BROKER
                 AND turn.t_CLIENTCONTR = :new.t_CLIENTCONTR
                 AND turn.t_GenAgrID    = :new.t_GenAgrID
                 AND turn.t_DATE        = :new.T_DATE_CLR;
           ELSE
              IF( :new.t_AMOUNT + v_LONGPOSITION > v_SHORTPOSITION ) THEN
                 RAISE_APPLICATION_ERROR(-20511,''); -- Овердрафт по короткой позиции
              END IF;

              :new.t_TURNAMOUNT := v_LONGPOSITION;
              :new.t_TURNCOST   := v_LONGPOSITIONCOST;

              UPDATE ddvfiturn_dbt turn
                 SET turn.t_SHORTEXECUTION    = turn.t_SHORTEXECUTION + :new.t_AMOUNT,
                     turn.t_LONGPOSITION      = 0,
                     turn.t_LONGPOSITIONCOST  = 0,
                     turn.t_SHORTPOSITION     = turn.t_SHORTPOSITION     - (:new.t_AMOUNT + :new.t_TURNAMOUNT),
                     turn.t_SHORTPOSITIONCOST = turn.t_SHORTPOSITIONCOST - (:new.t_POSITIONCOST + :new.t_TURNCOST)
               WHERE turn.t_IsTrust     = :new.t_IsTrust
                 AND turn.t_FIID        = :new.t_FIID
                 AND turn.t_DEPARTMENT  = :new.t_DEPARTMENT
                 AND turn.t_BROKER      = :new.t_BROKER
                 AND turn.t_CLIENTCONTR = :new.t_CLIENTCONTR
                 AND turn.t_GenAgrID    = :new.t_GenAgrID
                 AND turn.t_DATE        = :new.T_DATE_CLR;
           END IF;

        END IF;

        IF( rsb_derivatives.DV_Setting_AccExContracts = 1 ) THEN -- По сделке
           TRGPCKG_DDVDEAL_DBT_TRBIUD.v_NumEnt := TRGPCKG_DDVDEAL_DBT_TRBIUD.v_NumEnt + 1;
           TRGPCKG_DDVDEAL_DBT_TRBIUD.v_t_Action(TRGPCKG_DDVDEAL_DBT_TRBIUD.v_NumEnt) := 1; -- "Исполнение"
           TRGPCKG_DDVDEAL_DBT_TRBIUD.v_t_ExecID(TRGPCKG_DDVDEAL_DBT_TRBIUD.v_NumEnt) := v_NewID;
        END IF;

     END IF;

     IF UPDATING THEN
        FOR DC IN CDlCom LOOP
           rsb_derivatives.RSI_DV_CalcPosCom( :new.t_FIID,
                                              :new.t_DEPARTMENT,
                                              :new.t_BROKER,
                                              :new.t_ClientContr,
                                              :new.T_DATE_CLR,
                                              DC.t_ComissID,
                                              DC.t_SUM,
                                              DC.t_NDS,
                                              :new.t_GenAgrID );
        END LOOP;

        IF( rsb_derivatives.DV_Setting_AccExContracts = 1 ) THEN -- По сделке
           FOR T IN CDlTurn LOOP
              --По клиентским сделкам уже учтена маржа в итогах позиции
              IF( (:new.t_Client > 0) AND (:new.t_POSACC = CHR(88)) AND (:old.t_POSACC = CHR(0)) ) THEN 
                v_Margin := 0;
              ELSE
                v_Margin := T.T_MARGIN;
              END IF;
              rsb_derivatives.RSI_DV_OnUpdateDealTurn( :new.T_FIID,
                                                       :new.T_DEPARTMENT,
                                                       :new.T_BROKER,
                                                       :new.T_CLIENTCONTR,
                                                       T.T_DATE,
                                                       v_Margin,
                                                       T.T_GUARANTY,
                                                       T.T_FAIRVALUE,
                                                       1,
                                                       TRUE,
                                                       :new.t_GenAgrID );
           END LOOP;
        END IF;
     END IF;
   END IF;

   -- Откат учета по позиции
   IF( (((rsb_derivatives.DV_Setting_AccExContracts = 0) OR (:old.t_TYPE = 'E') OR (:old.t_TYPE = 'R')) AND (DELETING OR (UPDATING AND (:new.t_STATE = 1)) OR (UPDATING AND (:new.t_STATE = 0))) AND (:old.t_STATE = 2)) OR
       ((rsb_derivatives.DV_Setting_AccExContracts = 1) AND (DELETING OR (UPDATING AND (:new.t_POSACC = CHR(0)))) AND (:old.t_POSACC = CHR(88))) ) THEN

      IF( ((rsb_derivatives.DV_Setting_AccExContracts = 0) OR (:old.t_TYPE = 'E') OR (:old.t_TYPE = 'R')) AND UPDATING ) THEN -- По позиции
         :new.t_POSACC := CHR(0);
      END IF;

      v_state_dvoper    := -1;
      v_oper_totalcalc  := CHR(0);

      BEGIN
         SELECT t_STATE, t_TOTALCALC INTO v_state_dvoper, v_oper_totalcalc
           FROM ddvoper_dbt dvoper, doprkoper_dbt oprkoper
          WHERE dvoper.t_DOCKIND    = rsb_derivatives.DV_GetDocKind
            AND dvoper.t_DATE       = :old.T_DATE_CLR
            AND dvoper.t_DEPARTMENT = :old.t_DEPARTMENT
            AND (((dvoper.t_Flag1 = rsb_derivatives.ALG_SP_MONEY_SOURCE_OWN) AND (:OLD.t_Client <= 0)) OR
                 ((dvoper.t_Flag1 = rsb_derivatives.ALG_SP_MONEY_SOURCE_CLIENT) AND (:OLD.t_Client >= 1)) OR
                 ((dvoper.t_Flag1 = rsb_derivatives.ALG_SP_MONEY_SOURCE_TRUST) AND (:OLD.t_IsTrust = chr(88))) OR
                 (dvoper.t_Flag1 = 0))
            AND dvoper.t_GenAgrID = :old.t_GenAgrID
            AND dvoper.t_PARTY = DECODE(:old.t_BROKER, -1, (SELECT fin.t_ISSUER FROM dfininstr_dbt fin WHERE fin.t_FIID = :old.t_FIID), :old.t_BROKER)
            AND ( (:old.t_BROKER = -1) OR (:old.t_BROKER != -1 AND dvoper.t_PARTYCONTR = :old.T_BROKERCONTR) )
            AND oprkoper.t_Kind_Operation = dvoper.t_OperKind
            AND instr(oprkoper.t_SysTypes, 'C') = 0
            AND instr(oprkoper.t_SysTypes, 'S') = 0;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            v_state_dvoper := -1;
         WHEN eNUMS_ROW_MORE THEN
            RAISE_APPLICATION_ERROR(-20524,''); -- За день введены несколько операций расчетов с одинаковыми параметрами
      END;

      IF( v_state_dvoper != -1 ) THEN
         IF( v_state_dvoper = 2 ) THEN
            RAISE_APPLICATION_ERROR(-20508,''); -- Операция расчетов по производным инструментам закрыта
         ELSIF( v_oper_totalcalc = 'X' ) THEN
            RAISE_APPLICATION_ERROR(-20528,''); -- По операции расчетов выполнен расчет итогов
         END IF;
      END IF;

      v_state_fiturn := -999;

      BEGIN
         SELECT turn.t_STATE INTO v_state_fiturn
           FROM ddvfiturn_dbt turn
          WHERE turn.t_IsTrust     = :old.t_IsTrust
            AND turn.t_FIID        = :old.t_FIID
            AND turn.t_DEPARTMENT  = :old.t_DEPARTMENT
            AND turn.t_BROKER      = :old.t_BROKER
            AND turn.t_CLIENTCONTR = :old.t_CLIENTCONTR
            AND turn.t_GenAgrID    = :old.t_GenAgrID
            AND turn.t_DATE        = :old.T_DATE_CLR;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            v_state_fiturn := -999;
      END;

      IF( v_state_fiturn = -999 ) THEN
        RAISE_APPLICATION_ERROR(-20512,''); -- Не открыт день по позиции
      ELSIF( v_state_fiturn = 2 ) THEN
        RAISE_APPLICATION_ERROR(-20509,''); -- День по позиции закрыт
      END IF;

      IF( :old.t_TYPE = 'B' OR :old.t_TYPE = 'D' ) THEN
         UPDATE ddvfiturn_dbt turn
            SET turn.t_BUY              = turn.t_BUY              - :old.t_AMOUNT,
                turn.t_LONGPOSITION     = turn.t_LONGPOSITION     - :old.t_AMOUNT,
                turn.t_LONGPOSITIONCOST = turn.t_LONGPOSITIONCOST - :old.t_POSITIONCOST,
                turn.t_PaidBonus        = turn.t_PaidBonus        - :old.t_POSITIONBONUS
          WHERE turn.t_IsTrust     = :old.t_IsTrust
            AND turn.t_FIID        = :old.t_FIID
            AND turn.t_DEPARTMENT  = :old.t_DEPARTMENT
            AND turn.t_BROKER      = :old.t_BROKER
            AND turn.t_CLIENTCONTR = :old.t_CLIENTCONTR
            AND turn.t_GenAgrID    = :old.t_GenAgrID
            AND turn.t_DATE        = :old.T_DATE_CLR;

      ELSIF( :old.t_TYPE = 'S' OR :old.t_TYPE = 'G') THEN

         UPDATE ddvfiturn_dbt turn
            SET turn.t_SALE              = turn.t_SALE              - :old.t_AMOUNT,
                turn.t_SHORTPOSITION     = turn.t_SHORTPOSITION     - :old.t_AMOUNT,
                turn.t_SHORTPOSITIONCOST = turn.t_SHORTPOSITIONCOST - :old.t_POSITIONCOST,
                turn.t_RECEIVEDBONUS     = turn.t_RECEIVEDBONUS     - :old.t_POSITIONBONUS
          WHERE turn.t_IsTrust     = :old.t_IsTrust
            AND turn.t_FIID        = :old.t_FIID
            AND turn.t_DEPARTMENT  = :old.t_DEPARTMENT
            AND turn.t_BROKER      = :old.t_BROKER
            AND turn.t_CLIENTCONTR = :old.t_CLIENTCONTR
            AND turn.t_GenAgrID    = :old.t_GenAgrID
            AND turn.t_DATE        = :old.T_DATE_CLR;

      ELSIF( :old.t_TYPE = 'E' OR :old.t_TYPE = 'R') THEN

         v_LONGPOSITION       := 0;
         v_LONGPOSITIONCOST   := 0;
         v_SHORTPOSITION      := 0;
         v_SHORTPOSITIONCOST  := 0;

         SELECT t_LONGPOSITION, t_LONGPOSITIONCOST, t_SHORTPOSITION, t_SHORTPOSITIONCOST INTO v_LONGPOSITION, v_LONGPOSITIONCOST, v_SHORTPOSITION, v_SHORTPOSITIONCOST
           FROM ddvfiturn_dbt turn
          WHERE turn.t_IsTrust     = :old.t_IsTrust
            AND turn.t_FIID        = :old.t_FIID
            AND turn.t_DEPARTMENT  = :old.t_DEPARTMENT
            AND turn.t_BROKER      = :old.t_BROKER
            AND turn.t_CLIENTCONTR = :old.t_CLIENTCONTR
            AND turn.t_GenAgrID    = :old.t_GenAgrID
            AND turn.t_DATE        = :old.T_DATE_CLR;

         IF( :old.t_POSITION = rsb_derivatives.DV_POSITION_LONG ) THEN

            IF( :old.T_TURN != 'X') THEN
               UPDATE ddvfiturn_dbt turn
                  SET turn.t_LONGEXECUTION    = turn.t_LONGEXECUTION    - :old.t_AMOUNT,
                      turn.t_LONGPOSITION     = turn.t_LONGPOSITION     + :old.t_AMOUNT,
                      turn.t_LONGPOSITIONCOST = turn.t_LONGPOSITIONCOST + :old.t_POSITIONCOST
                WHERE turn.t_IsTrust     = :old.t_IsTrust
                  AND turn.t_FIID        = :old.t_FIID
                  AND turn.t_DEPARTMENT  = :old.t_DEPARTMENT
                  AND turn.t_BROKER      = :old.t_BROKER
                  AND turn.t_CLIENTCONTR = :old.t_CLIENTCONTR
                  AND turn.t_GenAgrID    = :old.t_GenAgrID
                  AND turn.t_DATE        = :old.T_DATE_CLR;
            ELSE
               IF( UPDATING ) THEN
                  :new.t_TURNAMOUNT   := 0;
                  :new.t_TURNCOST     := 0;
                  :new.T_POSITIONCOST := 0;
                  :new.T_TURN         := CHR(0);
               END IF;

               UPDATE ddvfiturn_dbt turn
                  SET turn.t_LONGEXECUTION     = turn.t_LONGEXECUTION  -  :old.t_AMOUNT,
                      turn.t_SHORTPOSITION     = :old.t_TURNAMOUNT,
                      turn.t_SHORTPOSITIONCOST = :old.t_TURNCOST,
                      turn.t_LONGPOSITION      = turn.t_LONGPOSITION + (:old.t_AMOUNT + :old.t_TURNAMOUNT),
                      turn.t_LONGPOSITIONCOST  = turn.t_LONGPOSITIONCOST + (:old.t_POSITIONCOST + :old.t_TURNCOST)
                WHERE turn.t_IsTrust     = :old.t_IsTrust
                  AND turn.t_FIID        = :old.t_FIID
                  AND turn.t_DEPARTMENT  = :old.t_DEPARTMENT
                  AND turn.t_BROKER      = :old.t_BROKER
                  AND turn.t_CLIENTCONTR = :old.t_CLIENTCONTR
                  AND turn.t_GenAgrID    = :old.t_GenAgrID
                  AND turn.t_DATE        = :old.T_DATE_CLR;
            END IF;

         ELSIF( :old.t_POSITION = rsb_derivatives.DV_POSITION_SHORT ) THEN

            IF( :old.T_TURN != 'X') THEN
               UPDATE ddvfiturn_dbt turn
                  SET turn.t_SHORTEXECUTION    = turn.t_SHORTEXECUTION    - :old.t_AMOUNT,
                      turn.t_SHORTPOSITION     = turn.t_SHORTPOSITION     + :old.t_AMOUNT,
                      turn.t_SHORTPOSITIONCOST = turn.t_SHORTPOSITIONCOST + :old.t_POSITIONCOST
                WHERE turn.t_IsTrust     = :old.t_IsTrust
                  AND turn.t_FIID        = :old.t_FIID
                  AND turn.t_DEPARTMENT  = :old.t_DEPARTMENT
                  AND turn.t_BROKER      = :old.t_BROKER
                  AND turn.t_CLIENTCONTR = :old.t_CLIENTCONTR
                  AND turn.t_GenAgrID    = :old.t_GenAgrID
                  AND turn.t_DATE        = :old.T_DATE_CLR;
            ELSE
               IF( UPDATING ) THEN
                  :new.t_TURNAMOUNT := 0;
                  :new.t_TURNCOST   := 0;
               END IF;

               UPDATE ddvfiturn_dbt turn
                  SET turn.t_SHORTEXECUTION    = turn.t_SHORTEXECUTION - :old.t_AMOUNT,
                      turn.t_LONGPOSITION      = :old.t_TURNAMOUNT,
                      turn.t_LONGPOSITIONCOST  = :old.t_TURNCOST,
                      turn.t_SHORTPOSITION     = turn.t_SHORTPOSITION     + (:old.t_AMOUNT + :old.t_TURNAMOUNT),
                      turn.t_SHORTPOSITIONCOST = turn.t_SHORTPOSITIONCOST + (:old.t_POSITIONCOST + :old.t_TURNCOST)
                WHERE turn.t_IsTrust     = :old.t_IsTrust
                  AND turn.t_FIID        = :old.t_FIID
                  AND turn.t_DEPARTMENT  = :old.t_DEPARTMENT
                  AND turn.t_BROKER      = :old.t_BROKER
                  AND turn.t_CLIENTCONTR = :old.t_CLIENTCONTR
                  AND turn.t_GenAgrID    = :old.t_GenAgrID
                  AND turn.t_DATE        = :old.T_DATE_CLR;
            END IF;
         END IF;

         IF( rsb_derivatives.DV_Setting_AccExContracts = 1 ) THEN -- По сделке
            TRGPCKG_DDVDEAL_DBT_TRBIUD.v_NumEnt := TRGPCKG_DDVDEAL_DBT_TRBIUD.v_NumEnt + 1;
            TRGPCKG_DDVDEAL_DBT_TRBIUD.v_t_Action(TRGPCKG_DDVDEAL_DBT_TRBIUD.v_NumEnt) := 2; -- "Откат исполнения"
            TRGPCKG_DDVDEAL_DBT_TRBIUD.v_t_ExecID(TRGPCKG_DDVDEAL_DBT_TRBIUD.v_NumEnt) := v_NewID;
         END IF;

      END IF;

      FOR DC IN CDlComOld LOOP
         rsb_derivatives.RSI_DV_CalcPosCom( :old.t_FIID,
                                            :old.t_DEPARTMENT,
                                            :old.t_BROKER,
                                            :old.t_ClientContr,
                                            :old.T_DATE_CLR,
                                            DC.t_ComissID,
                                            -DC.t_SUM,
                                            -DC.t_NDS,
                                            :old.t_GenAgrID );
      END LOOP;

      IF(rsb_derivatives.DV_Setting_AccExContracts = 1) THEN -- По сделке
         FOR T IN CDlTurnOld LOOP
            --По клиентским сделкам уже учтена маржа в итогах позиции
            IF( (:new.t_Client > 0) AND (:new.t_POSACC = CHR(0)) AND (:old.t_POSACC = CHR(88)) ) THEN
              v_Margin := 0;
            ELSE
              v_Margin := -T.T_MARGIN;
            END IF;
            rsb_derivatives.RSI_DV_OnUpdateDealTurn( :old.T_FIID,
                                                     :old.T_DEPARTMENT,
                                                     :old.T_BROKER,
                                                     :old.T_CLIENTCONTR,
                                                     T.T_DATE,
                                                     v_Margin,
                                                     -T.T_GUARANTY,
                                                     -T.T_FAIRVALUE,
                                                     -1,
                                                     TRUE,
                                                     :old.t_GenAgrID );
         END LOOP;
      END IF;

  END IF;

  -- Откат перевода в открытые
  IF( (DELETING OR (UPDATING AND :new.t_STATE = 0)) AND (:old.t_STATE = 1 OR :old.t_STATE = 2 ) ) THEN

     rsb_derivatives.RSI_DV_DetachPositionTurn(:old.T_FIID, :old.T_DEPARTMENT, :old.T_BROKER, :old.T_CLIENTCONTR, :old.T_DATE_CLR, :old.t_GenAgrID);

     IF(rsb_derivatives.DV_Setting_AccExContracts = 1 ) THEN -- По сделке
        rsb_derivatives.RSI_DV_DetachDealTurn(:old.t_ID, :old.T_DATE_CLR, CHR(88));
     END IF;

     IF( UPDATING ) THEN

        :new.T_TURN := CHR(0);

        IF( (rsb_derivatives.DV_Setting_AccExContracts = 1) AND (:old.t_TYPE != 'E') AND (:old.t_TYPE != 'R') ) THEN
           :new.t_DEALCOST := :old.t_POSITIONCOST;
           UPDATE DDVDLTURN_DBT
              SET t_DEALCOST = :old.t_POSITIONCOST
            WHERE t_DealID = :old.t_ID
              AND t_Date   = :old.T_DATE_CLR;
        END IF;

     END IF;

  END IF;

  IF( INSERTING ) THEN
     :new.t_ID := v_NewID;
  END IF;

  TRGPCKG_DDVDEAL_DBT_TRBIUD.v_InTrgr := False;

EXCEPTION
  WHEN OTHERS THEN
    TRGPCKG_DDVDEAL_DBT_TRBIUD.v_NumEnt := 0;
    TRGPCKG_DDVDEAL_DBT_TRBIUD.v_InTrgr := False;
    RAISE;
END DDVDEAL_DBT_TRBU;
/