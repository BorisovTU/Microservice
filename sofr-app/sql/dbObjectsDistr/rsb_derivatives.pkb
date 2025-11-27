
-- Author  : Azartsov V.V.
-- Created : //AV 25.07.05
CREATE OR REPLACE PACKAGE BODY RSB_Derivatives IS

------------------------------------------------------------------------------------
  FUNCTION DV_Setting_RunFromTrust RETURN CHAR
  IS
  BEGIN
     return vDV_Setting_RunFromTrust;
  END;

  PROCEDURE RSI_DV_InitError
  AS
  BEGIN
     v_LastErrPackage := '';
  END;

  PROCEDURE RSI_DV_SetError( ErrNum IN INTEGER, ErrMes IN VARCHAR2 DEFAULT NULL )
  AS
  BEGIN
     IF( ErrMes IS NULL ) THEN
        v_LastErrPackage := '';
     ELSE
        v_LastErrPackage := ErrMes;
     END IF;
     RAISE_APPLICATION_ERROR( ErrNum,'' );
  END;

  PROCEDURE DV_GetLastErrPackage( ErrPkg OUT VARCHAR2 )
  IS
  BEGIN
     ErrPkg := v_LastErrPackage;
     v_LastErrPackage := '';
  END;

   -- Значение категории для субъекта "Учет ГО при посделочном учете бирж. контрактов"
  FUNCTION RSI_DV_PartyAttrGuaranty( PartyID IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
     CategoryValue dobjattr_dbt.t_NumInList % TYPE;
  BEGIN
     BEGIN
         SELECT Attr.t_NumInList INTO CategoryValue
           FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
          WHERE AtCor.t_ObjectType = 3  -- OBJTYPE_PARTY
            AND AtCor.t_GroupID    = 51 -- Учет ГО при посделочном учете бирж. контрактов
            AND AtCor.t_Object     = LPAD( PartyID, 10, '0' )
            AND Attr.t_AttrID      = AtCor.t_AttrID
            AND Attr.t_ObjectType  = AtCor.t_ObjectType
            AND Attr.t_GroupID     = AtCor.t_GroupID;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN CategoryValue := chr(0);
        WHEN OTHERS THEN
           return 0;
     END;

     IF( CategoryValue <> chr(0) ) THEN
        return to_number(CategoryValue);
     ELSE
        return 0;
     END IF;

     RETURN 0;
  END; -- RSI_DV_PartyAttrGuaranty

   -- Значение категории для субъекта "Расчет итог. сумм сделок по ПИ"
  FUNCTION RSI_DV_PartyCalcTotalAmount( PartyID IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
     CategoryValue dobjattr_dbt.t_NumInList % TYPE;
  BEGIN
     BEGIN
         SELECT Attr.t_NumInList INTO CategoryValue
           FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
          WHERE AtCor.t_ObjectType = 3  -- OBJTYPE_PARTY
            AND AtCor.t_GroupID    = 29 -- Расчет итог. сумм сделок по ПИ
            AND AtCor.t_Object     = LPAD( PartyID, 10, '0' )
            AND Attr.t_AttrID      = AtCor.t_AttrID
            AND Attr.t_ObjectType  = AtCor.t_ObjectType
            AND Attr.t_GroupID     = AtCor.t_GroupID;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN CategoryValue := chr(0);
        WHEN OTHERS THEN
           return 0;
     END;

     IF( CategoryValue <> chr(0) ) THEN
        return to_number(CategoryValue);
     ELSE
        return 0;
     END IF;

     RETURN 0;
  END; -- RSI_DV_PartyCalcTotalAmount

  -- настройки, инициализируются при старте ПИ
  --  DV_Setting_TurnPos NUMBER := 1;      -- Сворачивать позиции при исполнении контрактов
                                           --  0 - нет, 1 - по фьючерсам (умолч), 2 - по опционам, 3 - по всем контрактам
  --  DV_Setting_NullingTurn NUMBER := 1;  -- Обнулять итоги по нулевой нетто-позиции
                                           --  0 - нет, 1 - по фьючерсам (умолч), 2 - по опционам, 3 - по всем контрактам
  -- процедура осуществляет проверку позиции по заданным параметрам, и при необходимости, открывает ее.
  PROCEDURE RSI_DV_CheckAndOpenPosition(
              v_FIID         IN INTEGER, -- производный инструмент
              v_Department   IN INTEGER, -- Филиал
              v_Broker       IN INTEGER, -- Брокер
              v_Client       IN INTEGER, -- Клиент
              v_BrokerContr  IN INTEGER, -- Договор с брокером
              v_ClientContr  IN INTEGER, -- Договор с клиентом
              v_OperType     IN ddvdeal_dbt.t_Type%TYPE, -- Тип операции
              v_IsTrust      IN CHAR,
              v_OFBU         IN CHAR,
              v_GenAgrID     IN INTEGER
           )
  IS
     fipos             ddvfipos_dbt%ROWTYPE;
     v_oper_totalcalc  ddvoper_dbt.t_TOTALCALC%TYPE;
     v_exist_rec    INTEGER;
     v_exist_pos    INTEGER;
     v_GuarantyonDeal CHAR;
     v_Party        INTEGER;
  BEGIN

     if( v_BROKER > 0 AND v_BROKERCONTR <= 0 ) then
           RAISE_APPLICATION_ERROR(-20543,''); -- Не указан договор с брокером
     elsif( v_BROKER <= 0 AND v_BROKERCONTR > 0 ) then
           RAISE_APPLICATION_ERROR(-20544,''); -- Нельзя указывать договор с брокером в биржевой сделке
     elsif( v_Client > 0 AND v_CLIENTCONTR <= 0 ) then
           RAISE_APPLICATION_ERROR(-20545,''); -- Не указан договор с клиентом
     elsif( v_CLIENT <= 0 AND v_CLIENTCONTR > 0 ) then
           RAISE_APPLICATION_ERROR(-20546,''); -- Нельзя указывать договор с клиентом в сделке банка
     else
        if( v_ISTRUST !=  'X' ) then
           if( v_OFBU = 'X' ) then
              RAISE_APPLICATION_ERROR(-20547,''); -- Неверный тип клиента
           end if;
        else
           if( v_Client <= 0 ) then
              RAISE_APPLICATION_ERROR(-20549,''); -- Не задан клиент ДУ
           elsif( v_OFBU = 'X' ) then
              SELECT count(1)
                INTO v_exist_rec
                FROM ddvfipos_dbt
               WHERE t_CLIENT = v_Client
                 AND t_CLIENTCONTR != v_ClientContr;

              if( v_exist_rec > 0 ) then
                 RAISE_APPLICATION_ERROR(-20550,''); -- Неверный договор с ОФБУ
              end if;
           end if;

        end if;
     end if;

     BEGIN
        SELECT * INTO fipos
          FROM ddvfipos_dbt
         WHERE t_FIID        = v_FIID
           AND t_DEPARTMENT  = v_Department
           AND t_Broker      = v_Broker
           AND t_ClientContr = v_ClientContr
           AND t_GenAgrID    = v_GenAgrID;

        IF( fipos.t_state = DVPOS_STATE_CLOSE ) THEN
           RAISE_APPLICATION_ERROR(-20506,''); --Позиция по производному инструменту закрыта
        END IF;

        if( v_Broker > 0 AND v_BrokerContr != fipos.t_brokercontr ) then
           RAISE_APPLICATION_ERROR(-20551,''); -- Неверный договор с брокером
        end if;

        if( v_IsTrust != fipos.t_IsTrust OR v_OFBU != fipos.t_OFBU ) then
           RAISE_APPLICATION_ERROR(-20547,''); -- Неверный тип  клиента
        end if;

     EXCEPTION
       WHEN NO_DATA_FOUND THEN
        BEGIN
           IF( v_OperType = 'E' OR v_OperType = 'R') THEN
              RAISE_APPLICATION_ERROR(-20505,''); --Не открыта позиция по производному инструменту
           ELSE

              v_GuarantyonDeal := chr(0);
              IF( DV_Setting_AccExContracts = 1 ) THEN
                 v_Party := -1;
                 IF( v_Broker > 0 ) THEN
                    v_Party := v_Broker;
                 ELSE
                    BEGIN
                       SELECT fin.T_ISSUER INTO v_Party
                         FROM dfininstr_dbt fin
                        WHERE fin.t_FIID = v_FIID;
                       EXCEPTION WHEN OTHERS THEN v_Party := -1;
                    END;
                 END IF;

                 IF( RSI_DV_PartyAttrGuaranty(v_Party) = 2 ) THEN -- По сделке
                    v_GuarantyonDeal := chr(88);
                 END IF;
              END IF;

              INSERT INTO ddvfipos_dbt ins (ins.t_IsTrust, ins.t_OFBU, ins.t_FIID, ins.t_DEPARTMENT, ins.t_BROKER, ins.t_CLIENT, ins.t_BROKERCONTR, ins.t_CLIENTCONTR, ins.t_STATE, ins.t_GUARANTYONDEAL, ins.t_Sector, ins.t_GenAgrID)
                   VALUES (v_IsTrust, v_OFBU, v_FIID, v_Department, v_Broker, v_Client, v_BrokerContr, v_ClientContr, DVPOS_STATE_OPEN, v_GuarantyonDeal, chr(0), v_GenAgrID);
           END IF;
        END;
     END;
  END; --RSI_DV_CheckAndOpenPosition

  -- Привязка используемой записи по итогам дня по позиции.
  -- Процедура проверяет наличие и состояние записи по итогам дня по позиции и, при необходимости, создает ее. Счетчик ссылок увеличивается.
  -- Состояние позиции не проверяется (должно проверяться в вызывающей процедуре).
  -- Процедура должна вызываться всякий раз, когда возникает необходимость изменить данные по позиции: при вставке сделок, при импорте данных по позиции, при обработке позиций в операциях расчетов.
  PROCEDURE RSI_DV_AttachPositionTurn( v_FIID        IN INTEGER, -- производный инструмент
                                       v_DEPARTMENT  IN INTEGER, -- Филиал
                                       v_BROKER      IN INTEGER, -- Брокер
                                       v_ClientContr IN INTEGER, -- Договор с клиентом
                                       v_DATE        IN DATE,    -- Дата
                                       v_GenAgrID    IN INTEGER  -- Генеральное соглашение
                                     )
   IS
      v_exist_turn INTEGER;
      v_LONGPOSITION       ddvfiturn_dbt.t_LONGPOSITION%TYPE;
      v_SHORTPOSITION      ddvfiturn_dbt.t_SHORTPOSITION%TYPE;
      v_LONGPOSITIONCOST   ddvfiturn_dbt.t_LONGPOSITIONCOST%TYPE;
      v_SHORTPOSITIONCOST  ddvfiturn_dbt.t_SHORTPOSITIONCOST%TYPE;

      CURSOR FiTurn IS SELECT turn.t_FIID,turn.t_STATE
                         FROM ddvfiturn_dbt turn
                        WHERE turn.t_FIID        = v_FIID
                          AND turn.t_DEPARTMENT  = v_DEPARTMENT
                          AND turn.t_BROKER      = v_BROKER
                          AND turn.t_ClientContr = v_ClientContr
                          AND turn.t_DATE        = v_DATE
                          AND turn.t_GenAgrID    = v_GenAgrID;

      CURSOR FiTurnNoDate IS SELECT turn.t_FIID,turn.t_DATE, turn.t_STATE, turn.t_LONGPOSITION, turn.t_SHORTPOSITION, turn.t_LONGPOSITIONCOST, turn.t_SHORTPOSITIONCOST
                               FROM ddvfiturn_dbt turn
                              WHERE turn.t_FIID        = v_FIID
                                AND turn.t_DEPARTMENT  = v_DEPARTMENT
                                AND turn.t_BROKER      = v_BROKER
                                AND turn.t_ClientContr = v_ClientContr
                                AND turn.t_DATE        < v_DATE
                                AND turn.t_GenAgrID    = v_GenAgrID
                           ORDER BY t_DATE DESC;
   BEGIN
      SELECT count(1) INTO v_exist_turn
        FROM ddvfiturn_dbt turn
       WHERE turn.t_FIID        = v_FIID
         AND turn.t_DEPARTMENT  = v_DEPARTMENT
         AND turn.t_BROKER      = v_BROKER
         AND turn.t_ClientContr = v_ClientContr
         AND turn.t_DATE        > v_DATE
         AND turn.t_GenAgrID    = v_GenAgrID;

      IF( v_exist_turn > 0 ) THEN
         --Открыт день по позиции за большую дату
         RAISE_APPLICATION_ERROR(-20518,'');
      END IF;

      v_exist_turn := 0;
      -- если итоги есть есть - проверим их
      FOR fi_turn IN FiTurn LOOP

         v_exist_turn := 1;

         IF( fi_turn.t_STATE = DVTURN_STATE_CLOSE ) THEN
            RAISE_APPLICATION_ERROR(-20509,''); -- День по позиции закрыт
         ELSE
            UPDATE ddvfiturn_dbt SET t_REFCOUNTER = t_REFCOUNTER + 1
             WHERE t_FIID        = v_FIID
               AND t_DEPARTMENT  = v_DEPARTMENT
               AND t_BROKER      = v_BROKER
               AND t_ClientContr = v_ClientContr
               AND t_DATE        = v_DATE
               AND t_GenAgrID    = v_GenAgrID;
         END IF;

      END LOOP;

      -- если итогов за заданную дату нет - проверим те что есть и если все нормально добавим новую запись итогов
      IF( v_exist_turn = 0 ) THEN

         v_LONGPOSITION       := 0;
         v_SHORTPOSITION      := 0;
         v_LONGPOSITIONCOST   := 0;
         v_SHORTPOSITIONCOST  := 0;

         FOR fi_turn_no_date IN FiTurnNoDate LOOP

            IF( fi_turn_no_date.t_STATE != DVTURN_STATE_CLOSE ) THEN
               RAISE_APPLICATION_ERROR(-20519,''); -- Не закрыт предыдущий день по позиции
            ELSE -- существует закрытая запись итогов за предыдущий день
               v_LONGPOSITION       := fi_turn_no_date.t_LONGPOSITION;
               v_SHORTPOSITION      := fi_turn_no_date.t_SHORTPOSITION;
               v_LONGPOSITIONCOST   := fi_turn_no_date.t_LONGPOSITIONCOST;
               v_SHORTPOSITIONCOST  := fi_turn_no_date.t_SHORTPOSITIONCOST;
               EXIT;
            END IF;
         END LOOP;

         -- добавим запись итогов
         INSERT INTO ddvfiturn_dbt ( t_FIID,t_DEPARTMENT,t_BROKER,t_DATE,
                                     t_BUY,t_SALE,t_LONGEXECUTION,t_SHORTEXECUTION,
                                     t_LONGPOSITION, t_SHORTPOSITION, t_LONGPOSITIONCOST, t_SHORTPOSITIONCOST,
                                     t_MARGIN, t_PaidBonus, t_RECEIVEDBONUS,
                                     t_STATE,
                                     t_REFCOUNTER,
                                     t_GUARANTY,
                                     t_FAIRVALUE,
                                     t_FAIRVALUECALC,
                                     t_ClientContr,
                                     t_BrokerContr,
                                     t_Client,
                                     t_IsTrust,
                                     T_SETMARGIN, T_SETGUARANTY, T_SETFAIRVALUE, T_FAIRVALUECALCNUMBER,
                                     t_Sector,
                                     t_OldMargin,
                                     t_OldSetMargin,
                                     t_OldFairValueCalc,
                                     t_OldFairValue,
                                     t_OldSetFairValue,
                                     t_GenAgrID
                                   )
                                   VALUES
                                   ( v_FIID,v_DEPARTMENT,v_BROKER,v_DATE,
                                     0,0,0,0,
                                     v_LONGPOSITION,v_SHORTPOSITION,v_LONGPOSITIONCOST,v_SHORTPOSITIONCOST,
                                     0,0,0,
                                     DVTURN_STATE_OPEN, -- "открытая"
                                     1,
                                     0,      -- "T_GUARANTY "
                                     0,      -- t_FAIRVALUE
                                     chr(0), -- t_FAIRVALUECALC
                                     v_ClientContr,
                                     (SELECT t_BrokerContr
                                        FROM DDVFIPOS_DBT pos
                                       WHERE pos.t_FIID        = v_FIID
                                         AND pos.t_DEPARTMENT  = v_DEPARTMENT
                                         AND pos.t_BROKER      = v_BROKER
                                         AND pos.t_ClientContr = v_ClientContr
                                         AND pos.t_GenAgrID    = v_GenAgrID
                                     ),
                                     (SELECT t_Client
                                        FROM DDVFIPOS_DBT pos
                                       WHERE pos.t_FIID        = v_FIID
                                         AND pos.t_DEPARTMENT  = v_DEPARTMENT
                                         AND pos.t_BROKER      = v_BROKER
                                         AND pos.t_ClientContr = v_ClientContr
                                         AND pos.t_GenAgrID    = v_GenAgrID
                                     ),
                                     (SELECT t_IsTrust
                                        FROM DDVFIPOS_DBT pos
                                       WHERE pos.t_FIID        = v_FIID
                                         AND pos.t_DEPARTMENT  = v_DEPARTMENT
                                         AND pos.t_BROKER      = v_BROKER
                                         AND pos.t_ClientContr = v_ClientContr
                                         AND pos.t_GenAgrID    = v_GenAgrID
                                     ),
                                     chr(0), chr(0), chr(0), 0,
                                     chr(0)/*v_Sector*/,
                                     0, chr(0), chr(0), 0, chr(0),
                                     v_GenAgrID
                                   );
      END IF;

   END; --RSI_DV_AttachPositionTurn

  -- Освобождение используемой записи по итогам дня по позиции.
  -- Процедура уменьшает счетчик ссылок. Если запись больше не используется, она удаляется.
  -- Процедура должна вызываться при откате операций, использующих записи по итогам дня.
  PROCEDURE RSI_DV_DetachPositionTurn
            (
               v_FIID         IN INTEGER, -- производный инструмент
               v_DEPARTMENT   IN INTEGER, -- Филиал
               v_BROKER       IN INTEGER, -- Брокер
               v_ClientContr  IN INTEGER, -- Клиент договор
               v_Date         IN DATE,    -- Дата
               v_GenAgrID     IN INTEGER  -- ГС
            )
  IS
     v_exist_turn INTEGER;

     CURSOR FiPos IS SELECT pos.t_FIID,pos.t_STATE
                       FROM ddvfipos_dbt pos
                      WHERE pos.t_FIID        = v_FIID
                        AND pos.t_DEPARTMENT  = v_DEPARTMENT
                        AND pos.t_BROKER      = v_BROKER
                        AND pos.t_ClientContr = v_ClientContr
                        AND pos.t_GenAgrID    = v_GenAgrID;

     CURSOR FiTurn IS SELECT turn.t_FIID, turn.t_STATE, turn.t_REFCOUNTER
                        FROM ddvfiturn_dbt turn
                       WHERE turn.t_FIID        = v_FIID
                         AND turn.t_DEPARTMENT  = v_DEPARTMENT
                         AND turn.t_BROKER      = v_BROKER
                         AND turn.t_ClientContr = v_ClientContr
                         AND turn.t_GenAgrID    = v_GenAgrID
                         AND turn.t_DATE        = v_DATE;
  BEGIN

     -- если позиция есть - проверим ее
     FOR fi_pos IN FiPos LOOP
        IF( fi_pos.t_STATE = DVPOS_STATE_CLOSE ) THEN
           RAISE_APPLICATION_ERROR(-20506,'');--Позиция по производному инструменту закрыта
        END IF;
     END LOOP;

     v_exist_turn := 0;
     -- если итоги есть - проверим их
     FOR fi_turn IN FiTurn LOOP

        v_exist_turn := 1;

        IF( fi_turn.t_STATE = DVTURN_STATE_CLOSE ) THEN
           RAISE_APPLICATION_ERROR(-20509,'');--День по позиции закрыт
        ELSE
           IF( fi_turn.t_REFCOUNTER <= 1 ) THEN
              DELETE FROM ddvfiturn_dbt
               WHERE t_FIID        = v_FIID
                 AND t_DEPARTMENT  = v_DEPARTMENT
                 AND t_BROKER      = v_BROKER
                 AND t_ClientContr = v_ClientContr
                 AND t_GenAgrID    = v_GenAgrID
                 AND t_DATE        = v_DATE;
           ELSE
              UPDATE ddvfiturn_dbt SET t_REFCOUNTER = t_REFCOUNTER - 1
               WHERE t_FIID        = v_FIID
                 AND t_DEPARTMENT  = v_DEPARTMENT
                 AND t_BROKER      = v_BROKER
                 AND t_ClientContr = v_ClientContr
                 AND t_GenAgrID    = v_GenAgrID
                 AND t_DATE        = v_DATE;
           END IF;
        END IF;

     END LOOP;

     IF( v_exist_turn = 0 ) THEN
        RAISE_APPLICATION_ERROR(-20512,'');--Не открыт день по позиции
     END IF;

  END; -- RSI_DV_DetachPositionTurn

  -- Возвращает число не рассчитываемых комиссий по позиции за день
  FUNCTION RSI_DV_GetCountNotCalcPosCom
            (
               v_FIID        IN INTEGER, -- производный инструмент
               v_DEPARTMENT  IN INTEGER, -- Филиал
               v_BROKER      IN INTEGER, -- Брокер
               v_ClientContr IN INTEGER, -- Клиент договор
               v_Date        IN DATE,    -- Дата
               v_GenAgrID    IN INTEGER  -- ГС
            ) RETURN NUMBER
  IS
     v_Count INTEGER;
  BEGIN
     SELECT COUNT(1) INTO v_Count
       FROM ddvfi_com_dbt
      WHERE t_FIID        =  v_FIID
        AND t_DEPARTMENT  =  v_DEPARTMENT
        AND t_BROKER      =  v_BROKER
        AND t_ClientContr =  v_ClientContr
        AND t_DATE        =  v_DATE
        AND t_GenAgrID    =  v_GenAgrID
        AND t_NOTCALC     =  CHR(88);

     RETURN v_Count;
  END; -- RSI_DV_GetCountNotCalcPosCom

  -- Закрытие одной позиции.
  -- Используется при закрытии позиции из интерфейса.
  -- Вход:    Производный инструмент FIID, Филиал DEPARTMENT, Брокер BROKER, Клиент CLIENT /*Дата закрытия CLOSEDATE - пока не реализуется
  PROCEDURE RSI_DV_CloseOnePosition
            (
               v_FIID        IN INTEGER, -- производный инструмент
               v_DEPARTMENT  IN INTEGER, -- Филиал
               v_BROKER      IN INTEGER, -- Брокер
               v_CLIENTCONTR IN INTEGER, -- Клиент
               v_GenAgrID    IN INTEGER  -- ГС
            )
   IS
      v_exist_turn    INTEGER;
      v_exist_fin     INTEGER;
      v_num_opers     INTEGER;
      v_LONGPOSITION  ddvfiturn_dbt.t_LONGPOSITION%TYPE;
      v_SHORTPOSITION ddvfiturn_dbt.t_SHORTPOSITION%TYPE;
      v_TurnMaxDate   ddvfiturn_dbt.t_DATE%TYPE := TO_DATE('01.01.0001', 'dd.mm.yyyy');
      v_DrawingDate   dfininstr_dbt.t_DRAWINGDATE%TYPE := TO_DATE('01.01.0001', 'dd.mm.yyyy');
   BEGIN
      -- 1
      BEGIN
         v_exist_turn := 1;
         SELECT turn.t_Date, turn.t_LONGPOSITION, turn.t_SHORTPOSITION INTO v_TurnMaxDate, v_LONGPOSITION, v_SHORTPOSITION
           FROM ddvfiturn_dbt turn
          WHERE turn.t_FIID        = v_FIID
            AND turn.t_DEPARTMENT  = v_DEPARTMENT
            AND turn.t_BROKER      = v_BROKER
            AND turn.t_CLIENTCONTR = v_CLIENTCONTR
            AND turn.t_GenAgrID    = v_GenAgrID
            AND turn.t_Date = ( SELECT Max(turn1.t_Date)
                                  FROM ddvfiturn_dbt turn1
                                 WHERE turn1.t_FIID        = v_FIID
                                   AND turn1.t_DEPARTMENT  = v_DEPARTMENT
                                   AND turn1.t_BROKER      = v_BROKER
                                   AND turn1.t_CLIENTCONTR = v_CLIENTCONTR
                                   AND turn1.t_GenAgrID    = v_GenAgrID
                              );
         EXCEPTION
           WHEN NO_DATA_FOUND THEN
             v_exist_turn := 0;
      END;

      -- 2
      IF( v_exist_turn = 0 ) THEN
         RAISE_APPLICATION_ERROR(-20534,''); --По позиции не было операций
      END IF;

      IF( v_exist_turn != 0 ) THEN
         BEGIN
            v_exist_fin := 1;
            SELECT fin.t_DrawingDate INTO v_DrawingDate
              FROM dfininstr_dbt fin
             WHERE fin.t_FIID = v_FIID;
            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                v_exist_fin := 0;
         END;

         -- 3
         IF( v_exist_fin != 0 AND v_TurnMaxDate >= v_DrawingDate ) THEN
            RAISE_APPLICATION_ERROR(-20535,''); --Дата последнего дня должна быть меньше даты исполнения
         END IF;

         -- 4
         IF( v_LONGPOSITION != 0 OR v_SHORTPOSITION != 0 ) THEN
            RAISE_APPLICATION_ERROR(-20536,'');
         END IF;

      END IF;

      -- 5
      -- обновить DDVFIPOS - установить статус - Закрыта и дату закрытия
      UPDATE ddvfipos_dbt SET t_STATE = DVPOS_STATE_CLOSE, T_CLOSEDATE = v_TurnMaxDate
       WHERE t_FIID        = v_FIID
         AND t_DEPARTMENT  = v_DEPARTMENT
         AND t_BROKER      = v_BROKER
         AND t_CLIENTCONTR = v_CLIENTCONTR
         AND t_GenAgrID    = v_GenAgrID;

   END; --RSI_DV_CloseOnePosition

  -- Откат закрытия позиции.
  -- Используется при откате закрытия (открытии) позиции из интерфейса.
  -- Вход:    Производный инструмент FIID, Филиал DEPARTMENT, Брокер BROKER, Клиент CLIENT, Флаг  - создавать записи итогов дня  CREATETURN
  PROCEDURE RSI_DV_RecoilCloseOnePosition
            (
              v_FIID         IN INTEGER, -- производный инструмент
              v_DEPARTMENT   IN INTEGER, -- Филиал
              v_BROKER       IN INTEGER, -- Брокер
              v_CLIENTCONTR  IN INTEGER, -- Клиент
              v_GenAgrID     IN INTEGER  -- ГС
            )
  IS
     v_exist_pos INTEGER;
     v_exist_fin INTEGER;
     v_num_opers INTEGER;

     v_CloseDate    ddvfipos_dbt.t_CLOSEDATE%TYPE := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     v_DrawingDate  dfininstr_dbt.t_DRAWINGDATE%TYPE := TO_DATE('01.01.0001', 'dd.mm.yyyy');

     CURSOR DvOperClose IS SELECT dvoper.t_Date OperDate
                             FROM ddvoper_dbt dvoper, doprkoper_dbt oprkoper
                            WHERE dvoper.t_DOCKIND    = RSB_Derivatives.DV_GetDocKind AND
                                  dvoper.t_DEPARTMENT = v_DEPARTMENT AND
                                  dvoper.t_GenAgrID   = v_GenAgrID AND
                                  dvoper.t_PARTY      = DECODE( v_BROKER, -1, (SELECT fin.t_ISSUER
                                                                                 FROM dfininstr_dbt fin
                                                                                WHERE fin.t_FIID = v_FIID), v_BROKER )
                                  AND
                                  (
                                    (v_BROKER = -1) OR
                                    (
                                       v_BROKER != -1
                                       AND dvoper.t_PARTYCONTR = (SELECT fipos.t_BROKERCONTR
                                                                    FROM ddvfipos_dbt fipos
                                                                   WHERE fipos.t_FIID = v_FIID
                                                                     AND fipos.t_BROKER = v_BROKER
                                                                     AND fipos.t_CLIENTCONTR = v_CLIENTCONTR
                                                                     AND fipos.t_DEPARTMENT = v_DEPARTMENT
                                                                     AND fipos.t_GenAgrID = v_GenAgrID)
                                    )
                                  ) AND
                                  dvoper.t_Date > v_CloseDate AND dvoper.t_Date < v_DrawingDate AND
                                  dvoper.t_State = DVOPER_STATE_CLOSE AND -- закрыта
                                  oprkoper.t_Kind_Operation = dvoper.t_OperKind AND
                                  instr(oprkoper.t_SysTypes, 'C') = 0;

  BEGIN
     -- 1
     BEGIN
        v_exist_pos := 1;
        SELECT pos.t_CloseDate INTO v_CloseDate
          FROM ddvfipos_dbt pos
         WHERE pos.t_FIID        = v_FIID
           AND pos.t_DEPARTMENT  = v_DEPARTMENT
           AND pos.t_BROKER      = v_BROKER
           AND pos.t_CLIENTCONTR = v_CLIENTCONTR
           AND pos.t_GenAgrID    = v_GenAgrID;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_exist_pos := 0;
     END;

     IF( v_exist_pos != 0 ) THEN
        BEGIN
           v_exist_fin := 1;
           SELECT fin.t_DrawingDate INTO v_DrawingDate
             FROM dfininstr_dbt fin
            WHERE fin.t_FIID = v_FIID;
           EXCEPTION
             WHEN NO_DATA_FOUND THEN
               v_exist_fin := 0;
        END;

        IF( v_exist_fin = 1 AND v_CloseDate >= v_DrawingDate ) THEN
           RAISE_APPLICATION_ERROR(-20520,''); --Позиция закрыта датой исполнения
        END IF;

     END IF;

     -- 2  Проверить DDVOPER
     SELECT COUNT(1) INTO v_num_opers
       FROM ddvoper_dbt dvoper, doprkoper_dbt oprkoper
      WHERE dvoper.t_DOCKIND    = RSB_Derivatives.DV_GetDocKind AND
            dvoper.t_DEPARTMENT = v_DEPARTMENT AND
            dvoper.t_GenAgrID = v_GenAgrID AND
            dvoper.t_PARTY = DECODE( v_BROKER, -1, (SELECT fin.t_ISSUER
                                                      FROM dfininstr_dbt fin
                                                     WHERE fin.t_FIID = v_FIID), v_BROKER )
            AND
            (
              (v_BROKER = -1) OR
              (
                 v_BROKER != -1
                 AND dvoper.t_PARTYCONTR = (SELECT fipos.t_BROKERCONTR
                                              FROM ddvfipos_dbt fipos
                                             WHERE fipos.t_FIID        = v_FIID
                                               AND fipos.t_BROKER      = v_BROKER
                                               AND fipos.t_CLIENTCONTR = v_CLIENTCONTR
                                               AND fipos.t_DEPARTMENT  = v_DEPARTMENT
                                               AND fipos.t_GenAgrID    = v_GenAgrID)
              )
            ) AND
            dvoper.t_State = DVOPER_STATE_OPEN AND -- открыта
            oprkoper.t_Kind_Operation = dvoper.t_OperKind AND
            instr(oprkoper.t_SysTypes, 'C') = 0;

     IF( v_num_opers <> 0 ) THEN
        RAISE_APPLICATION_ERROR(-20521,''); --По позиции есть открытые операции расчетов
     END IF;

     -- 3 обновить DDVFIPOS - установить статус - Открыта
     UPDATE ddvfipos_dbt SET t_STATE = DVPOS_STATE_OPEN
      WHERE t_FIID        = v_FIID
        AND t_DEPARTMENT  = v_DEPARTMENT
        AND t_BROKER      = v_BROKER
        AND t_CLIENTCONTR = v_CLIENTCONTR
        AND t_GenAgrID    = v_GenAgrID;

     -- 4. Если CREATETURN == "Да" то для всех записей в DDVOPER, таких что:
     IF( DV_Setting_CreateTurn <> 0 ) THEN
        FOR dvoper IN DvOperClose LOOP
           --   5.1.  Выполнить RSI_DV_AttachPositionTurn
           RSI_DV_AttachPositionTurn( v_FIID, v_DEPARTMENT, v_BROKER, v_CLIENTCONTR, dvoper.OperDate, v_GenAgrID );
           --   5.2
           UPDATE ddvfiturn_dbt SET t_STATE = DVTURN_STATE_CLOSE
            WHERE t_FIID        = v_FIID
              AND t_DEPARTMENT  = v_DEPARTMENT
              AND t_BROKER      = v_BROKER
              AND t_CLIENTCONTR = v_CLIENTCONTR
              AND t_DATE        = dvoper.OperDate
              AND t_GenAgrID    = v_GenAgrID;
        END LOOP;
     END IF;

  END; --RSI_DV_RecoilCloseOnePosition

  --Открытие итогов дня по позициям
  --Выполняется в операции расчетов.
  PROCEDURE RSI_DV_OpenPositionTurns( v_PARTYKIND    IN INTEGER, -- Вид контрагента по расчетам
                                      v_PARTY        IN INTEGER, -- Контрагент
                                      v_PARTYCONTR   IN INTEGER, -- Договор с контрагентом
                                      v_DEPARTMENT   IN INTEGER, -- Филиал
                                      v_Date         IN DATE,    -- Дата
                                      v_Flag1        IN INTEGER, -- Flag1 Собственные/Клиентские
                                      v_GenAgrID     IN INTEGER  -- ГС
                                    )
  IS
     CURSOR FiPos IS SELECT pos.t_FIID, pos.t_BROKER, pos.t_CLIENTCONTR, pos.t_GenAgrID
                       FROM ddvfipos_dbt pos
                      WHERE pos.t_DEPARTMENT = v_DEPARTMENT
                        AND ((v_PARTYKIND = 3  AND v_PARTY = ( SELECT fin.t_ISSUER FROM dfininstr_dbt fin WHERE fin.t_FIID = pos.t_FIID) AND pos.t_BROKER = -1) OR
                             (v_PARTYKIND = 22 AND pos.t_BROKER = v_PARTY AND pos.t_BROKERCONTR = v_PARTYCONTR))
                        AND pos.t_IsTrust = DV_Setting_RunFromTrust
                        AND pos.t_STATE = DVPOS_STATE_OPEN
                        AND (SELECT fin.t_DRAWINGDATE FROM dfininstr_dbt fin WHERE fin.t_FIID = pos.t_FIID) >= v_Date
                        AND (((v_Flag1 = ALG_SP_MONEY_SOURCE_OWN) AND (pos.t_Client <= 0)) OR
                             ((v_Flag1 = ALG_SP_MONEY_SOURCE_CLIENT) AND (pos.t_Client >= 1)) OR
                             ((v_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) AND (pos.t_IsTrust = chr(88))))
                        AND pos.t_GenAgrID = v_GenAgrID
                        AND EXISTS( SELECT 1
                                      FROM ddvfiturn_dbt turn
                                     WHERE turn.t_IsTrust     = pos.t_IsTrust
                                       AND turn.t_FIID        = pos.t_FIID
                                       AND turn.t_DEPARTMENT  = pos.t_DEPARTMENT
                                       AND turn.t_BROKER      = pos.t_BROKER
                                       AND turn.t_GenAgrID    = pos.t_GenAgrID
                                       AND turn.t_CLIENTCONTR = pos.t_CLIENTCONTR
                                       AND turn.t_DATE        < v_Date
                                  );
  BEGIN
     FOR fi_pos IN FiPos LOOP
        RSI_DV_AttachPositionTurn(fi_pos.t_FIID, v_DEPARTMENT, fi_pos.t_BROKER, fi_pos.t_CLIENTCONTR, v_DATE, fi_pos.t_GenAgrID);

        IF( DV_Setting_AccExContracts = 1 ) THEN -- По сделке
           DECLARE
              CURSOR DVDeal IS SELECT deal.t_ID
                                 FROM ddvdeal_dbt deal
                                WHERE deal.t_FIID        = fi_pos.t_FIID
                                  AND deal.t_DEPARTMENT  = v_DEPARTMENT
                                  AND deal.t_BROKER      = fi_pos.t_BROKER
                                  AND deal.t_CLIENTCONTR = fi_pos.t_CLIENTCONTR
                                  AND deal.t_GenAgrID    = fi_pos.t_GenAgrID
                                  AND EXISTS( SELECT 1
                                                FROM ddvdlturn_dbt turn
                                               WHERE turn.t_DealID = deal.t_ID
                                                 AND turn.t_DATE   < v_Date
                                            );
           BEGIN
              FOR Deal IN DVDeal LOOP
                 RSI_DV_AttachDealTurn(Deal.t_ID, v_Date);
              END LOOP;
           END;
        END IF;
     END LOOP;
  END; --RSI_DV_OpenPositionTurns

  --Откат открытия итогов дня по позициям
  --Процедура откатывает открытие дня.
  --Выполняется при откате операции расчетов.
  PROCEDURE RSI_DV_RecoilOpenPositionTurns( v_PARTYKIND  IN INTEGER, -- Вид контрагента по расчетам
                                            v_PARTY      IN INTEGER, -- Контрагент
                                            v_PARTYCONTR IN INTEGER, -- Договор с контрагентом
                                            v_DEPARTMENT IN INTEGER, -- Филиал
                                            v_Date       IN DATE,    -- Дата
                                            v_Flag1      IN INTEGER, -- Flag1 Собственные/Клиентские
                                            v_GenAgrID   IN INTEGER  -- ГС
                                          )
  IS
     CURSOR FiPos IS SELECT pos.t_FIID, pos.t_BROKER, pos.t_CLIENTCONTR, pos.t_Department, pos.t_GenAgrID
                       FROM ddvfipos_dbt pos
                      WHERE pos.t_DEPARTMENT = v_DEPARTMENT
                        AND pos.t_IsTrust = DV_Setting_RunFromTrust
                        AND ((v_PARTYKIND = 3  AND v_PARTY = ( SELECT fin.t_ISSUER FROM dfininstr_dbt fin WHERE fin.t_FIID = pos.t_FIID) AND pos.t_BROKER = -1) OR
                             (v_PARTYKIND = 22 AND pos.t_BROKER = v_PARTY AND pos.t_BROKERCONTR = v_PARTYCONTR))
                        AND (SELECT fin.t_DRAWINGDATE FROM dfininstr_dbt fin WHERE fin.t_FIID = pos.t_FIID) >= v_Date
                        AND (((v_Flag1 = ALG_SP_MONEY_SOURCE_OWN) AND (pos.t_Client <= 0)) OR
                             ((v_Flag1 = ALG_SP_MONEY_SOURCE_CLIENT) AND (pos.t_Client >= 1)) OR
                             ((v_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) AND (pos.t_IsTrust = chr(88))))
                        AND pos.t_GenAgrID = v_GenAgrID
                        AND EXISTS( SELECT 1
                                      FROM ddvfiturn_dbt turn
                                     WHERE turn.t_IsTrust     = pos.t_IsTrust
                                       AND turn.t_FIID        = pos.t_FIID
                                       AND turn.t_DEPARTMENT  = pos.t_DEPARTMENT
                                       AND turn.t_BROKER      = pos.t_BROKER
                                       AND turn.t_CLIENTCONTR = pos.t_CLIENTCONTR
                                       AND turn.t_GenAgrID    = pos.t_GenAgrID
                                       AND turn.t_DATE        < v_Date
                                  )
                        AND EXISTS( SELECT 1
                                      FROM ddvfiturn_dbt turn
                                     WHERE turn.t_IsTrust     = pos.t_IsTrust
                                       AND turn.t_FIID        = pos.t_FIID
                                       AND turn.t_DEPARTMENT  = pos.t_DEPARTMENT
                                       AND turn.t_BROKER      = pos.t_BROKER
                                       AND turn.t_CLIENTCONTR = pos.t_CLIENTCONTR
                                       AND turn.t_GenAgrID    = pos.t_GenAgrID
                                       AND turn.t_DATE        = v_Date
                                  );
  BEGIN

     FOR fi_pos IN FiPos LOOP
        RSI_DV_DetachPositionTurn(fi_pos.t_FIID, fi_pos.t_Department, fi_pos.t_BROKER, fi_pos.t_CLIENTCONTR, v_DATE, fi_pos.t_GenAgrID);

        IF( DV_Setting_AccExContracts = 1 ) THEN -- По сделке
           DECLARE
              CURSOR DVDeal IS SELECT deal.t_ID
                                 FROM ddvdeal_dbt deal
                                WHERE deal.t_FIID        = fi_pos.t_FIID
                                  AND deal.t_DEPARTMENT  = fi_pos.t_DEPARTMENT
                                  AND deal.t_BROKER      = fi_pos.t_BROKER
                                  AND deal.t_CLIENTCONTR = fi_pos.t_CLIENTCONTR
                                  AND deal.t_GenAgrID    = fi_pos.t_GenAgrID
                                  AND EXISTS( SELECT 1
                                                FROM ddvdlturn_dbt turn
                                               WHERE turn.t_DealID = deal.t_ID
                                                 AND turn.t_DATE   < v_Date
                                            );
           BEGIN
              FOR Deal IN DVDeal LOOP
                 RSI_DV_DetachDealTurn(Deal.t_ID, v_Date);
              END LOOP;
           END;
        END IF;
     END LOOP;

  END; --RSI_DV_RecoilOpenPositionTurns

  -- Закрытие итогов дня
  -- Выполняется в операции расчетов.
  PROCEDURE RSI_DV_ClosePositionTurns( v_PARTYKIND  IN INTEGER, -- Вид контрагента по расчетам
                                       v_PARTY      IN INTEGER, -- Контрагент
                                       v_PARTYCONTR IN INTEGER, -- Договор с контрагентом
                                       v_DEPARTMENT IN INTEGER, -- Филиал
                                       v_Date       IN DATE,    -- Дата
                                       v_Flag1      IN INTEGER, -- Flag1 Собственные/Клиентские
                                       v_GenAgrID   IN INTEGER  -- ГС
                                     )
  IS
     v_exist_dvdeal INTEGER;
  BEGIN
     -- проверим dvdeal
     BEGIN
        v_exist_dvdeal := 0;
        SELECT COUNT(*) INTO v_exist_dvdeal
          FROM ddvdeal_dbt deal
         WHERE deal.t_DEPARTMENT = v_DEPARTMENT
           AND deal.t_IsTrust = DV_Setting_RunFromTrust
           AND deal.t_Date_CLR = v_DATE
           AND deal.t_Amount     = deal.t_Execution /*PNV 516074*/
           AND ((v_PARTYKIND = 3  AND v_PARTY = (SELECT fin.t_ISSUER FROM dfininstr_dbt fin WHERE fin.t_FIID = deal.t_FIID) AND deal.t_BROKER = -1) OR
                (v_PARTYKIND = 22 AND deal.t_BROKER = v_PARTY AND deal.t_BROKERCONTR = v_PARTYCONTR))
           AND (((v_Flag1 = ALG_SP_MONEY_SOURCE_OWN) AND (deal.t_Client <= 0)) OR
                ((v_Flag1 = ALG_SP_MONEY_SOURCE_CLIENT) AND (deal.t_Client >= 1)) OR
                ((v_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) AND (deal.t_IsTrust = chr(88))))
           AND (((DV_Setting_AccExContracts = 0) and (deal.t_STATE != DVDEAL_STATE_CLOSE)) or
                ((DV_Setting_AccExContracts = 1) and (deal.t_PosAcc != chr(88))))
           AND deal.t_GenAgrID = v_GenAgrID;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN
           v_exist_dvdeal := 0;
     END;

     IF( v_exist_dvdeal > 0 ) THEN
        RAISE_APPLICATION_ERROR(-20515,'');  -- Не закрыты операции с контрагентом
     END IF;

     -- Для всех подходящих DDVFITURN проставим статус 'Закрыт'
     UPDATE ddvfiturn_dbt turn SET turn.t_State = DVTURN_STATE_CLOSE
      WHERE turn.t_Department = v_DEPARTMENT
        AND ((v_PARTYKIND = 3  AND v_PARTY = (SELECT fin.t_ISSUER FROM dfininstr_dbt fin WHERE fin.t_FIID = turn.t_FIID) AND turn.t_BROKER = -1) OR
             (v_PARTYKIND = 22 AND turn.t_BROKER = v_PARTY AND turn.t_BROKERCONTR = v_PARTYCONTR ))
        AND turn.t_IsTrust = DV_Setting_RunFromTrust
        AND turn.t_State   = DVTURN_STATE_OPEN
        AND (((v_Flag1 = ALG_SP_MONEY_SOURCE_OWN) AND (turn.t_Client <= 0)) OR
             ((v_Flag1 = ALG_SP_MONEY_SOURCE_CLIENT) AND (turn.t_Client >= 1)) OR
             ((v_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) AND (turn.t_IsTrust = chr(88))))
        AND turn.t_Date = v_DATE
        AND turn.t_GenAgrID = v_GenAgrID;

  END; -- RSI_DV_ClosePositionTurns

  -- Откат закрытия итогов дня.
  -- Процедура откатывает закрытие позиций и итогов дня дня.
  -- Выполняется при откате операции расчетов.
  PROCEDURE RSI_DV_RecoilClosePosTurns( v_PARTYKIND  IN INTEGER, -- Вид контрагента по расчетам
                                        v_PARTY      IN INTEGER, -- Контрагент
                                        v_PARTYCONTR IN INTEGER, -- Договор с контрагентом
                                        v_DEPARTMENT IN INTEGER, -- Филиал
                                        v_Date       IN DATE,    -- Дата
                                        v_Flag1      IN INTEGER, -- Flag1 Собственные/Клиентские
                                        v_GenAgrID   IN INTEGER  -- ГС
                                      )
  IS
     v_exist_fipos INTEGER;
  BEGIN
     -- проверим dvfipos
     BEGIN
        SELECT COUNT(1) INTO v_exist_fipos
          FROM ddvfipos_dbt pos, ddvfiturn_dbt turn
         WHERE pos.t_DEPARTMENT = v_DEPARTMENT AND
               ( (v_PARTYKIND = 3  AND v_PARTY = ( SELECT fin.t_ISSUER FROM dfininstr_dbt fin WHERE fin.t_FIID = pos.t_FIID) AND pos.t_BROKER = -1) OR
                 (v_PARTYKIND = 22 AND pos.t_BROKER = v_PARTY AND pos.t_BROKERCONTR = v_PARTYCONTR)
               ) AND
               pos.t_GenAgrID    = v_GenAgrID AND
               turn.t_GenAgrID   = pos.t_GenAgrID AND
               turn.t_IsTrust    = pos.t_IsTrust  AND
               turn.t_FIID       = pos.t_FIID     AND
               turn.t_DEPARTMENT = v_DEPARTMENT   AND
               turn.t_BROKER     = pos.t_BROKER   AND
               turn.t_CLIENTCONTR= pos.t_CLIENTCONTR AND
               turn.t_DATE       = v_DATE         AND
               (((v_Flag1 = ALG_SP_MONEY_SOURCE_OWN) AND (pos.t_Client <= 0)) OR
                ((v_Flag1 = ALG_SP_MONEY_SOURCE_CLIENT) AND (pos.t_Client >= 1)) OR
                ((v_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) AND (pos.t_IsTrust = chr(88)))) AND
               pos.t_STATE       =  DVPOS_STATE_CLOSE;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_exist_fipos := 0;
     END;

     IF( v_exist_fipos > 0 ) THEN
        RAISE_APPLICATION_ERROR(-20506,''); -- Позиция по производному инструменту закрыта
     END IF;

     -- Для всех подходящих DDVFITURN проставим статус 'Открыт'
     UPDATE ddvfiturn_dbt turn SET turn.t_STATE = DVTURN_STATE_OPEN
      WHERE turn.t_DEPARTMENT = v_DEPARTMENT AND
            ( (v_PARTYKIND = 3  AND v_PARTY = (SELECT fin.t_ISSUER FROM dfininstr_dbt fin WHERE fin.t_FIID = turn.t_FIID)  AND turn.t_BROKER = -1) OR
              (v_PARTYKIND = 22 AND turn.t_BROKER = v_PARTY AND turn.t_BROKERCONTR = v_PARTYCONTR)
            ) AND
            turn.t_GenAgrID = v_GenAgrID AND
            turn.t_IsTrust = DV_Setting_RunFromTrust AND
            turn.t_STATE  = DVTURN_STATE_CLOSE AND
            (((v_Flag1 = ALG_SP_MONEY_SOURCE_OWN) AND (turn.t_Client <= 0)) OR
             ((v_Flag1 = ALG_SP_MONEY_SOURCE_CLIENT) AND (turn.t_Client >= 1)) OR
             ((v_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) AND (turn.t_IsTrust = chr(88)))) AND
            turn.t_DATE   = v_DATE;
  END; -- RSI_DV_RecoilClosePosTurns

  -- Простановка признака расчета (t_TotalCalc) на итоги
  -- Выполняется в операции расчетов.
  PROCEDURE RSI_DV_SetTotalCalcPosTurns( v_PARTYKIND  IN INTEGER, -- Вид контрагента по расчетам
                                         v_PARTY      IN INTEGER, -- Контрагент
                                         v_PARTYCONTR IN INTEGER, -- Договор с контрагентом
                                         v_DEPARTMENT IN INTEGER, -- Филиал
                                         v_Date       IN DATE,    -- Дата
                                         v_Flag1      IN INTEGER, -- Flag1 Собственные/Клиентские
                                         v_GenAgrID   IN INTEGER  -- ГС
                                       )
  IS
  BEGIN
     -- проставим признак расчета итогов (t_TotalCalc) на операцию расчетов
     UPDATE ddvoper_dbt dvoper SET dvoper.t_TOTALCALC = 'X'
      WHERE dvoper.t_DOCKIND    = RSB_Derivatives.DV_GetDocKind AND
            dvoper.t_DATE       = v_DATE       AND
            dvoper.t_DEPARTMENT = v_DEPARTMENT AND
            dvoper.t_PARTY      = v_PARTY      AND
            dvoper.t_PARTYKIND  = v_PARTYKIND  AND
            dvoper.t_PARTYCONTR = v_PARTYCONTR AND
            dvoper.t_Flag1      = v_Flag1 AND
            dvoper.t_GenAgrID   = v_GenAgrID AND
            EXISTS( select oprkoper.*
                      from doprkoper_dbt oprkoper
                     where oprkoper.t_Kind_Operation = dvoper.t_OperKind
                       and instr(oprkoper.t_SysTypes, 'C') > 0
                  );
  END; -- RSI_DV_SetTotalCalcPosTurns

  -- Снятие признака расчета (t_TotalCalc) с итогов
  -- Выполняется при откате операции расчетов.
  PROCEDURE RSI_DV_UnSetTotalCalcPosTurns( v_PARTYKIND  IN INTEGER, -- Вид контрагента по расчетам
                                           v_PARTY      IN INTEGER, -- Контрагент
                                           v_PARTYCONTR IN INTEGER, -- Договор с контрагентом
                                           v_DEPARTMENT IN INTEGER, -- Филиал
                                           v_Date       IN DATE,    -- Дата
                                           v_Flag1      IN INTEGER, -- Flag1 Собственные/Клиентские
                                           v_GenAgrID   IN INTEGER  -- ГС
                                         )
  IS
  BEGIN
     -- снимем признак расчета итогов (t_TotalCalc) на операции расчетов
     UPDATE ddvoper_dbt dvoper SET dvoper.t_TOTALCALC = CHR(0)
      WHERE dvoper.t_DOCKIND    = RSB_Derivatives.DV_GetDocKind AND
            dvoper.t_DATE       = v_DATE       AND
            dvoper.t_DEPARTMENT = v_DEPARTMENT AND
            dvoper.t_PARTY      = v_PARTY      AND
            dvoper.t_PARTYKIND  = v_PARTYKIND  AND
            dvoper.t_PARTYCONTR = v_PARTYCONTR AND
            dvoper.t_Flag1      = v_Flag1 AND
            dvoper.t_GenAgrID   = v_GenAgrID AND
            EXISTS( select oprkoper.*
                      from doprkoper_dbt oprkoper
                     where oprkoper.t_Kind_Operation = dvoper.t_OperKind
                       and instr(oprkoper.t_SysTypes, 'C') > 0
                  );
  END; -- RSI_DV_UnSetTotalCalcPosTurns

  -- Обнуление итогов дня
  -- Выполняется в операции расчетов.
  PROCEDURE RSI_DV_NullingPositions( v_PARTYKIND    IN INTEGER, -- Вид контрагента по расчетам
                                     v_PARTY        IN INTEGER, -- Контрагент
                                     v_PARTYCONTR   IN INTEGER, -- Договор с контрагентом
                                     v_DEPARTMENT   IN INTEGER, -- Филиал
                                     v_Date         IN DATE,    -- Дата
                                     v_OPERID       IN INTEGER, -- Операция расчетов
                                     v_Flag1        IN INTEGER, -- Flag1 Собственные/Клиентские
                                     v_GenAgrID     IN INTEGER  -- ГС
                                   )
  IS
     CURSOR FiTurn IS SELECT turn.t_IsTrust, turn.t_DATE,turn.t_FIID,turn.t_DEPARTMENT,turn.t_BROKER,turn.t_CLIENTCONTR,turn.t_LONGPOSITION,turn.t_SHORTPOSITION,turn.t_LONGPOSITIONCOST,turn.t_SHORTPOSITIONCOST,turn.t_GenAgrID
                        FROM ddvfiturn_dbt turn
                       WHERE turn.t_DEPARTMENT = v_DEPARTMENT AND
                             ( (v_PARTYKIND = 3 AND v_PARTY = (SELECT fin.t_ISSUER FROM dfininstr_dbt fin WHERE fin.t_FIID = turn.t_FIID) AND turn.t_BROKER = -1) OR
                               (v_PARTYKIND = 22 AND turn.t_BROKER = v_PARTY AND turn.T_BROKERCONTR = v_PARTYCONTR)
                             ) AND
                             turn.t_GenAgrID = v_GenAgrID AND
                             turn.t_IsTrust = DV_Setting_RunFromTrust AND
                             turn.t_DATE   = v_DATE AND
                             turn.t_LONGPOSITION = turn.t_SHORTPOSITION AND
                             (((v_Flag1 = ALG_SP_MONEY_SOURCE_OWN) AND (turn.t_Client <= 0)) OR
                              ((v_Flag1 = ALG_SP_MONEY_SOURCE_CLIENT) AND (turn.t_Client >= 1)) OR
                              ((v_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) AND (turn.t_IsTrust = chr(88)))) AND
                             (
                               DV_Setting_NullingTurn = 3 OR
                               DV_Setting_NullingTurn = (SELECT fin.t_AVOIRKIND FROM dfininstr_dbt fin WHERE turn.t_FIID = fin.t_FIID)
                             );
  BEGIN
     IF( DV_Setting_NullingTurn = 0 ) THEN
        RAISE_APPLICATION_ERROR(-20527,''); -- Нельзя выполнять зануление итогов по позиции при снятой настройке.
     END IF;

     IF( DV_Setting_AccExContracts = 1 ) THEN -- По сделке
        RAISE_APPLICATION_ERROR(-20594,''); -- Установлен учёт биржевых контрактов по сделкам
     END IF;

     FOR fi_turn IN FiTurn LOOP
        INSERT INTO ddvoperps_dbt (
                                   t_OPERID,
                                   t_DOCKIND,
                                   t_DOCID,
                                   t_SUMM,
                                   t_SUMM1,
                                   t_SUMM2,
                                   t_FLAG
                                  )
                                  VALUES
                                  (
                                   v_OPERID,
                                   193,
                                   (SELECT fipos.t_ID
                                      FROM ddvfipos_dbt fipos
                                     WHERE fipos.t_IsTrust     = fi_turn.t_IsTrust
                                       AND fipos.t_FIID        = fi_turn.t_FIID
                                       AND fipos.t_DEPARTMENT  = fi_turn.t_DEPARTMENT
                                       AND fipos.t_BROKER      = fi_turn.t_BROKER
                                       AND fipos.t_CLIENTCONTR = fi_turn.t_CLIENTCONTR
                                       AND fipos.t_GenAgrID    = fi_turn.t_GenAgrID),
                                   fi_turn.t_LONGPOSITION,
                                   fi_turn.t_LONGPOSITIONCOST,
                                   fi_turn.t_SHORTPOSITIONCOST,
                                   4
                                  );

        UPDATE ddvfiturn_dbt turn
           SET turn.t_LONGPOSITION      = 0,
               turn.t_SHORTPOSITION     = 0,
               turn.t_LONGPOSITIONCOST  = 0,
               turn.t_SHORTPOSITIONCOST = 0
         WHERE turn.t_IsTrust     = fi_turn.t_IsTrust
           AND turn.t_FIID        = fi_turn.t_FIID
           AND turn.t_DEPARTMENT  = fi_turn.t_DEPARTMENT
           AND turn.t_BROKER      = fi_turn.t_BROKER
           AND turn.t_CLIENTCONTR = fi_turn.t_CLIENTCONTR
           AND turn.t_GenAgrID    = fi_turn.t_GenAgrID
           AND turn.t_DATE        = fi_turn.t_DATE;
     END LOOP;

  END; -- RSI_DV_NullingPositions

  -- Откат зануления итогов дня.
  -- Выполняется при откате операции расчетов.
  PROCEDURE RSI_DV_RecoilNullingPositions( v_OPERID IN INTEGER -- Операция расчетов
                                         )
  IS
     CURSOR DvOperPS IS SELECT operps.t_OPERID, operps.t_DOCID, operps.t_SUMM, operps.t_SUMM1, operps.t_SUMM2
                          FROM ddvoperps_dbt operps
                         WHERE operps.t_OPERID = v_OPERID
                           AND operps.t_FLAG = 4;

     v_FIID         INTEGER;
     v_DEPARTMENT   INTEGER;
     v_BROKER       INTEGER;
     v_CLIENTCONTR  INTEGER;
     v_DATE         DATE;
     v_IsTrust      CHAR;
     v_GenAgrID     INTEGER;
  BEGIN

     SELECT dvoper.t_DATE INTO v_DATE
       FROM ddvoper_dbt dvoper
      WHERE dvoper.t_ID = v_OPERID;

     FOR operps IN DvOperPS LOOP
        SELECT fipos.t_IsTrust, fipos.t_FIID, fipos.t_DEPARTMENT, fipos.t_BROKER, fipos.t_CLIENTCONTR, fipos.t_GenAgrID
          INTO v_IsTrust, v_FIID, v_DEPARTMENT, v_BROKER, v_CLIENTCONTR, v_GenAgrID
          FROM ddvfipos_dbt fipos
         WHERE fipos.t_ID = operps.t_DOCID;

        UPDATE ddvfiturn_dbt turn
           SET turn.t_LONGPOSITION     = operps.t_SUMM,
               turn.t_SHORTPOSITION    = operps.t_SUMM,
               turn.t_LONGPOSITIONCOST = operps.t_SUMM1,
               turn.t_SHORTPOSITIONCOST= operps.t_SUMM2
         WHERE turn.t_IsTrust     = v_IsTrust
           AND turn.t_FIID        = v_FIID
           AND turn.t_DEPARTMENT  = v_DEPARTMENT
           AND turn.t_BROKER      = v_BROKER
           AND turn.t_CLIENTCONTR = v_CLIENTCONTR
           AND turn.t_GenAgrID    = v_GenAgrID
           AND turn.t_DATE        = v_DATE;
     END LOOP;

     DELETE FROM ddvoperps_dbt operps WHERE operps.t_OPERID = v_OPERID AND operps.t_FLAG = 4;
  END; -- RSI_DV_RecoilNullingPositions

  -- Закрытие позиций
  -- Выполняется в операции расчетов.
  PROCEDURE RSI_DV_ClosePositions(v_DvoperID   IN INTEGER  /* Операция расчётов*/ )
  IS
   v_PartyKind     INTEGER := 0; -- Вид контрагента по расчетам
   v_Party         INTEGER := 0; -- Контрагент
   v_PartyContr    INTEGER := 0; -- Договор с контрагентом
   v_Department    INTEGER := 0; -- Филиал
   v_Date          DATE    := TO_DATE('01.01.0001', 'dd.mm.yyyy'); -- Дата
   v_Flag1         INTEGER := 0;
   v_GenAgrID      INTEGER := 0;  -- ГС
  BEGIN

    SELECT t_PartyKind, t_Party, t_PartyContr, t_Department, t_Date, t_Flag1, t_GenAgrID
      INTO v_PartyKind, v_Party, v_PartyContr, v_Department, v_Date, v_Flag1, v_GenAgrID
       FROM ddvoper_dbt
        WHERE t_ID = v_DvoperID AND t_DocKind = 194;

     UPDATE ddvfipos_dbt pos SET pos.t_STATE = DVPOS_STATE_CLOSE, pos.t_CLOSEDATE = v_Date
      WHERE pos.t_DEPARTMENT = v_DEPARTMENT AND
            ( (v_PARTYKIND = 3  AND v_PARTY = (SELECT fin.t_ISSUER FROM dfininstr_dbt fin WHERE fin.t_FIID = pos.t_FIID)  AND pos.t_BROKER = -1) OR
              (v_PARTYKIND = 22 AND pos.t_BROKER = v_PARTY AND pos.t_BROKERCONTR = v_PARTYCONTR)
            ) AND
            pos.t_IsTrust = DV_Setting_RunFromTrust AND
            pos.t_STATE  = DVPOS_STATE_OPEN AND
            pos.t_GenAgrID = v_GenAgrID AND
            (((v_Flag1 = ALG_SP_MONEY_SOURCE_OWN) AND (pos.t_Client <= 0)) OR
             ((v_Flag1 = ALG_SP_MONEY_SOURCE_CLIENT) AND (pos.t_Client >= 1)) OR
             ((v_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) AND (pos.t_IsTrust = chr(88)))) AND
            v_DATE  >= (SELECT fin.t_DRAWINGDATE FROM dfininstr_dbt fin WHERE fin.t_FIID = pos.t_FIID);
  END; -- RSI_DV_ClosePositions

  -- Откат закрытия итогов дня и позиций.
  -- Процедура откатывает закрытие позиций и итогов дня дня.
  -- Выполняется при откате операции расчетов.
  PROCEDURE RSI_DV_RecoilClosePositions(v_DvoperID   IN INTEGER  /* Операция расчётов*/)
  IS
   v_PartyKind     INTEGER := 0; -- Вид контрагента по расчетам
   v_Party         INTEGER := 0; -- Контрагент
   v_PartyContr    INTEGER := 0; -- Договор с контрагентом
   v_Department    INTEGER := 0; -- Филиал
   v_Date          DATE    := TO_DATE('01.01.0001', 'dd.mm.yyyy');   -- Дата
   v_Flag1         INTEGER := 0;
   v_GenAgrID      INTEGER := 0;  -- ГС
  BEGIN

    SELECT t_PartyKind, t_Party, t_PartyContr, t_Department, t_Date, t_Flag1, t_GenAgrID
      INTO v_PartyKind, v_Party, v_PartyContr, v_Department, v_Date, v_Flag1, v_GenAgrID
       FROM ddvoper_dbt
        WHERE t_ID = v_DvoperID AND t_DocKind = 194;

     UPDATE ddvfipos_dbt pos SET pos.t_STATE = DVPOS_STATE_OPEN
      WHERE pos.t_DEPARTMENT = v_DEPARTMENT AND
            ( (v_PARTYKIND = 3  AND v_PARTY = (SELECT fin.t_ISSUER FROM dfininstr_dbt fin WHERE fin.t_FIID = pos.t_FIID)  AND pos.t_BROKER = -1) OR
              (v_PARTYKIND = 22 AND pos.t_BROKER = v_PARTY AND pos.t_BROKERCONTR = v_PARTYCONTR)
            ) AND
            pos.t_IsTrust = DV_Setting_RunFromTrust AND
            pos.t_STATE  = DVPOS_STATE_CLOSE AND
            pos.t_CLOSEDATE = v_Date AND
            pos.t_GenAgrID = v_GenAgrID AND
            (((v_Flag1 = ALG_SP_MONEY_SOURCE_OWN) AND (pos.t_Client <= 0)) OR
             ((v_Flag1 = ALG_SP_MONEY_SOURCE_CLIENT) AND (pos.t_Client >= 1)) OR
             ((v_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) AND (pos.t_IsTrust = chr(88)))) AND
            v_DATE >= (SELECT fin.t_DRAWINGDATE FROM dfininstr_dbt fin WHERE fin.t_FIID = pos.t_FIID);
  END; -- RSI_DV_RecoilClosePositions

  -- Вставка данных по позиции. Используется в процедуре импорта и при вставке итогов и редактирования.
  PROCEDURE RSI_DV_InsertPositionTurn
           (
              v_FIID            IN INTEGER, -- Финансовый инструмент
              v_DEPARTMENT      IN INTEGER, -- Филиал
              v_BROKER          IN INTEGER, -- Брокер
              v_ClientContr     IN INTEGER, -- Клиент договор
              v_DATE            IN DATE,    -- Дата
              v_MARGIN          IN NUMBER,  -- Вариационная маржа
              v_GUARANTY        IN NUMBER,  -- Гарантийное обеспечение
              v_FAIRVALUECALC   IN CHAR,    -- Признак расчета справедливой стоимости
              v_FAIRVALUE       IN NUMBER,  -- Справедливая стоимость
              v_INSERTMARGIN    IN INTEGER, -- Вставка вариационной маржи
              v_INSERTGUARANTY  IN INTEGER, -- Вставка гарантийного обеспечения
              v_INSERTFAIRVALUE IN INTEGER, -- Вставка справедливой стоимости
              v_ACTION          IN INTEGER, -- Действие
              v_GenAgrID        IN INTEGER, -- ГС
              v_MARGINDAY       IN NUMBER,  -- Вариационная маржа на начало дня
              v_MARGINDEALS     IN NUMBER   -- Вариационная маржа по новым сделкам
           )
  IS
     v_exist_fiturn INTEGER;
     v_exist_fipos  INTEGER;
     T              ddvfiturn_dbt%ROWTYPE;
     v_GuarantyonDeal CHAR;
     Vv_INSERTMARGIN    INTEGER;
     Vv_INSERTGUARANTY  INTEGER;
     Vv_INSERTFAIRVALUE INTEGER;
  BEGIN
     -- обработаем dvfiturn
     BEGIN
       v_exist_fiturn := 1;

       SELECT * INTO T
         FROM ddvfiturn_dbt turn
        WHERE turn.t_FIID        = v_FIID
          AND turn.t_DEPARTMENT  = v_DEPARTMENT
          AND turn.t_BROKER      = v_BROKER
          AND turn.t_ClientContr = v_ClientContr
          AND turn.t_DATE        = v_DATE
          AND turn.t_GenAgrID    = v_GenAgrID;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN
           v_exist_fiturn := 0;
     END;

     IF( v_exist_fiturn = 1 and T.t_State = DVTURN_STATE_CLOSE ) THEN
        --День по позиции закрыт
        RAISE_APPLICATION_ERROR(-20509,'');

     ELSIF (v_ACTION IN (DV_ACTION_EDIT, DV_ACTION_IMPORT)) THEN

        IF( DV_Setting_AccExContracts = 1 ) THEN -- По сделке
           BEGIN
             v_exist_fipos := 1;
             SELECT pos.t_GUARANTYONDEAL INTO v_GuarantyonDeal
               FROM ddvfipos_dbt pos
              WHERE pos.t_FIID        = v_FIID
                AND pos.t_DEPARTMENT  = v_DEPARTMENT
                AND pos.t_BROKER      = v_BROKER
                AND pos.t_ClientContr = v_ClientContr
                AND pos.t_GenAgrID    = v_GenAgrID;
             EXCEPTION
               WHEN NO_DATA_FOUND THEN
                 v_exist_fipos := 0;
           END;

           IF( v_exist_fipos = 0 ) THEN
              RAISE_APPLICATION_ERROR(-20505,''); --Не открыта позиция по производному инструменту
           END IF;

           Vv_INSERTMARGIN := v_INSERTMARGIN;
           IF( v_GuarantyonDeal = chr(0) ) THEN
              Vv_INSERTGUARANTY := v_INSERTGUARANTY;
           ELSE
              Vv_INSERTGUARANTY := 0;
           END IF;
           Vv_INSERTFAIRVALUE := 0;
        ELSE
           Vv_INSERTMARGIN    := v_INSERTMARGIN;
           Vv_INSERTGUARANTY  := v_INSERTGUARANTY;
           Vv_INSERTFAIRVALUE := v_INSERTFAIRVALUE;
        END IF;

        IF( (Vv_INSERTMARGIN = 0) and (Vv_INSERTGUARANTY = 0) and (Vv_INSERTFAIRVALUE = 0) ) THEN
           RAISE_APPLICATION_ERROR(-20578,''); --Неверные параметры при вызове процедуры
        END IF;

        IF( v_exist_fiturn = 0 OR
            (T.T_SETMARGIN = CHR(0) AND
             T.T_SETGUARANTY = CHR(0) AND
             T.T_SETFAIRVALUE = CHR(0) AND
             RSI_DV_GetCountNotCalcPosCom(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_GenAgrID) = 0 )
          ) THEN
           RSI_DV_AttachPositionTurn( v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_GenAgrID );
        END IF;

        UPDATE ddvfiturn_dbt N SET N.T_MARGIN           = (CASE WHEN Vv_INSERTMARGIN    = 1 THEN v_MARGIN    ELSE N.T_MARGIN END),
                                   N.T_MARGINDAY        = (CASE WHEN Vv_INSERTMARGIN    = 1 THEN v_MARGINDAY    ELSE N.T_MARGINDAY END),
                                   N.T_MARGINDEALS      = (CASE WHEN Vv_INSERTMARGIN    = 1 THEN v_MARGINDEALS  ELSE N.T_MARGINDEALS END),
                                   N.T_SETMARGIN        = (CASE WHEN Vv_INSERTMARGIN    = 1 THEN CHR(88)     ELSE N.T_SETMARGIN END),
                                   N.T_GUARANTY         = (CASE WHEN Vv_INSERTGUARANTY  = 1 THEN v_GUARANTY  ELSE N.T_GUARANTY END),
                                   N.T_SETGUARANTY      = (CASE WHEN Vv_INSERTGUARANTY  = 1 THEN CHR(88)     ELSE N.T_SETGUARANTY END),
                                   N.T_FAIRVALUECALC    = (CASE WHEN Vv_INSERTFAIRVALUE = 1 THEN v_FAIRVALUECALC ELSE N.T_FAIRVALUECALC END),
                                   N.T_FAIRVALUE        = (CASE WHEN Vv_INSERTFAIRVALUE = 1 THEN (CASE WHEN v_FAIRVALUECALC = 'X' THEN v_FAIRVALUE ELSE 0 END) ELSE N.T_FAIRVALUE END),
                                   N.T_SETFAIRVALUE     = (CASE WHEN Vv_INSERTFAIRVALUE = 1 THEN CHR(88)     ELSE N.T_SETFAIRVALUE END),
                                   N.T_OLDMARGIN        = (CASE WHEN Vv_INSERTMARGIN    = 1 THEN v_MARGIN    ELSE N.T_OLDMARGIN END),
                                   N.T_OLDSETMARGIN     = (CASE WHEN Vv_INSERTMARGIN    = 1 THEN CHR(88)     ELSE N.T_OLDSETMARGIN END),
                                   N.T_OLDFAIRVALUECALC = (CASE WHEN Vv_INSERTFAIRVALUE = 1 THEN v_FAIRVALUECALC ELSE N.T_OLDFAIRVALUECALC END),
                                   N.T_OLDFAIRVALUE     = (CASE WHEN Vv_INSERTFAIRVALUE = 1 THEN (CASE WHEN v_FAIRVALUECALC = 'X' THEN v_FAIRVALUE ELSE 0 END) ELSE N.T_OLDFAIRVALUE END),
                                   N.T_OLDSETFAIRVALUE  = (CASE WHEN Vv_INSERTFAIRVALUE = 1 THEN CHR(88)     ELSE N.T_OLDSETFAIRVALUE END)

         WHERE N.t_FIID        = v_FIID
           AND N.t_DEPARTMENT  = v_DEPARTMENT
           AND N.t_BROKER      = v_BROKER
           AND N.t_ClientContr = v_ClientContr
           AND N.t_DATE        = v_DATE
           AND N.t_GenAgrID    = v_GenAgrID;

     ELSIF (v_ACTION = DV_ACTION_CALCITOG) THEN

        IF( DV_Setting_AccExContracts = 1 ) THEN -- По сделке
           RAISE_APPLICATION_ERROR(-20579,''); --Неверный режим учета
        END IF;

        -- Поскольку сохраняются данные для отката, счетчик увеличиваем всегда
        RSI_DV_AttachPositionTurn( v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_GenAgrID );

        UPDATE ddvfiturn_dbt N SET N.T_OLDMARGIN        = (CASE WHEN v_INSERTMARGIN    = 1 THEN N.T_MARGIN  ELSE N.T_OLDMARGIN END),
                                   N.T_OLDSETMARGIN     = (CASE WHEN v_INSERTMARGIN    = 1 THEN N.T_SETMARGIN ELSE N.T_OLDSETMARGIN END),
                                   N.T_OLDFAIRVALUECALC = (CASE WHEN v_INSERTFAIRVALUE = 1 THEN N.T_FAIRVALUECALC ELSE N.T_OLDFAIRVALUECALC END),
                                   N.T_OLDFAIRVALUE     = (CASE WHEN v_INSERTFAIRVALUE = 1 THEN N.T_FAIRVALUE ELSE N.T_OLDFAIRVALUE END),
                                   N.T_OLDSETFAIRVALUE  = (CASE WHEN v_INSERTFAIRVALUE = 1 THEN N.T_SETFAIRVALUE ELSE N.T_OLDSETFAIRVALUE END),
                                   N.T_MARGIN           = (CASE WHEN v_INSERTMARGIN    = 1 THEN v_MARGIN    ELSE N.T_MARGIN END),
                                   N.T_SETMARGIN        = (CASE WHEN v_INSERTMARGIN    = 1 THEN CHR(0)      ELSE N.T_SETMARGIN END),
                                   N.T_FAIRVALUECALC    = (CASE WHEN v_INSERTFAIRVALUE = 1 THEN v_FAIRVALUECALC ELSE N.T_FAIRVALUECALC END),
                                   N.T_FAIRVALUE        = (CASE WHEN v_INSERTFAIRVALUE = 1 THEN (CASE WHEN v_FAIRVALUECALC = 'X' THEN v_FAIRVALUE ELSE 0 END) ELSE N.T_FAIRVALUE END),
                                   N.T_SETFAIRVALUE     = (CASE WHEN v_INSERTFAIRVALUE = 1 THEN CHR(0)      ELSE N.T_SETFAIRVALUE END)
         WHERE N.t_FIID        = v_FIID
           AND N.t_DEPARTMENT  = v_DEPARTMENT
           AND N.t_BROKER      = v_BROKER
           AND N.t_ClientContr = v_ClientContr
           AND N.t_DATE        = v_DATE
           AND N.t_GenAgrID    = v_GenAgrID;
     ELSE
        --Действие не поддерживается
        RAISE_APPLICATION_ERROR(-20557,'');
     END IF;
  END; -- RSI_DV_InsertPositionTurn

  -- Вставка рассчитанных данных по позиции. Используется в операции расчетов по позиции.
  PROCEDURE RSI_DV_CalcPositionTurn
           (
              v_FIID            IN INTEGER, -- Финансовый инструмент
              v_DEPARTMENT      IN INTEGER, -- Филиал
              v_BROKER          IN INTEGER, -- Брокер
              v_ClientContr     IN INTEGER, -- Клиент договор
              v_DATE            IN DATE,    -- Дата
              v_MARGIN          IN NUMBER,  -- Вариационная маржа
              v_FAIRVALUE       IN NUMBER,  -- Справедливая стоимость
              v_CALCSUM         IN INTEGER, -- Расчет сумм 1=Да, 0=Нет
              v_CALCFAIRVALUE   IN INTEGER, -- Расчет справедливой стоимости
              v_GenAgrID        IN INTEGER  -- ГС
           )
  IS
     v_FAIRVALUECALC  CHAR;
  BEGIN

     IF v_CALCFAIRVALUE = 1 THEN
        v_FAIRVALUECALC := 'X';
     ELSE
        v_FAIRVALUECALC := CHR(0);
     END IF;

     RSI_DV_InsertPositionTurn(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE,
                               v_MARGIN, 0, v_FAIRVALUECALC, v_FAIRVALUE, v_CALCSUM, 0, v_CALCFAIRVALUE, DV_ACTION_CALCITOG, v_GenAgrID, 0, 0);
  END; --RSI_DV_CalcPositionTurn

  -- Массовый откат данных по сделке
  PROCEDURE RSI_DV_MassRecoilInsertPosTurn( v_PosID   IN INTEGER, -- Сделка
                                            v_DvoperID IN INTEGER, -- Операция расчётов
                                            v_Action   IN INTEGER  -- Действие
                                            )
  IS
     v_Date          DATE    := TO_DATE('01.01.0001', 'dd.mm.yyyy'); -- Дата
     v_FIID          INTEGER := 0; -- Финансовый инструмент
     v_DEPARTMENT    INTEGER := 0; -- Филиал
     v_BROKER        INTEGER := 0; -- Брокер
     v_ClientContr   INTEGER := 0; -- Клиент договор
     v_GenAgrID      INTEGER := 0;  -- ГС
  BEGIN
    SELECT t_Date
      INTO  v_Date
       FROM ddvoper_dbt
        WHERE t_ID = v_DvoperID AND t_DocKind = 194;
    BEGIN
     SELECT t_FIID, t_DEPARTMENT, t_BROKER, t_ClientContr, t_GenAgrID
      INTO  v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_GenAgrID
       FROM ddvfipos_dbt
        WHERE t_ID = v_PosID;
    END;
    RSI_DV_RecoilInsertPosTurn(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_ACTION, v_GenAgrID);
  END;

  -- Откат вставки данных по позиции
  -- Поддерживаются ACTION - "Расчеты" и для совместимости с предыдущими версиями "Расчет клиентской комиссии".
  -- Прочие виды действий зарезервированы для будущих применений.
  PROCEDURE RSI_DV_RecoilInsertPosTurn
           (
              v_FIID            IN INTEGER, -- Финансовый инструмент
              v_DEPARTMENT      IN INTEGER, -- Филиал
              v_BROKER          IN INTEGER, -- Брокер
              v_ClientContr     IN INTEGER, -- Клиент договор
              v_DATE            IN DATE,    -- Дата
              v_ACTION          IN INTEGER, -- Действие
              v_GenAgrID        IN INTEGER  -- ГС
           )
  IS
     CURSOR Cficom IS SELECT *
                        FROM ddvfi_com_dbt
                       WHERE t_FIID        = v_FIID
                         AND t_DEPARTMENT  = v_DEPARTMENT
                         AND t_BROKER      = v_BROKER
                         AND t_ClientContr = v_ClientContr
                         AND t_GenAgrID    = v_GenAgrID
                         AND t_DATE        = v_DATE
                         AND t_NOTCALC     = CHR(88);

     v_exist_fipos   INTEGER;
     v_state_fipos   INTEGER;
     v_exist_fiturn  INTEGER;
     v_exist_fiturnM INTEGER;
     v_SetFlag       INTEGER;
     T  ddvfiturn_dbt%ROWTYPE;
     M  ddvfiturn_dbt%ROWTYPE;
     v_GuarantyonDeal CHAR;
  BEGIN

     -- обработаем dvfipos
     BEGIN
       v_exist_fipos := 1;

       -- 1. Найти запись DDVFIPOS с T_FIID, T_DEPARTMENT, T_BROKER, T_CLIENTCONTR, t_GenAgrID равными заданным
       SELECT pos.t_STATE, pos.t_GUARANTYONDEAL INTO v_state_fipos, v_GuarantyonDeal
         FROM ddvfipos_dbt pos
        WHERE pos.t_FIID        = v_FIID
          AND pos.t_DEPARTMENT  = v_DEPARTMENT
          AND pos.t_BROKER      = v_BROKER
          AND pos.t_ClientContr = v_ClientContr
          AND pos.t_GenAgrID    = v_GenAgrID;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN
           v_exist_fipos := 0;
     END;

     IF( v_exist_fipos = 0) THEN
        RAISE_APPLICATION_ERROR(-20505,''); -- Не открыта позиция по производному инструменту
     ELSIF (v_state_fipos = DVPOS_STATE_CLOSE) THEN  -- Если ее T_STATE == "Закрыт"
        RAISE_APPLICATION_ERROR(-20506,''); -- Позиция по производному инструменту закрыта
     END IF;

     -- обработаем dvfiturn
     BEGIN
       v_exist_fiturn := 1;

       SELECT * INTO T
         FROM ddvfiturn_dbt turn
        WHERE turn.t_FIID        = v_FIID
          AND turn.t_DEPARTMENT  = v_DEPARTMENT
          AND turn.t_BROKER      = v_BROKER
          AND turn.t_ClientContr = v_ClientContr
          AND turn.t_GenAgrID    = v_GenAgrID
          AND turn.t_DATE        = v_DATE;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN
           v_exist_fiturn := 0;
     END;

     IF( v_exist_fiturn = 0 ) THEN
        -- 5.1.1. Завершить процедуру
        RETURN;
     ELSE
        -- 6.1.  Если в N T_STATE == "Закрыт"
        IF( T.t_state = DVTURN_STATE_CLOSE ) THEN
           --День по позиции закрыт
           RAISE_APPLICATION_ERROR(-20509,'');
        -- 6.2.  Если ACTION -  одно из ("Расчет итоговых сумм", "Расчеты", "Расчет клиентской комиссии")
        -- всегда учет по позициям
        ELSIF( v_ACTION IN (DV_ACTION_CALCITOG, DV_ACTION_CALC, DV_ACTION_CALCCOM) ) THEN

           --Откатываем расчеты
           RSI_DV_DetachPositionTurn(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_GenAgrID);

           IF( v_ACTION IN (DV_ACTION_CALC, DV_ACTION_CALCCOM) ) THEN
              FOR ficom IN Cficom LOOP
                 RSI_DV_UpdatePosCom( ficom.t_FIID, ficom.t_DEPARTMENT, ficom.t_BROKER, ficom.t_ClientContr, ficom.t_DATE, ficom.t_ComissID, ficom.t_GenAgrID );
              END LOOP;
           END IF;

           UPDATE ddvfiturn_dbt K SET K.T_MARGIN           = (CASE WHEN K.t_SetMargin = CHR(88) THEN K.t_Margin ELSE K.t_OldMargin END),
                                      K.T_SETMARGIN        = (CASE WHEN K.t_SetMargin = CHR(88) THEN K.t_SetMargin ELSE K.t_OldSetMargin END),
                                      K.T_FAIRVALUECALC    = (CASE WHEN K.t_SetFairValue = CHR(88) THEN K.t_FairValueCalc ELSE K.t_OldFairValueCalc END),
                                      K.T_FAIRVALUE        = (CASE WHEN K.t_SetFairValue = CHR(88) THEN K.t_FairValue ELSE K.t_OldFairValue END),
                                      K.T_SETFAIRVALUE     = (CASE WHEN K.t_SetFairValue = CHR(88) THEN K.t_SetFairValue ELSE K.t_OldSetFairValue END),
                                      K.T_OLDMARGIN        = 0,
                                      K.T_OLDSETMARGIN     = chr(0),
                                      K.T_OLDFAIRVALUECALC = chr(0),
                                      K.T_OLDFAIRVALUE     = 0,
                                      K.T_OLDSETFAIRVALUE  = chr(0)
            WHERE K.t_FIID        = v_FIID
              AND K.t_DEPARTMENT  = v_DEPARTMENT
              AND K.t_BROKER      = v_BROKER
              AND K.t_ClientContr = v_ClientContr
              AND K.t_GenAgrID    = v_GenAgrID
              AND K.t_DATE        = v_DATE;

        ELSIF( v_ACTION = DV_ACTION_EDIT ) THEN --предполагается полная очистка
           IF( T.T_SETMARGIN = CHR(88) OR
               T.T_SETGUARANTY = CHR(88) OR
               T.T_SETFAIRVALUE = CHR(88) OR
               RSI_DV_GetCountNotCalcPosCom(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_GenAgrID) > 0
             ) THEN
              RSI_DV_DetachPositionTurn(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_GenAgrID);

              FOR ficom IN Cficom LOOP
                 RSI_DV_UpdatePosCom(ficom.t_FIID, ficom.t_DEPARTMENT, ficom.t_BROKER, ficom.t_ClientContr, ficom.t_DATE, ficom.t_ComissID, ficom.t_GenAgrID);
              END LOOP;
           END IF;

           BEGIN
             v_exist_fiturnM := 1;

             SELECT * INTO M
               FROM ddvfiturn_dbt turn
              WHERE turn.t_FIID        = v_FIID
                AND turn.t_DEPARTMENT  = v_DEPARTMENT
                AND turn.t_BROKER      = v_BROKER
                AND turn.t_ClientContr = v_ClientContr
                AND turn.t_GenAgrID    = v_GenAgrID
                AND turn.t_DATE        = v_DATE;
             EXCEPTION
               WHEN NO_DATA_FOUND THEN
                 v_exist_fiturnM := 0;
           END;

           IF( v_exist_fiturnM = 1 ) THEN
              IF( DV_Setting_AccExContracts = 0 ) THEN -- По позиции
                 UPDATE ddvfiturn_dbt M SET M.T_MARGIN           = 0,
                                            M.T_SETMARGIN        = chr(0),
                                            M.T_GUARANTY         = 0,
                                            M.T_SETGUARANTY      = chr(0),
                                            M.T_FAIRVALUECALC    = chr(0),
                                            M.T_FAIRVALUE        = 0,
                                            M.T_SETFAIRVALUE     = chr(0),
                                            M.T_OLDMARGIN        = 0,
                                            M.T_OLDSETMARGIN     = chr(0),
                                            M.T_OLDFAIRVALUECALC = chr(0),
                                            M.T_OLDFAIRVALUE     = 0,
                                            M.T_OLDSETFAIRVALUE  = chr(0)
                  WHERE M.t_FIID        = v_FIID
                    AND M.t_DEPARTMENT  = v_DEPARTMENT
                    AND M.t_BROKER      = v_BROKER
                    AND M.t_ClientContr = v_ClientContr
                    AND M.t_GenAgrID    = v_GenAgrID
                    AND M.t_DATE        = v_Date;
              ELSE
                 IF( v_GuarantyonDeal = chr(0) ) THEN
                    UPDATE ddvfiturn_dbt M SET M.T_GUARANTY    = 0,
                                               M.T_SETGUARANTY = chr(0)
                     WHERE M.t_FIID        = v_FIID
                       AND M.t_DEPARTMENT  = v_DEPARTMENT
                       AND M.t_BROKER      = v_BROKER
                       AND M.t_ClientContr = v_ClientContr
                       AND M.t_DATE        = v_DATE;
                 END IF;
              END IF;
           END IF;

        ELSE
           --Действие не поддерживается
           RAISE_APPLICATION_ERROR(-20557,'');
        END IF;
     END IF;
  END; -- RSI_DV_RecoilInsertPosTurn

  --Выполняет вставку объекта, вызывается в макросах шагов, без проверки наличия существующего
  PROCEDURE RSI_DV_InsertFiCom(
                                pComissID    IN NUMBER,
                                pFIID        IN NUMBER,
                                pDepartment  IN NUMBER,
                                pBroker      IN NUMBER,
                                pClientContr IN NUMBER,
                                pDate        IN DATE,
                                pSum         IN NUMBER,
                                pNDS         IN NUMBER,
                                pGenAgrID    IN INTEGER  -- ГС
                              )
  IS
  BEGIN
     INSERT INTO DDVFI_COM_DBT (
                                T_FIID,
                                T_Department,
                                T_Broker,
                                T_ClientContr,
                                T_Date,
                                T_Sum,
                                T_NDS,
                                T_ID,
                                T_ComissID,
                                t_Sector,
                                t_GenAgrID
                               )
                        VALUES (
                                pFIID,
                                pDepartment,
                                pBroker,
                                pClientContr,
                                pDate,
                                pSum,
                                pNDS,
                                0,
                                pComissID,
                                chr(0)/*pSector*/,
                                pGenAgrID
                               );
  END; -- RSI_DV_InsertFiCom

  FUNCTION RSI_DV_GetValueLL( List IN NUMBER, Element IN NUMBER ) return VARCHAR2
  IS
     Code VARCHAR2(16);
  BEGIN
     BEGIN
        SELECT t_Code
          INTO Code
          FROM dllvalues_dbt
         WHERE t_List = List
           AND t_Element = Element;
     EXCEPTION WHEN NO_DATA_FOUND THEN Code := '';
     END;

     RETURN Code;
  END;

  --Выполняет вставку комиссии по операции
  PROCEDURE RSI_DV_InsertDlCom( pDealID   IN NUMBER,
                                pComissID IN NUMBER,
                                pSum      IN NUMBER,
                                pNDS      IN NUMBER
                              )
  IS
  BEGIN
     RSI_DV_InitError();
     INSERT INTO DDVDLCOM_DBT ( t_DealID, t_Sum, t_NDS, t_ID, t_ComissID )
                       VALUES ( pDealID,  pSum,  pNDS,  0,  pComissID  );
  EXCEPTION
     WHEN DUP_VAL_ON_INDEX THEN RSI_DV_SetError(-20597, TO_CHAR(pComissID));
  END; -- RSI_DV_InsertDlCom

  -- Массовая вставка комиссий по биржевым операциям
  PROCEDURE DV_MassInsertDlCom IS
     CURSOR InsCom IS ( SELECT DISTINCT deal.t_ID, comis.t_ComissID, sfdef.t_Sum, sfdef.t_SumNDS
                          FROM doprtemp_view opr, ddvdeal_dbt deal, dsfdef_dbt sfdef, dsfcomiss_dbt comis
                         WHERE deal.t_ID = TO_NUMBER(opr.t_DocumentID)
                           AND opr.t_DocKind = DL_DVDEAL
                           AND (((deal.t_Client > 0) AND (comis.t_ReceiverID = RsbSessionData.OurBank)) OR
                                (((RSB_Derivatives.RSI_DV_PartyCalcTotalAmount(RSB_Derivatives.RSI_DVGetContractorID(deal.t_ID)) <> 1)) AND (comis.t_ReceiverID <> RsbSessionData.OurBank))
                               )
                           AND sfdef.t_ID_Operation = opr.t_ID_Operation
                           AND sfdef.t_ID_Step      = opr.t_ID_Step
                           AND sfdef.t_Sum         <> 0
                           AND comis.t_FeeType      = sfdef.t_FeeType
                           AND comis.t_Number       = sfdef.t_CommNumber
                      );
  BEGIN
     FOR InsCom_rec IN InsCom LOOP
        RSI_DV_InsertDlCom(InsCom_rec.t_ID, InsCom_rec.t_ComissID, InsCom_rec.t_Sum + InsCom_rec.t_SumNDS, InsCom_rec.t_SumNDS);
     END LOOP;
  END;

  --Выполняет удаление комиссии по операции
  PROCEDURE RSI_DV_DeleteDlCom(
                                pDealID      IN NUMBER,
                                pComissID    IN NUMBER
                              )
  IS
  BEGIN
     DELETE FROM DDVDLCOM_DBT
      WHERE T_DEALID   = pDealID
        AND T_ComissID = pComissID;

  END; -- RSI_DV_DeleteDlCom

  -- Импорт данных по позиции.
  PROCEDURE RSI_DV_ImportPositionTurn
           (
              v_FIID            IN INTEGER, -- Финансовый инструмент
              v_DEPARTMENT      IN INTEGER, -- Филиал
              v_BROKER          IN INTEGER, -- Брокер
              v_Client          IN INTEGER, -- Клиент
              v_ClientContr     IN INTEGER, -- Клиент договор
              v_DATE            IN DATE,    -- Дата
              v_MARGIN          IN NUMBER,  -- Вариационная маржа
              v_GUARANTY        IN NUMBER,  -- Гарантийное обеспечение
              v_ISTRUST         IN CHAR,    -- Признак ДУ
              v_IMPORTMARGIN    IN INTEGER, -- Импорт вариационной маржи 1=Да, 0=Нет
              v_IMPORTGUARANTY  IN INTEGER, -- Импорт гарантийного обеспечения 1=Да, 0=Нет
              v_GenAgrID        IN INTEGER, -- ГС
              v_MARGINDAY       IN NUMBER,  -- Вариационная маржа на начало дня
              v_MARGINDEALS     IN NUMBER   -- Вариационная маржа по новым сделкам
           )
  IS
     v_exist_fipos       INTEGER;
     v_state_fipos       INTEGER;
     v_ClientContr_fipos INTEGER;
  BEGIN

     v_exist_fipos := 1;
     v_ClientContr_fipos := 0;

     -- проверим dvfipos
     BEGIN

     SELECT t_STATE INTO v_state_fipos
       FROM ddvfipos_dbt pos
      WHERE pos.t_FIID        = v_FIID AND
            pos.t_DEPARTMENT  = v_DEPARTMENT AND
            pos.t_BROKER      = v_BROKER AND
            pos.t_ClientContr = v_ClientContr AND
            pos.t_GenAgrID    = v_GenAgrID;

     v_ClientContr_fipos := v_ClientContr;

     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         v_exist_fipos := 0;
     END;

     IF( v_exist_fipos = 0 ) THEN
        RAISE_APPLICATION_ERROR(-20505,''); --Не открыта позиция по производному инструменту
     ELSE
        IF( v_state_fipos = DVPOS_STATE_CLOSE ) THEN
           RAISE_APPLICATION_ERROR(-20506,''); --Позиция по производному инструменту закрыта
        END IF;
     END IF;

     RSI_DV_InsertPositionTurn(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr_fipos, v_DATE, v_MARGIN,
                               v_GUARANTY, chr(0), 0, v_IMPORTMARGIN, v_IMPORTGUARANTY, 0, DV_ACTION_IMPORT, v_GenAgrID, v_MARGINDAY, v_MARGINDEALS);
  END; -- RSI_DV_ImportPositionTurn

  -- Вставка данных по позиции. Используется при вводе новых итогов из скроллинга.
  PROCEDURE RSI_DV_InputPositionTurn
          (
             v_FIID            IN INTEGER, -- Финансовый инструмент
             v_DEPARTMENT      IN INTEGER, -- Филиал
             v_BROKER          IN INTEGER, -- Брокер
             v_ClientContr     IN INTEGER, -- Клиент договор
             v_DATE            IN DATE,    -- Дата
             v_MARGIN          IN NUMBER,  -- Вариационная маржа
             v_GUARANTY        IN NUMBER,  -- Гарантийное обеспечение
             v_FAIRVALUECALC   IN CHAR,    -- Признак расчета справедливой стоимости
             v_FAIRVALUE       IN NUMBER,  -- Справедливая стоимость
             v_GenAgrID        IN INTEGER  -- ГС
          )
  IS
  BEGIN
     RSI_DV_InsertPositionTurn(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_MARGIN,
                               v_GUARANTY, v_FAIRVALUECALC, v_FAIRVALUE,
                               (CASE WHEN v_MARGIN <> 0   THEN 1 ELSE 0 END),
                               (CASE WHEN v_GUARANTY <> 0 THEN 1 ELSE 0 END),
                               (CASE WHEN v_FAIRVALUECALC = CHR(88) THEN 1 ELSE 0 END),
                               DV_ACTION_EDIT, v_GenAgrID, 0, 0);
  END; --RSI_DV_InputPositionTurn

  --Редактирование данных по позиции. Используется при вводе итогов из скроллинга или редактировании итогов.
  PROCEDURE RSI_DV_EditPositionTurn
           (
              v_FIID             IN INTEGER, -- Финансовый инструмент
              v_DEPARTMENT       IN INTEGER, -- Филиал
              v_BROKER           IN INTEGER, -- Брокер
              v_ClientContr      IN INTEGER, -- Клиент договор
              v_DATE             IN DATE,    -- Дата
              v_MARGIN           IN NUMBER,  -- Вариационная маржа
              v_GUARANTY         IN NUMBER,  -- Гарантийное обеспечение
              v_FAIRVALUECALC    IN CHAR,    -- Признак расчета справедливой стоимости
              v_FAIRVALUE        IN NUMBER,  -- Справедливая стоимость
              v_EDITMARGIN       IN INTEGER, -- Редактирование вариационной маржи 1=Да, 0=Нет
              v_EDITGUARANTY     IN INTEGER, -- Редактирование гарантийного обеспечения 1=Да, 0=Нет
              v_EDITFAIRVALUE    IN INTEGER, -- Редактирование справедливой стоимости 1=Да, 0=Нет
              v_COUNTNOTCALCCOMM IN INTEGER, -- Число не рассчитываемых комиссий
              v_GenAgrID         IN INTEGER  -- ГС
           )
  IS
     v_exist_fipos         INTEGER;
     v_state_fipos         INTEGER;
     T                     ddvfiturn_dbt%ROWTYPE;
     v_exist_fiturn        INTEGER;
     v_GuarantyonDeal      INTEGER;
     v_GuarantyonDeal_fipos CHAR;
     Vv_EDITMARGIN       INTEGER;
     Vv_EDITGUARANTY     INTEGER;
     Vv_EDITFAIRVALUE    INTEGER;
  BEGIN

     -- проверим dvfipos
     BEGIN
       v_exist_fipos := 1;

       SELECT t_STATE, t_GUARANTYONDEAL INTO v_state_fipos, v_GuarantyonDeal_fipos
         FROM ddvfipos_dbt pos
        WHERE pos.t_FIID        = v_FIID
          AND pos.t_DEPARTMENT  = v_DEPARTMENT
          AND pos.t_BROKER      = v_BROKER
          AND pos.t_ClientContr = v_ClientContr
          AND pos.t_GenAgrID    = v_GenAgrID;

       EXCEPTION
         WHEN NO_DATA_FOUND THEN
           v_exist_fipos := 0;
     END;

     IF( v_exist_fipos = 0 ) THEN
        RAISE_APPLICATION_ERROR(-20505,''); --Не открыта позиция по производному инструменту
     ELSE
        IF( v_state_fipos = DVPOS_STATE_CLOSE ) THEN
           RAISE_APPLICATION_ERROR(-20506,''); --Позиция по производному инструменту закрыта
        END IF;
     END IF;

     IF( DV_Setting_AccExContracts = 0 ) THEN -- По позиции
        v_GuarantyonDeal := 0;
     ELSE
        IF( v_GuarantyonDeal_fipos = chr(0) ) THEN
           v_GuarantyonDeal := 0;
        ELSE
           v_GuarantyonDeal := 1;
        END IF;
     END IF;

     IF( DV_Setting_AccExContracts = 0 ) THEN -- По позиции
        IF( (v_MARGIN = 0) AND (v_GUARANTY = 0) AND (v_FAIRVALUECALC = CHR(0)) AND (v_COUNTNOTCALCCOMM = 0) ) THEN
           RAISE_APPLICATION_ERROR(-20559,''); --Неверные параметры редактирования данных по позиции
        END IF;
     ELSE
        IF( ((v_GuarantyonDeal = 1) OR (v_GUARANTY = 0)) AND (v_COUNTNOTCALCCOMM = 0) )THEN
           RAISE_APPLICATION_ERROR(-20559,''); --Неверные параметры редактирования данных по позиции
        END IF;
     END IF;

     IF( DV_Setting_AccExContracts = 1 ) THEN -- По сделке
        Vv_EDITMARGIN    := 0;
        IF( v_GuarantyonDeal = 0 ) THEN
           Vv_EDITGUARANTY  := v_EDITGUARANTY;
        ELSE
           Vv_EDITGUARANTY  := 0;
        END IF;
        Vv_EDITFAIRVALUE := 0;
     ELSE
        Vv_EDITMARGIN    := v_EDITMARGIN;
        Vv_EDITGUARANTY  := v_EDITGUARANTY;
        Vv_EDITFAIRVALUE := v_EDITFAIRVALUE;
     END IF;

     -- обработаем dvfiturn
     BEGIN
       v_exist_fiturn := 1;

       SELECT * INTO T
         FROM ddvfiturn_dbt turn
        WHERE turn.t_FIID        = v_FIID
          AND turn.t_DEPARTMENT  = v_DEPARTMENT
          AND turn.t_BROKER      = v_BROKER
          AND turn.t_ClientContr = v_ClientContr
          AND turn.t_GenAgrID    = v_GenAgrID
          AND turn.t_DATE        = v_DATE;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN
           v_exist_fiturn := 0;
     END;

     IF( v_exist_fiturn = 0 OR
         (T.T_SETMARGIN = CHR(0) AND
          T.T_SETGUARANTY = CHR(0) AND
          T.T_SETFAIRVALUE = CHR(0) AND
          RSI_DV_GetCountNotCalcPosCom(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_GenAgrID) = 0
         )
       ) THEN
        RSI_DV_AttachPositionTurn(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_GenAgrID);
     ELSE
        IF( T.t_state = DVTURN_STATE_CLOSE ) THEN
           RAISE_APPLICATION_ERROR(-20509,''); --День по позиции закрыт
        END IF;
     END IF;

     IF( (Vv_EDITMARGIN = 1) OR (Vv_EDITGUARANTY = 1) OR (Vv_EDITFAIRVALUE = 1) ) THEN
        UPDATE ddvfiturn_dbt N SET N.T_MARGIN        = (CASE WHEN Vv_EDITMARGIN    = 1 THEN v_MARGIN    ELSE N.T_MARGIN END),
                                   N.T_SETMARGIN     = (CASE WHEN Vv_EDITMARGIN    = 1 THEN CHR(88)     ELSE N.T_SETMARGIN END),
                                   N.T_GUARANTY      = (CASE WHEN Vv_EDITGUARANTY  = 1 THEN v_GUARANTY  ELSE N.T_GUARANTY END),
                                   N.T_SETGUARANTY   = (CASE WHEN Vv_EDITGUARANTY  = 1 THEN CHR(88)     ELSE N.T_SETGUARANTY END),
                                   N.T_FAIRVALUECALC = (CASE WHEN Vv_EDITFAIRVALUE = 1 THEN v_FAIRVALUECALC ELSE N.T_FAIRVALUECALC END),
                                   N.T_FAIRVALUE     = (CASE WHEN Vv_EDITFAIRVALUE = 1 THEN (CASE WHEN v_FAIRVALUECALC = 'X' THEN v_FAIRVALUE ELSE 0 END) ELSE N.T_FAIRVALUE END),
                                   N.T_SETFAIRVALUE  = (CASE WHEN Vv_EDITFAIRVALUE = 1 THEN CHR(88)     ELSE N.T_SETFAIRVALUE END)
         WHERE N.t_FIID        =  v_FIID
           AND N.t_DEPARTMENT  =  v_DEPARTMENT
           AND N.t_BROKER      =  v_BROKER
           AND N.t_ClientContr =  v_ClientContr
           AND N.t_GenAgrID    =  v_GenAgrID
           AND N.t_DATE        =  v_DATE;
     END IF;
  END; -- RSI_DV_EditPositionTurn

  -- Переоценка стоимости по позиции. Используется в процедуре переоценки (только в модуле ПИ).
  PROCEDURE RSI_DV_OvervaluePositionTurn
           (
              v_FIID               IN INTEGER, -- Финансовый инструмент
              v_DEPARTMENT         IN INTEGER, -- Филиал
              v_BROKER             IN INTEGER, -- Брокер
              v_ClientContr        IN INTEGER, -- Договор с клиентом
              v_DATE               IN DATE,    -- Дата
              v_LONGPOSITIONCOST   IN NUMBER,  -- Стоимость длинных позиций
              v_SHORTPOSITIONCOST  IN NUMBER,  -- Стоимость коротких позиций
              v_OPERID             IN INTEGER, -- ID операции переоценки
              v_FLAG               IN INTEGER, -- Вид переоценки
              v_GenAgrID           IN INTEGER  -- ГС
           )
  IS
     v_exist_fiturn    INTEGER;
     v_maxdate_fiturn  DATE;
     v_exist_fiturn1   INTEGER;
     v_longposcost     NUMBER;
     v_shortposcost    NUMBER;
  BEGIN

     -- обработаем dvfiturn c max датой на момент переоценки
     BEGIN
       v_exist_fiturn := 1;
       v_maxdate_fiturn := V_DATE;

       SELECT Max(turn.t_DATE) INTO v_maxdate_fiturn
         FROM ddvfiturn_dbt turn
        WHERE turn.t_FIID        = v_FIID
          AND turn.t_DEPARTMENT  = v_DEPARTMENT
          AND turn.t_BROKER      = v_BROKER
          AND turn.t_ClientContr = v_ClientContr
          AND turn.t_GenAgrID    = v_GenAgrID
          AND turn.t_DATE       <= v_DATE;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN
           v_exist_fiturn := 0;
     END;

     IF( v_exist_fiturn = 1 AND v_maxdate_fiturn = v_DATE ) THEN
       -- временно установим статус в "открыт", чтобы сработала RSI_DV_AttachPositionTurn
       UPDATE ddvfiturn_dbt turn0 SET turn0.t_State = DVTURN_STATE_OPEN
        WHERE turn0.t_FIID        = v_FIID
          AND turn0.t_DEPARTMENT  = v_DEPARTMENT
          AND turn0.t_BROKER      = v_BROKER
          AND turn0.t_ClientContr = v_ClientContr
          AND turn0.t_GenAgrID    = v_GenAgrID
          AND turn0.t_DATE        = v_maxdate_fiturn;
     END IF;

     RSI_DV_AttachPositionTurn(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_GenAgrID);

     -- обработаем dvfiturn на дату переоценки
     BEGIN
       v_exist_fiturn1 := 1;

       SELECT turn1.t_LONGPOSITIONCOST,turn1.t_SHORTPOSITIONCOST INTO v_longposcost, v_shortposcost
         FROM ddvfiturn_dbt turn1
        WHERE turn1.t_FIID        = v_FIID
          AND turn1.t_DEPARTMENT  = v_DEPARTMENT
          AND turn1.t_BROKER      = v_BROKER
          AND turn1.t_ClientContr = v_ClientContr
          AND turn1.t_GenAgrID    = v_GenAgrID
          AND turn1.t_DATE        = v_DATE;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN
           v_exist_fiturn1 := 0;
     END;

     IF( v_exist_fiturn1 = 1 ) THEN

        UPDATE ddvoperps_dbt SET t_SUMM1 = v_longposcost,
                                 t_SUMM2 = v_shortposcost
         WHERE t_OPERID  = v_OPERID
           AND t_DOCKIND = 193
           AND t_DOCID IN ( SELECT t_ID
                              FROM ddvfipos_dbt pos
                             WHERE pos.t_FIID        = v_FIID
                               AND pos.t_DEPARTMENT  = v_DEPARTMENT
                               AND pos.t_BROKER      = v_BROKER
                               AND pos.t_ClientContr = v_ClientContr
                               AND pos.t_GenAgrID    = v_GenAgrID
                               AND pos.t_IsTrust     = chr(0)
                          )
           AND t_FLAG = v_FLAG;

        UPDATE ddvfiturn_dbt SET t_LONGPOSITIONCOST = v_LONGPOSITIONCOST,
                                 t_SHORTPOSITIONCOST = v_SHORTPOSITIONCOST,
                                 t_STATE = DVTURN_STATE_CLOSE
         WHERE t_FIID        = v_FIID
           AND t_DEPARTMENT  = v_DEPARTMENT
           AND t_BROKER      = v_BROKER
           AND t_ClientContr = v_ClientContr
           AND t_GenAgrID    = v_GenAgrID
           AND t_DATE        = v_DATE;
     END IF;

  END; -- RSI_DV_OvervaluePositionTurn

  -- Откат переоценки стоимости по позиции. Используется при откате переоценки.
  PROCEDURE RSI_DV_RecoilOvervaluePosTurn
           (
              v_FIID               IN INTEGER, -- Финансовый инструмент
              v_DEPARTMENT         IN INTEGER, -- Филиал
              v_BROKER             IN INTEGER, -- Брокер
              v_ClientContr        IN INTEGER, -- Договор с клиентом
              v_DATE               IN DATE,    -- Дата
              v_OPERID             IN INTEGER, -- ID операции переоценки
              v_FLAG               IN INTEGER, -- Вид переоценки
              v_GenAgrID           IN INTEGER  -- ГС
           )
  IS
     v_exist_fiturn1   INTEGER;
     v_state_fiturn1   INTEGER;
     v_exist_operps    INTEGER;
     v_longposcost     NUMBER;
     v_shortposcost    NUMBER;
  BEGIN

      -- обработаем dvfiturn на дату переоценки
      BEGIN
        v_exist_fiturn1 := 1;

        SELECT turn1.t_STATE  INTO v_state_fiturn1
          FROM ddvfiturn_dbt turn1
         WHERE turn1.t_FIID        = v_FIID
           AND turn1.t_DEPARTMENT  = v_DEPARTMENT
           AND turn1.t_BROKER      = v_BROKER
           AND turn1.t_ClientContr = v_ClientContr
           AND turn1.t_GenAgrID    = v_GenAgrID
           AND turn1.t_DATE        = v_DATE;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_exist_fiturn1 := 0;
      END;

      IF( v_exist_fiturn1 != 1 ) THEN
         RAISE_APPLICATION_ERROR(-20526,'');--"Не найдены итоги дня по позиции"
      ELSE
        IF( v_state_fiturn1 != DVTURN_STATE_CLOSE ) THEN
           RAISE_APPLICATION_ERROR(-20513,'');--"Не закрыт день по позиции"
        END IF;

        -- временно установим статус в "открыт", чтобы сработала RSI_DV_DetachPositionTurn
        UPDATE ddvfiturn_dbt turn SET t_STATE = DVTURN_STATE_OPEN
         WHERE turn.t_FIID        = v_FIID
           AND turn.t_DEPARTMENT  = v_DEPARTMENT
           AND turn.t_BROKER      = v_BROKER
           AND turn.t_ClientContr = v_ClientContr
           AND turn.t_GenAgrID    = v_GenAgrID
           AND turn.t_DATE        = v_DATE;

        RSI_DV_DetachPositionTurn(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_GenAgrID);

        BEGIN
           v_exist_operps := 1;
           SELECT t_SUMM1, t_SUMM2 INTO  v_longposcost, v_shortposcost
             FROM ddvoperps_dbt
            WHERE t_OPERID = v_OPERID
              AND t_DOCKIND = 193
              AND t_DOCID IN ( SELECT t_ID
                                 FROM ddvfipos_dbt pos
                                WHERE pos.t_FIID        = v_FIID
                                  AND pos.t_DEPARTMENT  = v_DEPARTMENT
                                  AND pos.t_BROKER      = v_BROKER
                                  AND pos.t_ClientContr = v_ClientContr
                                  AND pos.t_GenAgrID    = v_GenAgrID
                                  AND pos.t_IsTrust     = chr(0)
                             )
              AND t_FLAG = v_FLAG;
           EXCEPTION
             WHEN NO_DATA_FOUND THEN
               v_exist_operps := 0;
        END;

        IF( v_exist_operps = 1 ) THEN
           -- Если запись DDVFITURN  с заданными FIID, DEPARTMENT, BROKER, ClientContr, GenAgrID, DATE еще существует
           UPDATE ddvfiturn_dbt SET t_LONGPOSITIONCOST  = v_longposcost,
                                    t_SHORTPOSITIONCOST = v_shortposcost,
                                    t_STATE = DVTURN_STATE_CLOSE
            WHERE t_FIID        =  v_FIID
              AND t_DEPARTMENT  =  v_DEPARTMENT
              AND t_BROKER      =  v_BROKER
              AND t_ClientContr =  v_ClientContr
              AND t_GenAgrID    =  v_GenAgrID
              AND t_DATE        =  v_DATE;
        END IF;

      END IF;

  END; -- RSI_DV_RecoilOvervaluePosTurn

  --Импорт комиссии по позиции.
  PROCEDURE RSI_DV_ImportComPosition
           (
              v_FIID            IN INTEGER, -- Финансовый инструмент
              v_DEPARTMENT      IN INTEGER, -- Филиал
              v_BROKER          IN INTEGER, -- Брокер
              v_Client          IN INTEGER, -- Клиент
              v_ClientContr     IN INTEGER, -- Клиент договор
              v_DATE            IN DATE,    -- Дата
              v_ISTRUST         IN CHAR,    -- Признак ДУ
              v_ComissID        IN INTEGER, -- Комиссия
              v_SUM             IN NUMBER,  -- Сумма
              v_NDS             IN NUMBER,  -- НДС
              v_GenAgrID        IN INTEGER  -- ГС
           )
  IS
     v_exist_fiturn      INTEGER;
     v_exist_fipos       INTEGER;
     v_state_fiturn      INTEGER;
     v_ClientContr_fipos INTEGER;
     v_exist_ficom       INTEGER;
     v_COM               ddvfi_com_dbt%ROWTYPE;
  BEGIN

     v_ClientContr_fipos := 0;
     v_exist_fipos := 1;

     BEGIN
       SELECT t_ClientContr INTO v_ClientContr_fipos
         FROM ddvfipos_dbt pos
        WHERE pos.t_IsTrust     = DV_Setting_RunFromTrust AND
              pos.t_FIID        = v_FIID AND
              pos.t_DEPARTMENT  = v_DEPARTMENT AND
              pos.t_BROKER      = v_BROKER AND
              pos.t_ClientContr = v_ClientContr AND
              pos.t_GenAgrID    = v_GenAgrID;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         v_exist_fipos := 0;
     END;

     IF( v_exist_fipos = 0 ) THEN
        RAISE_APPLICATION_ERROR(-20505,''); --Не открыта позиция по производному инструменту
     END IF;

     v_exist_fiturn := 1;
     BEGIN
       SELECT t_STATE INTO v_state_fiturn
         FROM ddvfiturn_dbt turn
        WHERE turn.t_FIID       = v_FIID AND
              turn.t_DEPARTMENT = v_DEPARTMENT AND
              turn.t_BROKER     = v_BROKER AND
              turn.t_ClientContr= v_ClientContr AND
              turn.t_GenAgrID   = v_GenAgrID AND
              turn.t_DATE       = v_DATE;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN
         v_exist_fiturn := 0;
     END;

     IF( v_exist_fipos = 0 ) THEN
        RAISE_APPLICATION_ERROR(-20526,''); --Не найдены итоги дня по позиции
     ELSIF( v_state_fiturn = DVTURN_STATE_CLOSE ) THEN
        RAISE_APPLICATION_ERROR(-20560,''); --Итоги дня по позиции закрыты
     END IF;

     BEGIN
       v_exist_ficom := 1;

       SELECT * INTO v_COM
         FROM ddvfi_com_dbt
        WHERE t_FIID        = v_FIID
          AND t_DEPARTMENT  = v_DEPARTMENT
          AND t_BROKER      = v_BROKER
          AND t_ClientContr = v_ClientContr_fipos
          AND t_GenAgrID    = v_GenAgrID
          AND t_DATE        = v_DATE
          AND t_ComissID    = v_ComissID;

       EXCEPTION
         WHEN NO_DATA_FOUND THEN
           v_exist_ficom := 0;
     END;

     IF v_exist_ficom = 0 THEN
        INSERT INTO DDVFI_COM_DBT( T_DEPARTMENT,
                                   T_BROKER,
                                   T_CLIENTCONTR,
                                   T_CLIENT,
                                   T_FIID,
                                   T_DATE,
                                   T_SUM,
                                   T_NDS,
                                   T_NOTCALC,
                                   T_ID,
                                   T_COMISSID,
                                   t_Sector,
                                   t_GenAgrID
                                 )
                                 VALUES
                                 (
                                   v_DEPARTMENT,
                                   v_BROKER,
                                   v_ClientContr_fipos,
                                   v_Client,
                                   v_FIID,
                                   v_DATE,
                                   v_SUM,
                                   v_NDS,
                                   CHR(88),
                                   0,
                                   v_ComissID,
                                   chr(0)/*v_Sector*/,
                                   v_GenAgrID
                                 );
     END IF;

  END; -- RSI_DV_ImportComPosition

  --Процедура расчета комиссии по позиции
  PROCEDURE RSI_DV_CalcPosCom
           (
              pFIID            IN INTEGER, -- Финансовый инструмент
              pDEPARTMENT      IN INTEGER, -- Филиал
              pBROKER          IN INTEGER, -- Брокер
              pClientContr     IN INTEGER, -- Клиент договор
              pDATE            IN DATE,    -- Дата
              pComissID        IN INTEGER, -- Комиссия
              pSUM             IN NUMBER,  -- Сумма
              pNDS             IN NUMBER,  -- НДС
              pGenAgrID        IN INTEGER  -- ГС
           )
  IS
     v_exist_ficom  INTEGER;
     v_COM          ddvfi_com_dbt%ROWTYPE;
     v_Sum          NUMBER;
     v_NDS          NUMBER;
  BEGIN

     BEGIN
       v_exist_ficom := 1;

       SELECT * INTO v_COM
         FROM ddvfi_com_dbt
        WHERE t_FIID        = pFIID
          AND t_DEPARTMENT  = pDEPARTMENT
          AND t_BROKER      = pBROKER
          AND t_ClientContr = pClientContr
          AND t_GenAgrID    = pGenAgrID
          AND t_DATE        = pDATE
          AND t_ComissID    = pComissID;

       EXCEPTION
         WHEN NO_DATA_FOUND THEN
           v_exist_ficom := 0;
     END;

     IF( v_exist_ficom = 0 ) THEN
        IF( pSUM > 0 ) THEN
           INSERT INTO DDVFI_COM_DBT( T_DEPARTMENT,
                                      T_BROKER,
                                      T_CLIENTCONTR,
                                      T_FIID,
                                      T_DATE,
                                      T_SUM,
                                      T_NDS,
                                      T_NOTCALC,
                                      T_ID,
                                      T_COMISSID,
                                      t_Sector,
                                      t_GenAgrID
                                    )
                                    VALUES
                                    (
                                      pDEPARTMENT,
                                      pBROKER,
                                      pClientContr,
                                      pFIID,
                                      pDATE,
                                      pSUM,
                                      pNDS,
                                      CHR(0),
                                      0,
                                      pComissID,
                                      chr(0)/*pSector*/,
                                      pGenAgrID
                                    );
        ELSE
           RAISE_APPLICATION_ERROR(-20568,''); --Неверная сумма комиссии
        END IF;

     ELSIF( v_COM.T_NOTCALC = CHR(0) )THEN
        v_Sum := v_COM.T_SUM + pSum;
        v_NDS := v_COM.T_NDS + pNDS;

        IF( v_Sum < 0 ) THEN
           RAISE_APPLICATION_ERROR(-20568,''); --Неверная сумма комиссии
        ELSIF( v_Sum > 0 ) THEN
           UPDATE ddvfi_com_dbt
              SET T_SUM = v_Sum,
                  T_NDS = v_NDS
            WHERE t_FIID        = pFIID
              AND t_DEPARTMENT  = pDEPARTMENT
              AND t_BROKER      = pBROKER
              AND t_ClientContr = pClientContr
              AND t_GenAgrID    = pGenAgrID
              AND t_DATE        = pDATE
              AND t_ComissID    = pComissID;
        ELSE --v_Sum = 0
           DELETE FROM ddvfi_com_dbt
            WHERE t_FIID        = pFIID
              AND t_DEPARTMENT  = pDEPARTMENT
              AND t_BROKER      = pBROKER
              AND t_ClientContr = pClientContr
              AND t_GenAgrID    = pGenAgrID
              AND t_DATE        = pDATE
              AND t_ComissID    = pComissID;
        END IF;
     END IF;
  END; -- RSI_DV_CalcPosCom

  --Процедура обновления пересчитываемой комиссии по позиции
  PROCEDURE RSI_DV_UpdatePosCom
           (
              pFIID            IN INTEGER, -- Финансовый инструмент
              pDEPARTMENT      IN INTEGER, -- Филиал
              pBROKER          IN INTEGER, -- Брокер
              pClientContr     IN INTEGER, -- Клиент договор
              pDATE            IN DATE,    -- Дата
              pComissID        IN INTEGER, -- Комиссия
              pGenAgrID        IN INTEGER  -- ГС
           )
  IS
     v_exist_ficom  INTEGER;
     v_COM          ddvfi_com_dbt%ROWTYPE;
     v_Sum          NUMBER;
     v_NDS          NUMBER;
  BEGIN

     SELECT NVL(SUM(T_SUM),0), NVL(SUM(T_NDS),0) INTO v_Sum, v_NDS
       FROM ddvdlcom_dbt DC, ddvdeal_dbt DL
      WHERE DC.t_DealID      = DL.t_ID
        AND DL.t_DATE_CLR    = pDATE
        AND DL.t_FIID        = pFIID
        AND DL.t_DEPARTMENT  = pDEPARTMENT
        AND DL.t_BROKER      = pBROKER
        AND DL.t_ClientContr = pClientContr
        AND DL.t_GenAgrID    = pGenAgrID
        AND DC.t_ComissID    = pComissID
        AND (((DV_Setting_AccExContracts = 0) and (DL.t_State = DVDEAL_STATE_CLOSE)) or
             ((DV_Setting_AccExContracts = 1) and (DL.t_PosAcc = chr(88))));

     IF( v_Sum = 0 ) THEN
        DELETE FROM ddvfi_com_dbt
         WHERE t_FIID        =  pFIID
           AND t_DEPARTMENT  =  pDEPARTMENT
           AND t_BROKER      =  pBROKER
           AND t_ClientContr =  pClientContr
           AND t_GenAgrID    =  pGenAgrID
           AND t_DATE        =  pDATE
           AND t_ComissID    =  pComissID;
     ELSE

        BEGIN
          v_exist_ficom := 1;

          SELECT * INTO v_COM
            FROM ddvfi_com_dbt
           WHERE t_FIID        = pFIID
             AND t_DEPARTMENT  = pDEPARTMENT
             AND t_BROKER      = pBROKER
             AND t_ClientContr = pClientContr
             AND t_GenAgrID    = pGenAgrID
             AND t_DATE        = pDATE
             AND t_ComissID    = pComissID;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              v_exist_ficom := 0;
        END;

        IF( v_exist_ficom = 0 ) THEN
           INSERT INTO DDVFI_COM_DBT( T_DEPARTMENT,
                                      T_BROKER,
                                      T_CLIENTCONTR,
                                      T_FIID,
                                      T_DATE,
                                      T_SUM,
                                      T_NDS,
                                      T_NOTCALC,
                                      T_ID,
                                      T_COMISSID,
                                      t_Sector,
                                      t_GenAgrID
                                    )
                                    VALUES
                                    (
                                      pDEPARTMENT,
                                      pBROKER,
                                      pClientContr,
                                      pFIID,
                                      pDATE,
                                      v_Sum,
                                      v_NDS,
                                      CHR(0),
                                      0,
                                      pComissID,
                                      chr(0)/*pSector*/,
                                      pGenAgrID
                                    );
        ELSE
           UPDATE ddvfi_com_dbt
              SET T_SUM = v_Sum,
                  T_NDS = v_NDS,
                  T_NOTCALC = chr(0)
            WHERE t_FIID        = pFIID
              AND t_DEPARTMENT  = pDEPARTMENT
              AND t_BROKER      = pBROKER
              AND t_ClientContr = pClientContr
              AND t_GenAgrID    = pGenAgrID
              AND t_DATE        = pDATE
              AND t_ComissID    = pComissID;
        END IF;
     END IF;
  END; -- RSI_DV_UpdatePosCom

  -- Импорт комиссии по операции.
  PROCEDURE RSI_DV_ImportDealCom
           (
              pDealID          IN INTEGER,  -- ID сделки
              pComissID        IN INTEGER,  -- Комиссия
              pSUM             IN NUMBER,   -- Сумма
              pNDS             IN NUMBER    -- НДС
           )
  IS
     v_DealID      INTEGER;
  BEGIN
     INSERT INTO DDVDLCOM_DBT( T_DEALID,
                               T_SUM,
                               T_NDS,
                               T_ID,
                               T_ComissID
                             ) VALUES(
                               pDealID,
                               pSum,
                               pNDS,
                               0,
                               pComissID);
  END; -- RSI_DV_ImportDealCom

  --Процедура получения сумм комиссий по итогам дня
  PROCEDURE RSI_DV_GetTurnCom
           (
              pFIID            IN INTEGER, -- Финансовый инструмент
              pDEPARTMENT      IN INTEGER, -- Филиал
              pBROKER          IN INTEGER, -- Брокер
              pClientContr     IN INTEGER, -- Клиент договор
              pDATE            IN DATE,    -- Дата
              pComissID        IN INTEGER, -- Комиссия
              pSum             OUT NUMBER,
              pNDS             OUT NUMBER,
              pGenAgrID        IN INTEGER  -- ГС
           )
  IS
  BEGIN
     SELECT NVL(SUM(T_SUM),0), NVL(SUM(T_NDS),0) INTO pSum, pNDS
       FROM ddvdlcom_dbt DC, ddvdeal_dbt DL
      WHERE DC.t_DealID      = DL.t_ID
        AND DL.t_DATE_CLR    = pDATE
        AND DL.t_FIID        = pFIID
        AND DL.t_DEPARTMENT  = pDEPARTMENT
        AND DL.t_BROKER      = pBROKER
        AND DL.t_ClientContr = pClientContr
        AND DL.t_GenAgrID    = pGenAgrID
        AND DC.t_ComissID    = pComissID
        AND (((DV_Setting_AccExContracts = 0) and (DL.t_State = DVDEAL_STATE_CLOSE)) or
            ((DV_Setting_AccExContracts = 1) and (DL.t_PosAcc = chr(88))));
  END; -- RSI_DV_GetTurnCom

  -- Вид ПД в операции расчетов
  FUNCTION DV_GetDocKind RETURN NUMBER
  IS
  BEGIN
    IF( DV_Setting_RunFromTrust = 'X' ) THEN
      return 198; /*Расчеты по ПИ в ДУ*/
    ELSE
      return 194; /*Расчеты по ПИ в ПИ*/
    END IF;
  END;

  FUNCTION DV_GetDvDealDate (p_DealDate IN DATE, p_DealTime IN DATE) RETURN DATE
  IS
    v_TimeLate DATE;
  BEGIN
    v_TimeLate := TO_DATE('01.01.0001 19:00:00','DD.MM.YYYY HH24:MI:SS');
    IF( p_DealTime < v_TimeLate ) THEN
      return p_DealDate; /*Дата актуальная*/
    ELSE
      return RSI_RSBCALENDAR.GetDateAfterWorkDay(p_DealDate, 1); /*Сделка уходит на следующий день*/
    END IF;
  END;

  -- Функция, возвращает текущую стоимость минимального шага цены
  FUNCTION DV_TickCost(
    p_FIID IN INTEGER, p_TICKDATE IN DATE
  )
  RETURN NUMBER

  IS
     v_TICKCOST  NUMBER;
     v_PARENTFI  dfininstr_dbt.t_PARENTFI%TYPE;
  BEGIN

    v_TICKCOST := 0;
    SELECT fider.t_TICKCOST, fin.t_PARENTFI INTO v_TICKCOST, v_PARENTFI FROM dfideriv_dbt fider, dfininstr_dbt fin  WHERE fider.t_FIID = p_FIID AND fin.t_FIID = p_FIID;

    IF( v_TICKCOST != 0 ) THEN
      RETURN v_TICKCOST;

    ELSE
      -- вид курса из настройки "Стоимость минимального шага цены"
      IF( DV_Setting_KindMinTickCost != 0 ) THEN
         v_TICKCOST := RSI_rsb_fiinstr.ConvSumType( 1., p_FIID, v_PARENTFI, DV_Setting_KindMinTickCost, p_TICKDATE );
      END IF;

      IF( v_TICKCOST is NULL ) THEN
          v_TICKCOST:= 0.0;
      END IF;
    END IF;

    RETURN v_TICKCOST;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN 0.0;
    WHEN OTHERS THEN
      RETURN 0.0;
  END DV_TickCost;

  -- Переоценка стоимости по позиции для модуля ДУ.
  PROCEDURE RSI_TS_OvervaluePositionDV
           (
              v_OPERID             IN INTEGER, -- Операция расчетов
              v_PosID              IN INTEGER, -- Позиция
              v_Date               IN DATE,    -- Дата
              v_Summa              IN NUMBER,  -- Сумма переоценки
              v_Rate               IN NUMBER,  -- Курс
              v_NewLongCost        IN NUMBER,  -- Новая стоимость длинных позиций
              v_NewShortCost       IN NUMBER   -- Новая стоимость коротких позиций
           )
  IS
     v_exist_fiturn    INTEGER;
     v_maxdate_fiturn  DATE;
     v_exist_fiturn1   INTEGER;
     v_longposcost     NUMBER;
     v_shortposcost    NUMBER;
     v_ClientContr     NUMBER;
     v_FIID            NUMBER;
     v_DEPARTMENT      NUMBER;
     v_BROKER          NUMBER;
     v_GenAgrID        INTEGER;
  BEGIN

      BEGIN
        SELECT t_ClientContr, t_FIID, t_DEPARTMENT, t_BROKER, t_GenAgrID
          INTO v_ClientContr, v_FIID, v_DEPARTMENT, v_BROKER, v_GenAgrID
          FROM DDVFIPOS_DBT
         WHERE t_ID = v_PosID;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20526,'');--"Не найдены итоги дня по позиции"
      END;

      -- обработаем dvfiturn c max датой на момент переоценки
      BEGIN
        v_exist_fiturn := 1;
        v_maxdate_fiturn := V_DATE;

        SELECT Max(turn.t_DATE) INTO v_maxdate_fiturn
          FROM ddvfiturn_dbt turn
         WHERE turn.t_FIID        = v_FIID
           AND turn.t_DEPARTMENT  = v_DEPARTMENT
           AND turn.t_BROKER      = v_BROKER
           AND turn.t_ClientContr = v_ClientContr
           AND turn.t_GenAgrID    = v_GenAgrID
           AND turn.t_DATE       <= v_DATE;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_exist_fiturn := 0;
      END;

      IF( v_exist_fiturn = 1 AND v_maxdate_fiturn = v_DATE ) THEN
        -- временно установим статус в "открыт", чтобы сработала RSI_DV_AttachPositionTurn
        UPDATE ddvfiturn_dbt turn0
           SET turn0.t_State = DVTURN_STATE_OPEN
         WHERE turn0.t_FIID        = v_FIID
           AND turn0.t_DEPARTMENT  = v_DEPARTMENT
           AND turn0.t_BROKER      = v_BROKER
           AND turn0.t_ClientContr = v_ClientContr
           AND turn0.t_GenAgrID    = v_GenAgrID
           AND turn0.t_DATE        = v_maxdate_fiturn;
      END IF;

      RSI_DV_AttachPositionTurn(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_GenAgrID);

      -- обработаем dvfiturn на дату переоценки
      BEGIN
        v_exist_fiturn1 := 1;

        SELECT turn1.t_LONGPOSITIONCOST, turn1.t_SHORTPOSITIONCOST
          INTO v_longposcost, v_shortposcost
          FROM ddvfiturn_dbt turn1
         WHERE turn1.t_FIID        = v_FIID
           AND turn1.t_DEPARTMENT  = v_DEPARTMENT
           AND turn1.t_BROKER      = v_BROKER
           AND turn1.t_ClientContr = v_ClientContr
           AND turn1.t_GenAgrID    = v_GenAgrID
           AND turn1.t_DATE        = v_DATE;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_exist_fiturn1 := 0;
      END;

      IF( v_exist_fiturn1 = 1 ) THEN

        INSERT INTO ddvoperps_dbt (
                                   t_OPERID,
                                   t_DOCKIND,
                                   t_DOCID,
                                   t_SUMM,
                                   t_SUMM1,
                                   t_SUMM2,
                                   t_FLAG
                                  )
                                  VALUES
                                  (
                                   v_OPERID,
                                   196,
                                   v_PosID,
                                   v_Summa,
                                   v_longposcost,
                                   v_shortposcost,
                                   2
                                  );

        UPDATE ddvfiturn_dbt turn
           SET t_LONGPOSITIONCOST  = v_NewLongCost,
               t_SHORTPOSITIONCOST = v_NewShortCost,
               t_STATE             = DVTURN_STATE_CLOSE
         WHERE turn.t_FIID        = v_FIID
           AND turn.t_DEPARTMENT  = v_DEPARTMENT
           AND turn.t_BROKER      = v_BROKER
           AND turn.t_CLIENTCONTR = v_CLIENTCONTR
           AND turn.t_GenAgrID    = v_GenAgrID
           AND turn.t_DATE        = v_DATE;
      END IF;

  END; -- RSI_TS_OvervaluePositionDV

  -- Откат переоценки стоимости по позиции.
  PROCEDURE RSI_TS_RecoilOvervaluePosDV
           (
              v_OPERID             IN INTEGER, -- Операция расчетов
              v_PosID              IN INTEGER, -- Позиция
              v_Date               IN DATE     -- Дата
           )
  IS
     v_exist_fiturn1   INTEGER;
     v_state_fiturn1   INTEGER;
     v_exist_operps    INTEGER;
     v_longposcost     NUMBER;
     v_shortposcost    NUMBER;
     v_ClientContr     NUMBER;
     v_FIID            NUMBER;
     v_DEPARTMENT      NUMBER;
     v_BROKER          NUMBER;
     v_GenAgrID        INTEGER;
  BEGIN

      BEGIN
        SELECT t_ClientContr, t_FIID, t_DEPARTMENT, t_BROKER, t_GenAgrID
          INTO v_ClientContr, v_FIID, v_DEPARTMENT, v_BROKER, v_GenAgrID
          FROM DDVFIPOS_DBT
         WHERE t_ID = v_PosID;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20526,'');--"Не найдены итоги дня по позиции"
      END;

      -- обработаем dvfiturn на дату переоценки
      BEGIN
        v_exist_fiturn1 := 1;

        SELECT turn1.t_STATE  INTO v_state_fiturn1
          FROM ddvfiturn_dbt turn1
         WHERE turn1.t_FIID        = v_FIID
           AND turn1.t_DEPARTMENT  = v_DEPARTMENT
           AND turn1.t_BROKER      = v_BROKER
           AND turn1.t_ClientContr = v_ClientContr
           AND turn1.t_GenAgrID    = v_GenAgrID
           AND turn1.t_DATE        = v_DATE;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_exist_fiturn1 := 0;
      END;

      IF( v_exist_fiturn1 != 1 ) THEN
         RAISE_APPLICATION_ERROR(-20526,'');--"Не найдены итоги дня по позиции"
      ELSE
        IF( v_state_fiturn1 != DVTURN_STATE_CLOSE ) THEN
           RAISE_APPLICATION_ERROR(-20513,'');--"Не закрыт день по позиции"
        END IF;

        -- временно установим статус в "открыт", чтобы сработала RSI_DV_DetachPositionTurn
        UPDATE ddvfiturn_dbt turn SET t_STATE = DVTURN_STATE_OPEN
         WHERE turn.t_FIID        = v_FIID
           AND turn.t_DEPARTMENT  = v_DEPARTMENT
           AND turn.t_BROKER      = v_BROKER
           AND turn.t_ClientContr = v_ClientContr
           AND turn.t_GenAgrID    = v_GenAgrID
           AND turn.t_DATE        = v_DATE;

        RSI_DV_DetachPositionTurn(v_FIID, v_DEPARTMENT, v_BROKER, v_ClientContr, v_DATE, v_GenAgrID);

        BEGIN
           v_exist_operps := 1;
           SELECT t_SUMM1, t_SUMM2 INTO  v_longposcost, v_shortposcost
             FROM ddvoperps_dbt
            WHERE t_OPERID  = v_OPERID
              AND t_DOCKIND = 196
              AND t_DOCID   = v_PosID
              AND t_FLAG    = 2;
           EXCEPTION
             WHEN NO_DATA_FOUND THEN
               v_exist_operps := 0;
        END;

        IF( v_exist_operps = 1 ) THEN
           -- Если запись DDVFITURN еще существует
           UPDATE ddvfiturn_dbt SET t_LONGPOSITIONCOST  = v_longposcost,
                                    t_SHORTPOSITIONCOST = v_shortposcost,
                                    t_STATE = DVTURN_STATE_CLOSE
            WHERE t_FIID        = v_FIID
              AND t_DEPARTMENT  = v_DEPARTMENT
              AND t_BROKER      = v_BROKER
              AND t_ClientContr = v_ClientContr
              AND t_GenAgrID    = v_GenAgrID
              AND t_DATE        = v_DATE;

           DELETE FROM ddvoperps_dbt
            WHERE t_OPERID  = v_OPERID
              AND t_DOCKIND = 196
              AND t_DOCID   = v_PosID
              AND t_FLAG    = 2;
        END IF;

      END IF;

  END; -- RSI_TS_RecoilOvervaluePosDV

  FUNCTION DV_ParentFI( v_FIID IN NUMBER ) RETURN NUMBER
  IS
     ParentFIID  NUMBER;
  BEGIN

     BEGIN
       SELECT t_ParentFI
         INTO ParentFIID
         FROM dfininstr_dbt
        WHERE T_FIID = v_FIID;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN ParentFIID := 0;
     END;

     RETURN ParentFIID;
  END;

  FUNCTION DV_PositionBonusFIID( v_FIID IN NUMBER ) RETURN NUMBER
  IS
     BonusFIID  NUMBER;
  BEGIN

     BEGIN
       SELECT DECODE( t_AvoirKind,
                      DV_DERIVATIVE_OPTION, t_ParentFI,
                      -1
                    )
         INTO BonusFIID
         FROM dfininstr_dbt
        WHERE T_FIID = v_FIID;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN BonusFIID := 0;
     END;

     RETURN BonusFIID;
  END;

  FUNCTION DV_GetPosByTurn(
              v_FIID         IN INTEGER, -- производный инструмент
              v_DEPARTMENT   IN INTEGER, -- Филиал
              v_BROKER       IN INTEGER, -- Брокер
              v_ClientContr  IN INTEGER, -- Договор с клиентом
              v_GenAgrID     IN INTEGER  -- ГС

  ) RETURN NUMBER
  IS
     PosID  NUMBER;
  BEGIN

     BEGIN
       SELECT POS.T_ID
         INTO PosID
         FROM ddvfipos_dbt POS
        WHERE POS.T_DEPARTMENT  = v_DEPARTMENT
          AND POS.T_FIID        = v_FIID
          AND POS.T_BROKER      = v_BROKER
          AND POS.T_CLIENTCONTR = v_CLIENTCONTR
          AND POS.t_GenAgrID    = v_GenAgrID;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN PosID := 0;
     END;

     RETURN PosID;
  END DV_GetPosByTurn;

  FUNCTION DV_GetOperatonByTurn(
              v_FIID         IN INTEGER, -- производный инструмент
              v_DEPARTMENT   IN INTEGER, -- Филиал
              v_BROKER       IN INTEGER, -- Брокер
              v_ClientContr  IN INTEGER, -- Договор с клиентом
              v_Date         IN DATE,    -- Дата
              v_RefOperation OUT ddvoper_dbt%ROWTYPE,
              v_GenAgrID     IN INTEGER  -- ГС
  ) RETURN NUMBER
  IS
    OperID NUMBER;
  BEGIN

     BEGIN
        SELECT *
          INTO v_RefOperation
          FROM DDVOPER_DBT
         WHERE t_DocKind    = DV_GetDocKind
           AND T_DEPARTMENT = v_DEPARTMENT
           AND T_DATE       = v_DATE
           AND t_GenAgrID   = v_GenAgrID
           AND ( (     t_PARTYKIND = 3
                   AND t_PARTY     = (SELECT FI.T_ISSUER
                                        FROM dfininstr_dbt FI
                                       WHERE FI.T_FIID = v_FIID
                                     )
                   AND v_BROKER = -1
                 )
                 OR
                 (
                       t_PARTYKIND = 22
                   AND (t_PARTY, t_PARTYCONTR) IN ( SELECT POS.T_BROKER, POS.T_BROKERCONTR
                                                      FROM ddvfipos_dbt POS
                                                     WHERE POS.T_ID = DV_GetPosByTurn(v_FIID,v_DEPARTMENT,v_BROKER,v_ClientContr,v_GenAgrID)
                                                  )
                   AND v_BROKER <> -1
                 )
               );
     EXCEPTION
       WHEN NO_DATA_FOUND THEN return 0;
     END;

     BEGIN
       SELECT t_ID_Operation
         INTO OperID
         FROM DOPROPER_DBT oproper
        WHERE oproper.t_DocKind    = DV_GetDocKind
          AND oproper.t_DocumentID = LPAD( v_RefOperation.t_ID, 34,'0' );
     EXCEPTION
       WHEN NO_DATA_FOUND THEN return 0;
     END;

     RETURN OperID;
  END DV_GetOperatonByTurn;

  PROCEDURE RSI_DV_CreateDemandTrust( ID_Operation         IN INTEGER,
                                      UserMark             IN VARCHAR2,
                                      Role                 IN INTEGER,
                                      KVTO                 IN VARCHAR2,
                                      KUS                  IN VARCHAR2,
                                      KVDR                 IN VARCHAR2,
                                      Dplan                IN DATE,
                                      Summa                IN NUMBER,
                                      ActiveFIID           IN INTEGER,
                                      ActiveDepositoryID   IN INTEGER,
                                      SFClientContr        IN INTEGER,
                                      BaseFIID             IN INTEGER
                                    )
  IS
    Demand       dtsdemand_dbt%ROWTYPE;
    NewID        INTEGER;
    TsOrderID    NUMBER := 0;
    TsOrderKind  NUMBER := 0;
    tmp          VARCHAR2(100);
  BEGIN
    RSI_DV_InitError();

    Demand.t_Role             :=  Role;      -- Т/О
    Demand.t_InitialQuantity  :=  Summa;     -- исходная сумма = общая сумма сделки
    Demand.t_Kind             :=  TrustAPI.TS_DemandKind(KVTO);      -- Вид Т/О из классификатора "ДУ вид Т/О".
    Demand.t_FIID             :=  0; -- валюта учета
    Demand.t_PlanCloseDate    :=  Dplan;     -- планируемая дата исполнения
    Demand.t_BofficeKind      :=  'Ю';       -- бэкофис ПИ
    Demand.t_UserMark         :=  UserMark; /* метка*/
    Demand.t_EventID          :=  TrustAPI.TS_DemandEvent(KUS); -- учетное событие создания Т/О
    Demand.t_Department       :=  RsbSessionData.OperDprt();          -- филиал
    Demand.t_Account          :=  '';
    Demand.t_BaseFIID         :=  BaseFIID;
    Demand.t_KindProfit       :=  TrustAPI.TS_ProfitKind(KVDR);
    Demand.t_ID_Operation     :=  ID_Operation;

    BEGIN
       SELECT t_DocKind, t_ID
         INTO TsOrderKind, TsOrderID
         FROM dtsorder_dbt
        WHERE t_SfContrID = SFClientContr;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN NULL;
    END;

    TrustAPI.CreateDemandFromOtherBO_rec(
                                    SFClientContr,       /*SfContrID*/
                                    TrustAPI.RSI_TS_GenerateActiveCode( TrustAPI.TS_ACTIVE_KIND_EMISSIVE, ActiveFIID, ActiveDepositoryID, TsOrderKind, TsOrderID, tmp, tmp, tmp ),/*ActiveCode*/
                                    TrustAPI.TS_ACTIVE_KIND_EMISSIVE, /*ActiveKind*/
                                    ActiveFIID,          /*ActiveFIID*/
                                    ActiveDepositoryID,  /*DepositoryID*/
                                    0,                   /*BalanceFIID*/
                                    '',
                                    RsbSessionData.Oper(),
                                    Demand,
                                    NewID );
  EXCEPTION
    WHEN OTHERS THEN RSI_DV_SetError(SQLCODE, 'TrustAPI');
  END; -- RSI_DV_CreateDemandTrust

  PROCEDURE DV_CreateDemandBonus( v_UserMarkMain IN VARCHAR2,
                                  v_KindTOMain   IN INTEGER,
                                  v_ID_Operation IN INTEGER,
                                  v_Party        IN INTEGER,
                                  v_FIID         IN INTEGER, -- производный инструмент
                                  v_DEPARTMENT   IN INTEGER, -- Филиал
                                  v_BROKER       IN INTEGER, -- Брокер
                                  v_ClientContr  IN INTEGER, -- Договор с клиентом
                                  v_Date         IN DATE,    -- Дата
                                  v_Bonus        IN NUMBER
                                )
  IS
     PositionBonusFIID NUMBER;
  BEGIN
     PositionBonusFIID := DV_PositionBonusFIID( v_FIID );

     /*Т/О*/
     RSI_DV_CreateDemandTrust( v_ID_Operation,         /*ID_Operation*/
                           v_UserMarkMain,         /*UserMark*/
                           v_KindTOMain,           /*Role*/
                           '2'  /*оплата*/,        /*КВТО*/
                           '91' /*расчеты по СК*/, /*КУС*/
                           null /*ДР по опционам*/,/*КВДР*/
                           v_Date,                 /*Дплан*/
                           v_Bonus,                /*Summa*/
                           PositionBonusFIID,      /*ActiveFIID*/
                           v_Party,                /*ActiveDepositoryID*/
                           v_ClientContr,          /*SFClientContr*/
                           v_FIID                  /*BaseFIID*/
                      );
  END;

  PROCEDURE DV_CreateDemandMargin( v_UserMarkMain IN VARCHAR2,
                                  v_KindTOMain   IN INTEGER,
                                  v_ID_Operation IN INTEGER,
                                  v_Party        IN INTEGER,
                                  v_FIID         IN INTEGER, -- производный инструмент
                                  v_DEPARTMENT   IN INTEGER, -- Филиал
                                  v_BROKER       IN INTEGER, -- Брокер
                                  v_ClientContr  IN INTEGER, -- Договор с клиентом
                                  v_Date         IN DATE,    -- Дата
                                  v_Summa        IN NUMBER
                                )
  IS
     ParentFI NUMBER;
  BEGIN
     ParentFI := DV_ParentFI( v_FIID );

     /*Т/О*/
     RSI_DV_CreateDemandTrust( v_ID_Operation,         /*ID_Operation*/
                           v_UserMarkMain,         /*UserMark*/
                           v_KindTOMain,           /*Role*/
                           '2'  /*оплата*/,        /*КВТО*/
                           '91' /*расчеты по СК*/, /*КУС*/
                           null /*ДР по опционам*/,/*КВДР*/
                           v_Date,                 /*Дплан*/
                           v_Summa,                /*Summa*/
                           ParentFI,               /*ActiveFIID*/
                           v_Party,                /*ActiveDepositoryID*/
                           v_ClientContr,          /*SFClientContr*/
                           v_FIID                  /*BaseFIID*/
                      );
  END;

  PROCEDURE DV_CreateDemandCommiss(  v_UserMarkMain IN VARCHAR2,
                                     v_KindTOMain   IN INTEGER,
                                     v_ID_Operation IN INTEGER,
                                     v_Party        IN INTEGER,
                                     v_FIID         IN INTEGER, -- производный инструмент
                                     v_DEPARTMENT   IN INTEGER, -- Филиал
                                     v_BROKER       IN INTEGER, -- Брокер
                                     v_ClientContr  IN INTEGER, -- Договор с клиентом
                                     v_Date         IN DATE,    -- Дата
                                     v_Summa        IN NUMBER,
                                     v_SummaFIID    IN NUMBER
                                   )
  IS
  BEGIN
     /*Т/О*/
     RSI_DV_CreateDemandTrust( v_ID_Operation,         /*ID_Operation*/
                           v_UserMarkMain,         /*UserMark*/
                           v_KindTOMain,           /*Role*/
                           '2'  /*оплата*/,        /*КВТО*/
                           '91' /*расчеты по СК*/, /*КУС*/
                           null /*ДР по опционам*/,/*КВДР*/
                           v_Date,                 /*Дплан*/
                           v_Summa,                /*Summa*/
                           v_SummaFIID,            /*ActiveFIID*/
                           v_Party,                /*ActiveDepositoryID*/
                           v_ClientContr,          /*SFClientContr*/
                           v_FIID                  /*BaseFIID*/
                      );
  END;

  /*Изменить суммы Т/О модуля ДУ*/
  PROCEDURE DV_ChangeDemandTurn
           (
              v_ID_Operation     IN NUMBER,  -- ID операции
              v_UserMark         IN VARCHAR2,-- польз. метка
              v_NewSumma         IN NUMBER   -- польз. метка
           )
  IS
  BEGIN
     UPDATE dtsdemand_dbt
        SET t_InitialQuantity = v_NewSumma,
            t_CurrentQuantity = v_NewSumma
      WHERE     t_BofficeKind  = 'Ю'
            AND t_ID_Operation = v_ID_Operation
            AND t_State        = TrustAPI.TS_DEMAND_STATE_PLAN
            AND t_UserMark LIKE v_UserMark ESCAPE '|';
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END; -- DV_ChangeDemandTurn

  /* Определить есть ли среди шагов операции шаг c заданным символом на дату*/
  FUNCTION DV_IsExistOperStep( v_DealID IN NUMBER, v_DocKind IN NUMBER, v_BranchSymbol IN CHAR, v_IsExecute IN CHAR, v_RepDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY') ) RETURN NUMBER
  IS
     CountStep NUMBER;
  BEGIN

     CountStep := 0;
     BEGIN
       SELECT COUNT(1)
         INTO CountStep
         FROM doprstep_dbt step, doproper_dbt oper
        WHERE oper.t_DocKind      = v_DocKind
          AND (CASE WHEN v_RepDate = TO_DATE('01.01.0001','DD.MM.YYYY') THEN TO_DATE('01.01.0001','DD.MM.YYYY') ELSE step.T_PLAN_DATE END) <= v_RepDate
          AND oper.t_DocumentID   = lpad(v_DealID, 34, '0')
          AND step.t_ID_Operation = oper.t_ID_Operation
          AND step.t_Symbol       = v_BranchSymbol
          AND step.t_IsExecute    = v_IsExecute;
     EXCEPTION
       WHEN NO_DATA_FOUND THEN CountStep := 0;
     END;

     RETURN CountStep;
  END;

  FUNCTION DV_DealIsReadyForKvit( v_DealID IN NUMBER, v_DocKind IN NUMBER, v_Purpose IN NUMBER , v_PaymentId IN NUMBER ) RETURN NUMBER
  IS
     v_dvkind NUMBER := 0;
     v_state NUMBER := 0;
     v_type NUMBER := 0;
     v_IsForKvit NUMBER := 0;
     v_csaFDId NUMBER := 0;
     v_account VARCHAR2(100);
     v_chapter NUMBER := 0;
     v_accFiid NUMBER := 0;
     v_countCashAcc NUMBER := 0;
     v_fikind NUMBER := 0;
  BEGIN
    SELECT fin.t_fi_kind
      INTO v_fikind
      FROM dpmpaym_dbt pmpaym, dfininstr_dbt fin
     WHERE     pmpaym.t_paymentid = v_PaymentId
           AND fin.t_fiid = pmpaym.t_fiid;
     if v_DocKind = 140 and v_fikind = RSI_RSB_FIInstr.FIKIND_CURRENCY  then
        if RSB_Derivatives.DV_IsExistOperStep(v_DealID,v_DocKind,'Т',chr(82)) > 0 then
        v_IsForKvit := 1;
        end if;
     elsif( (v_DocKind = 199 or v_DocKind = 4813 or v_DocKind = 4815 or v_DocKind = 4627) and (v_Purpose = 1 or v_Purpose = 2 or v_Purpose = 3 or v_Purpose = 4 or v_Purpose = 6 or
                                                                                               v_Purpose = 84 or v_Purpose = 86 or
                                                                                               v_Purpose = 70 or v_Purpose = 73 or v_Purpose = 71 or v_Purpose = 72 or v_Purpose = 41) )then
        if (v_DocKind = 4627) then
           begin
              SELECT CASE WHEN opr.T_COMPLETED = CHR (88) THEN 0 ELSE 1 END AS t_NotClosed, csapm.T_CSAID
                INTO v_state, v_csaFDId
                FROM DDVCSAPM_DBT csapm, DDVCSA_DBT csa, DOPROPER_DBT opr
               WHERE     csapm.T_CSAID = csa.T_CSAID
                     AND csapm.T_DIRECTION = 0
                     AND opr.t_DocumentID = csa.t_CSAID
                     AND opr.t_dockind = 4626
                     AND opr.t_kind_operation = 2795
                     AND csapm.t_PMID = v_DealID;
           exception
             WHEN NO_DATA_FOUND THEN NULL;
           end;
        else
           begin
              select t_dvkind, t_state, t_type
                into v_dvkind, v_state, v_type
                from ddvndeal_dbt
               where t_id = v_DealID;
           exception
             WHEN NO_DATA_FOUND THEN NULL;
           end;
        end if;
        if v_state = 1 then
          if (v_DocKind = 199 or v_DocKind = 4815) and (v_fikind = RSI_RSB_FIInstr.FIKIND_CURRENCY or (v_dvkind = DV_FORWARD and v_fikind = RSI_RSB_FIInstr.FIKIND_METAL and v_type = ALG_DV_BUY)) 
          then
             if (v_dvkind = DV_FORWARD or v_dvkind = DV_OPTION or v_dvkind = DV_FORWARD_T3)
                and ((v_Purpose <> 70 and RSB_Derivatives.DV_IsExistOperStep(v_DealID,v_DocKind,'Т',chr(82)) > 0)
                    or ((v_Purpose = 70 or v_Purpose = 6) and RSB_Derivatives.DV_IsExistOperStep(v_DealID,v_DocKind,'n',chr(82)) > 0))
             then
                v_IsForKvit := 1;
             elsif v_dvkind = DV_PCTSWAP
                   and (((v_purpose = 1 or v_purpose = 2) and RSB_Derivatives.DV_IsExistOperStep(v_DealID,v_DocKind,'и',chr(82)) > 0) or
                         ((v_Purpose = 73 or v_Purpose = 70) and RSB_Derivatives.DV_IsExistOperStep(v_DealID,v_DocKind,'n',chr(82)) > 0) or
                        ((v_purpose = 3 or v_purpose = 4) and (RSB_Derivatives.DV_IsExistOperStep(v_DealID,v_DocKind,'И',chr(88)) = 1) and (RSB_Derivatives.DV_IsExistOperStep(v_DealID,v_DocKind,'И',chr(82)) = 1))
                       )
             then
                v_IsForKvit := 1;
             elsif (v_dvkind = DV_CURSWAP or v_dvkind = DV_CURSWAP_FX)
                   and (((v_purpose = 1 or v_purpose = 2) and (RSB_Derivatives.DV_IsExistOperStep(v_DealID,v_DocKind,'т',chr(82)) > 0)) or
                        ((v_Purpose = 70) and RSB_Derivatives.DV_IsExistOperStep(v_DealID,v_DocKind,'n',chr(82)) > 0) or
                        ((v_purpose = 3 or v_purpose = 4) and (RSB_Derivatives.DV_IsExistOperStep(v_DealID,v_DocKind,'Т',chr(82)) > 0))
                       )
             then
                v_IsForKvit := 1;
             end if;
          elsif v_DocKind = 4813 and v_fikind = RSI_RSB_FIInstr.FIKIND_CURRENCY then
             if (v_dvkind = DV_BANKNOTE_FX) then
                --Не показываем плановые платежи банкнотной сделки, если БА или КА - кассовый
                if (((v_purpose = 1) and (v_type = ALG_DV_SALE)) or ((v_purpose = 2) and (v_type = ALG_DV_BUY))) then
                    SELECT t_ReceiverAccount, t_Chapter
                      INTO v_account, v_chapter
                      FROM dpmpaym_dbt
                     WHERE t_paymentid = v_PaymentId;
                elsif (((v_purpose = 2) and (v_type = ALG_DV_SALE)) or ((v_purpose = 1) and (v_type = ALG_DV_BUY))) then
                    SELECT t_PayerAccount, t_Chapter
                      INTO v_account, v_chapter
                      FROM dpmpaym_dbt
                     WHERE t_paymentid = v_PaymentId;
                end if;
                if (v_purpose = 1) then
                    SELECT t_FIID
                      INTO v_accFiid
                      FROM DDVNFI_DBT DVNFI
                     WHERE DVNFI.T_DEALID = v_DealID AND DVNFI.T_TYPE = DV_NFIType_BaseActiv;
                elsif (v_purpose = 2) then
                    SELECT t_PriceFIID
                      INTO v_accFiid
                      FROM DDVNFI_DBT DVNFI
                     WHERE DVNFI.T_DEALID = v_DealID AND DVNFI.T_TYPE = DV_NFIType_BaseActiv;
                end if;
                SELECT COUNT (1)
                into      v_countCashAcc
                  FROM DACCOUNT_DBT
                 WHERE     t_Chapter = v_chapter
                       AND t_Code_Currency = v_accFiid
                       AND t_Account = v_account
                       AND INSTR (t_type_account, CHR (128)) <> 0;
                if (v_countCashAcc = 0) then
                  v_IsForKvit := 1;
                end if;
             else
               if RSB_Derivatives.DV_IsExistOperStep(v_DealID,v_DocKind,'Т',chr(82)) > 0
               then
                  v_IsForKvit := 1;
               end if;
             end if;
          elsif v_DocKind = 4627 and v_fikind = RSI_RSB_FIInstr.FIKIND_CURRENCY then
             if ((RSB_Derivatives.DV_IsExistOperStep(v_csaFDId,4626,'Т',chr(82)) > 0) or (RSB_Derivatives.DV_IsExistOperStep(v_csaFDId,4626,'Ф',chr(82)) > 0))
             then
                v_IsForKvit := 1;
             end if;
          end if;
        end if;
     elsif (v_DocKind = 4632 and v_fikind = RSI_RSB_FIInstr.FIKIND_CURRENCY) then
         if RSB_Derivatives.DV_IsExistOperStep(v_DealID,v_DocKind,'Р',chr(82)) > 0 or RSB_Derivatives.DV_IsExistOperStep(v_DealID,v_DocKind,'З',chr(82)) > 0
         then
        v_IsForKvit := 1;
     end if;
     end if;
     RETURN v_IsForKvit;
  END;

  -- Отобрать сделки в DDVFVOVER_TMP для проведения сервисной операции переоценки СС
  PROCEDURE DV_SelectNDealForOverFrVal( ServDocID IN NUMBER )
  IS
  BEGIN

     INSERT INTO DDVFVOVER_TMP( t_DealID, t_Code, t_DVKind, t_Forvard, t_FIID, t_OldFairValue, t_FairValue, t_NotCourse,
                                t_SetDemand, t_Demand, t_SetLiability, t_Liability, t_SetDemand2, t_Demand2, t_SetLiability2, t_Liability2, T_DOCKIND)
     SELECT distinct D.t_ID, D.t_Code, D.t_DVKind, D.t_Forvard, F.t_FIID,
                     /* остальное заполним в макросе */
                     0, 0, chr(0), chr(0), 0, chr(0), 0, chr(0), 0, chr(0), 0, D.T_DOCKIND
       FROM ddvndeal_dbt D, ddvnfi_dbt F, ddvnfi_dbt F0, ddvoper_dbt SrvOp, dfininstr_dbt finF
      WHERE SrvOp.t_ID        = ServDocID
        AND D.t_Department    = SrvOp.t_Department
        AND D.t_Date          <= SrvOp.t_Date
        AND D.t_IsTrust       = chr(0)
        AND ( (D.T_Marketkind    != 2) OR (D.T_DVKIND = 6 and D.T_DOCKIND = 199) ) /*Или значение поля Биржевой Рынок != Валютный или операция СВОП*/
        AND D.t_Client        = -1     /* UnknownParty */
        AND D.t_State         = DVDEAL_STATE_OPEN      /* Открыт */
        AND D.t_DVKind        = (CASE WHEN SrvOp.t_DVKind > 0 THEN SrvOp.t_DVKind ELSE D.t_DVKind END)
        AND D.t_DocKind       in(199,4815)
        AND ( D.t_IsPFI = 'X' or
              NOT EXISTS ( SELECT *
                             FROM DPARTYOWN_DBT
                            WHERE T_PARTYID   = D.t_Contractor
                              AND T_PARTYKIND = 3 -- PTK_MARKETPLASE
                         )
            )
        AND (DV_IsExistOperStep( D.t_ID, D.T_DOCKIND, 'Ф', chr(0) ) > 0 or DV_IsExistOperStep( D.t_ID, D.T_DOCKIND, 'Ф', chr(88) ) > 0)
        AND DV_IsExistOperStep( D.t_ID, D.T_DOCKIND, 'i', chr(88) ) <= 0
        AND DV_IsExistOperStep( D.t_ID, D.T_DOCKIND, 'Б', chr(88) ) <= 0
        AND DV_IsExistOperStep( D.t_ID, D.T_DOCKIND, 'b', chr(88) ) <= 0
        AND (DV_IsExistOperStep( D.t_ID, D.T_DOCKIND, 'и', chr(88) ) <= 0 OR
             DV_IsExistOperStep( D.t_ID, D.T_DOCKIND, 'И', chr(82) ) > 0  OR
         DV_IsExistOperStep( D.t_ID, D.T_DOCKIND, 'п', chr(82) ) > 0 /*CHVA*/)
        AND F.t_DealID        = D.t_ID
        AND F.t_Type          = (CASE WHEN D.t_Forvard = chr(0) THEN DV_NFIType_BaseActiv ELSE DV_NFIType_Forward END)
        AND F.t_FIID          = (CASE WHEN SrvOp.t_FIID > 0 THEN SrvOp.t_FIID ELSE F.t_FIID END)
        AND finF.t_FIID       = F.t_FIID
        AND finF.t_FI_Kind    = (CASE WHEN SrvOp.t_FI_Kind > 0 THEN SrvOp.t_FI_Kind ELSE finF.t_FI_Kind END)
        AND F0.t_DealID       = D.t_ID
        AND F0.t_Type         = DV_NFIType_BaseActiv
        AND F0.t_FairValueAlg > 0
        AND NOT EXISTS ( SELECT operps.t_OperID
                         FROM ddvoperps_dbt operps
                         WHERE operps.t_DocKind = 204 /*DL_DVOPER_OVERFRVAL*/
                         AND operps.t_DocID   = D.t_ID
                         AND operps.t_OperID  = SrvOp.t_ID
                       )
        AND NOT EXISTS ( select t_id
                         from ddvnfrval_dbt
                         where t_dealid = D.t_ID
                         AND t_Dockind = D.t_DocKind
                         AND  t_date >= SrvOp.t_Date
                       ) ;

  EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
  END;

  -- Отобрать сделки по сервисной операции в DDVFVOVER_TMP
  PROCEDURE DV_SelectNDealIncomOverFrVal( ServDocID IN NUMBER )
  IS
  BEGIN

     INSERT INTO DDVFVOVER_TMP( t_DealID, t_Code, t_DVKind, t_Forvard, t_FIID, t_OldFairValue, t_FairValue, t_NotCourse, T_DOCKIND )
     SELECT distinct D.t_ID, D.t_Code, D.t_DVKind, D.t_Forvard, F.t_FIID, V.t_ID/* правильно заполним в макросе*/, V.t_FairValue, (CASE WHEN operps.t_Flag = 1 THEN chr(88) ELSE chr(0) END), D.T_DOCKIND
       FROM ddvnfrval_dbt V, ddvndeal_dbt D, ddvnfi_dbt F, ddvoperps_dbt operps
      WHERE operps.t_OperID  = ServDocID
        AND operps.t_DocKind = 205 /*DL_DVHIST_CHANGE_FRVAL*/
        AND V.t_ID           = operps.t_DocID
        AND D.t_ID           = V.t_DealID
        AND D.T_DOCKIND  = V.T_DOCKIND
        AND F.t_DealID       = D.t_ID
        AND F.t_Type         = (CASE WHEN D.t_Forvard = chr(0) THEN DV_NFIType_BaseActiv ELSE DV_NFIType_Forward END);

  EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
  END;

  -- количество дней в году по базису
  FUNCTION DV_DaysInYearByBasis( basis in int,  -- базис
                                 d     in date  -- дата с искомым годом
                               ) RETURN int
  AS
     y date;
  BEGIN
     IF( basis = cnst.BASIS_ACTACT ) THEN -- act/act
        y := trunc(d, 'year');
        RETURN add_months(y, 12) - y;
     ELSIF( basis = cnst.BASIS_ACT365 ) THEN -- 365/act
        RETURN 365;
     ElSE  -- 360/30, 360/act, 360/31
        RETURN 360;
     END IF;
  END DV_DaysInYearByBasis;

  -- количество дней в месяце по базису
  FUNCTION DV_DaysInMonthByBasis( basis in int,  -- базис
                                  d     in date  -- дата с искомым месяцем
                                ) RETURN int
  AS
     m date;
  BEGIN
     if( (basis = cnst.BASIS_ACTACT) or
         (basis = cnst.BASIS_ACT360) or
         (basis = cnst.BASIS_ACT365) or
         (basis = cnst.BASIS_ACT360NOLEAP) )
     then
        m := trunc(d, 'month');
        return add_months(m, 1) - m;
     else
        return 30;
     end if;
  END DV_DaysInMonthByBasis;

  -- количество дней в периоде по базису
  FUNCTION DV_DaysInPeriodByBasis( basis      in int,  -- базис
                                   d          in date, -- дата начала периода
                                   Period     in int,  -- период
                                   PeriodKind in int   -- вид периода
                                 ) RETURN int
  AS
     RetVal int;
     d_new  DATE;
  BEGIN

     RetVal := 0;

     IF( PeriodKind = ALG_DV_PERIODKIND_PRRATE_DAY ) THEN
        RetVal := Period;
     ELSIF( PeriodKind = ALG_DV_PERIODKIND_PRRATE_MONTH ) THEN
        FOR i IN 1..Period LOOP
           RetVal := RetVal + DV_DaysInMonthByBasis(basis, add_months(d, i-1));
        END LOOP;
     ELSIF( PeriodKind = ALG_DV_PERIODKIND_PRRATE_YEAR ) THEN
        IF( basis = cnst.BASIS_ACTACT ) THEN -- act/act
           d_new  := add_months(d, 12*Period);
           RetVal := RetVal + (d_new - d);
        ELSE
           FOR i IN 1..Period LOOP
              RetVal := RetVal + DV_DaysInYearByBasis(basis, add_months(d, 12*(i-1)));
           END LOOP;
        END IF;
     END IF;

     return RetVal;
  END DV_DaysInPeriodByBasis;

  -- количество дней в периоде
  FUNCTION RSI_DV_DaysInPeriod( basis      IN NUMBER,                                          -- базис
                                d          IN DATE,                                            -- дата начала периода
                                Period     IN NUMBER,                                          -- период
                                PeriodKind IN NUMBER,                                          -- вид периода
                                LastDay    IN NUMBER,                                          -- начало первого периода последний день ?
                                dDay       IN NUMBER,                                          -- день начала первого периода
                                ExecDate   IN DATE DEFAULT TO_DATE('01.01.0001', 'DD.MM.YYYY') -- Дата выполнения
                              ) RETURN int
  AS
     RetVal   NUMBER;
     d_new    DATE;
  BEGIN
     RetVal := 0;

     IF( PeriodKind = ALG_DV_PERIODKIND_PRRATE_DAY ) THEN
        RetVal := Period;
     ELSIF( PeriodKind = ALG_DV_PERIODKIND_PRRATE_MONTH ) THEN
        d_new  := add_months(d, Period); -- сама учитывает последний ли это день месяца
        IF( LastDay = 0 ) THEN
           d_new := LAST_DAY(d_new);
           IF( EXTRACT(DAY FROM d_new) > dDay ) THEN
              d_new := TO_DATE(TO_CHAR(dDay) || '.' || TO_CHAR(d_new, 'MM') || '.' || TO_CHAR(d_new, 'YYYY'), 'DD.MM.YYYY');
           END IF;
        END IF;
        RetVal := RetVal + (d_new - d);
     ELSIF( PeriodKind = ALG_DV_PERIODKIND_PRRATE_YEAR ) THEN
        d_new  := add_months(d, 12*Period); -- сама учитывает последний ли это день месяца
        IF( LastDay = 0 ) THEN
           d_new := LAST_DAY(d_new);
           IF( EXTRACT(DAY FROM d_new) > dDay ) THEN
              d_new := TO_DATE(TO_CHAR(dDay) || '.' || TO_CHAR(d_new, 'MM') || '.' || TO_CHAR(d_new, 'YYYY'), 'DD.MM.YYYY');
           END IF;
        END IF;
        RetVal := RetVal + (d_new - d);
     ELSIF (PeriodKind = ALG_DV_PERIODKIND_PRRATE_PERIOD) THEN
        RetVal := ExecDate - d;
     END IF;

     return RetVal;
  END RSI_DV_DaysInPeriod;

  -- переменное количество дней в месяце
  -- аналог K_VarMonth из pcidc.c
  FUNCTION DV_VarMonth( start_dt    in DATE,    -- начало периода начисления
                        end_dt      in DATE,    -- окончние периода начисления
                        basis       in NUMBER   -- базис ставки (cnst.BASIS_***)
                      ) RETURN number
  AS
     l_start_dt date := start_dt + 1; -- исключение начальной даты
  BEGIN
     if( trunc(l_start_dt, 'year') = trunc(end_dt, 'year') ) then
        return (trunc(end_dt) - trunc(l_start_dt) + 1) / DV_DaysInYearByBasis(basis, start_dt);
     else
        return (trunc(end_dt) - trunc(end_dt, 'year') + 1) / DV_DaysInYearByBasis(basis, end_dt) +
               (add_months(trunc(l_start_dt, 'year'), 12) - trunc(l_start_dt)) / DV_DaysInYearByBasis(basis, l_start_dt) +
               extract(year from end_dt) - extract(year from l_start_dt) - 1;
     end if;
  END DV_VarMonth;

  -- постоянное количество дней в месяце
  -- аналог K_ConstMonth из pcidc.c
  FUNCTION DV_ConstMonth( start_dt    in DATE,    -- начало периода начисления
                          end_dt      in DATE,    -- окончние периода начисления
                          basis       in NUMBER   -- базис ставки (cnst.BASIS_***)
                        ) RETURN number
  AS
     dim         number := DV_DaysInMonthByBasis(basis, start_dt);
     l_start_dt  date   := start_dt + 1; -- исключение начальной даты
     start_day   number := extract(day   from l_start_dt);
     start_mon   number := extract(month from l_start_dt);
     start_year  number := extract(year  from l_start_dt);
     end_day     number := extract(day   from end_dt);
     end_mon     number := extract(month from end_dt);
     end_year    number := extract(year  from end_dt);
     res number;
  BEGIN
     start_day   := least(start_day, dim);
     end_day     := least(end_day, dim);

     if( trunc(l_start_dt, 'year') = trunc(end_dt, 'year') ) then
        res := (end_day - start_day + 1 + (end_mon - start_mon) * dim) / DV_DaysInYearByBasis(basis, l_start_dt);
     else
        res := (end_day + (end_mon - 1) * dim) / DV_DaysInYearByBasis(basis, end_dt) +
               (dim - start_day + 1 + (12 - start_mon) * dim) / DV_DaysInYearByBasis(basis, l_start_dt) +
               end_year - start_year - 1;
     end if;

     if( res > 0 ) then
        return res;
     else
        return 0;
     end if;
  END DV_ConstMonth;

  -- количество лет по базису
  -- аналог K_Universal из pcidc.c
  FUNCTION DV_Years( start_dt    in DATE,    -- начало периода начисления
                     end_dt      in DATE,    -- окончние периода начисления
                     basis       in NUMBER   -- базис ставки (cnst.BASIS_***)
                   ) RETURN number
  AS
  BEGIN
     if( (basis = cnst.BASIS_ACTACT) or
         (basis = cnst.BASIS_ACT360) or
         (basis = cnst.BASIS_ACT365) )
     then
        return DV_VarMonth(start_dt, end_dt, basis);
     elsif( (basis = cnst.BASIS_30360) or
            (basis = cnst.BASIS_31360) )
     then
        return DV_ConstMonth(start_dt, end_dt, basis);
     else
        raise_application_error(-20001, 'DV_Years: unknown basis');
     end if;

  END DV_Years;

  -- Процедура удаления графика платежей
  PROCEDURE RSI_DV_DeletePMGR( pDealID IN NUMBER, pSide IN NUMBER )
  IS
  BEGIN
     DELETE FROM DDVNPMGR_DBT PMGR
      WHERE PMGR.T_DEALID = pDealID
        AND PMGR.T_SIDE   = pSide
        AND NOT EXISTS( SELECT DVNACDL.T_ID
                          FROM DDVNACDL_DBT DVNACDL, DDVNFI_DBT DVNFI
                         WHERE DVNFI.T_DEALID = PMGR.T_DEALID
                           AND DVNFI.T_TYPE = DV_NFIType_BaseActiv
                           AND DVNACDL.T_PAYKIND = 70
                           AND DVNACDL.T_DATE = PMGR.T_PAYDATE
                           AND DVNACDL.T_DEALID = PMGR.T_DEALID
                           AND DVNACDL.T_DIRECTION = (CASE WHEN DVNFI.T_EXECTYPE = 1 THEN PMGR.T_SIDE ELSE DVNACDL.T_DIRECTION END)
                      );
  END;


  PROCEDURE RSI_DV_DeletePMGR_TMP( pDealID IN NUMBER, pSide IN NUMBER )
  IS
  BEGIN
     DELETE FROM DDVNPMGR_TMP PMGR
      WHERE PMGR.T_DEALID = pDealID
        AND PMGR.T_SIDE   = pSide
        AND NOT EXISTS( SELECT DVNACDL.T_ID
                          FROM DDVNACDL_DBT DVNACDL, DDVNFI_DBT DVNFI
                         WHERE DVNFI.T_DEALID = PMGR.T_DEALID
                           AND DVNFI.T_TYPE = DV_NFIType_BaseActiv
                           AND DVNACDL.T_PAYKIND = 70
                           AND DVNACDL.T_DATE = PMGR.T_PAYDATE
                           AND DVNACDL.T_DEALID = PMGR.T_DEALID
                           AND DVNACDL.T_DIRECTION = (CASE WHEN DVNFI.T_EXECTYPE = 1 THEN PMGR.T_SIDE ELSE DVNACDL.T_DIRECTION END)
                      );
  END;

  -- Функция получения даты платежа с корректировкой по календарям
  FUNCTION RSI_DV_GetPayDate( pDealID    IN NUMBER,  -- Сделка
                              pFIID      IN NUMBER,  -- Актив
                              pSide      IN NUMBER,  -- Сторона
                              pTypeCalc  IN NUMBER,  -- Тип расчета
                              pEndDate   IN DATE     -- Дата окончания
                            ) RETURN DATE
  AS
     v_PayDate   DATE := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     v_DateTemp  DATE := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     v_IsWorkDay NUMBER := 0;
     v_Count     NUMBER := 0;
     v_isSector  CHAR := chr(0);
     v_Kind      NUMBER := 0;
     v_Contractor NUMBER := 0;
     v_MarketKind NUMBER(5) := 0;
     v_CalendarId NUMBER(10) := 0;
     v_calparamarr RSI_DlCalendars.calparamarr_t;
  BEGIN
     select T_SECTOR, T_KIND, T_CONTRACTOR, t_MarketKind into v_isSector, v_Kind, v_Contractor, v_MarketKind from DDVNDEAL_DBT where T_ID = pDealID;

     v_calparamarr('Object') := RSI_DlCalendars.DL_GetOperNameByKind(v_Kind);
     v_calparamarr('ObjectType') := CASE WHEN v_isSector = chr(88) THEN RSI_DlCalendars.DL_CALLNK_MARKET ELSE RSI_DlCalendars.DL_CALLNK_OUTMARKET END;
     if v_isSector = chr(88) then
       v_calparamarr('Market') := v_Contractor;
     end if;
     v_calparamarr('MarketPlace') := CASE 
         WHEN v_MarketKind = RSB_SECUR.DV_MARKETKIND_SPFIMARKET THEN RSI_DlCalendars.DL_CALLNK_MARKETPLACE_SPFI
         ELSE RSI_DlCalendars.DL_CALLNK_MARKETPLACE_CUR
     END;
     v_calparamarr('Currency') := pFIID;
     
     v_CalendarId :=  RSI_DlCalendars.DL_GetCalendByDynParam(158, v_calparamarr);

     v_PayDate   := pEndDate;
     v_IsWorkDay := 0; -- Нет
     WHILE( v_IsWorkDay = 0 ) LOOP
        v_IsWorkDay := 1; -- Да

        FOR CalKind IN (SELECT K.t_CalKindID as t_CalKindID
                           FROM DDVNCALKIND_DBT K
                          WHERE K.T_DEALID = pDealID
                            AND K.T_FIID   = pFIID
                            AND K.T_SIDE   in(pSide, ALG_DV_PMGR_SIDE_UNDEF))
        LOOP
           IF( RSI_RsbCalendar.IsWorkDay(v_PayDate, v_CalendarId) = 0 ) THEN -- выходной
              IF( pTypeCalc = ALG_DV_TYPECALC_FOLLOWING ) THEN
                 v_PayDate := RSI_RsbCalendar.GetDateAfterWorkDay(v_PayDate, 0, v_CalendarId);
              ELSIF( pTypeCalc = ALG_DV_TYPECALC_MODIFIED ) THEN
                 v_DateTemp := RSI_RsbCalendar.GetDateAfterWorkDay(v_PayDate, 0, v_CalendarId);

                 v_Count := 0;
                 SELECT COUNT(1) INTO v_Count
                   FROM dual
                  WHERE EXTRACT(MONTH FROM v_PayDate) = EXTRACT(MONTH FROM v_DateTemp);

                 IF( v_Count = 0 ) THEN -- следующий рабочий в другом месяце
                    v_PayDate := RSI_RsbCalendar.GetDateAfterWorkDay(v_PayDate, -1, v_CalendarId);
                 ELSE
                    v_PayDate := v_DateTemp;
                 END IF;
              ELSIF( pTypeCalc = ALG_DV_TYPECALC_PRECEDING ) THEN
                 v_PayDate := RSI_RsbCalendar.GetDateAfterWorkDay(v_PayDate, -1, v_CalendarId);
              END IF;

              v_IsWorkDay := 0; -- Нет
           END IF;
        END LOOP;
     END LOOP;

     RETURN v_PayDate;
  END RSI_DV_GetPayDate;

  -- Функция получения даты фиксации с корректировкой по календарям
  FUNCTION RSI_DV_GetFixDate( pDealID    IN NUMBER,  -- Сделка
                              pFIID      IN NUMBER,  -- Актив
                              pSide      IN NUMBER,  -- Сторона
                              pTypeCalc  IN NUMBER,  -- Тип расчета
                              pBegDate   IN DATE,    -- Дата начала
                              pFixDays   IN NUMBER,  -- Дней до даты фиксации
                              pCalKindID IN NUMBER   -- Календарь из параметров стороны СВОПа
                            ) RETURN DATE
  AS
     v_FixDate   DATE := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     v_DateTemp  DATE := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     v_d         NUMBER := 0;
     v_Count     NUMBER := 0;
  BEGIN

        v_FixDate := pBegDate;
        v_d := 0;
        IF( pFixDays != 0 ) THEN
           WHILE( v_d < abs(pFixDays) ) LOOP
              IF( pFixDays > 0 ) THEN
                 v_FixDate := v_FixDate + 1;
              ELSIF( pFixDays < 0 ) THEN
                 v_FixDate := v_FixDate - 1;
              END IF;

              IF( RSI_RsbCalendar.IsWorkDay(v_FixDate, pCalKindID) != 0 ) THEN -- рабочий
                 v_d := v_d + 1;
              END IF;
           END LOOP;
        ELSE -- pFixDays = 0
           IF( RSI_RsbCalendar.IsWorkDay(v_FixDate, pCalKindID) = 0 ) THEN -- выходной
              IF( pTypeCalc = ALG_DV_TYPECALC_FOLLOWING ) THEN
                 v_FixDate := RSI_RsbCalendar.GetDateAfterWorkDay(v_FixDate, 0, pCalKindID);
              ELSIF( pTypeCalc = ALG_DV_TYPECALC_MODIFIED ) THEN
                 v_DateTemp := RSI_RsbCalendar.GetDateAfterWorkDay(v_FixDate, 0, pCalKindID);

                 v_Count := 0;
                 SELECT COUNT(1) INTO v_Count
                   FROM dual
                  WHERE EXTRACT(MONTH FROM v_FixDate) = EXTRACT(MONTH FROM v_DateTemp);

                 IF( v_Count = 0 ) THEN -- следующий рабочий в другом месяце
                    v_FixDate := RSI_RsbCalendar.GetDateAfterWorkDay(v_FixDate, -1, pCalKindID);
                 ELSE
                    v_FixDate := v_DateTemp;
                 END IF;
              ELSIF( pTypeCalc = ALG_DV_TYPECALC_PRECEDING ) THEN
                 v_FixDate := RSI_RsbCalendar.GetDateAfterWorkDay(v_FixDate, -1, pCalKindID);
              END IF;
           END IF;
        END IF;

     RETURN v_FixDate;
  END RSI_DV_GetFixDate;

  -- Процедура обновления дат в строках графика (при условии что "корректировка даты окончания" == Нет)
  PROCEDURE RSI_DV_UpdatePMGR_Dates( pID      IN NUMBER,   -- Строка графика платежей
                                     pDealID  IN NUMBER,   -- Сделка
                                     pSide    IN NUMBER,   -- Сторона
                                     pPayDate IN DATE,     -- Дата платежа
                                     pBegDate IN OUT DATE, -- Дата начала
                                     pEndDate IN OUT DATE  -- Дата окончания
                                   )
  IS
     v_N   NUMBER := 0;
     v_PM  DDVNPMGR_DBT%ROWTYPE;
  BEGIN
     SELECT count(1) INTO v_N
       FROM DDVNPMGR_DBT
      WHERE t_DealID  = pDealID
        AND t_Side    = pSide
        AND t_PayDate = pPayDate;

     IF( v_N > 0 ) THEN
        IF( v_N > 1 ) THEN
           RAISE_APPLICATION_ERROR(-20612,''); -- В графике платежей по сделке обнаружено более одного платежа в одну дату
        ELSE
           BEGIN
              SELECT * INTO v_PM
                FROM DDVNPMGR_DBT
               WHERE t_DealID  = pDealID
                 AND t_Side    = pSide
                 AND t_PayDate = pPayDate;
              EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
           END;

           pBegDate := least(pBegDate, v_PM.t_BegDate);
           pEndDate := greatest(pEndDate, v_PM.t_EndDate);

           -- объединение записей с одинаковыми датами платежей
           IF( pID != v_PM.T_ID ) THEN
              DELETE FROM DDVNPMGR_DBT
               WHERE t_ID = v_PM.t_ID;
           END IF;

           IF( pID > 0 ) THEN
              UPDATE DDVNPMGR_DBT
                 SET t_BegDate = pBegDate,
                     t_EndDate = pEndDate,
                     t_PayDate = pPayDate
               WHERE t_ID = pID;
           END IF;
        END IF;
     ELSE
        IF( pID > 0 ) THEN
           UPDATE DDVNPMGR_DBT
              SET t_PayDate = pPayDate
            WHERE t_ID = pID;
        END IF;
     END IF;
  END;

  PROCEDURE RSI_DV_UpdatePMGR_Dates_TMP( pID      IN NUMBER,   -- Строка графика платежей
                                         pDealID  IN NUMBER,   -- Сделка
                                         pSide    IN NUMBER,   -- Сторона
                                         pPayDate IN DATE,     -- Дата платежа
                                         pBegDate IN OUT DATE, -- Дата начала
                                         pEndDate IN OUT DATE  -- Дата окончания
                                   )
  IS
     v_N   NUMBER := 0;
     v_PM  DDVNPMGR_TMP%ROWTYPE;
  BEGIN
     SELECT count(1) INTO v_N
       FROM DDVNPMGR_TMP
      WHERE t_DealID  = pDealID
        AND t_Side    = pSide
        AND t_PayDate = pPayDate;

     IF( v_N > 0 ) THEN
        IF( v_N > 1 ) THEN
           RAISE_APPLICATION_ERROR(-20612,''); -- В графике платежей по сделке обнаружено более одного платежа в одну дату
        ELSE
           BEGIN
              SELECT * INTO v_PM
                FROM DDVNPMGR_TMP
               WHERE t_DealID  = pDealID
                 AND t_Side    = pSide
                 AND t_PayDate = pPayDate;
              EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
           END;

           pBegDate := least(pBegDate, v_PM.t_BegDate);
           pEndDate := greatest(pEndDate, v_PM.t_EndDate);

           -- объединение записей с одинаковыми датами платежей
           IF( pID != v_PM.T_ID ) THEN
              DELETE FROM DDVNPMGR_TMP
               WHERE t_ID = v_PM.t_ID;
           END IF;

           IF( pID > 0 ) THEN
              UPDATE DDVNPMGR_TMP
                 SET t_BegDate = pBegDate,
                     t_EndDate = pEndDate,
                     t_PayDate = pPayDate
               WHERE t_ID = pID;
           END IF;
        END IF;
     ELSE
        IF( pID > 0 ) THEN
           UPDATE DDVNPMGR_TMP
              SET t_PayDate = pPayDate
            WHERE t_ID = pID;
        END IF;
     END IF;
  END;

  -- Процедура обновления и проверки дат в строках графика - даты окончания в предыдущей строке и даты начала в следующей.
  PROCEDURE RSI_DV_SetPMGR_Dates_prev_next( pMode    IN  NUMBER, -- Режим работы процедуры, возможные значения - (1 - проверка, 2 - расчет)
                                            pID      IN  NUMBER, -- Текущая строка графика платежей
                                            pBegDate IN  DATE,   -- Новая дата начала в текущей строке графика
                                            pEndDate IN  DATE,   -- Новая дата окончания в текущей строке графика
                                            pWarning OUT NUMBER  -- Код предупреждения
                                          )
  IS
     v_P_cur  DDVNPMGR_DBT%ROWTYPE;
     v_P_prev DDVNPMGR_DBT%ROWTYPE;
     v_P_next DDVNPMGR_DBT%ROWTYPE;
     v_D      DDVNDEAL_DBT%ROWTYPE;
     v_F      DDVNFI_DBT%ROWTYPE;
  BEGIN
     pWarning := 0;

     -- Получить буфер текущей строки графика до изменения
     BEGIN
        SELECT * INTO v_P_cur
          FROM DDVNPMGR_DBT
         WHERE t_ID = pID;
        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
     END;

     -- Получить буфер сделки
     BEGIN
        SELECT * INTO v_D
          FROM DDVNDEAL_DBT
         WHERE t_ID = v_P_cur.t_DealID;
        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
     END;

     IF( (pBegDate != v_P_cur.t_BegDate) and (v_D.t_CorrectDate = chr(88)) ) THEN -- изменена дата начала
        -- предыдущая запись
        BEGIN
           SELECT * INTO v_P_prev
             FROM DDVNPMGR_DBT
            WHERE T_DEALID  = v_P_cur.t_DealID
              AND T_SIDE    = v_P_cur.t_Side
              AND T_ENDDATE = ( SELECT max(pmgr.T_ENDDATE)
                                  FROM DDVNPMGR_DBT pmgr
                                 WHERE pmgr.T_DEALID   = v_P_cur.t_DealID
                                   AND pmgr.T_SIDE     = v_P_cur.t_Side
                                   AND pmgr.T_ENDDATE <= v_P_cur.t_BegDate );
           EXCEPTION WHEN OTHERS THEN NULL;
        END;

        IF( v_P_prev.T_ID > 0 ) THEN
           IF( v_P_prev.T_BEGDATE = pBegDate ) THEN
              IF( pMode = 1 ) THEN -- Проверка
                 pWarning := 1; -- В предыдущей строке графика дата начала периода должна быть меньше даты окончания, эта строка графика будет удалена. Продолжить редактирование?
              ELSIF( pMode = 2 ) THEN -- Расчет
                 DELETE FROM DDVNPMGR_DBT
                  WHERE t_ID = v_P_prev.t_ID;
              END IF;
           ELSIF( v_P_prev.T_BEGDATE > pBegDate ) THEN
              RAISE_APPLICATION_ERROR(-20613, ''); -- В предыдущей строке графика дата начала периода больше даты окончания
           ELSE
              IF( pMode = 2 ) THEN -- Расчет
                 BEGIN
                   SELECT * INTO v_F
                     FROM DDVNFI_DBT
                    WHERE T_DEALID = v_P_cur.T_DEALID
                      AND T_TYPE   = (CASE WHEN v_P_cur.t_Side = ALG_DV_PMGR_SIDE_DEMAND THEN DV_NFIType_BaseActiv2 ELSE DV_NFIType_BaseActiv END);
                 EXCEPTION
                    WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
                 END;

                 IF( (v_F.T_EXECTYPE = DVSETTLEMET_CALC) AND (v_D.t_Type = ALG_DV_FIX_FLOAT) ) THEN
                    BEGIN
                      SELECT * INTO v_F
                        FROM DDVNFI_DBT
                       WHERE T_DEALID = v_P_cur.T_DEALID
                         AND T_TYPE   = DV_NFIType_BaseActiv2;
                    EXCEPTION
                       WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
                    END;
                 END IF;

                 UPDATE DDVNPMGR_DBT
                    SET T_ENDDATE = pBegDate,
                        T_PAYDATE = RSI_DV_GetPayDate(v_P_cur.T_DEALID, v_F.T_FIID, v_P_cur.T_SIDE, v_F.t_TypeCalc, pBegDate)
                  WHERE T_ID = v_P_prev.T_ID;
              END IF;
           END IF;
        END IF;
     END IF;

     IF( pEndDate != v_P_cur.t_EndDate ) THEN -- изменена дата окончания
        -- следующая запись
        BEGIN
           SELECT * INTO v_P_next
             FROM DDVNPMGR_DBT
            WHERE T_DEALID  = v_P_cur.t_DealID
              AND T_SIDE    = v_P_cur.t_Side
              AND T_BEGDATE = ( SELECT min(pmgr.T_BEGDATE)
                                  FROM DDVNPMGR_DBT pmgr
                                 WHERE pmgr.T_DEALID   = v_P_cur.t_DealID
                                   AND pmgr.T_SIDE     = v_P_cur.t_Side
                                   AND pmgr.T_BEGDATE >= v_P_cur.t_EndDate );
           EXCEPTION WHEN OTHERS THEN NULL;
        END;

        IF( v_P_next.T_ID > 0 ) THEN
           IF( v_P_next.T_ENDDATE = pEndDate ) THEN
              IF( pMode = 1 ) THEN -- Проверка
                 pWarning := 2; -- В следующей строке графика дата начала периода должна быть меньше даты окончания, эта строка графика будет удалена. Продолжить редактирование?
              ELSIF( pMode = 2 ) THEN -- Расчет
                 DELETE FROM DDVNPMGR_DBT
                  WHERE t_ID = v_P_next.t_ID;
              END IF;
           ELSIF( v_P_next.T_ENDDATE < pEndDate ) THEN
              RAISE_APPLICATION_ERROR(-20614, ''); -- В следующей строке графика дата начала периода больше даты окончания
           ELSE
              IF( pMode = 2 ) THEN -- Расчет
                 BEGIN
                   SELECT * INTO v_F
                     FROM DDVNFI_DBT
                    WHERE T_DEALID = v_P_cur.T_DEALID
                      AND T_TYPE   = (CASE WHEN v_P_cur.t_Side = ALG_DV_PMGR_SIDE_DEMAND THEN DV_NFIType_BaseActiv2 ELSE DV_NFIType_BaseActiv END);
                 EXCEPTION
                    WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
                 END;

                 IF( (v_F.T_EXECTYPE = DVSETTLEMET_CALC) AND (v_D.t_Type = ALG_DV_FIX_FLOAT) ) THEN
                    BEGIN
                      SELECT * INTO v_F
                        FROM DDVNFI_DBT
                       WHERE T_DEALID = v_P_cur.T_DEALID
                         AND T_TYPE   = DV_NFIType_BaseActiv2;
                    EXCEPTION
                       WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
                    END;
                 END IF;

                 UPDATE DDVNPMGR_DBT
                    SET T_BEGDATE = pEndDate,
                        T_FIXDATE = RSI_DV_GetFixDate(v_P_cur.T_DEALID, v_F.T_FIID, v_P_cur.T_SIDE, v_F.t_TypeCalc, pEndDate, v_F.T_FIXDAYS, v_F.t_CalKindID)
                  WHERE T_ID = v_P_next.T_ID;
              END IF;
           END IF;
        END IF;
     END IF;
  END;

  -- Процедура построения графика платежей
  PROCEDURE DV_CreatePMGR( pD IN DDVNDEAL_DBT%ROWTYPE, pF IN DDVNFI_DBT%ROWTYPE, pSide IN NUMBER )
  IS
     v_GR             DDVNPMGR_DBT%ROWTYPE;
     v_DATE           DATE := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     v_NextBegDate    DATE := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     v_CorrectPayDate NUMBER := 0;
     v_LastDay        NUMBER := 0;
     v_Day            NUMBER := 0;
  BEGIN
     RSI_DV_DeletePMGR(pD.T_ID, pSide);

     BEGIN
        SELECT NVL(MAX(T_ENDDATE), TO_DATE('01.01.0001', 'dd.mm.yyyy')) INTO v_DATE
          FROM DDVNPMGR_DBT PMGR
         WHERE PMGR.T_DEALID = pD.T_ID
           AND PMGR.T_SIDE   = pSide;

        EXCEPTION WHEN NO_DATA_FOUND THEN v_GR.T_BEGDATE := pD.T_BEGINDATE;
     END;

     IF (v_DATE != TO_DATE('01.01.0001', 'dd.mm.yyyy')) THEN
        v_GR.T_BEGDATE := v_DATE;
     ELSE
        v_GR.T_BEGDATE := pD.T_BEGINDATE;
     END IF;

     v_NextBegDate := v_GR.T_BEGDATE;

     v_GR.T_ID               := 0;
     v_GR.T_DEALID           := pF.T_DEALID;
     v_GR.T_SIDE             := pSide;
     v_GR.T_DEMANDSUM        := 0;
     v_GR.T_LIABILITYSUM     := 0;
     v_GR.T_PAYMENTSUM       := 0;
     v_GR.T_TRANSFERDATE     := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     v_GR.T_DEMANDACCOUNT    := CHR(1);
     v_GR.T_LIABILITYACCOUNT := CHR(1);
     v_GR.T_INSTANCE         := 0;
     v_GR.T_FLOATRATEVALUE   := 0;
     v_GR.T_NETTING_PMGR     := CHR(0);
     v_GR.T_NETTING_INCLUDE  := CHR(0);

     IF (LAST_DAY(v_GR.T_BEGDATE) = v_GR.T_BEGDATE) THEN
        v_LastDay := 1;
     ELSE
        v_Day := EXTRACT(DAY FROM v_GR.T_BEGDATE);
     END IF;

     WHILE (v_GR.T_BEGDATE < pF.T_EXECDATE) LOOP
        v_CorrectPayDate := 0;
        v_GR.T_ENDDATE   := LEAST(v_NextBegDate + RSI_DV_DaysInPeriod(pF.T_BASIS, v_GR.T_BEGDATE, pF.T_PERIOD, pF.T_PERIODKIND, v_LastDay, v_Day, pF.T_EXECDATE), pF.T_EXECDATE);
        v_NextBegDate    := v_GR.T_ENDDATE;
        v_GR.T_PAYDATE   := RSI_DV_GetPayDate(pD.T_ID, pF.T_FIID, pSide, pF.t_TypeCalc, v_GR.T_ENDDATE); -- дата платежа

        IF (pD.T_CORRECTDATE != chr(88)) THEN
            RSI_DV_UpdatePMGR_Dates(0, pF.T_DEALID, pSide, v_GR.T_PAYDATE, v_GR.T_BEGDATE, v_GR.T_ENDDATE);
        END IF;

        IF (v_GR.T_PAYDATE != v_GR.T_ENDDATE) THEN -- дата платежа изменена
            IF (pD.T_CORRECTDATE = chr(88)) THEN
                v_GR.T_ENDDATE   := v_GR.T_PAYDATE;
                v_CorrectPayDate := 1;
            END IF;
        END IF;

        v_GR.T_FIXDATE := RSI_DV_GetFixDate(pD.T_ID, pF.T_FIID, pSide, pF.t_TypeCalc, v_GR.T_BEGDATE, pF.T_FIXDAYS, pF.t_CalKindID);

        IF (v_GR.T_BEGDATE != v_GR.T_ENDDATE) THEN
           INSERT INTO DDVNPMGR_DBT VALUES v_GR;
        END IF;

        v_GR.T_BEGDATE := v_GR.T_ENDDATE;
     END LOOP;

     /*IF (v_CorrectPayDate = 1) THEN
        UPDATE DDVNFI_DBT
           SET T_EXECDATE = v_GR.T_ENDDATE
         WHERE T_ID       = pF.T_ID;

        IF (pSide = ALG_DV_PMGR_SIDE_UNDEF) THEN
           UPDATE DDVNFI_DBT
              SET T_EXECDATE = v_GR.T_ENDDATE
            WHERE T_DEALID   = pD.T_ID
              AND T_TYPE     = DV_NFIType_BaseActiv2;
        END IF;
     END IF;*/
  END;

   PROCEDURE DV_CreatePMGR_TMP( pD IN DDVNDEAL_DBT%ROWTYPE, pF IN DDVNFI_DBT%ROWTYPE, pSide IN NUMBER )
  IS
     v_GR             DDVNPMGR_TMP%ROWTYPE;
     v_DATE           DATE := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     v_NextBegDate    DATE := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     v_CorrectPayDate NUMBER := 0;
     v_LastDay        NUMBER := 0;
     v_Day            NUMBER := 0;
  BEGIN
     RSI_DV_DeletePMGR_TMP(pD.T_ID, pSide);

     BEGIN
        SELECT NVL(MAX(T_ENDDATE), TO_DATE('01.01.0001', 'dd.mm.yyyy')) INTO v_DATE
          FROM DDVNPMGR_TMP PMGR
         WHERE PMGR.T_DEALID = pD.T_ID
           AND PMGR.T_SIDE   = pSide;

        EXCEPTION WHEN NO_DATA_FOUND THEN v_GR.T_BEGDATE := pD.T_BEGINDATE;
     END;

     IF( v_DATE != TO_DATE('01.01.0001', 'dd.mm.yyyy') ) THEN
        v_GR.T_BEGDATE := v_DATE;
     ELSE
        v_GR.T_BEGDATE := pD.T_BEGINDATE;
     END IF;

     v_NextBegDate := v_GR.T_BEGDATE;

     v_GR.T_ID               := 0;
     v_GR.T_DEALID           := pF.T_DEALID;
     v_GR.T_SIDE             := pSide;
     v_GR.T_DEMANDSUM        := 0;
     v_GR.T_LIABILITYSUM     := 0;
     v_GR.T_PAYMENTSUM       := 0;
     v_GR.T_TRANSFERDATE     := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     v_GR.T_DEMANDACCOUNT    := CHR(1);
     v_GR.T_LIABILITYACCOUNT := CHR(1);
     v_GR.T_INSTANCE         := 0;
     v_GR.T_FLOATRATEVALUE   := 0;

     IF( LAST_DAY(v_GR.T_BEGDATE) = v_GR.T_BEGDATE ) THEN
        v_LastDay := 1;
     ELSE
        v_Day := EXTRACT(DAY FROM v_GR.T_BEGDATE);
     END IF;

     WHILE( v_GR.T_BEGDATE < pF.T_EXECDATE ) LOOP
        v_CorrectPayDate := 0;
        v_GR.T_ENDDATE := least(v_NextBegDate + RSI_DV_DaysInPeriod(pF.T_BASIS, v_GR.T_BEGDATE, pF.T_PERIOD, pF.T_PERIODKIND, v_LastDay, v_Day), pF.T_EXECDATE);
        v_NextBegDate  := v_GR.T_ENDDATE;
        v_GR.T_PAYDATE := RSI_DV_GetPayDate(pD.T_ID, pF.T_FIID, pSide, pF.t_TypeCalc, v_GR.T_ENDDATE); -- дата платежа

        IF( pD.T_CORRECTDATE = chr(88) ) THEN
           IF( v_GR.T_PAYDATE != v_GR.T_ENDDATE ) THEN -- дата платежа изменена
              v_GR.T_ENDDATE   := v_GR.T_PAYDATE;
              v_CorrectPayDate := 1;
           END IF;
        ELSE
           RSI_DV_UpdatePMGR_Dates_TMP(0, pF.T_DEALID, pSide, v_GR.T_PAYDATE, v_GR.T_BEGDATE, v_GR.T_ENDDATE);
        END IF;

        v_GR.T_FIXDATE := RSI_DV_GetFixDate(pD.T_ID, pF.T_FIID, pSide, pF.t_TypeCalc, v_GR.T_BEGDATE, pF.T_FIXDAYS, pF.t_CalKindID);

        IF( v_GR.T_ENDDATE != v_GR.T_BEGDATE ) THEN
           INSERT INTO DDVNPMGR_TMP VALUES v_GR;
        END IF;

        v_GR.T_BEGDATE := v_GR.T_ENDDATE;
     END LOOP;

     /*IF( v_CorrectPayDate != 0 ) THEN
        UPDATE DDVNFI_DBT
           SET T_EXECDATE = v_GR.T_ENDDATE
         WHERE T_ID = pF.T_ID;

        IF( pSide = ALG_DV_PMGR_SIDE_UNDEF ) THEN
           UPDATE DDVNFI_DBT
              SET T_EXECDATE = v_GR.T_ENDDATE
            WHERE T_DEALID = pD.T_ID
              AND T_TYPE = DV_NFIType_BaseActiv2;
        END IF;
     END IF;*/
  END;

  -- Процедура генерации графика платежей
  PROCEDURE DV_UpdatePMGR( pDealID IN NUMBER, pLiability IN NUMBER DEFAULT 1, pDemand IN NUMBER DEFAULT 1 )
  IS
     v_D  DDVNDEAL_DBT%ROWTYPE;
     v_F  DDVNFI_DBT%ROWTYPE;
     v_F2 DDVNFI_DBT%ROWTYPE;
  BEGIN

     BEGIN
        SELECT * INTO v_D
          FROM DDVNDEAL_DBT
         WHERE T_ID = pDealID;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
     END;

     BEGIN
        SELECT * INTO v_F
          FROM DDVNFI_DBT
         WHERE T_DEALID = pDealID
           AND T_TYPE = DV_NFIType_BaseActiv;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
     END;

     IF( /*v_F.T_EXECTYPE = DVSETTLEMET_CALC and */v_D.t_Netting_pmgr = chr(88) ) THEN
        IF( v_D.t_State = DVDEAL_STATE_PREP ) THEN -- В отложенных можно менять тип исполнения, поэтому удаляем записи от поставочного, если они есть
           RSI_DV_DeletePMGR(pDealID, ALG_DV_PMGR_SIDE_LIABILITY);
           RSI_DV_DeletePMGR(pDealID, ALG_DV_PMGR_SIDE_DEMAND);
        END IF;

        IF( v_D.t_Type = ALG_DV_FIX_FLOAT ) THEN
           BEGIN
              SELECT * INTO v_F2
                FROM DDVNFI_DBT
               WHERE T_DEALID = pDealID
                 AND T_TYPE = DV_NFIType_BaseActiv2;

              EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
           END;

           DV_CreatePMGR(v_D, v_F2, ALG_DV_PMGR_SIDE_UNDEF);
        ELSE
           DV_CreatePMGR(v_D, v_F, ALG_DV_PMGR_SIDE_UNDEF);
        END IF;
     ELSE
        IF( v_D.t_State = DVDEAL_STATE_PREP ) THEN -- В отложенных можно менять тип исполнения, поэтому удаляем записи от расчетного, если они есть
           RSI_DV_DeletePMGR(pDealID, ALG_DV_PMGR_SIDE_UNDEF);
        END IF;

        IF( pLiability = 0/*Да*/ ) THEN
           DV_CreatePMGR(v_D, v_F, ALG_DV_PMGR_SIDE_LIABILITY);
        END IF;

        IF( pDemand = 0/*Да*/ ) THEN
           BEGIN
              SELECT * INTO v_F2
                FROM DDVNFI_DBT
               WHERE T_DEALID = pDealID
                 AND T_TYPE = DV_NFIType_BaseActiv2;

              EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
           END;

           DV_CreatePMGR(v_D, v_F2, ALG_DV_PMGR_SIDE_DEMAND);
        END IF;
     END IF;
  END;


    -- Процедура генерации графика платежей
  PROCEDURE DV_UpdatePMGR_TMP( pDealID IN NUMBER, pLiability IN NUMBER DEFAULT 1, pDemand IN NUMBER DEFAULT 1 )
  IS
     v_D  DDVNDEAL_DBT%ROWTYPE;
     v_F  DDVNFI_DBT%ROWTYPE;
     v_F2 DDVNFI_DBT%ROWTYPE;
  BEGIN

     BEGIN
        SELECT * INTO v_D
          FROM DDVNDEAL_DBT
         WHERE T_ID = pDealID;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
     END;

     BEGIN
        SELECT * INTO v_F
          FROM DDVNFI_DBT
         WHERE T_DEALID = pDealID
           AND T_TYPE = DV_NFIType_BaseActiv;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
     END;

     IF( /*v_F.T_EXECTYPE = DVSETTLEMET_CALC and */v_D.t_Netting_pmgr = chr(88) ) THEN
        IF( v_D.t_State = DVDEAL_STATE_PREP ) THEN -- В отложенных можно менять тип исполнения, поэтому удаляем записи от поставочного, если они есть
           RSI_DV_DeletePMGR_TMP(pDealID, ALG_DV_PMGR_SIDE_LIABILITY);
           RSI_DV_DeletePMGR_TMP(pDealID, ALG_DV_PMGR_SIDE_DEMAND);
        END IF;

        IF( v_D.t_Type = ALG_DV_FIX_FLOAT ) THEN
           BEGIN
              SELECT * INTO v_F2
                FROM DDVNFI_DBT
               WHERE T_DEALID = pDealID
                 AND T_TYPE = DV_NFIType_BaseActiv2;

              EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
           END;

           DV_CreatePMGR_TMP(v_D, v_F2, ALG_DV_PMGR_SIDE_UNDEF);
        ELSE
           DV_CreatePMGR_TMP(v_D, v_F, ALG_DV_PMGR_SIDE_UNDEF);
        END IF;
     ELSE
        IF( v_D.t_State = DVDEAL_STATE_PREP ) THEN -- В отложенных можно менять тип исполнения, поэтому удаляем записи от расчетного, если они есть
           RSI_DV_DeletePMGR_TMP(pDealID, ALG_DV_PMGR_SIDE_UNDEF);
        END IF;

        IF( pLiability = 0/*Да*/ ) THEN
           DV_CreatePMGR_TMP(v_D, v_F, ALG_DV_PMGR_SIDE_LIABILITY);
        END IF;

        IF( pDemand = 0/*Да*/ ) THEN
           BEGIN
              SELECT * INTO v_F2
                FROM DDVNFI_DBT
               WHERE T_DEALID = pDealID
                 AND T_TYPE = DV_NFIType_BaseActiv2;

              EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
           END;

           DV_CreatePMGR_TMP(v_D, v_F2, ALG_DV_PMGR_SIDE_DEMAND);
        END IF;
     END IF;
  END;


  -- Установка признака учета по позиции на операции
  PROCEDURE RSI_DV_SetDealPosAcc( pDealID IN NUMBER )
  IS
  BEGIN
     UPDATE ddvdeal_dbt
        SET t_PosAcc = CHR(88)
      WHERE t_ID = pDealID;
  END;

  -- Установка признака учета по позиции на операции при массовом исполнении
  PROCEDURE DV_MassSetDealPosAcc IS
     CURSOR UpdtDeal IS (SELECT DISTINCT deal.t_ID
                           FROM doprtemp_view opr, ddvdeal_dbt deal
                          WHERE deal.t_ID = TO_NUMBER(opr.t_DocumentID)
                            AND opr.t_DocKind = DL_DVDEAL
                            AND (deal.t_Type = 'B' OR deal.t_Type = 'S' OR deal.t_Type = 'D' OR deal.t_Type = 'G')
                        );
  BEGIN
     FOR UpdtDeal_rec IN UpdtDeal LOOP
        RSI_DV_SetDealPosAcc(UpdtDeal_rec.t_ID);
     END LOOP;
  END;

  -- Сброс признака учета по позиции на операции
  PROCEDURE RSI_DV_UnsetDealPosAcc( pDealID IN NUMBER )
  IS
  BEGIN
     UPDATE ddvdeal_dbt
        SET t_PosAcc = CHR(0)
      WHERE t_ID = pDealID;
  END;

  PROCEDURE RSI_DV_LinkDeals( v_DvoperID IN INTEGER )
  IS
     v_Oper       DDVOPER_DBT%ROWTYPE;
     DB           DDVDEAL_DBT%ROWTYPE;
     v_A          NUMBER := 0;
     v_DC         NUMBER := 0;
     v_DCS        NUMBER := 0;
     V_SAmount    NUMBER := 0;
     V_SDealCost  NUMBER := 0;
  BEGIN

     BEGIN
        SELECT * INTO v_Oper
          FROM DDVOPER_DBT
         WHERE t_ID = v_DvoperID;
        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
     END;

     DECLARE
        CURSOR Deals IS SELECT *
          FROM ddvdeal_dbt deal
           WHERE deal.t_Type in('S', 'G')
           AND deal.t_DEPARTMENT  = v_Oper.t_DEPARTMENT
           AND deal.t_GenAgrID    = v_Oper.t_GenAgrID
           AND deal.t_State       = DVPOS_STATE_OPEN
           AND (deal.t_amount - deal.t_execution) > 0
           AND deal.t_date_clr <=   v_Oper.t_date
           AND ((v_Oper.t_PartyKind = 3  AND v_Oper.t_Party = (SELECT fin.t_Issuer FROM dfininstr_dbt fin WHERE fin.t_FIID = deal.t_FIID) AND deal.t_Broker = -1)  OR
              (v_Oper.t_PartyKind = 22 AND deal.t_Broker = v_Oper.t_Party AND deal.t_BrokerContr = v_Oper.t_PartyContr))
           AND (((v_Oper.t_Flag1 = ALG_SP_MONEY_SOURCE_OWN) AND (deal.t_Client <= 0)) OR
              ((v_Oper.t_Flag1 = ALG_SP_MONEY_SOURCE_CLIENT) AND (deal.t_Client >= 1)) OR
              ((v_Oper.t_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) AND (deal.t_IsTrust = chr(88))))
           AND (SELECT fin1.t_DrawingDate FROM dfininstr_dbt fin1 WHERE fin1.t_FIID = deal.t_FIID) >= v_Oper.t_Date
          ORDER BY deal.t_Date ASC, deal.t_Time ASC, deal.t_ExtCode ASC;
           BEGIN
              FOR DS IN Deals LOOP
                 V_SAmount   := DS.t_Amount - DS.t_Execution;
                 V_SDealCost := DS.t_DealCost;
                 v_A         := 0;
                 v_DC        := 0;
                 v_DCS       := 0;
                 LOOP
                    BEGIN
                       SELECT * INTO DB
                       FROM ( SELECT /*+ INDEX( Deal DDVDEAL_DBT_IDXA)*/ Deal.*
                         FROM DDVDEAL_DBT Deal
                          WHERE deal.t_Type in('B', 'D')
                          AND deal.t_FIID        = DS.t_FIID
                          AND deal.t_DEPARTMENT  = DS.t_DEPARTMENT
                          AND deal.t_broker      = DS.t_Broker
                          AND deal.t_ClientContr = DS.t_ClientContr
                          AND deal.t_GenAgrID    = DS.t_GenAgrID
                          AND deal.t_State       = DVPOS_STATE_OPEN
                          AND (deal.t_amount - deal.t_execution) > 0
                          AND deal.t_date_clr    <= v_Oper.t_date
                         ORDER BY deal.t_Date ASC, deal.t_Time ASC, deal.t_ExtCode ASC  )
                          WHERE ROWNUM = 1;
                    EXCEPTION WHEN NO_DATA_FOUND THEN
                       EXIT;
                    END;
                    IF ( (DB.t_Amount-DB.t_Execution) > V_SAmount) THEN
                       v_A := V_SAmount;
                    ELSE
                       v_A := DB.t_Amount - DB.t_Execution;
                    END IF;
                    V_SAmount := V_SAmount  - V_A;
                    IF ( V_SAmount = 0) THEN
                       V_DC  := ROUND( DB.T_DEALCOST * V_A/(DB.T_AMOUNT - DB.T_EXECUTION), 2);
                       V_DCS := V_SDealCost;
                    ELSE
                       V_DC  := DB.t_DealCost;
                       V_DCS := ROUND( DS.T_DEALCOST * V_A/(DS.T_AMOUNT - DS.T_EXECUTION), 2);
                    END IF;
                    INSERT INTO ddvdllnk_dbt( t_ID, t_DealID, t_ExecID, t_Execution, t_ExecCost, t_Kind, t_SaleDealID, t_SaleExecCost, t_Date, T_DvoperID )
                       VALUES( 0, DB.t_ID, 0, v_A, v_DC, 1, DS.T_ID, v_DCS, v_oper.T_Date, v_oper.T_ID );
                       V_SDealCost := V_SDealCost - v_DCS;
                 EXIT WHEN V_SAmount = 0;
                 END LOOP;
              END LOOP;
           END;
  END;

  PROCEDURE RSI_DV_RecoilLinkDeals( v_DvoperID IN INTEGER )
  IS
  BEGIN
     DELETE FROM DDVDLLNK_DBT
      WHERE T_DvoperID = v_DvoperID;
  END;

  -- Включение ПП в неттинг
  PROCEDURE RSI_DV_SetNettingPMGR( v_PMGRID IN INTEGER )
  IS
     v_prevSymb CHAR := chr(0);
     v_Symb     CHAR := chr(0);
  BEGIN
     SELECT t_Netting_Include INTO v_prevSymb
        FROM ddvnpmgr_dbt
      WHERE t_ID = v_PMGRID;

     IF (v_prevSymb = chr(82)) THEN
        v_Symb := chr(88);
     ELSE
        v_Symb := chr(82);
     END IF;

     UPDATE ddvnpmgr_dbt
        SET t_Netting_Include = v_Symb
      WHERE t_ID = v_PMGRID;
  END; -- RSI_DV_SetStateNDeal

  -- Исключение ПП из неттинга
  PROCEDURE RSI_DV_UnsetNettingPMGR( v_PMGRID IN INTEGER )
  IS
     v_prevSymb CHAR := chr(0);
     v_Symb     CHAR := chr(0);
  BEGIN
     SELECT t_Netting_Include INTO v_prevSymb
        FROM ddvnpmgr_dbt
      WHERE t_ID = v_PMGRID;

     IF (v_prevSymb = chr(88)) THEN
        v_Symb := chr(82);
     ELSE
        v_Symb := chr(0);
     END IF;

     UPDATE ddvnpmgr_dbt pm
        SET pm.t_Netting_Include = v_Symb
      WHERE pm.t_ID = v_PMGRID;
  END; -- RSI_DV_SetStateNDeal

  -- Процедура связывания сделок при исполнении
  PROCEDURE RSI_DV_ExecDeals( v_ExecID IN INTEGER /* Операция исполнения */ )
  IS
     v_Exec       DDVDEAL_DBT%ROWTYPE;
     v_Deal       DDVDEAL_DBT%ROWTYPE;
     v_S          NUMBER := 0;
     v_SBegin     NUMBER := 0;
     v_FA         NUMBER := 0;
     v_A          NUMBER := 0;
     v_C          NUMBER := 0;
     v_DC         NUMBER := 0;
     v_AvoirKind  NUMBER := 0;
     v_AvoirFIID  NUMBER := 0;
     v_AvoirDealKind  NUMBER := 0;
     v_AvoirDealType  CHAR := chr(0);
  BEGIN

     BEGIN
        SELECT * INTO v_Exec
          FROM DDVDEAL_DBT
         WHERE t_ID = v_ExecID;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
     END;

     v_S := v_Exec.t_Amount;

     WHILE( v_S > 0 ) LOOP

        BEGIN
           SELECT * INTO v_Deal
             FROM ( SELECT Deal.*
                      FROM DDVDEAL_DBT Deal
                     WHERE (((Deal.t_Type in('B', 'D')) and (v_Exec.t_Position = DV_POSITION_LONG)) or ((Deal.t_Type in('S', 'G')) and (v_Exec.t_Position = DV_POSITION_SHORT)))
                       AND Deal.t_FIID        = v_Exec.T_FIID
                       AND Deal.t_Department  = v_Exec.t_Department
                       AND Deal.t_Broker      = v_Exec.t_Broker
                       AND Deal.t_ClientContr = v_Exec.t_ClientContr
                       AND Deal.t_GenAgrID    = v_Exec.t_GenAgrID
                       AND Deal.t_State       = DVDEAL_STATE_OPEN
                       AND (Deal.t_Amount - Deal.t_Execution) > 0
                       AND Deal.t_Date_clr    <= v_Exec.t_Date_clr
                  ORDER BY Deal.t_Date ASC, Deal.t_Time ASC, Deal.t_ID ASC )
            WHERE ROWNUM = 1;
        EXCEPTION WHEN NO_DATA_FOUND THEN
           IF( v_Exec.t_Position = DV_POSITION_SHORT ) THEN
              RAISE_APPLICATION_ERROR(-20511, ''); -- Овердрафт по короткой позиции
           ELSE
              RAISE_APPLICATION_ERROR(-20510, ''); -- Овердрафт по длинной позиции
           END IF;
        END;

        v_FA := v_Deal.t_Amount - v_Deal.t_Execution;

        IF( v_FA <= v_S ) THEN -- списываем полностью
           v_A  := v_FA;
           v_DC := v_Deal.t_DealCost;
        ELSE
           v_A  := v_S;
           v_DC := v_Deal.t_DealCost * v_A/v_FA;
        END IF;

        INSERT INTO ddvdllnk_dbt( t_DealID, t_ExecID, t_Execution, t_ExecCost, t_Kind, t_SaleDealID, t_SaleExecCost, t_Date, t_DvoperID )
             VALUES( v_Deal.t_ID, v_Exec.t_ID, v_A, v_DC, 0, 0, 0, v_Exec.t_Date_clr, 0 );
        v_S := v_S - v_A;
     END LOOP;

     BEGIN
      SELECT finderiv.t_AVOIRKIND, finderiv.t_FIID INTO v_AvoirKind, v_AvoirFIID
        FROM dfininstr_dbt fin, dfininstr_dbt finderiv
        WHERE fin.t_FIID = v_Exec.t_FIID
          AND finderiv.t_FIID = fin.t_facevaluefi;
     EXCEPTION WHEN NO_DATA_FOUND THEN
        v_AvoirKind := 0;
     END;

     IF (v_AvoirKind = DV_DERIVATIVE_FUTURES) THEN
       v_S      := v_Exec.t_Amount;
       v_SBegin := v_Exec.t_Amount;
       TRGPCKG_DDVDEAL_DBT_TRBIUD.v_NumEnt := TRGPCKG_DDVDEAL_DBT_TRBIUD.v_NumEnt - 1;
       WHILE( v_S > 0 AND v_AvoirKind > 0 ) LOOP
          BEGIN
             SELECT * INTO v_Deal
               FROM ( SELECT Deal.*
                      FROM DDVDEAL_DBT Deal
                     WHERE (((Deal.t_Type in('B', 'D')) and (v_Exec.t_Position = DV_POSITION_SHORT)) or ((Deal.t_Type in('S', 'G')) and (v_Exec.t_Position = DV_POSITION_LONG)))
                       AND Deal.t_FIID        = v_AvoirFIID
                       AND Deal.t_Department  = v_Exec.t_Department
                       AND Deal.t_Broker      = v_Exec.t_Broker
                       AND Deal.t_ClientContr = v_Exec.t_ClientContr
                       AND Deal.t_GenAgrID    = v_Exec.t_GenAgrID
                       AND Deal.t_State       = DVDEAL_STATE_OPEN
                       AND (Deal.t_Amount - Deal.t_Execution) > 0
                       AND Deal.t_Date_clr    <= v_Exec.t_Date_clr
                  ORDER BY Deal.t_Date ASC, Deal.t_Time ASC, Deal.t_ID ASC )
            WHERE ROWNUM = 1;
          EXCEPTION WHEN NO_DATA_FOUND THEN
             v_AvoirKind := 0;
          END;
          IF ( v_AvoirKind > 0) THEN
             v_FA := v_Deal.t_Amount - v_Deal.t_Execution;

             IF( v_FA <= v_S ) THEN -- списываем полностью
                v_A  := v_FA;
                v_DC := v_Deal.t_DealCost;
             ELSE
                v_A  := v_S;
                v_DC := v_Deal.t_DealCost * v_A/v_FA; -- остаток суммы
             END IF;

             INSERT INTO ddvdllnk_dbt( t_DealID, t_ExecID, t_Execution, t_ExecCost, t_Kind, t_SaleDealID, t_SaleExecCost, t_Date, T_DvoperID )
                VALUES( v_Deal.t_ID, v_Exec.t_ID, v_A, v_DC, 0, 0, 0, v_Exec.t_Date_clr, 0 );
              v_S := v_S - v_A;
          END IF;
        END LOOP;

        IF (v_S > 0 AND v_Exec.t_Kind = DV_OPTIONEXEC) THEN
           v_C := v_Exec.t_Cost * v_S/v_SBegin;
           IF( v_Exec.t_Position = DV_POSITION_LONG ) THEN -- покупка
              IF( v_Exec.t_IsTrust = chr(88) ) THEN
                 v_AvoirDealType := 'D';
              ELSE
                 v_AvoirDealType := 'B';
              END IF;
              v_AvoirDealKind := DV_FUTURESBUY;
           ELSE -- продажа
              IF( v_Exec.t_IsTrust = chr(88) ) THEN
                 v_AvoirDealType := 'G';
              ELSE
                 v_AvoirDealType := 'S';
              END IF;
              v_AvoirDealKind := DV_FUTURESSELL;
           END IF;
           BEGIN
              INSERT INTO ddvdeal_dbt ( t_ID, t_Kind, t_Type, t_Date, t_Time, t_Date_CLR, t_Code, t_FIID, t_Broker, t_Contractor,
                                     t_Client, t_Amount, t_Position, t_Price, t_Point, t_Cost, t_Bonus, t_Department, t_Oper, t_State,
                                     t_PositionCost, t_PositionBonus, t_TurnAmount, t_TurnCost, t_BrokerContr, t_ClientContr, t_PosAcc, t_DealCost, t_GenAgrID   )
              VALUES( 0, v_AvoirDealKind, v_AvoirDealType, v_Exec.T_Date, v_Exec.t_Time, v_Exec.t_Date_CLR, CONCAT('Ex', v_Exec.t_Code), v_AvoirFIID, v_Exec.t_Broker, v_Exec.t_Contractor,
                      v_Exec.t_Client, v_S, 0, v_Exec.t_Price, v_Exec.t_Point, v_C, v_Exec.t_Bonus, v_Exec.t_Department, v_Exec.t_Oper, DVDEAL_STATE_PREP,
                      v_C, v_Exec.t_PositionBonus, v_Exec.t_TurnAmount, v_Exec.t_TurnCost, v_Exec.t_BrokerContr, v_Exec.t_ClientContr, chr(0), v_C, v_Exec.t_GenAgrID );
           EXCEPTION
              WHEN DUP_VAL_ON_INDEX THEN NULL;
           END;
        END IF;
     END IF;

  END; -- RSI_DV_ExecDeals

  -- Процедура отката связывания сделок при исполнении
  PROCEDURE RSI_DV_RecoilExecDeals( v_ExecID IN INTEGER /* Операция исполнения */ )
  IS
  BEGIN

     DELETE FROM DDVDLLNK_DBT
      WHERE t_ExecID = v_ExecID;

  END; -- RSI_DV_RecoilExecDeals

  -- Привязка используемой записи по итогам дня по сделке.
  -- Процедура проверяет наличие и состояние записи по итогам дня по сделке и, при необходимости, создает ее. Счетчик ссылок увеличивается.
  -- Состояние сделки проверяется в процедуре.
  -- Процедура должна вызываться всякий раз, когда возникает необходимость изменить данные по сделке: при вставке сделок, при импорте данных по сделке, при обработке сделок в операциях расчетов. Вызывается обычно другими ХП.
  PROCEDURE RSI_DV_AttachDealTurn( v_DealID   IN INTEGER,             -- Сделка
                                   v_Date     IN DATE,                -- Дата
                                   v_InTrg    IN CHAR DEFAULT CHR(0), -- Признак вызова из триггера
                                   v_DealCost IN NUMBER DEFAULT 0     -- Сумма по сделке
                                 )
  IS
     v_exist_turn   INTEGER := 0;
     v_exist_fiturn INTEGER := 0;
     v_D            DDVDEAL_DBT%ROWTYPE;
     v_Execution    NUMBER := 0;
     v_ExecCost     NUMBER := 0;
     Vv_DealCost    NUMBER := 0;
     v_Margin       NUMBER := 0;
     v_FairValue    NUMBER := 0;
  BEGIN

     SELECT count(1) INTO v_exist_turn
       FROM ddvdlturn_dbt
      WHERE t_DealID = v_DealID
        AND t_Date   > v_Date;

     IF( v_exist_turn > 0 ) THEN
        RAISE_APPLICATION_ERROR(-20581,''); -- Открыт день по сделке за большую дату
     END IF;

     IF( v_InTrg = CHR(0) ) THEN
        -- Найдём сделку
        BEGIN
           SELECT * INTO v_D
             FROM DDVDEAL_DBT
            WHERE t_ID = v_DealID;

           EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
        END;

        IF( v_D.t_State = DVDEAL_STATE_CLOSE ) THEN
           RAISE_APPLICATION_ERROR(-20582, ''); -- Сделка закрыта
        ELSIF( ((v_D.t_State = DVDEAL_STATE_PREP) AND (v_D.t_Date_CLR <> v_Date)) OR ((v_D.t_State = DVDEAL_STATE_OPEN) AND (v_D.t_Date_CLR > v_Date)) ) THEN
           RAISE_APPLICATION_ERROR(-20583, ''); -- Неверная дата итогов
        END IF;
     END IF;

     SELECT count(1) INTO v_exist_fiturn
       FROM ddvfiturn_dbt turn
      WHERE turn.t_FIID        = v_D.t_FIID
        AND turn.t_Department  = v_D.t_Department
        AND turn.t_Broker      = v_D.t_Broker
        AND turn.t_ClientContr = v_D.t_ClientContr
        AND turn.t_GenAgrID    = v_D.t_GenAgrID
        AND turn.t_Date        = v_Date
        AND turn.t_State       = DVTURN_STATE_CLOSE;

     IF( v_exist_fiturn > 0 ) THEN
        RAISE_APPLICATION_ERROR(-20509,''); -- День по позиции закрыт
     END IF;

     v_exist_turn := 0;
     SELECT count(1) INTO v_exist_turn
       FROM ddvdlturn_dbt
      WHERE t_DealID = v_DealID
        AND t_Date   = v_Date;

     IF( v_exist_turn > 0 ) THEN
        UPDATE ddvdlturn_dbt
           SET t_RefCounter = t_RefCounter + 1
         WHERE t_DealID = v_DealID
           AND t_Date   = v_Date;
     ELSE
        BEGIN
           SELECT turn.t_Execution, turn.t_ExecCost, turn.t_DealCost, 0, turn.t_FairValue INTO v_Execution, v_ExecCost, Vv_DealCost, v_Margin, v_FairValue
             FROM ddvdlturn_dbt turn
            WHERE turn.t_DealID = v_DealID
              AND turn.t_Date   = ( SELECT MAX(dlturn.t_Date)
                                      FROM ddvdlturn_dbt dlturn
                                     WHERE dlturn.t_DealID = v_DealID
                                       AND dlturn.t_Date   < v_Date
                                  );
           EXCEPTION WHEN NO_DATA_FOUND THEN
              v_Execution := 0;
              v_ExecCost  := 0;
              v_Margin    := 0;
              v_FairValue := 0;
              Vv_DealCost := (CASE WHEN v_InTrg = CHR(0) THEN v_D.t_PositionCost ELSE v_DealCost END);
        END;

        INSERT INTO ddvdlturn_dbt( t_DealID, t_Date, t_Execution, t_ExecCost, t_DealCost, t_Margin, t_FairValue )
             VALUES( v_DealID, v_Date, v_Execution, v_ExecCost, Vv_DealCost, v_Margin, v_FairValue );
     END IF;
  END; -- RSI_DV_AttachDealTurn

  -- Освобождение используемой записи по итогам дня по сделке.
  -- Процедура уменьшает счетчик ссылок. Если запись больше не используется, она удаляется.
  -- Процедура должна вызываться при откате операций, использующих записи по итогам дня.
  PROCEDURE RSI_DV_DetachDealTurn( v_DealID  IN INTEGER,            -- Сделка
                                   v_Date    IN DATE,               -- Дата
                                   v_InTrg   IN CHAR DEFAULT CHR(0) -- Признак вызова из триггера
                                 )
  IS
     v_exist_turn   INTEGER := 0;
     v_exist_fiturn INTEGER := 0;
     v_D            DDVDEAL_DBT%ROWTYPE;
     v_RefCounter   NUMBER := 0;
  BEGIN

     IF( v_InTrg = CHR(0) ) THEN
        -- Найдём сделку
        BEGIN
           SELECT * INTO v_D
             FROM DDVDEAL_DBT
            WHERE t_ID = v_DealID;

           EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
        END;

        IF( v_D.t_State = DVDEAL_STATE_CLOSE ) THEN
           RAISE_APPLICATION_ERROR(-20582, ''); -- Сделка закрыта
        END IF;
     END IF;

     SELECT count(1) INTO v_exist_fiturn
       FROM ddvfiturn_dbt turn
      WHERE turn.t_FIID        = v_D.t_FIID
        AND turn.t_Department  = v_D.t_Department
        AND turn.t_Broker      = v_D.t_Broker
        AND turn.t_ClientContr = v_D.t_ClientContr
        AND turn.t_GenAgrID    = v_D.t_GenAgrID
        AND turn.t_Date        = v_Date
        AND turn.t_State       = DVTURN_STATE_CLOSE;

     IF( v_exist_fiturn > 0 ) THEN
        RAISE_APPLICATION_ERROR(-20509,''); -- День по позиции закрыт
     END IF;

     BEGIN
        SELECT t_RefCounter INTO v_RefCounter
          FROM ddvdlturn_dbt
         WHERE t_DealID = v_DealID
           AND t_Date   = v_Date;

        EXCEPTION WHEN NO_DATA_FOUND THEN v_RefCounter := 0; -- Если не открыт день по сделке, пропускаем действия со счётчиком
     END;

     IF( v_RefCounter = 1 ) THEN -- Удаляем, когда счётчик достигает нуля
        DELETE FROM ddvdlturn_dbt
         WHERE t_DealID = v_DealID
           AND t_Date   = v_Date;
     ELSIF (v_RefCounter <> 0) THEN
        UPDATE ddvdlturn_dbt
           SET t_RefCounter = t_RefCounter - 1
         WHERE t_DealID = v_DealID
           AND t_Date   = v_Date;
     END IF;
  END; -- RSI_DV_DetachDealTurn

  -- Вставка итогов по сделке. Используется в процедурах импорта, вставке рассчитанных итогов и редактирования.
  PROCEDURE RSI_DV_InsertDealTurn( v_DealID          IN INTEGER, -- Сделка
                                   v_Date            IN DATE,    -- Дата
                                   v_Margin          IN NUMBER,  -- Вариационная маржа
                                   v_Guaranty        IN NUMBER,  -- Гарантийное обеспечение
                                   v_FairValueCalc   IN CHAR,    -- Признак расчета справедливой стоимости
                                   v_FairValue       IN NUMBER,  -- Справедливая стоимость
                                   v_InsertMargin    IN INTEGER, -- Вставка вариационной маржи
                                   v_InsertGuaranty  IN INTEGER, -- Вставка гарантийного обеспечения
                                   v_InsertFairValue IN INTEGER, -- Вставка справедливой стоимости
                                   v_Action          IN INTEGER  -- Действие
                                 )
  IS
     v_Deal            DDVDEAL_DBT%ROWTYPE;
     v_DlTurn          DDVDLTURN_DBT%ROWTYPE;
     v_exist_dlturn    INTEGER;
     v_exist_fiturn    INTEGER := 0;
     v_GuarantyonDeal  CHAR;
     Vv_InsertGuaranty INTEGER;
  BEGIN

     -- Найдём сделку
     BEGIN
        SELECT * INTO v_Deal
          FROM ddvdeal_dbt
         WHERE t_ID = v_DealID;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
     END;

     IF( v_Deal.t_State = DVDEAL_STATE_CLOSE ) THEN
        RAISE_APPLICATION_ERROR(-20582, ''); -- Сделка закрыта
     END IF;

     SELECT count(1) INTO v_exist_fiturn
       FROM ddvfiturn_dbt turn
      WHERE turn.t_FIID        = v_Deal.t_FIID
        AND turn.t_Department  = v_Deal.t_Department
        AND turn.t_Broker      = v_Deal.t_Broker
        AND turn.t_ClientContr = v_Deal.t_ClientContr
        AND turn.t_GenAgrID    = v_Deal.t_GenAgrID
        AND turn.t_Date        = v_Date
        AND turn.t_State       = DVTURN_STATE_CLOSE;

     IF( v_exist_fiturn > 0 ) THEN
        RAISE_APPLICATION_ERROR(-20509,''); -- День по позиции закрыт
     END IF;

     -- Найдём dlturn
     BEGIN
        v_exist_dlturn := 1;
        SELECT * INTO v_DlTurn
          FROM ddvdlturn_dbt
         WHERE t_DealID = v_DealID
           AND t_Date   = v_Date;

        EXCEPTION WHEN NO_DATA_FOUND THEN v_exist_dlturn := 0;
     END;

     IF( v_Action IN (DV_ACTION_EDIT, DV_ACTION_IMPORT) ) THEN

        BEGIN
          SELECT pos.t_GuarantyonDeal INTO v_GuarantyonDeal
            FROM ddvfipos_dbt pos
           WHERE pos.t_FIID        = v_Deal.t_FIID
             AND pos.t_Department  = v_Deal.t_Department
             AND pos.t_Broker      = v_Deal.t_Broker
             AND pos.t_ClientContr = v_Deal.t_ClientContr
             AND pos.t_GenAgrID    = v_Deal.t_GenAgrID;
        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20505,''); --Не открыта позиция по производному инструменту
        END;

        IF( v_GuarantyonDeal = chr(88) ) THEN
           Vv_InsertGuaranty := v_InsertGuaranty;
        ELSE
           Vv_InsertGuaranty := 0;
        END IF;

        IF( (v_InsertMargin = 0) and (Vv_InsertGuaranty = 0) and (v_InsertFairValue = 0) ) THEN
           RAISE_APPLICATION_ERROR(-20578,''); --Неверные параметры при вызове процедуры
        END IF;

        IF( (v_exist_dlturn = 0) OR (v_DlTurn.t_SetMargin = CHR(0) AND v_DlTurn.t_SetGuaranty = CHR(0) AND v_DlTurn.t_SetFairValue = CHR(0)) ) THEN
           RSI_DV_AttachDealTurn(v_DealID, v_Date);
        END IF;

        UPDATE ddvdlturn_dbt N
           SET N.t_Margin        = (CASE WHEN v_InsertMargin    = 1 THEN v_Margin    ELSE N.t_Margin END),
               N.t_SetMargin     = (CASE WHEN v_InsertMargin    = 1 THEN CHR(88)     ELSE N.t_SetMargin END),
               N.t_Guaranty      = (CASE WHEN Vv_InsertGuaranty = 1 THEN v_Guaranty  ELSE N.t_Guaranty END),
               N.t_SetGuaranty   = (CASE WHEN Vv_InsertGuaranty = 1 THEN CHR(88)     ELSE N.t_SetGuaranty END),
               N.t_FairValueCalc = (CASE WHEN v_InsertFairValue = 1 THEN v_FairValueCalc ELSE N.t_FairValueCalc END),
               N.t_FairValue     = (CASE WHEN v_InsertFairValue = 1 THEN (CASE WHEN v_FairValueCalc = 'X' THEN v_FairValue ELSE 0 END) ELSE N.t_FairValue END),
               N.t_SetFairValue  = (CASE WHEN v_InsertFairValue = 1 THEN CHR(88)     ELSE N.t_SetFairValue END),
               N.t_OldMargin     = (CASE WHEN v_InsertMargin    = 1 THEN v_Margin    ELSE N.t_OldMargin END),
               N.t_OldSetMargin  = (CASE WHEN v_InsertMargin    = 1 THEN CHR(88)     ELSE N.t_OldSetMargin END),
               N.t_OldFairValueCalc = (CASE WHEN v_InsertFairValue = 1 THEN v_FairValueCalc ELSE N.t_OldFairValueCalc END),
               N.t_OldFairValue     = (CASE WHEN v_InsertFairValue = 1 THEN (CASE WHEN v_FairValueCalc = 'X' THEN v_FairValue ELSE 0 END) ELSE N.t_OldFairValue END),
               N.t_OldSetFairValue  = (CASE WHEN v_InsertFairValue = 1 THEN CHR(88)  ELSE N.t_OldSetFairValue END),
               N.t_ActionType       = v_Action
         WHERE N.t_DealID = v_DealID
           AND N.t_Date   = v_Date;

     ELSIF (v_Action IN (DV_ACTION_CALCITOG, DV_ACTION_DISTRIBITOG) ) THEN
        -- Поскольку сохраняются данные для отката, счетчик увеличиваем всегда
        RSI_DV_AttachDealTurn(v_DealID, v_Date);

        UPDATE ddvdlturn_dbt N
           SET N.t_OldMargin        = (CASE WHEN v_InsertMargin    = 1 THEN N.t_Margin        ELSE N.t_OldMargin END),
               N.t_OldSetMargin     = (CASE WHEN v_InsertMargin    = 1 THEN N.t_SetMargin     ELSE N.t_OldSetMargin END),
               N.t_OldFairValueCalc = (CASE WHEN v_InsertFairValue = 1 THEN N.t_FairValueCalc ELSE N.t_OldFairValueCalc END),
               N.t_OldFairValue     = (CASE WHEN v_InsertFairValue = 1 THEN N.t_FairValue     ELSE N.t_OldFairValue END),
               N.t_OldSetFairValue  = (CASE WHEN v_InsertFairValue = 1 THEN N.t_SetFairValue  ELSE N.t_OldSetFairValue END),
               N.t_Margin           = (CASE WHEN v_InsertMargin    = 1 THEN v_Margin          ELSE N.t_Margin END),
               N.t_SetMargin        = (CASE WHEN v_InsertMargin    = 1 THEN CHR(0)            ELSE N.t_SetMargin END),
               N.t_FairValueCalc    = (CASE WHEN v_InsertFairValue = 1 THEN v_FairValueCalc   ELSE N.t_FairValueCalc END),
               N.t_FairValue        = (CASE WHEN v_InsertFairValue = 1 THEN (CASE WHEN v_FairValueCalc = 'X' THEN v_FairValue ELSE 0 END) ELSE N.t_FairValue END),
               N.t_SetFairValue     = (CASE WHEN v_InsertFairValue = 1 THEN CHR(0) ELSE N.t_SetFairValue END),
               N.t_Guaranty         = (CASE WHEN v_InsertGuaranty  = 1 THEN v_Guaranty        ELSE N.t_Guaranty END),
               N.t_SetGuaranty      = (CASE WHEN v_InsertGuaranty  = 1 THEN CHR(0)            ELSE N.t_SetGuaranty END),
               N.t_ActionType       = v_Action
         WHERE N.t_DealID = v_DealID
           AND N.t_Date   = v_Date;
     ELSE
        --Действие не поддерживается
        RAISE_APPLICATION_ERROR(-20557,'');
     END IF;

  END; -- RSI_DV_InsertDealTurn

  -- Массовый откат данных по сделке
  PROCEDURE RSI_DV_MassRecoilInsertDealTurn(v_DealID   IN INTEGER, -- Сделка
                                            v_DvoperID IN INTEGER, -- Операция расчётов
                                            v_Action   IN INTEGER  -- Действие
                                            )
  IS
     v_Date DATE := TO_DATE('01.01.0001', 'dd.mm.yyyy'); -- Дата
  BEGIN
    SELECT t_Date
      INTO  v_Date
       FROM ddvoper_dbt
        WHERE t_ID = v_DvoperID AND t_DocKind = 194;
        RSI_DV_RecoilInsertDealTurn(v_DealID, v_Date, v_Action);
  END;

  -- Откат вставки данных по сделке
  PROCEDURE RSI_DV_RecoilInsertDealTurn( v_DealID IN INTEGER, -- Сделка
                                         v_Date   IN DATE,    -- Дата
                                         v_Action IN INTEGER  -- Действие
                                       )
  IS
     v_Deal            DDVDEAL_DBT%ROWTYPE;
     v_DlTurn          DDVDLTURN_DBT%ROWTYPE;
     M                 DDVDLTURN_DBT%ROWTYPE;
     v_exist_dlturnM   INTEGER;
     v_exist_fiturn    INTEGER := 0;
     v_GuarantyonDeal  CHAR;
  BEGIN

     -- Найдём сделку
     BEGIN
        SELECT * INTO v_Deal
          FROM ddvdeal_dbt
         WHERE t_ID = v_DealID;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
     END;

     IF( v_Deal.t_State = DVDEAL_STATE_CLOSE ) THEN
        RAISE_APPLICATION_ERROR(-20582, ''); -- Сделка закрыта
     END IF;

     SELECT count(1) INTO v_exist_fiturn
       FROM ddvfiturn_dbt turn
      WHERE turn.t_FIID        = v_Deal.t_FIID
        AND turn.t_Department  = v_Deal.t_Department
        AND turn.t_Broker      = v_Deal.t_Broker
        AND turn.t_ClientContr = v_Deal.t_ClientContr
        AND turn.t_GenAgrID    = v_Deal.t_GenAgrID
        AND turn.t_Date        = v_Date
        AND turn.t_State       = DVTURN_STATE_CLOSE;

     IF( v_exist_fiturn > 0 ) THEN
        RAISE_APPLICATION_ERROR(-20509,''); -- День по позиции закрыт
     END IF;

     -- Найдём dlturn
     BEGIN
        SELECT * INTO v_DlTurn
          FROM ddvdlturn_dbt
         WHERE t_DealID = v_DealID
           AND t_Date   = v_Date;

        EXCEPTION WHEN NO_DATA_FOUND THEN RETURN; -- Завершить процедуру
     END;

     -- Если ACTION == "Расчет итоговых сумм"
     IF( v_Action IN (DV_ACTION_CALCITOG, DV_ACTION_DISTRIBITOG) ) THEN
        -- Откатываем расчеты
        RSI_DV_DetachDealTurn(v_DealID, v_Date);

        UPDATE ddvdlturn_dbt K
           SET K.t_Margin           = (CASE WHEN K.t_SetMargin = CHR(88) THEN K.t_Margin ELSE K.t_OldMargin END),
               K.t_SetMargin        = (CASE WHEN K.t_SetMargin = CHR(88) THEN K.t_SetMargin ELSE K.t_OldSetMargin END),
               K.t_FairValueCalc    = (CASE WHEN K.t_SetFairValue = CHR(88) THEN K.t_FairValueCalc ELSE K.t_OldFairValueCalc END),
               K.t_FairValue        = (CASE WHEN K.t_SetFairValue = CHR(88) THEN K.t_FairValue ELSE K.t_OldFairValue END),
               K.t_SetFairValue     = (CASE WHEN K.t_SetFairValue = CHR(88) THEN K.t_SetFairValue ELSE K.t_OldSetFairValue END),
               K.t_OldMargin        = 0,
               K.t_OldSetMargin     = chr(0),
               K.t_OldFairValueCalc = chr(0),
               K.t_OldFairValue     = 0,
               K.t_OldSetFairValue  = chr(0),
               K.t_ActionType       = 0
         WHERE K.t_DealID = v_DealID
           AND K.t_Date   = v_Date;

     ELSIF (v_ACTION = DV_ACTION_EDIT) THEN -- предполагается полная очистка

        IF( v_DlTurn.t_SetMargin = CHR(88) OR v_DlTurn.t_SetGuaranty = CHR(88) OR v_DlTurn.t_SetFairValue = CHR(88) ) THEN
           RSI_DV_DetachDealTurn(v_DealID, v_Date);
        END IF;

        BEGIN
          v_exist_dlturnM := 1;

          SELECT * INTO M
            FROM ddvdlturn_dbt
           WHERE t_DealID = v_DealID
             AND t_Date   = v_Date;
        EXCEPTION WHEN NO_DATA_FOUND THEN v_exist_dlturnM := 0;
        END;

        IF( v_exist_dlturnM = 1 ) THEN  -- Если запись DDVDLTURN M с T_DEALID, T_DATE, равными заданным, все еще существует
           BEGIN
             SELECT pos.t_GuarantyonDeal INTO v_GuarantyonDeal
               FROM ddvfipos_dbt pos
              WHERE pos.t_FIID        = v_Deal.t_FIID
                AND pos.t_Department  = v_Deal.t_Department
                AND pos.t_Broker      = v_Deal.t_Broker
                AND pos.t_ClientContr = v_Deal.t_ClientContr
                AND pos.t_GenAgrID    = v_Deal.t_GenAgrID;
           EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20505,''); --Не открыта позиция по производному инструменту
           END;

           UPDATE ddvdlturn_dbt M
              SET M.t_Margin          =  0,
                  M.t_SetMargin       =  chr(0),
                  M.t_Guaranty        =  (CASE WHEN v_GuarantyonDeal = chr(88) THEN 0      ELSE M.t_Guaranty    END),
                  M.t_SetGuaranty     =  (CASE WHEN v_GuarantyonDeal = chr(88) THEN chr(0) ELSE M.t_SetGuaranty END),
                  M.t_FairValueCalc   =  chr(0),
                  M.t_FairValue       =  0,
                  M.t_SetFairValue    =  chr(0),
                  M.t_OldMargin        = 0,
                  M.t_OldSetMargin     = chr(0),
                  M.t_OldFairValueCalc = chr(0),
                  M.t_OldFairValue     = 0,
                  M.t_OldSetFairValue  = chr(0)
             WHERE t_DealID = v_DealID
              AND t_Date   = v_Date;
        END IF;

     ELSE
        --Действие не поддерживается
        RAISE_APPLICATION_ERROR(-20557,'');
     END IF;
  END; -- RSI_DV_RecoilInsertDealTurn

  -- Вставка рассчитанных данных по сделке. Используется в операции расчетов.
  PROCEDURE RSI_DV_CalcDealTurn( v_DealID        IN INTEGER, -- Сделка
                                 v_Date          IN DATE,    -- Дата
                                 v_Margin        IN NUMBER,  -- Вариационная маржа
                                 v_FairValue     IN NUMBER,  -- Справедливая стоимость
                                 v_CalcSum       IN INTEGER, -- Расчет суммы вариационной маржи 1=Да, 0=Нет
                                 v_CalcFairValue IN INTEGER, -- Расчет справедливой стоимости
                                 v_Action        IN INTEGER  -- Тип изменения итогов дня по сделке
                               )
  IS
  BEGIN
     RSI_DV_InsertDealTurn( v_DealID, v_Date, v_Margin, 0, (CASE WHEN v_CalcFairValue = 1 THEN CHR(88) ELSE CHR(0) END), v_FairValue, v_CalcSum, 0, v_CalcFairValue, v_Action );
  END; -- RSI_DV_CalcDealTurn
  
  -- Вставка данных по сделке. Используется при вводе новых итогов из скроллинга.
  PROCEDURE RSI_DV_GuarantDealTurn( v_DealID        IN INTEGER, -- Сделка
                                    v_Date          IN DATE,    -- Дата
                                    v_Guaranty      IN NUMBER,  -- гар.обеспечения
                                    v_Action        IN INTEGER  -- Тип изменения итогов дня по сделке
                                    )
  IS
  BEGIN
     RSI_DV_InsertDealTurn( v_DealID, v_Date, 0, v_Guaranty, CHR(0), 0,0,(CASE WHEN v_Guaranty <> 0 THEN 1 ELSE 0 END), 0, v_Action );
  END; -- RSI_DV_InputDealTurn

  -- Импорт данных по сделке
  PROCEDURE RSI_DV_ImportDealTurn( v_DealID          IN INTEGER, -- Сделка
                                   v_Date            IN DATE,    -- Дата
                                   v_Margin          IN NUMBER,  -- Вариационная маржа
                                   v_Guaranty        IN NUMBER,  -- Гарантийное обеспечение
                                   v_ImportMargin    IN INTEGER, -- Импорт вариационной маржи 1=Да, 0=Нет
                                   v_ImportGuaranty  IN INTEGER  -- Импорт гарантийного обеспечения 1=Да, 0=Нет
                                 )
  IS
  BEGIN
     RSI_DV_InsertDealTurn( v_DealID, v_Date, v_Margin, v_Guaranty, CHR(0), 0, v_ImportMargin, v_ImportGuaranty, 0, DV_ACTION_IMPORT );
  END; -- RSI_DV_ImportDealTurn

  -- Вставка данных по сделке. Используется при вводе новых итогов из скроллинга.
  PROCEDURE RSI_DV_InputDealTurn( v_DealID          IN INTEGER, -- Сделка
                                  v_Date            IN DATE,    -- Дата
                                  v_Margin          IN NUMBER,  -- Вариационная маржа
                                  v_Guaranty        IN NUMBER,  -- Гарантийное обеспечение
                                  v_FairValueCalc   IN CHAR,    -- Признак расчета справедливой стоимости
                                  v_FairValue       IN NUMBER   -- Справедливая стоимость
                                )
  IS
  BEGIN
     RSI_DV_InsertDealTurn( v_DealID, v_Date, v_Margin, v_Guaranty, v_FairValueCalc, v_FairValue,
                            (CASE WHEN v_Margin <> 0   THEN 1 ELSE 0 END),
                            (CASE WHEN v_Guaranty <> 0 THEN 1 ELSE 0 END),
                            (CASE WHEN v_FairValueCalc = CHR(88) THEN 1 ELSE 0 END),
                            DV_ACTION_EDIT );
  END; -- RSI_DV_InputDealTurn

  -- Редактирование данных по сделке. Используется при вводе итогов из скроллинга или редактировании итогов.
  PROCEDURE RSI_DV_EditDealTurn( v_DealID         IN INTEGER, -- Сделка
                                 v_Date           IN DATE,    -- Дата
                                 v_Margin         IN NUMBER,  -- Вариационная маржа
                                 v_Guaranty       IN NUMBER,  -- Гарантийное обеспечение
                                 v_FairValueCalc  IN CHAR,    -- Признак расчета справедливой стоимости
                                 v_FairValue      IN NUMBER,  -- Справедливая стоимость
                                 v_EditMargin     IN INTEGER, -- Редактирование вариационной маржи 1=Да, 0=Нет
                                 v_EditGuaranty   IN INTEGER, -- Редактирование гарантийного обеспечения 1=Да, 0=Нет
                                 v_EditFairValue  IN INTEGER  -- Редактирование справедливой стоимости 1=Да, 0=Нет
                               )
  IS
     v_Deal            DDVDEAL_DBT%ROWTYPE;
     v_GuarantyonDeal  CHAR;
     v_exist_dlturn    INTEGER;
     v_exist_fiturn    INTEGER := 0;
     T                 DDVDLTURN_DBT%ROWTYPE;
  BEGIN

     -- Найдём сделку
     BEGIN
        SELECT * INTO v_Deal
          FROM ddvdeal_dbt
         WHERE t_ID = v_DealID;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
     END;

     IF( v_Deal.t_State = DVDEAL_STATE_CLOSE ) THEN
        RAISE_APPLICATION_ERROR(-20582, ''); -- Сделка закрыта
     END IF;

     SELECT count(1) INTO v_exist_fiturn
       FROM ddvfiturn_dbt turn
      WHERE turn.t_FIID        = v_Deal.t_FIID
        AND turn.t_Department  = v_Deal.t_Department
        AND turn.t_Broker      = v_Deal.t_Broker
        AND turn.t_ClientContr = v_Deal.t_ClientContr
        AND turn.t_GenAgrID    = v_Deal.t_GenAgrID
        AND turn.t_Date        = v_Date
        AND turn.t_State       = DVTURN_STATE_CLOSE;

     IF( v_exist_fiturn > 0 ) THEN
        RAISE_APPLICATION_ERROR(-20509,''); -- День по позиции закрыт
     END IF;

     BEGIN
       SELECT pos.t_GuarantyonDeal INTO v_GuarantyonDeal
         FROM ddvfipos_dbt pos
        WHERE pos.t_FIID        = v_Deal.t_FIID
          AND pos.t_Department  = v_Deal.t_Department
          AND pos.t_Broker      = v_Deal.t_Broker
          AND pos.t_ClientContr = v_Deal.t_ClientContr
          AND pos.t_GenAgrID    = v_Deal.t_GenAgrID;
     EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20505,''); --Не открыта позиция по производному инструменту
     END;

     IF( ((v_Margin = 0) AND (v_Guaranty = 0) AND (v_FairValueCalc = CHR(0))) OR
         ((v_EditMargin = 0) AND (v_EditGuaranty = 0) AND (v_EditFairValue = 0))
       ) THEN
        RAISE_APPLICATION_ERROR(-20587,''); -- Неверные параметры редактирования данных по сделке
     END IF;

     BEGIN
       v_exist_dlturn := 1;

       SELECT * INTO T
         FROM ddvdlturn_dbt
        WHERE t_DealID = v_DealID
          AND t_Date   = v_Date;
     EXCEPTION WHEN NO_DATA_FOUND THEN v_exist_dlturn := 0;
     END;

     /*IF( v_exist_dlturn = 0 OR (T.t_SetMargin = CHR(0) AND T.t_SetGuaranty = CHR(0) AND T.t_SetFairValue = CHR(0)) ) THEN
        RSI_DV_AttachDealTurn(v_DealID, v_Date);
     END IF;*/

     UPDATE ddvdlturn_dbt N
        SET N.t_Margin          =  (CASE WHEN v_EditMargin    = 1 THEN v_Margin    ELSE N.t_Margin END),
            N.t_SetMargin       =  (CASE WHEN v_EditMargin    = 1 THEN CHR(88)     ELSE N.t_SetMargin END),
            N.t_Guaranty        =  (CASE WHEN v_EditGuaranty  = 1 THEN v_Guaranty  ELSE N.t_Guaranty END),
            N.t_SetGuaranty     =  (CASE WHEN v_EditGuaranty  = 1 THEN CHR(88)     ELSE N.t_SetGuaranty END),
            N.t_FairValueCalc   =  (CASE WHEN v_EditFairValue = 1 THEN v_FairValueCalc ELSE N.t_FairValueCalc END),
            N.t_FairValue       =  (CASE WHEN v_EditFairValue = 1 THEN (CASE WHEN v_FairValueCalc = 'X' THEN v_FairValue ELSE 0 END) ELSE N.t_FairValue END),
            N.t_SetFairValue    =  (CASE WHEN v_EditFairValue = 1 THEN CHR(88)     ELSE N.t_SetFairValue END),
            N.t_ActionType      =   DV_ACTION_EDIT
      WHERE N.t_DealID = v_DealID
        AND N.t_Date   = v_Date;
  END; -- RSI_DV_EditDealTurn

 -- Массовая вставка данных по сервисной операции
  PROCEDURE RSI_DV_MassSaveServAction( v_ID_Operation IN INTEGER, -- ID операции расчётов
                                       v_ID_Step      IN INTEGER  -- ID шага расчётов
                                     )
  IS
     CURSOR c_DlServActionTmp IS SELECT t_DocKind, t_DocID, t_Action, t_ActionDate FROM DDLSERVACTION_TMP;
  BEGIN
     FOR DlServActionTmp IN c_DlServActionTmp LOOP
       INSERT INTO DDLSERVACTION_DBT( t_ID_Operation, t_ID_Step, t_DocKind, t_DocID, t_Action, t_ActionDate )
         VALUES( v_ID_Operation, v_ID_Step, DlServActionTmp.t_DocKind, DlServActionTmp.t_DocID, DlServActionTmp.t_Action, DlServActionTmp.t_ActionDate );
     END LOOP;
  END; -- RSI_DV_MassCalcDealTurn

    -- Массовый откат данных по сервисной операции
  PROCEDURE RSI_DV_MassRecoilServAction  ( v_ID_Operation IN INTEGER, -- ID операции расчётов
                                           v_ID_Step      IN INTEGER  -- ID шага расчётов
                                         )
  IS
  BEGIN
    DELETE FROM DDLSERVACTION_DBT
      WHERE t_ID_Operation = v_ID_Operation
        AND t_ID_Step      = v_ID_Step;
  END; -- RSI_DV_MassCalcDealTurn

  -- Массовая вставка связей проводок со сделками срочного рынка
  PROCEDURE RSI_DV_MassInsertDvdealTrnLink  ( v_ID_Operation IN INTEGER, -- ID операции расчётов
                                              v_ID_Step      IN INTEGER  -- ID шага расчётов
                                           )
  IS
  BEGIN
    INSERT INTO DDVDEALTRN_DBT
    SELECT * FROM DDVDEALTRN_TMP tmp
      WHERE tmp.t_ID_Operation = v_ID_Operation
        AND tmp.t_ID_Step      = v_ID_Step;
  END; -- RSI_DV_MassInsertDvdealTrnLink
 
  -- Массовый откат связей проводок со сделками срочного рынка
  PROCEDURE RSI_DV_MassRecoilDvdealTrnLink  ( v_ID_Operation IN INTEGER, -- ID операции расчётов
                                              v_ID_Step      IN INTEGER  -- ID шага расчётов
                                           )
  IS
  BEGIN
    DELETE FROM DDVDEALTRN_DBT
      WHERE t_ID_Operation = v_ID_Operation
        AND t_ID_Step      = v_ID_Step;
  END; -- RSI_DV_MassRecoilDvdealTrnLink

  -- Изменение признака обработки биржевой информации по процентному платежу
  PROCEDURE RSI_DV_SetPmGrGt  ( v_DealCode IN VARCHAR2, -- Код сделки
                                v_Date     IN DATE,     -- Дата изменения данных ПП
                                v_Action   IN INTEGER   -- 1 - установка признака, 0 - снятие признака
                              )
  IS
    v_ImportedChr CHAR;
  BEGIN
     IF (v_Action = 1) THEN --Установка
       v_ImportedChr := chr(88);
     ELSE
       v_ImportedChr := chr(0);
     END IF;
     UPDATE DDVNPMGRGT_DBT
        SET t_Imported = v_ImportedChr
      WHERE t_DealCode = v_DealCode
        AND t_ImpDate  = v_Date;
  END; -- RSI_DV_SetPmGrGt
 
  -- Выполняется при изменении сумм в итогах дня по сделке. Вызывается внутри триггеров или других ХП.
  -- Выполняет синхронное обновление итогов дня по позиции.
  PROCEDURE RSI_DV_OnUpdateDealTurn( v_FIID                IN INTEGER, -- производный инструмент
                                     v_Department          IN INTEGER, -- Филиал
                                     v_Broker              IN INTEGER, -- Брокер
                                     v_ClientContr         IN INTEGER, -- Договор с клиентом
                                     v_Date                IN DATE,    -- Дата
                                     v_Margin              IN NUMBER,  -- Вариационная маржа
                                     v_Guaranty            IN NUMBER,  -- Гарантийное обеспечение
                                     v_FairValue           IN NUMBER,  -- Справедливая стоимость
                                     v_FairValueCalcNumber IN INTEGER, -- Изменение числа расчетов справедливой стоимости -1/0/+1
                                     v_CreateErr           IN BOOLEAN, -- Генерировать ошибку при отсутствии записи
                                     v_GenAgrID            IN INTEGER  -- ГС
                                   )
  IS
     v_state_fipos          INTEGER;
     v_GuarantyonDeal_fipos CHAR;
     v_exist_fiturn         INTEGER;
     v_state_fiturn         INTEGER;
     Vv_Guaranty            NUMBER;
  BEGIN

     BEGIN
       SELECT pos.t_State, pos.t_GuarantyonDeal INTO v_state_fipos, v_GuarantyonDeal_fipos
         FROM ddvfipos_dbt pos
        WHERE pos.t_FIID        = v_FIID
          AND pos.t_Department  = v_Department
          AND pos.t_Broker      = v_Broker
          AND pos.t_ClientContr = v_ClientContr
          AND pos.t_GenAgrID    = v_GenAgrID;
     EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20505,''); -- Не открыта позиция по производному инструменту
     END;

     IF( v_state_fipos = DVPOS_STATE_CLOSE ) THEN
        RAISE_APPLICATION_ERROR(-20506,''); -- Позиция по производному инструменту закрыта
     END IF;


     BEGIN
       v_exist_fiturn := 1;

       SELECT t_State INTO v_state_fiturn
         FROM ddvfiturn_dbt
        WHERE t_FIID        = v_FIID
          AND t_Department  = v_Department
          AND t_Broker      = v_Broker
          AND t_ClientContr = v_ClientContr
          AND t_GenAgrID    = v_GenAgrID
          AND t_Date        = v_Date;
     EXCEPTION WHEN NO_DATA_FOUND THEN v_exist_fiturn := 0;
     END;

     IF( v_exist_fiturn != 1 AND v_CreateErr) THEN
        RAISE_APPLICATION_ERROR(-20512,''); -- Не открыт день по позиции
     END IF;

     IF( v_state_fiturn = DVTURN_STATE_CLOSE ) THEN
        RAISE_APPLICATION_ERROR(-20509,''); -- День по позиции закрыт
     END IF;

     IF v_exist_fiturn = 1 THEN
       IF( v_GuarantyonDeal_fipos = chr(88) ) THEN
          Vv_Guaranty := v_Guaranty;
       ELSE
          Vv_Guaranty := 0;
       END IF;

       UPDATE ddvfiturn_dbt
          SET t_Margin    = t_Margin    + v_Margin,
              t_Guaranty  = t_Guaranty  + Vv_Guaranty,
              t_FairValue = t_FairValue + v_FairValue,
              t_FairValueCalcNumber = t_FairValueCalcNumber + v_FairValueCalcNumber,
              t_FairValueCalc = (CASE WHEN ((t_FairValueCalcNumber + v_FairValueCalcNumber) > 0) THEN CHR(88) ELSE CHR(0) END)
        WHERE t_FIID        = v_FIID
          AND t_Department  = v_Department
          AND t_Broker      = v_Broker
          AND t_ClientContr = v_ClientContr
          AND t_GenAgrID    = v_GenAgrID
          AND t_Date        = v_Date;
      END IF;
  END; -- RSI_DV_OnUpdateDealTurn

  -- Переоценка стоимости по сделке. Используется в процедуре переоценки не в ДУ.
  PROCEDURE RSI_DV_OvervalueDealTurn( v_DealID   IN INTEGER, -- Сделка
                                      v_Date     IN DATE,    -- Дата
                                      v_DealCost IN NUMBER,  -- Стоимость неисполненных контрактов
                                      v_OperID   IN INTEGER, -- ID операции переоценки
                                      v_Flag     IN INTEGER  -- Вид переоценки
                                    )
  IS
     v_exist_operps  INTEGER := 0;
     v_exist_dlturn  INTEGER := 0;
     v_exist_fiturn  INTEGER := 0;
     v_D             DDVDEAL_DBT%ROWTYPE;
  BEGIN

     IF( DV_Setting_AccExContracts = 0 ) THEN -- По позиции
        RAISE_APPLICATION_ERROR(-20595,''); -- Установлен учет биржевых контрактов по позиции
     END IF;

     BEGIN
        SELECT * INTO v_D
          FROM DDVDEAL_DBT
         WHERE t_ID = v_DealID;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
     END;

     IF( v_D.t_PosAcc = CHR(0) ) THEN
        RAISE_APPLICATION_ERROR(-20596, ''); -- Сделка не учтена по позиции
     END IF;

     SELECT count(1) INTO v_exist_operps
       FROM ddvoperps_dbt
      WHERE t_OperID  = v_OperID
        AND t_DocKind = DL_DVDEAL
        AND t_DocID   = v_DealID
        AND t_Flag    = v_Flag;

     IF( v_exist_operps <= 0 ) THEN
        RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
     END IF;

     UPDATE ddvoperps_dbt
        SET t_Summ1 = v_D.t_DealCost
      WHERE t_OperID  = v_OperID
        AND t_DocKind = DL_DVDEAL
        AND t_DocID   = v_DealID
        AND t_Flag    = v_Flag;

     UPDATE ddvdeal_dbt
        SET t_DealCost = v_DealCost
      WHERE t_ID = v_DealID;

/**/
     -- чтобы не отвалилась
     UPDATE ddvfiturn_dbt
        SET t_State = DVTURN_STATE_OPEN
      WHERE t_FIID        = v_D.t_FIID
        AND t_Department  = v_D.t_Department
        AND t_Broker      = v_D.t_Broker
        AND t_ClientContr = v_D.t_ClientContr
        AND t_GenAgrID    = v_D.t_GenAgrID
        AND t_Date        = v_Date;
/**/

     RSI_DV_AttachDealTurn(v_D.t_ID, v_Date);

     SELECT count(1) INTO v_exist_dlturn
       FROM ddvdlturn_dbt
      WHERE t_DealID = v_DealID
        AND t_Date   = v_Date;

     IF( v_exist_dlturn <= 0 ) THEN
        RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
     END IF;

     UPDATE ddvdlturn_dbt
        SET t_DealCost = v_DealCost
      WHERE t_DealID = v_DealID
        AND t_Date   = v_Date;

/*
     -- чтобы не отвалилась
     UPDATE ddvfiturn_dbt
        SET t_State = DVTURN_STATE_OPEN
      WHERE t_FIID        = v_D.t_FIID
        AND t_Department  = v_D.t_Department
        AND t_Broker      = v_D.t_Broker
        AND t_ClientContr = v_D.t_ClientContr
        AND t_Date        = v_Date;
*/

     RSI_DV_AttachPositionTurn(v_D.t_FIID, v_D.t_Department, v_D.t_Broker, v_D.t_ClientContr, v_Date, v_D.t_GenAgrID);

     SELECT count(1) INTO v_exist_fiturn
       FROM ddvfiturn_dbt
      WHERE t_FIID        = v_D.t_FIID
        AND t_Department  = v_D.t_Department
        AND t_Broker      = v_D.t_Broker
        AND t_ClientContr = v_D.t_ClientContr
        AND t_GenAgrID    = v_D.t_GenAgrID
        AND t_Date        = v_Date;

     IF( v_exist_fiturn <= 0 ) THEN
        RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
     END IF;

     IF( v_D.t_Type in('B', 'D') ) THEN
        UPDATE ddvfiturn_dbt
           SET t_State = DVTURN_STATE_CLOSE,
               t_LongPositionCost = t_LongPositionCost + v_DealCost - v_D.t_DealCost
         WHERE t_FIID        = v_D.t_FIID
           AND t_Department  = v_D.t_Department
           AND t_Broker      = v_D.t_Broker
           AND t_ClientContr = v_D.t_ClientContr
           AND t_GenAgrID    = v_D.t_GenAgrID
           AND t_Date        = v_Date;
     ELSIF( v_D.t_Type in('S', 'G') ) THEN
        UPDATE ddvfiturn_dbt
           SET t_State = DVTURN_STATE_CLOSE,
               t_ShortPositionCost = t_ShortPositionCost + v_DealCost - v_D.t_DealCost
         WHERE t_FIID        = v_D.t_FIID
           AND t_Department  = v_D.t_Department
           AND t_Broker      = v_D.t_Broker
           AND t_ClientContr = v_D.t_ClientContr
           AND t_GenAgrID    = v_D.t_GenAgrID
           AND t_Date        = v_Date;
     END IF;

  END; -- RSI_DV_OvervalueDealTurn

  -- Откат переоценки стоимости по сделке. Используется при откате переоценки не в ДУ.
  PROCEDURE RSI_DV_RecoilOvervalueDealTurn( v_DealID   IN INTEGER, -- Сделка
                                            v_Date     IN DATE,    -- Дата
                                            v_OperID   IN INTEGER, -- ID операции переоценки
                                            v_Flag     IN INTEGER  -- Вид переоценки
                                          )
  IS
     v_Sum           NUMBER := 0;
     v_exist_fiturn  INTEGER := 0;
     v_D             DDVDEAL_DBT%ROWTYPE;
  BEGIN

     IF( DV_Setting_AccExContracts = 0 ) THEN -- По позиции
        RAISE_APPLICATION_ERROR(-20595,''); -- Установлен учет биржевых контрактов по позиции
     END IF;

     BEGIN
        SELECT * INTO v_D
          FROM DDVDEAL_DBT
         WHERE t_ID = v_DealID;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
     END;

     IF( v_D.t_PosAcc = CHR(0) ) THEN
        RAISE_APPLICATION_ERROR(-20596, ''); -- Сделка не учтена по позиции
     END IF;

     BEGIN
        SELECT t_Summ1 INTO v_Sum
         FROM ddvoperps_dbt
        WHERE t_OperID  = v_OperID
          AND t_DocKind = DL_DVDEAL
          AND t_DocID   = v_DealID
          AND t_Flag    = v_Flag;
     EXCEPTION WHEN OTHERS THEN v_Sum := 0;
     END;

     UPDATE ddvdeal_dbt
        SET t_DealCost = v_Sum
      WHERE t_ID = v_DealID;

     SELECT count(1) INTO v_exist_fiturn
       FROM ddvfiturn_dbt
      WHERE t_FIID        = v_D.t_FIID
        AND t_Department  = v_D.t_Department
        AND t_Broker      = v_D.t_Broker
        AND t_ClientContr = v_D.t_ClientContr
        AND t_GenAgrID    = v_D.t_GenAgrID
        AND t_Date        = v_Date;

     IF( v_exist_fiturn <= 0 ) THEN
        RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
     END IF;

     -- чтобы не отвалилась
     UPDATE ddvfiturn_dbt
        SET t_State = DVTURN_STATE_OPEN
      WHERE t_FIID        = v_D.t_FIID
        AND t_Department  = v_D.t_Department
        AND t_Broker      = v_D.t_Broker
        AND t_ClientContr = v_D.t_ClientContr
        AND t_GenAgrID    = v_D.t_GenAgrID
        AND t_Date        = v_Date;

     RSI_DV_DetachDealTurn(v_DealID, v_Date);

     UPDATE ddvdlturn_dbt
        SET t_DealCost = v_Sum
      WHERE t_DealID = v_DealID
        AND t_Date   = v_Date;

     RSI_DV_DetachPositionTurn(v_D.t_FIID, v_D.t_Department, v_D.t_Broker, v_D.t_ClientContr, v_Date, v_D.t_GenAgrID);

     IF( v_D.t_Type in('B', 'D') ) THEN
        UPDATE ddvfiturn_dbt
           SET t_State = DVTURN_STATE_CLOSE,
               t_LongPositionCost = t_LongPositionCost + v_Sum - v_D.t_DealCost
         WHERE t_FIID        = v_D.t_FIID
           AND t_Department  = v_D.t_Department
           AND t_Broker      = v_D.t_Broker
           AND t_ClientContr = v_D.t_ClientContr
           AND t_GenAgrID    = v_D.t_GenAgrID
           AND t_Date        = v_Date;
     ELSIF( v_D.t_Type in('S', 'G') ) THEN
        UPDATE ddvfiturn_dbt
           SET t_State = DVTURN_STATE_CLOSE,
               t_ShortPositionCost = t_ShortPositionCost + v_Sum - v_D.t_DealCost
         WHERE t_FIID        = v_D.t_FIID
           AND t_Department  = v_D.t_Department
           AND t_Broker      = v_D.t_Broker
           AND t_ClientContr = v_D.t_ClientContr
           AND t_GenAgrID    = v_D.t_GenAgrID
           AND t_Date        = v_Date;
     END IF;

  END; -- RSI_DV_RecoilOvervalueDealTurn

  -- Закрытие сделок
  -- Выполняется в операции расчетов
  PROCEDURE RSI_DV_CloseDeals( v_DvoperID   IN INTEGER  /* Операция расчётов*/  )
  IS
   v_PartyKind     INTEGER := 0; -- Вид контрагента по расчетам
   v_Party         INTEGER := 0; -- Контрагент
   v_PartyContr    INTEGER := 0; -- Договор с контрагентом
   v_Department    INTEGER := 0; -- Филиал
   v_Date          DATE    := TO_DATE('01.01.0001', 'dd.mm.yyyy');    -- Дата
   v_IsTrust       CHAR    := CHR(0);    -- Признак ДУ
   v_Flag1         INTEGER := 0;
   v_GenAgrID      INTEGER := 0;  -- ГС
  BEGIN
     IF( DV_Setting_AccExContracts = 0 ) THEN -- По позиции
        RAISE_APPLICATION_ERROR(-20595,''); -- Установлен учет биржевых контрактов по позиции
     END IF;

     SELECT t_PartyKind, t_Party, t_PartyContr, t_Department, t_Date, t_Flag1, t_GenAgrID
      INTO v_PartyKind, v_Party, v_PartyContr, v_Department, v_Date, v_Flag1, v_GenAgrID
       FROM ddvoper_dbt
        WHERE t_ID = v_DvoperID AND t_DocKind = 194;

     IF(v_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) THEN
        v_IsTrust := chr(88);
     ELSE
        v_IsTrust := chr(0);
     END IF;

     UPDATE ddvdeal_dbt D
        SET D.t_State = DVDEAL_STATE_CLOSE
      WHERE D.t_ID in( SELECT sD.t_ID
                         FROM ddvdeal_dbt sD, ddvdllnk_dbt sL, ddvdeal_dbt sE
                        WHERE sD.t_Type in('B', 'D', 'S', 'G')
                          AND sE.t_Type in('E', 'R')
                          AND sD.t_Department = v_Department
                          AND sD.t_IsTrust    = v_IsTrust
                          AND sD.t_GenAgrID   = v_GenAgrID
                          AND sD.t_ID         = sL.t_DealID
                          AND sE.t_ID         = sL.t_ExecID
                          AND sE.t_Date_clr   = v_Date
                          AND sL.t_Date       = sE.t_Date_clr
                          AND ((v_PartyKind = 3  AND v_Party = (SELECT fin.t_Issuer FROM dfininstr_dbt fin WHERE fin.t_FIID = sD.t_FIID) AND sD.t_Broker = -1)  OR
                               (v_PartyKind = 22 AND sD.t_Broker = v_Party AND sD.t_BrokerContr = v_PartyContr))
                          AND sD.t_State      = DVDEAL_STATE_OPEN
                          AND sD.t_Amount     = sD.t_Execution
                          AND (((v_Flag1 = ALG_SP_MONEY_SOURCE_OWN) AND (sD.t_Client <= 0)) OR
                               ((v_Flag1 = ALG_SP_MONEY_SOURCE_CLIENT) AND (sD.t_Client >= 1)) OR
                               ((v_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) AND (sD.t_IsTrust = chr(88)))) );

      UPDATE ddvdeal_dbt D
        SET D.t_State = DVDEAL_STATE_CLOSE
      WHERE D.t_ID in( SELECT sD.t_ID
                         FROM ddvdeal_dbt sD, ddvdllnk_dbt sL
                        WHERE sD.t_Type in('B', 'D', 'S', 'G')
                          AND sD.t_State      = DVDEAL_STATE_OPEN
                          AND sD.t_Amount     = sD.t_Execution
                          AND ( (sL.t_DealID = sD.t_ID) or (sL.t_SaleDealID = sD.t_ID) )
                          AND sL.t_Kind = 1
                          AND sL.t_DvoperID = v_dvoperid
                           );

  END; -- RSI_DV_CloseDeals

  -- Откат закрытия сделок
  -- Выполняется при откате  операции расчетов
  PROCEDURE RSI_DV_RecoilCloseDeals( v_DvoperID   IN INTEGER  /*Операция расчётов*/ )
  IS
   v_PartyKind     INTEGER := 0; -- Вид контрагента по расчетам
   v_Party         INTEGER := 0; -- Контрагент
   v_PartyContr    INTEGER := 0; -- Договор с контрагентом
   v_Department    INTEGER := 0; -- Филиал
   v_Date          DATE    := TO_DATE('01.01.0001', 'dd.mm.yyyy');    -- Дата
   v_IsTrust       CHAR    := CHR(0);    -- Признак ДУ
   v_Flag1         INTEGER := 0;
   v_GenAgrID      INTEGER := 0;  -- ГС
  BEGIN
    IF( DV_Setting_AccExContracts = 0 ) THEN -- По позиции
       RAISE_APPLICATION_ERROR(-20595,''); -- Установлен учет биржевых контрактов по позиции
    END IF;

    SELECT t_PartyKind, t_Party, t_PartyContr, t_Department, t_Date, t_Flag1, t_GenAgrID
      INTO v_PartyKind, v_Party, v_PartyContr, v_Department, v_Date, v_Flag1, v_GenAgrID
       FROM ddvoper_dbt
        WHERE t_ID = v_DvoperID AND t_DocKind = 194;

    IF(v_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) THEN
       v_IsTrust := chr(88);
    ELSE
       v_IsTrust := chr(0);
    END IF;

    UPDATE ddvdeal_dbt D
       SET D.t_State = DVDEAL_STATE_OPEN
     WHERE D.t_ID in( SELECT sD.t_ID
                        FROM ddvdeal_dbt sD, ddvdllnk_dbt sL, ddvdeal_dbt sE
                       WHERE sD.t_Type in('B', 'D', 'S', 'G')
                         AND sE.t_Type in('E', 'R')
                         AND sD.t_Department = v_Department
                         AND sD.t_IsTrust    = v_IsTrust
                         AND sD.t_GenAgrID   = v_GenAgrID
                         AND sD.t_ID         = sL.t_DealID
                         AND sE.t_ID         = sL.t_ExecID
                         AND sE.t_Date_clr   = v_Date
                         AND sL.t_Date       = sE.t_Date_clr
                         AND ((v_PartyKind = 3  AND v_Party = (SELECT fin.t_Issuer FROM dfininstr_dbt fin WHERE fin.t_FIID = sD.t_FIID) AND sD.t_Broker = -1)  OR
                              (v_PartyKind = 22 AND sD.t_Broker = v_Party AND sD.t_BrokerContr = v_PartyContr))
                         AND sD.t_State      = DVDEAL_STATE_CLOSE
                         AND sD.t_Amount     = sD.t_Execution
                         AND (((v_Flag1 = ALG_SP_MONEY_SOURCE_OWN) AND (sD.t_Client <= 0)) OR
                              ((v_Flag1 = ALG_SP_MONEY_SOURCE_CLIENT) AND (sD.t_Client >= 1)) OR
                              ((v_Flag1 = ALG_SP_MONEY_SOURCE_TRUST) AND (sD.t_IsTrust = chr(88)))) );
      UPDATE ddvdeal_dbt D
        SET D.t_State = DVDEAL_STATE_OPEN
      WHERE D.t_ID in( SELECT sD.t_ID
                         FROM ddvdeal_dbt sD, ddvdllnk_dbt sL
                        WHERE sD.t_Type in('B', 'D', 'S', 'G')
                          AND sD.t_State      = DVDEAL_STATE_CLOSE
                          AND sD.t_Amount     = sD.t_Execution
                          AND ( (sL.t_DealID = sD.t_ID) or (sL.t_SaleDealID = sD.t_ID) )
                          AND sL.t_Kind = 1
                          AND sL.t_DvoperID = v_dvoperid
                       );
  END; -- RSI_DV_RecoilCloseDeals

  -- Изменение статуса внебиржевой сделки
  PROCEDURE RSI_DV_SetStateNDeal( v_DealID IN INTEGER, -- ID сделки
                                  v_State  IN INTEGER  -- Статус
                                )
  IS
  BEGIN
     UPDATE ddvndeal_dbt D
        SET D.t_State = v_State
      WHERE D.t_ID = v_DealID;
  END; -- RSI_DV_SetStateNDeal

  -- Выполняет сохранение архивных данных промежуточного платежа
  PROCEDURE RSI_DVSavePm( pPmID         IN NUMBER, -- Изменяемый платеж
                          pID_Operation IN NUMBER, -- Операция
                          pID_Step      IN NUMBER, -- Шаг операции
                          pChangeDate   IN DATE,   -- Дата изменения
                          pAction       IN NUMBER  -- Действие
                        )
  IS
     v_pmgr        DDVNPMGR_DBT%ROWTYPE;
     v_pmgrbc      DDVNPMGRBC_DBT%ROWTYPE;
     v_ExistChange NUMBER := 0;
  BEGIN
     RSI_DV_InitError();

     -- найдём строку графика
     BEGIN
       SELECT * INTO v_pmgr
         FROM DDVNPMGR_DBT
        WHERE t_ID = pPmID;
     EXCEPTION
        WHEN OTHERS THEN RSI_DV_SetError(-20604); -- Не найдена строка графика промежуточных платежей
     END;

     -- проверка более поздних изменений
     SELECT count(1) INTO v_ExistChange
       FROM DDVNPMGRBC_DBT
      WHERE t_PmGrID      = pPmID
        AND t_ChangeDate  > pChangeDate;

     IF( v_ExistChange > 0 ) THEN
        RSI_DV_SetError(-20605); -- По платежу были операции за более позднюю дату
     END IF;

     -- вставка истории
     v_pmgrbc.t_ID               := 0;
     v_pmgrbc.t_PmGrID           := v_pmgr.t_ID;
     v_pmgrbc.t_Instance         := v_pmgr.t_Instance;
     v_pmgrbc.t_Action           := pAction;
     v_pmgrbc.t_ID_Operation     := pID_Operation;
     v_pmgrbc.t_ID_Step          := pID_Step;
     v_pmgrbc.t_ChangeDate       := pChangeDate;
     v_pmgrbc.t_DemandSum        := v_pmgr.t_DemandSum;
     v_pmgrbc.t_LiabilitySum     := v_pmgr.t_LiabilitySum;
     v_pmgrbc.t_PaymentSum       := v_pmgr.t_PaymentSum;
     v_pmgrbc.t_TransferDate     := v_pmgr.t_TransferDate;
     v_pmgrbc.t_DemandAccount    := v_pmgr.t_DemandAccount;
     v_pmgrbc.t_LiabilityAccount := v_pmgr.t_LiabilityAccount;
     v_pmgrbc.t_FloatRateValue   := v_pmgr.t_FloatRateValue;

     INSERT INTO DDVNPMGRBC_DBT VALUES v_pmgrbc;

     -- обновим instance
     UPDATE DDVNPMGR_DBT
        SET t_Instance = t_Instance + 1
      WHERE t_ID = pPmID;
  END; -- RSI_DVSavePm

  -- Выполняет восстановление платежа по архивным данным
  PROCEDURE RSI_RestorePM( pID_Operation IN NUMBER, -- Операция
                           pID_Step      IN NUMBER  -- Шаг операции
                         )
  IS
     v_pmgr DDVNPMGR_DBT%ROWTYPE;
     CURSOR c_PmGrBC IS SELECT *
                          FROM DDVNPMGRBC_DBT
                         WHERE t_ID_Operation = pID_Operation
                           AND t_ID_Step      = pID_Step
                        ORDER BY t_Instance DESC;
  BEGIN
     RSI_DV_InitError();

     IF( (pID_Operation = 0) AND (pID_Step = 0)) THEN
        RETURN;
     END IF;

     FOR PmGrBC IN c_PmGrBC LOOP
        -- найдём строку графика
        BEGIN
          SELECT * INTO v_pmgr
            FROM DDVNPMGR_DBT
           WHERE t_ID = PmGrBC.t_PmGrID;
        EXCEPTION
           WHEN OTHERS THEN RSI_DV_SetError(-20604); -- Не найдена строка графика промежуточных платежей
        END;

        -- проверка более поздних изменений
        IF( v_pmgr.t_Instance != (PmGrBC.t_Instance + 1) ) THEN
           RSI_DV_SetError(-20606); -- Откатываемая операция по платежу не является последней
        END IF;

        -- обновим строку графика из истории
        UPDATE DDVNPMGR_DBT
           SET t_DemandSum        = PmGrBC.t_DemandSum,
               t_LiabilitySum     = PmGrBC.t_LiabilitySum,
               t_PaymentSum       = PmGrBC.t_PaymentSum,
               t_TransferDate     = PmGrBC.t_TransferDate,
               t_DemandAccount    = PmGrBC.t_DemandAccount,
               t_LiabilityAccount = PmGrBC.t_LiabilityAccount,
               t_FloatRateValue   = PmGrBC.t_FloatRateValue,
               t_Instance         = v_pmgr.t_Instance - 1
         WHERE t_ID = v_pmgr.t_ID;

        -- удалим запись истории
        DELETE FROM DDVNPMGRBC_DBT
         WHERE t_ID = PmGrBC.t_ID;
     END LOOP;
  END; -- RSI_RestorePM

  -- Переоценка промежуточного платежа
  PROCEDURE RSI_DVOvervaluePm( pPmID           IN NUMBER, -- Изменяемый платеж
                               pID_Operation   IN NUMBER, -- Операция
                               pID_Step        IN NUMBER, -- Шаг операции
                               pChangeDate     IN DATE,   -- Дата изменения
                               pDemandSum      IN NUMBER, -- Новая сумма требования
                               pLiabilitySum   IN NUMBER, -- Новая сумма обязательства
                               pPaymentSum     IN NUMBER, -- Новая сумма платежа
                               pFloatRateValue IN NUMBER, -- Новая процентная ставка
                               pAction         IN NUMBER  -- Действие
                             )
  IS
  BEGIN
     RSI_DV_InitError();

     -- сохраним в историю
     RSI_DVSavePm(pPmID, pID_Operation, pID_Step, pChangeDate, pAction);

     -- обновим строку графика
     UPDATE DDVNPMGR_DBT
        SET t_DemandSum      = pDemandSum,
            t_LiabilitySum   = pLiabilitySum,
            t_PaymentSum     = pPaymentSum,
            t_FloatRateValue = pFloatRateValue
      WHERE t_ID = pPmID;
  END; -- RSI_DVOvervaluePm

  -- Перенос по срокам промежуточного платежа
  PROCEDURE RSI_DVTransferPm( pPmID             IN NUMBER,   -- Изменяемый платеж
                              pID_Operation     IN NUMBER,   -- Операция
                              pID_Step          IN NUMBER,   -- Шаг операции
                              pChangeDate       IN DATE,     -- Дата изменения
                              pTransferDate     IN DATE,     -- Новая дата переноса
                              pDemandAccount    IN VARCHAR2, -- Новый счет требований
                              pLiabilityAccount IN VARCHAR2, -- Новый счет обязательства
                              pAction           IN NUMBER    -- Вид изменения в истории
                            )
  IS
  BEGIN
     RSI_DV_InitError();

     -- сохраним в историю
     RSI_DVSavePm(pPmID, pID_Operation, pID_Step, pChangeDate, pAction/*DV_PMGR_CHANGE_TRANSFER*/);

     -- обновим строку графика
     UPDATE DDVNPMGR_DBT
        SET t_TransferDate     = pTransferDate,
            t_DemandAccount    = pDemandAccount,
            t_LiabilityAccount = pLiabilityAccount
      WHERE t_ID = pPmID;
  END; -- RSI_DVTransferPm

  -- Функция получения ID календаря по ФИ
  FUNCTION RSI_DV_GetLinkCalKind( pFIID in NUMBER  -- Идентификатор ФИ
                                ) RETURN NUMBER
  AS
     v_CalendarID NUMBER := 0;
  BEGIN

     BEGIN
       SELECT dcalcor_dbt.t_CalendarID INTO v_CalendarID
         FROM dcalcor_dbt
        WHERE t_ObjectType = cnst.OBJTYPE_CURRENCY
          AND t_Object     = LPAD(pFIID, 10, '0');
     EXCEPTION
        WHEN OTHERS THEN v_CalendarID := 0;
     END;

     return v_CalendarID;

  END RSI_DV_GetLinkCalKind;

  -- Процедура генерации и актуализации списка календарей
  PROCEDURE RSI_DV_CreateCALKIND( pDealID in NUMBER  -- Идентификатор сделки
                                )
  IS
     v_IsInsert  NUMBER := 0;
     v_D         DDVNDEAL_DBT%ROWTYPE;
     v_F         DDVNFI_DBT%ROWTYPE;
     v_F2        DDVNFI_DBT%ROWTYPE;
  BEGIN
     RSI_DV_InitError();

     BEGIN
        SELECT * INTO v_D
          FROM DDVNDEAL_DBT
         WHERE T_ID = pDealID;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
     END;

     BEGIN
        SELECT * INTO v_F
          FROM DDVNFI_DBT
         WHERE T_DEALID = pDealID
           AND T_TYPE = DV_NFIType_BaseActiv;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
     END;

     BEGIN
        SELECT * INTO v_F2
          FROM DDVNFI_DBT
         WHERE T_DEALID = pDealID
           AND T_TYPE = DV_NFIType_BaseActiv2;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, '');
     END;

     SELECT COUNT(1) INTO v_IsInsert
       FROM DDVNCALKIND_DBT
      WHERE T_DEALID = pDealID;

     IF( v_IsInsert <= 0 ) THEN
        IF( v_F.T_FIID = v_F2.T_FIID ) THEN
           INSERT INTO DDVNCALKIND_DBT (t_ID, t_DealID, t_Side, t_FIID, t_CalKindID)
                VALUES (0, pDealID, RSB_Derivatives.ALG_DV_PMGR_SIDE_UNDEF, v_F.T_FIID, RSI_DV_GetLinkCalKind(v_F.T_FIID));
        ELSE
           INSERT INTO DDVNCALKIND_DBT (t_ID, t_DealID, t_Side, t_FIID, t_CalKindID)
                VALUES (0, pDealID, RSB_Derivatives.ALG_DV_PMGR_SIDE_LIABILITY, v_F.T_FIID, RSI_DV_GetLinkCalKind(v_F.T_FIID));

           INSERT INTO DDVNCALKIND_DBT (t_ID, t_DealID, t_Side, t_FIID, t_CalKindID)
                VALUES (0, pDealID, RSB_Derivatives.ALG_DV_PMGR_SIDE_DEMAND, v_F2.T_FIID, RSI_DV_GetLinkCalKind(v_F2.T_FIID));
        END IF;
     ELSE
        IF( v_F.T_FIID = v_F2.T_FIID ) THEN
           FOR CalKind IN (SELECT K.*
                              FROM DDVNCALKIND_DBT K
                             WHERE K.T_DEALID = pDealID
                          )
           LOOP
              UPDATE DDVNCALKIND_DBT
                 SET t_FIID = v_F.T_FIID,
                     t_SIDE = RSB_Derivatives.ALG_DV_PMGR_SIDE_UNDEF,
                     t_CalKindID = RSI_DV_GetLinkCalKind(v_F.T_FIID)
               WHERE t_ID = CalKind.t_ID;

              DELETE FROM DDVNCALKIND_DBT K
               WHERE K.t_ID = CalKind.t_ID
                 AND EXISTS( SELECT CK.t_ID
                               FROM DDVNCALKIND_DBT CK
                              WHERE CK.t_DealID    = K.t_DealID
                                AND CK.t_Side      = K.t_Side
                                AND CK.t_FIID      = K.t_FIID
                                AND CK.t_CalKindID = K.t_CalKindID
                                AND CK.t_ID       != K.t_ID
                           );
           END LOOP;
        ELSE
           FOR CalKind IN (SELECT K.*
                              FROM DDVNCALKIND_DBT K
                             WHERE K.T_DEALID = pDealID
                               AND K.T_SIDE   = RSB_Derivatives.ALG_DV_PMGR_SIDE_UNDEF
                          )
           LOOP
              INSERT INTO DDVNCALKIND_DBT (t_ID, t_DealID, t_Side, t_FIID, t_CalKindID)
                   VALUES (0, pDealID, RSB_Derivatives.ALG_DV_PMGR_SIDE_LIABILITY, v_F.T_FIID, RSI_DV_GetLinkCalKind(v_F.T_FIID));

              INSERT INTO DDVNCALKIND_DBT (t_ID, t_DealID, t_Side, t_FIID, t_CalKindID)
                   VALUES (0, pDealID, RSB_Derivatives.ALG_DV_PMGR_SIDE_DEMAND, v_F2.T_FIID, RSI_DV_GetLinkCalKind(v_F2.T_FIID));

             DELETE FROM DDVNCALKIND_DBT K
              WHERE K.t_ID = CalKind.t_ID;
           END LOOP;

           FOR CalKind IN (SELECT K.*
                              FROM DDVNCALKIND_DBT K
                             WHERE K.T_DEALID = pDealID
                               AND K.T_SIDE   = RSB_Derivatives.ALG_DV_PMGR_SIDE_LIABILITY
                               AND K.T_FIID  != v_F.T_FIID
                          )
           LOOP
              UPDATE DDVNCALKIND_DBT
                 SET t_FIID = v_F.T_FIID,
                     t_CalKindID = RSI_DV_GetLinkCalKind(v_F.T_FIID)
               WHERE t_ID = CalKind.t_ID;
           END LOOP;

           FOR CalKind IN (SELECT K.*
                              FROM DDVNCALKIND_DBT K
                             WHERE K.T_DEALID = pDealID
                               AND K.T_SIDE   = RSB_Derivatives.ALG_DV_PMGR_SIDE_DEMAND
                               AND K.T_FIID  != v_F2.T_FIID
                          )
           LOOP
              UPDATE DDVNCALKIND_DBT
                 SET t_FIID = v_F2.T_FIID,
                     t_CalKindID = RSI_DV_GetLinkCalKind(v_F2.T_FIID)
               WHERE t_ID = CalKind.t_ID;
           END LOOP;
        END IF;
     END IF;

  END; -- RSI_DV_CreateCALKIND

  -- Используются ли срочные счета при учете сделки
  FUNCTION DV_UseUrgentAccount( pID in NUMBER  -- Идентификатор сделки
                              ) RETURN NUMBER
  AS
     v_RetVal NUMBER := 0;
     v_D DDVDEAL_DBT%ROWTYPE;
     v_MinDate     date := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     v_DrawingDate date := TO_DATE('01.01.0001', 'dd.mm.yyyy');
  BEGIN

     BEGIN
        SELECT * INTO v_D
          FROM DDVDEAL_DBT
         WHERE t_ID = pID;

        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
     END;

     BEGIN
        IF( DV_Setting_AccExContracts = 1 ) THEN -- По сделке
           SELECT MIN(turn.t_Date) INTO v_MinDate
             FROM ddvdlturn_dbt turn
            WHERE turn.t_DealID = v_D.t_ID;
        ELSE
           SELECT MIN(turn.t_Date) INTO v_MinDate
             FROM ddvfiturn_dbt turn
            WHERE turn.t_Department  = v_D.t_Department
              AND turn.t_FIID        = v_D.t_FIID
              AND turn.t_Broker      = v_D.t_Broker
              AND turn.t_ClientContr = v_D.t_ClientContr;
        END IF;

        EXCEPTION WHEN OTHERS THEN v_MinDate := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     END;

     BEGIN
        SELECT FI.t_DrawingDate INTO v_DrawingDate
          FROM dfininstr_dbt FI
         WHERE FI.t_FIID = v_D.t_FIID;

        EXCEPTION WHEN OTHERS THEN v_DrawingDate := TO_DATE('01.01.0001', 'dd.mm.yyyy');
     END;

     IF( (v_MinDate != TO_DATE('01.01.0001', 'dd.mm.yyyy')) AND (v_DrawingDate != TO_DATE('01.01.0001', 'dd.mm.yyyy')) AND (v_DrawingDate > RSI_RsbCalendar.GetDateAfterWorkDay(v_MinDate, 2)) ) THEN
        v_RetVal := 1;
     END IF;

     RETURN v_RetVal;

  END DV_UseUrgentAccount;

  -- Получение номера договора для начисления комиссий ПЗО по биржевой операции
  FUNCTION RSI_GetSfContrID( p_DealID IN NUMBER ) RETURN NUMBER
  AS
    v_deal     ddvdeal_dbt%rowtype;
    v_ContrID  NUMBER := 0;
    v_Party    NUMBER := -1;

    FUNCTION DV_GetSfContrIDbyPartyID( pDealDate IN DATE, pPartyID IN NUMBER )
      RETURN NUMBER
    AS
      v_ID NUMBER := 0;
    BEGIN
      select T_ID into v_ID
        from ( select T_ID
                 from dsfcontr_dbt
                where t_ServKind = 15 /*PTSK_DV*/
                  and t_partyID = RsbSessionData.OurBank
                  and t_ContractorID = pPartyID
                  and t_dateBegin <= pDealDate
                  and (t_dateClose = TO_DATE('01.01.0001','DD.MM.YYYY') or t_dateClose >= pDealDate )
               order by t_ServKind, t_ContractorID, t_partyID, t_DateConc
             )
       where rownum = 1;
       return v_ID;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RETURN 0;
      WHEN OTHERS THEN
        RETURN 0;
    END;

  BEGIN

    SELECT * INTO v_deal
      FROM ddvdeal_dbt
     WHERE t_ID = p_DealID;

    IF( v_deal.t_Client > 0 ) THEN
       v_ContrID := v_deal.t_ClientContr;
    ELSE
       IF( v_deal.t_Broker > 0 ) THEN
          v_ContrID := v_deal.t_BrokerContr;
       ELSE
          BEGIN
             SELECT fin.t_Issuer INTO v_Party
               FROM dfininstr_dbt fin
              WHERE fin.t_FIID = v_deal.t_FIID;
             EXCEPTION WHEN OTHERS THEN v_Party := -1;
          END;

          v_ContrID := DV_GetSfContrIDbyPartyID(v_deal.t_Date, v_Party);
       END IF;
    END IF;

    RETURN v_ContrID;

    EXCEPTION WHEN OTHERS THEN RETURN 0;

  END RSI_GetSfContrID;

  -- Получение контрагента по биржевой операции
  FUNCTION RSI_DVGetContractorID( p_DealID IN NUMBER ) RETURN NUMBER
  AS
    v_deal  ddvdeal_dbt%rowtype;
    v_Party NUMBER := -1;
  BEGIN

     BEGIN
        SELECT * INTO v_deal
          FROM ddvdeal_dbt
         WHERE t_ID = p_DealID;
        EXCEPTION WHEN OTHERS THEN return -1;
     END;

     IF( v_deal.t_Broker > 0 ) THEN
        v_Party := v_deal.t_Broker;
     ELSE
        BEGIN
           SELECT fin.t_Issuer INTO v_Party
             FROM dfininstr_dbt fin
            WHERE fin.t_FIID = v_deal.t_FIID;
           EXCEPTION WHEN OTHERS THEN v_Party := -1;
        END;
     END IF;

     RETURN v_Party;
  END RSI_DVGetContractorID;

  -- Отбор комиссий по биржевой сделке
  FUNCTION DV_ChooseComBatch RETURN INTEGER
  AS
     v_choosetype number;
  BEGIN

     for rec in ( select oprtemp.t_ID_Operation, oprtemp.t_ID_Step,
                         sfchcom.t_CommNumber, sfchcom.t_FeeType, sfchcom.t_ConComID, sfcom.t_ReceiverID,
                         deal.t_Client, RSI_DVGetContractorID(deal.t_ID) as t_ContractorID
                    from doprtemp_tmp oprtemp, dsfchcomparm_tmp sfchcom, dsfcomiss_dbt sfcom, ddvdeal_dbt deal
                   where oprtemp.t_DocKind      = DL_DVDEAL
                     and sfchcom.t_ID_Operation = oprtemp.t_ID_Operation
                     and sfchcom.t_ID_Step      = oprtemp.t_ID_Step
                     and sfcom.t_Number         = sfchcom.t_CommNumber
                     and sfcom.t_FeeType        = sfchcom.t_FeeType
                     and deal.t_ID              = oprtemp.t_OrderID
                )
     loop
        v_choosetype := 0;

        if( rec.t_ConComID <= 0 ) then
           v_choosetype := 1;
        else
           if( rec.t_ReceiverID = RsbSessionData.OurBank ) then -- комиссии банку
              if( rec.t_Client > 0 ) then
                 v_choosetype := 3;
              else
                 v_choosetype := 1;
              end if;
           else
              if( rec.t_ContractorID > 0 ) then
                 if( RSI_DV_PartyCalcTotalAmount(rec.t_ContractorID) = 1 ) then -- Не выполняется
                    v_choosetype := 1;
                 else
                    v_choosetype := 3;
                 end if;
              end if;
           end if;
        end if;

        update dsfchcomparm_tmp com
           set com.t_ChooseType   = v_choosetype
         where com.t_CommNumber   = rec.t_CommNumber
           and com.t_FeeType      = rec.t_FeeType
           and com.t_ID_Operation = rec.t_ID_Operation
           and com.t_ID_Step      = rec.t_ID_Step;
     end loop;

     return 0;
  END DV_ChooseComBatch;

  -- Получение алгоритма для единовременной комиссии
  FUNCTION DV_GetAlgNameCom( p_DealID IN NUMBER ) return VARCHAR2
  IS
     AlgName VARCHAR2(80) := '';
     v_IsFUTURES   NUMBER := 0;
     v_FaceValueFI NUMBER := -1;
     v_Client      NUMBER := -1;
  BEGIN

     BEGIN
        SELECT RSB_SECUR.IsFUTURES(RSB_SECUR.get_OperationGroup(RSB_SECUR.get_OperSysTypes(deal.t_Kind, DL_DVDEAL))), fin.t_FaceValueFI, deal.t_Client
          INTO v_IsFUTURES, v_FaceValueFI, v_Client
          FROM DDVDEAL_DBT deal, DFININSTR_DBT fin
         WHERE deal.t_ID = p_DealID
           AND fin.t_FIID = deal.t_FIID;
     EXCEPTION WHEN NO_DATA_FOUND THEN return '';
     END;

     IF( (v_IsFUTURES = 1) and (v_FaceValueFI = RSI_RSB_FIInstr.NATCUR) ) THEN
        IF( v_Client > 0 ) THEN
           AlgName := COMM_ALG_ABC;
        ELSE
           AlgName := COMM_ALG_AB;
        END IF;
     END IF;

     RETURN AlgName;
  END;

  -- Получение алгоритма из таблицы dsfcalcal_dbt
  FUNCTION RSI_DV_GetAlgNameSfCalCal( p_CalcalID IN NUMBER ) return VARCHAR2
  IS
     AlgName VARCHAR2(80) := '';
  BEGIN

     BEGIN
        SELECT trim(replace(utl_raw.cast_to_varchar2(dbms_lob.substr(T_FMTBLOBDATA_XXXX, 80, 1)),CHR(0),''))
          INTO AlgName
          FROM dsfcalcal_dbt
         WHERE t_ID = p_CalcalID;
     EXCEPTION WHEN OTHERS THEN return '';
     END;

     RETURN AlgName;
  END;

  -- Получение базовой суммы для расчета единовременной комиссии
  FUNCTION DV_GetBaseQuont( p_DealID IN NUMBER, p_CalcalID IN NUMBER ) return NUMBER
  IS
     AlgName VARCHAR2(80) := '';
     AlgBlob VARCHAR2(80) := '';
     BaseQuont NUMBER := 0;
  BEGIN
     AlgName := DV_GetAlgNameCom(p_DealID);
     AlgBlob := RSI_DV_GetAlgNameSfCalCal(p_CalcalID);

     IF( (AlgName = AlgBlob) OR
         ((AlgName = COMM_ALG_AB) AND ((AlgBlob = COMM_ALG_A) OR (AlgBlob = COMM_ALG_B))) OR
         ((AlgName = COMM_ALG_ABC) AND ((AlgBlob = COMM_ALG_A) OR (AlgBlob = COMM_ALG_B) OR (AlgBlob = COMM_ALG_C)))
       ) THEN
        BEGIN
           SELECT t_Amount
             INTO BaseQuont
             FROM DDVDEAL_DBT
            WHERE t_ID = p_DealID;
        EXCEPTION WHEN OTHERS THEN return 0;
        END;
     END IF;

     RETURN BaseQuont;
  END;

  -- Получение идентификатора субъекта по коду
  FUNCTION DV_GetPartyIDByCode( p_Code IN VARCHAR2, p_CodeKind IN NUMBER ) RETURN NUMBER
  IS
     v_PartyID NUMBER := -1;
  BEGIN
     SELECT NVL(t_ObjectID, -1) INTO v_PartyID
       FROM dobjcode_dbt
      WHERE t_ObjectType = 3
        AND t_CodeKind   = p_CodeKind
        AND t_State      = 0
        AND t_Code       = p_Code
        AND rownum      <= 1;

     RETURN v_PartyID;

  EXCEPTION
     WHEN NO_DATA_FOUND THEN RETURN -1;
  END;

  -- Получение идентификатора ММВБ
  FUNCTION DV_GetPartyIDMMVB RETURN NUMBER
  IS
  BEGIN
     RETURN DV_GetPartyIDByCode('ММВБ', cnst.PTCK_CONTR);
  END;

  -- Получение идентификатора НРД
  FUNCTION DV_GetPartyIDNRD RETURN NUMBER
  IS
  BEGIN
     RETURN DV_GetPartyIDByCode('НРД', cnst.PTCK_CONTR);
  END;

  PROCEDURE RSI_DV_MassCloseDeal
  IS
     v_UpdDeal NUMBER := 1;
     v_State   NUMBER := 0;
     v_ExCode  NUMBER := 0;
     v_ErrorMessage VARCHAR2(2000) := '';
     v_ErrPkg  VARCHAR2(50) := '';
  BEGIN
     for rec in ( select oprtemp.t_OprCompleted, deal.t_ID, deal.t_Type, deal.t_State
                    from doprtemp_tmp oprtemp, ddvdeal_dbt deal
                   where oprtemp.t_SkipDocument = 0
                     and oprtemp.t_DocKind      = DL_DVDEAL
                     and deal.t_ID              = oprtemp.t_OrderID
                )
     loop
        v_UpdDeal := 1; -- true

        if( (rec.t_OprCompleted is null) or (rec.t_OprCompleted <> chr(88)) ) then
           v_State := DVDEAL_STATE_OPEN;
        else
           if( (rec.t_TYPE = 'E') OR (rec.t_TYPE = 'R') ) then
              v_State := DVDEAL_STATE_CLOSE;
           else
              if( DV_Setting_AccExContracts = 0 ) then
                 v_State := DVDEAL_STATE_CLOSE;
              else
                 v_UpdDeal := 0; -- false
              end if;
           end if;
        end if;

        if( (v_UpdDeal = 1) and (rec.t_State <> v_State) ) then
           BEGIN
              UPDATE ddvdeal_dbt deal SET deal.t_State = v_State WHERE deal.t_ID = rec.t_ID;
           EXCEPTION WHEN OTHERS THEN
              v_ExCode := ABS(SQLCODE);

              IF( SQLCODE = -20649 ) THEN
                 DV_GetLastErrPackage(v_ErrPkg);
                 v_ErrorMessage := rsi_errors.CreateErrorStr(v_ExCode, v_ErrPkg);
              ELSIF( (-20999 <= SQLCODE) and (SQLCODE <= -20000) ) THEN
                 v_ErrorMessage := rsi_errors.CreateErrorStr(v_ExCode);
              ELSE
                 v_ErrorMessage := SQLERRM;
              END IF;

              UPDATE doprtemp_tmp oprtemp
                 SET oprtemp.t_ErrorStatus  = v_ExCode,
                     oprtemp.t_ErrorMessage = v_ErrorMessage
               WHERE oprtemp.t_OrderID = rec.t_ID;
           END;
        end if;
     end loop;
  END;

  -- Получение идентификатора базового актива, но не ПИ
  FUNCTION DV_BaseFINotDV( pFIID IN NUMBER ) RETURN NUMBER
  IS
     v_BaseFINotDV NUMBER := -1;
  BEGIN

     BEGIN
        SELECT DECODE(BaseFI.t_FI_Kind, RSI_RSB_FIInstr.FIKIND_DERIVATIVE, BaseFI.t_FaceValueFI, BaseFI.t_FIID) INTO v_BaseFINotDV
          FROM dfininstr_dbt FI, dfininstr_dbt BaseFI
         WHERE FI.t_FIID = pFIID
           AND BaseFI.t_FIID = FI.t_FaceValueFI;

        EXCEPTION WHEN OTHERS THEN v_BaseFINotDV := -1;
     END;

     RETURN v_BaseFINotDV;
  END;

   -- Значение категории "Отнесение к ФИСС" для внебиржевой сделки и сделки КО в ПИ
  FUNCTION DV_DealAttrIsFISS( pDealID IN NUMBER, pObjectType IN NUMBER ) RETURN NUMBER DETERMINISTIC
  IS
     CategoryValue dobjattr_dbt.t_NumInList % TYPE;
  BEGIN
     BEGIN
         SELECT Attr.t_NumInList INTO CategoryValue
           FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
          WHERE AtCor.t_ObjectType = pObjectType
            AND AtCor.t_GroupID    = 1 -- Отнесение к ФИСС
            AND AtCor.t_Object     = LPAD(pDealID, 34, '0')
            AND Attr.t_AttrID      = AtCor.t_AttrID
            AND Attr.t_ObjectType  = AtCor.t_ObjectType
            AND Attr.t_GroupID     = AtCor.t_GroupID;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN CategoryValue := chr(0);
        WHEN OTHERS THEN return 0;
     END;

     IF( CategoryValue <> chr(0) ) THEN
        return to_number(CategoryValue);
     ELSE
        return 0;
     END IF;

     RETURN 0;
  END; -- RSI_DV_DealAttrIsFISS

  -- Проверка является ли внебиржевая сделка или сделка Т+3 ФИСС
  FUNCTION DV_NDealIsFISS( pDealID IN NUMBER ) RETURN NUMBER
  IS
     v_RetVal NUMBER := 0; -- Нет
  BEGIN

     IF( DV_DealAttrIsFISS(pDealID, OBJTYPE_OUTOPER_DV) = 1 ) THEN -- Да
        v_RetVal := 1; -- Да
     END IF;

     IF( v_RetVal = 0 ) THEN
        SELECT count(1) INTO v_RetVal
          FROM ddvndeal_dbt DVNDeal, ddvnfi_dbt nFI_Base
         WHERE DVNDeal.t_ID      = pDealID
           AND nFI_Base.t_DealID = DVNDeal.t_ID
           AND nFI_Base.t_Type   = DV_NFIType_BaseActiv
           AND (
           (nFI_Base.t_ExecType = DVSETTLEMET_CALC)
           OR (DVNDeal.t_DVKind in(DV_PCTSWAP, DV_CURSWAP, DV_CURSWAP_FX, DV_OPTION))
           OR ( (DVNDeal.t_DVKind  = DV_FORWARD) and (nFI_Base.t_ExecType = DVSETTLEMET_STATE) and (DVNDeal.t_ISPFI = CHR(88)) )
           OR ( (DVNDeal.t_DVKind  = DV_FORWARD_T3) and (DVNDeal.t_ISPFI = CHR(88)) )
           );
     END IF;

     RETURN v_RetVal;
  END;

/*  -- Значение категории "Инструмент Хэджирования" для операции "Процентный СВОП" на дату
  FUNCTION DV_DealAttrIsHedge( pDealID IN NUMBER, pOperationalDate IN Date ) RETURN NUMBER DETERMINISTIC
  IS
     CategoryValue dobjattr_dbt.t_NumInList % TYPE;
  BEGIN
     BEGIN
         SELECT Attr.t_NumInList INTO CategoryValue
           FROM dobjatcor_dbt AtCor, dobjattr_dbt Attr
          WHERE AtCor.t_ObjectType = OBJTYPE_OUTOPER_DV
            AND AtCor.t_GroupID    = 4 -- Инструмент Хэджирования
            AND AtCor.t_Object     = LPAD(pDealID, 34, '0')
            AND Attr.t_AttrID      = AtCor.t_AttrID
            AND Attr.t_ObjectType  = AtCor.t_ObjectType
            AND Attr.t_GroupID     = AtCor.t_GroupID
            AND AtCor.t_validfromdate = ( SELECT Max(t_validfromdate)
                                           FROM dobjatcor_dbt AtCor
                                            WHERE AtCor.t_ObjectType = OBJTYPE_OUTOPER_DV
                                            AND AtCor.t_GroupID    = 4 -- Инструмент Хэджирования
                                            AND AtCor.t_Object     = LPAD(pDealID, 34, '0')
                                            AND AtCor.t_validfromdate <= pOperationalDate)                                            ;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN CategoryValue := chr(0);
        WHEN OTHERS THEN return 0;
     END;

     IF( CategoryValue <> chr(0) ) THEN
        return to_number(CategoryValue);
     ELSE
        return 0;
     END IF;

     RETURN 0;
  END; -- DV_DealAttrIsHedge*/

  -- Проверка наличия активных отношений хеджирования за дату
  FUNCTION DV_ActiveHdgRelation( pDocID IN NUMBER, pDocKind IN NUMBER, pCheckDate IN Date ) RETURN NUMBER DETERMINISTIC
  IS
     v_RelationCount NUMBER;
  BEGIN
    v_RelationCount := 0;
     IF( pDocKind = DL_DVNDEAL ) THEN
     BEGIN
        SELECT count(1) into v_RelationCount
          FROM DDLHDGRELATION_DBT
         WHERE T_INSTRID = pDocID
           AND T_INSTRDOCKIND = pDocKind
           AND T_BEGINDATE <= pCheckDate
           AND T_ENDDATE >= pCheckDate;
     EXCEPTION
           WHEN NO_DATA_FOUND THEN v_RelationCount := 0;
        WHEN OTHERS THEN return 0;
     END;
     ELSE
     BEGIN
        SELECT count(1) into v_RelationCount
          FROM DDLHDGRELATION_DBT
         WHERE T_OBJID = pDocID
           AND T_OBJDOCKIND = pDocKind
           AND T_BEGINDATE <= pCheckDate
           AND T_ENDDATE >= pCheckDate;
     EXCEPTION
           WHEN NO_DATA_FOUND THEN v_RelationCount := 0;
       WHEN OTHERS THEN return 0;
     END;
     END IF;

     IF( v_RelationCount > 0 ) THEN
        return 1;
     ELSE
        return 0;
     END IF;
  END; -- DV_ActiveHdgRelation

  --Проставить/Снять на комиссию признак dvnacdl.t_ServOperID. Используется на шаге Экспорт ПЗО БО ЦБ.
  PROCEDURE RSI_DV_SetCommisDVNACDLExport( pComID IN INTEGER, pServId IN INTEGER )
  IS
  BEGIN

     UPDATE ddvnacdl_dbt
        SET T_SERVOPERID = pServId
      WHERE T_ID = pComID;

  END; -- RSI_DV_SetCommisDVNACDLExport;

  --Проставить/Снять на комиссию признак dvdlcom.t_ServOperID. Используется на шаге Экспорт ПЗО БО ЦБ.
  PROCEDURE RSI_DV_SetCommisDVDLCOMExport( pComID IN INTEGER, pServId IN INTEGER )
  IS
  BEGIN

     UPDATE ddvdlcom_dbt
         SET T_SERVOPERID = pServId
       WHERE T_ID = pComID;

  END; -- RSI_DV_SetCommisDVDLCOMExport;

  -- Процедура сохранения расчетных операций
  PROCEDURE RSI_DV_SaveDV_VALUE( p_OperID IN NUMBER DEFAULT -1 )
  IS
     v_Count NUMBER;
  BEGIN

     v_Count := 0;
     SELECT COUNT(1) INTO v_Count
       FROM DDL_VALUE_DBT dbt, DDV_VALUE_TMP tmp
      WHERE tmp.t_DLValueID = dbt.t_ID
        AND tmp.t_Version  != dbt.t_Version;

     IF( v_Count > 0 ) THEN
        RAISE_APPLICATION_ERROR(-20624, ''); -- Конфликт. Документ изменен другим операционистом
     END IF;

     -- При вставке сделки установим актуальный DealID
     BEGIN
        IF( p_OperID > 0 ) THEN
           UPDATE DDV_VALUE_TMP SET t_DocID = p_OperID
            WHERE t_DocID <= 0;
        END IF;
        EXCEPTION WHEN OTHERS THEN NULL;
     END;

     -- Удаление записей
     BEGIN
        DELETE FROM DDL_VALUE_DBT dbt
         WHERE EXISTS( SELECT tmp.t_ID FROM DDV_VALUE_TMP tmp WHERE tmp.t_DLValueID = dbt.t_ID AND tmp.t_Delete = CHR(88) );
        EXCEPTION WHEN OTHERS THEN NULL;
     END;

     -- Обновление записей
     BEGIN
        UPDATE DDL_VALUE_DBT dbt SET( dbt.t_Kind, dbt.t_Sum, dbt.t_SumFIID ) =
                             ( SELECT tmp.t_Kind, tmp.t_Sum, tmp.t_SumFIID
                                 FROM DDV_VALUE_TMP tmp
                                WHERE tmp.t_DLValueID = dbt.t_ID AND tmp.t_Delete != CHR(88) )
         WHERE EXISTS( SELECT tmp.t_ID FROM DDV_VALUE_TMP tmp WHERE tmp.t_DLValueID = dbt.t_ID AND tmp.t_Delete != CHR(88) );
        EXCEPTION WHEN OTHERS THEN NULL;
     END;

     -- Вставка новых записей
     BEGIN
        INSERT INTO DDL_VALUE_DBT ( t_ID,
                                    t_DocKind,
                                    t_DocID,
                                    t_Kind,
                                    t_Sum,
                                    t_SumFIID )
                             SELECT 0,
                                    tmp.t_DocKind,
                                    tmp.t_DocID,
                                    tmp.t_Kind,
                                    tmp.t_Sum,
                                    tmp.t_SumFIID
                               FROM DDV_VALUE_TMP tmp
                              WHERE tmp.t_DLValueID <= 0 AND tmp.t_Delete != CHR(88);
        EXCEPTION WHEN OTHERS THEN NULL;
     END;
  END; -- RSI_DV_SaveDV_VALUE

  PROCEDURE RSI_DV_DeleteTurnByLastDeal
            (
               v_FIID         IN INTEGER, -- производный инструмент
               v_DEPARTMENT   IN INTEGER, -- Филиал
               v_BROKER       IN INTEGER, -- Брокер
               v_ClientContr  IN INTEGER, -- Клиент договор
               v_Date         IN DATE,    -- Дата
               v_GenAgrID     IN INTEGER  -- ГС
            )
  IS
     v_num_deals INTEGER := 0;
  BEGIN

     SELECT count(1) into v_num_deals
       FROM ddvdeal_dbt
      WHERE t_FIID        = v_FIID
        AND t_DEPARTMENT  = v_DEPARTMENT
        AND t_BROKER      = v_BROKER
        AND t_ClientContr = v_ClientContr
        AND t_DATE_CLR    = v_DATE
        AND t_GenAgrID    = v_GenAgrID;

     --удаляется последняя сделка, а с ней и итоги
     if( v_num_deals = 0 )then

        DELETE FROM ddvfiturn_dbt
         WHERE t_FIID        = v_FIID
           AND t_DEPARTMENT  = v_DEPARTMENT
           AND t_BROKER      = v_BROKER
           AND t_ClientContr = v_ClientContr
           AND t_DATE        = v_DATE
           AND t_GenAgrID    = v_GenAgrID
           AND (T_SETMARGIN  = 'X' or T_SETGUARANTY = 'X' or T_SETFAIRVALUE = 'X')
           AND t_BUY = 0 AND t_SALE = 0 AND t_LONGEXECUTION = 0 AND t_SHORTEXECUTION = 0
           AND t_STATE != 2;

     end if;

  END; -- RSI_DV_DeleteTurnByLastDeal

  -- Пересчёт итогов операции расчётов на валютном рынке.
  PROCEDURE DV_SetTotal ( v_ID IN INTEGER, v_Total IN NUMBER,  v_LateCom IN NUMBER  )
  IS
  BEGIN
     UPDATE DDL_TOTALOFFI_DBT
        SET T_TOTALTRANSACTION = v_Total,
        T_LATECOMISSION        = v_LateCom,
        T_DISPERANCYAMOUNT = T_CLEARINGRESULT - v_Total
     WHERE t_ID = v_ID;
  END; -- DV_RecalcTotal

  --Связывание внебиржевых сделок на валютных торгах
  PROCEDURE RSI_DV_LinkNDeals( v_CommID IN INTEGER )
  IS
     v_Comm       ddl_comm_dbt%ROWTYPE;
     DB           ddvndeal_dbt%ROWTYPE;
     v_isInsert   NUMBER := 0;
  BEGIN

     BEGIN
        SELECT * INTO v_Comm
          FROM ddl_comm_dbt
         WHERE T_DocumentID = v_CommID;
        EXCEPTION WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20572, ''); -- Не найдена запись
     END;

     DECLARE
        CURSOR Deals IS SELECT deal.*, nfi.t_execdate, nfi.t_FIID, nfi.t_PriceFIID, nfi.t_Amount
          FROM ddvnfi_dbt nfi, ddvndeal_dbt deal, doproper_dbt oper
           WHERE deal.t_Type in(2, 6)
           AND deal.t_State = DVPOS_STATE_OPEN
           AND deal.t_Sector = chr(88)
           AND deal.t_marketkind = 2
           AND deal.t_Date <= v_Comm.t_CommDate
           AND EXISTS( SELECT koper.T_KIND_OPERATION from DOPRKOPER_DBT koper WHERE koper.T_KIND_OPERATION = deal.t_KIND AND koper.T_NOTINUSE != CHR(88) AND koper.T_SYSTYPES IN ('NS', 'NU') )
           AND ltrim(oper.t_documentid) = deal.t_id
           AND oper.t_kind_operation = deal.t_kind
           AND NOT EXISTS( SELECT step.T_ID_OPERATION from DOPRSTEP_DBT step WHERE step.T_ID_OPERATION = oper.t_id_operation AND step.T_KIND_OPERATION = oper.T_KIND_OPERATION AND step.T_SYMBOL IN ('Т', 'О', 'b') AND step.t_isexecute = chr(88) )
           AND nfi.t_dealid = deal.t_id
           AND nfi.t_type = 0
           AND ((v_Comm.t_opersubkind = 1 AND deal.t_client = -1) OR (v_Comm.t_opersubkind = 2 AND deal.t_client > -1))
          ORDER BY deal.t_Date ASC, deal.t_Time ASC, deal.t_ExtCode ASC;
           BEGIN
              FOR DS IN Deals LOOP
                 BEGIN
                    SELECT * INTO DB
                       FROM ( SELECT deal.*
                         FROM ddvnfi_dbt nfi, ddvndeal_dbt deal, doproper_dbt oper
                          WHERE deal.t_Type in(1, 5)
                          AND deal.t_State = DVPOS_STATE_OPEN
                          AND deal.t_Sector = chr(88)
                          AND deal.t_marketkind = 2
                          AND deal.t_Date <= v_Comm.t_CommDate
                          AND deal.t_client = DS.t_client
                          AND deal.t_KIND = DS.t_KIND
                          AND ltrim(oper.t_documentid) = deal.t_id
                          AND oper.t_kind_operation = deal.t_kind
                          AND NOT EXISTS( SELECT step.T_ID_OPERATION from DOPRSTEP_DBT step WHERE step.T_ID_OPERATION = oper.t_id_operation AND step.T_KIND_OPERATION = oper.T_KIND_OPERATION AND step.T_SYMBOL IN ('Т', 'О', 'b') AND step.t_isexecute = chr(88) )
                          AND nfi.t_dealid = deal.t_id
                          AND nfi.t_type = 0
                          AND nfi.t_execdate = DS.t_execdate
                          AND nfi.t_FIID = DS.t_FIID
                          AND nfi.t_PriceFIID = DS.t_PriceFIID
                          AND nfi.t_Amount = DS.t_Amount
                         ORDER BY deal.t_Date ASC, deal.t_Time ASC, deal.t_ExtCode ASC  )
                          WHERE ROWNUM = 1;
                    EXCEPTION WHEN NO_DATA_FOUND THEN
                       CONTINUE;
                 END;
                 SELECT COUNT(1) INTO v_isInsert
                  FROM ddvdllnk_dbt
                   WHERE T_DealID     = DB.t_ID
                   AND   t_SaleDealID = DS.T_ID;
                  IF( v_IsInsert <= 0 ) THEN
                     --IF(DS.t_Date = v_Comm.t_CommDate) THEN
                        SELECT COUNT(1) INTO v_isInsert
                           FROM doproper_dbt oper
                            WHERE ltrim(oper.t_documentid) = DS.t_id
                            AND oper.t_kind_operation = DS.t_kind
                            AND ( EXISTS (SELECT * FROM doprstep_dbt step WHERE DS.t_dvkind = 1 AND step.T_ID_OPERATION = oper.t_id_operation AND step.T_KIND_OPERATION = oper.T_KIND_OPERATION AND step.T_SYMBOL IN ('Ф') AND step.t_isexecute = chr(88) )
                             OR ( EXISTS (SELECT * FROM doprstep_dbt step WHERE DB.t_dvkind = 3 AND step.T_ID_OPERATION = oper.t_id_operation AND step.T_KIND_OPERATION = oper.T_KIND_OPERATION AND step.T_SYMBOL IN ('т') AND step.t_isexecute = chr(88) )
                            AND EXISTS (SELECT * FROM doprstep_dbt step WHERE DB.t_dvkind = 3 AND step.T_ID_OPERATION = oper.t_id_operation AND step.T_KIND_OPERATION = oper.T_KIND_OPERATION AND step.T_SYMBOL IN ('о') AND step.t_isexecute = chr(88) ) ) );
                        IF(v_isInsert != 1) THEN
                           CONTINUE;
                        END IF;
                     --END IF;
                     --IF(DB.t_Date = v_Comm.t_CommDate) THEN
                        SELECT COUNT(1) INTO v_isInsert
                           FROM doproper_dbt oper
                            WHERE ltrim(oper.t_documentid) = DB.t_id
                            AND oper.t_kind_operation = DB.t_kind
                           AND ( EXISTS (SELECT * FROM doprstep_dbt step WHERE DB.t_dvkind = 1 AND step.T_ID_OPERATION = oper.t_id_operation AND step.T_KIND_OPERATION = oper.T_KIND_OPERATION AND step.T_SYMBOL IN ('Ф') AND step.t_isexecute = chr(88) )
                            OR ( EXISTS (SELECT * FROM doprstep_dbt step WHERE DB.t_dvkind = 3 AND step.T_ID_OPERATION = oper.t_id_operation AND step.T_KIND_OPERATION = oper.T_KIND_OPERATION AND step.T_SYMBOL IN ('т') AND step.t_isexecute = chr(88) )
                            AND EXISTS (SELECT * FROM doprstep_dbt step WHERE DB.t_dvkind = 3 AND step.T_ID_OPERATION = oper.t_id_operation AND step.T_KIND_OPERATION = oper.T_KIND_OPERATION AND step.T_SYMBOL IN ('о') AND step.t_isexecute = chr(88) ) ) );

                        IF(v_isInsert != 1) THEN
                           CONTINUE;
                        END IF;
                     --END IF;

                    INSERT INTO ddvdllnk_dbt( t_ID, t_DealID, t_ExecID, t_Execution, t_ExecCost, t_Kind, t_SaleDealID, t_SaleExecCost, t_Date, T_DvoperID )
                       VALUES( 0, DB.t_ID, 0, 0, 0, 2, DS.T_ID, 0, v_Comm.T_CommDate, v_Comm.T_DocumentID );
                  END IF;
              END LOOP;
           END;
  END;

  --Откат связей внебиржевых сделок на валютных торгах
  PROCEDURE RSI_DV_RecoilLinkNDeals( v_CommID IN INTEGER )
  IS
  BEGIN
     DELETE FROM ddvdllnk_dbt
        WHERE T_DvoperID = v_CommID
        AND   t_Kind     = 2;
  END;

  --Закрытие расчётов по ФИ
  PROCEDURE RSI_DV_CurMarketCompleteCalc( v_DocID IN INTEGER, v_ID_Step IN INTEGER )
  IS
  BEGIN
     UPDATE DDL_TOTALOFFI_DBT
        SET T_SETTLEMENTEND = chr(88),
        T_ID_STEP = v_ID_Step
     WHERE t_DocID = v_DocID
      AND  T_SETTLEMENTEND != chr(88)
      AND  T_PARENT != 0;
  END;

  --Откат закрытия расчётов по ФИ
  PROCEDURE RSI_DV_RecoilCurMarketCompleteCalc( v_DocID IN INTEGER, v_ID_Step IN INTEGER )
  IS
  BEGIN
     UPDATE DDL_TOTALOFFI_DBT
        SET T_SETTLEMENTEND = chr(0),
         T_ID_STEP = 0
     WHERE t_DocID = v_DocID
      AND T_ID_STEP = v_ID_Step
      AND T_PARENT != 0;
  END;

  --Сохранение изменение номиналов
  PROCEDURE RSI_DV_SaveHistoryFaceValue (DealID         IN INTEGER,
                                         Side           IN INTEGER,
                                         OldSum         IN NUMBER,
                                         NewSum         IN NUMBER,
                                         FIID           IN INTEGER,
                                         OldInstance    IN INTEGER,
                                         ID_Operation   IN INTEGER,
                                         ID_Step        IN INTEGER,
                                         ChangeDate     IN DATE)
  IS
  BEGIN
     INSERT INTO DDVNFVH_DBT (t_ID,
                              t_DealID,
                              t_Date,
                              t_Side,
                              t_Sum,
                              t_OldSum,
                              t_Fiid,
                              t_OldInstance,
                              t_ID_Operation,
                              t_ID_Step)
            VALUES (0,
                    DealID,
                    ChangeDate,
                    Side,
                    NewSum,
                    OldSum,
                    FIID,
                    OldInstance,
                    ID_Operation,
                    ID_Step);
  END; --RSI_DV_SaveHistoryFaceValue

  --Откат истории изменения номиналов
  PROCEDURE RSI_DV_BackOutHistoryFaceValue (v_DealID         IN INTEGER,
                                            v_Instance       IN INTEGER,
                                            v_ID_Operation   IN INTEGER,
                                            v_ID_Step        IN INTEGER)
  IS
  BEGIN
     DELETE FROM DDVNFVH_DBT
      WHERE t_DealID = v_DealID AND t_OldInstance = v_Instance;
  END; --RSI_DV_BackOutHistoryFaceValue

   -- Проверка, является ли сделка сделкой валютного рынка, принятой в клиринг
  FUNCTION DV_CurNDealInCliring( v_DealID IN NUMBER, v_OperDate IN DATE, v_OperSubKind IN NUMBER ) RETURN NUMBER
  IS
     p_InCliring NUMBER;
  BEGIN
     BEGIN
         SELECT 1 INTO p_InCliring
         FROM ddvndeal_dbt ndeal, ddvnfi_dbt nFI, dfininstr_dbt fin
         WHERE ndeal.t_ID = v_DealID
         AND ndeal.t_Sector = CHR(88)
         AND ndeal.t_MarketKind = Rsb_Secur.DV_MARKETKIND_CURRENCY
         AND ndeal.t_State > DVDEAL_STATE_PREP
         AND ndeal.t_Date <= v_OperDate
         AND nFI.t_DealID = ndeal.t_ID
         AND nFI.t_Type = 0 /*Проверяем только БА первой части, т.к. в свопах и так могут быть только валюта и др.маталл*/
         AND fin.t_FIID = nFI.t_FIID
         AND fin.t_FI_Kind in(1,6)
         AND (    (v_OperSubKind = RSB_Derivatives.ALG_SP_MONEY_SOURCE_OWN AND ndeal.t_Client = -1)
               OR (v_OperSubKind = RSB_Derivatives.ALG_SP_MONEY_SOURCE_CLIENT AND ndeal.t_Client != -1))
         AND EXISTS (SELECT *
                       FROM dpmpaym_dbt paym
                      WHERE paym.t_DocumentID = v_DealID
                        AND paym.t_DocKind = ndeal.t_DocKind
                        AND paym.t_ValueDate = v_OperDate
                        AND paym.t_PaymStatus in(Rsb_Payment.PM_FINISHED, Rsb_Payment.PM_CLOSED_W_M_MOVEMENT));
     EXCEPTION
        WHEN NO_DATA_FOUND THEN p_InCliring := 0;
        WHEN OTHERS THEN p_InCliring := 0;
     END;

     RETURN p_InCliring;
  END; -- DV_CurNDealInCliring

     -- Проверка, является ли сделка сделкой рынка СПФИ, принятой в клиринг
  FUNCTION DV_SPFI_NDealInCliring( v_DealID IN NUMBER, v_OperDate IN DATE, v_OperSubKind IN NUMBER ) RETURN NUMBER
  IS
     p_InCliring NUMBER;
  BEGIN
     BEGIN
         SELECT 1 INTO p_InCliring
         FROM ddvndeal_dbt ndeal, ddvnfi_dbt nFI
         WHERE ndeal.t_ID = v_DealID
         AND ndeal.t_MarketKind = Rsb_Secur.DV_MARKETKIND_SPFIMARKET
         AND ndeal.t_State > DVDEAL_STATE_PREP
         AND ndeal.t_Date <= v_OperDate
         AND nFI.t_DealID = ndeal.t_ID
         AND nFI.t_Type = 0
         AND (    (v_OperSubKind = RSB_Derivatives.ALG_SP_MONEY_SOURCE_OWN AND ndeal.t_Client = -1)
               OR (v_OperSubKind = RSB_Derivatives.ALG_SP_MONEY_SOURCE_CLIENT AND ndeal.t_Client != -1))
         AND (    ndeal.t_Date = v_OperDate
               OR EXISTS (SELECT *
                            FROM dpmpaym_dbt paym
                           WHERE paym.t_DocumentID = v_DealID
                             AND paym.t_DocKind = ndeal.t_DocKind
                             AND paym.t_ValueDate = v_OperDate
                             AND paym.t_PaymStatus in(Rsb_Payment.PM_FINISHED, Rsb_Payment.PM_CLOSED_W_M_MOVEMENT)
                         )
             );
     EXCEPTION
        WHEN NO_DATA_FOUND THEN p_InCliring := 0;
        WHEN OTHERS THEN p_InCliring := 0;
     END;

     RETURN p_InCliring;
  END; -- DV_SPFI_NDealInCliring

  -- Проверка, является ли биржевая сделка сделкой срочного рынка, принятой в клиринг
  FUNCTION DV_DerivDealInCliring( v_DealID IN NUMBER, v_OperDate IN DATE, v_OperSubKind IN NUMBER ) RETURN NUMBER
  IS
     p_InCliring NUMBER;
  BEGIN
     BEGIN
         SELECT 1 INTO p_InCliring
         FROM ddvdeal_dbt deal
         WHERE deal.t_ID = v_DealID
         AND deal.t_State > DVDEAL_STATE_PREP
         AND deal.t_Date_CLR <= v_OperDate
         AND (    (v_OperSubKind = RSB_Derivatives.ALG_SP_MONEY_SOURCE_OWN AND deal.t_Client = -1)
               OR (v_OperSubKind = RSB_Derivatives.ALG_SP_MONEY_SOURCE_CLIENT AND deal.t_Client != -1))
         AND (    deal.t_Date_CLR = v_OperDate
               OR EXISTS (SELECT *
                            FROM ddvdlturn_dbt turn
                           WHERE turn.t_DealID = v_DealID
                             AND turn.t_Date = v_OperDate
                             AND turn.t_Margin <> 0)
               OR NVL(( SELECT max(sE.t_Date_clr)
                        FROM ddvdllnk_dbt sL, ddvdeal_dbt sE
                        WHERE sE.t_Type in('E', 'R')
                          AND sE.t_ID = sL.t_ExecID
                          AND sL.t_DealID = v_DealID
                          AND deal.t_State = DVDEAL_STATE_CLOSE
                          AND deal.t_Amount = deal.t_Execution
                          ),to_date('01.01.9999','DD.MM.YYYY')) = v_OperDate);
     EXCEPTION
        WHEN NO_DATA_FOUND THEN p_InCliring := 0;
        WHEN OTHERS THEN p_InCliring := 0;
     END;

     RETURN p_InCliring;
  END; -- DV_DerivDealInCliring

  --Формирование проводок по неттингу требований\обязательств в разрезе валют счетов
  PROCEDURE RSI_DV_FillNtgAccTrnTmpByFIID( p_FIID IN NUMBER, p_MarketSchemeID IN NUMBER )
  IS
    TYPE docoff_t IS TABLE OF DDOCOFF_TMP%ROWTYPE;
    docoff_ins docoff_t := docoff_t();
    v_docoff DDOCOFF_TMP%ROWTYPE;
    v_acc_rec DDLNTGACC_TMP%ROWTYPE;
    v_Break BOOLEAN := false;
  BEGIN
    BEGIN
       SELECT t.* INTO v_acc_rec
         FROM ( SELECT *
                  FROM DDLNTGACC_TMP
                 WHERE T_COMPLETED != 'X'
                   AND T_CURRENCY = p_FIID
                   AND T_MARKETSCHEMEID = p_MarketSchemeID
                ORDER BY T_ID
              ) t
       WHERE ROWNUM = 1;
    EXCEPTION
       WHEN NO_DATA_FOUND THEN RETURN;
       WHEN OTHERS THEN RETURN;
    END;
    LOOP
       v_Break := true;
       FOR v_acc_pair IN(SELECT *
                           FROM DDLNTGACC_TMP
                          WHERE T_COMPLETED != 'X'
                            AND T_CURRENCY = p_FIID
                            AND T_MARKETSCHEMEID = p_MarketSchemeID
                            AND T_COMREQ = (case when v_acc_rec.T_COMREQ = 0 then 1 else 0 end)
                         ORDER BY T_ID
                        )
       LOOP
          v_Break := false;
          v_docoff.T_CHAPTER := v_acc_rec.T_CHAPTER;
          v_docoff.T_DTFIID := v_acc_rec.T_CURRENCY;
          v_docoff.T_CTFIID := v_acc_rec.T_CURRENCY;

          IF( v_acc_rec.T_COMREQ = 1 )THEN
             v_docoff.T_CTACCOUNT := v_acc_rec.T_ACCOUNT;
             v_docoff.T_DTACCOUNT := v_acc_pair.T_ACCOUNT;
          ELSE
             v_docoff.T_CTACCOUNT := v_acc_pair.T_ACCOUNT;
             v_docoff.T_DTACCOUNT := v_acc_rec.T_ACCOUNT;
          END IF;

          IF( v_acc_rec.T_REST <= v_acc_pair.T_REST )THEN

             v_docoff.T_DTSUMMA := v_acc_rec.T_REST;
             v_docoff.T_CTSUMMA := v_acc_rec.T_REST;

             update DDLNTGACC_TMP
                set T_COMPLETED = 'X',
                    T_REST = 0
              where T_ID = v_acc_rec.T_ID;

             docoff_ins.extend;
             docoff_ins(docoff_ins.LAST) := v_docoff;

             if( v_acc_rec.T_REST = v_acc_pair.T_REST )then

                update DDLNTGACC_TMP
                   set T_COMPLETED = 'X',
                       T_REST = 0
                 where T_ID = v_acc_pair.T_ID;

                BEGIN
                   SELECT * INTO v_acc_rec
                     FROM ( SELECT *
                              FROM DDLNTGACC_TMP
                             WHERE T_COMPLETED != 'X'
                               AND T_CURRENCY = p_FIID
                               AND T_MARKETSCHEMEID = p_MarketSchemeID
                            ORDER BY T_ID
                          )
                   WHERE ROWNUM = 1;
                EXCEPTION
                   WHEN NO_DATA_FOUND THEN
                     v_Break := true;
                     EXIT;
                END;
             else
                v_acc_pair.T_REST := v_acc_pair.T_REST - v_docoff.T_DTSUMMA;

                update DDLNTGACC_TMP
                   set T_REST = v_acc_pair.T_REST
                 where T_ID = v_acc_pair.T_ID;

                v_acc_rec := v_acc_pair;

                EXIT;
             end if;
          ELSE
             v_docoff.T_DTSUMMA := v_acc_pair.T_REST;
             v_docoff.T_CTSUMMA := v_acc_pair.T_REST;

             docoff_ins.extend;
             docoff_ins(docoff_ins.LAST) := v_docoff;

             update DDLNTGACC_TMP
                set T_COMPLETED = 'X',
                    T_REST = 0
              where T_ID = v_acc_pair.T_ID;

             v_acc_rec.T_REST := v_acc_rec.T_REST - v_docoff.T_DTSUMMA;

             update DDLNTGACC_TMP
                set T_REST = v_acc_rec.T_REST
              where T_ID = v_acc_rec.T_ID;
          END IF;
       END LOOP;
    EXIT WHEN v_Break = true;
    END LOOP;

    IF docoff_ins IS NOT EMPTY THEN
       FORALL i IN docoff_ins.FIRST .. docoff_ins.LAST
          INSERT INTO DDOCOFF_TMP
             VALUES docoff_ins(i);
       docoff_ins.delete;
    END IF;
  END RSI_DV_FillNtgAccTrnTmpByFIID;

  --Процедура формирования проводок по неттингу требований\обязательств в операции обработки итогов торгов (во временной таблице)
  PROCEDURE DV_FillNtgAccTrnTmpByOp(p_DocID IN NUMBER, p_DocKind IN NUMBER)
  IS
    v_dl_comm ddl_comm_dbt%ROWTYPE;
    TYPE acc_t IS TABLE OF DDLNTGACC_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
    v_acc acc_t;
    v_ByOpenDate BOOLEAN := null;
    v_ByDesc BOOLEAN := null;
    v_OrderBy NUMBER := 0;
    v_settlDate DATE;
  BEGIN

    EXECUTE IMMEDIATE 'TRUNCATE TABLE DDOCOFF_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DDLNTGACC_TMP';

    --пока только для операции обработки валютных итогов торгов
    if( p_DocKind != Rsb_Secur.DL_DVCURMARKET ) then
       return;
    end if;

    begin
       SELECT * INTO v_dl_comm FROM DDL_COMM_DBT WHERE T_DocumentID = p_DocID and T_DocKind = p_DocKind;
    exception
      when OTHERS then return;
    end;

    v_settlDate := RSI_DlCalendars.GetBalanceDateAfterWorkDayByCalendar(
       v_dl_comm.t_CommDate,
       0,
       RSI_DlCalendars.DL_GetCalendByParam(RSI_DlCalendars.DL_GetOperNameByKind(v_dl_comm.T_OPERATIONKIND), RSI_DlCalendars.DL_CALLNK_MARKET, 158, v_dl_comm.t_partyID)
    );

    --формируем проводки только для собственных операций
    if( v_dl_comm.t_OperSubKind != ALG_SP_MONEY_SOURCE_OWN ) then
       return;
    end if;

    v_ByOpenDate := Rsb_Common.GetRegBoolValue('COMMON\НЕТТИНГ\ОТБОР ПЛАТ. ПО ДАТЕ ОТКР. СЧЕТА');
    v_ByDesc := Rsb_Common.GetRegBoolValue('COMMON\НЕТТИНГ\ОТБОР ПЛАТЕЖЕЙ ПО УБЫВАНИЮ');
    --вычисляем режим сортировки
    if( v_ByOpenDate = TRUE )then
       if( v_ByDesc = TRUE )then
          v_OrderBy := 1;
       else
          v_OrderBy := 2;
       end if;
    else
       if( v_ByDesc = TRUE )then
          v_OrderBy := 3;
       else
          v_OrderBy := 4;
       end if;
    end if;

    --только в валюте (не драгметаллы)
    FOR FI IN( SELECT TOTALOFFI.T_CODEFI T_FIID, TOTALOFFI.T_MARKETSCHEMEID
                FROM DDL_TOTALOFFI_DBT TOTALOFFI, DFININSTR_DBT FI
               WHERE TOTALOFFI.T_DOCKIND = p_DocKind
                 AND TOTALOFFI.T_DOCID   = p_DocID
                 AND TOTALOFFI.T_SETTLEMENTEND != 'X'
                 AND TOTALOFFI.T_PARENT != 0
                 AND FI.T_FIID = TOTALOFFI.T_CODEFI
                 AND FI.T_FI_KIND = RSI_RSB_FIInstr.FIKIND_CURRENCY
             )LOOP

       select 0,
              q.T_ACCOUNT,
              q.T_CURRENCY,
              q.T_CHAPTER,
              q.T_OPEN_DATE,
              q.T_REST,
              case when substr(q.T_ACCOUNT,1,5) = '47408' then 1 else 0 end,
              chr(0),
              q.T_REST,
              q.T_MARKETSCHEMEID
       BULK COLLECT INTO v_acc
       from ( with mc as(select MC.T_ACCOUNT, MC.T_CURRENCY, MC.T_CHAPTER, /*DEAL.T_MARKETSCHEMEID*/FI.T_MARKETSCHEMEID /*PNV 533112*/
                           from dmcaccdoc_dbt mc , ddvndeal_dbt deal
                          where MC.T_DOCKIND in(Rsb_Secur.DL_DVNDEAL,Rsb_Secur.DL_DVFXDEAL,Rsb_Secur.DL_DVDEALT3)
                            and substr(MC.T_ACCOUNT,1,5) in ('47407','47408')
                            and MC.T_CURRENCY = FI.T_FIID
                            and DEAL.T_DOCKIND = MC.T_DOCKIND
                            and DEAL.T_ID = MC.T_DOCID
                            and DEAL.T_MARKETKIND = Rsb_Secur.DV_MARKETKIND_CURRENCY
                            and DEAL.T_SECTOR = 'X'
                            and DEAL.T_MARKETSCHEMEID in( FI.T_MARKETSCHEMEID, 0) /*PNV 533112*/
                            and DEAL.T_MARKETID =  v_dl_comm.t_partyID
                           group by MC.T_ACCOUNT, MC.T_CURRENCY, MC.T_CHAPTER, /*DEAL.T_MARKETSCHEMEID*/ FI.T_MARKETSCHEMEID/*PNV 533112*/
                           having NVL(rsb_account.restall(mc.t_Account, mc.t_Chapter, mc.t_Currency, v_settlDate), 0) <> 0
                        )
              select MC.T_ACCOUNT, MC.T_CURRENCY, MC.T_CHAPTER, AC.T_OPEN_DATE, MC.T_MARKETSCHEMEID,
                     abs(NVL(rsb_account.restall(mc.t_Account, mc.t_Chapter, mc.t_Currency, v_settlDate), 0)) T_REST
                from mc, daccount_dbt ac
               where AC.T_ACCOUNT = MC.T_ACCOUNT
                 and AC.T_CODE_CURRENCY = MC.T_CURRENCY
                 and AC.T_CHAPTER  = MC.T_CHAPTER
              order by (case when v_OrderBy=1 then AC.T_OPEN_DATE else to_date('01.01.0001','DD.MM.YYYY') end) desc,
                       (case when v_OrderBy=2 then AC.T_OPEN_DATE else to_date('01.01.0001','DD.MM.YYYY') end) asc,
                       (case when v_OrderBy=3 then T_REST else 0 end) desc,
                       (case when v_OrderBy=4 then T_REST else 0 end) asc
            ) q;

       IF v_acc.COUNT > 0 THEN
          FORALL indx IN v_acc.FIRST .. v_acc.LAST
             INSERT INTO DDLNTGACC_TMP
                  VALUES v_acc (indx);
          --во временной таблице формируем проводки неттинга ТО в разрезе валют
          RSI_DV_FillNtgAccTrnTmpByFIID(FI.T_FIID, FI.T_MARKETSCHEMEID);
       END IF;

    END LOOP;

  END DV_FillNtgAccTrnTmpByOp;

  PROCEDURE DV_FillNtgAccTrnTmpByOpSPFI(p_DocID IN NUMBER, p_DocKind IN NUMBER)
  IS
    v_dl_comm DDVOPER_DBT%ROWTYPE;
    TYPE acc_t IS TABLE OF DDLNTGACC_TMP%ROWTYPE INDEX BY BINARY_INTEGER;
    v_acc acc_t;
    v_ByOpenDate BOOLEAN := null;
    v_ByDesc BOOLEAN := null;
    v_OrderBy NUMBER := 0;
  BEGIN

    EXECUTE IMMEDIATE 'TRUNCATE TABLE DDOCOFF_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DDLNTGACC_TMP';

    begin
       SELECT * INTO v_dl_comm FROM DDVOPER_DBT WHERE T_ID = p_DocID and T_DocKind = p_DocKind;
    exception
      when OTHERS then return;
    end;
    v_ByOpenDate := Rsb_Common.GetRegBoolValue('COMMON\НЕТТИНГ\ОТБОР ПЛАТ. ПО ДАТЕ ОТКР. СЧЕТА');
    v_ByDesc := Rsb_Common.GetRegBoolValue('COMMON\НЕТТИНГ\ОТБОР ПЛАТЕЖЕЙ ПО УБЫВАНИЮ');
    --вычисляем режим сортировки
    if( v_ByOpenDate = TRUE )then
       if( v_ByDesc = TRUE )then
          v_OrderBy := 1;
       else
          v_OrderBy := 2;
       end if;
    else
       if( v_ByDesc = TRUE )then
          v_OrderBy := 3;
       else
          v_OrderBy := 4;
       end if;
    end if;

    FOR FI IN( SELECT distinct  MC.T_CURRENCY T_FIID
             FROM dmcaccdoc_dbt mc, ddvndeal_dbt deal , DFININSTR_DBT FI
            WHERE     MC.T_DOCKIND IN (Rsb_Secur.DL_DVNDEAL,Rsb_Secur.DL_DVFXDEAL,Rsb_Secur.DL_DVDEALT3)
                  AND SUBSTR (MC.T_ACCOUNT, 1, 5) IN ('47407', '47408')
                  AND DEAL.T_DOCKIND = MC.T_DOCKIND
                  AND DEAL.T_ID = MC.T_DOCID
                  AND DEAL.T_MARKETKIND = Rsb_Secur.DV_MARKETKIND_SPFIMARKET
                  AND DEAL.T_SECTOR = 'X'
                  AND DEAL.T_MARKETSCHEMEID = v_dl_comm.t_MarketSchemeID
                  AND FI.T_FIID = MC.T_CURRENCY
                  AND FI.T_FI_KIND = RSI_RSB_FIInstr.FIKIND_CURRENCY
                  AND NVL(rsb_account.restall(mc.t_Account, mc.t_Chapter, mc.t_Currency, v_dl_comm.t_Date), 0) <> 0
             )LOOP

       select 0,
              q.T_ACCOUNT,
              q.T_CURRENCY,
              q.T_CHAPTER,
              q.T_OPEN_DATE,
              q.T_REST,
              case when substr(q.T_ACCOUNT,1,5) = '47408' then 1 else 0 end,
              chr(0),
              q.T_REST,
              q.T_MARKETSCHEMEID
       BULK COLLECT INTO v_acc
       from ( with mc as(select MC.T_ACCOUNT, MC.T_CURRENCY, MC.T_CHAPTER, DEAL.T_MARKETSCHEMEID
                           from dmcaccdoc_dbt mc , ddvndeal_dbt deal
                          where MC.T_DOCKIND in(Rsb_Secur.DL_DVNDEAL,Rsb_Secur.DL_DVFXDEAL,Rsb_Secur.DL_DVDEALT3)
                            and substr(MC.T_ACCOUNT,1,5) in ('47407','47408')
                            and MC.T_CURRENCY = FI.T_FIID
                            and DEAL.T_DOCKIND = MC.T_DOCKIND
                            and DEAL.T_ID = MC.T_DOCID
                            and DEAL.T_MARKETKIND = Rsb_Secur.DV_MARKETKIND_SPFIMARKET
                            and DEAL.T_SECTOR = 'X'
                            and DEAL.T_MARKETSCHEMEID = v_dl_comm.t_MarketSchemeID
                           group by MC.T_ACCOUNT, MC.T_CURRENCY, MC.T_CHAPTER, DEAL.T_MARKETSCHEMEID
                           having NVL(rsb_account.restall(mc.t_Account, mc.t_Chapter, mc.t_Currency, v_dl_comm.t_Date), 0) <> 0
                        )
              select MC.T_ACCOUNT, MC.T_CURRENCY, MC.T_CHAPTER, AC.T_OPEN_DATE, MC.T_MARKETSCHEMEID,
                     abs(NVL(rsb_account.restall(mc.t_Account, mc.t_Chapter, mc.t_Currency, v_dl_comm.t_Date), 0)) T_REST
                from mc, daccount_dbt ac
               where AC.T_ACCOUNT = MC.T_ACCOUNT
                 and AC.T_CODE_CURRENCY = MC.T_CURRENCY
                 and AC.T_CHAPTER  = MC.T_CHAPTER
              order by (case when v_OrderBy=1 then AC.T_OPEN_DATE else to_date('01.01.0001','DD.MM.YYYY') end) desc,
                       (case when v_OrderBy=2 then AC.T_OPEN_DATE else to_date('01.01.0001','DD.MM.YYYY') end) asc,
                       (case when v_OrderBy=3 then T_REST else 0 end) desc,
                       (case when v_OrderBy=4 then T_REST else 0 end) asc
            ) q;

       IF v_acc.COUNT > 0 THEN
          FORALL indx IN v_acc.FIRST .. v_acc.LAST
             INSERT INTO DDLNTGACC_TMP
                  VALUES v_acc (indx);
          --во временной таблице формируем проводки неттинга ТО в разрезе валют
          RSI_DV_FillNtgAccTrnTmpByFIID(FI.T_FIID, v_dl_comm.t_MarketSchemeID);
       END IF;

    END LOOP;

  END DV_FillNtgAccTrnTmpByOpSPFI;

  FUNCTION DV_DealIsInPFI( v_DealID IN NUMBER) RETURN NUMBER
  IS
     p_IsInPFI NUMBER;
  BEGIN
     BEGIN
         SELECT 1 INTO p_IsInPFI
         FROM ddvndeal_dbt deal
         WHERE deal.t_ID = v_DealID
         AND deal.t_IsInPFI = CHR(88)
/*         AND EXISTS (SELECT 1 FROM dobjlink_dbt lnk
                     WHERE LPAD (deal.t_ID, 34, 0) = lnk.t_OBJECTID
                     AND lnk.t_OBJECTTYPE = 145 --OBJTYPE_OUTOPER_DV
                     AND lnk.t_GROUPID = 2
                     AND lnk.t_ATTRTYPE = 12) */
                          ;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN p_IsInPFI := 0;
        WHEN OTHERS THEN p_IsInPFI := 0;
     END;

     RETURN p_IsInPFI;

  END DV_DealIsInPFI;

  FUNCTION DV_IsCurPcSWAP( v_DealID IN NUMBER) RETURN NUMBER
  IS
     p_IsCurPcSWAP NUMBER;
  BEGIN
     BEGIN
         SELECT 1 INTO p_IsCurPcSWAP
         FROM ddvndeal_dbt deal, ddvnfi_dbt fi1, ddvnfi_dbt fi2
         WHERE deal.t_ID = v_DealID
         AND fi1.t_DealID = deal.t_ID
         AND fi1.t_Type = 0 --DV_NFIType_BaseActiv
         AND fi2.t_DealID = deal.t_ID
         AND fi2.t_Type = 2 --DV_NFIType_BaseActiv2
         AND fi1.t_FIID != fi2.t_FIID;
     EXCEPTION
        WHEN NO_DATA_FOUND THEN p_IsCurPcSWAP := 0;
        WHEN OTHERS THEN p_IsCurPcSWAP := 0;
     END;

     RETURN p_IsCurPcSWAP;

  END DV_IsCurPcSWAP;
  
  FUNCTION IsHdgDPRestAccExists( DealID IN NUMBER, DocKind IN NUMBER,
                           SvOpDate IN DATE)
        RETURN NUMBER
  IS
        SumNat NUMBER(31,12) := 0;
        IsRestExists NUMBER(5) := 0;
  BEGIN

    SELECT NVL(SUM(ABS(rsb_account.restac (q.t_Account,
                               q.t_Currency,
                               SvOpDate,
                               q.t_Chapter,
                               NULL))), 0) INTO SumNat
      FROM (SELECT DISTINCT accdoc.t_Account, accdoc.t_Currency, accdoc.t_Chapter
              FROM dmccateg_dbt cat, dmcaccdoc_dbt accdoc
             WHERE     cat.t_LevelType = 1
                   AND cat.t_Code IN ('+Корр,Хедж_ДП',
                                      '-Корр,Хедж_ДП')
                   AND accdoc.t_CatID = cat.t_ID
                   AND accdoc.t_DocID = DealID
                   AND accdoc.t_DocKind = DocKind              
                                              ) q;

    IF SumNat <> 0 THEN
        IsRestExists := 1;
    END IF;
    RETURN IsRestExists;
  END IsHdgDPRestAccExists;

FUNCTION GetFirstDateForObj (v_ObjId        IN NUMBER,
                             v_ObjDocKind   IN NUMBER,
                             v_ObjType      IN NUMBER,
                             v_SvOpDate     IN DATE)
   RETURN DATE
IS
   v_FirstDate   DATE := TO_DATE ('01.01.0001', 'dd.mm.yyyy');
BEGIN
   IF v_ObjDocKind = 12                                                   /**/
   THEN
      SELECT T_ISSUED
        INTO v_FirstDate
        FROM DFININSTR_DBT fin
       WHERE     fin.t_fiid = v_ObjId
             AND fin.t_FI_Kind =                           /*FIKIND_AVOIRISS*/
                                2
             AND fin.t_AvoirKind IN (SELECT t_AvoirKind
                                       FROM davrkinds_dbt
                                      WHERE t_FI_Kind =    /*FIKIND_AVOIRISS*/
                                                       2 AND t_Root = /*AVOIRISSKIND_BOND*/
                                                                     17);
   ELSE
      IF v_ObjDocKind = 24                                                /**/
      THEN
         IF v_ObjType = 3
         THEN
            SELECT tick.t_dealdate
              INTO v_FirstDate
              FROM ddl_tick_dbt tick,
                   dvsbnrbck_dbt bck,
                   doprdocs_dbt oprdocs,
                   doproper_dbt opr,
                   dvsordlnk_dbt lnk
             WHERE     bck.t_BCID = v_ObjId
                   AND bck.t_ChangeDate <= v_SvOpDate
                   AND bck.t_ABCStatus = 'X'
                   AND bck.t_NewABCStatus = 100    /*VABANNER_STATUS_ACCOUNT*/
                   AND oprdocs.t_DocKind = 191   --изменение учтенного векселя
                   AND oprdocs.t_DocumentID =
                          LTRIM (TO_CHAR (bck.t_ID, '0000000000'))
                   AND opr.t_ID_Operation = oprdocs.t_ID_Operation
                   AND tick.t_BOfficeKind = opr.t_DocKind
                   AND LPAD (tick.t_DealID, 34, '0') = opr.t_DocumentID;
         ELSE
            SELECT lnk.t_InterestChargeDate
              INTO v_FirstDate
              FROM dvsordlnk_dbt lnk,
                   doproper_dbt op,
                   doprdocs_dbt opd,
                   (  SELECT LPAD (hist.t_ID, 10, '0') t_ID
                        FROM dvsbnrbck_dbt hist
                       WHERE     hist.t_BCID = v_ObjId
                             AND hist.t_BCStatus = 'X'
                             AND hist.t_NewABCStatus = 20 /*VSBANNER_STATUS_SENDED*/
                    ORDER BY hist.t_ChangeDate DESC, hist.t_ID DESC) tmp
             WHERE     ROWNUM = 1
                   AND opd.t_DOCKind = 191                     /*DL_VSBNRBCK*/
                   AND opd.t_DocumentID = tmp.t_ID
                   AND op.t_ID_Operation = opd.t_ID_Operation
                   AND op.t_DocKind IN (124                      /*DL_VSSALE*/
                                           , 113          /*DL_VSBARTERORDER*/
                                                , 109       /*DL_VEKSELORDER*/
                                                     )
                   AND LPAD (lnk.t_contractid, 10, '0') = op.t_DocumentID
                   AND lnk.t_DocKind = op.t_DocKind
                   AND lnk.t_BCID = v_ObjId;
         END IF;
      ELSE
         IF v_ObjDocKind = 102                                            /**/
         THEN
            SELECT t_dealdate
              INTO v_FirstDate
              FROM ddl_tick_dbt tick
             WHERE tick.t_BofficeKind =                          /*DL_IBCDOC*/
                                       102 AND tick.t_dealid = v_ObjId;
         END IF;
      END IF;
   END IF;
   RETURN v_FirstDate;
END GetFirstDateForObj;

FUNCTION GetPrevDateCorr (v_DealID IN NUMBER, v_SvOpDate IN DATE)
   RETURN DATE
IS
   v_PrevDateCorr   DATE := TO_DATE ('01.01.0001', 'dd.mm.yyyy');
BEGIN
   BEGIN
      SELECT tmp.t_plan_date
        INTO v_PrevDateCorr
        FROM (  SELECT step.t_plan_date
          FROM ddvndeal_dbt NDeal, doprstep_dbt step, doproper_dbt oper 
         WHERE oper.t_DocKind      = NDeal.t_DocKind 
           AND oper.t_DocumentID   = lpad(NDeal.t_ID, 34, '0') 
           AND step.t_ID_Operation = oper.t_ID_Operation
           AND step.t_ServDocKind  = 4841 /*DV_SRVOP_COR_RHDP*/
           AND NDeal.t_ID =  v_DealID
           ORDER BY step.t_plan_date DESC
        ) tmp
       WHERE ROWNUM = 1;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         v_PrevDateCorr := v_SvOpDate - 1;
      WHEN OTHERS
      THEN
         v_PrevDateCorr := v_SvOpDate - 1;
   END;

   RETURN v_PrevDateCorr;
END GetPrevDateCorr;

PROCEDURE FillHdgCorRHDPTable (SvOpId IN NUMBER, SvOpDate IN DATE, DealID IN NUMBER )
AS
BEGIN
   DELETE FROM ddv_hdgcorrhdp_dbt
         WHERE T_SVOPID = SvOpId;

   INSERT INTO ddv_hdgcorrhdp_dbt (T_SVOPID,
                                   T_DATE,
                                   T_OBJID,
                                   T_OBJTYPE,
                                   T_OBJDATEFIRST,
                                   T_INSTRID,
                                   T_INSTRCODE,
                                   T_INSTRDATE,
                                   T_RELATIONID,
                                   T_RELATIONCODE,
                                   T_EXT_PORTF,
                                   T_EXT_SUBPORTF,
                                   T_BEGINDATE,
                                   T_INSTRFAIRVAL,
                                   T_FAIRVAL,
                                   T_FAIRVALMIN,
                                   T_RESTONACC,
                                   T_SUMOVERVALUE,
                                   T_SUMCOR,
                                   T_SUMRHDP,
                                   T_DELTA1,
                                   T_DELTA)
      SELECT SvOpID,
             SvOpDate,
             hdg.t_objid,
             ALG.T_SZNAMEALG,
             GetFirstDateForObj (hdg.t_objid,
                                 hdg.t_objdockind,
                                 hdg.t_objtype,
                                 SvOpDate),
             deal.t_id,
             deal.t_code,
             deal.t_begindate,
             hdg.t_id,
             hdg.t_ext_hedgeid,
             hdg.t_ext_portf,
             hdg.t_ext_subportf,
             hdg.t_begindate,
             (SELECT NVL (tmp.t_fairvalue, 0)
                FROM (  SELECT t_fairvalue
                          FROM dDVNFRVAL_dbt frval
                         WHERE     frval.t_dealid = deal.t_id
                               AND frval.t_dockind = deal.t_dockind
                      ORDER BY t_date DESC) tmp
               WHERE ROWNUM = 1)
                SS_Instr,
             NVL ((SELECT hdgfrval.t_actualfv
                FROM DDLHDGRFAIRVAL_DBT hdgfrval
               WHERE     hdgfrval.t_relationid = hdg.t_id
                     AND hdgfrval.t_date = SvOpDate), 0)
                SS_Rel,
             0 AS SS_min,
             nvl((SELECT NVL(SUM(rsb_account.restac (q.t_Account,
                                                   q.t_Currency,
                                                   GetPrevDateCorr(deal.t_id, SvOpDate),
                                                   q.t_Chapter,
                                                   NULL)),
                          0)
                FROM (SELECT DISTINCT
                             accdoc.t_Account,
                             accdoc.t_Currency,
                             accdoc.t_Chapter
                        FROM dmccateg_dbt cat, dmcaccdoc_dbt accdoc
                       WHERE     cat.t_LevelType = 1
                             AND cat.t_Code IN ('+Корр,Хедж_ДП',
                                                '-Корр,Хедж_ДП')
                             AND accdoc.t_CatID = cat.t_ID
                             AND accdoc.t_DocID = deal.t_id
                             AND accdoc.t_DocKind = deal.t_dockind) q),0)
                ORHDP,
         ( SELECT SUM(Trn_sum) FROM (SELECT NVL ( 
                     CASE 
                        WHEN trn.t_accountid_receiver = acc.t_accountid 
                        THEN 
                           TRN.T_SUM_NATCUR 
                        ELSE 
                           (TRN.T_SUM_NATCUR * (-1)) 
                     END, 
                     0) Trn_sum 
             FROM dmccateg_dbt cat, 
                  dmcaccdoc_dbt accdoc, 
                  daccount_dbt acc, 
                  dacctrn_dbt trn, 
                  doprdocs_dbt docs, 
                  DOPRSTEP_DBT step 
            WHERE cat.t_LevelType = 1 
                  AND cat.t_Code IN 
                         ('+Корр,Хедж_ДП', 
                          '-Корр,Хедж_ДП') 
                  AND accdoc.t_CatID = cat.t_ID 
                  AND accdoc.t_DocID = deal.t_id 
                  AND accdoc.t_DocKind = deal.t_dockind 
                  AND acc.t_Chapter = accdoc.t_Chapter 
                  AND acc.t_Account = accdoc.t_Account 
                  AND acc.t_Code_Currency = accdoc.t_Currency 
                  AND ( (trn.t_accountid_payer = acc.t_accountid 
                         AND NOT EXISTS 
                                    (SELECT * 
                                       FROM dmccateg_dbt cat_tmp1, 
                                            dmcaccdoc_dbt accdoc_tmp1 
                                      WHERE accdoc_tmp1.t_Account = 
                                               trn.t_account_receiver 
                                            AND cat_tmp1.t_Code IN 
                                                   ('+Корр,Хедж_ДП', 
                                                    '-Корр,Хедж_ДП') 
                                            AND accdoc_tmp1.t_CatID = 
                                                   cat_tmp1.t_ID 
                                            AND accdoc_tmp1.t_DocID = 
                                                   deal.t_id)) 
                       OR (trn.t_accountid_receiver = acc.t_accountid 
                           AND NOT EXISTS 
                                      (SELECT * 
                                         FROM dmccateg_dbt cat_tmp, 
                                              dmcaccdoc_dbt accdoc_tmp 
                                        WHERE accdoc_tmp.t_Account = 
                                                 trn.t_account_payer 
                                              AND        cat_tmp.t_Code IN ('+Корр,Хедж_ДП', 
                                        '-Корр,Хедж_ДП') 
                     AND accdoc_tmp.t_CatID = cat_tmp.t_ID 
                     AND accdoc_tmp.t_DocID = deal.t_id))) 
                     AND TRN.T_DATE_CARRY =  SvOpDate
                     AND docs.t_acctrnid = trn.t_acctrnid 
                     AND step.t_id_operation = docs.t_id_operation 
                     AND step.t_id_step = docs.t_id_step 
                     AND step.t_symbol = 'С')) 
                AS SS_trn, 
             nvl((SELECT *
                FROM (  SELECT NVL (hdgcorr.t_corrreserv, 0)
                          FROM DDLHDGRFAIRVAL_DBT hdgcorr
                         WHERE     hdgcorr.t_relationid = hdg.t_id
                               AND hdgcorr.t_date < SvOpDate
                      ORDER BY hdgcorr.t_date DESC)
               WHERE ROWNUM = 1),0)
                SumCorRHDP,
             0 AS SumRHDP,
             0 AS Delta1,
             0 AS Delta_
        FROM ddvndeal_dbt deal, DDLHDGRELATION_DBT hdg, dnamealg_dbt alg
       WHERE     deal.t_dvkind = 4                              /*DV_PCTSWAP*/
             AND ((deal.t_id = DealID and DealID <> -1) OR DealID = -1)
             AND hdg.t_instrid = deal.t_id
             AND hdg.t_instrdockind = deal.t_dockind
             AND hdg.t_hedgetype = 2                                      /**/
             AND HDG.T_ENDDATE > SvOpDate
             AND HDG.T_BEGINDATE <= SvOpDate
             AND ALG.T_ITYPEALG = 7053
             AND ALG.T_INUMBERALG = hdg.t_objtype;

   UPDATE ddv_hdgcorrhdp_dbt
      SET T_FAIRVALMIN = LEAST (T_INSTRFAIRVAL, T_FAIRVAL),
          T_SUMRHDP = (T_RESTONACC + T_SUMOVERVALUE + T_SUMCOR),
          T_DELTA1 =
               ABS (T_RESTONACC + T_SUMOVERVALUE + T_SUMCOR)
             - ABS (LEAST (T_INSTRFAIRVAL, T_FAIRVAL)),
          T_DELTA =
             CASE
                WHEN (  ABS (T_RESTONACC + T_SUMOVERVALUE + T_SUMCOR)
                      - ABS (LEAST (T_INSTRFAIRVAL, T_FAIRVAL))) > 0
                THEN
                     ABS (T_RESTONACC + T_SUMOVERVALUE + T_SUMCOR)
                   - ABS (LEAST (T_INSTRFAIRVAL, T_FAIRVAL))
                ELSE
                   0
             END
    WHERE T_SVOPID = SvOpId AND T_DATE = SvOpDate;
END FillHdgCorRHDPTable;

  FUNCTION IsHdgDPExistsTrn( DealID IN NUMBER, DocKind IN NUMBER)
        RETURN NUMBER
  IS
        ret NUMBER(5) := 0;
        v_ExistTrn NUMBER(5) := 0;
  BEGIN

      SELECT COUNT (*) INTO v_ExistTrn
      FROM (SELECT 1
              FROM DACCTRN_DBT
             WHERE T_ACCOUNT_PAYER IN (SELECT T_ACCOUNT
                                         FROM DMCACCDOC_DBT DOC
                                              INNER JOIN DMCCATEG_DBT CATEG
                                                 ON DOC.T_CATID = CATEG.T_ID
                                        WHERE     DOC.T_ISUSABLE = 'X'
                                              AND DOC.T_DOCKIND = DocKind
                                              AND DOC.T_DOCID = DealID
                                              AND CATEG.T_CODE IN ('+Корр,Хедж_ДП',
                                                                   '-Корр,Хедж_ДП'))
            UNION ALL
            SELECT 1
              FROM DACCTRN_DBT
             WHERE T_ACCOUNT_RECEIVER IN (SELECT T_ACCOUNT
                                            FROM DMCACCDOC_DBT DOC
                                                 INNER JOIN DMCCATEG_DBT CATEG
                                                    ON DOC.T_CATID = CATEG.T_ID
                                           WHERE     DOC.T_ISUSABLE = 'X'
                                                 AND DOC.T_DOCKIND = DocKind
                                                 AND DOC.T_DOCID = DealID
                                                 AND CATEG.T_CODE IN ('+Корр,Хедж_ДП',
                                                                      '-Корр,Хедж_ДП')));
    IF v_ExistTrn <> 0 THEN
        ret := 1;
    END IF;
    RETURN ret;
  END IsHdgDPExistsTrn;
  
  FUNCTION HDGAmortCheckDate( RelationID IN NUMBER, SvOpDate IN DATE)
    RETURN NUMBER
  IS
        ret NUMBER(5) := 0;
        v_FirstDate DATE;
        v_CurDate DATE;
  BEGIN
    SELECT hdg.t_EndDate
      INTO v_CurDate
      FROM DDLHDGRELATION_DBT hdg
     WHERE HDG.T_ID = RelationID;

    BEGIN
       SELECT FirstDate
         INTO v_FirstDate
         FROM (  SELECT hist.t_EndDate FirstDate
                   FROM DDLHDGRELHIST_DBT hist
                  WHERE HIST.T_ID = RelationID
               ORDER BY HIST.T_HISTID ASC)
        WHERE ROWNUM = 1;
    EXCEPTION
       WHEN NO_DATA_FOUND
       THEN
          v_FirstDate := v_CurDate;
    END;

    IF v_FirstDate >= v_CurDate AND v_CurDate <= SvOpDate THEN
      ret := 1;
    END IF;

    RETURN ret;
  END HDGAmortCheckDate;

  PROCEDURE DV_CheckContrIISwithAnotherSP(p_ContrID IN NUMBER,  p_DealDate IN DATE)
  IS
     v_CntrNumber dsfcontr_dbt.t_Number%TYPE;
  BEGIN
     BEGIN          
         SELECT sfcontr.t_Number INTO v_CntrNumber 
           FROM ddlcontrmp_dbt contrmp, ddlcontr_dbt dlcontr, dsfcontr_dbt sfcontr 
          WHERE contrmp.t_SfContrID = p_ContrID
            AND dlcontr.t_DlContrID = contrmp.t_DlContrID 
            AND dlcontr.t_IIS = 'X' 
            AND rsb_secur.GetMainObjAttr(207, LPAD(dlcontr.t_DlContrID, 34, '0'), 121, p_DealDate) = 1
            AND rsb_secur.GetMainObjAttr(207, LPAD(dlcontr.t_DlContrID, 34, '0'), 122, p_DealDate) <> 1 
            AND sfcontr.t_ID = dlcontr.t_SfContrID;
 
         EXCEPTION WHEN NO_DATA_FOUND THEN v_CntrNumber := CHR(1);
      END;

      IF( v_CntrNumber <> CHR(1) ) THEN
         RSI_DV_SetError(-20649, v_CntrNumber); --По Договору <№ договора> клиентом не предоставлены документы о расторжении договора ИИС, заключенного с другим ПУ. Выполнение операции запрещено
      END IF;
  END DV_CheckContrIISwithAnotherSP;
-----------------------------------------------------------------------------------------------------
END RSB_Derivatives;
/
