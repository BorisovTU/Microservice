-- триггеры для операций построения связей НУ

--Глобальные переменные, необходимые для работы триггеров по DNPTXOP_DBT.
CREATE OR REPLACE PACKAGE RSI_TRG_DNPTXOP_DBT IS
   TYPE Number10Table_t IS TABLE OF NUMBER(10) NOT NULL
      INDEX BY BINARY_INTEGER;

   TYPE DateTable_t IS TABLE OF DATE NOT NULL
      INDEX BY BINARY_INTEGER;
      
   TYPE Number5Table_t IS TABLE OF NUMBER(5) NOT NULL
      INDEX BY BINARY_INTEGER;   

   TYPE typeIIS_chr1 IS TABLE OF CHAR(1) NOT NULL
      INDEX BY BINARY_INTEGER;

   --1. Данные для обработки перевода в открытые
   v_TC_ID       Number10Table_t;
   v_TC_Client   Number10Table_t;
   v_TC_IIS      typeIIS_chr1; 
   v_TC_OperDate DateTable_t;
   v_TC_PrevDate DateTable_t;
   v_TC_NumEnt   INTEGER := 0;
   v_TC_SubKind  Number5Table_t;
   v_TC_Contract Number10Table_t; 

   --2. Данные для проверки уникальности документов
   v_KCD_DocKind  Number10Table_t;
   v_KCD_Client   Number10Table_t;
   v_KCD_IIS      typeIIS_chr1; 
   v_KCD_OperDate DateTable_t;
   v_KCD_NumEnt   INTEGER := 0;
   v_KCD_SubKind  Number5Table_t;
   v_KCD_Contract Number10Table_t;

END RSI_TRG_DNPTXOP_DBT;
/

CREATE OR REPLACE TRIGGER DNPTXOP_DBT_TIUKCD
   BEFORE INSERT OR UPDATE OF T_DOCKIND, T_CLIENT, T_IIS, T_OPERDATE, T_CONTRACT
   ON DNPTXOP_DBT
   FOR EACH ROW
WHEN (new.t_DocKind in (4605, 4606) and new.t_Recalc = chr(0)) -- RSI_NPTXC.DL_CALCNDFL
BEGIN

   RSI_TRG_DNPTXOP_DBT.v_KCD_NumEnt := RSI_TRG_DNPTXOP_DBT.v_KCD_NumEnt + 1;
   RSI_TRG_DNPTXOP_DBT.v_KCD_DocKind(RSI_TRG_DNPTXOP_DBT.v_KCD_NumEnt)  := :NEW.T_DOCKIND;
   RSI_TRG_DNPTXOP_DBT.v_KCD_Client(RSI_TRG_DNPTXOP_DBT.v_KCD_NumEnt)   := :NEW.T_CLIENT;
   RSI_TRG_DNPTXOP_DBT.v_KCD_IIS(RSI_TRG_DNPTXOP_DBT.v_KCD_NumEnt)     := :NEW.T_IIS; 
   RSI_TRG_DNPTXOP_DBT.v_KCD_OperDate(RSI_TRG_DNPTXOP_DBT.v_KCD_NumEnt) := :NEW.T_OPERDATE;
   RSI_TRG_DNPTXOP_DBT.v_KCD_Contract(RSI_TRG_DNPTXOP_DBT.v_KCD_NumEnt)   := :NEW.T_CONTRACT;

   RSI_TRG_DNPTXOP_DBT.v_KCD_SubKind(RSI_TRG_DNPTXOP_DBT.v_KCD_NumEnt) := 0;
   IF :NEW.T_DOCKIND = 4605 THEN --Расчет НОБ для НДФЛ
     RSI_TRG_DNPTXOP_DBT.v_KCD_SubKind(RSI_TRG_DNPTXOP_DBT.v_KCD_NumEnt) := :NEW.T_SUBKIND_OPERATION;
   END IF;

END DNPTXOP_DBT_TIUKCD;
/

/*Если осуществляется пакетная вставка через ROWTYPE, то незаполненные значения не получают дефолтные, указанные для таблицы*/
CREATE OR REPLACE TRIGGER DNPTXOP_DBT_TIDEF
   BEFORE INSERT ON DNPTXOP_DBT
   FOR EACH ROW
BEGIN

   IF :NEW.T_PAYMEDICAL IS NULL THEN
     :NEW.T_PAYMEDICAL := CHR(0);
   END IF;

   IF :NEW.T_RECEIVER IS NULL THEN
     :NEW.T_RECEIVER := -1;
   END IF;

END DNPTXOP_DBT_TIUKCD;
/

CREATE OR REPLACE TRIGGER DNPTXOP_DBT_TIUKCDA
   AFTER INSERT OR UPDATE OF T_DOCKIND, T_CLIENT, T_OPERDATE, T_CONTRACT ON DNPTXOP_DBT
DECLARE
   v_count NUMBER;
   v_OPERDATE DATE := TO_DATE('01.01.0001', 'DD.MM.YYYY');
BEGIN

   IF RSI_TRG_DNPTXOP_DBT.v_KCD_NumEnt > 0 THEN

      for v_i in 1..RSI_TRG_DNPTXOP_DBT.v_KCD_NumEnt loop

         IF RSI_TRG_DNPTXOP_DBT.v_KCD_DocKind(v_i) = 4605 THEN

           SELECT count(1)
             INTO v_count
             FROM DNPTXOP_DBT
            WHERE T_DOCKIND  = RSI_TRG_DNPTXOP_DBT.v_KCD_DocKind(v_i) 
              AND T_CLIENT   = RSI_TRG_DNPTXOP_DBT.v_KCD_Client(v_i)
              AND T_CONTRACT = RSI_TRG_DNPTXOP_DBT.v_KCD_Contract(v_i)  
              AND T_RECALC   = chr(0)
              AND T_IIS      = RSI_TRG_DNPTXOP_DBT.v_KCD_IIS(v_i)
              AND T_OPERDATE = RSI_TRG_DNPTXOP_DBT.v_KCD_OperDate(v_i)
              AND (EXTRACT(YEAR FROM RSI_TRG_DNPTXOP_DBT.v_KCD_OperDate(v_i)) < RSI_NPTO.GetLucreStartTaxPeriod() 
                   OR (RSI_TRG_DNPTXOP_DBT.v_KCD_SubKind(v_i) = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND T_SUBKIND_OPERATION = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE)
                   OR (RSI_TRG_DNPTXOP_DBT.v_KCD_SubKind(v_i) <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND T_SUBKIND_OPERATION <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE)
                  );  

           SELECT nvl(max(T_OPERDATE),TO_DATE('01.01.0001', 'DD.MM.YYYY'))
             INTO v_OPERDATE
             FROM DNPTXOP_DBT
            WHERE T_DOCKIND  = RSI_TRG_DNPTXOP_DBT.v_KCD_DocKind(v_i) 
              AND T_CLIENT   = RSI_TRG_DNPTXOP_DBT.v_KCD_Client(v_i)
              AND T_CONTRACT = RSI_TRG_DNPTXOP_DBT.v_KCD_Contract(v_i)  
              AND T_RECALC   = chr(0)
              AND T_IIS      = RSI_TRG_DNPTXOP_DBT.v_KCD_IIS(v_i)     
              AND t_STATUS  != 0
              AND (EXTRACT(YEAR FROM RSI_TRG_DNPTXOP_DBT.v_KCD_OperDate(v_i)) < RSI_NPTO.GetLucreStartTaxPeriod() 
                   OR (RSI_TRG_DNPTXOP_DBT.v_KCD_SubKind(v_i) = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND T_SUBKIND_OPERATION = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE)
                   OR (RSI_TRG_DNPTXOP_DBT.v_KCD_SubKind(v_i) <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND T_SUBKIND_OPERATION <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE)
                  );


         ELSE

           SELECT count(1)
             INTO v_count
             FROM DNPTXOP_DBT
            WHERE T_DOCKIND  = RSI_TRG_DNPTXOP_DBT.v_KCD_DocKind(v_i) 
              AND T_CLIENT   = RSI_TRG_DNPTXOP_DBT.v_KCD_Client(v_i)
              AND T_CONTRACT = RSI_TRG_DNPTXOP_DBT.v_KCD_Contract(v_i)  
              AND T_RECALC   = chr(0)
              AND T_IIS      = RSI_TRG_DNPTXOP_DBT.v_KCD_IIS(v_i)
              AND T_OPERDATE = RSI_TRG_DNPTXOP_DBT.v_KCD_OperDate(v_i);

           SELECT nvl(max(T_OPERDATE),TO_DATE('01.01.0001', 'DD.MM.YYYY'))
             INTO v_OPERDATE
             FROM DNPTXOP_DBT
            WHERE T_DOCKIND  = RSI_TRG_DNPTXOP_DBT.v_KCD_DocKind(v_i) 
              AND T_CLIENT   = RSI_TRG_DNPTXOP_DBT.v_KCD_Client(v_i)
              AND T_CONTRACT = RSI_TRG_DNPTXOP_DBT.v_KCD_Contract(v_i)  
              AND T_RECALC   = chr(0)
              AND T_IIS      = RSI_TRG_DNPTXOP_DBT.v_KCD_IIS(v_i)     
              AND t_STATUS  != 0;
         END IF;

         IF v_count > 1 and v_OPERDATE > RSI_TRG_DNPTXOP_DBT.v_KCD_OperDate(v_i) THEN
           RSI_TRG_DNPTXOP_DBT.v_KCD_NumEnt := 0;
           --Уже существует документ данного вида по клиенту за дату операции. Повторный ввод возможен только за дату последней по дате операции.
           RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20601,''); 
         END IF;

      end loop;

      RSI_TRG_DNPTXOP_DBT.v_KCD_NumEnt := 0;

   END IF;

END DNPTXOP_DBT_TIUKCDA;
/

CREATE OR REPLACE TRIGGER DNPTXOP_DBT_TUDS
   BEFORE UPDATE OF T_STATUS OR DELETE
   ON DNPTXOP_DBT
   FOR EACH ROW
WHEN (new.t_DocKind IN (4605/*Расчет НОБ для НДФЛ*/, 4608/*Удержание НДФЛ*/))
BEGIN

  IF ((:NEW.t_Status = RSI_NPTXC.DL_TXOP_Prep and UPDATING) OR DELETING) THEN
    DELETE 
      FROM DNPTXMES_DBT 
     WHERE T_DOCID = :OLD.T_ID;
  END IF; 

END DNPTXOP_DBT_TUDS;
/


CREATE OR REPLACE TRIGGER DNPTXOP_DBT_TIUT
   BEFORE INSERT OR UPDATE OF T_STATUS 
   ON DNPTXOP_DBT
   FOR EACH ROW
WHEN (new.t_DocKind = 4608) -- RSI_NPTXC.DL_HOLDNDFL
BEGIN

  -- (NEW.T_STATUS = Открыт и (вставка или (обновление и OLD.T_STATUS = Отложен))) /*Перевод в открытые*/
  if ((:NEW.t_Status = RSI_NPTXC.DL_TXOP_Open and 
       (INSERTING OR (UPDATING and :OLD.t_Status = RSI_NPTXC.DL_TXOP_Prep))
      ) OR
  -- (NEW.T_STATUS = Закрыт и вставка и NEW.T_PREVDATE = Нач. дата ) /*Актуально для импорта*/
      (:NEW.t_Status = RSI_NPTXC.DL_TXOP_Close and INSERTING 
      )
     ) then

     IF :NEW.T_PREVDATE <= RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CLOSE, :NEW.T_CLIENT, :NEW.T_IIS ) THEN
       --Попытка выполнить удержание НДФЛ в закрытом налоговом периоде
       RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20640,'');
     END iF;

  end if;

END DNPTXOP_DBT_TIUT;
/

CREATE OR REPLACE TRIGGER DNPTXOP_DBT_TUDT
   BEFORE UPDATE OF T_STATUS OR DELETE
   ON DNPTXOP_DBT
   FOR EACH ROW
WHEN (old.t_DocKind = 4608/*Удержание НДФЛ*/)
BEGIN

  --  ((обновление и NEW.T_STATUS = Открыт) или удаление) и OLD.T_STATUS = Закрыт  /*Откат*/
  IF ((:NEW.t_Status = RSI_NPTXC.DL_TXOP_Open and UPDATING) OR DELETING) AND :OLD.t_Status = RSI_NPTXC.DL_TXOP_Close THEN
    IF :OLD.T_PREVDATE <= RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CLOSE, :OLD.T_CLIENT, :OLD.T_IIS ) THEN
       --Попытка выполнить удержание НДФЛ в закрытом налоговом периоде
       RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20641,'');
     END iF;
  END IF; 

END DNPTXOP_DBT_TUDT;
/


CREATE OR REPLACE TRIGGER DNPTXOP_DBT_TIUS
   BEFORE INSERT OR UPDATE OF T_STATUS 
   ON DNPTXOP_DBT
   FOR EACH ROW
WHEN (new.t_DocKind = 4605 and new.t_Recalc = chr(0)) -- RSI_NPTXC.DL_CALCNDFL
DECLARE
  v_OldPrevDate DATE;
  v_EndYearBeforeOperDate DATE;
  v_CntOpenDlContr NUMBER;
  v_PrevDate1 DATE;
  v_PrevDate2 DATE;
  v_SubKind NUMBER;
BEGIN

   -- (NEW.T_STATUS = Открыт и (вставка или (обновление и OLD.T_STATUS = Отложен))) /*Перевод в открытые*/
   if ((:NEW.t_Status = RSI_NPTXC.DL_TXOP_Open and 
        (INSERTING OR (UPDATING and :OLD.t_Status = RSI_NPTXC.DL_TXOP_Prep))
       ) OR
   -- (NEW.T_STATUS = Закрыт и вставка и NEW.T_PREVDATE = Нач. дата ) /*Актуально для импорта*/
       (:NEW.t_Status = RSI_NPTXC.DL_TXOP_Close and INSERTING and :NEW.t_PrevDate = TO_DATE('01.01.0001', 'DD.MM.YYYY')
       )
      ) then

      v_OldPrevDate := :NEW.T_PREVDATE;

      v_SubKind := 0;
      IF :NEW.T_SUBKIND_OPERATION = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE THEN
        v_SubKind := RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE;
      END IF;

      v_PrevDate1 := RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CALCLINKS, :NEW.T_CLIENT, :NEW.T_IIS, v_SubKind, :NEW.T_CONTRACT, 1 );
      v_PrevDate2 := RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CALCNDFL, :NEW.T_CLIENT, :NEW.T_IIS, v_SubKind, :NEW.T_CONTRACT, 1 );

      :NEW.T_PREVDATE := GREATEST(v_PrevDate1, v_PrevDate2);

      IF v_OldPrevDate <> :NEW.T_PREVDATE THEN 

        IF :NEW.t_IIS <> 'X' AND :NEW.T_PREVDATE > TO_DATE('01.01.0001','DD.MM.YYYY') THEN
          v_EndYearBeforeOperDate := TO_DATE('31.12.'||TO_CHAR(EXTRACT(YEAR FROM :NEW.t_OperDate) - 1),'DD.MM.YYYY');

          IF EXTRACT(YEAR FROM :NEW.T_PREVDATE) < EXTRACT(YEAR FROM :NEW.t_OperDate) AND :NEW.T_PREVDATE < v_EndYearBeforeOperDate AND :NEW.t_Recalc != 'X' THEN
            
            SELECT COUNT(1)
              INTO v_CntOpenDlContr
              FROM dsfcontr_dbt sf, ddlcontr_dbt dlc
             WHERE sf.t_PartyID = :NEW.t_Client
               AND sf.t_DateBegin <= v_EndYearBeforeOperDate
               AND (sf.t_DateCLose >= v_EndYearBeforeOperDate or sf.t_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY'))
               AND dlc.t_SfContrID = sf.t_ID
               AND dlc.t_IIS = :NEW.t_IIS;

            IF v_CntOpenDlContr = 0 THEN
              :NEW.T_PREVDATE := TO_DATE('01.01.0001','DD.MM.YYYY');
            END IF;
          END IF;

        END IF;
      END IF;

      IF ((:NEW.T_PREVDATE > :NEW.T_OPERDATE) OR (:NEW.T_OPERDATE > RsbSessionData.curdate)) THEN
         --Неверная дата операции
         RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20602,'');

      ELSIF :NEW.T_IIS <> 'X' AND :NEW.T_PREVDATE > TO_DATE('01.01.0001', 'DD.MM.YYYY') AND TO_NUMBER(TO_CHAR( :NEW.T_PREVDATE + 1, 'YYYY')) <> TO_NUMBER(TO_CHAR( :NEW.T_OPERDATE, 'YYYY')) AND :NEW.T_PREVDATE != :NEW.T_OPERDATE THEN
           --Не выполнен расчет НОБ для НДФЛ в конце календарного года
           RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20628,'');
      ELSIF (:NEW.T_PREVDATE + 1 <= RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CLOSE, :NEW.T_CLIENT, :NEW.T_IIS, 0, :NEW.T_CONTRACT )) THEN

          IF ((:NEW.T_IIS <> 'X') and (RSI_NPTO.IsAdmin(RsbSessionData.Oper) = false)) THEN
           --Попытка выполнить расчет НДР в закрытом налоговом периоде
           RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20603,''); 
          END IF;
      END IF;

      RSI_TRG_DNPTXOP_DBT.v_TC_NumEnt := RSI_TRG_DNPTXOP_DBT.v_TC_NumEnt + 1;
      RSI_TRG_DNPTXOP_DBT.v_TC_ID(RSI_TRG_DNPTXOP_DBT.v_TC_NumEnt)       := :NEW.T_ID;
      RSI_TRG_DNPTXOP_DBT.v_TC_Client(RSI_TRG_DNPTXOP_DBT.v_TC_NumEnt)   := :NEW.T_CLIENT;
      RSI_TRG_DNPTXOP_DBT.v_TC_Contract(RSI_TRG_DNPTXOP_DBT.v_TC_NumEnt) := :NEW.T_CONTRACT;
      RSI_TRG_DNPTXOP_DBT.v_TC_IIS(RSI_TRG_DNPTXOP_DBT.v_TC_NumEnt)      := :NEW.T_IIS;
      RSI_TRG_DNPTXOP_DBT.v_TC_OperDate(RSI_TRG_DNPTXOP_DBT.v_TC_NumEnt) := :NEW.T_OPERDATE;
      RSI_TRG_DNPTXOP_DBT.v_TC_PrevDate(RSI_TRG_DNPTXOP_DBT.v_TC_NumEnt) := :NEW.T_PREVDATE;
      RSI_TRG_DNPTXOP_DBT.v_TC_SubKind(RSI_TRG_DNPTXOP_DBT.v_TC_NumEnt)  := :NEW.T_SUBKIND_OPERATION;

   -- Иначе если NEW.T_STATUS = Отложен и обновление /*Откат*/
   --elsif (:NEW.t_Status = RSI_NPTXC.DL_TXOP_Prep and UPDATING) then
   --   :NEW.T_PREVDATE   := TO_DATE('01.01.0001', 'DD.MM.YYYY');
   end if;

END DNPTXOP_DBT_TIUS;
/

CREATE OR REPLACE TRIGGER DNPTXOP_DBT_TIUSA
   AFTER INSERT OR UPDATE OF T_STATUS ON DNPTXOP_DBT
DECLARE
   v_PrevTaxSum NUMBER;
   v_Count      NUMBER;
BEGIN

   IF RSI_TRG_DNPTXOP_DBT.v_TC_NumEnt > 0 THEN

      for v_i in 1..RSI_TRG_DNPTXOP_DBT.v_TC_NumEnt loop

        SELECT COUNT(1)
          INTO v_Count
          FROM DNPTXOP_DBT
         WHERE T_DOCKIND  = RSI_NPTXC.DL_CALCNDFL
           AND T_CLIENT   = RSI_TRG_DNPTXOP_DBT.v_TC_Client(v_i)
           AND T_CONTRACT = RSI_TRG_DNPTXOP_DBT.v_TC_Contract(v_i)  
           AND T_RECALC   = chr(0)
           AND T_IIS      = RSI_TRG_DNPTXOP_DBT.v_TC_IIS(v_i)  
           AND T_OPERDATE > RSI_TRG_DNPTXOP_DBT.v_TC_PrevDate(v_i) AND RSI_TRG_DNPTXOP_DBT.v_TC_PrevDate(v_i) > TO_DATE('01.01.0001','DD.MM.YYYY')
           AND T_OPERDATE != RSI_TRG_DNPTXOP_DBT.v_TC_OperDate(v_i)
           AND T_STATUS  IN (RSI_NPTXC.DL_TXOP_Open, RSI_NPTXC.DL_TXOP_Close)
           AND T_ID      <> RSI_TRG_DNPTXOP_DBT.v_TC_ID(v_i)
           AND (EXTRACT(YEAR FROM RSI_TRG_DNPTXOP_DBT.v_TC_OperDate(v_i)) < RSI_NPTO.GetLucreStartTaxPeriod() 
                   OR (RSI_TRG_DNPTXOP_DBT.v_TC_SubKind(v_i) = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND T_SUBKIND_OPERATION = RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE)
                   OR (RSI_TRG_DNPTXOP_DBT.v_TC_SubKind(v_i) <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE AND T_SUBKIND_OPERATION <> RSI_NPTXC.DL_TXBASECALC_OPTYPE_LUCRE)
                  );


         IF v_Count > 0 THEN
             RSI_TRG_DNPTXOP_DBT.v_TC_NumEnt := 0;
             --Уже существует операция расчета НОД для НДФЛ за указанный период
             RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20604,'');
         END IF;
      end loop;

      RSI_TRG_DNPTXOP_DBT.v_TC_NumEnt := 0;
   END IF;

END DNPTXOP_DBT_TIUSA;
/

CREATE OR REPLACE TRIGGER DNPTXOP_DBT_TAIUD
  AFTER INSERT OR DELETE OR UPDATE OF T_OPERDATE, T_TIME ON DNPTXOP_DBT
  FOR EACH ROW
  WHEN (NEW.T_DOCKIND = 4607 or OLD.T_DOCKIND = 4607 /*Rsb_Secur.DL_WRTMONEY*/)
DECLARE
  pDate DATE;
BEGIN

  IF( DELETING ) THEN
     pDate := :OLD.t_OperDate;
  ELSE
     pDate := :NEW.t_OperDate;
  END IF;

  Rsb_Secur.RSI_SetDateCalc(pDate);

END;
/

CREATE OR REPLACE TRIGGER NPTXOBJ_DBT_TIUD
   BEFORE INSERT OR UPDATE OR DELETE 
   ON DNPTXOBJ_DBT
   FOR EACH ROW
DECLARE
   v_IIS CHAR;
   v_IIS_NEW CHAR;
   v_IIS_OLD CHAR;
   v_Contract NUMBER := 0;
   v_Contract_NEW NUMBER := 0;
   v_Contract_OLD NUMBER := 0;
BEGIN

   IF INSERTING THEN
       IF (RSI_NPTO.CheckObjIIS(:NEW.t_AnaliticKind6, :NEW.t_Analitic6) = 1) THEN
         v_IIS := CHR(88);
         BEGIN
           SELECT MP.T_DLCONTRID INTO v_Contract FROM DDLCONTRMP_DBT MP WHERE MP.T_SFCONTRID = :NEW.t_Analitic6;
         EXCEPTION
           WHEN NO_DATA_FOUND THEN v_Contract := 0;
         END;
       ELSE
         v_IIS := CHR(0);
       END IF; 

      if (:NEW.t_Date <= RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CLOSE, :NEW.T_CLIENT, v_IIS, 0, v_Contract )) then
         --Попытка вставки объекта НДР в закрытом налоговом периоде
        if ((v_IIS <> 'X') and (RSI_NPTO.IsAdmin(RsbSessionData.Oper) = false)) then
          RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20636,''); 
        end if;
      end if;

      IF(:NEW.T_TAXPERIOD IS NULL) THEN
        :NEW.T_TAXPERIOD := 0;
      END IF;

      IF(:NEW.t_Level = 8 AND :NEW.T_TAXPERIOD = 0) THEN
        :NEW.T_TAXPERIOD := EXTRACT(YEAR FROM :NEW.T_DATE);
      END IF;

      IF(:NEW.t_Level = 8 AND :NEW.t_AnaliticKind2 <= 0) THEN
        IF(:NEW.T_KIND IN (1143, 870, 1144, 875)) THEN
          :NEW.t_AnaliticKind2 := RSI_NPTXC.TXOBJ_KIND2040;
          :NEW.t_Analitic2 := 201;
        ELSIF(:NEW.T_KIND IN (RSI_NPTXC.TXOBJ_PAIDGENERAL_15_9)) THEN
          :NEW.t_AnaliticKind2 := RSI_NPTXC.TXOBJ_KIND2040;
          :NEW.t_Analitic2 := 208;
        ELSIF(:NEW.T_KIND IN (1150, 1161, 1162, 1151)) THEN
          :NEW.t_AnaliticKind2 := RSI_NPTXC.TXOBJ_KIND2040;
          :NEW.t_Analitic2 := 218;
          IF :NEW.T_TAXPERIOD <= 2024 OR :NEW.T_ANALITICKIND1 IN (RSI_NPTXC.TXOBJ_KIND1115, RSI_NPTXC.TXOBJ_KIND1120, RSI_NPTXC.TXOBJ_KIND1125, RSI_NPTXC.TXOBJ_KIND1130) THEN
            :NEW.t_Analitic2 := 208;
          END IF;
        ELSIF(:NEW.T_KIND IN (RSI_NPTXC.TXOBJ_PAIDGENERAL_18_9)) THEN
          :NEW.t_AnaliticKind2 := RSI_NPTXC.TXOBJ_KIND2040;
          :NEW.t_Analitic2 := 215;
        ELSIF(:NEW.T_KIND IN (RSI_NPTXC.TXOBJ_PAIDGENERAL_20_9)) THEN
          :NEW.t_AnaliticKind2 := RSI_NPTXC.TXOBJ_KIND2040;
          :NEW.t_Analitic2 := 216;
        ELSIF(:NEW.T_KIND IN (RSI_NPTXC.TXOBJ_PAIDGENERAL_22_9)) THEN
          :NEW.t_AnaliticKind2 := RSI_NPTXC.TXOBJ_KIND2040;
          :NEW.t_Analitic2 := 217;
        END IF;
      END IF;

      /*BOSS-3884 Сумма для всех объектов НДР с 1 по 7 уровень округляется до 2 знаков после запятой*/
      IF(:NEW.t_level > 0 and :NEW.t_level < 8 ) THEN 
        :NEW.t_sum  := round(:NEW.t_sum, 2);
        :NEW.t_sum0 := round(:NEW.t_sum0, 2);
      END IF;

   ELSIF UPDATING THEN
       IF (RSI_NPTO.CheckObjIIS(:NEW.t_AnaliticKind6, :NEW.t_Analitic6) = 1) THEN
         v_IIS_NEW := CHR(88);
         BEGIN
          SELECT MP.T_DLCONTRID INTO v_Contract_NEW FROM DDLCONTRMP_DBT MP WHERE MP.T_SFCONTRID = :NEW.t_Analitic6;
         EXCEPTION
           WHEN NO_DATA_FOUND THEN v_Contract := 0;
         END;
       ELSE
         v_IIS_NEW := CHR(0);
       END IF; 

       IF (RSI_NPTO.CheckObjIIS(:OLD.t_AnaliticKind6, :OLD.t_Analitic6) = 1) THEN
         v_IIS_OLD := CHR(88);
         BEGIN
          SELECT MP.T_DLCONTRID INTO v_Contract_OLD FROM DDLCONTRMP_DBT MP WHERE MP.T_SFCONTRID = :OLD.t_Analitic6;
         EXCEPTION
           WHEN NO_DATA_FOUND THEN v_Contract := 0;
         END;
       ELSE
         v_IIS_OLD := CHR(0);
       END IF; 

      if ( (:NEW.t_Date <= RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CLOSE, :NEW.T_CLIENT, v_IIS_NEW, 0, v_Contract_NEW ) ) OR
           (:OLD.t_Date <= RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CLOSE, :OLD.T_CLIENT, v_IIS_OLD, 0, v_Contract_OLD ) )
         ) then
        if ((v_IIS_OLD <> 'X')  and (RSI_NPTO.IsAdmin(RsbSessionData.Oper) = false)) then
         --Попытка изменения объекта НДР в закрытом налоговом периоде
         RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20637,''); 
        end if;
      end if;

      IF(:NEW.T_TAXPERIOD IS NULL) THEN
        :NEW.T_TAXPERIOD := 0;
      END IF;

      /*BOSS-3884 Сумма для всех объектов НДР с 1 по 7 уровень округляется до 2 знаков после запятой*/
      IF(:NEW.t_level > 0 and :NEW.t_level < 8 ) THEN 
        :NEW.t_sum  := round(:NEW.t_sum, 2);
        :NEW.t_sum0 := round(:NEW.t_sum0, 2);
      END IF;
   ELSIF DELETING THEN

       IF (RSI_NPTO.CheckObjIIS(:OLD.t_AnaliticKind6, :OLD.t_Analitic6) = 1) THEN
         v_IIS_OLD := CHR(88);
         BEGIN
          SELECT MP.T_DLCONTRID INTO v_Contract_OLD FROM DDLCONTRMP_DBT MP WHERE MP.T_SFCONTRID = :OLD.t_Analitic6;
         EXCEPTION
           WHEN NO_DATA_FOUND THEN v_Contract := 0;
         END;
       ELSE
         v_IIS_OLD := CHR(0);
       END IF; 

      if (:OLD.t_Date <= RSI_NPTO.GetCalcPeriodDate( RSI_NPTXC.NPTXCALC_CLOSE, :OLD.T_CLIENT, v_IIS_OLD, 0, v_Contract_OLD )) then
        if ((v_IIS_OLD <> 'X')  and (RSI_NPTO.IsAdmin(RsbSessionData.Oper) = false)) then
         --Попытка удаления объекта НДР в закрытом налоговом периоде
         RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20638,''); 
        end if;
      end if;

      DELETE FROM DNPTXOBDC_DBT WHERE t_ObjID = :OLD.t_ObjID;
   END IF;

END NPTXOBJ_DBT_TIUD;
/

CREATE OR REPLACE TRIGGER NPTXPAY_DBT_TD
   BEFORE DELETE 
   ON DNPTXPAY_DBT
   FOR EACH ROW
BEGIN
   IF DELETING THEN
      DELETE FROM DNPTXPAYOBJ_DBT WHERE t_NPTXPAYID = :OLD.t_ID;
   END IF;
END NPTXPAY_DBT_TD;
/

CREATE OR REPLACE TRIGGER DNPTXOP_DBT_FOBJ
  AFTER INSERT ON DNPTXOP_DBT
  FOR EACH ROW
  WHEN (NEW.T_DOCKIND = 4605)
DECLARE
   v_PRIORITY  NUMBER := 0;
   v_cnt NUMBER := 0;
BEGIN
   SELECT Count(1) INTO v_cnt
     FROM DNPTXOPSKIPFO_TMP
    WHERE t_DocKind  = :NEW.T_DOCKIND
      AND t_Client   = :NEW.T_CLIENT
      AND t_OperDate = :NEW.T_OPERDATE
      AND t_Code     = :NEW.T_CODE;

   IF v_cnt = 0 THEN
     v_PRIORITY := Rsb_Common.GetRegIntValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ТЕХНИЧЕСКИЙ РАСЧЕТ НОБ\ПРИОРИТЕТ ОПЕР. НОБ ДЛЯ FUNCOBJ', 0);
     INSERT INTO DFUNCOBJ_DBT  ( T_ID
                               , T_OBJECTTYPE
                               , T_OBJECTID
                               , T_FUNCID
                               , T_PRIORITY
                               ) VALUES
                               ( 0
                               , :NEW.T_DOCKIND
                               , :NEW.T_ID
                               , 550
                               , v_PRIORITY
                               );
   END IF;
END DNPTXOP_DBT_FOBJ;
/

CREATE OR REPLACE TRIGGER NPTXTOTALBASE_DBT_TIU
   BEFORE UPDATE 
   ON DNPTXTOTALBASE_DBT
   FOR EACH ROW
DECLARE
BEGIN

   IF     :NEW.T_STORSTATE = RSI_NPTXC.NPTXTOTALBASE_STORSTATE_ACTIVE 
      AND :NEW.T_CONFIRMSTATE = RSI_NPTXC.NPTXTOTALBASE_CONFIRMSTATE_CONFIRMED 
      AND :OLD.T_CONFIRMSTATE <> RSI_NPTXC.NPTXTOTALBASE_CONFIRMSTATE_CONFIRMED 
      AND :NEW.T_INSTANCE > :OLD.T_INSTANCE
   THEN
     :NEW.T_SENDDATE := TRUNC(SYSDATE);
     :NEW.T_SENDTIME := TO_DATE('01-01-0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');
   END IF;

   IF     :NEW.T_STORSTATE = RSI_NPTXC.NPTXTOTALBASE_STORSTATE_CANCELED 
      AND :OLD.T_STORSTATE <> RSI_NPTXC.NPTXTOTALBASE_STORSTATE_CANCELED 
      AND :NEW.T_INSTANCE > :OLD.T_INSTANCE
   THEN
     :NEW.T_CANCELDATE := TRUNC(SYSDATE);
     :NEW.T_CANCELTIME := TO_DATE('01-01-0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');
   END IF;

END NPTXTOTALBASE_DBT_TIU;
/


CREATE OR REPLACE TRIGGER DNPTXNKDREQDIAS_DBT_UPD
  BEFORE UPDATE ON DNPTXNKDREQDIAS_DBT
  FOR EACH ROW
DECLARE
BEGIN
   :NEW.T_CHANGEDATE := TRUNC(SYSDATE);
   :NEW.T_CHANGETIME := TO_DATE('01-01-0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD-MM-YYYY:HH24:MI:SS');

END DNPTXNKDREQDIAS_DBT_UPD;
/

CREATE OR REPLACE TRIGGER DPTRESIDENTHIST_DBT_IEMIT
  AFTER INSERT OR UPDATE ON DPTRESIDENTHIST_DBT
  FOR EACH ROW
DECLARE
   v_IsIssuer NUMBER := 0;

   v_AttrID NUMBER := 0;
BEGIN

   SELECT COUNT(1) INTO v_IsIssuer
     FROM DPARTYOWN_DBT
    WHERE T_PARTYID = :NEW.T_PARTYID
      AND T_PARTYKIND = 5;

   IF v_IsIssuer > 0 THEN

     BEGIN
       SELECT RSB_SECUR.GetMainObjAttr(5, LPAD(cntr.t_CountryID, 10, '0'), 2, :NEW.T_DATE) 
         INTO v_AttrID
         FROM dcountry_dbt cntr
        WHERE cntr.t_CodeLat3 = :NEW.t_NRCountry;

       EXCEPTION
           WHEN OTHERS THEN v_AttrID := 0;

     END;

     IF v_AttrID = 3 THEN
       BEGIN
         categ_utils.save_categ(3, 112, LPAD(:NEW.T_PARTYID, 10, '0'), 1, :NEW.T_DATE);

         EXCEPTION
             WHEN OTHERS THEN NULL;
       END;
     ELSE
       UPDATE DOBJATCOR_DBT
          SET T_VALIDTODATE = :NEW.T_DATE - 1 
        WHERE T_OBJECTTYPE = 3
          AND T_GROUPID = 112
          AND T_OBJECT = LPAD(:NEW.T_PARTYID, 10, '0')
          AND T_VALIDFROMDATE < :NEW.T_DATE
          AND T_VALIDTODATE = TO_DATE('31.12.9999','DD.MM.YYYY');

       UPDATE DOBJATCOR_DBT
          SET T_ATTRID = 2 
        WHERE T_OBJECTTYPE = 3
          AND T_GROUPID = 112
          AND T_OBJECT = LPAD(:NEW.T_PARTYID, 10, '0')
          AND T_VALIDFROMDATE = :NEW.T_DATE
          AND T_VALIDTODATE = TO_DATE('31.12.9999','DD.MM.YYYY');

     END IF;

   END IF;

END DPTRESIDENTHIST_DBT;
/

CREATE OR REPLACE TRIGGER DPARTYOWN_DBT_IEMIT
  AFTER INSERT ON DPARTYOWN_DBT  FOR EACH ROW
WHEN (
NEW.T_PARTYKIND = 5
      )
DECLARE
   v_AttrID NUMBER := 0;
BEGIN

   BEGIN
     SELECT RSB_SECUR.GetMainObjAttr(5, LPAD(cntr.t_CountryID, 10, '0'), 2, TRUNC(SYSDATE)) 
       INTO v_AttrID
       FROM dparty_dbt pt, dcountry_dbt cntr
      WHERE pt.t_PartyID = :NEW.T_PARTYID
        AND cntr.t_CodeLat3 = pt.t_NRCountry;

     EXCEPTION
         WHEN OTHERS THEN v_AttrID := 0;

   END;

   IF v_AttrID = 3 AND RSB_SECUR.IsIssuerEAEU(:NEW.t_PartyID, TRUNC(SYSDATE)) = 0 THEN
   
     BEGIN
       categ_utils.save_categ(3, 112, LPAD(:NEW.T_PARTYID, 10, '0'), 1, TRUNC(SYSDATE));

       EXCEPTION
           WHEN OTHERS THEN NULL;
     END;
   END IF;

END DPARTYOWN_DBT_IEMIT;
/


CREATE OR REPLACE TRIGGER DNPTXMASSPROT_DBT_TBI
  BEFORE INSERT ON DNPTXMASSPROT_DBT FOR EACH ROW
BEGIN

  IF :NEW.T_CLIENTID <> CHR(1) THEN

    BEGIN
      SELECT t_Name INTO :NEW.t_ClientName
        FROM dparty_dbt
       WHERE t_PartyID = :NEW.T_CLIENTID;

      EXCEPTION
         WHEN OTHERS THEN :NEW.t_ClientName := CHR(1);
    END;

    BEGIN
      SELECT t_Code INTO :NEW.t_ClientCode
        FROM dobjcode_dbt
       WHERE T_OBJECTID = :NEW.T_CLIENTID
         AND T_OBJECTTYPE = 3
         AND T_CODEKIND = 101
         AND T_STATE = 0
         AND ROWNUM = 1;

      EXCEPTION
         WHEN OTHERS THEN :NEW.t_ClientCode := CHR(1);
    END;

  END IF; 

END DNPTXMASSPROT_DBT_TBI;
/