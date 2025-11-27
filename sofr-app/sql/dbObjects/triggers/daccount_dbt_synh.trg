CREATE OR REPLACE TRIGGER "DACCOUNT_DBT_SYNH"
AFTER /*DELETE OR*/ INSERT OR UPDATE ON DACCOUNT_DBT REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
-- 20190110 - 11:18
-- 20190111 - 19:31 - T_USERFIELD3
-- 20190128 - 13:08 - T_USERTYPEACCOUNT
-- 20190130 - 15:32 - T_BALANCE <> 302 - начинаем выгружать
-- 20190207 - 15:05 - разрешены 70602, 70607, и 30602, 30606 для ЮЛ
-- 20190213 - 20:48 - изменена проверка полей на попадание в таблицу событий
-- 20190409 -       - добавлено условие по счетам 47423
-- 20190507 -       - изменен алгоритм поиска v_legalform (определяется по-разному для разных счетов)
-- 20190626 -       - закомментирована проверка "по срочному рынку"
-- 20190906         - добавлены условия на событие закрытие счетов
-- 20190930 -       - 407* не выгружаются
-- 20201225       - 30603 разрешены к выгрузке Cherednichenko is 520662 
 OBJECT_TYPE            CONSTANT uTableProcessEvent_dbt.T_OBJECTTYPE%TYPE := 4; --дистрибутивный вид объекта Счет
 DICT_DEF_STAT          CONSTANT dllvalues_dbt.T_LIST%TYPE := 5008;
-- OBJECT_STATUS          CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := /*1*/ 11; --готов к обработке
 OBJECT_STATUS_DEF      dllvalues_dbt.T_FLAG%TYPE; -- статус по умолчанию
 OBJECT_STATUS_WAIT     CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := 11; -- ожидание обработки
 OBJECT_STATUS_READY    CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := 1; --готов к обработке

 v_Status               uTableProcessEvent_dbt.T_STATUS%TYPE;
 v_ObjectId             uTableProcessEvent_dbt.T_OBJECTID%TYPE;
 v_RecId                uTableProcessEvent_dbt.T_RECID%TYPE;
 v_Type                 uTableProcessEvent_dbt.T_TYPE%TYPE;
 v_skip                 daccount_dbt.t_accountid%TYPE := 0;

 v_Status_ins           uTableProcessEvent_dbt.T_STATUS%TYPE;
 v_legalform        dparty_dbt.t_legalform%type;  
 v_Isemployer       dpersn_dbt.t_Isemployer%type;

BEGIN

 -- Определяем статус по умолчанию для данного типа объекта
 BEGIN
     SELECT T_FLAG INTO OBJECT_STATUS_DEF
       FROM DLLVALUES_DBT
      WHERE T_LIST = DICT_DEF_STAT
        AND T_ELEMENT = OBJECT_TYPE;
 EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
         OBJECT_STATUS_DEF := OBJECT_STATUS_WAIT;
 END;

 --найдём запись по объекту синхронизации в таблице синхронизации 
 BEGIN

   SELECT A.T_RECID, A.T_OBJECTID, A.T_STATUS, A.T_TYPE INTO v_RecId, v_ObjectId, v_Status, v_Type
   FROM uTableProcessEvent_dbt A
   WHERE A.T_OBJECTTYPE = OBJECT_TYPE
    AND A.T_OBJECTID = COALESCE( :new.T_ACCOUNTID, :old.T_ACCOUNTID )
    AND A.T_RECID = ( SELECT MAX(B.T_RECID) 
                      FROM uTableProcessEvent_dbt B
                      WHERE B.T_OBJECTTYPE = A.T_OBJECTTYPE
                       AND B.T_OBJECTID = A.T_OBJECTID);
   
 EXCEPTION WHEN NO_DATA_FOUND THEN

  v_Status := NULL;
  v_RecId := NULL;
  v_Type := NULL;

 END;

 IF :new.T_USERFIELD3 = '1'
 THEN
     v_Status_ins := OBJECT_STATUS_READY;
 ELSE
     v_Status_ins := OBJECT_STATUS_DEF;
 END IF;
 
 if :new.T_BALANCE in ('30601','30606') then
     v_legalform := 0;
     begin
         select t_legalform into v_legalform
           from dparty_dbt p where t_partyid = COALESCE( :new.t_client, :old.t_client )
            and not exists (select 1 from ddp_dep_dbt dep where dep.t_partyid = p.t_partyid);
     exception
         when others then v_legalform := 0;
     end;

 elsif :new.T_BALANCE = '47423' then
     v_legalform := 0;
     begin
         select t_legalform into v_legalform
           from dparty_dbt p where t_partyid = COALESCE( :new.t_client, :old.t_client );
     exception
         when others then v_legalform := 0;
     end;

 else
     -- На всякий случай оставляем
     v_legalform := 0;
 end if; 

--shev 17.08.2020 выгружаем для ИП

 if :new.T_BALANCE in ('30601') then
     -- Cherednichenko 2020-08-26 На проверке юрики ошибочно сбрасывались в физиков is 514983
     --v_legalform := 0;
     v_Isemployer := 0;
     begin
         select decode(t_isemployer,chr(88),1,0) into v_Isemployer
           from dpersn_dbt p where p.t_personid = COALESCE( :new.t_client, :old.t_client )
            and not exists (select 1 from ddp_dep_dbt dep where dep.t_partyid = p.t_personid);
     exception
         when others then v_Isemployer := 0;
     end;

 elsif :new.T_BALANCE = '47423' then
     -- Cherednichenko 2020-08-20 На проверке юрики ошибочно сбрасывались в физиков is 514697
     --v_legalform := 0;
     v_Isemployer := 0;
     begin
        select decode(t_isemployer,chr(88),1,0) into v_Isemployer
           from dpersn_dbt p where p.t_personid = COALESCE ( :new.t_client, :old.t_client );
     exception
         when others then v_Isemployer := 0;
     end;

 else
     -- На всякий случай оставляем
     v_Isemployer := 0;
 end if; 


 IF (v_skip = 0) and (INSTR('134',TO_CHAR(:new.T_CHAPTER)) > 0)  THEN --счета Глав 1, 3, 4
    IF (INSTR(:new.T_BALANCE, '2') <> 1)
        AND (INSTR(:new.T_BALANCE, '301') <> 1)
        AND (:new.T_BALANCE <> '40817')
        AND (INSTR(:new.T_BALANCE, '304') <> 1)
        AND (   INSTR(:new.T_BALANCE, '306') <> 1
             OR (:new.T_BALANCE in ('30601','30606') and v_legalform = 1  )
             OR (:new.T_BALANCE in ('30601','30606') and v_Isemployer = 1)
             OR  :new.T_BALANCE = '30603' ) -- 2020-12-25 Cherednichenko is 520662 30603 разрешены к выгрузке
        AND (   :new.T_BALANCE <> '47423'
             OR (:new.T_BALANCE = '47423' and v_legalform = 1)
             OR (:new.T_BALANCE in ('47423','47423') and v_Isemployer = 1) )

        AND (   INSTR(:new.T_BALANCE, '603') <> 1
             OR :new.T_BALANCE in ('30603', '60347') ) -- 2020-12-25 Cherednichenko is 520662 30603 разрешены к выгрузке
        AND ( INSTR(:new.T_BALANCE, '7') <> 1 OR :new.T_BALANCE in ('70602', '70607') )
        AND (INSTR(:new.T_BALANCE, '407') <> 1)
    THEN
        IF INSERTING AND :new.T_OPEN_DATE >= TO_DATE('01012019','ddmmyyyy') THEN --спец. указание

            INSERT INTO uTableProcessEvent_dbt( T_TIMESTAMP, T_OBJECTTYPE, T_OBJECTID, T_TYPE, T_STATUS )
            VALUES( SYSDATE, OBJECT_TYPE, :new.T_ACCOUNTID, 1, v_Status_ins);
        -- работает с :new
        END IF;

        IF UPDATING THEN

            --если в таблице синхронизации нет записи обновления по счету
            IF v_Status IS NOT NULL AND v_Status = v_Status_ins THEN
                IF     :new.T_OPEN_CLOSE != :old.T_OPEN_CLOSE
                   AND :new.T_OPEN_CLOSE = 'З'
                   AND v_Type != 3
                THEN
                    -- Оставляем возможность выгрузки другого типа события
                    INSERT INTO uTableProcessEvent_dbt( T_TIMESTAMP, T_OBJECTTYPE, T_OBJECTID, T_TYPE, T_STATUS )
                    VALUES( SYSDATE, OBJECT_TYPE, :new.T_ACCOUNTID, 3, OBJECT_STATUS_WAIT);
                ELSE
                    IF v_Type != 3
                    THEN
                        UPDATE uTableProcessEvent_dbt
                           SET T_TYPE = 2, T_TIMESTAMP = SYSDATE
                         WHERE T_RECID = v_RecId;
                    ELSE
                        -- На случай изменения закрытого счета
                        -- считаем, что это невозможное событие                   
                        null;
                        --INSERT INTO uTableProcessEvent_dbt( T_TIMESTAMP, T_OBJECTTYPE, T_OBJECTID, T_TYPE, T_STATUS )
                        --VALUES( SYSDATE, OBJECT_TYPE, :new.T_ACCOUNTID, 3, OBJECT_STATUS_WAIT);
                    END IF;

                END IF;

            ELSE
                IF     :new.T_OPEN_CLOSE != :old.T_OPEN_CLOSE
                    OR :new.T_DEPARTMENT != :old.T_DEPARTMENT
                    OR :new.T_BRANCH != :old.T_BRANCH
                    OR :new.T_CLIENT != :old.T_CLIENT
                    OR :new.T_OPEN_DATE != :old.T_OPEN_DATE
                    OR :new.T_CLOSE_DATE != :old.T_CLOSE_DATE
                    OR :new.T_NAMEACCOUNT != :old.T_NAMEACCOUNT
                    OR :new.T_PAIRACCOUNT != :old.T_PAIRACCOUNT
--                    OR :new.T_USERFIELD3 != :old.T_USERFIELD3
                THEN
                
                    IF :new.T_OPEN_CLOSE = 'З'
                    THEN
                        if  v_Type != 3 or v_Status is null then  -- только если еще нет записи в таблице событий. неважно, с каким статусом
                            -- Для того, чтобы выгрузилась дата закрытия счета
                            INSERT INTO uTableProcessEvent_dbt( T_TIMESTAMP, T_OBJECTTYPE, T_OBJECTID, T_TYPE, T_STATUS )
                            VALUES( SYSDATE, OBJECT_TYPE, :new.T_ACCOUNTID, 3, OBJECT_STATUS_WAIT);
                        end if;
                    ELSE
                        INSERT INTO uTableProcessEvent_dbt( T_TIMESTAMP, T_OBJECTTYPE, T_OBJECTID, T_TYPE, T_STATUS )
                        VALUES( SYSDATE, OBJECT_TYPE, :new.T_ACCOUNTID, 2, v_Status_ins);
                    END IF;

/* Создаем событие только если изменили на '1' */
                ELSIF     :new.T_USERFIELD3 != :old.T_USERFIELD3
                      AND :new.T_USERFIELD3 = '1'
                THEN
                    INSERT INTO uTableProcessEvent_dbt( T_TIMESTAMP, T_OBJECTTYPE, T_OBJECTID, T_TYPE, T_STATUS )
                    VALUES( SYSDATE, OBJECT_TYPE, :new.T_ACCOUNTID, 2, v_Status_ins);
                END IF;

            END IF;

        -- работает с :old и с :new
        END IF;
    END IF;
 END IF;

EXCEPTION

  WHEN OTHERS
  THEN DBMS_OUTPUT.PUT_LINE(sqlerrm);

END;
/
