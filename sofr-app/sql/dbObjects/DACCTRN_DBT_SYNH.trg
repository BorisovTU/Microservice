CREATE OR REPLACE TRIGGER "DACCTRN_DBT_SYNH"
AFTER DELETE OR INSERT OR UPDATE
ON DACCTRN_DBT 
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
-- fix: 20190110 - 12:07
-- 20190110 - 13:46 - Result Carry <> 46
-- 20190111 - 19:33 - T_USERFIELD3
-- 20190115 - 12:50 - MASSIVE CHANGES
-- 20190117 - 17:18 - T_ACCOUNT_PAYER
-- 20190117 - 21:33 - Result Carry <> 82
-- 20190118 - 18:23 - Result Carry <> 83 (-); Result Carry <> 84 (-); 30102 кроме 10, 15, 20, 35 пачек (-)
-- 20190119 - 11:11 - Result Carry <> 46
-- 20190122 - 11:20 - условие на автовыгрузку для обновления userfield3 = 1 для пачки 30
-- 20190131 - 16:24 - не выгружаем проводки с 202* счетами по дебету или кредиту
-- 20190226 - 18:42 - не выгружаем проводки с 303* счетами по дебету или кредиту (чтобы не выгружались проводки в дополнение к платежам)
-- 20190227 - 19:31 - ВЫГРУЖАЕМ проводки по некоторым счетам 30114 по 10 пачке
-- 20190305 -       - ВЫГРУЖАЕМ проводки по некоторым счетам 30110 по 10 пачке
-- 20190314 -       - ВЫГРУЖАЕМ проводки со счетами 30111 и 30109 в дебете по 160 пачке
-- 20190315 -       - ВЫГРУЖАЕМ проводки со счетами 30109, 30111, 30112, 30113, 30116, 30117, 30122, 30123, 30301, 30303 в кредите по 160 пачке
-- 20190319 -       - Условия для 160 и 170 пачек дублируем в ветку UPDATING
-- 20190404 -       - ВЫГРУЖАЕМ проводки со счетами 30118 и 30119 по дебету или по кредиту
-- 20190409 -       - ВЫГРУЖАЕМ проводки 10 пачки со счетом 30114826400000000011 по дебету или по кредиту
-- 20190410 -       - При удалении проводки, если нет не обработанных событий - вставляем запись со статусом 1
-- 20190626 -       - закомментирована проверка "по срочному рынку"
-- 20190626 -       - Выгружаем проводки с пачкой 155
-- 20190718 -       - Выгружаем "ответную проводку на вх. платежи"
-- 20191016-        - Выгрузаем пачку 180 аналогично пачке 170
-- 20200227 -       - Условие по времени согласуем с триггером dacctrn_dbt_delete
-- 20200302 -       - Автоматическая выгрузка проводок по пачкам 20 и 11
-- 20200305 -       - Автоматическая выгрузка проводок по пачке 35
-- 20200310 -       - Автоматическая выгрузка проводок по пачке 10
-- 20200313 -       - Автоматическая выгрузка проводок по пачке 90
-- 20200319 -       - Автоматическая выгрузка проводок по пачке 40
-- 20200413 -       - При изменении пачки на (10, 11, 20, 35, 40, 90) считается, что это инициализация автовыгрузки. Cherednichenko
--20211116 -        - Перед удалением проводок иногда производится их апдейт в статус = 4, поэтому в utableprocessevent  последнее событие - это само удаление, а не создание/обновление, добавлено условие на t_type Гераськина
-- 20221226 - 28:59 - Для кредита 303* и userfield3 = 1 разрешена вставка события
--20230617          - При обновлении проводки userfield3 = 1 и существующем событии в статусе 2 вызываем ошибку
--20240212           -- DEF-59041 если проводка уже аннулирована, то при удалении не нужно создавать событие с типом 3 
    OBJECT_TYPE          CONSTANT uTableProcessEvent_dbt.T_OBJECTTYPE%TYPE := 1; -- дистрибутивный вид объекта Проводка

    OBJECT_OPER_TYPE_CRE CONSTANT uTableProcessEvent_dbt.T_TYPE%TYPE := 1; -- тип: Создание
    --OBJECT_OPER_TYPE_UPD CONSTANT uTableProcessEvent_dbt.T_TYPE%TYPE := 2; -- тип: Обновление
    OBJECT_OPER_TYPE_DEL CONSTANT uTableProcessEvent_dbt.T_TYPE%TYPE := 3; -- тип: Удаление

    DICT_DEF_STAT        CONSTANT dllvalues_dbt.T_LIST%TYPE := 5008;
    OBJECT_STATUS_DEF    dllvalues_dbt.T_FLAG%TYPE; -- статус по умолчанию
    OBJECT_STATUS_WAIT   CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := 11; -- статус: Ожидание ручной обработки
    OBJECT_STATUS_READY  CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := 1; -- статус: Готов к обработке
    OBJECT_STATUS_PROC   CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := 2; -- статус: Обработка
    OBJECT_STATUS_ARCH   CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := 4; -- статус: Архив
    OBJECT_STATUS_DEL    CONSTANT uTableProcessEvent_dbt.T_STATUS%TYPE := -45; -- статус: Проводка удалена (Deleted Entry - abcde)

    OBJECT_NOTE_DEL      CONSTANT uTableProcessEvent_dbt.T_NOTE%TYPE := 'Проводка удалена';
    OBJECT_NOTE_READY  CONSTANT uTableProcessEvent_dbt.T_NOTE%TYPE := 'Принудительная выгрузка';
    OBJECT_NOTE_WAIT  CONSTANT uTableProcessEvent_dbt.T_NOTE%TYPE := 'Принудительная выгрузка - отмена';    

    v_Status             uTableProcessEvent_dbt.T_STATUS%TYPE;
    v_ObjectId           uTableProcessEvent_dbt.T_OBJECTID%TYPE;
    v_RecId              uTableProcessEvent_dbt.T_RECID%TYPE;
    v_Type               uTableProcessEvent_dbt.T_TYPE%TYPE;
    v_Timestamp          uTableProcessEvent_dbt.T_TIMESTAMP%TYPE;

    v_skip               dacctrn_dbt.t_acctrnid%TYPE := 0;

    v_Status_ins         uTableProcessEvent_dbt.T_STATUS%TYPE;
    
    e_bad_status EXCEPTION;
    PRAGMA EXCEPTION_INIT (e_bad_status, -20003);
    
    PROCEDURE EventInsert(p_timestamp uTableProcessEvent_dbt.t_timestamp%type,
                          p_objecttype uTableProcessEvent_dbt.t_objecttype%type,
                          p_objectid uTableProcessEvent_dbt.t_objectid%type,
                          p_type uTableProcessEvent_dbt.t_type%type,
                          p_status uTableProcessEvent_dbt.t_status%type) IS
      v_rec uTableProcessEvent_dbt%rowtype;
    BEGIN
      v_rec.t_timestamp := p_timestamp;
      v_rec.t_lastupdate := p_timestamp;
      v_rec.t_objecttype := p_objecttype;
      v_rec.t_objectid := p_objectid;
      v_rec.t_type := p_type;
      v_rec.t_status := p_status;
      v_rec.t_oper := TO_NUMBER(SYS_CONTEXT('CLIENTCONTEXT', 'RSBANK_OPER'));
      
      trgpckg_dacctrn_dbt.v_tableprocessevent_ins(trgpckg_dacctrn_dbt.v_tableprocessevent_ins.count) := v_rec;
    END;
    
    PROCEDURE EventUpdate(p_recId uTableProcessEvent_dbt.t_recId%type,
                          p_status uTableProcessEvent_dbt.t_status%type,
                          p_note uTableProcessEvent_dbt.t_note%type,
                          p_timestamp uTableProcessEvent_dbt.t_timestamp%type) IS
      v_rec uTableProcessEvent_dbt%rowtype;
    BEGIN
      v_rec.t_recId := p_recId;
      v_rec.t_status := p_status;
      v_rec.t_note := p_note;
      v_rec.t_timestamp := p_timestamp;
      
      trgpckg_dacctrn_dbt.v_tableprocessevent_upd(trgpckg_dacctrn_dbt.v_tableprocessevent_upd.count) := v_rec;
    END; 
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
        IF INSERTING THEN
          v_ObjectId := :new.T_ACCTRNID;
        ELSE
          v_ObjectId := :old.T_ACCTRNID;
        END IF;
        
        SELECT A.T_RECID, A.T_OBJECTID, A.T_STATUS, A.T_TYPE, A.T_TIMESTAMP
          INTO v_RecId, v_ObjectId, v_Status, v_Type, v_Timestamp
          FROM uTableProcessEvent_dbt A
         WHERE A.T_OBJECTTYPE = OBJECT_TYPE
           AND A.T_OBJECTID = v_ObjectId
           AND A.T_RECID = (SELECT MAX(B.T_RECID) 
                              FROM uTableProcessEvent_dbt B
                             WHERE B.T_OBJECTTYPE = A.T_OBJECTTYPE
                               AND B.T_OBJECTID = A.T_OBJECTID);

    EXCEPTION WHEN NO_DATA_FOUND THEN
        v_ObjectId := NULL;
        v_Status := NULL;
        v_RecId := NULL;
    END;

    -- Значение статуса по умолчанию
    IF DELETING AND :old.T_USERFIELD3 = '1'
       OR 
       (INSERTING OR UPDATING) AND :new.T_USERFIELD3 = '1'
    THEN
        v_Status_ins := OBJECT_STATUS_READY;
    ELSE
        IF :new.T_NUMBER_PACK IN (10, 11, 20, 35, 40, 90)
        THEN
            v_Status_ins := OBJECT_STATUS_READY;
        ELSE
            v_Status_ins := OBJECT_STATUS_DEF;
        END IF;
    END IF;

/*--------*/
/* DELETE */

    IF DELETING AND v_RecId IS NOT NULL -- если не нашли следов события создания, то ничего не делаем
    THEN

        -- Если найдено не выгруженное событие создания проводки
        IF v_Status IS NOT NULL
            AND v_type = OBJECT_OPER_TYPE_CRE
            AND (v_Status = OBJECT_STATUS_WAIT
                 OR (v_Status = OBJECT_STATUS_READY AND v_Timestamp >= (sysdate - NUMTODSINTERVAL(1, 'MINUTE'))))
        THEN

           --BIQ-10484 CCBO-5829 CCBO-6443  не отправлять в ЦФТ для РОВУ по депозитарным комиссиям
           if not (SUBSTR(:old.T_ACCOUNT_PAYER, 1, 3) = '306'  
                and :old.t_userfield2 = '1' 
                and SUBSTR(:old.t_userfield4, -2, 2) = '#1') 
           then
              -- переведем в статус Проводка удалена
              EventUpdate(p_recId => v_RecId,
                          p_status => OBJECT_STATUS_DEL,
                          p_note => OBJECT_NOTE_DEL,
                          p_timestamp => SYSDATE);
           end if;

        ELSIF v_Status IS NOT NULL
               AND v_type = OBJECT_OPER_TYPE_CRE
               AND v_Status = OBJECT_STATUS_DEL AND v_Timestamp >= (sysdate - NUMTODSINTERVAL(1, 'MINUTE'))
        THEN
            NULL;
        
        ELSIF v_Status IS NOT NULL
               AND v_type = OBJECT_OPER_TYPE_DEL
        THEN
            NULL;

        ELSIF Instr(:old.T_USERFIELD4, 'АННУЛ') > 0 
        THEN 
            -- DEF-59041 если уже аннулировано, то событие не нужно создавать
            NULL;
        ELSE
            -- вставляем новую запись
            EventInsert(p_timestamp => SYSDATE,
                        p_objecttype => OBJECT_TYPE,
                        p_objectid => :old.T_ACCTRNID,
                        p_type => OBJECT_OPER_TYPE_DEL,
                        p_status => OBJECT_STATUS_READY);
        END IF;

    -- работает с :old
    END IF;

/* DELETE */
/*--------*/
/* UPDATE */

    IF UPDATING
       AND v_RecId IS NOT NULL -- если не нашли следов события создания, то ничего не делаем
       AND :new.T_STATE = 4
    THEN
        -- если в таблице синхронизации есть запись по счету
        IF v_Status IS NOT NULL
            AND v_type = OBJECT_OPER_TYPE_CRE
            AND (v_Status = OBJECT_STATUS_WAIT
                 OR (v_Status = OBJECT_STATUS_READY AND v_Timestamp >= (sysdate - NUMTODSINTERVAL(1, 'MINUTE'))))
        THEN
            -- переведем в статус Проводка удалена
            EventUpdate(p_recId => v_RecId,
                        p_status => OBJECT_STATUS_DEL,
                        p_note => OBJECT_NOTE_DEL,
                        p_timestamp => SYSDATE);

        ELSIF v_Status IS NOT NULL
               AND v_type = OBJECT_OPER_TYPE_CRE
               AND v_Status = OBJECT_STATUS_DEL AND v_Timestamp >= (sysdate - NUMTODSINTERVAL(1, 'MINUTE'))
        THEN
            NULL;

        ELSIF v_Status IS NOT NULL
               AND v_type = OBJECT_OPER_TYPE_DEL
        THEN
            NULL;
        ELSE
            -- вставляем новую запись
            EventInsert(p_timestamp => SYSDATE,
                        p_objecttype => OBJECT_TYPE,
                        p_objectid => :new.T_ACCTRNID,
                        p_type => OBJECT_OPER_TYPE_DEL,
                        p_status => OBJECT_STATUS_READY);
        END IF;

    ELSIF UPDATING AND v_RecId IS NOT NULL and :new.T_USERFIELD3 = '1' /*and :new.T_NUMBER_PACK = 30*/ THEN
       IF v_Status IS NOT NULL
          AND v_Status = OBJECT_STATUS_WAIT
       THEN
           -- переведем в статус Проводка готова к выгрузке
           EventUpdate(p_recId => v_RecId,
                       p_status => OBJECT_STATUS_READY,
                       p_note => OBJECT_NOTE_READY,
                       p_timestamp => SYSDATE);

       -- Для случая, когда найдено событие в статусе 2
       ELSIF v_Status IS NOT NULL
             AND :new.T_USERFIELD3 != :old.T_USERFIELD3
             AND v_Status = OBJECT_STATUS_PROC
       THEN
           -- вставляем новую запись
           -- DEF-42283 вызываем ошибку
           /*INSERT INTO uTableProcessEvent_dbt (T_TIMESTAMP, T_OBJECTTYPE, T_OBJECTID, T_TYPE, T_STATUS)
                VALUES (SYSDATE, OBJECT_TYPE, :new.T_ACCTRNID, OBJECT_OPER_TYPE_CRE, OBJECT_STATUS_READY);*/
                dbms_output.put_line('!');
           raise_application_error(-20003,'Выгрузка проводки не завершена');
       END IF;
       
    ELSIF UPDATING AND v_RecId IS NOT NULL and :old.T_USERFIELD3 = '1' and :new.T_USERFIELD3 != '1'  /*and :new.T_NUMBER_PACK = 30*/ THEN
        IF v_Status IS NOT NULL
           AND v_Status = OBJECT_STATUS_READY
           AND v_Timestamp < (sysdate - NUMTODSINTERVAL(1, 'MINUTE'))
        THEN
            -- переведем в статус Проводка ожидает дальнейшего развития событий
            EventUpdate(p_recId => v_RecId,
                        p_status => OBJECT_STATUS_WAIT,
                        p_note => OBJECT_NOTE_WAIT,
                        p_timestamp => SYSDATE);
        END IF;

    -- 13-04-2020 Cherednichenko при изменении пачки на (10, 11, 20, 35, 40, 90) считается, что это инициализация автовыгрузки.
    ELSIF UPDATING AND :new.T_NUMBER_PACK IN (10, 11, 20, 35, 40, 90) AND :old.T_NUMBER_PACK != :new.T_NUMBER_PACK
        THEN
            EventUpdate(p_recId => v_RecId,
                        p_status => OBJECT_STATUS_READY,
                        p_note => null,
                        p_timestamp => SYSDATE);

    -- Предполагается, что T_NUMBER_PACK обновляется отдельно от других полей
    ELSIF UPDATING
          AND v_RecId IS NULL
          AND (:new.T_NUMBER_PACK IN (160, 170, 180)
          AND ((SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 3) <> '301' -- исключаем проводки по кредиту кор.счета
                OR (SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 5) IN ('30109',
                                                              '30111',
                                                              '30112',
                                                              '30113',
                                                              '30116',
                                                              '30117',
                                                              '30122',
                                                              '30123')
                    AND :new.T_NUMBER_PACK = 160  )
                --OR (SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 5) =  ('30102')
                OR (SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 3) = ('301') -- выгружаем "ответные проводки на входящие платежи"
                    AND :new.T_NUMBER_PACK in ( 170, 180) )
               )
               AND (SUBSTR(:new.T_ACCOUNT_PAYER, 1, 3) <> '301' -- исключаем проводки по дебету кор.счета
                    OR (SUBSTR(:new.T_ACCOUNT_PAYER, 1, 5) IN ('30111',
                                                               '30109')
                        AND :new.T_NUMBER_PACK = 160 )
                    OR (SUBSTR(:new.T_ACCOUNT_PAYER, 1, 3) = '301'
                        AND :new.T_NUMBER_PACK in ( 170,180) )    
                        )
               AND (SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 3) <> '303' -- исключаем, чтобы не выгружались проводки в дополнение к платежам
                    OR (SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 5) IN ('30301',
                                                                  '30303')
                        AND :new.T_NUMBER_PACK = 160 ))
               AND SUBSTR(:new.T_ACCOUNT_PAYER, 1, 3) <> '303' -- исключаем, чтобы не выгружались проводки в дополнение к платежам
               AND SUBSTR(:new.T_ACCOUNT_PAYER, 1, 3) <> '202' -- исключаем проводки по дебету 202*
               AND SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 3) <> '202' -- исключаем проводки по кредиту 202*
               AND INSTR('134',TO_CHAR(:new.T_CHAPTER)) > 0 -- счета Глав 1, 3, 4
               AND :new.T_DATE_CARRY >= TO_DATE('01012019','ddmmyyyy') -- спец. указание
               AND :new.T_RESULT_CARRY <> 18 -- исключаем проводки переоценки
               AND :new.T_RESULT_CARRY <> 82 -- исключаем проводки по "переоценка исходящего остатка"
               AND :new.T_RESULT_CARRY <> 46 -- исключаем проводки по "документ урегулирования парных счетов" - "выгружаться НЕ должны" (принято решение фильтровать отдельно от верхнего условия)
              )
               OR :new.T_NUMBER_PACK = 155
               OR (:new.T_NUMBER_PACK NOT IN (160, 170, 180, 155) 
                         AND :new.T_USERFIELD3 = '1'  AND SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 5) IN ('30301', '30303') )
               )
    THEN
/*
        BEGIN
            SELECT 1
              INTO v_skip
              FROM doprdocs_dbt op, doproper_dbt do
             WHERE op.t_id_operation = do.t_id_operation
               AND op.t_acctrnid = :new.t_acctrnid
               AND op.t_dockind = 1
               AND do.t_kind_operation IN (12640,12630,12625,12620,12615,12610,12600)
               AND do.t_start_date >= TO_DATE('25.12.2018', 'dd.mm.yyyy');

        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                v_skip := 0;
        END;
*/
        IF v_skip = 0
        THEN
            EventInsert(p_timestamp => SYSDATE,
                        p_objecttype => OBJECT_TYPE,
                        p_objectid => :new.T_ACCTRNID,
                        p_type => OBJECT_OPER_TYPE_CRE,
                        p_status => v_Status_ins);
        END IF;

    -- работает с :old и с :new
    END IF;

/* UPDATE */
/*--------*/
/* INSERT */

/*
    BEGIN
--    select tr.t_acctrnid
--      into v_skip
--      from doprdocs_dbt op,dacctrn_dbt tr,doproper_dbt do
--     where tr.t_acctrnid = :new.t_acctrnid
--       and op.t_id_operation =do.t_id_operation  
--       and op.t_acctrnid=tr.t_acctrnid
--       and op.t_dockind = 1 
--       and do.t_kind_operation in (12640,12630,12625,12620,12615,12610,12600) 
--       and do.t_start_date>= to_date('25.12.2018','dd.mm.yyyy');

        SELECT 1
          INTO v_skip
          FROM doprdocs_dbt op, doproper_dbt do
         WHERE op.t_id_operation = do.t_id_operation
           AND op.t_acctrnid = :new.t_acctrnid
           AND op.t_dockind = 1
           AND do.t_kind_operation IN (12640,12630,12625,12620,12615,12610,12600)
           AND do.t_start_date >= TO_DATE('25.12.2018', 'dd.mm.yyyy');

    EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
            v_skip := 0;
    END;
*/

    -- Временно фильтруем не подходящие проводки
    IF ((v_skip = 0) /*AND (:new.T_RESULT_CARRY <> 46)*/) --maa150119-отключил проверку на 46 по просьбе Банка
    THEN
        IF INSERTING
           AND :new.T_RESULT_CARRY <> 18 -- исключаем проводки переоценки
           AND :new.T_RESULT_CARRY <> 82 -- исключаем проводки по "переоценка исходящего остатка"
           --AND :new.T_RESULT_CARRY <> 83 -- исключаем проводки по "курсовая разница в мультивалютной проводке"
           --AND :new.T_RESULT_CARRY <> 84 -- исключаем проводки по "доп.переоценка при выполнении операций"
           AND :new.T_RESULT_CARRY <> 46 -- исключаем проводки по "документ урегулирования парных счетов" - "выгружаться НЕ должны" (принято решение фильтровать отдельно от верхнего условия)
           AND (((SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 3) <> '301' -- исключаем проводки по кредиту кор.счета
                  OR SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 5) IN ('30118',
                                                               '30119')
                  OR (:new.T_ACCOUNT_RECEIVER IN ('30114810400000000001',
                                                  '30114810700000000002',
                                                  '30114840300000000029',
                                                  '30114840800000000011',
                                                  '30114978300000000030',
                                                  '30114978600000000031',
                                                  '30110810100000000033',
                                                  '30114826400000000011')
                      AND :new.T_NUMBER_PACK = 10)
                  OR (SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 5) IN ('30109',
                                                                '30111',
                                                                '30112',
                                                                '30113',
                                                                '30116',
                                                                '30117',
                                                                '30122',
                                                                '30123')
                      AND :new.T_NUMBER_PACK = 160 )
                  --OR (SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 5) =  ('30102')
                  OR (SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 3) = ('301') -- выгружаем "ответные проводки на входящие платежи"
                      AND :new.T_NUMBER_PACK in ( 170,180) )
                 )
           --AND (SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 5) <> '30102'
                --OR ((SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 5) = '30102') AND (:new.T_NUMBER_PACK NOT IN(10, 15, 20, 35))))
                AND (SUBSTR(:new.T_ACCOUNT_PAYER, 1, 3) <> '301' -- исключаем проводки по дебету кор.счета
                     OR SUBSTR(:new.T_ACCOUNT_PAYER, 1, 5) IN ('30118',
                                                               '30119')
                     OR (:new.T_ACCOUNT_PAYER IN ('30114810400000000001',
                                                  '30114810700000000002',
                                                  '30114840300000000029',
                                                  '30114840800000000011',
                                                  '30114978300000000030',
                                                  '30114978600000000031',
                                                  '30110810100000000033',
                                                  '30114826400000000011')
                         AND :new.T_NUMBER_PACK = 10)
                     OR (SUBSTR(:new.T_ACCOUNT_PAYER, 1, 5) IN ('30111',
                                                                '30109')
                         AND :new.T_NUMBER_PACK =  160) 
                     OR (SUBSTR(:new.T_ACCOUNT_PAYER, 1, 3) = '301'
                         AND :new.T_NUMBER_PACK in ( 170,180) )
                    )            
                AND (SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 3) <> '303' -- исключаем, чтобы не выгружались проводки в дополнение к платежам
                     OR (SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 5) IN ('30301',
                                                                   '30303')
                         AND :new.T_NUMBER_PACK = 160) 
                      OR (SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 3) = '303'  AND :new.t_userfield3 = '1')
                    )
                AND SUBSTR(:new.T_ACCOUNT_PAYER, 1, 3) <> '303' -- исключаем, чтобы не выгружались проводки в дополнение к платежам
           --AND SUBSTR(:new.T_ACCOUNT_PAYER, 1, 3) <> '202' -- исключаем проводки по дебету 202*
           --AND SUBSTR(:new.T_ACCOUNT_RECEIVER, 1, 3) <> '202' -- исключаем проводки по кредиту 202*
                )
                OR :new.T_NUMBER_PACK = 155
               )
           AND INSTR('134',TO_CHAR(:new.T_CHAPTER)) > 0 -- счета Глав 1, 3, 4
           AND :new.T_DATE_CARRY >= TO_DATE('01012019','ddmmyyyy') -- спец. указание
        THEN
            EventInsert(p_timestamp => SYSDATE,
                        p_objecttype => OBJECT_TYPE,
                        p_objectid => :new.T_ACCTRNID,
                        p_type => OBJECT_OPER_TYPE_CRE,
                        p_status => v_Status_ins);
            -- работает с :new
        END IF;
    END IF;

/* INSERT */
/*--------*/

EXCEPTION
    WHEN e_bad_status THEN 
        RAISE;
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(sqlerrm);

END;
/