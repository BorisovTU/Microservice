CREATE OR REPLACE PACKAGE BODY IT_TransferRates
IS
    PROCEDURE SendEmailOnError (p_Text VARCHAR2)
    IS
        p_Email      VARCHAR2 (100)
            := TRIM (
                   RSB_COMMON.GetRegStrValue (
                       'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ТРАНСФЕРНЫЕ СТАВКИ МАКС\EMAIL_ДЛЯ_ОШИБОК_ЗАГР_ТРАНСФ_СТ'));
        p_Head       VARCHAR2 (70);
        p_FullText   VARCHAR2 (200);
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        p_Head :=
            'Ошибка при загрузке трансферных ставок из системы МАКС';
        p_FullText := p_Text || TO_CHAR (SYSDATE, 'dd.mm.yyyy hh24:mi:ss');

        IF p_Email is NOT NULL AND p_Email <> CHR(1)
        THEN
            INSERT INTO DEMAIL_NOTIFY_DBT (T_DATEADD,
                                           T_EMAIL,
                                           T_HEAD,
                                           T_TEXT)
                 VALUES (SYSDATE,
                         p_Email,
                         p_Head,
                         p_FullText);

            COMMIT;
        ELSE
            RAISE_APPLICATION_ERROR (
                -20002,
                'Ошибка получения почты из настройки РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ТРАНСФЕРНЫЕ СТАВКИ МАКС\EMAIL_ДЛЯ_ОШИБОК_ЗАГР_ТРАНСФ_СТ');
        END IF;
    END;

    PROCEDURE SetRates (p_worklogid   IN     INTEGER,
                        p_messbody    IN     CLOB,
                        p_messmeta    IN     XMLTYPE,
                        o_msgid          OUT VARCHAR2,
                        o_MSGCode        OUT INTEGER,
                        o_MSGText        OUT VARCHAR2,
                        o_messbody       OUT CLOB,
                        o_messmeta       OUT XMLTYPE)
    IS
        v_iso_code       VARCHAR2 (3);
        v_fiid           NUMBER := -1;
        v_frequency      VARCHAR2 (100);
        v_frequencyInt   NUMBER := 0;
        v_CountRates NUMBER := 0;
        v_CountNullRatesData NUMBER := 0;
        v_ratesType VARCHAR2(100);
    BEGIN
        EXECUTE IMMEDIATE 'truncate table DIT_TRANSFERRATES_TMP';
        
        BEGIN
            SELECT LOWER(ratesType)
              INTO v_ratesType
              FROM JSON_TABLE (
                       p_messbody,
                       '$'
                       COLUMNS ratesType VARCHAR2 (100) PATH '$.ratesType');
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                v_ratesType := NULL;
        END;
        
        IF v_ratesType = 'fixed' or v_ratesType = 'short_term' THEN
        BEGIN
            SELECT iso_code
              INTO v_iso_code
              FROM JSON_TABLE (
                       p_messbody,
                       '$'
                       COLUMNS iso_code VARCHAR2 (3) PATH '$.isoCode');
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                v_iso_code := NULL;
        END;

        BEGIN
            SELECT T_FIID
              INTO v_fiid
              FROM dfininstr_dbt finin
             WHERE     finin.T_CCY = v_iso_code
                   AND  finin.t_fi_kind IN (1,6);
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                v_fiid := -1;
        END;

        IF v_fiid = -1
        THEN
            it_log.log_error('MAKS.TransferRates',  'Ошибка при поиске FIID c ISO кодом ' || v_iso_code );
            SendEmailOnError (
                   'Ошибка при поиске FIID c ISO кодом '
                || v_iso_code
                || '. ');
            RAISE_APPLICATION_ERROR (
                -20001,
                   'Ошибка при поиске FIID c ISO кодом '
                || v_iso_code);
        END IF;

        BEGIN
            SELECT frequency
              INTO v_frequency
              FROM JSON_TABLE (
                       p_messbody,
                       '$'
                       COLUMNS frequency VARCHAR2 (100) PATH '$.frequency');
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                v_frequency := NULL;
                v_frequencyInt := -1;
        END;

        IF v_frequencyInt = -1
        THEN
            it_log.log_error('MAKS.TransferRates',  'Ошибка при получении frequency из сообщения для валюты ' || v_iso_code );
            SendEmailOnError (
                'Ошибка при получении frequency из сообщения ');
            RAISE_APPLICATION_ERROR (
                -20001,
                'Ошибка при получении frequency из сообщения ');
        END IF;

        IF TRIM (v_frequency) = 'ежемесячно'
        THEN
            v_frequencyInt := 1;
        ELSIF TRIM (v_frequency) = 'ежеквартально'
        THEN
            v_frequencyInt := 2;
        ELSIF TRIM (v_frequency) = 'раз в полгода'
        THEN
            v_frequencyInt := 3;
        ELSIF TRIM (v_frequency) = 'раз в год'
        THEN
            v_frequencyInt := 4;
        ELSIF TRIM (v_frequency) = 'в конце срока'
        THEN
            v_frequencyInt := 5;
        END IF;

        BEGIN
            INSERT INTO DIT_TRANSFERRATES_TMP (T_FIID,
                                               T_RATE,
                                               T_STARTDATE,
                                               T_PERIOD)
                SELECT v_fiid,
                       it_xml.char_to_number(rate) * 100,
                       TO_DATE (
                           TO_CHAR (
                               FROM_TZ (
                                   CAST (
                                       TO_TIMESTAMP_TZ (
                                           startdate,
                                           'YYYY-MM-DD"T"HH24:MI:SS.FFTZR')
                                           AS TIMESTAMP),
                                   'UTC')
                                   AT TIME ZONE 'EUROPE/MOSCOW',
                               'dd.mm.yyyy'),
                           'dd.mm.yyyy'),
                       period
                  FROM JSON_TABLE (
                           p_messbody,
                           '$.rates[*]' ERROR ON ERROR
                           COLUMNS rate VARCHAR2 (100) PATH '$.rate'  NULL ON EMPTY,
                           startdate VARCHAR2 (100) PATH '$.startDate'  NULL ON EMPTY,
                           period NUMBER (10) PATH '$.period'  NULL ON EMPTY);
        EXCEPTION
            WHEN OTHERS
            THEN
                it_log.log_error('MAKS.TransferRates',  'Ошибка получения массива rates: ' || sqlerrm);
                SendEmailOnError (
                    'Ошибка получения массива rates. ');
                RAISE_APPLICATION_ERROR (
                    -20001,
                    'Ошибка получения массива rates');
        END;

        SELECT COUNT(1) INTO v_CountRates FROM DIT_TRANSFERRATES_TMP WHERE T_RATE IS NOT NULL OR T_STARTDATE IS NOT NULL OR T_PERIOD IS NOT NULL;
        IF v_CountRates <= 0 THEN
            it_log.log_error('MAKS.TransferRates',  'Ошибка получения массива rates: для валюты ' || v_iso_code || ' нет массива трансферных ставок');
            SendEmailOnError (
                    'Ошибка получения массива rates. ');
            RAISE_APPLICATION_ERROR (
                    -20001,
                    'Ошибка получения массива rates');
        END IF;
        
        SELECT COUNT(1) INTO v_CountNullRatesData 
          FROM DIT_TRANSFERRATES_TMP 
         WHERE T_RATE IS NULL;
         
        IF v_CountNullRatesData > 0 THEN 
            it_log.log_error('MAKS.TransferRates',  'Ошибка получения значения rate: для валюты ' || v_iso_code || ' в массиве трансферных ставок есть пустые значения rate');
            SendEmailOnError (
                    'Ошибка получения значения rate. ');
            RAISE_APPLICATION_ERROR (
                    -20001,
                    'Ошибка получения значения rate');
        END IF;
        
        SELECT COUNT(1) INTO v_CountNullRatesData 
          FROM DIT_TRANSFERRATES_TMP 
         WHERE T_STARTDATE IS NULL;
         
        IF v_CountNullRatesData > 0 THEN 
            it_log.log_error('MAKS.TransferRates',  'Ошибка получения значения StartDate: для валюты ' || v_iso_code || ' в массиве трансферных ставок есть пустые значения StartDate');
            SendEmailOnError (
                    'Ошибка получения значения StartDate. ');
            RAISE_APPLICATION_ERROR (
                    -20001,
                    'Ошибка получения значения StartDate');
        END IF;
        
        SELECT COUNT(1) INTO v_CountNullRatesData 
          FROM DIT_TRANSFERRATES_TMP 
         WHERE T_PERIOD IS NULL;
         
        IF v_CountNullRatesData > 0 THEN 
            it_log.log_error('MAKS.TransferRates',  'Ошибка получения значения Period: для валюты ' || v_iso_code || ' в массиве трансферных ставок есть пустые значения Period');
            SendEmailOnError (
                    'Ошибка получения значения Period. ');
            RAISE_APPLICATION_ERROR (
                    -20001,
                    'Ошибка получения значения Period');
        END IF;

        INSERT INTO dmarketrate_dbt (T_ID,
                                     T_SERVISEKIND,
                                     T_BRANCHID,
                                     T_NAME,
                                     T_FULLNAME,
                                     T_PRODUCTKINDID,
                                     T_PRODUCTID,
                                     T_CONTRACTYEAR,
                                     T_CONTRACTMONTHS,
                                     T_CONTRACTDAYS,
                                     T_CONTRACTSUM,
                                     T_FIID,
                                     T_CLIENTTYPE,
                                     T_INFO,
                                     T_NOTUSE,
                                     T_OPER,
                                     T_INPUTDATE,
                                     T_CORRECTDATE,
                                     T_CHANGEDATE,
                                     T_SOURCEDATALAYER,
                                     T_INTERESTFREQUENCY)
            SELECT 0,
                   0,
                   1,
                      v_iso_code
                   || DECODE (transfrates.T_period,
                              0, '',
                              transfrates.T_period)
                   || DECODE (v_frequencyInt, 0, '', v_frequency),
                      v_iso_code
                   || DECODE (transfrates.T_period,
                              0, '',
                              transfrates.T_period)
                   || DECODE (v_frequencyInt, 0, '', v_frequency),
                   0,
                   0,
                   0,
                   0,
                   transfrates.T_period,
                   0,
                   transfrates.T_FIID,
                   0,
                   CHR (1),
                   CHR (0),
                   RsbSessionData.Oper,
                   TRUNC (SYSDATE),
                   TO_DATE ('01.01.0001', 'dd.mm.yyyy'),
                   TRUNC (SYSDATE),
                   sd.COLUMN_VALUE,
                   v_frequencyInt
              FROM DIT_TRANSFERRATES_TMP  transfrates
                   CROSS JOIN TABLE (sys.odcinumberlist (1, 2, 3)) sd -- t_sourcedatalayer
                   LEFT JOIN DMARKETRATE_DBT DAYS_MARKET
                       ON     DAYS_MARKET.T_SERVISEKIND = 0
                          AND DAYS_MARKET.T_PRODUCTKINDID = 0
                          AND DAYS_MARKET.T_PRODUCTID = 0
                          AND DAYS_MARKET.T_CLIENTTYPE = 0
                          AND DAYS_MARKET.T_SOURCEDATALAYER = sd.COLUMN_VALUE
                          AND DAYS_MARKET.T_FIID = transfrates.T_FIID
                          AND DAYS_MARKET.T_CONTRACTYEAR = 0
                          AND DAYS_MARKET.T_CONTRACTMONTHS = 0
                          AND DAYS_MARKET.T_INTERESTFREQUENCY =
                              v_frequencyInt
                          AND DAYS_MARKET.T_CONTRACTDAYS =
                              transfrates.T_PERIOD
             WHERE DAYS_MARKET.T_ID IS NULL;


        INSERT INTO DVALINTMARKETRATE_DBT (T_ID,
                                           T_MARKETRATEID,
                                           T_BEGINDATE,
                                           T_MINVALUE,
                                           T_MAXVALUE,
                                           T_DELETEDATE,
                                           T_CORRECTDATE,
                                           T_CHANGEDATE,
                                           T_CHANGETIME,
                                           T_OPER)
            SELECT 0,
                   market.T_ID,
                   transfrates.T_STARTDATE,
                   transfrates.T_RATE,
                   transfrates.T_RATE,
                   TO_DATE ('01.01.0001', 'dd.mm.yyyy'),
                   TO_DATE ('01.01.0001', 'dd.mm.yyyy'),
                   TRUNC (SYSDATE),
                   TO_DATE ('01.01.0001' || TO_CHAR (SYSDATE, 'hh24:mi:ss'),
                            'dd.mm.yyyy hh24:mi:ss'),
                   RsbSessionData.Oper
              FROM DIT_TRANSFERRATES_TMP  transfrates
                   INNER JOIN DMARKETRATE_DBT MARKET
                       ON     MARKET.T_FIID = transfrates.T_FIID
                          AND MARKET.T_INTERESTFREQUENCY = v_frequencyInt
                          AND MARKET.T_CONTRACTYEAR = 0
                          AND MARKET.T_CONTRACTMONTHS = 0
                          AND MARKET.T_CONTRACTDAYS = transfrates.T_PERIOD
                   LEFT JOIN DVALINTMARKETRATE_DBT valinmr
                       ON     valinmr.t_marketrateid = market.t_ID
                          AND valinmr.T_BEGINDATE = transfrates.T_STARTDATE
                          AND valinmr.T_DELETEDATE =
                              TO_DATE ('01.01.0001', 'dd.mm.yyyy')
             WHERE valinmr.T_ID IS NULL;

        MERGE INTO DVALINTMARKETRATE_DBT valinmr_main
             USING (SELECT valinmr.t_ID valID, transfrates.T_RATE Rate
                      FROM DIT_TRANSFERRATES_TMP  transfrates
                           INNER JOIN DMARKETRATE_DBT MARKET
                               ON     MARKET.T_FIID = transfrates.T_FIID
                                  AND MARKET.T_INTERESTFREQUENCY =
                                      v_frequencyInt
                                  AND MARKET.T_CONTRACTYEAR = 0
                                  AND MARKET.T_CONTRACTMONTHS = 0
                                  AND MARKET.T_CONTRACTDAYS =
                                      transfrates.T_PERIOD
                           LEFT JOIN DVALINTMARKETRATE_DBT valinmr
                               ON     valinmr.t_marketrateid = market.t_ID
                                  AND valinmr.T_BEGINDATE =
                                      transfrates.T_STARTDATE
                                  AND valinmr.T_DELETEDATE =
                                      TO_DATE ('01.01.0001', 'dd.mm.yyyy')
                                  AND valinmr.T_ID IS NOT NULL) new_info
                ON (new_info.valID = valinmr_main.T_ID)
        WHEN MATCHED
        THEN
            UPDATE SET
                valinmr_main.t_minvalue = new_info.Rate,
                valinmr_main.t_maxvalue = new_info.Rate,
                valinmr_main.t_changedate = TRUNC (SYSDATE),
                valinmr_main.t_changetime =
                    TO_DATE ('01.01.0001' || TO_CHAR (SYSDATE, 'hh24:mi:ss'),
                             'dd.mm.yyyy hh24:mi:ss');

        UPDATE DVALINTMARKETRATE_DBT
           SET T_DELETEDATE = TRUNC (SYSDATE)
         WHERE T_ID IN
                   (SELECT valinmr.t_ID
                      FROM DVALINTMARKETRATE_DBT  valinmr,
                           DMARKETRATE_DBT        MARKET
                     WHERE     MARKET.T_FIID = v_fiid
                           AND valinmr.t_marketrateid = market.t_ID
                           AND valinmr.T_DELETEDATE =
                               TO_DATE ('01.01.0001', 'dd.mm.yyyy')
                           AND MARKET.T_INTERESTFREQUENCY = v_frequencyInt
                           AND MARKET.T_CONTRACTYEAR = 0
                           AND MARKET.T_CONTRACTMONTHS = 0
                           AND NOT EXISTS
                                   (SELECT transfrates.*
                                      FROM DIT_TRANSFERRATES_TMP transfrates
                                     WHERE     MARKET.T_FIID =
                                               transfrates.T_FIID
                                           AND MARKET.T_CONTRACTDAYS =
                                               transfrates.T_PERIOD
                                           AND valinmr.T_BEGINDATE =
                                               transfrates.T_STARTDATE));
                                               
    END IF;
    END;

END IT_TransferRates;
/