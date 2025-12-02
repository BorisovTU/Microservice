CREATE OR REPLACE FUNCTION ControlDateReportRun(
    T_BEGINDATE DATE,
    T_ENDDATE DATE,
    T_REPORTNAME VARCHAR2
) RETURN CLOB IS
    v_result CLOB;
BEGIN
WITH report_list AS (
    SELECT TRIM(REGEXP_SUBSTR(T_REPORTNAME, '[^,]+', 1, LEVEL)) AS report_name
    FROM DUAL
CONNECT BY REGEXP_SUBSTR(T_REPORTNAME, '[^,]+', 1, LEVEL) IS NOT NULL
    )
SELECT JSON_OBJECT(
               'GetLmitDayReport' VALUE JSON_OBJECT(
            'date' VALUE '',
            'date_begin' VALUE CASE
                                 WHEN T_BEGINDATE IS NOT NULL THEN TO_CHAR(T_BEGINDATE, 'DD.MM.YYYY')
                                 ELSE ''
                               END,
            'date_end' VALUE CASE
                               WHEN T_ENDDATE IS NOT NULL THEN TO_CHAR(T_ENDDATE, 'DD.MM.YYYY')
                               ELSE ''
                             END,
            'LimitDay_info' VALUE (
                SELECT JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'report_form' VALUE T_FORM,
                        'lim_date' VALUE TO_CHAR(T_LIMITDATE, 'DD.MM.YYYY'),
                        'send_day' VALUE NVL(TO_CHAR(T_SENDDAY, 'DD.MM.YYYY'), ''),
                        'reg_day' VALUE NVL(TO_CHAR(T_REGDATE, 'DD.MM.YYYY'), ''),
                        'status' VALUE NVL(T_STATUS, ''),
                        'mess' VALUE NVL(T_MESSAGE, ''),
                        'date' VALUE '',
                        'date_end' VALUE CASE
                                           WHEN T_ENDDATE IS NOT NULL THEN TO_CHAR(T_ENDDATE, 'DD.MM.YYYY')
                                           ELSE ''
                                         END
                    )
                )
                FROM DBDUI_REPORTPOSTINFO_DBT
                WHERE (
                    -- Если заданы обе границы диапазона
                    (T_BEGINDATE IS NOT NULL AND T_ENDDATE IS NOT NULL AND T_LIMITDATE BETWEEN T_BEGINDATE AND T_ENDDATE)
                    OR
                    -- Если задана только начальная дата
                    (T_BEGINDATE IS NOT NULL AND T_ENDDATE IS NULL AND T_LIMITDATE >= T_BEGINDATE)
                    OR
                    -- Если задана только конечная дата
                    (T_BEGINDATE IS NULL AND T_ENDDATE IS NOT NULL AND T_LIMITDATE <= T_ENDDATE)
                    OR
                    -- Если не заданы никакие даты - берем все записи
                    (T_BEGINDATE IS NULL AND T_ENDDATE IS NULL)
                )
                AND (T_FORM IN (SELECT report_name FROM report_list)
                     OR (T_REPORTNAME IS NULL AND T_FORM IS NULL))
            ) FORMAT JSON
        ) FORMAT JSON
       ) INTO v_result
FROM DUAL;

RETURN v_result;
END;
/
