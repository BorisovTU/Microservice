CREATE OR REPLACE FUNCTION ControlDateReportRun(
    T_BEGINDATE DATE,
    T_ENDDATE DATE,
    T_REPORTNAME TEXT
)
RETURNS JSON AS $$
DECLARE
result JSON;
BEGIN
SELECT json_build_object(
               'GetLmitDayReport', json_build_object(
                'date', '',
                'date_begin', CASE
                                  WHEN T_BEGINDATE IS NOT NULL THEN TO_CHAR(T_BEGINDATE, 'DD.MM.YYYY')
                                  ELSE ''
                    END,
                'date_end', CASE
                                WHEN T_ENDDATE IS NOT NULL THEN TO_CHAR(T_ENDDATE, 'DD.MM.YYYY')
                                ELSE ''
                    END,
                'LimitDay_info', COALESCE(
                        (SELECT json_agg(
                                        json_build_object(
                                                'report_form', T_FORM,
                                                'lim_date', TO_CHAR(T_LIMITDATE, 'DD.MM.YYYY'),
                                                'send_day', COALESCE(TO_CHAR(T_SENDDAY, 'DD.MM.YYYY'), ''),
                                                'reg_day', COALESCE(TO_CHAR(T_REGDATE, 'DD.MM.YYYY'), ''),
                                                'status', COALESCE(T_STATUS, ''),
                                                'mess', COALESCE(T_MESSAGE, ''),
                                                'date', '',
                                                'date_end', CASE
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
                           AND (TRIM(T_FORM) = ANY(
                             ARRAY(SELECT TRIM(unnest) FROM unnest(string_to_array(COALESCE(T_REPORTNAME, ''), ',')) AS unnest)
                             ) OR (T_REPORTNAME IS NULL AND T_FORM IS NULL))
                        ),
                        '[]'::json
                                 )
                                   )
       ) INTO result;

RETURN result;
END;
$$ LANGUAGE plpgsql;
