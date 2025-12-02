CREATE OR REPLACE FUNCTION ControlDateReportRunJson_RC_REPORTRUN(p_json_input CLOB)
RETURN CLOB IS
    v_result CLOB;
    v_begin_date DATE;
    v_end_date DATE;
    v_report_name VARCHAR2(4000);
BEGIN
    -- Парсинг JSON входных параметров
SELECT
    CASE WHEN json_value(p_json_input, '$.T_BEGINDATE') IS NOT NULL
             THEN TO_DATE(json_value(p_json_input, '$.T_BEGINDATE'), 'YYYY-MM-DD') END,
    CASE WHEN json_value(p_json_input, '$.T_ENDDATE') IS NOT NULL
             THEN TO_DATE(json_value(p_json_input, '$.T_ENDDATE'), 'YYYY-MM-DD') END,
    json_value(p_json_input, '$.T_REPORTNAME')
INTO
    v_begin_date, v_end_date, v_report_name
FROM DUAL;

-- Вызов основной функции
v_result := ControlDateReportRun(v_begin_date, v_end_date, v_report_name);

RETURN v_result;
EXCEPTION
    WHEN OTHERS THEN
        -- Возврат ошибки в формате JSON при возникновении исключений
        RETURN JSON_OBJECT(
            'error' VALUE SQLERRM,
            'code' VALUE SQLCODE,
            'input_json' VALUE p_json_input
        );
END;
/
