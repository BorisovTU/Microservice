CREATE OR REPLACE FUNCTION ControlDateReportRunJson_RC_REPORTRUN(p_json_input JSON)
RETURNS JSON AS $$
DECLARE
v_begin_date DATE;
    v_end_date DATE;
    v_report_name TEXT;
    result JSON;
BEGIN
    -- Парсинг JSON входных параметров
SELECT
    NULLIF(NULLIF(p_json_input->>'T_BEGINDATE', ''), 'null')::DATE,
    NULLIF(NULLIF(p_json_input->>'T_ENDDATE', ''), 'null')::DATE,
    NULLIF(NULLIF(p_json_input->>'T_REPORTNAME', ''), 'null')
INTO
    v_begin_date, v_end_date, v_report_name;

-- Вызов основной функции
result := ControlDateReportRun(v_begin_date, v_end_date, v_report_name);

RETURN result;
EXCEPTION
    WHEN OTHERS THEN
        -- Возврат ошибки в формате JSON при возникновении исключений
        RETURN json_build_object(
            'error', SQLERRM,
            'code', SQLSTATE,
            'input_json', p_json_input
        );
END;
$$ LANGUAGE plpgsql;
