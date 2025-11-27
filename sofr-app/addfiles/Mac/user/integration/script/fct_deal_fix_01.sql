DECLARE
    /* При необходимости заполнить строкой вида '@%db_link_name%' */
    v_upd_dblink VARCHAR2(25 CHAR) := '';
    /* Название схемы с таблицей справочника */
    v_upd_scheme VARCHAR2(50 CHAR) := 'RSHB_SEC_DEBUG3_LDR_INFA_20181203';
    /* Название таблицы справочника */
    v_upd_table  VARCHAR2(25 CHAR) := 'fct_deal';
    /* При необходимости заполнить строкой вида '@%db_link_name%' */
    v_inf_dblink VARCHAR2(25 CHAR) := '';
    /* Название схемы со вспомогательной информацией (RS-Bank) */
    v_inf_scheme VARCHAR2(25 CHAR) := 'RSHB_SEC_TEST2';

    v_subj_code_bnk_beg VARCHAR2(25 CHAR) := 'КОД_ФИЛИАЛА#BISQUIT#';
    v_subj_code_bnk_end VARCHAR2(25 CHAR) := '#banks';

    v_subj_code_corp_beg VARCHAR2(25 CHAR) := 'КОД_ФИЛИАЛА#BISQUIT#';
    v_subj_code_corp_end VARCHAR2(25 CHAR) := '#cust_corp';

    v_subj_code_fis_beg VARCHAR2(25 CHAR) := 'КОД_ФИЛИАЛА#BISQUIT#';

    v_dt_to_compare             VARCHAR2( 10 CHAR) := '01-01-0001';
    v_dt_to_update              VARCHAR2( 10 CHAR) := '01-01-1999';
    v_subject_code_to_update    VARCHAR2(250 CHAR);
    v_department_code_to_update VARCHAR2( 30 CHAR);
    v_party_id                  NUMBER(10);
    v_legalform                 NUMBER(5);
    v_isbank                    NUMBER(1);

    q_subject_code    VARCHAR2(250 CHAR);
    q_department_code VARCHAR2( 30 CHAR);

    v_q_dict LONG;
    v_q_aux  LONG;
    v_q_upd  LONG;

    TYPE cur_type IS REF CURSOR;

    v_cur cur_type;

    err_dif EXCEPTION;

BEGIN
    /* Исправляем поле dt */
    BEGIN
        v_q_upd :=            'UPDATE ' || v_upd_scheme || '.' || v_upd_table || v_upd_dblink;
        v_q_upd := v_q_upd ||   ' SET dt = :p_dt_to_update ';
        v_q_upd := v_q_upd || ' WHERE dt = :p_dt_to_compare ';

        EXECUTE IMMEDIATE v_q_upd USING IN v_dt_to_update, IN v_dt_to_compare;

        COMMIT;

    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.PUT_LINE('Обновление поля DT не выполнено!');
    END;

    /* Исправляем поле department_code */
    v_q_dict := 'SELECT DISTINCT(department_code) FROM ' || v_upd_scheme || '.' || v_upd_table || v_upd_dblink;

    OPEN v_cur FOR v_q_dict;
    LOOP
    FETCH v_cur
     INTO q_department_code;
    EXIT WHEN v_cur%NOTFOUND;
        BEGIN
            v_q_aux := 'SELECT DECODE(t_parentcode, 0, ''0000'', t_name) FROM ' || v_inf_scheme || '.ddp_dep_dbt' || v_inf_dblink;
            v_q_aux := v_q_aux || ' WHERE t_code = :p_department_code ';

            EXECUTE IMMEDIATE v_q_aux INTO v_department_code_to_update USING IN q_department_code;

            v_q_upd :=            'UPDATE ' || v_upd_scheme || '.' || v_upd_table || v_upd_dblink;
            v_q_upd := v_q_upd ||   ' SET department_code = :p_department_code_to_update ';
            v_q_upd := v_q_upd || ' WHERE department_code = :p_department_code ';

            EXECUTE IMMEDIATE v_q_upd USING IN v_department_code_to_update, IN q_department_code;

        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                NULL;
            WHEN OTHERS
            THEN
                DBMS_OUTPUT.PUT_LINE('Обновление department_code = ' || q_department_code || ' не удалось!');
        END;
    END LOOP;
    CLOSE v_cur;

    COMMIT;

    /* Исправляем поле subject_code */
    v_q_dict := 'SELECT DISTINCT(subject_code) FROM ' || v_upd_scheme || '.' || v_upd_table || v_upd_dblink;

    OPEN v_cur FOR v_q_dict;
    LOOP
    FETCH v_cur
     INTO q_subject_code;
    EXIT WHEN v_cur%NOTFOUND;
        BEGIN
            /* partyid */
            v_q_aux := 'SELECT t_objectid FROM ' || v_inf_scheme || '.dobjcode_dbt' || v_inf_dblink;
            v_q_aux := v_q_aux || ' WHERE t_objecttype = 3 ';
            v_q_aux := v_q_aux || '   AND t_codekind = 101 ';
            v_q_aux := v_q_aux || '   AND t_code = :p_code ';
            v_q_aux := v_q_aux || '   AND t_state = 0 ';

            EXECUTE IMMEDIATE v_q_aux INTO v_party_id USING IN q_subject_code;

            v_q_aux :=                'SELECT party.t_legalform, ';
            v_q_aux := v_q_aux ||           ' NVL2(ptown.t_partyid, 1, 0) ';
            v_q_aux := v_q_aux ||      ' FROM ' || v_inf_scheme || '.dparty_dbt' || v_inf_dblink || ' party ';
            v_q_aux := v_q_aux || ' LEFT JOIN ' || v_inf_scheme || '.dpartyown_dbt' || v_inf_dblink || ' ptown ';
            v_q_aux := v_q_aux ||        ' ON ptown.t_partyid = party.t_partyid AND t_partykind = 2 ';
            v_q_aux := v_q_aux || ' WHERE party.t_partyid = :p_partyid ';

            EXECUTE IMMEDIATE v_q_aux INTO v_legalform, v_isbank USING IN v_party_id;

            CASE
                /* Банк */
                WHEN v_isbank = 1
                THEN
                    v_subject_code_to_update := v_subj_code_bnk_beg || q_subject_code || v_subj_code_bnk_end;

                /* ЮЛ */
                WHEN v_legalform = 1
                THEN
                    v_subject_code_to_update := v_subj_code_corp_beg || q_subject_code || v_subj_code_corp_end;

                /* ФЛ */
                WHEN v_legalform = 2
                THEN
                    v_subject_code_to_update := v_subj_code_fis_beg || q_subject_code;
                ELSE
                    RAISE err_dif;
            END CASE;

            v_q_upd :=            'UPDATE ' || v_upd_scheme || '.' || v_upd_table || v_upd_dblink;
            v_q_upd := v_q_upd ||   ' SET subject_code = :p_subject_code_to_update ';
            v_q_upd := v_q_upd || ' WHERE subject_code = :p_subject_code ';

            EXECUTE IMMEDIATE v_q_upd USING IN v_subject_code_to_update, IN q_subject_code;

        EXCEPTION
            WHEN err_dif
            THEN
                DBMS_OUTPUT.PUT_LINE('Что-то пошло не так (subject_code = ' || q_subject_code || ')');
            WHEN OTHERS
            THEN
                DBMS_OUTPUT.PUT_LINE('Обновление subject_code = ' || q_subject_code || ' не удалось!');
        END;
    END LOOP;
    CLOSE v_cur;

    COMMIT;

END;