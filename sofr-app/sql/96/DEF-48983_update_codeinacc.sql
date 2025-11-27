DECLARE
    CURSOR c_fin IS
          SELECT fin.*, avr.t_isin
            FROM dfininstr_dbt fin, davoiriss_dbt avr
           WHERE     fin.t_fi_kind = 2
                 AND fin.t_isclosed = CHR (0)
                 AND fin.t_codeinaccount NOT IN (CHR (0), CHR (1))
                 AND avr.t_fiid = fin.t_fiid
                 AND SUBSTR (avr.t_isin, 1, 12)||fin.t_CodeInAccount NOT IN
                         (  SELECT SUBSTR (t_isin, 1, 12)||t_CodeInAccount
                              FROM davoiriss_dbt a, dfininstr_dbt f
                             WHERE a.t_fiid = f.t_fiid
                          GROUP BY SUBSTR (t_isin, 1, 12),t_CodeInAccount
                            HAVING COUNT (1) > 1)
                 AND (SELECT COUNT (1)
                        FROM dfininstr_dbt fin2
                       WHERE     fin2.t_fi_kind = 2
                             AND fin2.t_codeinaccount = fin.t_codeinaccount
                             AND fin2.t_isclosed = CHR (0)) > 1
        ORDER BY fin.t_codeinaccount ASC, fin.t_fiid ASC;

    v_ActualCodeInAcc   VARCHAR2 (7) := CHR (0);
    cur_date            DATE;
    cur_time            DATE;
    v_Value             NUMBER;
    v_Exist             NUMBER;
    stat                INTEGER DEFAULT 0;
BEGIN
    cur_date := rsu_rtlglobals.null_date;

    IF rsu_rtlglobals.curdate IS NOT NULL
    THEN cur_date := rsu_rtlglobals.curdate;
    END IF;

    cur_time :=
        TO_DATE ('01010001' || TO_CHAR (SYSDATE, 'hhmiss'), 'ddmmyyyyhhmiss');

    FOR v_fin IN c_fin
    LOOP
        BEGIN
            IF (v_ActualCodeInAcc = v_fin.t_codeinaccount)
            THEN
                BEGIN
                    LOOP
                        stat :=
                            rsi_rsb_refer.WldGetSequenceValue (363,
                                                               v_Value,
                                                               cur_date,
                                                               cur_time,
                                                               CHR (0),
                                                               1);

                        SELECT COUNT (1)
                          INTO v_Exist
                          FROM dfininstr_dbt
                         WHERE     t_FI_Kind = 2
                               AND t_CodeInAccount = LPAD (v_Value, 5, 0);

                        IF (v_Exist = 0 OR stat <> 0)
                        THEN
                            EXIT;
                        END IF;
                    END LOOP;

                    IF (stat = 0)
                    THEN
                        UPDATE dfininstr_dbt fin
                           SET fin.t_codeinaccount = LPAD (v_Value, 5, 0)
                         WHERE fin.t_fiid = v_fin.t_fiid;

                        DBMS_OUTPUT.put_line (
                               'ISIN = '
                            || v_fin.t_isin
                            || ' Наименование выпуска = '
                            || v_fin.t_name
                            || ' Старый код в номере счета = '
                            || v_fin.t_codeinaccount
                            || ' Новый код в номере счета '
                            || LPAD (v_Value, 5, 0));
                    ELSE
                        DBMS_OUTPUT.put_line (
                               'Ошибка генерации кода выпуска с ISIN = '
                            || v_fin.t_isin);
                    END IF;
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        DBMS_OUTPUT.put_line (
                               'Ошибка обновления кода выпуска с ISIN = '
                            || v_fin.t_isin);
                END;
            ELSE
                v_ActualCodeInAcc := v_fin.t_codeinaccount;
            END IF;
        EXCEPTION
            WHEN OTHERS
            THEN
                DBMS_OUTPUT.put_line (
                       'Ошибка обновления кода выпуска с ISIN = '
                    || v_fin.t_fiid);
        END;
    END LOOP;
    COMMIT;
END;
/