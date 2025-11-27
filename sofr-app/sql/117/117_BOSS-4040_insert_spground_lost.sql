--п.3.2.7 ТЗ
DECLARE
  logID VARCHAR2(32) := 'BOSS-4040';
  v_Cnt NUMBER := 0;
  l_spground_id  dspground_dbt.t_spgroundid%type;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;

BEGIN
  LogIt('Создание отсутствующих входящих документов по зачислениям');
  FOR i IN ( SELECT nvl(u.t_EntryNumber, chr(1)) t_EntryNumber, u.t_Date, op.t_ID, op.t_DocKind, op.t_Code,
                    TRUNC(u.t_SystemDate) t_SystDate, TO_DATE('01010001 ' || TO_CHAR(u.t_SystemDate, 'hh24:mi:ss'), 'ddmmyyyy hh24:mi:ss') t_SystTime,
                    DECODE(u.t_DocType, '01', 313, '06', 336, '06о', 337, '0106', 338, '03rv', 339, 340) t_GroundKind, 
                    CASE WHEN u.t_DebetAccount LIKE '4742%' THEN 1 ELSE op.t_Client END t_Party
               FROM USR_ACC306ENROLL_DBT u, DNPTXOP_DBT op         
              WHERE u.t_NptxOpID = op.t_ID 
                AND op.t_DocKind = 4607 
                AND op.t_SubKind_Operation = 10
                AND nvl(u.t_DocType, chr(1)) IN ('01','06','06о','0106','03rv','03pv')
           )
  LOOP
    spground_utils.save_ground(pio_spgroundid => l_spground_id,               
                               p_doclog       => 513,
                               p_kind         => i.t_GroundKind,
                               p_direction    => 1,
                               p_registrdate  => i.t_SystDate,
                               p_registrtime  => i.t_SystTime,
                               p_xld          => i.t_Code,
                               p_altxld       => i.t_EntryNumber,
                               p_signeddate   => i.t_Date,
                               p_backoffice   => 'S',
                               p_party        => i.t_Party,
                               p_partyname    => party_read.get_party_name(p_partyid => i.t_Party),
                               p_partycode    => nvl(party_read.get_party_code(p_party_id => i.t_Party, p_code_kind => 1), chr(1)),
                               p_methodapplic => 1);

    spground_utils.link_spground_to_doc(p_sourcedockind => i.t_DocKind,
                                        p_sourcedocid   => i.t_ID,
                                        p_spgroundid    => l_spground_id);

    v_Cnt := v_Cnt + 1;
  END LOOP;
  LogIt('Создано '||to_char(v_Cnt)||' входящих документов по зачислениям');

EXCEPTION WHEN OTHERS THEN 
  LogIt('Ошибка при создании входящих документов по зачислениям');
END;
/