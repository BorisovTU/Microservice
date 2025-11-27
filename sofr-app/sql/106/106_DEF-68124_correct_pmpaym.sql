-- Изменения по DEF-68124
-- Корректировка платежей у проблемного клиента
DECLARE
  logID VARCHAR2(32) := 'DEF-68124';
  x_Cnt NUMBER;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- корректировка платежей у проблемного клиента
  PROCEDURE correctPmPaym(p_FIID IN number, p_DocKind IN number, p_DocID IN number, p_Purpose IN number)
  IS
  BEGIN
    LogIt('корректировка валюты платежа, fiid='||p_FIID||', dockind='||p_DocKind||', docID='||p_DocID||', purpose='||p_Purpose);
    UPDATE dpmpaym_dbt p 
      SET 
        p.t_fiid = p_FIID, p.t_payfiid = p_FIID
        , P.T_FIID_FUTUREPAYACC = p_FIID, P.T_FIID_FUTURERECACC = p_FIID
        , p.t_basefiid = p_FIID, p.t_orderfiid = p_FIID
      WHERE 
        p.t_dockind = p_DocKind and p.t_documentid = p_DocID and p.t_purpose = p_Purpose
    ;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('произведена корректировка валюты платежа, fiid='||p_FIID||', dockind='||p_DocKind||', docID='||p_DocID||', purpose='||p_Purpose);
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('ошибка корректировки валюты платежа, fiid='||p_FIID||', dockind='||p_DocKind||', docID='||p_DocID||', purpose='||p_Purpose);
      EXECUTE IMMEDIATE 'ROLLBACK';
  END;
BEGIN
  correctPmPaym(7, 199, 271348, 72);						-- Выполнить корректировку платежей у проблемного клиента
  correctPmPaym(7, 199, 271349, 72);						-- Выполнить корректировку платежей у проблемного клиента
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/