-- Таблица 

DECLARE
  -- Процедура добавления поля t_BrokerRef varchar2(20) в таблицу d_cux23_tmp
  PROCEDURE addBrokerRef
  IS
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE d_cux23_tmp ADD t_BrokerRef varchar2(20)';
  EXCEPTION WHEN others THEN 
    NULL;
  END;
BEGIN
  -- добавление поля t_BrokerRef varchar2(20) в таблицу d_cux23_tmp
  addBrokerRef();
END;
/