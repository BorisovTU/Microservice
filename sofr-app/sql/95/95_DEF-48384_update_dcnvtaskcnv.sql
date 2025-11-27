-- Изменение размера пачек для конвейерной обработки отчёта "Индивидуальный счет ВУ"
DECLARE
BEGIN
  UPDATE DCNVTASKCNV_DBT
     SET T_PACKETSIZE = 1000
   WHERE T_SELECTMACRO = 'dlinaccia.mac';

EXCEPTION 
   WHEN OTHERS THEN NULL;
END;
/                                                      