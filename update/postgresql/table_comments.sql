-- Файл для PostgreSQL с комментариями в кодировке UTF-8
-- Учет регистра имен - используем двойные кавычки для сохранения регистра

COMMENT ON COLUMN "DBDUI_REPORTPOSTINFO_DBT"."T_FORM" IS 'Название отчетной формы';
COMMENT ON COLUMN "DBDUI_REPORTPOSTINFO_DBT"."T_LIMITDATE" IS 'Дата до которой должна быть направлена отчетность';
COMMENT ON COLUMN "DBDUI_REPORTPOSTINFO_DBT"."T_SENDDAY" IS 'Фактическая дата отправки отчетности';
COMMENT ON COLUMN "DBDUI_REPORTPOSTINFO_DBT"."T_REGDATE" IS 'Дата регистрации отчетной формы в БР';
COMMENT ON COLUMN "DBDUI_REPORTPOSTINFO_DBT"."T_STATUS" IS 'Краткий комментарий из квитанции БР с общей информацией есть ошибки/предупреждения, если есть, то в каких разделах. Или информация, что ошибок нет';
COMMENT ON COLUMN "DBDUI_REPORTPOSTINFO_DBT"."T_MESSAGE" IS 'Более подробное объяснение из квитанции БР в чем заключается ошибка/предупреждение';