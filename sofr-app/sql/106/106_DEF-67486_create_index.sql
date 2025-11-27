-- Добавление индекса для буферной таблицы
BEGIN
  EXECUTE IMMEDIATE 'CREATE INDEX KONDOR_SOFR_BUFFER_DBT_IDX2 ON KONDOR_SOFR_BUFFER_DBT (t_seqID) tablespace INDX';
END;
/