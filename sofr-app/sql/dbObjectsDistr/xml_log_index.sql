--
-- Скрипт создания индекса для таблицы DXML_DFISCLOG_DBT
--

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX DXML_DFISCLOG_DBT_IDX_DESC ON DXML_DFISCLOG_DBT ("T_DATE" DESC, "T_TIME" DESC, "T_RECID" DESC)';
EXCEPTION 
    WHEN OTHERS THEN NULL;
END;
/

--
-- Скрипт создания индекса для таблицы DXML_DOPERLOG_DBT
--

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX DXML_DOPERLOG_DBT_IDX_DESC ON DXML_DOPERLOG_DBT ("T_DATE" DESC, "T_TIME" DESC, "T_RECID" DESC)';
EXCEPTION 
    WHEN OTHERS THEN NULL;
END;
/

-- Индексы для временных таблиц
BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX dfisclog_tmp_idx_desc';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'CREATE INDEX dfisclog_tmp_idx_desc ON dfisclog_tmp (t_date DESC, t_time DESC, t_numdprt DESC, t_recid DESC)';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX doperlog_tmp_idx_desc';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'CREATE INDEX doperlog_tmp_idx_desc ON doperlog_tmp (t_date DESC, t_time DESC, t_numdprt DESC, t_recid DESC)';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX dxml_dfisclog_tmp_idx_desc';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'CREATE INDEX dxml_dfisclog_tmp_idx_desc ON dxml_dfisclog_tmp (t_date DESC, t_time DESC, t_numdprt DESC, t_recid DESC)';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX dxml_doperlog_tmp_idx_desc';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'CREATE INDEX dxml_doperlog_tmp_idx_desc ON dxml_doperlog_tmp (t_date DESC, t_time DESC, t_numdprt DESC, t_recid DESC)';
EXCEPTION 
  WHEN OTHERS THEN NULL;
END;
/

