-- Перестроение индекса (или создание нового)
declare
  PROCEDURE CreateIndex(aTableName varchar2, aIndexName varchar2, aFields varchar2) 
  IS
    v_Cnt number;
  BEGIN
    SELECT count(*) INTO v_Cnt FROM user_indexes i WHERE i.TABLE_NAME = aTableName and i.INDEX_NAME = aIndexName ;
    IF v_Cnt = 1 THEN
      EXECUTE IMMEDIATE 'DROP INDEX '||aIndexName;
    END IF;
    EXECUTE IMMEDIATE 'CREATE INDEX '||aIndexName||' ON '||aTableName||' ('||aFields||')';
  END;
BEGIN
  CreateIndex('DDLGRDEAL_DBT', 'DDLGRDEAL_DBT_IDX4', 'T_FIID, T_PLANDATE, T_DOCKIND, T_DOCID'); -- графики по сделкам
  CreateIndex('DDLGRACC_DBT', 'DDLGRACC_DBT_IDX4', 'T_STATE, T_ACCNUM'); -- учет по графику
  CreateIndex('DDL_TICK_DBT', 'DDL_TICK_DBT_USRA', 'T_CLIENTID, T_BOFFICEKIND, T_DEALID'); -- паспорт сделки
END;
/
