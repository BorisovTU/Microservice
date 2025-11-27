-- Наполнение таблиц

declare
    vcnt number;
begin
   select count(*) into vcnt from DCNVTYPE_DBT where upper(T_NAME) = 'КОНВЕРТАЦИЯ ОТЧЕТОВ БРОКЕРА';
   if vcnt = 0 then
      Insert into DCNVTYPE_DBT (T_CONVTYPEID, T_NAME, T_PACKETSIZE) Values (30, 'Конвертация отчетов брокера', 1);
      Insert into DCNVTASK_DBT (T_TASKID, T_NAME) Values (30, 'Конвертация отчетов брокера');
      Insert into DCNVOPRTYPE_DBT (T_OPERATIONID, T_NAME, T_SHORTNAME, T_PROC, T_MACRO, T_STATEIN, T_STATEOUT, T_SERVICE, T_THREADS, T_PANELCLASS, T_PANELMACRO) Values
       (25, 'Пакетная операция конвертации отчетов брокера', 'Конвертация отчетов брокера', 'ExecuteSrvRepBrkConvert', 'sc_srvrepbrkconvert.mac', 
        49, 50, chr(0), 15, chr(0), chr(0));
      Insert into DCNVOPR_DBT (T_CONVTYPEID, T_OPERATIONID, T_ORDER, T_NOTUSED, T_THREADS) Values (30, 25, 1, chr(0), 10);
      Insert into DCNVTASKCNV_DBT (T_TASKID, T_CONVTYPEID, T_SELECTPROC, T_SELECTMACRO, T_NOTUSED, T_PACKETSIZE) Values
       (30, 30, 'ОтобратьОбъектыДляКонвертОтчБр', 'scstartsrvrep.mac', chr(0), 0);
   end if;
end;
/