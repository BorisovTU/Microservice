CREATE OR REPLACE PACKAGE RSB_TEST_GT is   --Спецификация RSB_TEST_GT

  RGDLTC_MARKETREPORT_ID_DEAL     CONSTANT NUMBER := 36;
  RGDLTC_MARKETREPORT_ID_CLEARING CONSTANT NUMBER := 552;
  RGDVNDL_MARKETREPORT_ID         CONSTANT NUMBER := 615;
  RGDVFXDL_MARKETREPORT_ID        CONSTANT NUMBER := 665;
  RGDVDL_MARKETREPORT_ID          CONSTANT NUMBER := 101;

  function GetReplicRecords(p_ApplicationCond IN VARCHAR2, p_SeanseDate IN DATE, p_ObjType IN VARCHAR2, p_SeanseId IN INTEGER default 0) return clob;
  procedure DeleteReplicRecords;
  procedure LoadReplicObject(p_Clob IN CLOB);
  procedure LoadReplicObjectCode(p_Clob IN CLOB);
  procedure LoadReplicRecord(p_Clob IN CLOB);
  procedure LoadReplicRecordParam(p_Clob IN CLOB);
  procedure LoadReplicRecords(p_Clob IN CLOB);
  procedure AppendToFileStorageClob(p_id IN NUMBER, p_chunk IN CLOB);
  function GetReportCompare(p_ApplicationCond IN VARCHAR2, p_SeanseDate IN DATE, p_ObjType IN VARCHAR2, p_SeanseId IN INTEGER default 0) return clob;
  function CompareObject(p_ApplicationCond IN VARCHAR2, p_SeanseDate IN DATE, p_ObjType IN VARCHAR2, p_SeanseId IN INTEGER default 0) return clob;
  function CompareObjectCode(p_ObjGtId IN INTEGER, p_ObjReplId IN INTEGER, p_ApplicationCond IN VARCHAR2) return clob;
  function CompareObjectRecord return clob;
  function CompareRecordParam(p_RecGtId IN INTEGER, p_RecReplId IN INTEGER) return clob;

end RSB_TEST_GT;
/