CREATE OR REPLACE PACKAGE it_vekselrepchod IS

  --C_C_SYSTEM_NAME constant varchar2(128) := 'IPS_DFACTORY';

  -- Определить тип субъекта
  FUNCTION GetPartyTypeName(p_partyID dparty_dbt.t_partyid%type) RETURN VARCHAR2 DETERMINISTIC;

  FUNCTION GetPartyResponsibilityZone(p_partyID dparty_dbt.t_partyid%type) RETURN VARCHAR2 DETERMINISTIC;

  PROCEDURE ReportRun(p_fileName   IN VARCHAR2
                     ,p_startDate  IN DATE
                     ,p_endDate    IN DATE
                     ,o_GUID       OUT VARCHAR2
                     ,o_errorCode  OUT NUMBER
                     ,o_errorDesc  OUT VARCHAR2);

END it_vekselrepchod;
/