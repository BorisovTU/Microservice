CREATE OR REPLACE TRIGGER "DPTVSPROXY_DBT_SYNH" 
AFTER /*DELETE OR*/ INSERT OR UPDATE ON DPTVSPROXY_DBT REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
  v_PersonID number := 0;

BEGIN

 -- Определяем статус по умолчанию для данного типа объекта
 BEGIN
     SELECT T_PERSONID
         INTO V_PERSONID
       FROM DOFFICER_DBT
      WHERE T_PERSONID = :new.t_ProxyID
        AND T_PARTYID = :new.t_PartyID;
 EXCEPTION
     WHEN NO_DATA_FOUND THEN
     v_PersonID := 0;
 END;
 
 IF (v_PersonID = 0) THEN
     INSERT INTO DOFFICER_DBT (T_PERSONID, T_PARTYID, T_POST, T_ISFIRSTPERSON, T_ISSECONDPERSON, T_OFFICEID, T_MATOTV, T_ISFIRSTOFFICEPERSON, T_PHONENUMBER, T_DATEFROM, T_DATETO, T_ISTEMPSIGNATURE, T_HASSIGNRIGHT, T_SUBSTPERSONID, T_HASFMMSGSENDRIGHT, T_CREATEMODE)
         VALUES (:new.t_ProxyID, :new.t_PartyID, nvl((select t_name from dllvalues_dbt where t_list=1128 and t_element=:new.t_Post), chr(0)), chr(0), chr(0), 0, chr(0), chr(0), chr(1), :new.t_DocDate, :new.t_ValidityDate, chr(0), chr(0), 0, chr(0), 0);
 ELSE
     UPDATE DOFFICER_DBT
          SET T_POST = nvl((select t_name from dllvalues_dbt where t_list=1128 and t_element=:new.t_Post), chr(0)),
                 T_DATEFROM = :new.t_DocDate,
                 T_DATETO = :new.t_ValidityDate
      WHERE T_PERSONID = :new.t_ProxyID
          AND T_PARTYID = :new.t_PartyID;
 END IF;

END;
/