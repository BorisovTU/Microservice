/*Таблица для начальной выгрузки клиентов BIQ-10268*/
declare
    vcnt number;
    vsql varchar2(2000);
begin

   select count(*) into vcnt from user_sequences  where sequence_name = upper('biq10268_unload_seq');
   if vcnt = 0 then
      execute immediate  'CREATE SEQUENCE biq10268_unload_seq
                             START WITH 1
                             MAXVALUE 9999999999999999999999999999
                             MINVALUE 1';
   end if;
   
   select count(*) into vcnt from user_tables where table_name = upper('biq10268_unload');
   if vcnt = 0 then
      execute immediate  'CREATE TABLE biq10268_unload
                           (
                             t_partyid       NUMBER(10),
                             T_TIMESTAMP     DATE,
                             T_STATUS        NUMBER(5),
                             T_BATCHID       number(10),
                             PartyType       varchar2(10),
                             PartyStatus     varchar2(10),
                             ShortNameOrg    varchar2(320),
                             FullNameOrg     varchar2(320),
                             ShortNameOrgLat varchar2(320),
                             FullNameOrgLat  varchar2(320),
                             ResidentTax     varchar2(10),
                             ResidenceCountry   varchar2(10),
                             AuthorizedCapital  varchar2(40),
                             AuthorizedCapitalCurrency   varchar2(10),
                             INN             varchar2(35),
                             KPPRegistrationPlace  varchar2(35),
                             OGRN            varchar2(35),
                             KIO             varchar2(35),
                             OKPO            varchar2(35),
                             OKFS            varchar2(35),
                             OKOGU           varchar2(35),
                             OKTMO           varchar2(35),
                             OKATO           varchar2(35),
                             OKOPF           varchar2(35),
                             RegistrationDate      date,
                             OGRNRegistrationDate  date,
                             NonResidentRegistrationNumber  varchar2(35),
                             NonResidentRegistrationDate date
                           )';
      execute immediate 'CREATE INDEX biq10268_unload_IDX0 ON biq10268_unload (t_partyid, t_status)';
   end if;
   
end;
