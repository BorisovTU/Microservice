DECLARE
    cnt   NUMBER;
BEGIN
SELECT COUNT (*)
  INTO cnt
  FROM DPTDUPPRM_DBT
 WHERE T_PARTYTYPE = 1 AND T_PARAMSETNO = 7;

IF CNT = 0
THEN
  INSERT INTO DPTDUPPRM_DBT (
     T_ADDRESS, T_BIRTHDATE, T_COINCIDENCE, 
     T_FULLNAME, T_IDENTITYCARD, T_INDISTRIB, 
     T_INN, T_ISACTIVE, T_LASTNAME, 
     T_NAME, T_OGRN, T_PARAMSETID, 
     T_PARAMSETNO, T_PARTYTYPE, T_PATRONYMIC, 
     T_PENSIONCARD, T_PRIORITY, T_REGISTRATION, 
     T_RESIDENT, T_SHORTNAME, T_KIO, T_OKOPF, T_OKPO) 
  VALUES ( chr(0),
  chr(0),
  100,
  chr(0),
  chr(0),
  chr(88),
  chr(88),
  chr(88),
  chr(0),
  chr(0),
  chr(0),
  0,
  7,
  1,
  chr(0),
  chr(0),
  7,
  chr(0),
  chr(0),
  chr(0),
  chr(0),
  chr(0),
  chr(0));

  INSERT INTO DPTDUPPRM_DBT (
     T_ADDRESS, T_BIRTHDATE, T_COINCIDENCE, 
     T_FULLNAME, T_IDENTITYCARD, T_INDISTRIB, 
     T_INN, T_ISACTIVE, T_LASTNAME, 
     T_NAME, T_OGRN, T_PARAMSETID, 
     T_PARAMSETNO, T_PARTYTYPE, T_PATRONYMIC, 
     T_PENSIONCARD, T_PRIORITY, T_REGISTRATION, 
     T_RESIDENT, T_SHORTNAME, T_KIO, T_OKOPF, T_OKPO) 
  VALUES ( chr(0),
  chr(0),
  100,
  chr(0),
  chr(0),
  chr(88),
  chr(88),
  chr(88),
  chr(0),
  chr(0),
  chr(88),
  0,
  8,
  1,
  chr(0),
  chr(0),
  8,
  chr(0),
  chr(0),
  chr(0),
  chr(0),
  chr(0),
  chr(0));
 
  INSERT INTO DPTDUPPRM_DBT (
     T_ADDRESS, T_BIRTHDATE, T_COINCIDENCE, 
     T_FULLNAME, T_IDENTITYCARD, T_INDISTRIB, 
     T_INN, T_ISACTIVE, T_LASTNAME, 
     T_NAME, T_OGRN, T_PARAMSETID, 
     T_PARAMSETNO, T_PARTYTYPE, T_PATRONYMIC, 
     T_PENSIONCARD, T_PRIORITY, T_REGISTRATION, 
     T_RESIDENT, T_SHORTNAME, T_KIO, T_OKOPF, T_OKPO) 
  VALUES ( chr(0),
  chr(0),
  100,
  chr(0),
  chr(0),
  chr(88),
  chr(0),
  chr(88),
  chr(0),
  chr(0),
  chr(0),
  0,
  9,
  1,
  chr(0),
  chr(0),
  9,
  chr(0),
  chr(0),
  chr(0),
  chr(88),
  chr(0),
  chr(0));

  INSERT INTO DPTDUPPRM_DBT (
     T_ADDRESS, T_BIRTHDATE, T_COINCIDENCE, 
     T_FULLNAME, T_IDENTITYCARD, T_INDISTRIB, 
     T_INN, T_ISACTIVE, T_LASTNAME, 
     T_NAME, T_OGRN, T_PARAMSETID, 
     T_PARAMSETNO, T_PARTYTYPE, T_PATRONYMIC, 
     T_PENSIONCARD, T_PRIORITY, T_REGISTRATION, 
     T_RESIDENT, T_SHORTNAME, T_KIO, T_OKOPF, T_OKPO) 
  VALUES ( chr(0),
  chr(0),
  100,
  chr(0),
  chr(0),
  chr(88),
  chr(88),
  chr(88),
  chr(0),
  chr(0),
  chr(0),
  0,
  7,
  2,
  chr(0),
  chr(0),
  7,
  chr(0),
  chr(0),
  chr(0),
  chr(0),
  chr(88),
  chr(0));

  INSERT INTO DPTDUPPRM_DBT (
     T_ADDRESS, T_BIRTHDATE, T_COINCIDENCE, 
     T_FULLNAME, T_IDENTITYCARD, T_INDISTRIB, 
     T_INN, T_ISACTIVE, T_LASTNAME, 
     T_NAME, T_OGRN, T_PARAMSETID, 
     T_PARAMSETNO, T_PARTYTYPE, T_PATRONYMIC, 
     T_PENSIONCARD, T_PRIORITY, T_REGISTRATION, 
     T_RESIDENT, T_SHORTNAME, T_KIO, T_OKOPF, T_OKPO) 
  VALUES ( chr(0),
  chr(0),
  100,
  chr(0),
  chr(0),
  chr(88),
  chr(88),
  chr(88),
  chr(0),
  chr(0),
  chr(88),
  0,
  8,
  2,
  chr(0),
  chr(0),
  8,
  chr(0),
  chr(0),
  chr(0),
  chr(0),
  chr(0),
  chr(0));
  
COMMIT;
END IF;
  EXCEPTION WHEN others THEN 
    NULL;
END;