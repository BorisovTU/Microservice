/*Таблица DBRKREPGROUPMOVING_DBT*/
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE DBRKREPGROUPMOVING_DBT CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE DBRKREPGROUPMOVING_DBT (
    T_GROUPID   NUMBER(5)
  , T_GROUPTYPE VARCHAR2(200)
  , T_ISGROUP   CHAR(1)
  , T_DEB_ACC   VARCHAR2(20)
  , T_CR_ACC    VARCHAR2(20)
  , T_GROUND    VARCHAR2(200)
)
/

COMMENT ON TABLE DBRKREPGROUPMOVING_DBT IS 'Таблица группировки торговых операций ОБ'
/
COMMENT ON COLUMN DBRKREPGROUPMOVING_DBT.T_GROUPID IS 'Номер группы'
/
COMMENT ON COLUMN DBRKREPGROUPMOVING_DBT.T_GROUPTYPE IS 'Наименование группы'
/
COMMENT ON COLUMN DBRKREPGROUPMOVING_DBT.T_ISGROUP IS 'Признак "Подлежит группировке"'
/
COMMENT ON COLUMN DBRKREPGROUPMOVING_DBT.T_DEB_ACC IS 'Маска счета по дебету'
/
COMMENT ON COLUMN DBRKREPGROUPMOVING_DBT.T_CR_ACC IS 'Маска счета по кредиту'
/
COMMENT ON COLUMN DBRKREPGROUPMOVING_DBT.T_GROUND IS 'Ключевые слова из основания'
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DBRKREPGROUPMOVING_DBT_IDX0';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE UNIQUE INDEX DBRKREPGROUPMOVING_DBT_IDX0 ON DBRKREPGROUPMOVING_DBT (
   T_GROUPID ASC
)
/

BEGIN
  EXECUTE IMMEDIATE 'DROP SEQUENCE DBRKREPGROUPMOVING_DBT_SEQ';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE SEQUENCE DBRKREPGROUPMOVING_DBT_SEQ 
  START WITH 1
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  NOCACHE
  NOORDER
/

CREATE OR REPLACE TRIGGER DBRKREPGROUPMOVING_DBT_T0_AINC
  BEFORE INSERT OR UPDATE OF T_GROUPID ON DBRKREPGROUPMOVING_DBT FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.T_GROUPID = 0 OR :NEW.T_GROUPID IS NULL) THEN
    SELECT DBRKREPGROUPMOVING_DBT_SEQ.NEXTVAL INTO :NEW.T_GROUPID FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER('DBRKREPGROUPMOVING_DBT_SEQ');
    IF :NEW.T_GROUPID >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/

DECLARE
  PROCEDURE AddGroup(p_GroupType IN VARCHAR2, p_IsGroup IN CHAR, p_Deb_Acc IN VARCHAR2, p_Cr_Acc IN VARCHAR2, p_Ground IN VARCHAR2)
  IS
  BEGIN
     INSERT INTO DBRKREPGROUPMOVING_DBT 
       (T_GROUPTYPE, T_ISGROUP, T_DEB_ACC, T_CR_ACC, T_GROUND)
     VALUES
       (p_GroupType, p_IsGroup, p_Deb_Acc, p_Cr_Acc, p_Ground);
  END;   
BEGIN
  AddGroup('Комиссия брокера',     'X', '306',   '47423', 'Комиссия брокера');
  AddGroup('Комиссия брокера',     'X', '306',   '47423', 'Брокерская комиссия');
  AddGroup('Зачисление по сделке', 'X', '47404', '306',   'Репо. Сделка №');
  AddGroup('Зачисление по сделке', 'X', '47404', '306',   'Продажа. Сделка №');
  AddGroup('Зачисление по сделке', 'X', '47404', '306',   'ПКД при продаже');
  AddGroup('Зачисление по сделке', 'X', '47404', '306',   'Перевод средств по сделке');
  AddGroup('Зачисление по сделке', 'X', '47404', '306',   'Пкд по Репо. Сделка №');
  AddGroup('Списание по сделке',   'X', '306',   '47404', 'Репо. Сделка №');
  AddGroup('Списание по сделке',   'X', '306',   '47404', 'Пкд по Репо. Сделка №');
  AddGroup('Списание по сделке',   'X', '306',   '47404', 'Покупка. Сделка №');
  AddGroup('Списание по сделке',   'X', '306',   '47404', 'ПКД при покупке');
  AddGroup('Списание по сделке',   'X', '306',   '47404', 'Перевод средств по сделке');
  AddGroup('Вариационная маржа',   'X', '47404', '306',   'Вариационная маржа начисленная');
  AddGroup('Вариационная маржа',   'X', '306',   '47404', 'Вариационная маржа удержанная');
  AddGroup('Вариационная маржа',   'X', '47404', '306',   'Зачисление суммы полученной вар');
  AddGroup('Вариационная маржа',   'X', '306',   '47404', 'Списание суммы оплаченной вар');
  AddGroup('Удержание налога',     'X', '306',   '60301', 'Удержание налога на доходы');
  AddGroup('Комиссия биржи',       'X', '306',   '47423', 'Клиринговый сбор');
  AddGroup('Комиссия биржи',       'X', '306',   '47423', 'Уплата комиссии биржи');
END;
/  