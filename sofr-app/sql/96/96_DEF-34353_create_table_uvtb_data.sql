CREATE TABLE UVTB_DATA
(
    GKK             VARCHAR2 (2000 BYTE),
    SECONDNAME      VARCHAR2 (2000 BYTE),
    FIRSTNAME       VARCHAR2 (2000 BYTE),
    MIDDLENAME      VARCHAR2 (2000 BYTE),
    PHONE           VARCHAR2 (2000 BYTE),
    AGRNO           VARCHAR2 (2000 BYTE),
    ISIN            VARCHAR2 (2000 BYTE),
    TRANSH          VARCHAR2 (2000 BYTE),
    SECNAME         VARCHAR2 (2000 BYTE),
    OPERNO          VARCHAR2 (2000 BYTE),
    DATEPOS         VARCHAR2 (2000 BYTE),
    QTY             VARCHAR2 (2000 BYTE),
    CURR            VARCHAR2 (2000 BYTE),
    PRICE           VARCHAR2 (2000 BYTE),
    AMOUNT          VARCHAR2 (2000 BYTE),
    ACI             VARCHAR2 (2000 BYTE),
    CMS             VARCHAR2 (2000 BYTE),
    VALUERUR        VARCHAR2 (2000 BYTE),
    CMSRUR          VARCHAR2 (2000 BYTE),
    STORAGE         VARCHAR2 (2000 BYTE),
    NOMINAL         VARCHAR2 (2000 BYTE),
    NOMINALCURR     VARCHAR2 (2000 BYTE),
    PRICEQTY        VARCHAR2 (2000 BYTE),
    T_ID            NUMBER (10),
    T_DEALID        NUMBER (10),
    T_DEALSTATUS    NUMBER (10),
    T_CLIENTID      NUMBER (10),
    T_FIID          NUMBER (10),
    T_FIID_NEW      NUMBER (10),
    T_AMOUNT_NEW    NUMBER(32,12),
    T_DEALCODE      VARCHAR2 (200 BYTE),
    T_STATE         VARCHAR2 (10 BYTE),
    T_COMMENT       VARCHAR2 (1000),
    T_SYSDATE       DATE DEFAULT SYSDATE
)
/

COMMENT ON COLUMN UVTB_DATA.GKK IS 'Код ГКК' /
COMMENT ON COLUMN UVTB_DATA.SECONDNAME IS 'Фамилия' /
COMMENT ON COLUMN UVTB_DATA.FIRSTNAME IS 'Имя'/
COMMENT ON COLUMN UVTB_DATA.MIDDLENAME IS 'Отчество' /
COMMENT ON COLUMN UVTB_DATA.PHONE IS 'Телефон' /
COMMENT ON COLUMN UVTB_DATA.AGRNO IS 'Номер договора ВТБ' /
COMMENT ON COLUMN UVTB_DATA.ISIN IS 'ISIN' /
COMMENT ON COLUMN UVTB_DATA.TRANSH IS 'Транш' /
COMMENT ON COLUMN UVTB_DATA.SECNAME IS 'Наименование ц/б' /
COMMENT ON COLUMN UVTB_DATA.OPERNO IS 'Код операции приобретения' /
COMMENT ON COLUMN UVTB_DATA.DATEPOS IS 'Дата приобретения' /
COMMENT ON COLUMN UVTB_DATA.QTY IS 'Количество' /
COMMENT ON COLUMN UVTB_DATA.CURR IS 'Валюта сделки' /
COMMENT ON COLUMN UVTB_DATA.PRICE IS 'Цена приобретения' /
COMMENT ON COLUMN UVTB_DATA.AMOUNT IS 'Сумма сделки' /
COMMENT ON COLUMN UVTB_DATA.ACI IS 'НКД' /
COMMENT ON COLUMN UVTB_DATA.CMS IS 'Комиссионные затраты' /
COMMENT ON COLUMN UVTB_DATA.VALUERUR IS 'Сумма сделки (вкл НКД), руб' /
COMMENT ON COLUMN UVTB_DATA.CMSRUR IS 'Комиссионные затраты, руб' /
COMMENT ON COLUMN UVTB_DATA.STORAGE IS 'Место хранения' /
COMMENT ON COLUMN UVTB_DATA.NOMINAL IS 'Номинал' /
COMMENT ON COLUMN UVTB_DATA.NOMINALCURR IS 'Валюта номинала' /
COMMENT ON COLUMN UVTB_DATA.PRICEQTY IS 'Цена*Количество' /
COMMENT ON COLUMN UVTB_DATA.T_ID IS 'ID - уникальный идентификатор' /
COMMENT ON COLUMN UVTB_DATA.T_DEALID IS 'Идентификатор связанного НДФЛ-зачисления в СОФР' /
COMMENT ON COLUMN UVTB_DATA.T_DEALSTATUS IS 'Статус связанного НДФЛ-зачисления' /
COMMENT ON COLUMN UVTB_DATA.T_CLIENTID IS 'Идентификатор Клиента в СОФР' /
COMMENT ON COLUMN UVTB_DATA.T_FIID IS 'Идентификатор ценной бумаги в СОФР, соответствующий полю ISIN' /
COMMENT ON COLUMN UVTB_DATA.T_FIID_NEW IS 'Новый идентификатор ценной бумаги в СОФР, если исходный выпуск был сконвертирован' /
COMMENT ON COLUMN UVTB_DATA.T_AMOUNT_NEW IS 'Новое количество ценной бумаги в СОФР, если исходный выпуск был сконвертирован' /
COMMENT ON COLUMN UVTB_DATA.T_DEALCODE IS 'Код связанного НДФЛ-зачисления' /
COMMENT ON COLUMN UVTB_DATA.T_STATE IS 'Статус записи' /
COMMENT ON COLUMN UVTB_DATA.T_COMMENT IS 'Комментарий' /
COMMENT ON COLUMN UVTB_DATA.T_SYSDATE IS 'Дата последнего обновления' /

CREATE UNIQUE INDEX UVTB_DATA_IDX0
    ON UVTB_DATA (T_ID) /

CREATE INDEX UVTB_DATA_IDX1
    ON UVTB_DATA (T_DEALID) /

CREATE INDEX UVTB_DATA_IDX2
    ON UVTB_DATA (GKK) /

CREATE INDEX UVTB_DATA_IDX3
    ON UVTB_DATA (OPERNO) /

CREATE INDEX UVTB_DATA_IDX4
    ON UVTB_DATA (T_DEALCODE) /

CREATE INDEX UVTB_DATA_IDX5
    ON UVTB_DATA (T_DEALSTATUS) /

CREATE INDEX UVTB_DATA_IDX6
    ON UVTB_DATA (T_CLIENTID) /

CREATE INDEX UVTB_DATA_IDX7
    ON UVTB_DATA (T_STATE) /

CREATE SEQUENCE UVTB_DATA_SEQ START WITH 1
                              MAXVALUE 9999999999999999999999999999
                              MINVALUE 1
                              NOCYCLE
                              NOCACHE
                              NOORDER
/                              

CREATE OR REPLACE TRIGGER "UVTB_DATA_T0_AINC"
    BEFORE INSERT OR UPDATE OF t_id
    ON uvtb_data
    FOR EACH ROW
DECLARE
    v_id   INTEGER;
BEGIN
    IF (:new.t_dealid = 0 OR :new.t_id IS NULL)
    THEN
        SELECT uvtb_data_seq.NEXTVAL INTO :new.t_id FROM DUAL;
    ELSE
        SELECT last_number
          INTO v_id
          FROM user_sequences
         WHERE sequence_name = UPPER ('uvtb_data_SEQ');

        IF :new.t_id >= v_id
        THEN
            RAISE DUP_VAL_ON_INDEX;
        END IF;
    END IF;
END;
/