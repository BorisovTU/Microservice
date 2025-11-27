declare
  n integer;
begin
  select count(*) into n from user_sequences s where s.SEQUENCE_NAME = 'DDL_REGIABUF_DBT_SEQ';
  if n = 0
  then
    execute immediate ' create sequence DDL_REGIABUF_DBT_SEQ minvalue 1
                maxvalue 9999999999999999999999999999 start with 1 increment by 1 cache 20 ';
  end if;
    select count(*) into n from user_tables s where s.TABLE_NAME = 'DDL_REGIABUF_DBT';
  if n > 0
  then
    execute immediate 'drop table DDL_REGIABUF_DBT';
  end if;
  execute immediate ' create table DDL_REGIABUF_DBT
(
  t_sessionid           NUMBER(20),
  t_calcid              NUMBER(20),
  t_futuresprice        NUMBER(32,12),
  t_outstandclaims      NUMBER(32,12),
  t_outstandobl         NUMBER(32,12),
  t_isbuy               NUMBER(5),
  t_accrest             NUMBER(32,12),
  t_acccur              VARCHAR2(3),
  t_optionpremium       NUMBER(32,12),
  t_kindpfi             VARCHAR2(60),
  t_optiontype          VARCHAR2(4),
  t_autoinc             NUMBER,
  t_dealpart            NUMBER(5),
  t_clientid            NUMBER(10),
  t_pfi                 NUMBER(10),
  t_clientcontrid       NUMBER(10),
  t_part                NUMBER(5),
  t_dealid              NUMBER(10),
  t_currclaimobl        VARCHAR2(3),
  t_dealcode            VARCHAR2(30),
  t_accountname         VARCHAR2(1000),
  t_account             VARCHAR2(25),
  t_cashright           VARCHAR2(23),
  t_risklevel           VARCHAR2(12),
  t_clientcvalinv       VARCHAR2(16),
  t_clientresident      VARCHAR2(11),
  t_clientcountry       VARCHAR2(30),
  t_clientform          VARCHAR2(2),
  t_proxynum            VARCHAR2(200),
  t_proxyname           VARCHAR2(320),
  t_exchangecode        VARCHAR2(36),
  t_clientagrend        VARCHAR2(100),
  t_clientagrdate       VARCHAR2(100),
  t_clientagr           VARCHAR2(200),
  t_clientname          VARCHAR2(320),
  t_factdatem           VARCHAR2(128),
  t_plandatem           VARCHAR2(128),
  t_factdatesp          DATE,
  t_plandatesp          DATE,
  t_brokercomiss        NUMBER(32,12),
  t_interestamount      NUMBER(32,12),
  t_interestrate        FLOAT(53),
  t_setcurrency         VARCHAR2(3),
  t_nominalcurrency     VARCHAR2(3),
  t_pricecurrency       VARCHAR2(3),
  t_dealvaluerur        NUMBER(32,12),
  t_dealvalue           NUMBER(32,12),
  t_totalcostrur        NUMBER(32,12),
  t_totalcost           NUMBER(32,12),
  t_costnatcur          NUMBER(32,12),
  t_cost                NUMBER(32,12),
  t_principal           NUMBER(32,12),
  t_issuername          VARCHAR2(320),
  t_secseries           VARCHAR2(100),
  t_isin                VARCHAR2(70),
  t_seccode             VARCHAR2(70),
  t_secsubkind          VARCHAR2(36),
  t_seckind             VARCHAR2(30),
  t_dealkind            VARCHAR2(85),
  t_dealdirection       VARCHAR2(320),
  t_dealtypeadr         VARCHAR2(11),
  t_unqualifiedsec      VARCHAR2(2),
  t_genagr              VARCHAR2(100),
  t_dealcodets          VARCHAR2(30),
  t_dealdate            DATE,
  t_dealtime            DATE,
  t_marketname          VARCHAR2(320),
  t_markettype          VARCHAR2(100),
  t_extbroker           VARCHAR2(320),
  t_extbrokerdoc        VARCHAR2(20),
  t_dealtype            VARCHAR2(12),
  t_status_purcb        VARCHAR2(26),
  t_side1               VARCHAR2(320),
  t_side2               VARCHAR2(320),
  t_client_assign       VARCHAR2(50),
  t_confirmdoc          VARCHAR2(20),
  t_dueprocess          VARCHAR2(23),
  t_execmethod          VARCHAR2(20),
  t_formpayment         VARCHAR2(35),
  t_isbasket            NUMBER(5),
  t_dealsubkind         NUMBER(5),
  t_debet               NUMBER(32,12),
  t_credit              NUMBER(32,12),
  t_acctype             NUMBER(5),
  t_optionterms         VARCHAR2(100),
  t_futuresamount       NUMBER(32,12),
  t_quotecur            VARCHAR2(3),
  t_optiondealbonus     NUMBER(32,12),
  t_bakindname          VARCHAR2(30),
  t_dockind             NUMBER(10),
  t_margincall          CHAR(1) default CHR(0),
  t_invert_debet_credit CHAR(1) default CHR(0),
  t_princprecision      NUMBER(5),
  t_cond_susp           VARCHAR2(101)
) ';
 execute immediate 'comment on table DDL_REGIABUF_DBT is ''Отчет Реестр ВУ''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_futuresprice
  is ''Цена фьючерского контракта''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_outstandclaims
  is ''Неисполненные требования''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_outstandobl
  is ''Неисполненные обязательства''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_isbuy
  is ''Покупка''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_accrest
  is ''Остаток на счете''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_optionpremium
  is ''Премия по опциону''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_kindpfi
  is ''Тип ПФИ''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_optiontype
  is ''Тип опциона''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_part
  is ''Раздел отчета''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_dealid
  is ''Идентификатор сделки''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_currclaimobl
  is ''Валюта требования/ обязательства''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_dealcode
  is ''Номер сделки''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_accountname
  is ''Наименование счета ВУ''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_account
  is ''Номер счета ВУ''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_cashright
  is ''Право на использование денежных средств клиента банком''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_risklevel
  is ''Категория клиента''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_clientcvalinv
  is ''Клиент является/не является квалифицированным инвестором''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_clientresident
  is ''Клиент является/не является налоговым резидентом''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_clientcountry
  is ''Гражданство Клиента''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_clientform
  is ''Клиент является ФЛ/ЮЛ/ИП''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_proxynum
  is ''Доверенность клиента''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_proxyname
  is ''Представитель клиента''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_exchangecode
  is ''Код на бирже''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_clientagrend
  is ''Дата расторжения''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_clientagrdate
  is ''Дата заключения''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_clientagr
  is ''Договор с клиентом''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_clientname
  is ''Наименование / код клиента''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_factdatem
  is ''Дата оплаты ценных бумаг Факт''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_plandatem
  is ''Дата оплаты ценных бумаг План''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_factdatesp
  is ''Дата перерегистрации (поставки) ценных бумаг Факт''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_plandatesp
  is ''Дата перерегистрации  (поставки) ценных бумаг План''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_brokercomiss
  is ''Комиссия брокера''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_interestamount
  is ''Сумма процентов''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_interestrate
  is ''Cтавка % (РЕПО, займ)''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_setcurrency
  is ''Валюта расчетов''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_nominalcurrency
  is ''Валюта номинала''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_pricecurrency
  is ''Валюта цены''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_dealvaluerur
  is ''Стоимость сделки в рублевом эквиваленте''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_dealvalue
  is ''Стоимость сделки''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_totalcostrur
  is ''Сумма сделки в рублевом эквиваленте''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_totalcost
  is ''Сумма сделки в валюте''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_costnatcur
  is ''Цена одной ценной бумаги в рублевом эквиваленте''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_cost
  is ''Цена одной ценной бумаги в валюте''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_principal
  is ''Количество ценных бумаг''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_issuername
  is ''Эмитент ценной бумаги''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_secseries
  is ''Выпуск / транш /  серия ценной бумаги''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_isin
  is ''№гос.регистрации/ ISIN''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_seccode
  is ''Наименование/ код ценной бумаги''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_secsubkind
  is ''Подвид ценной бумаги''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_seckind
  is ''Вид ценной бумаги''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_dealkind
  is ''Вид сделки (покупка, продажа, займ, РЕПО и пр.)''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_dealdirection
  is ''Направление сделки''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_dealtypeadr
  is ''Вид заявки (адресная/ безадресная)''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_unqualifiedsec
  is ''Сделка с неквалифицированными ЦБ''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_genagr
  is ''Генеральное соглашение''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_dealcodets
  is ''Внешний номер сделки''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_dealdate
  is ''Дата заключения сделки''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_dealtime
  is ''Время заключения сделки''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_marketname
  is ''Место совершения сделки''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_markettype
  is ''Сектор биржи/режим торгов''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_extbroker
  is ''Внешний брокер''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_extbrokerdoc
  is ''Номер договора с внешним брокером''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_dealtype
  is ''Тип сделки''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_status_purcb
  is ''Сатус ПУРЦБ''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_side1
  is ''Сторона 1''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_side2
  is ''Сторона 2''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_client_assign
  is ''Поручение клиента на операцию''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_confirmdoc
  is ''Подтверждающий документ''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_dueprocess
  is ''Порядок исполнения обязательств''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_execmethod
  is ''Способ исполнения обязательств''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_formpayment
  is ''Форма расчетов''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_debet
  is ''Списано по сделке''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_credit
  is ''Зачслено по сделке''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_acctype
  is ''Тип счета ВУ (ЦБ, ДС, ФО)''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_optionterms
  is ''Условия по опциону''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_futuresamount
  is ''Количество фьючерских контрактор''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_quotecur
  is ''Валюта котировки, премии по опциону''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_optiondealbonus
  is ''Премия по опциону''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_bakindname
  is ''Базисный актив ПФИ''';
 execute immediate 'comment on column DDL_REGIABUF_DBT.t_dockind
  is ''Вид документа''';
 execute immediate 'create unique index DDL_REGIABUF_DBT_IDX0 on DDL_REGIABUF_DBT (T_AUTOINC)';
 execute immediate 'create index DDL_REGIABUF_DBT_IDX1 on DDL_REGIABUF_DBT (T_SESSIONID, T_CALCID, T_PART, T_DEALID, T_DEALSUBKIND, T_DEALPART, T_CLIENTCONTRID, T_ACCTYPE)';
 execute immediate 'create index DDL_REGIABUF_DBT_IDX2 on DDL_REGIABUF_DBT (T_SESSIONID, T_CALCID, T_PART, T_ACCTYPE)';
 execute immediate 'create index DDL_REGIABUF_DBT_IDX3 on DDL_REGIABUF_DBT (T_SESSIONID, T_CALCID, T_PART, T_CLIENTCONTRID, T_ACCTYPE)';
 
   
 execute immediate 'CREATE OR REPLACE TRIGGER "DDL_REGIABUF_DBT_T0_AINC"
  BEFORE INSERT OR UPDATE OF T_AUTOINC ON DDL_REGIABUF_DBT FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:NEW.T_AUTOINC = 0 OR :NEW.T_AUTOINC IS NULL) THEN
    SELECT DDL_REGIABUF_DBT_SEQ.NEXTVAL INTO :NEW.T_AUTOINC FROM DUAL;
  ELSE
    SELECT LAST_NUMBER INTO v_id FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME) = UPPER(''DDL_REGIABUF_DBT_SEQ'');
    IF :NEW.T_AUTOINC >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;' ;

end;
