DECLARE
  v_ObjectType number;
  TYPE llRec IS RECORD (t_code varchar2(16), t_name varchar2(70));
  TYPE llTab IS TABLE OF llRec;
  llArr llTab;
  FUNCTION ll(aCode varchar2, aName varchar2) RETURN llRec 
  IS
    r llRec;
  BEGIN
    r.t_code := aCode;
    r.t_name := aName;
    RETURN(r);
  END;
  FUNCTION getObject(aDefault number, aCode varchar2, aName varchar2) RETURN number 
  IS
    v_Return number;
  BEGIN
    v_Return := 0;
    BEGIN
      SELECT t_ObjectType INTO v_Return FROM dobjects_dbt WHERE t_Code = aCode;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN 
        v_Return := aDefault;
        INSERT INTO DOBJECTS_DBT ( 
          t_ObjectType, t_Name, t_Code, t_UserNumber, t_ParentObjectType, t_Module 
        ) VALUES (
          v_Return, aName, aCode, 0, 0, chr(0)
        );
    END;
    RETURN(v_Return);
  END;
  PROCEDURE addLLValues(aObjType number, aArr llTab) 
  IS
    v_Elem number;
  BEGIN
    v_Elem := 1;
    FOR i IN aArr.first .. aArr.last LOOP
      BEGIN
        INSERT INTO dllvalues_dbt (
          T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE
        ) VALUES (
          aObjType, v_Elem, aArr(i).t_code, aArr(i).t_name, 0, chr(1), chr(1)
        );
      EXCEPTION 
        WHEN DUP_VAL_ON_INDEX THEN 
          NULL;
      END;
      v_Elem := v_Elem + 1;
    END LOOP;
  END;
BEGIN
  -- п.1. ТЗ. Создание справочников и элементов справочников

  -- Добавить справочник и его элементы
  v_ObjectType := getObject(5060, 'ВнСдКДУ', 'Внебиржевые сделки для КДУ');
  llArr := llTab( 
     ll('12183', 'Покупка ц/б внебиржевая')
     , ll('12193', 'Продажа ц/б внебиржевая')
     , ll('32732', 'Зачисление ц/б из ДУ')
     , ll('12132', 'Обратное РЕПО без признания ц/б')
     , ll('12137', 'Прямое РЕПО без признания ц/б')
     , ll('3155', 'Покупка ВО внебиржевая')
     , ll('3156', 'Продажа ВО внебиржевая')
  );
  addLLValues(v_ObjectType, llArr);
  COMMIT;

  -- Другой справочник и его элементы
  v_ObjectType := getObject(5061, 'СдКорзКДУ', 'Сделки с корзиной для КДУ');
  llArr := llTab( 
     ll('12139', 'Прямое РЕПО на корзину ц/б')
     , ll('12138', 'Прямое РЕПО с ЦК на корзину ц/б')
     , ll('12134', 'Обратное РЕПО на корзину ц/б')
     , ll('12133', 'Обратное РЕПО с ЦК на корзину ц/б')
     , ll('2124', 'Междилерское обратное РЕПО на корзину ц/б')
  );
  addLLValues(v_ObjectType, llArr);
  COMMIT;

  -- И еще один
  v_ObjectType := getObject(5062, 'СдДУдКДУ', 'Сделки ДУ для КДУ');
  llArr := llTab( 
     ll('32732', 'Зачисление ц/б из ДУ')
     , ll('32742', 'Возврат ц/б в ДУ')
  );
  addLLValues(v_ObjectType, llArr);
  COMMIT;
END;
/

DECLARE
  v_SettaccID number;
  v_PartyID number := 1; -- Идентификатор бенефициара (наш банк)
  v_BankID number := 4; -- Идентификатор банка
  v_Account varchar2(35) := 'МА1212250033/21000000000000000'; -- Счет бенефициара
  v_Inn varchar2(35) := '1782010485'; -- ИНН бенефициара
  v_Recname varchar2(320) := '1. АО "РОССЕЛЬХОЗБАНК"';-- Наименование бенефициара
  v_Bankcodekind number := 1; -- Вид кода банка
  v_Bankcode varchar2(35);
  v_Bankname varchar2(320);
  v_Code varchar2(35);
  v_Description varchar2(210) := 'KIND:3155 - содержание операции Внебиржевой выкуп'; -- Описание СПИ
  v_Order number; -- Порядок сортировки
  v_Exist number;
BEGIN
  -- п.2. ТЗ. Создание СПИ и параметров выбора СПИ
  BEGIN
    -- поиск нужной СПИ
    SELECT t_settaccid 
      INTO v_SettaccID 
      FROM dsettacc_dbt r 
      WHERE r.t_fikind = 2 
      AND r.t_fiid = -1 AND r.t_bankid = 4 
      AND r.t_chapter = 0 AND r.t_account = v_Account
    ;
  EXCEPTION WHEN NO_DATA_FOUND THEN 
    -- СПИ не найдена, нужно создать
    SELECT nvl(max(t_order),0)+1 
      INTO v_Order 
      FROM dsettacc_dbt 
      WHERE t_Partyid = v_PartyID;
    SELECT t_code 
      INTO v_Bankcode
      FROM dpartcode_dbt r 
      WHERE r.t_partyid = v_BankID AND r.t_codekind = v_Bankcodekind;
    SELECT t_code 
      INTO v_Code
      FROM dpartcode_dbt r 
      WHERE r.t_partyid = v_PartyID AND r.t_codekind = 1;
    SELECT t_name 
      INTO v_Bankname
      FROM dparty_dbt r 
      WHERE r.t_partyid = v_BankID;
    INSERT INTO dsettacc_dbt r (
      t_settaccid, t_partyid, t_bankid, t_fiid, t_chapter, t_account, t_inn, t_Recname
      , t_bankcodekind, t_bankcode, t_bankname, t_bankcorrid, t_bankcorrcodekind, t_fikind
      , t_beneficiaryID, t_codekind, t_code, t_description, t_order
    ) VALUES (
      0, v_PartyID, 4, -1, 0, v_Account, v_Inn, v_Recname
      , v_Bankcodekind, v_Bankcode, v_Bankname, -1, 1, 2
      , v_PartyID, 1, v_Code, v_Description, v_Order
    )
    RETURNING t_settaccid 
    INTO v_SettaccID
    ;
    UPDATE dsettacc_dbt
      SET t_shortname = v_SettaccID
      WHERE t_settaccid = v_SettaccID
    ;
  END;

  BEGIN
    -- поиск параметров выбора нашей СПИ
    SELECT t_order 
      INTO v_Order 
      FROM dpmautoac_dbt r 
      WHERE r.t_settaccid = v_SettaccID
    ;
  EXCEPTION WHEN NO_DATA_FOUND THEN 
    -- нет записи в параметрах выбора нашей СПИ, создаем
    SELECT nvl(max(t_order),0)+1 
      INTO v_Order 
      FROM dpmautoac_dbt
      WHERE t_Partyid = v_PartyID;
    INSERT INTO dpmautoac_dbt r (
      t_partyid, t_fiid, t_kindoper, t_purpose, t_settaccid, t_fikind
      , t_servicekind, t_order, t_account
    ) VALUES (
      v_PartyID, -1, 3155, 0, v_SettaccID, 2, 1, v_Order, chr(1)
    );
  END;
  COMMIT;
END;
/
