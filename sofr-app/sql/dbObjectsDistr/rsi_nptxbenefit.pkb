CREATE OR REPLACE PACKAGE BODY RSI_NPTXBENEFIT
IS

  --Получить строку, которая выводится в поле скроллинга условий льготы, когда значение не задано
  FUNCTION GetDefaultStr RETURN VARCHAR2
  AS
    v_str dnamealg_dbt.t_sznamealg%type;
  BEGIN
    SELECT t_sznamealg INTO v_str FROM DNAMEALG_DBT WHERE t_iTypeAlg = 7349 AND t_iNumberAlg = 0;

    RETURN v_str;
  END;

  --Получить значение поля "Объект сделки" для скроллинга условий льготы
  FUNCTION PumpDealObjectBeneCond(p_DealObject IN NUMBER) RETURN VARCHAR2
  AS
    v_DealObjectName davrkinds_dbt.t_Name%type;
  BEGIN

    IF p_DealObject > 0 THEN
      BEGIN
        SELECT t_Name INTO v_DealObjectName FROM DAVRKINDS_DBT WHERE t_FI_Kind = 2 AND t_AvoirKind = p_DealObject;

        EXCEPTION
           WHEN NO_DATA_FOUND THEN v_DealObjectName := GetDefaultStr();
      END;
    ELSE
      v_DealObjectName := GetDefaultStr();
    END IF;

    RETURN v_DealObjectName;
  END;

  --Получить значение поля "Тип сделки форм. дох." для скроллинга условий льготы
  FUNCTION PumpDealTypeBeneCond(p_KindDealType IN NUMBER, p_DealType IN NUMBER) RETURN VARCHAR2
  AS
    v_DealTypeName doprkoper_dbt.t_Name%type;
  BEGIN

    IF p_DealType > 0 THEN
      BEGIN
        IF p_KindDealType = 1 THEN
          SELECT t_sznamealg INTO v_DealTypeName FROM DNAMEALG_DBT WHERE t_iTypeAlg = 7360 AND t_iNumberAlg = p_DealType;
        ELSIF p_KindDealType = 2 THEN
          SELECT t_Name INTO v_DealTypeName FROM DOPRKOPER_DBT WHERE t_Kind_Operation = p_DealType;
        ELSE
          v_DealTypeName := GetDefaultStr();
        END IF;

        EXCEPTION
           WHEN NO_DATA_FOUND THEN v_DealTypeName := GetDefaultStr();
      END;
    ELSE
      v_DealTypeName := GetDefaultStr();
    END IF;

    RETURN v_DealTypeName;
  END;

  --Получить значение поля "Налоговый статус ФЛ" для скроллинга условий льготы
  FUNCTION PumpResidentStatusBeneCond(p_ResidentStatus IN NUMBER) RETURN VARCHAR2
  AS
    v_ResidentStatusName dnamealg_dbt.t_sznamealg%type;
  BEGIN

    IF p_ResidentStatus > 0 THEN
      BEGIN
        SELECT t_sznamealg INTO v_ResidentStatusName FROM DNAMEALG_DBT WHERE t_iTypeAlg = 7547 AND t_iNumberAlg = p_ResidentStatus;

        EXCEPTION
           WHEN NO_DATA_FOUND THEN v_ResidentStatusName := GetDefaultStr();
      END;
    ELSE
      v_ResidentStatusName := GetDefaultStr();
    END IF;

    RETURN v_ResidentStatusName;
  END;

  --Получить значение поля "Срок владения" для скроллинга условий льготы
  FUNCTION PumpOwnerPeriodBeneCond(p_OwnerPeriodSign IN VARCHAR2, p_OwnerPeriod IN NUMBER) RETURN VARCHAR2
  AS
    v_OwnerPeriodStr dnamealg_dbt.t_sznamealg%type;
  BEGIN

    IF p_OwnerPeriod > 0 THEN
      v_OwnerPeriodStr := p_OwnerPeriodSign||' '||TO_CHAR(p_OwnerPeriod);
    ELSE
      v_OwnerPeriodStr := GetDefaultStr();
    END IF;

    RETURN v_OwnerPeriodStr;
  END;

  --Получить значение поля "Условие для страны" для скроллинга условий льготы
  FUNCTION PumpIssuerCountryCondBeneCond(p_IsExcept IN CHAR) RETURN VARCHAR2
  AS
    v_IssuerCountryCond VARCHAR2(40) := CHR(1);
  BEGIN
    IF p_IsExcept = 'X' THEN
      v_IssuerCountryCond := 'кроме';
    END IF;

    RETURN v_IssuerCountryCond;
  END;
  
  --Получить значение поля "Страна эмитент" для скроллинга условий льготы
  FUNCTION PumpIssuerCountryBeneCond(p_IssuerCountry IN VARCHAR2) RETURN VARCHAR2
  AS
    v_IssuerCountryName dcountry_dbt.t_Name%type;
  BEGIN
    IF p_IssuerCountry != CHR(1) THEN
      BEGIN
        SELECT t_Name INTO v_IssuerCountryName FROM DCOUNTRY_DBT WHERE t_CodeLat3 = p_IssuerCountry;

        EXCEPTION
           WHEN NO_DATA_FOUND THEN v_IssuerCountryName := GetDefaultStr();
      END;
    ELSE
      v_IssuerCountryName := GetDefaultStr();
    END IF;

    RETURN v_IssuerCountryName;
  END;

  --Получить значение поля "Признак льготы (актив)" для скроллинга условий льготы
  FUNCTION PumpBenefitSignOnFiBeneCond(p_BenefitSignOnFi IN NUMBER) RETURN VARCHAR2
  AS
    v_BenefitSignOnFiName dobjattr_dbt.t_Name%type;
  BEGIN
    IF p_BenefitSignOnFi > 0 THEN
      BEGIN
        SELECT t_Name INTO v_BenefitSignOnFiName FROM DOBJATTR_DBT WHERE t_ObjectType = 12 /*OBJTYPE_AVOIRISS*/ AND t_GroupID = 54 /*Льготное налогообложение*/ AND t_AttrID = p_BenefitSignOnFi;

        EXCEPTION
           WHEN NO_DATA_FOUND THEN v_BenefitSignOnFiName := GetDefaultStr();
      END;
    ELSE
      v_BenefitSignOnFiName := GetDefaultStr();
    END IF;

    RETURN v_BenefitSignOnFiName;
  END;

  --Получить значение поля "Обращаемость ц/б" для скроллинга условий льготы
  FUNCTION PumpCirculateBeneCond(p_Circulate IN NUMBER) RETURN VARCHAR2
  AS
    v_CirculateName dobjattr_dbt.t_Name%type;
  BEGIN
    IF p_Circulate > 0 THEN
      BEGIN
        SELECT t_Name INTO v_CirculateName FROM DOBJATTR_DBT WHERE t_ObjectType = 12 /*OBJTYPE_AVOIRISS*/ AND t_GroupID = 38 /*Обращается на ОРЦБ для целей НДФЛ*/ AND t_AttrID = p_Circulate;

        EXCEPTION
           WHEN NO_DATA_FOUND THEN v_CirculateName := GetDefaultStr();
      END;
    ELSE
      v_CirculateName := GetDefaultStr();
    END IF;

    RETURN v_CirculateName;
  END;

  --Получить значение поля "Признак экономически значимой организации на эмитенте" для скроллинга условий льготы
  FUNCTION PumpBenefitSignOnIssuerBeneCond(p_BenefitSignOnIssuer IN NUMBER) RETURN VARCHAR2
  AS
    v_BenefitSignOnIssuerName dobjattr_dbt.t_Name%type;
  BEGIN
    IF p_BenefitSignOnIssuer > 0 THEN
      BEGIN
        SELECT t_Name INTO v_BenefitSignOnIssuerName FROM DOBJATTR_DBT WHERE t_ObjectType = 3 /*OBJTYPE_PARTY*/ AND t_GroupID = 101 /*Экономически значимая организация*/ AND t_AttrID = p_BenefitSignOnIssuer;

        EXCEPTION
           WHEN NO_DATA_FOUND THEN v_BenefitSignOnIssuerName := GetDefaultStr();
      END;
    ELSE
      v_BenefitSignOnIssuerName := GetDefaultStr();
    END IF;

    RETURN v_BenefitSignOnIssuerName;
  END;

  --Получить значение поля "Признак льготы (ФЛ)" для скроллинга условий льготы
  FUNCTION PumpBenefitSignOnClientBeneCond(p_BenefitSignOnClient IN NUMBER) RETURN VARCHAR2
  AS
    v_BenefitSignOnClientName dobjattr_dbt.t_Name%type;
  BEGIN
    IF p_BenefitSignOnClient > 0 THEN
      BEGIN
        SELECT t_Name INTO v_BenefitSignOnClientName FROM DOBJATTR_DBT WHERE t_ObjectType = 3 /*OBJTYPE_PARTY*/ AND t_GroupID = 59 /*Льготный режим налогообложения*/ AND t_AttrID = p_BenefitSignOnClient;

        EXCEPTION
           WHEN NO_DATA_FOUND THEN v_BenefitSignOnClientName := GetDefaultStr();
      END;
    ELSE
      v_BenefitSignOnClientName := GetDefaultStr();
    END IF;

    RETURN v_BenefitSignOnClientName;
  END;

  --Получить значение поля "Дата приобретения актива" для скроллинга условий льготы
  FUNCTION PumpDealDateBeneCond(p_DealDateSign IN VARCHAR2, p_DealDate IN DATE) RETURN VARCHAR2
  AS
    v_DealDateStr VARCHAR2(20);
  BEGIN
    IF p_DealDate != TO_DATE('01.01.0001','DD.MM.YYYY') THEN
      v_DealDateStr := p_DealDateSign||' '||TO_CHAR(TRUNC(p_DealDate), 'DD.MM.YYYY');
    ELSE
      v_DealDateStr := GetDefaultStr();
    END IF;

    RETURN v_DealDateStr;
  END;

  --Получить значение поля "Особое условие" для скроллинга условий льготы
  FUNCTION PumpSpecialConditionBeneCond(p_SpecialCondition IN NUMBER) RETURN VARCHAR2
  AS
    v_SpecialConditionName dllvalues_dbt.t_Note%type;
  BEGIN
    IF p_SpecialCondition > 0 THEN
      BEGIN
        SELECT t_Note INTO v_SpecialConditionName FROM DLLVALUES_DBT WHERE t_List = 4164 AND t_Element = p_SpecialCondition;

        EXCEPTION
           WHEN NO_DATA_FOUND THEN v_SpecialConditionName := GetDefaultStr();
      END;
    ELSE
      v_SpecialConditionName := GetDefaultStr();
    END IF;

    RETURN v_SpecialConditionName;
  END;

  --Сгенерировать номер вида льготы
  FUNCTION GenerateBenefitType RETURN VARCHAR2
  AS
    v_nextnum NUMBER(10) := 0;
    v_minnum NUMBER(10) := 0;
    v_maxnum NUMBER(10) := 0;
    v_prevstr VARCHAR2(3) := 'Л_';
  BEGIN

    --Сначала ищем минимальный используемый номер. Если его нет или он больше 1, то используем 1
    SELECT NVL(MIN(TO_NUMBER(REPLACE(t.t_BenefitType, v_prevstr))), 0)
      INTO v_minnum
      FROM DNPTXBENEFITS_DBT t;

    IF v_minnum = 0 OR v_minnum > 1 THEN 
      v_nextnum := 1;
    END IF;

    IF v_nextnum = 0 THEN
      --Если номер не найден, то ищем минимальный пропущенный номер между существующими номерами
      SELECT NVL(MIN(n),0)
        INTO v_nextnum
        FROM (SELECT TO_NUMBER(REPLACE(t1.t_BenefitType, v_prevstr))+1 as n
                FROM DNPTXBENEFITS_DBT t1
                JOIN DNPTXBENEFITS_DBT t2 ON TO_NUMBER(REPLACE(t1.t_BenefitType, v_prevstr)) < TO_NUMBER(REPLACE(t2.t_BenefitType, v_prevstr))
              GROUP BY TO_NUMBER(REPLACE(t1.t_BenefitType, v_prevstr))
              HAVING TO_NUMBER(REPLACE(t1.t_BenefitType, v_prevstr))+1 < MIN(TO_NUMBER(REPLACE(t2.t_BenefitType, v_prevstr)))
             );
    END IF;

    IF v_nextnum = 0 THEN --Пропущенного номера нет, ищем максимальный используемый и увеличиваем на 1
      SELECT NVL(MAX(TO_NUMBER(REPLACE(t.t_BenefitType, v_prevstr))), 0)
        INTO v_maxnum
        FROM DNPTXBENEFITS_DBT t;

      v_nextnum := v_maxnum + 1;
    END IF;

    RETURN v_prevstr||TO_CHAR(v_nextnum);

  END;

  --Проверить номер вида льготы на соответствие шаблону
  FUNCTION CheckBenefitType(p_BenefitType IN VARCHAR2) RETURN NUMBER
  AS
    v_Check NUMBER := 0;
  BEGIN

    BEGIN
      SELECT 1 INTO v_Check FROM DUAL WHERE REGEXP_LIKE(p_BenefitType, '^Л_\d+$');
    END;

    RETURN v_Check;

  END;

  --Получить наименьшее неиспользуемое значение приоритета льгот
  FUNCTION GetNextMinBenefitPriority RETURN NUMBER
  AS
    v_nextnum NUMBER(10) := 0;
    v_minnum NUMBER(10) := 0;
    v_maxnum NUMBER(10) := 0;
  BEGIN

    --Сначала ищем минимальный используемый номер. Если его нет или он больше 1, то используем 1
    SELECT NVL(MIN(t.t_BenefitPriority), 0)
      INTO v_minnum
      FROM DNPTXBENEFITS_DBT t;

    IF v_minnum = 0 OR v_minnum > 1 THEN 
      v_nextnum := 1;
    END IF;

    IF v_nextnum = 0 THEN
      --Если номер не найден, то ищем минимальный пропущенный номер между существующими номерами
      SELECT NVL(MIN(n),0)
        INTO v_nextnum
        FROM (SELECT t1.t_BenefitPriority+1 as n
                FROM DNPTXBENEFITS_DBT t1
                JOIN DNPTXBENEFITS_DBT t2 ON t1.t_BenefitPriority < t2.t_BenefitPriority
              GROUP BY t1.t_BenefitPriority
              HAVING t1.t_BenefitPriority+1 < MIN(t2.t_BenefitPriority)
             );
    END IF;

    IF v_nextnum = 0 THEN --Пропущенного номера нет, ищем максимальный используемый и увеличиваем на 1
      SELECT NVL(MAX(t.t_BenefitPriority), 0)
        INTO v_maxnum
        FROM DNPTXBENEFITS_DBT t;

      v_nextnum := v_maxnum + 1;
    END IF;

    RETURN v_nextnum;

  END;

  --Выполнить проверку вида льготы при переводе в статус "Введена в действие"
  FUNCTION CheckBenefitToPutStatus(p_BenefitID IN NUMBER) RETURN VARCHAR2
  AS
    v_ErrStr VARCHAR2(200) := CHR(1);
    v_cnt NUMBER := 0;
  BEGIN

    --Если у льготы нет условий, то нельзя перевести в этот статус
    SELECT Count(1)
      INTO v_cnt
      FROM DNPTXBENEFITSCONDITIONS_DBT
     WHERE t_BenefitID = p_BenefitID;

    IF v_cnt = 0 THEN
      v_ErrStr := 'Невозможно перевести в статус "Введена в действие". У вида льготы отсутствуют условия.';
    END IF;

    IF v_ErrStr = CHR(1) THEN
      --Если есть другая действующая в этот же период льгота хотя бы с одним аналогичным условием, то вернуть ошибку
      FOR one_rec IN (SELECT benefit1.t_BenefitType
                        FROM DNPTXBENEFITS_DBT benefit, DNPTXBENEFITSCONDITIONS_DBT benecond, DNPTXBENEFITS_DBT benefit1, DNPTXBENEFITSCONDITIONS_DBT benecond1
                       WHERE benefit.t_BenefitID = p_BenefitID
                         AND benefit1.t_BenefitID != benefit.t_BenefitID
                         AND benefit1.t_Status = 2 /*Введена в действие*/
                         AND benefit.t_BegDateBenefit <= benefit1.t_EndDateBenefit
                         AND benefit.t_EndDateBenefit >= benefit1.t_BegDateBenefit
                         AND benecond.t_BenefitID = benefit.t_BenefitID
                         AND benecond1.t_BenefitID = benefit1.t_BenefitID
                         AND benecond1.t_KindDealType        = benecond.t_KindDealType       
                         AND benecond1.t_DealType            = benecond.t_DealType           
                         AND benecond1.t_DealObject          = benecond.t_DealObject         
                         AND benecond1.t_ResidentStatus      = benecond.t_ResidentStatus     
                         AND benecond1.t_OwnerPeriodSign     = benecond.t_OwnerPeriodSign    
                         AND benecond1.t_OwnerPeriod         = benecond.t_OwnerPeriod        
                         AND benecond1.t_IsExcept            = benecond.t_IsExcept           
                         AND benecond1.t_IssuerCountry       = benecond.t_IssuerCountry      
                         AND benecond1.t_BenefitSignOnFi     = benecond.t_BenefitSignOnFi    
                         AND benecond1.t_Circulate           = benecond.t_Circulate
                         AND benecond1.t_BenefitSignOnIssuer = benecond.t_BenefitSignOnIssuer 
                         AND benecond1.t_BenefitSignOnClient = benecond.t_BenefitSignOnClient
                         AND benecond1.t_DealDateSign        = benecond.t_DealDateSign       
                         AND benecond1.t_DealDate            = benecond.t_DealDate           
                         AND benecond1.t_SpecialCondition    = benecond.t_SpecialCondition   
                   )
      LOOP
        v_ErrStr := 'Невозможно перевести в статус "Введена в действие". У действующей в такой же период льготы '||one_rec.t_BenefitType||' есть аналогичное условие.';
        EXIT;
      END LOOP;

    END IF;

    RETURN v_ErrStr;
  END;


END RSI_NPTXBENEFIT;
/
