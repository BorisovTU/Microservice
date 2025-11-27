CREATE OR REPLACE PACKAGE RSI_NPTXBENEFIT
IS

  --Получить значение поля "Объект сделки" для скроллинга условий льготы
  FUNCTION PumpDealObjectBeneCond(p_DealObject IN NUMBER) RETURN VARCHAR2;

  --Получить значение поля "Тип сделки форм. дох." для скроллинга условий льготы
  FUNCTION PumpDealTypeBeneCond(p_KindDealType IN NUMBER, p_DealType IN NUMBER) RETURN VARCHAR2;

  --Получить значение поля "Налоговый статус ФЛ" для скроллинга условий льготы
  FUNCTION PumpResidentStatusBeneCond(p_ResidentStatus IN NUMBER) RETURN VARCHAR2;

  --Получить значение поля "Срок владения" для скроллинга условий льготы
  FUNCTION PumpOwnerPeriodBeneCond(p_OwnerPeriodSign IN VARCHAR2, p_OwnerPeriod IN NUMBER) RETURN VARCHAR2;

  --Получить значение поля "Условие для страны" для скроллинга условий льготы
  FUNCTION PumpIssuerCountryCondBeneCond(p_IsExcept IN CHAR) RETURN VARCHAR2;

  --Получить значение поля "Страна эмитент" для скроллинга условий льготы
  FUNCTION PumpIssuerCountryBeneCond(p_IssuerCountry IN VARCHAR2) RETURN VARCHAR2;

  --Получить значение поля "Признак льготы (актив)" для скроллинга условий льготы
  FUNCTION PumpBenefitSignOnFiBeneCond(p_BenefitSignOnFi IN NUMBER) RETURN VARCHAR2;

  --Получить значение поля "Обращаемость ц/б" для скроллинга условий льготы
  FUNCTION PumpCirculateBeneCond(p_Circulate IN NUMBER) RETURN VARCHAR2;

  --Получить значение поля "Признак экономически значимой организации на эмитенте" для скроллинга условий льготы
  FUNCTION PumpBenefitSignOnIssuerBeneCond(p_BenefitSignOnIssuer IN NUMBER) RETURN VARCHAR2;

  --Получить значение поля "Признак льготы (ФЛ)" для скроллинга условий льготы
  FUNCTION PumpBenefitSignOnClientBeneCond(p_BenefitSignOnClient IN NUMBER) RETURN VARCHAR2;

  --Получить значение поля "Дата приобретения актива" для скроллинга условий льготы
  FUNCTION PumpDealDateBeneCond(p_DealDateSign IN VARCHAR2, p_DealDate IN DATE) RETURN VARCHAR2;

  --Получить значение поля "Особое условие" для скроллинга условий льготы
  FUNCTION PumpSpecialConditionBeneCond(p_SpecialCondition IN NUMBER) RETURN VARCHAR2;

  --Сгенерировать номер вида льготы
  FUNCTION GenerateBenefitType RETURN VARCHAR2;

  --Проверить номер вида льготы на соответствие шаблону
  FUNCTION CheckBenefitType(p_BenefitType IN VARCHAR2) RETURN NUMBER;

  --Получить наименьшее неиспользуемое значение приоритета льгот
  FUNCTION GetNextMinBenefitPriority RETURN NUMBER;

  --Выполнить проверку вида льготы при переводе в статус "Введена в действие"
  FUNCTION CheckBenefitToPutStatus(p_BenefitID IN NUMBER) RETURN VARCHAR2;

END RSI_NPTXBENEFIT;
/
