CREATE OR REPLACE PACKAGE RSB_DL_PFICreditRisk
IS

  --Номер польз. категории на элементе справочника артикулов "Группа артикулов", где значение 1 - "Электричество", 2 - "Нефтегазовая отрасль", 3 - "Сельское хозяйство", 4 - "Металл", 5 - "Прочие"
  DL_CATEGORY_ARTICLEGROUP CONSTANT NUMBER := 101; 
  
  --Номер польз. категорий на договоре CSA в ФИССиКО (объект 152 "Соглашение CSA")
  DVCSA_CATEGORY_MARGINKIND CONSTANT NUMBER  := 102; --"Вид маржирования", где значение 1 - "Одностороннее", 2 - "Двухстороннее"
  DVCSA_CATEGORY_MARGINPAYER CONSTANT NUMBER := 103; --"Плательщик маржевых сумм", где значение 1 - "Банк", 2 - "Контрагент", 3 - "Оба"
  
  --Номера польз. примечаний на договоре CSA в ФИССиКО (объект 152 "Соглашение CSA")
  DVCSA_NOTE_MARGINPERIOD CONSTANT NUMBER      := 105; --"Периодичность маржирования" (типа STRING) 
  DVCSA_NOTE_MARGINFACTNUMDAYS CONSTANT NUMBER := 106; --"Кол-во рабочих дней между перечислением маржи и переоценкой обеспечения" (типа STRING) 
  
  --Номера польз. примечаний на сделке ФИССиКО (объект 145 "Внебиржевая операция с ПИ")
  DVNDEAL_NOTE_NETCOLLATERALSUM CONSTANT NUMBER  := 401; --"Чистое обеспечение" (типа MONEY) 
  DVNDEAL_NOTE_NETCOLLATERALCURR CONSTANT NUMBER := 402; --"Код валюты чистого обеспечения" (типа STRING)
 
  --Номера польз. примечаний на сделке БО ЦБ (объект 101 "Сделка с ценными бумагами")
  SC_NOTE_CSACODE CONSTANT NUMBER           := 401; --"Договор CSA" (типа STRING)
  SC_NOTE_NETCOLLATERALSUM CONSTANT NUMBER  := 402; --"Чистое обеспечение" (типа MONEY) 
  SC_NOTE_NETCOLLATERALCURR CONSTANT NUMBER := 403; --"Код валюты чистого обеспечения" (типа STRING)
  
  --Номера польз. категорий на контрагенте по сделке
  CONTRACTOR_RATING_NATIONAL CONSTANT NUMBER := 122; --"Рейтинг субъектов в нац валюте для отчетности"
  CONTRACTOR_RATING_FOREIGN  CONSTANT NUMBER := 123; --"Рейтинг субъектов в ин валюте для отчетности"
  
  --Номер польз. категории на стране контрагента "Рейтинг страны в национальной валюте"
  COUNTRY_RATING_NATIONAL CONSTANT NUMBER    := 101;
  
  STANDARTPOORS_RATING_PARENTID CONSTANT NUMBER       := 110; --StandartPoors 
  MOODYS_RATING_PARENTID CONSTANT NUMBER              := 210; --Moodys 
  FITCHRATINGS_RATING_PARENTID CONSTANT NUMBER        := 310; --Fitch Ratings
  EXPRA_RATING_PARENTID CONSTANT NUMBER               := 367; --Эксперт РА 
  NRA_RATING_PARENTID CONSTANT NUMBER                 := 379; --НРА
  AKRA_RATING_PARENTID CONSTANT NUMBER                := 436; --АКРА  
  STANDARTPOORS_3453Y_RATING_PARENTID CONSTANT NUMBER := 459; --StandartPoors в соответствии с Указанием 3453-У
  MOODYS_3453Y_RATING_PARENTID CONSTANT NUMBER        := 483; --Moodys в соответствии с Указанием 3453-У
  FITCHRATINGS_3453Y_RATING_PARENTID CONSTANT NUMBER  := 505; --Fitch Ratings в соответствии с Указанием 3453-У
  NCR_RATING_PARENTID CONSTANT NUMBER                 := 610; --НКР 
   
  --КУ, используемые для получения номинальной стоимости ПФИ внебиржевых сделок
  DVNDEAL_PLUS_CAT_NOMINAL  CONSTANT VARCHAR2(100) := '+Форвард, дрейф внебирж, +Форвард, ПФИ внебирж, +Форвард, прочие'; 
  DVNDEAL_MINUS_CAT_NOMINAL CONSTANT VARCHAR2(100) := '-Форвард, дрейф внебирж, -Форвард, ПФИ внебирж, -Форвард, прочие';
  --КУ, используемые для получения номинальной стоимости ПФИ позиций
  DVPOS_PLUS_CAT_NOMINAL  CONSTANT VARCHAR2(100)   := '+Форвард, дрейф, +Форвард, дрейф1, +Форвард, ПФИ, +Форвард, ФО';
  DVPOS_MINUS_CAT_NOMINAL CONSTANT VARCHAR2(100)   := '-Форвард, дрейф, -Форвард, дрейф1, -Форвард, ПФИ, -Форвард, ФО';
  --КУ, используемые для получения номинальной стоимости ПФИ сделок БО ЦБ
  SC_PLUS_CAT_NOMINAL  CONSTANT VARCHAR2(100)      := '+Форвард, дрейф, +Форвард, прочие, +Форвард, ОИ';
  SC_MINUS_CAT_NOMINAL CONSTANT VARCHAR2(100)      := '-Форвард, дрейф, -Форвард, прочие, -Форвард, ОИ';
    
  --Структура для хранения условий отбора рейтингов кредитного риска
  TYPE INRatingID_t IS TABLE OF VARCHAR2(32);
    
  /**
  * Функция получения остатка на счете на определенную дату*/
  FUNCTION GetAccRestOnDate( pDocID IN NUMBER,       --Идентификатор документа
                             pDocKind IN NUMBER,     --Вид документа
                             pRestDate IN DATE,      --Дата, на которую требуется получить остаток
                             pCatCodeStr IN VARCHAR2,--Список категорий учета
                             pNotEmpty IN NUMBER,    --Признак того, что остаток по счету должен быть ненулевой
                             pOnFI IN NUMBER,        --Валюта счёта, если она известна (применяется для определения счета по части свопа)
                             pToFI OUT NUMBER,       --Валюта, в которой найден остаток
                             pToAcc OUT VARCHAR2     --Счет, по которому найден остаток
                           ) 
   RETURN NUMBER;
  
    /**
  * Функция получения остатка на счете на определенную дату по позиции*/
  FUNCTION GetAccRestOnDatePos( pFiid IN NUMBER,       --Идентификатор фин инструмента
                             pRestDate IN DATE,      --Дата, на которую требуется получить остаток
                             pCatCodeStr IN VARCHAR2,--Список категорий учета
                             pNotEmpty IN NUMBER,    --Признак того, что остаток по счету должен быть ненулевой
                             pOnFI IN NUMBER,        --Валюта счёта, если она известна (применяется для определения счета по части свопа)
                             pToFI OUT NUMBER,       --Валюта, в которой найден остаток
                             pToAcc OUT VARCHAR2     --Счет, по которому найден остаток
                           ) 
   RETURN NUMBER;
   
  /**
  * Функция получает числовой код валюты по ее ID*/
  FUNCTION GetFI_Code( pFIID NUMBER ) RETURN VARCHAR2;

  /**
  * Функция получает ID валюты по ее числовому коду, полученнному из примечания*/  
  FUNCTION GetFIIDbyCode( pFI_Code VARCHAR2 ) RETURN NUMBER;
 
  /**
  * Функция определяет ID контрагента для сделок БО ЦБ*/ 
  FUNCTION GetContractorID( p_IsExchange IN NUMBER, p_PARTYID IN NUMBER, p_MARKETID IN NUMBER, p_BROKERID IN NUMBER ) RETURN NUMBER;
  
  /**
  * Функция определяет рейтинг контрагента/страны контрагента по сделке*/
  FUNCTION GetRatingRep( pObjType IN NUMBER, pObjID IN NUMBER, pNoteKind IN NUMBER, pParentId IN NUMBER, pRatingDate IN DATE) RETURN VARCHAR2;
    
  /**
 * Процедура формирования данных отчёта для внебиржевых сделок ФИССиКО */
  PROCEDURE CreateData_DV_NDEAL( OnDate IN DATE );
  
  /**
  * Процедура формирования данных отчёта для позиций ФИССиКО */
  PROCEDURE CreateData_DV_POS( OnDate IN DATE );
  
  /**
  * Процедура формирования данных отчёта для сделок БО ЦБ */
  PROCEDURE CreateData_SC( OnDate IN DATE );

END RSB_DL_PFICreditRisk;
/
