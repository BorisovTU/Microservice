CREATE OR REPLACE PACKAGE RSB_GTIM_CSV
IS

-- ФУНКЦИИ ДЛЯ ОБРАБОТКИ ИНФОРМАЦИИ ПО СДЕЛКАМ

/**
 * Очистка временной таблицы сделок F04 и O04
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */         
  FUNCTION DelFO04(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;
    
 /**
 * Заполнение временной таблицы по данным F04 на основании Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */     
  FUNCTION FillFromF04(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;

 /**
 * Заполнение временной таблицы по данным O04 на основании Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */     
  FUNCTION FillFromO04(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;
    
/**
 * Обновление ZRID загруженных сделок из F04 в таблице Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */     
  FUNCTION UpdateF04_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Обновление ZRID загруженных сделок из O04 в таблице Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */     
  FUNCTION UpdateO04_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Обновление информации по объектам шлюза и кодам сделок F04 и O04
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_DVKind Вид ПИ
 * @param p_PrmStr Прочие параметры передаваемые строкой (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */            
  FUNCTION UpdateObjCodeFO04(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_PrmStr IN VARCHAR2)
     RETURN NUMBER;
    
/**
 * Создание объектов записей репликаций по данным временной таблицы FO04
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @param p_DVKind Вид ПИ
 * @param p_PrmStr Прочие параметры передаваемые строкой (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_FO04(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_Time IN DATE, p_CalcDate IN DATE, p_PrmStr IN VARCHAR2)
    RETURN NUMBER;
    
-- ФУНКЦИИ ДЛЯ ОБРАБОТКИ ИТОГОВ ПО ПОЗИЦИЯМ 
  
/**
 * Очистка временной таблицы итогов по позициям FPOS и OPOS
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */         
  FUNCTION DelFOPOS(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;
    
 /**
 * Заполнение временной таблицы по данным FPOS на основании Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */     
  FUNCTION FillFromFPOS(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;
    
 /**
 * Заполнение временной таблицы по данным OPOS на основании Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */     
  FUNCTION FillFromOPOS(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;
    
/**
 * Обновление ZRID загруженных итогов позиций с ПИ из FPOS в таблице Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */     
  FUNCTION UpdateFPOS_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Обновление ZRID загруженных итогов позиций с ПИ из OPOS в таблице Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */     
  FUNCTION UpdateOPOS_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Обновление информации по объектам шлюза и кодам итогов по позициям FPOS и OPOS
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_DVKind Вид ПИ
 * @param p_PrmStr Прочие параметры передаваемые строкой (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */         
  FUNCTION UpdateObjCodeFOPOS(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_PrmStr IN VARCHAR2) 
     RETURN NUMBER;
    
/**
 * Создание объектов записей репликаций по данным временной таблицы FOPOS
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @param p_DVKind Вид ПИ
 * @param p_PrmStr Прочие параметры передаваемые строкой (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_FOPOS(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_PrmStr IN VARCHAR2)
    RETURN NUMBER;
  
  -- ФУНКЦИИ ДЛЯ ОБРАБОТКИ ИНФОРМАЦИИ ПО ЗАЯВКАМ  
    
/**
 * Очистка временной таблицы заявок FORDLOG и OORDLOG
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */         
  FUNCTION DelFOORDLOG(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;
    
 /**
 * Заполнение временной таблицы по данным FORDLOG на основании Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */     
  FUNCTION FillFromFORDLOG(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;
    
 /**
 * Заполнение временной таблицы по данным OORDLOG на основании Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */     
  FUNCTION FillFromOORDLOG(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;
    
    
/**
 * Обновление ZRID загруженных заявок из FORDLOG в таблице Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */     
  FUNCTION UpdateFORDLOG_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Обновление ZRID загруженных заявок из OORDLOG в таблице Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */     
  FUNCTION UpdateOORDLOG_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Обновление информации по объектам шлюза и кодам заявок FORDLOG и ORDLOG
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_DVKind Вид ПИ
 * @param p_PrmStr Прочие параметры передаваемые строкой (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */         
  FUNCTION UpdateObjCodeFOORDLOG(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_PrmStr IN VARCHAR2) 
     RETURN NUMBER;
     
/**
 * Создание объектов записей репликаций по данным временной таблицы FOORDLOG
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @param p_DVKind Вид ПИ
 * @param p_PrmStr Прочие параметры передаваемые строкой (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_FOORDLOG(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_PrmStr IN VARCHAR2)
    RETURN NUMBER;
    
-- ФУНКЦИИ ДЛЯ ОБРАБОТКИ ИНФОРМАЦИИ ПО ПРОИЗВОДНЫМ ИНСТРУМЕНТАМ  
    
/**
 * Очистка временной таблицы итогов f07 и o07
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_IsPFI Загружается ли новый ПИ. 1 - да, 0 - нет
 * @return Код ошибки
 */         
  FUNCTION DelFO07(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_IsPFI NUMBER)
    RETURN NUMBER;
    
   /**
 * Заполнение временной таблицы по данным f07 на основании Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @param p_IsPFI Загружается ли новый ПИ. 1 - да, 0 - нет
 * @return Код ошибки
 */     
  FUNCTION FillFromF07(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE, p_IsPFI NUMBER)
    RETURN NUMBER;
    
 /**
 * Заполнение временной таблицы по данным o07 на основании Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @param p_IsPFI Загружается ли новый ПИ. 1 - да, 0 - нет
 * @return Код ошибки
 */     
  FUNCTION FillFromO07(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE, p_IsPFI NUMBER)
    RETURN NUMBER;
    
/**
 * Обновление ZRID и IDZR2 загруженных итогов из f07 в таблице Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_IsPFI Загружается ли новый ПИ. 1 - да, 0 - нет
 * @return Код ошибки
 */     
  FUNCTION UpdateF07_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_isPFI IN NUMBER)
    RETURN NUMBER;

/**
 * Обновление ZRID и IDZR2 загруженных итогов из o07 в таблице Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_IsPFI Загружается ли новый ПИ. 1 - да, 0 - нет
 * @return Код ошибки
 */     
  FUNCTION UpdateO07_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_isPFI IN NUMBER)
    RETURN NUMBER;
    
/**
 * Обновление информации по объектам шлюза и кодам итогов f07 и o07
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_DVKind Вид ПИ
 * @param p_IsPFI Загружается ли новый ПИ. 1 - да, 0 - нет
 * @param p_PrmStr Прочие параметры передаваемые строкой (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */            
  FUNCTION UpdateObjCodeFO07(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_isPFI IN NUMBER, p_PrmStr IN VARCHAR2)
     RETURN NUMBER;
          
/**
 * Создание объектов записей репликаций по данным временной таблицы FO07
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_ImpDate Дата импорта
 * @param p_SourceCode Код источника
 * @param p_DVKind Вид ПИ
 * @param p_IsPFI Загружается ли новый ПИ. 1 - да, 0 - нет
 * @param p_PrmStr Прочие параметры передаваемые строкой (например 'prm1=15;prm2=дата2')
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_FO07(p_SeanceID IN NUMBER, p_ImpDate IN DATE, p_SourceCode IN VARCHAR2, p_DVKind IN NUMBER, p_isPFI IN NUMBER, p_PrmStr IN VARCHAR2)
    RETURN NUMBER;

-- ФУНКЦИИ ДЛЯ ОБРАБОТКИ ИНФОРМАЦИИ ПО КОМИССИЯМ И СБОРАМ

/**
 * Очистка временной таблицы комиссий и сборов PAY
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */         
  FUNCTION DelPAY(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;
    
 /**
 * Заполнение временной таблицы по данным PAY на основании Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */     
  FUNCTION FillFromPAY(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;

/**
 * Обновление ID уже существующих объектов во временной таблице PAY
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */            
  FUNCTION UpdateObjPAY(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
     RETURN NUMBER;

/**
 * Обновление ZRID загруженных комиссий и сборов из PAY в таблице Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */     
  FUNCTION UpdatePAY_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Создание объектов записей репликаций по данным временной таблицы PAY
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_PAY(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;
    
-- ФУНКЦИИ ДЛЯ ОБРАБОТКИ ИНФОРМАЦИИ О ДОГОВОРАХ ОБСЛУЖИВАНИЯ

/**
 * Очистка временной таблицы информации о договорах обслуживания MON
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */         
  FUNCTION DelMON(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;
    
 /**
 * Заполнение временной таблицы по данным MON на основании Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @param p_Beg_date Дата начала отбора
 * @param p_End_date Дата окончания отбора
 * @return Код ошибки
 */     
  FUNCTION FillFromMON(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2, p_Beg_date IN DATE, p_End_date IN DATE)
    RETURN NUMBER;
    
/**
 * Обновление ID уже существующих объектов во временной таблице MON
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */            
  FUNCTION UpdateObjMON(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
     RETURN NUMBER;

/**
 * Обновление ZRID загруженной информации о договорах обслуживания из MON в таблице Payments
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @param p_Synonim Название схемы Payments
 * @return Код ошибки
 */     
  FUNCTION UpdateMON_PM(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2, p_Synonim IN VARCHAR2)
    RETURN NUMBER;
    
/**
 * Создание объектов записей репликаций по данным временной таблицы MON
 * @qtest NO
 * @param p_SeanceID Идентификатор сеанса
 * @param p_SourceCode Код источника
 * @return Код ошибки
 */
  FUNCTION CreateReplRecByTmp_MON(p_SeanceID IN NUMBER, p_SourceCode IN VARCHAR2)
    RETURN NUMBER;

END RSB_GTIM_CSV;
/