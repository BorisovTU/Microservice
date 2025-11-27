CREATE OR REPLACE PACKAGE TRGPCKG_DDLGRDEAL_DBT_TAI IS

  v_ID_Operation  NUMBER := 0;
  v_ID_Step       NUMBER := 0;
  v_State         NUMBER := -1;

END TRGPCKG_DDLGRDEAL_DBT_TAI;
/

CREATE OR REPLACE PACKAGE RSI_DLGR AS

/**
 * Виды статусов видов учета по графику 
 */
       DLGRACC_STATE_NOTNEED  CONSTANT NUMBER := 0; --Не требуется (Н)
       DLGRACC_STATE_PLAN     CONSTANT NUMBER := 1; --Планируется (П)
       DLGRACC_STATE_FACTEXEC CONSTANT NUMBER := 2; --Фактически выполнен (Ф)

/**
 * Системные виды учета по графику 
 */
       DLGR_ACCKIND_BACKOFFICE  CONSTANT NUMBER := 1; --Бэк-офисный учет
       DLGR_ACCKIND_ACCOUNTING  CONSTANT NUMBER := 2; --Бухгалтерский учет
       DLGR_ACCKIND_INNER       CONSTANT NUMBER := 3; --Внутренний учет
       DLGR_ACCKIND_CUSTODY     CONSTANT NUMBER := 4; --Депозитарный учет
       DLGR_ACCKIND_REPOSITORY  CONSTANT NUMBER := 5; --Репозитарный учет
/**
 * Виды записей в DL_VALUE
 */
       
       DL_VALUE_KIND_PLUSTP         CONSTANT NUMBER := 1; -- +Переоценка, ц/б (ТП) // полож. п/о в ТП
       DL_VALUE_KIND_MINUSTP        CONSTANT NUMBER := 2; -- -Переоценка, ц/б (ТП) // отриц. п/о в ТП
       DL_VALUE_KIND_PLUSPPR        CONSTANT NUMBER := 3; -- +Переоценка, ц/б (ППР) // полож. п/о в ППР
       DL_VALUE_KIND_MINUSPPR       CONSTANT NUMBER := 4; -- -Переоценка, ц/б (ППР) // отриц. п/о в ППР
       DL_VALUE_KIND_PLUSDK         CONSTANT NUMBER := 5; -- - ПО ДК // отриц. разн. в добав. капитале
       DL_VALUE_KIND_MINUSDK        CONSTANT NUMBER := 6; -- + ПО ДК // полож. разн. в добав. капитале
       DL_VALUE_KIND_MINUSEXCHANGE  CONSTANT NUMBER := 7; -- - Биржа
       DL_VALUE_KIND_PLUSEXCHANGE   CONSTANT NUMBER := 8; -- + Биржа
       DL_VALUE_KIND_PLUSDEALTOVER  CONSTANT NUMBER := 9; -- Переоценка по сделке Т+ на счете "+ Переоценка ц/б"
       DL_VALUE_KIND_MINUSDEALTOVER CONSTANT NUMBER := 10;--Переоценка по сделке Т+ на счете "- Переоценка ц/б"


/**
 * Системные виды шаблонов по графику 
 */
       DLGR_TEMPL_PREVCOSTACC          CONSTANT NUMBER := 1;  --Учет предварительных затрат
       DLGR_TEMPL_REPOOFFBALANCE       CONSTANT NUMBER := 2;  --Постановка на внебаланс РЕПО
       DLGR_TEMPL_OFFBALANCE           CONSTANT NUMBER := 3;  --Постановка на внебаланс
       DLGR_TEMPL_TRANSFER             CONSTANT NUMBER := 4;  --Перенос по срокам
       DLGR_TEMPL_DELAYEDPAY           CONSTANT NUMBER := 5;  --Отложить исполнение оплаты
       DLGR_TEMPL_DELAYEDDELIVERY      CONSTANT NUMBER := 6;  --Отложить исполнение поставки
       DLGR_TEMPL_CHANGE               CONSTANT NUMBER := 7;  --Изменение условий
       DLGR_TEMPL_BALANCE              CONSTANT NUMBER := 8;  --Постановка на баланс
       DLGR_TEMPL_PAYCOM               CONSTANT NUMBER := 9;  --Оплата комиссий
       DLGR_TEMPL_PAYCOMCONTR          CONSTANT NUMBER := 10; --Оплата комиссий клиента-контрагента
       DLGR_TEMPL_REJECT               CONSTANT NUMBER := 11; --Отказ от сделки
       DLGR_TEMPL_OVERDUEAVANCE        CONSTANT NUMBER := 12; --Вынос на просрочку аванса
       DLGR_TEMPL_PAYAVANCE            CONSTANT NUMBER := 13; --Оплата аванса\задатка
       DLGR_TEMPL_OVERDUEPAY           CONSTANT NUMBER := 14; --/Вынос на просрочку оплаты
       DLGR_TEMPL_PAYMENT              CONSTANT NUMBER := 15; --Оплата
       DLGR_TEMPL_OVERDUEDELIVERY      CONSTANT NUMBER := 16; --Вынос на просрочку поставки
       DLGR_TEMPL_DELIVERY             CONSTANT NUMBER := 17; --Поставка
       DLGR_TEMPL_DELIVERYNTG          CONSTANT NUMBER := 18; --Поставка-неттинг
       DLGR_TEMPL_DELIVERYCONTR        CONSTANT NUMBER := 19; --Поставка клиента-контрагента
       DLGR_TEMPL_RECDELIVERY          CONSTANT NUMBER := 20; --Учет поставки
       DLGR_TEMPL_COMPDELIVERY         CONSTANT NUMBER := 21; --Компенсационная поставка
       DLGR_TEMPL_COMPDELIVERYCONTR    CONSTANT NUMBER := 22; --Комп. поставка клиента сонтрагента
       DLGR_TEMPL_RECCOMPDELIVERY      CONSTANT NUMBER := 23; --Учет комп. поставки
       DLGR_TEMPL_COMPPAYMENT          CONSTANT NUMBER := 24; --Компенсационная оплата
       DLGR_TEMPL_COUP                 CONSTANT NUMBER := 25; --Учет купона
       DLGR_TEMPL_PARTREP              CONSTANT NUMBER := 26; --Учет частичного погашения
       DLGR_TEMPL_OVERDUEAVANCE2       CONSTANT NUMBER := 27; --Вынос на просрочку аванса 2ч
       DLGR_TEMPL_PAYAVANCE2           CONSTANT NUMBER := 28; --Оплата аванса по 2-й части РЕПО
       DLGR_TEMPL_OVERDUEPAY2          CONSTANT NUMBER := 29; --Вынос на просрочку оплаты 2ч
       DLGR_TEMPL_PROLONGPAY2          CONSTANT NUMBER := 30; --Пролонгация оплаты 2ч
       DLGR_TEMPL_PAYMENT2             CONSTANT NUMBER := 31; --Оплата 2-й части РЕПО
       DLGR_TEMPL_OVERDUEDELIVERY2     CONSTANT NUMBER := 32; --Вынос на просрочку поставки 2ч
       DLGR_TEMPL_PROLONGDELIVERY2     CONSTANT NUMBER := 33; --Пролонгация поставки 2ч
       DLGR_TEMPL_DELIVERY2            CONSTANT NUMBER := 34; --Поставка по 2-й части РЕПО
       DLGR_TEMPL_DELIVERYCONTR2       CONSTANT NUMBER := 35; --Поставка клиента-контрагента по 2-й части РЕПО
       DLGR_TEMPL_RECDELIVERY2         CONSTANT NUMBER := 36; --Учет поставки по 2-й части РЕПО
       DLGR_TEMPL_DEPODRAFT            CONSTANT NUMBER := 37; --Формирование поруч. депо по поставке                                      
       DLGR_TEMPL_DEPODRAFT2           CONSTANT NUMBER := 38; --Формирование поруч. депо по поставке 2 ч. РЕПО                            
       DLGR_TEMPL_DEPODRAFTCONTR       CONSTANT NUMBER := 39; --Формирование поручение по поставке клиенту(-ом)-контрагенту(-ом)          
       DLGR_TEMPL_DEPODRAFTCONTR2      CONSTANT NUMBER := 40; --Формирование поручение по поставке клиенту(-ом)-контрагенту(-ом) 2 ч. РЕПО
       DLGR_TEMPL_REJECT2              CONSTANT NUMBER := 41; --Отказ от 2ч сделки
       DLGR_TEMPL_PAYPC                CONSTANT NUMBER := 42; --Оплата %% займа
       DLGR_TEMPL_CHANGEMSG            CONSTANT NUMBER := 47; --Сообщение по изменению условий
       DLGR_TEMPL_CHANGEMSGWTHREQUEST  CONSTANT NUMBER := 49; --Сообщение по изменению условийс ож. запроса
       DLGR_TEMPL_CLOSECONTR           CONSTANT NUMBER := 51; --Закрытие договора
       DLGR_TEMPL_CLOSECONTRREQUEST    CONSTANT NUMBER := 52; --Закрытие договора с ож. запроса
       DLGR_TEMPL_EXECDELAYMSG         CONSTANT NUMBER := 53; --Сообщение о просрочке исполнения
       DLGR_TEMPL_EXECDELAYMSGREQUEST  CONSTANT NUMBER := 54; --Сообщение о просрочке исполнения с ож. запроса
       DLGR_TEMPL_EXECHOLDMSG          CONSTANT NUMBER := 55; --Сообщение о приостановке исполнения
       DLGR_TEMPL_EXECHOLDMSGREQUEST   CONSTANT NUMBER := 56; --Сообщение о приостановке исполнения с ож. запроса
       DLGR_TEMPL_EARLYEXECMSG         CONSTANT NUMBER := 57; --Сообщение о досрочном исполнении
       DLGR_TEMPL_EARLYEXECMSGREQUEST  CONSTANT NUMBER := 58; --Сообщение о досрочном исполнении с ож. запроса
       DLGR_TEMPL_REJECTIONMSG         CONSTANT NUMBER := 59; --Сообщение об отказе от сделки
       DLGR_TEMPL_REJECTIONMSGREQUEST  CONSTANT NUMBER := 60; --Сообщение об отказе от сделки с ож. запроса
       DLGR_TEMPL_NETTINGSUSP          CONSTANT NUMBER := 61; --Прекращение обязательств неттингом
       DLGR_TEMPL_NETTINGSUSPREQUEST   CONSTANT NUMBER := 62; --Прекращение обязательств неттингом с ож. запроса
       DLGR_TEMPL_MAKECONTRACT         CONSTANT NUMBER := 63; --Заключение договора
       DLGR_TEMPL_MAKECONTRACTREQUEST  CONSTANT NUMBER := 64; --Заключение договора с ож. запроса
       DLGR_TEMPL_DELIVERYOWN          CONSTANT NUMBER := 67; --Поставка ОЭБ
       DLGR_TEMPL_MOVEACC              CONSTANT NUMBER := 70; --Перенос на счета "к исполнению"
       DLGR_TEMPL_EXECOWN              CONSTANT NUMBER := 71; --Исполнение ОЭБ
       DLGR_TEMPL_MAKEDEAL_CORRECTION  CONSTANT NUMBER := 72; --Заключение сделки (корр. сообщ.)
       DLGR_TEMPL_PAYCOMOWN            CONSTANT NUMBER := 73; --Оплата комиссии ОЭБ
       
       DLGR_TEMPL_FAIRVALUE            CONSTANT NUMBER := 75; --Сообщение о Переоценке СС
       DLGR_TEMPL_CSA_MARGIN_PAYMENT   CONSTANT NUMBER := 76; --Сообщение о выплате маржевых сумм
       DLGR_TEMPL_CSA_OPEN             CONSTANT NUMBER := 77; --открытие CSA

/**
 * Статусы ошибок */
       GR_ERROR_20900    CONSTANT INTEGER := -20900; -- Ошибка последовательности отката изменений графика по сделке       
       GR_ERROR_20901    CONSTANT INTEGER := -20901; -- По графику сделки найдены исполненные действия. Откат невозможен
       GR_ERROR_20902    CONSTANT INTEGER := -20902; -- Есть запланированные неисполненные действия по учёту сделки, сделку закрывать нельзя
       GR_ERROR_20903    CONSTANT INTEGER := -20903; -- Не найден шаблон вида действия с номером %s
       GR_ERROR_20904    CONSTANT INTEGER := -20904; -- Неверные параметры: данные вида учёта по графику уже изменены на этом шаге
       GR_ERROR_20905    CONSTANT INTEGER := -20905; -- Ошибка отката изменения графика на шаге. Данные по виду учёта графика не найдены
       GR_ERROR_20906    CONSTANT INTEGER := -20906; -- При попытке отката изменения плановой даты, найдены выполненные дейстия по другим видам учёта строки графика
       GR_ERROR_20907    CONSTANT INTEGER := -20907; -- При попытке отката изменения статуса, найдены выполненные дейстия по другим видам учёта строки графика
       GR_ERROR_20908    CONSTANT INTEGER := -20908; -- Неверный запуск процедуры отката вставки строки графика
       GR_ERROR_20909    CONSTANT INTEGER := -20909; -- Ошибка отката вставки строки графика - есть исполненные действия
       GR_ERROR_20910    CONSTANT INTEGER := -20910; -- Существуют исполненные учётные действия в изменяемую дату
       GR_ERROR_20911    CONSTANT INTEGER := -20911; -- Нарушена последовательность отката суммы по ПД
       GR_ERROR_20912    CONSTANT INTEGER := -20912; -- Не найдена сумма по документу
       GR_ERROR_20913    CONSTANT INTEGER := -20913; -- По сделке найдено более одной строки графика одного вида к обработке
       GR_ERROR_20914    CONSTANT INTEGER := -20914; -- Не найдена сделка с ID =
       GR_ERROR_20915    CONSTANT INTEGER := -20915; -- Найдено несколько строк графика по оплате комиссии на дату
       GR_ERROR_20916    CONSTANT INTEGER := -20916; -- Существуют обработанные виды учёта по строке графика оплаты комиссии за дату. Для ввода комиссии необходимо откатить обработку комиссий за эту дату.
       GR_ERROR_20917    CONSTANT INTEGER := -20917; -- Уже выполнены проводки переоценки за более позднюю дату.
       GR_ERROR_20918    CONSTANT INTEGER := -20918; -- По сделке найдены записи в регистре переоценки. Откат невозможен.
       GR_ERROR_20919    CONSTANT INTEGER := -20919; -- Ошибка при изменении статуса вида учета: ранее это значение уже было установлено 

/**
 * Режимы обновления времени по сделки 
 */
       DLGRDEAL_TIMEMODE_Dz  CONSTANT NUMBER := 1; --Занесение в график даты заключения сделки
       DLGRDEAL_TIMEMODE_Dp  CONSTANT NUMBER := 2; --Занесение в график даты поставки сделки / 1ч сделки
       DLGRDEAL_TIMEMODE_Dp2 CONSTANT NUMBER := 3; --Занесение в график даты поставки 2ч сделки


/**
 * Виды плановых дат 
 */
       DLGR_DATEKIND_DELIVERY  CONSTANT NUMBER := 2; --Дата поставки по сделке или 1-й части РЕПО 
       DLGR_DATEKIND_DELIVERY2 CONSTANT NUMBER := 5; --Дата поставки по 2-й части РЕПО 
       DLGR_DATEKIND_EXECCALC  CONSTANT NUMBER := 11; --Дата исполнения операции расчеты по сделке 


/**
 * Типы источников документов по строкам графиков 
 */
   DLGR_SOURCETYPE_NORMAL       CONSTANT NUMBER := 0; --Обычная запись
   DLGR_SOURCETYPE_PAYCOM       CONSTANT NUMBER := 1; --Оплата комисий
   DLGR_SOURCETYPE_NETTING      CONSTANT NUMBER := 2; --Неттинг
   DLGR_SOURCETYPE_CALCEXCHANGE CONSTANT NUMBER := 3; --Расчеты на бирже
   DLGR_SOURCETYPE_DEPOSUMMARY  CONSTANT NUMBER := 4; --В результате формирования сводного поручения ДЕПО
   DLGR_SOURCETYPE_CARRYSUMMARY CONSTANT NUMBER := 5; --В результате формирования сводной проводки
   DLGR_SOURCETYPE_DLRQ         CONSTANT NUMBER := 6; --По ТО
   DLGR_SOURCETYPE_ENS          CONSTANT NUMBER := 13; --обеспечение (в РЕПО на корзину)

   PROCEDURE GetLastErrorMessage( ErrMes OUT VARCHAR2 );
   PROCEDURE SetError( ErrNum IN INTEGER, ErrMes IN VARCHAR2 DEFAULT NULL );

/**
 * Процедура вставки строки графика исполнения сделки на шаге операции
 */
   PROCEDURE RSI_InsertGrDeal( pDocKind IN NUMBER, pDocID IN NUMBER, pID_Operation IN NUMBER, pID_Step IN NUMBER, pTemplNum IN NUMBER, pDate IN DATE, pTime IN DATE DEFAULT TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS'), pFIID IN NUMBER, pGUID IN VARCHAR DEFAULT CHR(1) );

/**
 * Процедура вставки строки графика исполнения сделки вне шага операции с возвратом ID вставленной записи
 */
   PROCEDURE RSI_InsertGrDealRet( pDocKind IN NUMBER, pDocID IN NUMBER, pID_Operation IN NUMBER, pID_Step IN NUMBER, pTemplNum IN NUMBER, pDate IN DATE, pTime IN DATE DEFAULT TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS'), pFIID IN NUMBER, pGrDealID OUT NUMBER );

/**
 * Процедура отката вставки строки графика исполнения сделки на шаге операции
 */
   PROCEDURE RSI_BackInsertGrDeal( pDocKind IN NUMBER, pDocID IN NUMBER, pID_Operation IN NUMBER, pID_Step IN NUMBER, pTemplNum IN NUMBER, pFIID IN NUMBER);

/**
 * Процедура удаления строки графика исполнения сделки
 */
   PROCEDURE RSI_DeleteGrDeal( pDocKind IN NUMBER, pDocID IN NUMBER, pTemplNum IN NUMBER, pDate IN DATE );

/**
 * Процедура обновления информации о состоянии учёта по событию графика исполнения сделки, на шаге операции
 */
   PROCEDURE RSI_UpdateGrDealAcc( pDocKind IN NUMBER, 
                                  pDocID IN NUMBER, 
                                  pFIID IN NUMBER,
                                  pTemplNum IN NUMBER,
                                  pAccNum IN NUMBER,
                                  pID_Operation IN NUMBER,
                                  pID_Step IN NUMBER,
                                  pPlanDate IN DATE,
                                  pState IN NUMBER,
                                  pExecCommit IN NUMBER DEFAULT 1
                                );

/**
 * Процедура отката обновления информации о состоянии учёта по событию графика исполнения сделки
 */
   PROCEDURE RSI_BackUpdateGrDealAcc( pDocKind IN NUMBER, 
                                      pDocID IN NUMBER, 
                                      pFIID IN NUMBER,
                                      pTemplNum IN NUMBER,
                                      pAccNum IN NUMBER,
                                      pID_Operation IN NUMBER,
                                      pID_Step IN NUMBER
                                    );

/**
 * Процедура установки даты по сделке на шаге операции, с сохранением информации для отката
 */
   PROCEDURE RSI_SetDateGrDeal( pDocKind IN NUMBER, 
                                pDocID IN NUMBER, 
                                pID_Operation IN NUMBER,
                                pID_Step IN NUMBER,
                                pDateKind IN NUMBER,
                                pDate IN DATE
                              );

/**
 * Процедура отката изменения даты по сделке на шаге операции
 */
   PROCEDURE RSI_BackSetDateGrDeal( pDocKind IN NUMBER, 
                                    pDocID IN NUMBER, 
                                    pID_Operation IN NUMBER,
                                    pID_Step IN NUMBER,
                                    pDateKind IN NUMBER
                                  );

/**
 * Процедура вставки информации о проводке на шаге операции, с привязкой к графику
 */
   PROCEDURE RSI_SetDocGrDeal( pGrDealID IN NUMBER, 
                               pDocKind IN NUMBER,
                               pDocID IN NUMBER,
                               pServDocKind IN NUMBER,
                               pServDocID IN NUMBER,
                               pGrpID IN NUMBER DEFAULT 0,
                               pSourceType IN NUMBER DEFAULT 0,
                               pExecCommit IN NUMBER DEFAULT 1
                              );

/**
 * Процедура удаления информации о документе при откате шага операцииу
 */
   PROCEDURE RSI_BackDocGrDeal( pGrDealID IN NUMBER, 
                                pDocKind IN NUMBER,
                                pDocID IN NUMBER,
                                pServDocKind IN NUMBER,
                                pServDocID IN NUMBER,
                                pGrpID IN NUMBER DEFAULT 0,
                                pSourceType IN NUMBER DEFAULT 0
                               );

/**
 * Процедура обновления информации о состоянии учёта по событию графика исполнения сделки по конкретной строке графика, без привязки изменения к шагу операции
 */
  PROCEDURE RSI_UpdateGrDealAccByID( pGrDealID IN NUMBER, 
                                     pAccNum IN NUMBER,
                                     pState IN NUMBER,
                                     pPlanDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'),
                                     pExecCommit IN NUMBER DEFAULT 1
                                   );
/**
 * Процедура отката статус действия по графикам
 */
  PROCEDURE RSI_BackUpdateAllGrDealAcc( pDealID         IN NUMBER,
                                        pDocKind        IN NUMBER,
                                        pCommDocKind    IN NUMBER,
                                        pCommDocumentID IN NUMBER
                                      );

/**
 * Процедура отката обновления информации о состоянии учёта по событию графика исполнения сделки
 */
   PROCEDURE RSI_BackUpdateGrDealAccByID( pGrDealID IN NUMBER, 
                                          pAccNum IN NUMBER,
                                          pCanDelete IN NUMBER
                                        );                                   


/**
 * Обновить сумму по документу. Зовётся при выполнении сервисной операции БУ
 */
   PROCEDURE SetDLSUM(p_DocKind IN NUMBER,
                      p_DocID IN NUMBER,
                      p_Kind IN NUMBER,
                      p_Currency IN NUMBER,
                      p_Date IN DATE,
                      p_Sum IN NUMBER,
                      p_NDS IN NUMBER,
                      p_GrpID IN NUMBER,
                      p_FIID IN NUMBER
                     );

/**
 * Откат всех действий с DLSUM по группе
 */
  PROCEDURE BackSetDLSUM(p_GrpID IN NUMBER);

/**
 * Процедура установки времени по сделке на график исполнения сделки при редактировании времени в панели сделки
 */
  PROCEDURE RSI_SetTimeGrDeal(p_DocKind IN NUMBER, 
                              p_DocID IN NUMBER, 
                              p_Mode IN NUMBER, 
                              p_Time IN DATE 
                             );

/**
 * Проверить необходимость установки флага "Неттинг" в СО БУ
 */
  FUNCTION RSI_GetDefaultFlagNetting(p_CommDate DATE) RETURN NUMBER;


/**
 * Проверить необходимость установки флага "Клиентские комиссии за обороты" в СО БУ
 */
  FUNCTION RSI_GetDefaultFlagClientCom(p_CommDate DATE, p_ClientID NUMBER, p_ContractID NUMBER, p_IsExclude NUMBER) RETURN NUMBER;

/**
 * Проверить необходимость установки флага "Расчеты на бирже" в СО БУ
 */
  FUNCTION RSI_GetDefaultFlagCalcExchange(p_CommDate DATE, 
                                          p_ClientID NUMBER, 
                                          p_ContractID NUMBER, 
                                          p_AvoirKind NUMBER,
                                          p_FIID NUMBER,
                                          p_Currency NUMBER,
                                          p_IsExclude NUMBER) RETURN NUMBER;

/**
 * Получить номер шаблона графика по комиссии
 */        
  FUNCTION GetTemplNumByCom(pDocKind IN NUMBER, pDocID IN NUMBER, pCONTRACT IN NUMBER) RETURN NUMBER DETERMINISTIC;

/**
 * Получить, есть ли строки графика нужного статуса по комиссии
 */        
  FUNCTION CheckExistGrDealByCom(pDocKind IN NUMBER, pDocID IN NUMBER, pCONTRACT IN NUMBER, pPLANPAYDATE IN DATE, pState IN NUMBER, pTemplNum IN NUMBER DEFAULT 0) RETURN NUMBER DETERMINISTIC;

/**
 * Функция проверки наличия запланированных строк графика по бумаге до даты
 */
  FUNCTION RSI_ExistPlanGrDealBeforeDate(p_FIID IN NUMBER, p_Date IN DATE) RETURN NUMBER;

/**
 * Функция получения суммы регистра на дату
 */
  FUNCTION RSI_GetOverRegistrValue(p_FIID IN NUMBER, p_Kind IN NUMBER, p_Date IN DATE, p_SumFIID IN NUMBER, p_Account IN VARCHAR2) RETURN NUMBER;

/**
 * Процедура вставки строки регистра переоценки на шаге операции
 */
  PROCEDURE RSI_InsertDL_VALUE(p_DocKind IN NUMBER, 
                               p_DocID IN NUMBER,
                               p_Kind IN NUMBER,
                               p_Date IN DATE,
                               p_Sum IN NUMBER,
                               p_SumFIID IN NUMBER,
                               p_ID_Operation IN NUMBER,
                               p_ID_Step IN NUMBER,
                               p_GrpID IN NUMBER
                              );

/**
 * Откат вставки строки регистра переоценки на шаге операции
 */
  PROCEDURE RSI_RollbackInsertDL_VALUE(p_ID_Operation IN NUMBER,
                                       p_ID_Step IN NUMBER
                                      );

/**
 * Обновить статус графика сделки (DDLGRDEAL_DBT) в транзакции массового исполнения шагов операций. 
 * @since 6.20.031
 * @qtest NO
 * @param TemplNum виды шаблона по графику (DLGR_TEMPL_...)
 * @param AccNum вид учета по графику (DLGR_ACCKIND_...)
 * @param State  Устанавливаемый статус
 */
   PROCEDURE Mass_UpdateGrDealAcc( TemplNum IN NUMBER, AccNum IN NUMBER, State IN NUMBER );

/**
 * Обновить статус графика сделки (DDLGRDEAL_DBT) в транзакции массового исполнения шагов операций. Для шага исполнения операции меняется сразу оплата и поставка. 
 * @since 6.20.031
 * @qtest NO
 */
   PROCEDURE Mass_UpdateGrDealAcc_ExecOper( DealPart IN NUMBER );

/**
 * Функция получения части сделки по виду шаблона строки графика
 */
   FUNCTION RSI_GetDealPartByGrTemplNum(p_TemplNum IN NUMBER) RETURN NUMBER;


/**
 * Процедура сохранения в таблицах закешированных данных при обновлении статуса учета строк графика.
 */
   PROCEDURE RSI_ExecCommitDLGR;

END RSI_DLGR;
/
