CREATE OR REPLACE PACKAGE rsb_NUrep
IS

/**
 * Формирует данные во временную таблицу для отчета НУНП
 * @since 6.20.031.52
 * @qtest NO
 * @param pBegDate Дата начала
 * @param pEndDate Дата окончания
 * @param pSessionID Идентификатор сессии
 * @param pReqID Идентификатор прогресса для веб
 */
  PROCEDURE CreateNUNPData( pBegDate IN DATE,
                            pEndDate IN DATE,
                            pSessionID IN NUMBER,
                            pReqID IN VARCHAR2 DEFAULT NULL );

/**
 * Формирует данные в постоянную таблицу для отчета НУСВОД
 * @since 6.20.031.52
 * @qtest NO
 * @param pBegDate Дата начала
 * @param pEndDate Дата окончания
 * @param pSessionID Идентификатор сессии
 * @param pParallelLevel Уровень распараллеливания
 * @param pReqID Идентификатор прогресса для веб
 */
  PROCEDURE CreateNU_SVOD_Data( pBegDate IN DATE,
                                pEndDate IN DATE,
                                pSessionID IN NUMBER,
                                pParallelLevel IN NUMBER,
                                pReqID IN VARCHAR2 DEFAULT NULL );

/**
 * Формирует данные в постоянную таблицу для отчета НУСВОД
 * @since 6.20.031.52
 * @qtest NO
 * @param pFIID Идентификатор ФИ
 * @param pSessionID Идентификатор сессии
 * @param pBegDate Дата начала
 * @param pEndDate Дата окончания
 * @param pReqID Идентификатор прогресса для веб
 * @param pIsParallel Признак запуска пакетом DBMS_PARALLEL
 */
  PROCEDURE ProcessFI_SVOD(pFIID IN NUMBER,
                           pSessionID IN NUMBER,
                           pBegDate IN DATE,
                           pEndDate IN DATE,
                           pReqID IN VARCHAR2 DEFAULT NULL,
                           pIsParallel IN NUMBER DEFAULT 1);

/**
 * Запустить стандартный (из RsFloatingWindow) индикатор прогресса веб (использует COMMIT !!!)
 * @since 6.20.031.52
 * @qtest NO
 * @param reqId идентификатор сессии
 * @param maxValue количество записей
 * @param text заголовок
 */
  PROCEDURE WebProgressIndicator_Start(reqId IN VARCHAR2,
                                       maxValue IN NUMBER,
                                       text IN VARCHAR2);

/**
 * Обновить стандартный (из RsFloatingWindow) индикатор прогресса веб (использует COMMIT !!!)
 * @since 6.20.031.52
 * @qtest NO
 * @param curValue количество обработанных записей
 * @param maxValue количество записей
 * @param text заголовок
 */
  PROCEDURE WebProgressIndicator_Update(curValue IN NUMBER,
                                        maxValue IN NUMBER DEFAULT 0,
                                        text IN VARCHAR2 DEFAULT '');

/**
 * Увеличить значение стандартного (из RsFloatingWindow) индикатора прогресса веб (использует COMMIT !!!)
 * @since 6.20.031.52
 * @qtest NO
 * @param reqId идентификатор сессии
 */
  PROCEDURE WebProcessState_IncreaseByReqID(reqId IN VARCHAR2);

/**
 * Остановить стандартный (из RsFloatingWindow) индикатор прогресса веб (использует COMMIT !!!)
 * @since 6.20.031.52
 * @qtest NO
 */
  PROCEDURE WebProgressIndicator_Stop;

END rsb_NUrep;
/
