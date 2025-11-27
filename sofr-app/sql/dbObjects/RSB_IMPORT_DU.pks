CREATE OR REPLACE PACKAGE RSB_IMPORT_DU
IS
    DealCodeTempl   CONSTANT VARCHAR2 (15) := 'ARNU_DU_';

    gl_DateDU                VARCHAR2 (15);

    /**
     @brief Получить код сделки для вставки
     @param[in] pDealNum Номер сделки
     @return Код сделки для вставки
    */
    FUNCTION GetDealCode (pDealNum IN NUMBER)
        RETURN VARCHAR2
        DETERMINISTIC;

    /**
     @brief Получить идентификатор сделки из СОФР
     @param[in] ISIN
     @param[in] DateImportDU Дата импорта в формате ddmmyyyy (Строка)
     @return Идентификатор сделки
    */
    FUNCTION GetDealIDSOFR (ISIN IN VARCHAR2, ImportID IN VARCHAR2, DateImportDU IN VARCHAR2)
        RETURN NUMBER
        DETERMINISTIC;

    /**
     @brief Получить сумму НКД на дату сделки
     @param[in] NKD НКД
     @param[in] CFI Финансовый инструмент
     @param[in] DealDate Дата сделки
    */
    FUNCTION GetSUMNDKCFI ( NKD IN NUMBER, NKDFIID IN NUMBER, CFI IN NUMBER, DealDate IN DATE)
        RETURN NUMBER;

    /**
     @brief Подгрузить данные из СОФР
     @param[in] DateImportDU Дата импорта в формате ddmmyyyy
    */
    PROCEDURE pumpDataSOFR (DateImportDU IN VARCHAR2);

    /**
     @brief Обновить данные в СОФР по сделкам ДУ
    */
    PROCEDURE updateSOFRdeals;

    /**
     @brief Запустить процедуру расчета связей
    */
    PROCEDURE TXCreateLotsForDUImport;
END;
/