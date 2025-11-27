CREATE OR REPLACE PACKAGE RSB_NDFLREG
IS

    NPTXPIT6_2021 CONSTANT INTEGER := 1; -- Отчет 6-НДФЛ 2021
    NPTXPIT6_2022 CONSTANT INTEGER := 2; -- Отчет 6-НДФЛ 2022
    NPTXPIT6_2023 CONSTANT INTEGER := 3; -- Отчет 6-НДФЛ 2023
    NPTXPIT6_2024 CONSTANT INTEGER := 4; -- Отчет 6-НДФЛ 2024

    /**
     @brief Удалить срез данных
     @param[in] pSliceNumDel номер среза
     @param[in] pBeginDate дата начала среза
     @param[in] pEndDate дата окончания среза
     @return 0 - ошибка удаления среза
     @return 1 - успешное удаление среза
    */
    FUNCTION delSlice (pSliceNumDel   IN NUMBER,
                       pBeginDate     IN DATE,
                       pEndDate       IN DATE)
        RETURN NUMBER;

    /**
     @brief Вставить срез данных
     @param[in] pSliceNum номер среза
     @param[in] pBeginDate дата начала среза
     @param[in] pEndDate дата окончания среза
     @param[in] pReportID идентификатор отчета
    */
    PROCEDURE insertSlice (pSliceNum      IN NUMBER,
                           pBeginDate     IN DATE,
                           pEndDate       IN DATE,
                           pReportID      IN NUMBER);

END RSB_NDFLREG;
/