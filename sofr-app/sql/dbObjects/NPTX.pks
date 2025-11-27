CREATE OR REPLACE PACKAGE NPTX
IS

/**
 * Определяет по виду сделки, является ли она продажей (выбытием). Для сделки из двух частей - по первой части.
 * @since 6.20.030
 * @qtest NO
 * @param p_Kind Вид сделки
 * @return 1 - да, 0 - нет
 */
    FUNCTION IsSale( p_Kind IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Определяет по типу сделки, является ли она виртуальной.
 * @since 6.20.030
 * @qtest NO
 * @param p_Type Тип сделки
 * @return 1 - да, 0 - нет
 */
    FUNCTION IsVirtual( p_Type IN NUMBER ) RETURN NUMBER DETERMINISTIC;

/**
 * Вызывается при сохранении/удалении операций NPTXOP
 * @since 6.20.031.52
 * @qtest NO
 * @param pMode Вид изменения (редактирование/удаление)
 * @param pDoc Буфер операции
 * @param pOldDoc Буфер операции до изменений (заполняется при редактировании)
 * @return stat
 */
  FUNCTION Check_Document( pMode IN NUMBER,
                           pDoc IN OUT NOCOPY DNPTXOP_DBT%ROWTYPE,
                           pOldDoc IN DNPTXOP_DBT%ROWTYPE ) RETURN NUMBER;

END NPTX;
/