CREATE OR REPLACE PACKAGE RSHB_VTB_LOADLOTNDFL
AS
    /**
    * Маппинг всех операций
    * @since 6.20.029
    * @qtest NO
    * @return Количество возникших ошибок при обработке
    */
    FUNCTION mapAll RETURN NUMBER;

    /**
    * Маппинг операции
    * @since 6.20.029
    * @qtest NO
    * @param p_ClientID Идентификатор клиента
    * @param p_OperNo   Номер операции
    * @param p_ID       Уникальный идентификатор
    * @return 0 - при успешной обработки 1 - при ошибке
    */
    FUNCTION mapOne (p_ClientID   IN NUMBER,
                     p_OperNo     IN VARCHAR2,
                     p_ID         IN NUMBER)
        RETURN NUMBER;

    /**
    * Маппинг множества операций
    * @since 6.20.029
    * @qtest NO
    * @param p_Qty      Количество ц/б
    * @param p_OperNo   Номер операции
    * @return 0 - при успешной обработки 1 - при ошибке
    */
    FUNCTION madMapping (p_Qty IN NUMBER, p_OperNo IN VARCHAR2)
        RETURN NUMBER;

END RSHB_VTB_LOADLOTNDFL;
/