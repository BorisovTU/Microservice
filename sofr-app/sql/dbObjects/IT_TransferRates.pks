CREATE OR REPLACE PACKAGE IT_TransferRates
IS

/**
 * Процедура для парсинга json файла с последующей записью ставок.
 * @param p_worklogid   необходимо для QManager, можно не использовать внутри процедуры 
 * @param p_messbody    Тело сообщения 
 * @param p_messmeta    необходимо для QManager, можно не использовать внутри процедуры 
 * @param o_msgid       необходимо для QManager, можно не использовать внутри процедуры 
 * @param o_MSGCode     необходимо для QManager, можно не использовать внутри процедуры 
 * @param o_MSGText     необходимо для QManager, можно не использовать внутри процедуры 
 * @param o_messbody    необходимо для QManager, можно не использовать внутри процедуры 
 * @param o_messmeta    необходимо для QManager, можно не использовать внутри процедуры 
 */
   PROCEDURE SetRates (p_worklogid   IN     INTEGER,
                       p_messbody    IN     CLOB,
                       p_messmeta    IN     XMLTYPE,
                       o_msgid          OUT VARCHAR2,
                       o_MSGCode        OUT INTEGER,
                       o_MSGText        OUT VARCHAR2,
                       o_messbody       OUT CLOB,
                       o_messmeta       OUT XMLTYPE);
END IT_TransferRates;
/