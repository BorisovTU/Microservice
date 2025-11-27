
CREATE OR REPLACE PACKAGE RSI_NPTMSG IS

/**
 * Вывод сообщения об ошибке */
   PROCEDURE PutMsg( pType IN NUMBER, pMessage IN VARCHAR2 );

/**
 * Вывод сообщения об ошибке в автономной транзакции (для использования с RSI_NPTO.SetError) */   
   PROCEDURE PutMsgAutonom( pType IN NUMBER, pMessage IN VARCHAR2 );

/**
 * Начинает запись в протокол для шага. */
   PROCEDURE SaveOperLog( pDocID IN NUMBER, pAction IN NUMBER, pNotDel IN NUMBER DEFAULT 0, pType IN NUMBER DEFAULT -1 );

/**
 * Откатывает протокол для шага. */
   PROCEDURE RecoilOperLog( pDocID IN NUMBER, pAction IN NUMBER, pType IN NUMBER DEFAULT -1 );

/**
 * Добавить сообщение для записи СНОБ во временную таблицу */
   PROCEDURE AddTbMesTMP(p_TBID IN NUMBER, p_Type IN NUMBER, p_Message IN VARCHAR2);

/**
 * Созранить сообщения записи СНОБ в постоянную таблицу из временной */
   PROCEDURE SaveTbMes(p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER);

/**
 * Откатывает сообщения записей СНОБ для шага. */
   PROCEDURE RecoilTbMes(p_ID_Operation IN NUMBER, p_ID_Step IN NUMBER);

END RSI_NPTMSG;
/
