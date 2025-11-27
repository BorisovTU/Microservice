CREATE OR REPLACE PACKAGE RSB_712
-- Форма 712
IS
    PROCEDURE DS712MOVE (
      p_SessionID IN VARCHAR2,
      p_DateEnd   IN VARCHAR2);

    PROCEDURE DS712IIS (
      p_SessionID IN VARCHAR2,
      p_DateBeg   IN VARCHAR2,
      p_DateEnd   IN VARCHAR2);

   PROCEDURE DS712IIS_RSHB (
      p_SessionID IN VARCHAR2,
      p_DateBeg   IN DATE,
      p_DateEnd   IN DATE);

    PROCEDURE DS712SumByFIID (
      p_SessionID IN VARCHAR2,
      p_DateEnd   IN VARCHAR2);

    PROCEDURE DelD712MOVE (p_SessionID IN VARCHAR2);

    PROCEDURE DelD712SumByFIID (p_SessionID IN VARCHAR2);

    PROCEDURE DelD712IIS (p_SessionID IN VARCHAR2);

END;
/
