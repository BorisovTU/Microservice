CREATE OR REPLACE PACKAGE BODY rsb_dbo_notifications_risk IS


  /*Номер вида примечания */
 C_NOTEKIND               NUMBER := 185;
 /*Вид сообщения
  Ежегодное Уведомление по базовому стандарту
 */
 C_CONTRMSGKIND           NUMBER := 515;
 /*Статус успешной отправки сообщения */
 C_CONTRMSGSUCCESS        NUMBER := 315;
 /*Статус неудачной отправки сообщения */
 C_CONTRMSGFAILED         NUMBER := 100;
 /*Тип Справочника строк. Хранит статусы для отправленных сообщений*/
 C_TYPEALG                NUMBER := 7535;


 /**
   @brief Функция возращает текст ошибки. в формате: SQLCODE + SQLERRM + ERROR_BACKTRACE()
   @param[in] p_sqlCode Код ошибки
   @param[in] p_errMessage Email адрес
   @return Результат выполнения валидации. 1 - успех,  0 - ошибка
  */
 FUNCTION getTextError(p_sqlCode      NUMBER,
                       p_errMessage   VARCHAR2) RETURN VARCHAR2

  IS
   v_textMessage VARCHAR2(4000);
  BEGIN

    v_textMessage:= 'Error code: '||p_sqlCode||'. '|| substr(p_errMessage,0, 300);
    RETURN v_textMessage;

  END;


 /**
   @brief Функция валидации Email адреса
   @param[in] p_email Email адрес
   @return Результат выполнения валидации. 1 - успех,  0 - ошибка
  */
 FUNCTION validateEmail(p_email VARCHAR2) RETURN NUMBER DETERMINISTIC

 IS

 BEGIN

   IF(instr(p_email,'@') <> 0
      AND instr(p_email,'.') <> 0
      AND instr(p_email,'/') = 0
      AND instr(p_email,' ') = 0) then
      return 1;
   END IF;

    return 0;

 END validateEmail;

  /**
   @brief Собрать данные во временную таблицу DSOFRSENDMSGEMAIL_TMP для печати отчета
   @param[in] p_dateBegin Дата начала отчетного периода
   @param[in] p_dateEnd Дата окончания отчетного периода
   @param[in] p_isAllClient Флаг формирования отчета по всем клиентам. Если [X] - все клиенты, иначе клиенты из таблицы dset_cln_u_tmp
  */
  PROCEDURE dboNotifications_Report(p_dateBegin   IN DATE,
                                    p_dateEnd     IN DATE,
                                    p_isAllClient IN CHAR)

  IS
  BEGIN

  EXECUTE IMMEDIATE 'TRUNCATE TABLE DSOFRSENDMSGEMAIL_TMP';
    
  INSERT INTO DSOFRSENDMSGEMAIL_TMP
  SELECT ROW_NUMBER() OVER (ORDER BY to_date(to_char(dm.t_senddate,'ddmmyyyy') || to_char(dm.t_sendtime,'hh24mi'),'ddmmyyyyhh24mi')) as T_ID,
         sfn.t_number as t_ContrNumber,
         sfn.t_id as T_SFCONTRID,
         NVL(RSB_SECUR.SC_GetObjCodeOnDate(3, 101, sfn.t_partyid), CHR(1)) as T_CFTID,
         sfn.t_partyid as T_CLIENTID,
         sfn.t_datebegin as T_SFDATEBEGIN,
         dl.t_dlcontrid,
         NULL as T_NOTETEXTID,
         dm.t_id as T_DLCONTRMSGID,
         NULL as T_EMAILNOTIFYID,
         p.t_name as T_NAMECLIENT,
         dm.t_recipientemail as T_RECIPIENTEMAIL,
         dm.t_senderemail as T_SENDEREMAIL,
         decode(dm.t_sendmesstate, C_CONTRMSGSUCCESS,a.t_sznamealg ,'Не отправлено')as T_MSGTEXT,
         to_char(to_date(to_char(dm.t_senddate,'ddmmyyyy') || to_char(dm.t_sendtime,'hh24mi'),'ddmmyyyyhh24mi'),'dd.mm.yyyy hh24:mi') as T_MSGDATE,
         NULL as T_MSGSTATUS
      FROM DDLCONTRMSG_DBT dm INNER JOIN Ddlcontr_Dbt Dl ON dm.t_dlcontrid = dl.t_dlcontrid and dm.t_kind = C_CONTRMSGKIND
                              INNER JOIN Dsfcontr_Dbt Sfn  ON (Sfn.t_Id = Dl.t_Sfcontrid)
                               LEFT JOIN DPARTY_DBT P ON p.t_partyid = sfn.t_partyid
                               LEFT JOIN Dnamealg_Dbt a ON a.t_inumberalg = dm.t_sendmesstate and a.t_itypealg = C_TYPEALG
                               LEFT JOIN dset_cln_u_tmp s ON sfn.t_partyid = s.t_clientid
         WHERE sfn.t_partyid <> 1
          AND dm.t_senddate BETWEEN trunc(p_dateBegin)  AND trunc(p_dateEnd)
          AND (p_isAllClient = CHR(88) or s.t_setflag = CHR(88))
          ;

  END dboNotifications_Report;

  /**
   @brief Собрать данные во временную таблицу DSOFRSENDMSGEMAIL_TMP для отправки уведомлений
   @param[in] p_dlContrId  Id договора, 0 - грузим все договора
  */
  PROCEDURE collectDboNotifications(p_dlContrId  NUMBER DEFAULT 0)

  IS

  BEGIN

   EXECUTE IMMEDIATE 'TRUNCATE TABLE DSOFRSENDMSGEMAIL_TMP';

   INSERT INTO DSOFRSENDMSGEMAIL_TMP
   SELECT row_number() over (order by 1) as T_ID,
         sfn.t_number as t_ContrNumber,
         sfn.t_id as T_SFCONTRID,
         NULL as t_CftId,
         sfn.t_partyid as t_ClientId,
         sfn.t_datebegin as T_SFDATEBEGIN,
         dl.t_dlcontrid,
         nt.t_id as T_NOTETEXTID,
         --смотрим по максимальному ID
         (SELECT max(d.t_id) FROM DDLCONTRMSG_DBT d
           WHERE d.t_Kind = C_CONTRMSGKIND
             AND d.t_dlcontrid = dl.t_dlcontrid) as T_DLCONTRMSGID,
         NULL as T_EMAILNOTIFYID,
         p.t_shortname as T_NAMECLIENT,
         COALESCE(
           (SELECT CASE
                      WHEN validateEmail(dl.t_email) = 0
                        THEN NULL
                        ELSE dl.t_email
                   END
             FROM DUAL),
           (SELECT COALESCE(t.ContactType1,t.ContactType2,t.ContactType3)
            FROM
          (SELECT DISTINCT
                  MAX( CASE WHEN c.t_ContactType = 8 and (validateEmail(c.t_value) = 1)  then c.t_value end) over()  as ContactType1,
                  MAX( CASE WHEN c.t_ContactType = 1005 and (validateEmail(c.t_value) = 1)  then c.t_value end) over()  as ContactType2,
                  MAX( CASE WHEN c.t_ContactType not in (8,1005) and (validateEmail(c.t_value) = 1)  then c.t_value end) over() as ContactType3
                FROM dcontact_dbt c
               WHERE c.t_PartyID = sfn.t_partyid
                 AND c.t_ContactKind =  5) t),'NO') as T_RECIPIENTEMAIL,
            NULL as T_SENDEREMAIL,
            NULL as T_MSGTEXT,
            NULL as T_MSGDATE,
            case
              when nt.t_date is NULL then
                case
                  when sysdate > add_months(sfn.t_datebegin,11)
                    then 1
                    else 2
                end
              when sysdate > add_months(nt.t_date,11) then 3
              else 4
            end  T_MSGSTATUS

          FROM Ddlcontr_Dbt Dl INNER JOIN Dsfcontr_Dbt Sfn  ON (Sfn.t_Id = Dl.t_Sfcontrid)
                                LEFT JOIN DNOTETEXT_DBT nt  ON nt.t_documentid = lpad(dl.t_dlcontrid,34, '0')
                                                            AND nt.t_objecttype = RSB_SECUR.OBJTYPE_BROKERCONTR_DL
                                                            AND nt.t_notekind = C_NOTEKIND
                                                            AND SYSDATE BETWEEN nt.t_date AND nvl(nt.t_validtodate,to_date('31129999','DDMMYYYY'))
                              LEFT JOIN DPARTY_DBT P ON p.t_partyid = sfn.t_partyid
         WHERE sfn.t_partyid <> 1
           AND ((p_dlContrId = 0) or (dl.t_dlcontrid = p_dlContrId))
           AND (sfn.t_dateclose = to_date('01.01.0001','dd.mm.yyyy') or  sfn.t_dateclose >= SYSDATE)
           AND NOT EXISTS (SELECT 1
                             FROM ddlcontrmp_dbt dlc
                             INNER JOIN Dobjatcor_Dbt Atc
                             ON dlc.t_dlcontrid = Dl.t_Dlcontrid
                             AND Atc.t_Object = LPAD(dlc.t_sfcontrid, 10, '0')
                             INNER JOIN Dsfcontr_Dbt sfn1 ON dlc.t_sfcontrid = sfn1.t_id
                             WHERE Atc.t_Objecttype = RSB_SECUR.OBJTYPE_SFCONTR
                               AND Atc.t_Groupid = 7
                               AND SYSDATE BETWEEN Atc.t_Validfromdate AND Atc.t_Validtodate
                               AND sfn1.t_servkind = 1
                               AND sfn1.t_servkindsub = 8
                               AND Atc.t_Attrid = 1)               
             ;

  end collectDboNotifications;

  /**
   @brief Инсерт данных в таблицу DDLCONTRMSG_DBT
   @param[in] p_Dlcontrid  ID договора
   @param[in] p_recipientEmail  Получатель письма
   @param[in] p_senderEmail  Отправитель письма
   @param[in] p_textEmail  Текст, который содержится в письме
   @param[in] p_sendMesState  Статус отправки, успешно обработалось почтовым шлюзом?
   @param[out] p_out_msgId  Id созданного сообщения ДБО в тестовом формате, дополненное нулями слева до 34 символов
   @param[out] p_out_errMsg  Возвращает текст ошибки
   @param[out] p_out_errRes  Возвращает статус ошибки. 1 - есть ошибка при выполнении, 0 - ошибки нет
  */
  PROCEDURE insertDlContrMsg(p_Dlcontrid       Ddlcontr_Dbt.t_Dlcontrid%TYPE,
                             p_recipientEmail  VARCHAR2,
                             p_senderEmail     VARCHAR2,
                             p_textEmail       CLOB,
                             p_sendMesState    NUMBER,
                             p_out_msgId       OUT VARCHAR2,
                             p_out_errMsg      OUT VARCHAR2,
                             p_out_errRes      OUT NUMBER)

  IS
  l_msgId DDLCONTRMSG_DBT.T_ID%TYPE;
  BEGIN


    INSERT INTO DDLCONTRMSG_DBT
    (t_Dlcontrid,
     t_Kind,
     t_Createdate,
     t_Createtime,
     t_Senddate,
     t_Sendtime,
     t_Senderemail,
     t_Recipientemail,
     t_Sendmesstate,
     t_xml
     )
    VALUES(
       p_Dlcontrid,
       C_CONTRMSGKIND,
       TRUNC(SYSDATE),
       TO_DATE('01010001 ' || To_Char(SYSDATE, 'HH24MISS'), 'DDMMYYYY HH24MISS'),
       TRUNC(sysdate),
       TO_DATE('01010001' || to_char(SYSDATE,'hh24miss'),'DDMMYYYYhh24miss'),
       p_senderEmail,
       p_recipientEmail,
       CASE WHEN p_sendMesState = 1
         THEN C_CONTRMSGSUCCESS
         ELSE C_CONTRMSGFAILED
       END,
       chr(1)
     )
     RETURNING t_Id INTO l_msgId;
     
     p_out_msgId  := Lpad(l_msgId, 34, '0');  
     p_out_errMsg := CHR(1);
     p_out_errRes := 0;

  EXCEPTION
    WHEN OTHERS
      THEN
        p_out_errMsg   := getTextError(SQLCODE,SQLERRM  || chr (10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE());
        p_out_errRes   := 1;
        p_out_msgId    := CHR(1);
        insertNotifyLog('insertDlContrMsg', p_recipientemail, p_senderemail, p_out_errMsg ,p_dlcontrid);

  END insertDlContrMsg;

  /**
   @brief Инсерт данных в таблицу DNOTETEXT_DBT
   @param[in] p_dlcontrID  ID договора
   @param[in] p_recipientEmail  Получатель письма
   @param[in] p_senderEmail  Отправитель письма
   @param[out] p_out_errMsg  Возвращает текст ошибки
   @param[out] p_out_errRes  Возвращает статус ошибки. 1 - есть ошибка при выполнении, 0 - ошибки нет
  */
  PROCEDURE insertNoteText(p_dlcontrID       DDLCONTR_DBT.T_DLCONTRID%TYPE,
                           p_recipientEmail  VARCHAR2,
                           p_senderEmail     VARCHAR2,
                           p_out_errMsg      OUT VARCHAR2,
                           p_out_errRes      OUT NUMBER)

  AS
  l_cnt       NUMBER;
  BEGIN

  SELECT COUNT(1)
    INTO l_cnt
    FROM DNOTETEXT_DBT d
    WHERE d.t_documentid = LPAD(p_dlcontrID, 34, '0')
      AND d.t_objecttype = RSB_SECUR.OBJTYPE_BROKERCONTR_DL
      AND d.t_notekind = c_notekind
      AND d.t_validtodate = to_date('31129999','DDMMYYYY');

 IF(l_cnt = 0)  THEN
 
   RSB_Struct.readStruct('dnotetext_dbt');
  
   INSERT INTO DNOTETEXT_DBT  ( T_OBJECTTYPE,
                                T_DOCUMENTID,
                                T_NOTEKIND,
                                T_OPER,
                                T_DATE,
                                T_TIME,
                                T_TEXT,
                                T_VALIDTODATE,
                                T_BRANCH,
                                T_NUMSESSION )
     VALUES(RSB_SECUR.OBJTYPE_BROKERCONTR_DL,
            LPAD(p_dlcontrID, 34, '0'),
            c_notekind,
            1,
            TRUNC(sysdate),
            to_date('01010001' || to_char(sysdate,'hh24miss'),'DDMMYYYYhh24miss'),
            RSB_Struct.PutDate('t_text', rpad('0',3000, '0'), trunc(sysdate), (-1)*53),
            to_date('31129999','DDMMYYYY'),
            1,
            0);
     p_out_errMsg := ' ';
     p_out_errRes := 0;
  ELSE
    p_out_errMsg := 'Произошла ошибка при вставке замечания';
    p_out_errRes := 1;
    insertNotifyLog('insertNoteText', p_recipientemail, p_senderEmail, p_out_errMsg ,p_dlcontrID);
  END IF;

  EXCEPTION
    WHEN OTHERS
      THEN
        p_out_errMsg   := getTextError(SQLCODE,SQLERRM  || chr (10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE());
        p_out_errRes   := 1;
        insertNotifyLog('insertNoteText', p_recipientemail, p_senderEmail, p_out_errMsg ,p_dlcontrID);

  END insertNoteText;

  /**
   @brief Update данных в таблице DDLCONTRMSG_DBT, закрытие пред. примечания, перед открытием нового
   @param[in] p_noteTextID  ID примечания
   @param[in] p_Dlcontrid  ID договора
   @param[in] p_recipientEmail  Получатель письма
   @param[in] p_senderEmail  Отправитель письма
   @param[out] p_out_errMsg  Возвращает текст ошибки
   @param[out] p_out_errRes  Возвращает статус ошибки. 1 - есть ошибка при выполнении, 0 - ошибки нет
  */
  PROCEDURE updateNoteText(p_noteTextID      DNOTETEXT_DBT.T_ID%TYPE,
                           p_dlcontrID       DDLCONTR_DBT.T_DLCONTRID%TYPE,
                           p_recipientEmail  VARCHAR2,
                           p_senderEmail     VARCHAR2,
                           p_out_errMsg      OUT VARCHAR2,
                           p_out_errRes      OUT NUMBER)

  IS
  l_cnt NUMBER;
  BEGIN



  IF(nvl(p_noteTextID,0) <> 0) THEN
    SELECT COUNT(1)
      INTO l_cnt
      FROM DNOTETEXT_DBT d
     WHERE d.t_id = p_noteTextID;
  END IF;

  IF(l_cnt <> 0) THEN

   UPDATE DNOTETEXT_DBT
    SET T_VALIDTODATE = trunc(SYSDATE) - 1
   WHERE T_ID = p_noteTextID;

  END IF;

  p_out_errMsg := ' ';
  p_out_errRes := 0;

  EXCEPTION
    WHEN OTHERS
      THEN
        p_out_errMsg   := getTextError(SQLCODE,SQLERRM  || chr (10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE());
        p_out_errRes   := 1;
        insertNotifyLog('updateNoteText', p_recipientemail, p_senderEmail, p_out_errMsg ,p_dlcontrid);

  END updateNoteText;

  /**
   @brief Процедура логгирования ошибок в ходе отправки уведомлений
   @param[in] p_head  Тема письма
   @param[in] p_recipientemail  Получатель письма
   @param[in] p_senderemail  Отправитель письма
   @param[in] p_errorText  Текст ошибки
   @param[in] p_dlcontrid  ID договора
  */
  PROCEDURE insertNotifyLog(p_head           VARCHAR2,
                            p_recipientemail VARCHAR2,
                            p_senderemail    VARCHAR2,
                            p_errorText      VARCHAR2,
                            p_dlcontrid      NUMBER)

  IS
   l_text VARCHAR2(4000);
  BEGIN
    BEGIN
        SELECT '. PartyID = ' || p.t_partyid || '; ФИО = ' || p.t_name   || '; EKK = ' || c.t_code || '; DlContrID = ' || dl.t_dlcontrid
         INTO l_text   
          FROM ddlobjcode_dbt c 
          JOIN ddlcontr_dbt dl on c.t_objectid = dl.t_dlcontrid
          JOIN dsfcontr_dbt sf on sf.t_id = dl.t_sfcontrid
          JOIN dparty_dbt p on p.t_partyid = sf.t_partyid   
         WHERE c.t_objecttype = RSB_SECUR.OBJTYPE_BROKERCONTR_DL
           AND c.t_codekind = 1
           AND c.t_objectid = p_dlcontrid;
   EXCEPTION
    WHEN OTHERS
      THEN  l_text := '';      
   END;

  INSERT INTO DEMAIL_RISK_NOTIFY_DBT (T_HEAD,
                                      T_DATECREATE,
                                      T_RECIPIENTEMAIL,
                                      T_SENDEREMAIL,
                                      T_ERRORTEXT,
                                      T_DLCONTRID)
    values(p_head,
           sysdate,
           p_recipientemail,
           p_senderemail,
           p_errorText || l_text,
           p_dlcontrid);

  END insertNotifyLog;

END rsb_dbo_notifications_risk;
/