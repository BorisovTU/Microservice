-- добавить записи в DREGPARMTAG_DBT
INSERT INTO DREGPARMTAG_DBT(T_ID
                          , T_KEYID
                          , T_TAG1 
                          , T_TAG2 
                          , T_TAG3 
                          , T_TAG4 
                          , T_TAG5 
                          , T_TAG6 
                          , T_TAG7 
                          , T_TAG8 
                          , T_TAG9 
                          , T_TAG10
                          )
SELECT 0,
       T_KEYID,
       'TAX',
       CHR(1),
       CHR(1),
       CHR(1),
       CHR(1),
       CHR(1),
       CHR(1),
       CHR(1),
       CHR(1),
       CHR(1)
  FROM DREGPARM_DBT
 WHERE T_NAME IN ('ПРЕДЕЛЬНЫЙ_СРОК_ВОЗВРАТА_НАЛОГА', 'ВКЛ. КОМ. С ПРОД. В РАСЧ. ДОХ.', 'УЧЕТ ЗАТРАТ ПРОШЛЫХ ЛЕТ')
/