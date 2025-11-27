/*Обновление налоговых расчетов по DEF-78016 */
DECLARE
   v_cnt   NUMBER := 0;
BEGIN
   FOR one_rec
      IN (SELECT cl.*
            FROM dnptxcalc_dbt cl
           WHERE cl.t_EndDate >= TO_DATE ('01.01.2024', 'DD.MM.YYYY')
                 AND cl.t_Kind = 2
                 AND NOT EXISTS
                            (SELECT 1
                               FROM dnptxcalc_dbt cl1
                              WHERE     cl1.t_Client = cl.t_Client
                                    AND cl1.t_EndDate = cl.t_EndDate
                                    AND cl1.t_Kind = 1
                                    AND cl1.t_IIS = cl.t_IIS)
                 AND EXISTS
                        (SELECT 1
                           FROM dnptxop_dbt op
                          WHERE     op.t_Client = cl.t_Client
                                AND op.t_DocKind = 4605
                                AND op.t_IIS = cl.t_IIS
                                AND op.t_OperDate = cl.t_EndDate
                                AND op.t_Status = 2))
   LOOP
      IF one_rec.t_SubKind = 0
      THEN
         SELECT COUNT (1)
           INTO v_cnt
           FROM DNPTXCALC_DBT
          WHERE     t_Client = one_rec.t_Client
                AND t_Kind = one_rec.t_Kind
                AND t_EndDate = one_rec.t_EndDate
                AND t_IIS = one_rec.t_IIS
                AND t_SubKInd = 20;

         IF v_cnt = 0
         THEN
            UPDATE DNPTXCALC_DBT
               SET t_SubKind = 20
             WHERE     t_Client = one_rec.t_Client
                   AND t_Kind = one_rec.t_Kind
                   AND t_EndDate = one_rec.t_EndDate
                   AND t_IIS = one_rec.t_IIS
                   AND t_SubKInd = one_rec.t_SubKind;
         END IF;
      END IF;

      SELECT COUNT (1)
        INTO v_cnt
        FROM DNPTXCALC_DBT
       WHERE     t_Client = one_rec.t_Client
             AND t_Kind = 1
             AND t_EndDate = one_rec.t_EndDate
             AND t_IIS = one_rec.t_IIS
             AND t_SubKInd = 0;

      IF v_cnt = 0
      THEN
         INSERT INTO DNPTXCALC_DBT (T_KIND,
                                    T_CLIENT,
                                    T_ENDDATE,
                                    T_COUNT,
                                    T_IIS,
                                    T_SUBKIND)
              VALUES (1,
                      one_rec.t_Client,
                      one_rec.t_EndDate,
                      1,
                      one_rec.t_IIS,
                      0);
      END IF;
   END LOOP;
END;
/