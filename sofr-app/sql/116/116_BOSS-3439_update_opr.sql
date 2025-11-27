
DECLARE
   v_CntContr   NUMBER;
   v_ContrID    NUMBER;
   v_number     VARCHAR2 (40);
   v_error      VARCHAR2 (200);
BEGIN
   FOR one_rec IN (SELECT *
                     FROM DNPTXOP_DBT dd
                    WHERE T_DOCKIND = 4605 AND T_KIND_OPERATION = 2035 AND T_IIS = 'X' AND T_CONTRACT = 0)
   LOOP
      v_CntContr := 0;
      v_ContrID := 0;
      v_error := ' ';
      v_number := ' ';

      SELECT COUNT (1)
        INTO v_CntContr
        FROM (SELECT DISTINCT OBJ.T_ANALITIC6
                FROM DNPTXOBDC_DBT odc, DNPTXOBJ_DBT obj
               WHERE t_docid = one_rec.t_id AND ODC.T_OBJID = OBJ.T_OBJID AND OBJ.T_ANALITICKIND6 = 6020);

      IF (v_CntContr > 1) THEN
         SELECT COUNT (1)
           INTO v_CntContr
           FROM (SELECT DISTINCT MP.T_DLCONTRID
                   FROM DDLCONTRMP_DBT MP
                  WHERE MP.t_sfcontrid IN
                           ( (SELECT DISTINCT OBJ.T_ANALITIC6
                                FROM DNPTXOBDC_DBT odc, DNPTXOBJ_DBT obj
                               WHERE     odc.t_docid = one_rec.t_id
                                     AND ODC.T_OBJID = OBJ.T_OBJID
                                     AND OBJ.T_ANALITICKIND6 = 6020
                                     AND EXISTS
                                            (SELECT 1
                                               FROM dsfcontr_Dbt
                                              WHERE     t_id = OBJ.T_ANALITIC6
                                                    AND t_datebegin <= one_rec.t_operdate
                                                    AND (   t_dateclose >= one_rec.t_operdate
                                                         OR t_dateclose = TO_DATE ('01010001', 'ddmmyyyy'))))));

         IF (v_CntContr > 1) THEN
            SELECT COUNT (1)
              INTO v_CntContr
              FROM (SELECT DISTINCT MP.T_DLCONTRID
                      FROM DNPTXOBDC_DBT odc, DNPTXOBJ_DBT obj, DDLCONTRMP_DBT MP
                     WHERE     odc.t_docid = one_rec.t_id
                           AND ODC.T_OBJID = OBJ.T_OBJID
                           AND OBJ.T_ANALITICKIND6 = 6020
                           AND EXISTS
                                  (SELECT 1
                                     FROM dsfcontr_Dbt
                                    WHERE t_id = OBJ.T_ANALITIC6 AND t_datebegin <= one_rec.t_operdate AND t_dateclose = one_rec.t_operdate)
                           AND MP.t_sfcontrid = OBJ.T_ANALITIC6);

            IF (v_CntContr = 1) THEN
               SELECT DISTINCT MP.T_DLCONTRID
                 INTO v_ContrID
                 FROM DNPTXOBDC_DBT odc, DNPTXOBJ_DBT obj, DDLCONTRMP_DBT MP
                WHERE     odc.t_docid = one_rec.t_id
                      AND ODC.T_OBJID = OBJ.T_OBJID
                      AND OBJ.T_ANALITICKIND6 = 6020
                      AND EXISTS
                             (SELECT 1
                                FROM dsfcontr_Dbt
                               WHERE t_id = OBJ.T_ANALITIC6 AND t_datebegin <= one_rec.t_operdate AND t_dateclose = one_rec.t_operdate)
                      AND MP.t_sfcontrid = OBJ.T_ANALITIC6;
            ELSE
               v_error :=
                  'Определено более двух договоров, действующих на дату расчета НОБ';
            END IF;
         ELSIF (v_CntContr = 0) THEN
            v_error := 'Не определен договор, действующий на дату расчета НОБ';
         ELSE
            SELECT DISTINCT MP.T_DLCONTRID
              INTO v_ContrID
              FROM DNPTXOBDC_DBT odc, DNPTXOBJ_DBT obj, DDLCONTRMP_DBT MP
             WHERE     odc.t_docid = one_rec.t_id
                   AND ODC.T_OBJID = OBJ.T_OBJID
                   AND OBJ.T_ANALITICKIND6 = 6020
                   AND EXISTS
                          (SELECT 1
                             FROM dsfcontr_Dbt
                            WHERE     t_id = OBJ.T_ANALITIC6
                                  AND t_datebegin <= one_rec.t_operdate
                                  AND (t_dateclose >= one_rec.t_operdate OR t_dateclose = TO_DATE ('01010001', 'ddmmyyyy')))
                   AND MP.t_sfcontrid = OBJ.T_ANALITIC6;
         END IF;
      ELSIF (v_CntContr = 0) THEN
         SELECT COUNT (1)
           INTO v_CntContr
           FROM (SELECT DISTINCT MP.T_DLCONTRID
                   FROM dsfcontr_Dbt SF, DDLCONTRMP_DBT MP
                  WHERE     SF.t_partyid = one_rec.t_client
                        AND EXISTS
                               (SELECT 1
                                  FROM ddlcontrmp_dbt mp2, ddlcontr_dbt dl
                                 WHERE mp2.t_SfContrID = SF.T_ID AND dl.t_DlContrID = mp2.t_DlContrID AND dl.t_IIS = 'X' AND ROWNUM = 1)
                        AND SF.t_datebegin <= one_rec.t_operdate
                        AND (SF.t_dateclose >= one_rec.t_operdate OR SF.t_dateclose = TO_DATE ('01010001', 'ddmmyyyy'))
                        AND SF.T_ID = MP.T_SFCONTRID);

         IF (v_CntContr > 1) THEN
            SELECT COUNT (1)
              INTO v_CntContr
              FROM (SELECT DISTINCT MP.T_DLCONTRID
                      FROM dsfcontr_Dbt SF, DDLCONTRMP_DBT MP
                     WHERE     SF.t_partyid = one_rec.t_client
                           AND EXISTS
                                  (SELECT 1
                                     FROM ddlcontrmp_dbt mp2, ddlcontr_dbt dl
                                    WHERE mp2.t_SfContrID = SF.T_ID AND dl.t_DlContrID = mp2.t_DlContrID AND dl.t_IIS = 'X' AND ROWNUM = 1)
                           AND SF.t_datebegin <= one_rec.t_operdate
                           AND SF.t_dateclose = one_rec.t_operdate
                           AND SF.T_ID = MP.T_SFCONTRID);

            IF (v_CntContr = 1) THEN
               SELECT DISTINCT MP.T_DLCONTRID
                 INTO v_ContrID
                 FROM dsfcontr_Dbt SF, DDLCONTRMP_DBT MP
                WHERE     SF.t_partyid = one_rec.t_client
                      AND EXISTS
                             (SELECT 1
                                FROM ddlcontrmp_dbt mp2, ddlcontr_dbt dl
                               WHERE mp2.t_SfContrID = SF.T_ID AND dl.t_DlContrID = mp2.t_DlContrID AND dl.t_IIS = 'X' AND ROWNUM = 1)
                      AND SF.t_datebegin <= one_rec.t_operdate
                      AND SF.t_dateclose = one_rec.t_operdate
                      AND SF.T_ID = MP.T_SFCONTRID;
            ELSE
               v_error :=
                  'Определено более двух договоров, действующих на дату расчета НОБ';
            END IF;
         ELSIF (v_CntContr = 0) THEN
            SELECT COUNT (1)
              INTO v_CntContr
              FROM (SELECT DISTINCT MP.T_DLCONTRID
                      FROM dsfcontr_Dbt SF, DDLCONTRMP_DBT MP
                     WHERE     SF.t_partyid = one_rec.t_client
                           AND EXISTS
                                  (SELECT 1
                                     FROM ddlcontrmp_dbt mp2, ddlcontr_dbt dl
                                    WHERE mp2.t_SfContrID = SF.T_ID AND dl.t_DlContrID = mp2.t_DlContrID AND dl.t_IIS = 'X' AND ROWNUM = 1)
                           AND SF.t_dateclose =
                                  (SELECT MAX (SF2.t_dateclose)
                                     FROM dsfcontr_dbt SF2
                                    WHERE     SF2.t_partyid = one_rec.t_client
                                          AND SF2.t_dateclose <= one_rec.t_operdate
                                          AND SF2.t_datebegin <= one_rec.t_operdate)
                           AND SF.T_ID = MP.T_SFCONTRID);

            IF (v_CntContr = 1) THEN
               SELECT DISTINCT MP.T_DLCONTRID
                 INTO v_ContrID
                 FROM dsfcontr_Dbt SF, DDLCONTRMP_DBT MP
                WHERE     SF.t_partyid = one_rec.t_client
                      AND EXISTS
                             (SELECT 1
                                FROM ddlcontrmp_dbt mp2, ddlcontr_dbt dl
                               WHERE mp2.t_SfContrID = SF.T_ID AND dl.t_DlContrID = mp2.t_DlContrID AND dl.t_IIS = 'X' AND ROWNUM = 1)
                      AND SF.t_dateclose =
                             (SELECT MAX (SF2.t_dateclose)
                                FROM dsfcontr_dbt SF2
                               WHERE     SF2.t_partyid = one_rec.t_client
                                     AND SF2.t_dateclose <= one_rec.t_operdate
                                     AND SF2.t_datebegin <= one_rec.t_operdate)
                      AND SF.T_ID = MP.T_SFCONTRID;
            ELSE
               v_error := 'Не определен договор, действующий на дату расчета НОБ';
            END IF;
         ELSE
            SELECT DISTINCT MP.T_DLCONTRID
              INTO v_ContrID
              FROM dsfcontr_Dbt SF, DDLCONTRMP_DBT MP
             WHERE     SF.t_partyid = one_rec.t_client
                   AND EXISTS
                          (SELECT 1
                             FROM ddlcontrmp_dbt mp2, ddlcontr_dbt dl
                            WHERE mp2.t_SfContrID = SF.T_ID AND dl.t_DlContrID = mp2.t_DlContrID AND dl.t_IIS = 'X' AND ROWNUM = 1)
                   AND SF.t_datebegin <= one_rec.t_operdate
                   AND (SF.t_dateclose >= one_rec.t_operdate OR SF.t_dateclose = TO_DATE ('01010001', 'ddmmyyyy'))
                   AND SF.T_ID = MP.T_SFCONTRID;
         END IF;
      ELSE
         SELECT DISTINCT MP.T_DLCONTRID
           INTO v_ContrID
           FROM DNPTXOBDC_DBT odc, DNPTXOBJ_DBT obj, DDLCONTRMP_DBT MP
          WHERE t_docid = one_rec.t_id AND ODC.T_OBJID = OBJ.T_OBJID AND OBJ.T_ANALITICKIND6 = 6020 AND OBJ.T_ANALITIC6 = MP.t_sfcontrid;
      END IF;

      IF (v_ContrID > 0) THEN
         SELECT SF.T_NUMBER
           INTO v_number
           FROM DDLCONTR_DBT DL, dsfcontr_Dbt SF
          WHERE DL.T_DLCONTRID = v_ContrID AND SF.T_ID = DL.T_SFCONTRID;

         UPDATE DNPTXOP_DBT SET T_CONTRACT = v_ContrID WHERE t_id = one_rec.t_id; 
      END IF;

   END LOOP;
   COMMIT;
END;
/