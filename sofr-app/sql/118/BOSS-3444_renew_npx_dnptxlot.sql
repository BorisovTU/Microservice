CREATE OR REPLACE FORCE VIEW V_NPX_DNPTXLOT
AS
( select lot.*,
         (case WHEN (lot.t_DocKind = 101)
               THEN 1010
               WHEN (lot.t_DocKind = 117)
               THEN 1020
               WHEN (lot.t_DocKind = 127)
               THEN 1070
               WHEN (lot.t_DocKind IN (135, 139))
               THEN 1030
               WHEN (lot.t_DocKind = 0)
               THEN 1080
          ELSE 0
          END
         ) AS t_AnaliticKind1,
         (case WHEN (lot.t_DocKind = 0)
               THEN lot.t_ID
          ELSE lot.t_DocID
          END
         ) AS t_Analitic1
    from dnptxlot_dbt lot
)
/
