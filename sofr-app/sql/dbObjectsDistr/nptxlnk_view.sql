/*Представление - связи НУ НДФЛ*/
CREATE OR REPLACE FORCE VIEW DV_NPTXLNK
(
   T_ID,
   T_CLIENT,
   T_CLIENTNAME,
   T_CONTRACT,
   T_CONTRNUMBER,
   T_FIID,
   T_FINAME,
   T_BUYID,
   T_BUYDEALCODE,
   T_BUYDEALCODETS,
   T_BUYKIND,
   T_SALEID,
   T_SALEDEALCODE,
   T_SALEDEALCODETS,
   T_SALEKIND,
   T_SOURCEID,
   T_SOURCEDEALCODE,
   T_SOURCEDEALCODETS,
   T_SOURCEKIND,
   T_TYPE,
   T_DATE,
   T_AMOUNT,
   T_SHORT,
   T_QUANTITY,
   T_VIRGIN,
   T_BENEFITID,
   T_BENEFITTYPE
)
AS
   (SELECT lnk.t_ID,
           lnk.t_Client,
           NVL (party.t_ShortName, CHR (1)) AS t_ClientName,
           lnk.t_Contract,
           NVL (contr.t_Number, CHR (1)) AS t_ContrNumber,
           lnk.t_FIID,
           NVL (fi.t_Name, CHR (1)) AS t_FIName,
           lnk.t_BuyID,
           NVL (buylot.t_DealCode, CHR (1)) AS t_BuyDealCode,
           NVL (buylot.t_DealCodeTS, CHR (1)) AS t_BuyDealCodeTS,
           NVL (buylot.t_Kind, 0) AS t_BuyKind,
           lnk.t_SaleID,
           NVL (salelot.t_DealCode, CHR (1)) AS t_SaleDealCode,
           NVL (salelot.t_DealCodeTS, CHR (1)) AS t_SaleDealCodeTS,
           NVL (salelot.t_Kind, 0) AS t_SaleKind,
           lnk.t_SourceID,
           NVL (sourcelot.t_DealCode, CHR (1)) AS t_SourceDealCode,
           NVL (sourcelot.t_DealCodeTS, CHR (1)) AS t_SourceDealCodeTS,
           NVL (sourcelot.t_Kind, 0) AS t_SourceKind,
           lnk.t_Type,
           lnk.t_Date,
           lnk.t_Amount,
           lnk.t_Short,
           (lnk.t_Amount - lnk.t_Short) AS t_Quantity,
           lnk.t_Virgin,
           lnk.t_BenefitID,
           benefit.t_BenefitType
      FROM dnptxlnk_dbt lnk
           LEFT JOIN dparty_dbt party
              ON lnk.t_Client = party.t_PartyID
           LEFT JOIN dsfcontr_dbt contr
              ON lnk.t_Contract = contr.t_ID
           LEFT JOIN dfininstr_dbt fi
              ON lnk.t_FIID = fi.t_FIID
           LEFT JOIN dnptxlot_dbt buylot
              ON lnk.t_BuyID = buylot.t_ID
           LEFT JOIN dnptxlot_dbt salelot
              ON lnk.t_SaleID = salelot.t_ID
           LEFT JOIN dnptxlot_dbt sourcelot
              ON lnk.t_SourceID = sourcelot.t_ID
           LEFT JOIN dnptxbenefits_dbt benefit
              ON benefit.t_BenefitID = lnk.t_BenefitID
    )
/