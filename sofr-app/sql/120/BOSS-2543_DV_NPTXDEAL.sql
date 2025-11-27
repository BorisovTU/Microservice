CREATE OR REPLACE FORCE VIEW DV_NPTXDEAL
(
   T_ID,
   T_DOCKIND,
   T_DOCID,
   T_DEALDATE,
   T_DEALTIME,
   T_DEALCODE,
   T_DEALCODETS,
   T_CLIENT,
   T_CLIENTNAME,
   T_CONTRACT,
   T_CONTRNUMBER,
   T_FIID,
   T_FINAME,
   T_KIND,
   T_TYPE,
   T_AMOUNT,
   T_SALE,
   T_QUANTITY,
   T_VIRGIN,
   T_PRICE,
   T_PRICEFIID,
   T_PRICEFICODE,
   T_TOTALCOST,
   T_NKD,
   T_TOTALCOSTREST,
   T_NKDREST,
   T_BUYDATE,
   T_SALEDATE,
   T_DATE1,
   T_DATE2,
   T_SORTDATE,
   T_NOTCOUNTEDONIIS
)
AS
   (SELECT lot.t_ID,
           lot.t_DocKind,
           lot.t_DocID,
           lot.t_DealDate,
           lot.t_DealTime,
           lot.t_DealCode,
           lot.t_DealCodeTS,
           lot.t_Client,
           NVL (party.t_ShortName, CHR (1)) AS t_ClientName,
           lot.t_Contract,
           NVL (contr.t_Number, CHR (1)) AS t_ContrNumber,
           lot.t_FIID,
           NVL (fi.t_Name, CHR (1)) AS t_FiName,
           lot.t_Kind,
           lot.t_Type,
           lot.t_Amount,
           lot.t_Sale,
           (lot.t_Amount - lot.t_Sale) AS t_Quantity,
           lot.t_Virgin,
           lot.t_Price,
           lot.t_PriceFIID,
           NVL (fi_ccy.t_Ccy, CHR (1)) AS t_PriceFiCode,
           lot.t_TotalCost,
           lot.t_NKD,
           (CASE WHEN (lot.t_Amount = 0) THEN 0 ELSE ROUND (lot.t_TotalCost * (lot.t_Amount - lot.t_Sale) / lot.t_Amount, 2) END)
              AS t_TotalCostRest,
           (CASE WHEN (lot.t_Amount = 0) THEN 0 ELSE ROUND (lot.t_NKD * (lot.t_Amount - lot.t_Sale) / lot.t_Amount, 2) END) AS t_NKDRest,
           lot.t_BuyDate,
           lot.t_SaleDate,
           (DECODE (lot.t_Kind,                                                                                  /*RSB_NPTXC.NPTXLOTS_SALE*/
                               2, lot.t_SaleDate,                                                             /*RSB_NPTXC.NPTXLOTS_LOANPUT*/
                                                 5, lot.t_SaleDate,                                              /*RSB_NPTXC.NPTXLOTS_REPO*/
                                                                   3, lot.t_SaleDate,  lot.t_BuyDate)) AS t_Date1,
           (DECODE (lot.t_Kind,                                                                                  /*RSB_NPTXC.NPTXLOTS_SALE*/
                               2, lot.t_BuyDate,                                                              /*RSB_NPTXC.NPTXLOTS_LOANPUT*/
                                                5, lot.t_BuyDate,                                                /*RSB_NPTXC.NPTXLOTS_REPO*/
                                                                 3, lot.t_BuyDate,  lot.t_SaleDate)) AS t_Date2,
           (DECODE (lot.t_Kind,                                                                                  /*RSB_NPTXC.NPTXLOTS_SALE*/
                               2, lot.t_BegSaleDate,  /*RSB_NPTXC.NPTXLOTS_LOANPUT*/
                                                      5, lot.t_BegSaleDate,  /*RSB_NPTXC.NPTXLOTS_REPO*/
                                                                             3, lot.t_BegSaleDate,  lot.t_BegBuyDate)) AS t_SortDate,
            lot.t_notcountedoniis
      FROM dnptxlot_dbt lot
           LEFT JOIN dparty_dbt party
              ON lot.t_Client = party.t_PartyID
           LEFT JOIN dsfcontr_dbt contr
              ON lot.t_Contract = contr.t_ID
           LEFT JOIN dfininstr_dbt fi
              ON lot.t_FIID = fi.t_FIID
           LEFT JOIN dfininstr_dbt fi_ccy
              ON lot.t_PriceFIID = fi_ccy.t_FIID
     WHERE lot.t_InAcc = 'X')
/