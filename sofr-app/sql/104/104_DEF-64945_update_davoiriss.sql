-- установка признака 'Право отказа от выплаты' на выбранных бумагах
BEGIN
  UPDATE davoiriss_dbt 
     SET t_CouponRefuseRight = CHR(0)
   WHERE t_CouponRefuseRight = CHR(88);
END;
/

BEGIN
  UPDATE davoiriss_dbt 
     SET t_CouponRefuseRight = CHR(88)
   WHERE t_ISIN IN ('RU000A0JWMZ1', -- 43403349B  RU000A0JWMZ1  РСХБ 06Т1
                    'RU000A0JWN22', -- 43503349B  RU000A0JWN22  РСХБ 07T1
                    'RU000A0JWV63', -- 43603349B  RU000A0JWV63  РСХБ 08T1
                    'RU000A0ZZ4T1', -- 42903349B  RU000A0ZZ4T1  РСХБ 01T1
                    'RU000A0ZZ505', -- 43703349B  RU000A0ZZ505  РСХБ 09T1
                    'RU000A0ZZY59', -- 44903349B  RU000A0ZZY59  РСХБ 11B1
                    'RU000A0ZZUS8', -- 47903349B  RU000A0ZZUS8  РСХБ 31T1
                    'RU000A101616');-- 40103349B002P  RU000A101616  Россельхозбанк C01E-01
END;
/