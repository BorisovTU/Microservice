DECLARE 
  planID number := 0;
  com_number_1 number := 0;
  parent_number_1 number := 0;
  com_number_2 number := 0;
  parent_number_2 number := 0;
  com_number_3 number := 0;
  parent_number_3 number := 0;
  com_number_4 number := 0;
  parent_number_4 number := 0;
  com_number_5 number := 0;
  parent_number_5 number := 0;
  com_number_6 number := 0;
  parent_number_6 number := 0;
  com_number_7 number := 0;
  parent_number_7 number := 0;

  PROCEDURE CopySFTarif(p_planID in number, p_com_number in number, p_parent_number in number, p_plan_name in varchar2, p_com_name in varchar2)
  IS
    com_tarsclID number(10);
    parent_tarsclID number(10);
  BEGIN
    IF p_com_number > 0 THEN
      BEGIN
         SELECT tarscl.T_ID
           INTO com_tarsclID
           FROM DSFCONCOM_DBT concom, DSFTARSCL_DBT tarscl
          WHERE concom.t_feetype = 1 
            AND concom.t_commnumber = p_com_number 
            AND concom.t_objectid = p_planID 
            AND concom.t_objecttype = 57 
            AND tarscl.t_concomID = concom.t_ID;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            com_tarsclID := 0;
            DBMS_OUTPUT.put_line('К ТП "'||p_plan_name||'" не привязана/привязана с ошибкой комиссия "'||p_com_name||'"!');
      END;

      BEGIN
         SELECT tarscl.T_ID
           INTO parent_tarsclID
           FROM DSFCONCOM_DBT concom, DSFTARSCL_DBT tarscl
          WHERE concom.t_feetype = 1 
            AND concom.t_commnumber = p_parent_number 
            AND concom.t_objectid = p_planID 
            AND concom.t_objecttype = 57 
            AND tarscl.t_concomID = concom.t_ID;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            parent_tarsclID := 0;
            DBMS_OUTPUT.put_line('На ТП "'||p_plan_name||'" нет соответствующей комиссии в EUR для копирования тарифной сетки в "'||p_com_name||'"!');
      END;

      IF com_tarsclID > 0 THEN
        DELETE FROM DSFTARIF_DBT WHERE T_TARSCLID = com_tarsclID;

        INSERT INTO DSFTARIF_DBT (T_ID,T_TARSCLID,T_SIGN,T_BASETYPE,T_BASESUM,T_TARIFTYPE,
                                  T_TARIFSUM,T_MINVALUE,T_MAXVALUE,T_SORT)
             SELECT 0,com_tarsclID,T_SIGN,T_BASETYPE,T_BASESUM,T_TARIFTYPE,
                                  T_TARIFSUM,T_MINVALUE,T_MAXVALUE,T_SORT
               FROM DSFTARIF_DBT WHERE T_TARSCLID = parent_tarsclID;
                 
        COMMIT;
      END IF;
    END IF; 
  END;

BEGIN

  select T_NUMBER into com_number_1 from dsfcomiss_dbt where T_CODE like ('ПАОМскБирж_CNY');
  select T_NUMBER into parent_number_1 from dsfcomiss_dbt where T_CODE like ('ПАОМскБирж_EUR');
  select T_NUMBER into com_number_2 from dsfcomiss_dbt where T_CODE like ('РЕПО_CNY');
  select T_NUMBER into parent_number_2 from dsfcomiss_dbt where T_CODE like ('РЕПО_EUR');
  select T_NUMBER into com_number_3 from dsfcomiss_dbt where T_CODE like ('СПЕЦРЕПО_CNY');
  select T_NUMBER into parent_number_3 from dsfcomiss_dbt where T_CODE like ('СПЕЦРЕПО_EUR');
  select T_NUMBER into com_number_4 from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржБаз_CNY');
  select T_NUMBER into parent_number_4 from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржБаз_EUR');
  select T_NUMBER into com_number_5 from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПерв_CNY');
  select T_NUMBER into parent_number_5 from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПерв_EUR');
  select T_NUMBER into com_number_6 from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПервРСХБ_CNY');
  select T_NUMBER into parent_number_6 from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПервРСХБ_EUR');
  select T_NUMBER into com_number_7 from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПервСТОР_CNY');
  select T_NUMBER into parent_number_7 from dsfcomiss_dbt where T_CODE like ('ПАОМскБиржПервСТОР_EUR');

  /*ТП "Базовый"*/ 
  select T_SFPLANID into planID from dsfplan_dbt where T_NAME like ('Базовый');
  if planID > 0 then
    CopySFTarif(planID, com_number_1, parent_number_1, 'Базовый', 'ПАОМскБирж_CNY');
    CopySFTarif(planID, com_number_2, parent_number_2, 'Базовый', 'РЕПО_CNY');
    CopySFTarif(planID, com_number_3, parent_number_3, 'Базовый', 'СПЕЦРЕПО_CNY');
  end if;

  /*ТП "Профессиональный"*/ 
  planID := 0;
  select T_SFPLANID into planID from dsfplan_dbt where T_NAME like ('Профессиональный');
  if planID > 0 then
    CopySFTarif(planID, com_number_1, parent_number_1, 'Профессиональный', 'ПАОМскБирж_CNY');
    CopySFTarif(planID, com_number_2, parent_number_2, 'Профессиональный', 'РЕПО_CNY');
    CopySFTarif(planID, com_number_3, parent_number_3, 'Профессиональный', 'СПЕЦРЕПО_CNY');
  end if;

  /*ТП "Трейдер"*/ 
  planID := 0;
  select T_SFPLANID into planID from dsfplan_dbt where T_NAME like ('Трейдер');
  if planID > 0 then
    CopySFTarif(planID, com_number_2, parent_number_2, 'Трейдер', 'РЕПО_CNY');
    CopySFTarif(planID, com_number_3, parent_number_3, 'Трейдер', 'СПЕЦРЕПО_CNY');
    CopySFTarif(planID, com_number_4, parent_number_4, 'Трейдер', 'ПАОМскБиржБаз_CNY');
    CopySFTarif(planID, com_number_6, parent_number_6, 'Трейдер', 'ПАОМскБиржПервРСХБ_CNY');
    CopySFTarif(planID, com_number_7, parent_number_7, 'Трейдер', 'ПАОМскБиржПервСТОР_CNY');
  end if;

  /*ТП "Базовый (для финансовых институтов)"*/ 
  select T_SFPLANID into planID from dsfplan_dbt where T_NAME like ('Базовый (для финансовых институтов)');
  if planID > 0 then
    CopySFTarif(planID, com_number_1, parent_number_1, 'Базовый (для финансовых институтов)', 'ПАОМскБирж_CNY');
    CopySFTarif(planID, com_number_2, parent_number_2, 'Базовый (для финансовых институтов)', 'РЕПО_CNY');
    CopySFTarif(planID, com_number_3, parent_number_3, 'Базовый (для финансовых институтов)', 'СПЕЦРЕПО_CNY');
  end if;

  /*ТП "Инвестор"*/ 
  planID := 0;
  select T_SFPLANID into planID from dsfplan_dbt where T_NAME like ('Инвестор');
  if planID > 0 then
    CopySFTarif(planID, com_number_2, parent_number_2, 'Инвестор', 'РЕПО_CNY');
    CopySFTarif(planID, com_number_3, parent_number_3, 'Инвестор', 'СПЕЦРЕПО_CNY');
    CopySFTarif(planID, com_number_4, parent_number_4, 'Инвестор', 'ПАОМскБиржБаз_CNY');
    CopySFTarif(planID, com_number_6, parent_number_6, 'Инвестор', 'ПАОМскБиржПервРСХБ_CNY');
    CopySFTarif(planID, com_number_7, parent_number_7, 'Инвестор', 'ПАОМскБиржПервСТОР_CNY');
  end if;

  /*ТП "Базовый (для малого, среднего и микробизнеса)"*/ 
  select T_SFPLANID into planID from dsfplan_dbt where T_NAME like ('Базовый (для малого, среднего и микробизнеса)');
  if planID > 0 then
    CopySFTarif(planID, com_number_1, parent_number_1, 'Базовый (для малого, среднего и микробизнеса)', 'ПАОМскБирж_CNY');
    CopySFTarif(planID, com_number_2, parent_number_2, 'Базовый (для малого, среднего и микробизнеса)', 'РЕПО_CNY');
    CopySFTarif(planID, com_number_3, parent_number_3, 'Базовый (для малого, среднего и микробизнеса)', 'СПЕЦРЕПО_CNY');
  end if;

  /*ТП "Базовый (для крупного бизнеса)"*/ 
  select T_SFPLANID into planID from dsfplan_dbt where T_NAME like ('Базовый (для крупного бизнеса)');
  if planID > 0 then
    CopySFTarif(planID, com_number_1, parent_number_1, 'Базовый (для крупного бизнеса)', 'ПАОМскБирж_CNY');
    CopySFTarif(planID, com_number_2, parent_number_2, 'Базовый (для крупного бизнеса)', 'РЕПО_CNY');
    CopySFTarif(planID, com_number_3, parent_number_3, 'Базовый (для крупного бизнеса)', 'СПЕЦРЕПО_CNY');
  end if;

  /*ТП "Профессиональный (для финансовых институтов)"*/ 
  select T_SFPLANID into planID from dsfplan_dbt where T_NAME like ('Профессиональный (для финансовых институтов)');
  if planID > 0 then
    CopySFTarif(planID, com_number_1, parent_number_1, 'Профессиональный (для финансовых институтов)', 'ПАОМскБирж_CNY');
    CopySFTarif(planID, com_number_2, parent_number_2, 'Профессиональный (для финансовых институтов)', 'РЕПО_CNY');
    CopySFTarif(planID, com_number_3, parent_number_3, 'Профессиональный (для финансовых институтов)', 'СПЕЦРЕПО_CNY');
  end if;

  /*ТП "Малый и средний бизнес"*/ 
  planID := 0;
  select T_SFPLANID into planID from dsfplan_dbt where T_NAME like ('Малый и средний бизнес');
  if planID > 0 then
    CopySFTarif(planID, com_number_2, parent_number_2, 'Малый и средний бизнес', 'РЕПО_CNY');
    CopySFTarif(planID, com_number_3, parent_number_3, 'Малый и средний бизнес', 'СПЕЦРЕПО_CNY');
    CopySFTarif(planID, com_number_4, parent_number_4, 'Малый и средний бизнес', 'ПАОМскБиржБаз_CNY');
    CopySFTarif(planID, com_number_6, parent_number_6, 'Малый и средний бизнес', 'ПАОМскБиржПервРСХБ_CNY');
    CopySFTarif(planID, com_number_7, parent_number_7, 'Малый и средний бизнес', 'ПАОМскБиржПервСТОР_CNY');
  end if;

  /*ТП "Крупный бизнес"*/ 
  planID := 0;
  select T_SFPLANID into planID from dsfplan_dbt where T_NAME like ('Крупный бизнес');
  if planID > 0 then
    CopySFTarif(planID, com_number_2, parent_number_2, 'Крупный бизнес', 'РЕПО_CNY');
    CopySFTarif(planID, com_number_3, parent_number_3, 'Крупный бизнес', 'СПЕЦРЕПО_CNY');
    CopySFTarif(planID, com_number_4, parent_number_4, 'Крупный бизнес', 'ПАОМскБиржБаз_CNY');
    CopySFTarif(planID, com_number_6, parent_number_6, 'Крупный бизнес', 'ПАОМскБиржПервРСХБ_CNY');
    CopySFTarif(planID, com_number_7, parent_number_7, 'Крупный бизнес', 'ПАОМскБиржПервСТОР_CNY');
  end if;

  /*ТП "Финансовые институты"*/ 
  planID := 0;
  select T_SFPLANID into planID from dsfplan_dbt where T_NAME like ('Финансовые институты');
  if planID > 0 then
    CopySFTarif(planID, com_number_2, parent_number_2, 'Финансовые институты', 'РЕПО_CNY');
    CopySFTarif(planID, com_number_3, parent_number_3, 'Финансовые институты', 'СПЕЦРЕПО_CNY');
    CopySFTarif(planID, com_number_4, parent_number_4, 'Финансовые институты', 'ПАОМскБиржБаз_CNY');
    CopySFTarif(planID, com_number_5, parent_number_5, 'Финансовые институты', 'ПАОМскБиржПерв_CNY');
  end if;

END;