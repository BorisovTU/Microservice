CREATE OR REPLACE PACKAGE Rsb_ChangeTariff
IS
  /**
   @brief Корректировка тарифной сетки существующих комиссий
  */                      
  PROCEDURE UpdateSfTarif;

  /**
   @brief Корректировка алгоритма расчета существующих комиссий
  */                      
  PROCEDURE UpdateSfCalCal;

  /**
   @brief Заведение новых комиссий
  */                      
  PROCEDURE AddComiss;

  /**
   @brief Добавление новых комиссий под тарифные планы
   @param[in]  p_ChangeDate    дата вступления изменений в силу
   @param[in]  p_TPName        наименование тарифного плана  
  */                      
  PROCEDURE LinkComissToSf(p_ChangeDate IN DATE, p_TPName IN VARCHAR2);

  /**
   @brief Исключение старых комиссий из под тарифных планов
   @param[in]  p_ChangeDate    дата вступления изменений в силу 
   @param[in]  p_TPName        наименование тарифного плана   
  */                      
  PROCEDURE ExclusionComissFromTP(p_ChangeDate IN DATE, p_TPName IN VARCHAR2);

END Rsb_ChangeTariff;