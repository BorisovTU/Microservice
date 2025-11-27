CREATE OR REPLACE PACKAGE Rsb_ChangeTariffUniversal
IS
  /**
   @brief Заведение новых комиссий
   @param[in]  p_ComissName       наименование комиссии  
   @param[in]  p_ComissComment    описание комиссии  
   @param[in]  p_ParentComissName наименование копируемой комиссии  
   @param[in]  p_TarifSum         тарифная ставка  
  */                      
  PROCEDURE AddComiss(p_ComissName IN VARCHAR2, p_ComissComment IN VARCHAR2, p_ParentComissName IN VARCHAR2, p_TarifSum IN NUMBER);

  /**
   @brief Добавление новых комиссий под тарифные планы
   @param[in]  p_ChangeDate    дата вступления изменений в силу
   @param[in]  p_TPName        наименование тарифного плана  
   @param[in]  p_ComissName    наименование комиссии  
   @param[in]  p_IsIndividual  признак индивидуальной  
  */                      
  PROCEDURE LinkComissToSf(p_ChangeDate IN DATE, p_TPName IN VARCHAR2, p_ComissName IN VARCHAR2, p_IsIndividual IN NUMBER);

END Rsb_ChangeTariffUniversal;