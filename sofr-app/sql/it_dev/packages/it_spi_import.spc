create or replace package it_spi_import
is 
 /**************************************************************************************************\
  BIQ-6664 / Загрузка справочника платежных инструкций СПИ
  **************************************************************************************************
  Изменения:
  ---------------------------------------------------------------------------------------------------
  Дата        Автор            Jira                             Описание 
  ----------  ---------------  ------------------------------   -------------------------------------
  31.01.2022  Мелихова О.С.    BIQ-6664 CCBO-506                Создание
 \**************************************************************************************************/
 
  --Запуск процесса
  procedure execute_process(p_xml in clob,
                            p_from_system in varchar2);
 
  --Получить длину (кол-во символов) текста результата                                                         
  function get_result_length return number;
  
  --Получить текст результата
  function get_result_text return clob;                          

end;

/* --Запуск процесса 
declare
 v_sql    clob;
 v_length number;
 v_result clob;
begin  
  v_sql := '
    <XML>
<FILE_DIR>D:\RSHB_SOFR_PSI\import\in\</FILE_DIR>
<FILE_NAME>СПИ (справочник)_170122_проверки_copy.xlsx</FILE_NAME>
<CREATE_USER>1</CREATE_USER>
<FROM_MODULE>!transfer_template.mac</FROM_MODULE>
<ROWSET>
 <ROW excel_rownum = "3">
  <PARENTGROUP>Group_8</PARENTGROUP>
  <GROUPNAME>CFT_0000_116138197430</GROUPNAME>
  <FULLNAME>LLC PRECHISTENSKIY DAIRY PRODUCT</FULLNAME>
  <ACCOUNT_NAME>BezLimit_LLC PRECHISTENSKIY DAIRY PRODUCT</ACCOUNT_NAME>
  <INSTRUMENT>FX.CROSS.RUB</INSTRUMENT>
  <SI_EXTERNAL>RSHB_RUB_1084</SI_EXTERNAL>
  <SI_INTERNAL>RUB_Standard_1</SI_INTERNAL>
  <IS_DEFAULT>Y</IS_DEFAULT>
  <DISABLED>false</DISABLED>
  <CFT_FILE_NAME>CFT_0000_116138197430</CFT_FILE_NAME>
  <CURRENCY>RUB</CURRENCY>
  <ACCOUNT>40702810935000001084</ACCOUNT>
  <BANK_NAME>North-Western branch PJSC ROSBANK</BANK_NAME>
  <BIC></BIC>
  <CORR_ACCOUNT>30101810100000000778</CORR_ACCOUNT>
  <CORR_BANK_NAME></CORR_BANK_NAME>
  <CORR_BIC></CORR_BIC>
  <SWIFT>RSBNRUMM</SWIFT>
  <ASUDR></ASUDR>
  <SPI_ID></SPI_ID>
 </ROW>
</ROWSET>
</XML>
  ';
  
    it_spi_import.execute_process(p_xml => v_sql); 
    v_length := it_spi_import.get_result_length;     
    v_result := it_spi_import.get_result_text; 
    dbms_output.putline(v_result);    
end; 

*/
/
