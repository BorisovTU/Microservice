create or replace package it_rcb_portf_7ep is

	/**************************************************************************************************\
   Отчет -7_ЭП_Обследование портрета клиента брокера
   **************************************************************************************************
   Изменения:
   ---------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание
   ----------  ---------------  ---------------------------   ----------------------------------------     
  05.08.2022  Зыков   М.В.     BIQ-12884                      Создание 
  */
	function get_agegrp(p_date date
										 ,p_born date) return number deterministic;

	function make_process(p_dtbegin       date
											 ,p_dtend         date
											 ,p_pack_id_begin in number
											 ,p_pack_id_end   in number) return number;

	function make_report_7ep(p_dtbegin date
													,p_dtend   date
													,p_id_pack in number) return number;

	function run(p_dtbegin date default to_date('01.01.2022', 'dd.mm.yyyy')
							,p_dtend   date default to_date('30.06.2022', 'dd.mm.yyyy')) return number;

end;
/
