create or replace package it_lim_exp is

  /***************************************************************************************************\
   ”®à¬¨à®¢ ­¨¥ ä ©«  «¨¬¨â®¢ ¤«ï ¢ë£àã§ª¨ ¢ QUIK
   **************************************************************************************************
    ˆ§¬¥­¥­¨ï:
   ---------------------------------------------------------------------------------------------------
   „ â         €¢â®à            Jira                          ¯¨á ­¨¥ 
   ----------  ---------------  ---------------------------   ----------------------------------------
   17.10.2023  ‡ëª®¢ Œ.‚.       BOSS-358                      BIQ-13699.2. ‘”. â ¯ 2 - ¤®¡ ¢«¥­¨¥ ä ©«  ®£à ­¨ç¥­¨© ¯® áà®ç­®¬ã àë­ªã ¢ ®¡à ¡®âªã IPS
   05.09.2022  ‡ëª®¢ Œ.‚.       BIQ-11358                     PRJ-2146 BIQ-11358 „®¡ ¢«¥­¨¥ ¯ à ¬¥âà  LimitCount
   26.07.2022  ‡ëª®¢ Œ.‚.       BIQ-11358                     PRJ-2146 BIQ-11358 ‡ £àã§ª  ä ©«  £«®¡ «ì­ëå «¨¬¨â®¢ ¨ ¢ë£àã§ª  ®¡é¥£® ä ©«  «¨¬¨â®¢
   25.04.2022  Œ¥«¨å®¢  .‘.    BIQ-11358                     ‘®§¤ ­¨¥
  \**************************************************************************************************/
  --ã¤ «¥­¨¥ ä ©«®¢ áâ àè¥ p_file_keep_day_cnt ¤­¥©
  procedure delete_file_lim_quik(p_file_keep_day_cnt in number);

  --¯®«ãç¥­¨e ¯®á«¥¤­¥£® áä®à¬¨à®¢ ­­®£® clob-ä ©«  á «¨¬¨â ¬¨ §  ãª § ­­ãî ¤ âã
  procedure get_last_file_lim_quik(p_date      in date
                                  ,p_file_code in varchar2
                                  ,p_file_clob out clob
                                  ,p_id_file   out number
                                  ,p_note      out varchar2);

  --§ ¯¨á âì ¨­ä® ® á®åà ­¥­¨¨ ä ©«  ­  ¤¨áª
  --p_xml => '<XML file_dir="d:\quik" file_name="file_quik.txt" id_file="1227918"/> '
  function ins_file_save(p_xml in clob) return number;

  --ä®à¬¨à®¢ ­¨¥ ¢ë£àã§ª¨
  function execute_process(p_date              in date default sysdate
                          ,p_file_keep_day_cnt in number
                          ,o_messtxt           out varchar2) return number;

  --„®¡ ¢«¥­¨¥ ä ©«  £«®¡ «ì­ëå «¨¬¨â®¢
  procedure add_global_limit(p_file_dir  varchar2
                            ,p_file_name varchar2
                            ,p_file_gl   clob
                            ,o_state     out varchar2);

end it_lim_exp;
/*
  --ã¤ «¥­¨¥ ä ©«®¢
  begin
    it_lim_exp.delete_file_lim_quik(1);
  end;   
 
  --á®åà ­¥­¨¥ ä ©«®¢
  begin
    it_lim_exp.ins_file_save(p_xml => '<XML file_dir="d:\quik" file_name="file_quik.txt" id_file="1227918"/> ');
  end;
*/
/
