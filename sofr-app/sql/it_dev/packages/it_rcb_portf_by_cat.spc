create or replace package it_rcb_portf_by_cat is

  /**************************************************************************************************\
   âç¥â - ˆ­ä®à¬ æ¨ï ¯® ª®­æ¥­âà æ¨¨  ªâ¨¢®¢ ª«¨¥­â®¢ ­  ¡à®ª¥àáª®¬ ®¡á«ã¦¨¢ ­¨¨ 
           ¯® à §­ë¬ ª â¥£®à¨ï¬ ¨­¢¥áâ®à®¢ ¯® § ¯à®áã  ­ª  ®áá¨¨
   **************************************************************************************************
   ˆ§¬¥­¥­¨ï:
   ---------------------------------------------------------------------------------------------------
   „ â         €¢â®à            Jira                          ¯¨á ­¨¥ 
   ----------  ---------------  ---------------------------   ----------------------------------------
   04.04.2023  ‡ëª®¢ Œ.‚.       DEF-30097                      BIQ-11362 („®à ¡®âª¨ ®âç¥â­®áâ¨ 6 ¢ à ¬ª å BIQ-11362)
   05.08.2022  ‡ëª®¢   Œ.‚.     BIQ-12884                      „®¡ ¢«¥­ë áâ®«¡æë foreign_priz ¨ etf_priz 
   29.03.2022  Œ¥«¨å®¢  .‘.    BIQ-11362                     ‘®§¤ ­¨¥
  */
  -- à¨§­ ª ¨­®áâà ­­®£® í¬¨â¥­â 
  function get_priz_notrezident(p_isin in varchar2) return varchar2 deterministic;

  function make_process(p_repdate date) return number;

  function run(p_rep_date in date) return clob;

  function make_report_detail(p_id_rcb_portf_by_cat_pack in number
                             ,p_part                     integer) return number;

  -- “¤ «¥­¨¥ à áç¥â®¢ ¨ ®âç¥â  
  procedure clear_report(p_id_rcb_portf_by_cat_pack in number);

  --äã­ªæ¨ï ¯¥ç â¨ â ¡«¨æë(¤¥â «¨§ æ¨ï)
  function print_tab (p_table_in          in   varchar2
                      ,p_where_in          in   varchar2 default null
                      ,p_colname_like_in   in   varchar2 := '%'
                      )return clob;

end it_rcb_portf_by_cat;
/*
begin
   it_rcb_portf_by_cat.run(p_rep_date => date'2022-06-30');
end;
*/
/
