create or replace type t_tp_commiss_link as object (tp_name varchar2(100), commiss_name varchar2(100));
/
create or replace type t_tp_commiss_link_list as table of t_tp_commiss_link;
/