/* исправлено удаление комиссии. ¬месте с комиссией удал€ютс€  все примечани€ и алгоритмы комиссии.  */

DELETE FROM DSFCALCAL_DBT calcal WHERE NOT EXISTS(SELECT t_Number FROM DSFCOMISS_DBT comiss WHERE comiss.t_Number = calcal.t_CommNumber AND comiss.t_FeeType = calcal.t_FeeType )
/

DELETE FROM DNOTETEXT_DBT note WHERE note.t_ObjectType = 650 AND  note.t_DocumentID NOT IN (SELECT CONCAT(LPAD(to_char(comiss.t_feetype), 5, '0'), LPAD(to_char(comiss.t_number), 5, '0')) FROM DSFCOMISS_DBT comiss)
/
