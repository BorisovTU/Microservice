update 
DDIASRESTDEPO_DBT 
set (value) = (721)
where 
ACCDEPOID=35720533 and ISIN=493 and 
REPORTDATE>=to_date('21.04.2022','dd.mm.yyyy')

/

update 
DDIASRESTDEPO_DBT 
set (value) = (49484)
where 
ACCDEPOID=43953458  and ISIN=6488  and 
REPORTDATE>=to_date('12.08.2022','dd.mm.yyyy')

/

update 
DDIASRESTDEPO_DBT 
set (value) = (11664)
where 
ACCDEPOID=43953458  and ISIN=6488  and 
REPORTDATE<to_date('12.08.2022','dd.mm.yyyy') and
REPORTDATE>=to_date('08.08.2022','dd.mm.yyyy')

/

update 
DDIASRESTDEPO_DBT 
set (value) = (13710)
where 
ACCDEPOID=47294756   and ISIN=6488  and 
REPORTDATE>=to_date('22.08.2022','dd.mm.yyyy')

/