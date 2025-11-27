declare
 cnt number;
begin
  select count(*) into cnt from user_indexes i where  i.INDEX_NAME= 'PKOWRITEOFF_IDX6' ;
  if cnt =0 then
    update pko_writeoff
    set errlist = '20000,'
    where id  in (
    select distinct max(id) over(partition by guid) mid
      from (select count(*) over(partition by w.guid) cm
                  ,sum(nvl2(w.errlist, 1, 0)) over(partition by w.guid) ce
                  ,w.*
              from pko_writeoff w) t
     where cm > 1
       and ce = 0 ) ;
       
    update pko_writeoff
    set errlist = '20000,'
    where id  in (  
       select distinct id from (
    select w2.errlist errlist2 ,t.errlist ,t.id from (
    select sum(nvl2(w.errlist,0,1)) over (partition by custodyorderid) cnt , 
           min(id) over (partition by w.guid) idf,
           count(*) over (partition by w.guid) cm ,
           w.* from pko_writeoff w
    ) t  join pko_writeoff w2 on t.idf = w2.id
    where idf != t.id
    and cnt > 1 
    and cm > 1
    and w2.errlist is not null
    and  t.errlist is null
      )) ;
      
    update pko_writeoff
    set errlist = '20000,'
    where id  in (  
       select distinct id from (
    select w2.errlist errlist2 ,t.errlist ,t.id from (
    select sum(nvl2(w.errlist,0,1)) over (partition by custodyorderid) cnt , 
           min(id) over (partition by w.custodyorderid) idf,
           count(*) over (partition by w.guid) cm ,
           w.* from pko_writeoff w
    ) t  join pko_writeoff w2 on t.idf = w2.id
    where idf = t.id
    and cnt > 1 
    and cm > 1
    and  t.errlist is null
      )) ;

    update pko_writeoff
    set errlist = '20000,'
    where id  in (  
       select distinct id from (
    select w2.errlist errlist2 ,t.errlist ,t.id from (
    select sum(nvl2(w.errlist,0,1)) over (partition by custodyorderid) cnt , 
           min(id) over (partition by w.custodyorderid) idf,
           count(*) over (partition by w.guid) cm ,
           w.* from pko_writeoff w
    ) t  join pko_writeoff w2 on t.idf = w2.id
    where idf = t.id
    and cnt > 1 
      )) ;
      execute immediate 'create unique index PKOWRITEOFF_IDX6 on PKO_WRITEOFF (nvl2(errlist,null,custodyorderid)) tablespace indx';
  end if;
end;
/

