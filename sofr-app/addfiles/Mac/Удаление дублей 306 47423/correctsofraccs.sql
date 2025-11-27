-- делаем бэкап и табличку лдя последующего мапинка старых и новых счетов
-- отбираем записи, привязанные к договорам срочного рынка такие, счет которых имеет привязку к другому договору.
create table dmcaccdoc_forts_dbt as
SELECT a.*, '                                    ' t_accnew, 0 t_newid
  FROM dmcaccdoc_dbt a, dsfcontr_dbt sfs
 WHERE     t_catnum IN (201, 5087)
       AND a.t_clientcontrid = sfs.t_id
       AND sfs.t_servkind = 15
       and a.t_owner <> 131997--мутная ситуация, требует отдельного разбора
       AND EXISTS
              (SELECT 1
                 FROM dmcaccdoc_dbt
                WHERE t_account = a.t_account
                     AND a.t_clientcontrid <> t_clientcontrid);
commit;
--удаляем забэкапированные 
delete from dmcaccdoc_dbt where t_id in ( select t_id from dmcaccdoc_forts_dbt );
commit;

DECLARE
   settaccid   NUMBER (10) := 0;
   tmp         NUMBER (5) := 0;
   isdebug number(5) :=0;/*1 - не удаляет, только выводит, что удалит. 0 - удаляет*/
BEGIN
   FOR i IN (SELECT * FROM dmcaccdoc_forts_dbt)
   LOOP
      settaccid := 0;
      tmp := 0;
DBMS_OUTPUT.put_line (i.t_account || '------------------'||i.t_clientcontrid);
      BEGIN
         SELECT t_setaccid INTO settaccid
           FROM dsfssi_dbt WHERE t_objecttype = 659
                AND t_objectid = LPAD (i.t_clientcontrid, 10, '0');
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            DBMS_OUTPUT.put_line (i.t_clientcontrid || ' не найден sfssi');
      END;
         DBMS_OUTPUT.put_line (2);
      IF settaccid > 0
      THEN
         SELECT COUNT (1)INTO tmp
           FROM dpmautoac_dbt  WHERE t_servicekind = 15 AND t_settaccid = settaccid;

         IF (tmp = 0)
         THEN
            DBMS_OUTPUT.put_line (i.t_clientcontrid || '   ' || settaccid|| '   не анйдены параметры выбора СПИ, хотя sfssi есть');
         ELSIF (tmp > 1)
         THEN
            DBMS_OUTPUT.put_line (i.t_clientcontrid || '   ' || settaccid               || '   найдено большего одного параметра выбора СПИ');
         END IF;

         SELECT COUNT (1)  INTO tmp
           FROM dsettacc_dbt WHERE t_account = i.t_account AND t_settaccid = settaccid;

         IF (tmp = 0)
         THEN
            DBMS_OUTPUT.put_line (i.t_clientcontrid|| '   '|| settaccid|| '   '|| i.t_account|| '  нет  СПИ по счету, какая-то дичь');
         ELSIF (tmp > 1)
         THEN
            DBMS_OUTPUT.put_line (i.t_clientcontrid|| '   '|| settaccid|| '   больше одного СПИ по счету '|| i.t_account);
         END IF;
         
          
         begin
           if isdebug = 0 then 
             delete  FROM dsfssi_dbt WHERE t_objecttype = 659 AND t_objectid = LPAD (i.t_clientcontrid, 10, '0');
             delete FROM dpmautoac_dbt  WHERE t_servicekind = 15 AND t_settaccid = settaccid;   
             delete FROM dsettacc_dbt WHERE t_account = i.t_account AND t_settaccid = settaccid;
           end if;  
         commit;
           DBMS_OUTPUT.put_line (i.t_clientcontrid|| '   '|| settaccid|| '   удалено '|| i.t_account);
         exception
         when  others then rollback;
          DBMS_OUTPUT.put_line (i.t_clientcontrid|| '   '|| settaccid|| '   откат  '|| i.t_account);
         end ;
      END IF;
   END LOOP;
END;