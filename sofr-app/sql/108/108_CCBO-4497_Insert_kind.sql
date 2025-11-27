DECLARE
  cnt NUMBER(10) := 0;
BEGIN
  select count(1) into cnt from DNPTXREGKIND_DBT;

  if (cnt = 0) 
  then
    insert into DNPTXREGKIND_DBT T_NPTXKIND
    select t_Element from dnptxkind_dbt where t_Code like 'MinusG%';

    insert into DNPTXREGKIND_DBT T_NPTXKIND
    select t_Element from dnptxkind_dbt where t_Level in (5) and t_Code like 'PlusG%';

    insert all
     into DNPTXREGKIND_DBT values (880)  --TXOBJ_PAIDSPECIAL
     into DNPTXREGKIND_DBT values (870)  --TXOBJ_PAIDGENERAL
     into DNPTXREGKIND_DBT values (865)  --TXOBJ_PAIDMATERIAL_IIS
     into DNPTXREGKIND_DBT values (885)  --TXOBJ_PAIDSPECIAL_IIS
     into DNPTXREGKIND_DBT values (875)  --TXOBJ_PAIDGENERAL_IIS
     into DNPTXREGKIND_DBT values (1143) --TXOBJ_PAIDBILL
     into DNPTXREGKIND_DBT values (490)  --TXOBJ_DIVPAY_SEC
     into DNPTXREGKIND_DBT values (1161) --TXOBJ_PAIDGENERAL_15_1
     into DNPTXREGKIND_DBT values (1162) --TXOBJ_PAIDGENERAL_15_2
     into DNPTXREGKIND_DBT values (1163) --TXOBJ_PAIDGENERAL_15_9
     into DNPTXREGKIND_DBT values (1151) --TXOBJ_PAIDGENERAL_15_IIS
     into DNPTXREGKIND_DBT values (860)  --TXOBJ_PAIDMATERIAL
     into DNPTXREGKIND_DBT values (1150) --TXOBJ_PAIDGENERAL_15
     into DNPTXREGKIND_DBT values (830)  --TXOBJ_BASEMATERIAL
     into DNPTXREGKIND_DBT values (835)  --TXOBJ_BASEMATERIAL_IIS
     into DNPTXREGKIND_DBT values (840)  --TXOBJ_BASEGENERAL
     into DNPTXREGKIND_DBT values (845)  --TXOBJ_BASEGENERAL_IIS
     into DNPTXREGKIND_DBT values (850)  --TXOBJ_BASESPECIAL
     into DNPTXREGKIND_DBT values (855)  --TXOBJ_BASESPECIAL_IIS
     into DNPTXREGKIND_DBT values (1152) --TXOBJ_BASEG1
     into DNPTXREGKIND_DBT values (1153) --TXOBJ_BASEG2
     into DNPTXREGKIND_DBT values (1154) --TXOBJ_BASEG3
     into DNPTXREGKIND_DBT values (1155) --TXOBJ_BASEG4
     into DNPTXREGKIND_DBT values (1156) --TXOBJ_BASEG5
     into DNPTXREGKIND_DBT values (1157) --TXOBJ_BASEG6
     into DNPTXREGKIND_DBT values (1158) --TXOBJ_BASEG7
     into DNPTXREGKIND_DBT values (1159) --TXOBJ_BASEG8
     into DNPTXREGKIND_DBT values (1160) --TXOBJ_BASEG9
    select * from dual;
    execute immediate 'COMMIT';
  end if;
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/
