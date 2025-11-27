BEGIN
    EXECUTE IMMEDIATE 'alter table dsctxtotal_dbt add T_NKDREPRUB3 NUMBER(32,12)';

    EXECUTE IMMEDIATE 'alter table dsctxtotal_dbt add T_NKDREPVN NUMBER(32,12)';

    EXECUTE IMMEDIATE 'alter table dsctxtotal_dbt add T_NKDREPCHGNUMRUB NUMBER(32,12)';
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'comment on column dsctxtotal_dbt.T_NKDREPRUB3 is ''НКД в рублях''';

    EXECUTE IMMEDIATE 'comment on column dsctxtotal_dbt.T_NKDREPVN is ''НКД в валюте''';

    EXECUTE IMMEDIATE 'comment on column dsctxtotal_dbt.T_NKDREPCHGNUMRUB is ''Процентные доходы''';
EXCEPTION
    WHEN OTHERS
    THEN
        NULL;
END;
/