--Обновить маски категорий учета
BEGIN
   EXECUTE IMMEDIATE 'UPDATE DMCCATEG_DBT SET T_MASK=''БББББВВВКФФФФННННННН'' WHERE T_LEVELTYPE=1 AND T_CODE IN (''ОД'',''-% к погашению'',''+% к погашению'',''ОДТФ'',''Р, ссуды'')';
END;
/