-- Исправление признака

declare
begin
   UPDATE DNPTXOBJ_DBT SET T_TECHNICAL = CHR(0) WHERE T_TECHNICAL IS NULL;
end;
/