-- Дылгеров
CREATE OR REPLACE PACKAGE USR_DEF34128 AS
  type cost_nc_array_by_comnumber_type is table of number
    index by pls_integer;
  PROCEDURE run;
END USR_DEF34128;
/
