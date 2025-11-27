CREATE OR REPLACE TRIGGER "SOFR_SVERKARESTDEPOIN_BEFORE_HIST_AIR"
BEFORE INSERT ON SOFR_SVERKARESTDEPOIN
REFERENCING NEW AS New OLD AS Old
FOR EACH ROW
DECLARE
  x_Isin DDIASISIN_DBT.t_isin%type := trim(:new.ISIN);
BEGIN
  begin
    select da.t_isin into x_Isin from DAVOIRISS_DBT da
      where da.T_LSIN = x_Isin and da.T_LSIN! = da.T_ISIN
      fetch first 1 rows only;
    -- новое значение ISIN передастся следующий триггер SOFR_SVERKARESTDEPOIN_HIST_AIR
    :new.ISIN := trim(x_Isin);
  exception
    when no_data_found then
      null;
  end;
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'Error', p_msg_type => it_log.c_msg_type__error);
    it_error.clear_error_stack;
    RAISE;
END;
/