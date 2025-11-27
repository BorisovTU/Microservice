BEGIN
  DELETE DBANK_MSG
    WHERE t_Number = 20650;
                     
  COMMIT;
END;