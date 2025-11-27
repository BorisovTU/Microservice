begin
  --à è¨àï¥¬ ¯®«¥ Market ¯®¤ ­ §¢ ­¨¥ ¬¥áâ  åà ­¥­¨ï ¢¬¥áâ® ãá«®¢­®£® âí£  
  execute immediate 'alter table u_depoacc_tradeplace modify t_market varchar2(124 byte)';

  delete from u_depoacc_tradeplace;

  --á®¡áâ¢¥­­® ¢áâ ¢«ï¥¬ §­ ç¥­¨ï
  INSERT ALL
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '6939', '€ ‘  ­ª', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '6336', 'Š € „', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '3939', '€ ‘  ­ª', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '2727', 'Š € „', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '2727', '€ ‘  ­ª', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '8383', 'Š € „', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '8383', '€ ‘  ­ª', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '3636', 'Š € „', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '3636', '€ ‘  ­ª', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '2323', 'Š € „', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '2323', '€ ‘  ­ª', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '3838', 'Š € „', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '3838', '€ ‘  ­ª', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '3637', 'Š € „', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '6436', 'Š € „', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '0027', 'Š € „', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '0027', '€ ‘  ­ª', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '3950', '€ ‘  ­ª', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '0050', 'Š € „', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '0050', '€ ‘  ­ª', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '3650', 'Š € „', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '3650', '€ ‘  ­ª', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '3610', 'Š € „', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '1010', '€ ‘  ­ª', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '0023', 'Š € „', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('T', '0023', '€ ‘  ­ª', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('D', '3636', 'Š € „', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('D', '3939', '€ ‘  ­ª', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('D', '0027', 'Š € „', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('D', '0027', '€ ‘  ­ª', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('D', '8383', 'Š € „', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('D', '8383', '€ ‘  ­ª', '_ä')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('D', '0023', 'Š € „', '_s')
  INTO u_depoacc_tradeplace(t_depoacc_firstletter, t_depoacc_middlenumber, t_market, t_agreement_suffix) VALUES ('D', '0023', '€ ‘  ­ª', '_ä')

  SELECT * FROM dual;
  
  commit;
exception when others then
  it_log.log('®è¨¡ª  ¯à¨ ®¡­®¢«¥­¨¨ â ¡«¨æë u_depoacc_tradeplace');
end;
