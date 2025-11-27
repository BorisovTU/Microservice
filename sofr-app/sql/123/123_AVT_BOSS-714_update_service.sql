begin
   UPDATE ITT_Q_SERVICE i
         SET i.servicename = 'CFT.NotifyBrokerDebtClient'
       WHERE i.service_proc = 'IT_CFT.WrtoffDebtSumFRomCFT';
end;
/