-- Добавление настроек банка в regparm, regval
declare
  idv number;
begin
  idv := RSI_RSB_REGVAL.AddRegFlagValue( 'COMMON\\EMAIL\\MAIL_USESMTPAUTH', CHR(88), CHR(0), CHR(0), 'Переключатель аутентификации SMTP при отправке почтовых сообщений', CHR(0) );
end;
/

