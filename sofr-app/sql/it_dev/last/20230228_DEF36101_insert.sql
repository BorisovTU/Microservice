begin
  delete itt_Q_SETTINGS where Qset_Name in ('DEBUG','QMANAGER_MESSPACK_COUNT','QWORKER_MINFREE_FOR_S','QWORKER_F_COUNT','QWORKER_PLUS_INTERVAL','QWORKER_MINUS_INTERVAL');
  insert into itt_Q_SETTINGS(Qset_Name, Value_Number, Qset_Comment) values('QMANAGER_MESSPACK_COUNT', 999999, 'Размер пакета обработки сообщений ( 1 N сообщение  гарантировано пройдет в 1 пакет )Если нет бытрых синхронов лучше установить большое число'); 
  insert into itt_Q_SETTINGS(Qset_Name, Value_Number, Qset_Comment) values('QWORKER_MINFREE_FOR_S', 0, 'Минимальное кол-во свободных worker оставленных для синхронных сообщений (Заполняем свободные воркеры асинхроном до ... последних)Если синхронов нет = 0 ');
  insert into itt_Q_SETTINGS(Qset_Name, Value_Number, Qset_Comment) values('QWORKER_F_COUNT', 0, 'Кол-во обработчиков для быстрых сообщений. Если синхронов F нет = 0 ');
  insert into itt_Q_SETTINGS(Qset_Name, Value_Number, Qset_Comment) values('QWORKER_PLUS_INTERVAL', 0, 'Добавление работников не чаще (сек) 0-сразу по необходимости ');
  insert into itt_Q_SETTINGS(Qset_Name, Value_Number, Qset_Comment) values('QWORKER_MINUS_INTERVAL', 30, 'Отключение работников не чаще (сек) ');

  delete from itt_q_service t where t.message_type = 'R'  and t.servicename = 'Test.1';
  insert into itt_q_service
    (message_type
    ,servicename
    ,service_proc
    ,service_caption
    ,stop_apostq)
  values
    ('R'
    ,'Test.1'
    ,'it_q_service.test1'
    ,'Сервис для тестирования обмена сообщениями'
    ,0);
  commit;
end;
/
