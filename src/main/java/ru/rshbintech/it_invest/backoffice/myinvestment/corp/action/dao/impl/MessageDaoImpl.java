package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.impl;

import jakarta.transaction.Transactional;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.MessageDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.message.IncomeMessage;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.MessageRepository;

@Component
public class MessageDaoImpl implements MessageDao {
    private final MessageRepository messageRepository;

    public MessageDaoImpl(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }

    @Transactional
    @Override
    public void saveIncomeDiasoftMessage(String payload) {
        IncomeMessage messageEntity = new IncomeMessage();
        messageEntity.setJson(payload);
        messageRepository.save(messageEntity);
    }
}

