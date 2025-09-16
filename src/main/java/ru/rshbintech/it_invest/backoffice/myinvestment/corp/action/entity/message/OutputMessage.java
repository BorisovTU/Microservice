package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.message;

import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

@Entity
@DiscriminatorValue(OutputMessage.TYPE)
public class OutputMessage extends BaseMessage{
    public static final String TYPE = "OUTPUT_TO_DIASOFT";
    @Override
    public String getMessageType() {
        return TYPE;
    }

}
