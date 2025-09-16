package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.message;

import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

@Entity
@DiscriminatorValue(ResponseFromMyInvestementMessage.TYPE)
public class ResponseFromMyInvestementMessage extends BaseMessage{
    public static final String TYPE = "RESPONSE_FROM_MY_INVESTMENT";
    @Override
    public String getMessageType() {
        return TYPE;
    }

}
