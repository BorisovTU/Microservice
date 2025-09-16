package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.message;

import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

@Entity
@DiscriminatorValue(IncomeMessage.TYPE)
public class IncomeMessage extends BaseMessage{
    public static final String TYPE = "INCOME_FROM_DIASOFT";
    @Override
    public String getMessageType() {
        return TYPE;
    }

}
