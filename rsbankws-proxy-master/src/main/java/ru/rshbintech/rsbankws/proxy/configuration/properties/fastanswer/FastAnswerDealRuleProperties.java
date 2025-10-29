package ru.rshbintech.rsbankws.proxy.configuration.properties.fastanswer;

import lombok.Getter;
import lombok.Setter;
import ru.rshbintech.rsbankws.proxy.model.enums.SeqType;

@Getter
@Setter
public class FastAnswerDealRuleProperties {

    private SeqType seqType;
    private String kind;

    @SuppressWarnings("unused")
    //Конструктор для Spring, чтобы он смог создать объект свойств из yml файла
    public FastAnswerDealRuleProperties() {
    }

    public FastAnswerDealRuleProperties(SeqType seqType) {
        this.seqType = seqType;
    }

    public FastAnswerDealRuleProperties(SeqType seqType, String kind) {
        this.seqType = seqType;
        this.kind = kind;
    }

}
