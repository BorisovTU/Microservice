package ru.rshbintech.rsbankws.proxy.configuration.properties.fastanswer;

import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import ru.rshbintech.rsbankws.proxy.model.enums.SeqType;

import java.util.Map;
import java.util.Set;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.fast-answer")
public class FastAnswerProperties {

    private static final FastAnswerDealRuleProperties DEFAULT_FAST_ANSWER_CASH_DEAL_RULE =
            new FastAnswerDealRuleProperties(SeqType.CASH);
    private static final FastAnswerDealRuleProperties DEFAULT_FAST_ANSWER_SEC_DEAL_RULE =
            new FastAnswerDealRuleProperties(SeqType.SECURITIES);

    /**
     * Список методов XML MethodCall, для которых нужно дать быстрый ответ.
     */
    private Set<String> methods = Set.of("RunMacro.ws_ProcessDeals.ProcessDeals");
    /**
     * Список правил для сделок XML ProcessDeals, для которых нужно дать быстрый ответ.
     */
    private Map<String, FastAnswerDealRuleProperties> dealRules;
    /**
     * Название хранимой процедуры для генерации нового id посредством sequence и вставки информации в
     * буферную таблицу СОФР.
     */
    @NotEmpty
    private String condorSofrBufferTableInsertStoredProcName = "IT_INTEGRATION.Condor_GetLastSOFRSequenceDeal";

}
