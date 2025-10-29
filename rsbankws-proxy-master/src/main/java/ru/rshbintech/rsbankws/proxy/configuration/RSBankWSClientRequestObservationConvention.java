package ru.rshbintech.rsbankws.proxy.configuration;

import io.micrometer.common.KeyValue;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;
import org.springframework.web.reactive.function.client.ClientRequestObservationContext;
import org.springframework.web.reactive.function.client.DefaultClientRequestObservationConvention;

import static org.springframework.web.reactive.function.client.ClientHttpObservationDocumentation.LowCardinalityKeyNames.CLIENT_NAME;

@Configuration
@SuppressWarnings("unused")
public class RSBankWSClientRequestObservationConvention extends DefaultClientRequestObservationConvention {

    /*
    Нужно учитывать, что эта настройка меняет client_name в метриках для всех WebClient глобально.
    Если в приложении будет больше одного WebClient, нужно будет разделить их имена.
     */
    @NonNull
    @Override
    protected KeyValue clientName(@NonNull ClientRequestObservationContext context) {
        return KeyValue.of(CLIENT_NAME, "RSBankWSClient");
    }

}
