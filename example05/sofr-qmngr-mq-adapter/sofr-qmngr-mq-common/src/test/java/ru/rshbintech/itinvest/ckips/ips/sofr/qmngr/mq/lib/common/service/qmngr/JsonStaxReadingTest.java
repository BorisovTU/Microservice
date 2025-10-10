package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.qmngr;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import java.io.File;
import java.util.Optional;
import lombok.SneakyThrows;

public class JsonStaxReadingTest {
  public static void main(String[] args) throws Exception {
    JsonFactory jf = JsonFactory.builder()
            .enable(JsonReadFeature.ALLOW_JAVA_COMMENTS)
            .build();

    try (
            var jp = jf.createParser(new File("C:/users/someo/Downloads/MESSBODY.json"))
    ) {
      var reqGuid = getReqGuid(jp);
      System.out.println("GUIDReq:" + reqGuid.orElse("not found"));
    }

  }

  @SneakyThrows
  private static Optional<String> getReqGuid(JsonParser jp) {
    if (jp.nextToken() != JsonToken.START_OBJECT) {
      return Optional.empty();
    }
    while (jp.nextToken() != null) {
     /* switch (jp.currentToken().) {
        case JsonToken.FIELD_NAME -> {};
      }*/
      var currentName = jp.currentName();
      jp.nextToken();
      var value = jp.getValueAsString();
      // System.out.println(currentName +" : "+ value);
      if ("GUIDReq".equals(currentName)) {
        return Optional.ofNullable(value);
      }
    }
    return Optional.empty();
  }
}
