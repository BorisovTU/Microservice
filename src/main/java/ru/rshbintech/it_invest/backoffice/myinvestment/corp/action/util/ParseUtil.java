package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util;

import jakarta.validation.ValidationException;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.time.LocalDate;

@UtilityClass
@Slf4j
public final class ParseUtil {
    public static void validateLong(String payload, String message) {
        if (payload != null) {
            try {
                Long.parseLong(payload);
            } catch (NumberFormatException e) {
                throw new ValidationException(message);
            }
        }
    }

    public static String toString(Object val) {
        if (val != null) {
            return val.toString();
        }
        return null;
    }

    public static Long parseLong(String payload, String message) {
        if (StringUtils.hasText(payload)) {
            try {
                return Long.parseLong(payload);
            } catch (NumberFormatException e) {
                log.error(message, payload);
            }
        }
        return null;
    }

    public static LocalDate parseLocalDate(String payload, String message) {
        if (StringUtils.hasText(payload)) {
            try {
                return LocalDate.parse(payload);
            } catch (Exception e) {
                log.error(message, payload);
            }
        }
        return null;
    }
}
