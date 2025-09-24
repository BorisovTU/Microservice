package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@UtilityClass
@Slf4j
public final class ParseUtil {
    private static final DateTimeFormatter[] FORMATTERS = {
            DateTimeFormatter.ISO_OFFSET_DATE_TIME,
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ISO_LOCAL_DATE
    };

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

    public static OffsetDateTime parseOffsetDateTime(String value, String message) throws IOException {
        for (DateTimeFormatter formatter : FORMATTERS) {
            try {
                if (formatter == DateTimeFormatter.ISO_LOCAL_DATE) {
                    LocalDate localDate = LocalDate.parse(value, formatter);
                    return localDate.atStartOfDay(ZoneOffset.UTC).toOffsetDateTime();
                } else if (formatter == DateTimeFormatter.ISO_LOCAL_DATE_TIME) {
                    LocalDateTime localDateTime = LocalDateTime.parse(value, formatter);
                    return localDateTime.atOffset(ZoneOffset.UTC);
                } else {
                    return OffsetDateTime.parse(value, formatter);
                }
            } catch (DateTimeParseException ignored) {
                log.error(message, value);
            }
        }
        return null;
    }
}
