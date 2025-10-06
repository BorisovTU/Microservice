package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@UtilityClass
@Slf4j
public final class ParseUtil {
    private static final DateTimeFormatter[] FORMATTERS = {
            DateTimeFormatter.ISO_OFFSET_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
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

    public static OffsetDateTime parseOffsetDateTime(String value, String message) {
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

        try {
            Instant instant = Instant.parse(value);
            return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            log.error("{}: {}", message, value, e);
        }

        return null;
    }
}
