package ru.rshbintech.rsbankws.proxy.model.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.lang.NonNull;
import ru.rshbintech.rsbankws.proxy.model.exception.IncorrectInputXmlDataException;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
@RequiredArgsConstructor
public enum ActionType {

    NEW("1", "N"),
    UPDATE("2", "U"),
    DELETE("3", "D");

    private static final Map<String, ActionType> ACTION_TYPES_MAP =
            Arrays.stream(ActionType.values())
                    .collect(Collectors.toUnmodifiableMap(ActionType::getValue, Function.identity()));

    private final String value;
    private final String condorValue;

    public static ActionType getActionType(@NonNull String actionTypeStringValue) {
        final ActionType actionType = ACTION_TYPES_MAP.get(actionTypeStringValue);
        if (actionType == null) {
            throw new IncorrectInputXmlDataException(
                    String.format("Неизвестный тип запроса с ActionType = %s", actionTypeStringValue)
            );
        }
        return actionType;
    }

}
