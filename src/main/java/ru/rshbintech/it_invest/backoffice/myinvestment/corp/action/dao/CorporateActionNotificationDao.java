package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCANotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.ViewCANotificationRepository;

import java.util.List;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class CorporateActionNotificationDao {
    private final ViewCANotificationRepository viewCanotificationRepository;

    public String getByCaIdAndCftId(String corporateActionIssuerId, String cftid) {
        return getByCaIdAndCftId(Long.parseLong(corporateActionIssuerId), Long.parseLong(cftid));
    }

    public String getByCaIdAndCftId(Long corporateActionIssuerId, Long cftid) {
        Optional<ViewCANotification> optional =
                viewCanotificationRepository.getFirstByCaidAndCftidOrderByCreateDateTimeDesc(corporateActionIssuerId, cftid);
        return optional.map(ViewCANotification::getPayload).orElse(null);
    }

    public List<ViewCANotification> findAllBy(String cftid, Boolean active, int limit, String sort, String from) {
        Long fromLong = null;
        if (from != null && !from.trim().isEmpty()) {
            fromLong = Long.valueOf(from);
        }

        return viewCanotificationRepository.findAllByCftidWithFilters(
                Long.parseLong(cftid),
                active,
                fromLong,
                limit
        );
    }
}
