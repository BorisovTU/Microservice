package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto;

import com.google.common.base.Strings;

public record RequestStatus(StatusValue status, String detail, String requestId) {
  public RequestStatus {
    if (null == status) {
      throw new IllegalArgumentException("status can`t be null");
    }
    if (Strings.isNullOrEmpty(requestId)) {
      throw new IllegalArgumentException("requestId can`t be null or empty");
    }
    if (null == detail) {
      detail = "";
    }
  }

  public enum StatusValue {
    OK,
    ERROR,
    IN_PROGRESS
  }
}
