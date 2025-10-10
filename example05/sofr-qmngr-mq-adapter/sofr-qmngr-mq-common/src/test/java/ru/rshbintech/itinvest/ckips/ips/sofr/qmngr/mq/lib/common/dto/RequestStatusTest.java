package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class RequestStatusTest {

  @Test
  public void testNormalCreation(){
    //given
    var status = new RequestStatus(RequestStatus.StatusValue.OK, "description", "test.topic");

    //when

    //then
    assertEquals("description", status.detail());
    assertEquals(RequestStatus.StatusValue.OK, status.status());
  }

  @Test
  public void testCreateWithDefaultValue(){
    //given
    var status = new RequestStatus(RequestStatus.StatusValue.ERROR, null, "test.topic");

    //when

    //then
    assertEquals("", status.detail());
    assertEquals(RequestStatus.StatusValue.ERROR, status.status());
  }

  @Test()
  public void testNullValueError() throws Exception{
    //given
    //when
    //then
    assertThrows(IllegalArgumentException.class, () -> new RequestStatus(null, null, null));
    assertThrows(IllegalArgumentException.class, () -> new RequestStatus(RequestStatus.StatusValue.OK, null, null));
  }

}