package ru.rshbintech.rsbankws.proxy.dao;

import org.apache.ibatis.annotations.Mapper;
import org.springframework.lang.NonNull;
import ru.rshbintech.rsbankws.proxy.model.storedproc.CondorGetLastSofrSequenceDealCall;

import java.util.Map;

@Mapper
public interface CondorSofrBufferTableDao {

    Map<String, Object> callCondorGetLastSofrSequenceDeal(
            @NonNull CondorGetLastSofrSequenceDealCall condorGetLastSofrSequenceDealCall);
}
