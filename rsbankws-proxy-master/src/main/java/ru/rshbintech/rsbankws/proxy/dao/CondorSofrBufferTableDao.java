package ru.rshbintech.rsbankws.proxy.dao;

import org.apache.ibatis.annotations.Mapper;
import org.springframework.lang.NonNull;
import ru.rshbintech.rsbankws.proxy.model.storedproc.CondorGetLastSofrSequenceDealCall;

@Mapper
public interface CondorSofrBufferTableDao {

    void callCondorGetLastSofrSequenceDeal(
            @NonNull CondorGetLastSofrSequenceDealCall condorGetLastSofrSequenceDealCall);

}
