package com.micro.analysis_service.repository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import org.springframework.data.cassandra.repository.CassandraRepository;

import com.micro.analysis_service.entity.ApiPrimaryKey;
import com.micro.analysis_service.entity.ApiRequest;

public interface ApiRequestRepository extends CassandraRepository<ApiRequest, ApiPrimaryKey> {

    List<ApiRequest> findByKeyServiceAndKeyRequestDate(
            String service,
            LocalDate requestDate);

    List<ApiRequest> findByKeyServiceAndKeyRequestDateAndKeyTimestampBetween(
            String service,
            LocalDate requestDate,
            LocalDateTime from,
            LocalDateTime to);

}
