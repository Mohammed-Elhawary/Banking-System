package com.micro.analysis_service.entity;

import java.time.LocalDate;
import java.time.LocalDateTime;

import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@PrimaryKeyClass
public class ApiPrimaryKey {

    @PrimaryKeyColumn(name = "service", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    String service;

    @PrimaryKeyColumn(name = "request_date", ordinal = 1, type = PrimaryKeyType.PARTITIONED)
    LocalDate requestDate;

    @PrimaryKeyColumn(name = "timestamp", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
    LocalDateTime timestamp;

    @PrimaryKeyColumn(name = "request_id", ordinal = 3, type = PrimaryKeyType.CLUSTERED)
    String requestId;

}
