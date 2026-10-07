package com.micro.analysis_service.entity;

import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Table("api_requests")
public class ApiRequest {

    @PrimaryKey
    ApiPrimaryKey key;

    @Column("duration_ms")
    long durationMs;

    String method;

    String path;

    int status;
}
