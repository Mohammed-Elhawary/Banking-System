package com.micro.analysis_service.kafka;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.micro.analysis_service.dto.ApiRequestEvent;
import com.micro.analysis_service.entity.ApiPrimaryKey;
import com.micro.analysis_service.entity.ApiRequest;
import com.micro.analysis_service.repository.ApiRequestRepository;
import com.micro.analysis_service.service.ApiMetrics;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class ApiRequestEventConsumer {

        private final ApiRequestRepository cassandraApiRequestRepository;
        private final ApiMetrics apiMetrics;

        @KafkaListener(topics = "api-request-events")
        public void consume(ApiRequestEvent event) {
                Instant instant = Instant.parse(event.getTimestamp());

                LocalDateTime timestamp = LocalDateTime.ofInstant(
                                instant,
                                ZoneOffset.UTC);
                ApiPrimaryKey apiPrimaryKey = ApiPrimaryKey.builder()
                                .service(event.getService())
                                .requestDate(timestamp.toLocalDate())
                                .timestamp(timestamp)
                                .requestId(event.getRequestId())
                                .build();

                ApiRequest apiRequest = ApiRequest.builder()
                                .key(apiPrimaryKey)
                                .durationMs(event.getDurationMs())
                                .method(event.getMethod())
                                .path(event.getPath())
                                .status(event.getStatus())
                                .build();
                cassandraApiRequestRepository.save(apiRequest);

                List<ApiRequest> requests = cassandraApiRequestRepository
                                .findByKeyServiceAndKeyRequestDateAndKeyTimestampBetween(
                                                event.getService(),
                                                timestamp.toLocalDate(),
                                                timestamp.minusMinutes(5),
                                                timestamp);
                apiMetrics.recordRequest(event);

                requests.forEach(System.out::println);
        }
}
