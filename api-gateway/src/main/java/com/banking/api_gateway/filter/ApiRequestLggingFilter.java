package com.banking.api_gateway.filter;

import java.time.Instant;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

import com.banking.api_gateway.dto.ApiRequestEvent;
import com.banking.api_gateway.event.ApiRequestEventProducer;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

/**
 * ApiRequestLoggingFilter
 */
@Component
@RequiredArgsConstructor
public class ApiRequestLggingFilter implements GlobalFilter {

    private final ApiRequestEventProducer producer;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {

        long startTime = System.currentTimeMillis();

        return chain.filter(exchange).then(Mono.fromRunnable(

                () -> {
                    long duration = System.currentTimeMillis() - startTime;
                    Route route = exchange.getAttribute(
                            ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);

                    String service = route != null
                            ? route.getId()
                            : "unknown";
                    int status = exchange.getResponse().getStatusCode().value();
                    ApiRequestEvent event = ApiRequestEvent.builder()
                            .requestId(exchange.getRequest().getId())
                            .method(exchange.getRequest().getMethod().name())
                            .path(exchange.getRequest().getURI().getPath())
                            .service(service)
                            .status(status)
                            .durationMs(duration)
                            .timestamp(Instant.now().toString())
                            .build();

                    producer.sendToAnalysisService(event);
                }));
    }

}
