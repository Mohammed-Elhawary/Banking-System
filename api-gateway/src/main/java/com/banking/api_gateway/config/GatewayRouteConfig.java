package com.banking.api_gateway.config;

import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GatewayRouteConfig {

        @Bean
        public RouteLocator gatewayRoutes(RouteLocatorBuilder builder) {
                return builder.routes()
                                .route("account-service", r -> r
                                                .path("/api/v1/accounts/**")
                                                .uri("http://localhost:8081"))
                                .route("transaction-service", r -> r
                                                .path("/api/v1/transactions/**")
                                                .uri("http://localhost:8082"))
                                .route("payment-service", r -> r
                                                .path("/api/v1/payments/**")
                                                .uri("http://localhost:8084"))
                                .route("fraud-detection-service", r -> r
                                                .path("/api/v1/fraud/**")
                                                .uri("http://localhost:8083"))
                                .route("account-service-api-docs", r -> r
                                                .path("/docs/account-service/v3/api-docs")
                                                .filters(f -> f.setPath("/v3/api-docs")
                                                                .preserveHostHeader())
                                                .uri("http://localhost:8081"))
                                .route("transaction-service-api-docs", r -> r
                                                .path("/docs/transaction-service/v3/api-docs")
                                                .filters(f -> f.setPath("/v3/api-docs")
                                                                .preserveHostHeader())
                                                .uri("http://localhost:8082"))
                                .route("fraud-detection-api-docs", r -> r
                                                .path("/docs/fraud-detection-service/v3/api-docs")
                                                .filters(f -> f.setPath("/v3/api-docs")
                                                                .preserveHostHeader())
                                                .uri("http://localhost:8083"))
                                .route("payment-service-api-docs", r -> r
                                                .path("/docs/payment-service/v3/api-docs")
                                                .filters(f -> f.setPath("/v3/api-docs")
                                                                .preserveHostHeader())
                                                .uri("http://localhost:8084"))
                                .route("notification-service-api-docs", r -> r
                                                .path("/docs/notification-service/v3/api-docs")
                                                .filters(f -> f.setPath("/v3/api-docs")
                                                                .preserveHostHeader())
                                                .uri("http://localhost:8086"))
                                .build();
        }
}
