package com.banking.notification_service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;

@Configuration
public class OpenApiConfiguration {

    @Bean
    public OpenAPI notificationServiceApiDocs() {
        return new OpenAPI().info(new Info()
                .title("Notification Service API")
                .description("Banking System Notification Service")
                .version("1.0")
                .contact(new Contact().email("hawary.xom@gmail.com")));
    }
}
