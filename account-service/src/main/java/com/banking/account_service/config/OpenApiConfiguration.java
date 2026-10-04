package com.banking.account_service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;

@Configuration
public class OpenApiConfiguration {

    @Bean
    public OpenAPI accountServiceApiDocs() {
        return new OpenAPI().info(new Info()
                .title("Account Service API")
                .description("Banking System Account Service")
                .version("1.0")
                .contact(new Contact().email("hawary.xom@gmail.com")));
    }
}
