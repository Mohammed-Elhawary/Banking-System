package com.banking.payment_service.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class PaymobIntentionResponse {

    private String id;

    @JsonProperty("intention_order_id")
    private String intentionOrderId;

    @JsonProperty("client_secret")
    private String clientSecret;

    @JsonProperty("special_reference")
    private String specialReference;

    private Boolean confirmed;

    private String status;
}
