package com.banking.fraud_detection_service.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FraudCheckResualt {

    private boolean fraud;
    private String reason;
}
