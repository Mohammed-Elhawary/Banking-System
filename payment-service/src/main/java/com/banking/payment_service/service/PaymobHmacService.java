package com.banking.payment_service.service;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.stereotype.Service;

import com.banking.payment_service.config.PaymobProperties;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PaymobHmacService {

    private final PaymobProperties paymobProperties;

    @SuppressWarnings("unchecked")
    public String calculateHmac(Map<String, Object> payload) {

        Map<String, Object> obj
                = (Map<String, Object>) payload.get("obj");

        Map<String, Object> order
                = (Map<String, Object>) obj.get("order");

        Map<String, Object> sourceData
                = (Map<String, Object>) obj.get("source_data");

        String hmacString
                = String.valueOf(obj.get("amount_cents"))
                + String.valueOf(obj.get("created_at"))
                + String.valueOf(obj.get("currency"))
                + String.valueOf(obj.get("error_occured"))
                + String.valueOf(obj.get("has_parent_transaction"))
                + String.valueOf(obj.get("id"))
                + String.valueOf(obj.get("integration_id"))
                + String.valueOf(obj.get("is_3d_secure"))
                + String.valueOf(obj.get("is_auth"))
                + String.valueOf(obj.get("is_capture"))
                + String.valueOf(obj.get("is_refunded"))
                + String.valueOf(obj.get("is_standalone_payment"))
                + String.valueOf(obj.get("is_voided"))
                + String.valueOf(order.get("id"))
                + String.valueOf(obj.get("owner"))
                + String.valueOf(obj.get("pending"))
                + String.valueOf(sourceData.get("pan"))
                + String.valueOf(sourceData.get("sub_type"))
                + String.valueOf(sourceData.get("type"))
                + String.valueOf(obj.get("success"));

        try {

            Mac mac = Mac.getInstance("HmacSHA512");

            SecretKeySpec secretKey
                    = new SecretKeySpec(
                            paymobProperties
                                    .hmacSecret()
                                    .getBytes(StandardCharsets.UTF_8),
                            "HmacSHA512"
                    );

            mac.init(secretKey);

            byte[] hash
                    = mac.doFinal(
                            hmacString.getBytes(StandardCharsets.UTF_8)
                    );

            return HexFormat
                    .of()
                    .formatHex(hash);

        } catch (Exception e) {

            throw new IllegalStateException(
                    "Failed to calculate Paymob HMAC",
                    e
            );
        }
    }
}
