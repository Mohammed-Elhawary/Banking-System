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

    /*
     * CALCULATE PAYMOB HMAC
     *
     * PURPOSE:
     * - Recompute the HMAC of an incoming webhook so the callback can
     * be authenticated.
     *
     * FLOW:
     * 1. Read the nested "obj", "order" and "source_data" maps.
     * 2. Concatenate the 21 required fields into one string, in the
     * fixed order Paymob specifies.
     * 3. Sign that string with HmacSHA512 using the configured secret.
     * 4. Return the digest as a lowercase hex string.
     *
     * NOTE:
     * - Field order and the exact string built here must match Paymob's
     * own calculation, otherwise every webhook is rejected.
     * - The concatenation is null-safe because String.valueOf is used
     * throughout; a missing field becomes the literal "null".
     * - Any failure is wrapped in IllegalStateException.
     *
     * @param payload Raw webhook body as sent by Paymob.
     *
     * @return Lowercase hex digest of the HMAC-SHA512 signature.
     */
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
