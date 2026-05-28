package com.example.notificationservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sesv2.SesV2Client;
import software.amazon.awssdk.services.sesv2.model.Body;
import software.amazon.awssdk.services.sesv2.model.Content;
import software.amazon.awssdk.services.sesv2.model.Destination;
import software.amazon.awssdk.services.sesv2.model.EmailContent;
import software.amazon.awssdk.services.sesv2.model.Message;
import software.amazon.awssdk.services.sesv2.model.SendEmailRequest;
import software.amazon.awssdk.services.sesv2.model.SendEmailResponse;
import software.amazon.awssdk.services.sesv2.model.SesV2Exception;

/**
 * Delivers transactional emails to customers via AWS SES v2.
 *
 * ── SES v2 request structure ─────────────────────────────────────────────────
 *
 *  SendEmailRequest
 *    ├── fromEmailAddress          → the verified sender address
 *    ├── destination
 *    │     └── toAddresses         → list of recipient addresses
 *    └── content
 *          └── simple (Message)
 *                ├── subject       → Content(data, charset)
 *                └── body
 *                      ├── text    → Content(data, charset)  ← plain-text fallback
 *                      └── html    → Content(data, charset)  ← rich HTML version
 *
 * Email clients display the HTML version by default. Plain-text is shown as a
 * fallback by older clients and is also used by spam filters for scoring — so
 * always include both.
 *
 * ── Error handling ───────────────────────────────────────────────────────────
 *
 * SesV2Exception is re-thrown so that the @SqsListener in NotificationConsumer
 * sees the failure. Spring Cloud AWS will then retry the SQS message up to the
 * configured maxReceiveCount before routing it to the Dead Letter Queue.
 * This is the correct behaviour for transient SES outages.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EmailService {

    private final SesV2Client sesV2Client;

    /**
     * The From address shown to the recipient.
     * This address MUST be verified in AWS SES before any email can be sent.
     * In sandbox mode, the To address must also be individually verified.
     */
    @Value("${notification.email.sender}")
    private String senderAddress;

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Sends a transactional email.
     *
     * @param toAddress  Recipient email address (must be SES-verified in sandbox)
     * @param subject    Email subject line
     * @param plainText  Plain-text body — shown by basic clients and used by spam filters
     * @param htmlBody   HTML body — shown by modern email clients (Gmail, Outlook, etc.)
     */
    public void sendEmail(String toAddress, String subject, String plainText, String htmlBody) {
        log.info("event=EMAIL_SEND_START to={} subject={}", toAddress, subject);

        try {
            SendEmailRequest request = SendEmailRequest.builder()
                    .fromEmailAddress(senderAddress)
                    .destination(Destination.builder()
                            .toAddresses(toAddress)
                            .build())
                    .content(EmailContent.builder()
                            .simple(Message.builder()
                                    .subject(Content.builder()
                                            .data(subject)
                                            .charset("UTF-8")
                                            .build())
                                    .body(Body.builder()
                                            .text(Content.builder()   // plain-text fallback
                                                    .data(plainText)
                                                    .charset("UTF-8")
                                                    .build())
                                            .html(Content.builder()   // rich HTML version
                                                    .data(htmlBody)
                                                    .charset("UTF-8")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build();

            SendEmailResponse response = sesV2Client.sendEmail(request);

            // SES assigns a unique Message ID to every accepted email.
            // Log it for traceability — you can look it up in SES delivery reports.
            log.info("event=EMAIL_SENT to={} sesMessageId={}", toAddress, response.messageId());

        } catch (SesV2Exception e) {
            // Re-throw so the @SqsListener can retry the SQS message.
            log.error("event=EMAIL_SEND_FAILED to={} statusCode={} error={}",
                    toAddress, e.statusCode(), e.awsErrorDetails().errorMessage());
            throw e;
        }
    }
}
