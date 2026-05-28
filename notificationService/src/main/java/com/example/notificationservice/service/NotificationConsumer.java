package com.example.notificationservice.service;

import com.example.notificationservice.dto.NotificationMessage;
import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Reads NotificationMessage objects from the three SQS queues and delivers
 * them as emails via EmailService → AWS SES.
 *
 * ── How @SqsListener works ───────────────────────────────────────────────────
 *
 * Spring Cloud AWS polls each queue continuously in a background thread.
 * When a message arrives it is deserialized from JSON into a NotificationMessage
 * record and passed to the annotated method.
 *
 * Message lifecycle:
 *   1. Message arrives in SQS → becomes INVISIBLE (visibility timeout starts)
 *   2. @SqsListener method executes
 *   3a. Method returns normally  → Spring Cloud AWS deletes the message from SQS ✅
 *   3b. Method throws exception  → message becomes VISIBLE again after timeout → retry
 *
 * If the same message fails maxReceiveCount times (configured on the queue),
 * SQS automatically moves it to the Dead Letter Queue for manual inspection.
 *
 * ── Email address resolution ─────────────────────────────────────────────────
 *
 * The NotificationMessage carries a customerId, not an email address.
 * resolveEmail() is a placeholder that must be replaced with a real lookup:
 *
 *   Option A — REST call to a User Service:
 *     return userServiceClient.getUser(customerId).email();
 *
 *   Option B — Direct DB query (if a users table is accessible):
 *     return userRepository.findById(customerId)
 *                          .map(User::getEmail)
 *                          .orElseThrow(...);
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationConsumer {

    private final EmailService emailService;

    // ── Order Confirmed ───────────────────────────────────────────────────────

    @SqsListener("${notification.queue.order-confirmed}")
    public void handleOrderConfirmed(NotificationMessage message) {
        log.info("event=SQS_CONSUMED type=CONFIRMED orderId={} customerId={}",
                message.orderId(), message.customerId());

        String to      = resolveEmail(message.customerId());
        String subject = "Your order has been confirmed!";
        String text    = buildPlainText("Order Confirmed", message.message());
        String html    = buildHtml("Order Confirmed ✅", message.orderId(), message.message(),
                                   "#2e7d32");  // green

        emailService.sendEmail(to, subject, text, html);
    }

    // ── Order Cancelled ───────────────────────────────────────────────────────

    @SqsListener("${notification.queue.order-cancelled}")
    public void handleOrderCancelled(NotificationMessage message) {
        log.info("event=SQS_CONSUMED type=CANCELLED orderId={} customerId={}",
                message.orderId(), message.customerId());

        String to      = resolveEmail(message.customerId());
        String subject = "Your order has been cancelled";
        String text    = buildPlainText("Order Cancelled", message.message());
        String html    = buildHtml("Order Cancelled", message.orderId(), message.message(),
                                   "#e65100");  // orange

        emailService.sendEmail(to, subject, text, html);
    }

    // ── Order Failed ──────────────────────────────────────────────────────────

    @SqsListener("${notification.queue.order-failed}")
    public void handleOrderFailed(NotificationMessage message) {
        log.info("event=SQS_CONSUMED type=FAILED orderId={} customerId={}",
                message.orderId(), message.customerId());

        String to      = resolveEmail(message.customerId());
        String subject = "There was a problem with your order";
        String text    = buildPlainText("Order Failed", message.message());
        String html    = buildHtml("Order Could Not Be Completed", message.orderId(),
                                   message.message(), "#c62828");  // red

        emailService.sendEmail(to, subject, text, html);
    }

    // ── Email address resolution ──────────────────────────────────────────────

    /**
     * TODO: Replace with a real user-service call or DB lookup.
     *
     * LOCAL SANDBOX: Both the From and To addresses must be individually
     * verified in AWS SES. Hardcoded to the verified address for local testing.
     * Remove the hardcode and implement the real lookup before going to production.
     */
    private String resolveEmail(String customerId) {
        // Sandbox testing — replace with your own verified SES email address
        return "manickamletchu@gmail.com";
    }

    // ── Email body builders ───────────────────────────────────────────────────

    /**
     * Plain-text fallback body — used by basic email clients and spam filters.
     */
    private String buildPlainText(String heading, String body) {
        return heading + "\n\n" + body + "\n\nThank you for shopping with us.";
    }

    /**
     * Minimal but clean HTML body.
     * accentColor controls the heading colour to visually distinguish
     * confirmed (green), cancelled (orange), and failed (red) emails.
     */
    private String buildHtml(String heading, String orderId, String body, String accentColor) {
        return """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                  <meta charset="UTF-8"/>
                  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
                </head>
                <body style="margin:0;padding:0;background:#f5f5f5;font-family:Arial,sans-serif;">
                  <table width="100%%" cellpadding="0" cellspacing="0">
                    <tr>
                      <td align="center" style="padding:40px 0;">
                        <table width="600" cellpadding="0" cellspacing="0"
                               style="background:#ffffff;border-radius:8px;
                                      box-shadow:0 2px 8px rgba(0,0,0,0.1);">
                          <tr>
                            <td style="padding:32px 40px;border-bottom:4px solid %s;">
                              <h2 style="margin:0;color:%s;font-size:22px;">%s</h2>
                            </td>
                          </tr>
                          <tr>
                            <td style="padding:32px 40px;">
                              <p style="margin:0 0 16px;color:#333;font-size:15px;">%s</p>
                              <p style="margin:0;color:#757575;font-size:13px;">
                                Order ID: <strong>%s</strong>
                              </p>
                            </td>
                          </tr>
                          <tr>
                            <td style="padding:24px 40px;background:#fafafa;
                                       border-top:1px solid #eeeeee;
                                       border-radius:0 0 8px 8px;">
                              <p style="margin:0;color:#9e9e9e;font-size:12px;">
                                This is an automated message. Please do not reply.
                              </p>
                            </td>
                          </tr>
                        </table>
                      </td>
                    </tr>
                  </table>
                </body>
                </html>
                """.formatted(accentColor, accentColor, heading, body, orderId);
    }
}
