package com.example.notificationservice.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sesv2.SesV2Client;

/**
 * Wires a synchronous SesV2Client for transactional email delivery.
 *
 * Why synchronous (SesV2Client) instead of async (SesV2AsyncClient)?
 *   Email sending inside an @SqsListener is already on a background thread
 *   managed by Spring Cloud AWS. Using a synchronous client here keeps the
 *   error-handling model simple: if sendEmail() throws, the exception
 *   propagates naturally up to the listener, and Spring Cloud AWS handles
 *   retry/DLQ routing accordingly.
 *
 * Credentials follow the same DefaultCredentialsProvider chain as SqsConfig:
 *   - Locally  → AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY env vars
 *   - On EC2   → EC2 Instance Profile (IAM Role)
 *   - On ECS   → ECS Task Role
 * The same IAM identity used for SQS simply needs ses:SendEmail added to it.
 */
@Configuration
public class SesConfig {

    @Value("${cloud.aws.region.static}")
    private String region;

    @Value("${cloud.aws.credentials.profile-name}")
    private String profileName;

    @Bean
    public SesV2Client sesV2Client() {
        return SesV2Client.builder()
                .region(Region.of(region))
                .credentialsProvider(ProfileCredentialsProvider.create(profileName))
                .build();
    }
}
