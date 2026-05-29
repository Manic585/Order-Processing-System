package com.example.notificationservice.config;

import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * Manually wires SqsAsyncClient and SqsTemplate so that the
 * Spring Cloud AWS auto-configuration backs off (both beans are
 * @ConditionalOnMissingBean in the auto-config).
 *
 * Credentials are resolved via DefaultCredentialsProvider — the AWS SDK's
 * standard credential chain. Locally it reads AWS_ACCESS_KEY_ID /
 * AWS_SECRET_ACCESS_KEY env vars; on EC2 it uses the Instance Profile; on
 * ECS/Fargate it uses the Task Role. No secrets are ever committed to source
 * control.
 */
@Configuration
public class SqsConfig {

    @Value("${cloud.aws.region.static}")
    private String region;

    @Bean
    public SqsAsyncClient sqsAsyncClient() {
        return SqsAsyncClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .build();
    }

    /**
     * SqsTemplate is the high-level Spring Cloud AWS 3.x API for sending
     * messages. newTemplate() uses Jackson (already on the classpath via
     * spring-boot-starter-web) for JSON serialization of the payload.
     */
    @Bean
    public SqsTemplate sqsTemplate(SqsAsyncClient sqsAsyncClient) {
        return SqsTemplate.newTemplate(sqsAsyncClient);
    }
}
