package com.example.orderservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * General application beans.
 *
 * PasswordEncoder lives here — not in SecurityConfig — to prevent a circular
 * dependency chain:
 *   SecurityConfig → ApplicationUserDetailsService → PasswordEncoder → SecurityConfig
 *
 * By extracting PasswordEncoder into its own config class, Spring can fully
 * construct it before either SecurityConfig or ApplicationUserDetailsService.
 */
@Configuration
public class AppConfig {

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }
}
