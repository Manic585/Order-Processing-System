package com.example.orderservice.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * In-memory UserDetailsService with two hardcoded accounts.
 * Replace the users map with a JPA-backed lookup when the user table is ready.
 *
 * PasswordEncoder is injected from AppConfig (not SecurityConfig) to break the
 * SecurityConfig → UserDetailsService → PasswordEncoder → SecurityConfig cycle.
 */
@Slf4j
@Service
public class ApplicationUserDetailsService implements UserDetailsService {

    private final Map<String, UserDetails> users;

    public ApplicationUserDetailsService(PasswordEncoder passwordEncoder) {
        users = Map.of(
                "user1", User.withUsername("user1")
                        .password(passwordEncoder.encode("password1"))
                        .roles("USER")
                        .build(),
                "admin", User.withUsername("admin")
                        .password(passwordEncoder.encode("admin123"))
                        .roles("ADMIN")
                        .build()
        );
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        UserDetails userDetails = users.get(username);
        if (userDetails == null) {
            log.warn("event=AUTH_FAILURE username={} reason=User not found", username);
            throw new UsernameNotFoundException("User not found: " + username);
        }
        return userDetails;
    }
}
