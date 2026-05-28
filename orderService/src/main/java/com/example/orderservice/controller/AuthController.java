package com.example.orderservice.controller;

import com.example.orderservice.dto.AuthRequest;
import com.example.orderservice.dto.AuthResponse;
import com.example.orderservice.security.JwtService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthenticationManager authenticationManager;
    private final JwtService jwtService;

    @Value("${app.jwt.expiration-ms}")
    private long expirationMs;

    /**
     * Authenticates the user and returns a signed JWT.
     *
     * POST /api/auth/login
     * Body: {"username":"user1","password":"password1"}
     *
     * Success (200): {"token":"eyJ...","expiresIn":86400000}
     * Failure (401): empty body
     */
    @PostMapping("/login")
    public ResponseEntity<AuthResponse> login(@Valid @RequestBody AuthRequest request) {
        try {
            Authentication authentication = authenticationManager.authenticate(
                    new UsernamePasswordAuthenticationToken(
                            request.username(), request.password()));

            String token = jwtService.generateToken(
                    request.username(), authentication.getAuthorities());

            log.info("event=AUTH_SUCCESS username={}", request.username());
            return ResponseEntity.ok(new AuthResponse(token, expirationMs));

        } catch (AuthenticationException e) {
            log.warn("event=AUTH_FAILURE username={} reason={}", request.username(), e.getMessage());
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
    }
}
