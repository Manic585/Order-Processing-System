package com.example.orderservice.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Service;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class JwtService {

    @Value("${app.jwt.secret}")
    private String jwtSecret;

    @Value("${app.jwt.expiration-ms}")
    private long expirationMs;

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Generates a token for the given username with no extra claims.
     * The role claim will be absent; use the overload when authorities are available.
     */
    public String generateToken(String username) {
        return buildToken(username, Map.of());
    }

    /**
     * Generates a token embedding a "role" claim populated from the provided authorities.
     * Called by AuthController, which has the Authentication object after login.
     */
    public String generateToken(String username,
                                Collection<? extends GrantedAuthority> authorities) {
        String roles = authorities.stream()
                .map(GrantedAuthority::getAuthority)
                .collect(Collectors.joining(","));
        return buildToken(username, Map.of("role", roles));
    }

    public String extractUsername(String token) {
        return extractClaim(token, Claims::getSubject);
    }

    /**
     * Returns true when the token signature is valid, the subject matches
     * userDetails, and the token has not expired.
     * parseSignedClaims() already validates signature + expiry; any JwtException
     * (including ExpiredJwtException) maps to false.
     */
    public boolean isTokenValid(String token, UserDetails userDetails) {
        try {
            Claims claims = extractAllClaims(token);
            return claims.getSubject().equals(userDetails.getUsername());
        } catch (JwtException e) {
            log.warn("event=TOKEN_INVALID username={} error={}", userDetails.getUsername(), e.getMessage());
            return false;
        }
    }

    public <T> T extractClaim(String token, Function<Claims, T> resolver) {
        return resolver.apply(extractAllClaims(token));
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private String buildToken(String username, Map<String, Object> extraClaims) {
        Date now        = new Date();
        Date expiration = new Date(now.getTime() + expirationMs);

        return Jwts.builder()
                .claims(extraClaims)           // must come before subject() to avoid overwrite
                .subject(username)
                .issuedAt(now)
                .expiration(expiration)
                .signWith(getSigningKey(), Jwts.SIG.HS256)
                .compact();
    }

    private Claims extractAllClaims(String token) {
        // JJWT 0.12.x API: Jwts.parser().verifyWith(key).build().parseSignedClaims(token)
        // parseSignedClaims() validates signature AND expiry; throws JwtException on any failure.
        return Jwts.parser()
                .verifyWith(getSigningKey())
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    private SecretKey getSigningKey() {
        // Plain-string secret from application.yml; must be ≥ 32 bytes for HS256.
        return Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8));
    }
}
