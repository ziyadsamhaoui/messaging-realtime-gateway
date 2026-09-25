package com.ziyadsamhaoui.messagingrealtimegateway.support;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public final class TestTokens {

    public static final String ISSUER = "http://localhost:8081";

    public final KeyPair keyPair;
    public final String keyId = "test-key-1";
    public final JwtDecoder decoder;
    private final RSASSASigner signer;

    public TestTokens() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            this.keyPair = generator.generateKeyPair();
            this.signer = new RSASSASigner(keyPair.getPrivate());
            DefaultJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
            JWSKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(
                    JWSAlgorithm.RS256,
                    new ImmutableJWKSet<>(new JWKSet(new RSAKey.Builder((RSAPublicKey) keyPair.getPublic())
                            .keyID(keyId)
                            .keyUse(KeyUse.SIGNATURE)
                            .build())));
            processor.setJWSKeySelector(keySelector);
            this.decoder = new TestDecoder(processor);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public String mint(String subject) {
        return mint(subject, Instant.now().plusSeconds(600));
    }

    public String mint(String subject, Instant expiry) {
        try {
            SignedJWT jwt = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT).keyID(keyId).build(),
                    new JWTClaimsSet.Builder()
                            .subject(subject)
                            .issuer(ISSUER)
                            .expirationTime(java.util.Date.from(expiry))
                            .issueTime(java.util.Date.from(Instant.now()))
                            .build());
            jwt.sign(signer);
            return jwt.serialize();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public String mintExpired(String subject) {
        return mint(subject, Instant.now().minusSeconds(600));
    }

    public String mintWithoutSubject() {
        try {
            SignedJWT jwt = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT).keyID(keyId).build(),
                    new JWTClaimsSet.Builder()
                            .issuer(ISSUER)
                            .expirationTime(java.util.Date.from(Instant.now().plusSeconds(600)))
                            .build());
            jwt.sign(signer);
            return jwt.serialize();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public String jwksJson() {
        return new JWKSet(new RSAKey.Builder((RSAPublicKey) keyPair.getPublic())
                .keyID(keyId)
                .keyUse(KeyUse.SIGNATURE)
                .build())
                .toString();
    }

    private final class TestDecoder implements JwtDecoder {
        private final com.nimbusds.jwt.proc.JWTProcessor<SecurityContext> processor;

        private TestDecoder(com.nimbusds.jwt.proc.JWTProcessor<SecurityContext> processor) {
            this.processor = processor;
        }

        @Override
        public Jwt decode(String token) {
            try {
                com.nimbusds.jwt.JWTClaimsSet claims = processor.process(token, null);
                return new Jwt(token,
                        claims.getIssueTime().toInstant(),
                        claims.getExpirationTime().toInstant(),
                        Map.of("alg", "RS256"),
                        claims.getClaims());
            } catch (Exception e) {
                throw new org.springframework.security.oauth2.jwt.JwtValidationException(
                        "Invalid token",
                        List.of(new org.springframework.security.oauth2.core.OAuth2Error("invalid_token", e.getMessage(), null)));
            }
        }
    }
}
