package com.ziyadsamhaoui.messagingrealtimegateway.stomp;

import org.springframework.security.oauth2.jwt.Jwt;

import java.security.Principal;

public class JwtPrincipal implements Principal {

    private final Jwt jwt;

    public JwtPrincipal(Jwt jwt) {
        this.jwt = jwt;
    }

    @Override
    public String getName() {
        return jwt.getSubject();
    }

    public String tokenValue() {
        return jwt.getTokenValue();
    }

    public Jwt jwt() {
        return jwt;
    }
}
