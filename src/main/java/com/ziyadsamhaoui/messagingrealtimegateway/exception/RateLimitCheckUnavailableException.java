package com.ziyadsamhaoui.messagingrealtimegateway.exception;

public class RateLimitCheckUnavailableException extends RuntimeException {

    public RateLimitCheckUnavailableException(String message) {
        super(message);
    }

    public RateLimitCheckUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
