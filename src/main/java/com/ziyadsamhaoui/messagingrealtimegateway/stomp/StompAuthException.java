package com.ziyadsamhaoui.messagingrealtimegateway.stomp;

import com.ziyadsamhaoui.messagingrealtimegateway.exception.ErrorCode;
import org.springframework.messaging.MessagingException;

public class StompAuthException extends MessagingException {

    private final int status;
    private final ErrorCode code;

    public StompAuthException(int status, ErrorCode code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public int getStatus() {
        return status;
    }

    public ErrorCode getCode() {
        return code;
    }
}
