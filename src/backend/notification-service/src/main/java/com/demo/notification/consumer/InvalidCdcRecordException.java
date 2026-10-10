package com.demo.notification.consumer;

public class InvalidCdcRecordException extends RuntimeException {

    public InvalidCdcRecordException(String message) {
        super(message);
    }

    public InvalidCdcRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
