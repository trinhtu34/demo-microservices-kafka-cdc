package com.demo.inventory.consumer;

public class InvalidCdcRecordException extends RuntimeException {

    public InvalidCdcRecordException(String message) {
        super(message);
    }

    public InvalidCdcRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
