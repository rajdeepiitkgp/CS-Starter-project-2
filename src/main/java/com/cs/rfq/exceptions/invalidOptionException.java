package com.cs.rfq.exceptions;

public class invalidOptionException extends RuntimeException {
    public invalidOptionException() {
    }

    public invalidOptionException(String please_enter_correct_response) {

    }

    public invalidOptionException(String message, Throwable cause) {
        super(message, cause);
    }

    public invalidOptionException(Throwable cause) {
        super(cause);
    }

    public invalidOptionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
