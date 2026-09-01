package com.myapp.kafka.exception;

import org.springframework.stereotype.Component;

@Component
public class BusinessValidationException extends RuntimeException
{
    public BusinessValidationException(String message)
    {
        super(message);
    }
}
