package com.kaua.event.driven.domain.validation;

public record Error(String property, String message) {

    public Error(String message) {
        this("", message);
    }
}
