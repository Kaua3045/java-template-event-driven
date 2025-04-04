package com.kaua.event.driven.domain.exceptions;

public class NotFoundException extends NoStackTraceException {

    private final String identifier;
    private final long version;

    public NotFoundException(String identifier, long version, String message) {
        super(message);
        this.identifier = identifier;
        this.version = version;
    }

    public static NotFoundException withIdentifierAndVersion(String identifier, long version) {
        return new NotFoundException(
                identifier,
                version,
                "Aggregate with identifier %s and version %s was not found".formatted(identifier, version)
        );
    }

    public static NotFoundException withIdentifier(String identifier) {
        return new NotFoundException(
                identifier,
                0,
                "Aggregate with identifier %s was not found".formatted(identifier)
        );
    }

    public static NotFoundException withMessage(String message) {
        return new NotFoundException(
                "",
                0,
                message
        );
    }

    public long getVersion() {
        return version;
    }

    public String getIdentifier() {
        return identifier;
    }
}
