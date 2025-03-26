package com.temesoft.fs;

/**
 * Exception signifying that an error happened during file storage action
 */
public class FileStorageException extends RuntimeException {

    /**
     * Constructs a new exception with the specified detail message and cause.
     */
    public FileStorageException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the specified detail message
     */
    public FileStorageException(final String message) {
        super(message);
    }
}
