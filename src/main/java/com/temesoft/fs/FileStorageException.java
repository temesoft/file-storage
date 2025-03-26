package com.temesoft.fs;

/**
 * Exception signifying that an error happened during file storage action
 */
public class FileStorageException extends RuntimeException {

    public FileStorageException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public FileStorageException(final String message) {
        super(message);
    }
}
