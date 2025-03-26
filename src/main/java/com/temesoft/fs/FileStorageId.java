package com.temesoft.fs;

/**
 * Abstract class for standard path generation from provided ID for use with {@link FileStorageService}
 */
public abstract class FileStorageId<T> {

    public static final String SEPARATOR = "/";

    private final T value;

    public FileStorageId(final T value) {
        this.value = value;
    }

    /**
     * Method returns a relative path generated from provided ID
     */
    public abstract String generatePath();

    /**
     * Returns actual value for provided ID
     */
    public T value() {
        return value;
    }

    @Override
    public String toString() {
        return generatePath();
    }
}